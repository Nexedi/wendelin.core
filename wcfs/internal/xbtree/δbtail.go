// Copyright (C) 2018-2022  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

package xbtree

// ΔBtail provides BTree-level history tail.
//
// It translates ZODB object-level changes to information about which keys of
// which BTree were modified, and provides service to query that information.
// See ΔBtail class documentation for usage details.
//
//
// ΔBtail organization
//
// ΔBtail keeps raw ZODB history in ΔZtail and uses BTree-diff algorithm(*) to
// turn δZ into BTree-level diff. For each tracked BTree a separate ΔTtail is
// maintained with tree-level history in ΔTtail.vδT .
//
// Because it is very computationally expensive(+) to find out for an object to
// which BTree it belongs, ΔBtail cannot provide full BTree-level history given
// just ΔZtail with δZ changes. Due to this ΔBtail requires help from
// users, which are expected to call ΔBtail.Track(treepath) to let ΔBtail know
// that such and such ZODB objects constitute a path from root of a tree to some
// of its leaf. After Track call the objects from the path and tree keys, that
// are covered by leaf node, become tracked: from now-on ΔBtail will detect
// and provide BTree-level changes caused by any change of tracked tree objects
// or tracked keys. This guarantee can be provided because ΔBtail now knows
// that such and such objects belong to a particular tree.
//
// To manage knowledge which tree part is tracked ΔBtail uses PPTreeSubSet.
// This data-structure represents so-called PP-connected set of tree nodes:
// simply speaking it builds on some leafs and then includes parent(leaf),
// parent(parent(leaf)), etc. In other words it's a "parent"-closure of the
// leafs. The property of being PP-connected means that starting from any node
// from such set, it is always possible to reach root node by traversing
// .parent links, and that every intermediate node went-through during
// traversal also belongs to the set.
//
// A new Track request potentially grows tracked keys coverage. Due to this,
// on a query, ΔBtail needs to recompute potentially whole vδT of the affected
// tree. This recomputation is managed by "vδTSnapForTracked*" and "_rebuild"
// functions and uses the same treediff algorithm, that Update is using, but
// modulo PPTreeSubSet corresponding to δ key coverage. Update also potentially
// needs to rebuild whole vδT history, not only append new δT, because a
// change to tracked tree nodes can result in growth of tracked key coverage.
//
// Queries are relatively straightforward code that work on vδT snapshot. The
// main complexity, besides BTree-diff algorithm, lies in recomputing vδT when
// set of tracked keys changes, and in handling that recomputation in such a way
// that multiple Track and queries requests could be all served in parallel.
//
//
// Concurrency
//
// In order to allow multiple Track and queries requests to be served in
// parallel ΔBtail employs special organization of vδT rebuild process where
// complexity of concurrency is reduced to math on merging updates to vδT and
// trackSet, and on key range lookup:
//
// 1. vδT is managed under read-copy-update (RCU) discipline: before making
//    any vδT change the mutator atomically clones whole vδT and applies its
//    change to the clone. This way a query, once it retrieves vδT snapshot,
//    does not need to further synchronize with vδT mutators, and can rely on
//    that retrieved vδT snapshot will remain immutable.
//
// 2. a Track request goes through 3 states: "new", "handle-in-progress" and
//    "handled". At each state keys/nodes of the Track are maintained in:
//
//    - ΔTtail.ktrackNew and .trackNew       for "new",
//    - ΔTtail.krebuildJobs                  for "handle-in-progress", and
//    - ΔBtail.trackSet                      for "handled".
//
//    trackSet keeps nodes, and implicitly keys, from all handled Track
//    requests. For all keys, covered by trackSet, vδT is fully computed.
//
//    a new Track(keycov, path) is remembered in ktrackNew and trackNew to be
//    further processed when a query should need keys from keycov. vδT is not
//    yet providing data for keycov keys.
//
//    when a Track request starts to be processed, its keys and nodes are moved
//    from ktrackNew/trackNew into krebuildJobs. vδT is not yet providing data
//    for requested-to-be-tracked keys.
//
//    all trackSet, trackNew/ktrackNew and krebuildJobs are completely disjoint:
//
//	trackSet ^ trackNew     = ø
//	trackSet ^ krebuildJobs = ø
//	trackNew ^ krebuildJobs = ø
//
// 3. when a query is served, it needs to retrieve vδT snapshot that takes
//    related previous Track requests into account. Retrieving such snapshots
//    is implemented in vδTSnapForTracked*() family of functions: there it
//    checks ktrackNew/trackNew, and if those sets overlap with query's keys
//    of interest, run vδT rebuild for keys queued in ktrackNew.
//
//    the main part of that rebuild can be run without any locks, because it
//    does not use nor modify any ΔBtail data, and for δ(vδT) it just computes
//    a fresh full vδT build modulo retrieved ktrackNew. Only after that
//    computation is complete, ΔBtail is locked again to quickly merge in
//    δ(vδT) update back into vδT.
//
//    This organization is based on the fact that
//
//	vδT/(T₁∪T₂) = vδT/T₁ | vδT/T₂
//
//      ( i.e. vδT computed for tracked set being union of T₁ and T₂ is the
//        same as merge of vδT computed for tracked set T₁ and vδT computed
//	  for tracked set T₂ )
//
//    and that
//
//	trackSet | (δPP₁|δPP₂) = (trackSet|δPP₁) | (trackSet|δPP₂)
//
//	( i.e. tracking set updated for union of δPP₁ and δPP₂ is the same
//	  as union of tracking set updated with δPP₁ and tracking set updated
//	  with δPP₂ )
//
//    these merge properties allow to run computation for δ(vδT) and δ(trackSet)
//    independently and with ΔBtail unlocked, which in turn enables running
//    several Track/queries in parallel.
//
// 4. while vδT rebuild is being run, krebuildJobs keeps corresponding keycov
//    entry to indicate in-progress rebuild. Should a query need vδT for keys
//    from that job, it first waits for corresponding job(s) to complete.
//
// Explained rebuild organization allows non-overlapping queries/track-requests
// to run simultaneously. This property is essential to WCFS because otherwise
// WCFS would not be able to serve several non-overlapping READ requests to one
// file in parallel.
//
// --------
//
// (*) implemented in treediff.go
// (+) full database scan

//go:generate ./blib/gen-rangemap _RangedMap_RebuildJob *_RebuildJob zrangemap_rebuildjob.go

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xsync"
	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/blib"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xtail"
)

const traceΔBtail = false
const debugΔBtail = false


// ΔBtail represents tail of revisional changes to BTrees.
//
// It semantically consists of
//
//	[]δB			; rev ∈ (tail, head]
//
// where δB represents a change in BTrees space
//
//	δB:
//		.rev↑
//		{} root -> {}(key, δvalue)
//
// It covers only changes to keys from tracked subset of BTrees parts.
// In particular a key that was not explicitly requested to be tracked, even if
// it was changed in δZ, is not guaranteed to be present in δB.
//
// ΔBtail provides the following operations:
//
//   .Track(path)	- start tracking tree nodes and keys; root=path[0], keys=path[-1].(lo,hi]
//
//   .Update(δZ) -> δB				- update BTree δ tail given raw ZODB changes
//   .ForgetPast(revCut)			- forget changes ≤ revCut
//   .SliceByRev(lo, hi) -> []δB		- query for all trees changes with rev ∈ (lo, hi]
//   .SliceByRootRev(root, lo, hi) -> []δT	- query for changes of a tree with rev ∈ (lo, hi]
//   .GetAt(root, key, at) -> (value, rev)	- get root[key] @at assuming root[key] ∈ tracked
//
// where δT represents a change to one tree
//
//	δT:
//		.rev↑
//		{}(key, δvalue)
//
// An example for tracked set is a set of visited BTree paths.
// There is no requirement that tracked set belongs to only one single BTree.
//
// See also zodb.ΔTail and zdata.ΔFtail
//
//
// Concurrency
//
// ΔBtail is safe to use in single-writer / multiple-readers mode. That is at
// any time there should be either only sole writer, or, potentially several
// simultaneous readers. The table below classifies operations:
//
//	Writers:  Update, ForgetPast
//	Readers:  Track + all queries (SliceByRev, SliceByRootRev, GetAt)
//
// Note that, in particular, it is correct to run multiple Track and queries
// requests simultaneously.
type ΔBtail struct {
	// raw ZODB changes; Kept to rebuild .byRoot after new Track.
	// includes all changed objects, not only tracked ones.
	δZtail *zodb.ΔTail

	// handle to make connections to access database.
	// TODO allow client to optionally provide zconnOld/zconnNew on e.g. Update()
	db *zodb.DB // to open connections to load new/old tree|buckets

	// mu protects ΔBtail data _and_ all _ΔTtail data for all roots.
	//
	// NOTE: even though this lock is global, since _ΔTtail.vδT is updated
	// via RCU, working with retrieved vδT snapshot does not need to hold the lock.
	mu sync.Mutex

	vδBroots  []_ΔBroots         // [] (rev↑, roots changed in this rev)
	byRoot map[zodb.Oid]*_ΔTtail // {} root -> [] k/v change history; only for keys ∈ tracked subset

	// set of tracked nodes as of @head state.
	// For this set all vδT are fully computed.
	// The set of keys(nodes) that were requested to be tracked, but were
	// not yet taken into account, is kept in _ΔTtail.ktrackNew & co.
	trackSet blib.PPTreeSubSet

	// set of trees for which _ΔTtail.ktrackNew is non-empty
	trackNewRoots setOid
}

// _ΔTtail represent tail of revisional changes to one BTree.
//
// See ΔBtail documentation for details.
type _ΔTtail struct {
	// changes to tree keys; rev↑. covers keys ∈ tracked subset
	// Note: changes to vδT go through RCU - see "Concurrency" in overview.
	vδT []ΔTree

	// set of keys that were requested to be tracked in this tree,
	// but for which vδT rebuild was not yet started	as of @head
	ktrackNew blib.RangedKeySet        // {keycov}
	// set of nodes corresponding to ktrackNew		as of @head
	trackNew blib.PPTreeSubSet         // PP{nodes}

	// set of keys(nodes) for which rebuild is in progress
	krebuildJobs _RangedMap_RebuildJob // {} keycov -> job
}

// _RebuildJob represents currently in-progress vδT rebuilding job.
type _RebuildJob struct {
	trackNew blib.PPTreeSubSet // rebuilding for this trackNew
	ready    chan struct{}     // closed when job completes
	err      error
}

// _ΔBroots represents roots-only part of ΔB.
//
// It describes which trees were changed, but does not provide δkv details for changed trees.
type _ΔBroots struct {
	Rev   zodb.Tid
	Roots setOid   // which roots changed in this revision
}

// ΔB represents a change in BTrees space.
type ΔB struct {
	Rev    zodb.Tid
	ByRoot map[zodb.Oid]map[Key]ΔValue // {} root -> {}(key, δvalue)
}

// ΔTree describes changes to one BTree in one revision.
type ΔTree struct {
	Rev zodb.Tid
	KV  map[Key]ΔValue
}


// NewΔBtail creates new empty ΔBtail object.
//
// Initial tracked set is empty.
// Initial coverage is (at₀, at₀].
//
// db will be used by ΔBtail to open database connections to load data from
// ZODB when needed.
func NewΔBtail(at0 zodb.Tid, db *zodb.DB) *ΔBtail {
	return &ΔBtail{
		δZtail:        zodb.NewΔTail(at0),
		vδBroots:      nil,
		byRoot:        map[zodb.Oid]*_ΔTtail{},
		trackSet:      blib.PPTreeSubSet{},
		trackNewRoots: setOid{},
		db:            db,
	}
}

// newΔTtail creates new empty _ΔTtail object.
func newΔTtail() *_ΔTtail {
	return &_ΔTtail{
		trackNew:  blib.PPTreeSubSet{},
	}
}

// Clone returns copy of ΔBtail.
func (orig *ΔBtail) Clone() *ΔBtail {
	klon := &ΔBtail{db: orig.db}

	// δZtail
	klon.δZtail = zodb.NewΔTail(orig.Tail())
	for _, δZ := range orig.δZtail.Data() {
		klon.δZtail.Append(δZ.Rev, δZ.Changev)
	}

	// vδBroots
	klon.vδBroots = make([]_ΔBroots, 0, len(orig.vδBroots))
	for _, origδBroots := range orig.vδBroots {
		klonδBroots := _ΔBroots{
			Rev:   origδBroots.Rev,
			Roots: origδBroots.Roots.Clone(),
		}
		klon.vδBroots = append(klon.vδBroots, klonδBroots)
	}

	// byRoot
	klon.byRoot = make(map[zodb.Oid]*_ΔTtail, len(orig.byRoot))
	for root, origΔTtail := range orig.byRoot {
		klon.byRoot[root] = origΔTtail.Clone()
	}

	// trackSet, trackNewRoots
	klon.trackSet = orig.trackSet.Clone()
	klon.trackNewRoots = orig.trackNewRoots.Clone()

	return klon
}

// Clone returns copy of _ΔTtail.
func (orig *_ΔTtail) Clone() *_ΔTtail {
	klon := &_ΔTtail{}
	klon.vδT = vδTClone(orig.vδT)
	klon.trackNew = orig.trackNew.Clone()
	return klon
}

// vδTClone returns deep copy of []ΔTree.
func vδTClone(orig []ΔTree) []ΔTree {
	if orig == nil {
		return nil
	}
	klon := make([]ΔTree, 0, len(orig))
	for _, origδT := range orig {
		klonδT := ΔTree{
			Rev: origδT.Rev,
			KV:  make(map[Key]ΔValue, len(origδT.KV)),
		}
		for k, δv := range origδT.KV {
			klonδT.KV[k] = δv
		}
		klon = append(klon, klonδT)
	}
	return klon
}

// (tail, head] coverage
func (δBtail *ΔBtail) Head() zodb.Tid { return δBtail.δZtail.Head() }
func (δBtail *ΔBtail) Tail() zodb.Tid { return δBtail.δZtail.Tail() }


// ---- Track/snapshot+rebuild/Update/Forget ----

// Track adds tree path to tracked set.
//
// path[0] signifies tree root.
// path[-1] signifies leaf node.
// keycov should be key range covered by the leaf node.
//
// ΔBtail will start tracking provided tree nodes and keys ∈ keycov.
//
// All path elements must be Tree except last one which, for non-empty tree, must be Bucket.
//
// Objects in the path must be with .PJar().At() == .Head()
func (δBtail *ΔBtail) Track(nodePath []Node, keycov KeyRange) {
	head := δBtail.Head()
	for _, node := range nodePath {
		nodeAt := node.PJar().At()
		if nodeAt != head {
			panicf("node.at (@%s)  !=  δBtail.head (@%s)", nodeAt, head)
		}
	}

	path := nodePathToPath(nodePath)
	δBtail.track(path, keycov)
}

// nodePathToPath converts path from []Node to []Oid.
func nodePathToPath(nodePath []Node) (path []zodb.Oid) {
	// assert nodePath = Tree Tree ... Tree Bucket
	l := len(nodePath)
	switch {
	case l == 0:
		panic("empty path")
	case l == 1:
		// must be empty Tree
		_ = nodePath[0].(*Tree)
	default:
		// must be Tree Tree ... Tree Bucket
		for _, node := range nodePath[:l-1] {
			_ = node.(*Tree)
		}
		_ = nodePath[l-1].(*Bucket)
	}

	path = make([]zodb.Oid, l)
	for i, node := range nodePath {
		path[i] = node.POid()
	}
	return path
}

func (δBtail *ΔBtail) track(path []zodb.Oid, keycov KeyRange) {
	δBtail.mu.Lock() // TODO verify that there is no in-progress writers
	defer δBtail.mu.Unlock()

	if traceΔBtail {
		pathv := []string{}
		for _, node := range path { pathv = append(pathv, node.String()) }
		tracefΔBtail("\nTrack %s %s\n", keycov, strings.Join(pathv, " -> "))
		tracefΔBtail("trackSet: %s\n", δBtail.trackSet)
	}

	// first normalize path: remove embedded bucket and check if it was an
	// empty artificial tree. We need to do the normalization because we
	// later check whether leaf path[-1] ∈ trackSet and without
	// normalization path[-1] can be InvalidOid.
	path = blib.NormPath(path)
	if len(path) == 0 {
		return // empty tree
	}

	root := path[0]
	leaf := path[len(path)-1]

	// assertSamePathToLeaf asserts that T.Path(leaf) == path.
	assertSamePathToLeaf := func(T blib.PPTreeSubSet, Tname string) {
		path_ := T.Path(leaf)
		if !pathEqual(path, path_) {
			panicf("BUG: keycov %s is already in %s via path=%v\ntrack requests path=%v", keycov, Tname, path_, path)
		}
	}

	// nothing to do if keycov is already tracked
	if δBtail.trackSet.Has(leaf) {
		tracefΔBtail("->T: nop  (already in trackSet)\n")
		assertSamePathToLeaf(δBtail.trackSet, "trackSet")
		return
	}

	δTtail, ok := δBtail.byRoot[root]
	if !ok {
		δTtail = newΔTtail()
		δBtail.byRoot[root] = δTtail
	}

	// nothing to do if keycov is already queued to be tracked in trackNew or krebuildJobs
	if δTtail.krebuildJobs.IntersectsRange(keycov) {
		tracefΔBtail("->T: nop  (already in krebuildJobs)\n")
		job, r, ok := δTtail.krebuildJobs.Get_(keycov.Lo)
		if !(ok && r == keycov) {
			panicf("BUG: keycov is already present in krebuildJobs, but only partly\nkeycov: %s\nkrebuildJobs: %v",
			       keycov, δTtail.krebuildJobs)
		}
		assertSamePathToLeaf(job.trackNew, "job.trackNew")
		return
	}

	if δTtail.trackNew.Has(leaf) {
		tracefΔBtail("->T: nop  (already in trackNew)\n")
		assertSamePathToLeaf(δTtail.trackNew, "trackNew")
		return
	}

	// keycov not in trackSet/trackNew/krebuildJobs -> queue it into trackNew
	δBtail.trackNewRoots.Add(root)
	δTtail.trackNew.AddPath(path)
	δTtail.ktrackNew.AddRange(keycov)
	tracefΔBtail("->T: [%s].trackNew  -> %s\n", root, δTtail.trackNew)
	tracefΔBtail("->T: [%s].ktrackNew -> %s\n", root, δTtail.ktrackNew)
}

// vδTSnapForTrackedKey returns vδT snapshot for root that takes into account
// at least all previous Track requests related to key.
//
// vδT is rebuilt if there are such not-yet-handled Track requests.
func (δBtail *ΔBtail) vδTSnapForTrackedKey(root zodb.Oid, key Key) (vδT []ΔTree, err error) {
	δBtail.mu.Lock() // TODO verify that there is no in-progress writers
	δTtail := δBtail.byRoot[root] // must be there
	if δTtail == nil {
		δBtail.mu.Unlock()
		panicf("δBtail: root<%s> not tracked", root)
	}

	// TODO key not tracked	-> panic  (check key ∈ lastRevOf)

	if !δTtail.ktrackNew.Has(key) {
		// key ∉ ktrackNew
		job, _, inJobs := δTtail.krebuildJobs.Get_(key)
		if !inJobs {
			// key ∉ krebuildJobs -> it should be already in trackSet
			vδT = δTtail.vδT
			δBtail.mu.Unlock()
			return vδT, nil
		}

		// rebuild for root[key] is in progress -> wait for corresponding job to complete
		δBtail.mu.Unlock()
		<-job.ready
		if job.err == nil {
			δBtail.mu.Lock()
			vδT = δTtail.vδT
			δBtail.mu.Unlock()
		}
		return vδT, job.err
	}

	// key ∈ ktrackNew  ->  this goroutine becomes responsible to rebuild vδT for it
	// run rebuild job for all keys queued in ktrackNew so far
	err = δTtail._rebuild(root, δBtail)
	if err == nil {
		vδT = δTtail.vδT
	}
	δBtail.mu.Unlock()

	return vδT, err
}

// vδTSnapForTracked returns vδT snapshot for root that takes into account all
// previous Track requests.
//
// vδT is rebuilt if there are such not-yet-handled Track requests.
func (δBtail *ΔBtail) vδTSnapForTracked(root zodb.Oid) (vδT []ΔTree, err error) {
	δBtail.mu.Lock() // TODO verify that there is no in-progress writers
	δTtail := δBtail.byRoot[root] // must be there
	if δTtail == nil {
		δBtail.mu.Unlock()
		panicf("δBtail: root<%s> not tracked", root)
	}

	// prepare to wait for all already running jobs, if any
	wg := xsync.NewWorkGroup(context.Background())
	for _, e := range δTtail.krebuildJobs.AllRanges() {
		job := e.Value
		wg.Go(func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-job.ready:
				return job.err
			}
		})
	}

	// run new rebuild job if there are not-yet-handled Track requests
	var errJob error
	if !δTtail.ktrackNew.Empty() {
		errJob = δTtail._rebuild(root, δBtail)
	}

	// wait for previous jobs to complete as well
	δBtail.mu.Unlock()
	errWait := wg.Wait()

	err = xerr.First(errJob, errWait)
	if err != nil {
		return nil, err
	}

	// now it is ok to take the snapshot
	δBtail.mu.Lock()
	vδT = δTtail.vδT
	δBtail.mu.Unlock()

	return vδT, nil
}

// _rebuild runs rebuild job for current .ktrackNew/.trackNew
//
// must be called with δBtail.mu locked.
// returns with        δBtail.mu locked.
func (δTtail *_ΔTtail) _rebuild(root zodb.Oid, δBtail *ΔBtail) (err error) {
	return δTtail.__rebuild(root, δBtail, /*releaseLock=*/true)
}
func (δTtail *_ΔTtail) __rebuild(root zodb.Oid, δBtail *ΔBtail, releaseLock bool) (err error) {
	defer xerr.Contextf(&err, "ΔBtail._rebuild root<%s>", root)

	trackNew  := δTtail.trackNew
	ktrackNew := δTtail.ktrackNew
	δTtail.trackNew  = blib.PPTreeSubSet{}
	δTtail.ktrackNew = blib.RangedKeySet{}

	job := &_RebuildJob{trackNew: trackNew, ready: make(chan struct{})}

	// krebuildJobs += ktrackNew
	for _, r := range ktrackNew.AllRanges() {
		// assert krebuildJobs ^ r = ø
		if δTtail.krebuildJobs.IntersectsRange(r) {
			panicf("BUG: rebuild: prologue: " +
			       "krebuildJobs ^ ktrackNew != ø:\nkrebuildJobs: %s\nktrackNew: %s",
			       δTtail.krebuildJobs, ktrackNew)
		}
		δTtail.krebuildJobs.SetRange(r, job)
	}
	delete(δBtail.trackNewRoots, root)

	// build δ(vδT) without the lock
	if releaseLock {
		δBtail.mu.Unlock()
	}
	vδTnew, δtrackSet, err := vδTBuild(root, trackNew, δBtail.δZtail, δBtail.db)
	if releaseLock {
		δBtail.mu.Lock()
	}

	// krebuildJobs -= ktrackNew
	for _, r := range ktrackNew.AllRanges() {
		// assert krebuildJobs[r] = job
		job_, r_ := δTtail.krebuildJobs.Get(r.Lo)
		if !(job_ == job && r_ == r) {
			panicf("BUG: rebuild: epilogue: " +
			       "krebuildJobs entry mutated:\nset in prologue [%s]=%p\ngot in epilogue: [%s]=%p",
			       r, job, r_, job_)
		}
		δTtail.krebuildJobs.DelRange(r)
	}

	// merge rebuild result
	if err == nil {
		// vδT <- vδTnew RCU;  trackSet += δtrackSet
		δTtail.vδT = vδTClone(δTtail.vδT)
		δrevSet := vδTMergeInplace(&δTtail.vδT, vδTnew)
		δBtail.trackSet.UnionInplace(δtrackSet)
		δBtail._vδBroots_Update(root, δrevSet)
	} else {
		// reinstate trackNew and ktrackNew back, so that data for those
		// keys are tried to be rebuilt next time, not silently remain
		// missing in vδT, i.e. corrupted.
		δTtail.trackNew.UnionInplace(trackNew)
		δTtail.ktrackNew.UnionInplace(&ktrackNew)
		δBtail.trackNewRoots.Add(root)
	}

	// we are done
	job.err = err
	close(job.ready)
	return err
}

// Update updates δB with object-level ZODB changes.
//
// Only those objects from δZ that belong to tracked set are guaranteed to be
// taken into account. In other words a tree history will assuredly include
// only those keys, that correspond to tracked subset of δZ.
//
// δZ must include all objects changed by ZODB transaction.
//
// TODO optionally accept zconnOld/zconnNew from client
func (δBtail *ΔBtail) Update(δZ *zodb.EventCommit) (_ ΔB, err error) {
	headOld := δBtail.Head()
	defer xerr.Contextf(&err, "ΔBtail.Update %s -> %s", headOld, δZ.Tid)

	δBtail.mu.Lock()
	defer δBtail.mu.Unlock()
	// TODO verify that there is no in-progress readers/writers

	δB1, err := δBtail._Update1(δZ)

	δB := ΔB{Rev: δZ.Tid, ByRoot: make(map[zodb.Oid]map[Key]ΔValue)}
	for root, δT1 := range δB1.ByRoot {
		δTtail := δBtail.byRoot[root] // must succeed

		// δtkeycov1 != ø   ->  rebuild δTtail with trackNew ~= δtkeycov1
		if !δT1.δtkeycov1.Empty() && δBtail.δZtail.Len() > 1 {
			trackNew := blib.PPTreeSubSet{}
			err := widenTrackNew(trackNew, δT1.δtkeycov1, root, δBtail.Head(), δBtail.db)
			if err != nil {
				return ΔB{}, err
			}

			// NOTE we cannot skip computing diff for HEAD~..HEAD
			// even after _Update1 because _Update1 was working with different trackNew.
			vδTnew, δtrackSet, err := vδTBuild(root, trackNew, δBtail.δZtail, δBtail.db)
			if err != nil {
				return ΔB{}, err
			}
			// vδT <- vδTnew RCU;  trackSet += δtrackSet
			δTtail.vδT = vδTClone(δTtail.vδT)
			δrevSet := vδTMergeInplace(&δTtail.vδT, vδTnew)
			δBtail.trackSet.UnionInplace(δtrackSet)
			δBtail._vδBroots_Update(root, δrevSet)
		}

		// build δB. Even if δT=ø after _Update1, but δtkeycov1 != ø, above
		// rebuild could result in head δT becoming != ø. Detect that δTtail head
		// is anew by comparing to δZ.Rev.
		l := len(δTtail.vδT)
		if l > 0 {
			δT := δTtail.vδT[l-1] // δT head
			if δT.Rev == δZ.Tid {
				δB.ByRoot[root] = δT.KV
			}
		}
	}

	// vδBroots += δB  (δB.Rev could be already there added by ^^^ rebuild)
	for root := range δB.ByRoot {
		δBtail._vδBroots_Update1(root, δB.Rev)
	}

	return δB, err
}

// _Update1 serves Update and performs direct update of δTtail head elements from δZ.
// On key coverage growth rebuilding tail of the history is done by Update itself.
//
// _Update1 is also used in tests to verify δtkeycov return from treediff.
type _ΔBUpdate1 struct {
	ByRoot map[zodb.Oid]*_ΔTUpdate1
}
type _ΔTUpdate1 struct {
	δtkeycov1 *blib.RangedKeySet // {} root -> δtrackedKeys after first treediff (always grow)
}
func (δBtail *ΔBtail) _Update1(δZ *zodb.EventCommit) (δB1 _ΔBUpdate1, err error) {
	headOld := δBtail.Head()
	defer xerr.Contextf(&err, "ΔBtail.update1 %s -> %s", headOld, δZ.Tid)

	tracefΔBtail("\nUpdate  @%s -> @%s    δZ: %v\n", δBtail.Head(), δZ.Tid, δZ.Changev)
	tracefΔBtail("trackSet:       %v\n", δBtail.trackSet)
	for _, root := range δBtail.trackNewRoots.SortedElements() {
		δTtail := δBtail.byRoot[root]
		tracefΔBtail("[%s].trackNew:  %v\n", root, δTtail.trackNew)
		tracefΔBtail("[%s].ktrackNew: %v\n", root, δTtail.ktrackNew)
	}

	δB1 = _ΔBUpdate1{ByRoot: make(map[zodb.Oid]*_ΔTUpdate1)}

	// update .trackSet and vδB from .trackNew
	err = δBtail._rebuildAll()
	if err != nil {
		return δB1, err
	}

	δBtail.δZtail.Append(δZ.Tid, δZ.Changev)

	// NOTE: keep vvv in sync with vδTBuild1

	δZTC, δtopsByRoot := δZConnectTracked(δZ.Changev, δBtail.trackSet)

	// skip opening DB connections if there is no change to any tree node
	if len(δtopsByRoot) == 0 {
		return δB1, nil
	}

	// open ZODB connections corresponding to "old" and "new" states
	// TODO caller should provide one of those (probably new) as usually it has it
	txn, ctx := transaction.New(context.TODO()) // TODO - merge in cancel via ctx arg
	defer txn.Abort()
	zconnOld, err := δBtail.db.Open(ctx, &zodb.ConnOptions{At: headOld})
	if err != nil {
		return δB1, err
	}
	zconnNew, err := δBtail.db.Open(ctx, &zodb.ConnOptions{At: δZ.Tid})
	if err != nil {
		return δB1, err
	}

	for root, δtops := range δtopsByRoot {
		δT, δtrack, δtkeycov, err := treediff(ctx, root, δtops, δZTC, δBtail.trackSet, zconnOld, zconnNew)
		if err != nil {
			return δB1, err
		}

		tracefΔBtail("\n-> root<%s>  δkv: %v  δtrack: %v  δtkeycov: %v\n", root, δT, δtrack, δtkeycov)

		δTtail := δBtail.byRoot[root] // must be there
		if len(δT) > 0 { // an object might be resaved without change
			// NOTE no need to clone .vδT here because we only append to it:
			//      Even though queries return vδT aliases, append
			//      cannot change any slice returned by query to users.
			δTtail.vδT = append(δTtail.vδT, ΔTree{Rev: δZ.Tid, KV: δT})
		}

		δBtail.trackSet.ApplyΔ(δtrack)
		δB1.ByRoot[root] = &_ΔTUpdate1{δtkeycov1: δtkeycov}
	}

	return δB1, nil
}

// _rebuildAll rebuilds ΔBtail taking all trackNew requests into account.
func (δBtail *ΔBtail) _rebuildAll() (err error) {
	defer xerr.Context(&err, "ΔBtail._rebuildAll")
	tracefΔBtail("\nRebuildAll @%s..@%s    trackNewRoots: %s\n", δBtail.Tail(), δBtail.Head(), δBtail.trackNewRoots)

	for root := range δBtail.trackNewRoots {
		δTtail := δBtail.byRoot[root] // must be there
		err = δTtail.__rebuild(root, δBtail, /*releaseLock=*/false)
		if err != nil {
			return err
		}
	}

	return nil
}

// _vδBroots_Update updates .vδBroots to remember that _ΔTtail for root has
// changed entries with δrevSet revisions.
//
// must be called with δBtail.mu locked.
func (δBtail *ΔBtail) _vδBroots_Update(root zodb.Oid, δrevSet setTid) {
	// TODO δrevSet -> []rev↑  and  merge them in one go
	for rev := range δrevSet {
		δBtail._vδBroots_Update1(root, rev)
	}
}

func (δBtail *ΔBtail) _vδBroots_Update1(root zodb.Oid, rev zodb.Tid) {
	l := len(δBtail.vδBroots)
	j := sort.Search(l, func(k int) bool {
		return rev <= δBtail.vδBroots[k].Rev
	})
	if j == l || rev != δBtail.vδBroots[j].Rev {
		δBroots := _ΔBroots{Rev: rev, Roots: setOid{}}
		// insert(@j, δBroots)
		δBtail.vδBroots = append(δBtail.vδBroots[:j],
					 append([]_ΔBroots{δBroots},
					 δBtail.vδBroots[j:]...)...)
	}
	δBroots := δBtail.vδBroots[j]
	δBroots.Roots.Add(root)
}

// ForgetPast forgets history entries with revision ≤ revCut.
func (δBtail *ΔBtail) ForgetPast(revCut zodb.Tid) {
	δBtail.mu.Lock()
	defer δBtail.mu.Unlock()
	// TODO verify that there is no in-progress readers/writers

	δBtail.δZtail.ForgetPast(revCut)

	// go through vδBroots till revcut -> find which trees to trim -> trim ΔTtails.

	totrim := setOid{} // roots whose _ΔTtail has changes ≤ revCut
	icut := 0
	for ; icut < len(δBtail.vδBroots); icut++ {
		δBroots := δBtail.vδBroots[icut]
		if δBroots.Rev > revCut {
			break
		}
		totrim.Update(δBroots.Roots)
	}

	// vδBroots[:icut] should be forgotten
	δBtail.vδBroots = append([]_ΔBroots(nil), δBtail.vδBroots[icut:]...)

	// trim roots
	for root := range totrim {
		δTtail := δBtail.byRoot[root] // must be present
		δTtail._forgetPast(revCut)
	}
}

func (δTtail *_ΔTtail) _forgetPast(revCut zodb.Tid) {
	icut := 0
	for ; icut < len(δTtail.vδT); icut++ {
		if δTtail.vδT[icut].Rev > revCut {
			break
		}
	}

	// vδT[:icut] should be forgotten
	// NOTE clones vδT because queries return vδT aliases
	δTtail.vδT = append([]ΔTree(nil), δTtail.vδT[icut:]...)
}


// ---- queries ----

// GetAt tries to retrieve root[key]@at from δBtail data.
//
// If δBtail has δB entry that covers root[key]@at, corresponding value
// (VDEL means deletion) and valueExact=true are returned. If δBtail data
// allows to determine revision of root[key]@at value, corresponding revision
// and revExact=true are returned. If revision of root[key]@at cannot be
// determined (rev=δBtail.Tail, revExact=false) are returned.
//
// In particular:
//
// If δBtail has no δB entry that covers root[key]@at, return is
//
//	value:      VDEL,
//	valueExact: false,
//	rev:        δBtail.Tail,
//	revExact:   false
//
// If δBtail has    δB entry that covers root[key]@at, return is
//
//	value:      δB.δvalue.New,
//	valueExact: true,
//	rev:        δB.rev,
//	revExact:   true
//
// If δBtail has    δB entry that covers value for root[key]@at via
// δB.δvalue.Old, but not entry that covers root[key]@at fully, return is:
//
//	value:      δB.δvalue.Old,
//	valueExact: true,
//	rev:        δBtail.Tail,
//	revExact:   false
//
// key must be tracked
// at  must ∈ {tail} ∪ (tail, head]
func (δBtail *ΔBtail) GetAt(root zodb.Oid, key Key, at zodb.Tid) (value Value, rev zodb.Tid, valueExact, revExact bool, err error) {
	defer xerr.Contextf(&err, "ΔBtail: root<%s>: get %d @%s", root, key, at)

	if traceΔBtail {
		tracefΔBtail("\nGet root<%s>[%s] @%s\n", root, kstr(key), at)
		defer func() {
			vexact := ""
			rexact := ""
			if !valueExact {
				vexact = "~"
			}
			if !revExact {
				rexact = "~"
			}
			tracefΔBtail("-> value: %s%s  rev: @%s%s\n", value, vexact, rev, rexact)
		}()
	}

	tail := δBtail.Tail()
	head := δBtail.Head()
	if !(tail <= at && at <= head) {
		panicf("at out of bounds: at: @%s,  (tail, head] = (@%s, @%s]", at, tail, head)
	}

	value = VDEL
	valueExact = false
	rev = tail
	revExact = false

	// retrieve vδT snapshot that is rebuilt to take Track(key) requests into account
	vδT, err := δBtail.vδTSnapForTrackedKey(root, key)
	if err != nil {
		return value, rev, valueExact, revExact, err
	}
	debugfΔBtail("  vδT: %v\n", vδT)

	// TODO key not tracked	-> panic  (check key ∈ lastRevOf  -- see vvv)
	// TODO -> index lastRevOf(key) | linear scan ↓ looking for change ≤ at
	for i := len(vδT)-1; i >= 0; i-- {
		δT := vδT[i]
		δvalue, ok_ := δT.KV[key]
		if ok_ {
			valueExact = true
			if δT.Rev > at {
				value = δvalue.Old
			} else {
				value = δvalue.New
				rev = δT.Rev
				revExact = true
				break
			}
		}
	}

	return value, rev, valueExact, revExact, nil
}

// TODO if needed
// func (δBtail *ΔBtail) SliceByRev(lo, hi zodb.Tid) /*readonly*/ []ΔB

// SliceByRootRev returns history of a tree changes in (lo, hi] range.
//
// it must be called with the following condition:
//
//	tail ≤ lo ≤ hi ≤ head
//
// the caller must not modify returned slice.
//
// Only tracked keys are guaranteed to be present.
//
// Note: contrary to regular go slicing, low is exclusive while high is inclusive.
func (δBtail *ΔBtail) SliceByRootRev(root zodb.Oid, lo, hi zodb.Tid) (/*readonly*/vδT []ΔTree, err error) {
	xtail.AssertSlice(δBtail, lo, hi)

	if traceΔBtail {
		tracefΔBtail("\nSlice root<%s> (@%s,@%s]\n", root, lo, hi)
		defer func() {
			tracefΔBtail("-> vδT(lo,hi]: %v\n", vδT)
		}()
	}

	// retrieve vδT snapshot that is rebuilt to take all previous Track requests into account
	vδT, err = δBtail.vδTSnapForTracked(root)
	if err != nil {
		return nil, err
	}
	debugfΔBtail("  vδT: %v\n", vδT)

	l := len(vδT)
	if l == 0 {
		return nil, nil
	}

	// find max j : [j].rev ≤ hi		linear scan -> TODO binary search
	j := l - 1
	for ; j >= 0 && vδT[j].Rev > hi; j-- {}
	if j < 0 {
		return nil, nil // ø
	}

	// find max i : [i].rev > lo		linear scan -> TODO binary search
	i := j
	for ; i >= 0 && vδT[i].Rev > lo; i-- {}
        i++

	// NOTE: no need to duplicate returned vδT slice because vδT is
	// modified via RCU: i.e. _ΔTtail.rebuild clones vδT before modifying it.
	// This way the data we return to caller will stay unchanged even if
	// rebuild is running simultaneously.
	return vδT[i:j+1], nil
}


// ---- vδTBuild/vδTMerge (rebuild core) ----

// vδTBuild builds vδT from vδZ for root/tracked=trackNew.
//
// It returns:
//
// - vδT,
// - trackNew* - a superset of trackNew accounting that potentially more keys
//   become tracked during the build process.
//
// NOTE ΔBtail calls vδTBuild(root, trackNew) to compute update for ΔTtail.vδT.
func vδTBuild(root zodb.Oid, trackNew blib.PPTreeSubSet, δZtail *zodb.ΔTail, db *zodb.DB) (vδT []ΔTree, trackNew_ blib.PPTreeSubSet, err error) {
	defer xerr.Contextf(&err, "root<%s>: build vδT", root)

	tracefΔBtail("\nvδTBuild %s @%s .. @%s\n", root, δZtail.Tail(), δZtail.Head())
	tracefΔBtail("trackNew:        %v\n", trackNew)

	if len(trackNew) == 0 {
		return nil, nil, nil
	}

	trackNew = trackNew.Clone() // it will become trackNew*

	// go backwards and compute vδT <- treediff(lo..hi/trackNew)
	vδZ := δZtail.Data()
	for {
		δtkeycov := &blib.RangedKeySet{} // all keys coming into tracking set during this lo<-hi scan
		trackNewCur := trackNew.Clone()  // trackNew adjusted as of when going to i<- entry
		for i := len(vδZ)-1; i>=0; i-- {
			δZ := vδZ[i]

			var atPrev zodb.Tid
			if i > 0 {
				atPrev = vδZ[i-1].Rev
			} else {
				atPrev = δZtail.Tail()
			}

			δkv, δtrackNew, δtkeycov_, err := vδTBuild1(atPrev, δZ, trackNewCur, db)
			if err != nil {
				return nil, nil, err
			}

			if len(δkv) > 0 {
				δT := ΔTree{Rev: δZ.Rev, KV: δkv}
				vδTMerge1Inplace(&vδT, δT)
			}
			trackNewCur.ApplyΔ(δtrackNew)
			δtkeycov.UnionInplace(δtkeycov_)
		}

		// an iteration closer to tail may turn out to add a key to the tracking set.
		// We have to recheck all entries newer that revision for changes to that key,
		// for example:
		//
		//              8        5*
		//             / \  <-  / \
		//            2   8    2*  7
		//
		//     here initial tracked set is 5*-2*. Going to earlier revision
		//     2'th keycov range is widen from [-∞,5) to [-∞,7), so 5*-7 in
		//     later revision have to be rechecked because 7 was added into
		//     tracking set.
		//
		// Implement this via restarting from head and cycling until
		// set of tracked keys does not grow anymore.
		if δtkeycov.Empty() {
			break
		}

		err := widenTrackNew(trackNew, δtkeycov, root, δZtail.Head(), db)
		if err != nil {
			return nil, nil, err
		}
	}

	tracefΔBtail("-> vδT:        %v\n", vδT)
	tracefΔBtail("-> trackNew*:  %v\n", trackNew)
	return vδT, trackNew, nil
}

// vδTBuild1 builds δT for single δZ.
//
// δtrackNew/δtkeycov represents how trackNew changes when going through `atPrev <- δZ.Rev` .
func vδTBuild1(atPrev zodb.Tid, δZ zodb.ΔRevEntry, trackNew blib.PPTreeSubSet, db *zodb.DB) (δT map[Key]ΔValue, δtrackNew *blib.ΔPPTreeSubSet, δtkeycov *blib.RangedKeySet, err error) {
	defer xerr.Contextf(&err, "build1 %s<-%s", atPrev, δZ.Rev)

	debugfΔBtail("\n  build1 @%s <- @%s\n", atPrev, δZ.Rev)
	debugfΔBtail("  δZ:\t%v\n", δZ.Changev)
	debugfΔBtail("  trackNew:  %v\n", trackNew)
	defer func() {
		debugfΔBtail("-> δT:          %v\n", δT)
		debugfΔBtail("-> δtrackNew:   %v\n", δtrackNew)
		debugfΔBtail("-> δtkeycov:    %v\n", δtkeycov)
		debugfΔBtail("\n\n")
	}()

	// NOTE: keep vvv in sync with ΔBtail._Update1

	δZTC, δtopsByRoot := δZConnectTracked(δZ.Changev, trackNew)

	// skip opening DB connections if there is no change to this tree
	if len(δtopsByRoot) == 0 {
		return nil, blib.NewΔPPTreeSubSet(), &blib.RangedKeySet{}, nil
	}

	if len(δtopsByRoot) != 1 {
		panicf("BUG: δtopsByRoot has > 1 entries: %v\ntrackNew: %v\nδZ: %v", δtopsByRoot, trackNew, δZ)
	}
	var root  zodb.Oid
	var δtops setOid
	for root_, δtops_ := range δtopsByRoot {
		root  = root_
		δtops = δtops_
	}


	// open ZODB connection corresponding to "current" and "prev" states
	txn, ctx := transaction.New(context.TODO()) // TODO - merge in cancel via ctx arg
	defer txn.Abort()

	zconnPrev, err := db.Open(ctx, &zodb.ConnOptions{At: atPrev})
	if err != nil {
		return nil, nil, nil, err
	}
	zconnCurr, err := db.Open(ctx, &zodb.ConnOptions{At: δZ.Rev})
	if err != nil {
		return nil, nil, nil, err
	}

	// diff backwards curr -> prev
	δT, δtrack, δtkeycov, err := treediff(ctx, root, δtops, δZTC, trackNew, zconnCurr, zconnPrev)
	if err != nil {
		return nil, nil, nil, err
	}

	debugfΔBtail("  -> root<%s>  δkv*: %v  δtrack*: %v  δtkeycov*: %v\n", root, δT, δtrack, δtkeycov)

	for k, δv := range δT {
		// the diff was backward; vδT entries are with diff forward
		δv.New, δv.Old = δv.Old, δv.New
		δT[k] = δv
	}

	return δT, δtrack, δtkeycov, nil
}

// vδTMergeInplace merges vδTnew into vδT.
//
// δrevSet indicates set of new revisions created in vδT.
// vδT is modified inplace.
func vδTMergeInplace(pvδT *[]ΔTree, vδTnew []ΔTree) (δrevSet setTid) {
	// TODO if needed: optimize to go through vδT and vδTnew sequentially
	δrevSet = setTid{}
	for _, δT := range vδTnew {
		newRevEntry := vδTMerge1Inplace(pvδT, δT)
		if newRevEntry {
			δrevSet.Add(δT.Rev)
		}
	}
	return δrevSet
}

// vδTMerge1Inplace merges one δT entry into vδT.
//
// newRevEntry indicates whether δT.Rev was not there before in vδT.
// vδT is modified inplace.
func vδTMerge1Inplace(pvδT *[]ΔTree, δT ΔTree) (newRevEntry bool) {
	if len(δT.KV) == 0 {
		return false // δT has no change
	}

	vδT := *pvδT

	l := len(vδT)
	j := sort.Search(l, func(k int) bool {
		return δT.Rev <= vδT[k].Rev
	})
	if j == l || vδT[j].Rev != δT.Rev {
		newRevEntry = true
		δTcurr := ΔTree{Rev: δT.Rev, KV: map[Key]ΔValue{}}
		// insert(@j, δTcurr)
		vδT = append(vδT[:j],
			     append([]ΔTree{δTcurr},
			     vδT[j:]...)...)
	}
	δTcurr := vδT[j]

	for k, δv := range δT.KV {
		δv_, already := δTcurr.KV[k]
		if already {
			if δv != δv_ {
				// TODO: return "conflict"
				panicf("[%v] inconsistent δv:\nδTcurr: %v\nδT:  %v", k, δTcurr, δT)
			}
		} else {
			δTcurr.KV[k] = δv
		}
	}

	*pvδT = vδT
	return newRevEntry
}

// widenTrackNew widens trackNew to cover δtkeycov.
func widenTrackNew(trackNew blib.PPTreeSubSet, δtkeycov *blib.RangedKeySet, root zodb.Oid, at zodb.Tid, db *zodb.DB) (err error) {
	defer xerr.Contextf(&err, "widenTrackNew root<%s> @%s +%s", root, at, δtkeycov)
	debugfΔBtail("\n  widenTrackNew %s @%s +%s", root, at, δtkeycov)

	txn, ctx := transaction.New(context.TODO()) // TODO - merge in cancel via ctx arg
	defer txn.Abort()

	zhead, err := db.Open(ctx, &zodb.ConnOptions{At: at});  /*X*/ if err != nil { return err }
	xtree, err := zgetNodeOrNil(ctx, zhead, root);          /*X*/ if err != nil { return err }
	if xtree == nil {
		// root deleted -> root node covers [-∞,∞)
		trackNew.AddPath([]zodb.Oid{root})
		return nil
	}
	tree := xtree.(*Tree) // must succeed

	top := &nodeInRange{prefix: nil, keycov: blib.KeyRange{KeyMin, KeyMax}, node: tree}
	V   := rangeSplit{top}
	for _, r := range δtkeycov.AllRanges() {
		lo := r.Lo
		for {
			b, err := V.GetToLeaf(ctx, lo);  /*X*/ if err != nil { return err }
			trackNew.AddPath(b.Path())

			// continue with next right bucket until r coverage is complete
			if r.Hi_ <= b.keycov.Hi_ {
				break
			}
			lo = b.keycov.Hi_ + 1
		}
	}
	return nil
}


// ----------------------------------------

// ΔZtail returns raw ZODB changes accumulated in δBtail so far.
//
// the caller must not modify returned δZtail.
func (δBtail *ΔBtail) ΔZtail() /*readonly*/*zodb.ΔTail {
	return δBtail.δZtail
}

// DB returns database handle that δBtail is using to access ZODB.
func (δBtail *ΔBtail) DB() *zodb.DB {
	return δBtail.db
}


func tracefΔBtail(format string, argv ...interface{}) {
	if traceΔBtail {
		fmt.Printf(format, argv...)
	}
}

func debugfΔBtail(format string, argv ...interface{}) {
	if debugΔBtail {
		fmt.Printf(format, argv...)
	}
}
