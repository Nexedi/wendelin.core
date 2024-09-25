// Copyright (C) 2020-2024  Nexedi SA and Contributors.
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

package xbtreetest
// T + friends

import (
	"context"
	"fmt"
	"sort"
	"testing"

	pickle "github.com/kisielk/og-rek"
	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/blib"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xzodb"
)

// T is tree-based testing environment.
//
// It combines TreeSrv and client side access to ZODB with committed trees.
// It should be created via NewT().
type T struct {
	*testing.T

	work    string   // working directory
	treeSrv *TreeSrv
	zstor   zodb.IStorage
	DB      *zodb.DB

	commitv []*Commit // all committed trees
	at0idx  int       // index of current "at₀" in commitv
}

// Commit represent test commit changing a tree.
type Commit struct {
	T       *T                    // created via T.Commit
	idx     int                   // lives in .T.commitv[idx]
	Tree    string                // the tree in topology-encoding
	Prev    *Commit               // previous commit
	At      zodb.Tid              // committed revision
	ΔZ      *zodb.EventCommit     // raw ZODB changes; δZ.tid == at
	Xkv     RBucketSet            // full tree state as of @at
	Δxkv    map[Key]Δstring       // full tree-diff against parent
	ZBlkTab map[zodb.Oid]ZBlkInfo // full snapshot of all ZBlk name/data @at
}

// ZBlkInfo describes one ZBlk object.
type ZBlkInfo struct {
	Name string // this ZBlk comes under root['treegen/values'][Name]
	Data string
}

// NewT creates new T.
func NewT(t *testing.T) *T {
	X := exc.Raiseif
	t.Helper()

	tt := &T{T: t, at0idx: 1 /* at₀ starts from first t.Commit */}

	var err error
	work := t.TempDir()
	tt.treeSrv, err = StartTreeSrv(work + "/1.fs"); X(err)
	t.Cleanup(func() {
		err := tt.treeSrv.Close(); X(err)
	})

	tt.zstor, err = zodb.Open(context.Background(), tt.treeSrv.zurl, &zodb.OpenOptions{
					 ReadOnly: true,
		       }); X(err)
	t.Cleanup(func() {
		err := tt.zstor.Close(); X(err)
	})

	tt.DB = zodb.NewDB(tt.zstor, &zodb.DBOptions{
		// We need objects to be cached, because otherwise it is too
		// slow to run the test for many testcases, especially
		// xverifyΔBTail_rebuild.
		CacheControl: &tZODBCacheEverything{},
	})
	t.Cleanup(func() {
		err := tt.DB.Close(); X(err)
	})

	head := tt.treeSrv.head
	t1 := &Commit{
		T:       tt,
		idx:     0,
		Tree:    "T/B:",	// treegen.py creates the tree as initially empty
		Prev:    nil,
		At:      head,
		Xkv:     xGetTree(tt.DB, head, tt.Root()),
		ZBlkTab: xGetBlkTab(tt.DB, head),
		ΔZ:      nil,
		Δxkv:    nil,
	}
	tt.commitv = []*Commit{t1}

	return tt
}

// tZODBCacheEverything is workaround for ZODB/go not implementing real
// live cache for now: Objects get dropped on PDeactivate if cache
// control does not say we need the object to stay in the cache.
type tZODBCacheEverything struct{}
func (_ *tZODBCacheEverything) PCacheClassify(_ zodb.IPersistent) zodb.PCachePolicy {
	return zodb.PCachePinObject | zodb.PCacheKeepState
}

// Root returns OID of root tree node.
func (t *T) Root() zodb.Oid {
	return t.treeSrv.treeRoot
}

// Head returns most-recently committed tree.
func (t *T) Head() *Commit {
	return t.commitv[len(t.commitv)-1]
}

// XGetCommit finds and returns Commit created with revision at.
func (t *T) XGetCommit(at zodb.Tid) *Commit {
	commit, _, _ := t.getCommit(at)
	if commit == nil {
		panicf("no commit corresponding to @%s", at)
	}
	return commit
}

func (t *T) getCommit(at zodb.Tid) (commit, cprev, cnext *Commit) {
	l := len(t.commitv)
	i := sort.Search(l, func(i int) bool {
		return at <= t.commitv[i].At
	})
	if i < l {
		commit = t.commitv[i]
		if commit.At != at {
			cnext = commit
			commit = nil
		} else if i+1 < l {
			cnext = t.commitv[i+1]
		}
	}
	if i > 0 {
		cprev = t.commitv[i-1]
	}
	if commit != nil && commit.idx != i {
		panicf("BUG: commit.idx (%d) != i (%d)", commit.idx, i)
	}
	return commit, cprev, cnext
}

// AtSymb returns symbolic representation of at, for example "at3".
//
// at should correspond to a Commit.
func (t *T) AtSymb(at zodb.Tid) string {
	commit, cprev, cnext := t.getCommit(at)
	if commit != nil {
		return commit.AtSymb()
	}

	// at does not correspond to commit - return something like ~at2<xxxx>at3
	s := "~"
	if cprev != nil {
		s += cprev.AtSymb() + "<"
	}
	s += at.String()
	if cnext != nil {
		s += ">" + cnext.AtSymb()
	}
	return s
}

// AtSymb returns symbolic representation of c.At, for example "at3".
func (c *Commit) AtSymb() string {
	return fmt.Sprintf("at%d", c.idx - c.T.at0idx)
}

// AtSymbReset shifts symbolic numbers and adjust AtSymb setup so that c.AtSymb() returns "at<i>".
func (t *T) AtSymbReset(c *Commit, i int) {
	t.at0idx = c.idx - i
}

// Commit commits tree via treegen server and returns Commit object corresponding to committed transaction.
func (t *T) Commit(tree string) *Commit {
	X := exc.Raiseif
	defer exc.Contextf("commit %s", tree)

	watchq := make(chan zodb.Event)
	at0 := t.zstor.AddWatch(watchq)
	defer t.zstor.DelWatch(watchq)

	tid, err := t.treeSrv.Commit(tree); X(err)

	if !(tid > at0) {
		exc.Raisef("treegen -> %s  ; want > %s", tid, at0)
	}

	zevent := <-watchq
	δZ := zevent.(*zodb.EventCommit)
	if δZ.Tid != tid {
		exc.Raisef("treegen -> %s  ; watchq -> %s", tid, δZ)
	}

	// load tree structure from the db
	// if the tree does not exist yet - report its structure as empty
	var xkv RBucketSet
	if tree != DEL {
		xkv = xGetTree(t.DB, δZ.Tid, t.Root())
	} else {
		// empty tree with real treeRoot as oid even though the tree is
		// deleted.  Having real oid in the root tests that after deletion,
		// root of the tree stays in the tracking set. We need root to stay
		// in trackSet because e.g. in
		//
		//	T1 -> ø -> T2
		//
		// where the tree is first deleted, then recreated, without root
		// staying in trackSet after ->ø, treediff will notice nothing when
		// it comes to ->T2.
		xkv = RBucketSet{
			&RBucket{
				Oid:    zodb.InvalidOid,
				Parent: &RTree{
					Oid:    t.Root(), // NOTE oid is not InvalidOid
					Parent: nil,
				},
				Keycov: blib.KeyRange{KeyMin, KeyMax},
				KV:     map[Key]string{},
			},
		}
	}

	ttree := &Commit{
		T:       t,
		Tree:    tree,
		At:      δZ.Tid,
		ΔZ:      δZ,
		Xkv:     xkv,
		ZBlkTab: xGetBlkTab(t.DB, δZ.Tid),
	}

	tprev := t.Head()
	ttree.Prev = tprev
	ttree.Δxkv = KVDiff(tprev.Xkv.Flatten(), ttree.Xkv.Flatten())

	ttree.idx = len(t.commitv)
	t.commitv = append(t.commitv, ttree)

	return ttree
}

// xGetBlkTab loads all ZBlk from db@at.
//
// it returns {} oid -> blkdata.
func xGetBlkTab(db *zodb.DB, at zodb.Tid) map[zodb.Oid]ZBlkInfo {
	defer exc.Contextf("%s: @%s: get blktab", db.Storage().URL(), at)
	X := exc.Raiseif

	blkTab := map[zodb.Oid]ZBlkInfo{}

	txn, ctx := transaction.New(context.Background())
	defer txn.Abort()
	zconn, err := db.Open(ctx, &zodb.ConnOptions{At: at}); X(err)

	xzroot, err := zconn.Get(ctx, 0); X(err)
	zroot, ok := xzroot.(*zodb.Map)
	if !ok {
		exc.Raisef("root: expected %s, got %s", xzodb.TypeOf(zroot), xzodb.TypeOf(xzroot))
	}

	err = zroot.PActivate(ctx); X(err)
	defer zroot.PDeactivate()

	xzblkdir, ok := zroot.Get_("treegen/values")
	if !ok {
		exc.Raisef("root['treegen/values'] missing")
	}
	zblkdir, ok := xzblkdir.(*zodb.Map)
	if !ok {
		exc.Raisef("root['treegen/values']: expected %s, got %s", xzodb.TypeOf(zblkdir), xzodb.TypeOf(xzblkdir))
	}

	err = zblkdir.PActivate(ctx); X(err)
	defer zblkdir.PDeactivate()

	zblkdir.Iter()(func(xname, xzblk any) bool {
		name, ok := xname.(pickle.ByteString)
		if !ok {
			exc.Raisef("root['treegen/values']: key [%q]: expected str, got %T", xname, xname)
		}

		zblk, ok := xzblk.(zodb.IPersistent)
		if !ok {
			exc.Raisef("root['treegen/values'][%q]: expected IPersistent, got %s", name, xzodb.TypeOf(xzblk))
		}

		oid := zblk.POid()
		data := xzgetBlkData(ctx, zconn, oid)
		blkTab[oid] = ZBlkInfo{string(name), data}

		return true
	})

	return blkTab
}

// XGetBlkData loads blk data for ZBlk<oid> @c.at
//
// For speed the load is done via preloaded c.blkDataTab instead of access to the DB.
func (c *Commit) XGetBlkData(oid zodb.Oid) string {
	if oid == VDEL {
		return DEL
	}
	zblki, ok := c.ZBlkTab[oid]
	if !ok {
		exc.Raisef("getBlkData ZBlk<%s> @%s: no such ZBlk", oid, c.At)
	}
	return zblki.Data
}

// XGetBlkByName returns ZBlk info associated with ZBlk<name>
func (c *Commit) XGetBlkByName(name string) (zodb.Oid, ZBlkInfo) {
	for oid, zblki := range c.ZBlkTab {
		if zblki.Name == name {
			return oid, zblki
		}
	}
	panicf("ZBlk<%q> not found", name)
	panic("")
}

// xGetTree loads Tree from zurl@at->obj<root>.
//
// Tree values must be ZBlk whose data is returned instead of references to ZBlk objects.
// The tree is returned structured by buckets as
//
//	[] [lo,hi){k->v}  k↑
func xGetTree(db *zodb.DB, at zodb.Tid, root zodb.Oid) RBucketSet {
	defer exc.Contextf("%s: @%s: get tree %s", db.Storage().URL(), at, root)
	X := exc.Raiseif

	txn, ctx := transaction.New(context.Background())
	defer txn.Abort()
	zconn, err := db.Open(ctx, &zodb.ConnOptions{At: at}); X(err)

	xztree, err := zconn.Get(ctx, root); X(err)
	ztree, ok := xztree.(*Tree)
	if !ok {
		exc.Raisef("expected %s, got %s", xzodb.TypeOf(ztree), xzodb.TypeOf(xztree))
	}

	rbucketv := RBucketSet{}
	xwalkDFS(ctx, KeyMin, KeyMax, ztree, func(rb *RBucket) {
		rbucketv = append(rbucketv, rb)
	})
	if len(rbucketv) == 0 { // empty tree -> [-∞,∞){}
		etree   := &RTree{
				Oid:    root,
				Parent: nil,
		}
		ebucket := &RBucket{
				Oid:    zodb.InvalidOid,
				Parent: etree,
				Keycov: blib.KeyRange{KeyMin, KeyMax},
				KV:     map[Key]string{},
		}
		rbucketv = RBucketSet{ebucket}
	}
	return rbucketv
}

// xwalkDFS walks ztree in depth-first order emitting bvisit callback on visited bucket nodes.
func xwalkDFS(ctx context.Context, lo, hi_ Key, ztree *Tree, bvisit func(*RBucket)) {
	_xwalkDFS(ctx, lo, hi_, ztree, /*rparent*/nil, bvisit)
}
func _xwalkDFS(ctx context.Context, lo, hi_ Key, ztree *Tree, rparent *RTree, bvisit func(*RBucket)) {
	X := exc.Raiseif

	err := ztree.PActivate(ctx); X(err)
	defer ztree.PDeactivate()

	rtree := &RTree{Oid: ztree.POid(), Parent: rparent}

	// [i].Key ≤ [i].Child.*.Key < [i+1].Key  i ∈ [0, len([]))
	//
	// [0].Key       = -∞ ; always returned so
	// [len(ev)].Key = +∞ ; should be assumed so
	ev := ztree.Entryv()
	for i := range ev {
		xlo  := lo;  if i   > 0       { xlo  = ev[i].Key() }
		xhi_ := hi_; if i+1 < len(ev) { xhi_ = ev[i+1].Key() - 1 }

		tchild, ok := ev[i].Child().(*Tree)
		if ok {
			_xwalkDFS(ctx, xlo, xhi_, tchild, rtree, bvisit)
			continue
		}

		zbucket := ev[i].Child().(*Bucket)
		err = zbucket.PActivate(ctx); X(err)
		defer zbucket.PDeactivate()

		bkv := make(map[Key]string)
		bentryv := zbucket.Entryv()

		for _, __ := range bentryv {
			k  := __.Key()
			xv := __.Value()
			pv, ok := xv.(zodb.IPersistent)
			if !ok {
				exc.Raisef("[%d] -> %s;  want IPersistent", k, xzodb.TypeOf(xv))
			}
			data, err := ZGetBlkData(ctx, pv.PJar(), pv.POid())
			if err != nil {
				exc.Raisef("[%d]: %s", k, err)
			}
			bkv[k] = data
		}


		b := &RBucket{
			Oid:    zbucket.POid(),
			Parent: rtree,
			Keycov: blib.KeyRange{xlo, xhi_},
			KV:     bkv,
		}
		bvisit(b)
	}
}
