// Copyright (C) 2018-2021  Nexedi SA and Contributors.
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

// treediff provides diff for BTrees
//
// Use δZConnectTracked + treediff to compute BTree-diff caused by δZ:
//
//	δZConnectTracked(δZ, trackSet)                         -> δZTC, δtopsByRoot
//	treediff(root, δtops, δZTC, trackSet, zconn{Old,New})  -> δT, δtrack, δtkeycov
//
// δZConnectTracked computes BTree-connected closure of δZ modulo tracked set
// and also returns δtopsByRoot to indicate which tree objects were changed and
// in which subtree parts. With that information one can call treediff for each
// changed root to compute BTree-diff and δ for trackSet itself.
//
//
// BTree diff algorithm
//
// diffT, diffB and δMerge constitute the diff algorithm implementation.
// diff(A,B) works on pair of A and B whole key ranges splitted into regions
// covered by tree nodes. The splitting represents current state of recursion
// into corresponding tree. If a node in particular key range is Bucket, that
// bucket contributes to δ- in case of A, and to δ+ in case of B. If a node in
// particular key range is Tree, the algorithm may want to expand that tree
// node into its children and to recourse into some of the children.
//
// There are two phases:
//
// - Phase 1 expands A top->down driven by δZTC, adds reached buckets to δ-,
//   and queues key regions of those buckets to be processed on B.
//
// - Phase 2 starts processing from queued key regions, expands them on B and
//   adds reached buckets to δ+. Then it iterates to reach consistency in between
//   A and B because processing buckets on B side may increase δ key coverage,
//   and so corresponding key ranges has to be again processed on A. Which in
//   turn may increase δ key coverage again, and needs to be processed on B side,
//   etc...
//
// The final δ is merge of δ- and δ+.
//
// diffT has more detailed explanation of phase 1 and phase 2 logic.

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/blib"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xzodb"
)

const traceDiff = false
const debugDiff = false // topoview from xbtree.py is also handy

// ΔValue represents change in value.
type ΔValue struct {
	Old Value
	New Value
}

// String is like default %v, but uses ø for VDEL.
func (δv ΔValue) String() string {
	old, new := "ø", "ø"
	if δv.Old != VDEL {
		old = δv.Old.String()
	}
	if δv.New != VDEL {
		new = δv.New.String()
	}
	return fmt.Sprintf("{%s %s}", old, new)
}


// δZConnectTracked computes connected closure of δZ/T.
//
// δZ    - all changes in a ZODB transaction.
// δZ/T  - subset of those changes intersecting with tracking set.
// δZ/TC - connected closure for δZ/T
//
// for example for e.g. t₀->t₁->b₂ if δZ/T={t₀ b₂} -> δZ/TC=δZ/T+{t₁}
//
// δtopsByRoot = {} root -> {top changed nodes in that tree}
func δZConnectTracked(δZv []zodb.Oid, T blib.PPTreeSubSet) (δZTC setOid, δtopsByRoot map[zodb.Oid]setOid) {
	δZ  := setOid{};  for _, δ := range δZv { δZ.Add(δ) }
	δZTC = setOid{}
	δtopsByRoot = map[zodb.Oid]setOid{}

	for δ := range δZ {
		track, ok := T[δ]
		if !ok {
			continue // not tracked at all
		}

		δZTC.Add(δ)

		// go up by .parent till root or till another tracked node in the tree
		// if root  -> δtopsByRoot[root] += δ
		// if !root -> δZTC += path through which we reached another node (forming connection)
		path := []zodb.Oid{}
		node := δ
		parent := track.Parent()
		for {
			// reached root
			if parent == zodb.InvalidOid {
				root := node
				δtops, ok := δtopsByRoot[root]
				if !ok {
					δtops = setOid{}
					δtopsByRoot[root] = δtops
				}
				δtops.Add(δ)
				break
			}

			// reached another tracked node
			if δZ.Has(parent) {
				for _, δp := range path {
					δZTC.Add(δp)
				}
				break
			}

			path = append(path, parent)
			trackUp, ok := T[parent]
			if !ok {
				panicf("BUG: .p%s -> %s, but %s is not tracked", node, parent, parent)
			}
			node = parent
			parent = trackUp.Parent()
		}
	}

	return δZTC, δtopsByRoot
}

// treediff computes δT/δtrack/δtkeycov for tree/trackSet specified by root in between old..new.
//
// δtops is set of top nodes for changed subtrees.
// δZTC is connected(δZ/T) - connected closure for subset of δZ(old..new) that
// touches tracked nodes of T.
//
// Use δZConnectTracked to prepare δtops and δZTC.
func treediff(ctx context.Context, root zodb.Oid, δtops setOid, δZTC setOid, trackSet blib.PPTreeSubSet, zconnOld, zconnNew *zodb.Connection) (δT map[Key]ΔValue, δtrack *blib.ΔPPTreeSubSet, δtkeycov *blib.RangedKeySet, err error) {
	defer xerr.Contextf(&err, "treediff %s..%s %s", zconnOld.At(), zconnNew.At(), root)

	δT = map[Key]ΔValue{}
	δtrack = blib.NewΔPPTreeSubSet()
	δtkeycov = &blib.RangedKeySet{}

	tracefDiff("\ntreediff %s  δtops: %v  δZTC: %v\n", root, δtops, δZTC)
	tracefDiff(" trackSet: %v\n", trackSet)
	defer tracefDiff("\n-> δT: %v\nδtrack: %v\nδtkeycov: %v\n", δT, δtrack, δtkeycov)

	δtrackv := []*blib.ΔPPTreeSubSet{}

	for top := range δtops {
		a, err1 := zgetNodeOrNil(ctx, zconnOld, top)
		b, err2 := zgetNodeOrNil(ctx, zconnNew, top)
		err := xerr.Merge(err1, err2)
		if err != nil {
			return nil, nil, nil, err
		}

		δtop, δtrackTop, δtkeycovTop, err := diffX(ctx, a, b, δZTC, trackSet)
		if err != nil {
			return nil, nil, nil, err
		}

		debugfDiff("-> δtop: %v\n", δtop)
		debugfDiff("-> δtrackTop: %v\n", δtrackTop)
		debugfDiff("-> δtkeycovTop: %v\n", δtkeycovTop)
		for k,δv := range δtop {
			// NOTE keys cannot migrate in between two disconnected subtrees
			δv_, kdup := δT[k]
			if kdup {
				panicf("BUG: key %v present in two disconnected subtrees; δv1: %s  δv2: %s", k, δv_, δv)
			}
			δT[k] = δv
		}

		δtrackv = append(δtrackv, δtrackTop)
		δtkeycov.UnionInplace(δtkeycovTop)
	}

	// trackSet should be adjusted by merge(δtrackTops)
	for _, δ := range δtrackv {
		δtrack.Update(δ)
	}

	return δT, δtrack, δtkeycov, nil
}

// diffX computes difference in between two revisions of a tree's subtree.
//
// a, b point to top of the subtree @old and @new revisions and must be of the
// same type - tree or bucket.
//
// δZTC is connected set of objects covering δZT (objects changed in this tree in old..new).
//
// a/b can be nil; a=nil means addition, b=nil means deletion.
//
//
// δtrack is trackSet δ that needs to be applied to trackSet to keep it
// consistent with b (= a + δ).
//
// δtkeycov represents how δtrack grows (always grows) tracking set key coverage.
func diffX(ctx context.Context, a, b Node, δZTC setOid, trackSet blib.PPTreeSubSet) (δ map[Key]ΔValue, δtrack *blib.ΔPPTreeSubSet, δtkeycov *blib.RangedKeySet, err error) {
	if a==nil && b==nil {
		// DEL..DEL -> ø diff
		return map[Key]ΔValue{}, blib.NewΔPPTreeSubSet(), &blib.RangedKeySet{}, nil
	}

	var aT, bT *Tree
	var aB, bB *Bucket
	isT := false

	if a != nil {
		aT, isT = a.(*Tree)
		aB, _   = a.(*Bucket)
		if aT == nil && aB == nil {
			panicf("a: bad type %T", a)
		}
	}
	if b != nil {
		bT, isT = b.(*Tree)
		bB, _   = b.(*Bucket)
		if bT == nil && bB == nil {
			panicf("b: bad type %T", b)
		}
	}

	if a != nil && b != nil {
		if a.POid() != b.POid() {
			panicf("BUG: a.oid != b.oid  ; a: %s  b: %s", a.POid(), b.POid())
		}
		if !((aT != nil && bT != nil) || (aB != nil && bB != nil)) {
			return nil, nil, nil, fmt.Errorf("object %s: type mutated %s -> %s", a.POid(),
				zodb.ClassOf(a), zodb.ClassOf(b))
		}
	}

	if isT {
		return diffT(ctx, aT, bT, δZTC, trackSet)
	} else {
		var δtrack *blib.ΔPPTreeSubSet
		δ, err := diffB(ctx, aB, bB)
		if δ != nil {
			δtrack = blib.NewΔPPTreeSubSet()
			δtkeycov = &blib.RangedKeySet{}
		}
		return δ, δtrack, δtkeycov, err
	}
}


// ---- diff algorithm ----

// nodeInRange represents a Node coming under [lo, hi_] key range in its tree.
//
// The following operations are provided:
//
//	Path() -> []oid			- get full path to this node.
type nodeInRange struct {
	prefix   []zodb.Oid    // path to this node goes via this objects
	keycov   blib.KeyRange // key coverage
	node     Node
	done     bool // whether this node was already taken into account while computing diff
}

// rangeSplit represents set of nodes covering a range.
// nodes come with key↑ and no intersection in between their [lo,hi)
//
// The following operations are provided:
//
//	Get(key) -> node		- get node covering key
//	Expand(node) -> children	- replace node with its children
//	GetToLeaf(key) -> leaf		- get/expand to leaf node that covers key
type rangeSplit []*nodeInRange // key↑


// diffT computes difference in between two subtrees.
//
// a, b point to top of subtrees @old and @new revisions.
// δZTC is connected set of objects covering δZT (objects changed in this tree in old..new).
func diffT(ctx context.Context, A, B *Tree, δZTC setOid, trackSet blib.PPTreeSubSet) (δ map[Key]ΔValue, δtrack *blib.ΔPPTreeSubSet, δtkeycov *blib.RangedKeySet, err error) {
	tracefDiff("  diffT %s %s\n", xzodb.XidOf(A), xzodb.XidOf(B))
	defer xerr.Contextf(&err, "diffT %s %s", xzodb.XidOf(A), xzodb.XidOf(B))

	δ = map[Key]ΔValue{}
	δtrack = blib.NewΔPPTreeSubSet()
	δtkeycov = &blib.RangedKeySet{}
	defer func() {
		tracefDiff("  -> δ: %v\n", δ)
		tracefDiff("  -> δtrack: %v\n", δtrack)
		tracefDiff("  -> δtkeycov: %v\n", δtkeycov)
	}()

	if A == nil && B == nil {
		return δ, δtrack, δtkeycov, nil // ø changes
	}

	// assert A.oid == B.oid
	if A != nil && B != nil {
		Aoid := A.POid()
		Boid := B.POid()
		if Aoid != Boid {
			panicf("A.oid (%s)  !=  B.oid (%s)", Aoid, Boid)
		}
	}

	var ABoid zodb.Oid
	var AB *Tree
	if A != nil {
		ABoid = A.POid()
		AB = A
	}
	if B != nil {
		ABoid = B.POid()
		AB = B
	}

	// path prefix to A and B
	ABpath := trackSet.Path(ABoid)

	// key coverage for A and B
	ABlo  := KeyMin
	ABhi_ := KeyMax
	node := AB
ABcov:
	for i := len(ABpath)-2; i >= 0; i-- {
		xparent, err := node.PJar().Get(ctx, ABpath[i]);  /*X*/if err != nil { return nil,nil,nil, err }
		err = xparent.PActivate(ctx);  /*X*/if err != nil { return nil,nil,nil, err}
		defer xparent.PDeactivate()

		parent := xparent.(*Tree) // must succeed
		// find node in parent children and constrain ABlo/ABhi accordingly
		entryv := parent.Entryv()
		for j, entry := range entryv {
			if entry.Child() == node {
				// parent.entry[j] points to node
				// [i].Key ≤ [i].Child.*.Key < [i+1].Key
				klo  := entryv[j].Key()
				khi_ := KeyMax
				if j+1 < len(entryv) {
					khi_ = entryv[j+1].Key() - 1
				}

				if klo > ABlo {
					ABlo = klo
				}
				if khi_ < ABhi_ {
					ABhi_ = khi_
				}

				node = parent
				continue ABcov
			}
		}

		emsg := fmt.Sprintf("BUG: T%s points to T%s as parent in trackSet, but not found in T%s children\n", node.POid(), parent.POid(), parent.POid())
		children := []string{}
		for _, entry := range entryv {
			children = append(children, vnode(entry.Child()))
		}
		emsg += fmt.Sprintf("T%s children: %v\n", parent.POid(), children)
		emsg += fmt.Sprintf("trackSet: %s\n", trackSet)
		panic(emsg)
	}


	if A == nil || B == nil {
		// top of the subtree must stay in the tracking set even if the subtree is removed
		// this way, if later, the subtree will be recreated, that change won't be missed
		δtrack.Del.AddPath(ABpath)
		δtrack.Add.AddPath(ABpath)
		// δtkeycov stays ø
	}

	// A|B == nil  -> artificial empty tree
	if A == nil {
		A = zodb.NewPersistent(reflect.TypeOf(Tree{}), /*jar*/nil).(*Tree)
	}
	Bempty := false
	if B == nil {
		B = zodb.NewPersistent(reflect.TypeOf(Tree{}), /*jar*/nil).(*Tree)
		Bempty = true
	}

	// initial split ranges for A and B
	ABcov := blib.KeyRange{ABlo, ABhi_}
	prefix := ABpath[:len(ABpath)-1]
	atop := &nodeInRange{prefix: prefix, keycov: ABcov, node: A}
	btop := &nodeInRange{prefix: prefix, keycov: ABcov, node: B}
	Av := rangeSplit{atop} // nodes expanded from A
	Bv := rangeSplit{btop} // nodes expanded from B

	// for phase 2:
	Akqueue := &blib.RangedKeySet{} // queue for keys in A to be processed for δ-
	Bkqueue := &blib.RangedKeySet{} // ----//---- in B for δ+
	Akdone  := &blib.RangedKeySet{} // already processed keys in A
	Bkdone  := &blib.RangedKeySet{} // ----//---- in B
	Aktodo := func(r blib.KeyRange) {
		if !Akdone.HasRange(r) {
			δtodo := &blib.RangedKeySet{}
			δtodo.AddRange(r)
			δtodo.DifferenceInplace(Akdone)
			debugfDiff("    Akq <- %s\n", δtodo)
			Akqueue.UnionInplace(δtodo)
		}
	}
	Bktodo := func(r blib.KeyRange) {
		if !Bkdone.HasRange(r) {
			δtodo := &blib.RangedKeySet{}
			δtodo.AddRange(r)
			δtodo.DifferenceInplace(Bkdone)
			debugfDiff("    Bkq <- %s\n", δtodo)
			Bkqueue.UnionInplace(δtodo)
		}
	}

	// {} oid -> nodeInRange for all nodes we've came through in Bv:
	//                       current and previously expanded - up till top B.
	BnodeIdx := map[zodb.Oid]*nodeInRange{}
	if !Bempty {
		BnodeIdx[ABoid] = btop
	}

	// δtkeycov will be = BAdd \ ADel
	δtkeycovADel := &blib.RangedKeySet{}
	δtkeycovBAdd := &blib.RangedKeySet{}

	// phase 1: expand A top->down driven by δZTC.
	// by default a node contributes to δ-
	// a node ac does not contribute to δ- and can be skipped, if:
	// - ac is not tracked, or
	// - ac ∉ δZTC && ∃ bc from B: ac.oid == bc.oid && ac.keycov == bc.keycov
	//   (ac+ac.children were not changed, ac stays in the tree with the same key range coverage)
	Aq := []*nodeInRange{atop} // queue for A nodes that contribute to δ-
	for len(Aq) > 0 {
		debugfDiff("\n")
		debugfDiff("  aq: %v\n", Aq)
		debugfDiff("  av: %s\n", Av)
		debugfDiff("  bv: %s\n", Bv)
		ra := pop(&Aq)
		err = ra.node.PActivate(ctx);  /*X*/if err != nil { return nil,nil,nil, err }
		defer ra.node.PDeactivate()
		debugfDiff("    a: %s\n", ra)

		switch a := ra.node.(type) {
		case *Bucket:
			// a is bucket -> δ-
			δA, err := diffB(ctx, a, nil);  /*X*/if err != nil { return nil,nil,nil, err }
			err = δMerge(δ, δA);		/*X*/if err != nil { return nil,nil,nil, err }
			δtrack.Del.AddPath(ra.Path())
			δtkeycovADel.AddRange(ra.keycov)
			debugfDiff("  δtrack - %s %v\n", ra.keycov, ra.Path())

			// Bkqueue <- ra.range
			Bktodo(ra.keycov)
			ra.done = true

		case *Tree:
			// empty tree - queue holes covered by it
			if len(a.Entryv()) == 0 {
				δtrack.Del.AddPath(ra.Path())
				δtkeycovADel.AddRange(ra.keycov)
				debugfDiff("  δtrack - %s %v\n", ra.keycov, ra.Path())
				Bktodo(ra.keycov)
				continue
			}

			// a is !empty tree - expand it and queue children
			// check for each children whether it can be skipped
			achildren := Av.Expand(ra)
			for _, ac := range achildren {
				acOid := ac.node.POid()
				at, tracked := trackSet[acOid]
				if !tracked && /*cannot skip embedded bucket:*/acOid != zodb.InvalidOid {
					continue
				}

				if !δZTC.Has(acOid) && /*cannot skip embedded bucket:*/acOid != zodb.InvalidOid {
					// check B children for node with ac.oid
					// while checking expand Bv till ac.lo and ac.hi_ point to the same node
					// ( this does not give exact answer but should be a reasonable heuristic;
					//   the diff is the same if heuristic does not work and we
					//   look into and load more nodes to compute δ )
					bc, found := BnodeIdx[acOid]
					if !found {
						for {
							blo  := Bv.Get(ac.keycov.Lo)
							bhi_ := Bv.Get(ac.keycov.Hi_)
							if blo != bhi_ {
								break
							}
							bloT, ok := blo.node.(*Tree)
							if !ok {
								break // bucket
							}
							err = bloT.PActivate(ctx);  /*X*/if err != nil { return nil,nil,nil, err }
							defer bloT.PDeactivate()

							if len(bloT.Entryv()) == 0 {
								break // empty tree
							}

							bchildren := Bv.Expand(blo)
							for _, bc_ := range bchildren {
									bc_Oid := bc_.node.POid()
									BnodeIdx[bc_Oid] = bc_
									if acOid == bc_Oid {
										found = true
										bc = bc_
									}
							}
							if found {
								break
							}
						}
					}
					if found {
						// ac can be skipped if key coverage stays the same
						if ac.keycov == bc.keycov {
							// adjust trackSet since path to the node could have changed
							apath := ac.Path()
							bpath := bc.Path()
							if !pathEqual(apath, bpath) {
								δtrack.Del.AddPath(apath)
								δtrack.Add.AddPath(bpath)
								if nc := at.NChild(); nc != 0 {
									δtrack.ΔnchildNonLeafs[acOid] = nc
								}
							}

							continue
						}
					}
				}

				// ac cannot be skipped
				push(&Aq, ac)
			}
		}
	}

	// phase 2: reach consistency in between A and B.
	// Every key removed in A has to be checked for whether it is present
	// in B and contribute to δ+. In B, in turn, adding that key can add
	// other keys to δ+. Those keys, in turn, have to be checked for
	// whether they were present in A and contribute to δ-. For example:
	//
	//   [  2    4  ]       [  3    5  ]
	//    ↓   ↓    ↓         ↓   ↓    ↓
	//   |1| |23| |45|     |12| |34| |56|
	//
	// if values for all keys change, tracked={1}, change to 1 adds
	// * -B1,  which queues B.1 and leads to
	// * +B12, which queues A.2 and leads to
	// * -B23, which queues B.3 and leads to
	// * +B23, ...
	debugfDiff("\nphase 2:\n")
	for {
		debugfDiff("\n")
		debugfDiff("  av: %s\n", Av)
		debugfDiff("  bv: %s\n", Bv)

		debugfDiff("\n")
		debugfDiff("  Bkq: %s\n", Bkqueue)
		if Bkqueue.Empty() {
			break
		}

		for _, r := range Bkqueue.AllRanges() {
			lo := r.Lo
			for {
				b, err := Bv.GetToLeaf(ctx, lo);  /*X*/if err != nil { return nil,nil,nil, err }
				debugfDiff("  B k%d -> %s\n", lo, b)
				// +bucket if that bucket is reached for the first time
				if !b.done {
					var δB map[Key]ΔValue
					bbucket, ok := b.node.(*Bucket)
					if ok { // !ok means ø tree
						δB, err = diffB(ctx, nil, bbucket);  /*X*/if err != nil { return nil,nil,nil, err }
					}

					// δ <- δB
					err = δMerge(δ, δB);	/*X*/if err != nil { return nil,nil,nil, err }
					δtrack.Add.AddPath(b.Path())
					δtkeycovBAdd.AddRange(b.keycov)
					debugfDiff("  δtrack + %s %v\n", b.keycov, b.Path())

					// Akqueue <- δB
					Bkdone.AddRange(b.keycov)
					Aktodo(b.keycov)

					b.done = true
				}

				// continue with next right bucket until r coverage is complete
				if r.Hi_ <= b.keycov.Hi_ {
					break
				}
				lo = b.keycov.Hi_ + 1
			}
		}
		Bkqueue.Clear()

		debugfDiff("\n")
		debugfDiff("  Akq: %s\n", Akqueue)
		for _, r := range Akqueue.AllRanges() {
			lo := r.Lo
			for {
				a, err := Av.GetToLeaf(ctx, lo);  /*X*/if err != nil { return nil,nil,nil, err }
				debugfDiff("  A k%d -> %s\n", lo, a)
				// -bucket if that bucket is reached for the first time
				if !a.done {
					var δA map[Key]ΔValue
					abucket, ok := a.node.(*Bucket)
					if ok { // !ok means ø tree
						δA, err = diffB(ctx, abucket, nil);  /*X*/if err != nil { return nil,nil,nil, err }
					}

					// δ <- δA
					err = δMerge(δ, δA);	/*X*/if err != nil { return nil,nil,nil, err }
					δtrack.Del.AddPath(a.Path())
					// NOTE adjust δtkeycovADel only if a was originally tracked
					_, tracked := trackSet[a.node.POid()]
					if tracked {
						δtkeycovADel.AddRange(a.keycov)
						debugfDiff("  δtrack - %s %v\n", a.keycov, a.Path())
					} else {
						debugfDiff("  δtrack - [) %v\n", a.Path())
					}

					// Bkqueue <- a.range
					Akdone.AddRange(a.keycov)
					Bktodo(a.keycov)

					a.done = true
				}

				// continue with next right bucket until r coverage is complete
				if r.Hi_ <= a.keycov.Hi_ {
					break
				}
				lo = a.keycov.Hi_ + 1
			}
		}
		Akqueue.Clear()
	}

	δtkeycov = δtkeycovBAdd.Difference(δtkeycovADel)
	return δ, δtrack, δtkeycov, nil
}


// δMerge merges changes from δ2 into δ.
// δ is total-building diff, while δ2 is diff from comparing some subnodes.
func δMerge(δ, δ2 map[Key]ΔValue) error {
	debugfDiff("  δmerge %v <- %v\n", δ, δ2)
	defer debugfDiff("      -> %v\n", δ)

	// merge δ <- δ2
	for k, δv2 := range δ2 {
		δv1, already := δ[k]
		if !already {
			δ[k] = δv2
			continue
		}

		// both δ and δ2 has [k] - it can be that key
		// entry migrated from one bucket into another.
		if !( (δv1.New == VDEL && δv2.Old == VDEL) ||
		      (δv1.Old == VDEL && δv2.New == VDEL) ) {
			return fmt.Errorf("BUG or btree corrupt: [%v] has " +
					  "duplicate entries: %v, %v", k, δv1, δv2)
		}

		δv := ΔValue{}
		switch {

		// x -> ø | ø -> x
		// ø -> ø
		case δv2.Old == VDEL && δv2.New == VDEL: // δv2 == hole
			δv = δv1

		// ø -> ø
		// y -> ø | ø -> y
		case δv1.Old == VDEL && δv1.New == VDEL: // δv1 == hole
			δv = δv2

		// ø -> x		-> y->x
		// y -> ø
		case δv2.New == VDEL:
			δv.Old = δv2.Old
			δv.New = δv1.New

		// x -> ø		-> x->y
		// ø -> y
		default:
			δv.Old = δv1.Old
			δv.New = δv2.New
		}

		debugfDiff("      [%v] merge %s %s  -> %s\n", k, δv1, δv2, δv)
		if δv.Old != δv.New {
			δ[k] = δv
		} else {
			delete(δ, k) // NOTE also annihilates hole migration
		}
	}

	return nil
}

// diffB computes difference in between two buckets.
// see diffX for details.
func diffB(ctx context.Context, a, b *Bucket) (δ map[Key]ΔValue, err error) {
	tracefDiff("  diffB %s %s\n", xzodb.XidOf(a), xzodb.XidOf(b))
	defer xerr.Contextf(&err, "diffB %s %s", xzodb.XidOf(a), xzodb.XidOf(b))

	var av []BucketEntry
	var bv []BucketEntry
	if a != nil {
		err = a.PActivate(ctx);  if err != nil { return nil, err }
		defer a.PDeactivate()
		av  = a.Entryv() // key↑
	}
	if b != nil {
		err = b.PActivate(ctx);  if err != nil { return nil, err }
		defer b.PDeactivate()
		bv  = b.Entryv() // key↑
	}

	δ = map[Key]ΔValue{}
	defer tracefDiff("    -> δb: %v\n", δ)
	//debugfDiff("    av: %v", av)
	//debugfDiff("    bv: %v", bv)

	for len(av) > 0 || len(bv) > 0 {
		ka, va := KeyMax, VDEL
		kb, vb := KeyMax, VDEL

		if len(av) > 0 {
			ka = av[0].Key()
			va, err = vOid(av[0].Value())
			if err != nil {
				return nil, fmt.Errorf("a[%v]: %s", ka, err)
			}
		}
		if len(bv) > 0 {
			kb = bv[0].Key()
			vb, err = vOid(bv[0].Value())
			if err != nil {
				return nil, fmt.Errorf("b[%v]: %s", kb, err)
			}
		}

		switch {
		case ka < kb: // -a[0]
			δ[ka] = ΔValue{va, VDEL}
			av = av[1:]

		case ka > kb: // +b[0]
			δ[kb] = ΔValue{VDEL, vb}
			bv = bv[1:]

		// ka == kb   // va->vb
		default:
			if va != vb {
				δ[ka] = ΔValue{va, vb}
			}
			av = av[1:]
			bv = bv[1:]
		}
	}

	return δ, nil
}

// vOid returns OID of a value object.
// it is an error if value is not persistent object.
func vOid(xvalue interface{}) (zodb.Oid, error) {
	value, ok := xvalue.(zodb.IPersistent)
	if !ok {
		return zodb.InvalidOid, fmt.Errorf("%T is not a persitent object", xvalue)
	}
	return value.POid(), nil
}


// ---- nodeInRange + rangeSplit ----

func (rn *nodeInRange) String() string {
	done := " ";   if rn.done         { done = "*" }
	return fmt.Sprintf("%s%s%s", done, rn.keycov, vnode(rn.node))
}

// Path returns full path to this node.
func (n *nodeInRange) Path() []zodb.Oid {
	// return full copy - else .prefix can become aliased in between children of a node
	return append([]zodb.Oid{}, append(n.prefix, n.node.POid())...)
}

func (rs rangeSplit) String() string {
	if len(rs) == 0 {
		return "ø"
	}

	s := ""
	for _, rn := range rs {
		if s != "" {
			s += " "
		}
		s += fmt.Sprintf("%s", rn)
	}
	return s
}


// Get returns node covering key k.
// Get panics if k is not covered.
func (rs rangeSplit) Get(k Key) *nodeInRange {
	rnode, ok := rs.Get_(k)
	if !ok {
		panicf("key %v not covered;  coverage: %s", k, rs)
	}
	return rnode
}

// Get_ returns node covering key k.
func (rs rangeSplit) Get_(k Key) (rnode *nodeInRange, ok bool) {
	i := sort.Search(len(rs), func(i int) bool {
		return k <= rs[i].keycov.Hi_
	})
	if i == len(rs) {
		return nil, false // key not covered
	}

	rn := rs[i]
	if !rn.keycov.Has(k) {
		panicf("BUG: get(%v) -> %s;  coverage: %s", k, rn, rs)
	}

	return rn, true
}

// Expand replaces rnode with its children.
//
// rnode must be initially in *prs.
// rnode.node must be tree.
// rnode.node must be already activated.
//
// inserted children are returned for convenience.
func (prs *rangeSplit) Expand(rnode *nodeInRange) (children rangeSplit) {
	rs := *prs
	i := sort.Search(len(rs), func(i int) bool {
		return rnode.keycov.Hi_ <= rs[i].keycov.Hi_
	})
	if i == len(rs) || rs[i] != rnode {
		panicf("%s not in rangeSplit;  coverage: %s", rnode, rs)
	}

	// [i].Key ≤ [i].Child.*.Key < [i+1].Key  i ∈ [0, len([]))
	//
	// [0].Key       = -∞ ; always returned so
	// [len(ev)].Key = +∞ ; should be assumed so
	tree  := rnode.node.(*Tree)
	treev := tree.Entryv()
	children = make(rangeSplit, 0, len(treev)+1)
	for i := range treev {
		lo := rnode.keycov.Lo
		if i > 0 {
			lo = treev[i].Key()
		}
		hi_ := rnode.keycov.Hi_
		if i < len(treev)-1 {
			hi_ = treev[i+1].Key()-1 // NOTE -1 because it is hi_] not hi)
		}

		children = append(children, &nodeInRange{
					prefix: rnode.Path(),
					keycov: blib.KeyRange{lo, hi_},
					node:   treev[i].Child(),
		})
	}

	// del[i]; insert(@i, children)
	*prs = append(rs[:i], append(children, rs[i+1:]...)...)
	return children
}

// GetToLeaf returns leaf node corresponding to key k.
//
// Leaf is usually bucket node, but, in the sole single case of empty tree, can be that root tree node.
// GetToLeaf expands step-by-step every tree through which it has to traverse to next depth level.
//
// GetToLeaf panics if k is not covered.
func (prs *rangeSplit) GetToLeaf(ctx context.Context, k Key) (*nodeInRange, error) {
	rnode, ok, err := prs.GetToLeaf_(ctx, k)
	if err == nil && !ok {
		panicf("key %v not covered;  coverage: %s", k, *prs)
	}
	return rnode, err
}

// GetToLeaf_ is comma-ok version of GetToLeaf.
func (prs *rangeSplit) GetToLeaf_(ctx context.Context, k Key) (rnode *nodeInRange, ok bool, err error) {
	rnode, ok = prs.Get_(k)
	if !ok {
		return nil, false, nil // key not covered
	}

	for {
		switch rnode.node.(type) {
		// bucket = leaf
		case *Bucket:
			return rnode, true, nil
		}

		// its tree -> activate to expand; check for ø case
		tree := rnode.node.(*Tree)
		err = tree.PActivate(ctx)
		if err != nil {
			return nil, false, err
		}
		defer tree.PDeactivate()

		// empty tree -> don't expand - it is already leaf
		if len(tree.Entryv()) == 0 {
			return rnode, true, nil
		}

		// expand tree children
		children := prs.Expand(rnode)
		rnode = children.Get(k) // k must be there
	}
}


// ---- stack of nodeInRange ----

// push pushes element to node stack.
func push(nodeStk *[]*nodeInRange, top *nodeInRange) {
	*nodeStk = append(*nodeStk, top)
}

// pop pops top element from node stack.
func pop(nodeStk *[]*nodeInRange) *nodeInRange {
	stk := *nodeStk
	l := len(stk)
	top := stk[l-1]
	*nodeStk = stk[:l-1]
	return top
}


func tracefDiff(format string, argv ...interface{}) {
	if traceDiff {
		fmt.Printf(format, argv...)
	}
}

func debugfDiff(format string, argv ...interface{}) {
	if debugDiff {
		fmt.Printf(format, argv...)
	}
}
