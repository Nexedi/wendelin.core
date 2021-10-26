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

package blib
// PP-connected subset of tree nodes.

import (
	"fmt"

	"lab.nexedi.com/kirr/neo/go/zodb"
)

const tracePPSet = false
const debugPPSet = false

// PPTreeSubSet represents PP-connected subset of tree node objects.
//
// It is
//
//	PP(xleafs)
//
// where PP(node) maps node to {node, node.parent, node.parent,parent, ...} up
// to top root from where the node is reached.
//
// The nodes in the set are represented by their Oid.
//
// Usually PPTreeSubSet is built as PP(some-leafs), but in general the starting
// nodes are arbitrary. PPTreeSubSet can also have many root nodes, thus not
// necessarily representing a subset of a single tree.
//
// Usual set operations are provided: Union, Difference and Intersection.
//
// Nodes can be added into the set via AddPath. Path is reverse operation - it
// returns path to tree node given its oid.
//
// Every node in the set comes with .parent pointer.
//
// XXX we only allow single parent/root case and report "tree corrupt" otherwise.
type PPTreeSubSet map[zodb.Oid]*nodeInTree

// nodeInTree represents tracking information about a node.
type nodeInTree struct {
	parent  zodb.Oid // parent node | InvalidOid for root
	nchild  int      // number of direct children in PPTreeSubSet referring to this node
}

// Parent returns parent of this node.
func (n *nodeInTree) Parent() zodb.Oid {
	return n.parent
}

// NChild returns number of children of this node in the tree subset.
func (n *nodeInTree) NChild() int {
	return n.nchild
}

// Has returns whether node is in the set.
func (S PPTreeSubSet) Has(oid zodb.Oid) bool {
	_, ok := S[oid]
	return ok
}

// Path returns path leading to the node specified by oid.
//
// The node must be in the set.
func (S PPTreeSubSet) Path(oid zodb.Oid) (path []zodb.Oid) {
	for {
		t, ok := S[oid]
		if !ok {
			panicf("node %s is not in the set  <- %v", oid, path)
		}

		path = append([]zodb.Oid{oid}, path...)
		oid = t.parent

		if oid == zodb.InvalidOid {
			break
		}
	}

	return path
}

// AddPath adds path to a node to the set.
//
// Note: embedded buckets (leaf node with InvalidOid) are removed from the path.
func (S PPTreeSubSet) AddPath(path []zodb.Oid) {
	S.verify()
	defer S.verify()

	l := len(path)
	if l == 0 {
		panic("empty path")
	}

	// normalize path: remove embedded bucket and check whether it was an
	// artificial empty tree.
	path = NormPath(path)

	// go through path and add nodes to the set
	parent := zodb.InvalidOid
	var pt *nodeInTree = nil
	for _, oid := range path {
		if oid == zodb.InvalidOid {
			panicf("path has node with invalid oid: %v", path)
		}

		t, already := S[oid]
		if !already {
			t = &nodeInTree{parent: parent, nchild: 0}
			S[oid] = t
		}
		if t.parent != parent {
			// XXX -> error (e.g. due to corrupt data in ZODB) ?
			panicf("node %s is reachable from multiple parents: %s %s",
				oid, t.parent, parent)
		}

		if pt != nil && !already {
			pt.nchild++
		}

		parent = oid
		pt = t
	}
}

// NormPath normalizes path.
//
// It removes embedded buckets and artificial empty trees.
// Returned slice is subslice of path and aliases its memory.
func NormPath(path []zodb.Oid) []zodb.Oid {
	l := len(path)

	// don't keep track of artificial empty tree
	if l == 1 && path[0] == zodb.InvalidOid {
		return nil
	}

	// don't explicitly keep track of embedded buckets - they all have
	// InvalidOid, and thus, if kept in S, e.g. T/B1:a and another
	// T/B2:b would lead to InvalidOid having multiple parents.
	if l == 2 && path[1] == zodb.InvalidOid {
		return path[:1]
	}

	return path
}

// ---- Union/Difference/Intersection ----

// Union returns U = PP(A.leafs | B.leafs)
//
// In other words it adds A and B nodes.
func (A PPTreeSubSet) Union(B PPTreeSubSet) PPTreeSubSet {
	U := A.Clone()
	U.UnionInplace(B)
	return U
}

// UnionInplace sets A = PP(A.leafs | B.leafs)
//
// In other words it adds B nodes to A.
func (A PPTreeSubSet) UnionInplace(B PPTreeSubSet) {
	if tracePPSet {
		fmt.Printf("\n\nUnion:\n")
		fmt.Printf("  A: %s\n", A)
		fmt.Printf("  B: %s\n", B)
		defer fmt.Printf("->U: %s\n", A)
	}

	A.verify()
	B.verify()
	defer A.verify()

	A.xUnionInplace(B)
}

// Difference returns D = PP(A.leafs \ B.leafs)
//
// In other words it removes B nodes from A while still maintaining A as PP-connected.
func (A PPTreeSubSet) Difference(B PPTreeSubSet) PPTreeSubSet {
	D := A.Clone()
	D.DifferenceInplace(B)
	return D
}


// DifferenceInplace sets A = PP(A.leafs \ B.leafs)
//
// In other words it removes B nodes from A while still maintaining A as PP-connected.
func (A PPTreeSubSet) DifferenceInplace(B PPTreeSubSet) {
	if tracePPSet {
		fmt.Printf("\n\nDifference:\n")
		fmt.Printf("  A: %s\n", A)
		fmt.Printf("  B: %s\n", B)
		defer fmt.Printf("->D: %s\n", A)
	}

	A.verify()
	B.verify()
	defer A.verify()

	A.xDifferenceInplace(B)
}

// TODO Intersection

func (A PPTreeSubSet) xUnionInplace(B PPTreeSubSet) {
	if tracePPSet {
		fmt.Printf("\n\n  xUnion:\n")
		fmt.Printf("    a: %s\n", A)
		fmt.Printf("    b: %s\n", B)
		defer fmt.Printf("  ->u: %s\n", A)
	}

	δnchild := map[zodb.Oid]int{}

	for oid, t2 := range B {
		t, already := A[oid]
		if !already {
			t = &nodeInTree{parent: t2.parent, nchild: 0}
			A[oid] = t
			// remember to nchild++ in parent
			if t.parent != zodb.InvalidOid {
				δnchild[t.parent] += 1
			}
		} else {
			if t2.parent != t.parent {
				// XXX or verify this at Track time and require
				// that update is passed only entries with the
				// same .parent? (then it would be ok to panic here)
				// XXX -> error (e.g. due to corrupt data in ZODB)
				panicf("node %s is reachable from multiple parents: %s %s",
					oid, t.parent, t2.parent)
			}
		}
	}

	A.fixup(δnchild)
}

func (A PPTreeSubSet) xDifferenceInplace(B PPTreeSubSet) {
	if tracePPSet {
		fmt.Printf("\n\n  xDifference:\n")
		fmt.Printf("    a: %s\n", A)
		fmt.Printf("    b: %s\n", B)
		defer fmt.Printf("  ->d: %s\n", A)
	}

	δnchild := map[zodb.Oid]int{}

	// remove B.leafs and their parents
	for oid, t2 := range B {
		if t2.nchild != 0 {
			continue // not a leaf
		}

		t, present := A[oid]
		if !present {
			continue // already not there
		}

		if t2.parent != t.parent {
			// XXX or verify this at Track time and require
			// that update is passed only entries with the
			// same .parent? (then it would be ok to panic here)
			// XXX -> error (e.g. due to corrupt data in ZODB)
			panicf("node %s is reachable from multiple parents: %s %s",
				oid, t.parent, t2.parent)
		}

		delete(A, oid)
		if t.parent != zodb.InvalidOid {
			δnchild[t.parent] -= 1
		}
	}

	A.fixup(δnchild)
}

// fixup performs scheduled δnchild adjustment.
func (A PPTreeSubSet) fixup(δnchild map[zodb.Oid]int) {
	A.xfixup(+1, δnchild)
}
func (A PPTreeSubSet) xfixup(sign int, δnchild map[zodb.Oid]int) {
	if debugPPSet {
		ssign := "+"
		if sign < 0 {
			ssign = "-"
		}
		fmt.Printf("\n  fixup:\n")
		fmt.Printf("      ·: %s\n", A)
		fmt.Printf("     %sδ: %v\n", ssign, δnchild)
		defer fmt.Printf("    ->·: %s\n\n", A)
	}

	gcq := []zodb.Oid{}
	for oid, δnc := range δnchild {
		t := A[oid] // t != nil as A is PP-connected
		t.nchild += sign*δnc
		if t.nchild == 0 {
			gcq = append(gcq, oid)
		}
	}

	// GC parents that became to have .nchild == 0
	for _, oid := range gcq {
		A.gc1(oid)
	}
}

// gc1 garbage-collects oid and cleans up its parent down-up.
func (S PPTreeSubSet) gc1(oid zodb.Oid) {
	t, present := S[oid]
	if !present {
		return // already not there
	}
	if t.nchild != 0 {
		panicf("gc %s %v  (nchild != 0)", oid, t)
	}

	delete(S, oid)
	oid = t.parent
	for oid != zodb.InvalidOid {
		t := S[oid]
		t.nchild--
		if t.nchild > 0 {
			break
		}
		delete(S, oid)
		oid = t.parent
	}
}


// ---- verify ----

// verify checks internal consistency of S.
func (S PPTreeSubSet) verify() {
	// TODO !debug -> return

	var badv []string
	badf := func(format string, argv ...interface{}) {
		badv = append(badv, fmt.Sprintf(format, argv...))
	}
	defer func() {
		if badv != nil {
			emsg := "S.verify: fail:\n\n"
			for _, bad := range badv {
				emsg += fmt.Sprintf("- %s\n", bad)
			}
			emsg += fmt.Sprintf("\nS: %s\n", S)
			panic(emsg)
		}
	}()

	// recompute {} oid -> children  and verify .nchild against it
	children := make(map[zodb.Oid]setOid, len(S))
	for oid, t := range S {
		if t.parent != zodb.InvalidOid {
			cc, ok := children[t.parent]
			if !ok {
				cc = make(setOid, 1)
				children[t.parent] = cc
			}
			cc.Add(oid)
		}
	}

	for oid, t := range S {
		cc := children[oid]
		if t.nchild != len(cc) {
			badf("[%s].nchild=%d  children: %s", oid, t.nchild, cc)
		}
	}

	// verify that all pointed-to parents are present in the set (= PP-connected)
	for oid := range children {
		_, ok := S[oid]
		if !ok {
			badf("oid %s is pointed to via some .parent, but is not present in the set", oid)
		}
	}
}


// ---- misc ----

// Clone returns copy of the set.
func (orig PPTreeSubSet) Clone() PPTreeSubSet {
	klon := make(PPTreeSubSet, len(orig))
	for oid, t := range orig {
		klon[oid] = &nodeInTree{parent: t.parent, nchild: t.nchild}
	}
	return klon
}

// Equal returns whether A == B.
func (A PPTreeSubSet) Equal(B PPTreeSubSet) bool {
	if len(A) != len(B) {
		return false
	}

	for oid, ta := range A {
		tb, ok := B[oid]
		if !ok {
			return false
		}

		if !(ta.parent == tb.parent && ta.nchild == tb.nchild) {
			return false
		}
	}

	return true
}

// Empty returns whether set is empty.
func (S PPTreeSubSet) Empty() bool {
	return len(S) == 0
}

func (t nodeInTree) String() string {
	return fmt.Sprintf("{p%s c%d}", t.parent, t.nchild)
}


// ---- diff/patch ----

// ΔPPTreeSubSet represents a change to PPTreeSubSet.
//
// It can be applied via PPTreeSubSet.ApplyΔ .
//
// The result B of applying δ to A is:
//
//	B = A.xDifference(δ.Del).xUnion(δ.Add)		(*)
//
// (*) NOTE δ.Del and δ.Add might have their leafs starting from non-leaf nodes in A/B.
//     This situation arises when δ represents a change in path to particular
//     node, but that node itself does not change, for example:
//
//            c*             c
//           / \            /
//         41*  42         41
//          |    |         | \
//         22   43        46  43
//               |         |   |
//              44        22  44
//
//     Here nodes {c, 41} are changed, node 42 is unlinked, and node 46 is added.
//     Nodes 43 and 44 stay unchanged.
//
//         δ.Del = c-42-43   | c-41-22
//         δ.Add = c-41-43   | c-41-46-22
//
//     The second component with "-22" builds from leaf, but the first
//     component with "-43" builds from non-leaf node.
//
//         ΔnchildNonLeafs = {43: +1}
//
//     Only complete result of applying all
//
//         - xfixup(-1, ΔnchildNonLeafs)
//         - δ.Del,
//         - δ.Add, and
//         - xfixup(+1, ΔnchildNonLeafs)
//
//     produces correctly PP-connected set.
type ΔPPTreeSubSet struct {
	Del PPTreeSubSet
	Add PPTreeSubSet
	ΔnchildNonLeafs map[zodb.Oid]int
}

// NewΔPPTreeSubSet creates new empty ΔPPTreeSubSet.
func NewΔPPTreeSubSet() *ΔPPTreeSubSet {
	return &ΔPPTreeSubSet{
		Del: PPTreeSubSet{},
		Add: PPTreeSubSet{},
		ΔnchildNonLeafs: map[zodb.Oid]int{},
	}
}

// Update updates δ to be combination of δ+δ2.
func (δ *ΔPPTreeSubSet) Update(δ2 *ΔPPTreeSubSet) {
	δ.Del.UnionInplace(δ2.Del)
	δ.Add.UnionInplace(δ2.Add)
	for oid, δnc := range δ2.ΔnchildNonLeafs {
		δ.ΔnchildNonLeafs[oid] += δnc
	}
}

// Reverse changes δ=diff(A->B) to δ'=diff(A<-B).
func (δ *ΔPPTreeSubSet) Reverse() {
	δ.Del, δ.Add = δ.Add, δ.Del
	// ΔnchildNonLeafs stays the same
}


// ApplyΔ applies δ to S.
//
// See ΔPPTreeSubSet documentation for details.
func (S PPTreeSubSet) ApplyΔ(δ *ΔPPTreeSubSet) {
	if tracePPSet {
		fmt.Printf("\n\nApplyΔ\n")
		fmt.Printf("  A: %s\n", S)
		fmt.Printf("  -: %s\n", δ.Del)
		fmt.Printf("  +: %s\n", δ.Add)
		fmt.Printf("  x: %v\n", δ.ΔnchildNonLeafs)
		defer fmt.Printf("\n->B: %s\n", S)
	}

	S.verify()
	δ.Del.verify()
	δ.Add.verify()
	defer S.verify()

	S.xfixup(-1, δ.ΔnchildNonLeafs)
	S.xDifferenceInplace(δ.Del)
	S.xUnionInplace(δ.Add)
	S.xfixup(+1, δ.ΔnchildNonLeafs)
}
