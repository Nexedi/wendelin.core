// Copyright (C) 2020-2021  Nexedi SA and Contributors.
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
// tests for δbtail.go / treediff.go
//
// This are the main tests for ΔBtail functionality. There are two primary testing concerns:
//
//   1) to verify treediff algorithm, and
//   2) to verify how ΔTtail rebuilds its history entries when set of tracked keys
//      grows upon either new Track requests, or upon Update that turned out to
//      trigger such growth of set of tracked keys.
//
// TestΔBTail*/Update and TestΔBTail*/rebuild exercise points "1" and "2" correspondingly.
//
// There are 2 testing approaches:
//
//   a) transition a BTree in ZODB through particular tricky tree topologies
//      and feed ΔBtail through created database transactions.
//   b) transition a BTree in ZODB through      random       tree topologies
//      and feed ΔBtail through created database transactions.
//
// TestΔBTail and TestΔBTailRandom implement approaches "a" and "b" correspondingly.

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/blib"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/xbtreetest"
)

type Δstring = xbtreetest.Δstring

// KAdjMatrix is adjacency matrix that describes how set of tracked keys
// changes (always grow) when tree topology is updated from A to B.
//
// Adjacency matrix
//
// A,  B  - topologies			ex T3/B1,2-B3        T3/B1-B3,4
// Av, Bv - topologies with values	ex T3/B1:a,2:b-B3:c  T3/B1:d-B3:c,4:e
//
// δ(Av, Bv) - full diff {k -> v} for changed keys; DEL = k -> ø
//             ex δ(T3/B1:a,2:b-B3:c, T3/B1:d-B3:c,4:e) = {-1:a +1:d -2:b +4:e}
//
// Δ(T, Av, Bv) - subset of δ(Av, Bv) corresponding to initial tracking set T
//             ex Δ({1}, T3/B1:a,2:b-B3:c, T3/B1:d-B3:c,4:e) = {-1:a +1:d -2:b}  (no +4:e)
//
// kadj(A,B) {} k -> {k'}:	- adjacency matrix
//	∃v1,v2:  k'∈ Δ({k}, Av1, Bv2)
//
//	ex kadj(T3/B1:a,2:b-B3:c, T3/B1:d-B3:c,4:e) =
//		= {1:{1,2} 2:{1,2} 3:{3,4,∞} 4:{3,4,∞} ∞:{3,4,∞}}	k ∈ A+B+{∞}
//		  + {0:{0,1,2} 5:{5,3,4,∞} + ... all possible keys}
//
// Δ(T, Av, Bv) = δ(Av, Bv)/kadj(A,B)[T]
//
// i.e. = δ(Av, Bv) for k: k ∈ U kadj(A,B)[·]
//                            ·∈T
//
// Use:
//
//	- KAdj(A,B)         to build adjacency matrix for A -> B transition.
//	- kadj.Map(keys)    to compute kadj·keys.
//	- kadj1.Mul(kadj2)  to compute kadj1·kadj2.
//
// Note: adjacency matrix is symmetric (KAdj verifies this at runtime):
//
//	KAdj(A,B) == KAdj(B,A)
type KAdjMatrix map[Key]setKey


// ΔBTestEntry represents one entry in ΔBTail tests.
type ΔBTestEntry struct {
	tree   string      // next tree topology
	kadjOK KAdjMatrix  // adjacency matrix against previous case (optional)
	flags  ΔBTestFlags
}

type ΔBTestFlags int
const ΔBTest_SkipUpdate  ΔBTestFlags = 1 // skip verifying Update  for this test entry
const ΔBTest_SkipRebuild ΔBTestFlags = 2 // skip verifying rebuild for this test entry

// ΔBTest converts xtest into ΔBTestEntry.
// xtest can be string|ΔBTestEntry.
func ΔBTest(xtest interface{}) ΔBTestEntry {
	var test ΔBTestEntry
	switch xtest := xtest.(type) {
	case string:
		test.tree   = xtest
		test.kadjOK = nil
		test.flags  = 0
	case ΔBTestEntry:
		test = xtest
	default:
		panicf("BUG: ΔBTest: bad type %T", xtest)
	}
	return test
}

// TestΔBTail verifies ΔBTail for explicitly provided tree topologies.
func TestΔBTail(t *testing.T) {
	// K is shorthand for setKey
	K := func(keyv ...Key) setKey {
		ks := setKey{}
		for _, k := range keyv { ks.Add(k) }
		return ks
	}
	// oo is shorthand for KeyMax
	const oo = KeyMax
	// A is shorthand for KAdjMatrix
	type A = KAdjMatrix
	// Δ is shorthand for ΔBTestEntry
	Δ := func(tree string, kadjOK A) (test ΔBTestEntry) {
		test.tree   = tree
		test.kadjOK = kadjOK
		return test
	}

	// test known cases going through tree1 -> tree2 -> ...
	testv := []interface{} {
		// start from non-empty tree to verify both ->empty and empty-> transitions
		"T/B1:a,2:b",

		// empty
		"T/B:",

		// +1
		Δ("T/B1:a",
			A{1:  K(1,oo),
			  oo: K(1,oo)}),

		// +2
		Δ("T/B1:a,2:b",
			A{1:  K(1,2,oo),
			  2:  K(1,2,oo),
			  oo: K(1,2,oo)}),

		// -1
		Δ("T/B2:b",
			A{1:  K(1,2,oo),
			  2:  K(1,2,oo),
			  oo: K(1,2,oo)}),

		// 2: b->c
		Δ("T/B2:c",
			A{2:  K(2,oo),
			  oo: K(2,oo)}),

		// +1 in new bucket (to the left)
		Δ("T2/B1:a-B2:c",
			A{1:  K(1,2,oo),
			  2:  K(1,2,oo),
			  oo: K(1,2,oo)}),

		// +3 in new bucket (to the right)
		Δ("T2,3/B1:a-B2:c-B3:c",
			A{1:  K(1),
			  2:  K(2,3,oo),
			  3:  K(2,3,oo),
			  oo: K(2,3,oo)}),

		// bucket split; +3 in new bucket
		"T/B1:a,2:b",
		Δ("T2/B1:a-B2:b,3:c",
			A{1:  K(1,2,3,oo),
			  2:  K(1,2,3,oo),
			  3:  K(1,2,3,oo),
			  oo: K(1,2,3,oo)}),

		// bucket split; +3 in new bucket; +4 +5 in another new bucket
		// everything becomes tracked because original bucket had [-∞,∞) coverage
		"T/B1:a,2:b",
		Δ("T2,4/B1:a-B2:b,3:c-B4:d,5:e",
			A{1:  K(1,2,3,4,5,oo),
			  2:  K(1,2,3,4,5,oo),
			  3:  K(1,2,3,4,5,oo),
			  4:  K(1,2,3,4,5,oo),
			  5:  K(1,2,3,4,5,oo),
			  oo: K(1,2,3,4,5,oo)}),

		// reflow of keys: even if tracked={1}, changes to all B nodes need to be rescanned:
		// +B12 forces to look in -B23 which adds -3 into δ, which
		// forces to look into +B34 and so on.
		"T2,4,6/B1:a-B2:b,3:c-B4:d,5:e-B6:f,7:g",
		Δ("T3,5,7/B1:g,2:f-B3:e,4:d-B5:c,6:b-B7:a",
			A{1:  K(1,2,3,4,5,6,7,oo),
			  2:  K(1,2,3,4,5,6,7,oo),
			  3:  K(1,2,3,4,5,6,7,oo),
			  4:  K(1,2,3,4,5,6,7,oo),
			  5:  K(1,2,3,4,5,6,7,oo),
			  6:  K(1,2,3,4,5,6,7,oo),
			  7:  K(1,2,3,4,5,6,7,oo),
			  oo: K(1,2,3,4,5,6,7,oo)}),

		// reflow of keys for rebuild: even if tracked1={}, tracked2={1}, changes to
		// all A/B/C nodes need to be rescanned. Contrary to the above case the reflow
		// is not detectable at separate diff(A,B) and diff(B,C) runs.
		"T3,5,7/B1:a,2:b-B3:c,4:d-B5:e,6:f-B7:g,8:h",
		"T/B1:b",
		"T2,4,6/B1:a-B2:b,3:c-B4:d,5:e-B6:f,7:g",
		// similar situation where rebuild has to detect reflow in between non-neighbour trees
		"T3,6/B1:a,2:b-B3:c,4:d-B6:f,7:g",
		"T4,7/B1:b-B4:d,5:e-B7:g,8:h",
		"T2,5,8/B1:a-B2:b,3:c-B5:e,6:f-B8:h,9:i",

		// depth=2; bucket split; +3 in new bucket; left T remain
		// _unchanged_ even though B under it is modified.
		"T/T/B1:a,2:b",
		Δ("T2/T-T/B1:a-B2:b,3:c",
			A{1:  K(1,2,3,oo),
			  2:  K(1,2,3,oo),
			  3:  K(1,2,3,oo),
			  oo: K(1,2,3,oo)}),

		// depth=2; like prev. case, but additional right arm with +4 +5 is added.
		"T/T/B1:a,2:b",
		Δ("T2,4/T-T-T/B1:a-B2:b,3:c-B4:d,5:e",
			A{1:  K(1,2,3,4,5,oo),
			  2:  K(1,2,3,4,5,oo),
			  3:  K(1,2,3,4,5,oo),
			  4:  K(1,2,3,4,5,oo),
			  5:  K(1,2,3,4,5,oo),
			  oo: K(1,2,3,4,5,oo)}),

		// depth=2; bucket split; +3 in new bucket; t0 and t1 split;
		// +right arm (T7/B45-B89).
		"T/T/B1:a,2:b",
		Δ("T4/T2-T7/B1:a-B2:b,3:c-B4:d,5:e-B8:h,9:i",
			A{1:  K(1,2,3,4,5,8,9,oo),
			  2:  K(1,2,3,4,5,8,9,oo),
			  3:  K(1,2,3,4,5,8,9,oo),
			  4:  K(1,2,3,4,5,8,9,oo),
			  5:  K(1,2,3,4,5,8,9,oo),
			  8:  K(1,2,3,4,5,8,9,oo),
			  9:  K(1,2,3,4,5,8,9,oo),
			  oo: K(1,2,3,4,5,8,9,oo)}),


		// 2 reflow to right B neighbour; 8 splits into new B; δ=ø
		"T3/B1:a,2:b-B4:d,8:h",
		"T2,5/B1:a-B2:b,4:d-B8:h",

		// case where kadj does not grow too much as leafs coverage remains stable
		"T4,8/B1:a,2:b-B5:d,6:e-B10:g,11:h",
		Δ("T4,8/B2:b,3:c-B6:e,7:f-B11:h,12:i",
			A{1:  K(1,2,3),
			  2:  K(1,2,3),
			  3:  K(1,2,3),
			  5:  K(5,6,7),
			  6:  K(5,6,7),
			  7:  K(5,6,7,),
			  10: K(10,11,12,oo),
			  11: K(10,11,12,oo),
			  12: K(10,11,12,oo),
			  oo: K(10,11,12,oo)}),


		// tree deletion
		// having ø in the middle of the test cases exercises all:
		// * `ø -> Tree ...`		(tree is created anew),
		// * `... Tree -> ø`		(tree is deleted), and
		// * `Tree -> ø -> Tree`	(tree is deleted and then recreated)
		xbtreetest.DEL,

		// tree rotation
		"T3/B2:b-B3:c,4:d",
		"T5/T3-T7/B2:a-B3:a,4:a-B6:a-B8:a",

		// found by AllStructs ([1] is not changed, but because B1 is
		// unlinked and 1 migrates to other bucket, changes in that
		// other bucket must be included into δT)
		"T1,2/B0:e-B1:d-B2:g,3:a",
		"T1/B0:d-B1:d,2:d",
		// ----//---- with depth=2
		"T1,2/T-T-T/B0:a-B1:b-B2:c,3:d",
		"T1/T-T/B0:e-B1:b,2:f",


		// degenerate topology from ZODB tests
		// https://github.com/zopefoundation/ZODB/commit/6cd24e99f89b
		// https://github.com/zopefoundation/BTrees/blob/4.7.2-1-g078ba60/BTrees/tests/testBTrees.py#L20-L57
		"T4/T2-T/T-T-T6,10/B1:a-B3:b-T-T-T/T-B7:c-B11:d/B5:e",
		"T/B1:e,5:d,7:c,8:b,11:a", // -3 +8

		// was leading treegen to generate corrupt trees
		"T/T1/T-T/B0:g-B1:e,2:d,3:h",
		"T1/T-T3/B0:g-T-T/B1:e,2:d-B3:h",

		// was leading to wrongly computed trackSet2 due to top not
		// being tracked to tree root.
		"T/T1/B0:a-B1:b",
		"T/T1/T-T/B0:c-B1:d",

		// was leading to wrongly computed trackSet2: leaf bucket not
		// reparented to root.
		"T/T/B0:a",
		"T/B0:a",

		// δtkeycov grows due to change in parent tree only
		"T3/B1:a-B8:c",
		"T7/B1:a-B8:c",
		// ----//----
		"T3/B1:a,2:b-B8:c,9:d",
		"T7/B1:a,2:b-B8:c,9:d",
		// ----//---- depth=2
		"T3/T-T/B1:a,2:b-B8:c,9:d",
		"T7/T-T/B1:a,2:b-B8:c,9:d",
		// ----//---- found by AllStructs
		"T1,3/B0:d-B1:a-B3:d,4:g",
		"T1,4/B0:e-B1:a-B4:c",
		// ----//---- found by AllStructs
		"T2,4/T-T-T/T1-T-B4:f/T-T-B3:f/B0:h-B1:f",
		"T4/T-T/B3:f-T/B4:a",


		// ---- found by AllStructs ----

		// trackSet2 wrongly computed due to top not being tracked to tree root
		"T2/T1-T/B0:g-B1:b-T/B2:b,3:a",
		"T2/T1-T/T-T-B2:a/B0:c-B1:g",

		// unchanged node is reparented
		"T1/B0:c-B1:f",
		"T1/T-T/B0:c-T/B1:h",

		// SIGSEGV in ApplyΔ
		"T1/T-T2/T-B1:c-B2:c/B0:g",
		"T1/T-T/B0:g-T/B1:e",

		// trackSet corruption: oid is pointed by some .parent but is not present
		"T1/T-T/B0:g-T2/B1:h-B2:g",
		"T/T1/T-T2/B0:e-B1:f-B2:g",

		// ApplyΔ -> xunion: node is reachable from multiple parents
		// ( because xdifference did not remove common non-leaf node
		//   under which there were also other changed, but not initially
		//   tracked, node )
		"T4/T1-T/T-T2-B4:c/T-T-T/B0:f-B1:h-B2:g,3:b",
		"T1/T-T/T-T2/T-T-T/B0:f-B1:h-B2:f",
		// ----//----
		"T3/T1-T/T-T2-T/B0:b-T-T-B3:h/B1:e-B2:a",
		"T1/T-T4/T-T2-T/T-T-T-T/B0:b-B1:e-B2:a,3:c-B4:e",
		// ----//----
		"T/T1,3/T-T2-T4/B0:b-T-T-B3:g-B4:c/B1:b-B2:e",
		"T1,4/T-T-T/T-T2-B4:f/T-T-T/B0:h-B1:b-B2:h,3:a",

		"T2/B1:a-B7:g",
		"T2,8/B1:a-B7:g-B9:i",

		"T2/B1:a-B2:b", "T/B1:a,2:b",
		"T2,3/B1:a-B2:b-B3:c", "T/B1:a,2:b",
		"T2,3/B1:a-B2:c-B3:c", "T/B1:a,2:b",

		"T2/B1:a-B2:c", "T2,3/B1:a-B2:c-B3:c",

		"T2/B1:a-B3:c",
		Δ("T2/T-T4/B1:b-B3:d-B99:h",
			A{1:  K(1),
			  3:  K(3,99,oo),
			  99: K(3,99,oo),
			  oo: K(3,99,oo)}),

		// Update was adding extra dup point to vδBroots
		"T4/T1,3-T/T-T-T-T/B0:b-B1:c,2:j-T-B4:d/B3:h",
		"T/T2,3/T-T-T/B1:d-B2:c-B3:i",
		"T2/B1:g-B2:c,3:i",
	}
	// direct  tree_i -> tree_{i+1} -> _{i+2} ...   plus
	// reverse ... tree_i <- _{i+1} <- _{i+2}
	kadjOK := ΔBTest(testv[len(testv)-1]).kadjOK
	for i := len(testv)-2; i >= 0; i-- {
		test := ΔBTest(testv[i])
		kadjOK, test.kadjOK = test.kadjOK, kadjOK
		testv = append(testv, test)
	}

	testq := make(chan ΔBTestEntry)
	go func() {
		defer close(testq)
		for _, test := range testv {
			testq <- ΔBTest(test)
		}
	}()
	testΔBTail(t, testq)

}

// TestΔBTailRandom verifies ΔBtail on random tree topologies generated by AllStructs.
func TestΔBTailRandom(t *testing.T) {
	X := exc.Raiseif

	// considerations:
	// - maxdepth↑ better for testing (more tricky topologies)
	// - maxsplit↑ not so better for testing (leave s=1, max s=2)
	// - |kmin - kmax| affects N(variants) significantly
	//   -> keep key range small  (dumb increase does not help testing)
	// - N(keys) affects N(variants) significantly
	//   -> keep Nkeys reasonably small/medium  (dumb increase does not help testing)
	//
	// - spawning python subprocess is very slow (takes 300-500ms for
	//   imports; https://github.com/pypa/setuptools/issues/510)
	//   -> we spawn `treegen allstructs` once and use request/response approach.

	maxdepth := xbtreetest.N(2,  3,  4)
	maxsplit := xbtreetest.N(1,  2,  2)
	n        := xbtreetest.N(10,10,100)
	nkeys    := xbtreetest.N(3,  5, 10)

	// server to generate AllStructs(kv, ...)
	sg, err := xbtreetest.StartAllStructsSrv(); X(err)
	defer func() {
		err := sg.Close(); X(err)
	}()

	// random-number generator
	rng, seed := xbtreetest.NewRand()
	t.Logf("# maxdepth=%d maxsplit=%d nkeys=%d n=%d seed=%d", maxdepth, maxsplit, nkeys, n, seed)

	// generate (kv1, kv2, kv3) randomly

	// keysv1, keysv2 and keysv3 are random shuffle of IntSets
	var keysv1 [][]int
	var keysv2 [][]int
	var keysv3 [][]int
	for keys := range IntSets(nkeys) {
		keysv1 = append(keysv1, keys)
		keysv2 = append(keysv2, keys)
		keysv3 = append(keysv3, keys)
	}
	v := keysv1
	rng.Shuffle(len(v), func(i,j int) { v[i], v[j] = v[j], v[i] })
	v = keysv2
	rng.Shuffle(len(v), func(i,j int) { v[i], v[j] = v[j], v[i] })
	v = keysv3
	rng.Shuffle(len(v), func(i,j int) { v[i], v[j] = v[j], v[i] })

	// given random (kv1, kv2, kv3) generate corresponding set of random tree
	// topology sets (T1, T2, T3). Then iterate through T1->T2->T3->T1...
	// elements such that all right-directed triplets are visited and only once.
	// Test Update and rebuild on the generated tree sequences.
	vv := "abcdefghij"
	randv := func() string {
		i := rng.Intn(len(vv))
		return vv[i:i+1]
	}

	// the number of pairs    is 3·n^2
	// the number of triplets is   n^3
	//
	// limit n for emitted triplets, so that the amount of work for Update
	// and rebuild tests is approximately of the same order.
	nrebuild := int(math.Ceil(math.Pow(3*float64(n*n), 1./3)))
	// in non-short mode rebuild tests are exercising more keys variants, plus every test case
	// takes more time. Compensate for that as well.
	if !testing.Short() {
		nrebuild -= 4
	}

	testq := make(chan ΔBTestEntry)
	go func() {
		defer close(testq)
		for i := range keysv1 {
			keys1 := keysv1[i]
			keys2 := keysv2[i]
			keys3 := keysv3[i]

			kv1 := map[Key]string{}
			kv2 := map[Key]string{}
			kv3 := map[Key]string{}
			for _, k := range keys1 { kv1[Key(k)] = randv() }
			for _, k := range keys2 { kv2[Key(k)] = randv() }
			for _, k := range keys3 { kv3[Key(k)] = randv() }

			treev1, err1 := sg.AllStructs(kv1, maxdepth, maxsplit, n, rng.Int63())
			treev2, err2 := sg.AllStructs(kv2, maxdepth, maxsplit, n, rng.Int63())
			treev3, err3 := sg.AllStructs(kv3, maxdepth, maxsplit, n, rng.Int63())
			err := xerr.Merge(err1, err2, err3)
			if err != nil {
				t.Fatal(err)
			}

			emit := func(tree string, flags ΔBTestFlags) {
				// skip emitting this entry if both Update and
				// Rebuild are requested to be skipped.
				if flags == (ΔBTest_SkipUpdate | ΔBTest_SkipRebuild) {
					return
				}
				testq <- ΔBTestEntry{tree, nil, flags}
			}

			URSkipIf := func(ucond, rcond bool) ΔBTestFlags {
				var flags ΔBTestFlags
				if ucond {
					flags |= ΔBTest_SkipUpdate
				}
				if rcond {
					flags |= ΔBTest_SkipRebuild
				}
				return flags
			}

			for j := range treev1 {
				for k := range treev2 {
					for l := range treev3 {
						// limit rebuild to subset of tree topologies,
						// because #(triplets) grow as n^3. See nrebuild
						// definition above for details.
						norebuild := (j >= nrebuild ||
						              k >= nrebuild ||
							      l >= nrebuild)

						// C_{l-1} -> Aj   (pair first seen on k=0)
						emit(treev1[j], URSkipIf(k != 0, norebuild))

						// Aj -> Bk   (pair first seen on l=0)
						emit(treev2[k], URSkipIf(l != 0, norebuild))

						// Bk -> Cl   (pair first seen on j=0)
						emit(treev3[l], URSkipIf(j != 0, norebuild))
					}
				}
			}
		}
	}()

	testΔBTail(t, testq)
}

// testΔBTail verifies ΔBTail on sequence of tree topologies coming from testq.
func testΔBTail(t_ *testing.T, testq chan ΔBTestEntry) {
	t := xbtreetest.NewT(t_)

	var t0 *xbtreetest.Commit
	for test := range testq {
		t1 := t.Head()
		t2 := t.Commit(test.tree)
		t.AtSymbReset(t2, 2)

		subj := fmt.Sprintf("%s -> %s", t1.Tree, t2.Tree)
		//t.Logf("\n\n\n**** %s ****\n\n", subj)

		// KAdj
		if kadjOK := test.kadjOK; kadjOK != nil {
			t.Run(fmt.Sprintf("KAdj/%s→%s", t1.Tree, t2.Tree), func(t *testing.T) {
				kadj := KAdj(t1, t2)
				if !reflect.DeepEqual(kadj, kadjOK) {
					t.Fatalf("BUG: computed kadj is wrong:\nkadjOK: %v\nkadj  : %v\n\n", kadjOK, kadj)
				}
			})
		}

		// ΔBTail.Update
		if test.flags & ΔBTest_SkipUpdate == 0 {
			xverifyΔBTail_Update(t.T, subj, t.DB, t.Root(), t1,t2)
		}

		// ΔBTail.rebuild
		if t0 != nil && (test.flags & ΔBTest_SkipRebuild == 0) {
			xverifyΔBTail_rebuild(t.T, t.DB, t.Root(), t0,t1,t2)
		}

		t0, t1 = t1, t2
	}
}


// xverifyΔBTail_Update verifies how ΔBTail handles ZODB update for a tree with changes in between t1->t2.
//
// Note: this test verifies only single treediff step of ΔBtail.Update.
//       the cycling phase of update, that is responsible to recompute older
//       entries when key coverage grows, is exercised by
//       xverifyΔBTail_rebuild.
func xverifyΔBTail_Update(t *testing.T, subj string, db *zodb.DB, treeRoot zodb.Oid, t1, t2 *xbtreetest.Commit) {
	// verify transition at1->at2 for all initial states of tracked {keys} from kv1 + kv2 + ∞
	t.Run(fmt.Sprintf("Update/%s→%s", t1.Tree, t2.Tree), func(t *testing.T) {
		allKeys := allTestKeys(t1, t2)
		allKeyv := allKeys.SortedElements()

		kadj12 := KAdj(t1, t2)

		// verify at1->at2 for all combination of initial tracked keys.
		for kidx := range IntSets(len(allKeyv)) {
			keys := setKey{}
			for _, idx := range kidx {
				keys.Add(allKeyv[idx])
			}

			// this t.Run allocates and keeps too much memory in -verylong
			// also it is not so useful as above "Update/t1->t2"
			//t.Run(fmt.Sprintf(" track=%s", keys), func(t *testing.T) {
				xverifyΔBTail_Update1(t, subj, db, treeRoot, t1,t2, keys, kadj12)
			//})
		}
	})
}

// xverifyΔBTail_Update1 verifies how ΔBTail handles ZODB update at1->at2 from initial
// tracked state defined by initialTrackedKeys.
func xverifyΔBTail_Update1(t *testing.T, subj string, db *zodb.DB, treeRoot zodb.Oid, t1,t2 *xbtreetest.Commit, initialTrackedKeys setKey, kadj KAdjMatrix) {
	X := exc.Raiseif
	//t.Logf("\n>>> Track=%s\n", initialTrackedKeys)

	δZ  := t2.ΔZ
	d12 := t2.Δxkv

	var TrackedδZ setKey = nil
	var kadjTrackedδZ setKey = nil
	var δT, δTok map[Key]Δstring = nil, nil
	δZset := setOid{}
	for _, oid := range δZ.Changev {
		δZset.Add(oid)
	}

	// badf queues error message to be reported on return.
	var badv []string
	badf := func(format string, argv ...interface{}) {
		badv = append(badv, fmt.Sprintf(format, argv...))
	}
	defer func() {
		if badv != nil || t.Failed() {
			emsg := fmt.Sprintf("%s  ; tracked=%v :\n\n", subj, initialTrackedKeys)
			emsg += fmt.Sprintf("d12:  %v\nδTok: %v\nδT:   %v\n\n", d12, δTok, δT)
			emsg += fmt.Sprintf("δZ:               %v\n", δZset)
			emsg += fmt.Sprintf("Tracked^δZ:       %v\n", TrackedδZ)
			emsg += fmt.Sprintf("kadj[Tracked^δZ]: %v\n", kadjTrackedδZ)
			emsg += fmt.Sprintf("kadj: %v\n\n", kadj)
			emsg += strings.Join(badv, "\n")
			emsg += "\n"

			t.Fatal(emsg)
		}
	}()


	// δbtail @at1 with initial tracked set
	δbtail := NewΔBtail(t1.At, db)
	trackKeys(δbtail, t1, initialTrackedKeys)

	// TrackedδZ = Tracked ^ δZ  (i.e. a tracked node has changed, or its coverage was changed)
	TrackedδZ = setKey{}
	for k := range initialTrackedKeys {
		leaf1 := t1.Xkv.Get(k)
		oid1 := leaf1.Oid
		if oid1 == zodb.InvalidOid { // embedded bucket
			oid1 = leaf1.Parent.Oid
		}
		leaf2 := t2.Xkv.Get(k)
		oid2 := leaf2.Oid
		if oid2 == zodb.InvalidOid { // embedded bucket
			oid2 = leaf2.Parent.Oid
		}
		if δZset.Has(oid1) || δZset.Has(oid2) || (leaf1.Keycov != leaf2.Keycov) {
			TrackedδZ.Add(k)
		}
	}

	kadjTrackedδZ = setKey{} // kadj[Tracked^δZ]  (all keys adjacent to tracked^δZ)
	for k := range TrackedδZ {
		kadjTrackedδZ.Update(kadj[k])
	}


	// assert TrackedδZ ∈ kadj[TrackedδZ]
	trackNotInKadj := TrackedδZ.Difference(kadjTrackedδZ)
	if len(trackNotInKadj) > 0 {
		badf("BUG: Tracked^δZ ∉ kadj[Tracked^δZ]  ; extra=%v", trackNotInKadj)
		return
	}

	//              k ∈ d12
	// k ∈ δT  <=>
	//              k ∈   U kadj[·]
	//                   ·∈tracking^δZ
	δTok = map[Key]Δstring{} // d12[all keys that should be present in δT]
	for k,δv := range d12 {
		if kadjTrackedδZ.Has(k) {
			δTok[k] = δv
		}
	}

	ø  := blib.PPTreeSubSet{}
	kø := &blib.RangedKeySet{}

	// trackSet1 = xkv1[tracked1]
	// trackSet2 = xkv2[tracked2]  ( = xkv2[kadj[tracked1]]
	trackSet1, tkeyCov1 := trackSetWithCov(t1.Xkv, initialTrackedKeys)
	trackSet2, tkeyCov2 := trackSetWithCov(t2.Xkv, initialTrackedKeys.Union(kadjTrackedδZ))

	// verify δbtail.trackSet against @at1
	δbtail.assertTrack(t, "1", ø, trackSet1, tkeyCov1)

	// δB <- δZ
	//
	// also call _Update1 directly to verify δtkeycov return from treediff
	// the result of Update and _Update1 should be the same since δbtail is initially empty.
	δbtail_ := δbtail.Clone()
	δB1, err := δbtail_._Update1(δZ);  X(err)

	// assert tkeyCov1 ⊂ tkeyCov2
	dkeycov12 := tkeyCov1.Difference(tkeyCov2)
	if !dkeycov12.Empty() {
		t.Errorf("BUG: tkeyCov1 ⊄ tkeyCov2:\n\ttkeyCov1: %s\n\ttkeyCov2: %s\n\ttkeyCov1 \\ tkeyCov2: %s", tkeyCov1, tkeyCov2, dkeycov12)
	}

	// assert δtkeycov == δ(tkeyCov1, tkeyCov2)
	δtkeycovOK := tkeyCov2.Difference(tkeyCov1)
	δtkeycov := &blib.RangedKeySet{}
	if __, ok := δB1.ByRoot[treeRoot]; ok {
		δtkeycov = __.δtkeycov1
	}
	if !δtkeycov.Equal(δtkeycovOK) {
		badf("δtkeycov wrong:\nhave: %s\nwant: %s", δtkeycov, δtkeycovOK)
	}

	δB, err  := δbtail.Update(δZ);     X(err)

	if δB.Rev != δZ.Tid {
		badf("δB: rev != δZ.Tid  ; rev=%s  δZ.Tid=%s", δB.Rev, δZ.Tid)
		return
	}

	// assert δBtail[root].vδT = δBtail_[root].vδT
	var vδT, vδT_ []ΔTree
	if δttail, ok := δbtail.byRoot[treeRoot]; ok {
		vδT = δttail.vδT
	}
	if δttail_, ok := δbtail_.byRoot[treeRoot]; ok {
		vδT_ = δttail_.vδT
	}
	if !reflect.DeepEqual(vδT, vδT_) {
		badf("δBtail.vδT differs after Update and _Update1:\n_Update1: %v\n  Update: %v", vδT_, vδT)
	}

	// verify δbtail.trackSet  against @at2
	δbtail.assertTrack(t, "2", trackSet2, ø, kø)


	// assert δB.ByRoot == {treeRoot -> ...}  if δTok != ø
	//                  == ø                  if δTok == ø
	rootsOK := setOid{}
	if len(δTok) > 0 {
		rootsOK.Add(treeRoot)
	}
	roots := setOid{}
	for root := range δB.ByRoot {
		roots.Add(root)
	}
	if !reflect.DeepEqual(roots, rootsOK) {
		badf("δB: roots != rootsOK  ; roots=%v  rootsOK=%v", roots, rootsOK)
	}
	_, inδB := δB.ByRoot[treeRoot]
	if !inδB {
		return
	}


	// δT <- δB
	δToid := δB.ByRoot[treeRoot]   // {} k -> δoid
	δT     = xgetδKV(t1,t2, δToid) // {} k -> δ(ZBlk(oid).data)

	// δT must be subset of d12.
	// changed keys, that are
	// - in tracked set	 -> must be present in δT
	// - outside tracked set -> may  be present in δT  (kadj gives exact answer)

	// δT is subset of d12
	for _, k := range sortedKeys(δT) {
		_, ind12 := d12[k]
		if !ind12 {
			badf("δT[%v] ∉  d12", k)
		}
	}

	// k ∈ tracked set ->  must be present in δT
	// k ∉ tracked set ->  may  be present in δT  (kadj gives exact answer)
	for _, k := range sortedKeys(d12) {
		_, inδT   := δT[k]
		_, inδTok := δTok[k]
		if inδT && !inδTok {
			badf("δT[%v] ∉  δTok", k)
		}

		if !inδT && inδTok {
			badf("δT    ∌  δTok[%v]", k)
		}

		if inδT {
			if δT[k] != d12[k] {
				badf("δT[%v] ≠  δTok[%v]", k, k)
			}
		}
	}
}

// xverifyΔBTail_rebuild verifies ΔBtail.rebuild during t0->t1->t2 transition.
//
// t0->t1 exercises from-scratch rebuild,
// t1->t2 further exercises incremental rebuild.
//
// It also exercises rebuild phase of ΔBtail.Update.
func xverifyΔBTail_rebuild(t *testing.T, db *zodb.DB, treeRoot zodb.Oid, t0, t1, t2 *xbtreetest.Commit) {
	t.Run(fmt.Sprintf("rebuild/%s→%s", t0.Tree, t1.Tree), func(t *testing.T) {
		tAllKeys := allTestKeys(t0, t1, t2)
		tAllKeyv := tAllKeys.SortedElements()

		//fmt.Printf("@%s:  %v\n", t0.AtSymb(), t0.Xkv.Flatten())
		//fmt.Printf("@%s:  %v\n", t1.AtSymb(), t1.Xkv.Flatten())
		//fmt.Printf("@%s:  %v\n", t2.AtSymb(), t2.Xkv.Flatten())

		kadj10 := KAdj(t1,t0, allTestKeys(t0,t1,t2))
		kadj21 := KAdj(t2,t1, allTestKeys(t0,t1,t2))
		kadj12 := KAdj(t1,t2, allTestKeys(t0,t1,t2))

		// kadj210 = kadj10·kadj21
		kadj210 := kadj10.Mul(kadj21)

		ø  := blib.PPTreeSubSet{}
		kø := &blib.RangedKeySet{}

		// verify t0 -> t1 Track(keys1) Rebuild -> t2 Track(keys2) Rebuild
		// for all combinations of keys1 and keys2
		for k1idx := range IntSets(len(tAllKeyv)) {
			keys1 := setKey{}
			for _, idx1 := range k1idx {
				keys1.Add(tAllKeyv[idx1])
			}

			// δkv1_1 = t1.δxkv / kadj10(keys1)
			keys1_0 := kadj10.Map(keys1)
			δkv1_1 := map[Key]Δstring{}
			for k := range keys1_0 {
				δv, ok := t1.Δxkv[k]
				if ok {
					δkv1_1[k] = δv
				}
			}

			Tkeys1, kTkeys1  := trackSetWithCov(t1.Xkv, keys1)
			Tkeys1_0         := trackSet(t1.Xkv, keys1_0)

			t.Run(fmt.Sprintf(" T%s;R", keys1), func(t *testing.T) {
				δbtail := NewΔBtail(t0.At, db)

				// assert trackSet=ø, trackNew=ø, vδB=[]
				δbtail.assertTrack(t, "@at0", ø, ø, kø)
				assertΔTtail(t, "@at0", δbtail, t0, treeRoot,
					/*vδT=ø*/)

				xverifyΔBTail_rebuild_U(t, δbtail, treeRoot, t0, t1,
					/*trackSet=*/ø,
					/*vδT=ø*/)
				xverifyΔBTail_rebuild_TR(t, δbtail, t1, treeRoot,
					// after Track(keys1)
					keys1,
					/*trackSet=*/  ø,
					/*trackNew=*/  Tkeys1,
					/*ktrackNew=*/ kTkeys1,

					// after rebuild
					/*trackSet=*/ Tkeys1_0,
					/*vδT=*/ δkv1_1)

				t.Run((" →" + t2.Tree), func(t *testing.T) {
					// keys1R2 is full set of keys that should become tracked after
					// Update()  (which includes rebuild)
					keys1R2 := kadj12.Map(keys1)
					for {
						keys1R2_ := kadj210.Map(keys1R2)
						if keys1R2.Equal(keys1R2_) {
							break
						}
						keys1R2 = keys1R2_
					}

					// δkvX_k1R2 = tX.δxkv / keys1R2
					δkv1_k1R2 := map[Key]Δstring{}
					δkv2_k1R2 := map[Key]Δstring{}
					for k := range keys1R2 {
						δv1, ok := t1.Δxkv[k]
						if ok {
							δkv1_k1R2[k] = δv1
						}
						δv2, ok := t2.Δxkv[k]
						if ok {
							δkv2_k1R2[k] = δv2
						}
					}

					Tkeys1R2, kTkeys1R2 := trackSetWithCov(t2.Xkv, keys1R2)

					xverifyΔBTail_rebuild_U(t, δbtail, treeRoot, t1, t2,
						/*trackSet=*/ Tkeys1R2,
						/*vδT=*/ δkv1_k1R2, δkv2_k1R2)

					// tRestKeys2      = tAllKeys - keys1
					// reduce that to  = tAllKeys - keys1R2   in short/medium mode
					// ( if key from keys2 already became tracked after Track(keys1) + Update,
					//   adding Track(that-key), is not adding much testing coverage to recompute paths )
					var tRestKeys2 setKey
					if !xbtreetest.VeryLong() {
						tRestKeys2 = tAllKeys.Difference(keys1R2)
					} else {
						tRestKeys2 = tAllKeys.Difference(keys1)
					}

					tRestKeyv2 := tRestKeys2.SortedElements()
					for k2idx := range IntSets(len(tRestKeyv2)) {
						keys2 := setKey{}
						for _, idx2 := range k2idx {
							keys2.Add(tRestKeyv2[idx2])
						}

						// keys12R2 is full set of keys that should become tracked after
						// Track(keys2) + rebuild
						keys12R2 := keys1R2.Union(keys2)
						for {
							keys12R2_ := kadj210.Map(keys12R2)
							if keys12R2.Equal(keys12R2_) {
								break
							}
							keys12R2 = keys12R2_
						}

						Tkeys2, kTkeys2  := trackSetWithCov(t2.Xkv, keys2)
						Tkeys12R2        := trackSet(t2.Xkv, keys12R2)
/*
						fmt.Printf("\n\n\nKKK\nkeys1=%s  keys2=%s\n", keys1, keys2)
						fmt.Printf("keys1R2:  %s\n", keys1R2)
						fmt.Printf("keys12R2: %s\n", keys12R2)

						fmt.Printf("t0.Xkv: %v\n", t0.Xkv)
						fmt.Printf("t1.Xkv: %v\n", t1.Xkv)
						fmt.Printf("t2.Xkv: %v\n", t2.Xkv)
						fmt.Printf("kadj21: %v\n", kadj21)
						fmt.Printf("kadj12: %v\n", kadj12)
						fmt.Printf("Tkeys2   -> %s\n", Tkeys2)
						fmt.Printf("Tkeys1R2 -> %s\n", Tkeys1R2)
						fmt.Printf("Tkeys2 \\ Tkeys1R2  -> %s\n", Tkeys2.Difference(Tkeys1R2))
						fmt.Printf("\n\n\n")
*/


						// δkvX_k12R2 = tX.δxkv / keys12R2
						δkv1_k12R2 := make(map[Key]Δstring, len(t1.Δxkv))
						δkv2_k12R2 := make(map[Key]Δstring, len(t2.Δxkv))
						for k := range keys12R2 {
							δv1, ok := t1.Δxkv[k]
							if ok {
								δkv1_k12R2[k] = δv1
							}
							δv2, ok := t2.Δxkv[k]
							if ok {
								δkv2_k12R2[k] = δv2
							}
						}

						// t.Run is expensive at this level of nest
						//t.Run(" T"+keys2.String()+";R", func(t *testing.T) {
							δbtail_ := δbtail.Clone()
							xverifyΔBTail_rebuild_TR(t, δbtail_, t2, treeRoot,
								// after Track(keys2)
								keys2,
								/*trackSet*/ Tkeys1R2,
								/*trackNew*/ Tkeys2.Difference(
									// trackNew should not cover ranges that are
									// already in trackSet
									Tkeys1R2),
								/*ktrackNew*/ kTkeys2.Difference(
									// see ^^^ about trackNew
									kTkeys1R2),

								// after rebuild
								/* trackSet=*/ Tkeys12R2,
								/*vδT=*/ δkv1_k12R2, δkv2_k12R2)
						//})
					}
				})
			})
		}
	})
}

// xverifyΔBTail_rebuild_U verifies ΔBtail state after Update(ti->tj).
func xverifyΔBTail_rebuild_U(t *testing.T, δbtail *ΔBtail, treeRoot zodb.Oid, ti, tj *xbtreetest.Commit, trackSet blib.PPTreeSubSet, vδTok ...map[Key]Δstring) {
	t.Helper()
	X := exc.Raiseif
	ø := blib.PPTreeSubSet{}
	kø := &blib.RangedKeySet{}

	subj := fmt.Sprintf("after Update(@%s→@%s)", ti.AtSymb(), tj.AtSymb())

	// Update ati -> atj
	δB, err := δbtail.Update(tj.ΔZ);  X(err)
	δbtail.assertTrack(t, subj, trackSet, ø, kø)
	assertΔTtail(t, subj, δbtail, tj, treeRoot, vδTok...)

	// assert δB = vδTok[-1]
	var δT, δTok map[Key]Δstring
	if l := len(vδTok); l > 0 {
		δTok = vδTok[l-1]
	}
	if len(δTok) == 0 {
		δTok = nil
	}
	δrootsOK := 1
	if δTok == nil {
		δrootsOK = 0
	}

	δroots := setOid{}
	for root := range δbtail.byRoot {
		δroots.Add(root)
	}
	δToid, ok := δB.ByRoot[treeRoot]
	if ok {
		δT = xgetδKV(ti, tj, δToid)
	}
	if δB.Rev != tj.At {
		t.Errorf("%s: δB.Rev: have %s  ; want %s", subj, δB.Rev, tj.At)
	}
	if len(δB.ByRoot) != δrootsOK {
		t.Errorf("%s: len(δB.ByRoot) != %d  ; δroots=%v", subj, δrootsOK, δroots)
	}
	if !δTEqual(δT, δTok) {
		t.Errorf("%s: δB.ΔBByRoot[%s]:\nhave: %v\nwant: %v", subj, treeRoot, δT, δTok)
	}
}

// xverifyΔBTail_rebuild_TR verifies ΔBtail state after Track(keys) + rebuild.
func xverifyΔBTail_rebuild_TR(t *testing.T, δbtail *ΔBtail, tj *xbtreetest.Commit, treeRoot zodb.Oid, keys setKey, trackSet, trackNew blib.PPTreeSubSet, ktrackNew *blib.RangedKeySet, trackSetAfterRebuild blib.PPTreeSubSet, vδTok ...map[Key]Δstring) {
	t.Helper()
	ø := blib.PPTreeSubSet{}
	kø := &blib.RangedKeySet{}

	// Track(keys)
	trackKeys(δbtail, tj, keys)

	subj := fmt.Sprintf("@%s: after Track%v", tj.AtSymb(), keys)
	δbtail.assertTrack(t, subj, trackSet, trackNew, ktrackNew)

	δbtail._rebuildAll()

	subj += " + rebuild"
	δbtail.assertTrack(t, subj, trackSetAfterRebuild, ø, kø)

	// verify δbtail.byRoot[treeRoot]
	assertΔTtail(t, subj, δbtail, tj, treeRoot, vδTok...)
}


// ----------------------------------------

func TestΔBtailForget(t_ *testing.T) {
	t := xbtreetest.NewT(t_)
	X := exc.Raiseif

	t0 := t.Commit("T/B:")
	t1 := t.Commit("T/B1:a")
	t2 := t.Commit("T2/B1:a-B2:b")
	t3 := t.Commit("T/B2:b")

	δbtail := NewΔBtail(t0.At, t.DB)
	_, err := δbtail.Update(t1.ΔZ);  X(err)
	_, err  = δbtail.Update(t2.ΔZ);  X(err)

	// start tracking. everything becomes tracked because t1's T/B1:a has [-∞,∞) coverage
	// By starting tracking after t2 we verify vδBroots update in both Update and rebuild
	_0 := setKey{}; _0.Add(0)
	trackKeys(δbtail, t2, _0)

	_, err  = δbtail.Update(t3.ΔZ);  X(err)

	// forget calls ForgetPast(revCut) and returns state of vδT right after that.
	forget := func(revCut zodb.Tid) []ΔTree {
		δbtail.ForgetPast(revCut)
		return δbtail.byRoot[t.Root()].vδT
	}

	vδT_init := δbtail.byRoot[t.Root()].vδT
	assertΔTtail(t.T, "init",         δbtail, t3, t.Root(), t1.Δxkv, t2.Δxkv, t3.Δxkv)
	vδT_at0 := forget(t0.At)
	assertΔTtail(t.T, "forget ≤ at0", δbtail, t3, t.Root(), t1.Δxkv, t2.Δxkv, t3.Δxkv)
	vδT_at1 := forget(t1.At)
	assertΔTtail(t.T, "forget ≤ at1", δbtail, t3, t.Root(),          t2.Δxkv, t3.Δxkv)
	_ = forget(t3.At)
	assertΔTtail(t.T, "forget ≤ at3", δbtail, t3, t.Root(),                          )

	// verify aliasing: init/at0 should be aliased, because there is no change @at0
	// at1 must be unaliased from at0; at3 must be unualiased from at1.

	vδT_at0[2].Rev = 0
	if vδT_init[2].Rev != 0 {
		t.Errorf("ForgetPast(at0) cloned vδT but should not")
	}
	if vδT_at1[1].Rev == 0 {
		t.Errorf("ForgetPast(at1) remained vδT aliased but should not")
	}

}

func TestΔBtailGetAt(t_ *testing.T) {
	// GetAt is thin wrapper around data in ΔTtail.vδT and (TODO) lastRevOf index.
	// Recomputing ΔTtail.vδT itself (and TODO lastRevOf) is exercised in depth
	// by xverifyΔBTail_rebuild. Here we verify only properties of the wrapper.
	t := xbtreetest.NewT(t_)
	X := exc.Raiseif
	const ø = "ø"

	t.Commit("T/B:")
	t1 := t.Commit("T/B2:b,3:c")	; at1 := t1.At	// 2:b 3:c
	t2 := t.Commit("T/B2:b,3:c,4:d")	; at2 := t2.At	// 4:d
	t3 := t.Commit("T/B2:b,3:e,4:d")	; at3 := t3.At	// 3:e
	t4 := t.Commit("T/B2:b,3:e,4:f")	; at4 := t4.At	// 4:f
	t5 := t.Commit("T/B2:b,3:g,4:f")	; at5 := t5.At	// 3:g

	δBtail := NewΔBtail(t1.At, t.DB)
	_, err := δBtail.Update(t2.ΔZ);  X(err)
	_, err  = δBtail.Update(t3.ΔZ);  X(err)
	_, err  = δBtail.Update(t4.ΔZ);  X(err)
	_, err  = δBtail.Update(t5.ΔZ);  X(err)

	// track everything
	_1234 := setKey{}; _1234.Add(1); _1234.Add(2); _1234.Add(3); _1234.Add(4)
	trackKeys(δBtail, t5, _1234)

	// assertGetAt asserts that GetAt returns expected result.
	assertGetAt := func(at zodb.Tid, key Key, valueOK string, revOK zodb.Tid, valueExactOK, revExactOK bool) {
		t.Helper()
		valueOid, rev, valueExact, revExact, err := δBtail.GetAt(t.Root(), key, at);  X(err)
		value := ø
		if valueOid != VDEL {
			value = t.XGetCommit(rev).XGetBlkData(valueOid)
		}

		if !(value == valueOK && rev == revOK && valueExact == valueExactOK && revExact == revExactOK) {
			t.Errorf("GetAt(%v@%s):\nhave: %s @%s %t %t\nwant: %s @%s %t %t",
				key, t.AtSymb(at),
				value, t.AtSymb(rev), valueExact, revExact,
				valueOK, t.AtSymb(revOK), valueExactOK, revExactOK,
			)
		}
	}

	//          @at  key  value rev  valueExact  revExact
	assertGetAt(at2, 1,   ø,    at1, false,      false)
	assertGetAt(at2, 2,   ø,    at1, false,      false)
	assertGetAt(at2, 3,   "c",  at1, true,       false)
	assertGetAt(at2, 4,   "d",  at2, true,       true)

	assertGetAt(at3, 1,   ø,    at1, false,      false)
	assertGetAt(at3, 2,   ø,    at1, false,      false)
	assertGetAt(at3, 3,   "e",  at3, true,       true)
	assertGetAt(at3, 4,   "d",  at2, true,       true)

	assertGetAt(at4, 1,   ø,    at1, false,      false)
	assertGetAt(at4, 2,   ø,    at1, false,      false)
	assertGetAt(at4, 3,   "e",  at3, true,       true)
	assertGetAt(at4, 4,   "f",  at4, true,       true)

	assertGetAt(at5, 1,   ø,    at1, false,      false)
	assertGetAt(at5, 2,   ø,    at1, false,      false)
	assertGetAt(at5, 3,   "g",  at5, true,       true)
	assertGetAt(at5, 4,   "f",  at4, true,       true)
}

func TestΔBtailSliceByRootRev(t_ *testing.T) {
	// SliceByRootRev is thin wrapper to return ΔTtail.vδT slice.
	// Recomputing ΔTtail.vδT itself is exercised in depth by xverifyΔBTail_rebuild.
	// Here we verify only properties of the wrapper.
	t := xbtreetest.NewT(t_)
	X := exc.Raiseif

	// ΔT is similar to ΔTree but uses Δstring instead of ΔValue for KV
	type ΔT struct {
		Rev zodb.Tid
		KV  map[Key]Δstring
	}
	// δ is shorthand for ΔT.KV
	type δ = map[Key]Δstring

	t0 := t.Commit("T2/B1:a-B2:f")
	t1 := t.Commit("T2/B1:b-B2:g")
	t2 := t.Commit("T2/B1:c-B2:h")

	const a, b, c    = "a", "b", "c"
	const f, g, h    = "f", "g", "h"

	δbtail := NewΔBtail(t0.At, t.DB)
	_, err := δbtail.Update(t1.ΔZ);  X(err)
	_, err  = δbtail.Update(t2.ΔZ);  X(err)

	// track 2 + rebuild.
	_2 := setKey{}; _2.Add(2)
	trackKeys(δbtail, t2, _2)
	err = δbtail._rebuildAll();  X(err)

	δttail := δbtail.byRoot[t.Root()]

	// assertvδT asserts that vδT matches vδTok
	assertvδT := func(subj string, vδT []ΔTree, vδTok ...ΔT) {
		t.Helper()
		// convert vδT from ΔTree to ΔT
		var vδT_ []ΔT
		for _, δT := range vδT {
			tj := t.XGetCommit(δT.Rev)
			δt := ΔT{δT.Rev, xgetδKV(tj.Prev, tj, δT.KV)}
			vδT_ = append(vδT_, δt)
		}

		if reflect.DeepEqual(vδT_, vδTok) {
			return
		}
		have := []string{}
		for _, δT := range vδT_ {
			have = append(have, fmt.Sprintf("@%s·%v", t.AtSymb(δT.Rev), δT.KV))
		}
		want := []string{}
		for _, δT := range vδTok {
			want = append(want, fmt.Sprintf("@%s·%v", t.AtSymb(δT.Rev), δT.KV))
		}
		t.Errorf("%s:\nhave: %s\nwant: %s", subj, have, want)
	}

	s00, err := δbtail.SliceByRootRev(t.Root(), t0.At, t0.At);  X(err)
	s01, err := δbtail.SliceByRootRev(t.Root(), t0.At, t1.At);  X(err)
	s02, err := δbtail.SliceByRootRev(t.Root(), t0.At, t2.At);  X(err)
	s12, err := δbtail.SliceByRootRev(t.Root(), t1.At, t2.At);  X(err)
	s22, err := δbtail.SliceByRootRev(t.Root(), t2.At, t2.At);  X(err)

	vδT := δttail.vδT
	assertvδT("t2.vδT",  vδT,        ΔT{t1.At, δ{2:{f,g}}},   ΔT{t2.At, δ{2:{g,h}}})
	assertvδT("t2.s00",  s00)
	assertvδT("t2.s01",  s01,        ΔT{t1.At, δ{2:{f,g}}})
	assertvδT("t2.s02",  s02,        ΔT{t1.At, δ{2:{f,g}}},   ΔT{t2.At, δ{2:{g,h}}})
	assertvδT("t2.s12",  s12,                                 ΔT{t2.At, δ{2:{g,h}}})
	assertvδT("t2.s22",  s22)

	// sXX should be all aliased to vδT
	gg, _ := t0.XGetBlkByName("g")
	hh, _ := t0.XGetBlkByName("h")
	vδT[0].Rev = t0.At; δkv0 := vδT[0].KV; vδT[0].KV = map[Key]ΔValue{11:{gg,gg}}
	vδT[1].Rev = t0.At; δkv1 := vδT[1].KV; vδT[1].KV = map[Key]ΔValue{12:{hh,hh}}
	assertvδT("t2.vδT*", vδT,        ΔT{t0.At, δ{11:{g,g}}},  ΔT{t0.At, δ{12:{h,h}}})
	assertvδT("t2.s00*", s00)
	assertvδT("t2.s01*", s01,        ΔT{t0.At, δ{11:{g,g}}})
	assertvδT("t2.s02*", s02,        ΔT{t0.At, δ{11:{g,g}}},  ΔT{t0.At, δ{12:{h,h}}})
	assertvδT("t2.s12*", s12,                                 ΔT{t0.At, δ{12:{h,h}}})
	assertvδT("2.s22*",  s22)
	vδT[0].Rev = t1.At; vδT[0].KV = δkv0
	vδT[1].Rev = t2.At; vδT[1].KV = δkv1
	assertvδT("t2.vδT+", vδT,        ΔT{t1.At, δ{2:{f,g}}},   ΔT{t2.At, δ{2:{g,h}}})
	assertvδT("t2.s00+", s00)
	assertvδT("t2.s01+", s01,        ΔT{t1.At, δ{2:{f,g}}})
	assertvδT("t2.s02+", s02,        ΔT{t1.At, δ{2:{f,g}}},   ΔT{t2.At, δ{2:{g,h}}})
	assertvδT("t2.s12+", s12,                                 ΔT{t2.At, δ{2:{g,h}}})
	assertvδT("t2.s22+", s22)


	// after track 1 + rebuild old slices remain unchanged, but new queries return updated data
	_1 := setKey{}; _1.Add(1)
	trackKeys(δbtail, t2, _1)
	err = δbtail._rebuildAll();  X(err)

	s00_, err := δbtail.SliceByRootRev(t.Root(), t0.At, t0.At);  X(err)
	s01_, err := δbtail.SliceByRootRev(t.Root(), t0.At, t1.At);  X(err)
	s02_, err := δbtail.SliceByRootRev(t.Root(), t0.At, t2.At);  X(err)
	s12_, err := δbtail.SliceByRootRev(t.Root(), t1.At, t2.At);  X(err)
	s22_, err := δbtail.SliceByRootRev(t.Root(), t2.At, t2.At);  X(err)

	vδT = δttail.vδT
	assertvδT("t12.vδT",   vδT,     ΔT{t1.At, δ{1:{a,b},2:{f,g}}},  ΔT{t2.At, δ{1:{b,c},2:{g,h}}})
	assertvδT("t12.s00",   s00)
	assertvδT("t12.s00_",  s00_)
	assertvδT("t12.s01",   s01,     ΔT{t1.At, δ{        2:{f,g}}})
	assertvδT("t12.s01_",  s01_,    ΔT{t1.At, δ{1:{a,b},2:{f,g}}})
	assertvδT("t12.s02",   s02,     ΔT{t1.At, δ{        2:{f,g}}},  ΔT{t2.At, δ{        2:{g,h}}})
	assertvδT("t12.s02_",  s02_,    ΔT{t1.At, δ{1:{a,b},2:{f,g}}},  ΔT{t2.At, δ{1:{b,c},2:{g,h}}})
	assertvδT("t12.s12",   s12,                                     ΔT{t2.At, δ{        2:{g,h}}})
	assertvδT("t12.s12_",  s12_,                                    ΔT{t2.At, δ{1:{b,c},2:{g,h}}})
	assertvδT("t12.s22",   s22)
	assertvδT("t12.s22_",  s22_)

	// sXX_ should be all aliased to vδT, but not sXX
	bb, _ := t0.XGetBlkByName("b")
	cc, _ := t0.XGetBlkByName("c")
	vδT[0].Rev = t0.At; δkv0 = vδT[0].KV; vδT[0].KV = map[Key]ΔValue{111:{bb,bb}}
	vδT[1].Rev = t0.At; δkv1 = vδT[1].KV; vδT[1].KV = map[Key]ΔValue{112:{cc,cc}}
	assertvδT("t12.vδT*",  vδT,     ΔT{t0.At, δ{111:{b,b}}},        ΔT{t0.At, δ{112:{c,c}}})
	assertvδT("t12.s00*",  s00)
	assertvδT("t12.s00_*", s00_)
	assertvδT("t12.s01*",  s01,     ΔT{t1.At, δ{        2:{f,g}}})
	assertvδT("t12.s01_*", s01_,    ΔT{t0.At, δ{111:{b,b}      }})
	assertvδT("t12.s02*",  s02,     ΔT{t1.At, δ{        2:{f,g}}},  ΔT{t2.At, δ{        2:{g,h}}})
	assertvδT("t12.s02_*", s02_,    ΔT{t0.At, δ{111:{b,b}      }},  ΔT{t0.At, δ{112:{c,c}      }})
	assertvδT("t12.s12*",  s12,                                     ΔT{t2.At, δ{        2:{g,h}}})
	assertvδT("t12.s12_*", s12_,                                    ΔT{t0.At, δ{112:{c,c}      }})
	assertvδT("t12.s22*",  s22)
	assertvδT("t12.s22_*", s22_)

	vδT[0].Rev = t1.At; vδT[0].KV = δkv0
	vδT[1].Rev = t2.At; vδT[1].KV = δkv1
	assertvδT("t12.vδT+",  vδT,     ΔT{t1.At, δ{1:{a,b},2:{f,g}}},  ΔT{t2.At,  δ{1:{b,c},2:{g,h}}})
	assertvδT("t12.s00+",  s00)
	assertvδT("t12.s00_+", s00_)
	assertvδT("t12.s01+",  s01,     ΔT{t1.At, δ{        2:{f,g}}})
	assertvδT("t12.s01_+", s01_,    ΔT{t1.At, δ{1:{a,b},2:{f,g}}})
	assertvδT("t12.s02+",  s02,     ΔT{t1.At, δ{        2:{f,g}}},  ΔT{t2.At, δ{        2:{g,h}}})
	assertvδT("t12.s02_+", s02_,    ΔT{t1.At, δ{1:{a,b},2:{f,g}}},  ΔT{t2.At, δ{1:{b,c},2:{g,h}}})
	assertvδT("t12.s12+",  s12,                                     ΔT{t2.At, δ{        2:{g,h}}})
	assertvδT("t12.s12_+", s12_,                                    ΔT{t2.At, δ{1:{b,c},2:{g,h}}})
	assertvδT("t12.s22+",  s22)
	assertvδT("t12.s22_+", s22_)
}


func TestΔBtailClone(t_ *testing.T) {
	// ΔBtail.Clone had bug that aliased klon data to orig
	t := xbtreetest.NewT(t_)
	X := exc.Raiseif

	t0 := t.Commit("T2/B1:a-B2:b")
	t1 := t.Commit("T2/B1:c-B2:d")
	δbtail := NewΔBtail(t0.At, t.DB)
	_, err := δbtail.Update(t1.ΔZ); X(err)
	_2 := setKey{}; _2.Add(2)
	trackKeys(δbtail, t1, _2)
	err = δbtail._rebuildAll(); X(err)

	δkv1_1 := map[Key]Δstring{2:{"b","d"}}
	assertΔTtail(t.T, "orig @at1", δbtail, t1, t.Root(), δkv1_1)
	δbklon := δbtail.Clone()
	assertΔTtail(t.T, "klon @at1", δbklon, t1, t.Root(), δkv1_1)

	t2 := t.Commit("T/B1:b,2:a")
	_, err = δbtail.Update(t2.ΔZ); X(err)

	δkv1_2 := map[Key]Δstring{1:{"a","c"}, 2:{"b","d"}}
	δkv2_2 := map[Key]Δstring{1:{"c","b"}, 2:{"d","a"}}
	assertΔTtail(t.T, "orig @at2", δbtail, t2, t.Root(), δkv1_2, δkv2_2)
	assertΔTtail(t.T, "klon @at1 after orig @at->@at2", δbklon, t1, t.Root(), δkv1_1)
}

// -------- vδTMerge --------

func TestVδTMerge(t *testing.T) {
	vδTMerge1 := func(vδT []ΔTree, δT ΔTree) (_ []ΔTree, newRevEntry bool) {
		vδT = vδTClone(vδT)
		newRevEntry = vδTMerge1Inplace(&vδT, δT)
		return vδT, newRevEntry
	}

	vδTMerge := func(vδT, vδTnew []ΔTree) (_ []ΔTree, δrevSet setTid) {
		vδT = vδTClone(vδT)
		δrevSet = vδTMergeInplace(&vδT, vδTnew)
		return vδT, δrevSet
	}

	assertMerge1 := func(vδT []ΔTree, δT ΔTree, newRevEntryOK bool, mergeOK []ΔTree) {
		t.Helper()
		merge, newRevEntry := vδTMerge1(vδT, δT)
		if !(reflect.DeepEqual(merge, mergeOK) && newRevEntry == newRevEntryOK) {
			t.Errorf("merge1 %v  +  %v:\nhave: %v  %t\nwant: %v  %t",
				 vδT, δT, merge, newRevEntry, mergeOK, newRevEntryOK)
		}
	}

	assertMerge := func(vδT, vδTnew []ΔTree, δrevSetOK setTid, mergeOK []ΔTree) {
		t.Helper()
		merge, δrevSet := vδTMerge(vδT, vδTnew)
		if !(reflect.DeepEqual(merge, mergeOK) && δrevSet.Equal(δrevSetOK)) {
			t.Errorf("merge  %v  +  %v:\nhave: %v  %v\nwant: %v  %v",
				 vδT, vδTnew, merge, δrevSet, mergeOK, δrevSetOK)
		}
	}


	// syntax sugar
	type Δ = ΔTree
	type δ = map[Key]ΔValue
	v := func(vδT ...Δ) []Δ {
		return vδT
	}
	r := func(tidv ...zodb.Tid) setTid {
		s := setTid{}
		for _, tid := range tidv {
			s.Add(tid)
		}
		return s
	}

	δ1 := δ{1:{1,2}}
	δ2 := δ{2:{2,3}};  δ22 := δ{22:{0,1}};  δ2_22 := δ{2:{2,3}, 22:{0,1}}
	δ3 := δ{3:{3,4}}
	δ4 := δ{4:{4,5}};  δ44 := δ{44:{0,1}};  δ4_44 := δ{4:{4,5}, 44:{0,1}}
	δ5 := δ{5:{5,6}}
	δ12 := δ{1:{1,2}, 2:{2,3}}
	Δ101 := Δ{0x101, δ1}
	Δ102 := Δ{0x102, δ2}
	Δ103 := Δ{0x103, δ3}
	Δ104 := Δ{0x104, δ4}
	Δ105 := Δ{0x105, δ5}

	// merge1
	assertMerge1(nil,			Δ{0x100, nil}, false,
		     nil)

	assertMerge1(v(Δ{0x100, δ1}),		Δ{0x100, δ2}, false,
		     v(Δ{0x100, δ12}))

	assertMerge1(v(Δ101, Δ103),		Δ102, true,
		     v(Δ101, Δ102, Δ103))

	assertMerge1(v(Δ101, Δ102),		Δ103, true,
		     v(Δ101, Δ102, Δ103))

	assertMerge1(v(Δ102, Δ103),		Δ101, true,
		     v(Δ101, Δ102, Δ103))

	// merge
	assertMerge(nil,			nil, r(),
		    nil)

	assertMerge(nil,			v(Δ101, Δ103),  r(0x101, 0x103),
		    v(Δ101, Δ103))

	assertMerge(v(Δ101, Δ103),		nil, r(),
		    v(Δ101, Δ103))

	assertMerge(v(Δ102, Δ104),		v(Δ101, Δ{0x102, δ22}, Δ103, Δ{0x104, δ44}, Δ105),
						r(0x101, 0x103, 0x105),
		    v(Δ101, Δ{0x102, δ2_22}, Δ103, Δ{0x104, δ4_44}, Δ105))
}

// -------- KAdj --------

// Map returns kadj·keys.
func (kadj KAdjMatrix) Map(keys setKey) setKey {
	res := make(setKey, len(keys))
	for k := range keys {
		to, ok := kadj[k]
		if !ok {
			panicf("kadj.Map: %d ∉ kadj\n\nkadj: %v", k, kadj)
		}
		res.Update(to)
	}
	return res
}

// Mul returns kadjA·kadjB.
//
// (kadjA·kadjB).Map(keys) = kadjA.Map(kadjB.Map(keys))
func (kadjA KAdjMatrix) Mul(kadjB KAdjMatrix) KAdjMatrix {
	// ~ assert kadjA.keys == kadjB.keys
	// check only len here; the rest will be asserted by Map
	if len(kadjA) != len(kadjB) {
		panicf("kadj.Mul: different keys:\n\nkadjA: %v\nkadjB: %v", kadjA, kadjB)
	}

	kadj := make(KAdjMatrix, len(kadjB))
	for k, tob := range kadjB {
		kadj[k] = kadjA.Map(tob)
	}
	return kadj
}

// KAdj computes adjacency matrix for t1 -> t2 transition.
//
// The set of keys for which kadj matrix is computed can be optionally provided.
// This set of keys defaults to allTestKeys(t1,t2).
//
// KAdj itself is verified by testΔBTail on entries with .kadjOK set.
func KAdj(t1, t2 *xbtreetest.Commit, keysv ...setKey) (kadj KAdjMatrix) {
	// assert KAdj(A,B) == KAdj(B,A)
	kadj12 := _KAdj(t1,t2, keysv...)
	kadj21 := _KAdj(t2,t1, keysv...)
	if !reflect.DeepEqual(kadj12, kadj21) {
		panicf("KAdj not symmetric:\nt1: %s\nt2: %s\nkadj12: %v\nkadj21: %v",
			t1.Tree, t2.Tree, kadj12, kadj21)
	}
	return kadj12
}

const debugKAdj = false
func debugfKAdj(format string, argv ...interface{}) {
	if debugKAdj {
		fmt.Printf(format, argv...)
	}
}

func _KAdj(t1, t2 *xbtreetest.Commit, keysv ...setKey) (kadj KAdjMatrix) {
	var keys setKey
	switch len(keysv) {
	case 0:
		keys = allTestKeys(t1, t2)
	case 1:
		keys = keysv[0]
	default:
		panic("multiple key sets on the call")
	}

	debugfKAdj("\n\n_KAdj\n")
	debugfKAdj("t1:   %s\n", t1.Tree)
	debugfKAdj("t2:   %s\n", t2.Tree)
	debugfKAdj("keys: %s\n", keys)
	defer func() {
		debugfKAdj("kadj -> %v\n", kadj)
	}()

	// kadj = {} k -> adjacent keys.
	// if k is tracked and covered by changed leaf -> changes to adjacents must be in Update(t1->t2).
	kadj = KAdjMatrix{}
	for k := range keys {
		adj1 := setKey{}
		adj2 := setKey{}

		q1 := &blib.RangedKeySet{}; q1.Add(k)
		q2 := &blib.RangedKeySet{}; q2.Add(k)
		done1 := &blib.RangedKeySet{}
		done2 := &blib.RangedKeySet{}

		debugfKAdj("\nk%s\n", kstr(k))
		for !q1.Empty() || !q2.Empty() {
			debugfKAdj("q1:   %s\tdone1:  %s\n", q1, done1)
			debugfKAdj("q2:   %s\tdone2:  %s\n", q2, done2)
			for _, r1 := range q1.AllRanges() {
				lo1 := r1.Lo
				for {
					b1 := t1.Xkv.Get(lo1)
					debugfKAdj("  b1: %s\n", b1)
					for k_ := range keys {
						if b1.Keycov.Has(k_) {
							adj1.Add(k_)
							debugfKAdj("    adj1 += %s\t-> %s\n", kstr(k_), adj1)
						}
					}
					done1.AddRange(b1.Keycov)
					// q2 |= (b1.keyrange \ done2)
					δq2 := &blib.RangedKeySet{}
					δq2.AddRange(b1.Keycov)
					δq2.DifferenceInplace(done2)
					q2.UnionInplace(δq2)
					debugfKAdj("q2 += %s\t-> %s\n", δq2, q2)

					// continue with next right bucket until r1 coverage is complete
					if r1.Hi_ <= b1.Keycov.Hi_ {
						break
					}
					lo1 = b1.Keycov.Hi_ + 1
				}
			}
			q1.Clear()

			for _, r2 := range q2.AllRanges() {
				lo2 := r2.Lo
				for {
					b2 := t2.Xkv.Get(lo2)
					debugfKAdj("  b2: %s\n", b2)
					for k_ := range keys {
						if b2.Keycov.Has(k_) {
							adj2.Add(k_)
							debugfKAdj("    adj2 += %s\t-> %s\n", kstr(k_), adj2)
						}
					}
					done2.AddRange(b2.Keycov)
					// q1 |= (b2.keyrange \ done1)
					δq1 := &blib.RangedKeySet{}
					δq1.AddRange(b2.Keycov)
					δq1.DifferenceInplace(done1)
					q1.UnionInplace(δq1)
					debugfKAdj("q1 += %s\t-> %s\n", δq1, q1)

					// continue with next right bucket until r2 coverage is complete
					if r2.Hi_ <= b2.Keycov.Hi_ {
						break
					}
					lo2 = b2.Keycov.Hi_ + 1
				}
			}
			q2.Clear()
		}

		adj := setKey{}; adj.Update(adj1); adj.Update(adj2)
		kadj[k] = adj
	}

	return kadj
}


// ----------------------------------------

// assertΔTtail verifies state of ΔTtail that corresponds to treeRoot in δbtail.
// it also verifies that δbtail.vδBroots matches ΔTtail data.
func assertΔTtail(t *testing.T, subj string, δbtail *ΔBtail, tj *xbtreetest.Commit, treeRoot zodb.Oid, vδTok ...map[Key]Δstring) {
	t.Helper()

	T := tj.T // TODO better require t to be xbtreetest.T instead

	l := len(vδTok)
	var vatOK  []zodb.Tid
	var vδTok_ []map[Key]Δstring
	t0 := tj
	for i := 0; i<l; i++ {
		// empty vδTok entries means they should be absent in vδT
		if δTok := vδTok[l-i-1]; len(δTok) != 0 {
			vatOK  = append([]zodb.Tid{t0.At}, vatOK...)
			vδTok_ = append([]map[Key]Δstring{δTok}, vδTok_...)
		}
		t0 = t0.Prev
	}
	vδTok = vδTok_
	δTtail, ok := δbtail.byRoot[treeRoot]
	var vδToid []ΔTree
	if ok {
		vδToid = δTtail.vδT
	}

	var vat []zodb.Tid
	var vδT []map[Key]Δstring
	atPrev := t0.At
	for _, δToid := range vδToid {
		vat = append(vat, δToid.Rev)
		δT := xgetδKV(T.XGetCommit(atPrev), T.XGetCommit(δToid.Rev), δToid.KV) // {} k -> δ(ZBlk(oid).data)
		vδT = append(vδT, δT)
		atPrev = δToid.Rev
	}

	var vatδB []zodb.Tid // δbtail.vδBroots/treeRoot
	for _, δBroots := range δbtail.vδBroots {
		if δBroots.Roots.Has(treeRoot) {
			vatδB = append(vatδB, δBroots.Rev)
		}
	}

	tok := tidvEqual(vat,   vatOK) && vδTEqual(vδT, vδTok)
	bok := tidvEqual(vatδB, vatOK)
	if !(tok && bok) {
		emsg := fmt.Sprintf("%s: vδT:\n", subj)
		have := ""
		for i := 0; i<len(vδT); i++ {
			have += fmt.Sprintf("\n\t@%s: %v", T.AtSymb(vat[i]), vδT[i])
		}
		emsg += fmt.Sprintf("have: %s\n", have)

		if !tok {
			want := ""
			for i := 0; i<len(vδTok); i++ {
				want += fmt.Sprintf("\n\t@%s: %v", T.AtSymb(vatOK[i]), vδTok[i])
			}
			emsg += fmt.Sprintf("want: %s\n", want)
		}

		if !bok {
			vδb_root := ""
			for i := 0; i<len(vatδB); i++ {
				vδb_root += fmt.Sprintf("\n\t@%s", T.AtSymb(vatδB[i]))
			}
			emsg += fmt.Sprintf("vδb/root: %s\n", vδb_root)
		}

		t.Error(emsg)
	}
}

// assertTrack verifies state of .trackSet and ΔTtail.(k)trackNew.
// it assumes that only one tree root is being tracked.
func (δBtail *ΔBtail) assertTrack(t *testing.T, subj string, trackSetOK blib.PPTreeSubSet, trackNewOK blib.PPTreeSubSet, ktrackNewOK *blib.RangedKeySet) {
	t.Helper()
	if !δBtail.trackSet.Equal(trackSetOK) {
		t.Errorf("%s: trackSet:\n\thave: %v\n\twant: %v", subj, δBtail.trackSet, trackSetOK)
	}

	roots := setOid{}
	for root := range δBtail.byRoot {
		roots.Add(root)
	}

	tEmpty := trackNewOK.Empty()
	kEmpty := ktrackNewOK.Empty()
	if tEmpty != kEmpty {
		t.Errorf("BUG: %s: empty(trackNewOK) != empty(ktrackNewOK)", subj)
		return
	}
	nrootsOK := 1
	if trackSetOK.Empty() && tEmpty {
		nrootsOK = 0
	}
	if len(roots) != nrootsOK {
		t.Errorf("%s: len(byRoot) != %d   ; roots=%v", subj, nrootsOK, roots)
		return
	}
	if nrootsOK == 0 {
		return
	}

	root := roots.Elements()[0]

	δTtail := δBtail.byRoot[root]

	trackNewRootsOK := setOid{}
	if !trackNewOK.Empty() {
		trackNewRootsOK.Add(root)
	}

	if !δBtail.trackNewRoots.Equal(trackNewRootsOK) {
		t.Errorf("%s: trackNewRoots:\n\thave: %v\n\twant: %v", subj, δBtail.trackNewRoots, trackNewRootsOK)
	}

	if !δTtail.trackNew.Equal(trackNewOK) {
		t.Errorf("%s: vδT.trackNew:\n\thave: %v\n\twant: %v", subj, δTtail.trackNew, trackNewOK)
	}
	if !δTtail.ktrackNew.Equal(ktrackNewOK) {
		t.Errorf("%s: vδT.ktrackNew:\n\thave: %v\n\twant: %v", subj, δTtail.ktrackNew, ktrackNewOK)
	}
}

// trackSet returns what should be ΔBtail.trackSet coverage for specified tracked key set.
func trackSet(rbs xbtreetest.RBucketSet, tracked setKey) blib.PPTreeSubSet {
	// nil = don't compute keyCover
	// (trackSet is called from inside hot inner loop of rebuild test)
	return _trackSetWithCov(rbs, tracked, nil)
}

// trackSetWithCov returns what should be ΔBtail.trackSet and its key coverage for specified tracked key set.
func trackSetWithCov(rbs xbtreetest.RBucketSet, tracked setKey) (trackSet blib.PPTreeSubSet, keyCover *blib.RangedKeySet) {
	keyCover = &blib.RangedKeySet{}
	trackSet = _trackSetWithCov(rbs, tracked, keyCover)
	return trackSet, keyCover
}

func _trackSetWithCov(rbs xbtreetest.RBucketSet, tracked setKey, outKeyCover *blib.RangedKeySet) (trackSet blib.PPTreeSubSet) {
	trackSet = blib.PPTreeSubSet{}
	for k := range tracked {
		kb := rbs.Get(k)
		if outKeyCover != nil {
			outKeyCover.AddRange(kb.Keycov)
		}
		trackSet.AddPath(kb.Path())
	}
	return trackSet
}


// trackKeys issues δbtail.Track requests for tree[keys].
func trackKeys(δbtail *ΔBtail, t *xbtreetest.Commit, keys setKey) {
	head := δbtail.Head()
	if head != t.At {
		panicf("BUG: δbtail.head: %s  ; t.at: %s", head, t.At)
	}

	for k := range keys {
		// NOTE: if tree is deleted - the following adds it to tracked
		// set with every key being a hole. This aligns with the
		// following situation
		//
		//	T1 -> ø -> T2
		//
		// where after T1->ø, even though the tree becomes deleted, its root
		// continues to be tracked and all keys migrate to holes in the
		// tracking set. By aligning initial state to the same as after
		// T1->ø, we test what will happen on ø->T2.
		b := t.Xkv.Get(k)
		δbtail.track(b.Path(), b.Keycov)
	}
}


// xgetδKV translates {k -> δ<oid>} to {k -> δ(ZBlk(oid).data)} according to t1..t2 db snapshots.
func xgetδKV(t1, t2 *xbtreetest.Commit, δkvOid map[Key]ΔValue) map[Key]Δstring {
	δkv := make(map[Key]Δstring, len(δkvOid))
	for k, δvOid := range δkvOid {
		δkv[k] = Δstring{
			Old: t1.XGetBlkData(δvOid.Old),
			New: t2.XGetBlkData(δvOid.New),
		}
	}
	return δkv
}


// -------- misc --------


// IntSets generates all sets of integers in range [0,N)
func IntSets(N int) chan []int {
	ch := make(chan []int)
	go intSets(ch, 0, N)
	return ch
}

// intSets generates all sets of integers in range [lo,hi)
func intSets(ch chan []int, lo, hi int) {
	ch <- nil // ø

	if lo < hi {
		for i := lo; i < hi; i++ {
			chTail := make(chan []int)
			go intSets(chTail, i+1, hi)
			for tail := range chTail {
				ch <- append([]int{i}, tail...) // i + tail
			}
		}
	}

	close(ch)
}

func TestIntSets(t *testing.T) {
	got := [][]int{}
	for is := range IntSets(3) {
		got = append(got, is)
	}

	I := func(v ...int) []int { return v }
	want := [][]int{I(),
			I(0), I(0,1), I(0,1,2), I(0,2),
			I(1), I(1,2),
			I(2),
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("error:\ngot:  %v\nwant: %v", got, want)
	}
}


// allTestKeys returns all keys from vt + ∞.
func allTestKeys(vt ...*xbtreetest.Commit) setKey {
	allKeys := setKey{}; allKeys.Add(KeyMax) // ∞ simulating ZBigFile.Size() query
	for _, t := range vt {
		for _, b := range t.Xkv {
			for k := range b.KV {
				allKeys.Add(k)
			}
		}
	}
	return allKeys
}

func sortedKeys(kv map[Key]Δstring) []Key {
	keyv := []Key{}
	for k := range kv {
		keyv = append(keyv, k)
	}
	sort.Slice(keyv, func(i, j int) bool {
		return keyv[i] < keyv[j]
	})
	return keyv
}

func tidvEqual(av, bv []zodb.Tid) bool {
	if len(av) != len(bv) {
		return false
	}
	for i, a := range av {
		if bv[i] != a {
			return false
		}
	}
	return true
}

func vδTEqual(vδa, vδb []map[Key]Δstring) bool {
	if len(vδa) != len(vδb) {
		return false
	}
	for i, δa := range vδa {
		if !δTEqual(δa, vδb[i]) {
			return false
		}
	}
	return true
}

func δTEqual(δa, δb map[Key]Δstring) bool {
	if len(δa) != len(δb) {
		return false
	}
	for k, δ := range δa {
		δ_, ok := δb[k]
		if !ok || δ != δ_ {
			return false
		}
	}
	return true
}
