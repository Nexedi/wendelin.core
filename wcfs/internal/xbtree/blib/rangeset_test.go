// Copyright (C) 2021  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

import (
	"testing"
	"unsafe"
)

func TestRangedKeySetTypes(t *testing.T) {
	// verify that size(void) == 0  and  that _RangedMap_voidEntry has the same layout as KeyRange
	sizeVoid := unsafe.Sizeof(void{})
	if sizeVoid != 0 {
		t.Errorf("sizeof(void) = %d  ; want 0", sizeVoid)
	}

	// verify that sizeof(set-entry) = sizeof(KeyRange)
	// NOTE for this to be true Value needs to come first in RangedMapEntry
	// (see github.com/golang/go/issues/48651)
	sizeKeyRange := unsafe.Sizeof(KeyRange{})
	sizeRangedMap_voidEntry := unsafe.Sizeof(_RangedMap_voidEntry{})
	if sizeRangedMap_voidEntry != sizeKeyRange {
		t.Errorf("sizeof(RangeMap_voidEntry)(%d)  !=  sizeof(KeyRange)(%d)",
			sizeRangedMap_voidEntry, sizeKeyRange)
	}
}

func TestRangedKeySet(t *testing.T) {
	type testEntry struct {
		A, B         *RangedKeySet
		Union        *RangedKeySet
		Difference   *RangedKeySet
		Intersection *RangedKeySet
	}
	E := func(A, B, U, D, I *RangedKeySet) testEntry {
		return testEntry{A, B, U, D, I}
	}

	// S is shorthand to create RangedKeySet, e.g. S(1,2, 4,5) will return {[1,2) [4,5)}
	S := func(kv ...Key) *RangedKeySet {
		l := len(kv)
		if l % 2 != 0 {
			panic("odd number of keys")
		}
		S := &RangedKeySet{}
		for i := 0; i < l/2; i++ {
			// construct .entryv directly, not via AddRange
			lo := kv[2*i]
			hi := kv[2*i+1]
			hi_ := hi
			if hi_ != oo {
				hi_--
			}
			S.m.entryv = append(S.m.entryv, _RangedMap_voidEntry{
					void{},
					KeyRange{lo, hi_},
			})
		}
		S.verify()
		return S
	}

	testv := []testEntry{
		E(
			S(),	// A
			S(),	// B
				S(),	// U
				S(),	// D
				S()),	// I

		E(
			S(),	// A
			S(1,2),	// B
				S(1,2),	// U
				S(),	// D
				S()),	// I

		E(
			S(1,2),	// A
			S(),	// B
				S(1,2),	// U
				S(1,2), // D
				S()),	// I

		E(
			S(1,2),	// A
			S(1,2),	// B
				S(1,2),	// U
				S(),	// D
				S(1,2)),// I

		// adjacent [1,3) [3,5)
		E(
			S(1,3), // A
			S(3,5), // B
				S(1,5),	// U
				S(1,3),	// D
				S()),	// I

		// overlapping [1,3) [2,4)
		E(
			S(1,3),	// A
			S(2,4),	// B
				S(1,4),	// U
				S(1,2),	// D
				S(2,3)),// I

		// [1,7) \ [3,5)  ->  [1,3) [5,7)
		E(
			S(1,7),	// A
			S(3,5),	// B
				S(1,7),		// U
				S(1,3, 5,7),	// D
				S(3,5)),	// I

		// several ranges \ [-∞,∞) -> ø
		E(
			S(1,3, 5,7, 11,100), // A
			S(noo, oo),          // B
				S(noo, oo),		// U
				S(),			// D
				S(1,3, 5,7, 11,100)),	// I

		// [1,3) [5,7) + insert [3,5) -> [1,7)
		E(
			S(1,3, 5,7),	// A
			S(3,5),		// B
				S(1,7),		// U
				S(1,3, 5,7),	// D
				S()),		// I

		// delete covering several ranges
		// [-1,0)  [1,3) [5,7) [9,11) [15,20)  [100,200)   \  [2,17)
		E(
			S(-1,0, 1,3, 5,7, 9,11, 15,20, 100,200),// A
			S(2,17),				// B
				S(-1,0, 1,20, 100,200),			// U
				S(-1,0, 1,2, 17,20, 100,200),		// D
				S(2,3, 5,7, 9,11, 15,17)),		// I
	}

	for _, tt := range testv {
		A := tt.A
		B := tt.B
		U := A.Union(B)
		D := A.Difference(B)
		I := A.Intersection(B)

		if !U.Equal(tt.Union) {
			t.Errorf("Union:\n  A:   %s\n  B:   %s\n  ->u: %s\n  okU: %s\n", A, B, U, tt.Union)
		}
		if !D.Equal(tt.Difference) {
			t.Errorf("Difference:\n  A:   %s\n  B:   %s\n  ->d: %s\n  okD: %s\n", A, B, D, tt.Difference)
		}
		if !I.Equal(tt.Intersection) {
			t.Errorf("Intersection:\n  A:   %s\n  B:   %s\n  ->i: %s\n  okI: %s\n", A, B, I, tt.Intersection)
		}

		// HasRange
		assertSetHasRanges(t, A, A.AllRanges(), true)
		assertSetHasRanges(t, B, B.AllRanges(), true)
		assertSetHasRanges(t, U, A.AllRanges(), true)
		assertSetHasRanges(t, U, B.AllRanges(), true)
		assertSetHasRanges(t, A, I.AllRanges(), true)
		assertSetHasRanges(t, B, I.AllRanges(), true)
		assertSetHasRanges(t, U, I.AllRanges(), true)

		Dab := D
		Dba := B.Difference(A)
		assertSetHasRanges(t, A, Dab.AllRanges(), true)
		assertSetHasRanges(t, B, Dab.AllRanges(), false)
		assertSetHasRanges(t, B, Dba.AllRanges(), true)
		assertSetHasRanges(t, A, Dba.AllRanges(), false)
		assertSetHasRanges(t, Dab, I.AllRanges(), false)
		assertSetHasRanges(t, Dba, I.AllRanges(), false)
		assertSetHasRanges(t, I,   Dab.AllRanges(), false)
		assertSetHasRanges(t, I,   Dba.AllRanges(), false)

		// IntersectsRange  (= (A^B)!=ø)
		assertSetIntersectsRanges(t, A, I.AllRanges(), !I.Empty())
		assertSetIntersectsRanges(t, B, I.AllRanges(), !I.Empty())
		assertSetIntersectsRanges(t, Dab, B.AllRanges(), false)
		assertSetIntersectsRanges(t, Dba, A.AllRanges(), false)
		assertSetIntersectsRanges(t, Dab, I.AllRanges(), false)
		assertSetIntersectsRanges(t, Dba, I.AllRanges(), false)
		assertSetIntersectsRanges(t, I,   Dab.AllRanges(), false)
		assertSetIntersectsRanges(t, I,   Dba.AllRanges(), false)
	}
}

// assertSetHasRanges asserts for all ranges from rangev that RangedSet S.HasRange(r) == hasOK.
func assertSetHasRanges(t *testing.T, S *RangedKeySet, rangev []KeyRange, hasOK bool) {
	t.Helper()
	for _, r := range rangev {
		has := S.HasRange(r)
		if has != hasOK {
			t.Errorf("HasRange:\n  S:   %s\n  r:   %s\n  ->: %v\n  ok: %v\n", S, r, has, hasOK)
		}
	}
}

// assertSetIntersectsRanges asserts for all ranges from rangev that RangedSet S.IntersectsRange(r) == intersectsOK.
func assertSetIntersectsRanges(t *testing.T, S *RangedKeySet, rangev []KeyRange, intersectsOK bool) {
	t.Helper()
	for _, r := range rangev {
		intersects := S.IntersectsRange(r)
		if intersects != intersectsOK {
			t.Errorf("IntersectsRange:\n  S:   %s\n  r:   %s\n  ->: %v\n  ok: %v\n", S, r, intersects, intersectsOK)
		}
	}
}
