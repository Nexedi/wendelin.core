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

//go:generate ./gen-rangemap _RangedMap_str string zrangemap_str.go

import (
	"reflect"
	"testing"
)

type (
	RangedMap      = _RangedMap_str
	RangedMapEntry = _RangedMap_strEntry
)

const (
	oo  = KeyMax
	noo = KeyMin
)

func TestRangedMap(t *testing.T) {
	type testEntry struct {
		M          *RangedMap
		X          RangedMapEntry
		Set        *RangedMap // M.SetRange(X.keycov, X.value)
		Del        *RangedMap // M.DelRange(X.keycov)
		Has        bool       // M.HasRange(X.keycov)
		Intersects bool       // M.IntersectsRange(X.Keycov)
	}
	E := func(M *RangedMap, X RangedMapEntry, S, D *RangedMap, H, I bool) testEntry {
		return testEntry{M, X, S, D, H, I}
	}

	// M is shorthand to create RangedMap, e.g. M(1,2,a, 3,4,b) will return {[1,2):a [3,4):b}.
	M := func(argv ...interface{}) *RangedMap {
		l := len(argv)
		if l % 3 != 0 {
			panic("non 3x number of arguments")
		}
		// asKey converts arg to Key
		asKey := func(arg interface{}) Key {
			// go through reflect to accept all int, int64, Key, ...
			return Key(reflect.ValueOf(arg).Int())
		}
		M := &RangedMap{}
		for i := 0; i < l/3; i++ {
			// construct .entryv directly, not via SetRange
			lo := asKey(argv[3*i])
			hi := asKey(argv[3*i+1])
			v  := argv[3*i+2].(string)
			hi_ := hi
			if hi_ != oo {
				hi_--
			}
			M.entryv = append(M.entryv, RangedMapEntry{
					v,
					KeyRange{lo, hi_},
			})
		}
		M.verify()
		return M
	}

	// X creates RangedMapEntry{v, [lo,hi)}
	X := func(lo,hi Key, v string) RangedMapEntry {
		hi_ := hi
		if hi_ != oo {
			hi_--
		}
		return RangedMapEntry{v, KeyRange{lo, hi_}}
	}

	// y, n alias true/false
	const y, n = true, false

	// a, b, c, ... are shorthands for corresponding strings
	const a, b, c, x = "a", "b", "c", "x"


	testv := []testEntry{
		// empty vs empty
		E(
			M(),			// M
			X(0,0,x),		// X
				M(),			// Set
				M(),			// Del
				y,			// Has
				n),			// Intersects

		// empty vs !empty
		E(
			M(),			// M
			X(1,2,x),		// X
				M(1,2,x),		// Set
				M(),			// Del
				n,			// Has
				n),			// Intersects

		// !empty vs empty
		E(
			M(1,2,a),		// M
			X(0,0,x),		// X
				M(1,2,a),		// Set
				M(1,2,a),		// Del
				y,			// Has
				n),			// Intersects

		// basic change
		E(
			M(1,2,a),		// M
			X(1,2,x),		// X
				M(1,2,x),		// Set
				M(),			// Del
				y,			// Has
				y),			// Intersects

		// adjacent [1,3) [3,5)
		E(
			M(1,3,a),		// M
			X(3,5,x),		// X
				M(1,3,a, 3,5,x),	// Set
				M(1,3,a),		// Del
				n,			// Has
				n),			// Intersects

		// overlapping [1,3) [2,4)
		E(
			M(1,3,a),		// M
			X(2,4,x),		// X
				M(1,2,a, 2,4,x),	// Set
				M(1,2,a),		// Del
				n,			// Has
				y),			// Intersects

		// [1,7) vs [3,5) -> split
		E(
			M(1,7,a),		// M
			X(3,5,x),		// X
				M(1,3,a, 3,5,x, 5,7,a),	// Set
				M(1,3,a,        5,7,a),	// Del
				y,			// Has
				y),			// Intersects

		// several ranges vs [-∞,∞)
		E(
			M(1,3,a, 5,7,b, 8,9,c),	// M
			X(noo,oo,x),		// X
				M(noo,oo,x),		// Set
				M(),			// Del
				n,			// Has
				y),			// Intersects

		E(
			M(1,2,a, 2,3,b),	// M
			X(1,3,x),		// X
				M(1,3,x),		// Set
				M(),			// Del
				y,			// Has
				y),			// Intersects

		// coalesce (same value, no overlap)
		E(
			M(1,2,a, 4,5,a),	// M
			X(2,4,a),		// X
				M(1,5,a),		// Set
				M(1,2,a, 4,5,a),	// Del
				n,			// Has
				n),			// Intersects

		// coalesce (same value, overlap)
		E(
			M(1,4,a, 5,8,a),	// M
			X(2,6,a),		// X
				M(1,8,a),		// Set
				M(1,2,a, 6,8,a),	// Del
				n,			// Has
				y),			// Intersects

		// - shrink left/right (value !same) + new entry
		E(
			M(1,4,a),		// M
			X(2,6,x),		// X
				M(1,2,a, 2,6,x),	// Set
				M(1,2,a),		// Del
				n,			// Has
				y),			// Intersects
		E(
			M(5,8,b),		// M
			X(2,6,x),		// X
				M(2,6,x, 6,8,b),	// Set
				M(       6,8,b),	// Del
				n,			// Has
				y),			// Intersects
		E(
			M(1,4,a, 5,8,b),	// M
			X(2,6,x),		// X
				M(1,2,a, 2,6,x, 6,8,b),	// Set
				M(1,2,a,        6,8,b),	// Del
				n,			// Has
				y),			// Intersects
	}

	for _, tt := range testv {
		M := tt.M
		X := tt.X
		r := X.KeyRange
		v := X.Value

		assertMapHasRange(t, M, r, tt.Has)
		assertMapIntersectsRange(t, M, r, tt.Intersects)
		Mset := M.Clone()
		Mdel := M.Clone()
		Mset.SetRange(r, v)
		Mdel.DelRange(r)

		if !Mset.Equal(tt.Set) {
			t.Errorf("SetRange:\n  M:   %s\n  e:   %s\n  ->·: %s\n  ok·: %s\n", M, X, Mset, tt.Set)

		}

		if !Mdel.Equal(tt.Del) {
			t.Errorf("DelRange:\n  M:   %s\n  r:   %s\n  ->·: %s\n  ok·: %s\n", M, r, Mdel, tt.Del)

		}

		assertMapHasRange(t, Mset, r, true)
		assertMapHasRange(t, Mdel, r, r.Empty())
		assertMapIntersectsRange(t, Mset, r, !r.Empty())
		assertMapIntersectsRange(t, Mdel, r, false)

		verifyGet(t, M)
		verifyGet(t, Mset)
		verifyGet(t, Mdel)
	}
}

// assertMapHasRange asserts that RangedMap M.HasRange(r) == hasOK.
func assertMapHasRange(t *testing.T, M *RangedMap, r KeyRange, hasOK bool) {
	t.Helper()
	has := M.HasRange(r)
	if !(has == hasOK) {
		t.Errorf("HasRange:\n  M:   %s\n  r:   %s\n  ->·: %t\n  ok·: %t\n", M, r, has, hasOK)
	}
}

// assertMapIntersectsRange asserts that RangedMap M.IntersectsRange(r) == intersectsOK.
func assertMapIntersectsRange(t *testing.T, M *RangedMap, r KeyRange, intersectsOK bool) {
	t.Helper()
	intersects := M.IntersectsRange(r)
	if !(intersects == intersectsOK) {
		t.Errorf("IntersectsRange:\n  M:   %s\n  r:   %s\n  ->·: %t\n  ok·: %t\n", M, r, intersects, intersectsOK)
	}
}

// verifyGet verifies RangedMap.Get .
func verifyGet(t *testing.T, M *RangedMap) {
	t.Helper()

	var Mranges []RangedMapEntry
	Mranges = append(Mranges, M.AllRanges()...) // copy just in case it changes on Get

	// verify "has-data"
	Z := KeyRange{-100,+100} // models [-∞,∞)
	for _, e := range Mranges {
		lo  := kmax(e.Lo,  Z.Lo)
		hi_ := kmin(e.Hi_, Z.Hi_)
		for k := lo; k <= hi_; k++ {
			v, r, ok := M.Get_(k)
			if !(v == e.Value && r == e.KeyRange && ok) {
				t.Errorf("%s\tGet(%s):\nhave: %q%s, %t\nwant: %q%s, true",
					M, KStr(k), v, r, ok, e.Value, e.KeyRange)
			}
		}
	}

	// verify "no-data"
	// NA = [-∞,∞) \ M
	na := RangedKeySet{}
	na.AddRange(Z)
	for _, e := range Mranges {
		na.DelRange(e.KeyRange)
	}

	for _, r := range na.AllRanges() {
		lo  := kmax(r.Lo,  Z.Lo)
		hi_ := kmin(r.Hi_, Z.Hi_)
		for k := lo; k <= hi_; k++ {
			v, r_, ok := M.Get_(k)
			if !(v == "" && r_.Empty() && !ok) {
				t.Errorf("%s\tGet(%s):\nhave: %q%s, %t\nwant: %q[), false",
					M, KStr(k), v, r_, ok, "")
			}
		}
	}
}


// kmin returns min(a,b).
func kmin(a, b Key) Key {
	if a < b {
		return a
	} else {
		return b
	}
}

// kmax returns max(a,b).
func kmax(a, b Key) Key {
	if a > b {
		return a
	} else {
		return b
	}
}
