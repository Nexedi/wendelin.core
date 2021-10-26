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
// set of [lo,hi) Key ranges.

//go:generate ./gen-rangemap _RangedMap_void void zrangemap_void.go


// RangedKeySet is set of Keys with adjacent keys coalesced into Ranges.
//
// Zero value represents empty set.
type RangedKeySet struct {
	m _RangedMap_void
}

// void is used as value type for RangedMap to be used as set.
type void struct{}
func (_ void) String() string { return "" }



// Add adds key k to the set.
func (S *RangedKeySet) Add(k Key) {
	S.AddRange(KeyRange{Lo: k, Hi_: k})
}

// Del removes key k from the set.
func (S *RangedKeySet) Del(k Key) {
	S.DelRange(KeyRange{Lo: k, Hi_: k})
}

// Has returns whether key k belongs to the set.
func (S *RangedKeySet) Has(k Key) bool {
	return S.HasRange(KeyRange{Lo: k, Hi_: k})
}


// AddRange adds range r to the set.
func (S *RangedKeySet) AddRange(r KeyRange) {
	S.m.SetRange(r, void{})
}

// DelRange removes range r from the set.
func (S *RangedKeySet) DelRange(r KeyRange) {
	S.m.DelRange(r)
}

// HasRange returns whether all keys from range r belong to the set.
func (S *RangedKeySet) HasRange(r KeyRange) bool {
	return S.m.HasRange(r)
}

// IntersectsRange returns whether some keys from range r belong to the set.
func (S *RangedKeySet) IntersectsRange(r KeyRange) bool {
	return S.m.IntersectsRange(r)
}


// Union returns RangedKeySet(A.keys | B.keys).
func (A *RangedKeySet) Union(B *RangedKeySet) *RangedKeySet {
	U := A.Clone()
	U.UnionInplace(B)
	return U
}

// Difference returns RangedKeySet(A.keys \ B.keys).
func (A *RangedKeySet) Difference(B *RangedKeySet) *RangedKeySet {
	D := A.Clone()
	D.DifferenceInplace(B)
	return D
}

// Intersection returns RangedKeySet(A.keys ^ B.keys).
func (A *RangedKeySet) Intersection(B *RangedKeySet) *RangedKeySet {
	I := A.Clone()
	I.IntersectionInplace(B)
	return I
}

func (A *RangedKeySet) UnionInplace(B *RangedKeySet) {
	A.verify()
	B.verify()
	defer A.verify()

	// XXX dumb
	for _, e := range B.m.entryv {
		A.AddRange(e.KeyRange)
	}
}

func (A *RangedKeySet) DifferenceInplace(B *RangedKeySet) {
	A.verify()
	B.verify()
	defer A.verify()

	// XXX dumb
	for _, e := range B.m.entryv {
		if A.Empty() {
			break
		}
		A.DelRange(e.KeyRange)
	}
}

func (A *RangedKeySet) IntersectionInplace(B *RangedKeySet) {
	A.verify()
	B.verify()
	defer A.verify()

	// XXX very dumb
	// A^B = (A∪B) \ (A\B ∪ B\A)
	AdB := A.Difference(B)
	BdA := B.Difference(A)
	ddd := AdB
	ddd.UnionInplace(BdA)
	A.UnionInplace(B)
	A.DifferenceInplace(ddd)
}


// --------

// verify checks RangedKeySet for internal consistency.
func (S *RangedKeySet) verify() {
	S.m.verify()
}

// Clone returns copy of the set.
func (orig *RangedKeySet) Clone() *RangedKeySet {
	return &RangedKeySet{*orig.m.Clone()}
}

// Empty returns whether the set is empty.
func (S *RangedKeySet) Empty() bool {
	return S.m.Empty()
}

// Equal returns whether A == B.
func (A *RangedKeySet) Equal(B *RangedKeySet) bool {
	return A.m.Equal(&B.m)
}

// Clear removes all elements from the set.
func (S *RangedKeySet) Clear() {
	S.m.Clear()
}

// AllRanges returns slice of all key ranges in the set.
//
// TODO -> iter?
func (S *RangedKeySet) AllRanges() /*readonly*/[]KeyRange {
	// TODO we could use unsafe and cast .m.AllRanges to []KeyRange to avoid extra alloc/copy
	// return []KeyRange(S.m.AllRanges())
	ev := S.m.AllRanges()
	rv := make([]KeyRange, len(ev))
	for i := range ev {
		rv[i] = ev[i].KeyRange
	}
	return rv
}

func (S RangedKeySet) String() string {
	// RangedMap<void> supports formatting for set out of the box
	return S.m.String()
}
