// Code generated by gen-rangemap _RangedMap_RebuildJob *_RebuildJob; DO NOT EDIT.

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

package xbtree

import "lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/blib"

// map [lo,hi) Key ranges to values.

import (
	"fmt"
	"sort"
)

const trace_RangedMap_RebuildJob = false
const debug_RangedMap_RebuildJob = false

// _RangedMap_RebuildJob is Key->*_RebuildJob map with adjacent keys mapped to the same value coalesced into Ranges.
//
// Zero value represents empty map.
type _RangedMap_RebuildJob struct {
	// TODO rework to use BTree lo->hi_ instead if in practice in treediff,
	// and other usage places, N(ranges) turns out to be not small
	// (i.e. performance turns out to be not acceptable)
	entryv []_RangedMap_RebuildJobEntry // lo↑
}

// _RangedMap_RebuildJobEntry represents one entry in _RangedMap_RebuildJob.
type _RangedMap_RebuildJobEntry struct {
	Value    *_RebuildJob
	blib.KeyRange
}


// Get returns value associated with key k.
//
// blib.KeyRange indicates all keys adjacent to k, that are too mapped to the same value.
func (M *_RangedMap_RebuildJob) Get(k Key) (*_RebuildJob, blib.KeyRange) {
	v, r, _ := M.Get_(k)
	return v, r
}

// Set changes M to map key k to value v.
func (M *_RangedMap_RebuildJob) Set(k Key, v *_RebuildJob) {
	M.SetRange(blib.KeyRange{Lo: k, Hi_: k}, v)
}

// Del removes key k.
func (M *_RangedMap_RebuildJob) Del(k Key) {
	M.DelRange(blib.KeyRange{Lo: k, Hi_: k})
}

// Has returns whether key k is present in the map.
func (M *_RangedMap_RebuildJob) Has(k Key) bool {
	_, _, ok := M.Get_(k)
	return ok
}



// Get_ is comma-ok version of Get.
func (M *_RangedMap_RebuildJob) Get_(k Key) (v *_RebuildJob, r blib.KeyRange, ok bool) {
	r = blib.KeyRange{0,-1} // zero value represents non-empty [0,1)
	if trace_RangedMap_RebuildJob {
		fmt.Printf("\n\nGet_:\n")
		fmt.Printf("  M: %s\n", M)
		fmt.Printf("  k: %s\n", blib.KStr(k))
		defer func() {
			fmt.Printf("->·: %v%s, %t\n", v, r, ok)
		}()
	}

	M.verify()

	// find first ilo: k < [ilo].hi
	l := len(M.entryv)
	ilo := sort.Search(l, func(i int) bool {
		return k <= M.entryv[i].Hi_
	})
	debugf_RangedMap_RebuildJob("\tilo: %d\n", ilo)

	if ilo == l { // not found
		return
	}

	e := M.entryv[ilo]
	if !(e.Lo <= k) { // not found
		return
	}

	// found
	return e.Value, e.KeyRange, true
}

// SetRange changes M to map key range r to value v.
func (M *_RangedMap_RebuildJob) SetRange(r blib.KeyRange, v *_RebuildJob) {
	e := _RangedMap_RebuildJobEntry{v,r}
	if trace_RangedMap_RebuildJob {
		fmt.Printf("\n\nSetRange:\n")
		fmt.Printf("  M: %s\n", M)
		fmt.Printf("  e: %s\n", e)
		defer fmt.Printf("->·: %s\n", M)
	}

	M.verify()
	defer M.verify()

	if r.Empty() {
		return
	}

	// clear range for r and insert new entry
	// TODO optimize for same-value/set case (just merge all covered
	// entries into one - see commented AddRange from set vvv)
	i := M.delRange(r)
	vInsert__RangedMap_RebuildJob(&M.entryv, i, e)
	debugf_RangedMap_RebuildJob("\tinsert %s\t-> %s\n", e, M)

	// check if we should merge inserted entry with right/left neighbours
	if i+1 < len(M.entryv) { // right
		x     := M.entryv[i]
		right := M.entryv[i+1]
		if (x.Hi_+1 == right.Lo) && (v == right.Value) {
			vReplaceSlice__RangedMap_RebuildJob(&M.entryv, i,i+2,
				_RangedMap_RebuildJobEntry{v, blib.KeyRange{x.Lo, right.Hi_}})
			debugf_RangedMap_RebuildJob("\tmerge right\t-> %s\n", M)
		}
	}

	if i > 0 { // left
		left := M.entryv[i-1]
		x    := M.entryv[i]
		if (left.Hi_+1 == x.Lo) && (left.Value == v) {
			vReplaceSlice__RangedMap_RebuildJob(&M.entryv, i-1,i+1,
				_RangedMap_RebuildJobEntry{v, blib.KeyRange{left.Lo, x.Hi_}})
			debugf_RangedMap_RebuildJob("\tmerge left\t-> %s\n", M)
		}
	}

	// done

/* how it was for just set:
	// find first ilo: r.Lo < [ilo].hi
	l := len(S.rangev)
	ilo := sort.Search(l, func(i int) bool {
		 return r.Lo <= S.rangev[i].Hi_
	})
	debugfRSet("\tilo: %d\n", ilo)

	if ilo == l { // not found
		S.rangev = append(S.rangev, r)
		l++
		debugfRSet("\tappend %s\t-> %s\n", r, S)
	}

	// find last jhi: [jhi].Lo < r.hi
	jhi := ilo
	for ;; jhi++ {
		if jhi == l {
			break
		}
		if S.rangev[jhi].Lo <= r.Hi_ {
			continue
		}
		break
	}
	debugfRSet("\tjhi: %d\n", jhi)

	// entries in [ilo:jhi) ∈ [r.Lo,r.hi) and should be merged into one
	if (jhi - ilo) > 1 {
		lo  := S.rangev[ilo].Lo
		hi_ := S.rangev[jhi-1].Hi_
		vReplaceSlice__RangedMap_RebuildJob(&S.rangev, ilo,jhi, blib.KeyRange{lo,hi_})
		debugfRSet("\tmerge S[%d:%d]\t-> %s\n", ilo, jhi, S)
	}
	jhi = -1 // no longer valid

	// if [r.lo,r.hi) was outside of any entry - create new entry
	if r.Hi_ < S.rangev[ilo].Lo {
		vInsert__RangedMap_RebuildJob(&S.rangev, ilo, r)
		debugfRSet("\tinsert %s\t-> %s\n", r, S)
	}

	// now we have covered entries merged as needed into [ilo]
	// extend this entry if r coverage is wider
	if r.Lo < S.rangev[ilo].Lo {
		S.rangev[ilo].Lo = r.Lo
		debugfRSet("\textend left\t-> %s\n", S)
	}
	if r.Hi_ > S.rangev[ilo].Hi_ {
		S.rangev[ilo].Hi_ = r.Hi_
		debugfRSet("\textend right\t-> %s\n", S)
	}

	// and check if we should merge it with right/left neighbours
	if ilo+1 < len(S.rangev) { // right
		if S.rangev[ilo].Hi_+1 == S.rangev[ilo+1].Lo {
			vReplaceSlice__RangedMap_RebuildJob(&S.rangev, ilo,ilo+2,
				blib.KeyRange{S.rangev[ilo].Lo, S.rangev[ilo+1].Hi_})
			debugfRSet("\tmerge right\t-> %s\n", S)
		}
	}

	if ilo > 0 { // left
		if S.rangev[ilo-1].Hi_+1 == S.rangev[ilo].Lo {
			vReplaceSlice__RangedMap_RebuildJob(&S.rangev, ilo-1,ilo+1,
				blib.KeyRange{S.rangev[ilo-1].Lo, S.rangev[ilo].Hi_})
			debugfRSet("\tmerge left\t-> %s\n", S)
		}
	}

	// done
*/
}

// DelRange removes range r from the map.
func (M *_RangedMap_RebuildJob) DelRange(r blib.KeyRange) {
	if trace_RangedMap_RebuildJob {
		fmt.Printf("\n\nDelRange:\n")
		fmt.Printf("  M: %s\n", M)
		fmt.Printf("  r: %s\n", r)
		defer fmt.Printf("->·: %s\n", M)
	}

	M.verify()
	defer M.verify()

	if r.Empty() {
		return
	}

	M.delRange(r)
}

// delRange deletes range r from the map and returns .entryv index where r
// should be inserted/appended if needed.
//
// r must be !empty.
func (M *_RangedMap_RebuildJob) delRange(r blib.KeyRange) (i int) {
	// find first ilo: r.Lo < [ilo].hi
	l := len(M.entryv)
	ilo := sort.Search(l, func(i int) bool {
		 return r.Lo <= M.entryv[i].Hi_
	})
	debugf_RangedMap_RebuildJob("\tilo: %d\n", ilo)

	if ilo == l { // not found
		debugf_RangedMap_RebuildJob("\tnon-overlap right\n")
		return l
	}

	// find last jhi: [jhi].Lo < r.hi
	jhi := ilo
	for ;; jhi++ {
		if jhi == l {
			break
		}
		if M.entryv[jhi].Lo <= r.Hi_ {
			continue
		}
		break
	}
	debugf_RangedMap_RebuildJob("\tjhi: %d\n", jhi)

	if jhi == 0 {
		debugf_RangedMap_RebuildJob("\tnon-overlap left\n")
		return 0
	}

	// [ilo+1:jhi-1] should be deleted
	// [ilo] and [jhi-1] overlap with [r.lo,r.hi) - they should be deleted, or shrinked,
	// or split+shrinked if ilo==jhi-1 and r is inside [ilo]
	if jhi-ilo == 1 && M.entryv[ilo].Lo < r.Lo && r.Hi_ < M.entryv[ilo].Hi_ {
		x := M.entryv[ilo]
		vInsert__RangedMap_RebuildJob(&M.entryv, ilo, x)
		jhi++
		debugf_RangedMap_RebuildJob("\tpresplit copy %s\t-> %s\n", x, M)
	}
	if M.entryv[ilo].Lo < r.Lo { // shrink left
		M.entryv[ilo].Hi_ = r.Lo-1
		debugf_RangedMap_RebuildJob("\tshrink [%d] left \t-> %s\n", ilo, M)
		ilo++
	}
	if r.Hi_ < M.entryv[jhi-1].Hi_ { // shrink right
		M.entryv[jhi-1].Lo = r.Hi_+1
		debugf_RangedMap_RebuildJob("\tshrink [%d] right\t-> %s\n", jhi-1, M)
		jhi--
	}

	if (jhi - ilo) > 0 {
		vDeleteSlice__RangedMap_RebuildJob(&M.entryv, ilo,jhi)
		debugf_RangedMap_RebuildJob("\tdelete M[%d:%d]\t-> %s\n", ilo, jhi, M)
	}

	// done
	return ilo
}

// HasRange returns whether all keys from range r belong to the map.
func (M *_RangedMap_RebuildJob) HasRange(r blib.KeyRange) (yes bool) {
	if trace_RangedMap_RebuildJob {
		fmt.Printf("\n\nHasRange:\n")
		fmt.Printf("  M: %s\n", M)
		fmt.Printf("  r: %s\n", r)
		defer func() {
			fmt.Printf("->·: %v\n", yes)
		}()
	}

	M.verify()

	if r.Empty() {
		return true
	}

	// find first ilo: r.lo < [ilo].hi
	l := len(M.entryv)
	ilo := sort.Search(l, func(i int) bool {
		 return r.Lo <= M.entryv[i].Hi_
	})
	debugf_RangedMap_RebuildJob("\tilo: %d\n", ilo)

	if ilo == l { // not found
		return false
	}

	// scan right and verify that whole r is covered
	lo := r.Lo
	for {
		e := M.entryv[ilo]
		debugf_RangedMap_RebuildJob("\te: %s\ttocheck: %s\n", e, blib.KeyRange{lo, r.Hi_})

		if lo < e.Lo {
			return false // hole in coverage
		}
		if r.Hi_ <= e.Hi_ {
			return true  // reached full coverage
		}

		lo = e.Hi_
		if lo < KeyMax {
			lo++
		}

		ilo++
		if ilo == l {
			return false // r's right not fully covered
		}
	}
}

// IntersectsRange returns whether some keys from range r belong to the map.
func (M *_RangedMap_RebuildJob) IntersectsRange(r blib.KeyRange) (yes bool) {
	if trace_RangedMap_RebuildJob {
		fmt.Printf("\n\nIntersectsRange:\n")
		fmt.Printf("  M: %s\n", M)
		fmt.Printf("  r: %s\n", r)
		defer func() {
			fmt.Printf("->·: %v\n", yes)
		}()
	}

	M.verify()

	if r.Empty() {
		return false
	}

	// find first ilo: r.lo < [ilo].hi
	l := len(M.entryv)
	ilo := sort.Search(l, func(i int) bool {
		 return r.Lo <= M.entryv[i].Hi_
	})
	debugf_RangedMap_RebuildJob("\tilo: %d\n", ilo)

	if ilo == l { // not found
		return false
	}

	// [ilo].hi may be either inside r (≤ r.hi), or > r.hi
	// - if it is inside  -> overlap is there,
	// - if it is > r.hi  -> overlap is there if [ilo].lo < r.hi
	// => in any case overlap is there if [ilo].lo < r.hi
	return M.entryv[ilo].Lo <= r.Hi_
}


// --------

// verify checks _RangedMap_RebuildJob for internal consistency:
// - ranges must be not overlapping and ↑
// - adjacent ranges must map to different values
func (M *_RangedMap_RebuildJob) verify() {
	// TODO !debug -> return

	var badv []string
	badf := func(format string, argv ...interface{}) {
		badv = append(badv, fmt.Sprintf(format, argv...))
	}
	defer func() {
		if badv != nil {
			emsg := "M.verify: fail:\n\n"
			for _, bad := range badv {
				emsg += fmt.Sprintf("- %s\n", bad)
			}
			emsg += fmt.Sprintf("\nM: %s\n", M)
			panic(emsg)
		}
	}()

	hi_Prev := KeyMin
	var v_Prev *_RebuildJob
	for i, e := range M.entryv {
		hiPrev := hi_Prev + 1
		if i > 0 {
			if (e.Value == v_Prev) {
				if !(hiPrev < e.Lo) { // NOTE not ≤ - adjacent ranges must be merged
					badf("[%d]: same value: !(hiPrev < e.lo)", i)
				}
			} else {
				if !(hi_Prev <= e.Lo) {
					badf("[%d]: different value: !(hiPrev ≤ e.lo)", i)
				}
			}
		}
		if !(e.Lo <= e.Hi_) {
			badf("[%d]: !(e.lo ≤ e.hi_)", i)
		}
		hi_Prev = e.Hi_
		v_Prev  = e.Value
	}
}

// Clone returns copy of the map.
//
// NOTE values are _not_ cloned.
func (orig *_RangedMap_RebuildJob) Clone() *_RangedMap_RebuildJob {
	klon := &_RangedMap_RebuildJob{}
	klon.entryv = append(klon.entryv, orig.entryv...)
	return klon
}

// Empty returns whether the map is empty.
func (M *_RangedMap_RebuildJob) Empty() bool {
	return len(M.entryv) == 0
}

// Equal returns whether A == B.
func (A *_RangedMap_RebuildJob) Equal(B *_RangedMap_RebuildJob) bool {
	if len(A.entryv) != len(B.entryv) {
		return false
	}
	for i, ea := range A.entryv {
		eb := B.entryv[i]
		if ea != eb {
			return false
		}
	}
	return true
}

// Clear removes all elements from the map.
func (M *_RangedMap_RebuildJob) Clear() {
	M.entryv = nil
}

// AllRanges returns slice of all key ranges in the set.
//
// TODO -> iter?
func (M *_RangedMap_RebuildJob) AllRanges() /*readonly*/[]_RangedMap_RebuildJobEntry {
	return M.entryv
}

func (M _RangedMap_RebuildJob) String() string {
	s := "{"
	for i, e := range M.entryv {
		if i > 0 {
			s += " "
		}
		s += e.String()
	}
	s += "}"
	return s
}

func (e _RangedMap_RebuildJobEntry) String() string {
	s := e.KeyRange.String()
	v := fmt.Sprintf("%v", e.Value)
	if v != "" { // omit ":<v>" in the case of set
		s += ":" + v
	}
	return s
}


func debugf_RangedMap_RebuildJob(format string, argv ...interface{}) {
	if !debug_RangedMap_RebuildJob {
		return
	}
	fmt.Printf(format, argv...)
}


// ---- slice ops ----

// vInsert__RangedMap_RebuildJob inserts e into *pv[i].
func vInsert__RangedMap_RebuildJob(pv *[]_RangedMap_RebuildJobEntry, i int, e _RangedMap_RebuildJobEntry) {
	v := *pv
	v = append(v, _RangedMap_RebuildJobEntry{})
	copy(v[i+1:], v[i:])
	v[i] = e
	*pv = v
}

// vDeleteSlice__RangedMap_RebuildJob deletes *pv[lo:hi].
func vDeleteSlice__RangedMap_RebuildJob(pv *[]_RangedMap_RebuildJobEntry, lo,hi int) {
	v := *pv
	n := copy(v[lo:], v[hi:])
	v = v[:lo+n]
	*pv = v
}

// vReplaceSlice__RangedMap_RebuildJob replaces *pv[lo:hi] with e.
func vReplaceSlice__RangedMap_RebuildJob(pv *[]_RangedMap_RebuildJobEntry, lo,hi int, e _RangedMap_RebuildJobEntry) {
	v := *pv
	n := copy(v[lo+1:], v[hi:])
	v[lo] = e
	v = v[:lo+1+n]
	*pv = v
}