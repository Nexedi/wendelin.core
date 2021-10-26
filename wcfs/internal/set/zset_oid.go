// Code generated by gen-set Oid _Oid; DO NOT EDIT.

// Copyright (C) 2015-2021  Nexedi SA and Contributors.
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

package set

import (
	"fmt"
	"sort"
	"strings"
)

// Oid is a set of _Oid.
type Oid map[_Oid]struct{}

// Add adds v to the set.
func (s Oid) Add(v _Oid) {
	s[v] = struct{}{}
}

// Del removes v from the set.
// it is noop if v was not in the set.
func (s Oid) Del(v _Oid) {
	delete(s, v)
}

// Has checks whether the set contains v.
func (s Oid) Has(v _Oid) bool {
	_, ok := s[v]
	return ok
}

// Update adds t values to s.
func (s Oid) Update(t Oid) {
	for v := range t {
		s.Add(v)
	}
}

// Elements returns all elements of set as slice.
func (s Oid) Elements() []_Oid {
	ev := make([]_Oid, len(s))
	i := 0
	for e := range s {
		ev[i] = e
		i++
	}
	return ev
}

// Union returns s ∪ t
func (s Oid) Union(t Oid) Oid {
	// l = max(len(s), len(t))
	l := len(s)
	if lt := len(t); lt > l {
		l = lt
	}

	u := make(Oid, l)

	for v := range s {
		u.Add(v)
	}
	for v := range t {
		u.Add(v)
	}
	return u
}

// Intersection returns s ∩ t
func (s Oid) Intersection(t Oid) Oid {
	i := Oid{}
	for v := range s {
		if t.Has(v) {
			i.Add(v)
		}
	}
	return i
}

// Difference returns s\t.
func (s Oid) Difference(t Oid) Oid {
	d := Oid{}
	for v := range s {
		if !t.Has(v) {
			d.Add(v)
		}
	}
	return d
}

// SymmetricDifference returns s Δ t.
func (s Oid) SymmetricDifference(t Oid) Oid {
	d := Oid{}
	for v := range s {
		if !t.Has(v) {
			d.Add(v)
		}
	}
	for v := range t {
		if !s.Has(v) {
			d.Add(v)
		}
	}
	return d
}

// Equal returns whether a == b.
func (a Oid) Equal(b Oid) bool {
	if len(a) != len(b) {
		return false
	}

	for v := range a {
		_, ok := b[v]
		if !ok {
			return false
		}
	}

	return true
}

// Clone returns copy of the set.
func (orig Oid) Clone() Oid {
	klon := make(Oid, len(orig))
	for v := range orig {
		klon.Add(v)
	}
	return klon
}

// --------

func (s Oid) SortedElements() []_Oid {
	ev := s.Elements()
	sort.Slice(ev, func(i, j int) bool {
		return ev[i] < ev[j]
	})
	return ev
}

func (s Oid) String() string {
	ev := s.SortedElements()
	strv := make([]string, len(ev))
	for i, v := range ev {
		strv[i] = fmt.Sprintf("%v", v)
	}
	return "{" + strings.Join(strv, " ") + "}"
}
