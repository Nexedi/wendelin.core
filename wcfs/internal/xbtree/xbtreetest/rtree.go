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

package xbtreetest
// RBucket + RBucketSet

import (
	"fmt"
	"sort"

	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/blib"
)

// RBucketSet represents set of buckets covering whole [-∞,∞) range.
type RBucketSet []*RBucket // k↑

// RBucket represents Bucket node with values covering [lo, hi_] key range in its Tree.
// NOTE it is not [lo,hi) but [lo,hi_] instead to avoid overflow at KeyMax.
type RBucket struct {
	Oid     zodb.Oid
	Parent  *RTree
	Keycov  blib.KeyRange
	KV      map[Key]string // bucket's k->v; values were ZBlk objects whose data is loaded instead.
}

// RTree represents Tree node that RBucket refers to as parent.
type RTree struct {
	Oid    zodb.Oid
	Parent *RTree
}


// Path returns path to this bucket from tree root.
func (rb *RBucket) Path() []zodb.Oid {
	path := []zodb.Oid{rb.Oid}
	p := rb.Parent
	for p != nil {
		path = append([]zodb.Oid{p.Oid}, path...)
		p = p.Parent
	}
	return path
}

// Get returns RBucket which covers key k.
func (rbs RBucketSet) Get(k Key) *RBucket {
	i := sort.Search(len(rbs), func(i int) bool {
		return k <= rbs[i].Keycov.Hi_
	})
	if i == len(rbs) {
		panicf("BUG: key %v not covered;  coverage: %s", k, rbs.coverage())
	}

	rb := rbs[i]
	if !rb.Keycov.Has(k) {
		panicf("BUG: get(%v) -> %s;  coverage: %s", k, rb.Keycov, rbs.coverage())
	}

	return rb
}

// coverage returns string representation of rbs coverage structure.
func (rbs RBucketSet) coverage() string {
	if len(rbs) == 0 {
		return "ø"
	}
	s := ""
	for _, rb := range rbs {
		if s != "" {
			s += " "
		}
		s += fmt.Sprintf("%s", rb.Keycov)
	}
	return s
}

// Flatten converts xkv with bucket structure into regular dict.
func (xkv RBucketSet) Flatten() map[Key]string {
	kv := make(map[Key]string)
	for _, b := range xkv {
		for k,v := range b.KV {
			kv[k] = v
		}
	}
	return kv
}

func (b *RBucket) String() string {
	return fmt.Sprintf("%sB%s{%s}", b.Keycov, b.Oid, KVTxt(b.KV))
}
