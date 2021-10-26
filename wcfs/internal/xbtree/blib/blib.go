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

// Package blib provides utilities related to BTrees.
package blib

import (
	"fmt"
	"math"

	"lab.nexedi.com/kirr/neo/go/zodb/btree"
)

// XXX instead of generics
type Tree   = btree.LOBTree
type Bucket = btree.LOBucket
type Node   = btree.LONode
type TreeEntry   = btree.LOEntry
type BucketEntry = btree.LOBucketEntry

type Key         = int64
type KeyRange    = btree.LKeyRange
const KeyMax Key = math.MaxInt64
const KeyMin Key = math.MinInt64


// KStr formats key as string.
func KStr(k Key) string {
	if k == KeyMin {
		return "-∞"
	}
	if k == KeyMax {
		return "∞"
	}
	return fmt.Sprintf("%d", k)
}
