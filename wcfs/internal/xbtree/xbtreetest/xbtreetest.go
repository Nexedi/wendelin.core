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

// Package xbtreetest provides infrastructure for testing LOBTree with ZBlk values.
package xbtreetest

import (
	"fmt"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/blib"
)

// XXX instead of generics
type Tree   = blib.Tree
type Bucket = blib.Bucket
type Node   = blib.Node
type TreeEntry   = blib.TreeEntry
type BucketEntry = blib.BucketEntry

type Key      = blib.Key
type KeyRange = blib.KeyRange
const KeyMax  = blib.KeyMax
const KeyMin  = blib.KeyMin


func panicf(format string, argv ...interface{}) {
	panic(fmt.Sprintf(format, argv...))
}
