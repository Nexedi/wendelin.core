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

// Package xbtree complements package lab.nexedi.com/kirr/neo/go/zodb/btree.
//
// It provides the following amendments:
//
// - ΔBtail (tail of revisional changes to BTrees).
package xbtree

// this file contains only tree types and utilities.
// main code lives in δbtail.go and treediff.go .

import (
	"context"
	"fmt"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/set"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/blib"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xzodb"
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

// value is assumed to be persistent reference.
// deletion is represented as VDEL.
type Value  = zodb.Oid
const VDEL  = zodb.InvalidOid

type setKey = set.I64
type setOid = set.Oid
type setTid = set.Tid


// pathEqual returns whether two paths are the same.
func pathEqual(patha, pathb []zodb.Oid) bool {
	if len(patha) != len(pathb) {
		return false
	}
	for i, a := range patha {
		if pathb[i] != a {
			return false
		}
	}
	return true
}

// vnode returns brief human-readable representation of node.
func vnode(node Node) string {
	kind := "?"
	switch node.(type) {
	case *Tree:   kind = "T"
	case *Bucket: kind = "B"
	}
	return kind + node.POid().String()
}

// zgetNodeOrNil returns btree node corresponding to zconn.Get(oid) .
// if the node does not exist, (nil, ok) is returned.
func zgetNodeOrNil(ctx context.Context, zconn *zodb.Connection, oid zodb.Oid) (node Node, err error) {
	defer xerr.Contextf(&err, "getnode %s@%s", oid, zconn.At())
	xnode, err := xzodb.ZGetOrNil(ctx, zconn, oid)
	if xnode == nil || err != nil {
		return nil, err
	}

	node, ok := xnode.(Node)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %s", zodb.ClassOf(xnode))
	}
	return node, nil
}


func kstr(key Key) string {
	return blib.KStr(key)
}

func panicf(format string, argv ...interface{}) {
	panic(fmt.Sprintf(format, argv...))
}
