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

// Package xbtreetest/init (ex imported from package A_test) should be imported
// in addition to xbtreetest (ex imported from package A) to initialize
// xbtreetest at runtime.
package init

// ZBlk-related part of xbtreetest

import (
	"context"
	"fmt"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/xbtreetest"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xzodb"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/zdata"
)

type Tree     = xbtreetest.Tree
type Node     = xbtreetest.Node
type Key      = xbtreetest.Key
type KeyRange = xbtreetest.KeyRange

type ZBlk = zdata.ZBlk


func init() {
	xbtreetest.ZGetBlkData = _ZGetBlkData
}


// _ZGetBlkData loads block data from ZBlk object specified by its oid.
func _ZGetBlkData(ctx context.Context, zconn *zodb.Connection, zblkOid zodb.Oid) (data string, err error) {
	defer xerr.Contextf(&err, "@%s: get blkdata from obj %s", zconn.At(), zblkOid)

	xblk, err := zconn.Get(ctx, zblkOid)
	if err != nil {
		return "", err
	}
	zblk, ok := xblk.(ZBlk)
	if !ok {
		return "", fmt.Errorf("expect ZBlk*; got %s", xzodb.TypeOf(xblk))
	}
	bdata, _, err := zblk.LoadBlkData(ctx)
	if err != nil {
		return "", err
	}
	return string(bdata), nil
}
