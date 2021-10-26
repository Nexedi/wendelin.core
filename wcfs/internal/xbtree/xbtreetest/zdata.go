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
// access to ZBlk data

import (
	"context"

	"lab.nexedi.com/kirr/go123/exc"

	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"
)

// ZBlk-related functions are imported at runtime by package xbtreetest/init
var (
	ZGetBlkData func(context.Context, *zodb.Connection, zodb.Oid) (string, error)
)

func zassertInitDone() {
	if ZGetBlkData == nil {
		panic("xbtreetest/zdata not initialized -> import xbtreetest/init to fix")
	}
}

// xzgetBlkData loads block data from ZBlk object specified by its oid.
func xzgetBlkData(ctx context.Context, zconn *zodb.Connection, zblkOid zodb.Oid) string {
	zassertInitDone()
	X := exc.Raiseif

	if zblkOid == VDEL {
		return DEL
	}

	data, err := ZGetBlkData(ctx, zconn, zblkOid); X(err)
	return string(data)
}
