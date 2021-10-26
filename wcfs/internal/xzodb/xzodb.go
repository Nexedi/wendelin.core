// Copyright (C) 2018-2021  Nexedi SA and Contributors.
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

// Package xzodb compements package zodb.
package xzodb

import (
	"context"
	"fmt"

	"lab.nexedi.com/kirr/go123/xcontext"

	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"
)


// TypeOf returns string for object's type.
//
//	- for ZODB objects, it uses zodb.ClassOf, which in particular supports
//	  printing zodb.Broken with details properly.
//
//	- for other objects, it uses %T.
func TypeOf(obj interface{}) string {
	switch obj := obj.(type) {
	case zodb.IPersistent:
		return zodb.ClassOf(obj)

	default:
		return fmt.Sprintf("%T", obj)
	}
}

// ZConn is zodb.Connection + associated read-only transaction under which
// objects of the connection are accessed.
type ZConn struct {
	*zodb.Connection

	// read-only transaction under which we access zodb.Connection data.
	TxnCtx context.Context // XXX -> better directly store txn
}

// ZOpen opens new connection to ZODB database + associated read-only transaction.
func ZOpen(ctx context.Context, zdb *zodb.DB, zopt *zodb.ConnOptions) (_ *ZConn, err error) {
	// create new read-only transaction
	txn, txnCtx := transaction.New(context.Background())
	defer func() {
		if err != nil {
			txn.Abort()
		}
	}()

	// XXX better ctx = transaction.PutIntoContext(ctx, txn)
	ctx, cancel := xcontext.Merge(ctx, txnCtx)
	defer cancel()

	zconn, err := zdb.Open(ctx, zopt)
	if err != nil {
		return nil, err
	}

	return &ZConn{
		Connection: zconn,
		TxnCtx:     txnCtx,
	}, nil
}
