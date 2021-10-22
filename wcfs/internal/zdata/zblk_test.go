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

package zdata

//go:generate ./testdata/zblk_test_gen.py

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"

	"github.com/stretchr/testify/require"
)

// TestZBlk verifies that ZBlk* and ZBigFile saved by Python can be read correctly by Go.
// TODO also test with data saved by Python3.
func TestZBlk(t *testing.T) {
	X := exc.Raiseif
	assert := require.New(t)
	ctx := context.Background()
	stor, err := zodb.Open(ctx, "testdata/zblk.fs", &zodb.OpenOptions{ReadOnly: true}); X(err)
	db := zodb.NewDB(stor, &zodb.DBOptions{})
	defer func() {
		err := db.Close();	X(err)
		err  = stor.Close();	X(err)
	}()

	txn, ctx := transaction.New(ctx)
	defer txn.Abort()

	conn, err := db.Open(ctx, &zodb.ConnOptions{});	X(err)

	xz0, err := conn.Get(ctx, z0_oid);	X(err)
	xz1, err := conn.Get(ctx, z1_oid);	X(err)
	xzf, err := conn.Get(ctx, zf_oid);	X(err)

	z0, ok := xz0.(*ZBlk0)
	if !ok {
		t.Fatalf("z0: want ZBlk0; got %T", xz0)
	}

	z1, ok := xz1.(*ZBlk1)
	if !ok {
		t.Fatalf("z1: want ZBlk1; got %T", xz1)
	}

	zf, ok := xzf.(*ZBigFile)
	if !ok {
		t.Fatalf("zf: want ZBigFile; got %T", xzf)
	}

	xactivate := func(obj zodb.IPersistent) {
		t.Helper()
		err := obj.PActivate(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}


	z0Data, z0Rev, err := z0.LoadBlkData(ctx);	X(err)
	z0DataOK := brange32(z0_len)
	assert.Equal(z0Data, z0DataOK,   "ZBlk0 data wrong")
	assert.Equal(z0Rev,  z0_rev,	 "ZBlk0 rev wrong")

	z1Data, z1Rev, err := z1.LoadBlkData(ctx);	X(err)
	z1DataOK := make([]byte, zf_blksize)                                  // zeros
	copy(z1DataOK[0:],                      brange32(z1_htlen))           // head
	copy(z1DataOK[len(z1DataOK)-z1_htlen:], breverse(brange32(z1_htlen))) // tail
	z1DataOK = bytes.TrimRight(z1DataOK, "\x00") // trailing 0 are not persisted
	assert.Equal(z1Data, z1DataOK,   "ZBlk1 data wrong")
	assert.Equal(z1Rev,  z1_rev,     "ZBlk1 rev wrong")


	xactivate(zf)
	if zf.blksize != zf_blksize {
		t.Fatalf("zf: blksize=%d;  want %d", zf.blksize, zf_blksize)
	}

	z0_, ok, err := zf.blktab.Get(ctx, 1);	X(err)
	if !(ok && z0_ == z0) {
		t.Fatalf("zf: [0] -> %#v; want z0", z0_)
	}

	z1_, ok, err := zf.blktab.Get(ctx, 3);	X(err)
	if !(ok && z1_ == z1) {
		t.Fatalf("zf: [1] -> %#v; want z1", z1_)
	}

	size, _, _, err := zf.Size(ctx);	X(err)
	assert.Equal(size, int64(zf_size),	"ZBigFile size wrong")


	// LoadBlk
	z0Data, _, _, _, _, err = zf.LoadBlk(ctx, 1);	X(err)
	assert.Equal(len(z0Data), int(zf.blksize))
	z0Data = bytes.TrimRight(z0Data, "\x00")
	assert.Equal(z0Data, z0DataOK)

	z1Data, _, _, _, _, err = zf.LoadBlk(ctx, 3);	X(err)
	assert.Equal(len(z1Data), int(zf.blksize))
	z1Data = bytes.TrimRight(z1Data, "\x00")
	assert.Equal(z1Data, z1DataOK)
}

// TODO verify PyGetState vs PySetState

// brange32 returns bytes with big-endian uint32 sequence filling them.
// returned bytes has len == size.
func brange32(size int) []byte {
	data := make([]byte, size)
	for i := 0; i < size / 4; i++ {
		binary.BigEndian.PutUint32(data[i*4:], uint32(i))
	}
	return data
}

// breverse returns bytes in the reverse order.
func breverse(b []byte) []byte {
	r := make([]byte, len(b))
	for i := range(b) {
		r[i] = b[len(b)-i-1]
	}
	return r
}
