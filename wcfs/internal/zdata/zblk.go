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

// Package zdata provides access for wendelin.core in-ZODB data.
//
// ZBlk* + ZBigFile.
package zdata

// module: "wendelin.bigfile.file_zodb"
//
// ZBigFile
//	.blksize	xint
//	.blktab		LOBtree{}  blk -> ZBlk*(blkdata)
//
// ZBlk0 (aliased as ZBlk)
//	str with trailing '\0' removed.
//
// ZBlk1
//	.chunktab	IOBtree{}  offset -> ZData(chunk)
//
// ZData
//	str (chunk)

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"syscall"

	"github.com/johncgriffin/overflow"
	pickle "github.com/kisielk/og-rek"
	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xsync"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/btree"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/pycompat"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xzodb"
)

// ZBlk is the interface that every ZBlk* block implements.
type ZBlk interface {
	zodb.IPersistent

	// LoadBlkData loads from database and returns data block stored by this ZBlk.
	//
	// If returned data size is less than the block size of containing ZBigFile,
	// the block trailing is assumed to be trailing \0.
	//
	// returns data and revision of ZBlk.
	LoadBlkData(ctx context.Context) (data []byte, rev zodb.Tid, _ error)
}

var _ ZBlk = (*ZBlk0)(nil)
var _ ZBlk = (*ZBlk1)(nil)

// ---- ZBlk0 ----

// ZBlk0 mimics ZBlk0 from python.
type ZBlk0 struct {
	zodb.Persistent

	// NOTE py source uses bytes(buf) but on python2 it still results in str
	blkdata string
}

type zBlk0State ZBlk0 // hide state methods from public API

// DropState implements zodb.Ghostable.
func (zb *zBlk0State) DropState() {
	zb.blkdata = ""
}

// PyGetState implements zodb.PyStateful.
func (zb *zBlk0State) PyGetState() interface{} {
	return zb.blkdata
}

// PySetState implements zodb.PyStateful.
func (zb *zBlk0State) PySetState(pystate interface{}) error {
	blkdata, ok := pystate.(string)
	if !ok {
		return fmt.Errorf("expect str; got %s", xzodb.TypeOf(pystate))
	}

	zb.blkdata = blkdata
	return nil
}

// LoadBlkData implements ZBlk.
func (zb *ZBlk0) LoadBlkData(ctx context.Context) (_ []byte, _ zodb.Tid, err error) {
	defer xerr.Contextf(&err, "ZBlk0(%s): loadBlkData", zb.POid())

	err = zb.PActivate(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer zb.PDeactivate()

	return mem.Bytes(zb.blkdata), zb.PSerial(), nil
}

// ---- ZBlk1 ---

// ZData mimics ZData from python.
type ZData struct {
	zodb.Persistent

	// NOTE py source uses bytes(buf) but on python2 it still results in str
	data string
}

type zDataState ZData // hide state methods from public API

// DropState implements zodb.Ghostable.
func (zd *zDataState) DropState() {
	zd.data = ""
}

// PyGetState implements zodb.PyStateful.
func (zd *zDataState) PyGetState() interface{} {
	return zd.data
}

// PySetState implements zodb.PyStateful.
func (zd *zDataState) PySetState(pystate interface{}) error {
	data, ok := pystate.(string)
	if !ok {
		return fmt.Errorf("expect str; got %s", xzodb.TypeOf(pystate))
	}

	zd.data = data
	return nil
}

// ZBlk1 mimics ZBlk1 from python.
type ZBlk1 struct {
	zodb.Persistent

	chunktab *btree.IOBTree // {} offset -> ZData(chunk)
}

type zBlk1State ZBlk1 // hide state methods from public API

// DropState implements zodb.Ghostable.
func (zb *zBlk1State) DropState() {
	zb.chunktab = nil
}

// PyGetState implements zodb.PyStateful.
func (zb *zBlk1State) PyGetState() interface{} {
	return zb.chunktab
}

// PySetState implements zodb.PyStateful.
func (zb *zBlk1State) PySetState(pystate interface{}) error {
	chunktab, ok := pystate.(*btree.IOBTree)
	if !ok {
		return fmt.Errorf("expect IOBTree; got %s", xzodb.TypeOf(pystate))
	}

	zb.chunktab = chunktab
	return nil
}

// LoadBlkData implements ZBlk.
func (zb *ZBlk1) LoadBlkData(ctx context.Context) (_ []byte, _ zodb.Tid, err error) {
	defer xerr.Contextf(&err, "ZBlk1(%s): loadBlkData", zb.POid())

	err = zb.PActivate(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer zb.PDeactivate()

	// get to all ZData objects; activate them and build
	//
	//	{} offset -> ZData
	//
	// with all ZData being live.
	var mu sync.Mutex
	chunktab := make(map[int32]*ZData)

	// on return deactivate all ZData objects loaded in chunktab
	defer func() {
		for _, zd := range chunktab {
			zd.PDeactivate()
		}
	}()


	wg := xsync.NewWorkGroup(ctx)

	// loadZData loads 1 ZData object into chunktab and leaves it activated.
	loadZData := func(ctx context.Context, offset int32, zd *ZData) (err error) {
		err = zd.PActivate(ctx)
		if err != nil {
			return err
		}
		// no PDeactivate, zd remains live

		//fmt.Printf("@%d -> zdata #%s (%d)\n", offset, zd.POid(), len(zd.data))
		mu.Lock()
		defer mu.Unlock()

		_, already := chunktab[offset]
		if already {
			return fmt.Errorf("duplicate offset %d", offset)
		}

		chunktab[offset] = zd
		return nil
	}

	// loadBucket loads all ZData objects from leaf BTree bucket.
	loadBucket := func(ctx context.Context, b *btree.IOBucket) (err error) {
		err = b.PActivate(ctx)
		if err != nil {
			return err
		}
		defer b.PDeactivate()

		// go through all bucket key/v -> chunktab
		defer xerr.Contextf(&err, "%s(%s)", xzodb.TypeOf(b), b.POid())

		//fmt.Printf("\nbucket: %v\n\n", b.Entryv())
		for i, e := range b.Entryv() {
			zd, ok := e.Value().(*ZData)
			if !ok {
				return fmt.Errorf("[%d]: !ZData (%s)", i, xzodb.TypeOf(e.Value()))
			}

			offset := e.Key()
			if offset < 0 {
				return fmt.Errorf("[%d]: offset < 0 (%d)", i, offset)
			}
			wg.Go(func(ctx context.Context) error {
				return loadZData(ctx, offset, zd)
			})
		}

		return nil
	}

	// loadBTree spawns loading of all BTree children.
	var loadBTree func(ctx context.Context, t *btree.IOBTree) error
	loadBTree = func(ctx context.Context, t *btree.IOBTree) error {
		err := t.PActivate(ctx)
		if err != nil {
			return err
		}
		defer t.PDeactivate()

		//fmt.Printf("\nbtree: %v\n\n", t.Entryv())
		for _, e := range t.Entryv() {
			switch child := e.Child().(type) {
			case *btree.IOBTree:
				wg.Go(func(ctx context.Context) error {
					return loadBTree(ctx, child)
				})

			case *btree.IOBucket:
				wg.Go(func(ctx context.Context) error {
					return loadBucket(ctx, child)
				})

			default:
				panicf("IOBTree has %s child", xzodb.TypeOf(child))
			}
		}

		return nil
	}

	wg.Go(func(ctx context.Context) error {
		return loadBTree(ctx, zb.chunktab)
	})

	err = wg.Wait()
	if err != nil {
		return nil, 0, err
	}

	// empty .chunktab -> ø
	if len(chunktab) == 0 {
		return nil, 0, nil
	}

	// glue all chunks from chunktab
	offv := make([]int32, 0, len(chunktab)) // ↑
	for off := range chunktab {
		offv = append(offv, off)
	}
	sort.Slice(offv, func(i, j int) bool {
		return offv[i] < offv[j]
	})

	//fmt.Printf("#chunktab: %d\n", len(chunktab))
	//fmt.Printf("offv: %v\n", offv)


	// find out whole blk len via inspecting tail chunk
	tailStart  := offv[len(offv)-1]
	tailChunk  := chunktab[tailStart]
	blklen, ok := overflow.Add32(tailStart, int32(len(tailChunk.data)))
	if !ok {
		return nil, 0, fmt.Errorf("invalid data: blklen overflow")
	}

	// whole buffer initialized as 0 + tail_chunk
	blkdata := make([]byte, blklen)
	copy(blkdata[tailStart:], tailChunk.data)

	// go through all chunks besides tail and extract them
	stop := int32(0)
	for _, start := range offv[:len(offv)-1] {
		chunk := chunktab[start]
		if !(start >= stop) { // verify chunks don't overlap
			return nil, 0, fmt.Errorf("invalid data: chunks overlap")
		}
		stop, ok = overflow.Add32(start, int32(len(chunk.data)))
		if !(ok && stop <= blklen) {
			return nil, 0, fmt.Errorf("invalid data: blkdata overrun")
		}
		copy(blkdata[start:], chunk.data)
	}

	return blkdata, zb.PSerial(), nil
}


// ----------------------------------------

// ZBigFile mimics ZBigFile from python.
type ZBigFile struct {
	zodb.Persistent

	// state: (.blksize, .blktab)
	blksize int64
	blktab  *btree.LOBTree // {}  blk -> ZBlk*(blkdata)
}

type zBigFileState ZBigFile // hide state methods from public API

// DropState implements zodb.Ghostable.
func (bf *zBigFileState) DropState() {
	bf.blksize = 0
	bf.blktab  = nil
}

// PyGetState implements zodb.PyStateful.
func (bf *zBigFileState) PyGetState() interface{} {
	return pickle.Tuple{bf.blksize, bf.blktab}
}

// PySetState implements zodb.PyStateful.
func (bf *zBigFileState) PySetState(pystate interface{}) (err error) {
	t, ok := pystate.(pickle.Tuple)
	if !ok {
		return fmt.Errorf("expect [2](); got %s", xzodb.TypeOf(pystate))
	}
	if len(t) != 2 {
		return fmt.Errorf("expect [2](); got [%d]()", len(t))
	}

	blksize, ok := pycompat.Int64(t[0])
	if !ok {
		return fmt.Errorf("blksize: expect integer; got %s", xzodb.TypeOf(t[0]))
	}
	if blksize <= 0 {
		return fmt.Errorf("blksize: must be > 0; got %d", blksize)
	}

	blktab, err := vBlktab(t[1])
	if err != nil {
		return err
	}

	bf.blksize = blksize
	bf.blktab  = blktab
	return nil
}

// BlkSize returns size of block used by this file.
//
// The file must be activated.
func (bf *ZBigFile) BlkSize() int64 {
	return bf.blksize
}

// LoadBlk loads data for file block #blk.
//
// it also returns:
//
//	- BTree path in .blktab to loaded block,
//	- blocks covered by leaf node in the BTree path,
//	- max(_.serial for _ in ZBlk(#blk), all BTree/Bucket that lead to ZBlk)
//	  which provides a rough upper-bound estimate for file[blk] revision.
//
// TODO load into user-provided buf.
func (bf *ZBigFile) LoadBlk(ctx context.Context, blk int64) (_ []byte, treePath []btree.LONode, blkCov btree.LKeyRange, zblk ZBlk, blkRevMax zodb.Tid, err error) {
	defer xerr.Contextf(&err, "bigfile %s: loadblk %d", bf.POid(), blk)
	kø := btree.LKeyRange{Lo: 0, Hi_: -1} // empty KeyRange

	err = bf.PActivate(ctx)
	if err != nil {
		return nil, nil, kø, nil, 0, err
	}
	defer bf.PDeactivate()

	blkRevMax = 0
	xzblk, ok, err := bf.blktab.VGet(ctx, blk, func(node btree.LONode, keycov btree.LKeyRange) {
		treePath = append(treePath, node)
		blkCov = keycov // will be set last for leaf
		blkRevMax = tidmax(blkRevMax, node.PSerial())
	})
	if err != nil {
		return nil, nil, kø, nil, 0, err
	}
	if !ok {
		return make([]byte, bf.blksize), treePath, blkCov, nil, blkRevMax, nil
	}

	zblk, err = vZBlk(xzblk)
	if err != nil {
		return nil, nil, kø, nil, 0, err
	}

	blkdata, zblkrev, err := zblk.LoadBlkData(ctx)
	if err != nil {
		return nil, nil, kø, nil, 0, err
	}
	blkRevMax = tidmax(blkRevMax, zblkrev)

	l := int64(len(blkdata))
	if l > bf.blksize {
		return nil, nil, kø, nil, 0, fmt.Errorf("zblk %s: invalid blk: size = %d (> blksize = %d)", zblk.POid(), l, bf.blksize)
	}

	// append trailing \0 to data to reach .blksize
	if l < bf.blksize {
		d := make([]byte, bf.blksize)
		copy(d, blkdata)
		blkdata = d
	}

	return blkdata, treePath, blkCov, zblk, blkRevMax, nil
}

// Size returns whole file size.
//
// it also returns BTree path scanned to obtain the size.
func (bf *ZBigFile) Size(ctx context.Context) (_ int64, treePath []btree.LONode, blkCov btree.LKeyRange, err error) {
	defer xerr.Contextf(&err, "bigfile %s: size", bf.POid())
	kø := btree.LKeyRange{Lo: 0, Hi_: -1} // empty KeyRange

	err = bf.PActivate(ctx)
	if err != nil {
		return 0, nil, kø, err
	}
	defer bf.PDeactivate()

	tailblk, ok, err := bf.blktab.VMaxKey(ctx, func(node btree.LONode, keycov btree.LKeyRange) {
		treePath = append(treePath, node)
		blkCov = keycov // will be set last for leaf
	})
	if err != nil {
		return 0, nil, kø, err
	}
	if !ok {
		return 0, treePath, blkCov, nil
	}

	size := (tailblk + 1) * bf.blksize
	if size / bf.blksize != tailblk + 1 {
		return 0, nil, kø, syscall.EFBIG // overflow
	}

	return size, treePath, blkCov, nil
}

// vZBlk checks and converts xzblk to a ZBlk object.
func vZBlk(xzblk interface{}) (ZBlk, error) {
	zblk, ok := xzblk.(ZBlk)
	if !ok {
		return nil, fmt.Errorf("expect ZBlk*; got %s", xzodb.TypeOf(xzblk))
	}
	return zblk, nil
}

// vBlktab checks and converts xblktab to LOBTree object.
func vBlktab(xblktab interface{}) (*btree.LOBTree, error) {
	blktab, ok := xblktab.(*btree.LOBTree)
	if !ok {
		return nil, fmt.Errorf("blktab: expect LOBTree; got %s", xzodb.TypeOf(xblktab))
	}
	return blktab, nil
}

// ----------------------------------------

// module of Wendelin ZODB py objects
const zwendelin = "wendelin.bigfile.file_zodb"

func init() {
	t := reflect.TypeOf
	zodb.RegisterClass(zwendelin + ".ZBlk0",    t(ZBlk0{}),    t(zBlk0State{}))
	zodb.RegisterClass(zwendelin + ".ZBlk1",    t(ZBlk1{}),    t(zBlk1State{}))
	zodb.RegisterClass(zwendelin + ".ZData",    t(ZData{}),    t(zDataState{}))
	zodb.RegisterClass(zwendelin + ".ZBigFile", t(ZBigFile{}), t(zBigFileState{}))

	// backward compatibility
	zodb.RegisterClassAlias(zwendelin + ".ZBlk", zwendelin + ".ZBlk0")
}
