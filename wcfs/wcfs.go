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

// Program wcfs provides filesystem server with file data backed by wendelin.core arrays.
//
// Intro
//
// Each wendelin.core array (ZBigArray) is actually a linear file (ZBigFile)
// and array metadata like dtype, shape and strides associated with it. This
// program exposes as files only ZBigFile data and leaves rest of
// array-specific handling to clients. Every ZBigFile is exposed as one separate
// file that represents whole ZBigFile's data.
//
// For a client, the primary way to access a bigfile should be to mmap
// head/bigfile/<bigfileX> which represents always latest bigfile data.
//
// In the usual situation when bigfiles are big
// there should be no need for any cache besides shared kernel cache of latest
// bigfile data.
//
//
// Filesystem organization
//
// Top-level structure of provided filesystem is as follows:
//
//	head/			; latest database view
//		...
//	@<rev1>/		; database view as of revision <revX>
//		...
//	@<rev2>/
//		...
//	...
//
// where head/ represents latest data as stored in upstream ZODB, and
// @<revX>/ represents data as of database revision <revX>.
//
// head/ has the following structure:
//
//	head/
//		at			; data inside head/ is as of this ZODB transaction
//		bigfile/		; bigfiles' data
//			<oid(ZBigFile1)>
//			<oid(ZBigFile2)>
//			...
//
// where /bigfile/<bigfileX> represents latest bigfile data as stored in
// upstream ZODB.
// /at describes precisely ZODB state for which bigfile data is currently
// exposed.
//
// @<revX>/ has the following structure:
//
//	@<revX>/
//		bigfile/		; bigfiles' data as of revision <revX>
//			<oid(ZBigFile1)>
//			<oid(ZBigFile2)>
//			...
//
// where /bigfile/<bigfileX> represent bigfile data as of revision <revX>.
//
// Unless accessed {head,@<revX>}/bigfile/<bigfileX> are not automatically visible in
// wcfs filesystem. Similarly @<revX>/ become visible only after access.
//
//
// Writes
//
// As each bigfile is represented by 1 synthetic file, there can be several
// write schemes:
//
// 1. mmap(MAP_PRIVATE) + writeout by client
//
// In this scheme bigfile data is mmapped in MAP_PRIVATE mode, so that local
// user changes are not automatically propagated back to the file. When there
// is a need to commit, client investigates via some OS mechanism, e.g.
// /proc/self/pagemap or something similar, which pages of this mapping it
// modified. Knowing this it knows which data it dirtied and so can write this
// data back to ZODB itself, without filesystem server providing write support.
//
// 2. mmap(MAP_SHARED, PROT_READ) + write-tracking & writeout by client
//
// In this scheme bigfile data is mmaped in MAP_SHARED mode with read-only pages
// protection. Then whenever write fault occurs, client allocates RAM from
// shmfs, copies faulted page to it, and then mmaps RAM page with RW protection
// in place of original bigfile page. Writeout implementation should be similar
// to "1", only here client already knows the pages it dirtied, and this way
// there is no need to consult /proc/self/pagemap.
//
// The advantage of this scheme over mmap(MAP_PRIVATE) is that in case
// there are several in-process mappings of the same bigfile with overlapping
// in-file ranges, changes in one mapping will be visible in another mapping.
// Contrary: whenever a MAP_PRIVATE mapping is modified, the kernel COWs
// faulted page into a page completely private to this mapping, so that other
// MAP_PRIVATE mappings of this file, including ones created from the same
// process, do not see changes made to the first mapping.
//
// Since wendelin.core needs to provide coherency in between different slices
// of the same array, this is the mode wendelin.core will actually use.
//
// 3. write to wcfs
//
// TODO we later could implement "write-directly" mode where clients would write
// data directly into the file.
package main

// Wcfs organization
//
// Wcfs is a ZODB client that translates ZODB objects into OS files as would
// non-wcfs wendelin.core do for a ZBigFile.
// It is organized as follows:
//
// 1) 1 ZODB connection for "latest data" for whole filesystem (zhead).
// 2) head/bigfile/* of all bigfiles represent state as of zhead.At .
// 7) when we receive a FUSE read(#blk) request to a head/bigfile/file, we process it as follows:
//
//   7.1) load blkdata for head/bigfile/file[blk] @zhead.at .
//   7.3) blkdata is returned to kernel.
//
// 8) serving FUSE reads from @<rev>/bigfile/file is organized similarly to
//    serving reads from head/bigfile/file, but with using dedicated per-<rev>
//    ZODB connection and without notifying any watches.
//
// 9) for every ZODB connection (zhead + one per @<rev>) a dedicated read-only
//    transaction is maintained.
//
// TODO 10) gc @rev/ and @rev/bigfile/<bigfileX> automatically on atime timeout


// Notation used
//
// f     - BigFile
// bfdir - BigFileDir

import (
	"context"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"

	log "github.com/golang/glog"

	"lab.nexedi.com/kirr/go123/xcontext"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xruntime/race"
	"lab.nexedi.com/kirr/go123/xsync"

	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/btree"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"

	"github.com/johncgriffin/overflow"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/pkg/errors"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xzodb"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/zdata"
)

// shorthands for ZBigFile and ZBlk*
type (
	ZBlk     = zdata.ZBlk
	ZBlk0    = zdata.ZBlk0
	ZBlk1    = zdata.ZBlk1
	ZData    = zdata.ZData
	ZBigFile = zdata.ZBigFile
)


// Root represents root of wcfs filesystem.
type Root struct {
	fsNode

	// ZODB storage we work with
	zstor zodb.IStorage

	// ZODB DB handle for zstor.
	// keeps cache of connections for @<rev>/ accesses.
	// only one connection is used for each @<rev>.
	zdb *zodb.DB

	// directory + ZODB connection for head/
	head *Head

	// directories + ZODB connections for @<rev>/
	revMu  sync.Mutex
	revTab map[zodb.Tid]*Head
}

// /(head|<rev>)/			- served by Head.
type Head struct {
	fsNode

	rev   zodb.Tid    // 0 for head/, !0 for @<rev>/
	bfdir *BigFileDir // bigfile/
	// at    - served by .readAt

	// ZODB connection for everything under this head
	zconn   *xzodb.ZConn
}

// /(head|<rev>)/bigfile/		- served by BigFileDir.
type BigFileDir struct {
	fsNode
	head *Head // parent head/ or @<rev>/

	// {} oid -> <bigfileX>
	fileMu  sync.Mutex
	fileTab map[zodb.Oid]*BigFile
}

// /(head|<rev>)/bigfile/<bigfileX>	- served by BigFile.
type BigFile struct {
	fsNode

	// this BigFile is under .head/bigfile/; it views ZODB via .head.zconn
	// parent's BigFileDir.head is the same.
	head	*Head

	// ZBigFile top-level object
	zfile	*ZBigFile

	// things read/computed from .zfile; constant during lifetime of current transaction.
	// i.e. changed under zhead.W
	blksize   int64    // zfile.blksize
	size      int64    // zfile.Size()
	revApprox zodb.Tid // approx last revision that modified zfile data

	// inflight loadings of ZBigFile from ZODB.
	// successful load results are kept here until blkdata is put into OS pagecache.
	//
	// Being a staging area for data to enter OS cache, loading has to be
	// consulted/invalidated whenever wcfs logic needs to consult/invalidate OS cache.
	loadMu  sync.Mutex
	loading map[int64]*blkLoadState // #blk -> {... blkdata}
}

// blkLoadState represents a ZBlk load state/result.
//
// when !ready the loading is in progress.
// when ready  the loading has been completed.
type blkLoadState struct {
	ready chan struct{}

	blkdata  []byte
	err      error
}

// -------- ZODB cache control --------

// zodbCacheControl implements zodb.LiveCacheControl to tune ZODB to never evict
// LOBTree/LOBucket from live cache. We want to keep LOBTree/LOBucket always alive
// because it is essentially the index where to find ZBigFile data.
//
// For the data itself - we put it to kernel pagecache and always deactivate
// from ZODB right after that.
type zodbCacheControl struct {}

func (_ *zodbCacheControl) PCacheClassify(obj zodb.IPersistent) zodb.PCachePolicy {
	switch obj.(type) {
	// don't let ZBlk*/ZData to pollute the cache
	case *ZBlk0:
		return zodb.PCacheDropObject | zodb.PCacheDropState
	case *ZBlk1:
		return zodb.PCacheDropObject | zodb.PCacheDropState
	case *ZData:
		return zodb.PCacheDropObject | zodb.PCacheDropState

	// keep ZBigFile and its btree index in cache to speedup file data access.
	//
	// ZBigFile is top-level object that is used on every block load, and
	// it would be a waste to evict ZBigFile from cache.
	case *ZBigFile:
		return zodb.PCachePinObject | zodb.PCacheKeepState
	case *btree.LOBTree:
		return zodb.PCachePinObject | zodb.PCacheKeepState
	case *btree.LOBucket:
		return zodb.PCachePinObject | zodb.PCacheKeepState
	}

	return 0
}


// -------- 7) FUSE read(#blk) --------

// /(head|<rev>)/bigfile/<bigfileX> -> Read serves reading bigfile data.
func (f *BigFile) Read(_ nodefs.File, dest []byte, off int64, fctx *fuse.Context) (fuse.ReadResult, fuse.Status) {
	// cap read request to file size
	end, ok := overflow.Add64(off, int64(len(dest)))
	if !ok {
		end = math.MaxInt64 // cap read request till max possible file size
	}
	if end > f.size {
		end = f.size
	}
	if end <= off {
		// the kernel issues e.g. [0 +4K) read for f.size=0 and expects to get (0, ok)
		// POSIX also says to return 0 if off >= f.size
		return fuse.ReadResultData(nil), fuse.OK
	}

	// widen read request to be aligned with blksize granularity
	// (we can load only whole ZBlk* blocks)
	aoff := off - (off % f.blksize)
	aend := end
	if re := end % f.blksize; re != 0 {
		aend += f.blksize - re
	}
	// TODO use original dest if it can fit the data
	dest = make([]byte, aend - aoff) // ~> [aoff:aend) in file

	// TODO better ctx = transaction.PutIntoContext(ctx, txn)
	ctx, cancel := xcontext.Merge(fctx, f.head.zconn.TxnCtx)
	defer cancel()

	// read/load all block(s) in parallel
	wg := xsync.NewWorkGroup(ctx)
	for blkoff := aoff; blkoff < aend; blkoff += f.blksize {
		blkoff := blkoff
		blk := blkoff / f.blksize
		wg.Go(func(ctx context.Context) error {
			δ := blkoff-aoff // blk position in dest
			//log.Infof("readBlk #%d dest[%d:+%d]", blk, δ, f.blksize)
			return f.readBlk(ctx, blk, dest[δ:δ+f.blksize])
		})
	}

	err := wg.Wait()
	if err != nil {
		return nil, err2LogStatus(err)
	}

	return fuse.ReadResultData(dest[off-aoff:end-aoff]), fuse.OK
}

// readBlk serves Read to read 1 ZBlk #blk into destination buffer.
//
// see "7) when we receive a FUSE read(#blk) request ..." in overview.
//
// len(dest) == blksize.
func (f *BigFile) readBlk(ctx context.Context, blk int64, dest []byte) (err error) {
	defer xerr.Contextf(&err, "%s: readblk #%d", f.path(), blk)

	// check if someone else is already loading this block
	f.loadMu.Lock()
	loading, already := f.loading[blk]
	if !already {
		loading = &blkLoadState{
			ready:   make(chan struct{}),
		}
		f.loading[blk] = loading
	}
	f.loadMu.Unlock()

	// if it is already loading - just wait for it
	if already {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-loading.ready:
			if loading.err == nil {
				copy(dest, loading.blkdata) // TODO avoid copy
			}
			return loading.err
		}
	}

	// noone was loading - we became responsible to load this block
	blkdata, _, _, _, _, err := f.zfile.LoadBlk(ctx, blk)

	loading.blkdata = blkdata
	loading.err = err

	// data loaded with error - cleanup .loading
	if loading.err != nil {
		close(loading.ready)
		f.loadMu.Lock()
		delete(f.loading, blk)
		f.loadMu.Unlock()
		return err
	}

	// data can be used now
	close(loading.ready)
	copy(dest, blkdata) // TODO avoid copy

	// store to kernel pagecache whole block that we've just loaded from database.
	// This way, even if the user currently requested to read only small portion from it,
	// it will prevent next e.g. consecutive user read request to again hit
	// the DB, and instead will be served by kernel from its pagecache.
	//
	// We cannot do this directly from reading goroutine - while reading
	// kernel FUSE is holding corresponding page in pagecache locked, and if
	// we would try to update that same page in pagecache it would result
	// in deadlock inside kernel.
	//
	// .loading cleanup is done once we are finished with putting the data into OS pagecache.
	// If we do it earlier - a simultaneous read covered by the same block could result
	// into missing both kernel pagecache (if not yet updated) and empty .loading[blk],
	// and thus would trigger DB access again.
	//
	// TODO if direct-io: don't touch pagecache
	// TODO upload parts only not covered by current read (not to e.g. wait for page lock)
	// TODO skip upload completely if read is wide to cover whole blksize
	go f.uploadBlk(blk, loading)

	return nil
}

// uploadBlk complements readBlk: it uploads loaded blkdata into OS cache.
func (f *BigFile) uploadBlk(blk int64, loading *blkLoadState) {
	oid := f.zfile.POid()

	st := gfsconn.FileNotifyStoreCache(f.Inode(), blk*f.blksize, loading.blkdata)

	f.loadMu.Lock()
	bug := (loading != f.loading[blk])
	if !bug {
		delete(f.loading, blk)
	}
	f.loadMu.Unlock()

	if bug {
		panicf("BUG: bigfile %s: blk %d: f.loading mutated while uploading data to pagecache", oid, blk)
	}

	if st == fuse.OK {
		return
	}

	// pagecache update failed, but it must not (we verified on startup that
	// pagecache control is supported by kernel). We can correctly live on
	// with the error, but data access will be likely very slow. Tell user
	// about the problem.
	log.Errorf("BUG: bigfile %s: blk %d: -> pagecache: %s  (ignoring, but reading from bigfile will be very slow)", oid, blk, st)
}


// ---- Lookup ----

// /(head|<rev>)/bigfile/ -> Lookup receives client request to create /(head|<rev>)/bigfile/<bigfileX>.
func (bfdir *BigFileDir) Lookup(out *fuse.Attr, name string, fctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	f, err := bfdir.lookup(out, name, fctx)
	var inode *nodefs.Inode
	if f != nil {
		inode = f.Inode()
	}
	return inode, err2LogStatus(err)

}

func (bfdir *BigFileDir) lookup(out *fuse.Attr, name string, fctx *fuse.Context) (f *BigFile, err error) {
	defer xerr.Contextf(&err, "%s: lookup %q", bfdir.path(), name)

	oid, err := zodb.ParseOid(name)
	if err != nil {
		return nil, eINVALf("not oid")
	}

	defer func() {
		if f != nil {
			f.getattr(out)
		}
	}()

	// check to see if dir(oid) is already there
	bfdir.fileMu.Lock()
	f, already := bfdir.fileTab[oid]
	bfdir.fileMu.Unlock()

	if already {
		return f, nil
	}

	// not there - without bfdir lock proceed to open BigFile from ZODB
	f, err = bfdir.head.bigfopen(fctx, oid)
	if err != nil {
		return nil, err
	}

	// relock bfdir and either register f or, if the file was maybe
	// simultaneously created while we were not holding bfdir.fileMu, return that.
	bfdir.fileMu.Lock()
	f2, already := bfdir.fileTab[oid]
	if already {
		bfdir.fileMu.Unlock()
		// f.Close() not needed - BigFile is all just garbage-collected
		return f2, nil
	}

	bfdir.fileTab[oid] = f
	bfdir.fileMu.Unlock()

	// mkfile takes filesystem treeLock - do it outside bfdir.fileMu
	mkfile(bfdir, name, f)

	return f, nil
}

// / -> Lookup receives client request to create @<rev>/.
func (root *Root) Lookup(out *fuse.Attr, name string, fctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	revd, err := root.lookup(name, fctx)
	var inode *nodefs.Inode
	if revd != nil {
		inode = revd.Inode()
		_ = revd.GetAttr(out, nil, fctx) // always ok
	}
	return inode, err2LogStatus(err)
}

func (root *Root) lookup(name string, fctx *fuse.Context) (_ *Head, err error) {
	defer xerr.Contextf(&err, "/: lookup %q", name)

	var rev zodb.Tid
	ok := false

	if strings.HasPrefix(name, "@") {
		rev, err = zodb.ParseTid(name[1:])
		ok = (err == nil)
	}
	if !ok {
		return nil, eINVALf("not @rev")
	}

	// check to see if dir(rev) is already there
	root.revMu.Lock()
	revDir, already := root.revTab[rev]
	root.revMu.Unlock()

	if already {
		// XXX race wrt simultaneous "FORGET @<rev>" ?
		return revDir, nil
	}

	// not there - without revMu lock proceed to open @rev view of ZODB
	zconnRev, err := xzodb.ZOpen(fctx, root.zdb, &zodb.ConnOptions{At: rev})
	if err != nil {
		return nil, err
	}

	// relock root and either register new revX/ directory or, if the
	// directory was maybe simultaneously created while we were not holding
	// revMu, return that.
	root.revMu.Lock()
	revDir, already = root.revTab[rev]
	if already {
		root.revMu.Unlock()
//		zconnRev.Release()
		transaction.Current(zconnRev.TxnCtx).Abort()
		return revDir, nil
	}

	revDir = &Head{
		// TODO how to test forgets:
		// echo 2 >/proc/sys/vm/drop_caches	(root)
		// mount -i -oremount $mntpt		(root ?)  (shrinks dcache)
		// notify invalidate dentry from inside fs
		fsNode: newFSNode(&fsOptions{Sticky: false}), // TODO + Head.OnForget() -> del root.revTab[]
		rev:    rev,
		zconn:  zconnRev, // TODO + Head.OnForget() -> release zconn (= abort zconn.TxnCtx)
	}

	bfdir := &BigFileDir{
		fsNode:  newFSNode(&fsOptions{Sticky: false}),	// TODO + BigFileDir.OnForget()
		head:    revDir,
		fileTab: make(map[zodb.Oid]*BigFile),
	}
	revDir.bfdir = bfdir

	root.revTab[rev] = revDir
	root.revMu.Unlock()

	// mkdir takes filesystem treeLock - do it outside revMu.
	mkdir(root, name, revDir)
	mkdir(revDir, "bigfile", bfdir)

	return revDir, nil
}


// bigfopen opens BigFile corresponding to oid on head.zconn.
//
// A ZBigFile corresponding to oid is activated and statted.
func (head *Head) bigfopen(ctx context.Context, oid zodb.Oid) (_ *BigFile, err error) {
	zconn := head.zconn
	defer xerr.Contextf(&err, "bigfopen %s @%s", oid, zconn.At())

	// TODO better ctx = transaction.PutIntoContext(ctx, txn)
	ctx, cancel := xcontext.Merge(ctx, zconn.TxnCtx)
	defer cancel()

	xzfile, err := zconn.Get(ctx, oid)
	if err != nil {
		switch errors.Cause(err).(type) {
		case *zodb.NoObjectError:
			return nil, eINVAL(err)
		case *zodb.NoDataError:
			return nil, eINVAL(err)
		default:
			return nil, err
		}
	}

	zfile, ok := xzfile.(*ZBigFile)
	if !ok {
		return nil, eINVALf("%s is not a ZBigFile", xzodb.TypeOf(xzfile))
	}

	// extract blksize, size and initial approximation for file revision
	err = zfile.PActivate(ctx)
	if err != nil {
		return nil, err
	}
	blksize := zfile.BlkSize()
	// NOTE file revision should be revision of both ZBigFile and its data. But we
	// cannot get data revision without expensive scan of all ZBigFile's objects.
	// -> approximate mtime initially with ZBigFile object mtime.
	revApprox := zfile.PSerial()
	zfile.PDeactivate()

	size, _, _, err := zfile.Size(ctx)
	if err != nil {
		return nil, err
	}

	f := &BigFile{
		fsNode:    newFSNode(&fsOptions{Sticky: false}), // TODO + BigFile.OnForget -> del .head.bfdir.fileTab[]
		head:      head,
		zfile:     zfile,
		blksize:   blksize,
		size:      size,
		revApprox: revApprox,
		loading:   make(map[int64]*blkLoadState),
	}

	return f, nil
}


// ---- misc ---

// /(head|<rev>)/at -> readAt serves read.
func (h *Head) readAt(fctx *fuse.Context) ([]byte, error) {
	return []byte(h.zconn.At().String()), nil
}

// /(head|<rev>)/ -> Getattr serves stat.
func (head *Head) GetAttr(out *fuse.Attr, _ nodefs.File, fctx *fuse.Context) fuse.Status {
	at := head.rev
	if at == 0 {
		at = head.zconn.At()
	}
	t := at.Time().Time

	out.Mode = fuse.S_IFDIR | 0555
	out.SetTimes(/*atime=*/nil, /*mtime=*/&t, /*ctime=*/&t)
	return fuse.OK
}

// /(head|<rev>)/bigfile/<bigfileX> -> Getattr serves stat.
func (f *BigFile) GetAttr(out *fuse.Attr, _ nodefs.File, fctx *fuse.Context) fuse.Status {
	f.getattr(out)
	return fuse.OK
}

func (f *BigFile) getattr(out *fuse.Attr) {
	out.Mode = fuse.S_IFREG | 0444
	out.Size = uint64(f.size)
	out.Blksize = uint32(f.blksize)	// NOTE truncating 64 -> 32
	// .Blocks

	mtime := f.revApprox.Time().Time
	out.SetTimes(/*atime=*/nil, /*mtime=*/&mtime, /*ctime=*/&mtime)
}




// FIXME gfsconn is tmp workaround for lack of way to retrieve FileSystemConnector from nodefs.Inode
// TODO:
//	- Inode += .Mount() -> nodefs.Mount
//	- Mount:
//		.Root()		-> root Inode of the fs
//		.Connector()	-> FileSystemConnector through which fs is mounted
var gfsconn *nodefs.FileSystemConnector

// TODO -> enable/disable fuse debugging dynamically (by write to .wcfs/debug ?)

func main() {
	//stdlog.SetPrefix("wcfs: ")	NOTE conflicts with log.CopyStandardLogTo
	log.CopyStandardLogTo("WARNING")
	defer log.Flush()

	err := _main()
	if err != nil {
		log.Fatal(err)
	}
}

func _main() (err error) {
	debug := flag.Bool("d", false, "debug")
	autoexit := flag.Bool("autoexit", false, "automatically stop service when there is no client activity")

	flag.Parse()
	if len(flag.Args()) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] zurl mntpt\n", os.Args[0])
		os.Exit(2)
	}
	zurl := flag.Args()[0]
	mntpt := flag.Args()[1]

	xclose := func(c io.Closer) {
		err = xerr.First(err, c.Close())
	}

	// debug -> precise t, no dates	(TODO(?) -> always precise t?)
	if *debug {
		stdlog.SetFlags(stdlog.Lmicroseconds)
	}

	log.Infof("start %q %q", mntpt, zurl)
	gover := "(built with " + runtime.Version()
	if race.Enabled {
		gover += " -race"
	}
	gover += ")"
	log.Info(gover)

	// open zodb storage/watch/db/connection
	ctx := context.Background() // TODO(?) + timeout?
	zstor, err := zodb.Open(ctx, zurl, &zodb.OpenOptions{
		ReadOnly: true,
	})
	if err != nil {
		return err
	}
	defer xclose(zstor)

	// TODO consider using zodbCacheControl for all connections
	// ( in addition to zhead, historic connections - that are used to access @rev -
	//   also need to traverse BigFile.blktab btree )
	zdb := zodb.NewDB(zstor, &zodb.DBOptions{})
	defer xclose(zdb)
	zhead, err := xzodb.ZOpen(ctx, zdb, &zodb.ConnOptions{
	})
	if err != nil {
		return err
	}
	zhead.Cache().Lock()
	zhead.Cache().SetControl(&zodbCacheControl{})
	zhead.Cache().Unlock()

	// mount root + head/
	head := &Head{
		fsNode:   newFSNode(fSticky),
		rev:      0,
		zconn:    zhead,
	}

	bfdir := &BigFileDir{
		fsNode:   newFSNode(fSticky),
		head:     head,
		fileTab:  make(map[zodb.Oid]*BigFile),
	}
	head.bfdir = bfdir

	root := &Root{
		fsNode: newFSNode(fSticky),
		zstor:  zstor,
		zdb:    zdb,
		head:   head,
		revTab: make(map[zodb.Tid]*Head),
	}

	opts := &fuse.MountOptions{
		FsName: zurl,
		Name:   "wcfs",

		DisableXAttrs: true,        // we don't use
		Debug:         *debug,
	}

	fssrv, fsconn, err := mount(mntpt, root, opts)
	if err != nil {
		return err
	}
	gfsconn = fsconn // FIXME temp workaround (see ^^^)

	// we require proper pagecache control (added to Linux 2.6.36 in 2010)
	kinit := fssrv.KernelSettings()
	kfuse := fmt.Sprintf("kernel FUSE (API %d.%d)", kinit.Major, kinit.Minor)
	supports := kinit.SupportsNotify
	if !supports(fuse.NOTIFY_STORE_CACHE) {
		return fmt.Errorf("%s does not support pagecache control", kfuse)
	}

	// add entries to /
	mkdir(root, "head", head)
	mkdir(head, "bigfile", bfdir)
	mkfile(head, "at", NewSmallFile(head.readAt)) // TODO mtime(at) = tidtime(at)

	// for debugging/testing
	_wcfs := newFSNode(fSticky)
	mkdir(root, ".wcfs", &_wcfs)
	mkfile(&_wcfs, "zurl", NewStaticFile([]byte(zurl)))

	// TODO handle autoexit
	// (exit when kernel forgets all our inodes - wcfs.py keeps .wcfs/zurl
	//  opened, so when all inodes has been forgotten - we know all wcfs.py clients exited)
	_ = autoexit

	defer xerr.Contextf(&err, "serve %s %s", mntpt, zurl)

	// spawn filesystem server.
	//
	// use `go serve` + `waitMount` not just `serve` - because waitMount
	// cares to disable OS calling poll on us.
	// ( if we don't disable polling - fs serving can get stuck - see
	//   https://github.com/hanwen/go-fuse/commit/4f10e248eb for details )
	serveCtx, serveCancel := context.WithCancel(context.Background())
	go func () {
		defer serveCancel()
		fssrv.Serve()
	}()
	err = fssrv.WaitMount()
	if err != nil {
		return err
	}

	// filesystem server is serving requests.

	// wait for unmount
	// NOTE the kernel does not send FORGETs on unmount - but we don't need
	// to release left node resources ourselves, because it is just memory.
	<-serveCtx.Done()
	log.Infof("stop %q %q", mntpt, zurl)
	return nil
}
