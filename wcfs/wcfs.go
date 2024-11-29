// Copyright (C) 2018-2024  Nexedi SA and Contributors.
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
// Clients that want to get isolation guarantee should subscribe for
// invalidations and re-mmap invalidated regions to file with pinned bigfile revision for
// the duration of their transaction. See "Isolation protocol" for details(*).
//
// In the usual situation when bigfiles are big, and there are O(1)/δt updates,
// there should be no need for any cache besides shared kernel cache of latest
// bigfile data.
//
// --------
//
// (*) wcfs servers comes accompanied by Python and C++ client packages that
// take care about isolation protocol details and provide to clients simple
// interface similar to regular files.
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
//		watch			; channel for bigfile invalidations
//		bigfile/		; bigfiles' data
//			<oid(ZBigFile1)>
//			<oid(ZBigFile2)>
//			...
//
// where /bigfile/<bigfileX> represents latest bigfile data as stored in
// upstream ZODB. As there can be some lag receiving updates from the database,
// /at describes precisely ZODB state for which bigfile data is currently
// exposed. Whenever bigfile data is changed in upstream ZODB, information
// about the changes is first propagated to /watch, and only after that
// /bigfile/<bigfileX> is updated. See "Isolation protocol" for details.
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
// Isolation protocol
//
// In order to support isolation, wcfs implements isolation protocol that
// must be cooperatively followed by both wcfs and client.
//
// First, client mmaps latest bigfile, but does not access it:
//
//	mmap(head/bigfile/<bigfileX>)
//
// Then client opens head/watch and tells wcfs through it for which ZODB state
// it wants to get bigfile's view:
//
//	C: 1 watch <bigfileX> @<at>
//
// The server then, after potentially sending initial pin and unpin messages
// (see below), reports either success or failure:
//
//	S: 1 ok
//	S: 1 error ...		; if <at> is too far away back from head/at
//
// The server sends "ok" reply only after head/at is ≥ requested <at>, and only
// after all initial pin/unpin messages are fully acknowledged by the client.
// The client can start to use mmapped data after it gets "ok".
// The server sends "error" reply e.g. if requested <at> is too far away back
// from head/at, or on any other error.
// TODO specify watch state after error.
//
// Upon watch request, either initially, or after sending "ok", the server will be notifying the
// client about file blocks that client needs to pin in order to observe file's
// data as of <at> revision:
//
// The filesystem server itself receives information about changed data from
// ZODB server through regular ZODB invalidation channel (as it is ZODB client
// itself). Then, separately for each changed file block, before actually
// updating head/bigfile/<bigfileX> content, it notifies through opened
// head/watch links to clients, that had requested it (separately to each
// client), about the changes:
//
//	S: <2·k> pin <bigfileX> #<blk> @<rev_max>	; @head means unpin
//
// and waits until all clients confirm that changed file block can be updated
// in global OS cache.
//
// The client in turn should now re-mmap requested to be pinned block to bigfile@<rev_max>
//
//	# mmapped at address corresponding to #blk
//	mmap(@<rev_max>/bigfile/<bigfileX>, #blk, MAP_FIXED)
//
// or, if given @head as @<rev_max>, to bigfile@head
//
//	mmap(head/bigfile/<bigfileX>, #blk, MAP_FIXED)
//
// and must send ack back to the server when it is done:
//
//	C: <2·k> ack
//
// The server sends pin notifications only for file blocks, that are known to
// be potentially changed after client's <at>, and <rev_max> describes the
// upper bound for the block revision as of <at> database view:
//
//	<rev_max> ≤ <at>	; block stays unchanged in (<rev_max>, <at>] range
//
// The server maintains short history tail of file changes to be able to
// support openings with <at> being slightly in the past compared to current
// head/at. The server might reject a watch request if <at> is too far away in
// the past from head/at. The client is advised to restart its transaction with
// more uptodate database view if it gets watch setup error.
//
// A later request from the client for the same <bigfileX> but with different
// <at>, overrides previous watch request for that file. A client can use "-"
// instead of "@<at>" to stop watching a file.
//
// A single client can send several watch requests through single head/watch
// open, as well as it can use several head/watch opens simultaneously.
// The server sends pin notifications for all files requested to be watched via
// every opened head/watch link.
//
// Note: a client could use a single watch to manage its several views for the same
// file but with different <at>. This could be achieved via watching with
// @<at_min>, and then deciding internally which views needs to be adjusted and
// which views need not. Wcfs does not oblige clients to do so though, and a
// client is free to use as many head/watch openings as it needs to.
//
// When clients are done with @<revX>/bigfile/<bigfileX> (i.e. client's
// transaction ends and array is unmapped), the server sees number of opened
// files to @<revX>/bigfile/<bigfileX> drops to zero, and automatically
// destroys @<revX>/bigfile/<bigfileX> after reasonable timeout.
//
// The client should send "bye" before closing head/watch file:
//
//	C: <2·k+1> bye
//
//
// Protection against slow or faulty clients
//
// If a client, on purpose or due to a bug or being stopped, is slow to respond
// with ack to file invalidation notification, it creates a problem because the
// server will become blocked waiting for pin acknowledgments, and thus all
// other clients, that try to work with the same file, will get stuck.
//
// The problem could be avoided, if wcfs would reside inside OS kernel and this
// way could be able to manipulate clients address space directly (then
// isolation protocol won't be needed). It is also possible to imagine
// mechanism, where wcfs would synchronously change clients' address space via
// injecting trusted code and running it on client side via ptrace to adjust
// file mappings.
//
// However ptrace does not work when client thread is blocked under pagefault,
// and that is exactly what wcfs would need to do to process invalidations
// lazily, because eager invalidation processing results in prohibitively slow
// file opens. See internal wcfs overview for details about why ptrace
// cannot be used and why lazy invalidation processing is required.
//
// Lacking OS primitives to change address space of another process and not
// being able to work it around with ptrace in userspace, wcfs takes approach
// to kill a slow or faulty client on 30 seconds timeout or on any other pin
// handling error. This way wcfs achieves progress and safety properties:
// processing does not get stuck even if there is a hung client, and there is
// no corruption in the data that is provided to all live and well-behaving
// clients.
//
// Killing a client with SIGBUS is similar to how OS kernel sends SIGBUS when
// a memory-mapped file is accessed and loading file data results in EIO. It is
// also similar to wendelin.core 1 where SIGBUS is raised if loading file block
// results in an error.
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
// of the same array, this is the mode wendelin.core actually uses.
//
// 3. write to wcfs
//
// TODO we later could implement "write-directly" mode where clients would write
// data directly into the file.
package main

// Wcfs organization
//
// Wcfs is a ZODB client that translates ZODB objects into OS files as would
// non-wcfs wendelin.core do for a ZBigFile. Contrary to non-wcfs wendelin.core,
// it keeps bigfile data in shared OS cache efficiently. It is organized as follows:
//
// 1) 1 ZODB connection for "latest data" for whole filesystem (zhead).
// 2) head/bigfile/* of all bigfiles represent state as of zhead.At .
// 3) for head/bigfile/* the following invariant is maintained:
//
//	#blk ∈ OS file cache	=>  all BTree/Bucket/ZBlk that lead to blk are tracked(%)
//
//    The invariant helps on invalidation: when δFtail (see below) sees a
//    changed oid, it is guaranteed that if the change affects block that was
//    ever provided to OS, δFtail will detect that this block has changed.
//    And if oid relates to a file block but is not in δFtail's tracking set -
//    we know that block is not cached and will trigger ZODB load on a future
//    file read.
//
//    Currently we maintain this invariant by adding ZBlk/LOBTree/LOBucket
//    objects to δFtail on every access, and never shrinking that tracking set.
//    In the future we may want to try to synchronize to kernel freeing its
//    pagecache pages.
//
// 4) when we receive an invalidation message from ZODB - we process it and
//    propagate invalidations to OS file cache of head/bigfile/*:
//
//	invalidation message:  δZ = (tid↑, []oid)
//
//    4.1) δF = δFtail.Update(δZ)
//
//	δFtail (see below) converts ZODB-level changes into information about
//	which blocks of which files were modified and need to be invalidated:
//
//	  δF = (tid↑, {} file -> []#blk)
//
//	Note that δF might be not full and reflects only changes to files and
//	blocks that were requested to be tracked. However because of the invariant
//	δF covers in full what needs to be invalidated in the OS file cache.
//
//    4.2) for all file/blk to invalidate we do:
//
//	- try to retrieve head/bigfile/file[blk] from OS file cache(*);
//	- if retrieved successfully -> store retrieved data back into OS file
//	  cache for @<rev>/bigfile/file[blk], where
//
//	    rev = δFtail.BlkRevAt(file, #blk, zhead.at)
//
//	- invalidate head/bigfile/file[blk] in OS file cache.
//
//	This preserves previous data in OS file cache in case it will be needed
//	by not-yet-uptodate clients, and makes sure file read of head/bigfile/file[blk]
//	won't be served from OS file cache and instead will trigger a FUSE read
//	request to wcfs.
//
//    4.3) no invalidation messages are sent to wcfs clients at this point(+).
//
//    4.4) processing ZODB invalidations and serving file reads (see 7) are
//      organized to be mutually exclusive.
//
//    4.5) similarly, processing ZODB invalidations and setting up watches (see
//      7.2) are organized to be mutually exclusive.
//
// 5) after OS file cache was invalidated, we resync zhead to new database
//    view corresponding to tid.
//
// 6) a ZBigFile-level history tail is maintained in δFtail.
//
//    δFtail translates ZODB object-level changes into information about which
//    blocks of which ZBigFile were modified, and provides service to query
//    that information.
//
//    It semantically consists of
//
//	[]δF
//
//    where δF represents a change in files space
//
//	δF:
//		.rev↑
//		{} file ->  {}blk
//
//    min(rev) in δFtail is min(@at) at which head/bigfile/file is currently watched (see below).
//
//    To support initial openings with @at being slightly in the past, we also
//    make sure that min(rev) is enough to cover last 1 minute of history
//    from head/at.
//
//    Scalability of δFtail plays important role in scalability of WCFS because
//    δFtail, besides other places, is queried and potentially rebuilt at every
//    FUSE read request (see 7 below).
//
//    See documentation in internal/zdata/δftail.go for more details on ΔFtail
//    and its scalability properties.
//
// 7) when we receive a FUSE read(#blk) request to a head/bigfile/file, we process it as follows:
//
//   7.1) load blkdata for head/bigfile/file[blk] @zhead.at .
//
//	while loading this also gives upper bound estimate of when the block
//	was last changed:
//
//	  rev(blk) ≤ max(_.serial for _ in (ZBlk(#blk), all BTree/Bucket that lead to ZBlk))
//
//	it is not exact because BTree/Bucket can change (e.g. rebalance)
//	but still point to the same k->ZBlk.
//
//	we also use δFtail to find either exact blk revision or another upper
//	bound if file[blk] has no change during δFtail coverage:
//
//	  rev(blk) = δFtail.BlkRevAt(file, #blk, zhead.at)
//
//	below rev'(blk) is min(of the estimates found):
//
//	  rev(blk) ≤ rev'(blk)		rev'(blk) = min(^^^)
//
//	Note: we delay recomputing δFtail.BlkRevAt(file, #blk, head) because
//	using just cheap revmax estimate can frequently result in all watches
//	being skipped.
//
//   7.2) for all registered client@at watches of head/bigfile/file:
//
//	- rev'(blk) ≤ at: -> do nothing
//	- rev'(blk) > at:
//	  - if blk ∈ watch.pinned -> do nothing
//	  - rev = δFtail.BlkRevAt(file, #blk, at)
//	  - watch.pin(file, #blk, @rev)
//	  - watch.pinned += blk
//
//	where
//
//	  watch.pin(file, #blk, @rev)
//
//	sends pin message according to "Isolation protocol", and is assumed
//	to cause
//
//	  remmap(file, #blk, @rev/bigfile/file)
//
//	on client.
//
//	( one could imagine adjusting mappings synchronously via running
//	  wcfs-trusted code via ptrace that wcfs injects into clients, but ptrace
//	  won't work when client thread is blocked under pagefault or syscall(^) )
//
//	in order to support watching for each head/bigfile/file
//
//	  [] of watch{client@at↑, pinned}
//
//	is maintained.
//
//   7.3) blkdata is returned to kernel.
//
//   Thus a client that wants latest data on pagefault will get latest data,
//   and a client that wants @rev data will get @rev data, even if it was this
//   "old" client that triggered the pagefault(~).
//
// 8) serving FUSE reads from @<rev>/bigfile/file is organized similarly to
//    serving reads from head/bigfile/file, but with using dedicated per-<rev>
//    ZODB connection and without notifying any watches.
//
// 9) for every ZODB connection (zhead + one per @<rev>) a dedicated read-only
//    transaction is maintained. For zhead, every time it is resynced (see "5")
//    the transaction associated with zhead is renewed.
//
// TODO 10) gc @rev/ and @rev/bigfile/<bigfileX> automatically on atime timeout
//
//
// (*) see notes.txt -> "Notes on OS pagecache control"
// (+) see notes.txt -> "Invalidations to wcfs clients are delayed until block access"
// (~) see notes.txt -> "Changing mmapping while under pagefault is possible"
// (^) see notes.txt -> "Client cannot be ptraced while under pagefault"
// (%) no need to keep track of ZData - ZBlk1 is always marked as changed on blk data change.


// Wcfs locking organization
//
// As it was said processing ZODB invalidations (see "4") and serving file
// reads (see "7") are organized to be mutually exclusive. To do so a major RW
// lock - zheadMu - is used. Whenever ZODB invalidations are processed and
// zhead.at is updated - zheadMu.W is taken. Contrary whenever file read is
// served and in other situations - which needs zhead to remain viewing
// database at the same state - zheadMu.R is taken.
//
// Several locks that protect internal data structures are minor to zheadMu -
// they need to be taken only under zheadMu.R (to protect e.g. multiple readers
// running simultaneously to each other), but do not need to be taken at all if
// zheadMu.W is taken. In data structures such locks are noted as follows
//
//	xMu  sync.Mutex // zheadMu.W  |  zheadMu.R + xMu
//
// If a lock is not minor to zheadMu, it is still ok to lock it under zheadMu.R
// as zheadMu, being the most major lock in wcfs, always comes locked first, if
// it needs to be locked.
//
// For watches, similarly to zhead, watch.at is protected by major-for-watch
// per-watch RW lock watch.atMu . When watch.at is updated during watch
// setup/upgrade time - watch.atMu.W is taken. Contrary whenever watch is
// notified with pin messages - watch.atMu.R is taken to make sure watch.at
// stays unchanged while pins are prepared and processed.
//
// For watches, similarly to zheadMu, there are several minor-to-atMu locks
// that protect internal data structures. Such locks are noted similarly to
// zheadMu enslavement.
//
// In addition to what is written above there are other ordering rules that are
// followed consistently to avoid hitting deadlock:
//
//	BigFile.watchMu		>  Watch.atMu
//	WatchLink.byfileMu	>  BigFile.watchMu
//	WatchLink.byfileMu	>  BigFileDir.fileMu
//	WatchLink.byfileMu	>  Watch.atMu


// Notation used
//
// δZ    - change in ZODB space
// δB    - change in BTree*s* space
// δT    - change in BTree(1) space
// δF    - change in File*s* space
// δfile - change in File(1) space
//
// f     - BigFile
// bfdir - BigFileDir
// wlink - WatchLink
// w     - Watch

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/golang/glog"

	"lab.nexedi.com/kirr/go123/xcontext"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xio"
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
	// (zhead is Resync'ed and is kept outside zdb pool)
	head *Head

	// directories + ZODB connections for @<rev>/
	revMu  sync.Mutex
	revTab map[zodb.Tid]*Head

	// time budget for a client to handle pin notification
	pinTimeout time.Duration

	// collected statistics
	stats *Stats
}

// /(head|<rev>)/			- served by Head.
type Head struct {
	fsNode

	rev   zodb.Tid    // 0 for head/, !0 for @<rev>/
	bfdir *BigFileDir // bigfile/
	// at    - served by .readAt
	// watch - implicitly linked to by fs

	// ZODB connection for everything under this head

	// zheadMu protects zconn.At & live _objects_ associated with it.
	// while it is rlocked zconn is guaranteed to stay viewing database at
	// particular view.
	//
	// zwatcher write-locks this and knows noone is using ZODB objects and
	// noone mutates OS file cache while zwatcher is running.
	//
	// it is also kept rlocked by OS cache uploaders (see BigFile.uploadBlk)
	// with additional locking protocol to avoid deadlocks (see below for
	// pauseOSCacheUpload + ...).
	//
	// TODO head.zheadMu -> special mutex with Lock(ctx) so that Lock wait could be canceled
	zheadMu sync.RWMutex
	zconn   *xzodb.ZConn // for head/ zwatcher resyncs head.zconn; others only read zconn objects.

	// zwatcher signals to uploadBlk to pause/continue uploads to OS cache to avoid deadlocks.
	// see notes.txt -> "Kernel locks page on read/cache store/..." for details.
	pauseOSCacheUpload    bool
	continueOSCacheUpload chan struct{}
	// uploadBlk signals to zwatcher that there are so many inflight OS cache uploads currently.
	inflightOSCacheUploads int32

	// head/watch opens
	wlinkMu  sync.Mutex
	wlinkTab map[*WatchLink]struct{}

	// waiters for zhead.At to become ≥ their at.
	hwaitMu sync.Mutex           // zheadMu.W  |  zheadMu.R + hwaitMu
	hwait   map[hwaiter]struct{} // set{(at, ready)}
}

// /(head|<rev>)/bigfile/		- served by BigFileDir.
type BigFileDir struct {
	fsNode
	head *Head // parent head/ or @<rev>/

	// {} oid -> <bigfileX>
	fileMu  sync.Mutex		// zheadMu.W  |  zheadMu.R + fileMu
	fileTab map[zodb.Oid]*BigFile

	// δ tail of tracked BTree nodes of all BigFiles + -> which file
	// (used only for head/, not revX/)
	δFtail *zdata.ΔFtail		// read/write access protected by zheadMu.{R,W}
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
			   // ( we can't know rev fully as some later blocks could be learnt only
			   //   while populating δFtail lazily. For simplicity we don't delve into
			   //   updating revApprox during lifetime of current transaction )

	// inflight loadings of ZBigFile from ZODB.
	// successful load results are kept here until blkdata is put into OS pagecache.
	//
	// Being a staging area for data to enter OS cache, loading has to be
	// consulted/invalidated whenever wcfs logic needs to consult/invalidate OS cache.
	loadMu  sync.Mutex              // zheadMu.W  |  zheadMu.R + loadMu
	loading map[int64]*blkLoadState // #blk -> {... blkdata}

	// watches attached to this file.
	//
	// both watches in already "established" state (i.e. initial watch
	// request was completed and answered with "ok"), and watches in
	// progress of being established are kept here.
	watchMu  sync.RWMutex
	watchTab map[*Watch]struct{}
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

// /head/watch				- served by WatchNode.
type WatchNode struct {
	fsNode

	head   *Head // parent head/
	idNext int32 // ID for next opened WatchLink
}

// /head/watch open			- served by WatchLink.
type WatchLink struct {
	sk   *FileSock // IO channel to client
	id   int32     // ID of this /head/watch handle (for debug log)
	head *Head

	// watches associated with this watch link.
	//
	// both already established, and watches being initialized in-progress are registered here.
	// (see setupWatch)
	byfileMu sync.Mutex
	byfile   map[zodb.Oid]*Watch // {} foid -> Watch

	// IO
	reqNext uint64 // stream ID for next wcfs-originated request; 0 is reserved for control messages
	txMu    sync.Mutex
	rxMu    sync.Mutex
	rxTab   map[/*stream*/uint64]chan string // client replies go via here

	// serve operates under .serveCtx and can be requested to stop via serveCancel
	serveCtx    context.Context
	serveCancel context.CancelFunc
	down1       sync.Once
	down        chan struct{}  // ready after shutdown completes
	pinWG       sync.WaitGroup // all pin handlers are accounted here

	client *os.Process // client that opened the WatchLink
}

// Watch represents watching for changes to 1 BigFile over particular watch link.
type Watch struct {
	link *WatchLink // link to client
	file *BigFile	// watching this file

	// setupMu is used to allow only 1 watch request for particular file to
	// be handled simultaneously for particular client. It complements atMu
	// by continuing to protect setupWatch from another setupWatch when
	// setupWatch non-atomically downgrades atMu.W to atMu.R .
	setupMu sync.Mutex

	// atMu, similarly to zheadMu, protects watch.at and pins associated with Watch.
	// atMu.R guarantees that watch.at is not changing, but multiple
	//        simultaneous pins could be running (used e.g. by readPinWatchers).
	// atMu.W guarantees that only one user has watch.at write access and
	//        that no pins are running (used by setupWatch).
	atMu sync.RWMutex
	at   zodb.Tid               // requested to be watched @at

	pinnedMu sync.Mutex             // atMu.W  |  atMu.R + pinnedMu
	pinned   map[int64]*blkPinState // {} blk -> {... rev}  blocks that are already pinned to be ≤ at
}

// blkPinState represents state/result of pinning one block.
//
// when !ready the pinning is in progress.
// when ready  the pinning has been completed.
type blkPinState struct {
	rev    zodb.Tid // revision to which the block is being or has been pinned

	ready  chan struct{}
	err    error
}

// Stats keeps collected statistics.
//
// The statistics is accessible via .wcfs/stats file served by _wcfs_Stats.
type Stats struct {
	pin     atomic.Int64 // # of times wcfs issued pin request
	pinkill atomic.Int64 // # of times a client was killed due to badly handling pin
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


// -------- 4) ZODB invalidation -> OS cache --------

func traceZWatch(format string, argv ...interface{}) {
	if !log.V(1) {
		return
	}
	log.InfoDepth(1, fmt.Sprintf("zwatcher: " + format, argv...))
}
func debugZWatch(format string, argv ...interface{}) {
	if !log.V(2) {
		return
	}
	log.InfoDepth(1, fmt.Sprintf("zwatcher: " + format, argv...))
}

// zwatcher watches for ZODB changes.
//
// see "4) when we receive an invalidation message from ZODB ..."
func (root *Root) zwatcher(ctx context.Context, zwatchq chan zodb.Event) (err error) {
	defer xerr.Contextf(&err, "zwatch %s", root.zstor.URL())
	traceZWatch(">>>")

	var zevent zodb.Event
	var ok bool

	for {
		debugZWatch("select ...")
		select {
		case <-ctx.Done():
			traceZWatch("cancel")
			return ctx.Err()

		case zevent, ok = <-zwatchq:
			if !ok {
				traceZWatch("zwatchq closed")
				return nil // closed
			}

		}

		traceZWatch("zevent: %s", zevent)

		switch zevent := zevent.(type) {
		default:
			return fmt.Errorf("unexpected event: %T", zevent)

		case *zodb.EventError:
			return zevent.Err

		case *zodb.EventCommit:
			err = root.handleδZ(ctx, zevent)
			if err != nil {
				return err
			}
		}
	}
}

// handleδZ handles 1 change event from ZODB notification.
func (root *Root) handleδZ(ctx context.Context, δZ *zodb.EventCommit) (err error) {
	defer xerr.Contextf(&err, "handleδZ @%s", δZ.Tid)

	head := root.head

	// while we are invalidating OS cache, make sure that nothing, that
	// even reads /head/bigfile/*, is running (see 4.4).
	//
	// also make sure that cache uploaders we spawned (uploadBlk) are all
	// paused, or else they could overwrite OS cache with stale data.
	// see notes.txt -> "Kernel locks page on read/cache store/..." for
	// details on how to do this without deadlocks.
	continueOSCacheUpload := make(chan struct{})
retry:
	for {
		// TODO ctx cancel
		head.zheadMu.Lock()
		head.pauseOSCacheUpload = true
		head.continueOSCacheUpload = continueOSCacheUpload

		// NOTE need atomic load, since inflightOSCacheUploads
		// decrement is done not under zheadMu.
		if atomic.LoadInt32(&head.inflightOSCacheUploads) != 0 {
			head.zheadMu.Unlock()
			continue retry
		}

		break
	}

	defer func() {
		head.pauseOSCacheUpload = false
		head.continueOSCacheUpload = nil
		head.zheadMu.Unlock()
		close(continueOSCacheUpload)
	}()

	// zheadMu.W taken and all cache uploaders are paused

	zhead := head.zconn
	bfdir := head.bfdir

	// invalidate kernel cache for data in changed files

	δF, err := bfdir.δFtail.Update(δZ) // δF <- δZ |tracked
	if err != nil {
		return err
	}

	if log.V(2) {
		// debug dump δF
		log.Infof("\n\nS: handleδZ: δF (#%d):\n", len(δF.ByFile))
		for foid, δfile := range δF.ByFile {
			blkv := δfile.Blocks.Elements()
			sort.Slice(blkv, func(i, j int) bool {
				return blkv[i] < blkv[j]
			})
			flags := ""
			if δfile.Size {
				flags += "S"
			}
			if δfile.Epoch {
				flags += "E"
			}
			log.Infof("S: \t- %s\t%2s %v\n", foid, flags, blkv)
		}
		log.Infof("\n\n")
	}

	// put zhead's transaction into ctx because we will potentially need to
	// access ZODB when processing invalidations.
	// TODO better ctx = transaction.PutIntoContext(ctx, txn)
	ctx0 := ctx
	ctx, cancel := xcontext.Merge(ctx0, zhead.TxnCtx)
	defer cancel()

	// invalidate kernel cache for file data
	wg := xsync.NewWorkGroup(ctx)
	for foid, δfile := range δF.ByFile {
		// file was requested to be tracked -> it must be present in fileTab
		file := bfdir.fileTab[foid]

		if δfile.Epoch {
			// XXX while invalidating whole file at epoch is easy,
			// it becomes not so easy to handle isolation if epochs
			// could be present. For this reason we forbid changes
			// to ZBigFile objects for now.
			return fmt.Errorf("ZBigFile<%s> changed @%s", foid, δF.Rev)
			// wg.Go(func(ctx context.Context) error {
			// 	return file.invalidateAll()  // NOTE does not accept ctx
			// })
		} else {
			for blk := range δfile.Blocks {
				blk := blk
				wg.Go(func(ctx context.Context) error {
					return file.invalidateBlk(ctx, blk)
				})
			}
		}
	}
	err = wg.Wait()
	if err != nil {
		return err
	}

	// invalidate kernel cache for attributes
	// we need to do it only if we see topology (i.e. btree) change
	//
	// do it after completing data invalidations.
	wg = xsync.NewWorkGroup(ctx)
	for foid, δfile := range δF.ByFile {
		if !δfile.Size {
			continue
		}
		file := bfdir.fileTab[foid] // must be present
		wg.Go(func(ctx context.Context) error {
			return file.invalidateAttr() // NOTE does not accept ctx
		})
	}
	err = wg.Wait()
	if err != nil {
		return err
	}

	// resync .zhead to δZ.tid

	// 1. abort old and resync to new txn/at
	transaction.Current(zhead.TxnCtx).Abort()
	_, txnCtx := transaction.New(context.Background())
	ctx, cancel = xcontext.Merge(ctx0, txnCtx) // TODO better transaction.PutIntoContext
	defer cancel()
	err = zhead.Resync(ctx, δZ.Tid)
	if err != nil {
		return err
	}
	zhead.TxnCtx = txnCtx

	// 2. restat invalidated ZBigFile
	// NOTE no lock needed since .blksize and .size are constant during lifetime of one txn.
	// TODO -> parallel
	for foid, δfile := range δF.ByFile {
		file := bfdir.fileTab[foid] // must be present
		zfile := file.zfile

		if δfile.Size {
			size, sizePath, blkCov, err := zfile.Size(ctx)
			if err != nil {
				return err
			}

			file.size = size
			// see "3) for */head/data the following invariant is maintained..."
			bfdir.δFtail.Track(zfile, -1, sizePath, blkCov, nil)
		}

		// NOTE we can miss a change to file if δblk is not yet tracked
		//      that's why revision is only approximated
		file.revApprox = zhead.At()
	}

	// notify .wcfs/debug/zhead
	gdebug.zheadSockTabMu.Lock()
	for sk := range gdebug.zheadSockTab {
		_, err := fmt.Fprintf(xio.BindCtxW(sk, ctx), "%s\n", δZ.Tid)
		if err != nil {
			log.Errorf("zhead: %s: write: %s  (detaching reader)", sk.file, err)
			sk.Close()
			delete(gdebug.zheadSockTab, sk)
		}
	}
	gdebug.zheadSockTabMu.Unlock()

	// shrink δFtail not to grow indefinitely.
	// cover history for at least 1 minute, but including all watches.
	//
	// TODO shrink δFtail only once in a while - there is no need to compute
	// revCut and cut δFtail on every transaction.
	revCut := zodb.TidFromTime(zhead.At().Time().Add(-1*time.Minute))
	head.wlinkMu.Lock()
	for wlink := range head.wlinkTab {
		for _, w := range wlink.byfile {
			if w.at < revCut {
				revCut = w.at
			}
		}
	}
	head.wlinkMu.Unlock()
	bfdir.δFtail.ForgetPast(revCut)

	// notify zhead.At waiters
	for hw := range head.hwait {
		if hw.at <= δZ.Tid {
			delete(head.hwait, hw)
			close(hw.ready)
		}
	}

	return nil
}

// hwaiter represents someone waiting for zhead to become ≥ at.
type hwaiter struct {
        at    zodb.Tid
        ready chan struct{}
}

// zheadWait waits till head.zconn.At becomes ≥ at.
//
// It returns error either if wcfs is down or ctx is canceled.
func (head *Head) zheadWait(ctx context.Context, at zodb.Tid) (err error) {
	defer xerr.Contextf(&err, "wait zhead ≥ %s", at)

	if head.rev != 0 {
		panic("must be called only for head/, not @revX/")
	}

	// TODO check wcfs.down

	// check if zhead is already ≥ at
	head.zheadMu.RLock()
	if head.zconn.At() >= at {
		head.zheadMu.RUnlock()
		return nil
	}

	// no - we have to wait for it
	ready := make(chan struct{})
	head.hwaitMu.Lock()
	head.hwait[hwaiter{at, ready}] = struct{}{}
	head.hwaitMu.Unlock()

	head.zheadMu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-ready:
		return nil // ok - zhead.At went ≥ at
	}
}

// invalidateBlk invalidates 1 file block in kernel cache.
//
// see "4.2) for all file/blk to in invalidate we do"
// called with zheadMu wlocked.
func (f *BigFile) invalidateBlk(ctx context.Context, blk int64) (err error) {
	defer xerr.Contextf(&err, "%s: invalidate blk #%d:", f.path(), blk)

	fsconn := gfsconn
	blksize := f.blksize
	off := blk*blksize

	var blkdata []byte = nil

	// first try to retrieve f.loading[blk];
	// make sure f.loading[blk] is invalidated.
	//
	// we are running with zheadMu wlocked - no need to lock f.loadMu
	loading, ok := f.loading[blk]
	if ok {
		if loading.err == nil {
			blkdata = loading.blkdata
		}
		delete(f.loading, blk)
	}

	// TODO skip retrieve/store if len(f.watchTab) == 0
	// try to retrieve cache of current head/data[blk], if we got nothing from f.loading
	if blkdata == nil {
		blkdata = make([]byte, blksize)
		n, st := fsconn.FileRetrieveCache(f.Inode(), off, blkdata)
		if st != fuse.OK {
			log.Errorf("%s: retrieve blk #%d from cache: %s (ignoring, but reading @revX/bigfile will be slow)", f.path(), blk, st)
		}
		blkdata = blkdata[:n]
	}

	// if less than blksize was cached - probably the kernel had to evict
	// some data from its cache already. In such case we don't try to
	// preserve the rest and drop what was read, to avoid keeping the
	// system overloaded.
	//
	// if we have the data - preserve it under @revX/bigfile/file[blk].
	if int64(len(blkdata)) == blksize {
		err := func() error {
			// store retrieved data back to OS cache for file @<rev>/file[blk]
			δFtail := f.head.bfdir.δFtail
			blkrev, _, err := δFtail.BlkRevAt(ctx, f.zfile, blk, f.head.zconn.At())
			if err != nil {
				return err
			}
			frev, funlock, err := groot.lockRevFile(blkrev, f.zfile.POid())
			if err != nil {
				return fmt.Errorf("BUG: %s", err)
			}
			defer funlock()

			st := fsconn.FileNotifyStoreCache(frev.Inode(), off, blkdata)
			if st != fuse.OK {
				return fmt.Errorf("BUG: %s: store cache: %s", frev.path(), st)
			}

			return nil
		}()
		if err != nil {
			log.Errorf("%s: invalidate blk #%d: %s (ignoring, but reading @revX/bigfile will be slow)", f.path(), blk, err)
		}
	}

	// invalidate file/head/data[blk] in OS file cache.
	st := fsconn.FileNotify(f.Inode(), off, blksize)
	if st != fuse.OK {
		return syscall.Errno(st)
	}

	return nil
}

// invalidateAttr invalidates file attributes in kernel cache.
//
// complements invalidateBlk and is used to invalidate file size.
// called with zheadMu wlocked.
func (f *BigFile) invalidateAttr() (err error) {
	defer xerr.Contextf(&err, "%s: invalidate attr", f.path())
	fsconn := gfsconn
	st := fsconn.FileNotify(f.Inode(), -1, -1) // metadata only
	if st != fuse.OK {
		return syscall.Errno(st)
	}
	return nil
}

// invalidateAll invalidates file attributes and all file data in kernel cache.
//
// complements invalidateAttr and invalidateBlk and is used to completely reset
// kernel file cache on ΔFtail epoch.
// called with zheadMu wlocked.
func (f *BigFile) invalidateAll() (err error) {
	defer xerr.Contextf(&err, "%s: invalidate all", f.path())
	fsconn := gfsconn
	st := fsconn.FileNotify(f.Inode(), 0, -1) // metadata + all data
	if st != fuse.OK {
		return syscall.Errno(st)
	}
	return nil
}


// lockRevFile makes sure inode ID of /@<rev>/bigfile/<fid> is known to kernel
// and won't change until unlock.
//
// We need node ID to be know to the kernel, when we need to store data into
// file's kernel cache - if the kernel don't have the node ID for the file in
// question, FileNotifyStoreCache will just fail.
//
// For kernel to know the inode lockRevFile issues regular filesystem lookup
// request which goes to kernel and should go back to wcfs. It is thus not safe
// to use lockRevFile from under FUSE request handler as doing so might deadlock.
//
// Caller must call unlock when inode ID is no longer required to be present.
// It is safe to simultaneously call multiple lockRevFile with the same arguments.
func (root *Root) lockRevFile(rev zodb.Tid, fid zodb.Oid) (_ *BigFile, unlock func(), err error) {
	fsconn := gfsconn

	frevpath := fmt.Sprintf("@%s/bigfile/%s", rev, fid) // relative to fs root for now
	defer xerr.Contextf(&err, "/: lockRevFile %s", frevpath)

	// open through kernel
	frevospath := gmntpt + "/" + frevpath // now starting from OS /
	f, err := os.Open(frevospath)
	if err != nil {
		return nil, nil, err
	}

	xfrev := fsconn.LookupNode(root.Inode(), frevpath)
	// must be !nil as open succeeded
	return xfrev.Node().(*BigFile), func() { f.Close() }, nil
}

// -------- 7) FUSE read(#blk) --------

// /(head|<rev>)/bigfile/<bigfileX> -> Read serves reading bigfile data.
func (f *BigFile) Read(_ nodefs.File, dest []byte, off int64, fctx *fuse.Context) (fuse.ReadResult, fuse.Status) {
	f.head.zheadMu.RLock()         // TODO +fctx to cancel
	defer f.head.zheadMu.RUnlock()

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
// called with head.zheadMu rlocked.
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
	blkdata, treepath, blkcov, zblk, blkrevMax, err := f.zfile.LoadBlk(ctx, blk)

	// head/ - update δFtail + pin watchers
	if f.head.rev == 0 && err == nil {
		// update δFtail index
		// see "3) for */head/data the following invariant is maintained..."
		δFtail := f.head.bfdir.δFtail
		δFtail.Track(f.zfile, blk, treepath, blkcov, zblk)

		// we have the data - it can be used after watchers are updated
		err = f.readPinWatchers(ctx, blk, blkrevMax)
		if err != nil {
			blkdata = nil
		}
	}

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
	head := f.head

	// rlock zheadMu and make sure zwatcher is not asking us to pause.
	// if it does - wait for a safer time not to deadlock.
	// see notes.txt -> "Kernel locks page on read/cache store/..." for details.
retry:
	for {
		head.zheadMu.RLock()
		// help zwatcher if it asks us to pause uploadings, so it can
		// take zheadMu wlocked without deadlocks.
		if head.pauseOSCacheUpload {
			ready := head.continueOSCacheUpload
			head.zheadMu.RUnlock()
			<-ready
			continue retry
		}

		break
	}

	// zheadMu rlocked.
	// zwatcher is not currently trying to pause OS cache uploads.

	// check if this block was already invalidated by zwatcher.
	// if so don't upload the block into OS cache.
	f.loadMu.Lock()
	loading_ := f.loading[blk]
	f.loadMu.Unlock()
	if loading != loading_ {
		head.zheadMu.RUnlock()
		return
	}

	oid := f.zfile.POid()

	// signal to zwatcher not to run while we are performing the upload.
	// upload with released zheadMu so that zwatcher can lock it even if to
	// check inflightOSCacheUploads status.
	atomic.AddInt32(&head.inflightOSCacheUploads, +1)
	head.zheadMu.RUnlock()

	st := gfsconn.FileNotifyStoreCache(f.Inode(), blk*f.blksize, loading.blkdata)

	f.loadMu.Lock()
	bug := (loading != f.loading[blk])
	if !bug {
		delete(f.loading, blk)
	}
	f.loadMu.Unlock()

	// signal to zwatcher that we are done and it can continue.
	atomic.AddInt32(&head.inflightOSCacheUploads, -1)

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

// -------- isolation protocol notification/serving --------
//
// (see "7.2) for all registered client@at watchers ...")

const _traceIso = false
func traceIso(format string, argv ...interface{}) {
	if !_traceIso {
		return
	}
	log.InfoDepth(1, fmt.Sprintf(format, argv...))
}

// pin makes sure that file[blk] on client side is the same as of @rev state.
//
// rev = zodb.TidMax means @head; otherwise rev must be ≤ w.at and there must
// be no rev_next changing file[blk]: rev < rev_next ≤ w.at.
//
// Pinning works under WatchLink.serveCtx + pinTimeout instead of explicitly
// specified context because pinning is critical operation whose failure leads
// to client being SIGBUS'ed and so pinning should not be interrupted arbitrarily.
//
// Corresponding watchlink is shutdown on any error.
//
// No error is returned as the only error that pin cannot handle itself inside
// is considered to be fatal and the filesystem is switched to EIO mode on that.
// See badPinKill documentation for details.
//
// pin is invoked by BigFile.readPinWatchers . It is called with atMu rlocked.
func (w *Watch) pin(blk int64, rev zodb.Tid) {
	w._pin(w.link.serveCtx, blk, rev)
}

// _pin serves pin and is also invoked directly by WatchLink.setupWatch .
//
// It is invoked with ctx being either WatchLink.serveCtx or descendant of it.
// In all cases it is called with atMu rlocked.
func (w *Watch) _pin(ctx context.Context, blk int64, rev zodb.Tid) {
	if ctx.Err() != nil {
		return // don't enter pinWG if watchlink is down
	}
	w.link.pinWG.Add(1)
	defer w.link.pinWG.Done()

	ctx, cancel := context.WithTimeout(ctx, groot.pinTimeout)
	defer cancel()

	err := w.__pin(ctx, blk, rev)
	if err != nil {
		w.link.shutdown(err)
	}
}

// PinError indicates to WatchLink shutdown that pinning a block failed and so
// badPinKill needs to be run.
type PinError struct {
	blk int64
	rev zodb.Tid
	err error
}

func (e *PinError) Error() string {
	return fmt.Sprintf("pin #%d @%s: %s", e.blk, isoRevstr(e.rev), e.err)
}

func (e *PinError) Unwrap() error {
	return e.err
}

func (w *Watch) __pin(ctx context.Context, blk int64, rev zodb.Tid) (err error) {
	defer func() {
		if err != nil {
			err = &PinError{blk, rev, err}
		}
	}()

	foid := w.file.zfile.POid()

	if !(rev == zodb.TidMax || rev <= w.at) {
		panicf("f<%s>: wlink%d: pin #%d @%s: watch.at (%s) < rev",
			foid, w.link.id, blk, rev, w.at)
	}

	w.pinnedMu.Lock()

	// check/wait for previous/simultaneous pin.
	// (pin could be called simultaneously e.g. by setupWatch and readPinWatchers)
	for {
		blkpin := w.pinned[blk]
		if blkpin == nil {
			break
		}

		w.pinnedMu.Unlock()
		<-blkpin.ready // TODO +ctx cancel

		if blkpin.rev == rev {
			// already pinned
			// (e.g. os cache for block was evicted and read called the second time)
			return blkpin.err
		}

		// relock the watch and check that w.pinned[blk] is the same. Retry if it is not.
		// ( w.pinned[blk] could have changed while w.mu was not held e.g. by
		//   simultaneous setupWatch if we were called by readPinWatchers )
		w.pinnedMu.Lock()
		if blkpin == w.pinned[blk] {
			if blkpin.rev == zodb.TidMax {
				w.pinnedMu.Unlock()
				panicf("f<%s>: wlink%d: pinned[#%d] = @head", foid, w.link.id, blk)
			}
			break
		}
	}

	// w.pinnedMu locked & previous pin is either nil or completed and its .rev != rev
	// -> setup new pin state
	blkpin := &blkPinState{rev: rev, ready: make(chan struct{})}
	w.pinned[blk] = blkpin

	// perform IO without w.pinnedMu
	w.pinnedMu.Unlock()
	groot.stats.pin.Add(1)
	ack, err := w.link.sendReq(ctx, fmt.Sprintf("pin %s #%d @%s", foid, blk, isoRevstr(rev)))
	w.pinnedMu.Lock()

	// check IO reply & verify/signal blkpin is ready
	defer func() {
		if rev == zodb.TidMax {
			delete(w.pinned, blk)
		}

		w.pinnedMu.Unlock()
		close(blkpin.ready)
	}()


	if err != nil {
		blkpin.err = err
		return err
	}

	if ack != "ack" {
		blkpin.err = fmt.Errorf("expect %q; got %q", "ack", ack)
		return blkpin.err
	}

	if blkpin != w.pinned[blk] {
		blkpin.err = fmt.Errorf("BUG: pinned[#%d] mutated while doing IO", blk)
		panicf("f<%s>: wlink%d: %s", foid, w.link.id, blkpin.err)
	}

	return nil
}

// badPinKill is invoked by shutdown to kill client that did not handle pin
// notification correctly and in time.
//
// Because proper pin handling is critical for safety it is considered to be a
// fatal error if the client could not be killed as wcfs no longer can
// continue to provide correct uncorrupted data to it. The filesystem is
// switched to EIO mode in such case.
func (wlink *WatchLink) badPinKill(reason error) {
	pid := wlink.client.Pid

	logf := func(format string, argv ...any) {
		emsg := fmt.Sprintf("pid%d: ", pid)
		emsg += fmt.Sprintf(format, argv...)
		log.Error(emsg)
	}
	logf("client failed to handle pin notification correctly and timely in %s: %s", groot.pinTimeout, reason)
	logf("-> killing it because else 1) all other clients will remain stuck, and 2) we no longer can provide correct data to the faulty client.")
	logf(`   (see "Protection against slow or faulty clients" in wcfs description for details)`)

	err := wlink._badPinKill()
	if err != nil {
		logf("failed to kill it: %s", err)
		logf("this is major unexpected event.")
		fatalEIO()
	}

	logf("terminated")
	groot.stats.pinkill.Add(1)
}

func (wlink *WatchLink) _badPinKill() error {
	client := wlink.client
	pid    := client.Pid

	// time budget for pin + wait + fatal-notify + kill = pinTimeout + 1 + 1/3·pinTimeout
	//                                                  < 2  ·pinTimeout      if pinTimeout > 3/2
	//
	// NOTE wcfs_faultyprot_test.py waits for 2·pinTimeout to reliably
	//      detect whether client was killed or not.
	timeout := groot.pinTimeout/3
	ctx := context.Background()
	ctx1, cancel := context.WithTimeout(ctx, timeout*1/2)
	defer cancel()

	ctx2, cancel := context.WithTimeout(ctx, timeout*2/2)
	defer cancel()

	//	SIGBUS => wait for some time; if still alive => SIGKILL
	// TODO kirr: "The kernel then sends SIGBUS on such case with the details about
	// access to which address generated this error going in si_addr field of
	// siginfo structure. It would be good if we can mimic that behaviour to a
	// reasonable extent if possible."
	log.Errorf("pid%d: <- SIGBUS", pid)
	err := client.Signal(syscall.SIGBUS)
	if err != nil {
		return err
	}

	ok, err := waitProcessEnd(ctx1, client)
	if err != nil && !errors.Is(err, ctx1.Err()) {
		return err
	}
	if ok {
		return nil
	}

	log.Errorf("pid%d:    is still alive after SIGBUS", pid)
	log.Errorf("pid%d: <- SIGKILL", pid)
	err = client.Signal(syscall.SIGKILL)
	if err != nil {
		return err
	}

	ok, err = waitProcessEnd(ctx2, client)
	if err != nil && !errors.Is(err, ctx2.Err()) {
		return err
	}
	if ok {
		return nil
	}

	err = fmt.Errorf("is still alive after SIGKILL")
	log.Errorf("pid%d:    %s", pid, err)
	return err
}

// readPinWatchers complements readBlk: it sends `pin blk` for watchers of the file
// after a block was loaded from ZODB but before block data is returned to kernel.
//
// See "7.2) for all registered client@at watchers ..."
//
// Must be called only for f under head/
// Must be called with f.head.zheadMu rlocked.
func (f *BigFile) readPinWatchers(ctx context.Context, blk int64, blkrevMax zodb.Tid) (err error) {
	defer xerr.Context(&err, "pin watchers") // f.path and blk is already put into context by readBlk

	// only head/ is being watched for
	if f.head.rev != 0 {
		panic("BUG: readPinWatchers: called for file under @revX/")
	}

	//fmt.Printf("S: read #%d -> pin watchers (#%d)\n", blk, len(f.watchTab))

	// make sure that file[blk] on clients side stays as of @w.at state.

	// try to use blkrevMax only as the first cheap criteria to skip updating watchers.
	// This is likely to be the case, since most watchers should be usually close to head.
	// If using blkrevMax only turns out to be not sufficient, we'll
	// consult δFtail, which might involve recomputing it.
	δFtail := f.head.bfdir.δFtail
	blkrev := blkrevMax
	blkrevRough := true

	wg := xsync.NewWorkGroup(ctx)
	defer func() {
		err2 := wg.Wait()
		if err == nil {
			err = err2
		}
	}()

	f.watchMu.RLock()
	defer f.watchMu.RUnlock()
	for w := range f.watchTab {
		w := w

		// make sure w.at stays unchanged while we prepare and pin the block
		w.atMu.RLock()

		// the block is already covered by @w.at database view
		if blkrev <= w.at {
			w.atMu.RUnlock()
			continue
		}

		// if blkrev is rough estimation and that upper bound is > w.at
		// we have to recompute ~exact file[blk] revision @head.
		if blkrevRough {
			// unlock atMu while we are (re-)calculating blkrev
			// we'll relock atMu again and recheck blkrev vs w.at after.
			w.atMu.RUnlock()

			var err error
			blkrev, _, err = δFtail.BlkRevAt(ctx, f.zfile, blk, f.head.zconn.At())
			if err != nil {
				return err
			}
			blkrevRough = false

			w.atMu.RLock()
			if blkrev <= w.at {
				w.atMu.RUnlock()
				continue
			}
		}

		// the block is newer - find out its revision as of @w.at and pin to that.
		//
		// We don't pin to w.at since if we would do so for several clients,
		// and most of them would be on different w.at - cache of the file will
		// be lost. Via pinning to particular block revision, we make sure the
		// revision to pin is the same on all clients, and so file cache is shared.
		wg.Go(func(ctx context.Context) error {
			defer w.atMu.RUnlock()
			pinrev, _, err := δFtail.BlkRevAt(ctx, f.zfile, blk, w.at)
			if err != nil {
				return err
			}
			//fmt.Printf("S: read #%d: watch @%s: pin -> @%s\n", blk, w.at, pinrev)

			// NOTE we do not propagate context to pin. Ideally update
			// watchers should be synchronous, and in practice we just use 30s timeout.
			// A READ interrupt should not cause watch update failure.
			w.pin(blk, pinrev) // only fatal error
			return nil
		})
	}

	return nil
}

// setupWatch sets up or updates a Watch when client sends `watch <file> @<at>` request.
//
// It sends "pin" notifications; final "ok" or "error" must be sent by caller.
func (wlink *WatchLink) setupWatch(ctx context.Context, foid zodb.Oid, at zodb.Tid) (err error) {
	defer xerr.Contextf(&err, "setup watch f<%s> @%s", foid, at)
	head := wlink.head
	bfdir := head.bfdir

	// wait for zhead.At ≥ at
	if at != zodb.InvalidTid {
		err = head.zheadWait(ctx, at)
		if err != nil {
			return err
		}
	}

	// make sure zhead.At stays unchanged while we are preparing the watch
	// (see vvv e.g. about unpin to @head for why it is needed)
	head.zheadMu.RLock()
	defer head.zheadMu.RUnlock()
	headAt := head.zconn.At()

	// TODO better ctx = transaction.PutIntoContext(ctx, txn)
	ctx, cancel := xcontext.Merge(ctx, head.zconn.TxnCtx)
	defer cancel()

	if at != zodb.InvalidTid && at < bfdir.δFtail.Tail() {
		return fmt.Errorf("too far away back from head/at (@%s); δt = %s",
			headAt, headAt.Time().Sub(at.Time().Time))
	}

	wlink.byfileMu.Lock()

	// if watch was already established - we need to update it
	w := wlink.byfile[foid]
	if w == nil {
		// watch was not previously established - set it up anew
		bfdir.fileMu.Lock()
		f := bfdir.fileTab[foid]
		bfdir.fileMu.Unlock()
		if f == nil {
			wlink.byfileMu.Unlock()
			// by "isolation protocol" watch is setup after data file was opened
			return fmt.Errorf("file not yet known to wcfs or is not a ZBigFile")
		}

		w = &Watch{
			link:   wlink,
			file:   f,
			at:     at,
			pinned: make(map[int64]*blkPinState),
		}
	}

	// allow only 1 setupWatch to run simultaneously for particular file
	w.setupMu.Lock()
	defer w.setupMu.Unlock()

	f := w.file
	f.watchMu.Lock()

	// at="-" (InvalidTid) means "remove the watch"
	if at == zodb.InvalidTid {
		delete(wlink.byfile, foid)
		delete(f.watchTab, w)
		f.watchMu.Unlock()
		wlink.byfileMu.Unlock()
		return nil
	}

	// request exclusive access to the watch to change .at and compute pins.
	// The lock will be downgraded from W to R after pins computation is done.
	// Pins will be executed with atMu.R only - with the idea not to block
	// other clients that read-access the file simultaneously to setupWatch.
	w.atMu.Lock()

	// check at >= w.at
	// TODO(?) we might want to allow going back in history if we need it.
	if !(at >= w.at) {
		w.atMu.Unlock()
		f.watchMu.Unlock()
		wlink.byfileMu.Unlock()
		return fmt.Errorf("going back in history is forbidden")
	}

	// register w to f early, so that READs going in parallel to us
	// preparing and processing initial pins, also send pins to w for read
	// blocks. If we don't, we can miss to send pin to w for a freshly read
	// block which could have revision > w.at:
	//
	//                1   3    2   4
	//	─────.────x───o────x───x──────]──────────
	//           ↑                        ↑
	//          w.at                     head
	//
	// Here blocks #1, #2 and #4 were previously accessed, are thus tracked
	// by δFtail and are changed after w.at - they will be returned by vvv
	// δFtail query and pin-sent to w. Block #3 was not yet accessed but
	// was also changed after w.at . As head/file[#3] might be accessed
	// simultaneously to watch setup, and f.readBlk will be checking
	// f.watchTab; if w ∉ f.watchTab at that moment, w will miss to receive
	// pin for #3.
	//
	// NOTE for `unpin blk` to -> @head we can be sure there won't be
	// simultaneous `pin blk` request, because:
	//
	// - unpin means blk was previously pinned,
	// - blk was pinned means it is tracked by δFtail,
	// - if blk is tracked and δFtail says there is no δblk ∈ (at, head],
	//   there is indeed no blk change in that region,
	// - which means that δblk with rev > w.at might be only > head,
	// - but such δblk are processed with zhead wlocked and we keep zhead
	//   rlocked during pin setup.
	//
	//          δ                      δ
	//      ────x────.────────────]────x────
	//               ↑            ↑
	//              w.at         head
	//
	// - also: there won't be simultaneous READs that would need to be
	//   unpinned, because we update w.at to requested at early.
	w.at = at
	f.watchTab[w] = struct{}{}
	wlink.byfile[foid] = w
	f.watchMu.Unlock()
	wlink.byfileMu.Unlock()

	// TODO defer -> unregister watch if error

	// pin all tracked file blocks that were changed in (at, head] range.
	toPin := map[int64]zodb.Tid{} // blk -> @rev

	δFtail := bfdir.δFtail
	vδf, err := δFtail.SliceByFileRevEx(f.zfile, at, headAt, zdata.QueryOptions{
		// blk might be in δFtail because it is adjacent in
		// ZBigFile.blktab to another blk that was explicitly tracked.
		// We do not want to get those to avoid unnecessarily pinning
		// potentially more blocks than needed.
		//
		// wcfs tests also verify that only blocks that were previously
		// explicitly accessed are included into watch setup pins.
		OnlyExplicitlyTracked: true,
	})
	if err != nil {
		return err
	}
	for _, δfile := range vδf {
		if δfile.Epoch {
			// file epochs are currently forbidden (see watcher), so the only
			// case when we could see an epoch here is creation of
			// the file if w.at is before that time:
			//
			//              create file
			//      ────.────────x────────]────
			//          ↑                 ↑
			//         w.at              head
			//
			// but then the file should not be normally accessed in that case.
			//
			// -> reject such watches with an error
			return fmt.Errorf("file epoch detected @%s in between (at,head=@%s]", δfile.Rev, headAt)
		}
		for blk := range δfile.Blocks {
			_, already := toPin[blk]
			if already {
				continue
			}

			toPin[blk], _, err = δFtail.BlkRevAt(ctx, f.zfile, blk, at)
			if err != nil {
				return err
			}
		}
	}

	// if a block was previously pinned, but ∉ δ(at, head] -> unpin it to head.
	for blk, pinPrev := range w.pinned {
		// only 1 setupWatch can be run simultaneously for one file

		pinNew, pinning := toPin[blk]
		if !pinning {
			toPin[blk] = zodb.TidMax // @head
		}

		// TODO don't bother to spawn .pin goroutines if pin revision is the same ?
		// if pinNew == pinPrev.rev && ready(pinPrev.ready) && pinPrev.err == nil {
		// 	delete(toPin, blk)
		// }
		_ = pinPrev
		_ = pinNew
	}

	// downgrade atMu.W -> atMu.R to let other clients to access the file.
	// NOTE there is no primitive to do Wlock->Rlock atomically, but we are
	// ok with that since:
	//
	//   * wrt readPinWatchers we prepared everything to handle
	//     simultaneous pins from other reads.
	//   * wrt setupWatch we can still be sure that no another setupWatch
	//     started to run simultaneously during atMu.Unlock -> atMu.RLock
	//     because we still hold setupMu.
	//
	// ( for the reference: pygolang provides RWMutex.UnlockToRLock while go
	//   rejected it in golang.org/issues/38891 )
	w.atMu.Unlock()
	w.atMu.RLock()
	defer w.atMu.RUnlock()

	wg := xsync.NewWorkGroup(ctx)
	for blk, rev := range toPin {
		blk := blk
		rev := rev
		wg.Go(func(ctx context.Context) error {
			w._pin(ctx, blk, rev) // only fatal error
			return nil
		})
	}
	err = wg.Wait()
	if err != nil {
		return err // should not fail
	}

	return nil
}

// Open serves /head/watch opens.
func (wnode *WatchNode) Open(flags uint32, fctx *fuse.Context) (nodefs.File, fuse.Status) {
	node, err := wnode.open(flags, fctx)
	return node, err2LogStatus(err)
}

func (wnode *WatchNode) open(flags uint32, fctx *fuse.Context) (_ nodefs.File, err error) {
	defer xerr.Contextf(&err, "/head/watch: open")

	// TODO(?) check flags
	head := wnode.head

	// remember our client who opened the watchlink.
	// We will need to kill the client if it will be e.g. slow to respond to pin notifications.
	client, err := findAliveProcess(int(fctx.Caller.Pid))
	if err != nil {
		return nil, err
	}

	serveCtx, serveCancel := context.WithCancel(context.TODO() /*TODO ctx of wcfs running*/)
	wlink := &WatchLink{
		sk:          NewFileSock(),
		id:          atomic.AddInt32(&wnode.idNext, +1),
		head:        head,
		byfile:      make(map[zodb.Oid]*Watch),
		rxTab:       make(map[uint64]chan string),
		serveCtx:    serveCtx,
		serveCancel: serveCancel,
		down:        make(chan struct{}),
		client:      client,
	}

	head.wlinkMu.Lock()
	head.wlinkTab[wlink] = struct{}{}
	head.wlinkMu.Unlock()

	go wlink.serve(serveCtx)
	return wlink.sk.File(), nil
}

// shutdown shuts down communication over watchlink due to specified reason and
// marks the watchlink as no longer active.
//
// The client is killed if the reason is due to "failed to pin".
// Only the first shutdown call has the effect, but all calls wait for the
// actual shutdown to complete.
//
// NOTE shutdown can be invoked under atMu.R from pin.
func (wlink *WatchLink) shutdown(reason error) {
	wlink.down1.Do(func() {
		// mark wlink as down; this signals serve loop to exit and cancels all in-progress pins
		wlink.serveCancel()

		// give client a chance to be notified if shutdown was due to some logical error
		kill := false
		if reason != nil {
			_, kill = reason.(*PinError)
			emsg := "error: "
			if kill {
				emsg = "fatal: "
			}
			emsg += reason.Error()

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			_ = wlink.send(ctx, 0, emsg)
		}

		// kill client if shutdown is due to faulty pin handling
		if kill {
			wlink.badPinKill(reason) // only fatal error
		}

		// NOTE unregistering watches and wlink itself is done on serve exit, not
		//      here, to avoid AB-BA deadlock on atMu and e.g. WatchLink.byfileMu .
		//	It is ok to leave some watches still present in BigFile.watchTab
		//	until final cleanup because pin becomes noop on down watchlink.

		close(wlink.down)
	})

	<-wlink.down
}

// serve serves client initiated watch requests and routes client replies to
// wcfs initiated pin requests.
func (wlink *WatchLink) serve(ctx context.Context) {
	err := wlink._serve(ctx)
	if err != nil {
		log.Error(err)
	}
}

func (wlink *WatchLink) _serve(ctx context.Context) (err error) {
	defer xerr.Contextf(&err, "wlink %d: serve rx", wlink.id)

	// final watchlink cleanup is done on serve exit
	defer func() {
		// unregister all watches created on this wlink
		wlink.byfileMu.Lock()
		for _, w := range wlink.byfile {
			w.file.watchMu.Lock()
			delete(w.file.watchTab, w)
			w.file.watchMu.Unlock()
		}
		wlink.byfile = nil
		wlink.byfileMu.Unlock()

		// unregister wlink itself
		head := wlink.head
		head.wlinkMu.Lock()
		delete(head.wlinkTab, wlink)
		head.wlinkMu.Unlock()

		// close .sk
		// closing .sk.tx wakes up rx on client side.
		err2 := wlink.sk.Close()
		if err == nil {
			err = err2
		}

		// release client process
		wlink.client.Release()
	}()

	// watch handlers are spawned in dedicated workgroup
	//
	// Pin handlers are run either inside - for pins run from setupWatch, or,
	// for pins run from readPinWatchers, under wlink.pinWG.
	// Upon serve exit we cancel them all and wait for their completion.
	wg := xsync.NewWorkGroup(ctx)
	defer func() {
		// cancel all watch and pin handlers on both error and ok return.
		//
		// For ok return, when we received "bye", we want to cancel
		// in-progress pin handlers without killing clients. That's why
		// we call shutdown ourselves.
		//
		// For error return, we want any in-progress, and so will
		// become failed, pin handler to result in corresponding client
		// to become killed. That's why we trigger only cancel
		// ourselves and let failed pin handlers to invoke shutdown
		// with their specific reason.
		//
		// NOTE this affects pin handlers invoked by both setupWatch and readPinWatchers.
		if err != nil {
			wlink.serveCancel()
		} else {
			wlink.shutdown(nil)
		}

		// wait for setupWatch and pin handlers spawned from it to complete
		err2 := wg.Wait()
		if err == nil {
			err = err2
		}

		// wait for all other pin handlers to complete
		wlink.pinWG.Wait()

		// make sure that shutdown is actually invoked if it was an
		// error and there were no in-progress pin handlers
		wlink.shutdown(err)
	}()

	// cancel main thread on any watch handler error
	ctx, mainCancel := context.WithCancel(ctx)
	defer mainCancel()
	wg.Go(func(ctx context.Context) error {
		// monitor is always canceled - either due to parent ctx cancel, error in workgroup,
		// or return from serve and running "cancel all watch handlers ..." above.
		<-ctx.Done()
		mainCancel()
		return nil
	})

	r := bufio.NewReader(xio.BindCtxR(wlink.sk, ctx))
	for {
		// NOTE r.Read is woken up by ctx cancel because wlink.sk implements xio.Reader natively
		l, err := r.ReadString('\n') // TODO limit accepted line len to prevent DOS
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			if errors.Is(err, ctx.Err()) {
				err = nil
			}
			return err
		}
		traceIso("S: wlink%d: rx: %q\n", wlink.id, l)

		stream, msg, err := parseWatchFrame(l)
		if err != nil {
			return err
		}

		// reply from client to wcfs
		reply := (stream % 2 == 0)
		if reply {
			wlink.rxMu.Lock()
			rxq := wlink.rxTab[stream]
			delete(wlink.rxTab, stream)
			wlink.rxMu.Unlock()

			if rxq == nil {
				return fmt.Errorf("%d: reply on unexpected stream", stream)
			}
			rxq <- msg
			continue
		}

		// client-initiated request

		// bye
		if msg == "bye" {
			return nil // deferred sk.Close will wake-up rx on client side
		}

		// watch ...
		wg.Go(func(ctx context.Context) error {
			return wlink.handleWatch(ctx, stream, msg)
		})
	}
}

// handleWatch handles watch request from client.
//
// returned error comes without full error prefix.
func (wlink *WatchLink) handleWatch(ctx context.Context, stream uint64, msg string) (err error) {
	defer xerr.Contextf(&err, "%d", stream)

	err = wlink._handleWatch(ctx, msg)
	reply := "ok"
	if err != nil {
		// logical error is reported back to client, but watch link remains live
		reply = fmt.Sprintf("error %s", err)
		err = nil
	}

	err = wlink.send(ctx, stream, reply)
	return err
}

func (wlink *WatchLink) _handleWatch(ctx context.Context, msg string) error {
	foid, at, err := parseWatch(msg)
	if err != nil {
		return err
	}

	err = wlink.setupWatch(ctx, foid, at)
	return err
}

// sendReq sends wcfs-originated request to client and returns client response.
func (wlink *WatchLink) sendReq(ctx context.Context, req string) (reply string, err error) {
	defer xerr.Context(&err, "sendReq") // wlink is already put into ctx by caller

	var stream uint64
	for stream == 0 {
		stream = atomic.AddUint64(&wlink.reqNext, +2)
	}

	rxq := make(chan string, 1)
	wlink.rxMu.Lock()
	_, already := wlink.rxTab[stream]
	if !already {
		wlink.rxTab[stream] = rxq
	}
	wlink.rxMu.Unlock()
	if already {
		panic("BUG: to-be-sent stream is present in rxtab")
	}

	defer func() {
		if err != nil {
			// remove rxq from rxTab
			// ( _serve could have already deleted it if unexpected
			//   reply came to the stream, but no other rxq should
			//   have registered on the [stream] slot )
			wlink.rxMu.Lock()
			delete(wlink.rxTab, stream)
			wlink.rxMu.Unlock()

			// no need to drain rxq - it was created with cap=1
		}
	}()

	err = wlink.send(ctx, stream, req)
	if err != nil {
		return "", err
	}

	defer xerr.Contextf(&err, "waiting for reply")
	select {
	case <-ctx.Done():
		return "", ctx.Err()

	case reply = <-rxq:
		return reply, nil
	}
}

// send sends a message to client over specified stream ID.
//
// Multiple send can be called simultaneously; send serializes writes.
func (wlink *WatchLink) send(ctx context.Context, stream uint64, msg string) (err error) {
	defer xerr.Contextf(&err, "send .%d", stream) // wlink is already put into ctx by caller

	// assert '\n' not in msg
	if strings.ContainsRune(msg, '\n') {
		panicf("BUG: msg contains \\n  ; msg: %q", msg)
	}

	wlink.txMu.Lock()
	defer wlink.txMu.Unlock()

	pkt := []byte(fmt.Sprintf("%d %s\n", stream, msg))
	traceIso("S: wlink%d: tx: %q\n", wlink.id, pkt)
	_, err = wlink.sk.Write(ctx, pkt)
	if err != nil {
		return err
	}

	return nil
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

	bfdir.head.zheadMu.RLock()         // TODO +fctx -> cancel
	defer bfdir.head.zheadMu.RUnlock()

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
		δFtail:  nil, // δFtail not needed/used for @revX/
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
//
// head.zheadMu must be locked.
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

	size, sizePath, blkCov, err := zfile.Size(ctx)
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

	// only head/ needs δFtail and watches.
	if head.rev == 0 {
		// see "3) for */head/data the following invariant is maintained..."
		head.bfdir.δFtail.Track(f.zfile, -1, sizePath, blkCov, nil)

		f.watchTab = make(map[*Watch]struct{})
	}

	return f, nil
}


// ---- misc ---

// /(head|<rev>)/at -> readAt serves read.
func (h *Head) readAt(fctx *fuse.Context) ([]byte, error) {
	h.zheadMu.RLock()         // TODO +fctx -> cancel
	defer h.zheadMu.RUnlock()

	return []byte(h.zconn.At().String()), nil
}

// /(head|<rev>)/ -> Getattr serves stat.
func (head *Head) GetAttr(out *fuse.Attr, _ nodefs.File, fctx *fuse.Context) fuse.Status {
	at := head.rev
	if at == 0 {
		head.zheadMu.RLock()   // TODO +fctx -> cancel
		at = head.zconn.At()
		head.zheadMu.RUnlock()
	}
	t := at.Time().Time

	out.Mode = fuse.S_IFDIR | 0555
	out.SetTimes(/*atime=*/nil, /*mtime=*/&t, /*ctime=*/&t)
	return fuse.OK
}

// /(head|<rev>)/bigfile/<bigfileX> -> Getattr serves stat.
func (f *BigFile) GetAttr(out *fuse.Attr, _ nodefs.File, fctx *fuse.Context) fuse.Status {
	f.head.zheadMu.RLock()         // TODO +fctx -> cancel
	defer f.head.zheadMu.RUnlock()

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




// FIXME groot/gfsconn is tmp workaround for lack of way to retrieve FileSystemConnector from nodefs.Inode
// TODO:
//	- Inode += .Mount() -> nodefs.Mount
//	- Mount:
//		.Root()		-> root Inode of the fs
//		.Connector()	-> FileSystemConnector through which fs is mounted
var groot   *Root
var gfsconn *nodefs.FileSystemConnector

// root of the filesystem is mounted here.
//
// we need to talk to kernel and lookup @<rev>/bigfile/<fid> before uploading
// data to kernel cache there. Referencing root of the filesystem via path is
// vulnerable to bugs wrt e.g. `mount --move` and/or mounting something else
// over wcfs. However keeping opened root fd will prevent wcfs to be unmounted,
// so we still have to reference the root via path.
var gmntpt string

// debugging
var gdebug = struct {
	// .wcfs/debug/zhead opens
	zheadSockTabMu sync.Mutex
	zheadSockTab   map[*FileSock]struct{}
}{}

func init() {
	gdebug.zheadSockTab = make(map[*FileSock]struct{})
}

// _wcfs_debug_Zhead serves .wcfs/debug/zhead .
type _wcfs_debug_Zhead struct {
	fsNode
}

// _wcfs_debug_ZheadH serves .wcfs/debug/zhead opens.
type _wcfs_debug_ZheadH struct {
	nodefs.File  // = .sk.file
	sk *FileSock
}

func (*_wcfs_debug_Zhead) Open(flags uint32, fctx *fuse.Context) (nodefs.File, fuse.Status) {
	// TODO(?) check flags
	sk := NewFileSock()
	sk.CloseRead()
	zh := &_wcfs_debug_ZheadH{
		File: sk.file,
		sk:   sk,
	}

	gdebug.zheadSockTabMu.Lock()      // TODO +fctx -> cancel
	gdebug.zheadSockTab[sk] = struct{}{}
	gdebug.zheadSockTabMu.Unlock()

	return WithOpenStreamFlags(zh), fuse.OK
}

func (zh *_wcfs_debug_ZheadH) Release() {
	gdebug.zheadSockTabMu.Lock()
	delete(gdebug.zheadSockTab, zh.sk)
	gdebug.zheadSockTabMu.Unlock()

	zh.File.Release()
}


// _wcfs_Stats serves .wcfs/stats reads.
//
// In the output:
//
//   - entries that start with capital letter, e.g. "Watch", indicate current
//     number of named instances. This numbers can go up and down.
//
//   - entries that start with lowercase letter, e.g. "pin", indicate number of
//     named event occurrences. This numbers are cumulative counters and should
//     never go down.
func _wcfs_Stats(fctx *fuse.Context) ([]byte, error) {
	stats := ""
	num := func(name string, value any) {
		stats += fmt.Sprintf("%s\t: %d\n", name, value)
	}

	root  := groot
	head  := root.head
	bfdir := head.bfdir

	// dump information detected at runtime
	root.revMu.Lock()
	lenRevTab := len(root.revTab)
	root.revMu.Unlock()

	head.wlinkMu.Lock()
	ΣWatch     := 0
	ΣPinnedBlk := 0
	lenWLinkTab := len(head.wlinkTab)
	for wlink := range head.wlinkTab {
		wlink.byfileMu.Lock()
		ΣWatch += len(wlink.byfile)
		for _, w := range wlink.byfile {
			w.atMu.RLock()
			w.pinnedMu.Lock()
			ΣPinnedBlk += len(w.pinned)
			w.pinnedMu.Unlock()
			w.atMu.RUnlock()
		}
		wlink.byfileMu.Unlock()
	}
	head.wlinkMu.Unlock()

	head.zheadMu.RLock()
	bfdir.fileMu.Lock()
	lenFileTab := len(bfdir.fileTab)
	bfdir.fileMu.Unlock()
	head.zheadMu.RUnlock()

	gdebug.zheadSockTabMu.Lock()
	lenZHeadSockTab := len(gdebug.zheadSockTab)
	gdebug.zheadSockTabMu.Unlock()

	num("BigFile",     lenFileTab)		// # of head/BigFile
	num("RevHead",     lenRevTab)		// # of @revX/ directories
	num("ZHeadLink",   lenZHeadSockTab)	// # of open .wcfs/debug/zhead handles
	num("WatchLink",   lenWLinkTab)		// # of open watchlinks
	num("Watch",       ΣWatch)		// # of setup watches
	num("PinnedBlk",   ΣPinnedBlk)		// # of currently on-client pinned blocks

	// dump information collected in root.stats
	s := root.stats
	num("pin",         s.pin.Load())
	num("pinkill",     s.pinkill.Load())

	return []byte(stats), nil
}

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
	debug := flag.Bool("debug", false, "enable debugging features")
	tracefuse := flag.Bool("trace.fuse", false, "trace FUSE exchange")
	autoexit := flag.Bool("autoexit", false, "automatically stop service when there is no client activity")
	pintimeout := flag.Duration("pintimeout", 30*time.Second, "clients are killed if they do not handle pin notification in pintimeout time")

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
	if *debug || *tracefuse {
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

	zwatchq := make(chan zodb.Event)
	at0 := zstor.AddWatch(zwatchq)
	defer zstor.DelWatch(zwatchq)

	// TODO consider using zodbCacheControl for all connections
	// ( in addition to zhead, historic connections - that are used to access @rev -
	//   also need to traverse BigFile.blktab btree )
	zdb := zodb.NewDB(zstor, &zodb.DBOptions{})
	defer xclose(zdb)
	zhead, err := xzodb.ZOpen(ctx, zdb, &zodb.ConnOptions{
		At: at0,

		// preserve zhead.cache across several transactions.
		// see "ZODB cache control"
		NoPool: true,
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
		wlinkTab: make(map[*WatchLink]struct{}),
		hwait:    make(map[hwaiter]struct{}),
	}

	wnode := &WatchNode{
		fsNode: newFSNode(fSticky),
		head:   head,
	}

	bfdir := &BigFileDir{
		fsNode:   newFSNode(fSticky),
		head:     head,
		fileTab:  make(map[zodb.Oid]*BigFile),
		δFtail:   zdata.NewΔFtail(zhead.At(), zdb),
	}
	head.bfdir = bfdir

	root := &Root{
		fsNode:     newFSNode(fSticky),
		zstor:      zstor,
		zdb:        zdb,
		head:       head,
		revTab:     make(map[zodb.Tid]*Head),
		pinTimeout: *pintimeout,
		stats:      &Stats{},
	}

	opts := &fuse.MountOptions{
		FsName: zurl,
		Name:   "wcfs",

		// We retrieve kernel cache in ZBlk.blksize chunks, which are 2MB in size.
		MaxWrite:      2*1024*1024,

		// TODO(?) tune MaxReadAhead? MaxBackground?

		// OS cache that we populate with bigfile data is precious;
		// we explicitly propagate ZODB invalidations into file invalidations.
		ExplicitDataCacheControl: true,

		DisableXAttrs: true,        // we don't use
		Debug:         *tracefuse,  // go-fuse "Debug" is mostly logging FUSE messages
	}

	fssrv, fsconn, err := mount(mntpt, root, opts)
	if err != nil {
		return err
	}
	groot   = root   // FIXME temp workaround (see ^^^)
	gfsconn = fsconn // FIXME ----//----
	gmntpt  = mntpt

	// we require proper pagecache control (added to Linux 2.6.36 in 2010)
	kinit := fssrv.KernelSettings()
	kfuse := fmt.Sprintf("kernel FUSE (API %d.%d)", kinit.Major, kinit.Minor)
	supports := kinit.SupportsNotify
	if !(supports(fuse.NOTIFY_STORE_CACHE) && supports(fuse.NOTIFY_RETRIEVE_CACHE)) {
		return fmt.Errorf("%s does not support pagecache control", kfuse)
	}
	// make a bold warning if kernel does not support explicit cache invalidation
	// (patch is in Linux 5.2+; see notes.txt -> "Notes on OS pagecache control")
	if kinit.Flags & fuse.CAP_EXPLICIT_INVAL_DATA == 0 {
		w1 := fmt.Sprintf("%s does not support explicit data cache invalidation", kfuse)
		w2 := "-> performance will be AWFUL."
		w3 := "-> you need kernel which includes git.kernel.org/linus/ad2ba64dd489."
		w4 := "-> (Linux 5.2+, or nxd-fuse-dkms package installed from navytux.spb.ru/pkg)"
		log.Error(w1); log.Error(w2); log.Error(w3); log.Error(w4)
		fmt.Fprintf(os.Stderr, "W: wcfs: %s\nW: wcfs: %s\nW: wcfs: %s\nW: wcfs: %s\n", w1, w2, w3, w4)
	}

	// add entries to /
	mkdir(root, "head", head)
	mkdir(head, "bigfile", bfdir)
	mkfile(head, "at", NewSmallFile(head.readAt)) // TODO mtime(at) = tidtime(at)
	mkfile(head, "watch", wnode)

	// information about wcfs itself
	_wcfs := newFSNode(fSticky)
	mkdir(root, ".wcfs", &_wcfs)
	mkfile(&_wcfs, "zurl", NewStaticFile([]byte(zurl)))
	mkfile(&_wcfs, "pintimeout", NewStaticFile([]byte(fmt.Sprintf("%.1f", float64(root.pinTimeout) / float64(time.Second)))))
	mkfile(&_wcfs, "stats", NewSmallFile(_wcfs_Stats)) // collected statistics

	// for debugging/testing
	if *debug {
		_wcfs_debug := newFSNode(fSticky)
		mkdir(&_wcfs, "debug", &_wcfs_debug)

		// .wcfs/debug/zhead - special file channel that sends zhead.at.
		//
		// If a user opens it, it will start to get tids of through which
		// zhead.at was, starting from the time when .wcfs/debug/zhead was opened.
		// There can be multiple openers. Once opened, the file must be read,
		// as wcfs blocks waiting for data to be read when processing
		// invalidations.
		//
		// zhead is debug-only since it is easy to deadlock wcfs by not
		// reading from opened zhead handle.
		mkfile(&_wcfs_debug, "zhead", &_wcfs_debug_Zhead{
			fsNode: newFSNode(fSticky),
		})
	}


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
	// run zwatcher and wait for it to complete.
	// zwatcher completes either normally - due to filesystem unmount, or fails.
	// if zwatcher fails - switch filesystem to return EIO instead of stale data.
	err = root.zwatcher(serveCtx, zwatchq)
	if errors.Cause(err) != context.Canceled {
		log.Error(err)
		log.Error("zwatcher failed")
		fatalEIO()
	}

	// wait for unmount
	// NOTE the kernel does not send FORGETs on unmount - but we don't need
	// to release left node resources ourselves, because it is just memory.
	<-serveCtx.Done()
	log.Infof("stop %q %q", mntpt, zurl)
	return nil
}
