// Copyright (C) 2018-2025  Nexedi SA and Contributors.
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

// Package wcfs provides WCFS client integrated with user-space virtual memory manager.
// See wcfs.h for package overview.


// Wcfs client organization
//
// Wcfs client provides to its users isolated bigfile views backed by data on
// WCFS filesystem. In the absence of Isolation property, wcfs client would
// reduce to just directly using OS-level file wcfs/head/f for a bigfile f. On
// the other hand there is a simple, but inefficient, way to support isolation:
// for @at database view of bigfile f - directly use OS-level file wcfs/@at/f.
// The latter works, but is very inefficient because OS-cache for f data is not
// shared in between two connections with @at1 and @at2 views. The cache is
// also lost when connection view of the database is resynced on transaction
// boundary. To support isolation efficiently, wcfs client uses wcfs/head/f
// most of the time, but injects wcfs/@revX/f parts into mappings to maintain
// f@at view driven by pin messages that wcfs server sends to client in
// accordance to WCFS isolation protocol(*).
//
// Wcfs server sends pin messages synchronously triggered by access to mmaped
// memory. That means that a client thread, that is accessing wcfs/head/f mmap,
// is completely blocked while wcfs server sends pins and waits to receive acks
// from all clients. In other words on-client handling of pins has to be done
// in separate thread, because wcfs server can also send pins to client that
// triggered the access.
//
// Wcfs client implements pins handling in so-called "pinner" thread(+). The
// pinner thread receives pin requests from wcfs server via watchlink handle
// opened through wcfs/head/watch. For every pin request the pinner finds
// corresponding Mappings and injects wcfs/@revX/f parts via Mapping._remmapblk
// appropriately.
//
// The same watchlink handle is used to send client-originated requests to wcfs
// server. The requests are sent to tell wcfs that client wants to observe a
// particular bigfile as of particular revision, or to stop watching it.
// Such requests originate from regular client threads - not pinner - via entry
// points like Conn.open, Conn.resync and FileH.close.
//
// Every FileH maintains fileh._pinned {} with currently pinned blk -> rev. This
// dict is updated by pinner driven by pin messages, and is used when either
// new fileh Mapping is created (FileH.mmap) or refreshed due to request from
// virtmem (Mapping.remmap_blk, see below).
//
// In wendelin.core a bigfile has semantic that it is infinite in size and
// reads as all zeros beyond region initialized with data. Memory-mapping of
// OS-level files can also go beyond file size, however accessing memory
// corresponding to file region after file.size triggers SIGBUS. To preserve
// wendelin.core semantic wcfs client mmaps-in zeros for Mapping regions after
// wcfs/head/f.size. Wcfs client restats wcfs/head/f at every transaction boundary
// (Conn.resync) and remembers f.size in FileH._headfsize for use during one
// transaction(%).
//
//
// Integration with wendelin.core virtmem layer
//
// Wcfs client integrates with virtmem layer to support virtmem handle
// dirtying pages of read-only base-layer that wcfs client provides via
// isolated Mapping. For wcfs-backed bigfiles every virtmem VMA is interlinked
// with Mapping:
//
//       VMA     -> BigFileH -> ZBigFile -----> Z
//        ↑↓                                    O
//      Mapping  -> FileH    -> wcfs server --> DB
//
// When a page is write-accessed, virtmem mmaps in a page of RAM in place of
// accessed virtual memory, copies base-layer content provided by Mapping into
// there, and marks that page as read-write.
//
// Upon receiving pin message, the pinner consults virtmem, whether
// corresponding page was already dirtied in virtmem's BigFileH (call to
// __fileh_page_isdirty), and if it was, the pinner does not remmap Mapping
// part to wcfs/@revX/f and just leaves dirty page in its place, remembering
// pin information in fileh._pinned.
//
// Once dirty pages are no longer needed (either after discard/abort or
// writeout/commit), virtmem asks wcfs client to remmap corresponding regions
// of Mapping in its place again via calls to Mapping.remmap_blk for previously
// dirtied blocks.
//
// The scheme outlined above does not need to split Mapping upon dirtying an
// inner page.
//
// See bigfile_ops interface (wendelin/bigfile/file.h) that explains base-layer
// and overlaying from virtmem point of view. For wcfs this interface is
// provided by small wcfs client wrapper in bigfile/file_zodb.cpp.
//
// --------
//
// (*) see wcfs.go documentation for WCFS isolation protocol overview and details.
// (+) currently, for simplicity, there is one pinner thread for each connection.
//     In the future, for efficiency, it might be reworked to be one pinner thread
//     that serves all connections simultaneously.
// (%) see _headWait comments on how this has to be reworked.


// Wcfs client locking organization
//
// Wcfs client needs to synchronize regular user threads vs each other and vs
// pinner. A major lock Conn.atMu protects updates to changes to Conn's view of
// the database. Whenever atMu.W is taken - Conn.at is changing (Conn.resync),
// and contrary whenever atMu.R is taken - Conn.at is stable (roughly speaking
// Conn.resync is not running).
//
// Similarly to wcfs.go(*) several locks that protect internal data structures
// are minor to Conn.atMu - they need to be taken only under atMu.R (to
// synchronize e.g. multiple fileh open running simultaneously), but do not
// need to be taken at all if atMu.W is taken. In data structures such locks
// are noted as follows
//
//      sync::Mutex xMu;    // atMu.W  |  atMu.R + xMu
//
// After atMu, Conn.filehMu protects registry of opened file handles
// (Conn._filehTab), and FileH.mmapMu protects registry of created Mappings
// (FileH.mmaps) and FileH.pinned.
//
// Several locks are RWMutex instead of just Mutex not only to allow more
// concurrency, but, in the first place for correctness: pinner thread being
// core element in handling WCFS isolation protocol, is effectively invoked
// synchronously from other threads via messages coming through wcfs server.
// For example Conn.resync sends watch request to wcfs server and waits for the
// answer. Wcfs server, in turn, might send corresponding pin messages to the
// pinner and _wait_ for the answer before answering to resync:
//
//        - - - - - -
//       |       .···|·····.        ---->   = request
//          pinner <------.↓        <····   = response
//       |           |   wcfs
//          resync -------^↓
//       |      `····|·····
//        - - - - - -
//       client process
//
// This creates the necessity to use RWMutex for locks that pinner and other
// parts of the code could be using at the same time in synchronous scenarios
// similar to the above. This locks are:
//
//      - Conn.atMu
//      - Conn.filehMu
//
// Note that FileH.mmapMu is regular - not RW - mutex, since nothing in wcfs
// client calls into wcfs server via watchlink with mmapMu held.
//
// To synchronize with virtmem layer, wcfs client takes and releases big
// virtmem lock around places that touch virtmem (calls to virt_lock and
// virt_unlock). Also virtmem calls several wcfs client entrypoints with
// virtmem lock already taken. Thus, to avoid AB-BA style deadlocks, wcfs
// client needs to take virtmem lock as the first lock, whenever it needs to
// take both virtmem lock, and another lock - e.g. atMu(%).
//
// The ordering of locks is:
//
//      virt_lock > Conn.atMu > Conn.filehMu > FileH.mmapMu
//
// The pinner takes the following locks:
//
//      - virt_lock
//      - wconn.atMu.R
//      - wconn.filehMu.R
//      - fileh.mmapMu (to read .mmaps  +  write .pinned)
//
//
// (*) see "Wcfs locking organization" in wcfs.go
// (%) see related comment in Conn.__pin1 for details.


// Handling of fork
//
// When a process calls fork, OS copies its memory and creates child process
// with only 1 thread. That child inherits file descriptors and memory mappings
// from parent. To correctly continue using Conn, FileH and Mappings, the child
// must recreate pinner thread and reconnect to wcfs via reopened watchlink.
// The reason here is that without reconnection - by using watchlink file
// descriptor inherited from parent - the child would interfere into
// parent-wcfs exchange and neither parent nor child could continue normal
// protocol communication with WCFS.
//
// For simplicity, since fork is seldomly used for things besides followup
// exec, wcfs client currently takes straightforward approach by disabling
// mappings and detaching from WCFS server in the child right after fork. This
// ensures that there is no interference into parent-wcfs exchange should child
// decide not to exec and to continue running in the forked thread. Without
// this protection the interference might come even automatically via e.g.
// Python GC -> PyFileH.__del__ -> FileH.close -> message to WCFS.


#include "wcfs_misc.h"
#include "wcfs.h"
#include "wcfs_watchlink.h"

#include <wendelin/bigfile/virtmem.h>
#include <wendelin/bigfile/ram.h>

#include <golang/errors.h>
#include <golang/fmt.h>
#include <golang/io.h>
#include <golang/time.h>

#include <algorithm>
#include <string>
#include <vector>

#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

using std::min;
using std::max;
using std::vector;


#define TRACE 0
#if TRACE
#  define trace(format, ...) log::Debugf(format, ##__VA_ARGS__)
#else
#  define trace(format, ...) do {} while (0)
#endif

// trace with op prefix taken from E.
#define etrace(format, ...) trace("%s", v(E(fmt::errorf(format, ##__VA_ARGS__))))


// wcfs::
namespace wcfs {

static error mmap_zero_into_ro(void *addr, size_t size);
static error mmap_efault_into(void *addr, size_t size);
static tuple<uint8_t*, error> mmap_ro(os::File f, off_t offset, size_t size);
static error mmap_into_ro(void *addr, size_t size, os::File f, off_t offset);

// _headWait waits till wcfs/head/at becomes ≥ at.
//
// _headWait is currently needed, because client stats wcfs/head/f to get f
// size assuming that f size only ↑. The assumption is not generally valid
// (e.g. f might be truncated = hole punched for block at tail), but holds true
// for now. However to get correct results wcfs/head/f has to be statt'ed
// _after_ wcfs view of the database becomes ≥ wconn.at.
//
// TODO extend isolation protocol to report f size as of @at database state at
// watch init/update(*). This way there won't be need for headWait as correct
// file size @at will be returned by wcfs itself, which will also work if
// wcfs/head/f size is changed arbitrarily.
//
// (*) equivalent might be to send something like "pin #<bsize>.. Z" (pin
//     blocks bsize till ∞ to zeros).
error WCFS::_headWait(zodb::Tid at) {
    WCFS& wc = *this;
    xerr::Contextf E("%s: headWait @%s", v(wc), v(at));
    etrace("");

    zodb::Tid xat;
    string    xatStr;
    error     err;

    // XXX dumb implementation, because _headWait should go away.
    while (1) {
        tie(xatStr, err) = os::ReadFile(wc._path("head/at"));
        if (err != nil)
            return E(err);

        tie(xat, err) = xstrconv::parseHex64(xatStr);
        if (err != nil)
            return E(fmt::errorf("head/at: %w", err));

        if (xat >= at)
            break;

        time::sleep(1*time::millisecond);
    }

    return nil;
}

// connect creates new Conn viewing WCFS state as of @at.
pair<Conn, error> WCFS::connect(zodb::Tid at) {
    WCFS *wc = this;
    xerr::Contextf E("%s: connect @%s", v(wc), v(at));
    etrace("");

    error err;

    // TODO support !isolated mode

    // need to wait till `wcfs/head/at ≥ at` because e.g. Conn.open stats
    // head/f to get f.headfsize.
    err = wc->_headWait(at);
    if (err != nil) {
        return make_pair(nil, E(err));
    }

    WatchLink wlink;
    tie(wlink, err) = wc->_openwatch();
    if (err != nil)
        return make_pair(nil, E(err));

    Conn wconn = adoptref(new _Conn());
    wconn->_wc      = wc;
    wconn->_at      = at;
    wconn->_wlink   = wlink;

    xos::RegisterAfterFork(newref(
        static_cast<xos::_IAfterFork*>( wconn._ptr() )
    ));

    context::Context pinCtx;
    tie(pinCtx, wconn->_pinCancel) = context::with_cancel(context::background());
    wconn->_pinWG = sync::NewWorkGroup(pinCtx);
    wconn->_pinWG->go([wconn](context::Context ctx) -> error {
        return wconn->_pinner(ctx);
    });

    return make_pair(wconn, nil);
}

static global<error> errConnClosed = errors::New("connection closed");

// close releases resources associated with wconn.
//
// opened fileh and mappings become invalid to use except close and unmap.
error _Conn::close() {
    // NOTE keep in sync with Conn.afterFork
    _Conn& wconn = *this;

    // lock virtmem early. TODO more granular virtmem locking (see __pin1 for
    // details and why virt_lock currently goes first)
    virt_lock();
    bool virtUnlocked = false;
    defer([&]() {
        if (!virtUnlocked)
            virt_unlock();
    });

    wconn._atMu.RLock();
    defer([&]() {
        wconn._atMu.RUnlock();
    });

    xerr::Contextf E("%s: close", v(wconn));
    etrace("");

    error err, eret;
    auto reterr1 = [&eret](error err) {
        if (eret == nil && err != nil)
            eret = err;
    };

    // mark wconn as closed, so that no new wconn.open might be spawned.
    bool alreadyClosed = false;
    wconn._filehMu.Lock();
    alreadyClosed = (wconn._downErr == errConnClosed);
    wconn._downErr = errConnClosed;
    wconn._filehMu.Unlock();
    if (alreadyClosed)
        return nil;

    // close all files - both that have no mappings and that still have opened
    // mappings. We have to close files before shutting down pinner, because
    // wcfs might send pin messages due to file access by other clients. So to
    // avoid being killed we have to unwatch all files before stopping the
    // pinner.
    //
    // NOTE after file is closed, its mappings could continue to survive, but
    // we can no longer maintain consistent view. For this reason we change
    // mappings to give EFAULT on access.
    while (1) {
        FileH f = nil;
        bool opening;

        // pick up any fileh
        wconn._filehMu.Lock();
        if (!wconn._filehTab.empty()) {
            f = wconn._filehTab.begin()->second;
            opening = (f->_state < _FileHOpened);
        }
        wconn._filehMu.Unlock();

        if (f == nil)
            break; // all closed

        // if fileh was "opening" - wait for the open to complete before calling close.
        if (opening) {
            f->_openReady.recv();
            if (f->_openErr != nil)
                continue; // failed open; f should be removed from wconn._filehTab by Conn.open itself
        }

        // force fileh close.
        // - virt_lock
        // - wconn.atMu.R
        // - wconn.filehMu unlocked
        err = f->_closeLocked(/*force=*/true);
        if (err != nil)
            reterr1(err);

        // wait for f close to complete, as it might be that f.close was called
        // simultaneously to us or just before. f is removed from
        // wconn.filehTab only after close is complete.
        f->_closedq.recv();
    }

    // close wlink and signal to pinner to stop.
    // we have to release virt_lock, to avoid deadlocking with pinner.
    virtUnlocked = true;
    virt_unlock();

    err = wconn._wlink->close();
    if (err != nil)
        reterr1(err);
    wconn._pinCancel();
    err = wconn._pinWG->wait();
    if (!errors::Is(err, context::canceled)) // canceled - ok
        reterr1(err);

    xos::UnregisterAfterFork(newref(
        static_cast<xos::_IAfterFork*>( &wconn )
    ));

    return E(eret);
}

// afterFork detaches from wcfs in child process right after fork.
//
// opened fileh are closed abruptly without sending "bye" not to interfere into
// parent-wcfs exchange. Existing mappings become invalid to use.
void _Conn::afterFork() {
    // NOTE keep in sync with Conn.close
    _Conn& wconn = *this;

    // ↓↓↓ parallels Conn::close, but without locking and exchange with wcfs.
    //
    // After fork in child we are the only thread that exists/runs.
    // -> no need to lock anything; trying to use locks could even deadlock,
    // because locks state is snapshotted from at fork time, when a lock could
    // be already locked by some thread.

    bool alreadyClosed = (wconn._downErr == errConnClosed);
    if (alreadyClosed)
        return;

    // close all files and make mappings efault.
    while (!wconn._filehTab.empty()) {
        FileH f = wconn._filehTab.begin()->second;

        // close f even if f->_state < _FileHOpened
        // (in parent closure of opening-in-progress files is done by
        // Conn::open, but in child we are the only one to release resources)
        f->_afterFork();
    }

    // NOTE no need to wlink->close() - wlink handles afterFork by itself.
    // NOTE no need to signal pinner, as fork does not clone the pinner into child.
}

// _pinner receives pin messages from wcfs and adjusts wconn file mappings.
error _Conn::_pinner(context::Context ctx) {
    Conn wconn = newref(this); // newref for go
    error err = wconn->__pinner(ctx);

    // if pinner fails, wcfs will kill us.
    // log pinner error so that the error is not hidden.
    // print to stderr as well as by default log does not print to there.
    // TODO also catch panic/exc ?
    if (!(err == nil || errors::Is(err, context::canceled))) { // canceled = .close asks pinner to stop
        log::Fatalf("CRITICAL: %s", v(err));
        log::Fatalf("CRITICAL: wcfs server will likely kill us soon.");
        fprintf(stderr, "CRITICAL: %s\n", v(err));
        fprintf(stderr, "CRITICAL: wcfs server will likely kill us soon.\n");

        // mark the connection non-operational if pinner fails.
        //
        // XXX go because wconn.close might deadlock wrt Conn.resync on
        // wconn._filehMu, because Conn.resync sends "watch" updates under
        // wconn._filehMu (however Conn.open and FileH.close send "watch"
        // _without_ wconn._filehMu). If pinner fails - we already have serious
        // problems... TODO try to resolve the deadlock.
        go([wconn]() {
            wconn->close();
        });
    }

    return err;
}

error _Conn::__pinner(context::Context ctx) {
    _Conn& wconn = *this;
    xerr::Contextf E("pinner"); // NOTE pinner error goes to Conn::close who has its own context
    etrace("");

    PinReq req;
    error  err;

    while (1) {
        err = wconn._wlink->recvReq(ctx, &req);
        if (err != nil) {
            // it is ok if we receive EOF due to us (client) closing the connection
            if (err == io::EOF_) {
                wconn._filehMu.RLock();
                err = (wconn._downErr == errConnClosed) ? nil : io::ErrUnexpectedEOF;
                wconn._filehMu.RUnlock();
            }
            return E(err);
        }

        // we received request to pin/unpin file block. handle it
        err = wconn._pin1(&req);
        if (err != nil) {
            return E(err);
        }
    }
}

// pin1 handles one pin request received from wcfs.
error _Conn::_pin1(PinReq *req) {
    _Conn& wconn = *this;
    xerr::Contextf E("pin f<%s> #%ld @%s", v(req->foid), req->blk, v(req->at));
    etrace("");

    error err = wconn.__pin1(req);

    // reply either ack or nak on error
    string ack = "ack";
    if (err != nil)
        ack = fmt::sprintf("nak: %s", v(err));
    // NOTE ctx=bg to always send reply even if we are canceled
    error err2 = wconn._wlink->replyReq(context::background(), req, ack);
    if (err == nil)
        err = err2;

    return E(err);
}

error _Conn::__pin1(PinReq *req) {
    _Conn& wconn = *this;

    FileH f;
    bool  ok;

    // lock virtmem first.
    //
    // The reason we do it here instead of closely around call to
    // mmap->_remmapblk() is to avoid deadlocks: virtmem calls FileH.mmap,
    // Mapping.remmap_blk and Mapping.unmap under virt_lock locked. In those
    // functions the order of locks is
    //
    //      virt_lock, wconn.atMu.R, fileh.mmapMu
    //
    // So if we take virt_lock right around mmap._remmapblk(), the order of
    // locks in pinner would be
    //
    //      wconn.atMu.R, wconn.filehMu.R, fileh.mmapMu, virt_lock
    //
    // which means there is AB-BA deadlock possibility.
    //
    // TODO try to take virt_lock only around virtmem-associated VMAs and with
    // better granularity. NOTE it is possible to teach virtmem to call
    // FileH.mmap and Mapping.unmap without virtmem locked. However reworking
    // virtmem to call Mapping.remmap_blk without virt_lock is not so easy.
    virt_lock();
    defer([&]() {
        virt_unlock();
    });

    wconn._atMu.RLock();
    defer([&]() {
        wconn._atMu.RUnlock();
    });

    // lock wconn.filehMu.R to lookup fileh in wconn.filehTab.
    //
    // keep wconn.filehMu.R locked during whole __pin1 run to make sure that
    // e.g.  simultaneous FileH.close does not remove f from wconn.filehTab.
    // TODO keeping filehMu.R during whole pin1 is not needed and locking can be made more granular.
    //
    // NOTE no deadlock wrt Conn.resync, Conn.open, FileH.close - they all send
    // "watch" requests to wcfs server outside of wconn.filehMu.
    wconn._filehMu.RLock();
    defer([&]() {
        wconn._filehMu.RUnlock();
    });

    tie(f, ok) = wconn._filehTab.get_(req->foid);
    if (!ok) {
        // why wcfs sent us this update?
        return fmt::errorf("unexpected pin: f<%s> not watched", v(req->foid));
    }

    // NOTE no need to check f._state as we need to go only through f.mmaps, and
    // wcfs server can send us pins at any state, including "opening" - to pin
    // our view to requested @at, and "closing" - due to other clients
    // accessing wcfs/head/f simultaneously.

    f->_mmapMu.lock();
    defer([&]() {
        f->_mmapMu.unlock();
    });

    for (auto mmap : f->_mmaps) {   // TODO use ↑blk_start for binary search
        if (!(mmap->blk_start <= req->blk && req->blk < mmap->blk_stop()))
            continue;   // blk ∉ mmap

        trace("\tremmapblk %d @%s", req->blk, (req->at == TidHead ? "head" : v(req->at)));

        // pin only if virtmem did not dirtied page corresponding to this block already
        // if virtmem dirtied the page - it will ask us to remmap it again after commit or abort.
        bool do_pin= true;
        error err;
        if (mmap->vma != nil) {
            mmap->_assertVMAOk();

            // see ^^^ about deadlock
            //virt_lock();

            BigFileH *virt_fileh = mmap->vma->fileh;
            TODO (mmap->fileh->blksize != virt_fileh->ramh->ram->pagesize);
            do_pin = !__fileh_page_isdirty(virt_fileh, req->blk);
        }

        if (do_pin)
            err = mmap->_remmapblk(req->blk, req->at);

        // see ^^^ about deadlock
        //if (mmap->vma != nil)
        //    virt_unlock();

        // on error don't need to continue with other mappings - all fileh and
        // all mappings become marked invalid on pinner failure.
        if (err != nil)
            return err;

        trace("\t-> remmaped");
    }

    // update f._pinned
    if (req->at == TidHead) {
        f->_pinned.erase(req->blk);      // unpin to @head
    }
    else {
        f->_pinned[req->blk] = req->at;
    }

    return nil;
}

// at returns database state corresponding to the connection.
zodb::Tid _Conn::at() {
    _Conn& wconn = *this;
    wconn._atMu.RLock();
    defer([&]() {
        wconn._atMu.RUnlock();
    });

    return wconn._at;
}

// resync resyncs connection and its file mappings onto different database view.
//
// bigfile/_file_zodb.pyx arranges to call Conn.resync at transaction boundaries
// to keep Conn view in sync with updated zconn database view.
error _Conn::resync(zodb::Tid at) {
    _Conn& wconn = *this;
    error err;

    wconn._atMu.RLock();
    xerr::Contextf E("%s: resync -> @%s", v(wconn), v(at));
    etrace("");

    wconn._filehMu.RLock();
    err = wconn._downErr;
    wconn._filehMu.RUnlock();
    wconn._atMu.RUnlock();

    if (err != nil)
        return E(err);

    // wait for wcfs/head to be >= at.
    // we need this e.g. to be sure that head/f.size is at least as big that it will be @at state.
    err = wconn._wc->_headWait(at);
    if (err != nil)
        return E(err);

    // bring wconn + fileh + mmaps down on error
    bool retok = false;
    defer([&]() {
        if (!retok)
            wconn.close(); // ignore error
    });

    // lock wconn._atMu.W . This excludes everything else, and in
    // particular _pinner_, from running and mutating files and mappings.
    //
    // NOTE we'll relock atMu as R in the second part of resync, so we prelock
    // wconn._filehMu.R as well while under atMu.W, to be sure that set of opened
    // files and their states stay the same during whole resync.
    bool atMuWLocked = true;
    wconn._atMu.Lock();
    wconn._filehMu.RLock();
    defer([&]() {
        wconn._filehMu.RUnlock();
        if (atMuWLocked)
            wconn._atMu.Unlock();
        else
            wconn._atMu.RUnlock();
    });

    err = wconn._downErr;
    if (err != nil)
        return E(err);

    // set new wconn.at early, so that e.g. Conn.open running simultaneously
    // to second part of resync (see below) uses new at.
    wconn._at = at;

    // go through all files opened under wconn and pre-adjust their mappings
    // for viewing data as of new @at state.
    //
    // We are still holding atMu.W, so we are the only mutators of mappings,
    // because, in particular, pinner is not running.
    //
    // Don't send watch updates for opened files to wcfs yet - without running
    // pinner those updates will get stuck.
    for (auto fit : wconn._filehTab) {
        //zodb::Oid  foid = fit.first;
        FileH        f    = fit.second;

        // TODO if file has no mappings and was not used during whole prev
        // cycle - forget and stop watching it?

        // "opening" or "closing" fileh - their setup/teardown is currently
        // handled by Conn.open and FileH.close correspondingly.
        if (f->_state != _FileHOpened)
            continue;

        // update f._headfsize and remmap to head/f zero regions that are now covered by head/f
        struct stat st;
        err = f->_headf->Stat(&st);
        if (err != nil)
            return E(err);

        if ((size_t)st.st_blksize != f->blksize)    // blksize must not change
            return E(fmt::errorf("wcfs bug: blksize changed: %zd -> %ld", f->blksize, st.st_blksize));
        auto headfsize = st.st_size;
        /** XXX If 'discard_data' has been called, file size shrinks -
         **     we explicitly zero removed blocks, but not sure if this
         **     is enough to make dropping this safe. **/
        // if (!(f->_headfsize <= headfsize))          // head/file size ↑=
        //     return E(fmt::errorf("wcfs bug: head/file size not ↑="));
        if (!(headfsize % f->blksize == 0))
            return E(fmt::errorf("wcfs bug: head/file size %% blksize != 0"));

        // If file size shrank, zero removed regions in all mappings to prevent SIGBUS
        if (f->_headfsize > headfsize) {
            for (auto mapping : f->_mmaps) {
                size_t blk_offset = mapping->blk_start * f->blksize;
                uint8_t* zero_start = max(mapping->mem_start,
                    mapping->mem_start + (headfsize - blk_offset));
                uint8_t* zero_end = min(mapping->mem_stop,
                    mapping->mem_start + (f->_headfsize - blk_offset));
                if (zero_end > zero_start) {
                    err = mmap_zero_into_ro(zero_start, zero_end - zero_start);
                    if (err != nil)
                        return E(err);
                }
            }
        }

        // replace zero regions in f mappings in accordance to adjusted f._headfsize.
        // NOTE it is ok to access f._mmaps without locking f._mmapMu because we hold wconn.atMu.W
        for (auto mmap : f->_mmaps) {
            //trace("  resync -> %s: unzero [%lu:%lu)", v(at), f->_headfsize/f->blksize, headfsize/f->blksize);
            uint8_t *mem_unzero_start = min(mmap->mem_stop,
                            mmap->mem_start + (f->_headfsize - mmap->blk_start*f->blksize));
            uint8_t *mem_unzero_stop  = min(mmap->mem_stop,
                            mmap->mem_start + (   headfsize  - mmap->blk_start*f->blksize));

            if (mem_unzero_stop - mem_unzero_start > 0) {
                err = mmap_into_ro(mem_unzero_start, mem_unzero_stop-mem_unzero_start, f->_headf, f->_headfsize);
                if (err != nil)
                    return E(err);
            }
        }

        f->_headfsize = headfsize;
    }

    // atomically downgrade atMu.W to atMu.R before issuing watch updates to wcfs.
    // - we need atMu to be not Wlocked, because under atMu.W pinner cannot run simultaneously to us.
    // - we need to hold atMu.R to avoid race wrt e.g. other resync which changes at.
    // - we cannot just do regular `atMu.Unlock + atMu.RLock()` because then
    //   there is e.g. a race window in between Unlock and RLock where wconn.at can be changed.
    //   Also if we Unlock and Rlock, it will produce deadlock, because locking
    //   order will change to reverse: wconn._filehMu.R + wconn._atMu.R
    //
    // Now other calls, e.g. Conn.open, can be running simultaneously to us,
    // but since we already set wconn.at to new value it is ok. For example
    // Conn.open, for not-yet-opened file, will use new at to send "watch".
    //
    // NOTE we are still holding wconn._filehMu.R, so wconn._filehTab and fileh
    // states are the same as in previous pass above.
    wconn._atMu.UnlockToRLock();
    atMuWLocked = false;

    // send watch updates to wcfs.
    // the pinner is now running and will be able to serve pin requests triggered by our watch.
    //
    // update only fileh in "opened" state - for fileh in "opening" and
    // "closing" states, watch setup/teardown is currently in-progress and
    // performed by Conn.open and FileH.close correspondingly.
    for (auto fit : wconn._filehTab) {
        zodb::Oid  foid = fit.first;
        FileH      f    = fit.second;

        if (f->_state != _FileHOpened)
            continue;

        string ack;
        tie(ack, err) = wconn._wlink->sendReq(context::background(),
                            fmt::sprintf("watch %s @%s", v(foid), v(at)));
        if (err != nil)
            return E(err);
        if (ack != "ok")
            return E(fmt::errorf("%s", v(ack)));
    }

    retok = true;
    return nil;
}

// open opens FileH corresponding to ZBigFile foid.
pair<FileH, error> _Conn::open(zodb::Oid foid) {
    _Conn& wconn = *this;
    error err;

    wconn._atMu.RLock();
    defer([&]() {
        wconn._atMu.RUnlock();
    });

    xerr::Contextf E("%s: open f<%s>", v(wconn), v(foid));
    etrace("");

retry:
    wconn._filehMu.Lock();

    if (wconn._downErr != nil) {
        err = wconn._downErr;
        wconn._filehMu.Unlock();
        return make_pair(nil, E(err));
    }

    FileH f; bool ok;
    tie(f, ok) = wconn._filehTab.get_(foid);
    if (ok) {
        bool closing;

        if (f->_state <= _FileHOpened) {
            f->_nopen++;
            closing = false;
        } else {
            closing = true;
        }
        wconn._filehMu.Unlock();

        // if the file was closing|closed, we should wait for the close to
        // complete and retry the open.
        if (closing) {
            f->_closedq.recv();
            goto retry;
        }

        // the file was opening|opened. wait for open to complete and return the result.
        // we can be sure there won't be last close simultaneous to us as we did ._nopen++
        f->_openReady.recv();
        if (f->_openErr != nil) {
            // don't care about f->_nopen-- since f is not returned anywhere
            return make_pair(nil, E(f->_openErr));
        }

        return make_pair(f, nil);
    }

    // create "opening" FileH entry and perform open with wconn._filehMu released.
    // NOTE wconn._atMu.R is still held because FileH._open relies on wconn.at being stable.
    f = adoptref(new _FileH());
    f->wconn      = newref(&wconn);
    f->foid       = foid;
    f->_openReady = makechan<structZ>();
    f->_closedq   = makechan<structZ>();
    f->_openErr   = nil;
    f->_headf     = nil;
    f->blksize    = 0;
    f->_headfsize = 0;
    f->_state     = _FileHOpening;
    f->_nopen     = 1;

    bool retok = false;
    wconn._filehTab[foid] = f;
    wconn._filehMu.Unlock();
    defer([&]() {
        wconn._filehMu.Lock();
        if (wconn._filehTab.get(foid) != f) {
            wconn._filehMu.Unlock();
            panic("BUG: wconn.open: wconn.filehTab[foid] mutated while file open was in progress");
        }
        if (!retok) {
            // don't care about f->_nopen-- since f is not returned anywhere
            wconn._filehTab.erase(foid);
        } else {
            f->_state = _FileHOpened;
        }
        wconn._filehMu.Unlock();
        f->_openReady.close();
    });

    // do the actual open.
    // we hold only wconn.atMu.R, but neither wconn.filehMu, nor f.mmapMu .
    f->_openErr = f->_open();
    if (f->_openErr != nil)
        return make_pair(nil, E(f->_openErr));

    // NOTE no need to recheck that wconn was not closed while the open was in
    // progress: we'll return "success" but Conn.close will close the fileh.
    // However it is indistinguishable from the following scenario:
    //
    //      T1                  T2
    //
    //  f = wconn.open()
    //  # completes ok
    //                      wconn.close()
    //
    //  # use f -> error

    retok = true;
    return make_pair(f, nil);
}

// _open performs actual open of FileH marked as "in-flight-open" in wconn.filehTab.
//
// Called with:
// - wconn.atMu     held
// - wconn.filehMu  not locked
// - f.mmapMu       not locked
error _FileH::_open() {
    _FileH& f = *this;
    Conn    wconn = f.wconn;
    error err;

    tie(f._headf, err)
                = wconn->_wc->_open(fmt::sprintf("head/bigfile/%s", v(foid)));
    if (err != nil)
        return err;

    bool retok = false;
    defer([&]() {
        if (!retok)
            f._headf->Close();
    });

    struct stat st;
    err = f._headf->Stat(&st);
    if (err != nil)
        return err;
    f.blksize    = st.st_blksize;
    f._headfsize = st.st_size;
    if (!(f._headfsize % f.blksize == 0))
        return fmt::errorf("wcfs bug: %s size (%d) %% blksize (%d) != 0",
                        v(f._headf->Name()), f._headfsize, f.blksize);

    // start watching f
    // NOTE we are _not_ holding wconn.filehMu nor f.mmapMu - only wconn.atMu to rely on wconn.at being stable.
    // NOTE wcfs will reply "ok" only after wcfs/head/at ≥ wconn.at
    string ack;
    tie(ack, err) = wconn->_wlink->sendReq(context::background(),
                        fmt::sprintf("watch %s @%s", v(foid), v(wconn->_at)));
    if (err != nil)
        return err;
    if (ack != "ok")
        return fmt::errorf("watch: %s", v(ack));

    retok = true;
    return nil;
}

// close releases resources associated with FileH.
//
// Left fileh mappings become invalid to use except unmap.
error _FileH::close() {
    _FileH& fileh = *this;
    Conn    wconn = fileh.wconn;

    // lock virtmem early. TODO more granular virtmem locking (see __pin1 for
    // details and why virt_lock currently goes first)
    virt_lock();
    defer([&]() {
        virt_unlock();
    });

    wconn->_atMu.RLock();
    defer([&]() {
        wconn->_atMu.RUnlock();
    });

    return fileh._closeLocked(/*force=*/false);
}

// _closeLocked serves FileH.close and Conn.close.
//
// Must be called with the following locks held by caller:
// - virt_lock
// - wconn.atMu
error _FileH::_closeLocked(bool force) {
    // NOTE keep in sync with FileH._afterFork
    _FileH& fileh = *this;
    Conn    wconn = fileh.wconn;

    wconn->_filehMu.Lock();
    defer([&]() {
        wconn->_filehMu.Unlock();
    });

    // fileh.close can be called several times. just return nil for second close.
    if (fileh._state >= _FileHClosing)
        return nil;

    // decref open count; do real close only when last open goes away.
    if (fileh._nopen <= 0)
        panic("BUG: fileh.close: fileh._nopen <= 0");
    fileh._nopen--;
    if (fileh._nopen > 0 && !force)
        return nil;

    // last open went away - real close.
    xerr::Contextf E("%s: %s: close", v(wconn), v(fileh));
    etrace("");

    ASSERT(fileh._state == _FileHOpened); // there can be no open-in-progress, because
    fileh._state = _FileHClosing;         // .close() can be called only on "opened" fileh

    // unlock wconn._filehMu to stop watching the file outside of this lock.
    // we'll relock wconn._filehMu again before updating wconn.filehTab.
    wconn->_filehMu.Unlock();


    error err, eret;
    auto reterr1 = [&eret](error err) {
        if (eret == nil && err != nil)
            eret = err;
    };

    // stop watching f
    string ack;
    tie(ack, err) = wconn->_wlink->sendReq(context::background(),
                        fmt::sprintf("watch %s -", v(foid)));
    if (err != nil)
        reterr1(err);
    else if (ack != "ok")
        reterr1(fmt::errorf("unwatch: %s", v(ack)));


    // relock wconn._filehMu again and remove fileh from wconn._filehTab
    wconn->_filehMu.Lock();
    if (wconn->_filehTab.get(fileh.foid)._ptr() != &fileh)
        panic("BUG: fileh.close: wconn.filehTab[fileh.foid] != fileh");
    wconn->_filehTab.erase(fileh.foid);

    reterr1(fileh._headf->Close());

    // change all fileh.mmaps to cause EFAULT on any access after fileh.close
    fileh._mmapMu.lock();
    defer([&]() {
        fileh._mmapMu.unlock();
    });

    for (auto mmap : fileh._mmaps) {
        err = mmap->__remmapAsEfault();
        if (err != nil)
            reterr1(err);
    }

    // fileh close complete
    fileh._state = _FileHClosed;
    fileh._closedq.close();

    return E(eret);
}

// _afterFork is similar to _closeLocked and releases FileH resource and
// mappings right after fork.
void _FileH::_afterFork() {
    // NOTE keep in sync with FileH._closeLocked
    _FileH& fileh = *this;
    Conn    wconn = fileh.wconn;

    // ↓↓↓ parallels FileH._closeLocked but without locking and wcfs exchange.
    //
    // There is no locking (see Conn::afterFork for why) and we shutdown file
    // even if ._state == _FileHClosing, because that state was copied from
    // parent and it is inside parent where it is another thread that is
    // currently closing *parent's* FileH.

    if (fileh._state == _FileHClosed) // NOTE _not_ >= _FileHClosing
        return;

    // don't send to wcfs "stop watch f" not to disrupt parent-wcfs exchange.
    // just close the file.
    if (wconn->_filehTab.get(fileh.foid)._ptr() != &fileh)
        panic("BUG: fileh.closeAfterFork: wconn.filehTab[fileh.foid] != fileh");
    wconn->_filehTab.erase(fileh.foid);

    fileh._headf->Close(); // ignore err

    // change all fileh.mmaps to cause EFAULT on access
    for (auto mmap : fileh._mmaps) {
        mmap->__remmapAsEfault(); // ignore err
    }

    // done
    fileh._state = _FileHClosed;
}

// mmap creates file mapping representing file[blk_start +blk_len) data as of wconn.at database state.
//
// If vma != nil, created mapping is associated with that vma of user-space virtual memory manager:
// virtmem calls FileH::mmap under virtmem lock when virtmem fileh is mmapped into vma.
pair<Mapping, error> _FileH::mmap(int64_t blk_start, int64_t blk_len, VMA *vma) {
    _FileH& f = *this;

    // NOTE virtmem lock is held by virtmem caller
    f.wconn->_atMu.RLock();     // e.g. f._headfsize
    f.wconn->_filehMu.RLock();  // f._state  TODO -> finer grained (currently too coarse)
    f._mmapMu.lock();           // f._pinned, f._mmaps
    defer([&]() {
        f._mmapMu.unlock();
        f.wconn->_filehMu.RUnlock();
        f.wconn->_atMu.RUnlock();
    });

    xerr::Contextf E("%s: %s: mmap [#%ld +%ld)", v(f.wconn), v(f), blk_start, blk_len);
    etrace("");

    if (f._state >= _FileHClosing)
        return make_pair(nil, E(os::ErrClosed));

    error err;

    if (blk_start < 0)
        panic("blk_start < 0");
    if (blk_len < 0)
        panic("blk_len < 0");

    int64_t blk_stop; // = blk_start + blk_len
    if (__builtin_add_overflow(blk_start, blk_len, &blk_stop))
        panic("blk_start + blk_len overflow int64");

    int64_t stop;//  = blk_stop *f.blksize;
    if (__builtin_mul_overflow(blk_stop, f.blksize, &stop))
        panic("(blk_start + blk_len)*f.blksize overflow int64");
    int64_t start    = blk_start*f.blksize;


    // create memory with head/f mapping and applied pins
    // mmap-in zeros after f.size (else access to memory after file.size will raise SIGBUS)
    uint8_t *mem_start, *mem_stop;
    tie(mem_start, err) = mmap_ro(f._headf, start, blk_len*f.blksize);
    if (err != nil)
        return make_pair(nil, E(err));
    mem_stop = mem_start + blk_len*f.blksize;

    bool retok = false;
    defer([&]() {
        if (!retok)
            mm::unmap(mem_start, mem_stop - mem_start); // ignore error
    });

    // part of mmapped region is beyond file size - mmap that with zeros - else
    // access to memory after file.size will raise SIGBUS. (assumes head/f size ↑=)
    if (stop > f._headfsize) {
        uint8_t *zmem_start = mem_start + (max(f._headfsize, start) - start);
        err = mmap_zero_into_ro(zmem_start, mem_stop - zmem_start);
        if (err != nil)
            return make_pair(nil, E(err));
    }

    Mapping mmap = adoptref(new _Mapping());
    mmap->fileh     = newref(&f);
    mmap->blk_start = blk_start;
    mmap->mem_start = mem_start;
    mmap->mem_stop  = mem_stop;
    mmap->vma       = vma;
    mmap->efaulted  = false;

    for (auto _ : f._pinned) {  // TODO keep f._pinned ↑blk and use binary search
        int64_t    blk = _.first;
        zodb::Tid  rev = _.second;
        if (!(blk_start <= blk && blk < blk_stop))
            continue;   // blk ∉ this mapping
        err = mmap->_remmapblk(blk, rev);
        if (err != nil)
            return make_pair(nil, E(err));
    }

    if (vma != nil) {
        if (vma->mmap_overlay_server != nil)
            panic("vma is already associated with overlay server");
        if (!(vma->addr_start == 0 && vma->addr_stop == 0))
            panic("vma already covers !nil virtual memory area");
        mmap->incref(); // vma->mmap_overlay_server is keeping ref to mmap
        vma->mmap_overlay_server = mmap._ptr();
        vma->addr_start = (uintptr_t)mmap->mem_start;
        vma->addr_stop  = (uintptr_t)mmap->mem_stop;
        mmap->_assertVMAOk(); // just in case
    }

    f._mmaps.push_back(mmap);   // TODO keep f._mmaps ↑blk_start

    retok = true;
    return make_pair(mmap, nil);
}

// __remmapAsEfault remmaps Mapping memory to cause SIGSEGV on access.
//
// It is used on FileH shutdown to turn all fileh mappings into incorrect ones,
// because after fileh is down, it is not possible to continue to provide
// correct f@at data view.
//
// Must be called with the following locks held by caller:
// - virt_lock
// - fileh.mmapMu
error _Mapping::__remmapAsEfault() {
    _Mapping& mmap = *this;
    FileH f = mmap.fileh;

    // errctx: no need for wconn and f: __remmapAsEfault is called only from
    // FileH._closeLocked who adds them.
    xerr::Contextf E("%s: remmap as efault", v(mmap));
    etrace("");

    error err = mmap_efault_into(mmap.mem_start, mmap.mem_stop - mmap.mem_start);
    mmap.efaulted = true;
    return E(err);
}

// __remmapBlkAsEfault is similar to __remmapAsEfault, but remmaps memory of only 1 block.
// blk must be in mapped range.
error _Mapping::__remmapBlkAsEfault(int64_t blk) {
    _Mapping& mmap = *this;
    FileH f = mmap.fileh;

    xerr::Contextf E("%s: remmapblk #%ld as efault", v(mmap), blk);
    etrace("");

    ASSERT(mmap.blk_start <= blk && blk < mmap.blk_stop());
    uint8_t *blkmem = mmap.mem_start + (blk - mmap.blk_start)*f->blksize;
    error err = mmap_efault_into(blkmem, 1*f->blksize);
    return E(err);
}

// unmap releases mapping memory from address space.
//
// After call to unmap the mapping must no longer be used.
// The association in between mapping and linked virtmem VMA is reset.
//
// Virtmem calls Mapping.unmap under virtmem lock when VMA is unmapped.
error _Mapping::unmap() {
    Mapping mmap = newref(this); // newref for std::remove
    FileH f = mmap->fileh;

    // NOTE virtmem lock is held by virtmem caller
    f->wconn->_atMu.RLock();
    f->_mmapMu.lock();
    defer([&]() {
        f->_mmapMu.unlock();
        f->wconn->_atMu.RUnlock();
    });

    xerr::Contextf E("%s: %s: %s: unmap", v(f->wconn), v(f), v(mmap));
    etrace("");

    // double unmap = ok
    if (mmap->mem_start == nil)
        return nil;

    if (mmap->vma != nil) {
        mmap->_assertVMAOk();
        VMA *vma = mmap->vma;
        vma->mmap_overlay_server = nil;
        mmap->decref(); // vma->mmap_overlay_server was holding a ref to mmap
        vma->addr_start = 0;
        vma->addr_stop  = 0;
        mmap->vma = nil;
    }

    error err = mm::unmap(mmap->mem_start, mmap->mem_stop - mmap->mem_start);
    mmap->mem_start = nil;
    mmap->mem_stop  = nil;

    //f->_mmaps.remove(mmap);
    f->_mmaps.erase(
        std::remove(f->_mmaps.begin(), f->_mmaps.end(), mmap),
        f->_mmaps.end());

    return E(err);
}

// _remmapblk remmaps mapping memory for file[blk] to be viewing database as of @at state.
//
// at=TidHead means unpin to head/ .
// NOTE this does not check whether virtmem already mapped blk as RW.
//
// _remmapblk must not be called after Mapping is switched to efault.
//
// The following locks must be held by caller:
// - f.wconn.atMu
// - f._mmapMu
error _Mapping::_remmapblk(int64_t blk, zodb::Tid at) {
    _Mapping& mmap = *this;
    FileH f = mmap.fileh;
    xerr::Contextf E("_remmapblk #%ld @%s", blk, v(at));
    etrace("");

    ASSERT(mmap.blk_start <= blk && blk < mmap.blk_stop());
    // a mmapping is efaulted only for closed files, i.e. fileh is removed from wconn._filehTab
    // -> pinner should not see the fileh and so should not see this mapping.
    ASSERT(!mmap.efaulted);

    uint8_t *blkmem = mmap.mem_start + (blk - mmap.blk_start)*f->blksize;
    error err;
    os::File fsfile;
    bool fclose = false;
    if (at == TidHead) {
        fsfile = f->_headf;
    }
    else {
        // TODO share @rev fd until wconn is resynced?
        tie(fsfile, err) = f->wconn->_wc->_open(
                fmt::sprintf("@%s/bigfile/%s", v(at), v(f->foid)));
        if (err != nil)
            return E(err);
        fclose = true;
    }
    defer([&]() {
        if (fclose)
            fsfile->Close();
    });

    struct stat st;
    err = fsfile->Stat(&st);
    if (err != nil)
        return E(err);
    if ((size_t)st.st_blksize != f->blksize)
        return E(fmt::errorf("wcfs bug: blksize changed: %zd -> %ld", f->blksize, st.st_blksize));

    // block is beyond file size - mmap with zeros - else access to memory
    // after file.size will raise SIGBUS. (assumes head/f size ↑=)
    if ((blk+1)*f->blksize > (size_t)st.st_size) {
        err = mmap_zero_into_ro(blkmem, 1*f->blksize);
        if (err != nil)
            return E(err);
    }
    // block is inside file - mmap in file data
    else {
        err = mmap_into_ro(blkmem, 1*f->blksize, fsfile, blk*f->blksize);
        if (err != nil)
            return E(err);
    }

    return nil;
}

// remmap_blk remmaps file[blk] in its place again.
//
// Virtmem calls Mapping.remmap_blk under virtmem lock to remmap a block after
// RW dirty page was e.g. discarded or committed.
error _Mapping::remmap_blk(int64_t blk) {
    _Mapping& mmap = *this;
    FileH f = mmap.fileh;

    error err;

    // NOTE virtmem lock is held by virtmem caller
    f->wconn->_atMu.RLock();
    f->_mmapMu.lock();
    defer([&]() {
        f->_mmapMu.unlock();
        f->wconn->_atMu.RUnlock();
    });

    xerr::Contextf E("%s: %s: %s: remmapblk #%ld", v(f->wconn), v(f), v(mmap), blk);
    etrace("");

    if (!(mmap.blk_start <= blk && blk < mmap.blk_stop()))
        panic("remmap_blk: blk out of Mapping range");

    // it should not happen, but if, for a efaulted mapping, virtmem asks us to
    // remmap base-layer blk memory in its place again, we reinject efault into it.
    if (mmap.efaulted) {
        log::Warnf("%s: remmapblk called for already-efaulted mapping", v(mmap));
        return E(mmap.__remmapBlkAsEfault(blk));
    }

    // blkrev = rev | @head
    zodb::Tid blkrev; bool ok;
    tie(blkrev, ok) = f->_pinned.get_(blk);
    if (!ok)
        blkrev = TidHead;

    err = mmap._remmapblk(blk, blkrev);
    if (err != nil)
        return E(err);

    return nil;
}

// ---- WCFS raw file access ----

// _path returns path for object on wcfs.
// - str:        wcfs root + obj;
string WCFS::_path(const string &obj) {
    WCFS& wc = *this;
    return wc.mountpoint + "/" + obj;
}

tuple<os::File, error> WCFS::_open(const string &path, int flags) {
    WCFS& wc = *this;
    string path_ = wc._path(path);
    return os::Open(path_, flags);
}


// ---- misc ----

// mmap_zero_into serves mmap_zero_into_ro and mmap_efault_into.
static error mmap_zero_into(void *addr, size_t size, int prot) {
    xerr::Contextf E("mmap zero");
    etrace("");

    // mmap /dev/zero with MAP_NORESERVE and MAP_SHARED
    // this way the mapping will be able to be read, but no memory will be allocated to keep it.
    os::File z;
    error err;
    tie(z, err) = os::Open("/dev/zero");
    if (err != nil)
        return E(err);
    defer([&]() {
        z->Close();
    });
    err = mm::map_into(addr, size, prot, MAP_SHARED | MAP_NORESERVE, z, 0);
    if (err != nil)
        return E(err);
    return nil;
}

// mmap_zero_into_ro mmaps read-only zeros into [addr +size) so that region is all zeros.
// created mapping, even after it is accessed, does not consume memory.
static error mmap_zero_into_ro(void *addr, size_t size) {
    return mmap_zero_into(addr, size, PROT_READ);
}

// mmap_efault_into changes [addr +size) region to generate SIGSEGV on read/write access.
// Any previous mapping residing in that virtual address range is released.
static error mmap_efault_into(void *addr, size_t size) {
    xerr::Contextf E("mmap efault");
    etrace("");

    // mmaping /dev/zero with PROT_NONE gives what we need.
    return E(mmap_zero_into(addr, size, PROT_NONE));
}


// mmap_ro mmaps read-only fd[offset +size).
// The mapping is created with MAP_SHARED.
static tuple<uint8_t*, error> mmap_ro(os::File f, off_t offset, size_t size) {
    return mm::map(PROT_READ, MAP_SHARED, f, offset, size);
}

// mmap_into_ro mmaps read-only fd[offset +size) into [addr +size).
// The mapping is created with MAP_SHARED.
static error mmap_into_ro(void *addr, size_t size, os::File f, off_t offset) {
    return mm::map_into(addr, size, PROT_READ, MAP_SHARED, f, offset);
}


// _assertVMAOk() verifies that mmap and vma are related to each other and cover
// exactly the same virtual memory range.
//
// It panics if mmap and vma do not exactly relate to each other or cover
// different virtual memory range.
void _Mapping::_assertVMAOk() {
    _Mapping* mmap = this;
    VMA *vma = mmap->vma;

    if (!(vma->mmap_overlay_server == static_cast<void*>(mmap)))
        panic("BUG: mmap and vma do not link to each other");
    if (!(vma->addr_start == uintptr_t(mmap->mem_start) &&
          vma->addr_stop  == uintptr_t(mmap->mem_stop)))
        panic("BUG: mmap and vma cover different virtual memory ranges");

    // verified ok
}


string WCFS::String() const {
    const WCFS& wc = *this;
    return fmt::sprintf("wcfs %s", v(wc.mountpoint));
}

// NOTE String must be called with Conn.atMu locked.
string _Conn::String() const {
    const _Conn& wconn = *this;
    // XXX don't include wcfs as prefix here?
    // (e.g. to use Conn.String in tracing without wcfs prefix)
    // (if yes -> go and correct all xerr::Contextf calls)
    return fmt::sprintf("%s: conn%d @%s", v(wconn._wc), wconn._wlink->fd(), v(wconn._at));
}

string _FileH::String() const {
    const _FileH& f = *this;
    return fmt::sprintf("f<%s>", v(f.foid));
}

string _Mapping::String() const {
    const _Mapping& mmap = *this;
    return fmt::sprintf("m[#%ld +%ld) v[%p +%lx)",
                mmap.blk_start, mmap.blk_stop() - mmap.blk_start,
                mmap.mem_start, mmap.mem_stop   - mmap.mem_start);
}

_Conn::_Conn()  {}
_Conn::~_Conn() {}
void _Conn::incref() {
    object::incref();
}
void _Conn::decref() {
    if (__decref())
        delete this;
}

_FileH::_FileH()  {}
_FileH::~_FileH() {}
void _FileH::decref() {
    if (__decref())
        delete this;
}

_Mapping::_Mapping()  {}
_Mapping::~_Mapping() {}
void _Mapping::decref() {
    if (__decref())
        delete this;
}

dict<int64_t, zodb::Tid> _tfileh_pinned(FileH fileh) {
    return fileh->_pinned;
}

}   // wcfs::
