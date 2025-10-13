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
//
// This client package takes care about WCFS isolation protocol details and
// provides to clients simple interface to isolated view of bigfile data on
// WCFS similar to regular files: given a particular revision of database @at,
// it provides synthetic read-only bigfile memory mappings with data
// corresponding to @at state, but using /head/bigfile/* most of the time to
// build and maintain the mappings.
//
// For its data a mapping to bigfile X mostly reuses kernel cache for
// /head/bigfile/X with amount of data not associated with kernel cache for
// /head/bigfile/X being proportional to δ(bigfile/X, at..head). In the usual
// case where many client workers simultaneously serve requests, their database
// views are a bit outdated, but close to head, which means that in practice
// the kernel cache for /head/bigfile/* is being used almost 100% of the time.
//
// A mapping for bigfile X@at is built from OS-level memory mappings of
// on-WCFS files as follows:
//
//                                           ___        /@revA/bigfile/X
//         __                                           /@revB/bigfile/X
//                _                                     /@revC/bigfile/X
//                            +                         ...
//      ───  ───── ──────────────────────────   ─────   /head/bigfile/X
//
// where @revR mmaps are being dynamically added/removed by this client package
// to maintain X@at data view according to WCFS isolation protocol(*).
//
//
// Integration with wendelin.core virtmem layer
//
// This client package can be used standalone, but additionally provides
// integration with wendelin.core userspace virtual memory manager: when a
// Mapping is created, it can be associated as serving base layer for a
// particular virtmem VMA via FileH.mmap(vma=...). In that case, since virtmem
// itself adds another layer of dirty pages over read-only base provided by
// Mapping(+)
//
//                  ┌──┐                      ┌──┐
//                  │RW│                      │RW│    ← virtmem VMA dirty pages
//                  └──┘                      └──┘
//                            +
//                                                    VMA base = X@at view provided by Mapping:
//
//                                           ___        /@revA/bigfile/X
//         __                                           /@revB/bigfile/X
//                _                                     /@revC/bigfile/X
//                            +                         ...
//      ───  ───── ──────────────────────────   ─────   /head/bigfile/X
//
// the Mapping will interact with virtmem layer to coordinate
// updates to mapping virtual memory.
//
//
// API overview
//
//  - `WCFS` represents filesystem-level connection to wcfs server.
//  - `Conn` represents logical connection that provides view of data on wcfs
//    filesystem as of particular database state.
//  - `FileH` represent isolated file view under Conn.
//  - `Mapping` represents one memory mapping of FileH.
//
// A path from WCFS to Mapping is as follows:
//
//  WCFS.connect(at)                    -> Conn
//  Conn.open(foid)                     -> FileH
//  FileH.mmap([blk_start +blk_len))    -> Mapping
//
// A connection can be resynced to another database view via Conn.resync(at').
//
// Documentation for classes provides more thorough overview and API details.
//
// --------
//
// (*) see wcfs.go documentation for WCFS isolation protocol overview and details.
// (+) see bigfile_ops interface (wendelin/bigfile/file.h) that gives virtmem
//     point of view on layering.

#ifndef _NXD_WCFS_H_
#define _NXD_WCFS_H_

#include <golang/libgolang.h>
#include <golang/cxx.h>
#include <golang/sync.h>

#include <tuple>
#include <utility>

#include "wcfs_misc.h"
#include <wendelin/bug.h>

// from wendelin/bigfile/virtmem.h
extern "C" {
struct VMA;
}


// wcfs::
namespace wcfs {

using namespace golang;
namespace os        = golang::os;
namespace xos       = xgolang::xos;
namespace mm        = xgolang::xmm;
namespace xstrconv  = xgolang::xstrconv;
namespace log       = xgolang::xlog;
using cxx::dict;
using cxx::set;
using std::tuple;
using std::pair;


typedef refptr<struct _Conn> Conn;
typedef refptr<struct _Mapping> Mapping;
typedef refptr<struct _FileH> FileH;
typedef refptr<struct _WatchLink> WatchLink;
typedef refptr<struct _AuthLink> AuthLink;
struct PinReq;


// WCFS represents filesystem-level connection to wcfs server.
//
// Use wcfs.join in Python API to create it.
//
// The primary way to access wcfs is to open logical connection viewing on-wcfs
// data as of particular database state, and use that logical connection to
// create base-layer mappings. See .connect and Conn for details.
//
// WCFS logically mirrors ZODB.DB .
// It is safe to use WCFS from multiple threads simultaneously.
struct WCFS {
    string  mountpoint;
    string authkeyfile;

    pair<Conn, error>       connect(zodb::Tid at);
    pair<WatchLink, error>  _openwatch();
    pair<AuthLink, error>   _openauth();

    // Read authentication key from authkeyfile
    pair<string, error>     readAuthKey();

    string String() const;
    error _headWait(zodb::Tid at);

    // at OS-level, on-WCFS raw files can be accessed via ._path and ._open.
    string                  _path(const string &obj);
    tuple<os::File, error>  _open(const string &path, int flags=O_RDONLY);
};

// Conn represents logical connection that provides view of data on wcfs
// filesystem as of particular database state.
//
// It uses /head/bigfile/* and notifications received from /head/watch to
// maintain isolated database view while at the same time sharing most of data
// cache in OS pagecache of /head/bigfile/*.
//
// Use WCFS.connect(at) to create Conn.
// Use .open to create new FileH.
// Use .resync to resync Conn onto different database view.
//
// Conn logically mirrors ZODB.Connection .
// It is safe to use Conn from multiple threads simultaneously.
typedef refptr<struct _Conn> Conn;
struct _Conn : xos::_IAfterFork, object {
    WCFS        *_wc;
    WatchLink   _wlink; // watch/receive pins for mappings created under this conn
    AuthLink    _alink; // authentication link under this conn

    // atMu protects .at.
    // While it is rlocked, .at is guaranteed to stay unchanged and Conn
    // viewing the database at particular state. .resync write-locks this and
    // knows noone is using the connection for reading simultaneously.
    sync::RWMutex _atMu;
    zodb::Tid     _at;

    sync::RWMutex           _filehMu;  // _atMu.W  |  _atMu.R + _filehMu
    error                   _downErr;  // !nil if connection is closed or no longer operational
    dict<zodb::Oid, FileH>  _filehTab; // {} foid -> fileh

    sync::WorkGroup _pinWG;     // pin/unpin messages from wcfs are served by _pinner
    func<void()>    _pinCancel; // spawned under _pinWG.

    // don't new - create via WCFS.connect
private:
    _Conn();
    virtual ~_Conn();
    friend pair<Conn, error> WCFS::connect(zodb::Tid at);
public:
    void incref();
    void decref();

public:
    zodb::Tid at();
    pair<FileH, error> open(zodb::Oid foid);
    error close();
    error resync(zodb::Tid at);

    string String() const;

private:
    error _pinner(context::Context ctx);
    error __pinner(context::Context ctx);
    error _pin1(PinReq *req);
    error __pin1(PinReq *req);

    void afterFork();
};

// FileH represent isolated file view under Conn.
//
// The file view is maintained to be as of @Conn.at database state even in the
// presence of simultaneous database changes. The file view uses
// /head/<file>/data primarily and /@revX/<file>/data pin overrides.
//
// Use .mmap to map file view into memory.
//
// It is safe to use FileH from multiple threads simultaneously.
enum _FileHState {
    // NOTE order of states is semantically important
    _FileHOpening   = 0,    // FileH open is in progress
    _FileHOpened    = 1,    // FileH is opened and can be used
    _FileHClosing   = 2,    // FileH close is in progress
    _FileHClosed    = 3,    // FileH is closed
};
typedef refptr<struct _FileH> FileH;
struct _FileH : object {
    Conn        wconn;
    zodb::Oid   foid;       // ZBigFile root object ID (does not change after fileh open)

    // protected by wconn._filehMu
    _FileHState _state;  // opening/opened/closing/closed
    int         _nopen;  // number of times Conn.open returned this fileh

    chan<structZ> _openReady; // in-flight open completed
    error         _openErr;   // error result from open
    chan<structZ> _closedq;   // in-flight close completed

    os::File      _headf;     // file object of head/file
    size_t        blksize;    // block size of this file (does not change after fileh open)

    // head/file size is known to be at least headfsize (size ↑=)
    // protected by .wconn._atMu
    off_t       _headfsize;

    sync::Mutex               _mmapMu; // atMu.W  |  atMu.R + _mmapMu
    dict<int64_t, zodb::Tid>  _pinned; // {} blk -> rev   that wcfs already sent us for this file
    vector<Mapping>           _mmaps;  // []Mapping ↑blk_start    mappings of this file

    // don't new - create via Conn.open
private:
    _FileH();
    ~_FileH();
    friend pair<FileH, error> _Conn::open(zodb::Oid foid);
public:
    void decref();

public:
    error close();
    pair<Mapping, error> mmap(int64_t blk_start, int64_t blk_len, VMA *vma=nil);
    string String() const;

    error _open();
    error _closeLocked(bool force);
    void  _afterFork();
};

// Mapping represents one memory mapping of FileH.
//
// The mapped memory is [.mem_start, .mem_stop)
// Use .unmap to release virtual memory resources used by mapping.
//
// Except unmap, it is safe to use Mapping from multiple threads simultaneously.
typedef refptr<struct _Mapping> Mapping;
struct _Mapping : object {
    FileH   fileh;
    int64_t blk_start;  // offset of this mapping in file

    // protected by fileh._mmapMu
    uint8_t  *mem_start;    // mmapped memory [mem_start, mem_stop)
    uint8_t  *mem_stop;
    VMA      *vma;          // mmapped under this virtmem VMA | nil if created standalone from virtmem
    bool     efaulted;      // y after mapping was switched to be invalid (gives SIGSEGV on access)

    int64_t blk_stop() const {
        ASSERT((mem_stop - mem_start) % fileh->blksize == 0);
        return blk_start + (mem_stop - mem_start) / fileh->blksize;
    }

    error remmap_blk(int64_t blk); // for virtmem-only
    error unmap();

    void  _assertVMAOk();
    error _remmapblk(int64_t blk, zodb::Tid at);
    error __remmapAsEfault();
    error __remmapBlkAsEfault(int64_t blk);

    // don't new - create via FileH.mmap
private:
    _Mapping();
    ~_Mapping();
    friend pair<Mapping, error> _FileH::mmap(int64_t blk_start, int64_t blk_len, VMA *vma);
public:
    void decref();

    string String() const;
};


// for testing
dict<int64_t, zodb::Tid> _tfileh_pinned(FileH fileh);


}   // wcfs::

#endif
