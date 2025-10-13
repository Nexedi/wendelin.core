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

// wcfs_watchlink provides WatchLink class that implements message exchange
// over /head/watch on wcfs.

#ifndef _NXD_WCFS_WATCHLINK_H_
#define _NXD_WCFS_WATCHLINK_H_

#include <golang/libgolang.h>
#include <golang/context.h>
#include <golang/cxx.h>
#include <golang/sync.h>
using namespace golang;
using cxx::dict;
using cxx::set;

#include "wcfs.h"
#include "wcfs_misc.h"
#include "wcfs_link.h"

// wcfs::
namespace wcfs {

struct PinReq;

// WatchLink represents /head/watch link opened on wcfs.
//
// It is created by WCFS._openwatch().
//
// .sendReq()/.recvReq() provides raw IO in terms of wcfs isolation protocol messages.
// .close() closes the link.
//
// It is safe to use WatchLink from multiple threads simultaneously.
typedef refptr<class _WatchLink> WatchLink;

struct _WatchLink : _Link {
private:
    _WatchLink();
    virtual ~_WatchLink();
    friend pair<WatchLink, error> WCFS::_openwatch();
public:
    void incref();
    void decref();
public:
    // watch-protocol-specific methods
    error recvReq(context::Context ctx, PinReq *prx);
    error replyReq(context::Context ctx, const PinReq *req, const string& reply);

    string String() const;
};

// PinReq represents 1 server-initiated wcfs pin request received over /head/watch link.
struct PinReq {
    StreamID    stream; // request was received with this stream ID
    zodb::Oid   foid;   // request is about this file
    int64_t     blk;    // ----//---- about this block
    zodb::Tid   at;     // pin to this at;  TidHead means unpin to head

    string      msg;    // XXX raw message for tests (TODO kill)
};

}   // wcfs::

#endif
