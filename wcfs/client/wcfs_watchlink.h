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

// wcfs::
namespace wcfs {

struct PinReq;


// StreamID stands for ID of a stream multiplexed over WatchLink.
typedef uint64_t StreamID;

// rxPkt internally represents data of one message received over WatchLink.
struct rxPkt {
    // stream over which the data was received
    StreamID stream;

    // raw data received/to-be-sent.
    // XXX not e.g. string, as chan<T> currently does not support types with
    //     non-trivial copy. Note: we anyway need to limit rx line length to
    //     avoid DoS, but just for DoS the limit would be higher.
    uint16_t datalen;
    char     data[256 - sizeof(StreamID) - sizeof(uint16_t)];

    error  from_string(const string& rx);
    string to_string() const;
};
static_assert(sizeof(rxPkt) == 256, "rxPkt miscompiled"); // NOTE 128 is too low for long error message


// WatchLink represents /head/watch link opened on wcfs.
//
// It is created by WCFS._openwatch().
//
// .sendReq()/.recvReq() provides raw IO in terms of wcfs isolation protocol messages.
// .close() closes the link.
//
// It is safe to use WatchLink from multiple threads simultaneously.
typedef refptr<class _WatchLink> WatchLink;
class _WatchLink : public os::_IAfterFork, object {
    WCFS            *_wc;
    os::File        _f;      // head/watch file handle
    string          _rxbuf;  // buffer for data already read from _f

    // iso.protocol message IO
    chan<rxPkt>     _acceptq;   // server originated messages go here
    sync::Mutex     _rxmu;
    bool            _down;      // y when the link is no-longer operational
    bool            _rxeof;     // y if EOF was received from server
    dict<StreamID, chan<rxPkt>>
                    _rxtab;     // {} stream -> rxq    server replies go via here
    set<StreamID>   _accepted;  // streams we accepted but did not replied yet

    StreamID        _req_next;  // stream ID for next client-originated request TODO -> atomic
    sync::Mutex     _txmu;      // serializes writes
    sync::Once      _txclose1;

    sync::WorkGroup _serveWG;   // _serveRX is running under _serveWG
    func<void()>    _serveCancel;

    // XXX for tests
public:
    vector<string>  fatalv; // ad-hoc, racy. TODO rework to send messages to control channel
    chan<structZ>   rx_eof; // becomes ready when wcfs closes its tx side

    // don't new - create only via WCFS._openwatch()
private:
    _WatchLink();
    virtual ~_WatchLink();
    friend pair<WatchLink, error> WCFS::_openwatch();
public:
    void incref();
    void decref();

public:
    error close();
    error closeWrite();
    pair<string, error> sendReq(context::Context ctx, const string &req);
    error recvReq(context::Context ctx, PinReq *rx_into);
    error replyReq(context::Context ctx, const PinReq *req, const string& reply);

    string String() const;
    int    fd() const;

private:
    error _serveRX(context::Context ctx);
    tuple<string, error> _readline();
    error _send(StreamID stream, const string &msg);
    error _write(const string &pkt);
    StreamID _nextReqID();
    tuple<chan<rxPkt>, error> _sendReq(context::Context ctx, StreamID stream, const string &req);

    void afterFork();

    friend error _twlinkwrite(WatchLink wlink, const string &pkt);
};

// PinReq represents 1 server-initiated wcfs pin request received over /head/watch link.
struct PinReq {
    StreamID    stream; // request was received with this stream ID
    zodb::Oid   foid;   // request is about this file
    int64_t     blk;    // ----//---- about this block
    zodb::Tid   at;     // pin to this at;  TidHead means unpin to head

    string      msg;    // XXX raw message for tests (TODO kill)
};


// for testing
error _twlinkwrite(WatchLink wlink, const string &pkt);


}   // wcfs::

#endif
