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

// wcfs_link provides Link base class that implements message exchange
// over wcfs protocol connections.

#ifndef _NXD_WCFS_LINK_H_  
#define _NXD_WCFS_LINK_H_

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


// StreamID stands for ID of a stream multiplexed over Link.
typedef uint64_t StreamID;

// rxPkt internally represents data of one message received over Link.
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


// Link represents link opened on wcfs.
//
// .sendReq()/.recvReq() provides raw IO in terms of wcfs isolation protocol messages.
// .close() closes the link.
//
// It is safe to use Link from multiple threads simultaneously.
typedef refptr<class _Link> Link;
class _Link : public xos::_IAfterFork, public object {

protected:
    WCFS            *_wc;
    os::File        _f;      // file handle
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

protected:
    _Link();
    virtual ~_Link();
public:
    void incref();
    void decref();

public:
    error close();
    error closeWrite();
    pair<string, error> sendReq(context::Context ctx, const string &req);
    error recvReq(context::Context ctx, rxPkt *prx);
    error replyReq(context::Context ctx, StreamID stream, const string& reply);

    string String() const;
    int    fd() const;

protected:
    error _serveRX(context::Context ctx);
    tuple<string, error> _readline();
    error _send(StreamID stream, const string &msg);
    error _write(const string &pkt);
    StreamID _nextReqID();
    tuple<chan<rxPkt>, error> _sendReq(context::Context ctx, StreamID stream, const string &req);

    void afterFork();

    friend error _tlinkwrite(Link link, const string &pkt);
};

// for testing
error _tlinkwrite(Link link, const string &pkt);

// Helper for converting WatchLink to Link (for Cython bindings)
Link watchlink_to_link(WatchLink wlink);

}   // wcfs::

#endif
