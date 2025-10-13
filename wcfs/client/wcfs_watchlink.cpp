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

#include "wcfs_watchlink.h"
#include "wcfs_link.h"

#include <golang/errors.h>
#include <golang/fmt.h>
#include <golang/strings.h>

// wcfs::
namespace wcfs {

// _openwatch opens new watch link on wcfs.
pair<WatchLink, error> WCFS::_openwatch() {
    WCFS *wc = this;
    xerr::Contextf E("%s: openwatch", v(wc));

    // head/watch handle.
    os::File f;
    error err;
    tie(f, err) = wc->_open("head/watch", O_RDWR);
    if (err != nil)
        return make_pair(nil, E(err));

    WatchLink wlink = adoptref(new(_WatchLink));
    wlink->_wc        = wc;
    wlink->_f         = f;
    wlink->_acceptq   = makechan<rxPkt>();
    wlink->_down      = false;
    wlink->_rxeof     = false;
    wlink->_req_next  = 1;

    wlink->rx_eof     = makechan<structZ>();

    xos::RegisterAfterFork(newref(
        static_cast<xos::_IAfterFork*>( wlink._ptr() )
    ));

    context::Context serveCtx;
    tie(serveCtx, wlink->_serveCancel) = context::with_cancel(context::background());
    wlink->_serveWG = sync::NewWorkGroup(serveCtx);
    wlink->_serveWG->go([wlink](context::Context ctx) -> error {
        return wlink->_serveRX(ctx);
    });

    return make_pair(wlink, nil);
}

// recvReq receives client <- server request.
//
// it returns EOF when server closes the link.
static error _parsePinReq(PinReq *pin, const rxPkt *pkt);
error _WatchLink::recvReq(context::Context ctx, PinReq *prx) {
    _WatchLink& wlink = *this;
    xerr::Contextf E("%s: recvReq", v(wlink));

    rxPkt pkt;
    error err = _Link::recvReq(ctx, &pkt);
    if (err != nil)
        return E(err);

    return E(_parsePinReq(prx, &pkt));
}

// replyReq sends reply to client <- server pin request received via recvReq.
error _WatchLink::replyReq(context::Context ctx, const PinReq *req, const string& answer) {
    return _Link::replyReq(ctx, req->stream, answer);
}

// _parsePinReq parses message into PinReq according to wcfs isolation protocol.
static error _parsePinReq(PinReq *pin, const rxPkt *pkt) {
    pin->stream = pkt->stream;
    string msg  = pkt->to_string();
    pin->msg    = msg;

    xerr::Contextf E("bad pin: '%s'", v(msg));

    // pin <foid>) #<blk> @<at>
    if (!strings::has_prefix(msg, "pin ")) {
        return E(fmt::errorf("not a pin request"));
    }

    auto argv = strings::split(msg.substr(4), ' ');
    if (argv.size() != 3)
        return E(fmt::errorf("expected 3 arguments, got %zd", argv.size()));

    error err;
    tie(pin->foid, err) = xstrconv::parseHex64(argv[0]);
    if (err != nil)
        return E(fmt::errorf("invalid foid"));

    if (!strings::has_prefix(argv[1], '#'))
        return E(fmt::errorf("invalid blk"));
    tie(pin->blk, err)  = xstrconv::parseInt(argv[1].substr(1));
    if (err != nil)
        return E(fmt::errorf("invalid blk"));

    if (!strings::has_prefix(argv[2], '@'))
        return E(fmt::errorf("invalid at"));
    auto at = argv[2].substr(1);
    if (at == "head") {
        pin->at = TidHead;
    } else {
        tie(pin->at, err) = xstrconv::parseHex64(at);
        if (err != nil)
            return E(fmt::errorf("invalid at"));
    }

    return nil;
}

_WatchLink::_WatchLink()    {}
_WatchLink::~_WatchLink()   {}
void _WatchLink::incref() {
    object::incref();
}
void _WatchLink::decref() {
    if (__decref())
        delete this;
}

string _WatchLink::String() const {
    const _WatchLink& wlink = *this;
    // XXX don't include wcfs as prefix here? (see Conn.String for details)
    return fmt::sprintf("%s: wlink%d", v(wlink._wc), wlink._f->_sysfd());
}

}   // wcfs::
