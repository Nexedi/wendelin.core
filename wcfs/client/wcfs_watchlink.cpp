// Copyright (C) 2018-2022  Nexedi SA and Contributors.
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

#include <golang/errors.h>
#include <golang/fmt.h>
#include <golang/io.h>
#include <golang/strings.h>
#include <string.h>


#define TRACE 0
#if TRACE
#  define trace(format, ...) log::Debugf(format, ##__VA_ARGS__)
#else
#  define trace(format, ...) do {} while (0)
#endif


// wcfs::
namespace wcfs {

// ErrLinkDown is the error indicating that WCFS watch link is no-longer operational.
global<error> ErrLinkDown = errors::New("link is down");


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

// close closes the link.
error _WatchLink::close() {
    _WatchLink& wlink = *this;
    xerr::Contextf E("%s: close", v(wlink));

    error err = wlink.closeWrite();
    wlink._serveCancel();
    // NOTE we can get stuck here if wcfs does not behave correctly by closing
    // its side in reply to our "bye" message.
    //
    // TODO -> better pthread_kill(SIGINT) instead of relying on wcfs proper behaviour?
    error err2 = wlink._serveWG->wait();
    if (errors::Is(err2, context::canceled) ||  // we canceled _serveWG
        errors::Is(err2, io::EOF_)          ||  // EOF received from WCFS
        errors::Is(err2, ErrLinkDown))          // link shutdown due to logic error; details logged
        err2 = nil;

    error err3 = wlink._f->Close();
    if (err == nil)
        err = err2;
    if (err == nil)
        err = err3;

    xos::UnregisterAfterFork(newref(
        static_cast<xos::_IAfterFork*>( &wlink )
    ));

    return E(err);
}

// afterFork detaches from wcfs in child process right after fork.
void _WatchLink::afterFork() {
    _WatchLink& wlink = *this;

    // in child right after fork we are the only thread to run; in particular
    // _serveRX is not running. Just release the file handle, that fork
    // duplicated, to make sure that child cannot send anything to wcfs and
    // interfere into parent-wcfs exchange.
    wlink._f->Close(); // ignore err
}

// closeWrite closes send half of the link.
error _WatchLink::closeWrite() {
    _WatchLink& wlink = *this;
    xerr::Contextf E("%s: closeWrite", v(wlink));

    wlink._txclose1.do_([&]() {
        // ask wcfs to close its tx & rx sides; wcfs.close(tx) wakes up
        // _serveRX on client (= on us). The connection can be already closed
        // by wcfs - so ignore errors when sending bye.
        (void)wlink._send(wlink._nextReqID(), "bye");

        // NOTE vvv should be ~ shutdown(wlink._f, SHUT_WR), however shutdown does
        // not work for non-socket file descriptors. And even if we dup link
        // fd, and close only one used for TX, peer's RX will still be blocked
        // as fds are referring to one file object which stays in opened
        // state. So just use ^^^ "bye" as "TX closed" message.
        // wlink._wtx.close();
    });

    return nil;
}

// _serveRX receives messages from ._f and dispatches them according to
// streamID either to .recvReq, or to .sendReq waiting for reply.
error _WatchLink::_serveRX(context::Context ctx) {
    _WatchLink& wlink = *this;
    xerr::Contextf E("%s: serve rx", v(wlink));

    bool rxeof = false;

    // when finishing - wakeup everyone waiting for rx
    defer([&]() {
        wlink._rxmu.lock();
        wlink._rxeof = rxeof;
        wlink._down  = true; // don't allow new rxtab registers; mark the link as down
        for (auto _ : wlink._rxtab) {
            auto rxq = _.second;
            rxq.close();
        }
        wlink._rxmu.unlock();
        wlink._acceptq.close();
    });

    string l;
    error  err;
    rxPkt  pkt;

    while (1) {
        // NOTE: .close() makes sure ._f.read*() will wake up
        tie(l, err) = wlink._readline();
        if (err != nil) {
            // peer closed its tx
            if (err == io::EOF_) {
                rxeof = true;
                wlink.rx_eof.close();
            }
            return E(err);
        }
        trace("C: %s: rx: \"%s\"", v(wlink), v(l));

        err = pkt.from_string(l);
        if (err != nil)
            return E(err);

        if (pkt.stream == 0) { // control/fatal message from wcfs
            log::Errorf("%s: rx fatal: %s\n", v(wlink), v(l));
            wlink.fatalv.push_back(pkt.to_string()); // TODO -> wlink.errorq
            continue; // wcfs should close link after error
        }

        bool reply = (pkt.stream % 2 != 0);

        // wcfs replies to our request
        if (reply) {
            chan<rxPkt> rxq;
            bool ok;

            wlink._rxmu.lock();
            tie(rxq, ok) = wlink._rxtab.pop_(pkt.stream);
            wlink._rxmu.unlock();
            if (!ok) {
                // wcfs sent reply on unexpected stream -> shutdown wlink.
                log::Errorf("%s: .%lu: wcfs sent reply on unexpected stream", v(wlink), pkt.stream);
                return E(ErrLinkDown);
            }
            int _ = select({
                ctx->done().recvs(),    // 0
                rxq.sends(&pkt),        // 1
            });
            if (_ == 0)
                return E(ctx->err());
        }

        // wcfs originated request
        else {
            wlink._rxmu.lock();
                if (wlink._accepted.has(pkt.stream)) {
                    wlink._rxmu.unlock();
                    // wcfs request on already used stream
                    log::Errorf("%s: .%lu: wcfs sent request on already used stream", v(wlink), pkt.stream);
                    return E(ErrLinkDown);
                }
                wlink._accepted.insert(pkt.stream);
            wlink._rxmu.unlock();
            int _ = select({
                ctx->done().recvs(),            // 0
                wlink._acceptq.sends(&pkt),     // 1
            });
            if (_ == 0)
                return E(ctx->err());
        }
    }
}

// recvReq receives client <- server request.
//
// it returns EOF when server closes the link.
static error _parsePinReq(PinReq *pin, const rxPkt *pkt);
error _WatchLink::recvReq(context::Context ctx, PinReq *prx) {
    _WatchLink& wlink = *this;
    xerr::Contextf E("%s: recvReq", v(wlink));

    rxPkt pkt;
    bool ok;
    int _ = select({
        ctx->done().recvs(),                // 0
        wlink._acceptq.recvs(&pkt, &ok),    // 1
    });
    if (_ == 0)
        return E(ctx->err());

    if (!ok) {
        wlink._rxmu.lock();
        bool rxeof = wlink._rxeof;
        wlink._rxmu.unlock();

        if (rxeof)
            return io::EOF_; // NOTE EOF goes without E
        return E(ErrLinkDown);
    }

    return E(_parsePinReq(prx, &pkt));
}

// replyReq sends reply to client <- server request received via recvReq.
error _WatchLink::replyReq(context::Context ctx, const PinReq *req, const string& answer) {
    _WatchLink& wlink = *this;
    xerr::Contextf E("%s: replyReq .%d", v(wlink), req->stream);

    wlink._rxmu.lock();
    bool ok   = wlink._accepted.has(req->stream);
    bool down = wlink._down;
    wlink._rxmu.unlock();
    if (!ok)
        panic("reply to not accepted stream");
    if (down)
        return E(ErrLinkDown);

    error err = wlink._send(req->stream, answer);

    wlink._rxmu.lock();
        ok = wlink._accepted.has(req->stream);
        if (ok)
            wlink._accepted.erase(req->stream);
    wlink._rxmu.unlock();

    if (!ok)
        panic("BUG: stream vanished from wlink._accepted while reply was in progress");

    // TODO also track as answered for some time and don't accept new requests with the same ID?
    return E(err);
}


// sendReq sends client -> server request and returns server reply.
pair</*reply*/string, error> _WatchLink::sendReq(context::Context ctx, const string &req) {
    _WatchLink& wlink = *this;
    StreamID stream = wlink._nextReqID();
    xerr::Contextf E("%s: sendReq .%d", v(wlink), stream);

    rxPkt       rx; bool ok;
    chan<rxPkt> rxq;
    error       err;
    tie(rxq, err) = wlink._sendReq(ctx, stream, req);
    if (err != nil)
        return make_pair("", E(err));

    // wait for reply
    E = xerr::Contextf("%s: sendReq .%d: recvReply", v(wlink), stream);

    int _ = select({
        ctx->done().recvs(),    // 0
        rxq.recvs(&rx, &ok),    // 1
    });
    if (_ == 0)
        return make_pair("", E(ctx->err()));

    if (!ok) {
        wlink._rxmu.lock();
        bool down = wlink._down;
        wlink._rxmu.unlock();

        return make_pair("", E(down ? ErrLinkDown : io::ErrUnexpectedEOF));
    }
    string reply = rx.to_string();
    return make_pair(reply, nil);
}

tuple</*rxq*/chan<rxPkt>, error> _WatchLink::_sendReq(context::Context ctx, StreamID stream, const string &req) {
    _WatchLink& wlink = *this;

    auto rxq = makechan<rxPkt>(1);
    wlink._rxmu.lock();
        if (wlink._down) {
            wlink._rxmu.unlock();
            return make_tuple(nil, ErrLinkDown);
        }
        if (wlink._rxtab.has(stream)) {
            wlink._rxmu.unlock();
            panic("BUG: to-be-sent stream is present in rxtab");
        }
        wlink._rxtab[stream] = rxq;
    wlink._rxmu.unlock();

    error err = wlink._send(stream, req);
    if (err != nil) {
        // remove rxq from rxtab
        wlink._rxmu.lock();
        wlink._rxtab.erase(stream);
        wlink._rxmu.unlock();
        // no need to drain rxq - it was created with cap=1

        rxq = nil;
    }

    return make_tuple(rxq, err);
}

// _send sends raw message via specified stream.
//
// multiple _send can be called in parallel - _send serializes writes.
// msg must not include \n.
error _WatchLink::_send(StreamID stream, const string &msg) {
    _WatchLink& wlink = *this;
    xerr::Contextf E("%s: send .%d", v(wlink), stream);

    if (msg.find('\n') != string::npos)
        panic("msg has \\n");
    string pkt = fmt::sprintf("%lu %s\n", stream, v(msg));
    return E(wlink._write(pkt));
}

error _twlinkwrite(WatchLink wlink, const string &pkt) {
    return wlink->_write(pkt);
}
error _WatchLink::_write(const string &pkt) {
    _WatchLink& wlink = *this;
    // no errctx

    wlink._txmu.lock();
    defer([&]() {
        wlink._txmu.unlock();
    });

    trace("C: %s: tx: \"%s\"", v(wlink), v(pkt));

    int n;
    error err;
    tie(n, err) = wlink._f->Write(pkt.c_str(), pkt.size());
    return err;
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

// _readline reads next raw line sent from wcfs.
tuple<string, error> _WatchLink::_readline() {
    _WatchLink& wlink = *this;
    char buf[128];

    size_t nl_searchfrom = 0;
    while (1) {
        auto nl = wlink._rxbuf.find('\n', nl_searchfrom);
        if (nl != string::npos) {
            auto line = wlink._rxbuf.substr(0, nl+1);
            wlink._rxbuf = wlink._rxbuf.substr(nl+1);
            return make_tuple(line, nil);
        }
        nl_searchfrom = wlink._rxbuf.length();

        // limit line length to avoid DoS
        if (wlink._rxbuf.length() > 128)
            return make_tuple("", fmt::errorf("input line is too long"));

        int n;
        error err;
        tie(n, err) = wlink._f->Read(buf, sizeof(buf));
        if (n > 0) {
            wlink._rxbuf += string(buf, n);
            continue;
        }
        if (err == nil)
            panic("read returned (0, nil)");
        if (err == io::EOF_ && wlink._rxbuf.length() != 0)
            err = io::ErrUnexpectedEOF;
        return make_tuple("", err);
    }
}

// from_string parses string into rxPkt.
error rxPkt::from_string(const string &rx) {
    rxPkt& pkt = *this;
    xerr::Contextf E("invalid pkt");

    // <stream> ... \n
    auto sp = rx.find(' ');
    if (sp == string::npos)
        return E(fmt::errorf("no SP"));
    if (!strings::has_suffix(rx, '\n'))
        return E(fmt::errorf("no LF"));
    string sid  = rx.substr(0, sp);
    string smsg = strings::trim_suffix(rx.substr(sp+1), '\n');

    error err;
    tie(pkt.stream, err) = xstrconv::parseUint(sid);
    if (err != nil)
        return E(fmt::errorf("invalid stream ID"));

    auto msglen = smsg.length();
    if (msglen > ARRAY_SIZE(pkt.data))
        return E(fmt::errorf("len(msg) > %zu", ARRAY_SIZE(pkt.data)));

    memcpy(pkt.data, smsg.c_str(), msglen);
    pkt.datalen = msglen;
    return nil;
}

// to_string converts rxPkt data into string.
string rxPkt::to_string() const {
    const rxPkt& pkt = *this;
    return string(pkt.data, pkt.datalen);
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

int _WatchLink::fd() const {
    const _WatchLink& wlink = *this;
    return wlink._f->_sysfd();
}

// _nextReqID returns stream ID for next client-originating request to be made.
StreamID _WatchLink::_nextReqID() {
    _WatchLink& wlink = *this;

    wlink._txmu.lock(); // TODO ._req_next -> atomic (currently uses arbitrary lock)
        StreamID stream = wlink._req_next;
        wlink._req_next = (wlink._req_next + 2); // wraparound at uint64 max
    wlink._txmu.unlock();
    return stream;
}

}   // wcfs::
