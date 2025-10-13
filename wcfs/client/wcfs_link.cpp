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

#include "wcfs_link.h"
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

// ErrLinkDown is the error indicating that WCFS link is no-longer operational.
global<error> ErrLinkDown = errors::New("link is down");

// close closes the link.
error _Link::close() {
    _Link& link = *this;
    xerr::Contextf E("%s: close", v(link));

    error err = link.closeWrite();
    link._serveCancel();
    // NOTE we can get stuck here if wcfs does not behave correctly by closing
    // its side in reply to our "bye" message.
    //
    // TODO -> better pthread_kill(SIGINT) instead of relying on wcfs proper behaviour?
    error err2 = link._serveWG->wait();
    if (errors::Is(err2, context::canceled) ||  // we canceled _serveWG
        errors::Is(err2, io::EOF_)          ||  // EOF received from WCFS
        errors::Is(err2, ErrLinkDown))          // link shutdown due to logic error; details logged
        err2 = nil;

    error err3 = link._f->Close();
    if (err == nil)
        err = err2;
    if (err == nil)
        err = err3;

    xos::UnregisterAfterFork(newref(
        static_cast<xos::_IAfterFork*>( &link )
    ));

    return E(err);
}

// afterFork detaches from wcfs in child process right after fork.
void _Link::afterFork() {
    _Link& link = *this;

    // in child right after fork we are the only thread to run; in particular
    // _serveRX is not running. Just release the file handle, that fork
    // duplicated, to make sure that child cannot send anything to wcfs and
    // interfere into parent-wcfs exchange.
    link._f->Close(); // ignore err
}

// closeWrite closes send half of the link.
error _Link::closeWrite() {
    _Link& link = *this;
    xerr::Contextf E("%s: closeWrite", v(link));

    link._txclose1.do_([&]() {
        // ask wcfs to close its tx & rx sides; wcfs.close(tx) wakes up
        // _serveRX on client (= on us). The connection can be already closed
        // by wcfs - so ignore errors when sending bye.
        (void)link._send(link._nextReqID(), "bye");

        // NOTE vvv should be ~ shutdown(link._f, SHUT_WR), however shutdown does
        // not work for non-socket file descriptors. And even if we dup link
        // fd, and close only one used for TX, peer's RX will still be blocked
        // as fds are referring to one file object which stays in opened
        // state. So just use ^^^ "bye" as "TX closed" message.
        // link._wtx.close();
    });

    return nil;
}

// _serveRX receives messages from ._f and dispatches them according to
// streamID either to .recvReq, or to .sendReq waiting for reply.
error _Link::_serveRX(context::Context ctx) {
    _Link& link = *this;
    xerr::Contextf E("%s: serve rx", v(link));

    bool rxeof = false;

    // when finishing - wakeup everyone waiting for rx
    defer([&]() {
        link._rxmu.lock();
        link._rxeof = rxeof;
        link._down  = true; // don't allow new rxtab registers; mark the link as down
        for (auto _ : link._rxtab) {
            auto rxq = _.second;
            rxq.close();
        }
        link._rxmu.unlock();
        link._acceptq.close();
    });

    string l;
    error  err;
    rxPkt  pkt;

    while (1) {
        // NOTE: .close() makes sure ._f.read*() will wake up
        tie(l, err) = link._readline();
        if (err != nil) {
            // peer closed its tx
            if (err == io::EOF_) {
                rxeof = true;
                link.rx_eof.close();
            }
            return E(err);
        }
        trace("C: %s: rx: \"%s\"", v(link), v(l));

        err = pkt.from_string(l);
        if (err != nil)
            return E(err);

        if (pkt.stream == 0) { // control/fatal message from wcfs
            log::Errorf("%s: rx fatal: %s\n", v(link), v(l));
            link.fatalv.push_back(pkt.to_string()); // TODO -> link.errorq
            continue; // wcfs should close link after error
        }

        bool reply = (pkt.stream % 2 != 0);

        // wcfs replies to our request
        if (reply) {
            chan<rxPkt> rxq;
            bool ok;

            link._rxmu.lock();
            tie(rxq, ok) = link._rxtab.pop_(pkt.stream);
            link._rxmu.unlock();
            if (!ok) {
                // wcfs sent reply on unexpected stream -> shutdown link.
                log::Errorf("%s: .%lu: wcfs sent reply on unexpected stream", v(link), pkt.stream);
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
            link._rxmu.lock();
                if (link._accepted.has(pkt.stream)) {
                    link._rxmu.unlock();
                    // wcfs request on already used stream
                    log::Errorf("%s: .%lu: wcfs sent request on already used stream", v(link), pkt.stream);
                    return E(ErrLinkDown);
                }
                link._accepted.insert(pkt.stream);
            link._rxmu.unlock();
            int _ = select({
                ctx->done().recvs(),            // 0
                link._acceptq.sends(&pkt),      // 1
            });
            if (_ == 0)
                return E(ctx->err());
        }
    }
}

// recvReq receives client <- server request.
//
// it returns EOF when server closes the link.
error _Link::recvReq(context::Context ctx, rxPkt *prx) {
    _Link& link = *this;
    xerr::Contextf E("%s: recvReq", v(link));

    rxPkt pkt;
    bool ok;
    int _ = select({
        ctx->done().recvs(),                // 0
        link._acceptq.recvs(&pkt, &ok),     // 1
    });
    if (_ == 0)
        return E(ctx->err());

    if (!ok) {
        link._rxmu.lock();
        bool rxeof = link._rxeof;
        link._rxmu.unlock();

        if (rxeof)
            return io::EOF_; // NOTE EOF goes without E
        return E(ErrLinkDown);
    }

    *prx = pkt;  // Return raw packet
    return nil;
}

// replyReq sends reply to client <- server request received via recvReq.
error _Link::replyReq(context::Context ctx, StreamID stream, const string& answer) {
    _Link& link = *this;
    xerr::Contextf E("%s: replyReq .%d", v(link), stream);

    link._rxmu.lock();
    bool ok   = link._accepted.has(stream);
    bool down = link._down;
    link._rxmu.unlock();
    if (!ok)
        panic("reply to not accepted stream");
    if (down)
        return E(ErrLinkDown);

    error err = link._send(stream, answer);

    link._rxmu.lock();
        ok = link._accepted.has(stream);
        if (ok)
            link._accepted.erase(stream);
    link._rxmu.unlock();

    if (!ok)
        panic("BUG: stream vanished from link._accepted while reply was in progress");

    // TODO also track as answered for some time and don't accept new requests with the same ID?
    return E(err);
}


// sendReq sends client -> server request and returns server reply.
pair</*reply*/string, error> _Link::sendReq(context::Context ctx, const string &req) {
    _Link& link = *this;
    StreamID stream = link._nextReqID();
    xerr::Contextf E("%s: sendReq .%d", v(link), stream);

    rxPkt       rx; bool ok;
    chan<rxPkt> rxq;
    error       err;
    tie(rxq, err) = link._sendReq(ctx, stream, req);
    if (err != nil)
        return make_pair("", E(err));

    // wait for reply
    E = xerr::Contextf("%s: sendReq .%d: recvReply", v(link), stream);

    int _ = select({
        ctx->done().recvs(),    // 0
        rxq.recvs(&rx, &ok),    // 1
    });
    if (_ == 0)
        return make_pair("", E(ctx->err()));

    if (!ok) {
        link._rxmu.lock();
        bool down = link._down;
        link._rxmu.unlock();

        return make_pair("", E(down ? ErrLinkDown : io::ErrUnexpectedEOF));
    }
    string reply = rx.to_string();
    return make_pair(reply, nil);
}

tuple</*rxq*/chan<rxPkt>, error> _Link::_sendReq(context::Context ctx, StreamID stream, const string &req) {
    _Link& link = *this;

    auto rxq = makechan<rxPkt>(1);
    link._rxmu.lock();
        if (link._down) {
            link._rxmu.unlock();
            return make_tuple(nil, ErrLinkDown);
        }
        if (link._rxtab.has(stream)) {
            link._rxmu.unlock();
            panic("BUG: to-be-sent stream is present in rxtab");
        }
        link._rxtab[stream] = rxq;
    link._rxmu.unlock();

    error err = link._send(stream, req);
    if (err != nil) {
        // remove rxq from rxtab
        link._rxmu.lock();
        link._rxtab.erase(stream);
        link._rxmu.unlock();
        // no need to drain rxq - it was created with cap=1

        rxq = nil;
    }

    return make_tuple(rxq, err);
}

// _send sends raw message via specified stream.
//
// multiple _send can be called in parallel - _send serializes writes.
// msg must not include \n.
error _Link::_send(StreamID stream, const string &msg) {
    _Link& link = *this;
    xerr::Contextf E("%s: send .%d", v(link), stream);

    if (msg.find('\n') != string::npos)
        panic("msg has \\n");
    string pkt = fmt::sprintf("%lu %s\n", stream, v(msg));
    return E(link._write(pkt));
}

error _tlinkwrite(Link link, const string &pkt) {
    return link->_write(pkt);
}
error _Link::_write(const string &pkt) {
    _Link& link = *this;
    // no errctx

    link._txmu.lock();
    defer([&]() {
        link._txmu.unlock();
    });

    trace("C: %s: tx: \"%s\"", v(link), v(pkt));

    int n;
    error err;
    tie(n, err) = link._f->Write(pkt.c_str(), pkt.size());
    return err;
}

// _readline reads next raw line sent from wcfs.
tuple<string, error> _Link::_readline() {
    _Link& link = *this;
    char buf[128];

    size_t nl_searchfrom = 0;
    while (1) {
        auto nl = link._rxbuf.find('\n', nl_searchfrom);
        if (nl != string::npos) {
            auto line = link._rxbuf.substr(0, nl+1);
            link._rxbuf = link._rxbuf.substr(nl+1);
            return make_tuple(line, nil);
        }
        nl_searchfrom = link._rxbuf.length();

        // limit line length to avoid DoS
        if (link._rxbuf.length() > 128)
            return make_tuple("", fmt::errorf("input line is too long"));

        int n;
        error err;
        tie(n, err) = link._f->Read(buf, sizeof(buf));
        if (n > 0) {
            link._rxbuf += string(buf, n);
            continue;
        }
        if (err == nil)
            panic("read returned (0, nil)");
        if (err == io::EOF_ && link._rxbuf.length() != 0)
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


_Link::_Link()    {}
_Link::~_Link()   {}
void _Link::incref() {
    object::incref();
}
void _Link::decref() {
    if (__decref())
        delete this;
}

string _Link::String() const {
    const _Link& link = *this;
    // XXX don't include wcfs as prefix here? (see Conn.String for details)
    return fmt::sprintf("%s: link%d", v(link._wc), link._f->_sysfd());
}

int _Link::fd() const {
    const _Link& link = *this;
    return link._f->_sysfd();
}

// _nextReqID returns stream ID for next client-originating request to be made.
StreamID _Link::_nextReqID() {
    _Link& link = *this;

    link._txmu.lock(); // TODO ._req_next -> atomic (currently uses arbitrary lock)
        StreamID stream = link._req_next;
        link._req_next = (link._req_next + 2); // wraparound at uint64 max
    link._txmu.unlock();
    return stream;
}

// Helper for converting WatchLink to Link (for Cython bindings)
Link watchlink_to_link(WatchLink wlink) {
    if (wlink == nil)
        return nil;
    _Link* base = wlink._ptr();
    return newref(base);
}

}   // wcfs::
