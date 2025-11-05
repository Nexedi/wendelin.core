// Copyright (C) 2025  Nexedi SA and Contributors.
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

#include "wcfs_authlink.h"
#include "wcfs.h"

#include <golang/errors.h>
#include <golang/fmt.h>
#include <golang/io.h>

using namespace golang;
using namespace xgolang;

#define TRACE 0
#if TRACE
#  define trace(format, ...) log::Debugf(format, ##__VA_ARGS__)
#else
#  define trace(format, ...) do {} while (0)
#endif

// wcfs::
namespace wcfs {

// _openauth opens new authentication link on wcfs.
pair<AuthLink, error> WCFS::_openauth() {
    WCFS *wc = this;
    xerr::Contextf E("%s: openauth", v(wc));

    // /.wcfs/auth handle
    os::File f;
    error err;
    tie(f, err) = wc->_open(".wcfs/auth", O_RDWR);
    if (err != nil)
        return make_pair(nil, E(err));

    AuthLink alink = adoptref(new(_AuthLink));
    alink->_wc        = wc;
    alink->_f         = f;
    alink->_acceptq   = makechan<rxPkt>();
    alink->_down      = false;
    alink->_rxeof     = false;
    alink->_req_next  = 1;

    alink->rx_eof     = makechan<structZ>();

    xos::RegisterAfterFork(newref(
        static_cast<xos::_IAfterFork*>( alink._ptr() )
    ));

    context::Context serveCtx;
    tie(serveCtx, alink->_serveCancel) = context::with_cancel(context::background());
    alink->_serveWG = sync::NewWorkGroup(serveCtx);
    alink->_serveWG->go([alink](context::Context ctx) -> error {
        return alink->_serveRX(ctx);
    });

    return make_pair(alink, nil);
}

// Constructor/destructor
_AuthLink::_AuthLink()    {}
_AuthLink::~_AuthLink()   {}

void _AuthLink::incref() {
    object::incref();
}
void _AuthLink::decref() {
    if (__decref())
        delete this;
}

string _AuthLink::String() const {
    const _AuthLink& alink = *this;
    return fmt::sprintf("%s: alink%d", v(alink._wc), alink._f->_sysfd());
}

// authenticate sends authentication credentials to the server.
error _AuthLink::authenticate(context::Context ctx, const string& authkey) {
    _AuthLink& alink = *this;
    xerr::Contextf E("%s: authenticate", v(alink));

    string ack;
    error err;
    tie(ack, err) = alink.sendReq(ctx, fmt::sprintf("auth %s", v(authkey)));
    if (err != nil)
        return E(err);

    if (ack != "ok")
        return E(fmt::errorf("authentication failed: %s", v(ack)));

    return nil;
}

}   // wcfs::
