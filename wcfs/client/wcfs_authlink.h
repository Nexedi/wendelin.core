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

// wcfs_authlink.h provides authentication link for WCFS client.
//
// _AuthLink handles the authentication protocol between WCFS client and server

#ifndef _NXD_WCFS_AUTHLINK_H_
#define _NXD_WCFS_AUTHLINK_H_

#include <golang/libgolang.h>
#include <golang/cxx.h>

#include "wcfs_misc.h"
#include "wcfs_link.h"  // Base _Link class

// wcfs::
namespace wcfs {

using namespace golang;

typedef refptr<struct _AuthLink> AuthLink;

// WCFS needs to be able to create AuthLink
struct WCFS;

// _AuthLink represents /wcfs/auth opened link for authentication protocol.
//
// It inherits from _Link which provides the stream-based communication
// infrastructure. _AuthLink adds authentication-specific message handling.
//
// The authentication protocol:
// - Client sends "auth <key>" to authenticate
//
// Use WCFS._openauth() to create it.
// It is safe to use _AuthLink from multiple threads simultaneously.
struct _AuthLink : _Link {
    // don't new - create via WCFS._openauth
private:
    _AuthLink();
    virtual ~_AuthLink();
    friend pair<AuthLink, error> WCFS::_openauth();
public:
    void incref();
    void decref();

public:
    // auth-protocol-specific methods
    error authenticate(context::Context ctx, const string& authkey);

    string String() const;
};

}   // wcfs::

#endif