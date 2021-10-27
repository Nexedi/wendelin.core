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

// Package wcfs provides WCFS client.

#ifndef _NXD_WCFS_H_
#define _NXD_WCFS_H_

#include <golang/libgolang.h>

#include <tuple>

#include "wcfs_misc.h"


// wcfs::
namespace wcfs {

using namespace golang;
using std::tuple;
using std::pair;


typedef refptr<struct _WatchLink> WatchLink;
struct PinReq;


// WCFS represents filesystem-level connection to wcfs server.
//
// Use wcfs.join in Python API to create it.
//
// WCFS logically mirrors ZODB.DB .
// It is safe to use WCFS from multiple threads simultaneously.
struct WCFS {
    string  mountpoint;

    pair<WatchLink, error>  _openwatch();

    string String() const;

    // at OS-level, on-WCFS raw files can be accessed via ._path and ._open.
    string                  _path(const string &obj);
    tuple<os::File, error>  _open(const string &path, int flags=O_RDONLY);
};


}   // wcfs::

#endif
