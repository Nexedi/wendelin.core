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

#include "wcfs_misc.h"
#include "wcfs.h"
#include "wcfs_watchlink.h"

#include <golang/errors.h>
#include <golang/fmt.h>


// wcfs::
namespace wcfs {


// ---- WCFS raw file access ----

// _path returns path for object on wcfs.
// - str:        wcfs root + obj;
string WCFS::_path(const string &obj) {
    WCFS& wc = *this;
    return wc.mountpoint + "/" + obj;
}

tuple<os::File, error> WCFS::_open(const string &path, int flags) {
    WCFS& wc = *this;
    string path_ = wc._path(path);
    return os::open(path_, flags);
}


// ---- misc ----

string WCFS::String() const {
    const WCFS& wc = *this;
    return fmt::sprintf("wcfs %s", v(wc.mountpoint));
}

}   // wcfs::
