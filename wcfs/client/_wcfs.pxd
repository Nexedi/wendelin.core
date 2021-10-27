# -*- coding: utf-8 -*-
# Copyright (C) 2018-2021  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Free Software licenses or any of the Open Source
# Initiative approved licenses and Convey the resulting work. Corresponding
# source of such a combination shall include the source code for all other
# software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# See https://www.nexedi.com/licensing for rationale and options.

# cython: language_level=2
# distutils: language=c++

# Package _wcfs provides Python-wrappers for C++ wcfs client package.
#
# It wraps WCFS and WatchLink.

from golang cimport chan, structZ, string, error, refptr
from golang cimport context

from libc.stdint cimport int64_t, uint64_t
from libcpp.utility cimport pair
from libcpp.vector cimport vector


cdef extern from "wcfs/client/wcfs_misc.h" namespace "zodb" nogil:
    ctypedef uint64_t Tid
    ctypedef uint64_t Oid

cdef extern from "wcfs/client/wcfs_misc.h" namespace "wcfs" nogil:
    const Tid TidHead


# pyx/nogil description for C++ classes
cdef extern from "wcfs/client/wcfs_watchlink.h" namespace "wcfs" nogil:
    cppclass _WatchLink:
        error close()
        error closeWrite()
        pair[string, error] sendReq(context.Context ctx, const string &req)
        error recvReq(context.Context ctx, PinReq *prx)
        error replyReq(context.Context ctx, const PinReq *req, const string& reply);

        vector[string] fatalv
        chan[structZ]  rx_eof

    cppclass WatchLink (refptr[_WatchLink]):
        # WatchLink.X = WatchLink->X in C++
        error               close       "_ptr()->close"     ()
        error               closeWrite  "_ptr()->closeWrite"()
        pair[string, error] sendReq     "_ptr()->sendReq"   (context.Context ctx, const string &req)
        error               recvReq     "_ptr()->recvReq"   (context.Context ctx, PinReq *prx)
        error               replyReq    "_ptr()->replyReq"  (context.Context ctx, const PinReq *req, const string& reply);

        vector[string]      fatalv      "_ptr()->fatalv"
        chan[structZ]       rx_eof      "_ptr()->rx_eof"

    cppclass PinReq:
        Oid     foid
        int64_t blk
        Tid     at
        string  msg

    error _twlinkwrite(WatchLink wlink, const string& pkt)


cdef extern from "wcfs/client/wcfs.h" namespace "wcfs" nogil:
    cppclass WCFS:
        string  mountpoint

        pair[WatchLink, error] _openwatch()


# ---- python bits ----

cdef class PyWCFS:
    cdef WCFS wc

cdef class PyWatchLink:
    cdef WatchLink wlink

cdef class PyPinReq:
    cdef PinReq pinreq
