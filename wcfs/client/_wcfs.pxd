# -*- coding: utf-8 -*-
# Copyright (C) 2018-2025  Nexedi SA and Contributors.
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
# cython: legacy_implicit_noexcept=True
# distutils: language=c++

# Package _wcfs provides Python-wrappers for C++ wcfs client package.
#
# It wraps WCFS/Conn/FileH/Mapping and WatchLink to help client_test.py unit-test
# WCFS base-layer mmap functionality. At functional level WCFS client (and especially
# pinner) is verified when running wendelin.core array tests in wcfs mode.

from golang cimport chan, structZ, string, error, refptr
from golang cimport context, cxx

from libc.stdint cimport int64_t, uint64_t, uint8_t
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
        pair[Conn, error]      connect(Tid at)

    cppclass _Conn:
        Tid                 at()
        pair[FileH, error]  open(Oid foid)
        error               close()
        error               resync(Tid at)

    cppclass Conn (refptr[_Conn]):
        # Conn.X = Conn->X in C++
        Tid                 at          "_ptr()->at"        ()
        pair[FileH, error]  open        "_ptr()->open"      (Oid foid)
        error               close       "_ptr()->close"     ()
        error               resync      "_ptr()->resync"    (Tid at)

    cppclass _FileH:
        size_t               blksize
        error                close()
        pair[Mapping, error] mmap(int64_t blk_start, int64_t blk_len)   # `VMA *vma=nil` not exposed

    cppclass FileH (refptr[_FileH]):
        # FileH.X = FileH->X in C++
        size_t               blksize    "_ptr()->blksize"
        error                close      "_ptr()->close"     ()
        pair[Mapping, error] mmap       "_ptr()->mmap"      (int64_t blk_start, int64_t blk_len)

    cppclass _Mapping:
        FileH   fileh
        int64_t blk_start
        int64_t blk_stop()  const
        uint8_t *mem_start
        uint8_t *mem_stop

        error   unmap()

    cppclass Mapping (refptr[_Mapping]):
        # Mapping.X = Mapping->X in C++
        FileH   fileh           "_ptr()->fileh"
        int64_t blk_start       "_ptr()->blk_start"
        int64_t blk_stop        "_ptr()->blk_stop"      ()  const
        uint8_t *mem_start      "_ptr()->mem_start"
        uint8_t *mem_stop       "_ptr()->mem_stop"

        error   unmap           "_ptr()->unmap"         ()


    cxx.dict[int64_t, Tid] _tfileh_pinned(FileH wfileh)


# ---- python bits ----

cdef class PyWCFS:
    cdef WCFS wc

cdef class PyConn:
    cdef Conn wconn
    cdef readonly PyWCFS wc # PyWCFS that was used to create this PyConn

cdef class PyFileH:
    cdef FileH wfileh

cdef class PyMapping:
    cdef Mapping wmmap
    cdef readonly PyFileH fileh

cdef class PyWatchLink:
    cdef WatchLink wlink

cdef class PyPinReq:
    cdef PinReq pinreq
