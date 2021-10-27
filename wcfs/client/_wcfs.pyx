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
# cython: auto_pickle=False
# distutils: language=c++

# Package _wcfs provides Python-wrappers for C++ wcfs client package.
# See _wcfs.pxd for package overview.

from golang cimport pychan, pyerror, nil
from golang cimport io

from ZODB.utils import p64

cdef class PyWCFS:

    property mountpoint:
        def __get__(PyWCFS pywc):
            return pywc.wc.mountpoint
        def __set__(PyWCFS pywc, string v):
            pywc.wc.mountpoint = v


cdef class PyWatchLink:

    def __init__(PyWatchLink pywlink, PyWCFS pywc):
        with nogil:
            _ = wcfs_openwatch_pyexc(&pywc.wc)
            pywlink.wlink = _.first
            err           = _.second

        if err != nil:
            raise pyerr(err)

    def __dealloc__(PyWatchLink pywlink):
        pywlink.wlink = nil


    def close(PyWatchLink pywlink):
        with nogil:
            err = wlink_close_pyexc(pywlink.wlink)
        if err != nil:
            raise pyerr(err)

    def closeWrite(PyWatchLink pywlink):
        with nogil:
            err = wlink_closeWrite_pyexc(pywlink.wlink)
        if err != nil:
            raise pyerr(err)


    def sendReq(PyWatchLink pywlink, context.PyContext pyctx, string req):  # -> reply(string)
        with nogil:
            _ = wlink_sendReq_pyexc(pywlink.wlink, pyctx.ctx, req)
            reply = _.first
            err   = _.second

        if err != nil:
            raise pyerr(err)

        return reply

    def recvReq(PyWatchLink pywlink, context.PyContext pyctx):  # -> PinReq | None when EOF
        cdef PyPinReq pyreq = PyPinReq.__new__(PyPinReq)
        with nogil:
            err = wlink_recvReq_pyexc(pywlink.wlink, pyctx.ctx, &pyreq.pinreq)

        if err.eq(io.EOF):
            return None
        if err != nil:
            raise pyerr(err)

        return pyreq

    def replyReq(PyWatchLink pywlink, context.PyContext pyctx, PyPinReq pyreq, string reply):
        with nogil:
            err = wlink_replyReq_pyexc(pywlink.wlink, pyctx.ctx, &pyreq.pinreq, reply)

        if err != nil:
            raise pyerr(err)

        return


    # XXX for tests
    property fatalv:
        def __get__(PyWatchLink pywlink):
            return pywlink.wlink.fatalv
    property rx_eof:
        def __get__(PyWatchLink pywlink):
            return pychan.from_chan_structZ(pywlink.wlink.rx_eof)


cdef class PyPinReq:

    property foid:
        def __get__(PyPinReq pypin):
            return p64(pypin.pinreq.foid)

    property blk:
        def __get__(PyPinReq pypin):
            return pypin.pinreq.blk

    property at:
        def __get__(PyPinReq pypin):
            at = pypin.pinreq.at
            if at == TidHead:
                return None
            return p64(at)

    # wcfs_test.py uses req.msg in several places
    property msg:
        def __get__(PyPinReq pypin):
            return pypin.pinreq.msg


def _tpywlinkwrite(PyWatchLink pywlink, bytes pypkt):
    cdef string pkt = pypkt
    with nogil:
        err = _twlinkwrite_pyexc(pywlink.wlink, pkt)
    if err != nil:
        raise pyerr(err)

# ---- misc ----

# pyerr converts error into python error.
cdef object pyerr(error err):
    return pyerror.from_error(err)


from golang cimport topyexc

cdef nogil:

    pair[WatchLink, error] wcfs_openwatch_pyexc(WCFS *wcfs)     except +topyexc:
        return wcfs._openwatch()

    error wlink_close_pyexc(WatchLink wlink)                    except +topyexc:
        return wlink.close()

    error wlink_closeWrite_pyexc(WatchLink wlink)               except +topyexc:
        return wlink.closeWrite()

    pair[string, error] wlink_sendReq_pyexc(WatchLink wlink, context.Context ctx, const string &req)   except +topyexc:
        return wlink.sendReq(ctx, req)

    error wlink_recvReq_pyexc(WatchLink wlink, context.Context ctx, PinReq *prx)    except +topyexc:
        return wlink.recvReq(ctx, prx)

    error wlink_replyReq_pyexc(WatchLink wlink, context.Context ctx, const PinReq *req, const string& reply)    except +topyexc:
        return wlink.replyReq(ctx, req, reply)

    error _twlinkwrite_pyexc(WatchLink wlink, const string& pkt)    except +topyexc:
        return _twlinkwrite(wlink, pkt)
