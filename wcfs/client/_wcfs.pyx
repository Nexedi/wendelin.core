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
# cython: auto_pickle=False
# distutils: language=c++

# Package _wcfs provides Python-wrappers for C++ wcfs client package.
# See _wcfs.pxd for package overview.

from golang cimport pychan, pyerror, nil
from golang import b as pyb  # TODO cimport directly after https://lab.nexedi.com/nexedi/pygolang/-/merge_requests/21 is merged
from golang cimport io

cdef extern from *:
    ctypedef bint cbool "bool"

from ZODB.utils import p64, u64
from cpython cimport PyBuffer_FillInfo
from libcpp.unordered_map cimport unordered_map


cdef class PyWCFS:

    property mountpoint:
        def __get__(PyWCFS pywc):
            return str(pyb(pywc.wc.mountpoint)) # TODO remove str(·) after bstr can be mixed with unicode in os.path.join
        def __set__(PyWCFS pywc, v):
            pywc.wc.mountpoint = pyb(v)

    property authkeyfile:
        def __get__(PyWCFS pywc):
            return str(pyb(pywc.wc.authkeyfile))
        def __set__(PyWCFS pywc, v):
            pywc.wc.authkeyfile = pyb(v)

    def connect(PyWCFS pywc, pyat): # -> PyConn
        cdef Tid at = u64(pyat)
        with nogil:
            _ = wcfs_connect_pyexc(&pywc.wc, at)
            wconn = _.first
            err   = _.second

        if err != nil:
            raise pyerr(err)

        cdef PyConn pywconn = PyConn.__new__(PyConn)
        pywconn.wconn = wconn
        pywconn.wc    = pywc
        return pywconn


cdef class PyConn:

    def __dealloc__(PyConn pywconn):
        pywconn.wconn = nil

    def at(PyConn pywconn):
        with nogil:
            at = wconn_at_pyexc(pywconn.wconn)
        return p64(at)

    def close(PyConn pywconn):
        with nogil:
            err = wconn_close_pyexc(pywconn.wconn)
        if err != nil:
            raise pyerr(err)

    def open(PyConn pywconn, pyfoid): # -> FileH
        cdef Oid foid = u64(pyfoid)
        with nogil:
            _ = wconn_open_pyexc(pywconn.wconn, foid)
            wfileh = _.first
            err    = _.second
        if err != nil:
            raise pyerr(err)

        cdef PyFileH pywfileh = PyFileH.__new__(PyFileH)
        pywfileh.wfileh = wfileh
        return pywfileh

    def resync(PyConn pywconn, pyat):
        cdef Tid at = u64(pyat)
        with nogil:
            err = wconn_resync_pyexc(pywconn.wconn, at)
        if err != nil:
            raise pyerr(err)


cdef class PyFileH:

    def __dealloc__(PyFileH pywfileh):
        pywfileh.wfileh = nil

    def close(PyFileH pywfileh):
        with nogil:
            err = wfileh_close_pyexc(pywfileh.wfileh)
        if err != nil:
            raise pyerr(err)

    def mmap(PyFileH pywfileh, int64_t blk_start, int64_t blk_len):
        with nogil:
            _ = wfileh_mmap_pyexc(pywfileh.wfileh, blk_start, blk_len)
            wmmap = _.first
            err   = _.second
        if err != nil:
            raise pyerr(err)

        assert wmmap.fileh .eq (pywfileh.wfileh)

        cdef PyMapping pywmmap = PyMapping.__new__(PyMapping)
        pywmmap.wmmap = wmmap
        pywmmap.fileh = pywfileh
        return pywmmap

    property blksize:
        def __get__(PyFileH pywfileh):
            return pywfileh.wfileh.blksize

    # XXX for tests
    property pinned:
        def __get__(PyFileH pywfileh):
            # XXX cast: needed for cython to automatically convert to py dict
            cdef dict p = <unordered_map[int64_t, Tid]> _tfileh_pinned(pywfileh.wfileh)
            for blk in p:
                p[blk] = p64(p[blk])    # rev(int64) -> rev(bytes)
            return p


cdef class PyMapping:

    def __dealloc__(PyMapping pywmmap):
        # unmap just in case (double unmap is ok)
        with nogil:
            err = wmmap_unmap_pyexc(pywmmap.wmmap)
        pywmmap.wmmap = nil
        if err != nil:
            raise pyerr(err)

    property blk_start:
        def __get__(PyMapping pywmmap):
            return pywmmap.wmmap.blk_start
    property blk_stop:
        def __get__(PyMapping pywmmap):
            return pywmmap.wmmap.blk_stop()

    def __getbuffer__(PyMapping pywmmap, Py_buffer *view, int flags):
        PyBuffer_FillInfo(view, pywmmap, pywmmap.wmmap.mem_start,
            pywmmap.wmmap.mem_stop - pywmmap.wmmap.mem_start, readonly=1, flags=flags)
    property mem:
        def __get__(PyMapping pywmmap) -> memoryview:
            return memoryview(pywmmap)

    def unmap(PyMapping pywmmap):
        with nogil:
            err = wmmap_unmap_pyexc(pywmmap.wmmap)
        if err != nil:
            raise pyerr(err)

# ----------------------------------------

cdef class PyAuthLink:
    cdef AuthLink alink
    def __init__(PyAuthLink pyalink, PyWCFS pywc):
        with nogil:
            _ = wcfs_openauth_pyexc(&pywc.wc)
            pyalink.alink = _.first
            err           = _.second

        if err != nil:
            raise pyerr(err)

    def __dealloc__(PyAuthLink pyalink):
        pyalink.alink = nil


    def close(PyAuthLink pyalink):
        with nogil:
            err = alink_close_pyexc(pyalink.alink)
        if err != nil:
            raise pyerr(err)

    def closeWrite(PyAuthLink pyalink):
        with nogil:
            err = alink_closeWrite_pyexc(pyalink.alink)
        if err != nil:
            raise pyerr(err)


    def sendReq(PyAuthLink pyalink, context.PyContext pyctx, pyreq):  # -> reply(bstr)
        cdef string req = pyb(pyreq)
        with nogil:
            _ = alink_sendReq_pyexc(pyalink.alink, pyctx.ctx, req)
            reply = _.first
            err   = _.second

        if err != nil:
            raise pyerr(err)

        return pyb(reply)



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


    def sendReq(PyWatchLink pywlink, context.PyContext pyctx, pyreq):  # -> reply(bstr)
        cdef string req = pyb(pyreq)
        with nogil:
            _ = wlink_sendReq_pyexc(pywlink.wlink, pyctx.ctx, req)
            reply = _.first
            err   = _.second

        if err != nil:
            raise pyerr(err)

        return pyb(reply)

    def recvReq(PyWatchLink pywlink, context.PyContext pyctx):  # -> PinReq | None when EOF
        cdef PyPinReq pyreq = PyPinReq.__new__(PyPinReq)
        with nogil:
            err = wlink_recvReq_pyexc(pywlink.wlink, pyctx.ctx, &pyreq.pinreq)

        if err.eq(io.EOF):
            return None
        if err != nil:
            raise pyerr(err)

        return pyreq

    def replyReq(PyWatchLink pywlink, context.PyContext pyctx, PyPinReq pyreq, pyreply):
        cdef string reply = pyb(pyreply)
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
            return pyb(pypin.pinreq.msg)


def _tpywlinkwrite(PyWatchLink pywlink, pypkt):
    cdef string pkt = pyb(pypkt)
    cdef Link link = watchlink_to_link(pywlink.wlink)
    with nogil:
        err = _tlinkwrite_pyexc(link, pkt)
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

    pair[AuthLink, error] wcfs_openauth_pyexc(WCFS *wcfs)       except +topyexc:
        return wcfs._openauth()

    pair[Conn, error] wcfs_connect_pyexc(WCFS *wcfs, Tid at)    except +topyexc:
        return wcfs.connect(at)

    Tid wconn_at_pyexc(Conn wconn)                              except +topyexc:
        return wconn.at()

    error wconn_close_pyexc(Conn wconn)                         except +topyexc:
        return wconn.close()

    pair[FileH, error] wconn_open_pyexc(Conn wconn, Oid foid)   except +topyexc:
        return wconn.open(foid)

    error wconn_resync_pyexc(Conn wconn, Tid at)                except +topyexc:
        return wconn.resync(at)

    error wfileh_close_pyexc(FileH wfileh)                      except +topyexc:
        return wfileh.close()

    pair[Mapping, error] wfileh_mmap_pyexc(FileH wfileh, int64_t blk_start, int64_t blk_len)   except +topyexc:
        return wfileh.mmap(blk_start, blk_len)

    error wmmap_unmap_pyexc(Mapping wmmap)                      except +topyexc:
        return wmmap.unmap()

    error alink_close_pyexc(AuthLink alink)                    except +topyexc:
        return alink.close()

    error alink_closeWrite_pyexc(AuthLink alink)               except +topyexc:
        return alink.closeWrite()

    pair[string, error] alink_sendReq_pyexc(AuthLink alink, context.Context ctx, const string &req)   except +topyexc:
        return alink.sendReq(ctx, req)

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

    error _tlinkwrite_pyexc(Link link, const string& pkt)    except +topyexc:
        return _tlinkwrite(link, pkt)
