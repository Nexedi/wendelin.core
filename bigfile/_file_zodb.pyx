# -*- coding: utf-8 -*-
# Wendelin.bigfile | WCFS part of BigFile ZODB backend
# Copyright (C) 2014-2021  Nexedi SA and Contributors.
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

"""Module _file_zodb.pyx complements file_zodb.py with things that cannot be
implemented in Python.

It provides wcfs integration for ZBigFile handles opened with _use_wcfs=True.
"""

from __future__ import print_function, absolute_import

cdef extern from "wcfs/client/wcfs.h":
    pass

cdef extern from "bigfile/_bigfile.h":
    struct PyBigFile:
        pass
    ctypedef extern class wendelin.bigfile._bigfile.BigFile[object PyBigFile]:
        pass

# ZBigFile_mmap_ops is virtmem mmap  functions for _ZBigFile.
cdef extern from "<wendelin/bigfile/file.h>" nogil:
    struct bigfile_ops:
        pass
cdef extern from * nogil:
    """
    extern const bigfile_ops ZBigFile_mmap_ops;
    """
    const bigfile_ops ZBigFile_mmap_ops

from wendelin.wcfs.client cimport _wcfs as wcfs, _wczsync as wczsync
from golang cimport error, nil, pyerror
from cpython cimport PyCapsule_New


# _ZBigFile is base class for ZBigFile that provides BigFile-line base.
#
# The other base line is from Persistent. It is not possible to inherit from
# both Persistent and BigFile at the same time since both are C types and their
# layouts conflict.
#
# _ZBigFile:
#
# - redirects loadblk/storeblk calls to ZBigFile.
# - provides blkmmapper with WCFS integration.
cdef public class _ZBigFile(BigFile) [object _ZBigFile, type _ZBigFile_Type]:
    cdef object     zself   # reference to ZBigFile
    cdef wcfs.FileH wfileh  # WCFS file handle. Initially nil, opened by blkmmapper

    # _new creates new _ZBigFile associated with ZBigFile zself.
    # XXX Cython does not allow __new__ nor to change arguments passed to __cinit__ / __init__
    @staticmethod
    def _new(zself, blksize):
        cdef _ZBigFile obj = _ZBigFile.__new__(_ZBigFile, blksize)
        obj.zself  = zself
        obj.wfileh = nil
        return obj

    def __dealloc__(_ZBigFile zf):
        cdef error err = nil
        if zf.wfileh != nil:
            err = zf.wfileh.close()
        zf.wfileh = nil
        if err != nil:
            raise pyerror.from_error(err)


    # redirect load/store to main class
    def loadblk(self, blk, buf):    return self.zself.loadblk(blk, buf)
    def storeblk(self, blk, buf):   return self.zself.storeblk(blk, buf)


    # blkmmapper complements loadblk/storeblk and is pycapsule with virtmem mmap
    # functions for _ZBigFile. MMap functions rely on .wfileh being initialized
    # by .fileh_open()
    blkmmapper = PyCapsule_New(<void*>&ZBigFile_mmap_ops, "wendelin.bigfile.IBlkMMapper", NULL)

    # fileh_open wraps BigFile.fileh_open and makes sure that WCFS file handle
    # corresponding to ZBigFile is opened if use_wcfs=True.
    def fileh_open(_ZBigFile zf, bint use_wcfs):
        mmap_overlay = False
        cdef wcfs.PyFileH pywfileh

        if use_wcfs:
            mmap_overlay = True
            if zf.wfileh == nil:
                zconn = zf.zself._p_jar
                assert zconn is not None

                # join zconn to wconn; link to wconn from _ZBigFile
                pywconn   = wczsync.pywconnOf(zconn)
                pywfileh  = pywconn.open(zf.zself._p_oid)
                zf.wfileh = pywfileh.wfileh

        return super(_ZBigFile, zf).fileh_open(mmap_overlay)
