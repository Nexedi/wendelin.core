# -*- coding: utf-8 -*-
# Wendelin.bigarray | RAM Array
# Copyright (C) 2014-2018  Nexedi SA and Contributors.
#                          Klaus Wölfel <klaus@nexedi.com>
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

"""Module array_ram provides RAMArray that mimics ZBigArray, but keeps data in RAM.

RAMArray mimics ZBigArray API and semantic, but keeps data in RAM.

RAMArray should be used for temporary objects only - its data is not
persisted in any way.
"""

from wendelin import bigarray
import mmap, os, threading, tempfile, errno
import numpy as np


# RAMArray mimics ZBigArray API and semantic, but keeps data in RAM.
class RAMArray(bigarray.BigArray):

    def __init__(self, shape, dtype, order='C'):
        # the whole functionality of RAMArray is in _RAMFileH
        super(RAMArray, self).__init__(shape, dtype, _RAMFileH(), order)


# _RAMFileH mimics _ZBigFileH with data kept in RAM in /dev/shm.
#
# ( we have to use mmap from a file in /dev/shm, not e.g. plain ndarray, because
#   BigArray append semantic is to keep aliasing the data from previously-created
#   views, and since ndarray.resize copies data, that property would not be preserved. )
class _RAMFileH(object):

    # we mmap data as read/write by default.
    # tests can overwrite this to be e.g. only PROT_READ to catch incorrect modifications.
    _prot = mmap.PROT_READ | mmap.PROT_WRITE

    def __init__(self):
        # create temporary file in dev/shm and unlink it.
        # ._fh keeps opened file descriptor to it.
        fh, path = tempfile.mkstemp(dir="/dev/shm", prefix="ramfile.")
        os.unlink(path)
        self._fh = fh

        # mmap(2) allows mmaping past the end, but python's mmap does not.
        # we workaround it with explicitly growing file as needed.
        # however we need to protect against races between concurrent .mmap() calls.
        # ._mmapmu is used for this.
        self._mmapmu = threading.Lock()

    def mmap(self, pgoffset, pglen):
        offset = pgoffset * bigarray.pagesize
        length = pglen    * bigarray.pagesize

        with self._mmapmu:
            # grow file, if needed, to cover mmaped range
            needsize = offset + length
            st = os.fstat(self._fh)
            if st.st_size < needsize:
                try:
                    os.ftruncate(self._fh, needsize)
                except OverflowError as e:
                    # OverflowError: Python int too large to convert to C long
                    raise MemoryError(e)

            # create requested mmap
            try:
                return _VMA(self._fh, pgoffset, pglen, bigarray.pagesize, self._prot)

            # ENOMEM -> MemoryError (similarly to BigFile)
            except mmap.error as e:
                if e.errno == errno.ENOMEM:
                    raise MemoryError(e)
                raise

    def __del__(self):
        os.close(self._fh)


# _VMA mimics PyVMA.
#
# it is just mmap.mmap, but, similarly to PyVMA, allows to set .pyuser and
# exposes other PyVMA compatible attributes.
class _VMA(mmap.mmap):
    __slots__ = ['_pgoffset', '_pglen', '_pagesize', 'pyuser']

    def __new__(cls, fh, pgoffset, pglen, pagesize, prot):
        vma = mmap.mmap.__new__(cls,
                fh,
                length  = pglen * pagesize,
                flags   = mmap.MAP_SHARED,
                prot    = prot,
                offset  = pgoffset * pagesize)

        vma._pgoffset   = pgoffset
        vma._pglen      = pglen
        vma._pagesize   = pagesize
        return vma

    def pagesize(self):
        return self._pagesize

    def filerange(self):
        return (self._pgoffset, self._pglen)

    @property
    def addr_start(self):
        # find out address where we are mmapped
        a = np.ndarray(shape=(len(self),), dtype=np.uint8, buffer=self)
        adata = a.__array_interface__.get('data')
        assert adata is not None, "TODO __array_interface__.data = None"
        assert isinstance(adata, tuple), "TODO __array_interface__.data is %r" % (adata,)
        # adata is (data, readonly)
        return adata[0]

    @property
    def addr_stop(self):
        return self.addr_start + len(self)
