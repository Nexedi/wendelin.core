# Wendelin.bigfile | benchmarks for virtmem
# Copyright (C) 2014-2015  Nexedi SA and Contributors.
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

# benchmark computing adler32 sum of a whole-hole file via BigFile_File backend
# and compare that to baseline of doing the same via standard OS services.
#
# then do the same benchmarks for file with data.
import os
from os import ftruncate, close, unlink, O_RDONLY, O_RDWR, O_TRUNC
from mmap import mmap, MAP_SHARED, MAP_ANONYMOUS, PROT_READ, PROT_WRITE
from io import FileIO
from wendelin.bigfile.file_file import BigFile_File
from wendelin.bigfile import WRITEOUT_STORE, WRITEOUT_MARKSTORED
from wendelin.lib.testing import Adler32, nulladler32_bysize, ffadler32_bysize
from wendelin.bigarray.tests.test_basic import BigFile_Zero
from wendelin.lib.mem import bzero, memset
from tempfile import NamedTemporaryFile

from six import PY2
from six.moves import range as xrange

# PY2 -> buffer
# PY3 -> memoryview
# (rationale: a lot of functions do not accept memoryview on py2 - e.g. adler32)
def xbuffer(obj, offset=0, size=None):
    if size is None:
        size = len(obj) - offset
    if PY2:
        return buffer(obj, offset, size)
    else:
        return memoryview(obj)[offset:offset+size]


tmpf = None
blksize  =   2*1024*1024   # XXX hardcoded
filesize = 512*1024*1024
nulladler32 = nulladler32_bysize(filesize)
ffadler32   = ffadler32_bysize(filesize)



# setup whole-hole OS file
def setup_module():
    global tmpf
    tmpf = NamedTemporaryFile(prefix='hole.', delete=False)
    tmpf.close()

    fd = os.open(tmpf.name, O_RDWR | O_TRUNC)
    ftruncate(fd, filesize)
    close(fd)

def teardown_module():
    unlink(tmpf.name)


# BigFile that reads as zeros and tracks last loadblk request
class BigFile_ZeroTrack(BigFile_Zero):

    def loadblk(self, blk, buf):
        #print('zload #%d' % blk)
        self.last_load = blk
        super(BigFile_ZeroTrack, self).loadblk(blk, buf)

# benchmark the time it takes for virtmem to handle pagefault with noop loadblk
# implemented  in Python.
def bench_pagefault_py(b):
    npage = b.N
    PS  = blksize   # XXX assumes blksize = pagesize

    f   = BigFile_ZeroTrack(PS)
    fh  = f.fileh_open()
    vma = fh.mmap(0, npage)
    m   = memoryview(vma)

    b.reset_timer()
    for p in xrange(npage):
        m[p*PS]
        assert f.last_load == p

    del m
    del vma # vma.close()
    del fh  # fh.close()
    del f   # f.close()


# compute hash via mmaping the file at OS-level
def _bench_file_mmapread(hasher, expect):
    fd = os.open(tmpf.name, O_RDONLY)
    fmap = mmap(fd, filesize, MAP_SHARED, PROT_READ)
    h = hasher()
    h.update(fmap)

    fmap.close()
    close(fd)
    assert h.digest() == expect


# compute hash via reading the file at OS-level
# uses intermediate 1-blksize buffer
def _bench_file_read(hasher, expect):
    f = FileIO(tmpf.name, 'r')
    b = bytearray(blksize)

    h = hasher()
    while 1:
        n = f.readinto(b)
        if n == 0:
            break

        h.update(xbuffer(b, 0, n))  # NOTE b[:n] does copy

    f.close()
    assert h.digest() == expect


# compute hash via reading the file at OS-level
# uses memory buffer ~ filesize
def _bench_file_readbig(hasher, expect):
    f = FileIO(tmpf.name, 'r')
    #b = mmap(-1, filesize, MAP_SHARED | MAP_ANONYMOUS, PROT_READ | PROT_WRITE)
    b = bytearray(filesize)
    bm= memoryview(b)

    h = hasher()
    pos = 0
    while 1:
        n = f.readinto(bm[pos:])
        if n == 0:
            break

        h.update(xbuffer(b, pos,n)) # NOTE b[pos:n] does copy
        pos += n

    del bm
    del b
    f.close()
    assert h.digest() == expect


# compute hash via mmaped BigFile_File
def _bench_bigf_read(hasher, expect):
    # bigfile & mapping
    f   = BigFile_File(tmpf.name, blksize)
    fh  = f.fileh_open()
    vma = fh.mmap(0, filesize//blksize)

    # hash of the whole content
    h = hasher()
    h.update(vma)

    # TODO cleanup
    del vma #vma.close()
    del fh  #fh.close()
    del f   #f.close()
    assert h.digest() == expect


def bench_file_mmapread_hole():     _bench_file_mmapread(Adler32,   nulladler32)
def bench_file_read_hole():         _bench_file_read    (Adler32,   nulladler32)
def bench_file_readbig_hole():      _bench_file_readbig (Adler32,   nulladler32)
def bench_bigf_read_hole():         _bench_bigf_read    (Adler32,   nulladler32)


# write to file via mmap at OS-level
def bench_file_mmapwrite0():
    fd = os.open(tmpf.name, O_RDWR)
    fmap = mmap(fd, filesize, MAP_SHARED, PROT_READ | PROT_WRITE)
    bzero(fmap)
    fmap.close()
    # NOTE calls munmap - thus no fmap.flush() is needed (and flush calls
    # msync(MS_SYNC) which also does fsync() which we don't want here)
    del fmap
    close(fd)


# write to file via OS write
def bench_file_write55():
    f = FileIO(tmpf.name, 'r+')
    zblk = b'\x55' * blksize
    for i in xrange(filesize // blksize):
        pos = 0
        while pos < blksize:
            n = f.write(memoryview(zblk)[pos:])
            assert n != 0
            pos += n
    f.close()


# write to file via mmaped BigFile_File
def bench_bigf_writeff():
    # bigfile & mapping
    f   = BigFile_File(tmpf.name, blksize)
    fh  = f.fileh_open()
    vma = fh.mmap(0, filesize//blksize)

    memset(vma, 0xff)
    fh.dirty_writeout(WRITEOUT_STORE | WRITEOUT_MARKSTORED)

    # TODO cleanup
    del vma #vma.close()
    del fh  #fh.close()
    del f   #f.close()



def bench_file_mmapread():          _bench_file_mmapread(Adler32,   ffadler32)
def bench_file_read():              _bench_file_read    (Adler32,   ffadler32)
def bench_file_readbig():           _bench_file_readbig (Adler32,   ffadler32)
def bench_bigf_read():              _bench_bigf_read    (Adler32,   ffadler32)
