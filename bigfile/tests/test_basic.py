# Wendeling.core.bigfile | Basic tests
# Copyright (C) 2014-2015  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Open Source Initiative approved licenses and Convey
# the resulting work. Corresponding source of such a combination shall include
# the source code for all other software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# TODO description
from wendelin.bigfile import BigFile, WRITEOUT_STORE, WRITEOUT_MARKSTORED
import struct
import weakref
import sys
import gc

from pytest import raises
from six import PY2

# read-only attributes are reported differently on py2 & py3
ROAttributeError = (PY2 and TypeError or AttributeError)

# on python2 memoryview[i] returns str, on python3 uint
bchr_py2 = (PY2 and chr or (lambda _: _))
bord_py3 = (PY2 and (lambda _: _) or ord)



# Synthetic bigfile that for #blk
#   - loads generated ~ #blk bytes and
#   - remembers stores #blk in .storev[]
class XBigFile(BigFile):

    def __new__(cls, head, blksize):
        obj = BigFile.__new__(cls, blksize)
        obj.head   = head
        obj.storev = []
        return obj


    # load: fill head with head, later with #blk
    def loadblk(self, blk, buf):
        # head + <byte-fill> to tail...
        hlen = len(self.head)
        tlen = self.blksize - hlen

        fillbyte = blk & 0xff
        v = [self.head] + [fillbyte]*tlen

        struct.pack_into("%is%iB" % (hlen, tlen), buf, 0, *v)


    # store: store #blk in storev
    def storeblk(self, blk, buf):
        self.storev.append(blk)


# XXX hack, hardcoded
MB = 1024*1024
PS = 2*MB

# basic loadblk/storeblk test
def test_basic():
    f = XBigFile(b'abcd', PS)

    assert f.blksize == PS
    raises(ROAttributeError, "f.blksize = 1") # RO attribute
    assert f.head == b'abcd'

    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(100, 4)

    m = memoryview(vma)
    if PY2:
        b = buffer(vma)     # `buffer` is not available in python3
    else:
        b = memoryview(vma) # no harm testing with memoryview twice

    assert len(m) == 4*PS
    assert len(b) == 4*PS

    # read
    for p in range(4):
        _ = p*PS
        assert b[_+0:_+4]   == b'abcd'
        assert b[_+5]       == bchr_py2(100+p)
        assert b[_+PS-1]    == bchr_py2(100+p)
        assert m[_+0:_+4]   == b'abcd'
        assert m[_+5]       == bchr_py2(100+p)
        assert m[_+PS-1]    == bchr_py2(100+p)



    # write
    m[2*PS + 7]     = bord_py3(b'A')    # write -> page[102]
    assert b[2*PS + 7] == bord_py3('A') # ensure b & m point to the same memory
    m[7]            = bord_py3(b'B')    # write -> page[100]

    assert f.storev == []
    fh.dirty_writeout(WRITEOUT_STORE)
    assert f.storev == [100, 102]   # pages we wrote in ascending order

    f.storev = []
    fh.dirty_writeout(WRITEOUT_STORE)
    assert f.storev == [100, 102]   # again, because was not marked yet

    f.storev = []
    fh.dirty_writeout(WRITEOUT_STORE | WRITEOUT_MARKSTORED)
    assert f.storev == [100, 102]   # again, because was not marked yet

    f.storev = []
    fh.dirty_writeout(WRITEOUT_STORE)
    assert f.storev == []           # nothing wrote - all dirty were just marked


    # TODO close f


# test that python exception state is preserved across pagefaulting
def test_pagefault_savestate():
    keep = []
    class BadFile(BigFile):
        def loadblk(self, blk, buf):
            # simulate some errors in-between to overwrite thread exception
            # state, and say we are done ok
            try:
                1/0
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()

            assert exc_type is ZeroDivisionError


            # NOTE there is a loop created here:
            #
            #   exc_traceback
            #     |        ^
            #     |        |
            #     v     .f_localsplus
            #    frame
            #
            # which result in holding additional ref to buf, but loadblk caller
            # will detect and handle this situation via garbage-collecting
            # above cycle.

            # and even if we keep traceback alive it will care to detach buf
            # from frame via substituting another stub object inplace of it
            exc_traceback.tb_frame.f_locals
            keep.append(exc_traceback)

            # check same when happenned in function one more level down
            self.func(buf)

            self.loadblk_run = 1


        def func(self, arg):
            try:
                1/0
            except:
                _, _, exc_traceback = sys.exc_info()

            assert exc_traceback is not None
            keep.append(exc_traceback)



    f   = BadFile(PS)
    fh  = f.fileh_open()
    vma = fh.mmap(0, 1)
    m   = memoryview(vma)

    f.loadblk_run = 0
    try:
        raise RuntimeError('hello')
    except RuntimeError:
        exc_type, exc_value, exc_tb = sys.exc_info()
        assert exc_type  is RuntimeError
        assert exc_value.args == ('hello',)

        assert m[0] == bchr_py2(0)      # NOTE causes pagefault handler to run
        assert f.loadblk_run == 1

        exc_type2, exc_value2, exc_tb2 = sys.exc_info()
        assert exc_type  is exc_type2
        assert exc_value is exc_value2
        assert exc_tb    is exc_tb2

    assert keep[0].tb_frame.f_locals['buf'] == "<pybuf>"  # the stub object
    assert keep[1].tb_frame.f_locals['arg'] == "<pybuf>"  # ----//----


    # TODO close f


# test that vma/fileh/file correctly refcount each other
def test_refcounting():
    # fileh holds file
    f  = XBigFile(b'abcd', PS)
    wf = weakref.ref(f)
    assert wf() is f
    fh = f.fileh_open()
    assert wf() is f
    idf = id(f)
    del f
    f_ = wf()
    assert f_ is not None
    assert id(f_) == idf
    del f_, fh
    assert wf() is None

    # vma holds fileh
    f   = XBigFile(b'abcd', PS)
    fh  = f.fileh_open()
    wfh = weakref.ref(fh)
    assert wfh() is fh
    vma = fh.mmap(0, 1)
    assert wfh() is fh
    idfh = id(fh)
    del fh
    fh_ = wfh()
    assert fh_ is not None
    assert id(fh_) == idfh
    del fh_, vma
    assert wfh() is None

    # vma is weakly-referencable too
    fh  = f.fileh_open()
    vma = fh.mmap(0, 1)
    wvma= weakref.ref(vma)
    assert wvma() is vma
    idvma = id(vma)
    m = memoryview(vma)
    del vma
    vma_ = wvma()
    assert vma_ is not None
    assert id(vma_) == idvma
    del vma_
    assert wvma() is not None
    del m
    assert wvma() is None


# test that the system works correctly, even if GC triggers while we are in
# segfault handler, and do virtmem calls
class DelBigFile(BigFile):

    def __new__(cls, obj_4del, blksize):
        obj = BigFile.__new__(cls, blksize)
        obj.obj_4del = obj_4del
        obj.marker_list = []
        return obj

    def loadblk(self, blk, buf):
        # we are in sighandler - delete obj_4del thus triggering it's dealloc
        assert self.obj_4del is not None
        w = weakref.ref(self.obj_4del)
        assert w() is self.obj_4del
        self.obj_4del = None
        assert w() is None  # make sure obj_4del was deleted

        # nothing to do - let the block be rest zerofilled, just mark we were here
        self.marker_list.append(1)


class C:
    pass

class GCBigFile(DelBigFile):

    def loadblk(self, blk, buf):
        # we are in sighandler - establish cycle which also referenced obj_4del and trigger full GC
        assert self.obj_4del is not None
        w = weakref.ref(self.obj_4del)
        assert w() is self.obj_4del

        # establish cycle with leaf ref to obj_4del
        a = C()
        b = C()
        a.b = b
        b.a = a
        a.obj_4del = self.obj_4del

        self.obj_4del = None
        assert w() is not None

        # del a=b cycle - it should stay alice, while gc is disabled
        gc_save = gc.isenabled()
        gc.disable()

        del a, b
        assert w() is not None

        # gc - a=b and obj_4del collected
        gc.collect()
        assert w() is None

        if gc_save:
            gc.enable()

        self.marker_list.append(2)


def test_gc_from_sighandler():
    f1  = XBigFile(b'abcd', PS)
    fh1 = f1.fileh_open()
    vma1= fh1.mmap(0, 1)

    f2  = DelBigFile(vma1, PS)
    del vma1, fh1, f1
    fh2 = f2.fileh_open()
    vma2= fh2.mmap(0, 1)

    m2 = memoryview(vma2)
    assert f2.marker_list == []
    m2[0]
    assert f2.marker_list == [1]

    f3  = GCBigFile(vma2, PS)
    del m2, vma2, fh2, f2
    fh3 = f3.fileh_open()
    vma3= fh3.mmap(0, 1)

    m3 = memoryview(vma3)
    assert f3.marker_list == []
    m3[0]
    assert f3.marker_list == [2]
