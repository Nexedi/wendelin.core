# Wendeling.core.bigarray | Basic tests
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

from wendelin.bigarray import BigArray
from wendelin.bigfile import BigFile
from wendelin.lib.mem import memcpy
from numpy import ndarray, dtype, int32, uint32, uint8, all, arange, multiply, array_equal

from pytest import raises


# Synthetic bigfile that just loads zeros, and ignores writes (= a-la /dev/zero)
class BigFile_Zero(BigFile):

    def loadblk(self, blk, buf):
        # Nothing to do here - the memory buf obtained from OS comes pre-cleared
        # XXX reenable once/if memory comes uninitialized here
        return

    def storeblk(self, blk, buf):
        return


PS = 2*1024*1024    # FIXME hardcoded, TODO -> ram.pagesize


# basic ndarray-compatibility attributes of BigArray
def test_bigarray_basic():
    Z  = BigFile_Zero(PS)
    Zh = Z.fileh_open()

    A = BigArray((10,3), int32, Zh)

    raises(TypeError, "A.data")
    assert A.strides    == (12, 4)
    assert A.dtype      == dtype(int32)
    # XXX .flags?
    # XXX .flat?    (non-basic)
    # XXX .imag?    (non-basic)
    # XXX .real?    (non-basic)
    assert A.size       == 10*3
    assert len(A)       == 10
    assert A.itemsize   == 4
    assert A.nbytes     == 4*10*3
    assert A.ndim       == 2
    assert A.shape      == (10,3)
    # XXX .ctypes   (non-basic)
    # TODO .base



# DoubleGet(obj1, obj2)[key] -> obj1[key], obj2[key]
class DoubleGet:
    def __init__(self, obj1, obj2):
        self.obj1 = obj1
        self.obj2 = obj2

    def __getitem__(self, key):
        return self.obj1[key], self.obj2[key]


# getitem/setitem (1d case)
def test_bigarray_indexing_1d():
    Z  = BigFile_Zero(PS)
    Zh = Z.fileh_open()

    A = BigArray((10*PS,), uint8, Zh)

    # ndarray of the same shape - we'll use it to get slices and compare result
    # shape/stride against BigArray.__getitem__
    A_= ndarray ((10*PS,), uint8)

    # AA[key] -> A[key], A_[key]
    AA = DoubleGet(A, A_)


    # "empty" slices
    assert A[10:5:1]        .size == 0
    assert A[5:10:-1]       .size == 0
    assert A[5:5]           .size == 0
    assert A[100*PS:200*PS] .size == 0


    # whole array
    a, _ = AA[:]
    assert isinstance(a, ndarray)
    assert a.dtype   == dtype(uint8)
    assert a.shape   == _.shape
    assert a.strides == _.strides

    assert a[0] == 0
    assert a[5*PS] == 0
    assert a[10*PS-1] == 0


    # overlaps with a
    b, _ = AA[4*PS:]
    assert isinstance(b, ndarray)
    assert b.dtype   == dtype(uint8)
    assert b.shape   == _.shape
    assert b.strides == _.strides

    assert b[0] == 0
    assert b[1*PS] == 0
    assert b[5*PS-1] == 0

    # a <-> b
    assert b[1*PS] == 0
    a[5*PS] = 1
    assert b[1*PS] == 1


    # non-pagesize aligned slice
    c, _ = AA[4*PS+3 : 9*PS-3]
    assert isinstance(c, ndarray)
    assert c.dtype   == dtype(uint8)
    assert c.shape   == _.shape
    assert c.strides == _.strides

    assert c[0]  == 0
    assert c[-1] == 0

    # a <-> b <-> c
    assert b[3] == 0
    assert a[4*PS+3] == 0
    c[0] = 3
    assert b[3] == 3
    assert a[4*PS+3] == 3

    assert b[5*PS-4] == 0
    assert a[9*PS-4] == 0
    c[-1] = 99
    assert b[5*PS-4] == 99
    assert a[9*PS-4] == 99

    # negative stride
    d, _ = AA[9*PS+1:4*PS-1:-1]
    assert isinstance(d, ndarray)
    assert d.dtype   == dtype(uint8)
    assert d.shape   == _.shape
    assert d.strides == _.strides

    assert all(d[:5] == 0)
    assert d[5] == 99               # c[-1]
    assert all(d[6:-(PS+1)] == 0)
    assert d[-(PS+1)] == 1          # a[5*PS]
    assert all(d[-PS:-4] == 0)
    assert d[-4] == 3               # c[0]
    assert all(d[-3:] == 0)

    # like c, but stride > 1
    e, _ = AA [4*PS+3 : 9*PS-3 : 7]
    assert isinstance(e, ndarray)
    assert e.dtype   == dtype(uint8)
    assert e.shape   == _.shape
    assert e.strides == _.strides
    c[0] = 4
    assert e[0] == c[0]
    c[0] = 5
    assert e[0] == c[0]
    c[7] = 7
    assert e[1] == c[7]
    c[7] = 8
    assert e[1] == c[7]
    # TODO check more

    # like d, but stride < -1
    f, _ = AA[9*PS+1:4*PS-1:-11]
    assert isinstance(f, ndarray)
    assert f.dtype   == dtype(uint8)
    assert f.shape   == _.shape
    assert f.strides == _.strides
    d[0] = 11
    assert f[0] == d[0]
    d[0] = 12
    assert f[0] == d[0]
    d[11] = 13
    assert f[1] == d[11]
    d[11] = 14
    assert f[1] == d[11]
    # TODO check more


    # setitem
    A[2*PS+1:3*PS+2] = 5
    assert all(a[2*PS+1 : 3*PS+2] == 5)
    assert a[2*PS] == 0
    assert a[3*PS+3] == 0

    A[2*PS+2:2*PS+5] = [6,7,8]
    assert a[2*PS+0] == 0
    assert a[2*PS+1] == 5
    assert a[2*PS+2] == 6
    assert a[2*PS+3] == 7
    assert a[2*PS+4] == 8
    assert a[2*PS+5] == 5
    assert a[2*PS+6] == 5

    assert raises(ValueError, 'A[:4] = range(5)')


# given dimension length n, yield index variants to test
def indices_to_test(n):
    # ":"
    yield slice(None)

    # int
    yield 0
    yield -1
    yield n//2

    # start:stop:stride
    yield slice(1,-1)
    yield slice(n//4+1, n*3//4-1, 2)
    yield slice(n//5+1, n*4//5-1, 3)


# geven shape, yield all Nd idx variant, where every index iterates full indices_to_test
def idx_to_test(shape, idx_prefix=()):
    leaf = len(shape) <= 1
    for i in indices_to_test(shape[0]):
        idx = idx_prefix + (i,)
        if leaf:
            yield idx
        else:
            # = yield from
            for _ in idx_to_test(shape[1:], idx):
                yield _


# getitem/setitem (Nd case)
def test_bigarray_indexing_Nd():
    # shape of tested array - all primes, total size for uint32 ~ 7 2M pages
    # XXX even less dimensions (to speed up tests)?
    shape = tuple(reversed( (17,23,101,103) ))

    # test data - all items are unique - so we can check array by content
    # NOTE +PS so that BigFile_Data has no problem loading last blk
    #      (else data slice will be smaller than buf)
    data  = arange(multiply.reduce(shape) + PS, dtype=uint32)

    # synthetic bigfile that loads data from `data`
    class BigFile_Data(BigFile):
        def loadblk(self, blk, buf):
            datab = data.view(uint8)
            x = datab[self.blksize * blk : self.blksize * (blk+1)]
            memcpy(buf, x)

        def storeblk(self, blk, buf):
            raise RuntimeError('tests should not try to change test data')

    f  = BigFile_Data(PS)
    fh = f.fileh_open()

    A  = BigArray(shape, uint32, fh)                    # bigarray with test data and shape
    A_ = data[:multiply.reduce(shape)].reshape(shape)   # ndarray  ----//----


    # now just go over combinations of various slice at each dimension, and see
    # whether slicing result is the same ndarray would do.
    for idx in idx_to_test(shape):
        a  = A [idx]
        a_ = A_[idx]

        assert array_equal(a, a_)

    # TODO ... -> expanded (0,1,2,negative), rejected if many
    # TODO newaxis
    # TODO nidx < len(shape)
    # TODO empty slice in major row, empty slice in secondary row
    """
    # ellipsis  - take some idx[a:b] and replace it by ...
    for ellipsis in range(2):   # 0 - no ellipsis

        # newaxis   - added after at some position(s)
        for newaxis in range(3):    # 0 - no newaxis
    """