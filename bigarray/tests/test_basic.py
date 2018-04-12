# -*- coding: utf-8 -*-
# Wendeling.core.bigarray | Basic tests
# Copyright (C) 2014-2018  Nexedi SA and Contributors.
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

from wendelin.bigarray import BigArray, ArrayRef, _flatbytev
from wendelin.bigfile import BigFile
from wendelin.lib.mem import memcpy
from wendelin.lib.calc import mul
from numpy import ndarray, dtype, int64, int32, uint32, int16, uint8, all, zeros, arange, \
        array_equal, asarray, newaxis, swapaxes
from numpy.lib.stride_tricks import as_strided
import numpy

from pytest import raises


# Synthetic bigfile that just loads zeros, and ignores writes (= a-la /dev/zero)
class BigFile_Zero(BigFile):

    def loadblk(self, blk, buf):
        # Nothing to do here - the memory buf obtained from OS comes pre-cleared
        # XXX reenable once/if memory comes uninitialized here
        return

    def storeblk(self, blk, buf):
        return


# Synthetic bigfile that loads/stores data from/to numpy array
class BigFile_Data(BigFile):

    def __new__(cls, data, blksize):
        obj = BigFile.__new__(cls, blksize)
        obj.datab = data.view(uint8)
        return obj

    def loadblk(self, blk, buf):
        x = self.datab[self.blksize * blk : self.blksize * (blk+1)]
        memcpy(buf, x)

    def storeblk(self, blk, buf):
        memcpy(self.datab[self.blksize * blk : self.blksize * (blk+1)], buf)


# synthetic bigfile that only loads data from numpy array
class BigFile_Data_RO(BigFile_Data):
    def storeblk(self, blk, buf):
        raise RuntimeError('tests should not try to change test data')


PS = 2*1024*1024    # FIXME hardcoded, TODO -> ram.pagesize


# make sure we don't let dtype with object to be used with BigArray
def test_bigarray_noobject():
    Z  = BigFile_Zero(PS)
    Zh = Z.fileh_open()

    # NOTE str & unicode are fixed-size types - if size is not explicitly given
    # it will become S0 or U0
    obj_dtypev = [numpy.object, 'O', 'i4, O', [('x', 'i4'), ('y', 'i4, O')]]
    for dtype_ in obj_dtypev:
        raises(TypeError, "BigArray((1,), dtype_, Zh)")


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


    B = BigArray((10,3), int32, Zh, order='F')

    raises(TypeError, "B.data")
    assert B.strides    == (4, 40)
    assert B.dtype      == dtype(int32)
    # XXX .flags?
    # XXX .flat?    (non-basic)
    # XXX .imag?    (non-basic)
    # XXX .real?    (non-basic)
    assert B.size       == 10*3
    assert len(B)       == 3
    assert B.itemsize   == 4
    assert B.nbytes     == 4*10*3
    assert B.ndim       == 2
    assert B.shape      == (10,3)
    # XXX .ctypes   (non-basic)
    # TODO .base



# DoubleGet(obj1, obj2)[key] -> obj1[key], obj2[key]
class DoubleGet:
    def __init__(self, obj1, obj2):
        self.obj1 = obj1
        self.obj2 = obj2

    def __getitem__(self, key):
        return self.obj1[key], self.obj2[key]


# DoubleCheck(A1, A2)[key] -> assert array_equal(A1[key], A2[key])
class DoubleCheck(DoubleGet):

    def __getitem__(self, key):
        a1, a2 = DoubleGet.__getitem__(self, key)
        assert array_equal(a1, a2)


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


    # BigArray does not support advanced indexes
    # (in numpy they create _copy_ picking up elements)
    A_[0:5] = range(0,10,2)
    assert array_equal(A_[[0,1,2,3,4]], [0,2,4,6,8])
    raises (TypeError, 'A[[0,1,2,3,4]]')

    # index out of range
    # - element access  -> raises IndexError
    # - slice access    -> empty
    A_[-1] = 0
    assert AA[10*PS-1] == (0,0)
    raises(IndexError, 'A_[10*PS]')
    raises(IndexError, 'A [10*PS]')
    a, _ = AA[10*PS:10*PS+1]
    assert isinstance(a, ndarray)
    assert array_equal(a, _)
    assert a.dtype == dtype(uint8)
    assert a.shape == (0,)


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


# indexing where accessed element overlaps edge between pages
def test_bigarray_indexing_pageedge():
    shape = (10, PS-1)
    data  = arange(mul(shape), dtype=uint32).view(uint8)    # NOTE 4 times bigger than uint8

    f  = BigFile_Data_RO(data, PS)
    fh = f.fileh_open()

    A  = BigArray(shape, uint8, fh)                     # bigarray with test data and shape
    A_ = data[:mul(shape)].reshape(shape)               # ndarray  ----//----

    # AA[key] -> assert array_equal(A[key], A_[key])
    AA = DoubleCheck(A, A_)

    AA[0]
    AA[1]           # tail of page0 - page1
    AA[1:2]         # ---- // ----
    AA[1:2:-1]      # []
    AA[1:0]         # []
    AA[1:0:-1]      # tail of page0 - page1


    shape = (10, PS+1)
    f  = BigFile_Data_RO(data, PS)
    fh = f.fileh_open()

    A  = BigArray(shape, uint8, fh)
    A_ = data[:mul(shape)].reshape(shape)

    AA = DoubleCheck(A, A_)

    AA[0]           # page0 - head of page1
    AA[0:1]         # ---- // ----
    AA[0:1:-1]      # []
    AA[1:0]         # []
    AA[1:0:-1]      # page0 - head of page1



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
    data  = arange(mul(shape) + PS, dtype=uint32)

    f  = BigFile_Data_RO(data, PS)
    fh = f.fileh_open()

    for order in ('C', 'F'):
        A  = BigArray(shape, uint32, fh, order=order)       # bigarray with test data and shape
        A_ = data[:mul(shape)].reshape(shape, order=order)  # ndarray  ----//----

        # AA[key] -> A[key], A_[key]
        AA = DoubleGet(A, A_)


        # now just go over combinations of various slice at each dimension, and see
        # whether slicing result is the same ndarray would do.
        for idx in idx_to_test(shape):
            a, a_ = AA[idx]
            assert array_equal(a, a_)


        # any part of index out of range
        # - element access  -> raises IndexError
        # - slice access    -> empty
        for idxpos in range(len(shape)):
            idx  = [0]*len(shape)
            # idx -> tuple(idx)
            # ( list would mean advanced indexing - not what we want )
            idxt = lambda : tuple(idx)

            # valid access element access
            idx[idxpos] = shape[idxpos] - 1     # 0, 0, 0,  Ni-1, 0 ,0, 0
            a, a_ = AA[idxt()]
            assert array_equal(a, a_)

            # out-of-range element access
            idx[idxpos] = shape[idxpos]         # 0, 0, 0,  Ni  , 0 ,0, 0
            raises(IndexError, 'A [idxt()]')
            raises(IndexError, 'A_[idxt()]')

            # out-of-range slice access
            idx[idxpos] = slice(shape[idxpos],  # 0, 0, 0,  Ni:Ni+1  , 0 ,0, 0
                                shape[idxpos]+1)
            a, a_ = AA[idxt()]
            assert array_equal(a, a_)
            assert a .size == 0
            assert a_.size == 0


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


def test_bigarray_resize():
    data = zeros(8*PS, dtype=uint32)
    f   = BigFile_Data(data, PS)
    fh  = f.fileh_open()

    # set first part & ensure it is set correctly
    A   = BigArray((10,3), uint32, fh)
    A[:,:] = arange(10*3, dtype=uint32).reshape((10,3))

    a = A[:]
    assert array_equal(a.ravel(), arange(10*3, dtype=uint32))

    # grow array
    A.resize((11,3))

    # a as already mapped, should stay the same
    assert array_equal(a.ravel(), arange(10*3, dtype=uint32))

    # mapping it once again maps it whole with new size
    b = A[:]
    assert isinstance(b, ndarray)
    assert b.shape  == (11,3)
    assert b.dtype  == dtype(uint32)

    # head data is the same as a
    assert array_equal(a, b[:10,:])

    # tail is zeros
    assert array_equal(b[10,:], zeros(3, dtype=uint32))

    # old mapping stays valid and changes propageate to/from it
    assert a[0,0] == 0
    assert b[0,0] == 0
    a[0,0] = 1
    assert b[0,0] == 1
    b[0,0] = 2
    assert a[0,0] == 2
    a[0,0] = 0
    assert b[0,0] == 0

    assert a[  -1,-1] == 10*3-1
    assert b[10-1,-1] == 10*3-1
    a[  -1,-1] = 1
    assert b[10-1,-1] == 1
    b[10-1,-1] = 2
    assert a[  -1,-1] == 2
    a[  -1,-1] = 10*3-1
    assert b[10-1,-1] == 10*3-1

    # we cannot access old mapping beyond it's end
    assert raises(IndexError, 'a[10,:]')

    # we can change tail
    b[10,:] = arange(10*3, (10+1)*3)

    # map it whole again and ensure we have correct data
    c = A[:]
    assert array_equal(c.ravel(), arange(11*3, dtype=uint32))


# ~ arange(n*3*2).reshape(n,3,2)
def arange32_c(start, stop, dtype=None):
    return arange(start*3*2, stop*3*2, dtype=dtype).reshape((stop-start),3,2)
def arange32_f(start, stop, dtype=None):
    return arange(start*3*2, stop*3*2, dtype=dtype).reshape(2,3,(stop-start), order='F')
    #return arange(start*3*2, stop*3*2, dtype=dtype).reshape(2,3,(stop-start))

def test_bigarray_append():
    for order in ('C', 'F'):
        data = zeros(8*PS, dtype=uint32)
        f   = BigFile_Data(data, PS)
        fh  = f.fileh_open()

        arange32 = {'C': arange32_c, 'F': arange32_f} [order]

        # first make sure arange32 works correctly
        x = numpy.append( arange32(0,4), arange32(4,7), axis={'C': 0, 'F': 2} [order] )
        #x = numpy.append( arange32(0,4), arange32(4,7), axis=0)
        #x = numpy.append( arange32(0,4), arange32(4,7))
        assert array_equal(x, arange32(0,7))
        assert array_equal(x.ravel(order), arange(7*3*2))

        # init empty BigArray of shape (x,3,2)
        A   = BigArray({'C': (0,3,2), 'F': (2,3,0)} [order], int64, fh, order=order)
        assert array_equal(A[:], arange32(0,0))

        # append initial data
        A.append(arange32(0,2))
        assert array_equal(A[:], arange32(0,2))
        A.append(arange32(2,3))
        assert array_equal(A[:], arange32(0,3))

        # append plain list (test for arg conversion)
        A.append({'C': [[[18,19], [20,21], [22,23]]],   \
                  'F': [[[18],[20],[22]], [[19],[21],[23]]]}  [order])
        assert array_equal(A[:], arange32(0,4))

        # append with incorrect shape - rejected, original stays the same
        assert raises(ValueError, 'A.append(arange(3))')
        assert array_equal(A[:], arange32(0,4))
        assert raises(ValueError, 'A.append(arange(3*2).reshape(3,2))')
        assert array_equal(A[:], arange32(0,4))

        # append with correct shape, but incompatible dtype - rejected, original stays the same
        assert raises(ValueError,
                {'C': 'A.append(asarray([[[0,1], [2,3], [4,"abc"]]], dtype=object))',
                 'F': 'A.append(asarray([[[0],[1],[2]], [[3],[4],["abc"]]], dtype=object))'} [order] )
        assert array_equal(A[:], arange32(0,4))




def test_bigarray_list():
    Z  = BigFile_Zero(PS)
    Zh = Z.fileh_open()
    A = BigArray((10,), uint8, Zh)

    # the IndexError for out-of-bound scalar access should allow, though
    # inefficient, for list(A) to work (instead of looping inside forever)
    l  = list(A)
    assert isinstance(l, list)
    assert l == [0]*10


def test_bigarray_to_ndarray():
    Z  = BigFile_Zero(PS)
    Zh = Z.fileh_open()
    A = BigArray((10,), uint8, Zh)

    # without IndexError on out-of-bound scalar access, the following
    # - would work with numpy-1.8
    # - would loop forever eating memory with numpy-1.9
    a = asarray(A)
    assert array_equal(a, A[:])


    # "medium"-sized array of 1TB. converting it to ndarray should work here
    # without hanging, becuse initially all data are unmapped, and we don't
    # touch mapped memory.
    B = BigArray((1<<40,), uint8, Zh)
    b = asarray(B)
    assert isinstance(b, ndarray)
    assert b.nbytes == 1<<40


    # array of size larger than virtual address space (~ 2^47 on linux/amd64)
    # converting it to ndarray is should be not possible
    for i in range(48,65):
        C = BigArray(((1<<i)-1,), uint8, Zh)
        raises(MemoryError, 'asarray(C)')




def test_arrayref():
    # test data - all items are unique - so we can check array by content
    data = zeros(PS, dtype=uint8)
    data32 = data.view(uint32)
    data32[:] = arange(len(data32), dtype=uint32)
    data[:256] = arange(256, dtype=uint8)   # first starting bytes are all unique

    # regular ndarray without parent at all
    ref = ArrayRef(data)
    assert ref.root is data
    assert ref.lo == 0
    assert ref.hi == len(data)
    assert ref.z0 == 0
    assert ref.shape == data.shape
    assert ref.stridev == data.strides
    assert ref.dtype == data.dtype
    assert array_equal(ref.deref(), data)

    # regular ndarrays with parent
    ref = ArrayRef(data32)
    assert ref.root is data
    assert ref.lo == 0
    assert ref.hi == len(data)
    assert ref.z0 == 0
    assert ref.shape == data32.shape
    assert ref.stridev == data32.strides
    assert ref.dtype == data32.dtype
    assert array_equal(ref.deref(), data32)

    a = data[100:140]
    ref = ArrayRef(a)
    assert ref.root is data
    assert ref.lo == 100
    assert ref.hi == 140
    assert ref.z0 == 0
    assert ref.shape == (40,)
    assert ref.stridev == (1,)
    assert ref.dtype == data.dtype
    assert array_equal(ref.deref(), a)

    a = data[140:100:-1]
    ref = ArrayRef(a)
    assert ref.root is data
    assert ref.lo == 101
    assert ref.hi == 141
    assert ref.z0 == 39
    assert ref.shape == (40,)
    assert ref.stridev == (-1,)
    assert ref.dtype == data.dtype
    assert array_equal(ref.deref(), a)

    a = data[100:140:-1]   # empty
    ref = ArrayRef(a)
    assert ref.root is data
    assert ref.lo == 0
    assert ref.hi == 1
    assert ref.z0 == 0
    assert ref.shape == (0,)
    assert ref.stridev == (1,)
    assert ref.dtype == data.dtype
    assert array_equal(ref.deref(), a)

    # rdata is the same as data[::-1] but without base - i.e. it is toplevel
    m = memoryview(data[::-1])
    rdata = asarray(m)
    assert array_equal(rdata[::-1], data)
    assert rdata.strides == (-1,)
    m_ = rdata.base
    assert isinstance(m_, memoryview)
    #assert m_ is m  XXX strangely it is another object, not exactly m
    # XXX however rdata.strides<0 and no rdata.base.base is enough for us here.
    raises(AttributeError, 'm_.base')

    a = rdata[100:140]
    ref = ArrayRef(a)
    assert ref.root is rdata
    assert ref.lo == PS - 140
    assert ref.hi == PS - 100
    assert ref.z0 == 39
    assert ref.shape == (40,)
    assert ref.stridev == (-1,)
    assert ref.dtype == data.dtype
    assert array_equal(ref.deref(), a)


    # BigArray with data backend.
    # data_ is the same as data but shifted to exercise vma and vma->broot offsets calculation.
    data_ = zeros(8*PS, dtype=uint8)
    data_[2*PS-1:][:PS] = data
    f  = BigFile_Data_RO(data_, PS)
    fh = f.fileh_open()
    A  = BigArray(data_.shape, data_.dtype, fh)
    assert array_equal(A[2*PS-1:][:PS], data)


    for root in (data, rdata, A):  # both ndarray and BigArray roots
        # refok verifies whether ArrayRef(x) works ok
        def refok(x):
            ref = ArrayRef(x)
            assert ref.root is root
            x_ = ref.deref()
            assert array_equal(x_, x)
            assert x_.dtype == x.dtype
            assert type(x_) == type(x)

            # check that deref won't access range outside lo:hi - by copying
            # root, setting bytes in adjusted root outside lo:hi to either 0x00
            # or 0xff and tweaking ref.root = root_.
            root_ = numpy.copy(_flatbytev(root[:]))
            root_[:ref.lo] = 0
            root_[ref.hi:] = 0
            ref.root = root_
            assert array_equal(ref.deref(), x)
            root_[:ref.lo] = 0xff
            root_[ref.hi:] = 0xff
            assert array_equal(ref.deref(), x)


        if isinstance(root, BigArray):
            a = root[2*PS-1:][:PS]      # get to `data` range
        # typeof(root) = ndarray
        elif root is rdata:
            a = root[::-1]              # rdata
        else:
            a = root[:]                 # data
        assert array_equal(a, data)

        # subslices that is possible to get by just indexing
        refok( a[:]         )
        refok( a[1:2]       )
        refok( a[1:10]      )
        refok( a[1:10:2]    )
        refok( a[1:10:3]    )
        refok( a[1:10:-1]   )   # empty (.size = 0)
        refok( a[10:1:-1]   )
        refok( a[10:1:-2]   )
        refok( a[10:1:-3]   )

        # long chain root -> a -> a[...] -> a[...] -> leaf
        l = a[2:118]
        l = l.view(uint32)[3:20]
        l = l[1:9]
        refok(l)

        # not aligned - it is not possible to get to resulting slice just by indexing A
        refok( a.view(uint8)[2:-2].view(uint32)         )
        refok( a.view(uint8)[2:-2].view(uint32)[::-1]   )

        refok( a.view(int64)        )   # change of type ↑ in size
        refok( a.view(int64)[::-1]  )
        refok( a.view(int16)        )   # change of type ↓ in size
        refok( a.view(int16)[::-1]  )

        # change of type to size not multiple of original
        refok( a[1:1+5*10].view('V5')       )   # 4 -> 5
        refok( a[1:1+5*10].view('V5')[::-1] )
        refok( a[1:1+3*10].view('V3')       )   # 4 -> 3
        refok( a[1:1+3*10].view('V3')[::-1] )

        # intermediate parent with <0 stride
        r = a[1:1+3*10].view('V3')[::-1]
        refok( r[-2:2:-1]   )

        # 2d array
        x = a.view(uint32).reshape((8, -1))
        y = swapaxes(x, 0,1)
        assert x.shape   == (8, PS//(4*8))
        assert x.strides == (PS//8, 4)
        assert y.shape   == (PS//(4*8), 8)
        assert y.strides == (4, PS//8)

        refok( x )
        refok( y )

        # array with both >0 and <0 strides
        x_ = x[:,::-1]
        y_ = y[:,::-1]
        assert x_.shape   == x.shape
        assert x_.strides == (PS//8, -4)
        assert y_.shape   == y.shape
        assert y_.strides == (4, -PS//8)

        refok( x_ )
        refok( y_ )

        # array with [1] dimension
        z1 = x[:, newaxis, :]
        assert z1.shape   == (8, 1, PS//(4*8))
        assert z1.strides == (PS//8, 0, 4)

        refok(z1)

        # array with [0] dimension
        z0 = z1[:, 0:0, :]
        assert z0.shape   == (8, 0, PS//(4*8))
        assert z0.strides == (PS//8, 0, 4)

        refok(z0)

        # tricky array overlapping itself
        t = a.view(uint32)
        assert t.shape    == (PS//4,)
        assert t.strides  == (4,)
        assert t.itemsize == 4
        t = as_strided(t, strides=(1,))
        assert t.shape    == (PS//4,)
        assert t.strides  == (1,)
        assert t.itemsize == 4

        refok(t)

        # structured dtype
        s = a.view(dtype=[('width', '<i2'), ('length', '<i2')])
        assert s.shape    == (PS//4,)
        assert s.strides  == (4,)
        assert s.itemsize == 4
        refok(s)

        s_ = s['length']
        assert s_.shape    == (PS//4,)
        assert s_.strides  == (4,)
        assert s_.itemsize == 2
        refok(s_)


        # ndarray subclass, e.g. np.recarray
        r = s.view(type=numpy.recarray)
        assert isinstance(r, numpy.recarray)
        assert r.shape    == (PS//4,)
        assert r.strides  == (4,)
        assert r.itemsize == 4
        assert array_equal(r.length, s['length'])
        refok(r)
