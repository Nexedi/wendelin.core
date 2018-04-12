# -*- coding: utf-8 -*-
# BigArray submodule for Wendelin
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

"""BigArrays are NumPy ndarray-like interface on top of BigFile memory mappings.

I.e. something like numpy.memmap for numpy.ndarray and OS files. The whole
bigarray cannot be used as a drop-in replacement for numpy arrays, but BigArray
_slices_ are real ndarrays and can be used everywhere ndarray can be used,
including in C/Fortran code. Slice size is limited by mapping-size (=
address-space size) limit, i.e. to ~ max 127TB on Linux/amd64.

Changes to bigarray memory are changes to bigfile memory mapping and as such
can be discarded or saved back to bigfile using mapping (= BigFileH) dirty
discard/writeout interface. For ZBigFile that means changes can be
discarded & saved via transactions.

For the same reason the whole amount of changes to memory is limited by amount
of physical RAM.
"""

from __future__ import print_function
from wendelin.lib.calc import mul
from numpy import ndarray, dtype, sign, newaxis, asarray, argmax, uint8
from numpy.lib.stride_tricks import DummyArray
import logging


pagesize = 2*1024*1024 # FIXME hardcoded, TODO -> fileh.ram.pagesize

class BigArray(object):
    # numpy.ndarray like
    # XXX can't use slots, because that would create "multiple bases have
    #     instance lay-out conflict" with Persistent for ZBigArray
    """
    __slots__ = (
        '_dtype',       # items data type  (numpy.dtype)
        '_shape',       # []int

        # ._stridev       []int
        #                           j-1
        #                   strj = prod shapej  # XXX *dtype.itemsize (for C order)
        #
        # ._order           'C' or 'F'

        '_v_fileh',     # bigfile memory mapping for this array
    )
    """


    # TODO doc -> see ndarray
    # NOTE does not accept strides
    # NOTE please be cooperative to ZBigArray and
    #      - name helper data members starting with _v_.
    #      - do not rename data members which go to DB (not starting with _v_).
    #      - if new argument is added - it has to have default which is
    #        semantically equivalent to how the code was working before.
    def __init__(self, shape, dtype_, bigfileh, order='C'):
        self._init0(shape, dtype_, order)
        self._v_fileh = bigfileh


    # __init__ part without fileh
    def _init0(self, shape, dtype_, order):
        _dtype = dtype(dtype_)
        if _dtype.hasobject:
            logging.warn("You tried to use dtype containing object (%r) with out-of-core array ..." % _dtype)
            logging.warn("... wendelin.core does not support it, because in case of dtype=object elements are")
            logging.warn("... really pointers and data for each object is stored in separate place in RAM")
            logging.warn("... with different per-object size.")
            logging.warn("... ")
            logging.warn("... As out-of-core arrays are really memory-mapping of data in external storage")
            logging.warn("... this won't work. It also does not essentially work with numpy.memmap() for the")
            logging.warn("... same reason.")
            raise TypeError("dtypes with object are not supported", _dtype)

        self._dtype = _dtype
        self._shape = shape
        self._order = order
        # TODO +offset ?
        # TODO +strides ?

        # order -> stride_in_items(i)
        ordering = {
            'C': lambda i:  mul(shape[i+1:]),
            'F': lambda i:  mul(shape[:i]),
        }
        Si = ordering.get(order)
        if Si is None:
            raise NotImplementedError('Order %s not supported' % order)

        # shape, dtype -> ._stridev
        # TODO take dtype.alignment into account ?
        self._stridev = tuple( Si(i) * self._dtype.itemsize  \
                                    for i in range(len(shape)) )

    # major axis for this array
    @property
    def _major_axis(self):
        return argmax(self._stridev)    # NOTE assumes _stridev >= 0



    # ~~~ ndarray-like attributes

    @property
    def data(self):
        raise TypeError("Direct access to data for BigArray is forbidden")

    @property
    def strides(self):
        return self._stridev

    @property
    def dtype(self):
        # TODO support assigning new dtype
        return self._dtype

    @property
    def shape(self):
        # TODO support assigning new shape
        return self._shape

    @property
    def size(self):
        return mul(self._shape)

    def __len__(self):
        # lengths of the major axis
        return self._shape[self._major_axis]

    @property
    def itemsize(self):
        return self._dtype.itemsize

    @property
    def nbytes(self):
        return self.itemsize * self.size

    @property
    def ndim(self):
        return len(self._shape)


    def view(self, dtype=None, type=None):
        raise NotImplementedError   # TODO


    # TODO more ndarray-like attributes
    #   .T
    #   .flags  <--
    #   .flat
    #   .imag
    #   .real
    #   .base


    # ~~~ ndarray-like with different semantics

    # resize BigArray in-place
    #
    # NOTE
    #
    # - ndarray.resize()  works in O(n) time
    #
    #   ( on-growth numpy allocates new memory for whole array and copies data
    #     there. This is done because numpy.ndarray has to be contiguously stored
    #     in memory. )
    #
    # - BigArray.resize() works in O(1) time
    #
    #   ( BigArrays are only mapped to contiguous virtual address-space, and
    #     storage is organized using separate data blocks. )
    #
    # NOTE even after BigArray is resized, already-established ndarray views of
    #      BigArray stay of original size.
    def resize(self, new_shape, refcheck=True):
        # NOTE refcheck is in args only for numpy API compatibility - as we
        # don't move memory we don't need to check anything before resizing.

        # for BigArray resizing is just changing .shape - BigFile currently
        # works as if it is infinite storage with non-set blocks automatically
        # reading as whole-zeros. So
        #
        # - if array grows, on further mapping we'll map new blocks from
        #   ._fileh
        #
        # - if array shrinks, we'll not let clients to map blocks past array
        #   end.
        #
        #   TODO discard data from backing file on shrinks.
        self._init0(new_shape, self.dtype, order=self._order)


    # append BigArray in-place
    #
    # NOTE
    #
    # - numpy.append(array, δ)  creates new array and copies array and δ there
    #                           (works in O(array + δ) time)
    #
    # - BigArray.append(δ)      resizes array and copies δ to tail
    #                           (works in O(δ) time)
    #
    # values    - must be ndarray-like with compatible dtype of the same shape
    #             as extended array, except major axis, e.g.
    #
    #   BigArray    (N,10,5)
    #   values      (3,10,5)
    def append(self, values):
        values = asarray(values)

        # make sure appended values, after major axis, are of the same shape
        M = self._major_axis
        if self.shape[:M] != values.shape[:M]  or  self.shape[M+1:] != values.shape[M+1:]:
            # NOTE the same exception as in numpy.append()
            raise ValueError('all the input array dimensions except for the'
                    'concatenation axis must match exactly')

        # resize us, and prepare to rollback, in case of e.g. dtype
        # incompatibility catched on follow-up assignment
        n, delta = self.shape[M], values.shape[M]
        self.resize( self.shape[:M] + (n+delta,) + self.shape[M+1:] )

        # copy values to prepared tail place, and we are done
        try:
            # delta_idx = [-delta:] in M, : in all other axis
            delta_idx = [slice(None)] * len(self.shape)
            delta_idx[M] = slice(-delta, None)
            self[tuple(delta_idx)] = values
        except:
            # in case of error - rollback the resize and re-raise
            self.resize( self.shape[:M] + (n,) + self.shape[M+1:] )
            raise



    # ~~~ get/set item/slice connect bigfile blocks to ndarray in RAM.
    #     only basic indexing is supported - see numpy/.../arrays.indexing.rst
    #
    #     NOTE it would be good if we could reuse prepare_index() &
    #     npy_index_info from numpy/mapping.[ch]


    # access to mapping via property, so that children could hook into it
    # (e.g. ZBigArray creates mapping lazily on 1st access)
    @property
    def _fileh(self):
        return self._v_fileh

    def __getitem__(self, idx):
        # NOTE basic indexing means idx = tuple(slice | int) + sugar(newaxis, ellipsis)
        #print('\n__getitem__', idx)

        # BigArray does not support advanced indexes:
        # In numpy they create _copy_, picking up elements, e.g.
        #   a = arange(10)
        #   a[ 0,3,2 ]  -> IndexError
        #   a[[0,3,2]]  -> [0,3,2]
        if isinstance(idx, list):
            raise TypeError('BigArray does not support advanced indexing ; idx = %r' % (idx,))

        # handle 1d slices uniformly with Nd
        if not isinstance(idx, tuple):
            idx = (idx,)

        idx = list(idx)

        # expand ellipsis
        try:
            ellidx = idx.index(Ellipsis)
        except ValueError:
            # no ellipsis - nothing to do
            pass
        else:
            # ellipsis present - check there is only 1
            if idx[ellidx+1,:].count(Ellipsis):
                raise IndexError('multiple ellipsis not allowed')

            # and expand with `:,:,...` in place of ellipsis
            # (no need to check nexpand < 0 -- [...]*-1 = []
            nexpand = len(self.shape) - (len(idx) - idx.count(newaxis) - 1)
            idx[ellidx:ellidx] = [slice(None)] * nexpand

        #print('...\t->', idx)

        # expand idx with : to match shape
        # (no need to check for expanding e.g. -1 times -- [...]*-1 = []
        idx.extend( [slice(None)] * (len(self.shape) - len(idx) - idx.count(newaxis)) )

        #print('expand\t->', idx)


        # 1) for newaxis - remember we'll need to increase dimensionality
        #    there after we take view
        #
        # 2) for scalars - convert `i -> i:i+1` and remember we'll need to reduce
        #    dimensionality at that position
        dim_adjust = [slice(None)] * len(idx)   # :,:,:,...
        for i in range(len(idx)):
            if idx[i] is newaxis:
                dim_adjust[i] = newaxis             # [newaxis] will increase ndim

            elif not isinstance(idx[i], slice):
                _ = idx[i]
                if _ < 0:
                    _ = self.shape[i] + _   # -1 -> N-1  (or else -1:-1+1 -> -1:0 = empty)
                idx[i] = slice(_, _+1)
                dim_adjust[i] = 0                   # [0] will reduce ndim

        # if it stays list, and all elements are int, numpy activates advanced indexing
        dim_adjust = tuple(dim_adjust)
        #print('scalars\t->', idx)
        #print('dim_adj\t->', dim_adjust)


        # filter-out newaxis from index - so we first work with concrete positions
        try:
            # XXX not optimal
            while 1:
                idx.remove(newaxis)
        except ValueError:
            # no more newaxis - ok
            pass

        #print('ønewax\t->', idx)

        # ensure there are no more indices than we can serve
        if len(idx) > len(self.shape):
            raise IndexError('too many indices')

        # above we cared to expand to shape, if needed
        assert len(idx) == len(self.shape)


        # now we have:
        # - idx and shape are of the same size
        # - idx contains only slice objects
        # - dim_adjust was prepared for taking scalar and newaxis indices into
        #   account after we take ndarray view

        # major index / stride
        M       = self._major_axis
        idxM    = idx[M]
        strideM = self._stridev[M]
        shapeM  = self._shape[M]

        # utility: replace M'th element in a sequence tuple/list -> tuple
        Mreplace = lambda t, value: tuple(t[:M]) + (value,) + tuple(t[M+1:])

        # major idx start/stop/stride
        try:
            idxM_start, idxM_stop, idxM_stride = idxM.indices(shapeM)
        except OverflowError as e:
            # overflow error here means slice indices do not fit into std long,
            # which also practically means we cannot allocate such amount of
            # address space.
            raise MemoryError(e)

        #print('idxM:\t', idxM, '-> [%s:%s:%s]' % (idxM_start, idxM_stop, idxM_stride))
        #print('stridM:\t', strideM)  #, self._stridev
        #print('shapeM:\t', shapeM)   #, self._shape


        # nitems in major row
        nitemsM = (idxM_stop - idxM_start - sign(idxM_stride)) // idxM_stride + 1
        #print('nitemM:\t', nitemsM)

        # if major row is "empty" slice, we can build view right away without creating vma.
        # e.g. 10:5:1, 5:10:-1, 5:5,  size+100:size+200  ->  []
        if nitemsM <= 0:
            view = ndarray(Mreplace(self._shape, 0), self._dtype)

        # create appropriate vma and ndarray view to it
        else:

            # major slice -> in bytes
            byteM_start  = idxM_start  * strideM
            byteM_stop   = idxM_stop   * strideM
            byteM_stride = idxM_stride * strideM
            #print('byteM:\t[%s:%s:%s]' % (byteM_start, byteM_stop, byteM_stride))

            # major slice -> in file pages, always increasing, inclusive
            if byteM_stride >= 0:
                pageM_min = byteM_start     // pagesize                 # TODO -> fileh.pagesize
                pageM_max = (byteM_stop-1)  // pagesize                 # TODO -> fileh.pagesize
            else:
                pageM_min = (byteM_stop  - byteM_stride)     // pagesize# TODO -> fileh.pagesize
                pageM_max = (byteM_start - byteM_stride - 1) // pagesize# TODO -> fileh.pagesize
            #print('pageM:\t[%s, %s]' % (pageM_min, pageM_max))


            # ~~~ mmap file part corresponding to full major slice into memory
            vmaM = self._fileh.mmap(pageM_min, pageM_max-pageM_min+1)

            # remember to which BigArray this vma belongs.
            # this is needed for ArrayRef to be able to find root array object.
            vmaM.pyuser = self


            # first get ndarray view with only major slice specified and rest indices being ":"
            viewM_shape   = Mreplace(self._shape, nitemsM)
            viewM_offset  = byteM_start - pageM_min * pagesize # TODO -> fileh.pagesize
            viewM_stridev = Mreplace(self._stridev, byteM_stride)
            #print('viewM_shape:\t', viewM_shape, self.shape)
            #print('viewM_stridv:\t', viewM_stridev)
            #print('viewM_offset:\t', viewM_offset)
            #print('len(vmaM):\t', len(vmaM))
            viewM = ndarray(viewM_shape, self._dtype, vmaM, viewM_offset, viewM_stridev)

            # now take into account indices after major one
            view  = viewM[Mreplace(idx, slice(None))]

            #print('viewM:\t', viewM.shape)
            #print('view:\t',  view.shape)

        #print('View:\t',  view)
        #print('view/d:\t', view[dim_adjust])
        # and finally take dimensions adjust into account and we are done
        return view[dim_adjust]


    def __setitem__(self, idx, v):
        # TODO idx = int, i.e. scalar assign

        # represent changed area by ndarray via getitem, then leverage ndarray assignment
        a = self.__getitem__(idx)
        a[:] = v



    # BigArray -> ndarray  (if enough address space available)
    #
    # BigArrays can be big - up to 2^64 bytes, and thus in general it is not
    # possible to represent whole BigArray as ndarray view, because address
    # space is usually smaller on 64bit architectures.
    #
    # However users often try to pass BigArrays to numpy functions as-is, and
    # numpy finds a way to convert, or start converting, BigArray to ndarray -
    # via detecting it as a sequence, and extracting elements one-by-one.
    # Which is slooooow.
    #
    # Because of the above, we provide users a well-defined service:
    # - if virtual address space is available - we succeed at creating ndarray
    #   view for whole BigArray, without delay and copying.
    # - if not - we report properly the error and give hint how BigArrays have
    #   to be processed in chunks.
    def __array__(self):
        # NOTE numpy also sometimes uses optional arguments |dtype,context
        #      but specifying dtype means the result should be a copy.
        #
        #      Copying BigArray data is not a good idea in all cases,
        #      so we don't support accepting dtype.
        try:
            return self[:]
        except MemoryError:
            logging.warn('You tried to map BigArray (~ %.1f GB) and it failed ...' %
                    (float(self.nbytes) // (1<<30)))
            logging.warn('... because there is no so much memory or so much virtual address')
            logging.warn('... space available. BigArrays larger than available virtual')
            logging.warn('... address space can not be mapped at once and have to be')
            logging.warn('... processed in chunks.')
            raise


# ----------------------------------------

# InvalidArrayRef is the exception raised when ArrayRef was tried to be
# dereferenced but found invalid.
class InvalidArrayRef(Exception):
    pass

# _flatbytev returns []byte view of array a with index ↑ along with memory
def _flatbytev(a):
    b = a[:].view(uint8)
    # XXX vvv strictly speaking this might raise internally if numpy sees
    # it cannot reshape without copy.
    b.shape = -1  # flatten without copy
    assert len(b.strides) == 1
    if b.strides[0] < 0:
        b = b[::-1]
    assert b.strides[0] == +1
    return b

# ArrayRef is a reference to NumPy array.
#
# The reference is represented by root array object and instructions how to
# create original array as some view of the root.
#
# Such reference could be useful in situations where one needs to pass arrays
# between processes and instead of copying array data, leverage the fact that
# top-level array, for example ZBigArray, is already persisted separately, and
# only send small amount of information referencing data in question.
#
# Use ArrayRef(array) to create reference to an ndarray.
#
# Use .deref() to convert ArrayRef to pointed array object.
#
# NOTE
#
# don't send ArrayRef unconditionally - for example when array object is
# small regular ndarray with also regular, but big, root ndarray, sending
# ArrayRef will send whole data for root object, not for small leaf.
#
# Sending ArrayRef only makes sense when root object is known to be already
# persisted by other means, for example something like below in ZODB context:
#
#   aref = ArrayRef(a)
#   if isinstance(aref.root, Persistent):
#       send aref
#   else:
#       send a
class ArrayRef(object):
    # .root         top-level array object
    #
    #   below broot is []byte view of .root array with index ↑ along with memory
    #
    # .lo, .hi      raw array data is broot[lo:hi]
    # .z0           array[0,0,...,0] is pointing ->z0 in broot[lo:hi]
    #
    # .shape        shape and strides of the array with data taken from
    # .stridev      broot[lo:hi] with .z0 as zero offset.
    #
    # .dtype        dtype of the array
    # .atype        type of the array  (e.g. np.ndarray, np.recarray, etc...)


    # deref returns ndarray represented by this reference.
    #
    # if the reference was found invalid - e.g. it had nonsensical data or
    # shape/stride out of range - InvalidArrayRef is raised.
    def deref(self):
        # broot is []byte view of root array with index ↑ along with memory
        broot = _flatbytev(self.root)

        # check lo:hi is in within correct range.
        if not (0 <= self.lo <= self.hi <= len(broot)):
            raise InvalidArrayRef("lo:hi ([%d, %d]) out of correct [0, %d] range" %
                    (self.lo, self.hi, len(broot)))

        # bchild is raw []byte underling data for recreated array
        bchild  = broot[self.lo:self.hi]
        if not (0 <= self.z0 < len(bchild)):
            raise InvalidArrayRef("z0 (%d) out of correct [0, %d) range" %
                    (self.z0, len(bchild)))

        # check .shape and .stridev are within correct range, before
        # applying unsafe stride tricks.
        if len(self.shape) != len(self.stridev):
            raise InvalidArrayRef("shape/stridev len mismatch  (#shape: %d;  #stridev: %d)" %
                    (len(self.shape), len(self.stridev)))

        # [boffmin:boffmax) is recreated array's byte range rooted at its zero point
        boffmin = 0
        boffmax = 0
        for n, s in zip(self.shape, self.stridev):
            if n < 0 or int(n) != n:
                raise InvalidArrayRef("shape (%s) has invalid element: %s" % (self.shape, n))
            if int(s) != s:
                raise InvalidArrayRef("stridev %s has invalid element: %s" % (self.stridev, s))
            if n == 0:
                continue    # [0] dimension - does not affect anything

            if s > 0:
                boffmax += s*(n-1)
            else:
                boffmin += s*(n-1)

        # when element is read - memory is accessed ↑ for itemsize bytes.
        # we need to adjust only boffmax because for boffmin it grows ↑ too.
        boffmax += self.dtype.itemsize

        # [xlo,xhi) is the bchild's range accessed with .shape and .stridev
        xlo = self.z0 + boffmin
        xhi = self.z0 + boffmax

        if not (0 <= xlo <= xhi <= len(bchild)):
            raise InvalidArrayRef(
                    "shape/stride invalid: cover [%d, %d) while raw data range is [%d, %d)" %
                    (self.lo + xlo, self.lo + xhi, self.lo, self.hi))


        # bchild_z0 points to the same underlying data buffer as bchild, but
        # zero offset corresponds to zero offset of original array.
        #
        # len(bchild_z0) == 1, so that we can be sure bchild_z0 -> bchild
        # memory pinning always works, not only for positive offsets(*).
        #
        # (*) we cannot make len(bchild_z0) == 0 since then, when creating
        #     empty slice, numpy won't adjust the array pointer at all.
        #
        #     it could be also possible to adjust 'data' in __array_interface__
        #     for += z0, but going explicitly via slicing is more safe.
        bchild_z0 = bchild[self.z0: self.z0 + 1]

        # restore original array shape/strides/dtype via unsafe trick.
        #
        # it should be safe to cover memory corresponding to both negative and
        # positive offsets to bchild_z0, because bchild_z0 holds reference to
        # bchild and bchild covers whole raw data range.
        #
        # it is also safe because we checked .shape and .stridev not to escape
        # from bchild data buffer.
        #
        # the code below is very close to
        #
        #   a = stride_tricks.as_strided(bchild_z0, shape=self.shape, strides=self.stridev)
        #
        # but we don't use as_strided() because we also have to change dtype
        # with shape and strides in one go - else changing dtype after either
        # via a.dtype = ..., or via a.view(dtype=...) can raise errors like
        #
        #   "When changing to a larger dtype, its size must be a
        #    divisor of the total size in bytes of the last axis
        #    of the array."
        aiface = dict(bchild_z0.__array_interface__)
        aiface['shape']   = tuple(self.shape)
        aiface['strides'] = tuple(self.stridev)
        # type: for now we only care that itemsize is the same
        aiface['typestr'] = '|V%d' % self.dtype.itemsize
        aiface['descr']   = [('', aiface['typestr'])]

        a = asarray(DummyArray(aiface, base=bchild_z0))

        # restore full dtype - it should not raise here, since itemsize is the same
        a.dtype = self.dtype

        # restore full array type
        a = a.view(type=self.atype)

        # we are done
        return a


    # ArrayRef(a) creates reference to ndarray a.
    def __init__(aref, a):
        # find root
        root = a            # top-level ndarray
        bigvma = None       # VMA, that is root.base, if there is one
        while 1:
            base = root.base

            # top-level ndarray
            if base is None:
                break

            # it was an ndarray (sub)view.
            if isinstance(base, ndarray):
                root = base
                continue

            # it might be also ndarray proxy - e.g. np.lib.stride_tricks.DummyArray
            # with holding valid .base but not being ndarray.
            basebase = getattr(base, 'base', None)
            if isinstance(basebase, ndarray):
                root = basebase
                continue

            # base is neither ndarray (sub)class nor ndarray proxy.
            #
            # either it is
            #
            # 1) top-level ndarray with base taken from an object
            # with buffer interface, e.g. as here:
            #
            #   In [1]: s = '123'
            #   In [2]: x = ndarray(shape=(1,), buffer=s, dtype='|S3')
            #   In [3]: x
            #   Out[3]: array(['123'], dtype='|S3')
            #   In [4]: x.base
            #   Out[4]: '123'
            #
            # and so it should be treated as top-level ndarray,
            #
            # 2) or it is a VMA created from under BigArray which will be
            # treated as top-level too, and corrected for in the end.
            basetype = type(base)
            if basetype.__module__ + "." + basetype.__name__ == "_bigfile.VMA":
            #if isinstance(base, _bigfile.VMA):  XXX _bigfile does not expose VMA
                bigvma = base
            break


        # broot is []byte view of root with idx ↑ along memory
        broot = _flatbytev(root)

        # [boffmin:boffmax) is a's byte range rooted at its zero point
        boffmin = 0
        boffmax = 0
        assert len(a.shape) == len(a.strides)
        for n, s in zip(a.shape, a.strides):
            assert n >= 0
            if n == 0:
                continue    # [0] dimension - does not affect anything
            assert n >= 1

            if s > 0:
                boffmax += s*(n-1)
            else:
                boffmin += s*(n-1)

        # when element is read - memory is accessed ↑ for itemsize bytes.
        # we need to adjust only boffmax because for boffmin it grows ↑ too.
        boffmax += a.itemsize


        # compute bytes δz in between broot's and a's zero points.
        # δz should  be >= 0, since broot ↑ along memory.
        adata = a.__array_interface__.get('data')
        rdata = broot.__array_interface__.get('data')
        assert adata is not None, "TODO __array_interface__.data = None"
        assert rdata is not None, "TODO __array_interface__.data = None"
        assert isinstance(adata, tuple), "TODO __array_interface__.data is %r" % (adata,)
        assert isinstance(rdata, tuple), "TODO __array_interface__.data is %r" % (rdata,)
        # {a,r}data is (data, readonly)
        zdelta = adata[0] - rdata[0]
        assert zdelta >= 0

        # broot[lo:hi] is raw []byte underlying data for a, with z0 pointing
        # to a's zero point.
        lo = zdelta + boffmin
        hi = zdelta + boffmax
        assert 0 <= lo <= hi <= len(broot)
        z0 = zdelta - lo

        # reference is ready for ndarray root
        aref.root    = root
        aref.lo      = lo
        aref.hi      = hi
        aref.z0      = z0
        aref.shape   = a.shape
        aref.stridev = a.strides
        aref.dtype   = a.dtype
        aref.atype   = type(a)

        # correct it, if the root is actually BigArray
        if bigvma is not None:
            assert bigvma.addr_start <= rdata[0]
            assert rdata[0] + len(broot) <= bigvma.addr_stop

            bigroot = bigvma.pyuser
            assert isinstance(bigroot, BigArray)

            # bigoff is broot position in bbigroot (both raw flat []byte ↑ along memory)
            pgoff, _ = bigvma.filerange()
            bigoff = pgoff * bigvma.pagesize()          # vma start offset
            bigoff += rdata[0] - bigvma.addr_start      # broot offset from vma start

            aref.root  = bigroot
            aref.lo   += bigoff
            aref.hi   += bigoff

        # we are done
        return
