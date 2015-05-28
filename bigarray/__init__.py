# -*- coding: utf-8 -*-
# BigArray submodule for Wendelin
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
from numpy import ndarray, dtype, multiply, sign, newaxis


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
        #                   strj = prod shapej  # XXX *dtype.itemsize
        # XXX on start translates to strides
        # .order            'C' or 'F'  XXX other orders?

        '_v_fileh',     # bigfile memory mapping for this array
    )
    """


    # TODO doc -> see ndarray
    # NOTE does not accept strides
    # TODO handle order
    # NOTE please be cooperative to ZBigArray and name helper data members starting with _v_
    def __init__(self, shape, dtype_, bigfileh, order='C'):
        self._init0(shape, dtype_, order)
        self._v_fileh = bigfileh


    # __init__ part without fileh
    def _init0(self, shape, dtype_, order):
        self._dtype = dtype(dtype_)
        self._shape = shape
        # TODO +offset ?
        # TODO +strides ?

        if order != 'C':
            raise NotImplementedError('Order %s not supported' % order)


        # shape, dtype -> ._stridev
        # TODO take dtype.alignment into account ?
        # NOTE (1,) so that multiply.reduce return 1 (not 1.0) for []
        self._stridev = tuple( multiply.reduce((1,) + shape[i+1:]) * self._dtype.itemsize  \
                                    for i in range(len(shape)) )



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
        return multiply.reduce(self._shape)

    def __len__(self):
        # lengths of the first axis
        return self._shape[0]    # FIXME valid only for C-order

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
        self._init0(new_shape, self.dtype, order='C')   # FIXME order hardcoded



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
        # FIXME assumes C ordering
        idx0    = idx[0]
        stride0 = self._stridev[0]
        shape0  = self._shape[0]

        # major idx start/stop/stride
        idx0_start, idx0_stop, idx0_stride = idx0.indices(shape0)

        #print('idx0:\t', idx0, '-> [%s:%s:%s]' % (idx0_start, idx0_stop, idx0_stride))
        #print('strid0:\t', stride0)  #, self._stridev
        #print('shape0:\t', shape0)   #, self._shape


        # nitems in major row
        nitems0 = (idx0_stop - idx0_start - sign(idx0_stride)) // idx0_stride + 1
        #print('nitem0:\t', nitems0)

        # if major row is "empty" slice, we can return right away without creating vma.
        # e.g. 10:5:1, 5:10:-1, 5:5,  size+100:size+200  ->  []
        if nitems0 <= 0:
            return ndarray((0,) + self._shape[1:], self._dtype)

        # major slice -> in bytes
        byte0_start  = idx0_start  * stride0
        byte0_stop   = idx0_stop   * stride0
        byte0_stride = idx0_stride * stride0

        # major slice -> in file pages, always increasing, inclusive
        page0_min  = min(byte0_start, byte0_stop+byte0_stride) // pagesize # TODO -> fileh.pagesize
        page0_max  = max(byte0_stop-byte0_stride, byte0_start) // pagesize # TODO -> fileh.pagesize


        # ~~~ mmap file part corresponding to full major slice into memory
        vma0 = self._fileh.mmap(page0_min, page0_max-page0_min+1)


        # first get ndarray view with only major slice specified and rest indices being ":"
        view0_shape   = (nitems0,) + self._shape[1:]
        view0_offset  = byte0_start - page0_min * pagesize # TODO -> fileh.pagesize
        view0_stridev = (byte0_stride,) + self._stridev[1:]
        #print('view0_shape:\t', view0_shape, self.shape)
        #print('view0_offset:\t', view0_offset)
        #print('len(vma0):\t', len(vma0))
        view0 = ndarray(view0_shape, self._dtype, vma0, view0_offset, view0_stridev)

        # now take into account indices after major one
        view  = view0[(slice(None),) + tuple(idx[1:])]

        #print('view0:\t', view0.shape)
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


    # XXX __array__(self) = self[:]     ?
    # (for numpy functions to accept bigarray as-is (if size permits))
