# -*- coding: utf-8 -*-
# NumPy-related utilities
# Copyright (C) 2018  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
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
import numpy as np
from numpy.lib import stride_tricks as npst

# _as_strided is similar to numpy.lib.stride_tricks.as_strided, but allows to
# set all shape/stridev/dtype in one go.
#
# It must be used with extreme care, because if there is math error in the
# arguments, the resulting array can cover wrong memory. Bugs here thus can
# lead to mysterious crashes.
def _as_strided(arr, shape, stridev, dtype):
    # the code below is very close to
    #
    #   a = stride_tricks.as_strided(arr, shape=shape, strides=stridev)
    #
    # but we don't use as_strided() because we also have to change dtype
    # with shape and strides in one go - else changing dtype after either
    # via a.dtype = ..., or via a.view(dtype=...) can raise errors like
    #
    #   "When changing to a larger dtype, its size must be a
    #    divisor of the total size in bytes of the last axis
    #    of the array."
    aiface = dict(arr.__array_interface__)
    aiface['shape']   = shape
    aiface['strides'] = stridev
    # type: for now we only care that itemsize is the same
    aiface['typestr'] = '|V%d' % dtype.itemsize
    aiface['descr']   = [('', aiface['typestr'])]

    a = np.asarray(npst.DummyArray(aiface, base=arr))

    # restore full dtype - it should not raise here, since itemsize is the same
    a.dtype = dtype

    # restore full array type (mimics subok=True)
    if type(a) is not type(arr):
        a = a.view(type=type(arr))

    # we are done
    return a


# structured creates view of the array interpreting its minor axis as fully covered by dtype.
#
# The minor axis of the array must be C-contiguous and be of dtype.itemsize in size.
#
# Structured is similar to arr.view(dtype) + corresponding reshape, but does
# not have limitations of ndarray.view(). For example:
#
#   In [1]: a = np.arange(3*3, dtype=np.int32).reshape((3,3))
#
#   In [2]: a
#   Out[2]:
#   array([[0, 1, 2],
#          [3, 4, 5],
#          [6, 7, 8]], dtype=int32)
#
#   In [3]: b = a[:2,:2]
#
#   In [4]: b
#   Out[4]:
#   array([[0, 1],
#          [3, 4]], dtype=int32)
#
#   In [5]: dtxy = np.dtype([('x', np.int32), ('y', np.int32)])
#
#   In [6]: dtxy
#   Out[6]: dtype([('x', '<i4'), ('y', '<i4')])
#
#   In [7]: b.view(dtxy)
#   ---------------------------------------------------------------------------
#   ValueError                                Traceback (most recent call last)
#   <ipython-input-66-af98529aa150> in <module>()
#   ----> 1 b.view(dtxy)
#
#   ValueError: To change to a dtype of a different size, the array must be C-contiguous
#
#   In [8]: structured(b, dtxy)
#   Out[8]: array([(0, 1), (3, 4)], dtype=[('x', '<i4'), ('y', '<i4')])
#
# Structured always creates view and never copies data.
def structured(arr, dtype):
    dtype = np.dtype(dtype) # convenience

    atype = arr.dtype
    # m* denotes minor *
    maxis   = np.argmin(np.abs(arr.strides))
    mstride = arr.strides[maxis]
    if mstride < 0:
        raise ValueError("minor-axis is not C-contiguous: stride (%d) < 0" % mstride)
    if mstride != atype.itemsize:
        raise ValueError("minor-axis is not C-contiguous: stride (%d) != itemsize (%d)" % (mstride, atype.itemsize))

    # verify dtype fully covers whole minor axis
    mnelem = arr.shape[maxis]
    msize  = mnelem * atype.itemsize
    if dtype.itemsize != msize:
        raise ValueError("dtype.itemsize (%d) != sizeof(minor-axis) (%d)" % (dtype.itemsize, msize))

    # ok to go
    shape   = arr.shape[:maxis] + arr.shape[maxis+1:]
    stridev = arr.strides[:maxis] + arr.strides[maxis+1:]

    # NOTE we cannot use just np.ndarray because if arr is a slice it can give:
    #   TypeError: expected a single-segment buffer object
    #return np.ndarray.__new__(type(arr), shape, dtype, buffer(arr), 0, stridev)
    return _as_strided(arr, shape, stridev, dtype)
