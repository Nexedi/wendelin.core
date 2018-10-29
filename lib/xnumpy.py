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
