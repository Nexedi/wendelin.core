# -*- coding: utf-8 -*-
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

from numpy import ndarray, arange, dtype, int32
from numpy.lib.stride_tricks import DummyArray
from wendelin.lib.xnumpy import structured
from pytest import raises

# xbase returns original object from which arr was _as_strided viewed.
def xbase(arr):
    b = arr.base    # arr -> typed view | DummyArray
    if type(b) is not DummyArray:
        b = b.base  # it was typed view -> DummyArray
    assert type(b) is DummyArray
    b = b.base      # -> origin
    return b

# verifies xnumpy.structured()
def test_structured():
    dtxy = dtype([('x', int32), ('y', int32)])

    # C order
    a = arange(4*3, dtype=int32).reshape((4,3))
    # 0  1  2
    # 3  4  5
    # 6  7  8
    # 9 10 11

    with raises(ValueError, match="minor-axis is not C-contiguous: stride \(-4\) < 0"):
        structured(a[:3,:2][:,::-1], dtxy)
    with raises(ValueError, match="minor-axis is not C-contiguous: stride \(8\) != itemsize \(4\)"):
        structured(a[:,::2], dtxy)
    with raises(ValueError, match="dtype.itemsize \(8\) != sizeof\(minor-axis\) \(12\)"):
        structured(a, dtxy)

    b = a[:3,:2]
    bxy = structured(b, dtxy)
    assert xbase(bxy) is b
    assert bxy.dtype == dtxy
    assert bxy.shape == (3,)
    assert bxy[0]['x'] == 0
    assert bxy[0]['y'] == 1
    assert bxy[1]['x'] == 3
    assert bxy[1]['y'] == 4
    assert bxy[2]['x'] == 6
    assert bxy[2]['y'] == 7
    bxy['x'][0] = 100
    assert bxy[0]['x'] == 100
    assert b[0,0] == 100
    assert a[0,0] == 100
    bxy['y'][2] = 200
    assert bxy[2]['y'] == 200
    assert b[2,1] == 200
    assert a[2,1] == 200

    # C contigous in minor; reverse in major
    a = arange(4*3, dtype=int32).reshape((4,3))
    # 0  1  2
    # 3  4  5
    # 6  7  8
    # 9 10 11
    b = a[:3,:2][::-1,:]
    bxy = structured(b, dtxy)
    assert xbase(bxy) is b
    assert bxy.dtype == dtxy
    assert bxy.shape == (3,)
    assert bxy[0]['x'] == 6
    assert bxy[0]['y'] == 7
    assert bxy[1]['x'] == 3
    assert bxy[1]['y'] == 4
    assert bxy[2]['x'] == 0
    assert bxy[2]['y'] == 1
    bxy['x'][0] = 100
    assert bxy[0]['x'] == 100
    assert b[0,0] == 100
    assert a[2,0] == 100
    bxy['y'][2] = 200
    assert bxy[2]['y'] == 200
    assert b[2,1] == 200
    assert a[0,1] == 200


    # F order
    a = arange(4*3, dtype=int32).reshape((4,3), order='F')
    # 0  4  8
    # 1  5  9
    # 2  6 10
    # 3  7 11
    b = a[:2,:3]
    bxy = structured(b, dtxy)
    assert xbase(bxy) is b
    assert bxy.dtype == dtxy
    assert bxy.shape == (3,)
    assert bxy[0]['x'] == 0
    assert bxy[0]['y'] == 1
    assert bxy[1]['x'] == 4
    assert bxy[1]['y'] == 5
    assert bxy[2]['x'] == 8
    assert bxy[2]['y'] == 9
    bxy['x'][0] = 100
    assert bxy[0]['x'] == 100
    assert b[0,0] == 100
    assert a[0,0] == 100
    bxy['y'][2] = 200
    assert bxy[2]['y'] == 200
    assert b[1,2] == 200
    assert a[1,2] == 200


    # custom class
    class MyArray(ndarray):
        pass

    a = arange(4*3, dtype=int32).reshape((4,3))
    # 0  1  2
    # 3  4  5
    # 6  7  8
    # 9 10 11

    a = a.view(type=MyArray)
    b = a[:3,:2]
    bxy = structured(b, dtxy)
    assert xbase(bxy) is b
    assert bxy.dtype == dtxy
    assert bxy.shape == (3,)
    assert type(bxy) is MyArray
