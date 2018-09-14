# -*- coding: utf-8 -*-
# Wendelin.bigarray | RAM BigArray
# Copyright (C) 2014-2018  Nexedi SA and Contributors.
#                          klaus Wölfel <klaus@nexedi.com>
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

"""BigArray that lives in RAM

RamArray simulates ZBigArray Api, but data lives in RAM.
Append is optimized compared to numpy append by preallocating
internal storage by 2^x+1 for every append.

Other than BigArray.resize(newshape), RamArray.resize() copies data internally
so a previously created view will not see changes made to the data after
the resize.
"""

from math import ceil, log 
from numpy import dtype, resize, zeros
from wendelin.bigarray import BigArray
from wendelin.lib.calc import mul

def calc_x(nbytes):
    # get x for 2^x >= nbytes
    if nbytes == 0: # lg 0 not possible
        return 0
    return int(ceil(log(nbytes, 2)))


class RamArray(BigArray):
    """
    Simulate ZBigArray Api for us in Temporaray context
    """

    def __init__(self, shape, dtype, order='C', blksize=None):
        self._init0(shape, dtype, order)
        # initialize with zeros as ZBigarray does
        self.storage = zeros((2**calc_x(self.nbytes),), 'b')

    def resize(self, new_shape, refcheck=True):
        # first set new shape
        # if too big then resize storage with at least 2^x+1
        # resize is done by copying, because numpy in-place resize
        # may change the memory layout, so previously created views
        # would not be correct anymore.
        self._shape = new_shape
        if self.nbytes > self.storage.size:
            x = max(calc_x(self.nbytes), calc_x(self.storage.size) + 1)
            self.storage = resize(self.storage, (2**x,))

    def _fileh(self):
        raise NotImplementedError('_fileh not used') 

    def __getitem__(self, idx):
        if isinstance(idx, list):
            raise TypeError('BigArray does not support advanced indexing ; idx = %r' % (idx,))
        a = self.storage[:self.nbytes].view(self._dtype)\
                                      .reshape(self._shape, order=self._order)
        return a.__getitem__(idx)

