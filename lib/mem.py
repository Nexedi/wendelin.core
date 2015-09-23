# -*- coding: utf-8 -*-
# Wendelin. Memory helpers
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
from numpy import ndarray, uint8, copyto


# zero buffer memory
def bzero(buf):
    a = ndarray(len(buf), buffer=buf, dtype=uint8)
    a[:] = 0


# set bytes in memory
def memset(buf, c):
    assert 0 <= c <= 0xff
    a = ndarray(len(buf), buffer=buf, dtype=uint8)
    a[:] = c


# copy src buffer memory to dst
# precondition: len(dst) == len(src)
def memcpy(dst, src):
    l = len(src)
    assert len(dst) >= l
    adst = ndarray(l, buffer=dst, dtype=uint8)
    asrc = ndarray(l, buffer=src, dtype=uint8)
    copyto(adst, asrc)
