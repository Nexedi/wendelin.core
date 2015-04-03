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
from numpy import ndarray, uint64, copyto


# zero buffer memory
def bzero(buf):
    assert len(buf)%8 == 0  # XXX for simplicity
    a = ndarray(len(buf)//8, buffer=buf, dtype=uint64)
    a[:] = 0


# set bytes in memory
def memset(buf, c):
    assert 0 <= c <= 0xff
    assert len(buf)%8 == 0  # XXX for simplicity
    a = ndarray(len(buf)//8, buffer=buf, dtype=uint64)
    a[:] = (c * 0x0101010101010101)


# copy src buffer memory to dst
# precondition: len(dst) == len(src)
def memcpy(dst, src):
    l = len(src)
    assert len(dst) == l
    assert l % 8 == 0    # XXX for simplicity
    adst = ndarray(l//8, buffer=dst, dtype=uint64)
    asrc = ndarray(l//8, buffer=src, dtype=uint64)
    copyto(adst, asrc)
