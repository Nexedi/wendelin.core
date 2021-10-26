# Copyright (C) 2019-2021  Nexedi SA and Contributors.
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

# cython: language_level=2

"""Package mm provides access to OS memory management interfaces."""

from posix cimport mman
from cpython.exc cimport PyErr_SetFromErrno

cdef extern from "<sys/user.h>":
    cpdef enum:
        PAGE_SIZE

from posix.types cimport off_t

# map_ro memory-maps fd[offset +size) as read-only.
# The mapping is created with MAP_SHARED.
def map_ro(int fd, off_t offset, size_t size):
    cdef void *addr

    addr = mman.mmap(NULL, size, mman.PROT_READ, mman.MAP_SHARED, fd, offset)
    if addr == mman.MAP_FAILED:
        PyErr_SetFromErrno(OSError)

    return <unsigned char[:size:1]>addr

# unmap unmaps memory covered by mem.
def unmap(const unsigned char[::1] mem not None):
    cdef const void *addr = &mem[0]
    cdef size_t      size = mem.shape[0]

    cdef err = mman.munmap(<void *>addr, size)
    if err:
        PyErr_SetFromErrno(OSError)

    return
