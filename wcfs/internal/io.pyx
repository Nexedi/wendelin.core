# Copyright (C) 2019-2025  Nexedi SA and Contributors.
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
# cython: legacy_implicit_noexcept=True

"""Package io complements IO facility provided by Python."""

from posix.unistd cimport pread
from cpython.exc cimport PyErr_SetFromErrno

# readat calls pread to read from fd@off into buf.
def readat(int fd, size_t off, unsigned char[::1] buf not None):    # -> n
    cdef void   *dest = &buf[0]
    cdef size_t  size = buf.shape[0]
    cdef ssize_t n

    with nogil:
        n = pread(fd, dest, size, off)

    if n < 0:
        PyErr_SetFromErrno(OSError)

    return n
