# Copyright (C) 2024-2025  Nexedi SA and Contributors.
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

"""Package wcfs.internal._os provides C-level part of package wcfs.internal.os ."""

from posix.types cimport pid_t
cdef extern from "<unistd.h>":
    pid_t c_gettid "gettid"()

# gettid returns ID of current thread.
#
# It is similar to _thread.get_native_id() which is available only on py3.
def gettid():
    return c_gettid()
