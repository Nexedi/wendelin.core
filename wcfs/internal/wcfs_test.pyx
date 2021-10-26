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
# distutils: language=c++

"""Module wcfs_test.pyx complements wcfs_test.py with things that cannot be
implemented in Python."""

from posix.signal cimport sigaction, sigaction_t, siginfo_t, SA_SIGINFO
from libc.signal cimport SIGBUS
from libc.stdlib cimport abort
from libc.string cimport strlen
from posix.unistd cimport write, sleep

from cpython.exc cimport PyErr_SetFromErrno

from golang cimport panic

# ---- signal handling ----
# TODO -> golang.signal ?

# install_sigbus_trap installs SIGBUS handler that prints python-level
# traceback before aborting.
#
# Such handler is useful, because when wcfs.go bugs/panics while handling file
# access from wcfs.py, wcfs.py receives SIGBUS signal and by default aborts.
def install_sigbus_trap():
    cdef sigaction_t act
    act.sa_sigaction = on_sigbus
    act.sa_flags     = SA_SIGINFO

    cdef int err = sigaction(SIGBUS, &act, NULL)
    if err:
        PyErr_SetFromErrno(OSError)

cdef void on_sigbus(int sig, siginfo_t *si, void *_uc) nogil:
    # - wait a bit to give time for other threads to complete their exception dumps
    #   (e.g. getting "Transport endpoint is not connected" after wcfs.go dying)
    # - dump py-level traceback and abort.
    # TODO turn SIGBUS into python-level exception? (see sigpanic in Go how to do).

    writeerr("\nC: SIGBUS received; giving time to other threads "
             "to dump their exceptions (if any) ...\n")
    with gil:
        pass
    sleep(1)

    writeerr("\nC: SIGBUS'ed thread traceback:\n")
    with gil:
        import traceback
        traceback.print_stack()
    writeerr("-> SIGBUS\n");
    # FIXME nothing is printed if pytest stdout/stderr capture is on (no -s given)
    abort()

# writeerr writes msg to stderr without depending on stdio buffering and locking.
cdef void writeerr(const char *msg) nogil:
    xwrite(2, msg)

# xwrite writes msg to fd without depending on stdio buffering and locking.
cdef void xwrite(int fd, const char *msg) nogil:
    cdef ssize_t n, left = strlen(msg)
    while left > 0:
        n = write(fd, msg, left)
        if n == -1:
            panic("write: failed")
        left -= n
        msg  += n
