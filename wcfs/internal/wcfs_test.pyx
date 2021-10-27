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

from posix.signal cimport sigaction, sigaction_t, siginfo_t, SA_SIGINFO, sigemptyset
from libc.signal cimport SIGBUS, SIGSEGV
from libc.setjmp cimport sigjmp_buf, sigsetjmp, siglongjmp
from libc.stdlib cimport abort
from libc.string cimport strlen
from posix.unistd cimport write, sleep
from posix.types cimport off_t

from cpython.exc cimport PyErr_SetFromErrno

from golang cimport chan, pychan, select, panic, topyexc, cbool
from golang cimport sync, time

# _tWCFS is pyx part of tWCFS.
cdef class _tWCFS:
    cdef readonly pychan _closed            # chan[structZ]
    cdef readonly pychan _wcfuseaborted     # chan[structZ]

    def __cinit__(_tWCFS t):
        t._closed        = pychan(dtype='C.structZ')
        t._wcfuseaborted = pychan(dtype='C.structZ')

    # _abort_ontimeout sends abort to fuse control file if timeout happens
    # before tDB is closed.
    #
    # It runs without GIL to avoid deadlock: e.g. if a code that is
    # holding GIL will access wcfs-mmapped memory, and wcfs will send pin,
    # but pin handler is failing one way or another - select will wake-up
    # but, if _abort_ontimeout uses GIL, won't continue to run trying to lock
    # GIL -> deadlock.
    def _abort_ontimeout(_tWCFS t, int fdabort, double dt, pychan nogilready not None):
        cdef chan[double] timeoutch = time.after(dt)
        emsg1 = "\nC: test timed out after %.1fs\n" % (dt / time.second)
        cdef char *_emsg1 = emsg1
        with nogil:
            # tell main thread that we entered nogil world
            nogilready.chan_structZ().close()
            t.__abort_ontimeout(dt, timeoutch, fdabort, _emsg1)

    cdef void __abort_ontimeout(_tWCFS t, double dt, chan[double] timeoutch,
                int fdabort, const char *emsg1) nogil except +topyexc:
        _ = select([
            timeoutch.recvs(),                  # 0
            t._closed.chan_structZ().recvs(),   # 1
        ])
        if _ == 1:
            return  # tDB closed = testcase completed

        # timeout -> force-umount wcfs
        writeerr(emsg1)
        writeerr("-> aborting wcfs fuse connection to unblock ...\n\n")
        xwrite(fdabort, b"1\n")
        t._wcfuseaborted.chan_structZ().close()


# read_exfault_nogil reads mem with GIL released and returns its content.
#
# If reading hits segmentation fault, it is converted to SegmentationFault exception.
class SegmentationFault(Exception): pass
cdef sync.Mutex exfaultMu     # one at a time as sigaction is per-process
cdef sigjmp_buf exfaultJmp
cdef cbool faulted
def read_exfault_nogil(const unsigned char[::1] mem not None) -> bytes:
    assert len(mem) == 1, "read_exfault_nogil: only [1] mem is supported for now"
    cdef unsigned char b
    global faulted
    cdef cbool faulted_

    # somewhat dup of MUST_FAULT in test_virtmem.c
    with nogil:
        exfaultMu.lock()

    faulted = False
    try:
        with nogil:
            b = _read_exfault(&mem[0])
    finally:
        faulted_ = faulted
        with nogil:
            exfaultMu.unlock()

    if faulted_:
        raise SegmentationFault()
    return bytes(bytearray([b]))

cdef void exfaultSighand(int sig) nogil:
    # return from sighandler to proper place with faulted=True
    global faulted
    faulted = True
    siglongjmp(exfaultJmp, 1)

cdef unsigned char _read_exfault(const unsigned char *p) nogil except +topyexc:
    global faulted

    cdef sigaction_t act, saveact
    act.sa_handler = exfaultSighand
    act.sa_flags   = 0

    err = sigemptyset(&act.sa_mask)
    if err != 0:
        panic("sigemptyset: failed")
    err = sigaction(SIGSEGV, &act, &saveact)
    if err != 0:
        panic("sigaction SIGSEGV -> exfaultSighand: failed")

    b = 0xff
    if sigsetjmp(exfaultJmp, 1) == 0:
        b = p[0] # should pagefault -> sighandler does longjmp
    else:
        # faulted
        if not faulted:
            panic("faulted, but !faulted")

    err = sigaction(SIGSEGV, &saveact, NULL)
    if err != 0:
        panic("sigaction SIGSEGV <- restore: failed")

    return b


# --------


cdef extern from "<fcntl.h>" nogil:
    int posix_fadvise(int fd, off_t offset, off_t len, int advice);
    enum: POSIX_FADV_DONTNEED

# fadvise_dontneed tells the kernel that file<fd>[offset +len) is not needed.
#
# see fadvise(2) for details.
def fadvise_dontneed(int fd, off_t offset, off_t len):
    cdef int err = posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED)
    if err:
        PyErr_SetFromErrno(OSError)

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
