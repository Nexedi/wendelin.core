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

"""Package mm provides access to OS memory management interfaces like mlock and mincore."""

from posix cimport mman
from cpython.exc cimport PyErr_SetFromErrno
#from libc.stdio cimport printf

# mlock2 is provided starting from glibc 2.27
cdef extern from *:
    """
    #if defined(__GLIBC__)
    #if !__GLIBC_PREREQ(2, 27)
    #include <unistd.h>
    #include <sys/syscall.h>
    static int mlock2(const void *addr, size_t len, int flags) {
    #ifndef SYS_mlock2
        errno = ENOSYS;
        return -1;
    #else
        long err = syscall(SYS_mlock2, addr, len, flags);
        if (err != 0) {
            errno = -err;
            return -1;
        }
        return 0;
    #endif
    }
    #endif
    #endif

    #ifndef MLOCK_ONFAULT
    # define MLOCK_ONFAULT  1
    #endif
    """
    pass

cdef extern from "<sys/user.h>":
    cpdef enum:
        PAGE_SIZE

cpdef enum:
    PROT_EXEC       = mman.PROT_EXEC
    PROT_READ       = mman.PROT_READ
    PROT_WRITE      = mman.PROT_WRITE
    PROT_NONE       = mman.PROT_NONE

    MLOCK_ONFAULT   = mman.MLOCK_ONFAULT
    MCL_CURRENT     = mman.MCL_CURRENT
    MCL_FUTURE      = mman.MCL_FUTURE
    #MCL_ONFAULT     = mman.MCL_ONFAULT

    MADV_NORMAL     = mman.MADV_NORMAL
    MADV_RANDOM     = mman.MADV_RANDOM
    MADV_SEQUENTIAL = mman.MADV_SEQUENTIAL
    MADV_WILLNEED   = mman.MADV_WILLNEED
    MADV_DONTNEED   = mman.MADV_DONTNEED
    #MADV_FREE       = mman.MADV_FREE
    MADV_REMOVE     = mman.MADV_REMOVE

    MS_ASYNC        = mman.MS_ASYNC
    MS_SYNC         = mman.MS_SYNC
    MS_INVALIDATE   = mman.MS_INVALIDATE

# incore returns bytearray vector indicating whether page of mem is in core or not.
#
# mem start must be page-aligned.
def incore(const unsigned char[::1] mem not None) -> bytearray:
    cdef size_t size = mem.shape[0]
    if size == 0:
        return bytearray()
    cdef const void *addr = &mem[0]

    # size in pages; rounded up
    cdef size_t pgsize = (size + (PAGE_SIZE-1)) // PAGE_SIZE

    #printf("\n\n%p %ld\n", addr, size)

    incore = bytearray(pgsize)
    cdef unsigned char[::1] incorev = incore

    cdef err = mman.mincore(<void *>addr, size, &incorev[0])
    if err:
        PyErr_SetFromErrno(OSError)

    return incore


# lock locks mem pages to be resident in RAM.
#
# see mlock2(2) for description of flags.
def lock(const unsigned char[::1] mem not None, int flags):
    cdef const void *addr = &mem[0]
    cdef size_t      size = mem.shape[0]

    cdef err = mman.mlock2(addr, size, flags)
    if err:
        PyErr_SetFromErrno(OSError)

    return


# unlock unlocks mem pages from being pinned in RAM.
def unlock(const unsigned char[::1] mem not None):
    cdef const void *addr = &mem[0]
    cdef size_t      size = mem.shape[0]

    cdef err = mman.munlock(addr, size)
    if err:
        PyErr_SetFromErrno(OSError)

    return


from posix.types cimport off_t

# map_ro memory-maps fd[offset +size) as read-only.
# The mapping is created with MAP_SHARED.
def map_ro(int fd, off_t offset, size_t size):
    cdef void *addr

    addr = mman.mmap(NULL, size, mman.PROT_READ, mman.MAP_SHARED, fd, offset)
    if addr == mman.MAP_FAILED:
        PyErr_SetFromErrno(OSError)

    return <unsigned char[:size:1]>addr

# map_into_ro is similar to map_ro, but mmaps fd[offset:...] into mem's memory.
def map_into_ro(unsigned char[::1] mem not None, int fd, off_t offset):
    cdef void   *addr = &mem[0]
    cdef size_t size  = mem.shape[0]

    addr = mman.mmap(addr, size, mman.PROT_READ, mman.MAP_FIXED |
                                                 mman.MAP_SHARED, fd, offset)
    if addr == mman.MAP_FAILED:
        PyErr_SetFromErrno(OSError)

    return

# unmap unmaps memory covered by mem.
def unmap(const unsigned char[::1] mem not None):
    cdef const void *addr = &mem[0]
    cdef size_t      size = mem.shape[0]

    cdef err = mman.munmap(<void *>addr, size)
    if err:
        PyErr_SetFromErrno(OSError)

    return

# map_zero_ro creates new read-only mmaping that all reads as zero.
# created mapping, even after it is accessed, does not consume memory.
def map_zero_ro(size_t size):
    cdef void *addr
    # mmap /dev/zero with MAP_NORESERVE and MAP_SHARED
    # this way the mapping will be able to be read, but no memory will be allocated to keep it.
    f = open("/dev/zero", "rb")
    addr = mman.mmap(NULL, size, mman.PROT_READ, mman.MAP_SHARED | mman.MAP_NORESERVE, f.fileno(), 0)
    f.close()
    if addr == mman.MAP_FAILED:
        PyErr_SetFromErrno(OSError)
        return

    return <unsigned char[:size:1]>addr

# map_zero_into_ro is similar to map_zero_ro, but mmaps zeros into mem's memory.
def map_zero_into_ro(unsigned char[::1] mem not None):
    cdef void   *addr = &mem[0]
    cdef size_t size  = mem.shape[0]

    f = open("/dev/zero", "rb")
    addr = mman.mmap(addr, size, mman.PROT_READ, mman.MAP_FIXED |
                                                 mman.MAP_SHARED | mman.MAP_NORESERVE, f.fileno(), 0)
    f.close()
    if addr == mman.MAP_FAILED:
        PyErr_SetFromErrno(OSError)
        return

    return


# advise advises kernel about use of mem's memory.
#
# see madvise(2) for details.
def advise(const unsigned char[::1] mem not None, int advice):
    cdef const void *addr = &mem[0]
    cdef size_t      size = mem.shape[0]

    cdef err = mman.madvise(<void *>addr, size, advice)
    if err:
        PyErr_SetFromErrno(OSError)

    return


# protect sets protection on a region of memory.
#
# see mprotect(2) for details.
def protect(const unsigned char[::1] mem not None, int prot):
    cdef const void *addr = &mem[0]
    cdef size_t      size = mem.shape[0]

    cdef err = mman.mprotect(<void *>addr, size, prot)
    if err:
        PyErr_SetFromErrno(OSError)

    return


# sync asks the kernel to synchronize the file with a memory map.
#
# see msync(2) for details.
def sync(const unsigned char[::1] mem not None, int flags):
    cdef const void *addr = &mem[0]
    cdef size_t      size = mem.shape[0]

    cdef err = mman.msync(<void *>addr, size, flags)
    if err:
        PyErr_SetFromErrno(OSError)

    return
