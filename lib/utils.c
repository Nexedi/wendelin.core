/* Wendelin. Miscellaneous utilities
 * Copyright (C) 2014-2015  Nexedi SA and Contributors.
 *                          Kirill Smelkov <kirr@nexedi.com>
 *
 * This program is free software: you can Use, Study, Modify and Redistribute
 * it under the terms of the GNU General Public License version 3, or (at your
 * option) any later version, as published by the Free Software Foundation.
 *
 * You can also Link and Combine this program with other software covered by
 * the terms of any of the Open Source Initiative approved licenses and Convey
 * the resulting work. Corresponding source of such a combination shall include
 * the source code for all other software used.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * See COPYING file for full licensing terms.
 */

#include <wendelin/utils.h>
#include <wendelin/bug.h>

#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>


/* ilog2 that must be exact */
unsigned ilog2_exact(unsigned x)
{
    BUG_ON(__builtin_popcount(x) != 1);
    return __builtin_ctz(x);
}


/* if allocating memory on DSO init or ram types registration fails, we have
 * bigger problems */
void *xzalloc(size_t size)
{
    void *addr;
    addr = zalloc(size);
    if (!addr)
        BUGe();
    return addr;
}


/* likewise */
char *xstrdup(const char *s)
{
    char *d;
    d = strdup(s);
    if (!d)
        BUGe();
    return d;
}


/* closing correct fd on shmfs-like filesystem should be ok */
void xclose(int fd)
{
    int err;
    err = close(fd);
    if (err)
        BUGe();
}

/* unmapping memory should not fail, if it was previously mmaped ok */
void xmunmap(void *addr, size_t len)
{
    int err;
    BUG_ON(!addr);  /* munmap(2) does not catch this XXX why ? */
    err = munmap(addr, len);
    if (err)
        BUGe();
}

/* changing protection to previously RW-mmaped memory should not fail */
void xmprotect(void *addr, size_t len, int prot)
{
    int err;
    err = mprotect(addr, len, prot);
    if (err)
        BUGe();
}

/* releasing memory with correct offset and len should not fail */
void xfallocate(int fd, int mode, off_t offset, off_t len)
{
    int err;
    err = fallocate(fd, mode, offset, len);
    if (err)
        BUGe();
}

/* truncating file to 0 size should be ok */
void xftruncate(int fd, off_t len)
{
    int err;
    err = ftruncate(fd, len);
    if (err)
        BUGe();
}


/* sig*set should not fail on any correct usage */
void xsigemptyset(sigset_t *set)
{
    int err;
    err = sigemptyset(set);
    if (err)
        BUGe();
}

void xsigaddset(sigset_t *set, int sig)
{
    int err;
    err = sigaddset(set, sig);
    if (err)
        BUGe();
}

/* pthread_sigmask() should not fail on any correct usage */
void xpthread_sigmask(int how, const sigset_t *set, sigset_t *oldset)
{
    int err;
    err = pthread_sigmask(how, set, oldset);
    if (err)
        BUGerr(err);
}
