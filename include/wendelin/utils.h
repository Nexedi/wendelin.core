#ifndef _WENDELIN_UTILS_H_
#define _WENDELIN_UTILS_H_

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

#include <ccan/build_assert/build_assert.h>
#include <stdlib.h>
#include <pthread.h>


/* compile-time exact ilog2(x); x must be 2^k */
#define BUILD_ILOG2_EXACT(x) (  \
    /* check that only 1 bit set - then it will be exact */     \
    BUILD_ASSERT_OR_ZERO(__builtin_popcount(x) == 1) +          \
    __builtin_ctz(x)                                            \
)

/* runtime analog of BUILD_ILOG2_EXACT */
unsigned ilog2_exact(unsigned x);


/* upcast from ptr-to-embedded type to ptr-to-container
 *
 * ex:
 *
 *      struct base { ... };
 *      struct child { ... struct base; ... };
 *
 *      struct base *pbase;
 *      struct child *pchild = upcast(struct child *, pbase);
 *
 * TODO give error on invalid usage (so far only warning)
 */
#define upcast(ptype, ptr) ({                   \
    ptype __child = NULL;                       \
    /* implicit offsetof(child, base); gives only warning on incorrect usage */    \
    typeof(ptr) __base = __child;               \
    (ptype)                                     \
        ( (char *)ptr - (uintptr_t)__base );    \
})



/* allocate zeroed memory */
static inline
void *zalloc(size_t size)
{
    return calloc(1, size);
}


/* should-not-fail helpers */
void *xzalloc(size_t size);
char *xstrdup(const char *s);
void xclose(int fd);
void xmunmap(void *addr, size_t len);
void xmprotect(void *addr, size_t len, int prot);
void xfallocate(int fd, int mode, off_t offset, off_t len);
void xftruncate(int fd, off_t len);

void xsigemptyset(sigset_t *set);
void xsigaddset(sigset_t *set, int sig);
int  xsigismember(const sigset_t *set, int sig);
void xpthread_sigmask(int how, const sigset_t *set, sigset_t *oldset);

void xpthread_mutex_lock(pthread_mutex_t *);
void xpthread_mutex_unlock(pthread_mutex_t *);

#endif
