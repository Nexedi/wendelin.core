/* Wendelin.bigfile | Low-level pagefault handler
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
 *
 * ~~~~~~~~
 *
 * low-level pagefault handler entry from OS
 *
 * its job is to lookup vma which is being accessed and whether it is
 * read/write, and tail to vma_on_pagefault().
 */

#include <wendelin/bug.h>

#include <signal.h>
#include <stdlib.h>
#include <errno.h>
#include <stdint.h>


/* "before us" previously installed SIGSEGV sigaction */
static struct sigaction prev_segv_act;
static int    segv_act_installed;

static int faulted_by(const struct ucontext *uc);


/* SIGSEGV handler */
static void on_pagefault(int sig, siginfo_t *si, void *_uc)
{
    struct ucontext *uc = _uc;
    unsigned write;

    BUG_ON(sig != SIGSEGV);
    BUG_ON(si->si_signo != SIGSEGV);

    /* determine what client wants - read or write */
    write = faulted_by(uc);

    /* we'll try to only handle "invalid permissions" faults (= read of page
     * with PROT_NONE | write to page with PROT_READ only).
     *
     * "address not mapped" (SEGV_MAPERR) and possibly anything else (e.g.
     * SI_USER for signals sent by kill - not by kernel) could not result from
     * valid access to prepared file address space, so we don't handle those. */
    if (si->si_code != SEGV_ACCERR)
        goto dont_handle;

    // XXX locking

    /* (1) addr -> vma  ;lookup VMA covering faulting memory address */
    // TODO
    goto dont_handle;

    /* now, since we found faulting address in registered memory areas, we know
     * we should serve this pagefault. */



    // TODO protect against different threads

    /* save/restore errno       XXX & the like ? */
    int save_errno = errno;

    // TODO handle pagefault at si->si_addr / write

    errno = save_errno;

    return;


dont_handle:
    /* pagefault not resulted from correct access to file memory.
     * Crash if no previous SIGSEGV handler was set, or tail to that.   */
    if (prev_segv_act.sa_flags & SA_SIGINFO)
        prev_segv_act.sa_sigaction(sig, si, _uc);
    else
    if (prev_segv_act.sa_handler != SIG_DFL &&
        prev_segv_act.sa_handler != SIG_IGN /* yes, SIGSEGV can't be ignored */)

        prev_segv_act.sa_handler(sig);

    else {
        /* no previous SIGSEGV handler was set - re-trigger to die
         *
         * NOTE here SIGSEGV was set blocked in thread sigmask by kernel
         * when invoking signal handler (we explicitly did not specify
         * SA_NODEFER when setting it up).
         *
         * Re-access original memory location, and it will fault with
         * coredump directly without calling signal handler again.  */
        // XXX how to know access size? we just proceed here with 1byte ...
        // FIXME don't touch memory on SI_USER - just BUG.
        volatile uint8_t *p = (uint8_t *)si->si_addr;
        if (write)
            *p = *p;
        else
            *p;

        /* could get here because ex. other thread remapped something in place
         * of old page. Die unconditionally */
        BUG();
    }
}


/* ensures pagefault handler for SIGSEGV is installed */
int pagefault_init(void)
{
    struct sigaction act;
    int err;

    /* protect from double sigaction installing. It is ok to be called twice. */
    if (segv_act_installed)
        goto done;

    act.sa_sigaction = on_pagefault;
    // |SA_RESTART(but does not work for read/write vs SIGSEGV?)
    /* NOTE no SA_ONSTACK - continue executing on the same stack
     * TODO stack overflow protection
     */
    /* NOTE we do not set SA_NODEFER. This means upon entry to signal handler,
     * SIGSEGV will be automatically blocked by kernel for faulting thread.
     *
     * This in particular means we'll get automatic protection from double
     * faults - in case handler or any other code it calls accesses memory
     * without appropriate protection prior set, the kernel will coredump.
     */
    act.sa_flags = SA_SIGINFO;

    /* do not want to block any other signals */
    err = sigemptyset(&act.sa_mask);
    if (err)
        return err;

    err = sigaction(SIGSEGV, &act, &prev_segv_act);
    if (err)
        return err;

    segv_act_installed = 1;
done:
    return 0;
}



/* determine what client faulted by - read or write
 *
 * @return  0 - read        !0 - write
 */
static int faulted_by(const struct ucontext *uc)
{
    int write;

#if defined(__x86_64__) || defined(__i386__)
    /*
     * http://stackoverflow.com/questions/17671869/how-to-identify-read-or-write-operations-of-page-fault-when-using-sigaction-hand
     * http://wiki.osdev.org/Exceptions#Page_Fault
     */
    write = uc->uc_mcontext.gregs[REG_ERR] & 0x2;
#else
# error TODO: implement read/write detection for pagefaults for your arch
#endif
    return write;
}
