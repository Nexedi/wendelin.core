/* Wendelin. Utilities for handling assertions and bugs
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

/* first include with NDEBUG unset to get __assert_* prototypes */
#undef NDEBUG
#include <assert.h>

#include <wendelin/bug.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>


void __bug_fail(const char *expr, const char *file, unsigned line, const char *func)
{
    /* just tail to libc */
    __assert_fail(expr, file, line, func);
}


void __bug(const char *file, unsigned line, const char *func)
{
    fprintf(stderr, "%s:%u %s\tBUG!\n", file, line, func);
    abort();
}


void __bug_errno(const char *file, unsigned line, const char *func)
{
    char errno_buf[128];
    char *errno_str;

    errno_str = strerror_r(errno, errno_buf, sizeof(errno_buf));
    fprintf(stderr, "%s:%u %s\tBUG! (%s)\n", file, line, func, errno_str);
    abort();
}


void __warn(const char *file, unsigned line, const char *func, const char *msg)
{
    fprintf(stderr, "%s:%u %s WARN: %s\n", file, line, func, msg);
}


void __todo(const char *expr, const char *file, unsigned line, const char *func)
{
    fprintf(stderr, "%s:%u %s\tTODO %s\n", file, line, func, expr);
    abort();
}


void __abort_thread(const char *file, unsigned line, const char *func)
{
    pid_t tgid = getpid();              /* thread-group id of current thread */
    pid_t tid  = syscall(SYS_gettid);   /* thread       id of current thread */
    int main_thread = (tgid == tid);
    pid_t pid;

    fprintf(stderr, "%s:%u %s\tABORT_THREAD %i/%i%s\n", file, line, func, tgid, tid, main_thread ? " (main)" : "");

    /* if it is main thread - terminate whole process -
     * - it is logical and this way we get proper exit code */
    if (main_thread)
        abort();

    /* else try to produce coredump, terminate current thread, but do not kill the whole process
     *
     * ( on Linux, if a thread gets fatal signal and does not handle it, whole
     *   thread-group is terminated by kernel, after dumping core. OTOH, there is
     *   no other way to make the kernel dump core of us than to get a fatal
     *   signal without handling it.
     *
     *   What could work, is to first remove current thread from it's
     *   thread-group, and then do usual abort(3) which is ~ raise(SIGABRT),
     *   but such leaving-thread-group functionality is non-existent as per linux-v4.1.
     *
     *   NOTE Once sys_ungroup(2) system call was mentioned as being handy long
     *   ago, but it not implemented anywhere:
     *
     *   https://git.kernel.org/cgit/linux/kernel/git/history/history.git/commit/?id=63540cea
     *   https://lkml.org/lkml/2002/9/15/125 )
     *
     * ~~~~
     *
     * vfork is ~ clone(CLONE_VFORK | CLONE_VM) without CLONE_THREAD.
     *
     * - without CLONE_THREAD means the child will leave current thread-group
     * - CLONE_VM means it will share memory
     * - CLONE_VFORK means current thread will pause before clone finishes
     *
     * so it looks all we have to do to get a coredump and terminate only
     * current thread is vfork + abort in clone + pthread_exit in current.
     *
     * But it is not so - because on coredumping, Linux terminates all
     * processes who share mm with terminating process, not only processes from
     * thread group, and it was done on purpose:
     *
     *   https://git.kernel.org/cgit/linux/kernel/git/history/history.git/commit/?id=d89f3847
     *   ("properly wait for all threads that share the same MM ...")
     *
     *
     * So the only thing we are left to do, is to do usual fork() and coredump
     * from forked child.
     */
    pid = fork();
    if (pid == -1) {
        /* fork failed for some reason */
        BUGe();
    }
    else if (!pid) {
        /* child - abort it - this way we can get coredump.
         * NOTE it does not affect parent */
        abort();
    }
    else {
        /* forked ok - wait for child to abort and exit current thread */
        int status;
        pid_t waited;

        waited = waitpid(pid, &status, 0);
        if (waited == -1)
            BUGe();

        ASSERT(waited == pid);          /* waitpid can only return for child */
        ASSERT(WIFSIGNALED(status));    /* child must terminated via SIGABRT */

        /* now we know child terminated the way we wanted it to terminate
         * -> we can exit current thread */
        pthread_exit(NULL);
    }
}
