/* Wendelin. Utilities for handling assertions and bugs
 * Copyright (C) 2014-2015  Nexedi SA and Contributors.
 *                          Kirill Smelkov <kirr@nexedi.com>
 *
 * This program is free software: you can Use, Study, Modify and Redistribute
 * it under the terms of the GNU General Public License version 3, or (at your
 * option) any later version, as published by the Free Software Foundation.
 *
 * You can also Link and Combine this program with other software covered by
 * the terms of any of the Free Software licenses or any of the Open Source
 * Initiative approved licenses and Convey the resulting work. Corresponding
 * source of such a combination shall include the source code for all other
 * software used.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * See COPYING file for full licensing terms.
 * See https://www.nexedi.com/licensing for rationale and options.
 */

/* first include with NDEBUG unset to get __assert_* prototypes */
#undef NDEBUG
#include <assert.h>

#include <wendelin/bug.h>

#include <stdio.h>
#include <stdlib.h>
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


void __bug_err(const char *file, unsigned line, const char *func, int err)
{
    char err_buf[128];
    char *err_str;

    err_str = strerror_r(err, err_buf, sizeof(err_buf));
    fprintf(stderr, "%s:%u %s\tBUG! (%s)\n", file, line, func, err_str);
    abort();
}


void __bug_errno(const char *file, unsigned line, const char *func)
{
    __bug_err(file, line, func, errno);
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
