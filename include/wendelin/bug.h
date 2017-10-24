#ifndef _WENDELIN_BUG_H_
#define _WENDELIN_BUG_H_

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


// XXX maybe not needed - just go with std assert()

#define TODO(expr)  do {                                    \
    if (expr)                                               \
        __todo(#expr, __FILE__, __LINE__, __func__);        \
} while (0)

#define WARN(msg) do {                              \
    __warn(__FILE__, __LINE__, __func__, msg);      \
} while (0)

#define BUG() do {                                  \
    __bug(__FILE__, __LINE__, __func__);            \
} while (0)

#define BUGerr(err) do {                            \
    __bug_err(__FILE__, __LINE__, __func__, err);   \
} while (0)

#define BUGe() do {                                 \
    __bug_errno(__FILE__, __LINE__, __func__);      \
} while (0)

/* like assert(expr) but works indepenently of NDEBUG */
// TODO unlikely
#define ASSERT(expr)    do {                                \
    if (!(expr))                                            \
        __bug_fail(#expr, __FILE__, __LINE__, __func__);    \
} while (0)

/* =ASSERT(!expr) */
#define BUG_ON(expr)    ASSERT(!(expr))



void __todo(const char *, const char *, unsigned, const char *)
    __attribute__((noreturn));
void __warn(const char *, unsigned, const char *, const char *);
void __bug(const char *, unsigned, const char *)
    __attribute__((noreturn));
void __bug_err(const char *, unsigned, const char *, int)
    __attribute__((noreturn));
void __bug_errno(const char *, unsigned, const char *)
    __attribute__((noreturn));
void __bug_fail(const char *, const char *, unsigned, const char *)
    __attribute__((noreturn));


#endif
