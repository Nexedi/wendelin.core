#ifndef _WENDELIN_TESTING_UTILS_H_
#define _WENDELIN_TESTING_UTILS_H_

/* Wendelin.bigfile | various testing utilities
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

#include <wendelin/bigfile/ram.h>

/* access to vma memory as byte[] and blk_t[] */
#define b(vma, idx) ( ((volatile uint8_t *)vma->addr_start) [ idx ] )
#define B(vma, idx) ( ((volatile blk_t *)vma->addr_start)   [ idx ] )


/* RAM with limit on #allocated pages
 *
 * NOTE allocated pages are linked to ->lru_list and backend->lru_list will be empty.
 */
struct RAMLimited {
    RAM;
    RAM *backend;
    size_t alloc_max;
    size_t nalloc;
};
typedef struct RAMLimited RAMLimited;

RAMLimited *ram_limited_new(RAM *backend, size_t alloc_max);

#endif
