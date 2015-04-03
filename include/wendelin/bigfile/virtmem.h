#ifndef _WENDELIN_BIGFILE_VIRTMEM_H_
#define _WENDELIN_BIGFILE_VIRTMEM_H_

/* Wendelin.bigfile | Virtual memory
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

#include <stdint.h>
#include <wendelin/list.h>

typedef struct RAMH RAMH;


/* Page - describes fixed-size item of physical RAM associated with content from file */
enum PageState {
    PAGE_EMPTY      = 0, /* file content has not been loaded yet */
    PAGE_LOADED     = 1, /* file content has     been loaded and was not modified */
    PAGE_DIRTY      = 2, /* file content has     been loaded and was     modified */
};
typedef enum PageState PageState;

struct Page {
    PageState   state;

    /* wrt ram - associated with */
    RAMH*       ramh;
    pgoff_t     ramh_pgoffset;

    /* in recently-used pages for ramh->ram (ram->lru_list -> _) */
    struct list_head lru;

    int     refcnt; /* each mapping in a vma counts here */
};
typedef struct Page Page;


/************
 * Internal *
 ************/

/* mmap page memory into address space
 *
 * @addr     NULL - mmap somewhere,    !NULL - mmap exactly there (MAP_FIXED)
 * @return  !NULL - mmapped ok there,   NULL - error
 *
 * NOTE to unmap memory either
 *
 *      - use usual munmap(2), or
 *      - mmap(2) something else in place of mmaped page memory.
 */
void *page_mmap(Page *page, void *addr, int prot);

void page_incref(Page *page);
void page_decref(Page *page);


/* allocate virtual memory address space */
void *mem_valloc(void *addr, size_t len);
void *mem_xvalloc(void *addr, size_t len);

#endif
