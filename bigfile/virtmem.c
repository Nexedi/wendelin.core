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

#include <wendelin/bigfile/virtmem.h>
#include <wendelin/bigfile/ram.h>

#include <sys/mman.h>
#include <errno.h>


/*
 * allocate virtual memory address space
 * the pages are initially protected to prevent any access
 *
 * @addr    NULL - at anywhere,     !NULL - exactly there
 * @return  !NULL - mapped there    NULL - error
 */
void *mem_valloc(void *addr, size_t len)
{
    void *a;
    a = mmap(addr, len, PROT_NONE,
            MAP_PRIVATE | MAP_ANONYMOUS
            /* don't try to (pre-)allocate memory - just virtual address space */
            | MAP_NORESERVE
            | (addr ? MAP_FIXED : 0),
            -1, 0);

    if (a == MAP_FAILED)
        a = NULL;

    if (a && addr)
        /* verify OS respected our MAP_FIXED request */
        BUG_ON(a != addr);

    return a;
}


/* like mem_valloc() but allocation must not fail */
void *mem_xvalloc(void *addr, size_t len)
{
    void *a;
    a = mem_valloc(addr, len);
    BUG_ON(!a);
    return a;
}


/********************
 * Internal helpers *
 ********************/

static size_t page_size(const Page *page)
{
    return page->ramh->ram->pagesize;
}

void page_incref(Page *page)
{
    page->refcnt++;     // XXX atomically ?
}

void page_decref(Page *page)
{
    page->refcnt--;     // XXX atomically ?
    BUG_ON(page->refcnt < 0);

    // TODO if unused delete self && clear pagemap ?
    // XXX  if dirty -> delete = not ok
}

void *page_mmap(Page *page, void *addr, int prot)
{
    RAMH *ramh = page->ramh;
    // XXX better call ramh_mmap_page() without tinkering wih ramh_ops?
    return ramh->ramh_ops->mmap_page(ramh, page->ramh_pgoffset, addr, prot);
}
