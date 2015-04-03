/* Wendelin.bigfile | Pgoffset -> page mapping
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
 * Implementation of get/set/del/clear for PageMap.
 * See wendelin/bigfile/pagemap.h for general PageMap description.
 */

#include <wendelin/bigfile/pagemap.h>
#include <wendelin/bug.h>

#include <sys/mman.h>


/* allocate 1 hw page from OS */
static void *os_alloc_page(void)
{
    // -> xmmap ?
    void *addr;
    addr = mmap(NULL, PAGE_SIZE, PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS /* | MAP_POPULATE */,
                -1, 0);
    if (addr == MAP_FAILED)
        addr = NULL;

    return addr;
}

static void *os_xalloc_page(void)
{
    void *addr = os_alloc_page();
    if(!addr)
        BUGe();
    return addr;
}

/* deallocate 1 hw page to OS */
static void os_free_page(void *addr)
{
    /* must not fail */
    xmunmap(addr, PAGE_SIZE);
}





void pagemap_init(PageMap *pmap, unsigned pageshift)
{
    unsigned height;

    height = (8*sizeof(pgoff_t) - pageshift + (PAGEMAP_LEVEL_BITS - 1)) /
                PAGEMAP_LEVEL_BITS;

    pmap->pmap0 = 0ULL;
    pmap->rshift0 = height * PAGEMAP_LEVEL_BITS;
}


Page *pagemap_get(const PageMap *pmap, pgoff_t pgoffset)
{
    unsigned rshift = pmap->rshift0;
    void ***tail, **tab;
    unsigned idx;

    BUG_ON(pgoffset >> rshift);

    /* walk through tables
     *
     * tail - points to entry in previous-level table,
     * tab  - to current-level table,
     * idx  - index of entry in current table
     *
     *                    tab
     *           +-----+    ,+-----+
     *           |     |   | |     |
     *      tail +-----+---  |     |
     *           +-----+     |     |    -> ...
     *           |     |     |     |   |
     *           |     | idx +-----+---
     *           +-----+     +-----+
     */
    tail = (void ***)&pmap->pmap0;
    while (1) {
        rshift -= PAGEMAP_LEVEL_BITS;
        idx = (pgoffset >> rshift) & PAGEMAP_NR_MASK;

        tab = *tail;
        if (!tab)
            return NULL;    /* table not present - not found */

        tab = PTR_POINTER(tab);

        if (!rshift)
            break;

        // XXX move up?
        tail = (void ***)&tab[idx];
    }

    /* final-level table found - get result from there */
    return tab[idx];
}


void pagemap_set(PageMap *pmap, pgoff_t pgoffset, Page *page)
{
    unsigned rshift = pmap->rshift0;
    void ***tail, **tab;
    unsigned idx;

    BUG_ON(pgoffset >> rshift);
    BUG_ON(!page);   // XXX or better call pagemap_del() ?

    /* walk through tables, allocating memory along the way, if needed
     * (see who is who in pagemap_get) */
    tail = (void ***)&pmap->pmap0;
    while(1) {
        rshift -= PAGEMAP_LEVEL_BITS;
        idx = (pgoffset >> rshift) & PAGEMAP_NR_MASK;

        tab = *tail;
        if (!tab) {
            tab = os_xalloc_page(); /* NOTE - is hw page aligned */
            BUG_ON(tab != PTR_POINTER(tab));
            /* NOTE for newly allocated tab we don't need to adjust count for
             *      *tail - xcount=0 means there is 1 entry (which we'll set) */
            *tail = tab;
        }
        else {
            tab = PTR_POINTER(tab);

            /* entry empty - adjust tail counter as we'll next either be setting
             * new table pointer, or page pointer to it */
            if (!tab[idx])
                *tail = PTR_XCOUNT_ADD(*tail, +1);
        }

        if (!rshift)
            break;

        // XXX move up?
        tail = (void ***)&tab[idx];
    }

    tab[idx] = page;
}


int pagemap_del(PageMap *pmap, pgoff_t pgoffset)
{
    unsigned rshift = pmap->rshift0;
    unsigned height = rshift / PAGEMAP_LEVEL_BITS;
    unsigned idx, l /* current level */;
    /* tailv[l] points to tail pointing to entry pointing to tab_i */
    void ***tailv[height], **tab;   // XXX height -> height+1 ?

    BUG_ON(pgoffset >> rshift);

    /* walk tables to the end and see if entry is there
     * (see who is who in pagemap_get) */
    l = 0;
    tailv[0] = (void ***)&pmap->pmap0;
    while (1) {
        rshift -= PAGEMAP_LEVEL_BITS;
        idx = (pgoffset >> rshift) & PAGEMAP_NR_MASK;

        tab = *tailv[l];
        if (!tab)
            return 0;   /* entry already missing */
        tab = PTR_POINTER(tab);

        if (!rshift)
            break;

        tailv[++l] = (void ***)&tab[idx];
    }

    if (!tab[idx])
        return 0;   /* not found in last-level */

    /* entry present - clear it and unwind back, decreasing counters and
     * freeing tables memory along the way */
    tab[idx] = NULL;
    do {
        void ***tail = tailv[l];

        if (PTR_XCOUNT(*tail)) {
            *tail = PTR_XCOUNT_ADD(*tail, -1);
            break;  /* other entries present - nothing to delete */
        }

        /* table became empty - free & forget it */
        os_free_page(*tail);
        *tail = NULL;
    } while (l--);

    return 1;
}


void pagemap_clear(PageMap *pmap)
{
    /* leverage pagemap walker logic, but nothing to do with leaf table
     * contents - the freeing is done when going each level up */
    __pagemap_for_each_leaftab(tab, tailv, l, pmap,
            /* when going level up: */
            {
                os_free_page(PTR_POINTER(*tailv[l]));

                /* clearing not needed - we'll free this memory at next step
                 * anyway, but easier for debugging for now */
                *tailv[l] = NULL;
            }
        )
            (void)tab; /* unused */
}
