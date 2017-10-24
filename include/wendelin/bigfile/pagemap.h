#ifndef _WENDELIN_BIGFILE_PAGEMAP_H_
#define _WENDELIN_BIGFILE_PAGEMAP_H_

/* Wendelin.bigfile | Pgoffset -> page mapping
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
 *
 * ~~~~~~~~
 *
 * BigFileH needs to maintain `{} f_pgoffset -> page` mapping. A hash or a
 * binary tree could be used there, but since we know files are most of the
 * time accessed sequentially and locally in pages-batches, we can also
 * organize the mapping in batches of keys.
 *
 * Specifically f_pgoffset bits are so divided into parts, that every part
 * addresses 1 entry in a table of hardware-page in size. To get to the actual
 * value, the system lookups first table by first part of f_pgoffset, then from
 * first table and next part from address - second table, etc.
 *
 * To clients this looks like a dictionary with get/set/del & clear methods,
 * but lookups are O(1) time always, and in contrast to hashes values are
 * stored with locality (= adjacent lookups almost always access the same tables).
 */

#include <wendelin/utils.h> /* BUILD_ILOG2_EXACT    */

#include <sys/user.h>       /* PAGE_SIZE & friends  */
/* FIXME Debian before Wheezy patches libc and replaces PAGE_SIZE with
 * `sysconf(...)` and removes PAGE_SHIFT. Temporary workaround follows: */
#ifndef PAGE_SHIFT
# if defined(__x86_64__) || defined(__i386__)
#  define PAGE_SHIFT     12
# else
#  error  TODO: please specify PAGE_SHIFT for your arch
# endif
# undef  PAGE_SIZE
# define PAGE_SIZE  (1UL << PAGE_SHIFT)
#endif

#include <wendelin/bigfile/types.h>

typedef struct Page Page;


/*
 * >>> `pgoffset -> page`  lookup pagetable
 *
 * Looking up page by pgoffset is done via sparsely-organized lookup tables -
 * each 9 bits in pgoffset index one such table.
 *
 * pointers in all levels except last contain two parts:
 *
 *  - pointer to next-level table                   (upper bits)
 *  - count of entries in next-level table minus 1  (lower bits)
 */

/* how many bits of key each level encodes (table fits exactly in 1 hw page)
 *
 * TODO it is possible to use near pointers and on amd64 reduce entry size from
 *      8 bytes to e.g. 4 bytes */
#define PAGEMAP_LEVEL_BITS      (PAGE_SHIFT - BUILD_ILOG2_EXACT(sizeof(void *)))
/* how many entries in each level */
#define PAGEMAP_LEVEL_ENTRIES   (1UL << PAGEMAP_LEVEL_BITS)
/* mask extracting nr from pointer to next-level table */
#define PAGEMAP_NR_MASK         (PAGEMAP_LEVEL_ENTRIES - 1)


/* "or" & "and" bits in pointer */
#define PTR_OR(ptr, mask)   ( (typeof(ptr)) ((uintptr_t)(ptr) | (mask)) )
#define PTR_AND(ptr, mask)  ( (typeof(ptr)) ((uintptr_t)(ptr) & (mask)) )

/* pointer without count bits */
#define PTR_POINTER(ptr)    PTR_AND(ptr, ~PAGEMAP_NR_MASK)

/* "count-1" extracted from pointer */
#define PTR_XCOUNT(ptr) ((unsigned)((uintptr_t)(ptr) & PAGEMAP_NR_MASK))

/* compute pointer with adjusted counter */
#define PTR_XCOUNT_ADD(ptr, v)  ({  \
    unsigned c = PTR_XCOUNT(ptr);                       \
    c += v;                                             \
    BUG_ON(c & ~PAGEMAP_NR_MASK);                       \
    PTR_OR(PTR_POINTER(ptr), c);                        \
})


struct PageMap {
    /* each pointer contains pointer & nr of entries in next-level table.
     * this is pointer to 0-level */
    uintptr_t   pmap0;

    /* = PAGEMAP_LEVEL_BITS * height */
    unsigned    rshift0;
};
typedef struct PageMap PageMap;


/* initialize new pagemap for looking up pages of (1 << pageshift) in size */
void  pagemap_init(PageMap *pmap, unsigned pageshift);

/* pmap[pgoffset] -> page */
Page *pagemap_get(const PageMap *pmap, pgoff_t pgoffset);

/* pmap[pgoffset] <- page */
void  pagemap_set(PageMap *pmap, pgoff_t pgoffset, Page *page);

/* del pmap[pgoffset]
 *
 * @return  0 - entry was already missing;  1 - entry was there
 */
int   pagemap_del(PageMap *pmap, pgoff_t pgoffset);

/* remove all entries from pagemap
 *
 * after such removal pagemap frees all memory allocated for tables.
 * The only memory left allocated is `struct PageMap` itself.
 */
void  pagemap_clear(PageMap *pmap);


/* (internal helper) iterate over all (!empty leaf) tables in pagemap
 *
 * tab, tailv, l - names of variables which will be declared internally:
 *
 *    void  **tab;                     iterates !empty leaf tables
 *    void ***tailv[pmap->height];     *tailv[l] - points to table level l
 *    unsigned l;
 *
 * CODE_ON_UP
 *
 *    code to execute right after going up one level
 *    (on all levels, not only on leafs)
 *
 * PageMap *pmap;
 */
#define __pagemap_for_each_leaftab(tab, tailv, l, pmap, CODE_ON_UP)             \
    /* loop-once just to declare 'unsigned' variables                   */      \
    /* (this is not a loop - just a workaround for C not allowing to    */      \
    /*  declare variables of different types in "for")                  */      \
    for (unsigned                                                               \
            __height = (pmap)->rshift0 / PAGEMAP_LEVEL_BITS,                    \
            l,      /* current level */                                         \
            __cont; /* whether to continue looping                      */      \
                    /* (vs client saying "break")                       */      \
                                                                                \
                __height;                                                       \
                __height = 0                                                    \
        )                                                                       \
                                                                                \
    /* recursion loop - going down/up through levels */                         \
    for (void  **tab,                                                           \
              ***tailv[__height+1], /* tail for each level XXX verify height+1?*/   \
              ***__down = (         /* whether down/up on next iter */          \
                                                                                \
                    /* loop start init */                                       \
                    tailv[0] = (void ***)&(pmap)->pmap0,                        \
                    tailv[1] = (void ***)PTR_POINTER(*tailv[0]),                \
                    l = tailv[1] ? 1 : 0,                                       \
                    __cont=1,                                                   \
                                                                                \
                    NULL);                                                      \
                                                                                \
            l && __cont;  /* !__cont - inner client code said "break"  */       \
                                                                                \
            /* by default go up 1 level, after current tab scanned to the end */\
            /* The inner loop may go ask us go down to next level.    */        \
            ({ if (__down)                                                      \
                  tailv[++l] = __down;                                          \
               else {                                                           \
                  --l;                                                          \
                  { CODE_ON_UP }                                                \
                  ++tailv[l];                                                   \
               }                                                                \
                                                                                \
               __down = NULL; })                                                \
        )                                                                       \
                                                                                \
        /* go down 1 level with tab */                                          \
        if (l < __height) {                                                     \
                                                                                \
            /* loop over entries in tab - continuing from where we stopped */   \
            for ( void **__tab_prev = PTR_POINTER(*tailv[l-1]),                 \
                       **__tab;                                                 \
                                                                                \
                    (void **)tailv[l] - __tab_prev < PAGEMAP_LEVEL_ENTRIES;     \
                                                                                \
                        ++tailv[l]                                              \
                )                                                               \
                                                                                \
                /* load entry; no entry -> next entry */                        \
                if (!(__tab = *tailv[l]))                                       \
                    continue;                                                   \
                                                                                \
                /* go down 1 level with tab */                                  \
                else {                                                          \
                    __down = (void ***)PTR_POINTER(__tab);                      \
                    break;  /* to control loop */                               \
                }                                                               \
        }                                                                       \
                                                                                \
        /* tailv[l] points at leaf table and is !NULL               */          \
        else if (tab = (void **)tailv[l], 1)                                    \
            /* client code goes here - any C statement - with or without {} */  \
            /* with break being special - to break the whole iteration you  */  \
            /* should break with __cont=0 set                               */


/* iterate over all !NULL entries in pagemap
 *
 * Page *page       - variable that will iterate.
 * PageMap *pmap;
 */
#define pagemap_for_each(page, pmap)                                            \
    __pagemap_for_each_leaftab(__tab, __tailv, __l, pmap, {})                   \
        for (unsigned __idx = (                                                 \
                __cont = 0,     /* for break - to break the whole iter  */      \
                page=__tab[0],                                                  \
                0);                                                             \
                                                                                \
                __idx < PAGEMAP_LEVEL_ENTRIES                                   \
                    /* turn off break handling on loop exit */                  \
                    || ((__cont=1), 0);                                         \
                                                                                \
                    ++__idx                                                     \
            )                                                                   \
            if ((page = __tab[__idx]))                                          \
                    /*                                                      */  \
                    /* CLIENT CODE GOES HERE:                               */  \
                    /*                                                      */  \
                    /* - any C statement allowed - with or without {}       */  \
                    /* - also it can say break and break the whole pagemap  */  \
                    /*   iteration                                          */


#endif
