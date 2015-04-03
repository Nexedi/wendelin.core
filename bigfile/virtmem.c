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
 *
 *
 * TODO description
 */

#include <wendelin/bigfile/virtmem.h>
#include <wendelin/bigfile/file.h>
#include <wendelin/bigfile/pagemap.h>
#include <wendelin/bigfile/ram.h>
#include <wendelin/bug.h>

#include <ccan/minmax/minmax.h>

#include <sys/mman.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>

static size_t   page_size(const Page *page);
static void     page_drop_memory(Page *page);
static void    *vma_page_addr(VMA *vma, Page *page);
static pgoff_t  vma_addr_fpgoffset(VMA *vma, uintptr_t addr);
static int      vma_page_ismapped(VMA *vma, Page *page);
static void     vma_page_ensure_unmapped(VMA *vma, Page *page);
static void     vma_page_ensure_notmappedrw(VMA *vma, Page *page);

#define VIRT_DEBUG   0
#if VIRT_DEBUG
# define TRACE(msg, ...) do { fprintf(stderr, msg, ## __VA_ARGS__); } while (0)
#else
# define TRACE(msg, ...) do {} while(0)
#endif

// TODO client code - block/unblock SIGSEGV so that we do not try to
// incorrectly handle pagefault and just die with coredump

/****************
 * OPEN / CLOSE *
 ****************/

// TODO block SIGSEGV
int fileh_open(BigFileH *fileh, BigFile *file, RAM *ram)
{
    bzero(fileh, sizeof(*fileh));
    fileh->ramh = ramh_open(ram);
    if (!fileh->ramh)
        goto out;

    fileh->file = file;
    INIT_LIST_HEAD(&fileh->mmaps);
    pagemap_init(&fileh->pagemap, ilog2_exact(ram->pagesize));

    return 0;

out:
    return -1;
}


// TODO block SIGSEGV
void fileh_close(BigFileH *fileh)
{
    Page *page;

    /* it's an error to close fileh with existing mappings */
    // XXX implement the same semantics usual files have wrt mmaps - if we release
    // fileh, but mapping exists - real fileh release is delayed to last unmap ?
    BUG_ON(!list_empty(&fileh->mmaps));

    /* drop all pages (dirty or not) associated with this fileh */
    pagemap_for_each(page, &fileh->pagemap) {
        page_drop_memory(page);
        list_del(&page->lru);
        bzero(page, sizeof(*page)); /* just in case */
        free(page);
    }

    /* and clear pagemap */
    pagemap_clear(&fileh->pagemap);

    if (fileh->ramh)
        ramh_close(fileh->ramh);

    bzero(fileh, sizeof(*fileh));
}



/****************
 * MMAP / UNMAP *
 ****************/

// TODO block SIGSEGV
int fileh_mmap(VMA *vma, BigFileH *fileh, pgoff_t pgoffset, pgoff_t pglen)
{
    void *addr;
    size_t len = pglen * fileh->ramh->ram->pagesize;

    /* alloc vma->page_ismappedv[] */
    bzero(vma, sizeof(*vma));

    vma->page_ismappedv = bitmap_alloc0(pglen);
    if (!vma->page_ismappedv)
        goto err;

    /* allocate address space somewhere */
    addr = mem_valloc(NULL, len);
    if (!addr)
        goto err;

    /* everything allocated - link it up */
    vma->addr_start  = (uintptr_t)addr;
    vma->addr_stop   = vma->addr_start + len;

    vma->fileh       = fileh;
    vma->f_pgoffset  = pgoffset;

    // XXX locking - linking up vs concurrent traversal

    // XXX need to init vma->virt_list first?
    /* hook vma to fileh->mmaps */
    list_add_tail(&vma->same_fileh, &fileh->mmaps);

    /* register vma for pagefault handling */
    virt_register_vma(vma);

    return 0;

err:
    free(vma->page_ismappedv);
    vma->page_ismappedv = NULL;
    return -1;
}


// TODO block SIGSEGV
void vma_unmap(VMA *vma)
{
    BigFileH *fileh = vma->fileh;
    size_t   len    = vma->addr_stop - vma->addr_start;
    size_t   pglen  = len / fileh->ramh->ram->pagesize;
    int i;
    pgoff_t  pgoffset;
    Page *page;

    // XXX locking vs concurrent access

    /* unregister from vmamap - so that pagefault handler does not recognize
     * this area as valid */
    virt_unregister_vma(vma);

    /* unlink from fileh.mmaps   XXX place ok ? */
    list_del_init(&vma->same_fileh);

    /* unmap whole vma at once - the kernel unmaps each mapping in turn.
     * NOTE error here would mean something is broken */
    xmunmap((void *)vma->addr_start, len);

    /* scan through mapped-to-this-vma pages and release them */
    for (i=0; i < pglen; ++i) {
        if (!bitmap_test_bit(vma->page_ismappedv, i))
            continue;

        pgoffset = vma->f_pgoffset + i;
        page = pagemap_get(&fileh->pagemap, pgoffset);
        BUG_ON(!page);
        page_decref(page);
    }

    /* free memory and be done */
    free(vma->page_ismappedv);

    bzero(vma, sizeof(*vma));
}


/**********************
 * WRITEOUT / DISCARD *
 **********************/

// XXX vs concurrent access in other threads
int fileh_dirty_writeout(BigFileH *fileh, enum WriteoutFlags flags)
{
    Page *page;
    BigFile *file = fileh->file;
    struct list_head *hmmap;
    sigset_t mask_segv, save_sigset;
    int err = 0;

    /* check flags */
    if (!(flags &  (WRITEOUT_STORE | WRITEOUT_MARKSTORED))   ||
          flags & ~(WRITEOUT_STORE | WRITEOUT_MARKSTORED))
        return -EINVAL;

    // TODO refactor it out of here
    /* block SIGSEGV - we are not client and should not access any not-mmapped
     * memory -> so on any pagefault we should die, not try to handle it
     *
     * NOTE sigmask is per-thread. There is no race here wrt other threads
     * correctly accessing data.    */
    xsigemptyset(&mask_segv);
    xsigaddset(&mask_segv, SIGSEGV);
    xpthread_sigmask(SIG_BLOCK, &mask_segv, &save_sigset);

    /* write out dirty pages */
    pagemap_for_each(page, &fileh->pagemap) {
        /* XXX we scan whole file pages which could be slow
         * TODO -> maintain something like separate dirty_list ? */
        if (page->state != PAGE_DIRTY)
            continue;

        /* ->storeblk() */
        if (flags & WRITEOUT_STORE) {
            TODO (file->blksize != page_size(page));
            blk_t blk = page->f_pgoffset;   // NOTE assumes blksize = pagesize

            void *pagebuf;
            int   mapped_tmp = 0;

            if (!page->refcnt) {
                /* page not mmaped anywhere - mmap it temporarily somewhere */
                pagebuf = page_mmap(page, NULL, PROT_READ);
                TODO(!pagebuf); // XXX err
                mapped_tmp = 1;
            }

            else {
                /* some vma mmaps page - use that memory directly */

                /* XXX this assumes there is small #vma and is ugly - in general it
                 * should be simpler via back-pointers from page?   */
                pagebuf = NULL;
                list_for_each(hmmap, &fileh->mmaps) {
                    VMA *vma = list_entry(hmmap, typeof(*vma), same_fileh);
                    if (vma_page_ismapped(vma, page)) {
                        pagebuf = vma_page_addr(vma, page);
                        break;
                    }
                }
                BUG_ON(!pagebuf);
            }

            err = file->file_ops->storeblk(file, blk, pagebuf);

            if (mapped_tmp)
                xmunmap(pagebuf, page_size(page));

            if (err)
                goto out;
        }

        /* page.state -> PAGE_LOADED and correct mappings RW -> R */
        if (flags & WRITEOUT_MARKSTORED) {
            page->state = PAGE_LOADED;

            list_for_each(hmmap, &fileh->mmaps) {
                VMA *vma = list_entry(hmmap, typeof(*vma), same_fileh);
                vma_page_ensure_notmappedrw(vma, page);
            }
        }
    }


    if (flags & WRITEOUT_MARKSTORED)
        fileh->dirty = 0;

out:
    xpthread_sigmask(SIG_SETMASK, &save_sigset, NULL);
    return err;
}


// TODO block SIGSEGV
// XXX vs concurrent access in other threads
void fileh_dirty_discard(BigFileH *fileh)
{
    Page *page;

    /* XXX we scan whole file pages which could be slow
     * TODO -> maintain something like separate dirty_list ? */
    pagemap_for_each(page, &fileh->pagemap)
        if (page->state == PAGE_DIRTY)
            page_drop_memory(page);

    fileh->dirty = 0;
}


/************************
 *  Lookup VMA by addr  *
 ************************/

/* list of all registered VMA(s) */
static LIST_HEAD(vma_list);

/* protects ^^^  XXX */
//spinlock_t vma_list_lock;



/* lookup VMA covering `addr`. NULL if not found */
// XXX protection against concurrent vma_list updates & lookups
// XXX virt_lookup_vma() operates without taking locks - XXX no -> we'll use spinlock
//     (we don't know whether
//     address is ours while calling it) - so it must operate correctly in
//     lock-free. Updates to vma_list should thus be also done carefully.
VMA *virt_lookup_vma(void *addr)
{
    uintptr_t uaddr = (uintptr_t)addr;
    struct list_head *h;
    VMA *vma;

    list_for_each(h, &vma_list) {
        // XXX -> list_for_each_entry
        vma = list_entry(h, typeof(*vma), virt_list);
        if (uaddr < vma->addr_stop)
            /*
             * here:  vma->addr_start ? uaddr < vma->addr_stop
             * vma->addr_stop is first such addr_stop
             */
            return (vma->addr_start <= uaddr) ? vma : NULL;
    }

    return NULL;    /* not found at all or no overlap */
}


/* register VMA `vma` as covering some file view */
// XXX protection against concurrent updates & lookups
void virt_register_vma(VMA *vma)
{
    uintptr_t uaddr = vma->addr_start;
    struct list_head *h;
    struct VMA *a;

    list_for_each(h, &vma_list) {
        a = list_entry(h, typeof(*a), virt_list);
        if (uaddr < a->addr_stop)
            break;
    }

    /* either before found vma or, if not found, at the end of the list */
    list_add_tail(&vma->virt_list, h);
}


/* remove `area` from VMA registry. `area` must be registered before */
// XXX protection against concurrent updates & lookups
void virt_unregister_vma(VMA *vma)
{
    /* _init - to clear links, just in case */
    list_del_init(&vma->virt_list);
}


/*****************************************/

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


/*********************
 * PAGEFAULT HANDLER *
 *********************/

/* pagefault entry when we know request came to our memory area */
void vma_on_pagefault(VMA *vma, uintptr_t addr, int write)
{
    pgoff_t pagen;
    Page *page;
    BigFileH *fileh;

    /* continuing on_pagefault() - see (1) there ... */

    /* (2) vma, addr -> fileh, pagen    ;idx of fileh page covering addr */
    fileh = vma->fileh;
    pagen = vma_addr_fpgoffset(vma, addr);

    /* (3) fileh, pagen -> page  (via pagemap) */
    page = pagemap_get(&fileh->pagemap, pagen);

    /* (4) no page found - allocate new from ram */
    while (!page) {
        page = ramh_alloc_page(fileh->ramh, pagen);
        if (!page) {
            /* try to release some memory back to OS */
            // XXX do we need and how to distinguish "no ram page" vs "no memory for `struct page`"?
            //     -> no we don't -- better allocate memory for struct pages for whole RAM at ram setup
            if (!ram_reclaim(fileh->ramh->ram))
                OOM();
            continue;
        }

        /* ramh set up .ramh, .ramh_pgoffset, .state?
         * now setup rest (link to fileh)  */
        page->fileh      = fileh;
        page->f_pgoffset = pagen;

        /* remember page in fileh->pagemap[pagen] */
        pagemap_set(&fileh->pagemap, pagen, page);
    }

    /* (5) if page was not yet loaded - load it */
    // XXX protect from concurrent loading of the same page (should be ok with mutex)
    if (page->state < PAGE_LOADED) {
        /* NOTE if we load data in-place, there would be a race with concurrent
         * access to the page here - after first enabling memory-access to
         * the page, other threads could end up reading corrupt data, while
         * loading had not finished.
         *
         * so to avoid it we first load data to separate memory address, then
         * mmap-duplicate that page into here, but it is more work compared to
         * what kernel internally does.
         *
         * TODO try to use remap_anon_pages() when it is ready
         *      (but unfortunately it is only for anonymous memory)
         * NOTE remap_file_pages() is going away...
         */
        blk_t blk;
        void *pageram;
        int err;

        /*
         * if pagesize < blksize - need to prepare several adjacent pages for blk;
         * if pagesize > blksize - will need to either 1) rescan which blk got
         *    dirty, or 2) store not-even-touched blocks adjacent to modified one.
         */
        TODO (fileh->file->blksize != page_size(page));

        // FIXME doing this mmap-to-temp/unmap is somewhat costly. Better
        // constantly have whole RAM mapping somewhere R/W and load there.
        // (XXX but then we'll either have
        //    - VMA fragmented  (if we manage whole RAM as 1 file of physram size),
        //    - or need to waste a lot of address space (size of each ramh can be very large)
        //
        //    generally this way it also has major problems)
        //
        // Also this way, we btw don't need to require python code to drop all
        // references to loading buf.

        /* mmap page memory temporarily somewhere
         * XXX better pre-map all ram pages r/w in another area to not need to mmap/unmap it here
         *     -> will run slightly faster (but major slowdown is in clear_page in kernel)
         */
        // TODO MAP_UNINITIALIZED somehow? (we'll overwrite that memory)
        pageram = page_mmap(page, NULL, PROT_READ | PROT_WRITE);
        TODO(!pageram); // XXX err

        /* loadblk() -> pageram memory */
        // XXX locking, vs gil?
        blk = page->f_pgoffset;     // NOTE because blksize = pagesize
        err = fileh->file->file_ops->loadblk(fileh->file, blk, pageram);
        /* TODO on error -> try to throw exception somehow to the caller, so
         *      that it can abort current transaction, but not die.
         *
         * NOTE for analogue situation when read for mmaped file fails, the
         *      kernel sends SIGBUS
         */
        TODO (err);

        xmunmap(pageram, page_size(page));

        page->state = PAGE_LOADED;
    }

    /* (6) page data ready. Mmap it atomically into vma address space, or mprotect
     * appropriately if it was already mmaped. */
    int prot = PROT_READ;
    PageState newstate = PAGE_LOADED;
    if (write || page->state == PAGE_DIRTY) {
        prot |= PROT_WRITE;
        newstate = PAGE_DIRTY;
    }

    if (!bitmap_test_bit(vma->page_ismappedv, page->f_pgoffset - vma->f_pgoffset)) {
        // XXX err
        page_mmap(page, vma_page_addr(vma, page), prot);
        bitmap_set_bit(vma->page_ismappedv, page->f_pgoffset - vma->f_pgoffset);
        page_incref(page);
    }
    else {
        /* just changing protection bits should not fail, if parameters ok */
        xmprotect(vma_page_addr(vma, page), page_size(page), prot);
    }

    // XXX also call page->markdirty() ?
    page->state = max(page->state, newstate);
    if (page->state == PAGE_DIRTY)
        fileh->dirty = 1;

    /* mark page as used recently */
    // XXX = list_move_tail()
    list_del(&page->lru);
    list_add_tail(&page->lru, &page->ramh->ram->lru_list);

    /*
     * (7) access to page prepared - now it is ok to return from signal handler
     *     - the caller will re-try executing faulting instruction.
     */
    return;
}


/***********
 * RECLAIM *
 ***********/

#define RECLAIM_BATCH   64      /* how many pages to reclaim at once */
int ram_reclaim(RAM *ram)
{
    struct list_head *lru_list = &ram->lru_list;
    struct list_head *hlru;
    Page *page;
    int batch = RECLAIM_BATCH, scanned = 0;

    TRACE("RAM_RECLAIM\n");
    hlru = lru_list->next;

    while (batch && hlru != lru_list) {
        page = list_entry(hlru, typeof(*page), lru);
        hlru = hlru->next;
        scanned++;

        /* can release ram only from loaded non-dirty pages */
        if (page->state == PAGE_LOADED) {
            page_drop_memory(page);
            batch--;
        }

        /* PAGE_EMPTY pages without mappers go away */
        if (page->state == PAGE_EMPTY) {
            BUG_ON(page->refcnt != 0);  // XXX what for then we have refcnt? -> vs discard

            /* delete page & its entry in fileh->pagemap */
            pagemap_del(&page->fileh->pagemap, page->f_pgoffset);
            list_del(&page->lru);
            bzero(page, sizeof(*page)); /* just in case */
            free(page);
        }

    }

    TRACE("\t-> reclaimed %i  scanned %i\n", RECLAIM_BATCH - batch, scanned);
    return RECLAIM_BATCH - batch;
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


static void page_drop_memory(Page *page)
{
    /* Memory for this page goes out. 1) unmap it from all mmaps */
    struct list_head *hmmap;

    if (page->state == PAGE_EMPTY)
        return;

    list_for_each(hmmap, &page->fileh->mmaps) {
        VMA *vma = list_entry(hmmap, typeof(*vma), same_fileh);
        vma_page_ensure_unmapped(vma, page);
    }

    /* 2) release memory to ram */
    ramh_drop_memory(page->ramh, page->ramh_pgoffset);
    page->state = PAGE_EMPTY;

    // XXX touch lru?
}



/* vma: page -> addr  where it should-be mmaped in vma */
static void *vma_page_addr(VMA *vma, Page *page)
{
    uintptr_t addr;
    ASSERT(vma->fileh == page->fileh);      // XXX needed here?

    addr = vma->addr_start + (page->f_pgoffset - vma->f_pgoffset) * page_size(page);
    ASSERT(vma->addr_start <= addr  &&
                              addr < vma->addr_stop);
    return (void *)addr;
}


/* vma: addr -> fileh pgoffset  with page containing addr */
static pgoff_t vma_addr_fpgoffset(VMA *vma, uintptr_t addr)
{
    return vma->f_pgoffset + (addr - vma->addr_start) / vma->fileh->ramh->ram->pagesize;
}



/* is `page` mapped to `vma` */
static int vma_page_ismapped(VMA *vma, Page *page)
{
    pgoff_t vma_fpgstop;
    ASSERT(vma->fileh == page->fileh);

    vma_fpgstop = vma_addr_fpgoffset(vma, vma->addr_stop);
    if (!(vma->f_pgoffset <= page->f_pgoffset  &&
                             page->f_pgoffset < vma_fpgstop))
        return 0;

    return bitmap_test_bit(vma->page_ismappedv, page->f_pgoffset - vma->f_pgoffset);
}


/* ensure `page` is not mapped to `vma` */
static void vma_page_ensure_unmapped(VMA *vma, Page *page)
{
    if (!vma_page_ismapped(vma, page))
        return;

    /* mmap empty PROT_NONE address space instead of page memory */
    mem_xvalloc(vma_page_addr(vma, page), page_size(page));

    bitmap_clear_bit(vma->page_ismappedv, page->f_pgoffset - vma->f_pgoffset);
    page_decref(page);
}


/* ensure `page` is not mapped RW to `vma`
 *
 * if mapped -> should be mapped as R
 * if not mapped - leave as is
 */
static void vma_page_ensure_notmappedrw(VMA *vma, Page *page)
{
    if (!vma_page_ismapped(vma, page))
        return;

    /* just changing protection - should not fail */
    // XXX PROT_READ always? (it could be mmaped with PROT_NONE before without
    // first access) - then it should not be mapped in page_ismappedv -> ok.
    xmprotect(vma_page_addr(vma, page), page_size(page), PROT_READ);
}


// XXX stub
void OOM(void)
{
    BUG();
}
