/* Wendelin.bigfile | Virtual memory
 * Copyright (C) 2014-2025  Nexedi SA and Contributors.
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
 *
 * TODO description
 */

#include <wendelin/bigfile/virtmem.h>
#include <wendelin/bigfile/file.h>
#include <wendelin/bigfile/pagemap.h>
#include <wendelin/bigfile/ram.h>
#include <wendelin/utils.h>
#include <wendelin/bug.h>

#include <ccan/minmax/minmax.h>
#include <ccan/bitmap/bitmap.h>

#include <sys/mman.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>

static size_t   page_size(const Page *page);
static void     page_drop_memory(Page *page);
static void     page_del(Page *page);
static void    *vma_page_addr(VMA *vma, Page *page);
static pgoff_t  vma_addr_fpgoffset(VMA *vma, uintptr_t addr);
static void     vma_mmap_page(VMA *vma, Page *page);
static int      vma_page_infilerange(VMA *vma, Page *page);
static int      vma_page_ismapped(VMA *vma, Page *page);
static void     vma_page_ensure_unmapped(VMA *vma, Page *page);
static void     vma_page_ensure_notmappedrw(VMA *vma, Page *page);
static int      __ram_reclaim(RAM *ram);

#define VIRT_DEBUG   0
#if VIRT_DEBUG
# define TRACE(msg, ...) do { fprintf(stderr, msg, ## __VA_ARGS__); } while (0)
#else
# define TRACE(msg, ...) do {} while(0)
#endif


/* global lock which protects manipulating virtmem data structures
 *
 * NOTE not scalable. */
static pthread_mutex_t virtmem_lock = PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP;
static const VirtGilHooks *virtmem_gilhooks;

void *virt_gil_ensure_unlocked(void)
{
    void *gilstate = NULL;

    if (virtmem_gilhooks)
        gilstate = virtmem_gilhooks->gil_ensure_unlocked();

    return gilstate;
}


void virt_gil_retake_if_waslocked(void *gilstate)
{
    if (gilstate)
        virtmem_gilhooks->gil_retake_if_waslocked(gilstate);
}

void virt_lock()
{
    void *gilstate = NULL;

    /* make sure we don't hold e.g. python GIL (not to deadlock, as GIL oscillates) */
    gilstate = virt_gil_ensure_unlocked();

    /* acquire virtmem lock */
    xpthread_mutex_lock(&virtmem_lock);

    /* retake GIL if we were holding it originally */
    virt_gil_retake_if_waslocked(gilstate);
}

void virt_unlock()
{
    xpthread_mutex_unlock(&virtmem_lock);
}


void virt_lock_hookgil(const VirtGilHooks *gilhooks)
{
    BUG_ON(virtmem_gilhooks);       /* prevent registering multiple times */
    virtmem_gilhooks = gilhooks;
}


/* block/restore SIGSEGV for current thread - non on-pagefault code should not
 * access any not-mmapped memory -> so on any pagefault we should just die with
 * coredump, not try to incorrectly handle the pagefault.
 *
 * NOTE sigmask is per-thread. When blocking there is no race wrt other threads
 * correctly accessing data via pagefaulting.   */
static void sigsegv_block(sigset_t *save_sigset)
{
    sigset_t mask_segv;
    xsigemptyset(&mask_segv);
    xsigaddset(&mask_segv, SIGSEGV);
    xpthread_sigmask(SIG_BLOCK, &mask_segv, save_sigset);
}

static void sigsegv_restore(const sigset_t *save_sigset)
{
    int how = xsigismember(save_sigset, SIGSEGV) ? SIG_BLOCK : SIG_UNBLOCK;
    sigset_t mask_segv;

    xsigemptyset(&mask_segv);
    xsigaddset(&mask_segv, SIGSEGV);
    xpthread_sigmask(how, &mask_segv, NULL);
}


/****************
 * OPEN / CLOSE *
 ****************/

int fileh_open(BigFileH *fileh, BigFile *file, RAM *ram, FileHOpenFlags flags)
{
    int err = 0;
    sigset_t save_sigset;
    const bigfile_ops *fops = file->file_ops;

    if (!(flags == 0 || flags == MMAP_OVERLAY || flags == DONT_MMAP_OVERLAY))
        return -EINVAL;
    if (flags == 0)
        flags = fops->mmap_setup_read ? MMAP_OVERLAY : DONT_MMAP_OVERLAY;
    if (flags & MMAP_OVERLAY && flags & DONT_MMAP_OVERLAY)
        return -EINVAL;
    if (flags == MMAP_OVERLAY) {
        ASSERT(fops->mmap_setup_read);
        ASSERT(fops->remmap_blk_read);
        ASSERT(fops->munmap);
    }
    if (flags == DONT_MMAP_OVERLAY)
        ASSERT(fops->loadblk);

    sigsegv_block(&save_sigset);
    virt_lock();

    bzero(fileh, sizeof(*fileh));
    fileh->ramh = ramh_open(ram);
    if (!fileh->ramh) {
        err = -1;
        goto out;
    }

    fileh->file = file;
    INIT_LIST_HEAD(&fileh->mmaps);
    INIT_LIST_HEAD(&fileh->dirty_pages);
    fileh->writeout_inprogress = 0;
    pagemap_init(&fileh->pagemap, ilog2_exact(ram->pagesize));

    fileh->mmap_overlay = (flags == MMAP_OVERLAY);

out:
    virt_unlock();
    sigsegv_restore(&save_sigset);
    return err;
}


void fileh_close(BigFileH *fileh)
{
    Page *page;
    sigset_t save_sigset;

    sigsegv_block(&save_sigset);
    virt_lock();

    /* it's an error to close fileh with existing mappings */
    // XXX implement the same semantics usual files have wrt mmaps - if we release
    // fileh, but mapping exists - real fileh release is delayed to last unmap ?
    BUG_ON(!list_empty(&fileh->mmaps));

    /* it's an error to close fileh while writeout is in progress */
    BUG_ON(fileh->writeout_inprogress);

    /* drop all pages (dirty or not) associated with this fileh */
    pagemap_for_each(page, &fileh->pagemap) {
        /* it's an error to close fileh to mapping of which an access is
         * currently being done in another thread */
        BUG_ON(page->state == PAGE_LOADING);
        page_drop_memory(page);
        page_del(page);
    }

    BUG_ON(!list_empty(&fileh->dirty_pages));

    /* and clear pagemap */
    pagemap_clear(&fileh->pagemap);

    if (fileh->ramh)
        ramh_close(fileh->ramh);

    bzero(fileh, sizeof(*fileh));
    virt_unlock();
    sigsegv_restore(&save_sigset);
}

/****************
 * HELPER       *
 ****************/

static Page *fileh_create_destroyed_page(BigFileH *fileh, pgoff_t pgoffset)
{
    Page *page = NULL;
    void *pageram;

    /* Retry page allocation until successful or OOM */
    while (!(page = ramh_alloc_page(fileh->ramh, pgoffset))) {
        if (!__ram_reclaim(fileh->ramh->ram)) {
            OOM();
        }
    }

    /* Initialize basic page fields */
    page->fileh = fileh;
    page->f_pgoffset = pgoffset;
    page->state = PAGE_DESTROYED;

    /* Allocate zero-initialized memory for the page */
    pageram = page_mmap(page, NULL, PROT_READ | PROT_WRITE);
    TODO(!pageram);  // Replace with real error handling if needed

    memset(pageram, 0, page_size(page));
    xmunmap(pageram, page_size(page));

    /* Insert into pagemap */
    pagemap_set(&fileh->pagemap, pgoffset, page);

    return page;
}

/****************
 * MMAP / UNMAP *
 ****************/

int fileh_mmap(VMA *vma, BigFileH *fileh, pgoff_t pgoffset, pgoff_t pglen)
{
    size_t len = pglen * fileh->ramh->ram->pagesize;
    BigFile *file = fileh->file;
    const bigfile_ops *fops = file->file_ops;
    int err = 0;
    sigset_t save_sigset;

    sigsegv_block(&save_sigset);
    virt_lock();

    /* start preparing vma */
    bzero(vma, sizeof(*vma));
    vma->fileh       = fileh;
    vma->f_pgoffset  = pgoffset;

    /* alloc vma->page_ismappedv[] */
    vma->page_ismappedv = bitmap_alloc0(pglen);
    if (!vma->page_ismappedv)
        goto fail;

    if (fileh->mmap_overlay) {
        /* wcfs: mmap(base, READ)
         * vma->addr_{start,stop} are initialized by mmap_setup_read */
        TODO (file->blksize != fileh->ramh->ram->pagesize);
        err = fops->mmap_setup_read(vma, file, pgoffset, pglen);
        if (err)
            goto fail;
    } else {
        /* !wcfs: allocate address space somewhere */
        void *addr = mem_valloc(NULL, len);
        if (!addr)
            goto fail;
        /* vma address range known */
        vma->addr_start  = (uintptr_t)addr;
        vma->addr_stop   = vma->addr_start + len;
    }

    /* wcfs: mmap(fileh->dirty_pages) over base */
    if (fileh->mmap_overlay) {
        Page* page;
        struct list_head *hpage;

        list_for_each(hpage, &fileh->dirty_pages) {
            page = list_entry(hpage, typeof(*page), in_dirty);
            BUG_ON(page->state != PAGE_DIRTY);

            if (!vma_page_infilerange(vma, page))
                continue; /* page is out of requested mmap coverage */

            vma_mmap_page(vma, page);
        }
    }

    /* wcfs: mmap(fileh->destroyed pages) over base */
    if (fileh->mmap_overlay && fileh->has_destroyed_range) {
        pgoff_t range_end = pgoffset + pglen;

        for (pgoff_t i = pgoffset; i < range_end; i++) {
            if (i < fileh->destroyed_from_page)
                continue;

            Page *page = pagemap_get(&fileh->pagemap, i);

            if (page) {
                if (page->state == PAGE_DESTROYED) {
                    /* Re-map existing destroyed page to current VMA */
                    vma_mmap_page(vma, page);
                } else {
                    /* Must be a dirty page if not destroyed */
                    BUG_ON(list_empty(&page->in_dirty));
                }
            } else {
                page = fileh_create_destroyed_page(fileh, i);
                vma_mmap_page(vma, page);
            }
        }
    }

    /* everything allocated - link it up */

    // XXX need to init vma->virt_list first?
    /* hook vma to fileh->mmaps */
    list_add_tail(&vma->same_fileh, &fileh->mmaps);

    /* register vma for pagefault handling */
    virt_register_vma(vma);

out:
    virt_unlock();
    sigsegv_restore(&save_sigset);
    return err;

fail:
    free(vma->page_ismappedv);
    bzero(vma, sizeof(*vma));
    err = -1;
    goto out;
}


void vma_unmap(VMA *vma)
{
    BigFileH *fileh = vma->fileh;
    size_t   len    = vma->addr_stop - vma->addr_start;
    size_t   pglen  = len / fileh->ramh->ram->pagesize;
    int i;
    pgoff_t  pgoffset;
    Page *page;
    sigset_t save_sigset;

    sigsegv_block(&save_sigset);
    virt_lock();

    /* unregister from vmamap - so that pagefault handler does not recognize
     * this area as valid */
    virt_unregister_vma(vma);

    /* unlink from fileh.mmaps   XXX place ok ? */
    list_del_init(&vma->same_fileh);

    /* unmap whole vma at once - the kernel unmaps each mapping in turn.
     * NOTE error here would mean something is broken */
    if (fileh->mmap_overlay) {
        int err = fileh->file->file_ops->munmap(vma, fileh->file);
        BUG_ON(err);
    } else {
        xmunmap((void *)vma->addr_start, len);
    }

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
    virt_unlock();
    sigsegv_restore(&save_sigset);
}


/**********************
 * WRITEOUT / DISCARD *
 **********************/

/* helper for sorting dirty pages by ->f_pgoffset */
static int hpage_indirty_cmp_bypgoffset(struct list_head *hpage1, struct list_head *hpage2, void *_)
{
    Page *page1 = list_entry(hpage1, typeof(*page1), in_dirty);
    Page *page2 = list_entry(hpage2, typeof(*page2), in_dirty);

    if (page1->f_pgoffset < page2->f_pgoffset)
        return -1;
    if (page1->f_pgoffset > page2->f_pgoffset)
        return +1;
    return 0;
}

int fileh_dirty_writeout(BigFileH *fileh, enum WriteoutFlags flags)
{
    Page *page;
    BigFile *file = fileh->file;
    struct list_head *hpage, *hpage_next, *hmmap;
    sigset_t save_sigset;
    int err = 0;

    /* check flags */
    if (!(flags &  (WRITEOUT_STORE | WRITEOUT_MARKSTORED))   ||
          flags & ~(WRITEOUT_STORE | WRITEOUT_MARKSTORED))
        return -EINVAL;

    sigsegv_block(&save_sigset);
    virt_lock();

    /* concurrent writeouts are not allowed */
    BUG_ON(fileh->writeout_inprogress);
    fileh->writeout_inprogress = 1;

    /* pages are stored (if stored) in sorted order
     * NOTE writeout of ZBlk format 'auto' relies on this */
    if (flags & WRITEOUT_STORE)
        list_sort(&fileh->dirty_pages, hpage_indirty_cmp_bypgoffset, NULL);

    /* write out dirty pages */
    list_for_each_safe(hpage, hpage_next, &fileh->dirty_pages) {
        page = list_entry(hpage, typeof(*page), in_dirty);
        BUG_ON(page->state != PAGE_DIRTY);

        /* ->storeblk() */
        if (flags & WRITEOUT_STORE) {
            TODO (file->blksize != page_size(page));
            blk_t blk = page->f_pgoffset;   // NOTE assumes blksize = pagesize

            void *pagebuf;

            /* mmap page temporarily somewhere
             *
             * ( we cannot use present page mapping in some vma directly,
             *   because while storeblk is called with virtmem lock released that
             *   mapping can go away ) */
            pagebuf = page_mmap(page, NULL, PROT_READ);
            TODO(!pagebuf); // XXX err

            /* unlock virtmem before calling storeblk()
             *
             * that call is potentially slow and external code can take other
             * locks. If that "other locks" are also taken before external code
             * calls e.g. fileh_invalidate_page() in different codepath a deadlock
             * can happen. (similar to loadblk case) */
            virt_unlock();

            err = file->file_ops->storeblk(file, blk, pagebuf);

            /* relock virtmem */
            virt_lock();

            xmunmap(pagebuf, page_size(page));

            if (err)
                goto out;
        }

        /* wcfs:  remmap RW pages to base layer
         * !wcfs: page.state -> PAGE_LOADED and correct mappings RW -> R
         *
         * NOTE for transactional storage (ZODB and ZBigFile) storeblk creates
         * new transaction on database side, but does not update current DB
         * connection to view that transaction. Thus if loadblk will be loaded
         * with not-yet-resynced DB connection, it will return old - not stored
         * - data. For !wcfs case this is partly mitigated by the fact that
         * stored pages are kept as PAGE_LOADED in ram, but it cannot be
         * relied as ram_reclaim can drop those pages and read access to them
         * will trigger loadblk from database which will return old data.
         * For wcfs case remapping to base layer will always return old data
         * until wcfs mapping is updated to view database at newer state.
         *
         * In general it is a bug to access data pages in between transactions,
         * so we accept those corner case difference in between wcfs and !wcfs.
         */
        if (flags & WRITEOUT_MARKSTORED) {
            page->state = PAGE_LOADED;
            list_del_init(&page->in_dirty);

            list_for_each(hmmap, &fileh->mmaps) {
                VMA *vma = list_entry(hmmap, typeof(*vma), same_fileh);
                if (fileh->mmap_overlay) {
                    /* wcfs:  RW -> base layer */
                    vma_page_ensure_unmapped(vma, page);
                } else {
                    /* !wcfs: RW -> R*/
                    vma_page_ensure_notmappedrw(vma, page);
                }
            }

            /* wcfs:  all vmas are using base layer now - drop page completely
             *        without unnecessarily growing RSS and relying on reclaim.
             * !wcfs: keep the page in RAM cache, even if it is not mapped anywhere */
            if (fileh->mmap_overlay) {
                ASSERT(page->refcnt == 0);
                pagemap_del(&fileh->pagemap, page->f_pgoffset);
                page_drop_memory(page);
                page_del(page);
            }
        }
    }


    /* if we successfully finished with markstored flag set - all dirty pages
     * should become non-dirty */
    if (flags & WRITEOUT_MARKSTORED)
        BUG_ON(!list_empty(&fileh->dirty_pages));

out:
    fileh->writeout_inprogress = 0;

    virt_unlock();
    sigsegv_restore(&save_sigset);
    return err;
}


void fileh_dirty_discard(BigFileH *fileh)
{
    Page *page;
    struct list_head *hpage, *hpage_next;
    sigset_t save_sigset;

    sigsegv_block(&save_sigset);
    virt_lock();

    /* discard is not allowed to run in parallel to writeout */
    BUG_ON(fileh->writeout_inprogress);

    list_for_each_safe(hpage, hpage_next, &fileh->dirty_pages) {
        page = list_entry(hpage, typeof(*page), in_dirty);
        BUG_ON(page->state != PAGE_DIRTY);

        page_drop_memory(page);
        // TODO consider doing pagemap_del + page_del unconditionally
        if (fileh->mmap_overlay) {
            pagemap_del(&fileh->pagemap, page->f_pgoffset);
            page_del(page);
        }
    }

    BUG_ON(!list_empty(&fileh->dirty_pages));

    virt_unlock();
    sigsegv_restore(&save_sigset);
}


/****************
 * INVALIDATION *
 ****************/

void fileh_invalidate_page(BigFileH *fileh, pgoff_t pgoffset)
{
    Page *page;
    sigset_t save_sigset;

    sigsegv_block(&save_sigset);
    virt_lock();

    /* it's an error to invalidate fileh while writeout is in progress */
    BUG_ON(fileh->writeout_inprogress);

    /* wcfs: even though the operation to invalidate a page is well defined (it
     * is subset of discard), we forbid it since wcfs handles invalidations
     * from ZODB by itself inside wcfs server.
     *
     * It was kind of mistake to expose in 92bfd03e (bigfile: ZODB -> BigFileH
     * invalidate propagation) fileh_invalidate_page as public API, since such
     * invalidation should be handled by a BigFile instance internally. */
    BUG_ON(fileh->mmap_overlay);

    page = pagemap_get(&fileh->pagemap, pgoffset);
    if (page) {
        /* for pages where loading is in progress, we just remove the page from
         * pagemap and mark it to be dropped by their loaders after it is done.
         * In the mean time, as pagemap entry is now empty, on next access to
         * the memory the page will be created/loaded anew */
        if (page->state == PAGE_LOADING) {
            pagemap_del(&fileh->pagemap, pgoffset);
            page->state = PAGE_LOADING_INVALIDATED;
        }
        /* else we just make sure to drop page memory */
        else {
            page_drop_memory(page);
        }
    }

    virt_unlock();
    sigsegv_restore(&save_sigset);
}

/***************************
 * DISCARD / RESTORE PAGES *
 **************************/

void fileh_discard_pages(BigFileH *fileh, pgoff_t discard_from_page)
{
    Page *page;
    sigset_t save_sigset;
    struct list_head *hmmap;
    PageMap *pagemap = &fileh->pagemap;
    pgoff_t pgoff;
    pgoff_t max_pgoff = 0;

    sigsegv_block(&save_sigset);
    virt_lock();

    /* It's an error to discard pages while writeout is in progress */
    BUG_ON(fileh->writeout_inprogress);

    if (!(fileh->has_destroyed_range && fileh->destroyed_from_page < discard_from_page))
        fileh->destroyed_from_page = discard_from_page;
    fileh->has_destroyed_range = true;

    /* Find maximum page offset across all VMAs */
    list_for_each(hmmap, &fileh->mmaps) {
        VMA *vma = list_entry(hmmap, typeof(*vma), same_fileh);
        pgoff_t vma_stop = vma_addr_fpgoffset(vma, vma->addr_stop);
        if (vma_stop > max_pgoff)
            max_pgoff = vma_stop;
    }

    for (pgoff = discard_from_page; pgoff < max_pgoff; pgoff++) {
        page = pagemap_get(pagemap, pgoff);

        if (page) {
            /* Existing page - assert it's not dirty and destroy */
            BUG_ON(!list_empty(&page->in_dirty));
            page_drop_memory(page);
            page_del(page);
            pagemap_del(pagemap, pgoff);
        }

        /* Create new destroyed page */
        page = fileh_create_destroyed_page(fileh, pgoff);

        /* Map the destroyed page into all VMAs covering its range */
        list_for_each(hmmap, &fileh->mmaps) {
            VMA *vma = list_entry(hmmap, typeof(*vma), same_fileh);
            if (vma_page_infilerange(vma, page))
                vma_mmap_page(vma, page);
        }
    }

    virt_unlock();
    sigsegv_restore(&save_sigset);
}

void fileh_restore_pages(BigFileH *fileh)
{
    Page *page, **pages_to_restore;
    int page_count = 0, i;
    sigset_t save_sigset;

    sigsegv_block(&save_sigset);
    virt_lock();

    BUG_ON(fileh->writeout_inprogress);

    fileh->has_destroyed_range = false;
    fileh->destroyed_from_page = 0;

    // First count how many destroyed pages
    pagemap_for_each(page, &fileh->pagemap) {
        if (page->state == PAGE_DESTROYED)
            page_count++;
    }

    if (page_count == 0)
        goto out_unlock;

    pages_to_restore = malloc(page_count * sizeof(Page *));
    if (!pages_to_restore)
        OOM();

    int idx = 0;
    pagemap_for_each(page, &fileh->pagemap) {
        if (page->state == PAGE_DESTROYED)
            pages_to_restore[idx++] = page;
    }

    for (i = 0; i < page_count; i++) {
        page = pages_to_restore[i];

        struct list_head *hmmap;
        list_for_each(hmmap, &fileh->mmaps) {
            VMA *vma = list_entry(hmmap, typeof(*vma), same_fileh);
            vma_page_ensure_unmapped(vma, page);
        }

        page_drop_memory(page);
        pagemap_del(&fileh->pagemap, page->f_pgoffset);
        page_del(page);
    }

    free(pages_to_restore);

out_unlock:
    virt_unlock();
    sigsegv_restore(&save_sigset);
}


/************************
 *  Lookup VMA by addr  *
 ************************/

/* list of all registered VMA(s) */
static LIST_HEAD(vma_list);

/* protects ^^^  XXX */
//spinlock_t vma_list_lock;



/* lookup VMA covering `addr`. NULL if not found
 * (should be called with virtmem lock held) */
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


/* register VMA `vma` as covering some file view
 * (should be called with virtmem lock held) */
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


/* remove `area` from VMA registry. `area` must be registered before
 * (should be called with virtmem lock held) */
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

/* pagefault entry when we know request came to our memory area
 *
 * (virtmem_lock already taken by caller)   */
VMFaultResult vma_on_pagefault(VMA *vma, uintptr_t addr, int write)
{
    pgoff_t pagen;
    Page *page;
    BigFileH *fileh;
    struct list_head *hmmap;

    /* continuing on_pagefault() - see (1) there ... */

    /* (2) vma, addr -> fileh, pagen    ;idx of fileh page covering addr */
    fileh = vma->fileh;
    pagen = vma_addr_fpgoffset(vma, addr);

    /* wcfs: we should get into SIGSEGV handler only on write access */
    if (fileh->mmap_overlay)
        BUG_ON(!write);

    /* (3) fileh, pagen -> page  (via pagemap) */
    page = pagemap_get(&fileh->pagemap, pagen);

    /* wcfs: all dirty pages are mmapped when vma is created.
     *       thus here, normally, if page is present in pagemap, it can be only either
     *       - a page we just loaded for dirtying, or
     *       - a page that is in progress of being loaded.
     *
     *       however it can be also a *dirty* page due to simultaneous write
     *       access from 2 threads:
     *
     *            T1                                   T2
     *
     *         write pagefault                      write pagefault
     *         virt_lock
     *         page.state = PAGE_LOADING
     *         virt_unlock
     *         # start loading the page
     *         ...
     *         # loading completed
     *         virt_lock
     *         page.state = PAGE_LOADED_FOR_WRITE
     *         virt_unlock
     *         return VM_RETRY
     *                                              virt_lock
     *                                              # sees page.state = PAGE_LOADED_FOR_WRITE
     *                                              page.state = PAGE_DIRTY
     *                                              virt_unlock
     *
     *         # retrying
     *         virt_lock
     *         # sees page.state = PAGE_DIRTY  <--
     *
     *
     * ( PAGE_LOADED_FOR_WRITE is used only to verify that in wcfs mode we
     *   always keep all dirty pages mmapped on fileh_open and so pagefault
     *   handler must not see a PAGE_LOADED page. )
     */
    if (fileh->mmap_overlay && page)
    ASSERT(page->state == PAGE_LOADED_FOR_WRITE || page->state == PAGE_LOADING ||
           page->state == PAGE_DIRTY || page->state == PAGE_DESTROYED);

    /* (4) no page found - allocate new from ram */
    while (!page) {
        page = ramh_alloc_page(fileh->ramh, pagen);
        if (!page) {
            /* try to release some memory back to OS */
            // XXX do we need and how to distinguish "no ram page" vs "no memory for `struct page`"?
            //     -> no we don't -- better allocate memory for struct pages for whole RAM at ram setup
            if (!__ram_reclaim(fileh->ramh->ram))
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

    /* (5a) if page was not yet loaded - start loading it */
    if (page->state == PAGE_EMPTY) {
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
        BigFile *file;

        /*
         * if pagesize < blksize - need to prepare several adjacent pages for blk;
         * if pagesize > blksize - will need to either 1) rescan which blk got
         *    dirty, or 2) store not-even-touched blocks adjacent to modified one.
         */
        file = fileh->file;
        TODO (file->blksize != page_size(page));

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

        /* load block -> pageram memory */
        blk = page->f_pgoffset;     // NOTE because blksize = pagesize

        /* mark page as loading and unlock virtmem before doing actual load via
         * loadblk() or wcfs.
         *
         * both calls are potentially slow and external code can take other
         * locks. If that "other locks" are also taken before external code
         * calls e.g. fileh_invalidate_page() in different codepath a deadlock
         * can happen. (similar to storeblk case) */
        page->state = PAGE_LOADING;
        virt_unlock();

        if (fileh->mmap_overlay) {
            /* wcfs: copy block data from read-only base mmap.
             * NOTE we'll get SIGBUG here if wcfs returns EIO when loading block data */
            memcpy(pageram, vma_page_addr(vma, page), page_size(page));
        }
        else {
            /* !wcfs: call loadblk */
            err = file->file_ops->loadblk(file, blk, pageram);

            /* TODO on error -> try to throw exception somehow to the caller, so
             *      that it can abort current transaction, but not die.
             *
             * NOTE for analogue situation when read for mmaped file fails, the
             *      kernel sends SIGBUS
             */
            TODO (err);
        }

        /* relock virtmem */
        virt_lock();

        xmunmap(pageram, page_size(page));

        /* if the page was invalidated while we were loading it, we have to drop
         * it's memory and the Page structure completely - invalidater already
         * removed it from pagemap */
        if (page->state == PAGE_LOADING_INVALIDATED) {
            page_drop_memory(page);
            page_del(page);
        }

        /* else just mark the page as loaded ok */
        else
            page->state = (write ? PAGE_LOADED_FOR_WRITE : PAGE_LOADED);

        /* we have to retry the whole fault, because the vma could have been
         * changed while we were loading page with virtmem lock released */
        return VM_RETRY;
    }

    /* (5b) page is currently being loaded by another thread - wait for load to complete
     *
     * NOTE a page is protected from being concurrently loaded by two threads at
     * the same time via:
     *
     *   - virtmem lock - we get/put pages from fileh->pagemap only under it
     *   - page->state is set PAGE_LOADING for loading in progress pages
     *   - such page is inserted in fileh->pagepam
     *
     * so if second thread faults at the same memory page, and the page is
     * still loading, it will find the page in PAGE_LOADING state and will just
     * wait for it to complete. */
    if (page->state == PAGE_LOADING) {
        /* XXX polling instead of proper completion */
        void *gilstate;
        virt_unlock();
        gilstate = virt_gil_ensure_unlocked();
        usleep(10000);  // XXX with 1000 usleep still busywaits
        virt_gil_retake_if_waslocked(gilstate);
        virt_lock();
        return VM_RETRY;
    }

    /* (5c) page is destroyed - discard_pages already zero fill - add write */
    if (page->state == PAGE_DESTROYED && write) {
        page->state = PAGE_LOADED_FOR_WRITE;
    }

    /* (6) page data ready. Mmap it atomically into vma address space, or mprotect
     * appropriately if it was already mmaped. */
    PageState newstate = PAGE_LOADED;
    if (write || page->state == PAGE_DIRTY || page->state == PAGE_LOADED_FOR_WRITE) {
        newstate = PAGE_DIRTY;
    }

    // XXX also call page->markdirty() ?
    if (newstate == PAGE_DIRTY  &&  newstate != page->state) {
        /* it is not allowed to modify pages while writeout is in progress */
        BUG_ON(fileh->writeout_inprogress);

        list_add_tail(&page->in_dirty, &fileh->dirty_pages);
    }
    page->state = max(page->state, newstate);

    vma_mmap_page(vma, page);
    /* wcfs: also mmap the page to all wcfs-backed vmas. If we don't, the
     * memory on those vmas will read with stale data */
    if (fileh->mmap_overlay) {
        list_for_each(hmmap, &fileh->mmaps) {
            VMA *vma2 = list_entry(hmmap, typeof(*vma2), same_fileh);
            if (vma2 == vma)
                continue;
            if (!vma_page_infilerange(vma2, page))
                continue; /* page is out of vma2 file-range coverage */

            vma_mmap_page(vma2, page);
        }
    }

    /* mark page as used recently */
    // XXX = list_move_tail()
    list_del(&page->lru);
    list_add_tail(&page->lru, &page->ramh->ram->lru_list);

    /*
     * (7) access to page prepared - now it is ok to return from signal handler
     *     - the caller will re-try executing faulting instruction.
     */
    return VM_HANDLED;
}


/***********
 * RECLAIM *
 ***********/

#define RECLAIM_BATCH   64      /* how many pages to reclaim at once */
static int __ram_reclaim(RAM *ram)
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

        /* can release ram only from loaded non-dirty pages
         * NOTE PAGE_LOADING pages are not dropped - they just continue to load
         *
         * NOTE PAGE_LOADED_FOR_WRITE are dropped too - even if normally they
         * are going to be dirtied in a moment, due to VM_RETRY logic and so
         * VMA might be changing simultaneously to pagefault handling, a
         * page might remain in pagemap in PAGE_LOADED_FOR_WRITE state
         * indefinitely unused and without actually being dirtied.
         *
         * TODO drop PAGE_LOADED_FOR_WRITE only after all PAGE_LOADED have been reclaimed. */
        if (page->state == PAGE_LOADED || page->state == PAGE_LOADED_FOR_WRITE) {
            page_drop_memory(page);
            batch--;
        }

        /* PAGE_EMPTY pages without mappers go away */
        if (page->state == PAGE_EMPTY) {
            BUG_ON(page->refcnt != 0);  // XXX what for then we have refcnt? -> vs discard

            /* delete page & its entry in fileh->pagemap */
            pagemap_del(&page->fileh->pagemap, page->f_pgoffset);
            page_del(page);
        }

    }

    TRACE("\t-> reclaimed %i  scanned %i\n", RECLAIM_BATCH - batch, scanned);
    return RECLAIM_BATCH - batch;
}


int ram_reclaim(RAM *ram)
{
    int ret;
    sigset_t save_sigset;

    sigsegv_block(&save_sigset);
    virt_lock();

    ret = __ram_reclaim(ram);

    virt_unlock();
    sigsegv_restore(&save_sigset);
    return ret;
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
    // XXX better call ramh_mmap_page() without tinkering with ramh_ops?
    return ramh->ramh_ops->mmap_page(ramh, page->ramh_pgoffset, addr, prot);
}


/* page_drop_memory frees RAM associated with page and transitions page state into PAGE_EMPTY.
 *
 * The Page struct itself is not released - see page_del for that. */
static void page_drop_memory(Page *page)
{
    /* Memory for this page goes out. 1) unmap it from all mmaps */
    struct list_head *hmmap;

    /* NOTE we try not to drop memory for loading-in-progress pages.
     *      so if this is called for such a page - it is a bug. */
    BUG_ON(page->state == PAGE_LOADING);
    /* same for storing-in-progress */
    BUG_ON(page->fileh->writeout_inprogress && page->state == PAGE_DIRTY);

    if (page->state == PAGE_EMPTY)
        return;

    list_for_each(hmmap, &page->fileh->mmaps) {
        VMA *vma = list_entry(hmmap, typeof(*vma), same_fileh);
        vma_page_ensure_unmapped(vma, page);
    }

    /* 2) release memory to ram */
    ramh_drop_memory(page->ramh, page->ramh_pgoffset);
    if (page->state == PAGE_DIRTY)
        list_del_init(&page->in_dirty);
    page->state = PAGE_EMPTY;

    // XXX touch lru?
}

/* page_del deletes Page struct (but not page memory - see page_drop_memory).
 *
 * The page must be in PAGE_EMPTY state.
 * The page is removed from ram->lru.
 */
static void page_del(Page *page) {
    BUG_ON(page->refcnt != 0);
    BUG_ON(page->state != PAGE_EMPTY);
    BUG_ON(!list_empty(&page->in_dirty));

    list_del(&page->lru);
    bzero(page, sizeof(*page)); /* just in case */
    free(page);
}



/* vma: page -> addr  where it should-be mmaped in vma */
static void *vma_page_addr(VMA *vma, Page *page)
{
    uintptr_t addr;
    ASSERT(vma->fileh == page->fileh);

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


/* vma_mmap_page mmaps page into vma.
 *
 * the page must belong to covered file.
 * mmap protection is PROT_READ if page is PAGE_LOADED or PROT_READ|PROT_WRITE
 * if page is PAGE_DIRTY.
 *
 * must be called under virtmem lock.
 */
static void vma_mmap_page(VMA *vma, Page *page) {
    pgoff_t pgoff_invma;
    int prot = (page->state == PAGE_DIRTY ? PROT_READ|PROT_WRITE : PROT_READ);

    // NOTE: PAGE_LOADED_FOR_WRITE not passed here
    ASSERT(page->state == PAGE_LOADED || page->state == PAGE_DIRTY || page->state == PAGE_DESTROYED);
    ASSERT(vma->f_pgoffset <= page->f_pgoffset &&
                              page->f_pgoffset < vma_addr_fpgoffset(vma, vma->addr_stop));

    pgoff_invma = page->f_pgoffset - vma->f_pgoffset;
    if (!bitmap_test_bit(vma->page_ismappedv, pgoff_invma)) {
        void *mmapped = page_mmap(page, vma_page_addr(vma, page), prot);
        BUG_ON(!mmapped);
        bitmap_set_bit(vma->page_ismappedv, pgoff_invma);
        page_incref(page);
    }
    else {
        /* just changing protection bits should not fail, if parameters ok */
        xmprotect(vma_page_addr(vma, page), page_size(page), prot);
    }
}

/* is `page->fpgoffset` in file-range covered by `vma` */
static int vma_page_infilerange(VMA *vma, Page *page)
{
    pgoff_t vma_fpgstop;
    ASSERT(vma->fileh == page->fileh);

    vma_fpgstop = vma_addr_fpgoffset(vma, vma->addr_stop);
    return (vma->f_pgoffset <= page->f_pgoffset  &&
                               page->f_pgoffset < vma_fpgstop);
}

/* is `page` mapped to `vma` */
static int vma_page_ismapped(VMA *vma, Page *page)
{
    ASSERT(vma->fileh == page->fileh);

    if (!vma_page_infilerange(vma, page))
        return 0;

    return bitmap_test_bit(vma->page_ismappedv, page->f_pgoffset - vma->f_pgoffset);
}


/* ensure `page` is not mapped to `vma` */
static void vma_page_ensure_unmapped(VMA *vma, Page *page)
{
    if (!vma_page_ismapped(vma, page))
        return;

    if (vma->fileh->mmap_overlay) {
        /* wcfs: remmap readonly to base image */
        BigFile *file = vma->fileh->file;
        int err;

        TODO (file->blksize != page_size(page));
        err = file->file_ops->remmap_blk_read(vma, file, /* blk = */page->f_pgoffset);
        BUG_ON(err); /* must not fail */
    }
    else {
        /* !wcfs: mmap empty PROT_NONE address space instead of page memory */
        mem_xvalloc(vma_page_addr(vma, page), page_size(page));
    }

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

/* __fileh_page_isdirty returns whether fileh page is dirty or not.
 *
 * must be called under virtmem lock.
 */
bool __fileh_page_isdirty(BigFileH *fileh, pgoff_t pgoffset)
{
    Page *page;

    page = pagemap_get(&fileh->pagemap, pgoffset);
    if (!page)
        return false;

    return (page->state == PAGE_DIRTY);
}


// XXX stub
void OOM(void)
{
    BUG();
}
