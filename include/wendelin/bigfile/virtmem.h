#ifndef _WENDELIN_BIGFILE_VIRTMEM_H_
#define _WENDELIN_BIGFILE_VIRTMEM_H_

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
 * ~~~~~~~~
 *
 * Virtual memory connects BigFile content and RAM pages into file memory
 * mappings.
 *
 * Read access to mapped pages cause their on-demand loading, and write access
 * marks modified pages as dirty. Dirty pages then can be on request either
 * written out back to file or discarded.
 *
 *
 * Mmap overlaying
 *
 * A particular BigFile implementation can optionally provide functionality to
 * mmap its data into memory. For BigFile handles opened in such mode, virtmem
 * does not allocate RAM for read access and will only allocate RAM when pages
 * are dirtied. The mode in which BigFile handle is opened is specified via
 * fileh_open(flags=...).
 *
 * The primary user of "mmap overlay" functionality is wcfs - virtual
 * filesystem that provides access to ZBigFile data via OS-level files(*).
 *
 * (*) see wcfs/client/wcfs.h and wcfs/wcfs.go
 */

#include <stdint.h>
#include <stdbool.h>
#include <wendelin/list.h>
#include <wendelin/bigfile/types.h>
#include <wendelin/bigfile/pagemap.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct bitmap bitmap;

typedef struct RAM RAM;
typedef struct RAMH RAMH;
typedef struct Page Page;
typedef struct BigFile BigFile;


/* BigFile Handle
 *
 * BigFile handle is a representation of file snapshot that could be locally
 * modified in-memory. The changes could be later either discarded or stored
 * back to file. One file can have many opened handles each with its own
 * modifications and optionally ram.
 */
struct BigFileH {
    BigFile *file;

    /* ram handle, backing this fileh mappings */
    RAMH    *ramh;

    /* fileh mappings (list of VMA)
     * NOTE current design assumes there will be not many mappings
     *      so instead of backpointers from pages to vma mapping entries, we'll
     *      scan all page->fileh->mmaps to overlap with page.
     */
    struct list_head mmaps; /* _ -> vma->same_fileh */

    /* {} f_pgoffset -> page */
    PageMap     pagemap;


    /* fileh dirty pages */
    struct list_head dirty_pages;   /* _ -> page->in_dirty */

    /* whether writeout is currently in progress */
    int writeout_inprogress;

    /* whether base data for all VMAs of this fileh are taken as base-layer mmap
     *
     * ( we require all VMAs under one fileh to be of the same kind to easily
     *   make decision whether after writeout to keep a page in RAM or to
     *   completely drop it not to waste RSS unnecessarily ) */
    unsigned    mmap_overlay : 1;

    pgoff_t destroyed_from_page;  // Start of destroyed range
    bool has_destroyed_range;     // Whether destruction was applied
};
typedef struct BigFileH BigFileH;


/* Page - describes fixed-size item of physical RAM associated with content from fileh */
enum PageState {
    PAGE_EMPTY      = 0, /* file content has not been loaded yet */
    PAGE_LOADING    = 1, /* file content              loading is  in progress */
    PAGE_LOADING_INVALIDATED
                    = 2, /* file content              loading was in progress
                            while request to invalidate the page came in */
    PAGE_LOADED     = 3, /* file content has     been loaded and was not modified */
    PAGE_LOADED_FOR_WRITE
                    = 4, /* file content has     been loaded and is going to be modified */
    PAGE_DIRTY      = 5, /* file content has     been loaded and was     modified */
    PAGE_DESTROYED  = 6, /* file content has     been destroyed */
};
typedef enum PageState PageState;

struct Page {
    PageState   state;

    /* wrt fileh - associated with */
    BigFileH    *fileh;
    pgoff_t     f_pgoffset;

    /* wrt ram - associated with */
    RAMH*       ramh;
    pgoff_t     ramh_pgoffset;

    /* in recently-used pages for ramh->ram (ram->lru_list -> _) */
    struct list_head lru;

    /* in dirty pages for fileh (fileh->dirty_pages -> _) */
    struct list_head in_dirty;

    int     refcnt; /* each mapping in a vma counts here */
};
typedef struct Page Page;



/* VMA - virtual memory area representing one fileh mapping
 *
 * NOTE areas may not overlap in virtual address space
 *      (in file space they can overlap).
 */
typedef struct VMA VMA;
struct VMA {
    uintptr_t   addr_start, addr_stop;    /* [addr_start, addr_stop) */

    BigFileH    *fileh;         /* for which fileh */
    pgoff_t     f_pgoffset;     /* where starts, in pages */

    /* FIXME For approximation 0, VMA(s) are kept in sorted doubly-linked
     * list, which is not good for lookup/add/remove performance O(n), but easy to
     * program. This should be ok for first draft, as there are not many fileh
     * views taken simultaneously.
     *
     * TODO for better performance, some binary-search-tree should be used.
     */
    struct list_head virt_list; /* (virtmem.c::vma_list -> _) */

    /* VMA's for the same fileh (fileh->mmaps -> _) */
    struct list_head same_fileh;

    /* whether corresponding to pgoffset-f_offset page is mapped in this VMA */
    bitmap      *page_ismappedv;    /* len ~ Δaddr / pagesize */

    /* BigFile-specific field used when VMA was created from fileh opened with
     * MMAP_OVERLAY flag. bigfile_ops.mmap_setup_read can initialize this to
     * object pointer specific to serving created base overlay mapping.
     *
     * For example WCFS uses this to link VMA -> wcfs.Mapping to know which
     * wcfs-specific mapping is serving particular virtmem VMA.
     *
     * NULL for VMAs created from under DONT_MMAP_OVERLAY fileh. */
    void        *mmap_overlay_server;
};


/*****************************
 *      API for clients      *
 *****************************/

/* flags for fileh_open */
enum FileHOpenFlags {
    /* use "mmap overlay" mode for base file data of all mappings created
     * for this fileh.
     *
     * The file must have .mmap_setup_read & friends != NULL in file_ops.
     */
    MMAP_OVERLAY        = 1 << 0,

    /* don't use "mmap overlay" mode */
    DONT_MMAP_OVERLAY   = 1 << 1,

    /* NOTE: if both MMAP_OVERLAY and DONT_MMAP_OVERLAY are not given,
     * the behaviour is to use mmap overlay if .mmap_* fops != NULL and
     * regular loads otherwise. */
};
typedef enum FileHOpenFlags FileHOpenFlags;

/* open handle for a BigFile
 *
 * @fileh[out]  BigFileH handle to initialize for this open
 * @file
 * @ram         RAM that will back created fileh mappings
 * @flags       flags for this open - see FileHOpenFlags
 *
 * @return  0 - ok, !0 - fail
 */
int fileh_open(BigFileH *fileh, BigFile *file, RAM *ram, FileHOpenFlags flags);


/* close fileh
 *
 * it's an error to call fileh_close with existing mappings
 * it's an error to call fileh_close while writeout for fileh is in progress
 */
void fileh_close(BigFileH *fileh);


/* map fileh part into memory
 *
 * This "maps" fileh part [pgoffset, pglen) in pages into process address space.
 *
 * @vma[out]    vma to initialize for this mmap
 * @return      0 - ok, !0 - fail
 */
int fileh_mmap(VMA *vma, BigFileH *fileh, pgoff_t pgoffset, pgoff_t pglen);


/* unmap mapping created by fileh_mmap()
 *
 * This removes mapping created by fileh_mmap() from process address space.
 * Changes made to fileh pages are preserved (to e.g. either other mappings and
 * later commit/discard).
 */
void vma_unmap(VMA *vma);


/* what to do at writeout */
enum WriteoutFlags {
    /* store dirty pages back to file
     *
     * - call file.storeblk() for all dirty pages;
     * - pages state remains PAGE_DIRTY.
     *
     * to "finish" the storage use WRITEOUT_MARKSTORED in the same or separate
     * call.
     */
    WRITEOUT_STORE          = 1 << 0,

    /* mark dirty pages as stored to file ok
     *
     * wcfs:  all mmaps are updated to map read-only to base layer.
     * !wcfs: pages state becomes PAGE_LOADED and all mmaps are updated to map
     *        pages as R/O to track further writes.
     */
    WRITEOUT_MARKSTORED     = 1 << 1,
};

/* write changes made to fileh memory back to file
 *
 * Perform write-related actions according to flags (see WriteoutFlags).
 *
 * @return  0 - ok      !0 - fail
 *          NOTE single WRITEOUT_MARKSTORED can not fail.
 *
 * No guarantee is made about atomicity - e.g. if this call fails, some
 * pages could be written and some left in memory in dirty state.
 *
 * it's an error for a given fileh to call several fileh_dirty_writeout() in
 * parallel.
 *
 * it's an error for a given fileh to modify its pages while writeout is in
 * progress: until fileh_dirty_writeout(... | WRITEOUT_STORE) has finished.
 */
int fileh_dirty_writeout(BigFileH *fileh, enum WriteoutFlags flags);


/* discard changes made to fileh memory
 *
 * For each fileh dirty page:
 *
 *   - it is unmapped from all mmaps;
 *   - its content is discarded;
 *   - its backing memory is released to OS.
 *
 * it's an error for a given fileh to call fileh_dirty_discard() while writeout
 * is in progress.
 */
void fileh_dirty_discard(BigFileH *fileh);


/* discard pages from specified offset onwards
 *
 * For each page from discard_from_page onwards:
 *
 *   - its state transitions to PAGE_DESTROYED;
 *   - pages return zeros on read access and transition to dirty on write.
 *
 * This function is only supported for fileh opened in MMAP_OVERLAY mode.
 *
 * it's an error to call fileh_discard_pages() while writeout is in progress.
 * it's recommended to call fileh_dirty_discard() before this function.
 */
void fileh_discard_pages(BigFileH *fileh, pgoff_t discard_from_page);

/* restore pages from PAGE_DESTROYED state
 *
 * Remove all pages in PAGE_DESTROYED state from fileh memory and pagemap.
 * For each destroyed page:
 *
 *   - it is unmapped from all mmaps;
 *   - its content is discarded;
 *   - its backing memory is released to OS;
 *   - it is removed from pagemap.
 *
 * This effectively undoes the effects of fileh_discard_pages() and forces
 * future access to those page offsets to trigger page faults that will
 * load fresh data from storage.
 *
 * it's an error for a given fileh to call fileh_restore_pages() while writeout
 * is in progress.
 */
void fileh_restore_pages(BigFileH *fileh);


/* invalidate fileh page
 *
 * Make sure that page corresponding to pgoffset is not present in fileh memory.
 *
 * The page could be in either dirty or loaded/loading or empty state. In all
 * cases page transitions to empty state and its memory is forgotten.
 *
 * ( Such invalidation is needed to synchronize fileh memory, when we know a
 *   file was changed externally )
 *
 * it's an error to call fileh_invalidate_page() while writeout for fileh is in
 * progress, or for fileh opened in MMAP_OVERLAY mode.
 */
void fileh_invalidate_page(BigFileH *fileh, pgoff_t pgoffset);


/* pagefault handler
 *
 * serves read/write access to protected memory: loads data from file on demand
 * and tracks which pages were made dirty.
 *
 * (clients call this indirectly via triggering SIGSEGV on read/write to memory)
 */
enum VMFaultResult {
    VM_HANDLED  = 0, /* pagefault handled */
    VM_RETRY    = 1, /* pagefault handled partly - handling have to be retried */
};
typedef enum VMFaultResult VMFaultResult;
VMFaultResult vma_on_pagefault(VMA *vma, uintptr_t addr, int write);
int pagefault_init(void);   /* in pagefault.c */


/* release some non-dirty ram back to OS; protect PROT_NONE related mappings
 *
 * This should be called when system is low on memory - it will scan through
 * RAM pages and release some LRU non-dirty pages ram memory back to OS.
 *
 * (this is usually done automatically under memory pressure)
 *
 * @return  how many RAM pages were reclaimed
 * XXX int -> size_t ?
 */
int ram_reclaim(RAM *ram);



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


/* lookup VMA by addr */
VMA *virt_lookup_vma(void *addr);
void virt_register_vma(VMA *vma);
void virt_unregister_vma(VMA *vma);

/* allocate virtual memory address space */
void *mem_valloc(void *addr, size_t len);
void *mem_xvalloc(void *addr, size_t len);

/* big virtmem lock */
void virt_lock(void);
void virt_unlock(void);

/* for thirdparty to hook into locking big virtmem lock process
 * (e.g. for python to hook in its GIL release/reacquire)  */
struct VirtGilHooks {
    /* drop gil, if current thread hold it */
    void *  (*gil_ensure_unlocked)      (void);
    /* retake gil, if we were holding it at ->ensure_unlocked() stage */
    void    (*gil_retake_if_waslocked)  (void *);
};
typedef struct VirtGilHooks VirtGilHooks;

void virt_lock_hookgil(const VirtGilHooks *gilhooks);

bool __fileh_page_isdirty(BigFileH *fileh, pgoff_t pgoff);

// XXX is this needed? think more
/* what happens on out-of-memory */
void OOM(void);

#ifdef __cplusplus
}
#endif

#endif
