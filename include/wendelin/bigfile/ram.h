#ifndef _WENDELIN_BIGFILE_RAM_H_
#define _WENDELIN_BIGFILE_RAM_H_

/* Wendelin.bigfile | Interfaces to work with RAM
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
 * TODO write why this needed (shmfs, hugetlbfs, ...)
 *
 *
 * - need to track pages state      (to support commit / abort)
 *
 * - need to "unload" non-dirty pages to free place for requested new data (reclaim)
 *
 * - need to be able to map a page into several places (to support
 *   overlapping-in-file mappings done not neccessarily adjacent-in-time to
 *   each other - there is no guarantee mapping them adjacent in address space
 *   is possible)
 *
 * XXX
 */

#include <wendelin/list.h>
#include <wendelin/bigfile/types.h>


typedef struct RAMH RAMH;
typedef struct Page Page;


/* RAM - something that provides access to memory (ex. via shmfs, hugetlbfs).
 *
 * Can create pages from memory allocated from backend and give memory back to
 * system on request.
 *
 * Pages allocation/deallocation is done through handles (see RAMH).
 */
struct RAM {
    const struct ram_ops *ram_ops;

    size_t pagesize;
    struct list_head    lru_list;   /* RAM pages in usage order (_ -> page->lru) */
};
typedef struct RAM RAM;


/* RAM operations - implemented by RAM backend */
struct ram_ops {
    size_t  (*get_current_maxsize)  (RAM *ram);
    RAMH *  (*ramh_open)            (RAM *ram);
    void    (*close)                (RAM *ram);
};



/* get RAM current max size (in pages)
 *
 * Maximum size is RAM current whole size, which is shared by RAM Handles and
 * other independent-from-us users (possibly from another processes).
 *
 * So amount of ram allocated for all RAM Handles could a) not be bigger than
 * this, and b) there is no guarantee that this maximum could be achieved via
 * allocating for RAMH only.
 *
 * Maximum is "current" because it can change dynamically - ex. via RAM
 * hotplug.
 */
size_t ram_get_current_maxsize(RAM *ram);


/* open RAM handle
 *
 * Open new handle for memory inside RAM. Close the handle with ramh_close()
 */
RAMH *ramh_open(RAM *ram);


/* close RAM
 *
 * TODO text
 */
void ram_close(RAM *ram);


/* RAM Handle - handle to allocate/free pages from/to RAM
 *
 * Additional level on top of RAM which allows to group pages allocation.
 *
 * RAM backends are assumed to be organized that for a RAM handle, all pages
 * allocated via that handle are provided by a single file in the OS kernel.
 *
 * With such organization, if 2 pages are allocated with adjacent pgoffset
 * and mapped adjacent to each-other in address space - there will be only 1
 * in-os-kernel VMA representing those 2 pages mapping.
 *
 * ( #os-vma should be kept to a minimum, because on every pagefault OS kernel
 *   needs to lookup faulting_addr -> os_vma )
 */
struct RAMH {
    const struct ramh_ops *ramh_ops;

    RAM     *ram;
};
typedef struct RAMH RAMH;

struct ramh_ops {
#define RAMH_PGOFF_ALLOCFAIL    ((pgoff_t)-1ULL)
    /* @return: allocated ram_pgoffset  | RAMH_PGOFF_ALLOCFAIL  */
    pgoff_t (*alloc_page)   (RAMH *ramh, pgoff_t pgoffset_hint);

    void *  (*mmap_page)    (RAMH *ramh, pgoff_t ramh_pgoffset, void *addr, int prot);

    void    (*drop_memory)  (RAMH *ramh, pgoff_t ramh_pgoffset);
    void    (*close)        (RAMH *ramh);
};


/* allocate new page for ramh memory
 *
 * @pgoffset_hint   hint at which offset to allocate memory -
 *
 *                  - could be used so that f_offsets coincide with ramh_offsets
 *                  and as the result, allocated areas constitute of contiguous
 *                  ramh memory = only 1 kernel VMA for whole area.
 *
 * @return          new page | NULL
 *
 * XXX write on how to free pages (= drop & free(struct Page) ?)
 *
 * NOTE after allocation, page->fileh & page->f_pgoffset are unset
 */
Page *ramh_alloc_page(RAMH *ramh, pgoff_t pgoffset_hint);


/* release ramh memory-page at ramh_pgoffset to OS
 *
 * After this call previous content of the memory-page is lost and the memory
 * is released to OS.
 *
 * The memory is still accessible for mmaping but will read as all zeros - on
 * first access OS would again allocate memory for it from scratch.
 */
void  ramh_drop_memory(RAMH *ramh, pgoff_t ramh_pgoffset);


/* close RAMH handle
 *
 * NOTE it is an error to call close() with mappings from ramh left
 */
void ramh_close(RAMH *ramh);



/* get default RAM by type
 *
 * @ram_type    str for ram-type    |NULL - get for default type
 */
RAM *ram_get_default(const char *ram_type);

/* create new RAM instance
 *
 * @ram_type    str for ram-type    |NULL - create for default type
 * @arg         str to pass to ram_type RAM constructor  (NULL - use defaults)
 */
RAM *ram_new(const char *ram_type, const char *arg);


/* RAM type registration (for RAM implementers) */
struct ram_type {
    const char *name;
    RAM *   (*ram_new) (const char *arg);
};

void ram_register_type(const struct ram_type *ram_type);


#endif
