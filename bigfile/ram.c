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
 *
 * TODO write why this needed (shmfs, hugetlbfs, ...)
 */

#include <wendelin/bigfile/ram.h>
#include <wendelin/bigfile/file.h>
#include <wendelin/bigfile/virtmem.h>
#include <wendelin/bigfile/pagemap.h>
#include <wendelin/utils.h>
#include <wendelin/bug.h>


/* ramh_ops */

Page *ramh_alloc_page(RAMH *ramh, pgoff_t pgoffset_hint)
{
    struct Page *page;
    pgoff_t ramh_pgoffset;

    // XXX ok to malloc, or better do more structured allocations?
    page = zalloc(sizeof(*page));
    if (!page)
        return NULL;

    ramh_pgoffset = ramh->ramh_ops->alloc_page(ramh, pgoffset_hint);
    if (ramh_pgoffset == RAMH_PGOFF_ALLOCFAIL) {
        free(page);
        return NULL;
    }

    page->state = PAGE_EMPTY;
    /* ->file & ->f_pgoffset are left unset */
    page->ramh  = ramh;
    page->ramh_pgoffset = ramh_pgoffset;
    INIT_LIST_HEAD(&page->lru); /* NOTE ->lru    left unlinked */
    INIT_LIST_HEAD(&page->in_dirty);	/* initially not in dirty list */
    page->refcnt = 0;

    return page;
}


void ramh_drop_memory(RAMH *ramh, pgoff_t ramh_pgoffset)
{
    return ramh->ramh_ops->drop_memory(ramh, ramh_pgoffset);
}


void ramh_close(RAMH *ramh)
{
    /* NOTE ->close() should free ramh structure itself */
    ramh->ramh_ops->close(ramh);
}



/* ram_ops */
size_t ram_get_current_maxsize(RAM *ram)
{
    return ram->ram_ops->get_current_maxsize(ram);
}

RAMH *ramh_open(RAM *ram)
{
    return ram->ram_ops->ramh_open(ram);
}

void ram_close(RAM *ram)
{
    WARN("TODO ram_close()");   // XXX
    //return ram->ram_ops->close(ram);
}



/* RAM types */
static LIST_HEAD(ram_types);
struct ram_type_entry {
    const struct ram_type *ram_type;
    RAM *default_ram;

    struct list_head    list;
};


void ram_register_type(const struct ram_type *ram_type)
{
    struct ram_type_entry *rte = xzalloc(sizeof(*rte));

    rte->ram_type = ram_type;
    list_add_tail(&rte->list, &ram_types);
}


static const char ram_type_default[] = "shmfs"; /* default type for !ram_type */

RAM *ram_new(const char *ram_type, const char *arg)
{
    struct list_head *h;

    if (!ram_type)
        ram_type = ram_type_default;

    list_for_each(h, &ram_types) {
        struct ram_type_entry *rte = list_entry(h, typeof(*rte), list);
        const struct ram_type *rt  = rte->ram_type;

        if (!strcmp(rt->name, ram_type))
            return rt->ram_new(arg);
    }

    return NULL;
}


RAM *ram_get_default(const char *ram_type)
{
    struct list_head *h;

    if (!ram_type)
        ram_type = ram_type_default;

    list_for_each(h, &ram_types) {
        struct ram_type_entry *rte = list_entry(h, typeof(*rte), list);
        const struct ram_type *rt  = rte->ram_type;

        if (strcmp(rt->name, ram_type))
            continue;

        if (!rte->default_ram)
            rte->default_ram = rt->ram_new(NULL);

        BUG_ON(!rte->default_ram);
        return rte->default_ram;
    }

    BUG();
}
