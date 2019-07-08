/* Copyright (C) 2014-2019  Nexedi SA and Contributors.
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
#include "t_utils.h"

#include <wendelin/utils.h>

#include <sys/time.h>


static const struct ram_ops ram_limited_ops;
static const struct ramh_ops ramh_limited_ops;


RAMLimited *ram_limited_new(RAM *backend, size_t alloc_max)
{
    RAMLimited *ram;

    ram = zalloc(sizeof(*ram));
    if (!ram)
        return NULL;

    ram->backend    = backend;
    ram->pagesize   = backend->pagesize;

    /* NOTE allocated pages will be linked here (instead of backend->lru_list)
     * automatically, as upper code thinks _we_ allocated the page */
    INIT_LIST_HEAD(&ram->lru_list);

    ram->alloc_max  = alloc_max;
    ram->nalloc     = 0;

    ram->ram_ops    = &ram_limited_ops;

    return ram;
}


struct RAMHLimited {
    RAMH;
    RAMH *backend;
};
typedef struct RAMHLimited RAMHLimited;


size_t ram_limited_get_current_maxsize(RAM *ram0)
{
    RAMLimited  *ram  = upcast(RAMLimited *,  ram0);
    return ram_get_current_maxsize(ram->backend);
}


RAMH *ram_limited_ramh_open(RAM *ram0)
{
    RAMLimited  *ram  = upcast(RAMLimited *,  ram0);
    RAMHLimited *ramh;

    ramh = zalloc(sizeof(*ramh));
    if (!ramh)
        goto out;

    ramh->backend  = ramh_open(ram->backend);
    if (!ramh->backend)
        goto out;

    ramh->ram      = ram;
    ramh->ramh_ops = &ramh_limited_ops;
    return ramh;

out:
    free(ramh);
    return NULL;
}


void ram_limited_close(RAM *ram0)
{
    //RAMLimited  *ram  = upcast(RAMLimited *,  ram0);

    // XXX close if owning?
    // ram_close(ram->backend);
}


static const struct ram_ops ram_limited_ops = {
    .get_current_maxsize    = ram_limited_get_current_maxsize,
    .ramh_open              = ram_limited_ramh_open,
    .close                  = ram_limited_close,
};


pgoff_t ramh_limited_alloc_page(RAMH *ramh0, pgoff_t pgoffset_hint)
{
    RAMHLimited *ramh = upcast(RAMHLimited *, ramh0);
    RAMLimited  *ram  = upcast(RAMLimited *,  ramh->ram);
    pgoff_t pgoff;

    /* deny allocation when max #pages already allocated */
    if (ram->nalloc >= ram->alloc_max)
        return RAMH_PGOFF_ALLOCFAIL;

    pgoff = ramh->backend->ramh_ops->alloc_page(ramh->backend, pgoffset_hint);
    if (pgoff != RAMH_PGOFF_ALLOCFAIL)
        ram->nalloc++;

    return pgoff;
}


void ramh_limited_drop_memory(RAMH *ramh0, pgoff_t ramh_pgoffset)
{
    RAMHLimited *ramh = upcast(RAMHLimited *, ramh0);
    RAMLimited  *ram  = upcast(RAMLimited *,  ramh->ram);

    ramh->backend->ramh_ops->drop_memory(ramh->backend, ramh_pgoffset);
    ram->nalloc--;
}


void *ramh_limited_mmap_page(RAMH *ramh0, pgoff_t ramh_pgoffset, void *addr, int prot)
{
    RAMHLimited *ramh = upcast(RAMHLimited *, ramh0);
    return ramh->backend->ramh_ops->mmap_page(ramh->backend, ramh_pgoffset, addr, prot);
}


void ramh_limited_close(RAMH *ramh0)
{
    RAMHLimited *ramh = upcast(RAMHLimited *, ramh0);
    ramh->backend->ramh_ops->close(ramh->backend);
    free(ramh);
}


static const struct ramh_ops ramh_limited_ops = {
    .alloc_page     = ramh_limited_alloc_page,
    .drop_memory    = ramh_limited_drop_memory,

    .mmap_page      = ramh_limited_mmap_page,
    .close          = ramh_limited_close,
};


double microtime() {
        int err;
        struct timeval tv;

        err = gettimeofday(&tv, NULL);
        if (err == -1) {
                perror("gettimeofday");
                abort();
        }

        return tv.tv_sec + 1E-6 * tv.tv_usec;
}
