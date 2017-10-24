/* Wendelin.bigfile | shmfs (aka tmpfs) ram backend
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
 * TODO description
 */

#include <wendelin/bigfile/ram.h>
#include <wendelin/utils.h>
#include <wendelin/bug.h>

#include <fcntl.h>
/* FIXME glibc in Debian before Jessie does not define FALLOC_FL_KEEP_SIZE and
 * FALLOC_FL_PUNCH_HOLE, even when kernel supports it
 * http://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/tree/include/uapi/linux/falloc.h
 */
#ifndef FALLOC_FL_KEEP_SIZE
# define FALLOC_FL_KEEP_SIZE    0x01
#endif
#ifndef FALLOC_FL_PUNCH_HOLE
# define FALLOC_FL_PUNCH_HOLE   0x02
#endif

#include <unistd.h>
#include <sys/vfs.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <errno.h>


/* we'll manage RAM in "pages" of 2M
 *
 * Compared to std 4K pages, this will reduce per-page overhead and also
 * coincides with huge page size on x86/x86_64).
 *
 * Hardware pages will still be of usual 4K size - we'll just manage them in
 * 512-pages groups.
 */
#define SHMFS_PAGE_SIZE     (2*1024*1024ULL)


/* default prefix & ramh files template */
static const char shmfs_ram_prefix_default[] = "/dev/shm";
static const char shmfs_ramh_template[]      = "ramh.XXXXXX";


/* RAM on shmfs */
struct SHMFS_RAM {
    RAM;

    const char *prefix; /* prefix where to create ramh files */
};
typedef struct SHMFS_RAM SHMFS_RAM;


/* RAM Handle on shmfs */
struct SHMFS_RAMH {
    RAMH;

    int    ramh_fd;
    size_t ramh_fpgsize;  /* current file size in pagesize units */
};
typedef struct SHMFS_RAMH SHMFS_RAMH;



static void *shmfs_mmap_page(RAMH *ramh0, pgoff_t ramh_pgoffset, void *addr, int prot)
{
    SHMFS_RAMH *ramh = upcast(SHMFS_RAMH *, ramh0);
    size_t pagesize = ramh->ram->pagesize;

    // XXX MAP_POPULATE so that we can access mmaped memory without additional pagefault?
    //     tried -> this mmap becomes slow, and overall the whole run is slower. XXX why?
    addr = mmap(addr, pagesize,
                prot,
                MAP_SHARED
                | (addr ? MAP_FIXED : 0),
                ramh->ramh_fd,
                ramh_pgoffset * pagesize);
    if (addr == MAP_FAILED)
        addr = NULL;

    return addr;
}


pgoff_t shmfs_alloc_page(RAMH *ramh0, pgoff_t pgoffset_hint)
{
    // FIXME double calls with same pgoffset_hint ? (or move ->pagemap to ramh ?)
    SHMFS_RAMH *ramh = upcast(SHMFS_RAMH *, ramh0);
    pgoff_t ramh_pgoffset = pgoffset_hint;
    size_t pagesize = ramh->ram->pagesize;
    int err;

    /*
     * - allocate space for page at ramh_pgoffset,
     * - hole-grow file to size covering that page, if file was smaller,
     *
     * all in one go.
     *
     * We allocate filesystem space so that we know we really allocated that
     * memory now and that client code will not get SIGBUS on memory read/write
     * or EFAULT on syscalls read/write, when accessing it later.
     *
     * It is easier to handle ENOMEM synchronously.
     */
    err = fallocate(ramh->ramh_fd, 0 /* without KEEP_SIZE */,
            ramh_pgoffset * pagesize, pagesize);

    if (err) {
        /* warn users explicitly, if fallocate() is not supported */
        if (errno == EOPNOTSUPP)
            WARN("fallocate() not supported");
        return RAMH_PGOFF_ALLOCFAIL;
    }

    if (ramh_pgoffset >= ramh->ramh_fpgsize)
        ramh->ramh_fpgsize = ramh_pgoffset+1;

    return ramh_pgoffset;
}


static void shmfs_drop_memory(RAMH *ramh0, pgoff_t ramh_pgoffset)
{
    SHMFS_RAMH *ramh = upcast(SHMFS_RAMH *, ramh0);
    size_t pagesize = ramh->ram->pagesize;

    BUG_ON(ramh_pgoffset >= ramh->ramh_fpgsize);

    // XXX state -> empty ?

    /* punch hole and this way release memory to OS.
     * this should not fail - if it is, something is wrong */
    xfallocate(ramh->ramh_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            ramh_pgoffset * pagesize, pagesize);
}


static void shmfs_close(RAMH *ramh0)
{
    SHMFS_RAMH *ramh = upcast(SHMFS_RAMH *, ramh0);

    // XXX verify no mapping left?

    /* drop all memory & close */
    xftruncate(ramh->ramh_fd, 0);
    xclose(ramh->ramh_fd);
    ramh->ramh_fd = -1;
    ramh->ramh_fpgsize = 0;

    free(ramh);
}


static const struct ramh_ops shmfs_ramh_ops = {
    .alloc_page     = shmfs_alloc_page,
    .mmap_page      = shmfs_mmap_page,
    .drop_memory    = shmfs_drop_memory,
    .close          = shmfs_close,
};


static size_t shmfs_get_current_maxsize(RAM *ram0)
{
    SHMFS_RAM *ram = upcast(SHMFS_RAM *, ram0);
    struct statfs sf;
    int err;

    // XXX races with fs remount/change under prefix
    err = statfs(ram->prefix, &sf);
    if (err)
        BUGe();

    return sf.f_blocks * sf.f_bsize / ram->pagesize;
}


static RAMH *shmfs_ramh_open(RAM *ram0)
{
    SHMFS_RAM *ram = upcast(SHMFS_RAM *, ram0);
    SHMFS_RAMH *ramh;
    char *s,   *ramh_filename = NULL;
    int err;

    ramh = zalloc(sizeof(*ramh));
    if (!ramh)
        goto out;

    ramh->ramh_ops = &shmfs_ramh_ops;
    ramh->ram      = ram;


    ramh_filename = malloc(strlen(ram->prefix) + 1/*"/"*/ +
                           strlen(shmfs_ramh_template) + 1/*NUL*/);
    if (!ramh_filename)
        goto out;

    s = ramh_filename;
    s = stpcpy(s, ram->prefix);
    s = stpcpy(s, "/");
    s = stpcpy(s, shmfs_ramh_template);

    ramh->ramh_fd = mkstemp(ramh_filename);
    if (ramh->ramh_fd == -1)
        goto out;

    // XXX maybe by default show and unlink atexit / on close
    /* unlink ramh file, if not asked to leave it show for debugging */
    s = getenv("WENDELIN_RAMH_HIDE");
    if (!s || (s && s[0] == 'y')) {
        err = unlink(ramh_filename);
        if (err)
            BUGe();
    }

    free(ramh_filename);

    ramh->ramh_fpgsize = 0;
    return ramh;

out:
    free(ramh);
    free(ramh_filename);
    return NULL;
}


static const struct ram_ops shmfs_ram_ops = {
    .get_current_maxsize    = shmfs_get_current_maxsize,
    .ramh_open              = shmfs_ramh_open,
    //.close    = shmfs_ram_dtor
};


/* shmfs ram type */
static RAM *shmfs_ram_new(const char *arg)
{
    SHMFS_RAM *ram = xzalloc(sizeof(*ram));

    ram->ram_ops   = &shmfs_ram_ops;
    ram->pagesize  = SHMFS_PAGE_SIZE;
    INIT_LIST_HEAD(&ram->lru_list);

    // TODO ensure prefix points to somewhere on shmfs
    ram->prefix    = xstrdup(arg ?: shmfs_ram_prefix_default);

    return ram;
};


// TODO shmfs_ram_dtor


static const struct ram_type shmfs_ram_type = {
    .name     = "shmfs",
    .ram_new  = shmfs_ram_new,
};



__attribute__((constructor))
static void  shmfs_init(void)
{
    ram_register_type(&shmfs_ram_type);
}
