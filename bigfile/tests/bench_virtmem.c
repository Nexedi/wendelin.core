/* Wendelin.bigfile | virtual memory benchmarks
 * Copyright (C) 2017-2021  Nexedi SA and Contributors.
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

// XXX better link with it
#include "../virtmem.c"
#include    "../pagemap.c"
#include    "../ram.c"
#include "../ram_shmfs.c"
#include "../pagefault.c"

#include <ccan/tap/tap.h>

#include "../../t/t_utils.h"
#include   "../../t/t_utils.c"

/* file that reads as zeros and tracks last loadblk request */
struct BigFile_ZeroTrack {
    BigFile;
    blk_t last_load;
};
typedef struct BigFile_ZeroTrack BigFile_ZeroTrack;

int zero_loadblk(BigFile *file0, blk_t blk, void *buf)
{
    BigFile_ZeroTrack *file = upcast(BigFile_ZeroTrack *, file0);

    //diag("zload #%ld", blk);

    // Nothing to do here - the memory buf obtained from OS comes pre-cleared
    // XXX reenable once/if memory comes uninitialized here
    file->last_load = blk;
    return 0;
}

static const struct bigfile_ops filez_ops = {
    .loadblk    = zero_loadblk,
    .storeblk   = NULL, // XXX
    .release    = NULL, // XXX
};

/* benchmark the time it takes for virtmem to handle pagefault with noop loadblk */
void bench_pagefault() {
    RAM *ram;
    BigFileH fh_struct, *fh = &fh_struct;
    VMA vma_struct, *vma = &vma_struct;
    pgoff_t p, npage = 10000;
    size_t PS;
    int err;

    double Tstart, Tend;

    ok1(!pagefault_init());

    ram = ram_new(NULL,NULL);
    ok1(ram);
    PS = ram->pagesize;

    /* setup zero file */
    BigFile_ZeroTrack f = {
        .blksize    = ram->pagesize,    /* artificially blksize = pagesize */
        .file_ops   = &filez_ops,
    };

    /* setup f mapping */
    err = fileh_open(fh, &f, ram, DONT_MMAP_OVERLAY);
    ok1(!err);

    err = fileh_mmap(vma, fh, 0, npage);
    ok1(!err);

    Tstart = microtime();

    // access first byte of every page
    for (p = 0; p < npage; p++) {
        b(vma, p * PS);
        if (f.last_load != p)
            fail("accessed page #%ld but last loadblk was for block #%ld", p, f.last_load);
    }

    Tend = microtime();

    printf("BenchmarkPagefaultC\t%ld\t%.3lf µs/op\n", npage, (Tend - Tstart) * 1E6 / npage);

    vma_unmap(vma);
    fileh_close(fh);
    ram_close(ram);
    free(ram);
}

int main()
{
    int i, nrun=3;
    tap_fail_callback = abort;  // XXX to catch failure immediately

    for (i=0; i<nrun; i++)
        bench_pagefault();

    return 0;
}
