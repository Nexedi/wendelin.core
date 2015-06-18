/* Wendelin.bigfile | tests for real faults leading to crash
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
 * ~~~~
 *
 * Most tests here end up crashing via segmentation violation. The calling
 * driver verifies test output prior to crash and that the crash happenned in
 * the right place.
 *
 * See t/tfault-run and `test.fault` in Makefile for driver details.
 */

// XXX better link with it
#include "../virtmem.c"
#include    "../pagemap.c"
#include    "../ram.c"
#include "../ram_shmfs.c"
#include "../pagefault.c"

#include <ccan/tap/tap.h>
#include <ccan/array_size/array_size.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "../../t/t_utils.h"


static void prefault()
{
    diag("going to fault...");
    fflush(stdout);
    fflush(stderr);
}


void fault_read()
{
    diag("Testing pagefault v.s. incorrect read");
    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    prefault();
    ((volatile int *)NULL) [0];
}


void fault_write()
{
    diag("Testing pagefault v.s. incorrect write");
    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    prefault();
    ((volatile int *)NULL) [0] = 0;
}


/* fault in loadblk (= doublefault) */
void fault_in_loadblk()
{
    RAM *ram;
    BigFileH fh;
    VMA vma_struct, *vma = &vma_struct;
    size_t PS;
    int err;

    diag("testing pagefault v.s. fault in loadblk");

    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    ram = ram_new(NULL,NULL);
    ok1(ram);
    PS = ram->pagesize;

    /* loadblk, simulating error in storage layer, touches memory in vma for
     * another blk -> doublefault */
    int faulty_loadblk(BigFile *file, blk_t blk, void *buf)
    {
        /* touch page[1] - should crash here */
        b(vma, 1*PS);

        return 0;
    }

    const struct bigfile_ops faulty_ops = {
        .loadblk    = faulty_loadblk,
    };

    BigFile f = {
        .blksize    = ram->pagesize,    /* artificial */
        .file_ops   = &faulty_ops,
    };

    err = fileh_open(&fh, &f, ram);
    ok1(!err);

    err = fileh_mmap(vma, &fh, 0, 2);
    ok1(!err);

    /* touch page[0] - should dive into loadblk and doublefault there */
    prefault();
    b(vma, 0);
}


/* fault in storeblk (single fault - but should die) */
void fault_in_storeblk()
{
    RAM *ram;
    BigFileH fh;
    VMA vma_struct, *vma = &vma_struct;
    size_t PS;
    int err;

    diag("testing pagefault v.s. fault in storeblk");

    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    ram = ram_new(NULL,NULL);
    ok1(ram);
    PS = ram->pagesize;

    /* empty loadblk - memory will just stay as it is (all 0) */
    int empty_loadblk(BigFile *file, blk_t blk, void *buf)
    {   return 0;   }

    /* storeblk "incorrectly" accesses other protected memory which should be
     * catched and SIGSEGV */
    int faulty_storeblk(BigFile *file, blk_t blk, const void *buf)
    {
        /* read page[1] - should crash here */
        b(vma, 1*PS);

        return 0;
    }


    const struct bigfile_ops faulty_ops = {
        .loadblk    = empty_loadblk,
        .storeblk   = faulty_storeblk,
    };

    BigFile f = {
        .blksize    = ram->pagesize,    /* artificial */
        .file_ops   = &faulty_ops,
    };

    err = fileh_open(&fh, &f, ram);
    ok1(!err);

    err = fileh_mmap(vma, &fh, 0, 2);
    ok1(!err);

    /* write to page[0] -> page[0] becomes dirty */
    b(vma, 0) = 1;

    /* writeout calls storeblk which faults */
    prefault();
    fileh_dirty_writeout(&fh, WRITEOUT_STORE);
}



/* BigFile, which .loadblk() always return error */
int err_loadblk(BigFile *file, blk_t blk, void *buf)
{
    return -1;
}

const struct bigfile_ops err_ops = {
    .loadblk = err_loadblk,
};


/* loadblk error in main thread -> full abort */
void abort_loadblkerr_t0()
{
    RAM *ram;
    BigFileH fh;
    VMA vma_struct, *vma = &vma_struct;
    int err;

    diag("testing loadblk error in main thread");

    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    ram = ram_new(NULL,NULL);
    ok1(ram);

    BigFile f = {
        .blksize    = ram->pagesize,
        .file_ops   = &err_ops,
    };

    err = fileh_open(&fh, &f, ram);
    ok1(!err);

    err = fileh_mmap(vma, &fh, 0, 2);
    ok1(!err);

    /* touch page[0] - should abort whole processe because loadblk() returns -1 */
    prefault();
    b(vma, 0);
}


/* loadblk error in second thread -> abort only that thread, main continues to run */
void abort_loadblkerr_t1()
{
    RAM *ram;
    BigFileH fh;
    VMA vma_struct, *vma = &vma_struct;
    int err;
    pthread_t t1;

    diag("testing loadblk error in second thread");

    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    ram = ram_new(NULL,NULL);
    ok1(ram);

    BigFile f = {
        .blksize    = ram->pagesize,
        .file_ops   = &err_ops,
    };

    err = fileh_open(&fh, &f, ram);
    ok1(!err);

    err = fileh_mmap(vma, &fh, 0, 2);
    ok1(!err);

    void *__t1(void *arg)
    {
        /* touch page[0] - should abort t1 processe because loadblk() returns -1 */
        prefault();
        b(vma, 0);

        /* should not get here - abort whole process */
        abort();
    }

    err = pthread_create(&t1, NULL, __t1, NULL);
    ok1(!err);

    /* but main thread stays alive */
    err = pthread_join(t1, NULL);
    ok1(!err);

    diag("I: main thread is still alive");
    exit(0);
}



static const struct {
    const char *name;
    void (*test)(void);
} tests[] = {
    // XXX fragile - test names must start exactly with `{"fault` - Makefile extracts them this way
    // name                                        mustdie traceback    signal
    {"faultr",          fault_read},                // on_pagefault     SIGSEGV
    {"faultw",          fault_write},               // on_pagefault     SIGSEGV
    {"fault_loadblk",   fault_in_loadblk},          // faulty_loadblk   SIGSEGV
    {"fault_storeblk",  fault_in_storeblk},         // faulty_storeblk

    {"fault_loadblkerr_t0", abort_loadblkerr_t0},   // __GI_raise,__GI_abort,__abort_thread,vma_on_pagefault,on_pagefault,sighandler,abort_loadblkerr_t0 SIGABRT
    {"fault_loadblkerr_t1", abort_loadblkerr_t1},   // __GI_raise,__GI_abort,__abort_thread,vma_on_pagefault,on_pagefault,sighandler,__t1 SIGABRT 0
};

int main(int argc, char *argv[])
{
    int i;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <test>\n", argv[0]);
        exit(1);
    }


    tap_fail_callback = abort;  // XXX to catch failure immediately

    for (i=0; i<ARRAY_SIZE(tests); i++) {
        if (strcmp(argv[1], tests[i].name))
            continue;

        tests[i].test();
        fail("should not get here");
    }

    fail("unknown test '%s'", argv[1]);
    return 1;
}
