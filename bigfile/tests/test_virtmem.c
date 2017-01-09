/* Wendelin.bigfile | virtual memory tests
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
 */

// XXX better link with it
#include "../virtmem.c"
#include    "../pagemap.c"
#include    "../ram.c"
#include "../ram_shmfs.c"
#include "../pagefault.c"

#include <ccan/tap/tap.h>
#include <setjmp.h>
#include <signal.h>
#include <errno.h>

#include "../../t/t_utils.h"
#include   "../../t/t_utils.c"


void test_vmamap()
{
    VMA vma1, vma2, vma3;

    vma1.addr_start = 0x1000;
    vma1.addr_stop  = 0x2000;

    vma2.addr_start = 0x2000;
    vma2.addr_stop  = 0x3000;

    vma3.addr_start = 0x3000;
    vma3.addr_stop  = 0x4000;

    VMA *L(uintptr_t addr)  { return virt_lookup_vma((void *)addr); }

    diag("Testing vmamap");
    ok1(list_empty(&vma_list));
    ok1(!L(0));
    ok1(!L(0x1000-1));
    ok1(!L(0x1000));
    ok1(!L(0x1800));
    ok1(!L(0x2000-1));
    ok1(!L(0x2000));

    virt_register_vma(&vma3);
    ok1(!L(0x3000-1));
    ok1( L(0x3000) == &vma3 );
    ok1( L(0x3800) == &vma3 );
    ok1( L(0x4000-1) == &vma3 );
    ok1(!L(0x4000));

    virt_register_vma(&vma1);
    ok1(!L(0x1000-1));
    ok1( L(0x1000) == &vma1 );
    ok1( L(0x1800) == &vma1 );
    ok1( L(0x2000-1) == &vma1 );
    ok1(!L(0x2000));
    ok1(!L(0x3000-1));
    ok1( L(0x3000) == &vma3 );
    ok1( L(0x3800) == &vma3 );
    ok1( L(0x4000-1) == &vma3 );
    ok1(!L(0x4000));

    virt_register_vma(&vma2);
    ok1(!L(0x1000-1));
    ok1( L(0x1000) == &vma1 );
    ok1( L(0x1800) == &vma1 );
    ok1( L(0x2000-1) == &vma1 );
    ok1( L(0x2000) == &vma2 );
    ok1( L(0x2800) == &vma2 );
    ok1( L(0x3000-1) == &vma2);
    ok1( L(0x3000) == &vma3 );
    ok1( L(0x3800) == &vma3 );
    ok1( L(0x4000-1) == &vma3 );
    ok1(!L(0x4000));

    virt_unregister_vma(&vma3);
    ok1(!L(0x1000-1));
    ok1( L(0x1000) == &vma1 );
    ok1( L(0x1800) == &vma1 );
    ok1( L(0x2000-1) == &vma1 );
    ok1( L(0x2000) == &vma2 );
    ok1( L(0x2800) == &vma2 );
    ok1( L(0x3000-1) == &vma2);
    ok1(!L(0x3000));
    ok1(!L(0x3800));
    ok1(!L(0x4000-1));
    ok1(!L(0x4000));

    virt_register_vma(&vma3);
    ok1(!L(0x1000-1));
    ok1( L(0x1000) == &vma1 );
    ok1( L(0x1800) == &vma1 );
    ok1( L(0x2000-1) == &vma1 );
    ok1( L(0x2000) == &vma2 );
    ok1( L(0x2800) == &vma2 );
    ok1( L(0x3000-1) == &vma2);
    ok1( L(0x3000) == &vma3 );
    ok1( L(0x3800) == &vma3 );
    ok1( L(0x4000-1) == &vma3 );
    ok1(!L(0x4000));

    virt_unregister_vma(&vma2);
    ok1(!L(0x1000-1));
    ok1( L(0x1000) == &vma1 );
    ok1( L(0x1800) == &vma1 );
    ok1( L(0x2000-1) == &vma1 );
    ok1(!L(0x2000));
    ok1(!L(0x2800));
    ok1(!L(0x3000-1));
    ok1( L(0x3000) == &vma3 );
    ok1( L(0x3800) == &vma3 );
    ok1( L(0x4000-1) == &vma3 );
    ok1(!L(0x4000));

    virt_unregister_vma(&vma1);
    ok1(!L(0x1000-1));
    ok1(!L(0x1000));
    ok1(!L(0x1800));
    ok1(!L(0x2000-1));
    ok1(!L(0x2000));
    ok1(!L(0x2800));
    ok1(!L(0x3000-1));
    ok1( L(0x3000) == &vma3 );
    ok1( L(0x3800) == &vma3 );
    ok1( L(0x4000-1) == &vma3 );
    ok1(!L(0x4000));

    virt_unregister_vma(&vma3);
    ok1(!L(0x1000-1));
    ok1(!L(0x1000));
    ok1(!L(0x1800));
    ok1(!L(0x2000-1));
    ok1(!L(0x2000));
    ok1(!L(0x2800));
    ok1(!L(0x3000-1));
    ok1(!L(0x3000));
    ok1(!L(0x3800));
    ok1(!L(0x4000-1));
    ok1(!L(0x4000));

    ok1(list_empty(&vma_list));
}


/* file that reads #blk on loadblk(blk) */
struct BigFileIdentity {
    BigFile;
};
typedef struct BigFileIdentity BigFileIdentity;


int fileid_loadblk(BigFile *file, blk_t blk, void *buf)
{
    blk_t  *bbuf  = buf;
    size_t  bsize = file->blksize / sizeof(*bbuf);

    while (bsize--)
        *bbuf++ = blk;

    return 0;
}


static const struct bigfile_ops fileid_ops = {
    .loadblk    = fileid_loadblk,
    .storeblk   = NULL, // XXX
    .release    = NULL, // XXX
};



/* tell ASAN we are using own SIGSEGV handler in MUST_FAULT */
const char *__asan_default_options()
{
    return "allow_user_segv_handler=1";
}

/* tell TSAN we are OK with calling async-sig-unsafe fucnctions from sync SIGSEGV */
const char *__tsan_default_options()
{
    return "report_signal_unsafe=0";
}


/* whether appropriate page of vma is mapped */
int M(VMA *vma, pgoff_t idx) {  return bitmap_test_bit(vma->page_ismappedv, idx);   }


/* check that
 *
 * - page != NULL,
 * - page is the same as fileh->pagemap[pgoffset],
 * - with expected page->state and page->refcnt.
 */
#define __CHECK_PAGE(page, fileh, pgoffset, pgstate, pgrefcnt) do { \
    ok1(page);                                                      \
    ok1(page == pagemap_get(&(fileh)->pagemap, (pgoffset)));        \
    ok1((page)->state  == (pgstate));                               \
    ok1((page)->refcnt == (pgrefcnt));                              \
} while (0)

/* check that fileh->pagemap[pgfosset] is empty */
#define __CHECK_NOPAGE(fileh, pgoffset) do {            \
    ok1(!pagemap_get(&(fileh)->pagemap, (pgoffset)));   \
} while (0)


/* vma_on_pagefault() assumes virtmem_lock is taken by caller and can ask it to
 * retry. Handle fault to the end, like on_pagefault() does. */
void xvma_on_pagefault(VMA *vma, uintptr_t addr, int write) {
    virt_lock();
    while (1) {
        VMFaultResult vmres;
        vmres = vma_on_pagefault(vma, addr, write);

        if (vmres == VM_HANDLED)
            break;
        if (vmres == VM_RETRY)
            continue;

        fail("Unexpected return code from vma_on_pagefault: %i", vmres);
    }
    virt_unlock();
}


/* test access to file mappings via explicit vma_on_pagefault() calls */
void test_file_access_synthetic(void)
{
    RAM *ram, *ram0;
    BigFileH fh_struct, *fh = &fh_struct;
    VMA vma_struct, *vma = &vma_struct;
    Page *page0, *page1, *page2, *page3;
    blk_t *b0, *b2;
    size_t PS, PSb;
    int err;

    /* MUST_FAULT(code) - checks that code faults */
    sigjmp_buf    fault_jmp;
    volatile int  fault_expected = 0;
    void sigfault_handler(int sig) {
        if (!fault_expected) {
            diag("Unexpected fault - abort");
            abort();
        }
        /* just return from sighandler to proper place */
        fault_expected = 0;
        siglongjmp(fault_jmp, 1);
    }
#define MUST_FAULT(code) do {                                   \
    fault_expected = 1;                                         \
    if (!sigsetjmp(fault_jmp, 1)) {                             \
        code; /* should pagefault -> sighandler does longjmp */ \
        fail("'" #code "' did not cause fault");                \
    }                                                           \
    else {                                                      \
        pass("'" #code "' faulted");                            \
    }                                                           \
} while (0)


    diag("Testing file access (synthetic)");

    struct sigaction act, saveact;
    act.sa_handler = sigfault_handler;
    act.sa_flags   = 0;
    ok1(!sigemptyset(&act.sa_mask));
    ok1(!sigaction(SIGSEGV, &act, &saveact));

    /* ram limited to exactly 3 pages (so that we know we trigger reclaim on
     * exactly when allocating more) */
    ram0 = ram_new(NULL, NULL);
    ok1(ram0);
    ram = ram_limited_new(ram0, 3);
    ok1(ram);
    PS = ram->pagesize;
    PSb = PS / sizeof(blk_t);   /* page size in blk_t units */

    /* ensure we are starting from new ram */
    ok1(list_empty(&ram->lru_list));

    /* setup id file */
    struct bigfile_ops x_ops = {.loadblk = fileid_loadblk};
    BigFileIdentity fileid = {
        .blksize    = ram->pagesize,    /* artificially blksize = pagesize */
        .file_ops   = &x_ops,
    };

    err = fileh_open(fh, &fileid, ram);
    ok1(!err);
    ok1(list_empty(&fh->mmaps));

/* implicitly use fileh=fh */
#define CHECK_PAGE(page, pgoffset, pgstate, pgrefcnt)   \
                __CHECK_PAGE(page, fh, pgoffset, pgstate, pgrefcnt)
#define CHECK_NOPAGE(pgoffset)  __CHECK_NOPAGE(fh, pgoffset)


    err = fileh_mmap(vma, fh, 100, 4);
    ok1(!err);

    ok1(fh->mmaps.next == &vma->same_fileh);
    ok1(vma->same_fileh.next == &fh->mmaps);

    /* all pages initially unmapped
     * M                R                               W                           */
    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1(!fh->dirty);
    CHECK_NOPAGE(         100                      );
    CHECK_NOPAGE(         101                      );
    CHECK_NOPAGE(         102                      );
    CHECK_NOPAGE(         103                      );

    ok1(list_empty(&ram->lru_list));


    /* simulate read access to page[0] - it should load it */
    diag("read page[0]");
    xvma_on_pagefault(vma, vma->addr_start + 0*PS, 0);

    ok1( M(vma, 0));                B(vma, 0*PSb);      MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1(!fh->dirty);
    page0 = pagemap_get(&fh->pagemap, 100);
    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    1);
    CHECK_NOPAGE(         101                      );
    CHECK_NOPAGE(         102                      );
    CHECK_NOPAGE(         103                      );

    ok1(B(vma, 0*PSb + 0) == 100);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);

    ok1(ram->lru_list.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* simulate write access to page[2] - it should load it and mark page dirty */
    diag("write page[2]");
    xvma_on_pagefault(vma, vma->addr_start + 2*PS, 1);

    ok1( M(vma, 0));                B(vma, 0*PSb);      MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    page2 = pagemap_get(&fh->pagemap, 102);
    CHECK_PAGE  (page0,   100, PAGE_LOADED,   1);
    CHECK_NOPAGE(         101                  );
    CHECK_PAGE  (page2,   102, PAGE_DIRTY,    1);
    CHECK_NOPAGE(         103                  );

    ok1(B(vma, 0*PSb + 0) == 100);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);
    ok1(B(vma, 2*PSb + 0) ==  12);  /* overwritten at fault w check */
    ok1(B(vma, 2*PSb + 1) == 102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* read access to page[3] - load */
    diag("read page[3]");
    xvma_on_pagefault(vma, vma->addr_start + 3*PS, 0);

    ok1( M(vma, 0));                B(vma, 0*PSb);      MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1( M(vma, 3));                B(vma, 3*PSb);      MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    page3 = pagemap_get(&fh->pagemap, 103);
    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    1);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    ok1(B(vma, 0*PSb + 0) == 100);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);
    ok1(B(vma, 2*PSb + 0) ==  12);
    ok1(B(vma, 2*PSb + 1) == 102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);
    ok1(B(vma, 3*PSb + 0) == 103);
    ok1(B(vma, 3*PSb + 1) == 103);
    ok1(B(vma, 3*PSb + PSb - 1) == 103);

    ok1(ram->lru_list.prev == &page3->lru);
    ok1(page3->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* write access to page[0] - upgrade loaded -> dirty */
    diag("write page[0]");
    xvma_on_pagefault(vma, vma->addr_start + 0*PS, 1);

    ok1( M(vma, 0));                B(vma, 0*PSb);                  B(vma, 0*PSb) = 10;
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1( M(vma, 3));                B(vma, 3*PSb);      MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    ok1(B(vma, 0*PSb + 0) ==  10);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);
    ok1(B(vma, 2*PSb + 0) ==  12);
    ok1(B(vma, 2*PSb + 1) == 102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);
    ok1(B(vma, 3*PSb + 0) == 103);
    ok1(B(vma, 3*PSb + 1) == 103);
    ok1(B(vma, 3*PSb + PSb - 1) == 103);

    ok1(ram->lru_list.prev == &page0->lru); /* page0 became MRU */
    ok1(page0->lru.prev == &page3->lru);
    ok1(page3->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &ram->lru_list);


    /* read page[1]
     *
     * as 3 pages were already allocated it should trigger reclaim (we set up
     * RAMLimited with 3 allocated pages max). Evicted will be page[3] - as it
     * is the only PAGE_LOADED page. */
    diag("read page[1]");
    xvma_on_pagefault(vma, vma->addr_start + 1*PS, 0);

    ok1( M(vma, 0));                B(vma, 0*PSb);                  B(vma, 0*PSb) = 10;
    ok1( M(vma, 1));                B(vma, 1*PSb);      MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    page1 = pagemap_get(&fh->pagemap, 101);
    page3 = pagemap_get(&fh->pagemap, 103);
    ok1(!page3);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page1,   101,    PAGE_LOADED,    1);
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_NOPAGE(         103                      );

    ok1(B(vma, 0*PSb + 0) ==  10);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);
    ok1(B(vma, 1*PSb + 0) == 101);
    ok1(B(vma, 1*PSb + 1) == 101);
    ok1(B(vma, 1*PSb + PSb - 1) == 101);
    ok1(B(vma, 2*PSb + 0) ==  12);
    ok1(B(vma, 2*PSb + 1) == 102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);

    ok1(ram->lru_list.prev == &page1->lru);
    ok1(page1->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &ram->lru_list);


    /* now explicit reclaim - should evict page[1]  (the only PAGE_LOADED page) */
    diag("reclaim");
    ok1(1 == ram_reclaim(ram) );

    ok1( M(vma, 0));                B(vma, 0*PSb);                  B(vma, 0*PSb) = 10;
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    page1 = pagemap_get(&fh->pagemap, 101);
    ok1(!page1);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_NOPAGE(         103                      );

    ok1(B(vma, 0*PSb + 0) ==  10);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);
    ok1(B(vma, 2*PSb + 0) ==  12);
    ok1(B(vma, 2*PSb + 1) == 102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);

    /* page[3] went away */
    ok1(ram->lru_list.prev == &page0->lru);
    ok1(page0->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &ram->lru_list);


    /* unmap vma - dirty pages should stay in fh->pagemap and memory should
     * not be forgotten */
    diag("vma_unmap");
    vma_unmap(vma);

    ok1(list_empty(&fh->mmaps));

    ok1( fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         103                      );

    ok1(ram->lru_list.prev == &page0->lru);
    ok1(page0->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &ram->lru_list);

    b0 = page_mmap(page0, NULL, PROT_READ);
    ok1(b0);
    b2 = page_mmap(page2, NULL, PROT_READ);
    ok1(b2);

    ok1(b0[0] ==  10);
    ok1(b0[1] == 100);
    ok1(b0[PSb - 1] == 100);
    ok1(b2[0] ==  12);
    ok1(b2[1] == 102);
    ok1(b2[PSb - 1] == 102);

    xmunmap(b0, PS);
    xmunmap(b2, PS);


    /* map vma back - dirty pages should be there but not mapped to vma */
    diag("vma mmap again");
    err = fileh_mmap(vma, fh, 100, 4);
    ok1(!err);

    ok1(fh->mmaps.next == &vma->same_fileh);
    ok1(vma->same_fileh.next == &fh->mmaps);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         103                      );

    ok1(ram->lru_list.prev == &page0->lru);
    ok1(page0->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &ram->lru_list);


    /* read access to page[2] - should map it R/W - the page is in PAGE_DIRTY state */
    diag("read page[2]");
    xvma_on_pagefault(vma, vma->addr_start + 2*PS, 0);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_NOPAGE(         103                      );

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* discard - changes should go away */
    diag("discard");
    fileh_dirty_discard(fh);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1(!fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         103                      );

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* writeout in 3 variants - STORE, MARK, STORE+MARK */
    diag("writeout");

    /* storeblk which just remembers which blk was written out */
    blk_t blkv[16];
    size_t blkv_len;
    int storeblk_trace(BigFile *file, blk_t blk, const void *buf)
    {
        ok1(blkv_len < ARRAY_SIZE(blkv));
        blkv[blkv_len++] = blk;
        return 0;
    }
    x_ops.storeblk = storeblk_trace;


    /* read  page[3] (so that we have 1 PAGE_LOADED besides PAGE_DIRTY pages) */
    ok1(!pagemap_get(&fh->pagemap, 103));
    xvma_on_pagefault(vma, vma->addr_start + 3*PS, 0);
    page3 = pagemap_get(&fh->pagemap, 103);
    ok1(page3);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1( M(vma, 3));                B(vma, 3*PSb);      MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1(!fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    ok1(ram->lru_list.prev == &page3->lru);
    ok1(page3->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* prepare state (2 dirty pages, only 1 mapped) */
    void mkdirty2() {
        xvma_on_pagefault(vma, vma->addr_start + 2*PS, 1);  /* write page[2] */
        xvma_on_pagefault(vma, vma->addr_start + 0*PS, 1);  /* write page[0] */
        vma_unmap(vma);
        err = fileh_mmap(vma, fh, 100, 4);
        ok1(!err);
        xvma_on_pagefault(vma, vma->addr_start + 2*PS, 0);

        ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
        ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
        ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
        ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

        ok1( fh->dirty);
        CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
        CHECK_NOPAGE(         101                      );
        CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
        CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

        ok1(ram->lru_list.prev == &page2->lru);
        ok1(page2->lru.prev == &page0->lru);
        ok1(page0->lru.prev == &page3->lru);
        ok1(page3->lru.prev == &ram->lru_list);
    }

    diag("writeout (store)");
    mkdirty2();
    blkv_len = 0;
    ok1(!fileh_dirty_writeout(fh, WRITEOUT_STORE));

    ok1(blkv_len == 2);
    ok1(blkv[0] == 100);
    ok1(blkv[1] == 102);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);


    diag("writeout (mark)");
    blkv_len = 0;
    ok1(!fileh_dirty_writeout(fh, WRITEOUT_MARKSTORED));

    ok1(blkv_len == 0);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);      MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1(!fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_LOADED,    1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &page3->lru);
    ok1(page3->lru.prev == &ram->lru_list);


    diag("writeout (store+mark)");
    mkdirty2();
    blkv_len = 0;
    ok1(!fileh_dirty_writeout(fh, WRITEOUT_STORE | WRITEOUT_MARKSTORED));

    ok1(blkv_len == 2);
    ok1(blkv[0] == 100);
    ok1(blkv[1] == 102);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);      MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1(!fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_LOADED,    1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &page3->lru);
    ok1(page3->lru.prev == &ram->lru_list);


    diag("invalidate");
    mkdirty2();

    fileh_invalidate_page(fh, 101);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &page3->lru);
    ok1(page3->lru.prev == &ram->lru_list);


    fileh_invalidate_page(fh, 103);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page3,   103,    PAGE_EMPTY,     0);

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &page3->lru);
    ok1(page3->lru.prev == &ram->lru_list);


    fileh_invalidate_page(fh, 102);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1( fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_PAGE  (page3,   103,    PAGE_EMPTY,     0);

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &page3->lru);
    ok1(page3->lru.prev == &ram->lru_list);


    fileh_invalidate_page(fh, 100);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1(!fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_PAGE  (page3,   103,    PAGE_EMPTY,     0);

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &page3->lru);
    ok1(page3->lru.prev == &ram->lru_list);

    /* read page[3] back */
    xvma_on_pagefault(vma, vma->addr_start + 3*PS, 0);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1( M(vma, 3));                B(vma, 3*PSb);      MUST_FAULT( B(vma, 3*PSb) = 13  );

    ok1(!fh->dirty);
    CHECK_PAGE  (page0,   100,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    ok1(ram->lru_list.prev == &page3->lru);
    ok1(page3->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    diag("fileh_close");
    /* dirty some pages again - so that test fileh_close with not all pages being non-dirty */
    mkdirty2();
    vma_unmap(vma);

    /* ensure pages stay in ram lru with expected state */
    ok1(ram->lru_list.prev == &page2->lru);     ok1(page2->state == PAGE_DIRTY);
    ok1(page2->lru.prev == &page0->lru);        ok1(page0->state == PAGE_DIRTY);
    ok1(page0->lru.prev == &page3->lru);        ok1(page3->state == PAGE_LOADED);
    ok1(page3->lru.prev == &ram->lru_list);

    fileh_close(fh);

    /* pages associated with fileh should go away after fileh_close() */
    ok1(list_empty(&ram->lru_list));


    /* free resources & restore SIGSEGV handler */
    ram_close(ram);
    ram_close(ram0);

    ok1(!sigaction(SIGSEGV, &saveact, NULL));

#undef  CHECK_PAGE
#undef  CHECK_NOPAGE
}


/* file access via real pagefault
 *
 * this test tests that SIGSEGV pagefault handler works and only that. Most of
 * virtual memory behaviour is more explicitly tested in
 * test_file_access_synthetic().
 */
void test_file_access_pagefault()
{
    RAM *ram;
    BigFileH fh_struct, *fh = &fh_struct;
    VMA vma_struct, *vma = &vma_struct;
    Page *page0, *page2, *page3;
    size_t PS, PSb;
    int err;

    diag("Testing file access (pagefault)");

    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    ram = ram_new(NULL,NULL);
    ok1(ram);
    PS = ram->pagesize;
    PSb = PS / sizeof(blk_t);   /* page size in blk_t units */

    /* ensure we are starting from new ram */
    ok1(list_empty(&ram->lru_list));

    /* setup id file */
    BigFileIdentity fileid = {
        .blksize    = ram->pagesize,    /* artificially blksize = pagesize */
        .file_ops   = &fileid_ops,
    };

    err = fileh_open(fh, &fileid, ram);
    ok1(!err);

/* implicitly use fileh=fh */
#define CHECK_PAGE(page, pgoffset, pgstate, pgrefcnt)   \
                __CHECK_PAGE(page, fh, pgoffset, pgstate, pgrefcnt)
#define CHECK_NOPAGE(pgoffset)  __CHECK_NOPAGE(fh, pgoffset)

    err = fileh_mmap(vma, fh, 100, 4);
    ok1(!err);

    /* all pages initially unmapped */
    ok1(!M(vma, 0));    CHECK_NOPAGE(       100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(       101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(       102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(       103                      );

    ok1(list_empty(&ram->lru_list));

    /* read page[0] */
    ok1(B(vma, 0*PSb) == 100);

    page0 = pagemap_get(&fh->pagemap, 100);
    ok1( M(vma, 0));    CHECK_PAGE  (page0, 100,    PAGE_LOADED,    1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(       101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(       102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(       103                      );

    ok1(ram->lru_list.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* write to page[2] */
    B(vma, 2*PSb) = 12;

    page2 = pagemap_get(&fh->pagemap, 102);
    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    ok1(ram->lru_list.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* read page[3] */
    ok1(B(vma, 3*PSb) == 103);

    page3 = pagemap_get(&fh->pagemap, 103);
    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1( M(vma, 3));    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    ok1(ram->lru_list.prev == &page3->lru);
    ok1(page3->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &page0->lru);
    ok1(page0->lru.prev == &ram->lru_list);


    /* write to page[0] */
    B(vma, 0*PSb) = 10;

    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                       );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1( M(vma, 3));    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    ok1(ram->lru_list.prev == &page0->lru); /* page0 became MRU */
    ok1(page0->lru.prev == &page3->lru);
    ok1(page3->lru.prev == &page2->lru);
    ok1(page2->lru.prev == &ram->lru_list);


    /* unmap vma */
    vma_unmap(vma);


    /* free resources */
    fileh_close(fh);
    // ok1(list_empty(&ram->lru_list));
    ram_close(ram);
}


/*
 * test that pagefault saves/restores thread state correctly
 */

void test_pagefault_savestate()
{
    RAM *ram;
    BigFileH fh_struct, *fh = &fh_struct;
    VMA vma_struct, *vma = &vma_struct;
    int err;

    diag("Testing how pagefault handler saves/restores thread state");

    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    ram = ram_new(NULL,NULL);
    ok1(ram);

    /* setup bad file */
    volatile int loadblk_run;
    int badfile_loadblk(BigFile *file, blk_t blk, void *buf)
    {
        /* we are bad file - just say everything is ok... */

        /* and before that corrup thread state - to verify that pagefault handler
         * will restore it. */
        errno = 98;
        /* Also tell we were here via, so that the test can be sure we actually
         * tried to make things go bad. */
        loadblk_run = 1;

        return 0;
    }

    const struct bigfile_ops badfile_ops = {
        .loadblk    = badfile_loadblk,
    };

    BigFile f = {
        .blksize    = ram->pagesize,    /* artificial */
        .file_ops   = &badfile_ops,
    };

    err = fileh_open(fh, &f, ram);
    ok1(!err);

    err = fileh_mmap(vma, fh, 0, 1);
    ok1(!err);

    /* before we touched anything */
    errno = 1;
    loadblk_run = 0;
    ok1(errno == 1);
    ok1(!loadblk_run);

    /* read page[0] - it should trigger badfile_loadblk() */
    ok1(B(vma, 0) == 0);

    ok1(loadblk_run);
    ok1(errno == 1);


    /* free resources */
    vma_unmap(vma);
    fileh_close(fh);
    ram_close(ram);

#undef  CHECK_PAGE
#undef  CHECK_NOPAGE
}



// TODO test for loadblk that returns -1

int main()
{
    tap_fail_callback = abort;  // XXX to catch failure immediately

    test_vmamap();
    test_file_access_synthetic();
    test_file_access_pagefault();
    test_pagefault_savestate();
    return 0;
}
