/* Wendelin.bigfile | virtual memory tests
 * Copyright (C) 2014-2021  Nexedi SA and Contributors.
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
#include <setjmp.h>
#include <signal.h>
#include <errno.h>

#include "../../t/t_utils.h"
#include   "../../t/t_utils.c"

/* test_vmamap verifies addr -> VMA lookup. */
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

/* tell TSAN we are OK with calling async-sig-unsafe functions from sync SIGSEGV */
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

/* check that fileh->pagemap[pgoffset] is empty */
#define __CHECK_NOPAGE(fileh, pgoffset) do {            \
    ok1(!pagemap_get(&(fileh)->pagemap, (pgoffset)));   \
} while (0)


/* _pagev_str returns string representation for vector of pages.
 * returned memory has to be freed by user. */
char *_pagev_str(Page **pagev, int pagec) {
        char *vstr; size_t _;
        FILE *w = open_memstream(&vstr, &_);

        for (int i=0; i<pagec; i++)
            fprintf(w, "%sp%ld%s", (i == 0 ? "[" : ", "),
                    pagev[i]->f_pgoffset, (i == pagec - 1 ? "]" : ""));
        fclose(w);
        return vstr;
}

/* _assert_pagev asserts that two page vectors are the same */
void _assert_pagev(const char *subj, Page **vok, int nok, Page **pagev, int n,
                   const char *func, const char *file, int line)
{
    char *vstr = _pagev_str(pagev, n);

    if (!(n == nok && !memcmp(pagev, vok, n*sizeof(*pagev)))) {
        char *vstr_ok = _pagev_str(vok, nok);
        fprintf(stderr, "%s: failed\n", subj);
        fprintf(stderr, "have: %s\n", vstr);
        fprintf(stderr, "want: %s\n", vstr_ok);
        _gen_result(0, func, file, line, "%s failed", subj);
        free(vstr_ok);
    } else {
        pass("%s %s", subj, vstr);
    }

    free(vstr);
}

/* _check_mru checks that ram has MRU pages as specified by mruok. */
void _check_mru(RAM *ram, Page *mruok[], int nok, const char *func, const char *file, int line) {
    Page **mruv = NULL;
    int n = 0;
    struct list_head *h;

    // collect mruv
    list_for_each_backwardly(h, &ram->lru_list) {
        n++;
        mruv = realloc(mruv, n*sizeof(*mruv));
        mruv[n-1] = list_entry(h, Page, lru);
    }

    _assert_pagev("check_mru", mruok, nok, mruv, n, func, file, line);
    free(mruv);
}

/* __CHECK_MRU(ram, ...pagev) - assert that ram has MRU pages as expected */
#define __CHECK_MRU(ram, ...) do {                                                  \
    Page *__mruok[] = {__VA_ARGS__};                                                \
    _check_mru(ram, __mruok, ARRAY_SIZE(__mruok), __func__, __FILE__, __LINE__);    \
} while(0)


/* _check_dirty checks that fileh has dirty pages as specified.
 * the order of dirty list traversal is to go through most recently dirtied pages first. */
void _check_dirty(BigFileH *fileh, Page *dirtyok[], int nok, const char *func, const char *file, int line) {
    Page **dirtyv = NULL;
    int n = 0;
    struct list_head *h;

    // collect dirtyv
    list_for_each_backwardly(h, &fileh->dirty_pages) {
        n++;
        dirtyv = realloc(dirtyv, n*sizeof(*dirtyv));
        dirtyv[n-1] = list_entry(h, Page, in_dirty);
    }

    _assert_pagev("check_dirty", dirtyok, nok, dirtyv, n, func, file, line);
    free(dirtyv);

}

/* __CHECK_DIRTY(fileh, ...pagev) - assert that fileh has dirty pages as expected */
#define __CHECK_DIRTY(fileh, ...) do {                                                      \
    Page *__dirtyok[] = {__VA_ARGS__};                                                      \
    _check_dirty(fileh, __dirtyok, ARRAY_SIZE(__dirtyok), __func__, __FILE__, __LINE__);    \
} while(0)




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

    /* MUST_FAULT(code) - checks that code faults  */
    /* somewhat dup in wcfs/internal/wcfs_test.pyx */
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

/* implicitly use ram=ram */
#define CHECK_MRU(...)  __CHECK_MRU(ram, __VA_ARGS__)

    /* ensure we are starting from new ram */
    CHECK_MRU(/*empty*/);

    /* setup id file */
    struct bigfile_ops x_ops = {.loadblk = fileid_loadblk};
    BigFileIdentity fileid = {
        .blksize    = ram->pagesize,    /* artificially blksize = pagesize */
        .file_ops   = &x_ops,
    };

    err = fileh_open(fh, &fileid, ram, DONT_MMAP_OVERLAY);
    ok1(!err);
    ok1(list_empty(&fh->mmaps));

/* implicitly use fileh=fh */
#define CHECK_PAGE(page, pgoffset, pgstate, pgrefcnt)   \
                __CHECK_PAGE(page, fh, pgoffset, pgstate, pgrefcnt)
#define CHECK_NOPAGE(pgoffset)  __CHECK_NOPAGE(fh, pgoffset)
#define CHECK_DIRTY(...)        __CHECK_DIRTY(fh, __VA_ARGS__)


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

    CHECK_NOPAGE(         100                      );
    CHECK_NOPAGE(         101                      );
    CHECK_NOPAGE(         102                      );
    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (/*empty*/);
    CHECK_DIRTY (/*empty */);


    /* simulate read access to page[0] - it should load it */
    diag("read page[0]");
    xvma_on_pagefault(vma, vma->addr_start + 0*PS, 0);

    ok1( M(vma, 0));                B(vma, 0*PSb);      MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    page0 = pagemap_get(&fh->pagemap, 100);
    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    1);
    CHECK_NOPAGE(         101                      );
    CHECK_NOPAGE(         102                      );
    CHECK_NOPAGE(         103                      );

    ok1(B(vma, 0*PSb + 0) == 100);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);

    CHECK_MRU   (page0);
    CHECK_DIRTY (/*empty*/);


    /* simulate write access to page[2] - it should load it and mark page dirty */
    diag("write page[2]");
    xvma_on_pagefault(vma, vma->addr_start + 2*PS, 1);

    ok1( M(vma, 0));                B(vma, 0*PSb);      MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

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

    CHECK_MRU   (page2, page0);
    CHECK_DIRTY (page2);


    /* read access to page[3] - load */
    diag("read page[3]");
    xvma_on_pagefault(vma, vma->addr_start + 3*PS, 0);

    ok1( M(vma, 0));                B(vma, 0*PSb);      MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1( M(vma, 3));                B(vma, 3*PSb);      MUST_FAULT( B(vma, 3*PSb) = 13  );

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

    CHECK_MRU   (page3, page2, page0);
    CHECK_DIRTY (page2);


    /* write access to page[0] - upgrade loaded -> dirty */
    diag("write page[0]");
    xvma_on_pagefault(vma, vma->addr_start + 0*PS, 1);

    ok1( M(vma, 0));                B(vma, 0*PSb);                  B(vma, 0*PSb) = 10;
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1( M(vma, 3));                B(vma, 3*PSb);      MUST_FAULT( B(vma, 3*PSb) = 13  );

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

    CHECK_MRU   (page0, page3, page2);    /* page0 became MRU */
    CHECK_DIRTY (page0, page2);


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

    CHECK_MRU   (page1, page0, page2);
    CHECK_DIRTY (page0, page2);


    /* now explicit reclaim - should evict page[1]  (the only PAGE_LOADED page) */
    diag("reclaim");
    ok1(1 == ram_reclaim(ram) );

    ok1( M(vma, 0));                B(vma, 0*PSb);                  B(vma, 0*PSb) = 10;
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

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
    CHECK_MRU   (page0, page2);
    CHECK_DIRTY (page0, page2);


    /* unmap vma - dirty pages should stay in fh->pagemap and memory should
     * not be forgotten */
    diag("vma_unmap");
    vma_unmap(vma);

    ok1(list_empty(&fh->mmaps));

    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (page0, page2);
    CHECK_DIRTY (page0, page2);

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

    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (page0, page2);
    CHECK_DIRTY (page0, page2);


    /* read access to page[2] - should map it R/W - the page is in PAGE_DIRTY state */
    diag("read page[2]");
    xvma_on_pagefault(vma, vma->addr_start + 2*PS, 0);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (page2, page0);
    CHECK_DIRTY (page0, page2);


    /* discard - changes should go away */
    diag("discard");
    fileh_dirty_discard(fh);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    CHECK_PAGE  (page0,   100,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (page2, page0);
    CHECK_DIRTY (/*empty*/);


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

    CHECK_PAGE  (page0,   100,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    CHECK_MRU   (page3, page2, page0);
    CHECK_DIRTY (/*empty*/);


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

        CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
        CHECK_NOPAGE(         101                      );
        CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
        CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

        CHECK_MRU   (page2, page0, page3);
        CHECK_DIRTY (page0, page2);
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

    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

    /* NOTE - becomes sorted by ->f_pgoffset */
    CHECK_DIRTY (page2, page0); /* checked in reverse order */

    diag("writeout (mark)");
    blkv_len = 0;
    ok1(!fileh_dirty_writeout(fh, WRITEOUT_MARKSTORED));

    ok1(blkv_len == 0);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);      MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_LOADED,    1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

    CHECK_MRU   (page2, page0, page3);
    CHECK_DIRTY (/*empty*/);

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

    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_LOADED,    1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

    CHECK_MRU   (page2, page0, page3);
    CHECK_DIRTY (/*empty*/);

    /* invalidation */
    diag("invalidate");
    mkdirty2();

    fileh_invalidate_page(fh, 101);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    0);

    CHECK_MRU   (page2, page0, page3);
    CHECK_DIRTY (page0, page2);


    fileh_invalidate_page(fh, 103);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1( M(vma, 2));                B(vma, 2*PSb);                  B(vma, 2*PSb) = 12;
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    CHECK_PAGE  (page3,   103,    PAGE_EMPTY,     0);

    CHECK_MRU   (page2, page0, page3);
    CHECK_DIRTY (page0, page2);


    fileh_invalidate_page(fh, 102);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_PAGE  (page3,   103,    PAGE_EMPTY,     0);

    CHECK_MRU   (page2, page0, page3);
    CHECK_DIRTY (page0);


    fileh_invalidate_page(fh, 100);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1(!M(vma, 3));    MUST_FAULT( B(vma, 3*PSb) );    MUST_FAULT( B(vma, 3*PSb) = 13  );

    CHECK_PAGE  (page0,   100,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_PAGE  (page3,   103,    PAGE_EMPTY,     0);

    CHECK_MRU   (page2, page0, page3);
    CHECK_DIRTY (/*empty*/);

    /* read page[3] back */
    xvma_on_pagefault(vma, vma->addr_start + 3*PS, 0);

    ok1(!M(vma, 0));    MUST_FAULT( B(vma, 0*PSb) );    MUST_FAULT( B(vma, 0*PSb) = 10  );
    ok1(!M(vma, 1));    MUST_FAULT( B(vma, 1*PSb) );    MUST_FAULT( B(vma, 1*PSb) = 11  );
    ok1(!M(vma, 2));    MUST_FAULT( B(vma, 2*PSb) );    MUST_FAULT( B(vma, 2*PSb) = 12  );
    ok1( M(vma, 3));                B(vma, 3*PSb);      MUST_FAULT( B(vma, 3*PSb) = 13  );

    CHECK_PAGE  (page0,   100,    PAGE_EMPTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_EMPTY,     0);
    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    CHECK_MRU   (page3, page2, page0);
    CHECK_DIRTY (/*empty*/);


    diag("fileh_close");
    /* dirty some pages again - so that test fileh_close with not all pages being non-dirty */
    mkdirty2();
    vma_unmap(vma);

    /* ensure pages stay in ram lru with expected state */
    CHECK_MRU(page2, page0, page3);
    ok1(page2->state == PAGE_DIRTY);
    ok1(page0->state == PAGE_DIRTY);
    ok1(page3->state == PAGE_LOADED);

    fileh_close(fh);

    /* pages associated with fileh should go away after fileh_close() */
    CHECK_MRU(/*empty*/);


    /* free resources & restore SIGSEGV handler */
    ram_close(ram);  free(ram);
    ram_close(ram0); free(ram0);

    ok1(!sigaction(SIGSEGV, &saveact, NULL));

#undef  CHECK_MRU
#undef  CHECK_PAGE
#undef  CHECK_NOPAGE
#undef  CHECK_DIRTY
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

/* implicitly use ram=ram */
#define CHECK_MRU(...)  __CHECK_MRU(ram, __VA_ARGS__)

    /* ensure we are starting from new ram */
    CHECK_MRU(/*empty*/);

    /* setup id file */
    BigFileIdentity fileid = {
        .blksize    = ram->pagesize,    /* artificially blksize = pagesize */
        .file_ops   = &fileid_ops,
    };

    err = fileh_open(fh, &fileid, ram, DONT_MMAP_OVERLAY);
    ok1(!err);

/* implicitly use fileh=fh */
#define CHECK_PAGE(page, pgoffset, pgstate, pgrefcnt)   \
                __CHECK_PAGE(page, fh, pgoffset, pgstate, pgrefcnt)
#define CHECK_NOPAGE(pgoffset)  __CHECK_NOPAGE(fh, pgoffset)
#define CHECK_DIRTY(...)        __CHECK_DIRTY(fh, __VA_ARGS__)

    err = fileh_mmap(vma, fh, 100, 4);
    ok1(!err);

    /* all pages initially unmapped */
    ok1(!M(vma, 0));    CHECK_NOPAGE(       100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(       101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(       102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(       103                      );

    CHECK_MRU(/*empty*/);

    /* read page[0] */
    ok1(B(vma, 0*PSb) == 100);

    page0 = pagemap_get(&fh->pagemap, 100);
    ok1( M(vma, 0));    CHECK_PAGE  (page0, 100,    PAGE_LOADED,    1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(       101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(       102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(       103                      );

    CHECK_MRU(page0);


    /* write to page[2] */
    B(vma, 2*PSb) = 12;

    page2 = pagemap_get(&fh->pagemap, 102);
    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    CHECK_MRU(page2, page0);


    /* read page[3] */
    ok1(B(vma, 3*PSb) == 103);

    page3 = pagemap_get(&fh->pagemap, 103);
    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_LOADED,    1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1( M(vma, 3));    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    CHECK_MRU(page3, page2, page0);


    /* write to page[0] */
    B(vma, 0*PSb) = 10;

    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                       );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1( M(vma, 3));    CHECK_PAGE  (page3,   103,    PAGE_LOADED,    1);

    CHECK_MRU(page0, page3, page2);    /* page0 became MRU */


    /* unmap vma */
    vma_unmap(vma);


    /* free resources */
    fileh_close(fh);
    // ok1(list_empty(&ram->lru_list));
    ram_close(ram);
    free(ram);

#undef  CHECK_MRU
#undef  CHECK_PAGE
#undef  CHECK_NOPAGE
#undef  CHECK_DIRTY
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

        /* and before that corrupt thread state - to verify that pagefault handler
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

    err = fileh_open(fh, &f, ram, DONT_MMAP_OVERLAY);
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
    free(ram);
}

/* ---------------------------------------- */

/* test access to file mappings with file having .mmap* instead of .loadblk
 *
 * "mmap overlay" is virtmem mode used with wcfs: RAM pages are used only for
 * dirtied data and everything else comes as read-only mmap from wcfs file.
 */

/* BigFileMMap is BigFile that mmaps blkdata for read from a regular file.
 *
 * Store, contrary to load, is done via regular file writes. */
struct BigFileMMap {
    BigFile;
    int fd;         /* fd of file to mmap */
    int nstoreblk;  /* number of times storeblk         called  */
    int nremmapblk; /* ----//----      remmap_blk_read  called  */
    int nmunmap;    /* ----//----      munmap           called  */
};
typedef struct BigFileMMap BigFileMMap;

void mmapfile_release(BigFile *file) {
    BigFileMMap *f = upcast(BigFileMMap*, file);
    int err;

    err = close(f->fd);
    BUG_ON(err);
}

int mmapfile_storeblk(BigFile *file, blk_t blk, const void *buf) {
    BigFileMMap *f = upcast(BigFileMMap*, file);
    size_t n = f->blksize;
    off_t at = blk*f->blksize;

    f->nstoreblk++;

    while (n > 0) {
        ssize_t wrote;
        wrote = pwrite(f->fd, buf, n, at);
        if (wrote == -1)
            return -1;

        BUG_ON(wrote > n);
        n -= wrote;
        buf += wrote;
        at  += wrote;
    }

    return 0;
}

int mmapfile_mmap_setup_read(VMA *vma, BigFile *file, blk_t blk, size_t blklen) {
    BigFileMMap *f = upcast(BigFileMMap*, file);
    size_t len = blklen*f->blksize;
    void *addr;

    addr = mmap(NULL, len, PROT_READ, MAP_SHARED, f->fd, blk*f->blksize);
    if (addr == MAP_FAILED)
        return -1;

    vma->addr_start = (uintptr_t)addr;
    vma->addr_stop  = vma->addr_start + len;
    return 0;
}

int mmapfile_remmap_blk_read(VMA *vma, BigFile *file, blk_t blk) {
    BigFileMMap *f = upcast(BigFileMMap*, file);
    TODO (f->blksize != vma->fileh->ramh->ram->pagesize);
    ASSERT(vma->f_pgoffset <= blk && blk < vma_addr_fpgoffset(vma, vma->addr_stop));
    pgoff_t pgoff_invma = blk - vma->f_pgoffset;
    uintptr_t addr = vma->addr_start + pgoff_invma*f->blksize;
    void *mapped;

    f->nremmapblk++;
    mapped = mmap((void *)addr, 1*f->blksize, PROT_READ, MAP_SHARED | MAP_FIXED, f->fd, blk*f->blksize);
    if (mapped == MAP_FAILED)
        return -1;

    ASSERT(mapped == (void *)addr);
    return 0;
}

int mmapfile_munmap(VMA *vma, BigFile *file) {
    BigFileMMap *f = upcast(BigFileMMap*, file);
    size_t len = vma->addr_stop - vma->addr_start;

    f->nmunmap++;
    xmunmap((void *)vma->addr_start, len);
    return 0;
}

static const struct bigfile_ops mmapfile_ops = {
    .loadblk            = NULL,
    .storeblk           = mmapfile_storeblk,
    .mmap_setup_read    = mmapfile_mmap_setup_read,
    .remmap_blk_read    = mmapfile_remmap_blk_read,
    .munmap             = mmapfile_munmap,
    .release            = mmapfile_release,
};


/* verify virtmem behaviour when it is given BigFile with .mmap_* to handle data load. */
void test_file_access_mmapoverlay(void)
{
    RAM *ram;
    BigFileH fh_struct, *fh = &fh_struct;
    VMA vma_struct, *vma = &vma_struct;
    VMA vma2_struct, *vma2 = &vma2_struct;
    Page *page0, *page2, *page3;
    blk_t *b0, *b2;
    size_t PS, PSb;
    int fd, err;

    diag("Testing file access (mmap base)");

    // XXX save/restore sigaction ?
    ok1(!pagefault_init());

    ram = ram_new(NULL, NULL);
    ok1(ram);
    PS = ram->pagesize;
    PSb = PS / sizeof(blk_t);   /* page size in blk_t units */

/* implicitly use ram=ram */
#define CHECK_MRU(...)  __CHECK_MRU(ram, __VA_ARGS__)

    /* ensure we are starting from new ram */
    CHECK_MRU(/*empty*/);

    /* setup mmaped file */
    char path[] = "/tmp/bigfile_mmap.XXXXXX";
    fd = mkstemp(path);
    ok1(fd != -1);
    err = unlink(path);
    ok1(!err);

    BigFileMMap file = {
        .blksize    = ram->pagesize,    /* artificially blksize = pagesize */
        .file_ops   = &mmapfile_ops,
        .fd         = fd,
        .nstoreblk  = 0,
        .nremmapblk = 0,
        .nmunmap    = 0,
    };

    /* fstore stores data into file[blk] */
    void fstore(blk_t blk, blk_t data) {
        blk_t *buf;
        int i;
        buf = malloc(file.blksize);
        BUG_ON(!buf);
        for (i=0; i < file.blksize/sizeof(*buf); i++)
            buf[i] = data;
        err = file.file_ops->storeblk(&file, blk, buf);
        BUG_ON(err);
        free(buf);
    }

    /* initialize file[100 +4) */
    fstore(100, 100);
    fstore(101, 101);
    fstore(102, 102);
    fstore(103, 103);


    err = fileh_open(fh, &file, ram, MMAP_OVERLAY);
    ok1(!err);

/* implicitly use fileh=fh */
#define CHECK_PAGE(page, pgoffset, pgstate, pgrefcnt)   \
                __CHECK_PAGE(page, fh, pgoffset, pgstate, pgrefcnt)
#define CHECK_NOPAGE(pgoffset)  __CHECK_NOPAGE(fh, pgoffset)
#define CHECK_DIRTY(...)        __CHECK_DIRTY(fh, __VA_ARGS__)

    err = fileh_mmap(vma, fh, 100, 4);
    ok1(!err);

    ok1(fh->mmaps.next == &vma->same_fileh);
    ok1(vma->same_fileh.next == &fh->mmaps);

    /* all pages initially unmapped */
    ok1(!M(vma, 0));    CHECK_NOPAGE(       100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(       101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(       102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(       103                      );

    CHECK_MRU   (/*empty*/);
    CHECK_DIRTY (/*empty*/);

    /* read page[0] - served from base mmap and no RAM page is loaded */
    ok1(B(vma, 0*PSb + 0) == 100);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);

    ok1(!M(vma, 0));    CHECK_NOPAGE(       100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(       101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(       102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(       103                      );

    CHECK_MRU   (/*empty*/);
    CHECK_DIRTY (/*empty*/);

    /* write to page[2] - page2 is copy-on-write created in RAM */
    B(vma, 2*PSb) = 12;

    page2 = pagemap_get(&fh->pagemap, 102);
    ok1(!M(vma, 0));    CHECK_NOPAGE(         100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    ok1(B(vma, 2*PSb + 0) ==  12); /* set by write */
    ok1(B(vma, 2*PSb + 1) == 102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);

    CHECK_MRU   (page2);
    CHECK_DIRTY (page2);

    /* read page[3] - served from base mmap */
    ok1(B(vma, 3*PSb + 0) == 103);
    ok1(B(vma, 3*PSb + 1) == 103);
    ok1(B(vma, 3*PSb + PSb - 1) == 103);

    ok1(!M(vma, 0));    CHECK_NOPAGE(         100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (page2);
    CHECK_DIRTY (page2);

    /* write to page[0] - page COW'ed into RAM */
    B(vma, 0*PSb) = 10;

    page0 = pagemap_get(&fh->pagemap, 100);
    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    ok1(B(vma, 0*PSb + 0) ==  10); /* set by write */
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);

    CHECK_MRU   (page0, page2);
    CHECK_DIRTY (page0, page2);


    /* unmap vma - dirty pages should stay in fh->pagemap and memory should
     * not be forgotten */
    diag("vma_unmap");
    vma_unmap(vma);

    ok1(list_empty(&fh->mmaps));

    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         101                      );
    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     0);
    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (page0, page2);
    CHECK_DIRTY (page0, page2);

    b0 = page_mmap(page0, NULL, PROT_READ); ok1(b0);
    b2 = page_mmap(page2, NULL, PROT_READ); ok1(b2);

    ok1(b0[0] ==  10);
    ok1(b0[1] == 100);
    ok1(b0[PSb - 1] == 100);
    ok1(b2[0] ==  12);
    ok1(b2[1] == 102);
    ok1(b2[PSb - 1] == 102);

    xmunmap(b0, PS);
    xmunmap(b2, PS);


    /* map vma back - dirty pages should be there _and_ mapped to vma.
     * (this differs from !wcfs case which does not mmap dirty pages until access) */
    diag("vma mmap again");
    err = fileh_mmap(vma, fh, 100, 4);
    ok1(!err);

    ok1(fh->mmaps.next == &vma->same_fileh);
    ok1(vma->same_fileh.next == &fh->mmaps);

    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (page0, page2);
    CHECK_DIRTY (page0, page2);

    /* dirtying a page in one mapping should automatically mmap the dirty page
     * in all other wcfs mappings */
    diag("dirty page in vma2 -> dirties vma1");
    err = fileh_mmap(vma2, fh, 100, 4);
    ok1(!err);

    ok1(fh->mmaps.next == &vma->same_fileh);
    ok1(vma->same_fileh.next == &vma2->same_fileh);
    ok1(vma2->same_fileh.next == &fh->mmaps);

    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     2);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     2);
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    ok1( M(vma2, 0));
    ok1(!M(vma2, 1));
    ok1( M(vma2, 2));
    ok1(!M(vma2, 3));

    CHECK_MRU   (page0, page2);
    CHECK_DIRTY (page0, page2);

    B(vma2, 3*PSb) = 13;    /* write to page[3] via vma2 */

    page3 = pagemap_get(&fh->pagemap, 103);
    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     2);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     2);
    ok1( M(vma, 3));    CHECK_PAGE  (page3,   103,    PAGE_DIRTY,     2);

    ok1( M(vma2, 0));
    ok1(!M(vma2, 1));
    ok1( M(vma2, 2));
    ok1( M(vma2, 3));

    ok1(B(vma,  3*PSb + 0) ==  13); /* set by write */
    ok1(B(vma,  3*PSb + 1) == 103);
    ok1(B(vma,  3*PSb + PSb - 1) == 103);

    ok1(B(vma2, 3*PSb + 0) ==  13); /* set by write */
    ok1(B(vma2, 3*PSb + 1) == 103);
    ok1(B(vma2, 3*PSb + PSb - 1) == 103);

    CHECK_MRU   (page3, page0, page2);
    CHECK_DIRTY (page3, page0, page2);

    /* unmap vma2 */
    diag("unmap vma2");
    vma_unmap(vma2);

    ok1(fh->mmaps.next == &vma->same_fileh);
    ok1(vma->same_fileh.next == &fh->mmaps);

    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1( M(vma, 2));    CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     1);
    ok1( M(vma, 3));    CHECK_PAGE  (page3,   103,    PAGE_DIRTY,     1);

    CHECK_MRU   (page3, page0, page2);
    CHECK_DIRTY (page3, page0, page2);


    /* discard - changes should go away */
    diag("discard");
    ok1(file.nremmapblk == 0);
    fileh_dirty_discard(fh);
    ok1(file.nremmapblk == 3); /* 3 previously dirty pages remmaped from base layer */

    CHECK_NOPAGE(         100                      );

    ok1(!M(vma, 0));    CHECK_NOPAGE(         100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(         102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    CHECK_MRU   (/*empty*/);
    CHECK_DIRTY (/*empty*/);

    /* discarded pages should read from base layer again */
    ok1(B(vma, 0*PSb + 0) == 100);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);

    ok1(B(vma, 2*PSb + 0) == 102);
    ok1(B(vma, 2*PSb + 1) == 102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);

    ok1(B(vma, 3*PSb + 0) == 103);
    ok1(B(vma, 3*PSb + 1) == 103);
    ok1(B(vma, 3*PSb + PSb - 1) == 103);

    ok1(!M(vma, 0));    CHECK_NOPAGE(         100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(         102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(         103                      );

    /* writeout in 3 variants - STORE, MARK, STORE+MARK */
    diag("writeout");

    /* mkdirty2 prepares state with 2 dirty pages only 1 of which is mapped */
    void mkdirty2(int gen) {
        vma_unmap(vma);
        CHECK_NOPAGE(         100                      );
        CHECK_NOPAGE(         101                      );
        CHECK_NOPAGE(         102                      );
        CHECK_NOPAGE(         103                      );
        page0 = page2 = page3 = NULL;

        err = fileh_mmap(vma, fh, 100, 4);
        ok1(!err);

        B(vma, 2*PSb) = gen + 2;
        B(vma, 0*PSb) = gen + 0;
        vma_unmap(vma);
        page0 = pagemap_get(&fh->pagemap, 100); ok1(page0);
        page2 = pagemap_get(&fh->pagemap, 102); ok1(page2);

        err = fileh_mmap(vma, fh, 100, 2); /* note - only 2 pages */
        ok1(!err);

        ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
        ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
                            CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     0);
                            CHECK_NOPAGE(         103                      );

        CHECK_MRU   (page0, page2);
        CHECK_DIRTY (page0, page2);
    }

    diag("writeout (store)");
    file.nstoreblk  = 0;
    file.nremmapblk = 0;
    mkdirty2(10);
    ok1(!fileh_dirty_writeout(fh, WRITEOUT_STORE));
    ok1(file.nstoreblk  == 2);
    ok1(file.nremmapblk == 0);

    ok1( M(vma, 0));    CHECK_PAGE  (page0,   100,    PAGE_DIRTY,     1);
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
                        CHECK_PAGE  (page2,   102,    PAGE_DIRTY,     0);
                        CHECK_NOPAGE(         103                      );

    CHECK_MRU   (page0, page2);
    CHECK_DIRTY (page2, page0); /* note becomes sorted by f_pgoffset */

    ok1(B(vma, 0*PSb + 0) ==  10);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);

    b0 = page_mmap(page0, NULL, PROT_READ); ok1(b0);
    b2 = page_mmap(page2, NULL, PROT_READ); ok1(b2);

    ok1(b0[0] ==  10);
    ok1(b0[1] == 100);
    ok1(b0[PSb - 1] == 100);
    ok1(b2[0] ==  12);
    ok1(b2[1] == 102);
    ok1(b2[PSb - 1] == 102);

    xmunmap(b0, PS);
    xmunmap(b2, PS);

    diag("writeout (mark)");
    file.nstoreblk  = 0;
    file.nremmapblk = 0;
    ok1(!fileh_dirty_writeout(fh, WRITEOUT_MARKSTORED));
    ok1(file.nstoreblk  == 0);
    ok1(file.nremmapblk == 1); /* only 1 (not 2) page was mmaped */

    ok1(!M(vma, 0));    CHECK_NOPAGE(         100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
                        CHECK_NOPAGE(         102                      );
                        CHECK_NOPAGE(         103                      );

    CHECK_MRU   (/*empty*/);
    CHECK_DIRTY (/*empty*/);

    vma_unmap(vma);
    err = fileh_mmap(vma, fh, 100, 4);

    /* data saved; served from base layer */
    ok1(B(vma, 0*PSb + 0) ==  10);
    ok1(B(vma, 0*PSb + 1) == 100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);

    ok1(B(vma, 2*PSb + 0) ==  12);
    ok1(B(vma, 2*PSb + 1) == 102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);

    ok1(!M(vma, 0));    CHECK_NOPAGE(       100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(       101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(       102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(       103                      );

    diag("writeout (store+mark)");
    mkdirty2(1000);
    file.nstoreblk  = 0;
    file.nremmapblk = 0;
    ok1(!fileh_dirty_writeout(fh, WRITEOUT_STORE | WRITEOUT_MARKSTORED));
    ok1(file.nstoreblk  == 2);
    ok1(file.nremmapblk == 1); /* only 1 (not 2) page was mmaped */

    ok1(!M(vma, 0));    CHECK_NOPAGE(         100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(         101                      );
                        CHECK_NOPAGE(         102                      );
                        CHECK_NOPAGE(         103                      );

    CHECK_MRU   (/*empty*/);
    CHECK_DIRTY (/*empty*/);

    vma_unmap(vma);
    err = fileh_mmap(vma, fh, 100, 4);

    /* data saved; served from base layer */
    ok1(B(vma, 0*PSb + 0) == 1000);
    ok1(B(vma, 0*PSb + 1) ==  100);
    ok1(B(vma, 0*PSb + PSb - 1) == 100);

    ok1(B(vma, 2*PSb + 0) == 1002);
    ok1(B(vma, 2*PSb + 1) ==  102);
    ok1(B(vma, 2*PSb + PSb - 1) == 102);

    ok1(!M(vma, 0));    CHECK_NOPAGE(       100                      );
    ok1(!M(vma, 1));    CHECK_NOPAGE(       101                      );
    ok1(!M(vma, 2));    CHECK_NOPAGE(       102                      );
    ok1(!M(vma, 3));    CHECK_NOPAGE(       103                      );

    /* no invalidation - fileh_invalidate_page is forbidden for "mmap overlay" mode */


    /* free resources */
    file.nmunmap = 0;
    vma_unmap(vma);
    ok1(file.nmunmap == 1);

    fileh_close(fh);
    ram_close(ram);
    free(ram);

#undef  CHECK_MRU
#undef  CHECK_PAGE
#undef  CHECK_NOPAGE
#undef  CHECK_DIRTY
}


// TODO test for loadblk that returns -1

int main()
{
    tap_fail_callback = abort;  // XXX to catch failure immediately

    test_vmamap();
    test_file_access_synthetic();
    test_file_access_pagefault();
    test_pagefault_savestate();
    test_file_access_mmapoverlay();
    return 0;
}
