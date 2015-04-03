/* Wendelin.bigfile | pagemap tests
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
#include "../pagemap.c"

#include <ccan/tap/tap.h>
#include <ccan/array_size/array_size.h>


int main()
{
    PageMap pmap;
    Page *pageptr;

    /* helpers bound to pmap; also translate pointers to uintptr_t to simplify testing */
    uintptr_t get(pgoff_t pgoffset)             { return (uintptr_t)pagemap_get(&pmap, pgoffset); }
    void  set(pgoff_t pgoffset, uintptr_t page) { pagemap_set(&pmap, pgoffset, (Page *)page);  }
    int   del(pgoff_t pgoffset)                 { return pagemap_del(&pmap, pgoffset); }
    void  init(unsigned pageshift)              { pagemap_init(&pmap, pageshift); }

    diag("Testing pagemap");
    tap_fail_callback = abort;  // XXX to catch failure immediately

    int N  = PAGEMAP_LEVEL_BITS;
    int I1 = 1<<PAGEMAP_LEVEL_BITS; /* pgoffset -> T2[1] */
    int I2 = 2*I1;                  /* pgoffset -> T2[2] */
    int i;

    // FIXME numbers hardcoded
    init(64-  N);   ok1(pmap.rshift0 ==   N);
    init(64-2*N);   ok1(pmap.rshift0 == 2*N);
    init(12);       ok1(pmap.rshift0 == 6*N);   /* 4K */
    init(21);       ok1(pmap.rshift0 == 5*N);   /* 2M */

    /* go on testing with 2-level pagetab */
    init(64-2*N);   ok1(pmap.rshift0 == 2*N);

    /* access to p: pointer  n: counter  at appropriate level && v: value in final entry */
    pgoff_t pgidx;
    void      **__x0()  { return (void **)pmap.pmap0; }
    void      **__p0()  { return PTR_POINTER(__x0()); }
    unsigned    __n0()  { return PTR_XCOUNT(__x0()); }

    void      **__x1()  { return __p0()[pgidx >> PAGEMAP_LEVEL_BITS];  }
    void      **__p1()  { return PTR_POINTER(__x1()); }
    unsigned    __n1()  { return PTR_XCOUNT(__x1()); }

    uintptr_t   __v()   { return (uintptr_t)__p1()[pgidx & PAGEMAP_NR_MASK]; }

#define p0  (__p0())
#define n0  (__n0() + (__p0() ? 1 : 0) )
#define p1  (__p1())
#define n1  (__n1() + (__p1() ? 1 : 0) )
#define v   (__v())


/* check that first level has n0 entries */
#define CHECK0(N0) do {               \
    /* no pgidx setup needed - n0 & p0 are independent of it */   \
    ok(n0 == N0 && (!!N0 == !!p0),          \
            "CHECK0(%i)", N0); \
} while (0)

/* check that first & second levels have n0 & n1 entries */
#define CHECK1(PGIDX,N0,N1) do {                    \
    pgidx = PGIDX;                                  \
    ok(n0 == N0 && p0 && n1 == N1 && (!!N1 == !!p1),\
            "CHECK1(0x%x, %i, %i)", PGIDX, N0, N1); \
} while (0)

/* check first & second levels for #entries, and also final entry for value */
#define CHECK(PGIDX,N0,N1,V) do {   \
    pgidx = PGIDX;                  \
    ok(n0 == N0 && p0 && n1 == N1 && p1 && v == V,  \
            "CHECK(0x%x, %u, %u, %u)", PGIDX, N0, N1, V); \
} while (0)

/* like check, but report only on error */
#define __CHECK(PGIDX,N0,N1,V) do { \
    pgidx = PGIDX;                  \
    if (!(n0 == N0 && p0 && n1 == N1 && p1 && v == V))  \
        fail("CHECK(0x%x, %u, %u, %u)", PGIDX, N0, N1, V);  \
} while (0)


    /* get/set */
    diag("get/set");

    ok1(!get(0));
    ok1(!get(123));

    /*      pgidx  n0  n1   v   */
    CHECK0  (      0);

    /* I0[0] */
    set(0, 77);
    ok1(get(0) == 77);

    CHECK1  (0,    1,  1);
    CHECK1  (I1,   1,  0);
    CHECK   (0,    1,  1,  77);

    /* I0[1] */
    ok1(!get(1));
    set(1, 88);
    ok1(get(1) == 88);
    CHECK   (0,    1,  2,  77);
    CHECK   (1,    1,  2,  88);

    /* I1[0] */
    ok1(!get(I1));
    set(I1, 99);
    ok1(get(I1) == 99);
    CHECK   (0,    2,  2,  77);
    CHECK   (1,    2,  2,  88);
    CHECK   (I1,   2,  1,  99);
    CHECK1  (I2,   2,  0);

    /* del */
    diag("del");

    /* if no entry was there - should not change anything */
    ok1(!get(I2));
    ok1(!del(I2));
    CHECK   (0,    2,  2,  77);
    CHECK   (1,    2,  2,  88);
    CHECK   (I1,   2,  1,  99);
    CHECK1  (I2,   2,  0);

    /* del I0[0] - only one counter changes and no tables are deallocated */
    ok1(get(0) == 77);
    ok1(del(0));
    ok1(!get(0));
    CHECK   (0,    2,  1,  0);  /* 0 = NULL */
    CHECK   (1,    2,  1,  88);
    CHECK   (I1,   2,  1,  99);
    CHECK1  (I2,   2,  0);

    /* del I0[1] - I0 should disappear */
    ok1(get(1) == 88);
    ok1(del(1));
    ok1(!get(1));
    CHECK1  (0,    1,  0);
    CHECK1  (1,    1,  0);
    CHECK   (I1,   1,  1,  99);
    CHECK1  (I2,   1,  0);

    /* del I1[0] - I1 & first-level table should disappear */
    ok1(get(I1) == 99);
    ok1(del(I1));
    ok1(!get(I1));
    CHECK0  (      0);


    /* set so that all entries are set in T2 table (checks for counter-in-ptr
     * overflow) */
    diag("xcounter overflow");

#   define XVALUE(i)    (0x8000 + (i))  /* to distinguish pgoffset from set "pointer" */
    for (i = 0; i < PAGEMAP_LEVEL_ENTRIES; ++i) {
        set(i, XVALUE(i));
        ASSERT(get(i) == XVALUE(i));
        __CHECK (i, 1, i+1, XVALUE(i));
    }
    CHECK   (0,    1,   (unsigned)PAGEMAP_LEVEL_ENTRIES,  XVALUE(0));

    for (i = PAGEMAP_LEVEL_ENTRIES-1; i >= 0; --i) {
        ASSERT(del(i));
        ASSERT(!get(i));
        if (i)
            __CHECK (i, 1, i, 0);
    }
    CHECK0  (      0);


    /* pagemap_for_each */
    diag("pagemap_for_each");

    /* iterate over empty - empty */
    i = 0;
    pagemap_for_each(pageptr, &pmap) {
        i = 1;
    }

    ok1(i == 0);

    /* wrestle with test data */
    struct { pgoff_t pgoffset; uintptr_t page; }
    testv[] = {
                {0,        1},
                {1,        2},
                {32,      32},
                {I1-1,    99},
                {I1,     100},
                {I1+1,   101},
                {I1+32,  132},
                {I2-1,   199},
                {I2,     200},
                {I2+1,   201},
    };


    for (i = 0; i < ARRAY_SIZE(testv); ++i)
        set(testv[i].pgoffset, testv[i].page);

    for (i = 0; i < ARRAY_SIZE(testv); ++i)
        ok( get(testv[i].pgoffset) == testv[i].page,
                "get(%lu) == %lu", testv[i].pgoffset, testv[i].page);

    i = 0;
    pagemap_for_each(pageptr, &pmap) {
        ok( (uintptr_t)pageptr == testv[i].page,
            "pagemap_for_each(%i) == %lu", i, testv[i].page );
        ++i;
    }

    ok1(i == ARRAY_SIZE(testv));

    /* test for break - breaking out entirely from pagemap_for_each() */
    i = 0;
    pagemap_for_each(pageptr, &pmap) {
        ok( (uintptr_t)pageptr == testv[i].page,
            "pagemap_for_each(%i) == %lu", i, testv[i].page );

        if (i == 2)
            break;

        ++i;
    }

    ok1(i == 2);


    /* test for pagemap_for_each without {} */
    i = 0;
    pagemap_for_each(pageptr, &pmap)
        if (++i, (uintptr_t)pageptr == 99)
            break;

    ok1(i == 4);


    diag("pagemap_clear()");

    pagemap_clear(&pmap);

    i = 0;
    pagemap_for_each(pageptr, &pmap) {
        i = 1;
    }

    // TODO check it did not leak
    ok(i == 0, "cleared ok (a)");
    CHECK0  (      0);

    return 0;
}
