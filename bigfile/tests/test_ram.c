/* Wendelin.bigfile | ram tests
 * Copyright (C) 2014-2019  Nexedi SA and Contributors.
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

// RUNWITH: t/t_with-tmpfs /dev/shm 7m

// XXX better link with it
#include "../ram.c"
#include    "../pagemap.c"
#include    "../virtmem.c"
#include "../ram_shmfs.c"

#include <ccan/tap/tap.h>


int main()
{
    RAM *ram;
    RAMH *ramh;
    Page *page0, *page1, *page2, *page3;
    uint8_t *p0, *p01, *p02, *p03, *p1, *p2, *_;
    size_t ps, ram_maxsize;

    tap_fail_callback = abort;  // XXX to catch failure immediately
    diag("Testing ram");

    ram = ram_new("shmfs", NULL);
    ok1(ram);

    ps = ram->pagesize;
    ok1(ps == 2*1024*1024); // to be sure it correlates with 7m (=3.5 pages) in setup

    ramh = ramh_open(ram);
    ok1(ramh);

    page0 = ramh_alloc_page(ramh, 0);
    ok1(page0);
    ok1(page0->state == PAGE_EMPTY);

    ok1(ram->pagesize == page_size(page0));


    /* mmap page0 into 2 places somewhere */
    p01 = page_mmap(page0, NULL, PROT_READ | PROT_WRITE);
    ok1(p01);

    p02 = page_mmap(page0, NULL, PROT_READ | PROT_WRITE);
    ok1(p02);
    ok1(p02 != p01);

    ok1(p01[0] == 0);   ok1(p01[ps-1] == 0);
    ok1(p02[0] == 0);   ok1(p02[ps-1] == 0);

    /* mappings should be to the same memory */
    p01[0] = 1;     ok1(p02[0] == 1);
    p02[ps-1] = 2;  ok1(p01[ps-1] == 2);


    /* mmap page0 to fixed addr and check memory is the same */
    p03 = mem_xvalloc(NULL, ps);     /* allocate virt address space somewhere */
    ok1(p03);

    _ = page_mmap(page0, p03, PROT_READ | PROT_WRITE);
    ok1(_);
    ok1(_ == p03);

    ok1(p03[0] == 1);
    ok1(p03[ps-1] == 2);

    p03[0] = 4;     ok1(p01[0] == 4);    ok1(p02[0] == 4);
    p01[ps-1] = 5;  ok1(p02[ps-1] == 5); ok1(p03[ps-1] == 5);


    /* memory is forgotten after drop */
    ramh_drop_memory(ramh, page0->ramh_pgoffset);
    ok1(p01[0]    == 0);    ok1(p02[0] == 0);    ok1(p03[0] == 0);
    ok1(p01[ps-1] == 0);    ok1(p02[ps-1] == 0); ok1(p03[ps-1] == 0);


    /* let's allocate memory with pgoffset > current ram_maxsize */
    ram_maxsize = ram_get_current_maxsize(ram);
    ok1(ram_maxsize);

    page2 = ramh_alloc_page(ramh, 2*ram_maxsize);
    ok1(page2);

    /* see if we can access & drop it */
    p1 = page_mmap(page2, NULL, PROT_READ | PROT_WRITE);
    ok1(p1);

    p1[0] = 1;
    p1[ps-1] = 1;

    ramh_drop_memory(ramh, page2->ramh_pgoffset);
    ok1(p1[0] == 0);
    ok1(p1[ps-1] == 0);

    xmunmap(p1, ps);
    xmunmap(p01, ps);
    xmunmap(p02, ps);
    xmunmap(p03, ps);
    free(page0);
    // page1: was not yet allocated
    free(page2);
    // page3: was not yet allocated


    /* ensure we get "no memory" when overallocating (not doing so would lead
     * to getting SIGBUS on accessing memory and EFAULT on read/write
     * syscalls). */
    ok1(ram_maxsize == 3);  /* NOTE must correlate with size in XRUN setup */

    page0 = ramh_alloc_page(ramh, 0);
    ok1(page0);
    page1 = ramh_alloc_page(ramh, 1);
    ok1(page1);
    page2 = ramh_alloc_page(ramh, 2);
    ok1(page2);
    page3 = ramh_alloc_page(ramh, 3);
    ok1(!page3);    /* must fail - there is no such amount of memory */

    p0 = page_mmap(page0, NULL, PROT_READ | PROT_WRITE);    ok1(p0);
    p1 = page_mmap(page1, NULL, PROT_READ | PROT_WRITE);    ok1(p1);
    p2 = page_mmap(page2, NULL, PROT_READ | PROT_WRITE);    ok1(p2);

    /* touch all memory - so that we know we can use it without getting SIGBUS */
    memset(p0, 0xff, ps);
    memset(p1, 0xff, ps);
    memset(p2, 0xff, ps);


    // TODO allocate memory amount = 2*ram_maxsize and touch it linearly


    xmunmap(p0, ps);
    xmunmap(p1, ps);
    xmunmap(p2, ps);
    free(page0);
    free(page1);
    free(page2);
    ok1(!page3);

    ramh_close(ramh);
    ram_close(ram);
    free(ram);

    return 0;
}
