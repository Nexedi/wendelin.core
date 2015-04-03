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
 * All tests here end up crashing via segmentation violation. The calling
 * driver verifies test output prior to crash and that the crash happenned in
 * the right place.
 *
 * See t/tfault-run and `test.fault` in Makefile for driver details.
 */

// XXX better link with it
#include "../pagefault.c"

#include <ccan/tap/tap.h>
#include <ccan/array_size/array_size.h>
#include <stdio.h>
#include <string.h>


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


static const struct {
    const char *name;
    void (*test)(void);
} tests[] = {
    // XXX fragile - test names must start exactly with `{"fault` - Makefile extracts them this way
    // name                                func-where-it-dies
    {"faultr",          fault_read},            // on_pagefault
    {"faultw",          fault_write},           // on_pagefault
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
