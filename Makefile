# Wendelin.core | Instructions to build & test
# Copyright (C) 2014-2021  Nexedi SA and Contributors.
#                     	   Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Free Software licenses or any of the Open Source
# Initiative approved licenses and Convey the resulting work. Corresponding
# source of such a combination shall include the source code for all other
# software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# See https://www.nexedi.com/licensing for rationale and options.
all	:

PYTHON	?= python
PYTEST	?= $(PYTHON) -m pytest
PYBENCH ?= $(PYTHON) -m golang.cmd.pybench
VALGRIND?= valgrind

# use the same C compiler as python
# (for example it could be `gcc -m64` on a 32bit userspace)
CC	:= $(shell $(PYTHON) -c "from __future__ import print_function;	\
		import sysconfig as _;	\
		print(_.get_config_vars()['CC'])")
ifeq ($(CC),)
$(error "Cannot defermine py-CC")
endif

all	: bigfile/_bigfile.so


ccan_config := 3rdparty/ccan/config.h

bigfile/_bigfile.so : $(ccan_config) FORCE
	$(PYTHON) setup.py ll_build_ext --inplace


FORCE	:


# TODO add FORCE?
$(ccan_config): 3rdparty/ccan/Makefile
	$(MAKE) -C $(@D) $(@F)

# if there is no ccan/Makefile - ccan submodule has not been initialized
# if there is - it is ok, as that Makefile does not need to be rebuilt
3rdparty/ccan/Makefile:
	@echo 'E: 3rdparty/ccan submodule not initialized'
	@echo 'E: please do `git submodule update --init`'
	@false


# -*- testing -*-

# XXX dup with setup.py
CPPFLAGS:= -Iinclude -I3rdparty/ccan -I3rdparty/include
CFLAGS	:= -g -Wall -D_GNU_SOURCE -std=gnu99 -fplan9-extensions	\
	   -Wno-declaration-after-statement	\
	   -Wno-error=declaration-after-statement	\

# XXX hack ugly
LOADLIBES=lib/bug.c lib/utils.c 3rdparty/ccan/ccan/tap/tap.c
TESTS	:= $(patsubst %.c,%,$(wildcard bigfile/tests/test_*.c))
test	: test.t test.py test.fault test.asan test.tsan test.vgmem test.vghel test.vgdrd

# TODO move XFAIL markers into *.c

# Before calling our SIGSEGV handler, Memcheck first reports "invalid read|write" error.
# A solution could be to tell memcheck via VALGRIND_MAKE_MEM_DEFINED that VMA
# address space is ok to access _before_ handling pagefault.
# http://valgrind.org/docs/manual/mc-manual.html#mc-manual.clientreqs
XFAIL_bigfile/tests/test_virtmem.vgmemrun	:= y


# extract what goes after RUNWITH: marker from command source, or empty if no marker
runwith = $(shell grep -oP '(?<=^// RUNWITH: ).*' $(basename $1).c)

# run a test, not failing if failure is expected
xrun	= $1 $(if $(XFAIL_$@),|| echo "($@ - expected failure)")
XRUN<	= $(call xrun,$(call runwith,$<) $<)


LINKC	= $(LINK.c) $< $(LOADLIBES) $(LDLIBS) -o $@

# tests without instrumentation
test.t	: $(TESTS:%=%.trun)
%.trun	: %.t
	$(XRUN<)

%.t	: %.c $(ccan_config) FORCE
	$(LINKC)

# test with AddressSanitizer
test.asan: $(TESTS:%=%.asanrun)
%.asanrun: %.asan
	$(XRUN<)

%.asan	: CFLAGS += -fsanitize=address
%.asan	: %.c $(ccan_config)
	$(LINKC)


# test with ThreadSanitizer

# TSAN works only on x86_64
# (can't rely on `uname -m` - could have 32bit userspace on 64bit kernel)
ifneq ($(shell $(CPP) -dM - </dev/null | grep __x86_64__),)
test.tsan: $(TESTS:%=%.tsanrun)
%.tsanrun: %.tsan
	$(XRUN<)
else
test.tsan:
	@echo "Skip $@	# ThreadSanitizer does not support \"`$(CC) -v 2>&1 | grep '^Target:'`\""
endif



%.tsan	: CFLAGS += -fsanitize=thread -pie -fPIC
%.tsan	: %.c $(ccan_config)
	$(LINKC)


# run valgrind so errors affect exit code
# TODO stop on first error
# (http://stackoverflow.com/questions/16345555/is-there-a-way-to-stop-valgrind-on-the-first-error-it-finds
#  but it still asks interactively, whether to "run debugger")
VALGRINDRUN  = $(VALGRIND) --error-exitcode=1

# to track memory access on each instruction (e.g. without this reads from NULL are ignored)
# XXX why =allregs-at-mem-access is not sufficient?
#     see "Handling of Signals" in http://valgrind.org/docs/manual/manual-core.html
#     without this option our SIGSEGV handler is not always called
#     see also: https://bugs.kde.org/show_bug.cgi?id=124035
VALGRINDRUN += --vex-iropt-register-updates=allregs-at-each-insn


# like XRUN< for valgrind
vgxrun	= $(call xrun,$(call runwith,$2) $(VALGRINDRUN) $1 $2)

# test with valgrind/memcheck
test.vgmem: $(TESTS:%=%.vgmemrun)
%.vgmemrun: %.t
	$(call vgxrun,--tool=memcheck, $<)


# test with valgrind/helgrind
test.vghel: $(TESTS:%=%.vghelrun)
%.vghelrun: %.t
	$(call vgxrun,--tool=helgrind, $<)


# test with valgrind/drd
test.vgdrd: $(TESTS:%=%.vgdrdrun)
%.vgdrdrun: %.t
	$(call vgxrun,--tool=drd, $<)


# run python tests
PYTEST_IGNORE	:=  --ignore=3rdparty --ignore=build --ignore=t
test.py	: bigfile/_bigfile.so
	$(PYTEST) $(PYTEST_IGNORE)

# test.py via Valgrind (very slow)
test.py.vghel: bigfile/_bigfile.so
	$(call vgxrun,--tool=helgrind, $(PYTEST) $(PYTEST_IGNORE))

test.py.drd: bigfile/_bigfile.so
	$(call vgxrun,--tool=drd, $(PYTEST) $(PYTEST_IGNORE))



# test pagefault for double/real faults - it should crash
tfault	:= bigfile/tests/tfault
# XXX FAULTS extraction fragile
FAULTS	:= $(shell grep '{"fault.*"' $(tfault).c | sed 's/"/ /g' |awk '{print $$2}')
test.fault : $(FAULTS:%=%.tfault)

$(tfault).t: CFLAGS += -rdynamic	# so that backtrace_symbols works
%.tfault : $(tfault).t
	t/tfault-run $< $* $(shell grep '{"$*"' $(tfault).c | awk '{print $$NF}')


# -*- benchmarking -*-
BENCHV.C:= $(patsubst %.c,%,$(wildcard bigfile/tests/bench_*.c))
bench	: bench.t bench.py

bench.t	: $(BENCHV.C:%=%.trun)

bench.py: bigfile/_bigfile.so
	$(PYBENCH) --count=3 --forked $(PYTEST_IGNORE)
