# Wendelin.core | Instructions to build & test
# Copyright (C) 2014-2015  Nexedi SA and Contributors.
#                     	   Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Open Source Initiative approved licenses and Convey
# the resulting work. Corresponding source of such a combination shall include
# the source code for all other software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
all	:

PYTHON	?= python

# use the same C compiler as python
# (for example it could be `gcc -m64` on a 32bit userspace)
CC	:= $(shell $(PYTHON) -c "from __future__ import print_function;	\
		import sysconfig as _;	\
		print(_.get_config_vars()['CC'])")
ifeq ($(CC),)
$(error "Cannot defermine py-CC")
endif

all	: bigfile/_bigfile.so


bigfile/_bigfile.so : 3rdparty/ccan/config.h FORCE
	$(PYTHON) setup.py ll_build_ext --inplace


FORCE	:


# TODO add FORCE?
3rdparty/ccan/config.h: 3rdparty/ccan/Makefile
	$(MAKE) -C $(@D) $(@F)

# if there is no ccan/Makefile - ccan submodule has not been initialized
# if there is - it is ok, as that Makefile does not need to be rebuilt
3rdparty/ccan/Makefile:
	@echo 'E: 3rdparty/ccan submodule not initialized'
	@echo 'E: please do `git submodule update --init`'
	@false
