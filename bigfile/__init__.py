# -*- coding: utf-8 -*-
# BigFile submodule for Wendelin
# Copyright (C) 2014-2024  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
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

"""TODO big module-level picture description"""

# preload golang.so -> libgolang.so. This way dynamic linker discovers where
# libgolang.so is, and so there will be no link failure due to libgolang.so not
# found, when our C++ libraries, that use libgolang.so, are loaded (e.g. libwcfs.so).
#
# https://github.com/mdavidsaver/setuptools_dso/issues/11#issuecomment-808258994
import golang

from wendelin.bigfile._bigfile import BigFile, WRITEOUT_STORE, WRITEOUT_MARKSTORED, ram_reclaim
