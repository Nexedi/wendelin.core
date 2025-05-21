# -*- coding: utf-8 -*-
# Copyright (C) 2019-2025  Nexedi SA and Contributors.
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

"""Package wcfs.internal.os complements operating system facilities provided by
standard package os.

- gettid returns OS-level identifier of current thread.
- readfile and writefile are handy utilities to read/write a file as a whole.
"""

from __future__ import print_function, absolute_import

from wendelin.wcfs.internal._os import gettid


# readfile reads file @ path.
def readfile(path):
    with open(path) as f:
        return f.read()

# writefile writes data to file @ path.
def writefile(path, data):
    with open(path, "w") as f:
        f.write(data)
