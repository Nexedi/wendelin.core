# -*- coding: utf-8 -*-
# Copyright (C) 2018-2021  Nexedi SA and Contributors.
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

"""Module wcfs.py provides python gateway for spawning wcfs server.
"""

from __future__ import print_function, absolute_import

import os, sys
from os.path import dirname


# _wcfs_exe returns path to wcfs executable.
def _wcfs_exe():
    return '%s/wcfs' % dirname(__file__)

def main():
    argv = sys.argv[1:]
    os.execv(_wcfs_exe(), [_wcfs_exe()] + argv)
