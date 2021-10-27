#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2019-2021  Nexedi SA and Contributors.
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
"""Program wcfs_readcancel is helper for wcfs_test to verify that
sysread(/head/watch) is unblocked and canceled when kernel asks WCFS to cancel
that read request.

Without proper FUSE INTERRUPT handling on WCFS side, such reads are not
cancelled, which results in processes that were aborted or even `kill-9`ed being
stuck forever waiting for WCFS to release them.
"""

from __future__ import print_function, absolute_import

from golang import select, default
from golang import context, sync, time

import os, sys

def main():
    wcfs_root = sys.argv[1]
    f = open("%s/head/watch" % wcfs_root)
    wg = sync.WorkGroup(context.background())
    def _(ctx):
        data = f.read()    # should block forever
        raise AssertionError("read: woken up: data=%r" % data)
    wg.go(_)
    def _(ctx):
        time.sleep(100*time.millisecond)
        _, _rx = select(
            default,            # 0
            ctx.done().recv,    # 1
        )
        if _ == 1:
            raise ctx.err()
        os._exit(0)
    wg.go(_)
    wg.wait()
    raise AssertionError("should be unreachable")


if __name__ == '__main__':
    main()
