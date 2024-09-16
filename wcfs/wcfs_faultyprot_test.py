# -*- coding: utf-8 -*-
# Copyright (C) 2018-2024  Nexedi SA and Contributors.
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
"""wcfs_faultyprot_test.py complements wcfs_test.py with tests that exercise
protection against slow/faulty clients in isolation protocol."""

from __future__ import print_function, absolute_import

from golang import select, func, defer
from golang import context, sync, time

import pytest; xfail = pytest.mark.xfail
from pytest import fail, fixture
from wendelin.wcfs.wcfs_test import tDB, h, \
        setup_module, teardown_module, setup_function, teardown_function


# tests in this module require WCFS to promptly react to pin handler
# timeouts so that verifying WCFS killing logic does not take a lot of time.
@fixture
def with_prompt_pintimeout(monkeypatch):
    tkill = 3*time.second
    return monkeypatch.setenv("WENDELIN_CORE_WCFS_OPTIONS", "-pintimeout %.1fs" % tkill, prepend=" ")


# verify that wcfs kills slow/faulty client who does not reply to pin in time.
@xfail  # protection against faulty/slow clients
@func
def test_wcfs_pintimeout_kill(with_prompt_pintimeout):
    t = tDB(); zf = t.zfile
    defer(t.close)

    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2'})
    f = t.open(zf)
    f.assertData(['','','c2'])

    # XXX move into subprocess not to kill whole testing
    ctx, _ = context.with_timeout(context.background(), 2*t.pintimeout)

    wl = t.openwatch()
    wg = sync.WorkGroup(ctx)
    def _(ctx):
        # send watch. The pin handler won't be replying -> we should never get reply here.
        wl.sendReq(ctx, b"watch %s @%s" % (h(zf._p_oid), h(at1)))
        fail("watch request completed (should not as pin handler is stuck)")
    wg.go(_)
    def _(ctx):
        req = wl.recvReq(ctx)
        assert req is not None
        assert req.msg == b"pin %s #%d @%s" % (h(zf._p_oid), 2, h(at1))

        # sleep > wcfs pin timeout - wcfs must kill us
        _, _rx = select(
            ctx.done().recv,               # 0
            time.after(t.pintimeout).recv, # 1
        )
        if _ == 0:
            raise ctx.err()
        fail("wcfs did not killed stuck client")
    wg.go(_)
    wg.wait()
