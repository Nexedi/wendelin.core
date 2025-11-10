# -*- coding: utf-8 -*-
# Copyright (C) 2018-2025  Nexedi SA and Contributors.
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

from wendelin.lib.zodb import zstor_2zurl
from wendelin.wcfs.internal import multiprocessing as xmp
from wendelin import wcfs

from golang import select, func, defer
from golang import context, sync, time

from pytest import mark, fixture
from wendelin.wcfs.wcfs_test import tDB, h, tAt, \
        setup_module, teardown_module, setup_function, teardown_function


# tests in this module require WCFS to promptly react to pin handler
# timeouts so that verifying WCFS killing logic does not take a lot of time.
@fixture
def with_prompt_pintimeout(monkeypatch):
    tkill = 3*time.second
    return monkeypatch.setenv("WENDELIN_CORE_WCFS_OPTIONS", "-pintimeout %.1fs" % tkill, prepend=" ")


# tFaultySubProcess runs f(tFaultyClient, *argv, *kw) in subprocess.
# It's a small convenience wrapper over xmp.SubProcess - please see its documentation for details.
class tFaultySubProcess(xmp.SubProcess):
    def __init__(fproc, t, f, *argv, **kw):
        kw.setdefault('zurl',       zstor_2zurl(t.root._p_jar.db().storage))
        kw.setdefault('zfile_oid',  t.zfile._p_oid)
        kw.setdefault('_procname',  f.__name__)

        kw.setdefault('pintimeout', t.pintimeout)
        tremain = t.ctx.deadline() - time.now()
        assert t.pintimeout < tremain/3 # 2·pintimeout is needed to reliably detect wcfs kill reaction

        for k,v in list(kw.items()):
            if isinstance(v, tAt):  # tAt is not picklable
                kw[k] = v.raw

        super(tFaultySubProcess, fproc).__init__(_tFaultySubProcess_start, f.__name__, *argv, **kw)
        assert fproc.cout.recv() == "f: start"

@func
def _tFaultySubProcess_start(cin, cout, funcname, **kw):
    f = tFaultyClient()
    f.cin  = cin
    f.cout = cout
    f.zurl = kw.pop('zurl')
    f.zfile_oid = kw.pop('zfile_oid')
    f.pintimeout = kw.pop('pintimeout')
    f.wc = wcfs.join(f.zurl, autostart=False);  defer(f.wc.close)
    # we do not need to implement timeouts precisely in the child process
    # because parent will kill us on its timeout anyway.
    ctx = context.background()
    f.cout.send("f: start")
    testf = globals()[funcname]
    testf(ctx, f, **kw)

# tFaultyClient is placeholder for arguments + WCFS connection for running test
# function inside tFaultySubProcess.
class tFaultyClient:
    # .cin
    # .cout
    # .zurl
    # .zfile_oid
    # .wc
    # .pintimeout
    pass


# ---- tests ----


# verify that wcfs kills slow/faulty client who does not handle pin
# notifications correctly and in time during watch setup.
#
# This verifies setupWatch codepath.

@func   # faulty client that does not read pin notifications during watch setup.
def _bad_watch_no_pin_read(ctx, f, at):
    wlf = f.wc._open("head/watch", mode='r+b')  ; defer(wlf.close)

    # wait for command to start watching
    _ = f.cin.recv()
    assert _ == "start watch", _

    # send watch; the write should go ok.
    wlf.write(b"1 watch %s @%s\n" % (h(f.zfile_oid), h(at)))

    # there is no pin handler, because noone reads pin notifications
    # -> wcfs must kill us after timing out with sending pin request
    f.assertKilled(ctx, "wcfs did not kill client that does not read pin requests")

@func   # faulty client that terminates connection abruptly after receiving pin during watch setup.
def _bad_watch_eof_pin_reply(ctx, f, at):
    wlf = f.wc._open("head/watch", mode='r+b')  ; defer(wlf.close)

    # wait for command to start watching
    _ = f.cin.recv()
    assert _ == "start watch", _

    # send watch; the write should go ok.
    wlf.write(b"1 watch %s @%s\n" % (h(f.zfile_oid), h(at)))

    # pin notification must be coming
    _ = wlf.readline()
    assert _.startswith(b"2 pin "), _
    f.cout.send(_[2:].rstrip())  # received message without sequence number and trailing \n

    # we don't reply to pin notification and just close the connection instead
    # NOTE it is different from WatchLink.closeWrite which sends "bye" before doing OS-level close
    wlf.close()

    # wcfs must kill us right after receiving EOF
    f.assertKilled(ctx, "wcfs did not kill client that replied EOF to pin")


@func   # faulty client that behaves in problematic way in its pin handler during watch setup.
def __bad_watch_pinh(ctx, f, at, pinh, pinhFailReason):
    wl = wcfs.WatchLink(f.wc)   ; defer(wl.close)

    # wait for command to start watching
    _ = f.cin.recv()
    assert _ == "start watch", _

    wg = sync.WorkGroup(ctx)
    def _(ctx):
        # send watch. The pin handler either won't be replying or will reply with an error
        # -> we should never get reply here.
        _ = wl.sendReq(ctx, b"watch %s @%s" % (h(f.zfile_oid), h(at)))
        raise AssertionError("watch request completed (should not as pin handler %s); reply: %r" % (pinhFailReason, _))
    wg.go(_)
    def _(ctx):
        pinh(ctx, wl)
    wg.go(_)
    wg.wait()

def _bad_watch_no_pin_reply (ctx, f, at):  __bad_watch_pinh(ctx, f, at, f._pinner_no_pin_reply,  "is stuck")
def _bad_watch_nak_pin_reply(ctx, f, at):  __bad_watch_pinh(ctx, f, at, f._pinner_nak_pin_reply, "replies nak")

@mark.parametrize('faulty', [
    _bad_watch_no_pin_read,
    _bad_watch_no_pin_reply,
    _bad_watch_eof_pin_reply,
    _bad_watch_nak_pin_reply,
])
@func
def test_wcfs_pinhfaulty_kill_on_watch(faulty, with_prompt_pintimeout):
    t = tDB(multiproc=True); zf = t.zfile
    defer(t.close)

    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2'})
    f = t.open(zf)
    f.assertData(['','','c2'])

    # launch faulty process that should be killed by wcfs on problematic pin during watch setup
    p = tFaultySubProcess(t, faulty, at=at1)
    defer(p.close)
    t.assertStats({'pinkill': 0})

    # wait till faulty client issues its watch, receives pin and pauses/misbehaves
    p.send("start watch")
    if faulty != _bad_watch_no_pin_read:
        assert p.recv(t.ctx) == b"pin %s #%d @%s" % (h(zf._p_oid), 2, h(at1))

    # issue our watch request - it should be served well and without any delay
    wl = t.openwatch()
    wl.watch(zf, at1, {2:at1})

    # the faulty client must become killed by wcfs
    p.join(t.ctx)
    assert p.exitcode is not None
    t.assertStats({'pinkill': 1})


# verify that wcfs kills slow/faulty client who does not handle pin
# notifications correctly and in time caused by asynchronous read access.
#
# This verifies readPinWatchers codepath.

@func   # faulty client that does not read pin notifications triggered by read.
def _bad_pinh_no_pin_read(ctx, f, at):
    wlf = f.wc._open("head/watch", mode='r+b')  ; defer(wlf.close)

    # initial watch setup goes ok
    wlf.write(b"1 watch %s @%s\n" % (h(f.zfile_oid), h(at)))
    _ = wlf.readline()
    assert _ == b"1 ok\n",  _
    f.cout.send("f: watch setup ok")

    # sleep > wcfs pin timeout - wcfs must kill us
    f.assertKilled(ctx, "wcfs did not kill client that does not read pin requests")

@func   # faulty client that terminates connection abruptly after receiving pin triggered by read.
def _bad_pinh_eof_pin_reply(ctx, f, at):
    wlf = f.wc._open("head/watch", mode='r+b')  ; defer(wlf.close)

    # initial watch setup goes ok
    wlf.write(b"1 watch %s @%s\n" % (h(f.zfile_oid), h(at)))
    _ = wlf.readline()
    assert _ == b"1 ok\n",  _
    f.cout.send("f: watch setup ok")

    # wait for "pin ..." due to read access in the parent
    _ = wlf.readline()
    assert _.startswith(b"2 pin "), _
    f.cout.send(_[2:].rstrip())

    # close connection abruptly.
    # NOTE it is different from WatchLink.closeWrite which sends "bye" before doing OS-level close
    wlf.close()

    # wcfs must kill us right after receiving EOF
    f.assertKilled(ctx, "wcfs did not kill client that replied EOF to pin")

@func   # faulty client that behaves in problematic way in its pin notifications triggered by read.
def __bad_pinh(ctx, f, at, pinh):
    wl = wcfs.WatchLink(f.wc)   ; defer(wl.close)

    # initial watch setup goes ok
    _ = wl.sendReq(ctx, b"watch %s @%s" % (h(f.zfile_oid), h(at)))
    assert _ == b"ok",  _
    f.cout.send("f: watch setup ok")

    # wait for "pin ..." due to read access in the parent
    pinh(ctx, wl)

def _bad_pinh_no_pin_reply (ctx, f, at):  __bad_pinh(ctx, f, at, f._pinner_no_pin_reply)
def _bad_pinh_nak_pin_reply(ctx, f, at):  __bad_pinh(ctx, f, at, f._pinner_nak_pin_reply)

@mark.parametrize('faulty', [
    _bad_pinh_no_pin_read,
    _bad_pinh_no_pin_reply,
    _bad_pinh_eof_pin_reply,
    _bad_pinh_nak_pin_reply,
])
@func
def test_wcfs_pinhfaulty_kill_on_access(faulty, with_prompt_pintimeout):
    t = tDB(multiproc=True); zf = t.zfile; at0=t.at0
    defer(t.close)

    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2'})
    f = t.open(zf)
    f.assertData(['','','c2'])

    # issue our watch request - it should be served well
    wl = t.openwatch()
    wl.watch(zf, at1, {2:at1})

    # spawn faulty client and wait until it setups its watch
    p = tFaultySubProcess(t, faulty, at=at2)
    defer(p.close)
    assert p.recv(t.ctx) == "f: watch setup ok"
    t.assertStats({'pinkill': 0})

    # commit new transaction and issue read access to modified block
    # our read should be served well even though faulty client is either stuck
    # or behaves in problematic way in its pin handler.
    # As the result the faulty client should be killed by wcfs.
    at3 = t.commit(zf, {1:'b3'})
    wg = sync.WorkGroup(t.ctx)
    def _(ctx):
        f.assertBlk(1, 'b3', {wl: {1:at0}}, timeout=2*t.pintimeout)
    wg.go(_)
    def _(ctx):
        if faulty != _bad_pinh_no_pin_read:
            assert p.recv(ctx) == b"pin %s #%d @%s" % (h(zf._p_oid), 1, h(at0))
    wg.go(_)
    wg.wait()

    p.join(t.ctx)
    assert p.exitcode is not None
    t.assertStats({'pinkill': 1})


# _pinner_<problem> simulates faulty pinner inside client that behaves in
# problematic way in its pin notification handler.

@func(tFaultyClient)
def _pinner_no_pin_reply(f, ctx, wl):
    req = wl.recvReq(ctx)
    assert req is not None
    f.cout.send(req.msg)

    # sleep > wcfs pin timeout - wcfs must kill us
    f.assertKilled(ctx, "wcfs did not kill stuck client")

@func(tFaultyClient)
def _pinner_nak_pin_reply(f, ctx, wl):
    req = wl.recvReq(ctx)
    assert req is not None
    f.cout.send(req.msg)

    wl.replyReq(ctx, req, b"nak")

    # wcfs must kill us right after receiving the nak
    f.assertKilled(ctx, "wcfs did not kill client that replied nak to pin")


# assertKilled assert that the current process becomes killed after time goes after pintimeout.
@func(tFaultyClient)
def assertKilled(f, ctx, failmsg):
    # sleep > wcfs pin timeout - wcfs must kill us
    _, _rx = select(
        ctx.done().recv,                    # 0
        time.after(2*f.pintimeout).recv,    # 1
    )
    if _ == 0:
        raise ctx.err()
    raise AssertionError(failmsg)
