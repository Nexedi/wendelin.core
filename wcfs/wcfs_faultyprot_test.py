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

from wendelin.lib.zodb import zstor_2zurl
from wendelin import wcfs

import sys, os, subprocess, traceback
import six
from golang import select, func, defer
from golang import context, sync, time

import pytest; xfail = pytest.mark.xfail
from pytest import fixture
from wendelin.wcfs.wcfs_test import tDB, h, tAt, eprint, \
        setup_module, teardown_module, setup_function, teardown_function

if six.PY2:
    from _multiprocessing import Connection as MPConnection
else:
    from multiprocessing.connection import Connection as MPConnection


# tests in this module require WCFS to promptly react to pin handler
# timeouts so that verifying WCFS killing logic does not take a lot of time.
@fixture
def with_prompt_pintimeout(monkeypatch):
    tkill = 3*time.second
    return monkeypatch.setenv("WENDELIN_CORE_WCFS_OPTIONS", "-pintimeout %.1fs" % tkill, prepend=" ")


# tSubProcess provides infrastructure to run a function in separate process.
#
# It runs f(cin, cout, *argv, **kw) in subprocess with cin and cout
# connected to parent via multiprocessing.Connection .
#
# It is similar to multiprocessing.Process in spawn mode that is available on py3.
# We need to use spawn mode - not fork - because fork does not work well when
# parent process is multithreaded, as many things, that are relying on the
# additional threads in the original process, stop to function in the forked
# child without additional care. For example pygolang timers and signals
# currently stop to work after the fork, and in general it is believed that in
# multithreaded programs the only safe thing to do after the fork is exec.
# Please see section "NOTES" in
#
#   https://man7.org/linux/man-pages/man3/pthread_atfork.3.html
#
# for details about this issue.
class tSubProcess(object):
    def __init__(proc, f, *argv, **kw):
        exev = [sys.executable, '-c', 'from wendelin.wcfs import wcfs_faultyprot_test as t; '
                                      't.tSubProcess._start(%r)' % f.__name__]
        proc.popen = subprocess.Popen(exev, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
        try:
            proc.cin  = MPConnection(proc.popen.stdin.fileno(),  readable=False)
            proc.cout = MPConnection(proc.popen.stdout.fileno(), writable=False)
            proc.send(argv)
            proc.send(kw)
        except:
            proc.popen.kill()
            raise

    # _start is trampoline ran in the subprocess to launch to user function.
    @staticmethod
    def _start(funcname):
        cin  = MPConnection(sys.stdin.fileno(),  writable=False)
        cout = MPConnection(sys.stdout.fileno(), readable=False)
        argv = cin.recv()
        kw   = cin.recv()
        f = globals()[funcname]
        procname = kw.pop('_procname', f.__name__)
        try:
            f(cin, cout, *argv, **kw)
            _ = 'END'
        except BaseException as exc:
            # dump traceback so it appears in the log because Traceback objects are not picklable
            eprint("\nException in subprocess %s (pid%d):" % (procname, os.getpid()))
            traceback.print_exc()
            _ = exc
        cout.send(_)
        cout.close()

    # close releases resources associated with subprocess.
    def close(proc):
        if proc.popen.returncode is None:
            proc.popen.kill()

    # exitcode returns subprocess exit code or None if subprocess has not yet terminated.
    @property
    def exitcode(proc):
        return proc.popen.returncode

    # join waits for the subprocess to end.
    def join(proc, ctx):
        gotend = False
        goteof = False
        joined = False
        while not (goteof and joined):
            if ctx.err() is not None:
                raise ctx.err()

            if not joined:
                joined = (proc.popen.poll() is not None)

            # recv from proc to see if it was END or exception
            # make sure to recv at least once after joined to read buffered messages / exception
            if goteof:
                time.sleep(0.1*time.second)
            else:
                try:
                    _, ok = proc.tryrecv()
                except EOFError:
                    goteof = True
                else:
                    if ok:
                        if not gotend:
                            assert _ == 'END'
                            gotend = True
                        else:
                            raise AssertionError("got %r after END" % (_,))

    # send sends object to subprocess input.
    def send(proc, obj):
        proc.cin.send(obj)

    # recv receives object/exception from subprocess output.
    def recv(proc, ctx): # -> obj | raise exception | EOFError
        while 1:
            if ctx.err() is not None:
                raise ctx.err()
            _, ok = proc.tryrecv()
            if ok:
                return _

    # tryrecv tries to receive an object/exception from subprocess output.
    # It does so without blocking.
    def tryrecv(proc): # -> (obj, ok) | raise exception | EOFError
        _ = proc.cout.poll(0.1*time.second)
        if not _:
            return None, False
        _ = proc.cout.recv()
        if isinstance(_, BaseException):
            raise _
        return _, True


# tFaultySubProcess runs f(tFaultyClient, *argv, *kw) in subprocess.
# It's a small convenience wrapper over tSubProcess - please see its documentation for details.
class tFaultySubProcess(tSubProcess):
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


# verify that wcfs kills slow/faulty client who does not reply to pin in time.

@func
def _bad_watch_no_pin_reply(ctx, f, at):
    wl = wcfs.WatchLink(f.wc)    ; defer(wl.close)

    # wait for command to start watching
    _ = f.cin.recv()
    assert _ == "start watch", _

    wg = sync.WorkGroup(ctx)
    def _(ctx):
        # send watch. The pin handler won't be replying -> we should never get reply here.
        wl.sendReq(ctx, b"watch %s @%s" % (h(f.zfile_oid), h(at)))
        raise AssertionError("watch request completed (should not as pin handler is stuck)")
    wg.go(_)
    def _(ctx):
        req = wl.recvReq(ctx)
        assert req is not None
        f.cout.send(req.msg)

        # sleep > wcfs pin timeout - wcfs must kill us
        f.assertKilled(ctx, "wcfs did not kill stuck client")
    wg.go(_)
    wg.wait()

@xfail  # protection against faulty/slow clients
@func
def test_wcfs_pintimeout_kill(with_prompt_pintimeout):
    t = tDB(multiproc=True); zf = t.zfile
    defer(t.close)

    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2'})
    f = t.open(zf)
    f.assertData(['','','c2'])

    # launch faulty process that should be killed by wcfs on problematic pin during watch setup
    p = tFaultySubProcess(t, _bad_watch_no_pin_reply, at=at1)
    defer(p.close)

    # wait till faulty client issues its watch, receives pin and pauses/misbehaves
    p.send("start watch")
    assert p.recv(t.ctx) == b"pin %s #%d @%s" % (h(zf._p_oid), 2, h(at1))

    # issue our watch request - it should be served well and without any delay
    wl = t.openwatch()
    wl.watch(zf, at1, {2:at1})

    # the faulty client must become killed by wcfs
    p.join(t.ctx)
    assert p.exitcode is not None


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
