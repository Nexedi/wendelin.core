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
"""Package wcfs.internal.multiprocessing complements stdandard multiprocessing package."""

from __future__ import print_function, absolute_import

import sys, os, subprocess, traceback, importlib
import six
from golang import time

if six.PY2:
    from _multiprocessing import Connection as MPConnection
else:
    from multiprocessing.connection import Connection as MPConnection


# SubProcess provides infrastructure to run a function in separate process.
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
class SubProcess(object):
    def __init__(proc, f, *argv, **kw):
        exev = [sys.executable, '-c', 'from wendelin.wcfs.internal import multiprocessing as xmp; '
                                      'xmp.SubProcess._start(%r)' % '%s.%s' % (f.__module__, f.__name__)]
        proc.popen = subprocess.Popen(exev, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
        try:
            proc.cin  = MPConnection(os.dup(proc.popen.stdin.fileno()),  readable=False)
            proc.cout = MPConnection(os.dup(proc.popen.stdout.fileno()), writable=False)
            proc.popen.stdin  = open(os.devnull, 'w')
            proc.popen.stdout = open(os.devnull, 'r')
            proc.send(argv)
            proc.send(kw)
        except:
            proc.popen.kill()
            raise

    # _start is trampoline ran in the subprocess to launch to user function.
    @staticmethod
    def _start(funcpath):
        cin  = MPConnection(os.dup(sys.stdin.fileno()),  writable=False)
        cout = MPConnection(os.dup(sys.stdout.fileno()), readable=False)
        sys.stdin  = open(os.devnull, 'r')
        sys.stdout = open(os.devnull, 'w')
        argv = cin.recv()
        kw   = cin.recv()
        modname, funcname = funcpath.rsplit('.', 1)
        mod = importlib.import_module(modname)
        f = getattr(mod, funcname)
        procname = kw.pop('_procname', funcpath)
        try:
            f(cin, cout, *argv, **kw)
            _ = 'END'
        except BaseException as exc:
            # dump traceback so it appears in the log because Traceback objects are not picklable
            print("\nException in subprocess %s (pid%d):" % (procname, os.getpid()), file=sys.stderr)
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
