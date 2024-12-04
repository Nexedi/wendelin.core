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

"""Module wcfs.py provides python gateway for spawning, stopping, monitoring
and interoperating with wcfs server.

Serve (zurl) starts and runs WCFS server for ZODB at zurl.
Start (zurl) starts WCFS server for ZODB at zurl and returns corresponding Server object.
Stop  (zurl) makes sure that WCFS server for ZODB at zurl is not running.
Status(zurl) verifies whether WCFS server for ZODB at zurl is running in correct state.

Join(zurl) joins wcfs server for ZODB at zurl and returns WCFS object that
represents filesystem-level connection to joined wcfs server. If wcfs server
for zurl is not yet running, it will be automatically started if join is given
`autostart=True` option.

The rest of wcfs.py merely wraps C++ wcfs client package:

- `WCFS` represents filesystem-level connection to wcfs server.
- `Conn` represents logical connection that provides view of data on wcfs
  filesystem as of particular database state.
- `FileH` represent isolated file view under Conn.
- `Mapping` represents one memory mapping of FileH.

A path from WCFS to Mapping is as follows:

    WCFS.connect(at)                    -> Conn
    Conn.open(foid)                     -> FileH
    FileH.mmap([blk_start +blk_len))    -> Mapping

Classes in wcfs.py logically mirror classes in ZODB:

    wcfs.WCFS   <->  ZODB.DB
    wcfs.Conn   <->  ZODB.Connection

Please see wcfs/client/wcfs.h for more thorough overview and further details.


Environment variables
---------------------

The following environment variables can be used to control wcfs.py client:

  $WENDELIN_CORE_WCFS_AUTOSTART
      yes       join: spawn wcfs server if no one was found and no explicit
                      autostart=X was given   (default)
      no        join: don't spawn wcfs server unless explicitly requested via autostart=True

  $WENDELIN_CORE_WCFS_OPTIONS
      ""        serve/start/join: additional options to pass to wcfs server when spawning it
"""

from __future__ import print_function, absolute_import

import os, sys, hashlib, subprocess, stat
import logging; log = logging.getLogger('wcfs')
from os.path import dirname
from stat import S_ISDIR
from errno import ENOENT, ENOTCONN, EEXIST
from signal import SIGTERM, SIGQUIT, SIGKILL

from golang import chan, select, default, func, defer, b
from golang import context, errors, sync, time
from golang.gcompat import qq

from persistent import Persistent
from zodbtools.util import ashex as h

from wendelin.lib.zodb import zurl_normalize_main
from wendelin.wcfs.internal import glog
from wendelin.wcfs.client._wcfs import \
    PyWCFS          as _WCFS,       \
    PyWatchLink     as WatchLink    \

from wendelin.wcfs.internal import os as xos, multiprocessing as xmp


# Server represents running wcfs server.
#
# Use start to create it.
class Server:
    # ._mnt         mount entry
    # ._proc        wcfs process:
    #                   \/ subprocess.Popen     ; we spawned it
    #                   \/ xos.Proc | None      ; we discovered it during status or stop
    # ._fuseabort   opened /sys/fs/fuse/connections/X/abort for server's mount
    # ._stopOnce
    pass


# WCFS represents filesystem-level connection to wcfs server.
#
# Use join to create it.
#
# The primary way to access wcfs is to open logical connection viewing on-wcfs
# data as of particular database state, and use that logical connection to create
# base-layer mappings. See .connect and Conn in C++ API for details.
#
# Raw files on wcfs can be accessed with ._path/._read/._stat/._open .
#
# WCFS logically mirrors ZODB.DB .
class WCFS(_WCFS):
    # .mountpoint   path to wcfs mountpoint
    # ._fwcfs       /.wcfs/zurl opened to keep the server from going away (at least cleanly)
    # ._njoin       this connection was returned for so many joins

    # ._wcsrv       wcfs Server if it was opened by this WCFS | None
    pass


# ---- WCFS raw file access (primarily for tests) ----

# _path returns path for object on wcfs.
# - str:        wcfs root + obj;
# - Persistent: wcfs root + (head|@<at>)/bigfile/obj
@func(WCFS)
def _path(wc, obj, at=None):
    if isinstance(obj, Persistent):
        #assert type(obj) is ZBigFile   XXX import cycle
        objtypestr = type(obj).__module__ + "." + type(obj).__name__
        assert objtypestr == "wendelin.bigfile.file_zodb.ZBigFile", objtypestr
        head = "head/" if at is None else ("@%s/" % h(at))
        obj  = "%s/bigfile/%s" % (head, h(obj._p_oid))
        at   = None
    assert isinstance(obj, str)
    assert at is None  # must not be used with str
    return os.path.join(wc.mountpoint, obj)

# _read reads file corresponding to obj on wcfs.
@func(WCFS)
def _read(wc, obj, at=None):
    path = wc._path(obj, at=at)
    with open(path, 'rb') as f:
        return f.read()

# _stat stats file corresponding to obj on wcfs.
@func(WCFS)
def _stat(wc, obj, at=None):
    path = wc._path(obj, at=at)
    return os.stat(path)

# _open opens file corresponding to obj on wcfs.
@func(WCFS)
def _open(wc, obj, mode='rb', at=None):
    path = wc._path(obj, at=at)
    return open(path, mode, 0)  # unbuffered


# ---- join/run wcfs ----

_wcmu = sync.Mutex()
_wcregistry    = {} # mntpt -> WCFS
_wcautostarted = [] # of WCFS, with ._wcsrv != None, for wcfs we ever autostart'ed  (for tests)

@func(WCFS)
def __init__(wc, mountpoint, fwcfs, wcsrv):
    wc.mountpoint = mountpoint
    wc._fwcfs     = fwcfs
    wc._njoin     = 1
    wc._wcsrv     = wcsrv

# close must be called to release joined connection after it is no longer needed.
@func(WCFS)
def close(wc):
    with _wcmu:
        wc._njoin -= 1
        if wc._njoin == 0:
            del _wcregistry[wc.mountpoint]
            # NOTE not unmounting wcfs - it either runs as separate service, or
            # is spawned on demand with -autoexit.
            # NOTE ._fwcfs.close can raise IOError (e.g. ENOTCONN after wcfs server crash)
            wc._fwcfs.close()

# _default_autostart returns default autostart setting for join.
#
# Out-of-the-box we want wcfs to be automatically started, to ease developer
# experience when wendelin.core is standalone installed. However in environments
# like SlapOS, it is more preferable to start and monitor wcfs service explicitly.
# SlapOS & co. should thus set $WENDELIN_CORE_WCFS_AUTOSTART=no.
def _default_autostart():
    autostart = os.environ.get("WENDELIN_CORE_WCFS_AUTOSTART", "yes")
    autostart = autostart.lower()
    return {"yes": True, "no": False}[autostart]

# join connects to wcfs server for ZODB @ zurl.
#
# If wcfs for that zurl is already running, join connects to it.
# Otherwise it starts wcfs for zurl if autostart is True.
#
# For the same zurl join returns the same WCFS object.
def join(zurl, autostart=_default_autostart()): # -> WCFS
    mntpt = _mntpt_4zurl(zurl)
    with _wcmu:
        # check if we already have connection to wcfs server from this process
        wc = _wcregistry.get(mntpt)
        if wc is not None:
            wc._njoin += 1
            return wc

        # no. try opening .wcfs - if we succeed - wcfs is already running.
        fwcfs, trylockstartf = _try_attach_wcsrv(mntpt)
        if fwcfs is not None:
            # already have it
            wc = WCFS(mntpt, fwcfs, None)
            _wcregistry[mntpt] = wc
            return wc

        if not autostart:
            raise RuntimeError("wcfs: join %s: server not running" % zurl)

        # start wcfs with telling it to automatically exit when there is no client activity.
        trylockstartf() # XXX retry access if another wcfs was started in the meantime

        wcsrv, fwcfs = _start(zurl, "-autoexit")
        wc = WCFS(mntpt, fwcfs, wcsrv)
        _wcautostarted.append(wc)
        assert mntpt not in _wcregistry
        _wcregistry[mntpt] = wc

        return wc


# _try_attach_wcsrv tries to attach to running wcfs server.
#
# if successful, it returns fwcfs - opened file handle for /.wcfs/zurl
# if unsuccessful, it returns fwcfs=None, and trylockstartf function that can
# be used to prepare to start new WCFS server.
def _try_attach_wcsrv(mntpt): # -> (fwcfs, trylockstartf)
    # try opening .wcfs - if we succeed - wcfs is already running.
    unclean = False
    try:
        fwcfs = open(mntpt + "/.wcfs/zurl")
    except IOError as e:
        if   e.errno == ENOENT:     # wcfs cleanly unmounted
            pass
        elif e.errno == ENOTCONN:   # wcfs crashed/killed
            unclean = True
        else:
            raise
    else:
        return (fwcfs, None)

    # the server is not running.
    # return func to prepare start of another wcfs server
    def trylockstartf():
        # XXX race window if external process starts after ^^^ check
        # TODO -> fs-level locking
        if unclean:
            _fuse_unmount(mntpt)
    return (None, trylockstartf)


# start starts wcfs server for ZODB @ zurl.
#
# optv can be optionally given to pass flags to wcfs.
def start(zurl, *optv): # -> Server
    # verify that wcfs is not already running
    mntpt = _mntpt_4zurl(zurl)

    fwcfs, trylockstartf = _try_attach_wcsrv(mntpt)
    if fwcfs is not None:
        fwcfs.close()
        raise RuntimeError("wcfs: start %s: already running" % zurl)

    # seems to be ok to start
    trylockstartf() # XXX -> "already running" if lock fails

    wcsrv, fwcfs = _start(zurl, *optv)
    fwcfs.close()
    return wcsrv


# _optv_with_wcfs_defaults returns optv prepended with default WCFS options taken from environment.
def _optv_with_wcfs_defaults(optv): # -> optv
    optv_defaults = os.environ.get("WENDELIN_CORE_WCFS_OPTIONS", "").split()
    return tuple(optv_defaults) + tuple(optv)


# _start serves start and join.
@func
def _start(zurl, *optv): # -> Server, fwcfs
    mntpt = _mntpt_4zurl(zurl)
    optv  = _optv_with_wcfs_defaults(optv)
    log.info("starting for %s ...", zurl)

    # XXX errctx "wcfs: start"

    # spawn wcfs and wait till filesystem-level access to it is ready
    wcsrv = Server(None, None, None)
    wg = sync.WorkGroup(context.background())
    fsready = chan(dtype='C.structZ')
    def _(ctx):
        # XXX errctx "spawn"
        argv = [_wcfs_exe()] + list(optv) + [zurl, mntpt]
        proc = subprocess.Popen(argv, close_fds=True)
        while 1:
            ret = proc.poll()
            if ret is not None:
                raise RuntimeError("exited with %s" % ret)

            _, _rx = select(
                ctx.done().recv,    # 0
                fsready.recv,       # 1
                default,            # 2
            )
            if _ == 0:
                proc.terminate()
                raise ctx.err()
            if _ == 1:
                # startup was ok - don't monitor spawned wcfs any longer
                wcsrv._proc = proc
                return

            time.sleep(0.1*time.second)
    wg.go(_)

    def _(ctx):
        # XXX errctx "waitmount"
        fwcfs = _waitmount(ctx, zurl, mntpt)
        wcsrv._fwcfs = fwcfs
        fsready.close()
    wg.go(_)

    wg.wait()
    wcsrv._mnt = _lookup_mnt(mntpt)
    log.info("started pid%d @ %s", wcsrv._proc.pid, mntpt)

    fwcfs = wcsrv._fwcfs
    del wcsrv._fwcfs

    # open fuse abort control file
    # shutdown wcsrv if that open fails
    try:
        x = os.minor(os.stat(wcsrv.mountpoint).st_dev)
        wcsrv._fuseabort = open("/sys/fs/fuse/connections/%d/abort" % x, "wb")
    except:
        defer(wcsrv.stop)
        defer(fwcfs.close)
        raise

    return wcsrv, fwcfs

# _waitmount waits for wcfs filesystem for zurl @mntpt to become ready.
def _waitmount(ctx, zurl, mntpt): # -> fwcfs
    while 1:
        try:
            f = open("%s/.wcfs/zurl" % mntpt)
        except IOError as e:
            # ENOTCONN (wcfs crashed/killed) is an error here
            if e.errno != ENOENT:
                raise
        else:
            dotwcfs = f.read()
            if dotwcfs != zurl:
                raise RuntimeError(".wcfs/zurl != zurl  (%s != %s)" % (qq(dotwcfs), qq(zurl)))

            return f

        _, _rx = select(
            ctx.done().recv,    # 0
            default,            # 1
        )
        if _ == 0:
            raise ctx.err()

        time.sleep(0.1*time.second)


@func(Server)
def __init__(wcsrv, mnt, proc, ffuseabort):
    wcsrv._mnt       = mnt
    wcsrv._proc      = proc
    wcsrv._fuseabort = ffuseabort
    wcsrv._stopOnce  = sync.Once()

# mountpoint returns path to wcfs mountpoint.
@func(Server)
@property
def mountpoint(wcsrv):
    return wcsrv._mnt.point

# stop shutdowns the server.
@func(Server)
def stop(wcsrv, ctx=None):
    if ctx is None:
        ctx, cancel = context.with_timeout(context.background(), 25*time.second)
        defer(cancel)
    wcsrv._stop(ctx)

@func(Server)
def _stop(wcsrv, ctx, _on_wcfs_stuck=None, _on_fs_busy=None, _on_last_unmount_try=None):
    def _():
        wcsrv.__stop(ctx, _on_wcfs_stuck, _on_fs_busy, _on_last_unmount_try)
    wcsrv._stopOnce.do(_)

@func(Server)
def __stop(wcsrv, ctx, _on_wcfs_stuck, _on_fs_busy, _on_last_unmount_try):
    wcstr = "(not running)"
    if wcsrv._proc is not None:
        wcstr = "pid%d" % wcsrv._proc.pid
    log.info("unmount/stop wcfs %s @ %s", wcstr, wcsrv.mountpoint)

    deadline = ctx.deadline()
    if deadline is None:
        deadline = float('inf')
    timeoutTotal = (deadline - time.now())
    if timeoutTotal < 0:
        timeoutTotal = 0.
    # timeoutFrac returns ctx with `timeout ~= fraction·totalTimeout`
    # however if the context is already cancelled, returned timeout is 0.1s to
    # give chance for an operation to complete.
    def timeoutFrac(fraction):
        if _ready(ctx.done()):
            tctx, _ = context.with_timeout(context.background(), 0.1*time.second)
        else:
            tctx, _ = context.with_timeout(ctx, fraction*timeoutTotal)
        return tctx

    # unmount and wait for wcfs to exit
    # kill wcfs and abort FUSE connection if clean unmount fails
    # at the end make sure mount entry and mountpoint directory are removed

    def _():
        # when stop runs:
        # - wcsrv could be already `fusermount -u`'ed from outside
        # - the mountpoint could be also already removed from outside
        _rmdir_ifexists(wcsrv.mountpoint)
    defer(_)

    def _():
        # second unmount try, if first unmount failed and we had to kill
        # wcfs.go, abort FUSE connection and kill clients. This still can fail
        # because e.g. killing clients had not enough permissions, or because
        # new clients arrived.
        # TODO the second problem can be addressed with first mounting an empty
        #      directory over previous mount, but it is a privileged operation.
        #      Maybe we can do that via small "empty" fuse fs.
        if _is_mountpoint(wcsrv.mountpoint):
            log.warn("last unmount try")
            if _on_last_unmount_try is not None:
                _on_last_unmount_try(wcsrv.mountpoint)
            else:
                _fuse_unmount(wcsrv.mountpoint)
    defer(_)

    def _():
        # if mount is still there - it is likely because clients keep opened file descriptors to it
        # -> kill clients to free the mount
        #
        # NOTE if we do `fusermount -uz` (lazy unmount = MNT_DETACH), we will
        #      remove the mount from filesystem tree and /proc/mounts, but the
        #      clients will be left alive and using the old filesystem which is
        #      left in a bad ENOTCONN state. From this point of view restart of
        #      the clients is more preferred compared to leaving them running
        #      but actually disconnected from the data.
        #
        # TODO try to teach wcfs clients to detect filesystem in ENOTCONN state
        #      and reconnect automatically instead of being killed. Then we could
        #      use MNT_DETACH.
        if _is_mountpoint(wcsrv.mountpoint):
            lsof = list(wcsrv._mnt.lsof())
            for (proc, use) in lsof:
                log.warn("the mount is still used by %s:" % proc)
                for key, path in use.items():
                    log.warn("\t%s\t-> %s\n" % (key, path))

            if len(lsof) > 0:
                if _on_fs_busy is not None:
                    _on_fs_busy()
                else:
                    wg  = sync.WorkGroup(timeoutFrac(0.2))
                    def kill(ctx, proc):
                        dt  = ctx.deadline() - time.now()
                        log.warn("%s: <- SIGTERM" % proc)
                        os.kill(proc.pid, SIGTERM)
                        ctx1, _ = context.with_timeout(ctx, dt/2)
                        if _procwait_(ctx1, proc):
                            log.warn("%s: terminated" % proc)
                            return
                        if ctx.err() is not None:
                            raise ctx.err()
                        log.warn("%s: is still alive after SIGTERM" % proc)
                        log.warn("%s: <- SIGKILL" % proc)
                        os.kill(proc.pid, SIGKILL)
                        ctx2, _ = context.with_timeout(ctx, dt/2)
                        if _procwait_(ctx2, proc):
                            log.warn("%s: terminated" % proc)
                            return
                        log.warn("%s: does not exit after SIGKILL")

                    for (proc, _) in lsof:
                        wg.go(kill, proc)
                    wg.wait()
    defer(_)

    def _():
        if wcsrv._fuseabort is not None:
            wcsrv._fuseabort.close()
    defer(_)

    @func
    def _():
        # kill wcfs.go harder in case it is deadlocked and does not exit by itself
        if wcsrv._proc is None:
            return

        if _procwait_(timeoutFrac(0.2), wcsrv._proc):
            return

        log.warn("wcfs.go does not exit")
        log.warn(wcsrv._stuckdump())
        log.warn("-> kill -QUIT wcfs.go ...")
        os.kill(wcsrv._proc.pid, SIGQUIT)

        if _procwait_(timeoutFrac(0.2), wcsrv._proc):
            return
        log.warn("wcfs.go does not exit (after SIGQUIT)")
        log.warn(wcsrv._stuckdump())
        log.warn("-> kill -KILL wcfs.go ...")
        os.kill(wcsrv._proc.pid, SIGKILL)

        if _procwait_(timeoutFrac(0.2), wcsrv._proc):
            return
        log.warn("wcfs.go does not exit (after SIGKILL; probably it is stuck in kernel)")
        log.warn(wcsrv._stuckdump())
        log.warn("-> nothing we can do...")
        if _on_wcfs_stuck is not None:
            _on_wcfs_stuck()
        else:
            _procwait(context.background(), wcsrv._proc)
    defer(_)

    try:
        if _is_mountpoint(wcsrv.mountpoint): # could be unmounted from outside
            _fuse_unmount(wcsrv.mountpoint)
    except Exception as e:
        # if clean unmount failed -> kill -TERM wcfs and force abort of fuse connection.
        #
        # aborting fuse connection is needed in case wcfs/kernel will be stuck
        # in a deadlock even after being `kill -9`. See comments in tWCFS for details.
        def _():
            if wcsrv._proc is not None:
                log.warn("-> kill -TERM wcfs.go ...")
                os.kill(wcsrv._proc.pid, SIGTERM)
                if _procwait_(timeoutFrac(0.2), wcsrv._proc):
                    return
                log.warn("wcfs.go does not exit")
                log.warn(wcsrv._stuckdump())
            if wcsrv._fuseabort is not None:
                log.warn("-> abort FUSE connection ...")
                wcsrv._fuseabort.write(b"1\n")
                wcsrv._fuseabort.flush()
        defer(_)

        # treat first unmount failure as temporary - e.g. due to "device or resource is busy".
        # we'll be retrying to unmount the filesystem the second time after kill/fuse-abort.
        if not isinstance(e, _FUSEUnmountError):
            raise

# stop stops wcfs server for ZODB @ zurl.
#
# It makes sure that wcfs server is stopped and tries to unmount corresponding
# tree from the filesystem namespace. This unmount can fail with EBUSY if there
# are present client processes that still use anything under WCFS mountpoint.
# In such a case all present clients are terminated to proceed with successful
# unmount.
#
# TODO consider doing lazy unmount (MNT_DETACH) instead of killing clients.
#      See comments in Server.__stop for details.
#
# TODO tests
def stop(zurl):
    log.info("stop %s ...", zurl)

    # find mount entry for mntpt
    mnt = _lookup_wcmnt(zurl, nomount_ok=True)
    if mnt is None:
        log.info("not mounted")
        return
    log.info("mount entry: %s  (%s)" % (mnt.point, _devstr(mnt.dev)))

    # find server process that serves the mount
    wcsrv = _lookup_wcsrv(mnt, nosrv_ok=True)
    log.info("served by %s" % wcsrv._proc)
    wcsrv.stop()


# status verifies whether wcfs server for ZODB @ zurl is running in correct state.
#
# TODO tests
def status(zurl):
    log.info("status %s ...", zurl)

    def ok(msg):
        print("ok - %s" % msg)
    def fail(msg):
        print("fail - %s" % msg)
        raise RuntimeError('(failed)')

    # NOTE status has to be careful when touching mounted filesystem because
    #      any operation can hang if wcfs is deadlocked.

    # find mount entry for mntpt
    try:
        mnt = _lookup_wcmnt(zurl)
    except Exception as e:
        fail(e)
    ok("mount entry: %s  (%s)" % (mnt.point, _devstr(mnt.dev)))

    # find server process that serves the mount
    try:
        wcsrv = _lookup_wcsrv(mnt)
    except Exception as e:
        fail(e)
    ok("wcfs server: %s" % wcsrv._proc)

    # try to stat mountpoint and access set of files on the filesystem
    # any of this operation can hang if wcfs is deadlocked
    @func
    def verify(subj, f, *argv, **kw):
        # NOTE we cannot do IO checks in the same process even if in different thread:
        #
        #      If the check hangs then the file descriptor to wcfs will be still
        #      open and check's IO request on that fd will be still in
        #      progress. Which will prevent `wcfs status` process to exit
        #      because on exit the kernel will put the process into Z
        #      state (zombie) and try to close all its opened file descriptors.
        #      But when closing to-wcfs fd the kernel will see outstanding IO
        #      and try to cancel that via sending INTERRUPT to WCFS. That will
        #      cancel the IO only if wcfs is behaving correctly, but if wcfs
        #      is not behaving correctly, e.g. deadlocked, it will result in
        #      the IO request still be hanging, and so `wcfs status` also
        #      being stuck in Z state forever. Avoid that by doing all IO
        #      checks, that touch WCFS, in separate process. This way even if
        #      test process will get stuck, the main process of `wcfs status`
        #      will have no fd-relation to wcfs, and so will not get hang even
        #      if wcfs itself is deadlocked.
        kw = kw.copy()
        retcheck = kw.pop('retcheck', None)

        ctx, cancel = context.with_timeout(context.background(), 5*time.second)
        defer(cancel)

        exc = None
        try:
            ret = _proccall(ctx, f, *argv, **kw)
            if retcheck is not None:
                retcheck(ret)
        except Exception as e:
            exc = e

        if ctx.err() != None:
            def _():
                print(wcsrv._stuckdump())
            defer(_)
            fail("%s: timed out (wcfs might be stuck)" % subj)
        if exc is not None:
            fail("%s: %s" % (subj, exc))
        ok(subj)

    # stat mountpoint verifies that FUSE roundtrip to the server works
    # serving this takes lookupLock in nodefs but does not get zheadMu or any other wcfs-specific locks
    def _(st):
        if st.st_dev != mnt.dev:
            raise AssertionError("st_dev mismatch: got %s" % (_devstr(st.st_dev)))
    verify("stat mountpoint", os.stat, mnt.point, retcheck=_)

    # read .wcfs/zurl verifies FUSE roundtrip going a bit deeper to WCFS internals
    # but still does not take any WCFS-specific locks
    def _(_):
        if _ != zurl:
            raise AssertionError("zurl mismatch: got %s" % qq(_))
    verify("read .wcfs/zurl", xos.readfile, "%s/.wcfs/zurl" % mnt.point, retcheck=_)

    # read .wcfs/stats verifies FUSE roundtrip going more deeper into WCFS and
    # taking zheadMu and most other wcfs-specific locks while serving. As the
    # result this should likely hang if wcfs is deadlocked somewhere.
    verify("read .wcfs/stats", xos.readfile, "%s/.wcfs/stats" % mnt.point)


# _lookup_wcsrv returns WCFS Server with server process that is serving filesystem under mnt.
def _lookup_wcsrv(mnt, nosrv_ok=False): # -> Server  (with ._proc=None if nosrv_ok and no server proc is found)
    assert isinstance(mnt, xos.Mount)
    assert mnt.fstype == "fuse.wcfs"

    # lookup fuse connection and deduce user, that runs wcfs, from it
    fuseabort = open("/sys/fs/fuse/connections/%d/abort" % os.minor(mnt.dev), "wb")
    wcuid = os.fstat(fuseabort.fileno()).st_uid

    # find wcfs server process that is serving mounted filesystem
    # linux does not provide builtin mechanism to find FUSE server, so we use a
    # heuristic to do so.
    pdbc = xos.ProcDB.open(isolation_level=xos.ISOLATION_REPEATABLE_READ)
    def iswcfssrv(proc):
        # avoid checking .exe and .fd on processes we likely cannot access
        if not (proc.uid == wcuid and proc.comm == "wcfs"):
            return False
        if proc.exe not in (_wcfs_exe(), _wcfs_exe() + " (deleted)") :
            return False
        for ifd in proc.fd.values():
            # XXX strictly speaking need to check major/minor of corresponding
            # device discovered at runtime
            if ifd.path == "/dev/fuse":
                break
        else:
            return False
        if proc.argv[-2:] != (mnt.fssrc, mnt.point):
            return False
        return True

    _ = pdbc.query(iswcfssrv, eperm="warn")
    _ = list(_)
    if len(_) > 1:
        raise RuntimeError("multiple wcfs servers found: %s" % (_))
    if len(_) == 0:
        if not nosrv_ok:
            raise RuntimeError("no wcfs server found")
        wcproc = None
    else:
        wcproc = _[0]

    wcsrv = Server(mnt, wcproc, fuseabort)
    return wcsrv


# _stuckdump returns text that is useful for debug analysis in case wcfs is stuck.
#
# The text contains kernel tracebacks of wcfs and its clients as well the list
# of files those clients currently have opened on wcfs.
@func(Server)
def _stuckdump(wcsrv): # -> str
    v = []
    def emit(text=''):
        v.append(text+"\n")

    emit("\nwcfs ktraceback:")
    emit(wcsrv._proc.get('ktraceback', eperm="strerror", gone="strerror"))

    emit("\nwcfs clients:")
    for (proc, use) in wcsrv._mnt.lsof():
        emit("  %s %s" % (proc, proc.argv))
        for key, path in use.items():
            emit("\t%s\t-> %s" % (key, path))
        emit()
        emit(_indent("\t", proc.get('ktraceback', eperm="strerror", gone="strerror")))

    return ''.join(v)


# ---- misc ----

# _wcfs_exe returns path to wcfs executable.
def _wcfs_exe():
    return '%s/wcfs' % dirname(__file__)

# _mntpt_4zurl returns wcfs should-be mountpoint for ZODB @ zurl.
#
# it also makes sure the mountpoint exists.
def _mntpt_4zurl(zurl):
    # normalize zurl so that even if we have e.g. two neos:// urls coming
    # with different paths to ssl keys, or with different order in the list of
    # masters, we still have them associated with the same wcfs mountpoint.
    zurl = zurl_normalize_main(zurl)

    m = hashlib.sha1()
    m.update(b(zurl))

    # WCFS mounts are located under /dev/shm/wcfs. /dev/shm is already used by
    # userspace part of wendelin.core memory manager for dirtied pages.
    # In a sense WCFS mount provides shared read-only memory backed by ZODB.

    # mkdir /dev/shm/wcfs with stiky bit. This way multiple users can create subdirectories inside.
    wcfsroot = "/dev/shm/wcfs"
    wcfsmode = 0o777 | stat.S_ISVTX
    if _mkdir_p(wcfsroot):
        os.chmod(wcfsroot, wcfsmode)
    else:
        # migration workaround for the situation when /dev/shm/wcfs was created by
        # code that did not yet set sticky bit.
        _ = os.stat(wcfsroot)
        if _.st_uid == os.getuid():
            if _.st_mode != wcfsmode:
                os.chmod(wcfsroot, wcfsmode)

    mntpt = "%s/%s" % (wcfsroot, m.hexdigest())
    _mkdir_p(mntpt)
    return mntpt

# _lookup_wcmnt returns mount entry corresponding to WCFS service for ZODB @ zurl.
def _lookup_wcmnt(zurl, nomount_ok=False): # -> xos.Mount  (| None if nomount_ok)
    mntpt = _mntpt_4zurl(zurl)
    mnt = _lookup_mnt(mntpt, nomount_ok)
    if mnt is not None:
        if mnt.fstype != 'fuse.wcfs':
            raise RuntimeError("mount entry: fstype mismatch: %s", mnt.fstype)
        if mnt.fssrc  != zurl:
            raise RuntimeError("mount entry: zurl mismatch: %s", mnt.fssrc)
    return mnt

# _lookup_mnt returns mount entry corresponding to mntpt mountpoint.
def _lookup_mnt(mntpt, nomount_ok=False): # -> xos.Mount  (| None if nomount_ok)
    mdbc = xos.MountDB.open()
    _ = mdbc.query(lambda mnt: mnt.point == mntpt)
    _ = list(_)
    if len(_) == 0:
        if nomount_ok:
            return None
        raise RuntimeError("no mount entry for %s" % mntpt)
    if len(_) >  1:
        # NOTE if previous wcfs was lazy unmounted - there won't be multiple mount entries
        #      because MNT_DETACH (what lazy-unmount uses) removes entry from mount registry
        raise RuntimeError("multiple mount entries for %s" % mntpt)
    mnt = _[0]
    return mnt


# mkdir -p.
def _mkdir_p(path, mode=0o777): # -> created(bool)
    try:
        os.makedirs(path, mode)
    except OSError as e:
        if e.errno != EEXIST:
            raise
        return False
    return True

# rmdir if path exists.
def _rmdir_ifexists(path):
    try:
        os.rmdir(path)
    except OSError as e:
        if e.errno != ENOENT:
            raise

# _fuse_unmount calls `fusermount -u` + logs details if unmount failed.
#
# Additional options to fusermount can be passed via optv.
class _FUSEUnmountError(RuntimeError):
    pass
@func
def _fuse_unmount(mntpt, *optv):
    mdbc = xos.MountDB.open()
    _ = mdbc.query(lambda mnt: mnt.point == mntpt)
    _ = list(_)
    if len(_) == 0:
        raise RuntimeError("not a mountpoint: %s" % mntpt)
    assert len(_) == 1, _
    mnt = _[0]
    return _mnt_fuse_unmount(mnt, *optv)

@func
def _mnt_fuse_unmount(mnt, *optv):
    ret, out = _sysproccallout(["fusermount", "-u"] + list(optv) + [mnt.point])
    if ret != 0:
        # unmount failed, usually due to "device is busy".
        # Log which files are still opened by who and reraise
        def _():
            log.warn("# lsof %s" % mnt.point)
            try:
                _ = _lsof(mnt)
            except:
                log.exception("lsof failed")
            else:
                log.warn(_)
        defer(_)

        out = out.rstrip() # kill trailing \n\n
        opts = ' '.join(optv)
        if opts != '':
            opts += ' '
        emsg = "fuse_unmount %s%s: failed: %s" % (opts, mnt.point, out)
        log.warn(emsg)
        raise _FUSEUnmountError("%s\n(more details logged)" % emsg)

# lsof returns text description of which processes and which their file
# descriptors use specified mount.
def _lsof(mnt): # -> str
    # NOTE lsof(8) fails to work after wcfs goes into EIO mode
    #      fuser(1) works a bit better, but we still do it ourselves because we
    #      anyway need to customize output and integrate it with ktraceback
    s = ""
    for (proc, use) in mnt.lsof():
        s += "  %s %s\n" % (proc, proc.get("argv", eperm="strerror", gone="strerror"))
        for key, path in use.items():
            s += "\t%s\t-> %s\n" % (key, path)
    return s

# _is_mountpoint returns whether path is a mountpoint
def _is_mountpoint(path):    # -> bool
    # NOTE we don't call mountpoint directly on path, because if FUSE
    # fileserver failed, the mountpoint will also fail and print ENOTCONN
    try:
        _ = os.lstat(path)
    except OSError as e:
        if e.errno == ENOENT:
            return False
        # "Transport endpoint is not connected" -> it is a failed FUSE server
        # (XXX we can also grep /proc/mounts)
        if e.errno == ENOTCONN:
            return True
        raise

    if not S_ISDIR(_.st_mode):
        return False

    mounted = (0 == _sysproccall(["mountpoint", "-q", path]))
    return mounted


# _proccall invokes f(*argv, **kw) in freshly started subprocess and returns its result.
#
# ctx controls how long to wait for that subprocess to complete.
@func
def _proccall(ctx, f, *argv, **kw):
    kw = kw.copy()
    kw.setdefault('_nocincout', True)

    p = xmp.SubProcess(f, *argv, **kw)
    defer(p.close)

    ret = p.join(ctx)   # raises ctx.err or exception raised by f
    return ret


# _sysproc creates subprocess.Popen for "system" command.
#
# System commands are those that reside either in /bin or /usr/bin and which
# should be found even if $PATH does no contain those directories. For example
# runUnitTest in ERP5 sets $PATH without /bin, and this way executing
# fusermount via subprocess.Popen instead of _sysproc would fail.
def _sysproc(argv, **kw): # -> subprocess.Popen
    env = kw.get('env', None)
    if env is None:
        env = os.environ
    env = env.copy()
    path = env.get('PATH', '')
    if path:
        path += ':'
    path += '/bin:/usr/bin'
    env['PATH'] = path
    return subprocess.Popen(argv, env=env, close_fds=True, **kw)

# _sysproccall calls _sysproc and waits for spawned program to complete.
def _sysproccall(argv, **kw): # -> retcode
    return _sysproc(argv, **kw).wait()

# _sysproccallout calls _sysproc, waits for spawned program to complete and returns combined out/err.
def _sysproccallout(argv, **kw): # -> retcode, output
    proc = _sysproc(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kw)
    out, _ = proc.communicate()
    return proc.returncode, out


# _procwait waits for a process (subprocess.Popen | xos.Proc) to terminate.
def _procwait(ctx, proc):
    _waitfor(ctx, lambda: not _proc_isalive(proc))

# _procwait_, similarly to _procwait, waits for a process (subprocess.Popen | xos.Proc) to terminate.
#
# it returns bool whether process terminated or not - e.g. due to context being canceled.
def _procwait_(ctx, proc):   # -> ok
    return _waitfor_(ctx, lambda: not _proc_isalive(proc))

# _proc_isalive returns whether process (subprocess.Popen | xos.Proc) is alive or not.
def _proc_isalive(proc):  # -> bool
    assert isinstance(proc, (subprocess.Popen, xos.Proc))
    if isinstance(proc, subprocess.Popen):
        return proc.poll() is None
    if isinstance(proc, xos.Proc):
        # proc can be from connection with isolation_level being repeatable-read or snapshot
        # so proc cannot be used directly to recheck its status
        # -> use another proc with isolation=none to poll the status
        pdbc = xos.ProcDB.open(isolation_level=xos.ISOLATION_NONE)
        p = pdbc.get(proc.pid)
        if p is None:
            return False
        try:
            st = p.status['State']
        except xos.ProcGone:            # process completed and waited on by its parent
            return False
        return not st.startswith('Z')   # zombie


# _waitfor waits for condf() to become true.
def _waitfor(ctx, condf):
    wg = sync.WorkGroup(ctx)
    def _(ctx):
        while 1:
            if _ready(ctx.done()):
                raise ctx.err()
            if condf():
                return
            time.sleep(10*time.millisecond)
    wg.go(_)
    wg.wait()

# _waitfor_, similarly to _waitfor, waits for condf() to become true.
#
# it returns bool whether target condition was reached or not - e.g. due to
# context being canceled.
def _waitfor_(ctx, condf):   # -> ok
    try:
        _waitfor(ctx, condf)
    except Exception as e:
        if errors.Is(e, context.canceled) or errors.Is(e, context.deadlineExceeded):
            return False
        raise
    return True

# _ready reports whether chan ch is ready.
def _ready(ch):
    _, _rx = select(
        default,    # 0
        ch.recv,    # 1
    )
    return bool(_)


# serve starts and runs wcfs server for ZODB @ zurl.
#
# it mounts wcfs at a location that is with 1-1 correspondence with zurl.
# it then waits for wcfs to exit (either due to unmount or an error).
#
# it is an error if wcfs is already running.
#
# optv is list of options to pass to wcfs server.
# if exec_ is True, wcfs is not spawned, but executed into.
#
# serve(zurl, exec_=False).
def serve(zurl, optv, exec_=False, _tstartingq=None):
    mntpt = _mntpt_4zurl(zurl)
    optv  = _optv_with_wcfs_defaults(optv)
    log.info("serving %s ...", zurl)

    # try opening .wcfs - it is an error if we can do it.
    fwcfs, trylockstartf = _try_attach_wcsrv(mntpt)
    if fwcfs is not None:
        fwcfs.close()
        raise RuntimeError("wcfs: serve %s: already running" % zurl)

    # seems to be ok to start
    trylockstartf() # XXX -> "already running" if lock fails

    if _tstartingq is not None:
        _tstartingq.close()
    argv = [_wcfs_exe()] + list(optv) + [zurl, mntpt]
    if not exec_:
        subprocess.check_call(argv, close_fds=True)
    else:
        os.execv(argv[0], argv)


# _devstr returns human readable form of dev_t.
def _devstr(dev): # -> str  "major:minor"
    return "%d:%d" % (os.major(dev), os.minor(dev))

# _indent returns text with each line of it indented with prefix.
def _indent(prefix, text): # -> str
    linev = text.splitlines(True)
    return ''.join(prefix + _ for _ in linev)



# ----------------------------------------

# if called as main -> serve as frontend to wcfs service:
#
# wcfs serve  <zurl>
# wcfs status <zurl>
# wcfs stop   <zurl>
def _usage(w):
    progname = os.path.basename(sys.argv[0])
    print("Wcfs serves WCFS filesystem for ZODB at zurl for wendelin.core .\n", file=w)
    print("Usage: %s (serve|stop|status) [-h | wcfs.go options] zurl" % progname, file=w)
    sys.exit(2)

@func
def main():
    argv = sys.argv[1:]
    if len(argv) < 2 or argv[0] == '-h':
        _usage(sys.stderr)

    cmd  = argv[0]
    argv = argv[1:]
    zurl = argv[-1]     # -a -b zurl    -> zurl
    optv = argv[:-1]    # -a -b zurl    -> -a -b

    # setup log.warn/error to go to stderr, so that details could be seen on
    # e.g. "fuse_unmount: ... failed (more details logged)"
    # tune logging to use the same glog output format as on wcfs.go side.
    glog.basicConfig(stream=sys.stderr, level=logging.INFO)

    if cmd == "serve":
        if argv[0] == '-h':
            os.execv(_wcfs_exe(), [_wcfs_exe(), '-h'])
        serve(zurl, optv, exec_=True)

    elif cmd == "status":
        if optv:
            _usage(sys.stderr)
        status(zurl)

    elif cmd == "stop":
        if optv:
            _usage(sys.stderr)
        stop(zurl)

    else:
        print("wcfs: unknown command %s" % qq(cmd), file=sys.stderr)
        sys.exit(2)
