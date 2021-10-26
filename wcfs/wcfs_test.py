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
"""wcfs_test.py tests wcfs filesystem from outside as python client process.
"""

from __future__ import print_function, absolute_import

from wendelin.lib.testing import getTestDB
from wendelin.lib.zodb import dbclose, zstor_2zurl
from wendelin.lib.mem import memcpy
from wendelin.bigfile.file_zodb import ZBigFile
from wendelin.bigfile.tests.test_filezodb import blksize
from wendelin import wcfs

import transaction
from persistent import Persistent
from persistent.timestamp import TimeStamp

import sys, os, os.path
from thread import get_ident as gettid
from time import gmtime
from errno import EINVAL, ENOTCONN
from golang import go, chan, select, func, defer, b
from golang import context, time
from zodbtools.util import ashex as h
import pytest; xfail = pytest.mark.xfail
from pytest import raises, fail
from wendelin.wcfs.internal import io, mm
from wendelin.wcfs.internal.wcfs_test import install_sigbus_trap
from wendelin.wcfs import _is_mountpoint as is_mountpoint


# setup:
# - create test database, compute zurl and mountpoint for wcfs
# - at every test: make sure wcfs is not running before & after the test.

testdb = None
testzurl = None     # URL of testdb
testmntpt = None    # wcfs is mounted here
def setup_module():
    # if wcfs.py receives SIGBUS because wcfs.go panics while serving mmap'ed
    # read, we want to see python-level traceback instead of being killed.
    install_sigbus_trap()

    # if wcfs.go is built with race detector and detects a race - make it fail
    # current test loudly on the first wcfs.go race.
    gorace = os.environ.get("GORACE", "")
    if gorace != "":
        gorace += " "
    os.environ["GORACE"] = gorace + "halt_on_error=1"

    global testdb, testzurl, testmntpt
    testdb = getTestDB()
    testdb.setup()

    zstor = testdb.getZODBStorage()
    testzurl = zstor_2zurl(zstor)
    zstor.close()
    testmntpt = wcfs._mntpt_4zurl(testzurl)
    os.rmdir(testmntpt)

def teardown_module():
    testdb.teardown()

# make sure we start every test without wcfs server running.
def setup_function(f):
    assert not os.path.exists(testmntpt)
    with raises(KeyError):
        procmounts_lookup_wcfs(testzurl)

# make sure we unmount wcfs after every test.
# (tDB checks this in more detail, but join tests don't use tDB)
def teardown_function(f):
    mounted = is_mountpoint(testmntpt)
    if mounted:
        fuse_unmount(testmntpt)
    if os.path.exists(testmntpt):
        os.rmdir(testmntpt)
    with raises(KeyError):
        procmounts_lookup_wcfs(testzurl)

# fuse_unmount unmounts FUSE filesystem mounted @ mntpt.
def fuse_unmount(mntpt):
    assert is_mountpoint(mntpt)
    wcfs._fuse_unmount(mntpt)


# ---- test join/autostart/serve ----

# test that join works.
@func
def test_join():
    zurl = testzurl
    with raises(RuntimeError, match="wcfs: join .*: server not running"):
        wcfs.join(zurl, autostart=False)

    assert wcfs._wcregistry == {}
    def _():
        assert wcfs._wcregistry == {}
    defer(_)

    wcsrv = wcfs.start(zurl)
    defer(wcsrv.stop)
    assert wcsrv.mountpoint == testmntpt
    assert readfile(wcsrv.mountpoint + "/.wcfs/zurl") == zurl
    assert os.path.isdir(wcsrv.mountpoint + "/head")
    assert os.path.isdir(wcsrv.mountpoint + "/head/bigfile")

    wc = wcfs.join(zurl, autostart=False)
    defer(wc.close)
    assert wc.mountpoint == wcsrv.mountpoint
    assert wc._njoin == 1
    assert wc._wcsrv is None

    wc2 = wcfs.join(zurl, autostart=False)
    defer(wc2.close)
    assert wc2 is wc
    assert wc._njoin == 2

# test that join(autostart=y) works.
@func
def test_join_autostart():
    zurl = testzurl
    with raises(RuntimeError, match="wcfs: join .*: server not running"):
        wcfs.join(zurl, autostart=False)

    assert wcfs._wcregistry == {}
    def _():
        assert wcfs._wcregistry == {}
    defer(_)

    wc = wcfs.join(zurl, autostart=True)
    defer(wc.close)
    assert wc.mountpoint == testmntpt
    assert wc._njoin == 1
    assert readfile(wc.mountpoint + "/.wcfs/zurl") == zurl
    assert os.path.isdir(wc.mountpoint + "/head")
    assert os.path.isdir(wc.mountpoint + "/head/bigfile")


# verify that join successfully starts wcfs if previous wcfs exited uncleanly.
@func
def test_join_after_crash():
    zurl  = testzurl
    mntpt = testmntpt

    wc = start_and_crash_wcfs(zurl, mntpt)

    # start the server again - it should start ok despite that FUSE connection
    # to previously aborted wcfs is still there
    wc2 = wcfs.join(zurl, autostart=True)
    assert wc2 is not wc
    assert wcfs._wcregistry[mntpt] is wc2
    assert wc2.mountpoint == mntpt
    assert readfile(mntpt + "/.wcfs/zurl") == zurl

    # /proc/mounts should contain wcfs entry
    assert procmounts_lookup_wcfs(zurl) == mntpt

    # stop the server
    wc2.close()
    fuse_unmount(mntpt)

    # /proc/mounts entry should be gone
    with raises(KeyError):
        procmounts_lookup_wcfs(zurl)


# verify that start successfully starts server if previous wcfs exited uncleanly.
@func
def test_start_after_crash():
    zurl  = testzurl
    mntpt = testmntpt

    wc = start_and_crash_wcfs(zurl, mntpt)

    wcsrv = wcfs.start(zurl)
    defer(wcsrv.stop)
    assert wcsrv.mountpoint == mntpt
    assert readfile(mntpt + "/.wcfs/zurl") == zurl

    # /proc/mounts should contain wcfs entry
    assert procmounts_lookup_wcfs(zurl) == mntpt

    # stop the server - /proc/mounts entry should be gone
    wcsrv.stop()
    with raises(KeyError):
        procmounts_lookup_wcfs(zurl)


# verify that serve successfully starts if previous wcfs exited uncleanly.
@func
def test_serve_after_crash():
    zurl  = testzurl
    mntpt = testmntpt

    wc = start_and_crash_wcfs(zurl, mntpt)

    serve_starting = chan(dtype='C.structZ')
    serve_done     = chan(dtype='C.structZ')
    @func
    def _():
        defer(serve_done.close)
        wcfs.serve(zurl, [], _tstartingq=serve_starting)
    go(_)

    def _():
        fuse_unmount(mntpt)
        serve_done.recv()
    defer(_)

    serve_starting.recv() # wait before serve is going to spawn wcfs after cleanup
    wcfs._waitmount(timeout(), zurl, mntpt)

    assert readfile(mntpt + "/.wcfs/zurl") == zurl
    assert procmounts_lookup_wcfs(zurl) == mntpt


# start_and_crash_wcfs starts wcfs and then kills it.
# it returns closed WCFS connection that was connected to the killed WCFS server.
def start_and_crash_wcfs(zurl, mntpt): # -> WCFS
    # /proc/mounts should not contain wcfs entry
    with raises(KeyError):
        procmounts_lookup_wcfs(zurl)

    # start the server with attached client
    wcsrv = wcfs.start(zurl)
    assert wcsrv.mountpoint == mntpt
    assert mntpt not in wcfs._wcregistry

    wc = wcfs.join(zurl, autostart=False)
    assert wcfs._wcregistry[mntpt] is wc
    assert wc.mountpoint == mntpt
    assert readfile(mntpt + "/.wcfs/zurl") == zurl

    # /proc/mounts should now contain wcfs entry
    assert procmounts_lookup_wcfs(zurl) == mntpt


    # kill the server
    wcsrv._proc.kill() # sends SIGKILL
    assert wcsrv._proc.wait() != 0

    # access to filesystem should raise "Transport endpoint not connected"
    with raises(IOError) as exc:
        readfile(mntpt + "/.wcfs/zurl")
    assert exc.value.errno == ENOTCONN

    # client close should also raise "Transport endpoint not connected" but remove wc from _wcregistry
    assert wcfs._wcregistry[mntpt] is wc
    with raises(IOError) as exc:
        wc.close()
    assert exc.value.errno == ENOTCONN
    assert mntpt not in wcfs._wcregistry

    # /proc/mounts should still contain wcfs entry
    assert procmounts_lookup_wcfs(zurl) == mntpt

    return wc


# ---- infrastructure for data access tests ----
#
# Testing infrastructure consists of tDB and tFile that
# jointly organize wcfs behaviour testing. See individual classes for details.

# many tests need to be run with some reasonable timeout to detect lack of wcfs
# response. with_timeout and timeout provide syntactic shortcuts to do so.
def with_timeout(parent=context.background()):  # -> ctx, cancel
    return context.with_timeout(parent, 3*time.second)

def timeout(parent=context.background()):   # -> ctx
    ctx, _ = with_timeout()
    return ctx


# DF represents a change in files space.
class DF:
    # .rev      tid
    # .byfile   {} ZBigFile -> DFile
    def __init__(dF):
        # rev set from outside
        dF.byfile = {}

# DFile represents a change to one file.
class DFile:
    # .rev      tid
    # .ddata    {} blk -> data
    def __init__(dfile):
        # rev set from outside
        dfile.ddata = {}

# tDB/tWCFS provides database/wcfs testing environment.
#
# Database root and wcfs connection are represented by .root and .wc correspondingly.
# The database is initialized with one ZBigFile created and opened via ZODB connection as .zfile .
#
# The primary way to access wcfs is by opening BigFiles.
# A BigFile   opened under tDB is represented as tFile      - see .open for details.
#
# The database can be mutated (via !wcfs codepath) with .change + .commit .
# Current database head is represented by .head .
# The history of the changes is kept in .dFtail .
# There are various helpers to query history (_blkDataAt, ...)
#
# tDB must be explicitly closed once no longer used.
#
# TODO(?) print -> t.trace/debug() + t.verbose depending on py.test -v -v ?
class tWCFS(object):
    @func
    def __init__(t):
        assert not os.path.exists(testmntpt)
        wc = wcfs.join(testzurl, autostart=True)
        assert wc.mountpoint == testmntpt
        assert os.path.exists(wc.mountpoint)
        assert is_mountpoint(wc.mountpoint)
        t.wc = wc

        # force-unmount wcfs on timeout to unstuck current test and let it fail.
        # Force-unmount can be done reliably only by writing into
        # /sys/fs/fuse/connections/<X>/abort. For everything else there are
        # cases, when wcfs, even after receiving `kill -9`, will be stuck in kernel.
        # ( git.kernel.org/linus/a131de0a482a makes in-kernel FUSE client to
        #   still wait for request completion even after fatal signal )
        t._closed = chan()
        t._wcfuseaborted = chan()
        t._wcfuseabort = os.fdopen(os.dup(wc._wcsrv._fuseabort.fileno()), 'w')
        go(t._abort_ontimeout, 10*time.second)  # NOTE must be: with_timeout << · << wcfs_pin_timeout

    # _abort_ontimeout sends abort to fuse control file if timeout happens
    # before tDB is closed.
    def _abort_ontimeout(t, dt):
        _, _rx = select(
            time.after(dt).recv,    # 0
            t._closed.recv,         # 1
        )
        if _ == 1:
            return  # tDB closed = testcase completed

        # timeout -> force-umount wcfs
        eprint("\nC: test timed out after %.1fs" % (dt / time.second))
        eprint("-> aborting wcfs fuse connection to unblock ...\n")
        t._wcfuseabort.write(b"1\n")
        t._wcfuseabort.flush()
        t._wcfuseaborted.close()

    # close closes connection to wcfs, unmounts the filesystem and makes sure
    # that wcfs server exits.
    @func
    def close(t):
        def _():
            os.close(t._wcfuseabort)
        defer(t._closed.close)

        # unmount and wait for wcfs to exit
        def _():
            # run `fusermount -u` the second time after if wcfs was killed to
            # cleanup /proc/mounts.
            if is_mountpoint(t.wc.mountpoint):
                fuse_unmount(t.wc.mountpoint)
            assert not is_mountpoint(t.wc.mountpoint)
            os.rmdir(t.wc.mountpoint)
        defer(_)
        def _():
            def onstuck():
                fail("wcfs.go does not exit even after SIGKILL")
            t.wc._wcsrv._stop(timeout(), _onstuck=onstuck)
        defer(_)
        defer(t.wc.close)
        assert is_mountpoint(t.wc.mountpoint)


class tDB(tWCFS):
    @func
    def __init__(t):
        t.root = testdb.dbopen()
        def _(): # close/unlock db if __init__ fails
            exc = sys.exc_info()[1]
            if exc is not None:
                dbclose(t.root)
        defer(_)

        # start wcfs after testdb is created
        super(tDB, t).__init__()


        # ZBigFile(s) scheduled for commit
        t._changed = {} # ZBigFile -> {} blk -> data

        # committed: (tail, head] + δF history
        t.tail   = t.root._p_jar.db().storage.lastTransaction()
        t.dFtail = [] # of DF; head = dFtail[-1].rev

        # tracked opened tFiles
        t._files    = set()

        # ID of the thread which created tDB
        # ( transaction plays dirty games with threading.local and we have to
        #   check the thread is the same when .root is used )
        t._maintid = gettid()

        # prepare initial objects for test: zfile, nonzfile
        t.root['!file'] = t.nonzfile  = Persistent()
        t.root['zfile'] = t.zfile     = ZBigFile(blksize)
        t.at0 = t.commit()

    @property
    def head(t):
        return t.dFtail[-1].rev

    # close closes test database as well as all tracked files and wcfs.
    # it also prints change history to help developer overview current testcase.
    @func
    def close(t):
        defer(super(tDB, t).close)
        defer(lambda: dbclose(t.root))

        defer(t.dump_history)
        for tf in t._files.copy():
            tf.close()
        assert len(t._files)   == 0

    # open opens wcfs file corresponding to zf@at and starts to track it.
    # see returned tFile for details.
    def open(t, zf, at=None):   # -> tFile
        return tFile(t, zf, at=at)

    # change schedules zf to be changed according to changeDelta at commit.
    #
    # changeDelta: {} blk -> data.
    # data can be both bytes and unicode.
    def change(t, zf, changeDelta):
        assert isinstance(zf, ZBigFile)
        zfDelta = t._changed.setdefault(zf, {})
        for blk, data in changeDelta.iteritems():
            data = b(data)
            assert len(data) <= zf.blksize
            zfDelta[blk] = data

    # commit commits transaction and makes sure wcfs is synchronized to it.
    #
    # It updates .dFtail and returns committed transaction ID.
    #
    # zf and changeDelta can be optionally provided, in which case .change(zf,
    # changeDelta) call is made before actually committing.
    def commit(t, zf=None, changeDelta=None):   # -> tAt
        if zf is not None:
            assert changeDelta is not None
            t.change(zf, changeDelta)

        # perform modifications scheduled by change.
        # use !wcfs mode so that we prepare data independently of wcfs code paths.
        dF = DF()
        zconns = set()
        for zf, zfDelta in t._changed.items():
            dfile = DFile()
            zconns.add(zf._p_jar)
            zfh = zf.fileh_open()   # NOTE does not use wcfs
            for blk, data in zfDelta.iteritems():
                dfile.ddata[blk] = data
                data += b'\0'*(zf.blksize - len(data))  # trailing \0
                vma = zfh.mmap(blk, 1)
                memcpy(vma, data)
            dF.byfile[zf] = dfile

        # verify that all changed objects come from the same ZODB connection
        assert len(zconns) in (0, 1)    # either nothing to commit or all from the same zconn
        if len(zconns) == 1:
            zconn = zconns.pop()
            root = zconn.root()
        else:
            # no objects to commit
            root = t.root
            assert gettid() == t._maintid

        # perform the commit. NOTE there is no clean way to retrieve tid of
        # just committed transaction - we use last._p_serial as workaround.
        root['_last'] = last = Persistent()
        last._p_changed = 1
        transaction.commit()
        head = tAt(t, last._p_serial)

        dF.rev = head
        for dfile in dF.byfile.values():
            dfile.rev = head
        t.dFtail.append(dF)
        assert t.head == head   # self-check

        print('\nM: commit -> %s' % head)
        for zf, zfDelta in t._changed.items():
            print('M:      f<%s>\t%s' % (h(zf._p_oid), sorted(zfDelta.keys())))
        t._changed = {}

        # synchronize wcfs to db, and we are done
        t._wcsync()
        return head

    # _wcsync makes sure wcfs is synchronized to latest committed transaction.
    def _wcsync(t):
        # XXX stub: unmount/remount + close/reopen files until wcfs supports invalidations
        files = t._files.copy()
        for tf in files:
            tf.close()
        tWCFS.close(t)
        tWCFS.__init__(t)
        for tf in files:
            tf.__init__(t, tf.zf, tf.at)
        assert len(t._files) == len(files)


# tFile provides testing environment for one bigfile opened on wcfs.
#
# ._blk() provides access to data of a block.
# .assertBlk/.assertData assert
# on state of data.
class tFile:
    # maximum number of pages we mmap for 1 file.
    _max_tracked_pages = 8

    def __init__(t, tdb, zf, at=None):
        assert isinstance(zf, ZBigFile)
        t.tdb = tdb
        t.zf  = zf
        t.at  = at
        t.f   = tdb.wc._open(zf, at=at)
        t.blksize = zf.blksize
        t.fmmap = None
        tdb._files.add(t)

        # make sure that wcfs reports zf.blksize as preferred block size for IO.
        # wcfs.py also uses .st_blksize in blk -> byte offset computation.
        st = os.fstat(t.f.fileno())
        assert st.st_blksize == t.blksize

        # mmap the file past the end up to _max_tracked_pages
        assert t.blksize % mm.PAGE_SIZE == 0
        t.fmmap = mm.map_ro(t.f.fileno(), 0, t._max_tracked_pages*t.blksize)

    def close(t):
        t.tdb._files.remove(t)
        if t.fmmap is not None:
            mm.unmap(t.fmmap)
        t.f.close()

    # _blk returns memoryview of file[blk].
    # when/if block memory is accessed, the user has to notify tFile with _blkaccess call.
    def _blk(t, blk):
        assert blk <= t._max_tracked_pages
        return memoryview(t.fmmap[blk*t.blksize:(blk+1)*t.blksize])

    def _blkaccess(t, blk):
        if t.at is None:    # notify tDB only for head/file access
            t.tdb._blkheadaccess(t.zf, blk)

    # _sizeinblk returns file size in blocks.
    def _sizeinblk(t):
        st = os.fstat(t.f.fileno())
        assert st.st_blksize == t.blksize   # just in case
        assert st.st_size % t.blksize == 0
        assert st.st_size // t.blksize <= t._max_tracked_pages
        return st.st_size // t.blksize

    # assertBlk asserts that file[blk] has data as expected.
    #
    # Expected data may be given with size < t.blksize. In such case the data
    # is implicitly appended with trailing zeros. Data can be both bytes and unicode.
    @func
    def assertBlk(t, blk, dataok):
        # TODO -> assertCtx('blk #%d' % blk)
        def _():
            assertCtx = 'blk #%d' % blk
            _, e, _ = sys.exc_info()
            if isinstance(e, AssertionError):
                assert len(e.args) == 1 # pytest puts everything as args[0]
                e.args = (assertCtx + "\n" + e.args[0],)
        defer(_)

        dataok = b(dataok)
        blkdata, _ = t.tdb._blkDataAt(t.zf, blk, t.at)
        assert blkdata == dataok, "computed vs explicit data"
        t._assertBlk(blk, dataok)

    @func
    def _assertBlk(t, blk, dataok):
        assert len(dataok) <= t.blksize
        dataok += b'\0'*(t.blksize - len(dataok))   # tailing zeros
        assert blk < t._sizeinblk()

        blkview = t._blk(blk)

        # verify full data of the block
        # TODO(?) assert individually for every block's page? (easier debugging?)
        assert blkview.tobytes() == dataok


    # assertData asserts that file has data blocks as specified.
    #
    # Expected blocks may be given with size < zf.blksize. In such case they
    # are implicitly appended with trailing zeros. If a block is specified as
    # 'x' - this particular block is not accessed and is not checked.
    #
    # The file size and optionally mtime are also verified.
    def assertData(t, dataokv, mtime=None):
        st = os.fstat(t.f.fileno())
        assert st.st_blksize == t.blksize
        assert st.st_size == len(dataokv)*t.blksize
        if mtime is not None:
            assert st.st_mtime == tidtime(mtime)

        for blk, dataok in enumerate(dataokv):
            if dataok == 'x':
                continue
            t.assertBlk(blk, dataok)


# ---- infrastructure: helpers to query dFtail/accessed history ----

# _blkDataAt returns expected zf[blk] data and its revision as of @at database state.
#
# If the block is hole - (b'', at0) is returned.  XXX -> @z64?
# Hole include cases when the file does not exists, or when blk is > file size.
@func(tDB)
def _blkDataAt(t, zf, blk, at): # -> (data, rev)
    if at is None:
        at = t.head

    # all changes to zf
    vdf = [_.byfile[zf] for _ in t.dFtail if zf in _.byfile]

    # changes to zf[blk] <= at
    blkhistoryat = [_ for _ in vdf if blk in _.ddata and _.rev <= at]
    if len(blkhistoryat) == 0:
        # blk did not existed @at
        data = b''
        rev  = t.dFtail[0].rev  # was hole - at0
    else:
        _ = blkhistoryat[-1]
        data = _.ddata[blk]
        rev  = _.rev

    assert rev <= at
    return data, rev


# -------------------------------------
# ---- actual tests to access data ----

# exercise wcfs functionality
# plain data access.
@func
def test_wcfs_basic():
    t = tDB(); zf = t.zfile
    defer(t.close)

    # >>> lookup non-BigFile -> must be rejected
    with raises(OSError) as exc:
        t.wc._stat("head/bigfile/%s" % h(t.nonzfile._p_oid))
    assert exc.value.errno == EINVAL

    # >>> file initially empty
    f = t.open(zf)
    f.assertData ([], mtime=t.at0)

    # >>> (@at1) commit data -> we can see it on wcfs
    at1 = t.commit(zf, {2:'c1'})

    f.assertData (['','','c1']) # TODO + mtime=t.head

    # >>> (@at2) commit again -> we can see both latest and snapshotted states
    # NOTE blocks e(4) and f(5) will be accessed only in the end
    at2 = t.commit(zf, {2:'c2', 3:'d2', 5:'f2'})

    # f @head
    f.assertData (['','', 'c2', 'd2', 'x','x']) # TODO + mtime=t.head

    # f @at1
    f1 = t.open(zf, at=at1)
    f1.assertData (['','','c1']) # TODO + mtime=at1


    # >>> (@at3) commit again without changing zf size
    f2 = t.open(zf, at=at2)
    at3 = t.commit(zf, {0:'a3', 2:'c3', 5:'f3'})

    # f @head
    f.assertData (['a3','','c3','d2','x','x']) # TODO + mtime=t.head

    # f @at2
    # NOTE f(2) is accessed but via @at/ not head/  ; f(2) in head/zf remains unaccessed
    f2.assertData (['','','c2','d2','','f2']) # TODO mtime=at2

    # f @at1
    f1.assertData (['','','c1']) # TODO mtime=at1


# verify that read after file size returns (0, ok)
# (the same behaviour as on e.g. ext4 and as requested by posix)
@func
def test_wcfs_basic_read_aftertail():
    t = tDB(); zf = t.zfile
    defer(t.close)

    t.commit(zf, {2:'c1'})
    f = t.open(zf)
    f.assertData(['','','c1'])

    def _(off): # -> bytes read from f[off +4)
        buf = bytearray(4)
        n = io.readat(f.f.fileno(), off, buf)
        return bytes(buf[:n])

    assert _(0*blksize)     == b'\x00\x00\x00\x00'
    assert _(1*blksize)     == b'\x00\x00\x00\x00'
    assert _(2*blksize)     == b'c1\x00\x00'
    assert _(3*blksize-4)   == b'\x00\x00\x00\x00'
    assert _(3*blksize-3)   == b'\x00\x00\x00'
    assert _(3*blksize-2)   == b'\x00\x00'
    assert _(3*blksize-1)   == b'\x00'
    assert _(3*blksize-0)   == b''
    assert _(3*blksize+1)   == b''
    assert _(3*blksize+2)   == b''
    assert _(3*blksize+3)   == b''
    assert _(4*blksize)     == b''
    assert _(8*blksize)     == b''
    assert _(100*blksize)   == b''


# ---- misc ---

# readfile reads file @ path.
def readfile(path):
    with open(path) as f:
        return f.read()

# writefile writes data to file @ path.
def writefile(path, data):
    with open(path, "w") as f:
        f.write(data)

# tidtime converts tid to transaction commit time.
def tidtime(tid):
    t = TimeStamp(tid).timeTime()

    # ZODB/py vs ZODB/go time resolution is not better than 1µs
    # see e.g. https://lab.nexedi.com/kirr/neo/commit/9112f21e
    #
    # NOTE pytest.approx supports only ==, not e.g. <, so we use plain round.
    return round(t, 6)

# tidfromtime converts time into corresponding transaction ID.
def tidfromtime(t):
    f = t - int(t)      # fraction of seconds
    t = int(t)
    _ = gmtime(t)
    s = _.tm_sec + f    # total seconds

    ts = TimeStamp(_.tm_year, _.tm_mon, _.tm_mday, _.tm_hour, _.tm_min, s)
    return ts.raw()

# verify that tidtime is precise enough to show difference in between transactions.
# verify that tidtime -> tidfromtime is identity within rounding tolerance.
@func
def test_tidtime():
    t = tDB()
    defer(t.close)

    # tidtime not rough
    atv = [t.commit()]
    for i in range(10):
        at = t.commit()
        assert tidtime(at) > tidtime(atv[-1])
        atv.append(at)

    # tidtime -> tidfromtime
    for at in atv:
        tat  = tidtime(at)
        at_  = tidfromtime(tat)
        tat_ = tidtime(at_)
        assert abs(tat_ - tat) <= 2E-6


# tAt is bytes whose repr returns human readable string considering it as `at` under tDB.
#
# It gives both symbolic version and raw hex forms, for example:
#   @at2 (03cf7850500b5f66)
#
# tAt is used everywhere with the idea that e.g. if an assert comparing at, or
# e.g. dicts containing at, fails, everything is printed in human readable
# form instead of raw hex that is hard to visibly map to logical transaction.
class tAt(bytes):
    def __new__(cls, tdb, at):
        tat = bytes.__new__(cls, at)
        tat.tdb = tdb
        return tat

    def __repr__(at):
        t = at.tdb
        for i, dF in enumerate(t.dFtail):
            if dF.rev == at:
                    return "@at%d (%s)" % (i, h(at))
        return "@" + h(at)
    __str__ = __repr__


# zfiles returns ZBigFiles that were ever changed under t.
@func(tDB)
def zfiles(t):
    zfs = set()
    for dF in t.dFtail:
        for zf in dF.byfile:
            if zf not in zfs:
                zfs.add(zf)
    return zfs


# dump_history prints t's change history in tabular form.
#
# the output is useful while developing or analyzing a test failure: to get
# overview of how file(s) are changed in tests.
@func(tDB)
def dump_history(t):
    print('>>> Change history by file:')
    for zf in t.zfiles():
        print('\nf<%s>:' % h(zf._p_oid))
        indent = '\t%s\t' % (' '*len('%s' % t.head),)
        print('%s%s' % (indent, ' '.join('01234567')))
        print('%s%s' % (indent, ' '.join('abcdefgh')))
        for dF in t.dFtail:
            df = dF.byfile.get(zf)
            emitv = []
            if df is not None:
                dblk = set(df.ddata.keys())
                for blk in range(max(dblk)+1):
                    if blk in dblk:
                        emitv.append('%d' % blk)
                    else:
                        emitv.append(' ')

            print('\t%s\t%s' % (dF.rev, ' '.join(emitv)))
    print()


# procmounts_lookup_wcfs returns /proc/mount entry for wcfs mounted to serve zurl.
def procmounts_lookup_wcfs(zurl): # -> mountpoint | KeyError
    for line in readfile('/proc/mounts').splitlines():
        # <zurl> <mountpoint> fuse.wcfs ...
        zurl_, mntpt, typ, _ = line.split(None, 3)
        if typ != 'fuse.wcfs':
            continue
        if zurl_ == zurl:
            return mntpt
    raise KeyError("lookup wcfs %s: no /proc/mounts entry" % zurl)

# eprint prints msg to stderr
def eprint(msg):
    print(msg, file=sys.stderr)
