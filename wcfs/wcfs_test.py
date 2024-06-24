# -*- coding: utf-8 -*-
# Copyright (C) 2018-2022  Nexedi SA and Contributors.
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

Virtmem layer provided by wcfs client package is unit-tested by
wcfs/client/client_test.py .

At functional level, the whole wendelin.core test suite is used to verify
wcfs.py/wcfs.go while running tox tests in wcfs mode.
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
from ZODB.utils import z64, u64, p64

import sys, os, os.path, subprocess
import six
from six.moves._thread import get_ident as gettid
from time import gmtime
from errno import EINVAL, ENOTCONN
from resource import setrlimit, getrlimit, RLIMIT_MEMLOCK
from golang import go, chan, select, func, defer, error, b
from golang import context, errors, sync, time
from zodbtools.util import ashex as h, fromhex
import pytest; xfail = pytest.mark.xfail
from pytest import raises, fail
from wendelin.wcfs.internal import io, mm
from wendelin.wcfs.internal.wcfs_test import _tWCFS, read_exfault_nogil, SegmentationFault, install_sigbus_trap, fadvise_dontneed
from wendelin.wcfs.client._wcfs import _tpywlinkwrite as _twlinkwrite
from wendelin.wcfs import _is_mountpoint as is_mountpoint, _procwait as procwait, _ready as ready, _rmdir_ifexists as rmdir_ifexists


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

    # ↑ memlock soft-limit till its hard maximum
    # (tFile needs ~ 64M to mlock while default memlock soft-limit is usually 64K)
    memlockS, memlockH = getrlimit(RLIMIT_MEMLOCK)
    if memlockS != memlockH:
        setrlimit(RLIMIT_MEMLOCK, (memlockH, memlockH))

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
    rmdir_ifexists(testmntpt)
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
# Testing infrastructure consists of tDB, tFile, tWatch and tWatchLink that
# jointly organize wcfs behaviour testing. See individual classes for details.

# many tests need to be run with some reasonable timeout to detect lack of wcfs
# response. with_timeout and timeout provide syntactic shortcuts to do so.
def with_timeout(parent=context.background()):  # -> ctx, cancel
    return context.with_timeout(parent, 3*time.second)

def timeout(parent=context.background()):   # -> ctx
    ctx, _ = with_timeout()
    return ctx

# tdelay is used in places where we need to delay a bit in order to e.g.
# increase probability of a bug due to race condition.
def tdelay():
    time.sleep(10*time.millisecond)


# DF represents a change in files space.
# it corresponds to ΔF in wcfs.go .
class DF:
    # .rev      tid
    # .byfile   {} ZBigFile -> DFile
    def __init__(dF):
        # rev set from outside
        dF.byfile = {}

# DFile represents a change to one file.
# it is similar to ΔFile in wcfs.go .
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
# The primary way to access wcfs is by opening BigFiles and WatchLinks.
# A BigFile   opened under tDB is represented as tFile      - see .open for details.
# A WatchLink opened under tDB is represented as tWatchLink - see .openwatch for details.
#
# The database can be mutated (via !wcfs codepath) with .change + .commit .
# Current database head is represented by .head .
# The history of the changes is kept in .dFtail .
# There are various helpers to query history (_blkDataAt, _pinnedAt, .iter_revv, ...)
#
# tDB must be explicitly closed once no longer used.
#
# TODO(?) print -> t.trace/debug() + t.verbose depending on py.test -v -v ?
class tWCFS(_tWCFS):
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
        nogilready = chan(dtype='C.structZ')
        t._wcfuseabort = os.dup(wc._wcsrv._fuseabort.fileno())
        go(t._abort_ontimeout, t._wcfuseabort, 10*time.second, nogilready)   # NOTE must be: with_timeout << · << wcfs_pin_timeout
        nogilready.recv()   # wait till _abort_ontimeout enters nogil

    # _abort_ontimeout is in wcfs_test.pyx

    # close closes connection to wcfs, unmounts the filesystem and makes sure
    # that wcfs server exits.
    @func
    def close(t):
        def _():
            os.close(t._wcfuseabort)
        defer(_)
        defer(t._closed.close)

        # unmount and wait for wcfs to exit
        def _():
            # run `fusermount -u` the second time after if wcfs was killed to
            # cleanup /proc/mounts.
            if is_mountpoint(t.wc.mountpoint):
                fuse_unmount(t.wc.mountpoint)
            assert not is_mountpoint(t.wc.mountpoint)
        defer(_)
        def _():
            def onstuck():
                fail("wcfs.go does not exit even after SIGKILL")
            t.wc._wcsrv._stop(timeout(), _onstuck=onstuck)
        defer(_)
        defer(t.wc.close)
        assert is_mountpoint(t.wc.mountpoint)


class tDB(tWCFS):
    # __init__ initializes test database and wcfs.
    #
    # old_data can be optinally provided to specify ZBigFile revisions to
    # create before wcfs startup. old_data is []changeDelta - see .commit
    # and .change for details.
    @func
    def __init__(t, old_data=[]):
        t.root = testdb.dbopen()
        def _(): # close/unlock db if __init__ fails
            exc = sys.exc_info()[1]
            if exc is not None:
                dbclose(t.root)
        defer(_)

        # ZBigFile(s) scheduled for commit
        t._changed = {} # ZBigFile -> {} blk -> data

        # committed: (tail, head] + δF history
        t.tail   = t.root._p_jar.db().storage.lastTransaction()
        t.dFtail = [] # of DF; head = dFtail[-1].rev

        # ID of the thread which created tDB
        # ( transaction plays dirty games with threading.local and we have to
        #   check the thread is the same when .root is used )
        t._maintid = gettid()

        # prepare initial objects for test: zfile, nonzfile
        t.root['!file'] = t.nonzfile  = Persistent()
        t.root['zfile'] = t.zfile     = ZBigFile(blksize)
        t.at0 = t._commit()

        # commit initial data before wcfs starts
        for changeDelta in old_data:
            t._commit(t.zfile, changeDelta)

        # start wcfs after testdb is created and initial data is committed
        super(tDB, t).__init__()

        # fh(.wcfs/zhead) + history of zhead read from there
        t._wc_zheadfh = open(t.wc.mountpoint + "/.wcfs/zhead")

        # whether head/ ZBigFile(s) blocks were ever accessed via wcfs.
        # this is updated only explicitly via ._blkheadaccess() .
        t._blkaccessedViaHead = {} # ZBigFile -> set(blk)

        # tracked opened tFiles & tWatchLinks
        t._files    = set()
        t._wlinks   = set()

    @property
    def head(t):
        return t.dFtail[-1].rev

    # close closes test database as well as all tracked files, watch links and wcfs.
    # it also prints change history to help developer overview current testcase.
    @func
    def close(t):
        defer(super(tDB, t).close)
        defer(lambda: dbclose(t.root))

        defer(t.dump_history)
        for tf in t._files.copy():
            tf.close()
        for tw in t._wlinks.copy():
            tw.close()
        assert len(t._files)   == 0
        assert len(t._wlinks)  == 0
        t._wc_zheadfh.close()

    # open opens wcfs file corresponding to zf@at and starts to track it.
    # see returned tFile for details.
    def open(t, zf, at=None):   # -> tFile
        return tFile(t, zf, at=at)

    # openwatch opens /head/watch on wcfs.
    # see returned tWatchLink for details.
    def openwatch(t):   # -> tWatchLink
        return tWatchLink(t)

    # change schedules zf to be changed according to changeDelta at commit.
    #
    # changeDelta: {} blk -> data.
    # data can be both bytes and unicode.
    def change(t, zf, changeDelta):
        assert isinstance(zf, ZBigFile)
        zfDelta = t._changed.setdefault(zf, {})
        for blk, data in six.iteritems(changeDelta):
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
        head = t._commit(zf, changeDelta)

        # make sure wcfs is synchronized to committed transaction
        l = t._wc_zheadfh.readline()
        #print('> zhead read: %r' % l)
        l = l.rstrip('\n')
        wchead = tAt(t, fromhex(l))
        if wchead != t.dFtail[-1].rev:
            raise RuntimeError("commit #%d: wcsync: wczhead (%s) != zhead (%s)" %
                                    (len(t.dFtail), wchead, t.dFtail[-1].rev))
        assert t.wc._read("head/at") == h(head)

        return head

    def _commit(t, zf=None, changeDelta=None):  # -> tAt
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
            zfh = zf.fileh_open(_use_wcfs=False)
            for blk, data in six.iteritems(zfDelta):
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

        return head

    # _blkheadaccess marks head/zf[blk] accessed.
    def _blkheadaccess(t, zf, blk):
        t._blkaccessed(zf).add(blk)

    # _blkaccessed returns set describing which head/zf blocks were ever accessed.
    def _blkaccessed(t, zf): # set(blk)
        return t._blkaccessedViaHead.setdefault(zf, set())


# tFile provides testing environment for one bigfile opened on wcfs.
#
# ._blk() provides access to data of a block. .cached() gives state of which
# blocks are in OS pagecache. .assertCache and .assertBlk/.assertData assert
# on state of cache and data.
class tFile:
    # maximum number of pages we mmap for 1 file.
    # this should be not big not to exceed mlock limit.
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

        # mmap the file past the end up to _max_tracked_pages and setup
        # invariants on which we rely to verify OS cache state:
        #
        # 1. lock pages with MLOCK_ONFAULT: this way after a page is read by
        #    mmap access we have the guarantee from kernel that the page will
        #    stay in pagecache.
        #
        # 2. madvise memory with MADV_SEQUENTIAL and MADV_RANDOM in interleaved
        #    mode. This adjusts kernel readahead (which triggers for
        #    MADV_NORMAL or MADV_SEQUENTIAL vma) to not go over to next block
        #    and thus a read access to one block won't trigger implicit read
        #    access to its neighbour block.
        #
        #      https://www.quora.com/What-heuristics-does-the-adaptive-readahead-implementation-in-the-Linux-kernel-use
        #      https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/mm/madvise.c?h=v5.2-rc4#n51
        #
        #    we don't use MADV_NORMAL instead of MADV_SEQUENTIAL, because for
        #    MADV_NORMAL, there is not only read-ahead, but also read-around,
        #    which might result in accessing previous block.
        #
        #    we don't disable readahead universally, since enabled readahead
        #    helps to test how wcfs handles simultaneous read triggered by
        #    async kernel readahead vs wcfs uploading data for the same block
        #    into OS cache. Also, fully enabled readahead is how wcfs is
        #    actually used in practice.
        assert t.blksize % mm.PAGE_SIZE == 0
        t.fmmap = mm.map_ro(t.f.fileno(), 0, t._max_tracked_pages*t.blksize)

        mm.lock(t.fmmap, mm.MLOCK_ONFAULT)

        for blk in range(t._max_tracked_pages):
            blkmmap = t.fmmap[blk*t.blksize:(blk+1)*t.blksize]
            # NOTE the kernel does not start readahead from access to
            # MADV_RANDOM vma, but for a MADV_{NORMAL/SEQUENTIAL} vma it starts
            # readahead which can go _beyond_ vma that was used to decide RA
            # start. For this reason - to prevent RA started at one block to
            # overlap with the next block, we put MADV_RANDOM vma at the end of
            # every block covering last 1/8 of it.
            # XXX implicit assumption that RA window is < 1/8·blksize
            #
            # NOTE with a block completely covered by MADV_RANDOM the kernel
            # issues 4K sized reads; wcfs starts uploading into cache almost
            # immediately, but the kernel still issues many reads to read the
            # full 2MB of the block. This works slowly.
            # XXX -> investigate and maybe make read(while-uploading) wait for
            # uploading to complete and only then return? (maybe it will help
            # performance even in normal case)
            _ = len(blkmmap)*7//8
            mm.advise(blkmmap[:_], mm.MADV_SEQUENTIAL)
            mm.advise(blkmmap[_:], mm.MADV_RANDOM)

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

    # cached returns [] with indicating whether a file block is cached or not.
    # 1 - cached, 0 - not cached, fractional (0,1) - some pages of the block are cached some not.
    def cached(t):
        l = t._sizeinblk()
        incorev = mm.incore(t.fmmap[:l*t.blksize])
        # incorev is in pages; convert to in blocks
        assert t.blksize % mm.PAGE_SIZE == 0
        blkpages = t.blksize // mm.PAGE_SIZE
        cachev = [0.]*l
        for i, v in enumerate(incorev):
            blk = i // blkpages
            cachev[blk] += bool(v)
        for blk in range(l):
            cachev[blk] /= blkpages
            if cachev[blk] == int(cachev[blk]):
                cachev[blk] = int(cachev[blk])  # 0.0 -> 0, 1.0 -> 1
        return cachev

    # _sizeinblk returns file size in blocks.
    def _sizeinblk(t):
        st = os.fstat(t.f.fileno())
        assert st.st_blksize == t.blksize   # just in case
        assert st.st_size % t.blksize == 0
        assert st.st_size // t.blksize <= t._max_tracked_pages
        return st.st_size // t.blksize

    # assertCache asserts on state of OS cache for file.
    #
    # incorev is [] of 1/0 representing whether block data is present or not.
    def assertCache(t, incorev):
        assert t.cached() == incorev

    # assertBlk asserts that file[blk] has data as expected.
    #
    # Expected data may be given with size < t.blksize. In such case the data
    # is implicitly appended with trailing zeros. Data can be both bytes and unicode.
    #
    # It also checks that file watches are properly notified on data access -
    # - see "7.2) for all registered client@at watchers ..."
    #
    # pinokByWLink: {} tWatchLink -> {} blk -> at.
    # pinokByWLink can be omitted - in that case it is computed only automatically.
    #
    # The automatic computation of pinokByWLink is verified against explicitly
    # provided pinokByWLink when it is present.
    @func
    def assertBlk(t, blk, dataok, pinokByWLink=None):
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
        t._assertBlk(blk, dataok, pinokByWLink)

    @func
    def _assertBlk(t, blk, dataok, pinokByWLink=None, pinfunc=None):
        assert len(dataok) <= t.blksize
        dataok += b'\0'*(t.blksize - len(dataok))   # tailing zeros
        assert blk < t._sizeinblk()

        # access to this block must not trigger access to other blocks
        incore_before = t.cached()
        def _():
            incore_after = t.cached()
            incore_before[blk] = 'x'
            incore_after [blk] = 'x'
            assert incore_before == incore_after
        defer(_)

        cached = t.cached()[blk]
        assert cached in (0, 1) # every check accesses a block in full
        shouldPin = False       # whether at least one wlink should receive a pin

        # watches that must be notified if access goes to @head/file
        wpin = {}   # tWatchLink -> pinok
        blkrev = t.tdb._blkRevAt(t.zf, blk, t.at)
        if t.at is None: # @head/...
            for wlink in t.tdb._wlinks:
                pinok = {}
                w = wlink._watching.get(t.zf._p_oid)
                if w is not None and w.at < blkrev:
                    if cached == 1:
                        # @head[blk].rev is after w.at - w[blk] must be already pinned
                        assert blk in w.pinned
                        assert w.pinned[blk] <= w.at
                    else:
                        assert cached == 0
                        # even if @head[blk] is uncached, the block could be
                        # already pinned by setup watch
                        if blk not in w.pinned:
                            pinok = {blk: t.tdb._blkRevAt(t.zf, blk, w.at)}
                            shouldPin = True
                wpin[wlink] = pinok

        if pinokByWLink is not None:
            assert wpin == pinokByWLink, "computed vs explicit pinokByWLink"
        pinokByWLink = wpin

        # doCheckingPin expects every wlink entry to also contain zf
        for wlink, pinok in pinokByWLink.items():
            pinokByWLink[wlink] = (t.zf, pinok)

        # access 1 byte on the block and verify that wcfs sends us correct pins
        blkview = t._blk(blk)
        assert t.cached()[blk] == cached

        def _(ctx, ev):
            assert t.cached()[blk] == cached
            ev.append('read pre')

            # access data with released GIL so that the thread that reads data from
            # head/watch can receive pin message. Be careful to handle cancellation,
            # so that on error in another worker we don't get stuck and the
            # error can be propagated to wait and reported.
            #
            # we handle cancellation by spawning read in another thread and
            # waiting for either ctx cancel, or read thread to complete. This
            # way on ctx cancel (e.g. assertion failure in another worker), the
            # read thread can remain running even after _assertBlk returns, and
            # in particular till the point where the whole test is marked as
            # failed and shut down. But on test shutdown .fmmap is unmapped for
            # all opened tFiles, and so read will hit SIGSEGV. Prepare to catch
            # that SIGSEGV here.
            have_read = chan(1)
            def _():
                try:
                    b = read_exfault_nogil(blkview[0:1])
                except SegmentationFault:
                    b = 'FAULT'
                t._blkaccess(blk)
                have_read.send(b)
            go(_)
            _, _rx = select(
                ctx.done().recv,    # 0
                have_read.recv,     # 1
            )
            if _ == 0:
                raise ctx.err()
            b = _rx

            ev.append('read ' + b)
        ev = doCheckingPin(_, pinokByWLink, pinfunc)

        # XXX hack - wlinks are notified and emit events simultaneously - we
        # check only that events begin and end with read pre/post and that pins
        # are inside (i.e. read is stuck until pins are acknowledged).
        # Better do explicit check in tracetest style.
        assert ev[0]  == 'read pre', ev
        assert ev[-1] == 'read ' + dataok[0], ev
        ev = ev[1:-1]
        if not shouldPin:
            assert ev == []
        else:
            assert 'pin rx' in ev
            assert 'pin ack pre' in ev

        assert t.cached()[blk] > 0

        # verify full data of the block
        # TODO(?) assert individually for every block's page? (easier debugging?)
        assert blkview.tobytes() == dataok

        # we just accessed the block in full - it has to be in OS cache completely
        assert t.cached()[blk] == 1


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

        cachev = t.cached()
        for blk, dataok in enumerate(dataokv):
            if dataok == 'x':
                continue
            t.assertBlk(blk, dataok)
            cachev[blk] = 1

        # all accessed blocks must be in cache after we touched them all
        t.assertCache(cachev)


# tWatch represents watch for one file setup on a tWatchLink.
class tWatch:
    def __init__(w, foid):
        w.foid   = foid
        w.at     = z64  # not None - always concrete
        w.pinned = {}   # blk -> rev

# tWatchLink provides testing environment for /head/watch link opened on wcfs.
#
# .watch() setups/adjusts a watch for a file and verifies that wcfs correctly sends initial pins.
class tWatchLink(wcfs.WatchLink):

    def __init__(t, tdb):
        super(tWatchLink, t).__init__(tdb.wc)
        t.tdb = tdb
        tdb._wlinks.add(t)

        # this tWatchLink currently watches the following files at particular state.
        t._watching = {}    # {} foid -> tWatch

    def close(t):
        t.tdb._wlinks.remove(t)
        super(tWatchLink, t).close()

        # disable all established watches
        for w in t._watching.values():
            w.at     = z64
            w.pinned = {}
        t._watching = {}


# ---- infrastructure: watch setup/adjust ----

# watch sets up or adjusts a watch for file@at.
#
# During setup it verifies that wcfs sends correct initial/update pins.
#
# pinok: {} blk -> rev
# pinok can be omitted - in that case it is computed automatically.
#
# The automatic computation of pinok is verified against explicitly provided
# pinok when it is present.
@func(tWatchLink)
def watch(twlink, zf, at, pinok=None): # -> tWatch
    foid = zf._p_oid
    t = twlink.tdb
    w = twlink._watching.get(foid)
    if w is None:
        w = twlink._watching[foid] = tWatch(foid)
        at_prev = None
    else:
        at_prev = w.at  # we were previously watching zf @at_prev

    at_from = ''
    if at_prev is not None:
        at_from = '(%s ->) ' % at_prev
    print('\nC: setup watch f<%s> %s%s' % (h(foid), at_from, at))

    accessed    = t._blkaccessed(zf)
    lastRevOf   = lambda blk: t._blkRevAt(zf, blk, t.head)

    pin_prev = {}
    if at_prev is not None:
        assert at_prev <= at, 'TODO %s -> %s' % (at_prev, at)
        pin_prev = t._pinnedAt(zf, at_prev)
    assert w.pinned == pin_prev

    pin = t._pinnedAt(zf, at)

    if at_prev != at and at_prev is not None:
        print('# pin@old: %s\n# pin@new: %s' % (t.hpin(pin_prev), t.hpin(pin)))

    for blk in set(pin_prev.keys()).union(pin.keys()):
        # blk ∉ pin_prev,   blk ∉ pin       -> cannot happen
        assert (blk in pin_prev) or (blk in pin)

        # blk ∉ pin_prev,   blk ∈ pin       -> cannot happen, except on first start
        if blk not in pin_prev and blk in pin:
            if at_prev is not None:
                fail('#%d pinned %s; not pinned %s' % (blk, at_prev, at))

            # blk ∈ pin     -> blk is tracked; has rev > at
            # (see criteria in _pinnedAt)
            assert blk in accessed
            assert at  <  lastRevOf(blk)

        # blk ∈ pin_prev,   blk ∉ pin       -> unpin to head
        elif blk in pin_prev and blk not in pin:
            # blk ∈ pin_prev -> blk is tracked; has rev > at_prev
            assert blk in accessed
            assert at_prev < lastRevOf(blk)

            # blk ∉ pin      -> last blk revision is ≤ at
            assert lastRevOf(blk) <= at

            pin[blk] = None     # @head

        # blk ∈ pin_prev,   blk ∈ pin       -> if rev different: use pin
        elif blk in pin_prev and blk in pin:
            # blk ∈ pin_prev, pin   -> blk is tracked; has rev > at_prev, at
            assert blk in accessed
            assert at_prev < lastRevOf(blk)
            assert at      < lastRevOf(blk)

            assert pin_prev[blk] <= pin[blk]
            if pin_prev[blk] == pin[blk]:
                del pin[blk]    # would need to pin to what it is already pinned

    #print('-> %s' % t.hpin(pin))

    # {} blk -> at that have to be pinned.
    if pinok is not None:
        assert pin == pinok,    "computed vs explicit pinok"
    pinok = pin
    print('#  pinok: %s' % t.hpin(pinok))

    # send watch request and check that we receive pins for tracked (previously
    # accessed at least once) blocks changed > at.
    twlink._watch(zf, at, pinok, "ok")

    w.at = at

    # `watch ... -> at_i -> at_j`  must be the same as  `watch ø -> at_j`
    assert w.pinned == t._pinnedAt(zf, at)

    return w


# stop_watch instructs wlink to stop watching the file.
@func(tWatchLink)
def stop_watch(twlink, zf):
    foid = zf._p_oid
    assert foid in twlink._watching
    w = twlink._watching.pop(foid)

    twlink._watch(zf, b"-", {}, "ok")
    w.at = z64
    w.pinned = {}


# _watch sends watch request for zf@at, expects initial pins specified by pinok and final reply.
#
# at also can be b"-" which means "stop watching"
#
# pinok: {} blk -> at that have to be pinned.
# if replyok ends with '…' only reply prefix until the dots is checked.
@func(tWatchLink)
def _watch(twlink, zf, at, pinok, replyok):
    if at == b"-":
        xat = at
    else:
        xat = b"@%s" % h(at)

    def _(ctx, ev):
        reply = twlink.sendReq(ctx, b"watch %s %s" % (h(zf._p_oid), xat))
        if replyok.endswith('…'):
            rok = replyok[:-len('…')]
            assert reply[:len(rok)] == rok
        else:
            assert reply == replyok

    doCheckingPin(_, {twlink: (zf, pinok)})


# doCheckingPin calls f and verifies that wcfs sends expected pins during the
# time f executes.
#
# f(ctx, eventv)
# pinokByWLink: {} tWatchLink -> (zf, {} blk -> at).
# pinfunc(wlink, foid, blk, at) | None.
#
# pinfunc is called after pin request is received from wcfs, but before pin ack
# is replied back. Pinfunc must not block.
def doCheckingPin(f, pinokByWLink, pinfunc=None): # -> []event(str)
    # call f and check that we receive pins as specified.
    # Use timeout to detect wcfs replying less pins than expected.
    #
    # XXX detect not sent pins via ack'ing previous pins as they come in (not
    # waiting for all of them) and then seeing that we did not received expected
    # pin when f completes?
    ctx, cancel = with_timeout()
    wg = sync.WorkGroup(ctx)
    ev = []

    for wlink, (zf, pinok) in pinokByWLink.items():
        def _(ctx, wlink, zf, pinok):
            w = wlink._watching.get(zf._p_oid)
            if len(pinok) > 0:
                assert w is not None

            pinv = wlink._expectPin(ctx, zf, pinok)
            if len(pinv) > 0:
                ev.append('pin rx')

            # increase probability to receive erroneous extra pins
            tdelay()

            if len(pinv) > 0:
                if pinfunc is not None:
                    for p in pinv:
                        pinfunc(wlink, p.foid, p.blk, p.at)
                ev.append('pin ack pre')
                for p in pinv:
                    assert w.foid == p.foid
                    if p.at is None:    # unpin to @head
                        assert p.blk in w.pinned    # must have been pinned before
                        del w.pinned[p.blk]
                    else:
                        w.pinned[p.blk] = p.at

                    #p.reply(b"ack")
                    wlink.replyReq(ctx, p, b"ack")

            # check that we don't get extra pins before f completes
            try:
                req = wlink.recvReq(ctx)
            except Exception as e:
                if errors.Is(e, context.canceled):
                    return # cancel is expected after f completes
                raise

            fail("extra pin message received: %r" % req.msg)
        wg.go(_, wlink, zf, pinok)

    def _(ctx):
        f(ctx, ev)
        # cancel _expectPin waiting upon completing f
        # -> error that missed pins were not received.
        cancel()
    wg.go(_)

    wg.wait()
    return ev


# _expectPin asserts that wcfs sends expected pin messages.
#
# expect is {} blk -> at
# returns [] of received pin requests.
@func(tWatchLink)
def _expectPin(twlink, ctx, zf, expect): # -> []SrvReq
    expected = set()    # of expected pin messages
    for blk, at in expect.items():
        hat = h(at) if at is not None else 'head'
        msg = b"pin %s #%d @%s" % (h(zf._p_oid), blk, hat)
        assert msg not in expected
        expected.add(msg)

    reqv = []   # of received requests
    while len(expected) > 0:
        try:
            req = twlink.recvReq(ctx)
        except Exception as e:
            raise RuntimeError("%s\nnot all pin messages received - pending:\n%s" % (e, expected))
        assert req is not None  # channel not closed
        assert req.msg in expected
        expected.remove(req.msg)
        reqv.append(req)

    return reqv


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

# _blkRevAt returns expected zf[blk] revision as of @at database state.
@func(tDB)
def _blkRevAt(t, zf, blk, at): # -> rev
    _, rev = t._blkDataAt(zf, blk, at)
    return rev


# _pinnedAt returns which blocks need to be pinned for zf@at compared to zf@head
# according to wcfs isolation protocol.
#
# Criteria for when blk must be pinned as of @at view:
#
#   blk ∈ pinned(at)   <=>   1) ∃ r = rev(blk): at < r  ; blk was changed after at
#                            2) blk ∈ tracked           ; blk was accessed at least once
#                                                       ; (and so is tracked by wcfs)
@func(tDB)
def _pinnedAt(t, zf, at):  # -> pin = {} blk -> rev
    # all changes to zf
    vdf = [_.byfile[zf] for _ in t.dFtail if zf in _.byfile]

    # {} blk -> at for changes ∈ (at, head]
    pin = {}
    for df in [_ for _ in vdf if _.rev > at]:
        for blk in df.ddata:
            if blk in pin:
                continue
            if blk in t._blkaccessed(zf):
                pin[blk] = t._blkRevAt(zf, blk, at)

    return pin

# iter_revv iterates through all possible at_i -> at_j -> at_k ... sequences.
# at_i < at_j       NOTE all sequences go till head.
@func(tDB)
def iter_revv(t, start=z64, level=0):
    dFtail = [_ for _ in t.dFtail if _.rev > start]
    #print(' '*level, 'iter_revv', start, [_.rev for _ in dFtail])
    if len(dFtail) == 0:
        yield []
        return

    for dF in dFtail:
        #print(' '*level, 'QQQ', dF.rev)
        for tail in t.iter_revv(start=dF.rev, level=level+1):
            #print(' '*level, 'zzz', tail)
            yield ([dF.rev] + tail)


# -------------------------------------
# ---- actual tests to access data ----

# exercise wcfs functionality without wcfs isolation protocol.
# plain data access + wcfs handling of ZODB invalidations.
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
    f.assertCache([])
    f.assertData ([], mtime=t.at0)

    # >>> (@at1) commit data -> we can see it on wcfs
    at1 = t.commit(zf, {2:'c1'})

    f.assertCache([0,0,0])  # initially not cached
    f.assertData (['','','c1'], mtime=t.head)

    # >>> (@at2) commit again -> we can see both latest and snapshotted states
    # NOTE blocks e(4) and f(5) will be accessed only in the end
    at2 = t.commit(zf, {2:'c2', 3:'d2', 5:'f2'})

    # f @head
    f.assertCache([1,1,0,0,0,0])
    f.assertData (['','', 'c2', 'd2', 'x','x'], mtime=t.head)
    f.assertCache([1,1,1,1,0,0])

    # f @at1
    f1 = t.open(zf, at=at1)
    f1.assertCache([0,0,1])
    f1.assertData (['','','c1']) # TODO + mtime=at1


    # >>> (@at3) commit again without changing zf size
    f2 = t.open(zf, at=at2)
    at3 = t.commit(zf, {0:'a3', 2:'c3', 5:'f3'})

    f.assertCache([0,1,0,1,0,0])

    # f @head is opened again -> cache must not be lost
    f_ = t.open(zf)
    f_.assertCache([0,1,0,1,0,0])
    f_.close()
    f.assertCache([0,1,0,1,0,0])

    # f @head
    f.assertCache([0,1,0,1,0,0])
    f.assertData (['a3','','c3','d2','x','x'], mtime=t.head)

    # f @at2
    # NOTE f(2) is accessed but via @at/ not head/  ; f(2) in head/zf remains unaccessed
    f2.assertCache([0,0,1,0,0,0])
    f2.assertData (['','','c2','d2','','f2']) # TODO mtime=at2

    # f @at1
    f1.assertCache([1,1,1])
    f1.assertData (['','','c1']) # TODO mtime=at1


    # >>> f close / open again -> cache must not be lost
    # XXX a bit flaky since OS can evict whole f cache under pressure
    f.assertCache([1,1,1,1,0,0])
    f.close()
    f = t.open(zf)
    if f.cached() != [1,1,1,1,0,0]:
        assert sum(f.cached()) > 4*1/2  # > 50%

    # verify all blocks
    f.assertData(['a3','','c3','d2','','f3'])
    f.assertCache([1,1,1,1,1,1])


# verify how wcfs processes ZODB invalidations when hole becomes a block with data.
@func
def test_wcfs_basic_hole2zblk():
    t = tDB(); zf = t.zfile
    defer(t.close)

    f = t.open(zf)
    t.commit(zf, {2:'c1'})  # b & a are holes
    f.assertCache([0,0,0])
    f.assertData(['','','c1'])

    t.commit(zf, {1:'b2'})  # hole -> zblk
    f.assertCache([1,0,1])
    f.assertData(['','b2','c1'])

# TODO ZBlk copied from blk1 -> blk2 ; for the same file and for file1 -> file2
# TODO ZBlk moved  from blk1 -> blk2 ; for the same file and for file1 -> file2

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



# ---- verify wcfs functionality that depends on isolation protocol ----

# verify that watch setup is robust to client errors/misbehaviour.
@func
def test_wcfs_watch_robust():
    t = tDB(); zf = t.zfile
    defer(t.close)

    # sysread(/head/watch) can be interrupted
    p = subprocess.Popen(["%s/testprog/wcfs_readcancel.py" %
                                os.path.dirname(__file__), t.wc.mountpoint])
    procwait(timeout(), p)


    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2'})

    # file not yet opened on wcfs
    wl = t.openwatch()
    assert wl.sendReq(timeout(), b"watch %s @%s" % (h(zf._p_oid), h(at1))) == \
        "error setup watch f<%s> @%s: " % (h(zf._p_oid), h(at1)) + \
        "file not yet known to wcfs or is not a ZBigFile"
    wl.close()

    # closeTX/bye cancels blocked pin handlers
    f = t.open(zf)
    f.assertBlk(2, 'c2')
    f.assertCache([0,0,1])

    wl = t.openwatch()
    wg = sync.WorkGroup(timeout())
    def _(ctx):
        # TODO clarify what wcfs should do if pin handler closes wlink TX:
        #   - reply error + close, or
        #   - just close
        # t = when reviewing WatchLink.serve in wcfs.go
        #assert wl.sendReq(ctx, b"watch %s @%s" % (h(zf._p_oid), h(at1))) == \
        #        "error setup watch f<%s> @%s: " % (h(zf._p_oid), h(at1)) + \
        #        "pin #%d @%s: context canceled" % (2, h(at1))
        #with raises(error, match="unexpected EOF"):
        with raises(error, match="recvReply: link is down"):
            wl.sendReq(ctx, b"watch %s @%s" % (h(zf._p_oid), h(at1)))

    wg.go(_)
    def _(ctx):
        req = wl.recvReq(ctx)
        assert req is not None
        assert req.msg == b"pin %s #%d @%s" % (h(zf._p_oid), 2, h(at1))
        # don't reply to req - close instead
        wl.closeWrite()
    wg.go(_)
    wg.wait()
    wl.close()
    # NOTE if wcfs.go does not fully cleanup this canceled watch and leaves it
    # in half-working state, it will break on further commit, as pin to the
    # watch won't be handled.
    at3 = t.commit(zf, {2:'c3'})

    # invalid requests -> wcfs replies error
    wl = t.openwatch()
    assert wl.sendReq(timeout(), b'bla bla') ==  \
            b'error bad watch: not a watch request: "bla bla"'

    # invalid request not following frame structure -> fatal + wcfs must close watch link
    assert wl.fatalv == []
    _twlinkwrite(wl, b'zzz hello\n')
    _, _rx = select(
        timeout().done().recv,
        wl.rx_eof.recv,
    )
    if _ == 0:
        raise RuntimeError("%s: did not rx EOF after bad frame " % wl)
    assert wl.fatalv == [b'error: invalid frame: "zzz hello\\n" (invalid stream)']
    wl.close()

    # watch with @at < δtail.tail -> rejected
    wl = t.openwatch()
    atpast = p64(u64(t.tail)-1)
    wl._watch(zf, atpast, {}, "error setup watch f<%s> @%s: too far away back from"
            " head/at (@%s); …" % (h(zf._p_oid), h(atpast), h(t.head)))
    wl.close()

# verify that `watch file @at` -> error, for @at when file did not existed.
@func
def test_wcfs_watch_before_create():
    t = tDB(); zf = t.zfile
    defer(t.close)

    at1 = t.commit(zf, {2:'c1'})
    zf2 = t.root['zfile2'] = ZBigFile(blksize)  # zf2 created @at2
    at2 = t.commit()
    at3 = t.commit(zf2, {1:'β3'})

    # force wcfs to access/know zf2
    f2 = t.open(zf2)
    f2.assertData(['','β3'])

    wl = t.openwatch()
    assert wl.sendReq(timeout(), b"watch %s @%s" % (h(zf2._p_oid), h(at1))) == \
        "error setup watch f<%s> @%s: " % (h(zf2._p_oid), h(at1)) + \
        "file epoch detected @%s in between (at,head=@%s]" % (h(at2), h(t.head))
    wl.close()


# verify that watch @at_i -> @at_j ↓ is rejected
# TODO(?) we might want to allow going back in history later.
@func
def test_wcfs_watch_going_back():
    t = tDB(); zf = t.zfile
    defer(t.close)

    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2'})
    f = t.open(zf)
    f.assertData(['','','c2'])

    wl = t.openwatch()
    wl.watch(zf, at2, {})
    wl.sendReq(timeout(), b"watch %s @%s" % (h(zf._p_oid), h(at1))) == \
        "error setup watch f<%s> @%s: " % (h(zf._p_oid), h(at1)) + \
        "going back in history is forbidden"
    wl.close()


# verify that wcfs kills slow/faulty client who does not reply to pin in time.
@xfail  # protection against faulty/slow clients
@func
def test_wcfs_pintimeout_kill():
    # adjusted wcfs timeout to kill client who is stuck not providing pin reply
    tkill = 3*time.second
    t = tDB(); zf = t.zfile     # XXX wcfs args += tkill=<small>
    defer(t.close)

    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2'})
    f = t.open(zf)
    f.assertData(['','','c2'])

    # XXX move into subprocess not to kill whole testing
    ctx, _ = context.with_timeout(context.background(), 2*tkill)

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
            ctx.done().recv,        # 0
            time.after(tkill).recv, # 1
        )
        if _ == 0:
            raise ctx.err()
        fail("wcfs did not killed stuck client")
    wg.go(_)
    wg.wait()


# watch with @at > head - must wait for head to become >= at.
# TODO(?) too far ahead - reject?
@func
def test_wcfs_watch_setup_ahead():
    t = tDB(); zf = t.zfile
    defer(t.close)

    f = t.open(zf)
    at1 = t.commit(zf, {2:'c1'})
    f.assertData(['','x','c1'])     # NOTE #1 not accessed for watch @at1 to receive no pins

    wg = sync.WorkGroup(timeout())
    dt = 100*time.millisecond
    committing = chan() # becomes ready when T2 starts to commit

    # T1: watch @(at1+1·dt)
    @func
    def _(ctx):
        wl = t.openwatch()
        defer(wl.close)

        wat = tidfromtime(tidtime(at1) + 1*dt)  # > at1, but < at2
        rx = wl.sendReq(ctx, b"watch %s @%s" % (h(zf._p_oid), h(wat)))
        assert ready(committing)
        assert rx == b"ok"
    wg.go(_)

    # T2: sleep(10·dt); commit
    @func
    def _(ctx):
        # reopen connection to database as we are committing from another thread
        conn = t.root._p_jar.db().open()
        defer(conn.close)
        root = conn.root()
        zf = root['zfile']

        time.sleep(10*dt)
        committing.close()
        at2 = t.commit(zf, {1:'b2'})
        assert tidtime(at2) - tidtime(at1) >= 10*dt
    wg.go(_)

    wg.wait()


# verify that watch setup/update sends correct pins.
@func
def test_wcfs_watch_setup():
    t = tDB(); zf = t.zfile; at0=t.at0
    defer(t.close)

    f = t.open(zf)
    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2', 3:'d2', 4:'e2', 5:'f2'})
    at3 = t.commit(zf, {0:'a3', 2:'c3', 5:'f3'})

    f.assertData(['a3','','c3','d2','x','f3'])  # access everything except e as of @at3
    f.assertCache([1,1,1,1,0,1])

    # change again, but don't access e and f
    at4 = t.commit(zf, {2:'c4', 4:'e4', 5:'f4'})
    at5 = t.commit(zf, {3:'d5', 5:'f5'})
    f.assertData(['a3','','c4','d5','x','x'])
    f.assertCache([1,1,1,1,0,0])

    # some watch setup/update requests with explicit pinok (also partly
    # verifies how tWatchLink.watch computes automatic pinok)

    # new watch setup ø -> at
    def assertNewWatch(at, pinok):
        wl = t.openwatch()
        wl.watch(zf, at, pinok)
        wl.close()
    assertNewWatch(at1, {0:at0,  2:at1,  3:at0,  5:at0})
    assertNewWatch(at2, {0:at0,  2:at2,  3:at2,  5:at2})
    assertNewWatch(at3, {        2:at3,  3:at2,  5:at3})    # f(5) is pinned, even though it was not
    assertNewWatch(at4, {                3:at2,  5:at4})    # accessed after at3
    assertNewWatch(at5, {                             })

    # new watch + update at_i -> at_j
    wl = t.openwatch()
    wl.watch(zf, at0, {0:at0,  2:at0,  3:at0,  5:at0})  #     -> at0 (new watch)    XXX at0 -> ø?
    wl.watch(zf, at1, {        2:at1,               })  # at0 -> at1
    wl.watch(zf, at2, {        2:at2,  3:at2,  5:at2})  # at1 -> at2
    wl.watch(zf, at3, {0:None, 2:at3,          5:at3})  # at2 -> at3
    wl.watch(zf, at4, {        2:None,         5:at4})  # at3 -> at4 f(5) pinned even it was not accessed >=4
    wl.watch(zf, at5, {                3:None, 5:None}) # at4 -> at5 (current head)
    wl.close()

    # all valid watch setup/update requests going at_i -> at_j -> ... with automatic pinok
    for zf in t.zfiles():
        for revv in t.iter_revv():
            print('\n--------')
            print(' -> '.join(['%s' % _ for _ in revv]))
            wl = t.openwatch()
            wl.watch(zf, revv[0])
            wl.watch(zf, revv[0])    # verify at_i -> at_i
            for at in revv[1:]:
                wl.watch(zf, at)
            wl.close()


# verify that already setup watch(es) receive correct pins on block access.
@func
def test_wcfs_watch_vs_access():
    t = tDB(); zf = t.zfile; at0=t.at0
    defer(t.close)

    f = t.open(zf)
    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2', 3:'d2', 5:'f2'})
    at3 = t.commit(zf, {0:'a3', 2:'c3', 5:'f3'})

    f.assertData(['a3','','c3','d2','x','x'])
    f.assertCache([1,1,1,1,0,0])

    # watched + commit -> read -> receive pin messages.
    # read vs pin ordering is checked by assertBlk.
    #
    # f(5) is kept not accessed to check later how wcfs.go handles δFtail
    # rebuild after it sees not yet accessed ZBlk that has change history.
    wl3  = t.openwatch();  w3 = wl3.watch(zf, at3);  assert at3 == t.head
    assert w3.at     == at3
    assert w3.pinned == {}

    wl3_ = t.openwatch();  w3_ = wl3_.watch(zf, at3)
    assert w3_.at     == at3
    assert w3_.pinned == {}

    wl2  = t.openwatch();  w2 = wl2.watch(zf, at2)
    assert w2.at     == at2
    assert w2.pinned == {0:at0, 2:at2}

    # w_assertPin asserts on state of .pinned for {w3,w3_,w2}
    def w_assertPin(pinw3, pinw3_, pinw2):
        assert w3.pinned   == pinw3
        assert w3_.pinned  == pinw3_
        assert w2.pinned   == pinw2

    f.assertCache([1,1,1,1,0,0])
    at4 = t.commit(zf, {1:'b4', 2:'c4', 5:'f4', 6:'g4'})
    f.assertCache([1,0,0,1,0,0,0])

    f.assertBlk(0, 'a3', {wl3: {},                     wl3_: {},                     wl2: {}})
    w_assertPin(               {},                           {},                          {0:at0, 2:at2})

    f.assertBlk(1, 'b4', {wl3: {1:at0},                wl3_: {1:at0},                wl2: {1:at0}})
    w_assertPin(               {1:at0},                      {1:at0},                     {0:at0, 1:at0, 2:at2})

    f.assertBlk(2, 'c4', {wl3: {2:at3},                wl3_: {2:at3},                wl2: {}})
    w_assertPin(               {1:at0, 2:at3},               {1:at0, 2:at3},              {0:at0, 1:at0, 2:at2})

    f.assertBlk(3, 'd2', {wl3: {},                     wl3_: {},                     wl2: {}})
    w_assertPin(               {1:at0, 2:at3},               {1:at0, 2:at3},              {0:at0, 1:at0, 2:at2})

    # blk4 is hole @head - the same as at earlier db view - not pinned
    f.assertBlk(4, '',   {wl3: {},                     wl3_: {},                     wl2: {}})
    w_assertPin(               {1:at0, 2:at3},               {1:at0, 2:at3},              {0:at0, 1:at0, 2:at2})

    # f(5) is kept unaccessed (see ^^^)
    assert f.cached()[5] == 0

    f.assertBlk(6, 'g4', {wl3: {6:at0},                wl3_: {6:at0},                wl2: {6:at0}}) # XXX at0->ø?
    w_assertPin(               {1:at0, 2:at3, 6:at0},        {1:at0, 2:at3, 6:at0},       {0:at0, 1:at0, 2:at2, 6:at0})

    # commit again:
    # - c(2) is already pinned  -> wl3 not notified
    # - watch stopped (wl3_)    -> watch no longer notified
    # - wlink closed (wl2)      -> watch no longer notified
    # - f(5) is still kept unaccessed (see ^^^)
    f.assertCache([1,1,1,1,1,0,1])
    at5 = t.commit(zf, {2:'c5', 3:'d5', 5:'f5'})
    f.assertCache([1,1,0,0,1,0,1])

    wl3_.stop_watch(zf) # w3_ should not be notified
    wl2.close()         # wl2:* should not be notified
    def w_assertPin(pinw3):
        assert w3.pinned   == pinw3
        assert w3_.pinned  == {}; assert w3_.at == z64  # wl3_ unsubscribed from zf
        assert w2.pinned   == {}; assert w2.at  == z64  # wl2 closed

    f.assertBlk(0, 'a3', {wl3: {},                          wl3_: {}})  # no change
    w_assertPin(               {1:at0, 2:at3, 6:at0})

    f.assertBlk(1, 'b4', {wl3: {},                          wl3_: {}})
    w_assertPin(               {1:at0, 2:at3, 6:at0})

    f.assertBlk(2, 'c5', {wl3: {},                          wl3_: {}})  # c(2) already pinned on wl3
    w_assertPin(               {1:at0, 2:at3, 6:at0})

    f.assertBlk(3, 'd5', {wl3: {3:at2},                     wl3_: {}})  # d(3) was not pinned on wl3; wl3_ not notified
    w_assertPin(               {1:at0, 2:at3, 3:at2, 6:at0})

    f.assertBlk(4, '',   {wl3: {},                          wl3_: {}})
    w_assertPin(               {1:at0, 2:at3, 3:at2, 6:at0})

    # f(5) is kept still unaccessed (see ^^^)
    assert f.cached()[5] == 0

    f.assertBlk(6, 'g4', {wl3: {},                          wl3_: {}})
    w_assertPin(               {1:at0, 2:at3, 3:at2, 6:at0})


    # advance watch - receives new pins/unpins to @head.
    # this is also tested ^^^ in `at_i -> at_j -> ...` watch setup/adjust.
    # NOTE f(5) is not affected because it was not pinned previously.
    wl3.watch(zf, at4, {1:None, 2:at4, 6:None}) # at3 -> at4
    w_assertPin(       {2:at4, 3:at2})

    # access f(5) -> wl3 should be correctly pinned
    assert f.cached() == [1,1,1,1,1,0,1]  # f(5) was not yet accessed
    f.assertBlk(5, 'f5', {wl3: {5:at4},                 wl3_: {}})
    w_assertPin(               {2:at4, 3:at2, 5:at4})

    # advance watch again
    wl3.watch(zf, at5, {2:None, 3:None, 5:None})    # at4 -> at5
    w_assertPin(       {})

    wl3.close()


# verify that on pin message, while under pagefault, we can mmap @at/f[blk]
# into where head/f[blk] was mmaped; the result of original pagefaulting read
# must be from newly inserted mapping.
#
# TODO same with two mappings to the same file, but only one changing blk mmap
#      -> one read gets changed data, one read gets data from @head.
@func
def test_wcfs_remmap_on_pin():
    t = tDB(); zf = t.zfile
    defer(t.close)

    at1 = t.commit(zf, {2:'hello'})
    at2 = t.commit(zf, {2:'world'})

    f  = t.open(zf)
    f1 = t.open(zf, at=at1)
    wl = t.openwatch()
    wl.watch(zf, at1, {})

    f.assertCache([0,0,0])
    def _(wlink, foid, blk, at):
        assert wlink is wl
        assert foid  == zf._p_oid
        assert blk   == 2
        assert at    == at1
        mm.map_into_ro(f._blk(blk), f1.f.fileno(), blk*f.blksize)

    f._assertBlk(2, 'hello', {wl: {2:at1}}, pinfunc=_)     # NOTE not world


# verify that pin message is not sent for the same blk@at twice.
@func
def test_wcfs_no_pin_twice():
    t = tDB(); zf = t.zfile
    defer(t.close)

    f = t.open(zf)
    at1 = t.commit(zf, {2:'c1'})
    at2 = t.commit(zf, {2:'c2'})
    wl = t.openwatch()
    w = wl.watch(zf, at1, {})
    f.assertCache([0,0,0])

    f.assertBlk(2, 'c2', {wl: {2:at1}})
    f.assertCache([0,0,1])
    assert w.pinned == {2:at1}

    # drop file[blk] from cache, access again -> no pin message sent the second time
    #
    # ( we need both madvise(DONTNEED) and fadvise(DONTNEED) - given only one of
    #   those the kernel won't release the page from pagecache; madvise does
    #   not work without munlock. )
    mm.unlock(f._blk(2))
    mm.advise(f._blk(2), mm.MADV_DONTNEED)
    fadvise_dontneed(f.f.fileno(), 2*blksize, 1*blksize)
    f.assertCache([0,0,0])

    f.assertBlk(2, 'c2', {wl: {}})
    f.assertCache([0,0,1])


# verify watching for 2 files over single watch link.
#
# NOTE this test also verifies how wcfs handles ZBigFile created after wcfs startup.
@func
def test_wcfs_watch_2files():
    t = tDB(); zf1 = t.zfile
    defer(t.close)

    t.root['zfile2'] = zf2 = ZBigFile(blksize)
    t.commit()

    t.change(zf1, {0:'a2', 2:'c2'})
    t.change(zf2, {1:'β2', 3:'δ2'})
    at2 = t.commit()

    t.change(zf1, {0:'a3', 2:'c3'})
    t.change(zf2, {1:'β3', 3:'δ3'})
    at3 = t.commit()

    f1 = t.open(zf1)
    f2 = t.open(zf2)

    f1.assertData(['a3', '',   'x'    ])
    f2.assertData(['',   'β3', '', 'x'])

    wl = t.openwatch()
    w1 = wl.watch(zf1, at2, {0:at2})
    w2 = wl.watch(zf2, at2, {1:at2})

    def w_assertPin(pinw1, pinw2):
        assert w1.pinned == pinw1
        assert w2.pinned == pinw2

    w_assertPin(               {0:at2},             {1:at2})
    f1.assertBlk(2, 'c3', {wl: {2:at2}})
    w_assertPin(               {0:at2, 2:at2},      {1:at2})
    f2.assertBlk(3, 'δ3', {wl:                      {3:at2}})
    w_assertPin(               {0:at2, 2:at2},      {1:at2, 3:at2})

    wl.watch(zf1, at3, {0:None, 2:None})
    w_assertPin(               {},                  {1:at2, 3:at2})
    wl.watch(zf2, at3, {1:None, 3:None})
    w_assertPin(               {},                  {})



# TODO new watch request while previous watch request is in progress (over the same /head/watch handle)
# TODO @revX/ is automatically removed after some time

# ----------------------------------------

# verify that wcfs does not panic with "no current transaction" / "at out of
# bounds" on read/invalidate/watch codepaths.
@func
def test_wcfs_crash_old_data():
    # start wcfs with ΔFtail/ΔBtail not covering initial data.
    t = tDB(old_data=[{0:'a'}]); zf = t.zfile; at1 = t.head
    defer(t.close)

    f = t.open(zf)

    # ΔFtail coverage is currently (at1,at1]
    wl = t.openwatch()
    wl.watch(zf, at1, {})

    # wcfs was crashing on readPinWatcher -> ΔFtail.BlkRevAt with
    #   "at out of bounds: at: @at1,  (tail,head] = (@at1,@at1]
    # because BlkRevAt(at=tail) query was disallowed.
    f.assertBlk(0, 'a')          # [0] becomes tracked

    at2 = t.commit(zf, {1:'b1'}) # arbitrary commit to non-0 blk

    # wcfs was crashing on processing invalidation to blk 0 because
    # 1. ΔBtail.GetAt([0], head) returns valueExact=false, and so
    # 2. ΔFtail.BlkRevAt activates "access ZODB" codepath,
    # 3. but handleδZ was calling ΔFtail.BlkRevAt without putting zhead's transaction into ctx.
    # -> panic.
    at3 = t.commit(zf, {0:'a2'})

    # just in case
    f.assertBlk(0, 'a2')

    # wcfs was crashing in setting up watch because of "1" and "2" from above, and
    # 3. setupWatch was calling ΔFtail.BlkRevAt without putting zhead's transaction into ctx.
    wl2 = t.openwatch()
    wl2.watch(zf, at2, {0:at1})


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

# hpin returns human-readable representation for {}blk->rev.
@func(tDB)
def hpin(t, pin):
    pinv = []
    for blk in sorted(pin.keys()):
        if pin[blk] is None:
            s = '@head'
        else:
            s = '%s' % pin[blk]
        pinv.append('%d: %s' % (blk, s))
    return '{%s}' % ', '.join(pinv)


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
