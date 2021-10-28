# Wendelin.core.bigfile | Tests for ZODB BigFile backend
# Copyright (C) 2014-2021  Nexedi SA and Contributors.
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
from wendelin.bigfile.file_zodb import ZBigFile, ZBlk_fmt_registry
from wendelin.bigfile import file_zodb, ram_reclaim
from wendelin.bigfile.tests.test_thread import NotifyChannel
from wendelin.lib.zodb import LivePersistent, dbclose
from wendelin.lib.tests.test_zodb import cacheInfo, kkey
from wendelin.lib.testing import getTestDB
from persistent import UPTODATE
import transaction
from transaction import TransactionManager
from ZODB.POSException import ConflictError
from numpy import ndarray, array_equal, uint32, zeros, arange
from golang import defer, func, chan
from golang import context, sync
from six.moves import _thread
from six import b
import struct
import weakref
import gc

from pytest import raises
import pytest; xfail = pytest.mark.xfail
from six.moves import range as xrange


testdb = None
blksize  = 2*1024*1024   # XXX hardcoded
blen     = 32            # 32*2 = 64MB      # TODO set it higher by default ?


def dbopen():
    return testdb.dbopen()

def setup_module():
    global testdb
    testdb = getTestDB()
    testdb.setup()

def teardown_module():
    testdb.teardown()


# reclaim all pages
def ram_reclaim_all():
    reclaimed = 0
    while 1:
        n = ram_reclaim()   # TODO + ram
        if n == 0:
            break
        reclaimed += n
    return reclaimed


# i'th memory block as u32 ndarray
blksize32 = blksize // 4
def Blk(vma, i):
    return ndarray(blksize32, offset=i*blksize, buffer=vma, dtype=uint32)

def test_bigfile_filezodb():
    ram_reclaim_all()   # reclaim pages allocated by previous tests

    root = dbopen()
    root['zfile'] = f = ZBigFile(blksize)
    transaction.commit()

    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    # verify that empty file reads as all zeros
    data0 = zeros(blksize32, dtype=uint32)
    dataX = lambda i: arange(i*blksize32, (i+1)*blksize32, dtype=uint32)
    for i in xrange(blen):
        assert array_equal(data0, Blk(vma, i))

    # dirty data
    for i in xrange(blen):
        Blk(vma, i)[:] = dataX(i)

    # verify that the changes are lost after abort
    transaction.abort()
    for i in xrange(blen):
        assert array_equal(data0, Blk(vma, i))


    # dirty & abort once again
    # (verifies that ZBigFile data manager re-registers with transaction)
    for i in xrange(blen):
        Blk(vma, i)[:] = dataX(i)

    transaction.abort()
    for i in xrange(blen):
        assert array_equal(data0, Blk(vma, i))


    # dirty data & commit
    for i in xrange(blen):
        Blk(vma, i)[:] = dataX(i)

    transaction.commit()


    # close DB and reopen everything
    # vma.unmap()
    del vma
    #fh.close()
    del fh

    dbclose(root)
    del root

    root = dbopen()
    f = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    # verify data as re-loaded
    for i in xrange(blen):
        assert array_equal(Blk(vma, i), dataX(i))


    # evict all loaded pages and test loading them again
    # (verifies ZBlk.loadblkdata() & loadblk logic when loading data the second time)
    reclaimed = ram_reclaim_all()
    if fh.uses_mmap_overlay():
        # in mmap-overlay mode no on-client RAM is allocated for read data
        assert reclaimed == 0
    else:
        assert reclaimed >= blen    # XXX assumes pagesize=blksize

    for i in xrange(blen):
        assert array_equal(Blk(vma, i), dataX(i))

    # dirty once again & commit
    # (verified ZBlk.__setstate__() & storeblk logic when storing data the second time)
    for i in xrange(blen):
        Blk(vma, i)[0] = i+1

    transaction.commit()


    # close DB and reopen everything
    del vma
    del fh
    dbclose(root)
    del root

    root = dbopen()
    f = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    # verify data as re-loaded
    for i in xrange(blen):
        assert Blk(vma, i)[0] == i+1
        assert array_equal(Blk(vma, i)[1:], dataX(i)[1:])


    # ZBigFile should survive Persistent cache clearing and not go to ghost
    # state (else logic to propagate changes from pages to objects would subtly
    # brake after Persistent cache gc)
    db = root._p_jar.db()
    ci = cacheInfo(db)
    assert ci[kkey(ZBigFile)] == 1
    assert f._p_state == UPTODATE
    db.cacheMinimize()
    ci = cacheInfo(db)
    assert ci[kkey(ZBigFile)] == 1
    assert f._p_state == UPTODATE   # it would be GHOST without LivePersistent protection

    # verify that data changes propagation continue to work
    assert Blk(vma, 0)[0] == 1
    assert array_equal(Blk(vma, 0)[1:], dataX(0)[1:])
    Blk(vma, 0)[0] = 99
    transaction.commit()

    del vma
    del fh
    dbclose(root)
    del db, root

    root = dbopen()
    f = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    # verify data as re-loaded
    assert Blk(vma, 0)[0] == 99
    assert array_equal(Blk(vma, 0)[1:], dataX(0)[1:])
    for i in xrange(1, blen):
        assert Blk(vma, i)[0] == i+1
        assert array_equal(Blk(vma, i)[1:], dataX(i)[1:])


    dbclose(root)



# connection can migrate between threads handling requests.
# verify _ZBigFileH properly adjusts.
# ( NOTE this test is almost dupped at test_zbigarray_vs_conn_migration() )
@func
def test_bigfile_filezodb_vs_conn_migration():
    root01 = dbopen()
    conn01 = root01._p_jar
    db     = conn01.db()
    conn01.close()
    del root01
    defer(db.close)

    c12_1 = NotifyChannel()   # T11 -> T21
    c21_1 = NotifyChannel()   # T21 -> T11

    # open, modify, commit, close, open, commit
    T11ident = [None] # [0] = gettid(T11)
    def T11(ctx):
        T11ident[0] = _thread.get_ident()
        tell, wait = c12_1.tell, c21_1.wait

        conn11_1 = db.open()
        assert conn11_1 is conn01

        # setup zfile with ZBigArray-like satellite,
        root11_1 = conn11_1.root()
        root11_1['zfile2'] = f11 = ZBigFile(blksize)
        transaction.commit()

        root11_1['zarray2'] = a11 = LivePersistent()
        a11._v_fileh = fh11 = f11.fileh_open()
        transaction.commit()

        # set zfile initial data
        vma11 = fh11.mmap(0, 1)

        Blk(vma11, 0)[0] = 11
        transaction.commit()

        # close conn, wait till T21 reopens it
        del vma11, fh11, a11, f11, root11_1
        conn11_1.close()
        tell(ctx, 'T1-conn11_1-closed')
        wait(ctx, 'T2-conn21-opened')

        # open another connection (e.g. for handling next request) which does
        # not touch  zfile at all, and arrange timings so that T2 modifies
        # zfile, but do not yet commit, and then commit here.
        conn11_2 = db.open()
        assert conn11_2 is not conn11_1
        root11_2 = conn11_2.root()

        wait(ctx, 'T2-zfile2-modified')

        # XXX do we want to also modify some other objects?
        # (but this have side effect for joining conn11_2 to txn)
        transaction.commit()    # should be nothing
        tell(ctx, 'T1-txn12-committed')

        wait(ctx, 'T2-conn21-closed')
        del root11_2
        conn11_2.close()

        # hold on this thread until main driver tells us
        wait(ctx, 'T11-exit-command')


    # open, modify, abort
    T21done = chan()
    @func
    def T21(ctx):
        defer(T21done.close)
        tell, wait = c21_1.tell, c12_1.wait

        # - wait until T1 finish setting up initial data for zfile and closes connection.
        # - open that connection before T1 is asleep - because ZODB organizes
        #   connection pool as stack (with correction for #active objects),
        #   we should get exactly the same connection T1 had.
        wait(ctx, 'T1-conn11_1-closed')
        conn21 = db.open()
        assert conn21 is conn01
        tell(ctx, 'T2-conn21-opened')

        # modify zfile and arrange timings so that T1 commits after zfile is
        # modified, but before we commit/abort.
        root21 = conn21.root()
        a21 = root21['zarray2']

        fh21  = a21._v_fileh
        vma21 = fh21.mmap(0, 1)

        Blk(vma21, 0)[0] = 21

        tell(ctx, 'T2-zfile2-modified')
        wait(ctx, 'T1-txn12-committed')

        # abort - zfile2 should stay unchanged
        transaction.abort()

        del vma21, fh21, a21, root21
        conn21.close()
        tell(ctx, 'T2-conn21-closed')


    wg = sync.WorkGroup(context.background())
    wg.go(T11)
    wg.go(T21)
    T21done.recv()  # NOTE not joining t11 yet

    # now verify that zfile2 stays at 11 state, i.e. T21 was really aborted
    conn02 = db.open()
    # NOTE top of connection stack is conn21(=conn01), because conn11_2 has 0
    # active objects
    assert conn02 is conn01
    root02 = conn02.root()

    f02 = root02['zfile2']
    # NOTE verification is done using afresh fileh to avoid depending on
    # leftover state from T11/T21.
    fh02  = f02.fileh_open()
    vma02 = fh02.mmap(0, 1)

    assert Blk(vma02, 0)[0] == 11

    del vma02, fh02, f02, root02
    conn02.close()


    c12_2 = NotifyChannel()   # T12 -> T22
    c21_2 = NotifyChannel()   # T22 -> T12

    # open, abort
    T12done = chan()
    @func
    def T12(ctx):
        defer(T12done.close)
        tell, wait = c12_2.tell, c21_2.wait

        wait(ctx, 'T2-conn22-opened')

        conn12 = db.open()

        tell(ctx, 'T1-conn12-opened')
        wait(ctx, 'T2-zfile2-modified')

        transaction.abort()

        tell(ctx, 'T1-txn-aborted')
        wait(ctx, 'T2-txn-committed')

        conn12.close()


    # open, modify, commit
    T22done = chan()
    @func
    def T22(ctx):
        defer(T22done.close)
        tell, wait = c21_2.tell, c12_2.wait

        # make sure we are not the same thread which ran T11
        # (should be so because we cared not to stop T11 yet)
        assert _thread.get_ident() != T11ident[0]

        conn22 = db.open()
        assert conn22 is conn01
        tell(ctx, 'T2-conn22-opened')

        # modify zfile and arrange timings so that T1 does abort after we
        # modify, but before we commit
        wait(ctx, 'T1-conn12-opened')
        root22 = conn22.root()
        a22 = root22['zarray2']

        fh22  = a22._v_fileh
        vma22 = fh22.mmap(0, 1)

        Blk(vma22, 0)[0] = 22

        tell(ctx, 'T2-zfile2-modified')
        wait(ctx, 'T1-txn-aborted')

        # commit - changes should propagate to zfile
        transaction.commit()

        tell(ctx, 'T2-txn-committed')

        conn22.close()


    wg.go(T12)
    wg.go(T22)
    T12done.recv()
    T22done.recv()

    # tell T11 to stop also
    def _(ctx):
        c21_1.tell(ctx, 'T11-exit-command')
    wg.go(_)
    wg.wait()

    # now verify that zfile2 changed to 22 state, i.e. T22 was really committed
    conn03 = db.open()
    defer(conn03.close)
    # NOTE top of connection stack is conn22(=conn01), because it has most # of
    # active objects
    assert conn03 is conn01
    root03 = conn03.root()

    f03  = root03['zfile2']
    fh03 = f03.fileh_open()
    vma03 = fh03.mmap(0, 1)

    assert Blk(vma03, 0)[0] == 22

    del vma03, fh03, f03


# ZBlk should properly handle 'invalidate' messages from DB
# ( NOTE this test is almost dupped at test_zbigarray_vs_cache_invalidation() )
@func
def _test_bigfile_filezodb_vs_cache_invalidation(_drop_cache):
    root = dbopen()
    conn = root._p_jar
    db   = conn.db()
    conn.close()
    del root, conn
    defer(db.close)

    tm1 = TransactionManager()
    tm2 = TransactionManager()

    conn1 = db.open(transaction_manager=tm1)
    root1 = conn1.root()
    defer(conn1.close)

    # setup zfile with fileh view to it
    root1['zfile3'] = f1 = ZBigFile(blksize)
    tm1.commit()

    fh1 = f1.fileh_open()
    tm1.commit()

    # set zfile initial data
    vma1 = fh1.mmap(0, 1)
    Blk(vma1, 0)[0] = 1
    tm1.commit()


    # read zfile and setup fileh for it in conn2
    conn2 = db.open(transaction_manager=tm2)
    root2 = conn2.root()
    defer(conn2.close)

    f2 = root2['zfile3']
    fh2 = f2.fileh_open()
    vma2 = fh2.mmap(0, 1)

    assert Blk(vma2, 0)[0] == 1 # read data in conn2 + make sure read correctly

    # now zfile content is both in ZODB.Connection cache and in _ZBigFileH
    # cache for each conn1 and conn2. Modify data in conn1 and make sure it
    # fully propagate to conn2.

    Blk(vma1, 0)[0] = 2
    tm1.commit()

    # still should be read as old value in conn2
    assert Blk(vma2, 0)[0] == 1
    # and even after virtmem pages reclaim
    # ( verifies that _p_invalidate() in ZBlk.loadblkdata() does not lead to
    #   reloading data as updated )
    ram_reclaim_all()
    assert Blk(vma2, 0)[0] == 1

    # FIXME: this simulates ZODB Connection cache pressure and currently
    # removes ZBlk corresponding to blk #0 from conn2 cache.
    # In turn this leads to conn2 missing that block invalidation on follow-up
    # transaction boundary.
    #
    # See FIXME notes on ZBlkBase._p_invalidate() for detailed description.
    #conn2._cache.minimize()
    _drop_cache(conn2)  # TODO change to just conn2._cache.minimize after issue is fixed

    tm2.commit()                # transaction boundary for t2

    # data from tm1 should propagate -> ZODB -> ram pages for _ZBigFileH in conn2
    assert Blk(vma2, 0)[0] == 2

    del conn2, root2

def test_bigfile_filezodb_vs_cache_invalidation():
    _test_bigfile_filezodb_vs_cache_invalidation(_drop_cache=lambda conn: None)
@xfail  # NOTE passes with wcfs
def test_bigfile_filezodb_vs_cache_invalidation_with_cache_pressure():
    _test_bigfile_filezodb_vs_cache_invalidation(_drop_cache=lambda conn: conn._cache.minimize())


# verify that conflicts on ZBlk are handled properly
# ( NOTE this test is almost dupped at test_zbigarray_vs_conflicts() )
@func
def test_bigfile_filezodb_vs_conflicts():
    root = dbopen()
    conn = root._p_jar
    db   = conn.db()
    conn.close()
    del root, conn
    defer(db.close)

    tm1 = TransactionManager()
    tm2 = TransactionManager()

    conn1 = db.open(transaction_manager=tm1)
    root1 = conn1.root()
    defer(conn1.close)

    # setup zfile with fileh view to it
    root1['zfile3a'] = f1 = ZBigFile(blksize)
    tm1.commit()

    fh1 = f1.fileh_open()
    tm1.commit()

    # set zfile initial data
    vma1 = fh1.mmap(0, 1)
    Blk(vma1, 0)[0] = 1
    tm1.commit()

    # read zfile and setup fileh for it in conn2
    conn2 = db.open(transaction_manager=tm2)
    root2 = conn2.root()
    defer(conn2.close)

    f2 = root2['zfile3a']
    fh2 = f2.fileh_open()
    vma2 = fh2.mmap(0, 1)

    assert Blk(vma2, 0)[0] == 1 # read data in conn2 + make sure read correctly

    # now zfile content is both in ZODB.Connection cache and in _ZBigFileH
    # cache for each conn1 and conn2. Modify data in both conn1 and conn2 and
    # see how it goes.

    Blk(vma1, 0)[0] = 11
    Blk(vma2, 0)[0] = 12

    # txn1 should commit ok
    tm1.commit()

    # txn2 should raise ConflictError and stay at 11 state
    with raises(ConflictError):
        tm2.commit()
    tm2.abort()

    assert Blk(vma2, 0)[0] == 11    # re-read in conn2
    Blk(vma2, 0)[0] = 13
    tm2.commit()

    assert Blk(vma1, 0)[0] == 11    # not yet propagated to conn1
    tm1.commit()                    # transaction boundary

    assert Blk(vma1, 0)[0] == 13    # re-read in conn1



# verify that fileh are garbage-collected after user free them
@func
def test_bigfile_filezodb_fileh_gc():
    root1= dbopen()
    conn1= root1._p_jar
    db   = conn1.db()
    defer(db.close)
    root1['zfile4'] = f1 = ZBigFile(blksize)
    transaction.commit()

    fh1  = f1.fileh_open()
    vma1 = fh1.mmap(0, 1)
    wfh1 = weakref.ref(fh1)
    assert wfh1() is fh1

    conn1.close()
    del vma1, fh1, f1, root1


    conn2 = db.open()
    root2 = conn2.root()
    defer(conn2.close)
    f2 = root2['zfile4']

    fh2  = f2.fileh_open()
    vma2 = fh2.mmap(0, 1)

    gc.collect()
    assert wfh1() is None   # fh1 should be gone

    del vma2, fh2, f2


# verify how zblk format change works
@func
def test_bigfile_filezodb_fmt_change():
    root = dbopen()
    defer(lambda: dbclose(root))
    root['zfile5'] = f = ZBigFile(blksize)
    transaction.commit()

    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)

    # save/restore original ZBlk_fmt_write
    fmt_write_save = file_zodb.ZBlk_fmt_write
    def _():
        file_zodb.ZBlk_fmt_write = fmt_write_save
    defer(_)

    # check all combinations of format pairs via working with blk #0 and
    # checking internal f structure
    for src_fmt, src_type in ZBlk_fmt_registry.items():
        for dst_fmt, dst_type in ZBlk_fmt_registry.items():
            if src_fmt == dst_fmt:
                continue    # skip checking e.g. ZBlk0 -> ZBlk0

            file_zodb.ZBlk_fmt_write = src_fmt
            struct.pack_into('p', vma, 0, b(src_fmt))
            transaction.commit()

            assert type(f.blktab[0]) is src_type

            file_zodb.ZBlk_fmt_write = dst_fmt
            struct.pack_into('p', vma, 0, b(dst_fmt))
            transaction.commit()

            assert type(f.blktab[0]) is dst_type


# test that ZData are reused for changed chunks in ZBlk1 format
@func
def test_bigfile_zblk1_zdata_reuse():
    # set ZBlk_fmt_write to ZBlk1 for this test
    fmt_write_save = file_zodb.ZBlk_fmt_write
    file_zodb.ZBlk_fmt_write = 'ZBlk1'
    def _():
        file_zodb.ZBlk_fmt_write = fmt_write_save
    defer(_)

    root = dbopen()
    defer(lambda: dbclose(root))
    root['zfile6'] = f = ZBigFile(blksize)
    transaction.commit()

    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, 1)
    b   = Blk(vma, 0)

    # initially empty and zero
    assert len(f.blktab) == 0
    assert (b == 0).all()

    # set all to 1 and save ZData instances
    b[:] = 1
    assert (b == 1).all()
    transaction.commit()
    assert len(f.blktab) == 1
    zblk0_v1 = f.blktab[0]
    assert len(zblk0_v1.chunktab) == blksize / file_zodb.ZBlk1.CHUNKSIZE
    zdata_v1 = zblk0_v1.chunktab.values()

    # set all to 2 and verify ZBlk/ZData instances were reused
    b[:] = 2
    assert (b == 2).all()
    transaction.commit()
    assert len(f.blktab) == 1
    zblk0_v2 = f.blktab[0]
    assert zblk0_v2 is zblk0_v1
    assert len(zblk0_v2.chunktab) == blksize / file_zodb.ZBlk1.CHUNKSIZE
    zdata_v2 = zblk0_v2.chunktab.values()

    assert len(zdata_v1) == len(zdata_v2)
    for i in range(len(zdata_v1)):
        assert zdata_v1[i] is zdata_v2[i]
