# Wendeling.core.bigarray | Tests for ZBigArray
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
from wendelin.bigarray.array_zodb import ZBigArray
from wendelin.bigfile.tests.test_filezodb import ram_reclaim_all
from wendelin.bigfile.tests.test_thread import NotifyChannel
from wendelin.lib.zodb import dbclose
from wendelin.lib.tests.test_zodb import cacheInfo, kkey
from wendelin.lib.testing import getTestDB
from persistent import UPTODATE
import transaction
from transaction import TransactionManager
from ZODB.POSException import ConflictError
from numpy import dtype, uint8, all, array_equal, arange
from golang import defer, func, chan
from golang import context, sync
from six.moves import _thread

from pytest import raises

testdb = None
def setup_module():
    global testdb
    testdb = getTestDB()
    testdb.setup()

def teardown_module():
    testdb.teardown()


@func
def test_zbigarray():
    root = testdb.dbopen()
    defer(lambda: dbclose(root))

    root['zarray'] = ZBigArray((16*1024*1024,), uint8)
    transaction.commit()

    dbclose(root)


    root = testdb.dbopen()
    A = root['zarray']

    assert isinstance(A, ZBigArray)
    assert A.shape  == (16*1024*1024,)
    assert A.dtype  == dtype(uint8)

    assert all(A[:] == 0)

    a = A[:]
    a[1] = 1
    a[3] = 3
    a[5] = 5
    a[-1] = 99

    b = A[:]
    assert (b[0],b[1]) == (0,1)
    assert (b[2],b[3]) == (0,3)
    assert (b[4],b[5]) == (0,5)
    assert all(b[6:-1] == 0)
    assert b[-1] == 99

    # abort - should forget all changes
    transaction.abort()
    assert all(a[:] == 0)
    assert all(b[:] == 0)
    assert all(A[:] == 0)

    # now modify again and commit
    a[33] = 33
    a[-2] = 98
    assert all(b[:33] == 0)
    assert b[33] == 33
    assert all(b[33+1:-2] == 0)
    assert b[-2] == 98
    assert b[-1] == 0

    transaction.commit()

    # reload DB & array
    dbclose(root)
    del a,b, A


    root = testdb.dbopen()
    A = root['zarray']

    assert isinstance(A, ZBigArray)
    assert A.shape  == (16*1024*1024,)
    assert A.dtype  == dtype(uint8)

    a = A[:]
    assert all(a[:33] == 0)
    assert a[33] == 33
    assert all(a[33+1:-2] == 0)
    assert a[-2] == 98
    assert a[-1] == 0


    # like ZBigFile ZBigArray should survive Persistent cache clearing and not
    # go to ghost state (else logic to propagate changes from pages to objects
    # would subtly brake after Persistent cache gc)
    db = root._p_jar.db()
    ci = cacheInfo(db)
    assert ci[kkey(ZBigArray)] == 1
    assert A._p_state == UPTODATE
    db.cacheMinimize()
    ci = cacheInfo(db)
    assert ci[kkey(ZBigArray)] == 1
    assert A._p_state == UPTODATE   # it would be GHOST without LivePersistent protection

    a[-1] = 99  # would not propagate to file without ZBigFile preventing itself to go to ghost
    transaction.commit()


    # reload & verify changes
    dbclose(root)
    del a, A, db


    root = testdb.dbopen()
    A = root['zarray']

    assert isinstance(A, ZBigArray)
    assert A.shape  == (16*1024*1024,)
    assert A.dtype  == dtype(uint8)

    a = A[:]
    assert all(a[:33] == 0)
    assert a[33] == 33
    assert all(a[33+1:-2] == 0)
    assert a[-2] == 98
    assert a[-1] == 99


    # resize array & append data
    A.resize((24*1024*1024,))
    assert A.shape  == (24*1024*1024,)
    assert A.dtype  == dtype(uint8)

    b = A[:]
    assert array_equal(a, b[:16*1024*1024])

    b[16*1024*1024] = 100
    b[-1]           = 255

    A.append(arange(10, 14, dtype=uint8))


    # commit; reload & verify changes
    transaction.commit()
    dbclose(root)
    del a, b, A


    root = testdb.dbopen()
    A = root['zarray']

    assert isinstance(A, ZBigArray)
    assert A.shape  == (24*1024*1024 + 4,)
    assert A.dtype  == dtype(uint8)

    a = A[:]
    assert all(a[:33] == 0)
    assert a[33] == 33
    assert all(a[33+1:16*1024*1024-2] == 0)
    assert a[16*1024*1024-2] == 98
    assert a[16*1024*1024-1] == 99

    assert a[16*1024*1024]   == 100
    assert a[24*1024*1024-1] == 255

    assert a[24*1024*1024+0] ==  10
    assert a[24*1024*1024+1] ==  11
    assert a[24*1024*1024+2] ==  12
    assert a[24*1024*1024+3] ==  13


# test array ordering is saved properly into DB and is picked up in
# backward-compatible manner - for data saved before order parameter was
# introduced.
# (actual ordering indexing test is in BigArray tests, not here)
@func
def test_zbigarray_order():
    # make sure order is properly saved/restored to/from DB
    root = testdb.dbopen()
    defer(lambda: dbclose(root))
    root['carray'] = ZBigArray((16*1024*1024,), uint8)
    root['farray'] = ZBigArray((16*1024*1024,), uint8, order='F')
    transaction.commit()

    dbclose(root)

    root = testdb.dbopen()
    C = root['carray']
    F = root['farray']

    assert isinstance(C, ZBigArray)
    assert C.shape  == (16*1024*1024,)
    assert C.dtype  == dtype(uint8)
    assert C._order == 'C'

    assert isinstance(F, ZBigArray)
    assert F.shape  == (16*1024*1024,)
    assert F.dtype  == dtype(uint8)
    assert F._order == 'F'

    # make sure we can read previously saved data which had no order set
    root['coldarray'] = Cold = ZBigArray((16*1024*1024,), uint8)
    del Cold._order # simulate that it is without
    assert '_order' not in Cold.__getstate__()
    transaction.commit()

    dbclose(root)

    root = testdb.dbopen()
    Cold = root['coldarray']

    assert Cold._order == 'C'



# the same as test_bigfile_filezodb_vs_conn_migration but explicitly for ZBigArray
# ( NOTE this test is almost dup of test_zbigarray_vs_conn_migration() )
@func
def test_zbigarray_vs_conn_migration():
    root01 = testdb.dbopen()
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

        # setup zarray
        root11_1 = conn11_1.root()
        root11_1['zarray2'] = a11 = ZBigArray((10,), uint8)
        transaction.commit()

        # set initial data
        a11[0:1] = [11]     # XXX -> [0] = 11 after BigArray can
        transaction.commit()

        # close conn, wait till T21 reopens it
        del a11, root11_1
        conn11_1.close()
        tell(ctx, 'T1-conn11_1-closed')
        wait(ctx, 'T2-conn21-opened')

        # open nother connection. it must be different
        # (see appropriate place in zfile test about why)
        conn11_2 = db.open()
        assert conn11_2 is not conn11_1
        root11_2 = conn11_2.root()

        wait(ctx, 'T2-zarray2-modified')

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

        # wait until T1 finish setting up initial data and get its connection
        # (see appropriate place in zfile tests for details)
        wait(ctx, 'T1-conn11_1-closed')
        conn21 = db.open()
        assert conn21 is conn01
        tell(ctx, 'T2-conn21-opened')

        # modify zarray and arrange timings so that T1 commits after zarray is
        # modified, but before we commit/abort.
        root21 = conn21.root()
        a21 = root21['zarray2']

        a21[0:1] = [21]     # XXX -> [0] = 21 after BigArray can

        tell(ctx, 'T2-zarray2-modified')
        wait(ctx, 'T1-txn12-committed')

        # abort - zarray2 should stay unchanged
        transaction.abort()

        del a21, root21
        conn21.close()
        tell(ctx, 'T2-conn21-closed')


    wg = sync.WorkGroup(context.background())
    wg.go(T11)
    wg.go(T21)
    T21done.recv()  # NOTE not joining t11 yet

    # now verify that zarray2 stays at 11 state, i.e. T21 was really aborted
    conn02 = db.open()
    # NOTE top of connection stack is conn21(=conn01), becase conn11_2 has 0
    # active objects
    assert conn02 is conn01
    root02 = conn02.root()

    a02 = root02['zarray2']
    assert a02[0] == 11

    del a02, root02
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
        defer(conn12.close)

        tell(ctx, 'T1-conn12-opened')
        wait(ctx, 'T2-zarray2-modified')

        transaction.abort()

        tell(ctx, 'T1-txn-aborted')
        wait(ctx, 'T2-txn-committed')


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
        defer(conn22.close)
        assert conn22 is conn01
        tell(ctx, 'T2-conn22-opened')

        # modify zarray and arrange timings so that T1 does abort after we
        # modify, but before we commit
        wait(ctx, 'T1-conn12-opened')
        root22 = conn22.root()
        a22 = root22['zarray2']

        a22[0:1] = [22]     # XXX -> [0] = 22   after BigArray can

        tell(ctx, 'T2-zarray2-modified')
        wait(ctx, 'T1-txn-aborted')

        # commit - changes should propagate to zarray
        transaction.commit()

        tell(ctx, 'T2-txn-committed')


    wg.go(T12)
    wg.go(T22)
    T12done.recv()
    T22done.recv()

    # tell T11 to stop also
    def _(ctx):
        c21_1.tell(ctx, 'T11-exit-command')
    wg.go(_)
    wg.wait()

    # now verify that zarray2 changed to 22 state, i.e. T22 was really committed
    conn03 = db.open()
    defer(conn03.close)
    # NOTE top of connection stack is conn22(=conn01), becase it has most # of
    # active objectd
    assert conn03 is conn01
    root03 = conn03.root()

    a03  = root03['zarray2']
    assert a03[0] == 22

    del a03


# underlying ZBigFile/ZBigFileH should properly handle 'invalidate' messages from DB
# ( NOTE this test is almost dup of test_zbigarray_vs_cache_invalidation() )
@func
def test_zbigarray_vs_cache_invalidation():
    root = testdb.dbopen()
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

    # setup zarray
    root1['zarray3'] = a1 = ZBigArray((10,), uint8)
    tm1.commit()

    # set zarray initial data
    a1[0:1] = [1]           # XXX -> [0] = 1  after BigArray can
    tm1.commit()


    # read zarray in conn2
    conn2 = db.open(transaction_manager=tm2)
    root2 = conn2.root()
    defer(conn2.close)

    a2 = root2['zarray3']
    assert a2[0:1] == [1]   # read data in conn2 + make sure read correctly
                            # XXX -> [0] == 1  after BigArray can

    # now zarray content is both in ZODB.Connection cache and in _ZBigFileH
    # cache for each conn1 and conn2. Modify data in conn1 and make sure it
    # fully propagate to conn2.

    a1[0:1] = [2]           # XXX -> [0] = 2  after BigArray can
    tm1.commit()

    # still should be read as old value in conn2
    assert a2[0:1]  == [1]
    # and even after virtmem pages reclaim
    # ( verifies that _p_invalidate() in ZBlk.loadblkdata() does not lead to
    #   reloading data as updated )
    ram_reclaim_all()
    assert a2[0:1]  == [1]

    tm2.commit()            # transaction boundary for t2

    # data from tm1 should propagate -> ZODB -> ram pages for _ZBigFileH in conn2
    assert a2[0] == 2

    del conn2, root2


# verify that conflicts on array content are handled properly
# ( NOTE this test is almost dup of test_bigfile_filezodb_vs_conflicts() )
@func
def test_zbigarray_vs_conflicts():
    root = testdb.dbopen()
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

    # setup zarray
    root1['zarray3a'] = a1 = ZBigArray((10,), uint8)
    tm1.commit()

    # set zarray initial data
    a1[0:1] = [1]           # XXX -> [0] = 1  after BigArray can
    tm1.commit()

    # read zarray in conn2
    conn2 = db.open(transaction_manager=tm2)
    root2 = conn2.root()
    defer(conn2.close)

    a2 = root2['zarray3a']
    assert a2[0:1] == [1]   # read data in conn2 + make sure read correctly
                            # XXX -> [0] == 1  after BigArray can

    # now zarray content is both in ZODB.Connection cache and in _ZBigFileH
    # cache for each conn1 and conn2. Modify data in both conn1 and conn2 and
    # see how it goes.

    a1[0:1] = [11]          # XXX -> [0] = 11  after BigArray can
    a2[0:1] = [12]          # XXX -> [0] = 12  after BigArray can

    # txn1 should commit ok
    tm1.commit()

    # txn2 should raise ConflictError and stay at 11 state
    with raises(ConflictError):
        tm2.commit()
    tm2.abort()

    assert a2[0:1] == [11]  # re-read in conn2  XXX -> [0] == 11 after BigArray can
    a2[0:1] = [13]          # XXX -> [0] = 13 after BigArray can
    tm2.commit()

    assert a1[0:1] == [11]  # not yet propagated to conn1   XXX -> [0] == 11
    tm1.commit()            # transaction boundary

    assert a1[0:1] == [13]  # re-read in conn1  XXX -> [0] == 13


# verify that conflicts on array metadata are handled properly
# ( NOTE this test is close to test_zbigarray_vs_conflicts() )
@func
def test_zbigarray_vs_conflicts_metadata():
    root = testdb.dbopen()
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

    # setup zarray
    root1['zarray3b'] = a1 = ZBigArray((10,), uint8)
    tm1.commit()

    # set zarray initial data
    a1[0:1] = [1]           # XXX -> [0] = 1  after BigArray can
    tm1.commit()

    # read zarray in conn2
    conn2 = db.open(transaction_manager=tm2)
    root2 = conn2.root()
    defer(conn2.close)

    a2 = root2['zarray3b']
    assert a2[0:1] == [1]   # read data in conn2 + make sure read correctly
                            # XXX -> [0] == 1  after BigArray can

    # now zarray content is both in ZODB.Connection cache and in _ZBigFileH
    # cache for each conn1 and conn2. Resize arrays in both conn1 and conn2 and
    # see how it goes.

    a1.resize((11,))
    a2.resize((12,))

    # txn1 should commit ok
    tm1.commit()

    # txn2 should raise ConflictError and stay at 11 state
    with raises(ConflictError):
        tm2.commit()
    tm2.abort()

    assert len(a2) == 11    # re-read in conn2
    a2.resize((13,))
    tm2.commit()

    assert len(a1) == 11    # not yet propagated to conn1
    tm1.commit()            # transaction boundary

    assert len(a1) == 13    # re-read in conn1


# verify how ZBigArray behaves when plain properties are changed / invalidated
@func
def test_zbigarray_invalidate_shape():
    root = testdb.dbopen()
    conn = root._p_jar
    db   = conn.db()
    conn.close()
    del root, conn
    defer(db.close)

    print
    tm1 = TransactionManager()
    tm2 = TransactionManager()

    conn1 = db.open(transaction_manager=tm1)
    root1 = conn1.root()
    defer(conn1.close)

    # setup zarray
    root1['zarray4'] = a1 = ZBigArray((10,), uint8)
    tm1.commit()

    # set zarray initial data
    a1[0:1] = [1]           # XXX -> [0] = 1  after BigArray can
    tm1.commit()

    # read zarray in conn2
    conn2 = db.open(transaction_manager=tm2)
    root2 = conn2.root()
    defer(conn2.close)

    a2 = root2['zarray4']
    assert a2[0:1] == [1]   # read data in conn2 + make sure read correctly
                            # XXX -> [0] == 1  after BigArray can

    # append to a1 which changes both RAM pages and a1.shape
    assert a1.shape == (10,)
    a1.append([123])
    assert a1.shape == (11,)
    assert a1[10:11] == [123]   # XXX -> [10] = 123  after BigArray can
    tm1.commit()
    tm2.commit()            # just transaction boundary for t2

    # data from tm1 should propagate to tm
    assert a2.shape == (11,)
    assert a2[10:11] == [123]   # XXX -> [10] = 123  after BigArray can


    del conn2, root2, a2


# Ensure ZBigArray mimics the behavior of ndarray
# when initializing its shape property.
# (array.shape should always return a tuple of ints,
#  but during initialization it should allow sequences of
#  ints or ints)
def test_zbigarray_shape_initialization():
    def assert_shape_becomes(shape_input, shape_property):
        assert ZBigArray(shape=shape_input, dtype=float).shape == shape_property

    assert_shape_becomes([1, 4, 3], (1, 4, 3))
    assert_shape_becomes(42, (42,))
    assert_shape_becomes((4, 4), (4, 4))