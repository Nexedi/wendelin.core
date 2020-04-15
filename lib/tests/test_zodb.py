# Wendelin.core.bigfile | Tests for ZODB utilities
# Copyright (C) 2014-2020  Nexedi SA and Contributors.
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
from wendelin.lib.zodb import LivePersistent, deactivate_btree, dbclose, zconn_at, zmajor
from wendelin.lib.testing import getTestDB
from persistent import Persistent, UPTODATE, GHOST, CHANGED
from ZODB import DB, POSException
from BTrees.IOBTree import IOBTree
import transaction
from transaction import TransactionManager
from golang import defer, func
from pytest import raises
import pytest; xfail = pytest.mark.xfail

testdb = None

def dbopen():
    return testdb.dbopen()

def setup_module():
    global testdb
    testdb = getTestDB()
    testdb.setup()

def teardown_module():
    testdb.teardown()

# like db.cacheDetail(), but {} instead of []
def cacheInfo(db):
    return dict(db.cacheDetail())

# key for cacheInfo() result
def kkey(klass):
    return '%s.%s' % (klass.__module__, klass.__name__)

@func
def test_livepersistent():
    root = dbopen()
    transaction.commit()    # set root._p_jar
    db = root._p_jar.db()

    # ~~~ test `obj initially created` case
    root['live'] = lp = LivePersistent()
    assert lp._p_jar   is None          # connection does not know about it yet
    assert lp._p_state == UPTODATE      # object initially created in uptodate

    # should not be in cache yet & thus should stay after gc
    db.cacheMinimize()
    assert lp._p_jar   is None
    assert lp._p_state == UPTODATE
    ci = cacheInfo(db)
    assert kkey(LivePersistent) not in ci

    # should be registered to connection & cache after commit
    transaction.commit()
    assert lp._p_jar   is not None
    assert lp._p_state == UPTODATE
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1

    # should stay that way after cache gc
    db.cacheMinimize()
    assert lp._p_jar   is not None
    assert lp._p_state == UPTODATE
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1


    # ~~~ reopen & test `obj loaded from db` case
    dbclose(root)
    del root, db, lp

    root = dbopen()
    db = root._p_jar.db()

    # known to connection & cache & GHOST
    # right after first loading from DB
    lp = root['live']
    assert lp._p_jar   is not None
    assert lp._p_state is GHOST
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1

    # should be UPTODATE for sure after read access
    getattr(lp, 'attr', None)
    assert lp._p_jar   is not None
    assert lp._p_state is UPTODATE
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1

    # does not go back to ghost on cache gc
    db.cacheMinimize()
    assert lp._p_jar   is not None
    assert lp._p_state == UPTODATE
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1

    # ok
    dbclose(root)
    del root, db, lp


    # demo that upon cache invalidation LivePersistent can go back to ghost
    root = dbopen()
    conn = root._p_jar
    db   = conn.db()
    conn.close()
    del root, conn

    tm1 = TransactionManager()
    tm2 = TransactionManager()

    conn1 = db.open(transaction_manager=tm1)
    root1 = conn1.root()
    defer(lambda: dbclose(root1))
    lp1 = root1['live']

    conn2 = db.open(transaction_manager=tm2)
    root2 = conn2.root()
    defer(conn2.close)
    lp2 = root2['live']

    # 2 connections are setup running in parallel with initial obj state as ghost
    assert lp1._p_jar   is conn1
    assert lp2._p_jar   is conn2

    assert lp1._p_state is GHOST
    assert lp2._p_state is GHOST

    # conn1: modify  ghost -> changed
    lp1.attr = 1

    assert lp1._p_state is CHANGED
    assert lp2._p_state is GHOST

    # conn2: read    ghost -> uptodate
    assert getattr(lp1, 'attr', None) == 1
    assert getattr(lp2, 'attr', None) is None

    assert lp1._p_state is CHANGED
    assert lp2._p_state is UPTODATE

    # conn1: commit  changed -> uptodate; conn2 untouched
    tm1.commit()

    assert lp1._p_state is UPTODATE
    assert lp2._p_state is UPTODATE

    assert getattr(lp1, 'attr', None) == 1
    assert getattr(lp2, 'attr', None) is None

    # conn2: commit  (nothing changed - just transaction boundary)
    #                 uptodate -> ghost (invalidation)
    tm2.commit()

    assert lp1._p_state is UPTODATE
    assert lp2._p_state is GHOST

    assert getattr(lp1, 'attr', None) == 1

    # conn2: after reading, the state is again uptodate + changes from conn1 are here
    a = getattr(lp2, 'attr', None)
    assert lp2._p_state is UPTODATE
    assert a == 1

    del conn2, root2


class XInt(Persistent):
    def __init__(self, i):
        self.i = i

def objscachedv(jar):
    return [obj for oid, obj in jar._cache.lru_items()]

@func
def test_deactivate_btree():
    root = dbopen()
    defer(lambda: dbclose(root))

    # init btree with many leaf nodes
    leafv = []
    root['btree'] = B = IOBTree()
    for i in range(10000):
        B[i] = xi = XInt(i)
        leafv.append(xi)
    transaction.commit()

    for npass in range(2):
        # access all elements making them live
        for _ in B.values():
            _._p_activate()

        # now B or/and some leaf nodes should be up-to-date and in cache
        cached = objscachedv(root._p_jar)
        nlive = 0
        for obj in [B] + leafv:
            if obj._p_state == UPTODATE:
                assert obj in cached
                nlive += 1
        assert nlive > 0

        # check how deactivate_btree() works dependently from initially BTree state
        if npass == 0:
            B._p_activate()
        else:
            B._p_deactivate()

        # after btree deactivation B & all leaf nodes should be in ghost state and not in cache
        deactivate_btree(B)
        cached = objscachedv(root._p_jar)
        for obj in [B] + leafv:
            assert obj._p_state == GHOST
            assert obj not in cached


# verify that zconn_at gives correct answer.
@xfail(zmajor < 5, reason="zconn_at is TODO for ZODB4 and ZODB3")
@func
def test_zconn_at():
    stor = testdb.getZODBStorage()
    defer(stor.close)
    db  = DB(stor)

    zsync(stor)
    at0 = stor.lastTransaction()

    # open connection, it must be viewing the database @at0
    tm1 = TransactionManager()
    conn1 = db.open(transaction_manager=tm1)
    assert zconn_at(conn1) == at0

    # open another simultaneous connection
    tm2 = TransactionManager()
    conn2 = db.open(transaction_manager=tm2)
    assert zconn_at(conn2) == at0

    # commit in conn1
    root1 = conn1.root()
    root1['z'] = 1
    tm1.commit()
    zsync(stor)
    at1 = stor.lastTransaction()

    # after commit conn1 view is updated; conn2 view stays @at0
    assert zconn_at(conn1) == at1
    assert zconn_at(conn2) == at0

    # reopen conn1 -> view @at1
    conn1.close()
    with raises(POSException.ConnectionStateError):
        zconn_at(conn1)
    assert zconn_at(conn2) == at0
    conn1_ = db.open(transaction_manager=tm1)
    assert conn1_ is conn1   # returned from DB pool
    assert zconn_at(conn1) == at1
    assert zconn_at(conn2) == at0
    conn1.close()

    # commit empty transaction - view stays in sync with storage head
    conn1_ = db.open(transaction_manager=tm1)
    assert conn1_ is conn1   # from DB pool
    assert zconn_at(conn1) == at1
    assert zconn_at(conn2) == at0
    tm1.commit()
    zsync(stor)
    at1_ = stor.lastTransaction()

    assert zconn_at(conn1) == at1_
    assert zconn_at(conn2) == at0


    # reopen conn2 -> view upated to @at1_
    conn2.close()
    conn2_ = db.open(transaction_manager=tm1)
    assert conn2_ is conn2  # from DB pool
    assert zconn_at(conn1) == at1_
    assert zconn_at(conn2) == at1_

    conn1.close()
    conn2.close()


    # verify with historic connection @at0
    tm_old = TransactionManager()
    defer(tm_old.abort)
    conn_at0 = db.open(transaction_manager=tm_old, at=at0)
    assert conn_at0 is not conn1
    assert conn_at0 is not conn2
    assert zconn_at(conn_at0) == at0


# verify that ZODB.Connection.onResyncCallback works
@xfail(zmajor < 5, reason="ZODB.Connection.onResyncCallback is TODO for ZODB4 and ZODB3")
@func
def test_zodb_onresync():
    stor = testdb.getZODBStorage()
    defer(stor.close)
    db  = DB(stor)

    class T:
        def __init__(t):
            t.nresync = 0
        def on_connection_resync(t):
            t.nresync += 1

    t = T()

    conn = db.open()
    conn.onResyncCallback(t)
    assert t.nresync == 0

    # abort makes conn to enter new transaction
    transaction.abort()
    assert t.nresync == 1

    # close/reopen -> new transaction
    conn.close()
    assert t.nresync == 1
    conn_ = db.open()
    assert conn_ is conn
    assert t.nresync == 2

    # commit -> new transaction
    root = conn.root()
    root['r'] = 1
    assert t.nresync == 2
    transaction.commit()
    assert t.nresync == 3
    transaction.commit()
    assert t.nresync == 4
    transaction.commit()
    assert t.nresync == 5

    conn.close()


# ---- misc ----

# zsync syncs ZODB storage.
# it is noop, if zstor does not support syncing (i.e. FileStorage has no .sync())
def zsync(zstor):
    sync = getattr(zstor, 'sync', None)
    if sync is not None:
        sync()
