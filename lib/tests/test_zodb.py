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
from wendelin.lib.zodb import LivePersistent, deactivate_btree, dbclose
from wendelin.lib.testing import getTestDB
from persistent import Persistent, UPTODATE, GHOST, CHANGED
from BTrees.IOBTree import IOBTree
import transaction
from transaction import TransactionManager
from golang import defer, func

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
