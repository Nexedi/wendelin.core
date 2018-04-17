# Wendelin.core.bigfile | Tests for ZODB utilities
# Copyright (C) 2014-2016  Nexedi SA and Contributors.
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
from wendelin.lib.zodb import deactivate_btree, dbclose
from wendelin.lib.testing import getTestDB
from persistent import Persistent, UPTODATE, GHOST
from BTrees.IOBTree import IOBTree
import transaction
import gc

testdb = None

def dbopen():
    return testdb.dbopen()

def setup_module():
    global testdb
    testdb = getTestDB()
    testdb.setup()

def teardown_module():
    testdb.teardown()


class XInt(Persistent):
    def __init__(self, i):
        self.i = i

def objscachedv(jar):
    return [obj for oid, obj in jar._cache.lru_items()]

def test_deactivate_btree():
    root = dbopen()
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

    dbclose(root)
