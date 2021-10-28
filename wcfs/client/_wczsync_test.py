# -*- coding: utf-8 -*-
# Wendelin.bigfile | Tests for WCFS part of BigFile ZODB backend
# Copyright (C) 2020-2021  Nexedi SA and Contributors.
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

from wendelin.wcfs.client._wczsync import _ZSync, _zsync_wclose_wg
from wendelin import wcfs
from wendelin.lib.zodb import zstor_2zurl, zconn_at
from wendelin.lib.testing import getTestDB
from ZODB import DB
from ZODB.utils import p64
import transaction
from golang import defer, func, error
import weakref, gc

from pytest import raises

testdb = None
def setup_module():
    global testdb
    testdb = getTestDB()
    testdb.setup()
def teardown_module():
    testdb.teardown()


# _zsync_setup setups up DB, zconn and wconn _ZSync'ed to zconn.
@func
def _zsync_setup(zstor): # -> (db, zconn, wconn)
    zurl = zstor_2zurl(zstor)

    # create new DB that we'll precisely control
    db = DB(zstor)
    zconn = db.open()
    at0 = zconn_at(zconn)
    # create wconn
    wc = wcfs.join(zurl)
    wconn = wc.connect(at0)
    assert wconn.at() == at0
    # setup ZSync for wconn <-> zconn; don't keep zsync explicitly referenced
    # NOTE ZSync takes ownership of wconn.wc (= wc), so we don't wc.close
    _ZSync(zconn, wconn)

    assert wconn.at() == at0
    return db, zconn, wconn


# verify that ZSync closes wconn when db is closed.
@func
def test_zsync_db_close():
    zstor = testdb.getZODBStorage()
    defer(zstor.close)

    db, zconn, wconn = _zsync_setup(zstor)
    defer(wconn.close)

    # close db -> ZSync should close wconn and wc even though zconn stays referenced
    wc_njoin0 = wconn.wc._njoin
    db.close()
    _zsync_wclose_wg.wait()
    # NOTE db and zconn are still alive - not GC'ed
    with raises(error, match=": connection closed"):
        wconn.open(p64(0))
    assert wconn.wc._njoin == (wc_njoin0 - 1)


# verify that ZSync closes wconn when zconn is garbage-collected.
@func
def test_zsync_zconn_gc():
    zstor = testdb.getZODBStorage()
    defer(zstor.close)

    db, zconn, wconn = _zsync_setup(zstor)
    defer(wconn.close)

    # del zconn -> zconn should disappear and ZSync should close wconn and wc
    zconn_weak = weakref.ref(zconn)
    assert zconn_weak() is not None
    wc_njoin0 = wconn.wc._njoin
    del zconn
    # NOTE db stays alive and not closed
    gc.collect()
    assert zconn_weak() is None
    _zsync_wclose_wg.wait()
    with raises(error, match=": connection closed"):
        wconn.open(p64(0))
    assert wconn.wc._njoin == (wc_njoin0 - 1)


# verify that ZSync keeps wconn in sync wrt zconn.
@func
def test_zsync_resync():
    zstor = testdb.getZODBStorage()
    defer(zstor.close)

    db, zconn, wconn = _zsync_setup(zstor)
    defer(db.close)

    # commit something - ZSync should resync wconn to updated db state
    at0 = zconn_at(zconn)
    assert wconn.at() == at0
    root = zconn.root()
    root['tzync'] = 1
    transaction.commit()
    del root
    at1 = zconn_at(zconn)
    assert at1 != at0
    assert wconn.at() == at1

    # commit something via different zconn2 - wconn stays in sync with original zconn @at1
    tm2 = transaction.TransactionManager()
    zconn2 = db.open(transaction_manager=tm2)
    assert zconn_at(zconn2) == at1
    root2 = zconn2.root()
    root2['tzsync'] = 2
    tm2.commit()
    at2 = zconn_at(zconn2)
    assert at2 != at1
    assert zconn_at(zconn) == at1
    assert wconn.at() == at1

    # close zconn - it should stay in db.pool with wconn alive and still @at1
    zconn_weak = weakref.ref(zconn)
    assert zconn_weak() is zconn
    zconn.close()
    del zconn
    gc.collect()
    assert zconn_weak() is not None
    assert wconn.at() == at1

    # reopen db - it should return zconn and resynced to @at2; ZSync should keep wconn in sync
    zconn = db.open()
    assert zconn_weak() is zconn
    assert zconn_at(zconn) == at2
    assert wconn.at() == at2
