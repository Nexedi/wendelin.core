# -*- coding: utf-8 -*-
# Wendelin.core.bigfile | Tests for ZODB utilities and critical properties of ZODB itself
# Copyright (C) 2014-2022  Nexedi SA and Contributors.
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
from wendelin.lib.zodb import LivePersistent, deactivate_btree, dbclose, zconn_at, zstor_2zurl, zmajor, _zhasNXDPatch, dbstoropen, zurl_normalize
from wendelin.lib.testing import getTestDB
from wendelin.lib import testing
from persistent import Persistent, UPTODATE, GHOST, CHANGED
from ZODB import DB, POSException
from ZODB.FileStorage import FileStorage
from ZODB.MappingStorage import MappingStorage
from ZODB.DemoStorage import DemoStorage
from BTrees.IOBTree import IOBTree
import transaction
from transaction import TransactionManager
from golang import defer, func
from pytest import raises
import pytest; xfail = pytest.mark.xfail
from ZEO.ClientStorage import ClientStorage as ZEOStorage
from neo.client.Storage import Storage as NEOStorage
import os
from six.moves.urllib.parse import quote_plus

from wendelin.lib.tests.testprog import zopenrace, zloadrace

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
@func
def test_zconn_at():
    if zmajor == 4 and not _zhasNXDPatch('conn:MVCC-via-loadBefore-only'):
        pytest.xfail(reason="zconn_at needs https://lab.nexedi.com/nexedi/ZODB/merge_requests/1 to work on ZODB4")

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


    # reopen conn2 -> view updated to @at1_
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


# verify that ZODB.Connection.onShutdownCallback works
@func
def test_zodb_onshutdown():
    stor = testdb.getZODBStorage()
    defer(stor.close)
    db  = DB(stor)

    class T:
        def __init__(t):
            t.nshutdown = 0
        def on_connection_shutdown(t):
            t.nshutdown += 1

    t1 = T()
    t2 = T()

    # conn1 stays alive outside of db.pool
    conn1 = db.open()
    conn1.onShutdownCallback(t1)

    # conn2 stays alive inside db.pool
    conn2 = db.open()
    conn2.onShutdownCallback(t2)
    conn2.close()

    assert t1.nshutdown == 0
    assert t2.nshutdown == 0

    # db.close triggers conn1 and conn2 shutdown
    db.close()
    assert t1.nshutdown == 1
    assert t2.nshutdown == 1


# test that zurl does not change from one open to another storage open.
def test_zurlstable():
    if not isinstance(testdb, (testing.TestDB_FileStorage, testing.TestDB_ZEO, testing.TestDB_NEO)):
        pytest.xfail(reason="zstor_2zurl is TODO for %r" % testdb)
    zurl0 = None
    for i in range(10):
        zstor = testdb.getZODBStorage()
        zurl  = zstor_2zurl(zstor)
        zstor.close()
        if i == 0:
            zurl0 = zurl
        else:
            assert zurl == zurl0


# test that ZODB database opened via storage's zurl, provides access to the same data.
@func
def test_zurlsamedb():
    stor1 = testdb.getZODBStorage()
    defer(stor1.close)

    # skip on FileStorage - ZODB/py fails with LockError on attempt to create
    # two FileStorage's connected to the same data.
    if isinstance(stor1, FileStorage):
        pytest.skip("skipping on FileStorage")

    zurl = zstor_2zurl(stor1)
    stor2 = dbstoropen(zurl)
    defer(stor2.close)

    db1 = DB(stor1)
    db2 = DB(stor2)

    # get/set retrieves or sets root['X'] = x.
    @func
    def set(db, x):
        conn = db.open();   defer(conn.close)
        root = conn.root()
        root['X'] = x
        transaction.commit()
    @func
    def get(db):
        zsync(db.storage)
        conn = db.open();   defer(conn.close)
        root = conn.root()
        return root['X']

    # stor1/stor2 should have the same data
    set(db1, 1)
    assert get(db2) == 1
    set(db1, 'abc')
    assert get(db2) == 'abc'


# ensure zstor_2zurl returns expected zurl.
def test_zstor_2zurl(tmpdir, neo_ssl_dict):
    # fs1 returns new FileStorage located in tmpdir.
    def fs1(name):
        return FileStorage("%s/%s" % (tmpdir, name))

    # zeo returns new ZEO client for specified storage name and server address.
    #
    # server_addr can be either:
    # - str (specifying address of UNIX socket), or
    # - (host, addr) pair - specifying TCP address.
    #
    # NOTE the client is returned without waiting until server is connected.
    def zeo(storage_name, server_addr):
        if testing.TestDB_ZEO('').z5:
            return ZEOStorage(server_addr, storage=storage_name, wait=False)

        # It's better to use a mock storage for zeo == 4, because
        # we would have to wait for a long time. See here the
        # respective part in ZEO4 source code:
        #
        #   https://github.com/zopefoundation/ZEO/blob/4/src/ZEO/ClientStorage.py#L423-L430
        #
        # ..compared to ZEO5 which omits the else clause:
        #
        #   https://github.com/zopefoundation/ZEO/blob/5.3.0/src/ZEO/ClientStorage.py#L279-L286
        zeo_storage = type(
            "ClientStorage",
            (object,),
            {
                "_addr": server_addr,
                "_storage": storage_name,
                "close": lambda self: None,
                "getName": lambda self: self._storage
            }
        )()
        type(zeo_storage).__module__ = "ZEO.ClientStorage"
        type(zeo_storage).__name__ = "ClientStorage"
        return zeo_storage

    # neo returns new NEO client for specified cluster name and master address.
    # NOTE, similarly to ZEO, the client is returned without waiting until server nodes are connected.
    def neo(cluster_name, master_addr, ssl=0):
        kwargs = dict(master_nodes=master_addr, name=cluster_name)
        if ssl:
            kwargs.update(neo_ssl_dict)
        return NEOStorage(**kwargs)

    # demo returns new DemoStorage with specified base and delta.
    def demo(base, delta):
        return DemoStorage(base=base, changes=delta)

    # assert_zurl_is_correct verifies that zstor_2zurl(zstor) returns zurl_ok.
    # zstor is closed after this test.
    @func
    def assert_zurl_is_correct(zstor, zurl_ok):
        defer(zstor.close)
        assert zstor_2zurl(zstor) == zurl_ok

    # sslp is the ssl encryption uri part of an encrypted NEO node
    q = quote_plus
    sslp = ";".join(("%s=%s" % (q(k), q(v)) for k, v in sorted(neo_ssl_dict.items())))

    _ = assert_zurl_is_correct
    _(fs1("test.fs"),                         "file://%s/test.fs" % tmpdir)           # FileStorage
    _(zeo("1",     "/path/to/zeo.sock"),      "zeo:///path/to/zeo.sock")              # ZEO/unix
    _(zeo("test",  "/path/to/zeo.sock"),      "zeo:///path/to/zeo.sock?storage=test") #   + non-default storage name
    _(zeo("1",     ("127.0.0.1", 1234)),      "zeo://127.0.0.1:1234")                 # ZEO/ip4
    _(zeo("test",  ("127.0.0.1", 1234)),      "zeo://127.0.0.1:1234?storage=test")    #   + non-default storage name
    _(zeo("1",     ("::1",       1234)),      "zeo://[::1]:1234")                     # ZEO/ip6
    _(zeo("test",  ("::1",       1234)),      "zeo://[::1]:1234?storage=test")        #   + non-default storage name
    _(neo("test",  "127.0.0.1:1234"),         "neo://127.0.0.1:1234/test")            # NEO/ip4
    _(neo("test",  "127.0.0.1:1234", 1),      "neos://%s@127.0.0.1:1234/test" % sslp) #   + ssl
    _(neo("test",  "[::1]:1234"),             "neo://[::1]:1234/test")                # NEO/ip6
    _(neo("test",  "[::1]:1234", 1),          "neos://%s@[::1]:1234/test" % sslp)     #   + ssl
    _(neo("test",  "[::1]:1234\n[::2]:1234"), "neo://[::1]:1234,[::2]:1234/test")     #   + 2 master nodes
    _(demo(zeo("base", ("1.2.3.4",  5)),                                              # DemoStorage
           fs1("delta.fs")),                  "demo:(zeo://1.2.3.4:5?storage=base)/(file://%s/delta.fs)" % tmpdir)

    # Test exceptions
    #   invalid storage
    with raises(ValueError, match="in-RAM storages are not supported"):
        zstor_2zurl(MappingStorage())

    #   invalid object
    with raises(NotImplementedError):
        zstor_2zurl("I am not a storage.")


# neo_ssl_dict returns the path of precomputed static ssl certificate
# files.
@pytest.fixture
def neo_ssl_dict():
    ssl_files_base_path = "%s%s%s%s" % (
      os.path.dirname(__file__), os.sep, "testdata", os.sep
    )
    return {
        k: "%s%s" % (ssl_files_base_path, v)
        for k, v in dict(ca="ca.crt", key="node.key", cert="node.crt").items()
    }


# ---- tests for critical properties of ZODB ----

# verify race in between Connection.open and invalidations.
def test_zodb_zopenrace_basic():
    # exercises mostly logic inside ZODB around ZODB.Connection
    zopenrace.test(MappingStorage())
def test_zodb_zopenrace():
    # exercises ZODB.Connection + particular storage implementation
    zopenrace.main()

# verify race in between loading and invalidations.
def test_zodb_zloadrace():
    # skip testing with FileStorage - in ZODB/py opening simultaneous read-write
    # connections to the same file is not supported and will raise LockError.
    _ = testdb.getZODBStorage()
    _.close()
    if isinstance(_, FileStorage):
        pytest.skip("skipping on FileStorage")

    zloadrace.main()


# ---- misc ----


# zsync syncs ZODB storage.
# it is noop, if zstor does not support syncing (i.e. FileStorage has no .sync())
def zsync(zstor):
    # ZEOs default sync is effectless. We explicitly need to sync by
    # pinging to the server. For ZEO 5 it would actually be sufficient
    # to set init parameter 'server_sync' to 'True':
    #   https://github.com/zopefoundation/ZEO/blob/423cb8/src/ZEO/ClientStorage.py#L224-L246
    # But because our storage is already initiated this doesn't help.
    if isinstance(zstor, ZEOStorage):
        # ZEO >= 5 specifies ping
        #   https://github.com/zopefoundation/ZEO/blob/423cb8/src/ZEO/ClientStorage.py#L472-L478
        # ZEO < 5: we need to provide a ping method
        getattr(zstor, 'ping', lambda: zstor._server.lastTransaction())()
    sync = getattr(zstor, 'sync', None)
    if sync is not None:
        sync()


@pytest.mark.parametrize(
    "zurl,zurl_norm_ok",
    [
        # FileStorage
        ("file://Data.fs", "file://Data.fs"),
        # ZEO
        ("zeo://localhost:9001", "zeo://localhost:9001"),
        # NEO
        ("neo://127.0.0.1:1234/cluster", "neo://127.0.0.1:1234/cluster"),
        #   > 1 master nodes \w different order
        ("neo://abc:1,def:2/cluster", "neo://abc:1,def:2/cluster"),
        ("neo://def:2,abc:1/cluster", "neo://abc:1,def:2/cluster"),
        #   Different SSL paths
        ("neos://ca=a&key=b&cert=c@xyz:1/cluster", "neos://xyz:1/cluster"),
        ("neos://ca=α&key=β&cert=γ@xyz:1/cluster", "neos://xyz:1/cluster"),
    ],
)
def test_zurl_normalize(zurl, zurl_norm_ok):
    assert zurl_normalize(zurl) == zurl_norm_ok
    # also verify that zurl_normalize is stable
    assert zurl_normalize(zurl_norm_ok) == zurl_norm_ok


# 'zurl_normalize' must explicitly raise an exception if an unsupported
# zodburi scheme is used.
def test_zurl_normalize_invalid_scheme():
    for uri in "https://test postgres://a:b@c:5432/d".split(" "):
        with pytest.raises(NotImplementedError):
            zurl_normalize(uri)
