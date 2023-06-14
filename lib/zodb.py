# -*- coding: utf-8 -*-
# Wendelin.bigfile | common ZODB-related helpers
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
"""Package wendelin.lib.zodb provides ZODB-related utilities."""

import ZODB
from ZODB.FileStorage import FileStorage
from ZODB.DemoStorage import DemoStorage
from ZODB import DB
from ZODB import POSException
from ZODB.utils import p64, u64
from persistent import Persistent
import zodbtools.util
from weakref import WeakSet
import gc
from six.moves.urllib import parse as urlparse
import socket

import pkg_resources


# open db storage by uri
def dbstoropen(uri):
    return zodbtools.util.storageFromURL(uri)

# open stor/db/connection and return root obj
def dbopen(uri):
    stor = dbstoropen(uri)
    db   = DB(stor)
    conn = db.open()
    root = conn.root()
    return root


# close db/connection/storage identified by root obj
def dbclose(root):
    conn = root._p_jar
    db   = conn.db()
    stor = db.storage

    conn.close()
    db.close()
    stor.close()


# LivePersistent is Persistent that never goes to ghost state, if it was ever uptodate.
#
# NOTE
#
# On invalidation LivePersistent still goes to ghost state, because
# invalidation cannot be ignored, i.e. they indicate the object has been
# changed externally.
#
# Invalidation can happen only at transaction boundary, so during the course of
# transaction LivePersistent is guaranteed to stay uptodate.
class LivePersistent(Persistent):
    # don't allow us to go to ghost
    #
    # NOTE we can't use STICKY as that state is assumed as
    # short-lived-temporary by ZODB and is changed back to UPTODATE by
    # persistent code. In fact ZODB says: STICKY is UPTODATE+keep in memory.
    def _p_deactivate(self):
        # just returning here won't allow Persistent._p_deactivate() run and
        # thus we'll stay in non-ghost state.
        return

    # NOTE _p_invalidate() is triggered on invalidations. We do not override it.



# deactivate a btree, including all internal buckets and leaf nodes
def deactivate_btree(btree):
    # first activate btree, to make sure its first bucket is loaded at all.
    #
    # we have to do this because btree could be automatically deactivated
    # before by cache (the usual way) and then in its ghost state it does not
    # contain pointer to first bucket and thus we won't be able to start
    # bucket deactivation traversal.
    btree._p_activate()

    for _ in gc.get_referents(btree):
        # for top-level btree we ignore any direct referent besides bucket
        # (there are _p_jar, cache, etc)
        if type(_) is btree._bucket_type:
            _deactivate_bucket(_)

    btree._p_deactivate()

def _deactivate_bucket(bucket):
    # TODO also support objects in keys, when we need it
    for obj in bucket.values():
        if type(obj) == type(bucket):
            _deactivate_bucket(obj)
        elif isinstance(obj, Persistent):
            obj._p_deactivate()

    bucket._p_deactivate()


# zconn_at returns tid as of which ZODB connection is viewing the database.
#
# In other words zconn_at returns database state corresponding to database view
# of the connection.
def zconn_at(zconn): # -> tid
    assert isinstance(zconn, ZODB.Connection.Connection)
    if zconn.opened is None: # zconn must be in "opened" state
        raise POSException.ConnectionStateError("database connection is closed")

    # ZODB5 uses MVCC uniformly
    #
    # zconn.db._storage always provides IMVCCStorage - either raw storage provides it,
    # or DB wraps raw storage with MVCCAdapter.
    #
    # MVCCAdapter in turn uses either MVCCAdapterInstance (current) or
    # HistoricalStorageAdapter, or UndoAdapterInstance. Retrieving zconn.at from those:
    #
    # MVCCAdapterInstance
    #     ._start
    #
    # HistoricalStorageAdapter
    #     ._before
    #
    # UndoAdapterInstance
    #     # no way to retrieve `at`, but .undo_instance() through which
    #     # UndoAdapterInstance is returned, is not used anywhere.
    #
    # For the reference: FileStorage, ZEO and NEO do not provide IMVCCStorage, thus
    # for them we can rely on MVCCAdapterInstance.
    #
    # RelStorage is IMVCCStorage - TODO: how to extract at.
    if zmajor >= 5:
        zstor = zconn._storage
        if isinstance(zstor, ZODB.mvccadapter.MVCCAdapterInstance):
            return before2at(zstor._start)

        if isinstance(zstor, ZODB.mvccadapter.HistoricalStorageAdapter):
            return before2at(zstor._before)

        raise AssertionError("zconn_at: TODO: add support for zstor %r" % zstor)

    # ZODB4
    #
    # We rely on our patch in 4-nxd branch that reworks ZODB.Connection to
    # implement MVCC via always calling loadBefore(zconn._txn_time) to load objects.
    elif zmajor == 4:
        _zassertHasNXDPatch('conn:MVCC-via-loadBefore-only',
            "https://lab.nexedi.com/nexedi/ZODB/merge_requests/1")

        if zconn._mvcc_storage:
            raise NotImplementedError("Connection.at for IMVCCStorage is not implemented")
        if zconn.before is not None:
            return before2at(zconn.before)      # historical connection
        else:
            return before2at(zconn._txn_time)   # "current" connection

    # ZODB3
    else:
        raise AssertionError("zconn_at: ZODB3 is not supported anymore")


# before2at converts tid that specifies database state as "before" into tid that
# specifies database state as "at".
def before2at(before): # -> at
    return p64(u64(before) - 1)


# _zassertHasNXDPatch asserts that ZODB is patched with specified Nexedi-provided patch.
def _zassertHasNXDPatch(patch, details_link):
    if not _zhasNXDPatch(patch):
        raise AssertionError(
            "ZODB%s is not patched with required Nexedi patch %r\n\tSee %s for details" %
            (zmajor, patch, details_link))

# _zhasNXDPatch returns whether ZODB is patched with specified Nexedi-provided patch.
def _zhasNXDPatch(patch):
    nxd_patches = getattr(ZODB, 'nxd_patches', set())
    return (patch in nxd_patches)


# _zversion returns ZODB version object
def _zversion():
    dzodb3 = pkg_resources.working_set.find(pkg_resources.Requirement.parse('ZODB3'))
    dzodb  = pkg_resources.working_set.find(pkg_resources.Requirement.parse('ZODB'))
    v311   = pkg_resources.parse_version('3.11dev')

    if dzodb3 is None and dzodb is None:
        raise RuntimeError('ZODB is not installed')

    if dzodb3 is not None:
        if dzodb3.parsed_version >= v311:
            vzodb = dzodb.parsed_version # ZODB 3.11 just requires latest ZODB & ZEO
        else:
            vzodb = dzodb3.parsed_version
    else:
        vzodb = dzodb.parsed_version

    assert vzodb is not None
    return vzodb

# _zmajor returns major ZODB version.
def _zmajor():
    vzodb = _zversion()
    # XXX hack - packaging.version.Version provides no way to extract major?
    return int(vzodb.public.split('.')[0])  # 3.11.dev0 -> 3

# zmajor is set to major ZODB version.
zmajor = _zmajor()


# patch for ZODB.Connection to support callback on .open()
# NOTE on-open  callbacks are setup once and fire many times on every open
#      on-close callbacks are setup once and fire only once on next close
ZODB.Connection.Connection._onOpenCallbacks = None
def Connection_onOpenCallback(self, f):
    if self._onOpenCallbacks is None:
        # NOTE WeakSet does not work for bound methods - they are always created
        # anew for each obj.method access, and thus will go away almost immediately
        self._onOpenCallbacks = WeakSet()
    self._onOpenCallbacks.add(f)

assert not hasattr(ZODB.Connection.Connection, 'onOpenCallback')
ZODB.Connection.Connection.onOpenCallback = Connection_onOpenCallback

_orig_Connection_open = ZODB.Connection.Connection.open
def Connection_open(self, transaction_manager=None, delegate=True):
    _orig_Connection_open(self, transaction_manager, delegate)

    # FIXME method name hardcoded. Better not do it and allow f to be general
    # callable, but that does not work with bound method - see above.
    # ( Something like WeakMethod from py3 could help )
    if self._onOpenCallbacks:
        for f in self._onOpenCallbacks:
            f.on_connection_open()

ZODB.Connection.Connection.open = Connection_open


# patch for ZODB.Connection to support callback on after database view is changed
ZODB.Connection.Connection._onResyncCallbacks = None
def Connection_onResyncCallback(self, f):
    if zmajor <= 3:
        raise AssertionError("onResyncCallback: ZODB3 is not supported anymore")
    if self._onResyncCallbacks is None:
        # NOTE WeakSet does not work for bound methods - they are always created
        # anew for each obj.method access, and thus will go away almost immediately
        self._onResyncCallbacks = WeakSet()
    self._onResyncCallbacks.add(f)

assert not hasattr(ZODB.Connection.Connection, 'onResyncCallback')
ZODB.Connection.Connection.onResyncCallback = Connection_onResyncCallback

def Connection__callResyncCallbacks(self):
    # FIXME method name hardcoded. Better not do it and allow f to be general
    # callable, but that does not work with bound method - see above.
    # ( Something like WeakMethod from py3 could help )
    if self._onResyncCallbacks:
        for f in self._onResyncCallbacks:
            f.on_connection_resync()

assert not hasattr(ZODB.Connection.Connection, '_callResyncCallbacks')
ZODB.Connection.Connection._callResyncCallbacks = Connection__callResyncCallbacks

# ZODB5: hook into Connection.newTransaction
if zmajor >= 5:
    _orig_Connection_newTransaction = ZODB.Connection.Connection.newTransaction
    def _ZConnection_newTransaction(self, transaction, sync=True):
        _orig_Connection_newTransaction(self, transaction, sync)
        self._callResyncCallbacks()

    ZODB.Connection.Connection.newTransaction = _ZConnection_newTransaction


# ZODB4: hook into Connection._flush_invalidations and ._resetCache
elif zmajor == 4:
    # ._flush_invalidations is called by both Connection.open and Connection._storage_sync
    # however Connection.open might call ._resetCache instead if global reset is requested
    _orig_Connection__flush_invalidations = ZODB.Connection.Connection._flush_invalidations
    _orig_Connection__resetCache          = ZODB.Connection.Connection._resetCache

    def _ZConnection__flush_invalidations(self):
        _orig_Connection__flush_invalidations(self)
        self._callResyncCallbacks()
    def _ZConnection__resetCache(self):
        _orig_Connection__resetCache(self)
        self._callResyncCallbacks()

    ZODB.Connection.Connection._flush_invalidations = _ZConnection__flush_invalidations
    ZODB.Connection.Connection._resetCache          = _ZConnection__resetCache


# ZODB3
else:
    raise AssertionError("ZODB3 is not supported anymore")


# patch for ZODB.Connection to support callback on after database is closed
ZODB.Connection.Connection._onShutdownCallbacks = None
def Connection_onShutdownCallback(self, f):
    if self._onShutdownCallbacks is None:
        # NOTE WeakSet does not work for bound methods - they are always created
        # anew for each obj.method access, and thus will go away almost immediately
        self._onShutdownCallbacks = WeakSet()
    self._onShutdownCallbacks.add(f)

assert not hasattr(ZODB.Connection.Connection, 'onShutdownCallback')
ZODB.Connection.Connection.onShutdownCallback = Connection_onShutdownCallback

_orig_DB_close = ZODB.DB.close
def _ZDB_close(self):
    # the same code for ZODB3/4/5
    @self._connectionMap
    def _(conn):
        if conn._onShutdownCallbacks:
            for f in conn._onShutdownCallbacks:
                f.on_connection_shutdown()
    _orig_DB_close(self)

ZODB.DB.close = _ZDB_close


# zstor_2zurl converts a ZODB storage to URL to access it.
def zstor_2zurl(zstor):
    # There is, sadly, no unified way to do it, as even if storages are created via
    # zodburi, after creation its uri is lost. And storages could be created not
    # only through URI but e.g. via ZConfig and manually. We want to support all
    # those cases...
    #
    # For this reason extract URL with important for wcfs use-case parameters in
    # ad-hoc way.
    if isinstance(zstor, FileStorage):
        return "file://%s" % (zstor._file_name,)

    if isinstance(zstor, DemoStorage):
        # demo:(base_zurl)/(δ_zurl)
        return "demo:(%s)/(%s)" % (zstor_2zurl(zstor.base), zstor_2zurl(zstor.changes))

    ztype = type(zstor).__module__ + "." + type(zstor).__name__

    # ZEO
    if ztype == "ZEO.ClientStorage.ClientStorage":
        # zeo://(path|host:port)?storage=<storageID>
        u = "zeo://"
        _addr = zstor._addr
        addr  = None
        # ZEO4/3 for backward compatibility accept either a single "address" or single ("host", port) pair
        # https://github.com/zopefoundation/ZEO/blob/4.2.0b1-49-g47d3fbe8/src/ZEO/zrpc/client.py#L179
        if zmajor <= 4:
            if isinstance(_addr, str):
                addr = _addr
            elif len(_addr) == 2 and isinstance(_addr[0], str) and isinstance(_addr[1], int):
                addr = _addr
        if addr is None:
            if len(_addr) != 1:
                raise NotImplementedError("ZEO client has multiple configured servers: %r" % (addr,))
            addr = _addr[0]
        # addr is either TCP (host,port) or UNIX path
        if isinstance(addr, str):
            u += addr
        else:
            host, port = addr
            if _is_ipv6(host):
              host = "[%s]" % host
            u += '%s:%d' % (host, port)

        storage = zstor._storage
        if storage != "1":
            u += "?storage=%s" % storage

        # TODO ssl
        return u

    # NEO
    if ztype == "neo.client.Storage.Storage":
        # neo(s)://[<credentials>@]<master>/<cluster>
        app = zstor.app
        if not app.ssl:
            u = "neo://"
        else:
            q = urlparse.quote_plus
            u = "neos://"
            ca, cert, key = app.ssl_credentials # .ssl_credentials depend on kirr's patch
            u += "ca=%s;cert=%s;key=%s@" % (q(ca), q(cert), q(key))

        masterv = app.nm.getMasterList()
        if len(masterv) == 0:
            raise RuntimeError("%r has empty master list" % zstor)

        master_list = []
        for master in masterv:
            host, port = master.getAddress()

            if _is_ipv6(host):
                host = "[%s]" % host

            master_list.append("%s:%s" % (host, port))

        u += ",".join(master_list)
        u += "/%s" % app.name

        return u

    # MappingStorage:
    if ztype == "ZODB.MappingStorage.MappingStorage":
        raise ValueError(("%s is in-RAM storage\n"  +
            "\tin-RAM storages are not supported:\n" +
            "\ta zurl pointing to in-RAM storage in one process would lead to\n"
            "\tanother in-RAM storage in WCFS process.") % ztype)

    raise NotImplementedError("don't know how to extract zurl from %r" % zstor)


def _is_ipv6(host):
    try:
        socket.inet_pton(socket.AF_INET6, host)
    except socket.error:
        return False
    return True
