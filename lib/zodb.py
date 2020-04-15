# Wendelin.bigfile | common ZODB-related helpers
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
"""Package wendelin.lib.zodb provides ZODB-related utilities."""

import ZODB
from ZODB.FileStorage import FileStorage
from ZODB import DB
from persistent import Persistent
from weakref import WeakSet
import gc

import pkg_resources


# open db storage by uri
def dbstoropen(uri):
    # if we can - use zodbtools to open via zodburi
    try:
        import zodbtools.util
    except ImportError:
        return _dbstoropen(uri)

    return zodbtools.util.storageFromURL(uri)


# simplified fallback to open a storage by URI when zodbtools/zodburi are not available.
# ( they require ZODB, instead of ZODB3, and thus we cannot use
#   them together with ZODB 3.10 which we still support )
def _dbstoropen(uri):
    if uri.startswith('neo://'):
        # XXX hacky, only 1 master supported
        from neo.client.Storage import Storage
        name, master = uri[6:].split('@', 1)    # neo://db@master -> db, master
        stor = Storage(master_nodes=master, name=name)

    elif uri.startswith('zeo://'):
        # XXX hacky
        from ZEO.ClientStorage import ClientStorage
        host, port = uri[6:].split(':',1)       # zeo://host:port -> host, port
        port = int(port)
        stor = ClientStorage((host, port))

    else:
        stor = FileStorage(uri)

    return stor


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
