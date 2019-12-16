# Wendelin.bigfile | common ZODB-related helpers
# Copyright (C) 2014-2019  Nexedi SA and Contributors.
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

from ZODB.FileStorage import FileStorage
from ZODB import DB
from persistent import Persistent
import gc


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
