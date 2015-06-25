# Wendelin.bigfile | common ZODB-related helpers
# Copyright (C) 2014-2015  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Open Source Initiative approved licenses and Convey
# the resulting work. Corresponding source of such a combination shall include
# the source code for all other software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.

from ZODB.FileStorage import FileStorage
from ZODB import DB


# open db storage by uri
def dbstoropen(uri):
    # TODO better use repoze.zodbconn or zodburi
    #      ( but they require ZODB, instead of ZODB3, and thus we cannot use
    #        them together with ZODB 3.10 which we still support )
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
