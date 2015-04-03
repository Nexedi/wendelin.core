# Wendelin.bigfile | common bits for ZODB-related tests
# TODO copyright/license

from ZODB.FileStorage import FileStorage
from ZODB import DB


# open stor/db/connection and return root obj
def dbopen(path):
    stor = FileStorage(path)
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
