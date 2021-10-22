#!/usr/bin/env python
# originally reported to https://github.com/zopefoundation/ZEO/issues/155
"""Program zloadrace.py demonstrates concurrency bug in ZODB5/ZEO5 that leads
to data corruption.

The bug was not fully analyzed, but offhand it looks like ZEO5 does not
properly synchronize on-client loads with invalidations which lead to stale
live cache in ZODB connection and corrupt data provided by ZODB to application
similarly to https://github.com/zopefoundation/ZODB/issues/290.

The program simulates eight clients: every client once in a while modifies two
integer objects preserving invariant that their values stay equal. At every
iteration each client also verifies the invariant with access to one of the
objects always going through loading from the database. This way if live cache
becomes stale the bug is observed as invariant breakage.

Here is example failure:

    $ ./zloadrace5.py
    No handlers could be found for logger "ZEO.asyncio.server"
    Traceback (most recent call last):
      File "./zloadrace5.py", line 139, in <module>
        main()
      File "</home/kirr/src/tools/py/decorator/src/decorator.pyc:decorator-gen-1>", line 2, in main
      File "/home/kirr/src/tools/go/pygolang/golang/__init__.py", line 100, in _
        return f(*argv, **kw)
      File "./zloadrace5.py", line 123, in main
        wg.wait()
      File "golang/_sync.pyx", line 198, in golang._sync.PyWorkGroup.wait
        pyerr_reraise(pyerr)
      File "golang/_sync.pyx", line 178, in golang._sync.PyWorkGroup.go.pyrunf
        f(pywg._pyctx, *argv, **kw)
      File "</home/kirr/src/tools/py/decorator/src/decorator.pyc:decorator-gen-3>", line 2, in T
      File "/home/kirr/src/tools/go/pygolang/golang/__init__.py", line 100, in _
        return f(*argv, **kw)
      File "./zloadrace5.py", line 112, in T
        t1()
      File "</home/kirr/src/tools/py/decorator/src/decorator.pyc:decorator-gen-8>", line 2, in t1
      File "/home/kirr/src/tools/go/pygolang/golang/__init__.py", line 100, in _
        return f(*argv, **kw)
      File "./zloadrace5.py", line 94, in t1
        raise AssertionError("T%s: obj1.i (%d)  !=  obj2.i (%d)" % (name, i1, i2))
    AssertionError: T7: obj1.i (1)  !=  obj2.i (0)

--------

NOTE ZODB4/ZEO4 do not expose this **load vs invalidation** race because there
the issue was specially cared about. Here is relevant part of
`Connection._setstate()` on ZODB4:

https://github.com/zopefoundation/ZODB/blob/4.4.5-4-g7a1a49111/src/ZODB/Connection.py#L949-L964

Basically it is

1. check if oid was already invalidated and, if yes, load from storage with
   `before=._txn_time` i.e. transaction ID of first invalidated transaction this
   connection received after its transaction begin;
2. if oid was not invalidated - call `zstor.load(oid)` with **before=None** -
   i.e. load latest data for that oid;
3. check again after load whether oid was invalidated, and if yes - reload data
   again with `before=._txn_time`.

One can suppose that there can be a race window in that a new transaction was
committed before load in 2, but corresponding invalidation messages were not
yet sent by server, or not yet processed on client. If any of that were true,
it would result in breakage of Isolation property so that in-progress
transaction would observe changes from transactions going simultaneously to it.

After some analysis it looks like it should not be the case:

ZEO4 server explicitly guarantees that it does not mix processing load requests
inside tpc_finish + send invalidations. This way if load is processed after new
commit, load reply is guaranteed to come to client after invalidation message.
This was explicitly fixed by

https://github.com/zopefoundation/ZEO/commit/71eb1456 (search for callAsyncNoPoll there)

and later again by https://github.com/zopefoundation/ZEO/commit/94f275c3.

HOWEVER

ZODB5 shifts MVCC handling into storage layer and this way there is no 1-2-3
from the above in `Connection.setstate()`. ZEO5 server was also significantly
reworked compared to ZEO4 and it looks like that rework reintroduced some
concurrency bugs that lead to corrupt data.
"""

from __future__ import print_function
from ZODB import DB
from ZODB.POSException import ConflictError
import transaction
from persistent import Persistent
from random import randint

from wendelin.lib.testing import getTestDB

from golang import func, defer, select, default
from golang import sync, context


# PInt is persistent integer.
class PInt(Persistent):
    def __init__(self, i):
        self.i = i


@func
def main():
    tdb = getTestDB()
    tdb.setup()
    defer(tdb.teardown)

    def dbopen():
        zstor = tdb.getZODBStorage()
        db = DB(zstor)
        return db

    # init initializes the database with two integer objects - obj1/obj2 that are set to 0.
    @func
    def init():
        db = dbopen()
        defer(db.close)

        transaction.begin()
        zconn = db.open()

        root = zconn.root()
        root['obj1'] = PInt(0)
        root['obj2'] = PInt(0)

        transaction.commit()
        zconn.close()


    # T is a worker that accesses obj1/obj2 in a loop and verifies
    # `obj1.i == obj2.i` invariant.
    #
    # access to obj1 is organized to always trigger loading from zstor.
    # access to obj2 goes through zconn cache and so verifies whether the cache is not stale.
    #
    # Once in a while T tries to modify obj{1,2}.i maintaining the invariant as
    # test source of changes for other workers.
    @func
    def T(ctx, name, N):
        db = dbopen()
        defer(db.close)

        @func
        def t1():
            transaction.begin()
            zconn = db.open()
            defer(zconn.close)

            root = zconn.root()
            obj1 = root['obj1']
            obj2 = root['obj2']

            # obj1 - reload it from zstor
            # obj2 - get it from zconn cache
            obj1._p_invalidate()

            # both objects must have the same values
            i1 = obj1.i
            i2 = obj2.i
            if i1 != i2:
                #print('FAIL')
                raise AssertionError("T%s: obj1.i (%d)  !=  obj2.i (%d)" % (name, i1, i2))

            # change objects once in a while
            if randint(0,4) == 0:
                #print("T%s: modify" % name)
                obj1.i += 1
                obj2.i += 1

            try:
                transaction.commit()
            except ConflictError:
                #print('conflict -> ignore')
                transaction.abort()

        for i in range(N):
            if ready(ctx.done()):
                break
            #print('T%s.%d' % (name, i))
            t1()


    # run 8 T workers concurrently. As of 20200123, likely due to race conditions
    # in ZEO, it triggers the bug where T sees stale obj2 with obj1.i != obj2.i
    init()

    N = 1000
    wg = sync.WorkGroup(context.background())
    for x in range(8):
        wg.go(T, x, N)
    wg.wait()

    print('OK')


# ready returns whether channel ch is ready.
def ready(ch):
    _, _rx = select(
        default,        # 0
        ch.recv,        # 1
    )
    if _ == 0:
        return False
    return True

if __name__ == '__main__':
    main()
