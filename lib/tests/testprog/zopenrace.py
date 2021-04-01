#!/usr/bin/env python
# originally reported to https://github.com/zopefoundation/ZODB/issues/290
# fixed               in https://github.com/zopefoundation/ZODB/commit/b5895a5c
"""Program zopenrace.py demonstrates concurrency bug in ZODB Connection.open()
that leads to stale live cache and wrong data provided by database to users.

The bug is that when a connection is opened, it syncs to storage and processes
invalidations received from the storage in two _separate_ steps, potentially
leading to situation where invalidations for transactions _past_ opened
connection's view of the database are included into opened connection's cache
invalidation. This leads to stale connection cache and old data provided by
ZODB.Connection when it is reopened next time.

That in turn can lead to loose of Consistency of the database if mix of current
and old data is used to process a transaction. A classic example would be bank
accounts A, B and C with A<-B and A<-C transfer transactions. If transaction
that handles A<-C sees stale data for A when starting its processing, it
results in A loosing what it should have received from B.

Below is timing diagram on how the bug happens on ZODB5:

    Client1 or Thread1                                  Client2 or Thread2

    # T1 begins transaction and opens zodb connection
    newTransaction():
        # implementation in Connection.py[1]
        ._storage.sync()
        invalidated = ._storage.poll_invalidations():
            # implementation in MVCCAdapterInstance [2]

            # T1 settles on as of which particular database state it will be
            # viewing the database.
            ._storage._start = ._storage._storage.lastTrasaction() + 1:
                s = ._storage._storage
                s._lock.acquire()
                head = s._ltid
                s._lock.release()
                return head
                                                        # T2 commits here.
                                                        # Time goes by and storage server sends
                                                        # corresponding invalidation message to T1,
                                                        # which T1 queues in its _storage._invalidations

            # T1 retrieves queued invalidations which _includes_
            # invalidation for transaction that T2 just has committed past @head.
            ._storage._lock.acquire()
                r = _storage._invalidations
            ._storage._lock.release()
            return r

        # T1 processes invalidations for [... head] _and_ invalidations for past-@head transaction.
        # T1 thus will _not_ process invalidations for that next transaction when
        # opening zconn _next_ time. The next opened zconn will thus see _stale_ data.
        ._cache.invalidate(invalidated)


The program simulates two clients: one (T2) constantly modifies two integer
objects preserving invariant that their values stay equal. The other client
(T1) constantly opens the database and verifies the invariant. T1 forces access
to one of the object to always go through loading from the database, and this
way if live cache becomes stale the bug is observed as invariant breakage.

Here is example failure:

    $ taskset -c 1,2 ./zopenrace.py
    Exception in thread Thread-1:
    Traceback (most recent call last):
      File "/usr/lib/python2.7/threading.py", line 801, in __bootstrap_inner
        self.run()
      File "/usr/lib/python2.7/threading.py", line 754, in run
        self.__target(*self.__args, **self.__kwargs)
      File "./zopenrace.py", line 136, in T1
        t1()
      File "./zopenrace.py", line 130, in t1
        raise AssertionError("t1: obj1.i (%d)  !=  obj2.i (%d)" % (i1, i2))
    AssertionError: t1: obj1.i (147)  !=  obj2.i (146)

    Traceback (most recent call last):
      File "./zopenrace.py", line 179, in <module>
        main()
      File "./zopenrace.py", line 174, in main
        raise AssertionError('FAIL')
    AssertionError: FAIL

NOTE ZODB4 and ZODB3 do not have this particular open vs invalidation race.

[1] https://github.com/zopefoundation/ZODB/blob/5.5.1-29-g0b3db5aee/src/ZODB/Connection.py#L734-L742
[2] https://github.com/zopefoundation/ZODB/blob/5.5.1-29-g0b3db5aee/src/ZODB/mvccadapter.py#L130-L139
"""

from __future__ import print_function
from ZODB import DB
import transaction
from persistent import Persistent

from wendelin.lib.testing import getTestDB

from golang import func, defer, select, default
from golang import context, sync


# PInt is persistent integer.
class PInt(Persistent):
    def __init__(self, i):
        self.i = i

@func
def main():
    tdb = getTestDB()
    tdb.setup()
    defer(tdb.teardown)

    zstor = tdb.getZODBStorage()
    defer(zstor.close)
    test(zstor)

@func
def test(zstor):
    db = DB(zstor)
    defer(db.close)

    # init initializes the database with two integer objects - obj1/obj2 that are set to 0.
    def init():
        transaction.begin()
        zconn = db.open()

        root = zconn.root()
        root['obj1'] = PInt(0)
        root['obj2'] = PInt(0)

        transaction.commit()
        zconn.close()


    # T1 accesses obj1/obj2 in a loop and verifies that obj1.i == obj2.i
    #
    # access to obj1 is organized to always trigger loading from zstor.
    # access to obj2 goes through zconn cache and so verifies whether the cache is not stale.
    def T1(ctx, N):
        def t1():
            transaction.begin()
            zconn = db.open()

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
                raise AssertionError("T1: obj1.i (%d)  !=  obj2.i (%d)" % (i1, i2))

            transaction.abort() # we did not changed anything; also fails with commit
            zconn.close()

        for i in range(N):
            #print('T1.%d' % i)
            if ready(ctx.done()):
                raise ctx.err()
            t1()


    # T2 changes obj1/obj2 in a loop by doing `objX.i += 1`.
    #
    # Since both objects start from 0, the invariant that `obj1.i == obj2.i` is always preserved.
    def T2(ctx, N):
        def t2():
            transaction.begin()
            zconn = db.open()

            root = zconn.root()
            obj1 = root['obj1']
            obj2 = root['obj2']
            obj1.i += 1
            obj2.i += 1
            assert obj1.i == obj2.i

            transaction.commit()
            zconn.close()

        for i in range(N):
            #print('T2.%d' % i)
            if ready(ctx.done()):
                raise ctx.err()
            t2()


    # run T1 and T2 concurrently. As of 20191210, due to race condition in
    # Connection.open, it triggers the bug where T1 sees stale obj2 with obj1.i != obj2.i
    init()

    N = 1000
    wg = sync.WorkGroup(context.background())
    wg.go(T1, N)
    wg.go(T2, N)
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
