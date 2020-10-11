# Wendelin.core.bigfile | Threading tests
# Copyright (C) 2014-2021  Nexedi SA and Contributors.
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
from wendelin.bigfile import BigFile, WRITEOUT_STORE
from threading import Lock
from time import sleep

from wendelin.bigfile.tests.test_basic import bord_py3
from six.moves import _thread
from golang import chan, select
from golang import context, sync

# Notify channel for
#   - one thread to     .wait('condition')  for
#   - other thread to   .tell('condition')
class NotifyChannel:

    def __init__(self):
        self._ch = chan()

    def tell(self, ctx, condition):
        #print >>sys.stderr, '  tell %s\tthread_id: %s\n'    \
        #        % (condition, _thread.get_ident()),
        _, _rx = select(
            ctx.done().recv,            # 0
            (self._ch.send, condition), # 1
        )
        if _ == 0:
            raise ctx.err()

        #print >>sys.stderr, '  told %s\tthread_id: %s\n'     \
        #        % (condition, _thread.get_ident()),

    def wait(self, ctx, condition):
        #print >>sys.stderr, '  wait %s\tthread_id: %s\n'    \
        #        % (condition, _thread.get_ident()),
        _, _rx = select(
            ctx.done().recv,    # 0
            self._ch.recv,      # 1
        )
        if _ == 0:
            raise ctx.err()
        got = _rx
        if got != condition:
            raise RuntimeError('expected %s;  got %s' % (condition, got))

        #print >>sys.stderr, '  have %s\tthread_id: %s\n'    \
        #        % (condition, _thread.get_ident()),



# XXX hack, hardcoded
MB = 1024*1024
PS = 2*MB


# Test that it is possible to take a lock both:
#   - from-under virtmem, and
#   - on top of virtmem
# and not deadlock.
#
# ( this happens e.g. with ZEO:
#
#   T1                  T2
#
#   page-access         invalidation-from-server received
#   V -> loadblk
#                       Z   <- ClientStorage.invalidateTransaction()
#   Z -> zeo.load
#                       V   <- fileh_invalidate_page (possibly of unrelated page)
#
#   --------
#   and similarly for storeblk:
#
#   T1                  T2
#
#   commit              same as ^^^
#   V -> storeblk
#
#   Z -> zeo.store
def test_thread_lock_vs_virtmem_lock():
    Z = Lock()
    c12 = NotifyChannel()   # T1 -> T2
    c21 = NotifyChannel()   # T2 -> T1

    class ZLockBigFile(BigFile):
        # .t1ctx - set by T1
        def __new__(cls, blksize):
            obj = BigFile.__new__(cls, blksize)
            return obj

        def Zsync_and_lockunlock(self):
            tell, wait = c12.tell, c21.wait
            ctx = self.t1ctx

            # synchronize with invalidate in T2
            tell(ctx, 'T1-V-under')
            wait(ctx, 'T2-Z-taken')

            # this will deadlock, if V is plain lock and calling from under-virtmem
            # is done with V held
            Z.acquire()
            Z.release()

        def loadblk(self, blk, buf):
            self.Zsync_and_lockunlock()

        def storeblk(self, blk, buf):
            self.Zsync_and_lockunlock()

    f   = ZLockBigFile(PS)
    fh  = f.fileh_open()
    fh2 = f.fileh_open()
    vma = fh.mmap(0, 1)
    m   = memoryview(vma)

    def T1(ctx):
        f.t1ctx = ctx
        m[0]    # calls ZLockBigFile.loadblk()

        tell, wait = c12.tell, c21.wait
        wait(ctx, 'T2-Z-released: 0')
        m[0] = bord_py3(b'1')               # make page dirty
        fh.dirty_writeout(WRITEOUT_STORE)   # calls ZLockBigFile.storeblk()

        wait(ctx, 'T2-Z-released: 1')


    def T2(ctx):
        tell, wait = c21.tell, c12.wait

        # cycle 0: vs loadblk in T0
        # cycle 1: vs storeblk in T0
        for _ in range(2):
            wait(ctx, 'T1-V-under')
            Z.acquire()
            tell(ctx, 'T2-Z-taken')

            fh2.invalidate_page(0)  # NOTE invalidating page _not_ of fh
            Z.release()

            tell(ctx, 'T2-Z-released: %d' % _)

    wg = sync.WorkGroup(context.background())
    wg.go(T1)
    wg.go(T2)
    wg.wait()


# multiple access from several threads to the same page - block loaded only once
def test_thread_multiaccess_sameblk():
    d = {}  # blk -> #loadblk(blk)
    class CountBigFile(BigFile):

        def loadblk(self, blk, buf):
            d[blk] = d.get(blk, 0) + 1

            # make sure other threads has time and high probability to overlap
            # loadblk/loadblk
            sleep(1)

    f   = CountBigFile(PS)
    fh  = f.fileh_open()
    vma = fh.mmap(0, 1)
    m   = memoryview(vma)

    def T(ctx):
        m[0]    # calls CountBigFile.loadblk()

    wg = sync.WorkGroup(context.background())
    wg.go(T)
    wg.go(T)
    wg.wait()
    assert d[0] == 1


# multiple access from several threads to different pages - blocks loaded in parallel
def test_thread_multiaccess_parallel():
    # tid -> (T0 -> T<tid>, T<tid> -> T0)
    channels = {}
    # [0] = channels<T1>
    # [1] = channels<T2>
    channelv = [None, None]
    # tid -> ctx in T<tid>
    tidctx = {}

    class SyncBigFile(BigFile):

        def loadblk(self, blk, buf):
            # tell driver we are in loadblk and wait untill it says us to go
            tid = _thread.get_ident()
            ctx = tidctx[tid]
            cin, cout = channels[tid]
            cout.tell(ctx, 'ready')
            cin.wait(ctx, 'go')

    f   = SyncBigFile(PS)
    fh  = f.fileh_open()
    vma = fh.mmap(0, 2)
    m   = memoryview(vma)


    def T1(ctx):
        tid = _thread.get_ident()
        tidctx[tid] = ctx
        channelv[0] = channels[tid] = (NotifyChannel(), NotifyChannel())
        m[0*PS]

    def T2(ctx):
        tid = _thread.get_ident()
        tidctx[tid] = ctx
        channelv[1] = channels[tid] = (NotifyChannel(), NotifyChannel())
        m[1*PS]

    wg = sync.WorkGroup(context.background())
    wg.go(T1)
    wg.go(T2)
    while len(channels) != 2:
        pass
    c01, c10 = channelv[0]
    c02, c20 = channelv[1]

    def _(ctx):
        c10.wait(ctx, 'ready'); c20.wait(ctx, 'ready')
        c01.tell(ctx, 'go');    c02.tell(ctx, 'go')
    wg.go(_)

    wg.wait()


# loading vs invalidate of same page in another thread
def test_thread_load_vs_invalidate():
    c12 = NotifyChannel()   # T1 -> T2
    c21 = NotifyChannel()   # T2 -> T1
    tidctx = {}             # tid -> ctx used in T<n>

    class RetryBigFile(BigFile):
        def __new__(cls, blksize):
            obj = BigFile.__new__(cls, blksize)
            obj.cycle = 0
            return obj

        def loadblk(self, blk, buf):
            ctx = tidctx[_thread.get_ident()]
            tell, wait = c12.tell, c21.wait
            bufmem = memoryview(buf)

            # on the first cycle we synchronize with invalidate in T2
            if self.cycle == 0:
                tell(ctx, 'T1-loadblk0-ready')
                wait(ctx, 'T1-loadblk0-go')
                # here we know request to invalidate this page came in and this
                # '1' should be ignored by virtmem
                bufmem[0] = bord_py3(b'1')

            # this is code for consequent "after-invalidate" loadblk
            # '2' should be returned to clients
            else:
                bufmem[0] = bord_py3(b'2')

            self.cycle += 1


    f   = RetryBigFile(PS)
    fh  = f.fileh_open()
    vma = fh.mmap(0, 1)
    m   = memoryview(vma)

    def T1(ctx):
        tidctx[_thread.get_ident()] = ctx
        assert m[0] == bord_py3(b'2')

    def T2(ctx):
        tidctx[_thread.get_ident()] = ctx
        tell, wait = c21.tell, c12.wait

        wait(ctx, 'T1-loadblk0-ready')
        fh.invalidate_page(0)
        tell(ctx, 'T1-loadblk0-go')

    wg = sync.WorkGroup(context.background())
    wg.go(T1)
    wg.go(T2)
    wg.wait()
