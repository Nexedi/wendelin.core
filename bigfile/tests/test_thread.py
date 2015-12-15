# Wendelin.core.bigfile | Threading tests
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
from wendelin.bigfile import BigFile
from threading import Thread, Lock
from time import sleep

from wendelin.bigfile.tests.test_basic import bord_py3
from six.moves import _thread

# Notify channel for
#   - one thread to     .wait('condition'), until
#   - other thread does .tell('condition')
class NotifyChannel:

    def __init__(self):
        self.state = None

    def tell(self, condition):
        #print >>sys.stderr, '  tell %s\tthread_id: %s\n'    \
        #        % (condition, _thread.get_ident()),
        # wait until other thread reads previous tell
        while self.state is not None:
            pass

        self.state = condition

    def wait(self, condition):
        #print >>sys.stderr, '  wait %s\tthread_id: %s\n'    \
        #        % (condition, _thread.get_ident()),
        while self.state != condition:
            pass
        #print >>sys.stderr, '  have %s\tthread_id: %s\n'    \
        #        % (condition, _thread.get_ident()),
        self.state = None



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
#                       V   <- fileh_invalidate_page
def test_thread_lock_vs_virtmem_lock():
    Z = Lock()
    c12 = NotifyChannel()   # T1 -> T2
    c21 = NotifyChannel()   # T2 -> T1

    class ZLockBigFile(BigFile):
        def __new__(cls, blksize):
            obj = BigFile.__new__(cls, blksize)
            obj.cycle = 0
            return obj

        def loadblk(self, blk, buf):
            tell, wait = c12.tell, c21.wait

            # on the first cycle we synchronize with invalidate in T2
            if self.cycle == 0:
                tell('T1-V-under')
                wait('T2-Z-taken')

                # this will deadlock, if V is plain lock and calling from under-virtmem
                # is done with V held
                Z.acquire()
                Z.release()

            self.cycle += 1

    f   = ZLockBigFile(PS)
    fh  = f.fileh_open()
    vma = fh.mmap(0, 1)
    m   = memoryview(vma)

    def T1():
        m[0]    # calls ZLockBigFile.loadblk()


    def T2():
        tell, wait = c21.tell, c12.wait

        wait('T1-V-under')
        Z.acquire()
        tell('T2-Z-taken')

        fh.invalidate_page(0)
        Z.release()


    t1, t2 = Thread(target=T1), Thread(target=T2)
    t1.start(); t2.start()
    t1.join();  t2.join()


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

    def T():
        m[0]    # calls CountBigFile.loadblk()

    t1, t2 = Thread(target=T), Thread(target=T)
    t1.start(); t2.start()
    t1.join();  t2.join()
    assert d[0] == 1


# multiple access from several threads to different pages - blocks loaded in parallel
def test_thread_multiaccess_parallel():
    # tid -> (T0 -> T<tid>, T<tid> -> T0)
    channels = {}

    class SyncBigFile(BigFile):

        def loadblk(self, blk, buf):
            # tell driver we are in loadblk and wait untill it says us to go
            cin, cout = channels[_thread.get_ident()]
            cout.tell('ready')
            cin.wait('go')

    f   = SyncBigFile(PS)
    fh  = f.fileh_open()
    vma = fh.mmap(0, 2)
    m   = memoryview(vma)


    def T1():
        channels[_thread.get_ident()] = (NotifyChannel(), NotifyChannel())
        m[0*PS]

    def T2():
        channels[_thread.get_ident()] = (NotifyChannel(), NotifyChannel())
        m[1*PS]

    t1, t2 = Thread(target=T1), Thread(target=T2)
    t1.start(); t2.start()
    while len(channels) != 2:
        pass
    c01, c10 = channels[t1.ident]
    c02, c20 = channels[t2.ident]

    c10.wait('ready'); c20.wait('ready')
    c01.tell('go');    c02.tell('go')
    t1.join(); t2.join()


# loading vs invalidate in another thread
def test_thread_load_vs_invalidate():
    c12 = NotifyChannel()   # T1 -> T2
    c21 = NotifyChannel()   # T2 -> T1

    class RetryBigFile(BigFile):
        def __new__(cls, blksize):
            obj = BigFile.__new__(cls, blksize)
            obj.cycle = 0
            return obj

        def loadblk(self, blk, buf):
            tell, wait = c12.tell, c21.wait
            bufmem = memoryview(buf)

            # on the first cycle we synchronize with invalidate in T2
            if self.cycle == 0:
                tell('T1-loadblk0-ready')
                wait('T1-loadblk0-go')
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

    def T1():
        assert m[0] == bord_py3(b'2')

    def T2():
        tell, wait = c21.tell, c12.wait

        wait('T1-loadblk0-ready')
        fh.invalidate_page(0)
        tell('T1-loadblk0-go')

    t1, t2 = Thread(target=T1), Thread(target=T2)
    t1.start(); t2.start()
    t1.join();  t2.join()
