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
from threading import Thread
from time import sleep

from wendelin.bigfile.tests.test_basic import bchr_py2


# Synthetic bigfile that verifies there is no concurrent calls to loadblk
class XBigFile(BigFile):

    def __new__(cls, blksize):
        obj = BigFile.__new__(cls, blksize)
        obj.loadblk_counter = 0
        return obj

    def loadblk(self, blk, buf):
        assert self.loadblk_counter == 0

        # sleep with increased conter - to give chance for other threads to try
        # to load the same block and catch that
        self.loadblk_counter += 1
        sleep(3)

        # nothing to do - we just leave blk as is (all zeros)
        self.loadblk_counter -= 1
        assert self.loadblk_counter == 0


# XXX hack, hardcoded
MB = 1024*1024
PS = 2*MB

def test_thread_basic():
    f   = XBigFile(PS)
    fh  = f.fileh_open()
    vma = fh.mmap(0, 4)
    m   = memoryview(vma)

    # simple test that access vs access don't overlap
    # ( look for assert & sleep in XBigFile.loadblk() )
    Q   = []
    def access0():
        Q.append(m[0])

    t1 = Thread(target=access0)
    t2 = Thread(target=access0)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    assert Q == [bchr_py2(0), bchr_py2(0)]
