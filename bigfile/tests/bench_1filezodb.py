# Wendelin.bigfile | benchmarks for zodb backend
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

# TODO text
from wendelin.bigfile.file_zodb import ZBigFile
from wendelin.lib.mem import memset
from wendelin.lib.testing import getTestDB, Adler32, nulladler32_bysize, ffadler32_bysize
from wendelin.lib.zodb import dbclose
import transaction
from pygolang import defer, func

testdb = None
from wendelin.bigfile.tests.bench_0virtmem import filesize, blksize # to get comparable timings
blen = filesize // blksize
nulladler32 = nulladler32_bysize(blen * blksize)
ffadler32   = ffadler32_bysize(blen * blksize)


def setup_module():
    global testdb
    testdb = getTestDB()
    testdb.setup()

    root = testdb.dbopen()
    root['zfile'] = ZBigFile(blksize)
    transaction.commit()

    dbclose(root)


def teardown_module():
    testdb.teardown()


# NOTE runs before _writeff
def bench_bigz_readhole():  _bench_bigz_hash(Adler32,   nulladler32)

@func
def bench_bigz_writeff():
    root = testdb.dbopen()
    defer(lambda: dbclose(root))
    f   = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    memset(vma, 0xff)
    transaction.commit()

    del vma # TODO vma.close()
    del fh  # TODO fh.close()
    del f   # XXX  f.close() ?


@func
def _bench_bigz_hash(hasher, expect):
    root = testdb.dbopen()
    defer(lambda: dbclose(root))
    f   = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    h = hasher()
    h.update(vma)

    del vma # vma.close()
    del fh  # fh.close()
    del f   # f.close()
    assert h.digest() == expect


def bench_bigz_read():      _bench_bigz_hash(Adler32,   ffadler32)
