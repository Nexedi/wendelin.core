# Wendeling.core.bigfile | Tests for ZODB BigFile backend
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
from wendelin.bigfile.file_zodb import LivePersistent, ZBigFile
from wendelin.bigfile.tests.common_zodb import dbopen as z_dbopen, dbclose
from wendelin.bigfile import ram_reclaim
from persistent import UPTODATE, GHOST
import transaction
from tempfile import mkdtemp
from shutil import rmtree
from numpy import ndarray, array_equal, uint8, zeros

from pytest import raises
from six.moves import range as xrange


tmpd = None
blksize  = 2*1024*1024   # XXX hardcoded
blen     = 32            # 32*2 = 64MB      # TODO set it higher by default ?


def dbopen():
    return z_dbopen('%s/1.fs' % tmpd)

def setup_module():
    global tmpd
    tmpd = mkdtemp('', 'bigzodb.')

def teardown_module():
    rmtree(tmpd)



# like db.cacheDetail(), but {} instead of []
def cacheInfo(db):
    return dict(db.cacheDetail())

# key for cacheInfo() result
def kkey(klass):
    return '%s.%s' % (klass.__module__, klass.__name__)

def test_livepersistent():
    root = dbopen()
    transaction.commit()    # set root._p_jar
    db = root._p_jar.db()

    # ~~~ test `obj initially created` case
    root['live'] = lp = LivePersistent()
    assert lp._p_jar   is None          # connection does not know about it yet
    assert lp._p_state == UPTODATE      # object initially created in uptodate

    # should not be in cache yet & thus should stay after gc
    db.cacheMinimize()
    assert lp._p_jar   is None
    assert lp._p_state == UPTODATE
    ci = cacheInfo(db)
    assert kkey(LivePersistent) not in ci

    # should be registered to connection & cache after commit
    transaction.commit()
    assert lp._p_jar   is not None
    assert lp._p_state == UPTODATE
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1

    # should stay that way after cache gc
    db.cacheMinimize()
    assert lp._p_jar   is not None
    assert lp._p_state == UPTODATE
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1


    # ~~~ reopen & test `obj loaded from db` case
    dbclose(root)
    del root, db, lp

    root = dbopen()
    db = root._p_jar.db()

    # known to connection & cache & UPTODATE (ZODB < 3.10) or GHOST (ZODB >= 3.10)
    # right after first loading from DB
    lp = root['live']
    assert lp._p_jar   is not None
    assert lp._p_state in (UPTODATE, GHOST)
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1

    # should be UPTODATE for sure after read access
    getattr(lp, 'attr', None)
    assert lp._p_jar   is not None
    assert lp._p_state is UPTODATE
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1

    # does not go back to ghost on cache gc
    db.cacheMinimize()
    assert lp._p_jar   is not None
    assert lp._p_state == UPTODATE
    ci = cacheInfo(db)
    assert ci[kkey(LivePersistent)] == 1

    # ok
    dbclose(root)


# i'th memory block as u8 ndarray
def Blk(vma, i):
    return ndarray(blksize, offset=i*blksize, buffer=vma, dtype=uint8)

def test_bigfile_filezodb():
    root = dbopen()
    root['zfile'] = f = ZBigFile(blksize)
    transaction.commit()

    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    # verify that empty file reads as all zeros
    data0 = zeros(blksize, dtype=uint8)
    for i in xrange(blen):
        assert array_equal(data0, Blk(vma, i))

    # dirty data
    for i in xrange(blen):
        Blk(vma, i)[0] = i

    # verify that the changes are lost after abort
    transaction.abort()
    for i in xrange(blen):
        assert array_equal(data0, Blk(vma, i))


    # dirty & abort once again
    # (verifies that ZBigFile data manager re-registers with transaction)
    for i in xrange(blen):
        Blk(vma, i)[0] = i

    transaction.abort()
    for i in xrange(blen):
        assert array_equal(data0, Blk(vma, i))


    # dirty data & commit
    for i in xrange(blen):
        Blk(vma, i)[0] = i

    transaction.commit()


    # close DB and reopen everything
    # vma.unmap()
    del vma
    #fh.close()
    del fh

    dbclose(root)
    del root

    root = dbopen()
    f = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    # verify data as re-loaded
    for i in xrange(blen):
        assert Blk(vma, i)[0] == i


    # evict all loaded pages and test loading them again
    # (verifies ZBlk.loadblkdata() & loadblk logic when loading data the second time)
    reclaimed = 0
    while 1:
        n = ram_reclaim()   # TODO + ram
        if n == 0:
            break
        reclaimed += n
    assert reclaimed >= blen    # XXX assumes pagesize=blksize

    for i in xrange(blen):
        assert Blk(vma, i)[0] == i

    # dirty once again & commit
    # (verified ZBlk.__setstate__() & storeblk logic when storing data the second time)
    for i in xrange(blen):
        Blk(vma, i)[0] = i+1

    transaction.commit()


    # close DB and reopen everything
    del vma
    del fh
    dbclose(root)
    del root

    root = dbopen()
    f = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    # verify data as re-loaded
    for i in xrange(blen):
        assert Blk(vma, i)[0] == i+1


    # ZBigFile should survive Persistent cache clearing and not go to ghost
    # state (else logic to propagate changes from pages to objects would subtly
    # brake after Persistent cache gc)
    db = root._p_jar.db()
    ci = cacheInfo(db)
    assert ci[kkey(ZBigFile)] == 1
    assert f._p_state == UPTODATE
    db.cacheMinimize()
    ci = cacheInfo(db)
    assert ci[kkey(ZBigFile)] == 1
    assert f._p_state == UPTODATE   # it would be GHOST without LivePersistent protection

    # verify that data changes propagation continue to work
    assert Blk(vma, 0)[0] == 1
    Blk(vma, 0)[0] = 99
    transaction.commit()

    del vma
    del fh
    dbclose(root)
    del db, root

    root = dbopen()
    f = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    # verify data as re-loaded
    assert Blk(vma, 0)[0] == 99
    for i in xrange(1, blen):
        assert Blk(vma, i)[0] == i+1


    dbclose(root)
