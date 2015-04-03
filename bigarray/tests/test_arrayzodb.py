# Wendeling.core.bigarray | Tests for ZBigArray
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
from wendelin.bigarray.array_zodb import ZBigArray
from wendelin.bigfile.tests.common_zodb import dbopen, dbclose
from wendelin.bigfile.tests.test_filezodb import kkey, cacheInfo
from persistent import UPTODATE
import transaction
from numpy import dtype, uint8, all

def test_zbigarray(tmpdir):
    root = dbopen('%s/1.fs' % tmpdir)
    root['zarray'] = ZBigArray((16*1024*1024,), uint8)
    transaction.commit()

    dbclose(root)
    del root


    root = dbopen('%s/1.fs' % tmpdir)
    A = root['zarray']

    assert isinstance(A, ZBigArray)
    assert A.shape  == (16*1024*1024,)
    assert A.dtype  == dtype(uint8)

    assert all(A[:] == 0)

    a = A[:]
    a[1] = 1
    a[3] = 3
    a[5] = 5
    a[-1] = 99

    b = A[:]
    assert (b[0],b[1]) == (0,1)
    assert (b[2],b[3]) == (0,3)
    assert (b[4],b[5]) == (0,5)
    assert all(b[6:-1] == 0)
    assert b[-1] == 99

    # abort - should forget all changes
    transaction.abort()
    assert all(a[:] == 0)
    assert all(b[:] == 0)
    assert all(A[:] == 0)

    # now modify again and commit
    a[33] = 33
    a[-2] = 98
    assert all(b[:33] == 0)
    assert b[33] == 33
    assert all(b[33+1:-2] == 0)
    assert b[-2] == 98
    assert b[-1] == 0

    transaction.commit()

    # reload DB & array
    dbclose(root)
    del root, a,b, A


    root = dbopen('%s/1.fs' % tmpdir)
    A = root['zarray']

    assert isinstance(A, ZBigArray)
    assert A.shape  == (16*1024*1024,)
    assert A.dtype  == dtype(uint8)

    a = A[:]
    assert all(a[:33] == 0)
    assert a[33] == 33
    assert all(a[33+1:-2] == 0)
    assert a[-2] == 98
    assert a[-1] == 0


    # like ZBigFile ZBigArray should survive Persistent cache clearing and not
    # go to ghost state (else logic to propagate changes from pages to objects
    # would subtly brake after Persistent cache gc)
    db = root._p_jar.db()
    ci = cacheInfo(db)
    assert ci[kkey(ZBigArray)] == 1
    assert A._p_state == UPTODATE
    db.cacheMinimize()
    ci = cacheInfo(db)
    assert ci[kkey(ZBigArray)] == 1
    assert A._p_state == UPTODATE   # it would be GHOST without LivePersistent protection

    a[-1] = 99  # would not propagate to file without ZBigFile preventing itself to go to ghost
    transaction.commit()


    # reload & verify changes
    dbclose(root)
    del root, a, A, db


    root = dbopen('%s/1.fs' % tmpdir)
    A = root['zarray']

    assert isinstance(A, ZBigArray)
    assert A.shape  == (16*1024*1024,)
    assert A.dtype  == dtype(uint8)

    a = A[:]
    assert all(a[:33] == 0)
    assert a[33] == 33
    assert all(a[33+1:-2] == 0)
    assert a[-2] == 98
    assert a[-1] == 99
