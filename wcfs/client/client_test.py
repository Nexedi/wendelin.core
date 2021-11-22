# -*- coding: utf-8 -*-
# Copyright (C) 2018-2021  Nexedi SA and Contributors.
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
"""client_test.py unit-tests virtmem layer provided by wcfs client.

WCFS filesystem itself is unit-tested by wcfs/wcfs_test.py .

At functional level, the whole wendelin.core test suite is used to verify
wcfs.py/wcfs.go while running tox tests in wcfs mode.
"""

from __future__ import print_function, absolute_import

from golang import func, defer, error, b
from wendelin.bigfile.file_zodb import ZBigFile
from wendelin.wcfs.wcfs_test import tDB, tAt, timeout, eprint
from wendelin.wcfs import _waitfor_ as waitfor_
from wendelin.wcfs import wcfs_test
from wendelin.wcfs.internal.wcfs_test import read_mustfault, read_exfault_withgil
from wendelin.wcfs.internal import mm

from pytest import raises

import os, multiprocessing, gc


# so that e.g. testdb is set up + ...
def setup_module():         wcfs_test.setup_module()
def teardown_module():      wcfs_test.teardown_module()
def setup_function(f):      wcfs_test.setup_function(f)
def teardown_function(f):   wcfs_test.teardown_function(f)


# tMapping provides testing environment for Mapping.
class tMapping(object):
    def __init__(t, tdb, mmap):
        t.tdb  = tdb
        t.mmap = mmap

    # assertBlk asserts that mmap[·] with · corresponding to blk reads as dataok.
    # pinnedOK: {} blk -> rev of t.mmap.fileh.pinned after access.
    #
    # see also: tFile.assertBlk .
    # NOTE contrary to tFile, pinnedOK represents full fh.pinned state, not
    # only pins that wcfs sent to client after tested access.
    def assertBlk(t, blk, dataok, pinnedOK):
        assert t.mmap.blk_start <= blk < t.mmap.blk_stop
        blk_inmmap = blk - t.mmap.blk_start

        dataok = b(dataok)
        fh = t.mmap.fileh
        assert len(dataok) <= fh.blksize
        dataok += b'\0'*(fh.blksize - len(dataok))   # trailing zeros

        blkview = t.mmap.mem[blk_inmmap*fh.blksize:][:fh.blksize]
        # NOTE access to memory goes _with_ GIL: this verifies that wcfs pinner
        # is implemented in fully nogil mode because if that was not the case,
        # the pinner would deadlock trying to acquire GIL in its thread while
        # user thread that triggered the access is already holding the GIL.
        #
        #      - - - - - -
        #     |           |
        #        pinner <------.
        #     |           |   wcfs
        #        client -------^
        #     |           |
        #      - - - - - -
        #     client process
        #
        _ = read_exfault_withgil(blkview[0:1])
        assert _ == dataok[0]
        assert blkview.tobytes() == dataok

        assert fhpinned(t.tdb, fh) == pinnedOK

    # assertBlkFaults asserts that mmap[·] with · corresponding to blk raises
    # SIGSEGV on read access.
    def assertBlkFaults(t, blk):
        assert t.mmap.blk_start <= blk < t.mmap.blk_stop
        blk_inmmap = blk - t.mmap.blk_start

        fh = t.mmap.fileh
        blkview = t.mmap.mem[blk_inmmap*fh.blksize:][:fh.blksize]
        for i in range(0, len(blkview), mm.PAGE_SIZE):
            read_mustfault(blkview[i:][:1])


# fhpinned(fh) returns fh.pinned with rev wrapped into tAt.
# XXX better wrap FileH into tFileH and do this automatically in .pinned ?
def fhpinned(t, fh):
    p = fh.pinned.copy()
    for blk in p:
        p[blk] = tAt(t, p[blk])
    return p

# test_wcfs_client unit-tests virtmem layer of wcfs client.
@func
def test_wcfs_client():
    t = tDB(); zf = t.zfile; at0=t.at0
    defer(t.close)
    pinned = lambda fh: fhpinned(t, fh)

    at1 = t.commit(zf, {2:'c1', 3:'d1'})
    at2 = t.commit(zf, {2:'c2'})

    wconn = t.wc.connect(at1)
    defer(wconn.close)

    fh = wconn.open(zf._p_oid)
    defer(fh.close)

    # create mmap with 1 block beyond file size
    m1 = fh.mmap(2, 3)
    defer(m1.unmap)

    assert m1.blk_start == 2
    assert m1.blk_stop  == 5
    assert len(m1.mem)  == 3*zf.blksize

    tm1 = tMapping(t, m1)

    assert pinned(fh) == {}

    # verify initial data reads
    tm1.assertBlk(2, 'c1',  {2:at1})
    tm1.assertBlk(3, 'd1',  {2:at1})
    tm1.assertBlk(4, '',    {2:at1})

    # commit with growing file size -> verify data read as the same, #3 pinned.
    # (#4 is not yet pinned because it was not accessed)
    at3 = t.commit(zf, {3:'d3', 4:'e3'})
    assert pinned(fh) == {2:at1}
    tm1.assertBlk(2, 'c1',  {2:at1})
    tm1.assertBlk(3, 'd1',  {2:at1, 3:at1})
    tm1.assertBlk(4, '',    {2:at1, 3:at1})

    # resync at1 -> at2:    #2 must unpin to @head; #4 must stay as zero
    wconn.resync(at2)
    assert pinned(fh) == {3:at1}
    tm1.assertBlk(2, 'c2',  {       3:at1})
    tm1.assertBlk(3, 'd1',  {       3:at1})
    tm1.assertBlk(4, '',    {       3:at1,  4:at0})     # XXX at0->ø ?

    # resync at2 -> at3:    #3 must unpin to @head; #4 - start to read with data
    wconn.resync(at3)
    assert pinned(fh) == {}
    tm1.assertBlk(2, 'c2',  {})
    tm1.assertBlk(3, 'd3',  {})
    tm1.assertBlk(4, 'e3',  {})

    # mmap after .size completely (start > size)
    m2 = fh.mmap(5, 2); defer(m2.unmap); tm2 = tMapping(t, m2)
    tm2.assertBlk(5, '',    {})
    tm2.assertBlk(6, '',    {})

    # open same fh twice, close once - fh2 continue to work ok
    fh2 = wconn.open(zf._p_oid)
    defer(fh2.close)
    mfh2 = fh2.mmap(2, 3); defer(mfh2.unmap); tmfh2 = tMapping(t, mfh2)
    tm1.assertBlk(2, 'c2',  {});  tmfh2.assertBlk(2, 'c2',  {})
    tm1.assertBlk(3, 'd3',  {});  tmfh2.assertBlk(3, 'd3',  {})
    tm1.assertBlk(4, 'e3',  {});  tmfh2.assertBlk(4, 'e3',  {})
    fh2.close()
    tm1.assertBlk(2, 'c2',  {});  tmfh2.assertBlk(2, 'c2',  {})
    tm1.assertBlk(3, 'd3',  {});  tmfh2.assertBlk(3, 'd3',  {})
    tm1.assertBlk(4, 'e3',  {});  tmfh2.assertBlk(4, 'e3',  {})
    m3 = fh.mmap(2, 1); defer(m3.unmap); tm3 = tMapping(t, m3)
    tm3.assertBlk(2, 'c2',  {})


    # resync ↓ -> "forbidden"  (reject is from server) -> wconn is down.
    with raises(error, match=": going back in history is forbidden"): wconn.resync(at2)
    with raises(error, match=".*: connection closed"):                wconn.open(zf._p_oid)


# verify that on Conn/FileH down/closed -> Mappings switch to EFAULT on access.
@func
def test_wcfs_client_down_efault():
    t = tDB(); zf1 = t.zfile; at0=t.at0
    defer(t.close)

    at1 = t.commit(zf1, {2:'c1', 3:'d1'})
    zf2 = t.root['zfile2'] = ZBigFile(zf1.blksize)
    at2 = t.commit()
    at3 = t.commit(zf2, {1:'β3', 2:'γ3'})

    wconn = t.wc.connect(at3)
    defer(wconn.close)

    fh1 = wconn.open(zf1._p_oid);  defer(fh1.close)
    fh2 = wconn.open(zf2._p_oid);  defer(fh2.close)

    m11 = fh1.mmap(1, 4);  defer(m11.unmap);  tm11 = tMapping(t, m11)
    m12 = fh1.mmap(3, 3);  defer(m12.unmap);  tm12 = tMapping(t, m12)
    m21 = fh2.mmap(0, 4);  defer(m21.unmap);  tm21 = tMapping(t, m21)
    m22 = fh2.mmap(2, 3);  defer(m22.unmap);  tm22 = tMapping(t, m22)

    # initially fh1 and fh2 mmaps read ok.
    tm11.assertBlk(1, '',   {})
    tm11.assertBlk(2, 'c1', {})
    tm11.assertBlk(3, 'd1', {});  tm12.assertBlk(3, 'd1', {})
    tm11.assertBlk(4, '',   {});  tm12.assertBlk(4, '',   {})
    pass;                         tm12.assertBlk(5, '',   {})

    tm21.assertBlk(0, '',   {})
    tm21.assertBlk(1, 'β3', {})
    tm21.assertBlk(2, 'γ3', {});  tm22.assertBlk(2, 'γ3', {})
    tm21.assertBlk(3, '',   {});  tm22.assertBlk(3, '',   {})
    pass;                         tm22.assertBlk(4, '',   {})

    # close fh1 -> all fh1 mmaps must turn into efaulting memory; fh2 mmaps continue to work ok.
    fh1.close()
    tm11.assertBlkFaults(1)
    tm11.assertBlkFaults(2)
    tm11.assertBlkFaults(3);  tm12.assertBlkFaults(3)
    tm11.assertBlkFaults(4);  tm12.assertBlkFaults(4)
    pass;                     tm12.assertBlkFaults(5)

    tm21.assertBlk(0, '',   {})
    tm21.assertBlk(1, 'β3', {})
    tm21.assertBlk(2, 'γ3', {});  tm22.assertBlk(2, 'γ3', {})
    tm21.assertBlk(3, '',   {});  tm22.assertBlk(3, '',   {})
    pass;                         tm22.assertBlk(4, '',   {})

    # open f1 again - mapping created via old fh1 continue to efault; new mappings work ok.
    fh1_ = wconn.open(zf1._p_oid);  defer(fh1_.close)
    m11_ = fh1_.mmap(1, 4);  defer(m11_.unmap);  tm11_ = tMapping(t, m11_)

    tm11.assertBlkFaults(1);  tm11_.assertBlk(1, '',   {})
    tm11.assertBlkFaults(2);  tm11_.assertBlk(2, 'c1', {})
    tm11.assertBlkFaults(3);  tm11_.assertBlk(3, 'd1', {});  tm12.assertBlkFaults(3)
    tm11.assertBlkFaults(4);  tm11_.assertBlk(4, '',   {});  tm12.assertBlkFaults(4)
    pass;                                                    tm12.assertBlkFaults(5)

    tm21.assertBlk(0, '',   {})
    tm21.assertBlk(1, 'β3', {})
    tm21.assertBlk(2, 'γ3', {});  tm22.assertBlk(2, 'γ3', {})
    tm21.assertBlk(3, '',   {});  tm22.assertBlk(3, '',   {})
    pass;                         tm22.assertBlk(4, '',   {})

    # close wconn -> fh2 and fh1_ mmaps must turn into efaulting too.
    wconn.close()
    tm11.assertBlkFaults(1);  tm11_.assertBlkFaults(1)
    tm11.assertBlkFaults(2);  tm11_.assertBlkFaults(2)
    tm11.assertBlkFaults(3);  tm11_.assertBlkFaults(3);  tm12.assertBlkFaults(3)
    tm11.assertBlkFaults(4);  tm11_.assertBlkFaults(4);  tm12.assertBlkFaults(4)
    pass;                                                tm12.assertBlkFaults(5)

    tm21.assertBlkFaults(0)
    tm21.assertBlkFaults(1)
    tm21.assertBlkFaults(2);  tm22.assertBlkFaults(2)
    tm21.assertBlkFaults(3);  tm22.assertBlkFaults(3)
    pass;                     tm22.assertBlkFaults(4)

    # XXX vvv -> separate test?
    # verify that after wconn.close()
    #   wconn.open(), wconn.resync(), fh.mmap()  -> error
    with raises(error, match=".*: connection closed"):   wconn.open(zf1._p_oid)
    with raises(error, match=".*: connection closed"):   wconn.resync(at3)
    with raises(error, match=".*: file already closed"): fh2.mmap(2, 3) # NOTE we did not close fh2 yet
    # ----//---- after fileh.close
    with raises(error, match=".*: file already closed"): fh1.mmap(2, 3) # fh1 was explicitly closed ^^^


# verify that on fork client turns all child's wcfs mappings to efault and
# detaches from wcfs. (else even automatic FileH.__del__ - caused by GC in child
# - can send message to wcfs server and this way break parent-wcfs exchange).
@func
def test_wcfs_client_afterfork():
    t = tDB(); zf = t.zfile; at0=t.at0
    defer(t.close)

    # initial setup
    at1 = t.commit(zf, {1:'b1', 3:'d1'})

    wconn = t.wc.connect(at1)
    defer(wconn.close)

    fh = wconn.open(zf._p_oid);  defer(fh.close)
    m  = fh.mmap(0, 4);  tm = tMapping(t, m)

    tm.assertBlk(0, '',   {})
    tm.assertBlk(1, 'b1', {})
    tm.assertBlk(2, '',   {})
    tm.assertBlk(3, 'd1', {})

    # fork child and verify that it does not interact with wcfs
    def forkedchild():
        tm.assertBlkFaults(0)
        tm.assertBlkFaults(1)
        tm.assertBlkFaults(2)
        tm.assertBlkFaults(3)
        fh.close()   # must be noop in child
        gc.collect()
        os._exit(0)  # NOTE not sys.exit not to execute deferred cleanup prepared by parent
    p = multiprocessing.Process(target=forkedchild)
    p.start()
    if not waitfor_(timeout(), lambda: p.exitcode is not None):
        eprint("\nC: child stuck")
        eprint("-> kill it (pid %s) ...\n" % p.pid)
        p.terminate()
    p.join()
    assert p.exitcode == 0

    # make sure that parent can continue using wcfs normally
    at2 = t.commit(zf, {1:'b2'})
    tm.assertBlk(0, '',   {})
    tm.assertBlk(1, 'b1', {1:at1})  # pinned @at1
    tm.assertBlk(2, '',   {1:at1})
    tm.assertBlk(3, 'd1', {1:at1})

    wconn.resync(at2) # unpins 1 to @head
    tm.assertBlk(0, '',   {})
    tm.assertBlk(1, 'b2', {})
    tm.assertBlk(2, '',   {})
    tm.assertBlk(3, 'd1', {})


# TODO try to unit test at wcfs client level wcfs.Mapping with dirty RW page -
# that it stays in sync with DB after dirty discard.

# verify that read_mustfault works as expected.
def test_read_mustfault():
    mem = mm.map_zero_ro(mm.PAGE_SIZE)
    with raises(AssertionError, match="not faulted"): read_mustfault(mem[:1])
    mm.protect(mem, mm.PROT_NONE)
    read_mustfault(mem[:1])
    mm.protect(mem, mm.PROT_READ)
    with raises(AssertionError, match="not faulted"): read_mustfault(mem[:1])
