# -*- coding: utf-8 -*-
# Copyright (C) 2018-2025  Nexedi SA and Contributors.
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

# cython: language_level=2
# cython: legacy_implicit_noexcept=True
# cython: auto_pickle=False
# distutils: language=c++

# Package _wczsync provides way to keep WCFS and ZODB connections in sync.
# See _wczsync.pxd for package overview.

from wendelin import wcfs as pywcfs
from wendelin.lib import zodb as pyzodb

from ZODB.Connection import Connection as ZConnection
from ZODB.utils import u64
from wendelin.lib.zodb import zconn_at
import weakref


# pywconnOf establishes and returns (py) wcfs.Conn associated with zconn.
#
# returned wcfs.Conn will be maintained to keep in sync with zconn, and will be
# closed when zconn is destroyed.
#
# It is invalid to make multiple simultaneous calls to pywconnOf with the same zconn.
# (in ZODB/py objects for zconn must be used from under 1 thread only).
cdef wcfs.PyConn pywconnOf(zconn):
    assert isinstance(zconn, ZConnection)
    assert zconn.opened

    wconn = getattr(zconn, '_wcfs_wconn', None)
    if wconn is not None:
        return wconn

    # zconn is not yet associated with wconn
    zstor = zconn.db().storage
    zurl  = pyzodb.zstor_2zurl(zstor)
    wc    = pywcfs.join(zurl)
    wconn = wc.connect(zconn_at(zconn))

    zconn._wcfs_wconn = wconn

    # keep wconn view of the database in sync with zconn
    # wconn and wc (= wconn.wc) will be closed when zconn is garbage-collected or shutdown via DB.close
    _ZSync(zconn, wconn)

    return wconn


# _ZSync keeps wconn in sync with zconn.
#
# wconn will be closed once zconn is garbage-collected (not closed, which
# returns it back into DB pool), or once zconn.db is closed.
#
# _ZSync cares itself to stay alive as long as zconn stays alive.
_zsyncReg   = {} # id(zsync) -> zsync   (protected by GIL)
class _ZSync:
    # .zconn_ref    weakref[zodb.Connection]
    # .wconn        (py) wcfs.Connection

    def __init__(zsync, zconn, wconn):
        #print('ZSync: setup %r <-> %r' % (wconn, zconn))
        assert zconn.opened
        zsync.wconn     = wconn
        # notify us on zconn GC
        zsync.zconn_ref = weakref.ref(zconn, zsync.on_zconn_dealloc)
        # notify us on zconn.db.close
        zconn.onShutdownCallback(zsync)

        # notify us when zconn changes its view of the database
        # NOTE zconn.onOpenCallback is not enough: zconn.at can change even
        # without zconn.close/zconn.open, e.g.:
        # zconn = DB.open(transaction_manager=tm)
        # tm.commit()   # zconn.at updated          (zconn.afterCompletion -> zconn.newTransaction)
        # tm.commit()   # zconn.at updated again
        zconn.onResyncCallback(zsync)

        # keep zsync in _zsyncReg for zsync to stay alive independently of the caller.
        #
        # NOTE we cannot use regular mutex to protect _zsyncReg updates because
        #      the other _zsyncReg mutator (on_zconn_dealloc) is invoked by
        #      automatic GC that can be triggered any time.
        #
        #      on CPython dict updates are "atomic" - they happen without releasing GIL.
        if 1: # = `with gil:`  (GIL already held in python code)
            _zsyncReg[id(zsync)] = zsync

    # _release1 closes .wconn and releases zsync once.
    def _release1(zsync):
        # unregister zsync from being kept alive
        if 1: # = `with gil:`  (see note in __init__)
            _ = _zsyncReg.pop(id(zsync), None)
        if _ is None:
            return # another call already done/is simultaneously doing release1

        #print('ZSync: sched break %r <-> .' % (zsync.wconn,))
        # schedule wconn.close() + wconn.wc.close()
        _zsync_wclose_wg.add(1)
        go(_wclose1, zsync.wconn)

        # XXX how to safely schedule work to _zsync_releaser without blocking weakref callback?
        """
        # (we cannot do this from under weakref callback - see _zsync_releaser for details)
        _zsync_releaseq.append(...)
        _zsync_releaseq.send(zsync.wconn)
        """

    # .zconn dealloc -> wconn.close; release zsync.
    def on_zconn_dealloc(zsync, _):
        zsync._release1()

    # DB.close -> wconn.close; release zsync.
    def on_connection_shutdown(zsync):
        zsync._release1()

    # DB resyncs .zconn onto new database view.
    # -> resync .wconn to updated database view of ZODB connection.
    def on_connection_resync(zsync):
        zconn = zsync.zconn_ref()
        zsync.wconn.resync(zconn_at(zconn))

from golang import go
from golang import sync

# XXX disabled for now (not sure how to safely schedule work from under weakref callback)
#     -> doing straight `go _wclose1` every time.
"""
from golang import go, chan
import logging as log

# _zsync_releaser is dedicated thread that closes wconn/wc after ZSync detects
# that zconn - to which wconn was associated - is no longer alive.
#
# Requests to _zsync_releaser come from ZSync.on_zconn_dealloc - which is
# called from under weakref callback when zconn is garbage collected. Since it
# is not safe to take python-level locks from under __del__ or weakref callback
# (that can cause deadlocks), the releasing work is scheduled to be done in
# separate _zsync_releaser thread.
#
# The need to take locks: even though wconn.close() can work without taking
# py-level locks, wc.close needs to take wcfs._wcmu
_zsync_releaseq = [] # of wconn
_zsync_wakeup   = chan(1)
def _zsync_releaser():
    while 1:
        wconn, ok = _zsync_releaseq.recv_()
        if not ok:
            break # time to stop    XXX needed?

        # let's close wconn and its .wc
        try:
            1/0
            _wclose1(wconn)
        except:
            log.exception("zsync: releaser: wclose %r", wconn)

        # XXX nrelease += 1

go(_zsync_releaser)
"""

_zsync_wclose_wg = sync.WaitGroup()
def _wclose1(wconn):
    #print('ZSync: break %r <-> .' % (wconn,))
    wc = wconn.wc
    wconn.close()
    wc.close()
    _zsync_wclose_wg.done()


# at shutdown make sure there is no in-flight _wclose1
import atexit; atexit.register(_zsync_wclose_wg.wait)
