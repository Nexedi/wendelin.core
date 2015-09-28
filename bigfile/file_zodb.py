# -*- coding: utf-8 -*-
# Wendelin.bigfile | BigFile ZODB backend
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
""" BigFile backed by ZODB objects

To represent BigFile as ZODB objects, each file block is represented separately
either as

    1) one ZODB object, or          (ZBlk0)
    2) group of ZODB objects        (ZBlk1)

with top-level BTree directory #blk -> objects representing block.

For "1" we have

    - low-overhead access time (only 1 object loaded from DB), but
    - high-overhead in terms of ZODB size (with FileStorage / ZEO, every change
      to a block causes it to be written into DB in full again)

For "2" we have

    - low-overhead in terms of ZODB size (only part of a block is overwritten
      in DB on single change), but
    - high-overhead in terms of access time
      (several objects need to be loaded for 1 block)

In general it is not possible to have low-overhead for both i) access-time, and
ii) DB size, with approach where we do block objects representation /
management on *client* side.

On the other hand, if object management is moved to DB *server* side, it is
possible to deduplicate them there and this way have low-overhead for both
access-time and DB size with just client storing 1 object per file block. This
will be our future approach after we teach NEO about object deduplication.

~~~~

As file pages are changed in RAM with changes being managed by virtmem
subsystem, we need to propagate the changes to ZODB objects back at some time.

Two approaches exist:

    1) on every RAM page dirty, in a callback invoked by virtmem, mark
       corresponding ZODB object as dirty, and at commit time, in
       obj.__getstate__ retrieve memory content.

    2) hook into commit process, and before committing, synchronize RAM page
       state to ZODB objects state, propagating all dirtied pages to ZODB objects
       and then do the commit process as usual.

"1" is more natural to how ZODB works, but requires tight integration between
virtmem subsystem and ZODB (to be able to receive callback on a page dirtying).

"2" is less natural to how ZODB works, but requires less-tight integration
between virtmem subsystem and ZODB, and virtmem->ZODB propagation happens only
at commit time.

Since, for performance reasons, virtmem subsystem is going away and BigFiles
will be represented by real FUSE-based filesystem with virtual memory being
done by kernel, where we cannot get callback on a page-dirtying, it is more
natural to also use "2" here.
"""

from wendelin.bigfile import BigFile, WRITEOUT_STORE, WRITEOUT_MARKSTORED
from wendelin.lib.mem import bzero, memcpy

from transaction.interfaces import IDataManager, ISynchronizer
from persistent import Persistent, PickleCache, GHOST
from BTrees.LOBTree import LOBTree
from BTrees.IOBTree import IOBTree
from zope.interface import implementer
from ZODB.Connection import Connection
from weakref import WeakSet
import os

# TODO document that first data access must be either after commit or Connection.add

# FIXME peak 2·Ndirty memory consumption on write (should be 1·NDirty)


# Base class for data of 1 file block as stored in ZODB
class ZBlkBase(Persistent):
    # ._v_zfile     - ZBigFile | None
    # ._v_blk       - offset of this blk in ._v_zfile | None
    __slots__ = ('_v_zfile', '_v_blk')
    # NOTE _v_ - so that we can alter it without Persistent noticing -- we'll
    #      manage ZBlk states by ourselves explicitly.

    def __init__(self):
        self._v_zfile = None
        self._v_blk   = None


    # client requests us to load blkdata from DB, which will then go to memory
    # DB -> .blkdata  (-> memory-page)
    def loadblkdata(self):
        raise NotImplementedError()

    # client requests us to set blkdata to be later saved to DB
    # (DB <- )  .blkdata <- memory-page
    def setblkdata(self, buf):
        raise NotImplementedError()


    # make this ZBlk know it represents zfile[blk]
    # NOTE this has to be called by master every time ZBlk object potentially
    #      goes from GHOST to Live state
    # NOTE it is ok to keep reference to zfile (yes it creates
    #      ZBigFile->ZBlk->ZBigFile cycle but that does not hurt).
    def bindzfile(self, zfile, blk):
        # bind; if already bound, should be the same
        if self._v_zfile is None:
            self._v_zfile   = zfile
            self._v_blk     = blk
        else:
            assert self._v_zfile    is zfile
            assert self._v_blk      == blk


    # DB notifies this object has to be invalidated
    # (DB -> invalidate .blkdata -> invalidate memory-page)
    def _p_invalidate(self):
        # do real invalidation only once - else we already lost ._v_zfile last time
        if self._p_state is GHOST:
            return
        # on invalidation we must be already bound
        # (to know which ZBigFileH to propagate invalidation to)
        assert self._v_zfile    is not None
        assert self._v_blk      is not None
        self._v_zfile.invalidateblk(self._v_blk)
        Persistent._p_invalidate(self)



# ZBlk storage formats
# NOTE once established formats do not change on disk


# ZBlk format 0: raw bytes
class ZBlk0(ZBlkBase):
    # ._v_blkdata   - bytes
    __slots__ = ('_v_blkdata',)

    # DB -> ._v_blkdata  (-> memory-page)
    def loadblkdata(self):
        # ensure ._v_blkdata is loaded
        # (it could be not, if e.g. loadblk is called second time for the same
        #  blk - we loaded it first time and thrown ._v_blkdata away as unneeded
        #  intermediate copy not to waste memory - see below)
        if self._v_blkdata is None:
            # ZODB says: further accesses will cause object data to be reloaded.
            # NOTE directly Persistent._p_invalidate() to avoid
            # virtmem->loadblk->invalidate callback
            Persistent._p_invalidate(self)

        blkdata = self._v_blkdata
        assert blkdata is not None
        # do not waste memory - ._v_blkdata is used only as intermediate copy on path
        # from DB to fileh mapping. See also counterpart action in __getstate__().
        self._v_blkdata = None

        return blkdata

    # (DB <- )  ._v_blkdata <- memory-page
    def setblkdata(self, buf):
        blkdata = bytes(buf)                    # FIXME does memcpy
        # trim trailing \0
        self._v_blkdata = blkdata.rstrip(b'\0') # FIXME copy


    # DB (through pickle) requests us to emit state to save
    # DB <- ._v_blkdata  (<- memory-page)
    def __getstate__(self):
        # request to pickle should go in only when zblk was set changed (by
        # storeblk), and only once.
        assert self._v_blkdata is not None
        # NOTE self._p_changed is not necessarily true here - e.g. it is not
        # for newly created objects irregardless of initial ._p_changed=True

        blkdata = self._v_blkdata
        # do not waste memory for duplicated data - as soon as blkdata lands
        # into DB we can drop it here because ._v_blkdata was only an
        # intermediate copy from fileh memory to database.
        #
        # FIXME this works, but transaction first prepares all objects for
        #       commit and only then saves them. For use it means __getstate__
        #       gets called very late and for all objects and thus we'll be
        #       keeping ._v_blkdata for all of them before final phase =
        #       2·NDirty peak memory consumption.
        self._v_blkdata = None

        # XXX change .p_state to GHOST ?
        return blkdata


    # DB (through pickle) loads data to memory
    # DB -> ._v_blkdata  (-> memory-page)
    def __setstate__(self, state):
        super(ZBlk0, self).__init__()
        self._v_blkdata = state

    # ZBlk as initially created (empty placeholder)
    def __init__(self):
        self.__setstate__(None)


# ZBlk format 1: block splitted into chunks of fixed size in BTree
#
# NOTE zeros are not stored -> either no chunk at all, or trailing zeros
#      are stripped.

# data as Persistent object
class ZData(Persistent):
    __slots__ = ('data')
    def __init__(self, data):
        self.data = data

    def __getstate__(self):
        return self.data

    def __setstate__(self, state):
        self.data = state


class ZBlk1(ZBlkBase):
    # .chunktab {} offset -> ZData(chunk)
    __slots__ = ('chunktab')

    # NOTE the reader does not assume chunks are of this size - it decodes
    # .chunktab as it was originally encoded - only we write new chunks with
    # this size -> so CHUNKSIZE can be changed over time.
    CHUNKSIZE = 4096    # XXX ad-hoc ?  (but is a good number = OS pagesize)


    # DB -> .chunktab  (-> memory-page)
    def loadblkdata(self):
        # empty?
        if not self.chunktab:
            return b''

        # find out whole blk len via inspecting tail chunk
        tail_start = self.chunktab.maxKey()
        tail_chunk = self.chunktab[tail_start]
        blklen = tail_start + len(tail_chunk.data)

        # whole buffer initialized as 0 + tail_chunk
        blkdata = bytearray(blklen)
        blkdata[tail_start:] = tail_chunk.data

        # go through all chunks besides tail and extract them
        stop = 0
        for start, chunk in self.chunktab.items(max=tail_start, excludemax=True):
            assert start >= stop    # verify chunks don't overlap
            stop = start+len(chunk.data)
            blkdata[start:stop] = chunk.data

        # deactivate .chunktab to not waste memory
        # (see comments about why in ZBlk0.loadblkdata())
        for chunk in self.chunktab.values():
            chunk._p_deactivate()
        self.chunktab._p_deactivate()
        # TODO deactivate all chunktab buckets - XXX how?

        return blkdata


    # (DB <- )  .chunktab <- memory-page
    def setblkdata(self, buf):
        chunktab  = self.chunktab
        CHUNKSIZE = self.CHUNKSIZE

        # first make sure chunktab was previously written with the same CHUNKSIZE
        # (for simplicity we don't allow several chunk sizes to mix)
        for start, chunk in chunktab.items():
            if (start % CHUNKSIZE) or len(chunk.data) > CHUNKSIZE:
                chunktab.clear()
                break

        # scan over buf and update/delete changed chunks
        for start in range(0, len(buf), CHUNKSIZE):
            data = buf[start:start+CHUNKSIZE]   # FIXME copy on py2
            # make sure data is bytes
            # (else we cannot .rstrip() it below)
            if not isinstance(data, bytes):
                data = bytes(data)              # FIXME copy on py3
            # trim trailing \0
            data = data.rstrip(b'\0')           # FIXME copy
            chunk = chunktab.get(start)

            # all 0 -> make sure to remove chunk
            if not data:
                if chunk is not None:
                    del chunktab[start]

            # some !0 data -> compare and store if changed
            else:
                if chunk is None:
                    chunk = chunktab[start] = ZData(b'')

                if chunk.data != data:
                    chunk.data = data


    # DB (through pickle) requests us to emit state to save
    # DB <- .chunktab  (<- memory-page)
    def __getstate__(self):
        # TODO do not waste memory for duplicated data? (.chunktab memory is
        # only intermediate on path from memory-page to DB). The freeing could
        # be done with e.g. delayed deactivation after transaction completes.
        return self.chunktab

    # DB (through pickle) loads data to memory
    # DB -> .chunktab  (-> memory-page)
    def __setstate__(self, state):
        super(ZBlk1, self).__init__()
        self.chunktab = state


    # ZBlk1 as initially created (empty placeholder)
    def __init__(self):
        super(ZBlk1, self).__init__()
        self.__setstate__(IOBTree())


# backward compatibility (early versions wrote ZBlk0 named as ZBlk)
ZBlk = ZBlk0

# format-name -> blk format type
ZBlk_fmt_registry = {
    'ZBlk0':    ZBlk0,
    'ZBlk1':    ZBlk1,
}

# format for updated blocks
ZBlk_fmt_write = os.environ.get('WENDELIN_CORE_ZBLK_FMT', 'ZBlk1')
if ZBlk_fmt_write not in ZBlk_fmt_registry:
    raise RuntimeError('E: Unknown ZBlk format %r' % ZBlk_fmt_write)


# ----------------------------------------


# helper for ZBigFile - just redirect loadblk/storeblk back
# (because it is not possible to inherit from both Persistent and BigFile at
#  the same time - see below)
class _ZBigFile(BigFile):
    # .zself    - reference to ZBigFile

    def __new__(cls, zself, blksize):
        obj = BigFile.__new__(cls, blksize)
        obj.zself = zself
        return obj

    # redirect load/store to main class
    def loadblk(self, blk, buf):    return self.zself.loadblk(blk, buf)
    def storeblk(self, blk, buf):   return self.zself.storeblk(blk, buf)



# Persistent that never goes to ghost state, if it was ever uptodate.
#
# NOTE
#
# On invalidation LivePersistent still goes to ghost state, because
# invalidation cannot be ignored, i.e. they indicate the object has been
# changed externally.
#
# Invalidation can happen only at transaction boundary, so during the course of
# transaction LivePersistent is guaranteed to stay uptodate.
#
# XXX move to common place?
class LivePersistent(Persistent):
    # don't allow us to go to ghost
    #
    # NOTE we can't use STICKY as that state is assumed as
    # short-lived-temporary by ZODB and is changed back to UPTODATE by
    # persistent code. In fact ZODB says: STICKY is UPTODATE+keep in memory.
    def _p_deactivate(self):
        # just returning here won't allow Persistent._p_deactivate() run and
        # thus we'll stay in non-ghost state.
        return


    # NOTE _p_invalidate() is triggered on invalidations. We do not override it.


# NOTE Can't inherit from Persistent and BigFile at the same time - both are C
# types and their layout conflict. Persistent must be here for object to be
# tracked -> BigFile is moved to a data member (the same way and for the same
# reason it is done for PersistentList & PersistentDict).
class ZBigFile(LivePersistent):
    # file is split into blocks; each block is stored as separate object in the DB
    #
    #   .blksize
    #   .blktab       {} blk -> ZBlk*(blkdata)

    # ._v_file      _ZBigFile helper
    # ._v_filehset  weakset( _ZBigFileH ) that we created
    #
    # NOTE Live: don't allow us to go to ghost state not to loose ._v_file
    #      which DataManager _ZBigFileH refers to.

    def __init__(self, blksize):
        LivePersistent.__init__(self)
        self.__setstate__((blksize, LOBTree()))     # NOTE L enough for blk_t


    # state is (.blksize, .blktab)
    def __getstate__(self):
        return (self.blksize, self.blktab)

    def __setstate__(self, state):
        self.blksize, self.blktab = state
        self._v_file = _ZBigFile(self, self.blksize)
        self._v_filehset = WeakSet()


    # load data     ZODB obj -> page
    def loadblk(self, blk, buf):
        zblk = self.blktab.get(blk)
        # no data written yet - "hole" reads as all zeros
        if zblk is None:
            # Nothing to do here - the memory buf obtained from OS comes pre-cleared
            # XXX reenable once/if memory comes uninitialized here
            #bzero(buf)
            return

        # TODO use specialized unpickler and load data directly to blk.
        #      also in DB better store directly {#blk -> #dataref} without ZBlk overhead
        blkdata = zblk.loadblkdata()
        assert len(blkdata) <= self._v_file.blksize
        zblk.bindzfile(self, blk)
        memcpy(buf, blkdata)        # FIXME memcpy
        #bzero(buftail)             # not needed - buf comes pre-cleared from OS


    # store data    dirty page -> ZODB obj
    def storeblk(self, blk, buf):
        zblk = self.blktab.get(blk)
        zblk_type_write = ZBlk_fmt_registry[ZBlk_fmt_write]
        # if zblk was absent or of different type - we (re-)create it anew
        if zblk is None  or \
           type(zblk) is not zblk_type_write:
            zblk = self.blktab[blk] = zblk_type_write()

        zblk.setblkdata(buf)
        zblk._p_changed = True          # if zblk was already in DB: _p_state -> CHANGED
        zblk.bindzfile(self, blk)


    # invalidate data   .blktab[blk] invalidated -> invalidate page
    def invalidateblk(self, blk):
        for fileh in self._v_filehset:
            fileh.invalidate_page(blk)  # XXX assumes blksize == pagesize



    # bigfile-like
    def fileh_open(self):
        fileh = _ZBigFileH(self)
        self._v_filehset.add(fileh)
        return fileh




# patch for ZODB.Connection to support callback on .open()
# NOTE on-open  callbacks are setup once and fire many times on every open
#      on-close callbacks are setup once and fire only once on next close
Connection._onOpenCallbacks = None
def Connection_onOpenCallback(self, f):
    if self._onOpenCallbacks is None:
        # NOTE WeakSet does not work for bound methods - they are always created
        # anew for each obj.method access, and thus will go away almost immediately
        self._onOpenCallbacks = WeakSet()
    self._onOpenCallbacks.add(f)

assert not hasattr(Connection, 'onOpenCallback')
Connection.onOpenCallback = Connection_onOpenCallback

orig_Connection_open = Connection.open
def Connection_open(self, transaction_manager=None, delegate=True):
    orig_Connection_open(self, transaction_manager, delegate)

    # FIXME method name hardcoded. Better not do it and allow f to be general
    # callable, but that does not work with bound method - see above.
    # ( Something like WeakMethod from py3 could help )
    if self._onOpenCallbacks:
        for f in self._onOpenCallbacks:
            f.on_connection_open()

Connection.open = Connection_open
# ------------



# BigFileH wrapper that also acts as DataManager proxying changes back to ZODB
# objects at two-phase-commit (TPC) level.
#
# NOTE several fileh can be opened for ZBigFile - and this does not
#      conflict with the way ZODB organises its work - just for fileh
#      handles ZBigFile acts as a database, and for real ZODB database
#      it acts as (one of) connections.
#
# NOTE ISynchronizer is used to be able to join into transactions without
#      tracking intermediate changes to pages:
#
#      _ZBigFileH sticks to ZODB.Connection under which it was originally opened
#      and then participates in that connection lifecycle forever keeping sync on
#      that connection close and reopen.
#
#      This is required because ZBigFile and ZBigArray are both LivePersistent
#      (i.e. they never go to ghost state) and thus stays forever live (= active)
#      in Connection._cache.
#
#      If it was only ZBigFile, we could be opening new fileh every time for
#      each connection open and close/unref that fileh object on connection
#      close. But:
#
#       1. this scheme is inefficient (upon close, fileh forgets all its loaded
#          memory, and thus for newly opened fileh we'd need to reload file data
#          from scratch)
#
#       2. ZBigArray need to reference opened fileh --- since ZBigArray stays
#          live in Connection._cache, fileh also automatically stay live.
#
#      So in essence _ZBigFileH is a data manager which works in sync with ZODB
#      Connection propagating changes between fileh memory and ZODB objects.
#
# NOTE Bear in mind that after close, connection can be reopened in different
#      thread - that's why we have to adjust registration to per-thread
#      transaction_manager.
@implementer(IDataManager)
@implementer(ISynchronizer)
class _ZBigFileH(object):
    # .zfile        ZBigFile we were opened for
    # .zfileh       handle for ^^^

    def __init__(self, zfile):
        self.zfile  = zfile
        self.zfileh = zfile._v_file.fileh_open()

        # FIXME zfile._p_jar could be None (ex. ZBigFile is newly created
        #       before first commit)

        # IDataManager requires .transaction_manager
        self.transaction_manager = zfile._p_jar.transaction_manager

        # when connection will be reopened -> txn_manager.registerSynch(self)
        zfile._p_jar.onOpenCallback(self)   # -> self.on_connection_open()

        # when we are just initially created, the connection is already opened,
        # so manually compensate for it.
        self.on_connection_open()


    def on_connection_open(self):
        # when connection is closed -> txn_manager.unregisterSynch(self)
        # NOTE close callbacks are fired once, and thus we have to re-register
        #      it on every open.
        self.zfile._p_jar.onCloseCallback(self.on_connection_close)

        # attach us to _current_ _thread_ TM (staying in sync with Connection):
        #
        # Hook into txn_manager so that we get a chance to run before
        # transaction.commit().   (see .beforeCompletion() with more details)
        self.transaction_manager.registerSynch(self)


    def on_connection_close(self):
        # detach us from _current_ _thread_ TM (staying in sync with Connection)
        self.transaction_manager.unregisterSynch(self)

        # NOTE open callbacks are setup once and fire on every open - we don't
        #      need to resetup them here.


    # ~~~~ BigFileH wrapper ~~~~
    def mmap(self, pgoffset, pglen):    return self.zfileh.mmap(pgoffset, pglen)
    # .dirty_writeout?  -- not needed - these are handled via
    # .dirty_discard?   -- transaction commit/abort

    def invalidate_page(self, pgoffset):
        return self.zfileh.invalidate_page(pgoffset)


    # ~~~~ ISynchronizer ~~~~
    def beforeCompletion(self, txn):
        # if dirty, join every transaction so that it knows we want to participate in TPC.
        #
        # This is needed because we do not track every change to pages (and
        # then immediately join txn, like ZODB Connection do for objects), but
        # instead join txn here right before commit/abort.

        # make sure we are called only when connection is opened
        assert self.zfile._p_jar.opened

        if not self.zfileh.isdirty():
            return

        assert self not in txn._resources       # (not to join twice)
        txn.join(self)
        # XXX hack - join Connection manually before transaction.commit() starts.
        #
        # the reason we do it here, is that if we don't and Connection was not
        # yet joined to transaction (i.e. no changes were made to Connection's
        # objects), as storeblk() running inside commit will case changes to
        # ZODB objects, zconn will want to join transaction, and that is
        # currently forbidden.
        #
        # More cleaner fix would be to teach transaction to allow joining
        # DataManagers while commit is running, but that comes with difficulties
        # if wanting to support whole transaction semantics (i.e. whether to
        # call joined tpc_begin(), if we are already at commit()? And also what
        # to do about order - newly joined sortKey() could be lesser than
        # DataManagers already done at current TPC phase...
        zconn = self.zfile._p_jar
        assert txn is zconn.transaction_manager.get()
        if zconn._needs_to_join:                # same as Connection._register(obj)
            assert zconn not in txn._resources  # (not to join twice)
            txn.join(zconn)                     # on first obj, without registering
            zconn._needs_to_join = False        # anything.

    def afterCompletion(self, txn):
        pass

    # NOTE called not always - only at explicit transaction.begin()
    #      -> can't use as new-txn synchronization hook
    def newTransaction(self, txn):
        pass


    # ~~~~ IDataManager ~~~~

    # key ordering us wrt other DataManager in tpc_*() sequence calling
    # NOTE for our propagated-to-objects changes to propagate to ZODB, we need
    # to act earlier than ZODB DataManager managing zfile.blktab - hence the trick.
    def sortKey(self):
        # XXX _p_jar can be None - if that object is new?
        zkey = self.zfile._p_jar.sortKey()
        key  = "%s%s (%s:%s)" % (
                # first letter smaller and, if possible, readable
                zkey[0] > ' ' and ' ' or '\0',
                zkey[1:], type(self), id(self))
        assert key < zkey
        return key


    # abort txn which is not in TPC phase
    def abort(self, txn):
        self.zfileh.dirty_discard()


    def tpc_begin(self, txn):
        # TODO probably take some lock and mark dirty pages RO, so that other
        # threads could not change data while the transaction is being
        # committed
        pass


    def commit(self, txn):
        # propagate changes to objects, but does not mark pages as stored -
        # - in case of following tpc_abort() we'll just drop dirty pages and
        # changes to objects we'll be done by ZODB data manager (= Connection)
        self.zfileh.dirty_writeout(WRITEOUT_STORE)


    def tpc_vote(self, txn):
        # XXX what here? verify that zfile.blktab can be committed without errors?
        pass


    def tpc_finish(self, txn):
        # finish is just "mark dirty pages as stored ok"
        self.zfileh.dirty_writeout(WRITEOUT_MARKSTORED)


    # abort txn which is in TPC phase
    def tpc_abort(self, txn):
        # this is like .abort(), but any of (including none)
        # tpc_{begin,commit,vote,finish) were called
        #
        # we assume .tpc_finish() was not yet called XXX write why
        # (because for tpc_abort to work after tpc_finish, we'll need to store
        # which file parts we wrote and invalidate that -> simpler not to care
        # and also more right - tpc_finish is there assumed as non-failing by
        # ZODB design)
        self.abort(txn)
