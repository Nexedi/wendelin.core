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

TODO big picture module description

Things are done this way (vs more ZODB-like way) because:

    - compatibility with FS approach    (to be able to switch to fuse)
    - only one manager (vs two managers - bigfile & Connection and synchronizing them)
    - on abort no need to synchronize changes (= "only 1 manager" as above)
    - TODO ...
"""

from wendelin.bigfile import BigFile, WRITEOUT_STORE, WRITEOUT_MARKSTORED
from wendelin.lib.mem import bzero, memcpy

from transaction.interfaces import IDataManager, ISynchronizer
from persistent import Persistent, PickleCache
from BTrees.LOBTree import LOBTree
from zope.interface import implementer

# TODO document that first data access must be either after commit or Connection.add

# FIXME peak 2·Ndirty memory consumption on write (should be 1·NDirty)

# XXX write about why we can't memory-dirty -> ZBlk (memory could be modified
# later one more time)


# data of 1 file block as stored in ZODB
class ZBlk(Persistent):
    # ._v_blkdata   - bytes
    __slots__ = ('_v_blkdata')
    # NOTE _v_ - so that we can alter it without Persistent noticing -- we'll
    #      manage ZBlk states by ourselves explicitly.

    # TODO __getstate__ / __reduce__ so that it pickles minimally?

    # client requests us to load blkdata from DB
    # (DB -> ._v_blkdata -> memory-page)
    def loadblkdata(self):
        # ensure ._v_blkdata is loaded
        # (it could be not, if e.g. loadblk is called second time for the same
        #  blk - we loaded it first time and thrown ._v_blkdata away as unneeded
        #  intermediate copy not to waste memory - see below)
        if self._v_blkdata is None:
            # ZODB says: further accesses will cause object data to be reloaded.
            self._p_invalidate()

        blkdata = self._v_blkdata
        assert blkdata is not None
        # do not waste memory - ._v_blkdata is used only as intermediate copy on path
        # from DB to fileh mapping. See also counterpart action in __getstate__().
        self._v_blkdata = None

        return blkdata


    # DB (through pickle) requests us to emit state to save
    # (DB <- ._v_blkdata <- memory-page)
    def __getstate__(self):
        # request to pickle should go in only when zblk was set changed (by
        # storeblk), and only once.
        assert self._v_blkdata is not None
        # NOTE self._p_changed is not necessarily true here - e.g. it is not
        # for newly created objects irregardles of initial ._p_changed=True

        blkdata = self._v_blkdata
        # do not waste memory for duplicated data - as soon as blkdata lands
        # into DB we can drop it here becase ._v_blkdata was only an
        # intermediate copy from fileh memory to database.
        #
        # FIXME this works, but transaction first prepares all objects for
        #       commit and ony the saves thems. For use it means __getstate__
        #       gets called very late and for all objects and thus we'll be
        #       keeping ._v_blkdata for all of them before final phase =
        #       2·NDirty peak memory consumption.
        self._v_blkdata = None

        # XXX change .p_state to GHOST ?
        return blkdata


    # DB (through pickle) loads data to memory
    def __setstate__(self, state):
        self._v_blkdata = state



# XXX merge Persistent/BigFile comments into 1 place
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
# XXX move to common place?
class LivePersistent(Persistent):
    # NOTE in ZODB < 3.10 (exactly before commit a9cda7fb) objects coming from
    # db are first created in seemingly uptodate state and then deactivated
    # just to register them by the way to persistent machinery.
    #
    # In ZODB >= 3.10 such registration happens explicitly and no such first
    # deactivation is done.
    if not hasattr(PickleCache, 'new_ghost'):
        _v_registered = False   # were we registered to Persistent machinery?
    else:
        _v_registered = True

    # don't allow us to go to ghost
    #
    # NOTE we can't use STICKY as that state is assumed as
    # short-lived-temporary by ZODB and is changed back to UPTODATE by
    # persistent code. In fact ZODB says: STICKY is UPTODATE+keep in memory.
    def _p_deactivate(self):
        # on ZODB < 3.10 need to call Persistent._p_deactivate() the first time
        # - it registers the object with its own machinery on first ghostify
        # (which happens right before-on object load from DB) see comments in
        # Per__p_deactivate() in persistent.
        if not self._v_registered:
            Persistent._p_deactivate(self)
            # 1) prevents calling Persistent._p_deactivate() next time, and
            # 2) causes Persistent to change state to UPTODATE
            self._v_registered = True

        # just returning here won't allow Persistent._p_deactivate() run and
        # thus we'll stay in non-ghost state.
        return


    # when creating initially (contrast to loading from DB), no need to go to
    # Persistent._p_deactivate() even for the first time
    def __init__(self):
        Persistent.__init__(self)
        self._v_registered = True



# NOTE Can't inherit from Persistent and BigFile at the same time - both are C
# types and their layout conflict. Persistent must be here for object to be
# tracked -> BigFile is moved to a data member (the same way and for the same
# reason it is done for PersistentList & PersistentDict).
class ZBigFile(LivePersistent):
    # file is split into blocks; each block is stored as separate object in the DB
    #
    #   .blksize
    #   .blktab       {} blk -> ZBlk(blkdata)

    # ._v_file      _ZBigFile helper
    #
    # NOTE Live: don't allow us to go to ghost state not to loose ._v_file
    #      which DataManager _ZBigFileH refers to.

    def __init__(self, blksize):
        LivePersistent.__init__(self)
        self.__setstate__((blksize, LOBTree()))     # NOTE L enough for blk_t


    # TODO verify get,set state
    def __getstate__(self):
        return (self.blksize, self.blktab)

    def __setstate__(self, state):
        self.blksize, self.blktab = state
        self._v_file = _ZBigFile(self, self.blksize)


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
        assert len(blkdata) == self._v_file.blksize
        memcpy(buf, blkdata)        # FIXME memcpy


    # store data    dirty page -> ZODB obj
    def storeblk(self, blk, buf):
        zblk = self.blktab.get(blk)
        if zblk is None:
            zblk = self.blktab[blk] = ZBlk()

        zblk._v_blkdata = bytes(buf)    # FIXME does memcpy
        zblk._p_changed = True          # if zblk was already in DB: _p_state -> CHANGED


    # bigfile-like
    def fileh_open(self):   return _ZBigFileH(self)




# BigFileH wrapper that also acts as DataManager proxying changes back to ZODB
# objects at two-phase-commit (TPC) level.
#
# NOTE several fileh can be opened for ZBigFile - and this does not
#      conflict with the way ZODB organises its work - just for fileh
#      handles ZBigFile acts as a database, and for real ZODB database
#      it acts as (one of) connections.
#
# NOTE ISynchronizer is used only to be able to join into transactions without
#      tracking intermediate changes to pages.
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

        # Hook into txn_manager so that we get a chance to run before
        # transaction.commit().   (see .beforeCompletion() with more details)
        self.transaction_manager.registerSynch(self)

        # XXX txn_manager unregister synchs itself (it uses weakset to keep references)
        # XXX however that unregistration is delayed to gc.collect() time and
        # XXX maybe we should not perform any action right after Connection is closed
        #     (as it is now .beforeCompletion() continue to be getting notified)



    # ~~~~ BigFileH wrapper ~~~~
    def mmap(self, pgoffset, pglen):    return self.zfileh.mmap(pgoffset, pglen)
    # .dirty_writeout?  -- not needed - these are handled via
    # .dirty_discard?   -- transaction commit/abort


    # ~~~~ ISynchronizer ~~~~
    def beforeCompletion(self, txn):
        # if dirty, join every transaction so that it knows we want to participate in TPC.
        #
        # This is needed because we do not track every change to pages (and
        # then immediately join txn, like ZODB Connection do for objects), but
        # instead join txn here right before commit.

        if not self.zfileh.isdirty():
            return

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
        # DataManagers while commit is running, but that comes with dificulties
        # if wanting to support whole transaction semantics (i.e. whether to
        # call joined tpc_begin(), if we are already at commit()? And also what
        # to do about order - newly joined sortKey() could be lesser than
        # DataManagers already done at current TPC phase...
        zconn = self.zfile._p_jar
        assert txn is zconn.transaction_manager.get()
        if zconn._needs_to_join:            # same as Connection._register(obj)
            txn.join(zconn)                 # on first obj, without registering
            zconn._needs_to_join = False    # anything.

    def afterCompletion(self, txn):
        pass

    # NOTE called not always - only at explicit transaction.begin()
    #      -> can't use as new-txn synchronization hook
    def newTransaction(self, txn):
        pass


    # ~~~~ IDataManager ~~~~

    # key ordering us wrt other DataManager in tpc_*() sequence calling
    # NOTE for our propagated-to-objects changes to propagate to ZODB, we need
    # to act earlier than ZODB DataManager managin zfile.blktab - hence the trick.
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
