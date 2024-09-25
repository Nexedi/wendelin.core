#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2020-2024  Nexedi SA and Contributors.
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
"""Program treegen provides infrastructure to generate ZODB BTree states.

It is used as helper for ΔBtail and ΔFtail tests.

The following subcommands are provided:

- `trees` transition ZODB tree through requested tree states,
- `allstructs` generate subset of all possible tree topologies for a tree
  specified by key->value dict.

Because python/pkg_resources startup is very slow(*) all subcommands can be
used either in CLI or in server mode, where requests are continuously read from
stdin.

Subcommands documentation follows:


trees
-----

`treegen trees <zurl>` transitions ZODB LOBTree through requested tree states.
Tree states are specified on stdin as topology-encoded strings(+), 1 state per 1 line.
For every request the tree is changed to have specified keys, values and
topology, and modifications are committed to database. For every made commit
corresponding transaction ID is printed to stdout.

The tree, that `treegen trees` generates and works on, is accessible via
zconn.root()['treegen/tree'].

Trees protocol specification:

    S: tree.srv start @<head> root=<tree-root-oid>
    C: <tree₁>
    S: <tid₁>
    C: <tree₂>
    S: <tid₂>
    ...

session example:

    S: tree.srv start @03d85dd71ed0d2ee root=000000000000000b
    C: T/B1:a
    S: 03d85dd84fed7844
    C: T2/B1:a-B3:c
    S: 03d85dd871718899
    ...

The following topology is treated specially:

    - "ø" requests deletion of the tree.


allstructs
----------

`treegen allstructs` generates subset of all possible tree topologies for a tree
specified by key->value dict.

Given kv the following tree topologies are considered: 1) native (the one
that ZODB would usually create natively via regular usage), and 2) n-1 random
ones. Then those tree topologies are emitted.

The output of `treegen allstructs` is valid input for `treegen trees`.

Allstructs protocol specification:

    S: # allstructs.srv start
    C: <maxdepth> <maxsplit> <n>(/<seed>) <kv>
    S: # allstructs <kv>
    S: # maxdepth=<maxdepth> maxsplit=<maxsplit> n=<n> seed=<seed>
    S: <tree₀>
    S: <tree₁>
    S: <tree₂>
    ...
    S: # ----

session example:

    # allstructs.srv start
    1 1 10 1:a,2:b,3:c
    # allstructs 1:a,2:b,3:c
    # maxdepth=1 maxsplit=1 n=10 seed=1624901326
    T2/B1:a-B2:b,3:c
    T3/B1:a,2:b-B3:c
    T2/T-T3/B1:a-B2:b-B3:c
    T/T3/B1:a,2:b-B3:c
    T/T2/B1:a-B2:b,3:c
    T/B1:a,2:b,3:c
    T3/T2-T/B1:a-B2:b-B3:c
    T2/T-T/B1:a-B2:b,3:c
    T/T/B1:a,2:b,3:c
    T3/T-T/B1:a,2:b-B3:c
    # ----


ΔFtail support
--------------

In addition to pure-tree mode, the following topologies are treated specially
by `treegen trees`:

    - "øf" requests deletion of ZBigFile, that is referencing the tree,
    - "t<kv> D<data-kv>" changes both tree natively, and ZBlk data objects
      referenced via root['treegen/values'].


--------

(*) 300-500ms, see https://github.com/pypa/setuptools/issues/510.
(+) see wcfs/internal/xbtree.py
"""

from __future__ import print_function, absolute_import

import sys
from golang import func, defer, panic, b
from golang import time
from ZODB import DB
from ZODB.Connection import Connection

# XXX   vvv way interferes with recover / sys.exc_info on py2
# TODO  -> fix recover on py2
"""
try:
    from ZODB.Connection import TransactionMetaData
except ImportError: # ZODB4
    TransactionMetaData = None
"""
import ZODB.Connection
TransactionMetaData = getattr(ZODB.Connection, 'TransactionMetaData', None)

from ZODB.MappingStorage import MappingStorage
import transaction
import itertools
import random
import six

from wendelin.wcfs.internal import xbtree, xbtree_test
from wendelin.bigfile.file_zodb import ZBlk, ZBigFile
from zodbtools.util import storageFromURL, ashex

from persistent import CHANGED
from persistent.mapping import PersistentMapping

# XXX hack: set LOBTree.LOBTree -> XLOTree so that nodes are split often for
# regular tree updates (XLOTree is LOBTree with small .max_*_size). Do it this
# way so that generated database looks as if regular LOBTree was used. We use
# the hack because we cannot tune LOBTree directly.
XLOTree = xbtree_test.XLOTree
XLOTree.__module__ = 'BTrees.LOBTree'
XLOTree.__name__   = 'LOBTree'
import BTrees.LOBTree
BTrees.LOBTree.LOBTree = XLOTree
from BTrees.LOBTree import LOBTree


# Treegen works in strings domain. To help this:
#
#   - loadblkstr loads ZBlk data as   string.
#   - setblkstr  sets  ZBlk data from string.
#
# We and ΔBtail/ΔFtail tests store into ZBlks only small set of letters with
# optional digit suffix (e.g. "c" and "d4"), so using strings to handle ZBlk
# data is ok and convenient. Besides ZBlk everything else in treegen uses
# strings natively.
def loadblkstr(zblk): # -> bstr
    return b(zblk.loadblkdata())
def setblkstr(zblk, strdata):
    zblk.setblkdata(b(strdata))


# ZCtx represent treegen-level connection to ZODB.
# It wraps zconn + provides treegen-specif integration.
class ZCtx(object):
    # .zconn
    # .root
    # .valdict = zconn.root['treegen/values'] = {} v -> ZBlk(v)

    # ZCtx(zstor) opens connection to zstor and initializes .valdict.
    def __init__(zctx, zstor):
        zctx.db    = DB(zstor)
        zctx.zconn = zctx.db.open()
        zctx.root  = zctx.zconn.root()

        # init root['treegen/values'] = {} v -> ZBlk(v)
        valdict = zctx.root.get('treegen/values', None)
        if valdict is None:
            valdict = zctx.root['treegen/values'] = PersistentMapping()
        valv = 'abcdefghij'
        for v in valv:
            zblk = valdict.get(v, None)
            if zblk is not None and loadblkstr(zblk) == v:
                continue
            zblk = ZBlk()
            setblkstr(zblk, v)
            valdict[v] = zblk
        zctx.valdict = valdict
        commit('treegen/values: init %r' % valv, skipIfEmpty=True)

    # close releases resources associated with zctx.
    def close(zctx):
        zctx.zconn.close()
        zctx.db.close()

    # vdecode(vtxt) -> vobj  decodes value text into value object, e.g. 'a' -> ZBlk(a)
    # vencode(vobj) -> vtxt  encodes value object into value text, e.g. ZBlk(a) -> 'a'
    def vdecode(zctx, vtxt): # -> vobj
        return zctx.valdict[vtxt]
    def vencode(zctx, vobj): # -> vtxt
        for (k,v) in zctx.valdict.items():
            if v is vobj:
                return k
        raise KeyError("%r not found in value registry" % (vobj,))


# TreesSrv transitions ZODB tree through requested tree states.
# See top-level documentation for details.
@func
def TreesSrv(zstor, r):
    zctx = ZCtx(zstor)
    defer(zctx.close)

    ztree = zctx.root['treegen/tree'] = LOBTree()
    zfile = zctx.root['treegen/file'] = ZBigFile(blksize=4) # for ΔFtail tests
    zfile.blktab = ztree
    zdummy = zctx.root['treegen/dummy'] = PersistentMapping() # anything for ._p_changed=True
    head = commit('treegen/tree: init')
    xprint("tree.srv start @%s root=%s" % (ashex(head), ashex(ztree._p_oid)))
    treetxtPrev = zctx.ztreetxt(ztree)

    for treetxt in xreadlines(r):
        subj = "treegen/tree: %s" % treetxt

        # ø commands to delete the tree
        if treetxt == "ø":
            head = commitDelete(ztree, subj)
            xprint("%s" % ashex(head))
            continue

        # øf command to delete the file
        if treetxt == "øf":
            head = commitDelete(zfile, subj)
            xprint("%s" % ashex(head))
            continue

        # make sure we continue with undeleted ztree/zfile
        if deleted(ztree):
            undelete(ztree)
        if deleted(zfile):
            undelete(zfile)

        # t... D... commands to natively commit updates to tree and values
        if treetxt.startswith('t'):
            t, D = treetxt.split()
            assert D.startswith('D')
            kv = kvDecode(t[1:], zctx.vdecode)
            zv = _kvDecode(D[1:], kdecode=lambda ktxt: ktxt, vdecode=lambda vtxt: vtxt)
            patch(ztree,    diff(ztree, kv), kv)

            # ~ patch(valdict, diff(valdict,zv))  but sets zblk.value on change
            valdict = zctx.root['treegen/values']
            vkeys = set(valdict.keys())
            vkeys.update(zv.keys())
            for k in vkeys:
                zblk = valdict.get(k)
                v1 = None
                if zblk is not None:
                    v1 = loadblkstr(zblk)
                v2 = zv.get(k)
                if v1 != v2:
                    if v1 is None:
                        zblk = ZBlk()
                        valdict[k] = zblk
                    if v2 is not None:
                        setblkstr(zblk, v2)
                        zblk._p_changed = True
                    elif v2 is None:
                        del valdict[k]

            zdummy._p_changed = True # alayws non-empty commit
            head = commit(subj)
            xprint("%s" % ashex(head))
            continue

        # everything else is considerd to be a tree topology

        # mark something as changed if the same topology is requested twice.
        # this ensures we can actually make a non-empty commit
        if treetxt == treetxtPrev:
            zdummy._p_changed = True
        treetxtPrev = treetxt

        tree = zctx.TopoDecode(treetxt)

        # treekv[k]=v for all k
        # do tree.keys() via walkBFS
        treekv = {}
        for level in xbtree._walkBFS(tree):
            for node in level:
                if isinstance(node, xbtree.Bucket):
                    if node.valuev is None:
                        panic("%s: tree must be {key->value}, not set" % treetxt)
                    assert len(node.keyv) == len(node.valuev)
                    for (k,v) in zip(node.keyv, node.valuev):
                        treekv[k] = v

        # change ztree to requested kv
        d = diff(ztree, treekv)
        patch(ztree, d, treekv)

        # restructure ztree to requested topology
        xbtree.Restructure(ztree, tree)

        # commit tree to storage
        head = commit(subj)

        # verify what was persisted to storage is indeed what we wanted to persist
        zctx.zconn.cacheMinimize()
        treetxt_onstor = zctx.ztreetxt(ztree)
        if treetxt_onstor != treetxt:
            panic("BUG: tree wrongly saved to storage:\nsaved:     %s\nrequested: %s" %
                    (treetxt_onstor, treetxt))
        # verify saved tree consistency
        xbtree.zcheck(ztree)

        # ok
        xprint("%s" % ashex(head))


# AllStructsSrv is server version of AllStructs.
@func
def AllStructsSrv(r):
    xprint('# allstructs.srv start')
    for req in xreadlines(r):
        # maxdepth maxsplit n(/seed) kv
        maxdepth, maxsplit, n, kvtxt = req.split()
        maxdepth = int(maxdepth)
        maxsplit = int(maxsplit)
        seed = None
        if '/' in n:
            n, seeds = n.split('/')
            seed = int(seeds)
        n = int(n)
        if kvtxt == 'ø': kvtxt = ''

        AllStructs(kvtxt, maxdepth, maxsplit, n, seed)
        xprint('# ----')

# AllStructs generates subset of all possible topologies for a tree specified by kv dict.
# See top-level documentation for details.
@func
def AllStructs(kvtxt, maxdepth, maxsplit, n, seed=None):
    zstor = MappingStorage() # in RAM storage to create native ZODB topologies
    zctx = ZCtx(zstor)
    defer(zctx.close)

    kv = kvDecode(kvtxt, zctx.vdecode)

    print("# allstructs %s" % kvtxt)

    # create the tree
    ztree = zctx.root['ztree'] = LOBTree()
    commit('init')

    # initial kv state with topology prepared as ZODB would do natively
    patch(ztree, diff({}, kv), verify=kv)
    if kv == {}: ztree._p_changed = True # to avoid empty commit - see TreesSrv
    commit('kv')
    tstruct0 = xbtree.StructureOf(ztree)

    # seed
    if seed is None:
        seed = time.now()
    seed = int(seed)
    random.seed(seed)
    print("# maxdepth=%d maxsplit=%d n=%d seed=%d" % (maxdepth, maxsplit, n, seed))

    # emit native + n-1 random samples from all tree topologies that can represent kv
    tstructv = rsample(xbtree.AllStructs(kv.keys(), maxdepth, maxsplit, kv=kv), n-1)
    if tstruct0 in tstructv: tstructv.remove(tstruct0) # avoid dups
    tstructv.insert(0, tstruct0)

    for tstruct in tstructv:
        print(zctx.TopoEncode(tstruct))


# rsample returns k random samples from seq.
# it differs from random.sample in that it does not keep whole list(seq) in memory.
def rsample(seq, k): # -> [] of items; len <= k
    # based on https://stackoverflow.com/a/35671225/9456786
    # https://en.wikipedia.org/wiki/Reservoir_sampling
    if k <= 0:
        raise ValueError("negative sample size")
    it = iter(seq)
    sample = list(itertools.islice(it, k))
    random.shuffle(sample)

    i = k
    for item in it:
        i += 1
        j = random.randrange(i) # [0,i)
        if j < k:
            sample[j] = item
    return sample


# kvEncode encodes key->value mapping into text.
# e.g. {1:'a', 2:'b'} -> '1:a,2:b'
def kvEncode(kvDict, vencode): # -> kvText
    retv = []
    for k in sorted(kvDict.keys()):
        v = kvDict[k]
        retv.append('%d:%s' % (k, vencode(v)))
    return ','.join(retv)

# kvDecode decodes key->value mapping from text.
# e.g. '1:a,2:b' -> {1:'a', 2:'b'}
def kvDecode(kvText, vdecode): # -> kvDict
    return _kvDecode(kvText, int, vdecode)

def _kvDecode(kvText, kdecode, vdecode): # -> kvDict
    if kvText in ("", "ø"):
        return {}
    kv = {}
    for item in kvText.split(','):
        ktxt, vtxt = item.split(':')
        k = kdecode(ktxt)
        v = vdecode(vtxt)
        if k in kv:
            raise ValueError("key %s present multiple times" % k)
        kv[k] = v
    return kv


# diff computes difference in between mappings d1 and d2.
DEL = 'ø'
def diff(d1, d2): # -> [] of (k,v) to change; DEL means del[k]
    delta = []
    keys = set(d1.keys())
    keys.update(d2.keys())
    for k in sorted(keys):
        v1 = d1.get(k, DEL)
        v2 = d2.get(k, DEL)
        if v1 is not v2:
            delta.append((k,v2))
    return delta

# patch changes mapping d according to diff.
# diff = [] of (k,v) to change; DEL means del[k]
def patch(d, diff, verify):
    for (k,v) in diff:
        if v == DEL:
            del d[k]
        else:
            d[k] = v

    if verify is None:
        return
    keys  = set(d.keys())
    keyok = set(verify.keys())
    if keys != keyok:
        panic("patch: verify: different keys: %s" % keys.symmetric_difference(keyok))
    for k in keys:
        if d[k] is not verify[k]:
            panic("patch: verify: [%d] different: got %r;  want %r" % (k, d[k], verify[k]))


# commit commits current transaction with description.
def commit(description, skipIfEmpty=False): # -> tid | None
    txn = transaction.get()
    if skipIfEmpty and len(txn._resources) == 0:
        return None
    txn.description = description
    # XXX hack to retrieve committed transaction ID via ._p_serial of object changed in this transaction
    assert len(txn._resources) == 1, txn._resources
    zconn = txn._resources[0]
    assert isinstance(zconn, Connection)
    assert len(zconn._registered_objects) > 0
    # NOTE objects in zconn._registered_objects are not necessarily in CHANGED state:
    # https://github.com/zopefoundation/ZODB/blob/5.6.0-19-gdad778016/src/ZODB/Connection.py#L516-L530
    changed_objects = [obj for obj in zconn._registered_objects if obj._p_state == CHANGED]
    assert len(changed_objects) > 0
    obj = changed_objects[0]
    txn.commit()
    return obj._p_serial

# commitDelete commits deletion of obj with description.
def commitDelete(obj, description): # -> tid
    txn = transaction.get()
    zstor = obj._p_jar._db.storage

    # deleteObject works only at IStorage level, and at that low level
    # zstor requires ZODB.IStorageTransactionMetaData not txn (ITransaction)
    if TransactionMetaData is not None:
        txn_stormeta = TransactionMetaData(txn.user, description, txn.extension)
    else:
        txn_stormeta = txn # ZODB4
    zstor.tpc_begin(txn_stormeta)
    zstor.deleteObject(obj._p_oid, obj._p_serial, txn_stormeta)
    zstor.tpc_vote(txn_stormeta)
    _ = []
    zstor.tpc_finish(txn_stormeta, lambda tid: _.append(tid))
    assert len(_) == 1, _
    tid = _[0]

    # object in the database is now current as of committed tid
    # adjust obj's serial so that the next change to it (possibly recreating
    # the object), does not fail with ConflictError.
    obj._p_serial = tid

	# reset transaction to a new one
    transaction.begin()

    obj._v_deleted = True
    return tid

# deleted reports whether obj was deleted via commitDelete.
def deleted(obj): # -> bool
    return getattr(obj, '_v_deleted', False)

# undelete forces recreation for obj that was previously deleted via commitDelete.
def undelete(obj):
    obj._p_changed = True
    del obj._v_deleted


# ztreetxt returns text representation of a ZODB tree.
@func(ZCtx)
def ztreetxt(zctx, ztree): # -> txt
    assert ztree._p_jar is zctx.zconn
    return zctx.TopoEncode(xbtree.StructureOf(ztree))

# TopoEncode and TopoDecode perform trees encode/decode with values registry
# taken from zctx.valdict.
@func(ZCtx)
def TopoEncode(zctx, tree):
    return xbtree.TopoEncode(tree, zctx.vencode)
@func(ZCtx)
def TopoDecode(zctx, text):
    return xbtree.TopoDecode(text, zctx.vdecode)


# xreadlines iterates through lines in r skipping comments.
def xreadlines(r):
    while 1:
        l = r.readline()
        if l == '':
            break # EOF
        l = l.rstrip()  # trim trailing \n
        if l.startswith('#'):
            continue # skip comments
        yield l


@func
def cmd_allstructs(argv):
    if argv == ['-h']:
        print("Usage: cat requests |treegen allstructs",    file=sys.stderr)
        print("       treegen allstructs <request>",        file=sys.stderr)
        sys.exit(0)

    r = sys.stdin
    if len(argv) != 0:
        r = six.StringIO(' '.join(argv))

    AllStructsSrv(r)

@func
def cmd_trees(argv):
    if argv in ([], ['-h']):
        print("Usage: cat trees |treegen trees <zurl>",     file=sys.stderr)
        print("       treegen trees <zurl> <tree>+",        file=sys.stderr)
        sys.exit(1 if argv==[] else 0)

    zurl  = argv[0]
    zstor = storageFromURL(zurl)
    defer(zstor.close)

    r = sys.stdin
    treev = argv[1:]
    if len(treev) != 0:
        r = six.StringIO('\n'.join(treev))

    TreesSrv(zstor, r)


# xprint prints msg to stdout and flushes it.
def xprint(msg):
    print(msg)
    sys.stdout.flush()


cmdRegistry = {
    'allstructs':   cmd_allstructs,
    'trees':        cmd_trees,
}

def main():
    if len(sys.argv) < 2:
        print("Usage: treegen <command> ...", file=sys.stderr)
        sys.exit(1)

    cmd  = cmdRegistry.get(sys.argv[1], None)
    if cmd is None:
        print("E: treegen: unknown command %r" % sys.argv[1])
        sys.exit(1)
    argv = sys.argv[2:]
    cmd(argv)


if __name__ == '__main__':
    main()
