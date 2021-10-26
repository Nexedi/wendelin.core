# -*- coding: utf-8 -*-
# Copyright (C) 2020-2021  Nexedi SA and Contributors.
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
"""Package xbtree provides utilities for inspecting/manipulating internal
structure of integer-keyed BTrees.

It is primarily used to help verify ΔBTail in WCFS.

- `Tree` represents a tree node.
- `Bucket` represents a bucket node.
- `StructureOf` returns internal structure of ZODB BTree represented as Tree
  and Bucket nodes.
- `Restructure` reorganizes ZODB BTree instance according to specified topology
  structure.

- `AllStructs` generates all possible BTree topology structures with given keys.


Topology encoding
-----------------

Topology encoding provides way to represent structure of a Tree as path-like string.

TopoEncode converts Tree into its topology-encoded representation, while
TopoDecode decodes topology-encoded string back into Tree.

The following example illustrates topology encoding represented by string
"T3/T-T/B1-T5/B-B7,8,9":

      [ 3 ]             T3/         represents Tree([3])
       / \
     [ ] [ ]            T-T/        represents two empty Tree([])
      ↓   ↓
     |1|[ 5 ]           B1-T5/      represent Bucket([1]) and Tree([5])
         / \
        || |7|8|9|      B-B7,8,9    represents empty Bucket([]) and Bucket([7,8,9])


Topology encoding specification:

A Tree is encoded by level-order traversal, delimiting layers with "/".
Inside a layer Tree and Bucket nodes are signalled as

    "T<keys>"           ; Tree
    "B<keys>"           ; Bucket with only keys
    "B<keys+values>"    ; Bucket with keys and values

Keys are represented as ","-delimited list of integers. For example Tree
or Bucket with [1,3,5] keys are represented as

    "T1,3,5"        ; Tree([1,3,5])
    "B1,3,5"        ; Bucket([1,3,5])

Keys+values are represented as ","-delimited list of "<key>:<value>" pairs. For
example Bucket corresponding to {1:1, 2:4, 3:9} is represented as

    "B1:1,2:4,3:9"  ; Bucket([1,2,3], [1,4,9])

Empty keys+values are represented as ":" - an empty Bucket for key->value
mapping is represented as

    "B:"            ; Bucket([], [])

Nodes inside one layer are delimited with "-". For example a layer consisting
of an empty Tree, a Tree with [1,3] keys, and Bucket with [4,5] keys is
represented as

    "T-T1,3-B4,5"   ; layer with Tree([]), Tree([1,3]) and Bucket([4,5])

A layer consists of nodes that are followed by node-node links from upper layer
in left-to-right order.


Visualization
-------------

The following visualization utilities are provided to help understand BTrees
better:

- `topoview` displays BTree structure given its topology-encoded representation.
- `Tree.graphviz` returns Tree graph representation in dot language.
"""

from __future__ import print_function, absolute_import

from BTrees import check as zbcheck
from golang import func, panic, defer
from golang.gcompat import qq
import itertools
import re
inf = float('inf')

import numpy as np
import scipy.optimize
import copy


# Tree represents a tree node.
class Tree(object):
    #   .keyv       () of keys
    #   .children   () of children  len(.children) == len(.keyv) + 1
    def __init__(t, keyv, *children):
        keyv = tuple(keyv)
        assert len(children) == len(keyv) + 1, (keyv, children)
        _assertIncv(keyv)
        if len(children) > 0:
            # assert all children are of the same type
            childtypes = set([type(_) for _ in children])
            if len(childtypes) != 1:
                panic("children are of distinct types: %s" % (childtypes,))

            # assert type(child) is Tree | Bucket
            childtype = childtypes.pop()
            assert childtype in (Tree, Bucket), childtype

            # assert children keys are consistent
            v = (-inf,) + keyv + (+inf,)
            for i, (klo, khi) in enumerate(zip(v[:-1], v[1:])): # (klo, khi) = [] of (k_i, k_{i+1})
                for k in children[i].keyv:
                    if not (klo <= k < khi):
                        panic("children[%d] key %d is outside key range [%s, %s)" % (i, k, klo, khi))

        t.keyv     = keyv
        t.children = tuple(children)

    def __hash__(t):
        return hash(t.keyv) ^ hash(t.children)
    def __ne__(a, b):
        return not (a == b)
    def __eq__(a, b):
        if not isinstance(b, Tree):
            return False
        return (a.keyv == b.keyv) and (a.children == b.children)

    def __str__(t):
        s = "T([" + ",".join(['%s' % _ for _ in t.keyv]) + "]"
        for ch in t.children:
            s += ",\n"
            s += _indent(' '*4, str(ch))
        s += ")"
        return s
    __repr__ = __str__

    # copy returns a deep copy of the tree.
    # if onlyKeys=Y buckets in returned tree will contain only keys not values.
    def copy(t, onlyKeys=False):
        return Tree(t.keyv, *[_.copy(onlyKeys) for _ in t.children])

    # firstbucket returns Bucket reachable through leftmost child links.
    def firstbucket(t):
        child0 = t.children[0]
        if isinstance(child0, Bucket):
            return child0
        assert isinstance(child0, Tree)
        return child0.firstbucket()


# Bucket represents a bucket node.
class Bucket(object):
    #   .keyv   () of keys
    #   .valuev None | () of values    len(.valuev) == len(.keyv)
    def __init__(b, keyv, valuev):
        _assertIncv(keyv)
        b.keyv = tuple(keyv)
        if valuev is None:
            b.valuev = None
        else:
            assert len(valuev) == len(keyv)
            b.valuev = tuple(valuev)

    def __hash__(b):
        return hash(b.keyv) ^ hash(b.valuev)
    def __ne__(a, b):
        return not (a == b)
    def __eq__(a, b):
        if not isinstance(b, Bucket):
            return False
        return (a.keyv == b.keyv and a.valuev == b.valuev)

    def __str__(b):
        if b.valuev is None:
            kvv = ['%s' % k for k in b.keyv]
        else:
            assert len(b.keyv) == len(b.valuev)
            if len(b.keyv) == 0:
                kvv = [':']
            else:
                kvv = ['%s:%s' % (k,v) for (k,v) in zip(b.keyv, b.valuev)]
        return "B" + ','.join(kvv)
    __repr__ = __str__

    def copy(b, onlyKeys=False):
        if onlyKeys:
            return Bucket(b.keyv, None)
        else:
            return Bucket(b.keyv, b.valuev)


# StructureOf returns internal structure of a ZODB BTree.
#
# The structure is represented as Tree and Bucket nodes.
# If onlyKeys=Y values of the tree are not represented in returned structure.
def StructureOf(znode, onlyKeys=False):
    ztype = _zclassify(znode)

    if ztype.is_zbucket:
        keys, values = zbcheck.crack_bucket(znode, ztype.is_map)
        if (not ztype.is_map) or onlyKeys:
            return Bucket(keys, None)
        else:
            return Bucket(keys, values)

    if ztype.is_ztree:
        kind, keys, children = zbcheck.crack_btree(znode, ztype.is_map)
        if kind == zbcheck.BTREE_EMPTY:
            return Tree([], Bucket([], None if ((not ztype.is_map) or onlyKeys) else []))

        if kind == zbcheck.BTREE_ONE:
            b = znode._bucket_type()
            b.__setstate__(keys)    # it is keys+values for BTREE_ONE case
            return Tree([], StructureOf(b, onlyKeys))

        if kind == zbcheck.BTREE_NORMAL:
            return Tree(keys, *[StructureOf(_, onlyKeys) for _ in children])

        panic("bad tree kind %r" % kind)

    panic("unreachable")


# Restructure reorganizes ZODB BTree instance (not Tree) according to specified
# topology structure.
#
# The new structure should be usually given with key-only buckets.
# If new structure comes with values, values associated with keys must not be changed.
#
# NOTE ZODB BTree package does not tolerate structures with empty BTree nodes
# except for the sole single case of empty tree.
@func
def Restructure(ztree, newStructure):
    _ = _zclassify(ztree)
    assert _.is_ztree
    assert isinstance(newStructure, Tree)

    newStructOnlyKeys = newStructure.copy(onlyKeys=True)

    from_ = TopoEncode(StructureOf(ztree, onlyKeys=True))
    to_   = TopoEncode(newStructOnlyKeys)
    """
    def _():
        exc = recover() # FIXME for panic - returns unwrapped arg, not PanicError
        if exc is not None:
            # FIXME %w creates only .Unwrap link, not with .__cause__
            raise fmt.Errorf("Restructure %s -> %s: %w", from_, to_, exc)
    defer(_)
    """
    def _(): # XXX hack
        exc = sys.exc_info()[1]
        if exc is not None:
            assert len(exc.args) == 1
            exc.args = ("Restructure %s -> %s: %r" % (from_, to_, exc.args[0]),)
    defer(_)


    ztreeType   = type(ztree)
    zbucketType = ztreeType._bucket_type

    zcheck(ztree) # verify ztree before our tweaks

    # {} with all k->v from ztree
    kv = dict(ztree)

    # walk original and new structures level by level.
    # push buckets till the end, then
    # for each level we have A1...An "old" nodes, and B1...Bm "new" nodes.
    # if n == m - map A-B 1-to-1
    # if n < m  - we have to "insert" (m-n) new nodes
    # if n > m  - we have to "forget" (n-m) old nodes.
    # The process of insert/forget is organized as follows:
    # every node from B set will in the end be associated to a node from A set.
    # to make this association we:
    # - compute D(Ai,Bj) where D is distance in between ranges
    # - find solution to linear assignment problem A <- B with the cost given by D
    #   https://en.wikipedia.org/wiki/Assignment_problem
    #
    #                       2                2
    # D(A,B) = (A.lo - B.lo)  + (A.hi - B.hi)

    # we will modify nodes from new set:
    # - node.Z              will point to associated znode
    # - bucket.next_bucket  will point to bucket that is coming with next keys in the tree
    tnew = newStructure.copy()  # NOTE _with_ values

    # assign assigns tree nodes from RNv to ztree nodes from RZv in optimal way.
    # Bj ∈ RNv is mapped into Ai ∈ RZv such that that sum_j D(A_i, Bj) is minimal.
    # it is used to associate nodes in tnew to ztree nodes.
    def assign(RZv, RNv):
        #print()
        #print('assign %s <- %s' % (RZv, RNv))
        for rzn in RZv:
            assert isinstance(rzn, _NodeInRange)
            assert isinstance(rzn.node, (ztreeType, zbucketType))
        for rn in RNv:
            assert isinstance(rn, _NodeInRange)
            assert isinstance(rn.node, (Tree,Bucket))

            assert not hasattr(rn.node, 'Z')
            rn.node.Z = None

        # D(a,b)
        def D(a, b):
            def fin(v): # TeX hack: inf = 10000
                if v == +inf: return +1E4
                if v == -inf: return -1E4
                assert abs(v) < 1E3
                return v
            def d2(x,y):
                return (fin(x)-fin(y))**2
            return d2(a.range.klo, b.range.klo) + \
                   d2(a.range.khi, b.range.khi)

        # cost matrix
        C = np.zeros((len(RNv), len(RZv)))
        for j in range(len(RNv)):       # "workers"
            for i in range(len(RZv)):   # "jobs"
                C[j,i] = D(RZv[i], RNv[j])
        #print('\nCOST:')
        #print(C)

        # find the assignments.
        # TODO try to avoid scipy dependency; we could also probably make
        # assignments more efficiently taking into account that Av and Bv are
        # key↑ and the property of D function is so that if B2 > B1 (all keys
        # in B2 > all keys in B1) and A < B1.hi, then D(B2, A) > D(B1, A).
        jv, iv = scipy.optimize.linear_sum_assignment(C)
        for (j,i) in zip(jv, iv):
            RNv[j].node.Z = RZv[i].node

        # if N(old) > N(new) - some old nodes won't be linked to  (it is ok)
        # if N(old) < N(new) - some new nodes won't be linked from - link them
        # to newly created Z nodes.
        for j2 in range(len(RNv)):
            if j2 not in jv:
                n = RNv[j2].node
                assert n.Z is None
                assert isinstance(n, (Tree,Bucket))
                if isinstance(n, Tree):
                    zn = ztreeType()
                else:
                    zn = zbucketType()
                n.Z = zn

        if len(RZv) == len(RNv):
            # assert the result is 1-1 mapping
            for j in range(len(RNv)):
                if RNv[j].node.Z is not RZv[j].node:
                    panic("BUG: assign: not 1-1 mapping:\n RZv: %s\nRNv: %s" % (RZv, RNv))

        # XXX assert assignments are in key↑ order ?


    zrlevelv = list(__zwalkBFS(ztree))   # [] of _NodeInRange
    rlevelv  = list( __walkBFS(tnew))    # [] of _NodeInRange

    # associate every non-bucket node in tnew to a znode,
    # extract bucket nodes.
    zrbucketv = [] # of _NodeInRange
    rbucketv  = [] # of _NodeInRange
    rlevelv_orig = copy.copy(rlevelv)
    while len(rlevelv) > 0 or len(zrlevelv) > 0:
        rlevel  = []
        zrlevel = []
        if len(rlevelv) > 0:
            rlevel  = rlevelv .pop(0)
        if len(zrlevelv) > 0:
            zrlevel = zrlevelv.pop(0)

        # filter-out buckets to be processed in the end
        _, zrlevel = _filter2(zrlevel, lambda zrn: isinstance(zrn.node, zbucketType))
        zrbucketv.extend(_)
        _,  rlevel = _filter2(rlevel,  lambda  rn: isinstance(rn.node,  Bucket))
        rbucketv.extend(_)

        if len(rlevel) == 0:
            continue

        # associate nodes to znodes
        assign(zrlevel, rlevel)

    assert zrlevelv == []
    assert  rlevelv == []

    # order queued buckets and zbuckets by key↑
    zrbucketv.sort(key = lambda rn: rn.range.klo)
    rbucketv .sort(key = lambda rn: rn.range.klo)

    # verify that old keys == new keys
    zkeys = set()
    keys  = set()
    for _ in zrbucketv: zkeys.update(_.node.keys())
    for _ in  rbucketv:  keys.update(_.node.keyv)
    assert set(kv.keys()) == zkeys, (set(kv.keys()), zkeys)
    if zkeys != keys:
        raise ValueError("new keys != old keys\ndiff: %s" % zkeys.symmetric_difference(keys))

    # chain buckets via .next_bucket
    assert len(rbucketv) > 0
    for i in range(len(rbucketv)-1):
        assert rbucketv[i].range.khi <= rbucketv[i+1].range.klo
        rbucketv[i].node.next_bucket = rbucketv[i+1].node
    rbucketv[-1].node.next_bucket = None

    # associate buckets to zbuckets
    assign(zrbucketv, rbucketv)


    # set znode states according to established tnew->znode association
    rlevelv = rlevelv_orig
    for rnodev in reversed(rlevelv): # bottom -> up
        for rn in rnodev:
            node = rn.node
            assert isinstance(node, (Tree,Bucket))

            # https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeTemplate.c#L1087
            if isinstance(node, Tree):
                # special case for empty tree and tree with just one bucket without oid.
                #
                # we cannot special case e.g. T/T/B because on loading ZODB
                # wants to have !NULL T.firstbucket, and so if T/B is
                # represented as empty or as T with embedded B, for top-level T
                # there won't be a way to link to firstbucket and
                # Tree.__setstate__ will raise "TypeError: No firstbucket in
                # non-empty BTree".
                zstate = unset = object()
                if len(node.keyv) == 0:
                    child = node.children[0]
                    if len(rlevelv) == 2: # only 2 levels
                        assert len(rlevelv[0]) == 1     # top-one has empty tree
                        assert rlevelv[0][0].node is node
                        assert isinstance(child, Bucket)# bottom-one has 1 bucket
                        assert len(rlevelv[1]) == 1
                        assert rlevelv[1][0].node is child

                        # empty bucket -> empty tree
                        if len(child.keyv) == 0:
                            zstate = None

                        # tree with single bucket without oid -> tree with embedded bucket
                        elif child.Z._p_oid is None:
                            zstate = ((child.Z.__getstate__(),),)

                    # more than 2 levels. For .../T/B B._p_oid must be set - else
                    # T.__getstate__ will embed B instead of preserving what we
                    # pass into T.__setstate__
                    else:
                        if isinstance(child, Bucket) and child.Z._p_oid is None:
                            if ztree._p_jar is None:
                                raise ValueError("Cannot generate .../T/B topology not under ZODB connection")
                            ztree._p_jar.add(child.Z)
                            assert child.Z._p_oid is not None

                if zstate is unset:
                    # normal tree node
                    zstate = ()
                    assert len(node.children) == len(node.keyv) + 1
                    zstate += (node.children[0].Z,)
                    for (child, k) in zip(node.children[1:], node.keyv):
                        zstate += (k, child.Z)  # (child0, k0, child1, k1, ..., childN, kN, childN+1)
                    zstate = (zstate,)

                    # firstbucket
                    #print('  (firstbucket -> B %x)' % (id(node.firstbucket().Z),))
                    zstate += (node.firstbucket().Z,)

            # https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BucketTemplate.c#L1195
            if isinstance(node, Bucket):
                # if Bucket was specified with values - verify k->v do not
                # change from what was in original tree.
                if node.valuev is not None:
                    assert len(node.keyv) == len(node.valuev)
                    for (k,v) in zip(node.keyv, node.valuev):
                        if kv[k] is not v:
                            raise ValueError("target bucket changes [%d] %r -> %r" % (k, kv[k], v))

                zstate = ()
                for k in node.keyv:
                    zstate += (k, kv.pop(k))            # (k1, v1, k2, v2, ..., kN, vN)
                zstate = (zstate,)

                if node.next_bucket is not None:        # next
                    zstate += (node.next_bucket.Z,)

            #print('%s %x: ZSTATE: %r' % ('T' if _zclassify(node.Z).is_ztree else 'B', id(node.Z), zstate,))
            zstate_old = node.Z.__getstate__()
            if zstate_old != zstate:
                node.Z.__setstate__(zstate)
                node.Z._p_changed = True
                zstate2 = node.Z.__getstate__()
                if zstate2 != zstate:
                    panic("BUG: node.__getstate__ returns not what "
                          "we passed into node.__setstate__.\nnode: %r\n"
                          "__setstate__ <- %r\n__getstate__ -> %r" % (node.Z, zstate, zstate2))


    assert tnew.Z is ztree
    assert len(kv) == 0     # all keys must have been popped

    zcheck(ztree) # verify ztree after our tweaks
    tstruct = StructureOf(ztree, onlyKeys=True)
    if tstruct != newStructOnlyKeys:
        panic("BUG: result structure is not what was"
              " requested:\n%s\n\nwant:\n%s" % (tstruct, newStructOnlyKeys))


# AllStructs generates subset of all possible BTree structures for BTrees with
# specified keys and btree depth up-to maxdepth. Each tree node is split by
# up-to maxsplit points.
#
# kv is {} that defines values to be linked from buckets.
# By default kv=None and generated trees contains key-only Buckets.
#
# If allowEmptyBuckets=y tree structures with empty buckets are also generated.
# By default, except for empty-tree case, only tree structures with non-empty
# buckets are generated, because ZODB BTree package misbehaves if it finds an
# empty bucket in a trees.  ZODB.BTree._check also asserts if bucket is empty.
def AllStructs(keys, maxdepth, maxsplit, allowEmptyBuckets=False, kv=None): # -> i[] of Tree
    assert isinstance(maxdepth, int); assert maxdepth >= 0
    assert isinstance(maxsplit, int); assert maxsplit >= 0
    ks = set(keys)
    for k in keys:
        assert isinstance(k, int)
        assert k in ks  # no duplicates
        ks.remove(k)
    keyv = list(keys)
    keyv.sort()

    # initial [lo, hi) covering keys and such that split points will be there withing +-1 of min/max key
    if len(keyv) > 0:
        klo = keyv[0]  - 1 - 1
        khi = keyv[-1] + 1 + 1   # hi is ")", not "]"
    else:
        # the only possible case for empty tree is T/B
        if not allowEmptyBuckets:
            yield Tree([], Bucket([], None if kv is None else []))
            return

        # XXX ok? (ideally should be -inf,+inf)
        klo = 0
        khi = 0

    for tree in _allStructs(klo, khi, keyv, maxdepth, maxsplit, allowEmptyBuckets, kv):
        yield tree


def _allStructs(klo, khi, keyv, maxdepth, maxsplit, allowEmptyBuckets, kv):
    assert klo <= khi
    _assertIncv(keyv)
    if len(keyv) > 0:
        assert klo <= keyv[0]
        assert        keyv[-1] < khi

    #print('_allStructs [%s, %s)  keyv: %r,  maxdepth=%d, maxsplit=%d' %
    #        (klo, khi, keyv, maxdepth, maxsplit))

    for nsplit in range(0, maxsplit+1):
        if not allowEmptyBuckets:
            iksplitv = _iterSplitKeyvByN(klo, khi, keyv, nsplit)
        else:
            iksplitv = _iterSplitByN(klo, khi, nsplit)

        for ksplitv in iksplitv:
            # ksplitv = [klo, s1, s2, ..., sN, khi]
            #print('ksplitv: %r' % ksplitv)

            # emit Tree -> Buckets
            children = []
            for (xlo, xhi) in zip(ksplitv[:-1], ksplitv[1:]): # (klo, s1), (s1, s2), ..., (sN, khi)
                bkeyv = _keyvSliceBy(keyv, xlo, xhi)
                if not allowEmptyBuckets:
                    assert len(bkeyv) > 0
                valuev = None
                if kv is not None:
                    valuev = [kv[k] for k in bkeyv]
                children.append(Bucket(bkeyv, valuev))
            else:
                yield Tree(ksplitv[1:-1], *children) # (s1, s2, ..., sN)

            # emit Tree -> Trees -> ...
            if maxdepth == 0:
                continue
            ichildrenv = [] # of _allStructs for each child link
            for (xlo, xhi) in zip(ksplitv[:-1], ksplitv[1:]): # (klo, s1), (s1, s2), ..., (sN, khi)
                ckeyv = _keyvSliceBy(keyv, xlo, xhi)
                if not allowEmptyBuckets:
                    assert len(ckeyv) > 0
                ichildrenv.append( _allStructs(
                    xlo, xhi, ckeyv, maxdepth - 1, maxsplit, allowEmptyBuckets, kv))
            else:
                for children in itertools.product(*ichildrenv):
                    yield Tree(ksplitv[1:-1], *children) # (s1, s2, ..., sN)


# _keyvSliceBy returns [] of keys from keyv : k ∈ [klo, khi)
def _keyvSliceBy(keyv, klo, khi):
    assert klo <= khi
    _assertIncv(keyv)
    return list([k for k in keyv if (klo <= k < khi)])

# _iterSplitByN iterates through all nsplit splitting of [lo, hi) range.
#
# lo < si < s_{i+1} < hi
def _iterSplitByN(lo, hi, nsplit): # -> i[] of [lo, s1, s2, ..., sn, hi)
    assert lo <= hi
    assert nsplit >= 0

    if nsplit == 0:
        yield [lo, hi]
        return

    for s in range(lo+1, hi):   # [lo+1, hi-1]
        for tail in _iterSplitByN(s, hi, nsplit-1):
            yield [lo] + tail

# _iterSplitKeyvByN is similar to _iterSplitByN but makes sure that every
# splitted range contains at least one key from keyv.
def _iterSplitKeyvByN(lo, hi, keyv, nsplit): # -> i[] of [lo, s1, s2, ..., sn, hi)
    #print('_iterSplitKeyvByN [%s, %s) keyv=%r nsplit=%r' % (lo, hi, keyv, nsplit))
    assert lo <= hi
    assert 0 <= nsplit
    _assertIncv(keyv)
    assert lo <= keyv[0]
    assert       keyv[-1] < hi

    if nsplit >= len(keyv):
        return  # no split exists
    if nsplit == 0:
        yield [lo, hi]
        return

    for i in range(len(keyv)-nsplit):
        for s in range(keyv[i]+1, keyv[i+1]+1): # (ki, k_{i+1}]
            for tail in _iterSplitKeyvByN(s, hi, keyv[i+1:], nsplit-1):
                yield [lo] + tail


# ---- treewalk ----

# _Range represents a range under which a node is placed in its tree.
class _Range:
    # .klo
    # .khi

    def __init__(r, klo, khi):
        assert klo <= khi
        r.klo = klo
        r.khi = khi
    def __hash__(r):
        return hash(r.klo) ^ hash(r.khi)
    def __ne__(a, b):
        return not (a == b)
    def __eq__(a, b):
        if not isinstance(b, _Range):
            return False
        return (a.klo == b.klo) and (a.khi == b.khi)

    def __str__(r):
        return "[%s, %s)" % (r.klo, r.khi)
    __repr__ = __str__

# _NodeInRange represents a node (node or znode) coming under key range in its tree.
class _NodeInRange:
    # .range
    # .node
    def __init__(nr, r, node):
        nr.range = r
        nr.node  = node

    def __str__(nr):
        return "%s%s" % (nr.range, nr.node)
    __repr__ = __str__


# _walkBFS walks tree in breadth-first order layer by layer.
def _walkBFS(tree): # i[] of [](of nodes on each level)
    for level in __walkBFS(tree):
        yield tuple(rn.node for rn in level)

# _zwalkBFS, similarly to _walkBFS, walks ZODB BTree in breadth-first order layer by layer.
def _zwalkBFS(ztree): # i[] of [](of nodes on each level)
    for zlevel in __zwalkBFS(ztree):
        yield tuple(rn.node for rn in zlevel)

def __walkBFS(tree): # i[] of [](of _NodeInRange on each level)
    assert isinstance(tree, Tree)
    currentq = []
    nextq    = [_NodeInRange(_Range(-inf,+inf), tree)]

    while len(nextq) > 0:
        yield tuple(nextq)
        currentq = nextq
        nextq    = []
        while len(currentq) > 0:
            rn = currentq.pop(0)
            assert isinstance(rn.node, (Tree, Bucket))
            if isinstance(rn.node, Tree):
                v = (rn.range.klo,) + rn.node.keyv + (rn.range.khi,)
                rv = zip(v[:-1], v[1:])  # (klo,k1), (k1,k2), ..., (kN,khi)
                assert len(rv) == len(rn.node.children)

                for i in range(len(rv)):
                    nextq.append(_NodeInRange(_Range(*rv[i]), rn.node.children[i]))

def __zwalkBFS(ztree): # i[] of [](of _NodeInRange on each level)
    _ = _zclassify(ztree)
    assert _.is_ztree

    ztreeType    = type(ztree)
    zbucketType  = ztreeType._bucket_type

    currentq = []
    nextq    = [_NodeInRange(_Range(-inf,+inf), ztree)]

    while len(nextq) > 0:
        yield tuple(nextq)
        currentq = nextq
        nextq    = []
        while len(currentq) > 0:
            rn = currentq.pop(0)

            znode = rn.node
            ztype = _zclassify(znode)

            assert ztype.is_ztree or ztype.is_zbucket
            if ztype.is_zbucket:
                assert type(znode) is zbucketType

            if ztype.is_ztree:
                assert type(znode) is ztreeType

                kind, keyv, kids = zbcheck.crack_btree(znode, ztype.is_map)
                if kind == zbcheck.BTREE_EMPTY:
                    b = znode._bucket_type()
                    children = [b]

                elif kind == zbcheck.BTREE_ONE:
                    b = znode._bucket_type()
                    b.__setstate__(keyv)
                    keyv = []
                    children = [b]

                elif kind == zbcheck.BTREE_NORMAL:
                    children = kids

                else:
                    panic("bad tree kind %r" % kind)

                v = [rn.range.klo] + keyv + [rn.range.khi]
                rv = zip(v[:-1], v[1:])  # (klo,k1), (k1,k2), ..., (kN,khi)
                assert len(rv) == len(children)

                for i in range(len(rv)):
                    nextq.append(_NodeInRange(_Range(*rv[i]), children[i]))


# ---- topology encoding ----

# TopoEncode returns topology encoding for internal structure of the tree.
#
# Vencode specifies way to encode values referred-to by buckets.
# See top-level docstring for description of topology encoding.
def TopoEncode(tree, vencode=lambda v: '%d' % v):
    assert isinstance(tree, Tree)
    topo = ''

    # vdecode to be used in the verification at the end
    vencoded = {} # vencode(vobj) -> vobj
    def vdecode(vtxt):
        return vencoded[vtxt]

    # breadth-first traversal of the tree with '/' injected in between layers
    for nodev in _walkBFS(tree):
        if len(topo) != 0:
            topo += '/'
        tnodev = []
        for node in nodev:
            assert isinstance(node, (Tree, Bucket))
            tnode = ('T' if isinstance(node, Tree) else 'B')
            if isinstance(node, Bucket) and node.valuev is not None:
                # bucket with key and values
                assert len(node.keyv) == len(node.valuev)
                if len(node.keyv) == 0:
                    tnode += ':'
                else:
                    vtxtv = []
                    for v in node.valuev:
                        vtxt = vencode(v)
                        assert ' ' not in vtxt
                        assert ':' not in vtxt
                        assert ',' not in vtxt
                        assert '-' not in vtxt
                        vtxtv.append(vtxt)
                        if vtxt in vencoded:
                            assert vencoded[vtxt] == v
                        else:
                            vencoded[vtxt] = v
                    tnode += ','.join(['%d:%s' % (k,vtxt)
                                    for (k,vtxt) in zip(node.keyv, vtxtv)])
            else:
                # tree or bucket with keys
                tnode += ','.join(['%d' % _ for _ in node.keyv])

            tnodev.append(tnode)
        topo += '-'.join(tnodev)

    if 1: # make sure that every topology we emit, can be loaded back
        t2 = TopoDecode(topo, vdecode)
        if t2 != tree:
            panic("BUG: TopoEncode: D(E(·)) != identity\n·       = %s\n D(E(·) = %s" % (tree, t2))
    return topo


# TopoDecode decodes topology-encoded text into Tree structure.
#
# Vdecode specifies way to decode values referred-to by buckets.
# See top-level docstring for description of topology encoding.
class TopoDecodeError(Exception):
    pass
def TopoDecode(text, vdecode=int):
    levelv = text.split('/')  # T3/T-T/B1:a-T5/B-B7,8,9  -> T3 T-T B1:a-T5 B-B7,8,9
    # NOTE we don't forbid mixing buckets-with-value with buckets-without-value

    # build nodes from bottom-up
    currentv = [] # of nodes on current level (that we are building)
    bottomq  = [] # of nodes below current level that we are building
                  # shrinks as fifo as nodes added to currentv link to bottom
    while len(levelv) > 0:
        level  = levelv.pop()       # e.g. B1:a-T5
        tnodev = level.split('-')   # e.g. B1:a T5
        bottomq  = currentv
        currentv = []
        for tnode in tnodev:
            if   tnode[:1] == 'T':
                typ = Tree
            elif tnode[:1] == 'B':
                typ = Bucket
            else:
                raise TopoDecodeError("incorrect node %s: unknown prefix" % qq(tnode))
            tkeys = tnode[1:]    # e.g. '7,8,9' or '1:a,3:def' or ''
            if tkeys == '':
                tkeyv = []
            else:
                tkeyv = tkeys.split(',')    # e.g. 7 8 9
            withV = (typ is Bucket and ':' in tkeys)
            keyv  = []
            valuev= [] if withV else None
            if tkeys != ':': # "B:" indicates ø bucket with values
                for tkey in tkeyv:
                    ktxt = tkey
                    if withV:
                        ktxt, vtxt = tkey.split(':')
                        v = vdecode(vtxt)
                        valuev.append(v)
                    k = int(ktxt)
                    keyv.append(k)

            if typ is Bucket:
                node = Bucket(keyv, valuev)
            else:
                # Tree
                nchild = len(keyv) + 1
                if len(bottomq) < nchild:
                    raise TopoDecodeError(
                        "node %s at level %d: next level does not have enough children to link to" %
                        (qq(tnode), len(levelv)+1))
                children = bottomq[:nchild]
                bottomq  = bottomq[nchild:]
                node = Tree(keyv, *children)

            currentv.append(node)

        if len(bottomq) != 0:
            raise TopoDecodeError("level %d does not link to all nodes in the next level" %
                                  (len(levelv)+1,))

    if len(currentv) != 1:
        raise TopoDecodeError("first level has %d entries; must be 1" % len(currentv))

    root = currentv[0]
    return root


# ---- misc ----

# _indent returns text with each line of it indented with prefix.
def _indent(prefix, text): # -> text
    textv = text.split('\n')
    textv = [prefix+_ for _ in textv]
    text  = '\n'.join(textv)
    return text

# _filter2(l,pred) = filter(l,pred), filter(l,!pred)
def _filter2(l, pred):
    t, f = [], []
    for _ in l:
        if pred(_):
            t.append(_)
        else:
            f.append(_)
    return t,f

# _assertIncv asserts that values of vector v are strictly ↑
def _assertIncv(v):
    prev = -inf
    for i in range(len(v)):
        if not (v[i] > prev):
            panic("assert incv: [%d] not ↑: %s -> %s" % (i, v[i], prev))
        prev = v[i]


# _zclassify returns kind of btree node znode is.
# raises TypeError if znode is not a ZODB btree node.
class _ZNodeType:
    # .is_ztree     znode is a BTree node
    # .is_zbucket   znode is a Bucket node
    # .is_map       whether znode is k->v or just set(k)
    pass
def _zclassify(znode): # -> _ZNodeType
    # XXX-> use zbcheck.classify ?
    typ = type(znode)
    is_ztree   = ("Tree"   in typ.__name__)
    is_zset    = ("Set"    in typ.__name__)
    is_zbucket = (("Bucket" in typ.__name__) or re.match("..Set", typ.__name__))
    is_map     = (not is_zset)

    if not (is_ztree or is_zbucket):
        raise TypeError("type %r is not a ZODB BTree node" % typ)

    _ = _ZNodeType()
    _.is_ztree   = is_ztree
    _.is_zbucket = is_zbucket
    _.is_map     = is_map
    return _


# zcheck performs full consistency checks on ztree provided by ZODB.
#
# The checks are what is provided by BTree.check and node._check().
def zcheck(ztree):
    # verify internal C-level pointers consistency.
    #
    # Only valid to be called on root node and verifies whole tree.
    # If called on a non-root node will lead to assert "Bucket next pointer is
    # damaged" since it calls BTree_check_inner(ztree, next_bucket=nil)
    # assuming ztree is a root tree node, and its rightmost bucket should
    # indeed have ->next=nil. If ztree is not root, there can be right-adjacent
    # part of the tree, to which ztree's rightmost bucket must be linking to
    # via its ->next.
    ztree._check()

    # verify nodes values consistency
    zbcheck.check(ztree)



# ----------------------------------------

import sys, tempfile, shutil, subprocess

# graphviz returns tree graph representation in dot language.
@func(Tree)
def graphviz(t, clustername=''):
    assert isinstance(t, Tree)

    symtab = {} # id(node) -> name
    valtab = {} # value    -> name

    outv = []
    def emit(text):
        outv.append(text)

    #emit('subgraph %s {' % clustername)    FIXME kills arrows
    emit('  splines=false')

    for (level, nodev) in enumerate(_walkBFS(t)):
        for (i, node) in enumerate(nodev):
            assert isinstance(node, (Tree,Bucket))
            # register node in symtab
            assert id(node) not in symtab
            kind = ('T' if isinstance(node, Tree) else 'B')
            symtab[id(node)] = '%s%s%d:%d' % (clustername+'_', kind, level, i)

            # emit node itself
            # approach based on https://github.com/Felerius/btree-generator
            emit('  %s' % qq(symtab[id(node)]))
            emit('  [')
            emit('    shape = box')
            emit('    margin = 0')
            emit('    height = 0')
            emit('    width  = 0')
            if kind == 'T':
                emit('    style = rounded')
            emit('    label = <<table border="0" cellborder="0" cellspacing="0">')
            emit('                <tr>')
            for (j,key) in enumerate(node.keyv):
                if kind == 'T':
                    emit('                  <td port="con%d"></td>' % j)
                if 1:
                    emit('                  <td port="key%d">%d</td>' % (j,key))
            emit('                  <td port="con%d">%s</td>' % (len(node.keyv), \
                                            ('    ' if len(node.keyv) == 0 else '')))

            emit('                </tr>')
            emit('             </table>>')
            emit('  ]')

            # emit values
            if kind == 'B' and node.valuev is not None:
                assert len(node.keyv) == len(node.valuev)
                for (j,key) in enumerate(node.keyv):
                    v = node.valuev[j]
                    valtab[v] = '%sV%s' % (clustername+'_', v)

    # second pass: emit links + node ranks + links to values
    for nodev in _walkBFS(t):
        # same rank for nodes on the same level
        emit('')
        emit('  {rank=same; %s}' % ' '.join([qq(symtab[id(_)]) for _ in nodev]))
        # links
        for node in nodev:
            assert isinstance(node, (Tree,Bucket))
            if isinstance(node, Tree):
                for (j,child) in enumerate(node.children):
                    emit('  %s:"con%d" -> %s' % (qq(symtab[id(node)]), j, qq(symtab[id(child)])))

            elif node.valuev is not None:
                assert len(node.valuev) == len(node.keyv)
                for (j,key) in enumerate(node.keyv):
                    emit('  %s:"key%d" -> %s' % (qq(symtab[id(node)]), j, valtab[node.valuev[j]]))

    # third pass: emit values
    emit('')
    for v in sorted(valtab):
        emit('  %s' % qq(valtab[v]))
        emit('  [')
        emit('  shape = plain')
        emit('  label = "%s"' % v)
        emit('  margin = 0')
        emit('  height = 0')
        emit('  width  = 0')
        emit('  ]')

    #emit('}')

    emit('')
    return '\n'.join(outv)


# topoview displays topologies provided in argv.
@func
def topoview(argv):
    if len(argv) == 0:
        raise RuntimeError('E: empty argument')

    treev = [TopoDecode(_, vdecode=lambda v: v) for _ in argv]

    outv = []
    def emit(text):
        outv.append(text)

    emit('digraph {')

    emit('  label=%s' % qq('   '.join(argv)))
    emit('  labelloc="t"')

    for (i, tree) in enumerate(treev):
        emit(tree.graphviz(clustername='c%d' % i))
    emit('}')


    import graphviz as gv
    g = gv.Source('\n'.join(outv))

    tmpd = tempfile.mkdtemp('', 'xbtree')
    def _():
        shutil.rmtree(tmpd)
    defer(_)

    # set filename so that it shows in window title.
    filename = ' '.join([_.replace('/', '\\') for _ in argv]) # no / in filename
    g.render(filename, tmpd, format='svg')

    # XXX g.view spawns viewer, but does not wait for it to stop
    subprocess.check_call(["inkview", "%s/%s.svg" % (tmpd, filename)])


if __name__ == '__main__':
    topoview(sys.argv[1:])
