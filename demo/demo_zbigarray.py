#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Demo program that generates and computes on ZBigArray bigger than RAM

This program demonstrates how it is possible to work with arrays bigger than
RAM. There are 2 execution modes:

    gen:    generate signal
    read:   read generated signal and compute its mean/var/sum

Generation mode writes signal parts in limited-by-size blocks, because the
amount of changes in one transaction should be less than available RAM.

Read mode creates ndarray view for the whole array and process it in full.
"""

from __future__ import print_function

from wendelin.bigarray.array_zodb import ZBigArray
from wendelin.lib.zodb import dbopen, dbclose
import transaction

from numpy import float64, dtype, cumsum, sin
import psutil
import sys

KB = 1024
MB = 1024*KB
GB = 1024*MB


# read signal and compute its average/var/sum
# signalv - BigArray with signal
def read(signalv):
    print('sig: %s %s (= %.2fGB)' % \
            ('x'.join('%s' % _ for _ in signalv.shape),
             signalv.dtype, float(signalv.nbytes) / GB))
    a = signalv[:]   # BigArray -> ndarray
    print('<sig>:\t%s' % a.mean())
    #print('δ(sig):\t%s' % a.var())   # XXX wants to produce temps (var = S (a - <a>)^2
    print('S(sig):\t%s' % a.sum())


# generate signal S(t) = M⋅sin(f⋅t)
f = 0.2
M = 15
def gen(signalv):
    print('gen signal t=0...%.2e  %s  (= %.2fGB) ' %    \
                (len(signalv), signalv.dtype, float(signalv.nbytes) / GB))
    a = signalv[:]  # BigArray -> ndarray
    blocksize = 32*MB//a.itemsize   # will write out signal in such blocks
    for t0 in xrange(0, len(a), blocksize):
        ablk = a[t0:t0+blocksize]

        ablk[:] = 1             # at = 1
        cumsum(ablk, out=ablk)  # at = t-t0+1
        ablk += (t0-1)          # at = t
        ablk *= f               # at = f⋅t
        sin(ablk, out=ablk)     # at = sin(f⋅t)
        ablk *= M               # at = M⋅sin(f⋅t)

        note = 'gen signal blk [%s:%s]  (%.1f%%)' % (t0, t0+len(ablk), 100. * (t0+len(ablk)) / len(a))
        txn = transaction.get()
        txn.note(note)
        txn.commit()
        print(note)



def usage():
    print("Usage: %s (gen|read) <dburi>" % sys.argv[0], file=sys.stderr)
    sys.exit(1)


def main():
    try:
        act = sys.argv[1]
        dburi = sys.argv[2]
    except IndexError:
        usage()

    if act not in ('gen', 'read'):
        usage()

    ram_nbytes = psutil.virtual_memory().total
    print('I: RAM: %.2fGB' % (float(ram_nbytes) / GB))

    root = dbopen(dburi)

    if act == 'gen':
        sig_dtype = dtype(float64)
        sig_len   = (2*ram_nbytes) // sig_dtype.itemsize
        sig = ZBigArray((sig_len,), sig_dtype)
        root['signalv'] = sig

        # ZBigArray requirement: before we can compute it (with subobject
        # .zfile) have to be made explicitly known to connection or current
        # transaction committed
        transaction.commit()

        gen(sig)

    elif act == 'read':
        read(root['signalv'])

    import os
    p = psutil.Process(os.getpid())
    m = p.memory_info()
    print('VIRT: %i MB\tRSS: %iMB' % (m.vms//MB, m.rss//MB))

    dbclose(root)


if __name__ == '__main__':
    main()
