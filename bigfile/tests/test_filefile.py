# Wendeling.core.bigfile | Tests for BigFile_File
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
from wendelin.bigfile.file_file import BigFile_File
from wendelin.bigfile import WRITEOUT_STORE, WRITEOUT_MARKSTORED
from tempfile import NamedTemporaryFile
from os import unlink
from numpy import ndarray, asarray, dtype, arange, array_equal, uint8

from six.moves import range as xrange


tmpf = None
blksize  = 2*1024*1024   # XXX hardcoded
blen     = 32            # 32*2 = 128MB      # TODO set it higher by default ?
be4      = dtype('>u4')
blkitems = blksize // be4.itemsize  # items / block

def setup_module():
    global tmpf
    tmpf = NamedTemporaryFile(prefix='bigfile.', delete=False)

    # setup content with incrementing 4-byte integer
    for i in xrange(0, blen*blkitems, blkitems):
        blkdata = arange(i, i + blkitems, dtype=be4)
        tmpf.write(blkdata)

    tmpf.close()


def teardown_module():
    unlink(tmpf.name)


def test_bigfile_filefile():
    f   = BigFile_File(tmpf.name, blksize)
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    m = memoryview(vma)

    # verify via bigfile interface that file contents is the same we've wrote
    # to it.
    for i in xrange(0, blen*blkitems, blkitems):
        data0 = arange(i, i + blkitems, dtype=be4)
        # NOTE ndarray(..., buffer=m) does not work on py2
        data  = asarray(m[i*4:(i+blkitems)*4]).view(dtype=be4)
        assert array_equal(data0, data)


    # change file data
    m[0:5]                      = b'Hello'
    m[4*blksize+0:4*blksize+5]  = b'World'

    fh.dirty_writeout(WRITEOUT_STORE | WRITEOUT_MARKSTORED)

    del m
    del vma # TODO vma.unmap()
    del fh  # TODO fh.close()
    del f   # TODO close f

    f   = BigFile_File(tmpf.name, blksize)
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    m = memoryview(vma)

    # verify that the file content is original + changes
    for i in xrange(0, blen*blkitems, blkitems):
        data0 = arange(i, i + blkitems, dtype=be4)
        _     = data0.view(uint8)
        if i//blkitems == 0:
            _[0:5] = [ord(c) for c in 'Hello']
        if i//blkitems == 4:
            _[0:5] = [ord(c) for c in 'World']

        # NOTE ndarray(..., buffer=m) does not work on py2
        data  = asarray(m[i*4:(i+blkitems)*4]).view(dtype=be4)
        assert array_equal(data0, data)


    # TODO close f
