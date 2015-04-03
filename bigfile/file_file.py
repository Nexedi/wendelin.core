# -*- coding: utf-8 -*-
# Wendelin.bigfile | BigFile file backend
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
""" BigFile backed by OS file """

from wendelin.bigfile import BigFile
from io import FileIO, SEEK_SET

# XXX naming
class BigFile_File(BigFile):
    # .f        - io.FileIo to file

    def __new__(cls, path_or_fd, blksize):
        # XXX pass flags/mode as args to ctor ?
        f = FileIO(path_or_fd, 'r+')
        obj = BigFile.__new__(cls, blksize)
        obj.f = f
        return obj


    def loadblk(self, blk, buf):
        blksize = self.blksize
        f = self.f
        f.seek(blk * blksize, SEEK_SET)

        # XXX unfortunately buffer(buf, pos) creates readonly buffer, so we
        # have to use memoryviews
        # XXX not needed after BIGFILE_USE_OLD_BUFFER support is dropped
        bufmem = memoryview(buf)

        size = blksize
        while size > 0:
            n = f.readinto(bufmem[blksize-size:])
            # XXX n==0 -- EOF ?
            assert n != 0
            size -= n

        # ok
        return


    def storeblk(self, blk, buf):
        blksize = self.blksize
        f = self.f
        f.seek(blk * blksize, SEEK_SET)

        # NOTE unfortunately buffer[:] creates _data_ copy
        # XXX not needed after BIGFILE_USE_OLD_BUFFER support is dropped
        bufmem = memoryview(buf)

        size = blksize
        while size > 0:
            n = f.write(bufmem[blksize-size:])
            assert n != 0
            size -= n

        # ok
        return
