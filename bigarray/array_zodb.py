# -*- coding: utf-8 -*-
# Wendelin.bigarray | ZODB-Persistent BigArray
# Copyright (C) 2014-2019  Nexedi SA and Contributors.
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

"""Package array_zodb provides BigArray backed by ZODB.

ZBigArray provides BigArray with data stored in ZODB.

Please see documentation for BigArray (bigarray/__init__.py) and ZBigFile
(bigfile/file_zodb.py) as ZBigArray is just a minimal glue for those two and
inherits most of the properties and specifics from its parents.
"""

from wendelin.bigarray import BigArray
from wendelin.bigfile.file_zodb import ZBigFile
from wendelin.lib.zodb import LivePersistent
import inspect


# TODO document that first access data must be either before commit or Connection.add

# {} BigArray arg -> default value
_ = inspect.getargspec(BigArray.__init__)
BigArray_defaults = dict(zip(reversed(_.args), reversed(_.defaults)))


class ZBigArray(BigArray,
                # Live: don't allow us to go to ghost
                # (not to loose ._v_fileh which works as DataManager)
                LivePersistent):
    __slots__ = (
        'zfile',            # ZBigFile serving this array
    )


    # XXX default blksize hardcoded
    def __init__(self, shape, dtype, order='C', blksize=2*1024*1024, zblk_fmt=""):
        LivePersistent.__init__(self)
        # NOTE BigArray is cooperative to us - it names all helping (= not needing
        # to be saved) data members starting with _v_. Were it otherwise, we
        # could not inherit from BigArray and would need to keep its instance
        # in e.g. ._v_array helper and redirect all BigArray method from us to it.
        BigArray._init0(self, shape, dtype, order)
        self.zfile = ZBigFile(blksize, zblk_fmt=zblk_fmt)
        self._v_fileh = None


    def __setstate__(self, state):
        super(ZBigArray, self).__setstate__(state)

        # for backward compatibility: if a member is missing in state - set it
        # to BigArray default. Ex. `order` was not set in early versions of
        # ZBigArray and when loading such objects from DB, without adjustment,
        # they won't work properly.
        for k, v in BigArray_defaults.items():
            if not hasattr(self, '_'+k):
                setattr(self, '_'+k, v)

        # NOTE __setstate__() is done after either
        #   - 1st time loading from DB, or
        #   - loading from DB after invalidation.
        #
        # as invalidation can happen e.g. by just changing .shape in another DB
        # connection (e.g. resizing array and appending some data), via always
        # resetting ._v_fileh we discard all data from it.
        #
        # IOW we discard whole cache just because some data were appended.
        #
        # -> TODO (optimize) do not through ._v_fileh if we can (.zfile not changed, etc)
        self._v_fileh = None


    # open fileh lazily, so that when we open it, zfile was already associated
    # with Connection (i.e. zfile._p_jar is not None). This is needed for
    # ZBigFile working.
    @property
    def _fileh(self):
        if self._v_fileh is None:
            self._v_fileh = self.zfile.fileh_open()
        return self._v_fileh
