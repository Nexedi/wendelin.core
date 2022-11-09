# -*- coding: utf-8 -*-
# Utility functions for python part of wendelin.core
# Copyright (C) 2014-2018  Nexedi SA and Contributors.
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


import numbers
import six

if six.PY2:
  from collections import Sequence
else:
  from collections.abc import Sequence


def inttuple(int_or_int_sequence):
    # Imitate the behaviour of numpy by converting
    # int or sequence of int to a tuple of int and
    # raise error in case this is not possible.

    # See the following numpy + cpython references:
    #
    #   https://github.com/numpy/numpy/blob/28e9227565b2/numpy/core/src/multiarray/conversion_utils.c#L133-L144
    #   https://github.com/python/cpython/blob/9c4ae037b9c3/Objects/abstract.c#L1706-L1713

    def raise_if_not_integer(object_):
        # XXX: Use 'numbers.Integral' instead of 'int' to support
        # python2 long type.
        if not isinstance(object_, numbers.Integral):
            raise TypeError(
              "'%s' cannot be interpreted as integer" % type(object_)
            )

    # We need to explicitly add np.ndarray into checked types,
    # because np.ndarray aren't Sequences, see also:
    #
    #   https://github.com/numpy/numpy/issues/2776
    if isinstance(int_or_int_sequence, (Sequence, np.ndarray)):
        int_tuple = tuple(int_or_int_sequence)
        [raise_if_not_integer(object_) for object_ in int_tuple]
        return int_tuple

    raise_if_not_integer(int_or_int_sequence)
    return (int_or_int_sequence,)