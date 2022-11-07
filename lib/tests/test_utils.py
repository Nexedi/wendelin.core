# -*- coding: utf-8 -*-
# Wendelin.core.lib.utils | Tests
# Copyright (C) 2022 Nexedi SA and Contributors.
#                    Levin Zimmermann <levin.zimmermann@nexedi.com>
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

from fractions import Fraction
import numpy as np
from pytest import raises
import six

from wendelin.lib.utils import inttuple


def test_inttuple():
    # Test int -> tuple conversion
    assert inttuple(1) == (1,)
    if six.PY2:
        assert inttuple(long(4)) == (4,)
    
    # Test sequence -> tuple conversion
    assert inttuple([1, 2, 3]) == (1, 2, 3)
    assert inttuple(np.array([1, 2, 3])) == (1, 2, 3)
    assert inttuple((3, 2, 4)) == (3, 2, 4)

    # Test exceptions

    def assert_raises(object_):
        with raises(TypeError, match="cannot be interpreted as integer"):
            inttuple(object_)

    #   No none-integer single values are allowed
    #     non-integer numbers
    assert_raises(3.2)
    assert_raises(Fraction(3, 2))
    #     other objects
    assert_raises("privjet")
    assert_raises({1, 2, 3})
    assert_raises({0: 1, 1: 2})
    #   No none-integer values inside sequences are allowed
    assert_raises((1, 0.5))