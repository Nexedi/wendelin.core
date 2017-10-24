# -*- coding: utf-8 -*-
# Wendelin.core.calc | Tests
# Copyright (C) 2015  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
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
from wendelin.lib.calc import mul

def test_mul():
    assert mul([1]) == 1
    assert mul([1], 5) == 5
    assert mul([0], 5) == 0
    assert mul([1,2,3,4,5]) == 120
    assert mul([1,2,3,4,5,6]) == 720

    # check it does not overflow
    assert mul([1<<30, 1<<30, 1<<30]) == 1<<90
