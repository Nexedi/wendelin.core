# Copyright (C) 2024  Nexedi SA and Contributors.
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

from wendelin.lib.mem import memdelta

def test_memdelta():
    def _(a, b, ndelta):
        assert memdelta(a, b) == ndelta
    _(b'', b'', 0)
    _(b'', b'123', 3)
    _(b'ab', b'', 2)
    _(b'abc', b'abc', 0)
    _(b'aXc', b'aYc', 1)
    _(b'aXcZ', b'aYc', 2)
    _(b'aXcZ', b'aYcZ', 1)
    _(b'aXcZ', b'aYcQ', 2)
    _(b'aXcZ', b'aYcQR', 3)
    _(b'aXcZE', b'aYcQR', 3)
    _(b'aXcZEF', b'aYcQR', 4)
    _(b'aXcZEF', b'aYcQRS', 4)
    _(b'aXcdEF', b'aYcdRS', 3)
    _(b'aXcdeF', b'aYcdeS', 2)
    _(b'aXcdef', b'aYcdef', 1)
    _(b'abcdef', b'abcdef', 0)
