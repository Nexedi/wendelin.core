# Wendelin.core | Top-level in-tree python import redirector
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


# tell python wendelin.* modules hierarchy starts at top-level
#
# This allows e.g.          `import wendelin.bigarray`
# to resolve to importing   `bigarray/__init__.py`
#
# and thus avoid putting everything in additional top-level wendelin/
# directory in source tree.
#
# see https://www.python.org/doc/essays/packages/ about __path__
# XXX avoid being imported twice, e.g. `import wendelin.wendelin` should not work.

from os.path import dirname, realpath
__path__ = [realpath(dirname(__file__))]
del dirname, realpath


# Also tell setuptools/pkg_resources 'wendelin' is a namespace package
# ( so that wendelin.core installed in development mode does not brake
#   'wendelin' namespacing wrt other wendelin software )
__import__('pkg_resources').declare_namespace(__name__)

# pkg_resources will append '.../wendelin' to __path__ which is not right for
# in-tree setup and thus is not needed here. Remove it.
del __path__[1:]


# in the end we have:
# __path__ has 1 item
# __path__[0] points to working tree
# __name__ registered as namespace package
#
# so the following should work:
#   - importing from in-tree files
#   - importing from other children of wendelin packages
