# Wendelin.core | Top-level in-tree python import redirector
# Copyright (C) 2014-2015  Nexedi SA and Contributors.
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


# first make sure setuptools will recognize wendelin.py as package,
# but do not setup proper __path__ yet.
# ( _handle_ns() checks for __path__ attribute presence and refuses to further
#   process "not a package"
#
#   https://github.com/pypa/setuptools/blob/9803058d/pkg_resources/__init__.py#L2012 )
__path__ = []

# tell setuptools/pkg_resources 'wendelin' is a namespace package
# ( so that wendelin.core installed in development mode does not brake
#   'wendelin' namespacing wrt other wendelin software )
__import__('pkg_resources').declare_namespace(__name__)

# pkg_resources will append '.../wendelin.core/wendelin' to __path__ which is
# not right for in-tree setup and thus needs to be corrected:
# Rewrite '.../wendelin.core/wendelin' -> '.../wendelin.core'
from os.path import dirname, realpath, splitext
myfile = realpath(__file__)
mymod  = splitext(myfile)[0]    # .../wendelin.py   -> .../wendelin
mydir  = dirname(myfile)        # .../wendelin      -> ...
i = None    # in case vvv loop is empty, so we still can `del i` in the end
for i in range(len(__path__)):
    # NOTE realpath(...) for earlier setuptools, where __path__ entry could be
    # added as relative
    if realpath(__path__[i]) == mymod:
        __path__[i] = mydir
del dirname, realpath, splitext, myfile, mymod, mydir, i


# in the end we have:
# __path__ has >= 1 items
# __path__ entry for wendelin.core points to top of working tree
# __name__ registered as namespace package
#
# so the following should work:
#   - importing from in-tree files
#   - importing from other children of wendelin packages
