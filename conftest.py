# Wendelin.core | pytest config
# Copyright (C) 2020-2021  Nexedi SA and Contributors.
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

from __future__ import print_function, absolute_import

import pytest
import transaction

# reset transaction synchronizers before every test run.
#
# This isolates different tests from each other. When there is failure in test A,
# and it is not careful enough to close all ZODB Connections (but DB and
# storage are closed), those connections will continue to participate in
# transaction boundaries, and crash on access to closed storage.
#
# Fix it once and for all in one place here.
# It would be more easy if DB.close would call conn.close for all its opened
# connections, but unfortunately it is not the case.
#
# See also TestDB_Base.teardown, but it isolates only module Ma from module Mb,
# not test M.A from test M.B.
@pytest.fixture(autouse=True)
def transaction_reset():
    transaction.manager.clearSynchs()
    yield
    # nothing to run after test
