# Wendelin.core | pytest config
# Copyright (C) 2020-2024  Nexedi SA and Contributors.
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
from golang import func, defer
from functools import partial
import os, gc

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


# prepend_env prepends prefix + ' ' to environment variable var.
def prepend_env(var, prefix):
    v = os.environ.get(var, '')
    if v != '':
        v = ' ' + v
    v = prefix + v
    os.environ[var] = v


# enable debug features during testing
# enable log_cli on no-capture
# (output during a test is a mixture of print and log)
def pytest_configure(config):
    # put WCFS log into stderr instead of to many files in /tmp/wcfs.*.log
    # this way we don't leak those files and include relevant information in test output
    #
    # TODO put WCFS logs into dedicated dir without -v?
    prepend_env('WENDELIN_CORE_WCFS_OPTIONS', '-debug -logtostderr')

    if config.option.capture == "no":
        config.inicfg['log_cli'] = "true"
        assert config.getini("log_cli") is True
        # -v   -> verbose wcfs.py logs
        if config.option.verbose > 0:
            import logging
            wcfslog = logging.getLogger('wcfs')
            wcfslog.setLevel(logging.INFO)
        # -vv  -> verbose *.py logs
        # XXX + $WENDELIN_CORE_WCFS_OPTIONS="-trace.fuse -alsologtostderr -v=1" ?
        if config.option.verbose > 1:
            config.inicfg['log_cli_level'] = "INFO"


# Before pytest exits, teardown WCFS server(s) that we automatically spawned
# during test runs in bigfile/bigarray/...
#
# If we do not do this, spawned wcfs servers are left running _and_ connected
# by stdout to nxdtest input - which makes nxdtest to wait for them to exit.
@func
def pytest_unconfigure(config):
    # force collection of ZODB Connection(s) that were sitting in DB.pool(s)
    # (DB should be closed)
    gc.collect()

    from wendelin import wcfs
    for wc in wcfs._wcautostarted:
        # NOTE: defer instead of direct call - to call all wc.close if there
        # was multiple wc spawned, and proceeding till the end even if any
        # particular call raises exception.
        defer(partial(_wcclose_and_stop, wc))

@func
def _wcclose_and_stop(wc):
    defer(wc._wcsrv.stop)
    defer(wc.close)
