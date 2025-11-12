# -*- coding: utf-8 -*-
# Copyright (C) 2025  Nexedi SA and Contributors.
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
"""Verify that WCFS enforces file permissions correctly.

This test complements wcfs_tests.py::test_wcfs_permissions by
verifying that users are granted or denied access to WCFS
based on their group memberships.

It cannot be fully automated because it requires:
- running as a user with SUDO rights
- a globally installed Python{2,3} interpreter accessible by all users

Run with:

    pytest -q wcfs/testprog/wcfs_verify_permissions.py::test
"""

from __future__ import print_function, absolute_import

import os
import shutil
import subprocess
import sys
import tempfile

from golang import func, defer

from wendelin.wcfs.wcfs_test import tDB, \
        setup_module, teardown_module, setup_function, teardown_function

# Globally accessible Python interpreter for subprocesses run as other users.
# Must be executable by all users. Can be 'python3' or 'python2'.
PYTHON_BIN = 'python3'

@func
def test():
    tgroup = TempGroup("wcfstest_group"); defer(tgroup.close)

    # gooduser belongs to group with which WCFS shares access: all read requests
    # must be accepted
    gooduser = TempUser("wcfstest_gooduser", tgroup.name); defer(gooduser.close)
    # baduser doesn't belong to group with which WCFS is shared: therefore all read
    # requests must be denied
    baduser = TempUser("wcfstest_baduser"); defer(baduser.close)

    # configure WCFS to share with the temp group
    os.environ["WENDELIN_CORE_WCFS_OPTIONS"] = "%s -sharewith group:%s" % (
        os.environ["WENDELIN_CORE_WCFS_OPTIONS"], tgroup.name)

    t = tDB()
    defer(t.close)
    f = t.open(t.zfile)
    f.assertCache([])
    f.assertData ([], mtime=t.at0)
    t.commit(t.zfile, {2:'c1'})

    gooduser.run_test("accepted", t.wc.mountpoint)
    baduser.run_test("denied", t.wc.mountpoint)

    
# Create a temporary Unix group
class TempGroup(object):
    def __init__(self, name):
        self.name = name
        run_sudo(["groupadd", self.name])

    # Removes the group
    def close(self):
        run_sudo(["groupdel", self.name])

    
# Create a temporary Unix user
class TempUser(object):
    def __init__(self, name, group=None):
        self.name = name
        cmd = ["useradd", self.name]
        if group:
            cmd += ["-g", group]
        run_sudo(cmd)

    # Remove user + their home directory
    def close(self):
        run_sudo(["userdel", "-r", self.name])
    
    # Run test with user
    def run_test(self, state_ok, *args):
        def _(script_path):
            cmd = [PYTHON_BIN, script_path] + list(args)

            # Use sudo to switch to the temp user
            return run_sudo(["-u", self.name] + cmd)

        out, err = with_tmp_copy("wcfs/testprog/_tpermission.py", _)
        assert out == state_ok, err


# Run a command with sudo and return stdout.
def run_sudo(cmd):
    full_cmd = ["sudo"] + cmd
    result = subprocess.Popen(
        full_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = result.communicate()
    if result.returncode != 0:
        raise RuntimeError("%s failed:\n\n\tstdout:\n%s\n\n\tstderr:\n%s" % (full_cmd, out, err))
    if sys.version_info[0] >= 3:
        out = out.decode("utf-8")
        err = err.decode("utf-8")
    return out.strip(), err.strip()


# Temporarily copy a script to /tmp and run a callback with the new path.
# The copied script is removed after the callback finishes.
#
# This is needed to make a script executable for another user.
@func
def with_tmp_copy(script_path, callback):
    tmp_dir = tempfile.mkdtemp(prefix="tmp_script_", dir="/tmp")
    os.chmod(tmp_dir, 0o755)
    tmp_path = os.path.join(tmp_dir, os.path.basename(script_path))

    @defer
    def _():
        os.remove(tmp_path)
        os.rmdir(tmp_dir)

    shutil.copy(script_path, tmp_path)
    os.chmod(tmp_path, 0o755)
    return callback(tmp_path)