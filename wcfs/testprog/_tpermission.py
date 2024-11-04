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
"""Check if the current user can read all WCFS files under a given mountpoint.

Returns on stdout:
    - "accepted" if all read requests succeed
    - "denied" if all read requests fail
    - "mixed" if some succeed and some fail
    - "invalid" if no files were found to test

Returns on stderr:
    - debug messages indicating which files are readable or not

Usage:
    python check_wcfs_permissions.py <mountpoint>

This script is expected to be called from wcfs/testprog/wcfs_verify_permissions.py
"""

from __future__ import print_function, absolute_import

import os
import sys


def main():
    if len(sys.argv) != 2:
        print("Usage: {} <mountpoint>".format(sys.argv[0]), file=sys.stderr)
        sys.exit(1)

    mountpoint = sys.argv[1]
    status = check_permissions(mountpoint)
    print(status)


# Check read access for all files under mountpoint.
def check_permissions(mountpoint):
    results = []

    for dirpath, _, filenames in os.walk(mountpoint):
        for filename in filenames:
            if "watch" in filename:
                continue  # skip watch files
            path = os.path.join(dirpath, filename)
            try:
                with open(path, "rb") as f:
                    pass
            except (OSError, IOError) as e:
                debug("%s: can't open: %s" % (path, e))
                results.append(False)
            else:
                debug("%s: ok" % path)
                results.append(True)

    if not results:
        return "invalid"

    if all(results):
        return "accepted"
    elif not any(results):
        return "denied"
    return "mixed"


# Print debug messages to stderr
def debug(msg):
    sys.stderr.write(msg + "\n")


if __name__ == "__main__":
    main()