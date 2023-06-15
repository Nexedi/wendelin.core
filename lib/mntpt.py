# Wendelin.bigfile | calculation of WCFS mountpoint
# Copyright (C) 2023        Nexedi SA and Contributors.
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
"""Package wendelin.lib.mntpt provides utilities for calculation WCFS mountpoint"""

from errno import EEXIST
import hashlib
import os
import stat
from six.moves.urllib.parse import urlsplit, urlunsplit


# mntpt_4zurl returns wcfs should-be mountpoint for ZODB @ zurl.
#
# it also makes sure the mountpoint exists.
def mntpt_4zurl(zurl):
    # remove credentials from zurl.
    # The same database can be accessed from different clients with different
    # credentials, but we want to map them all to the same single WCFS
    # instance.
    scheme, netloc, path, query, frag = urlsplit(zurl)
    if "@" in netloc:
        netloc = netloc[netloc.index("@") + 1 :]
    zurl = urlunsplit((scheme, netloc, path, query, frag))

    m = hashlib.sha1()
    m.update(zurl)

    # WCFS mounts are located under /dev/shm/wcfs. /dev/shm is already used by
    # userspace part of wendelin.core memory manager for dirtied pages.
    # In a sense WCFS mount provides shared read-only memory backed by ZODB.

    # mkdir /dev/shm/wcfs with stiky bit. This way multiple users can create subdirectories inside.
    wcfsroot = "/dev/shm/wcfs"
    wcfsmode = 0o777 | stat.S_ISVTX
    if _mkdir_p(wcfsroot):
        os.chmod(wcfsroot, wcfsmode)
    else:
        # migration workaround for the situation when /dev/shm/wcfs was created by
        # code that did not yet set sticky bit.
        _ = os.stat(wcfsroot)
        if _.st_uid == os.getuid():
            if _.st_mode != wcfsmode:
                os.chmod(wcfsroot, wcfsmode)

    mntpt = "%s/%s" % (wcfsroot, m.hexdigest())
    _mkdir_p(mntpt)
    return mntpt


# mkdir -p.
def _mkdir_p(path, mode=0o777): # -> created(bool)
    try:
        os.makedirs(path, mode)
    except OSError as e:
        if e.errno != EEXIST:
            raise
        return False
    return True
