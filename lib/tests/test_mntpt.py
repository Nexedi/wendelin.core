# -*- coding: utf-8 -*-
# Copyright (C) 2023  Nexedi SA and Contributors.
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

import pytest

from wendelin.lib.mntpt import mntpt_4zurl


@pytest.mark.parametrize(
    "uri_tuple",
    [
        # FileStorage
        ("file://Data.fs", "file://Data.fs"),
        # ZEO
        ("zeo://localhost:9001", "zeo://localhost:9001"),
        # NEO
        ("neo://127.0.0.1:1234/cluster", "neo://127.0.0.1:1234/cluster"),
        #   > 1 master nodes \w different order
        (
            "neo://127.0.0.1:1234,127.0.0.2:1234/cluster",
            "neo://127.0.0.2:1234,127.0.0.1:1234/cluster",
        ),
        #   Different SSL paths
        (
            "neos://ca=ca.cert&key=neo.key&cert=neo.cert@127.0.0.1:1234/cluster",
            "neos://ca=a&key=b&cert=c@127.0.0.1:1234/cluster",
        ),
    ],
)
def test_stable_mntpt(uri_tuple):
    mntpt = None
    for uri in uri_tuple:
        nmntpt = mntpt_4zurl(uri)
        if mntpt is not None:
            assert nmntpt == mntpt
        mntpt = nmntpt


# lib/mntpt must explicitly raise an exception if an unsupported
# zodburi scheme is used.
def test_mntpt_4zurl_invalid_scheme():
    for uri in "https://test postgres://a:b@c:5432/d".split(" "):
        with pytest.raises(NotImplementedError):
            mntpt_4zurl(uri)
