# -*- coding: utf-8 -*-
# Copyright (C) 2019-2025  Nexedi SA and Contributors.
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

"""Package wcfs.internal.os complements operating system facilities provided by
standard package os.

- MountDB, MountDBConn and Mount provide access to database of mounted filesystems.
- gettid returns OS-level identifier of current thread.
- readfile and writefile are handy utilities to read/write a file as a whole.
"""

from __future__ import print_function, absolute_import

from wendelin.wcfs.internal._os import gettid

import os

from golang import func


# ISOLATION_* specify isolation levels.
#
# snapshot:         full state is observed at transaction start and remains constant
# repeatable-read:  state parts are observed on access, but observed once remain constant
# none:             no isolation from simultaneous changes
ISOLATION_SNAPSHOT        = "snapshot"
ISOLATION_REPEATABLE_READ = "repeatable-read"
ISOLATION_NONE            = "none"


# MountDB represents database of mounts.
#
# .open(isolation_level) creates read-only MountDBConn view of this database.
class MountDB(object):
    __slots__ = ()

# MountDBConn provides view to MountDB.
#
# Semantically it is {} mntid -> Mount; use get for point lookup.
# Query can be also used to find mount entries selected by arbitrary condition.
#
# It is safe to use MountDBConn only from single thread simultaneously.
class MountDBConn(object):
    __slots__ = (
        '_observed',        # {} mntid -> Mount
    )

# Mount represents one mount entry of Linux kernel.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html and
# https://www.kernel.org/doc/html/latest/filesystems/proc.html for details.
#
# It is safe to use MountDB only from single thread simultaneously.
class Mount(object):
    __slots__ = (
        'id',               # int
        'parent_id',        # int
        'dev',              # dev_t  (major:minor composed into one int)
        'root',             # str
        'point',            # str
        'options',          # str
        'tags',             # {} str->str|True
        'fstype',           # str
        'fssrc',            # str
        'super_options',    # str
    )


# ---- str/parse ----

@func(MountDBConn)
def __str__(mdbc):
    s = ""
    for mnt in mdbc.query(lambda mnt: True):
        s += "%s\n" % mnt
    return s

@func(Mount)
def __str__(mnt):
    s = "%d %d %d:%d %s %s %s" % (mnt.id, mnt.parent_id,
            os.major(mnt.dev), os.minor(mnt.dev), mnt.root, mnt.point, mnt.options)
    for k,v in mnt.tags.items():
        s += " %s" % (k,)
        if v is not True:
            s += ":%s" % (v,)
    s += " - %s %s %s" % (mnt.fstype, mnt.fssrc, mnt.super_options)
    return s

@func(Mount)
@staticmethod
def _parse(line): # -> Mount
    line = line.rstrip()
    assert '\n' not in line, line
    v = line.split()
    mnt = Mount()
    # XXX errctx "Mount._parse %q" % line
    mnt.id          = int(v[0])
    mnt.parent_id   = int(v[1])
    major, minor = v[2].split(':')
    mnt.dev         = os.makedev(int(major), int(minor))
    mnt.root        = v[3]
    mnt.point       = v[4]
    mnt.options     = v[5]
    v = v[6:]
    sep = v.index('-')
    mnt.tags = {}
    for _ in v[:sep]:
        if ':' in _:
            k, kv = _.split(':')
        else:
            k, kv = _, True
        if k in mnt.tags:
            raise ValueError("duplicate tag %s" % k)
        mnt.tags[k] = kv
    v = v[sep+1:]
    mnt.fstype  = v[0]
    mnt.fssrc   = v[1]
    mnt.super_options = v[2]
    return mnt


# ---- open ----

# open opens connection to mount database.
@func(MountDB)
@staticmethod
def open(isolation_level=ISOLATION_SNAPSHOT): # -> MountDBConn
    assert isolation_level in (ISOLATION_SNAPSHOT, ISOLATION_REPEATABLE_READ,
                               ISOLATION_NONE), isolation_level
    if isolation_level != ISOLATION_SNAPSHOT:
        raise NotImplementedError("support for isolation_level %r is not implemented" % isolation_level)
    mdbc = MountDBConn()
    mdbc._observed = {}
    for line in readfile("/proc/self/mountinfo").splitlines():
        mnt = Mount._parse(line)
        if mnt.id in mdbc._observed:
            raise ValueError("mount entries with duplicate ID:\n%s\n%s" % (mnt, mdbc._observed[mnt.id]))
        mdbc._observed[mnt.id] = mnt
    return mdbc


# ---- get/query ----

# get looks up mount by its mntid.
#
# None is returned if there is no such process.
@func(MountDBConn)
def get(mdbc, mntid): # -> Mount | None
    return mdbc._observed.get(mntid, None)

# query returns mount entries selected by selectf.
@func(MountDBConn)
def query(mdbc, selectf): # -> i[]Mount
    for mntid in sorted(mdbc._observed.keys()):
        mnt = mdbc._observed[mntid]
        if selectf(mnt):
            yield mnt


# ---- misc ----

# readfile reads file @ path.
def readfile(path):
    with open(path) as f:
        return f.read()

# writefile writes data to file @ path.
def writefile(path, data):
    with open(path, "w") as f:
        f.write(data)
