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
- ProcDB, ProcDBConn, Proc, Task and friends provide access to database of processes.
- gettid returns OS-level identifier of current thread.
- readfile and writefile are handy utilities to read/write a file as a whole.
"""

from __future__ import print_function, absolute_import

from wendelin.wcfs.internal._os import gettid

import os, os.path, pwd, logging as log
from errno import EACCES, ENOENT
inf = float('inf')

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


# ProcDB represents database of processes.
#
# .open(isolation_level) creates read-only ProcDBConn view of this database.
class ProcDB(object):
    __slots__ = ()

# ProcDBConn provides view to ProcDB.
#
# Semantically it is {} pid -> Proc; use get for point lookup.
# Query can be also used to find processes and their properties selected by arbitrary condition.
#
# It is safe to use ProcDBConn only from single thread simultaneously.
class ProcDBConn(object):
    __slots__ = (
        'isolation_level',  # ISOLATION_*
        '_prefix',          # str

        '_observed',                # {} pid -> Proc (| iso≠none: None if "no such process")
        '_rr_query_observed_max',   # iso≥RR:   max(pid) among all keys so far iterated during query
                                    #           +∞ after first full iteration completes.
                                    #           -∞ initially
                                    # iso=none: -∞
    )

# Proc represents information exposed about one process.
#
# See https://www.kernel.org/doc/html/latest/filesystems/proc.html for details.
#
# Accessing any Proc property can raise ProcPermissionDenied error due to OS security settings.
# For isolation levels < "snapshot" ProcGone error can be also raised on any property access.
#
# However for isolation levels ≥ "repeatable-read" if a property was
# successfully accessed once, it will remain to be successfully accessed.
#
# It is safe to use Proc only from single thread simultaneously.
class _Proc(object):
    __slots__ = (
        'pdbc',         # ProcDBConn
        '_observed',    # {} property -> value  ; used for iso ≥ RR
    )
class Proc(_Proc):
    __slots__ = (
        'pid',          # int
        # other properties are retrieved lazily
    )

# Task represents information exposed about one task of a process.
class Task(_Proc):
    __slots__ = (
        'proc',         # Proc
        'tid',          # int
        # other properties are retrieved lazily
    )

# ProcPermissionDenied is the error reported when access to process property is
# denied by OS due to security settings.
#
# For example Linux rejects reading ktraceback until the caller have CAP_SYS_ADMIN.
class ProcPermissionDenied(Exception):
    __slots__ = (
        'proc',         # _Proc
        'property',     # str
    )

# ProcGone is the error reported when a process is no longer present in the list of processes.
#
# An example is when process terminates and is waited by its parent.
class ProcGone(Exception):
    __slots__ = (
        'proc',         # Proc
    )


# FDInfo represents information about one file descriptor of a process.
class FDInfo:
    __slots__ = (
        'fd',       # int
        'path',     # str
        'pos',      # int
        'flags',    # int
        'mnt_id',   # int
        'ino',      # int
        'extra',    # {} k->v
    )

# MMap represents information about one memory mapping of a process.
class MMap:
    __slots__ = (
        'addr',     # str   TODO -> addr_lo + addr_hi int
        'perm',     # str
        'offset',   # int
        'dev',      # dev_t (major:minor composed into one int)
        'inode',    # int
        'path',     # str | None
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


@func(ProcDBConn)
def __str__(pdbc):
    s = ""
    for proc in pdbc.query(lambda proc: True):
        s += "%s\n" % proc
    return s

@func(Proc)
def __str__(proc):
    user = proc.get('user', eperm="strerror", gone="strerror")
    comm = proc.get('comm', eperm="strerror", gone="strerror")
    return "%9s %s %s" % ("pid%d" % proc.pid, user, comm)

@func(Task)
def __str__(task):
    user = task.get('user', eperm="strerror", gone="strerror")
    comm = task.get('comm', eperm="strerror", gone="strerror")
    return "%9s %s %s" % ("tid%d" % task.tid, user, comm)

@func(ProcPermissionDenied)
def __str__(e):
    s = ""
    if isinstance(e.proc, Proc):
        s += "pid%d" % e.proc.pid
    elif isinstance(e.proc, Task):
        s += "tid%d" % e.proc.tid
    s += ".%s: permission denied" % e.property
    return s

@func(ProcGone)
def __str__(e):
    return "pid%d: process disappeared" % e.proc.pid


@func(MMap)
@staticmethod
def _parse(line): # -> MMap
    line = line.rstrip()
    assert '\n' not in line, line
    v = line.split(None, 5)
    mmap = MMap()
    # XXX errctx "MMap._parse %q" % line
    # https://www.man7.org/linux/man-pages/man5/proc_pid_maps.5.html
    mmap.addr   = v[0]
    mmap.perm   = v[1]
    mmap.offset = int(v[2], 16)
    major, minor = v[3].split(':')
    mmap.dev    = os.makedev(int(major, 16), int(minor, 16))
    mmap.inode  = int(v[4])
    mmap.path = None
    if len(v) > 5:
        mmap.path = v[5]
    return mmap


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

# open opens connection to process database.
@func(ProcDB)
@staticmethod
def open(isolation_level=ISOLATION_SNAPSHOT): # -> ProcDBConn
    assert isolation_level in (ISOLATION_SNAPSHOT, ISOLATION_REPEATABLE_READ,
                               ISOLATION_NONE), isolation_level
    pdbc = ProcDBConn()
    pdbc._prefix = "/proc"
    pdbc._observed = {}
    pdbc._rr_query_observed_max = -inf

    if isolation_level == ISOLATION_SNAPSHOT:
        pdbc.isolation_level = ISOLATION_REPEATABLE_READ
        for proc in pdbc.query(lambda proc: True):
            for cls in Proc.__mro__:
                for name in getattr(cls, '_properties', {}):
                    try:
                        proc.get(name)
                    except ProcPermissionDenied:
                        pass
                    except ProcGone:
                        continue
        assert pdbc._rr_query_observed_max == inf

    pdbc.isolation_level = isolation_level
    return pdbc


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


# get looks up process by its pid.
#
# None is returned if there is no such process.
@func(ProcDBConn)
def get(pdbc, pid): # -> Proc | None
    procdir = "%s/%d" % (pdbc._prefix, pid)
    proc = pdbc._observed.get(pid, _missing)
    if proc is _missing:
        proc = None
        if pid > pdbc._rr_query_observed_max:  # else we already reported during query that pid does not exist
            if os.path.exists(procdir):
                proc = Proc()
                proc.pdbc = pdbc
                proc.pid  = pid
                proc._observed = {}

        # remember proc in ._observed for iso ≥ RR
        # for iso=none also remember present proc to return same Proc instance for same pid
        if not ((pdbc.isolation_level == ISOLATION_NONE)   and  (proc is None)):
            pdbc._observed[pid] = proc

    else:
        # for iso=none recheck if the process is still there
        # here we are ok to recheck only pid dir existence without going further to
        # see if proc is zombie because it might be useful for the caller to know
        # that the process is zombie.
        if pdbc.isolation_level == ISOLATION_NONE:
            assert proc is not None
            if not os.path.exists(procdir):
                proc = None
                pdbc._observed[pid] = None  # if pid will be reused it will be another Proc

    return proc


# query returns processes and/or their properties selected by selectf.
#
# - if selectf(proc) returns False nothing is yielded for that process
# - if selectf(proc) returns True  that proc is yielded
# - otherwise what selectf returns is yielded
#
# If selectf raises ProcPermissionDenied by default this error cancels the
# search and is reraised to query's caller. However with eperm="warn|ignore"
# processes for which ProcPermissionDenied was triggered are skipped without
# cancelling the search.
#
# If selectf raises ProcGone corresponding process is skipped and not
# yielded by query.
@func(ProcDBConn)
def query(pdbc, selectf, eperm="raise"): # -> i[](Proc | what selectf returns)
    assert eperm in ("raise", "warn", "ignore"), eperm
    rr_pidv = False
    if pdbc.isolation_level in (ISOLATION_REPEATABLE_READ, ISOLATION_SNAPSHOT):
        rr_pidv = True
    pidv = pdbc._pidlist(gt=pdbc._rr_query_observed_max)
    if rr_pidv:
        pidv = list(pdbc._observed.keys()) + pidv
        pidv.sort() # NOTE can have duplicates if .get() was invoked with pid > _rr_query_observed_max

    pid_prev = None
    for pid in pidv:
        dup = (pid == pid_prev)
        pid_prev = pid
        if dup:
            continue

        proc = pdbc.get(pid)
        if proc is None:
            continue    # pid > _rr_query_observed_max disappeared after pidlist
        if rr_pidv:
            if pid > pdbc._rr_query_observed_max:
                pdbc._rr_query_observed_max = pid

        try:
            x = selectf(proc)
        except ProcPermissionDenied: # e.g. accessing links in processes that disable ptrace
            if eperm == "raise":
                raise
            if eperm == "warn":
                argv = ['?']
                try:
                    argv = proc.argv
                except ProcPermissionDenied:
                    pass
                except ProcGone:
                    continue
                if len(argv) != 0:
                    argvstr = " (%s)" % ' '.join(argv)
                else:
                    argvstr = ""
                log.warning("no access to %s%s" % (proc, argvstr))
            continue
        except ProcGone: # disappeared process - ignore it
            continue

        do_yield = True
        if isinstance(x, bool):
            do_yield = x
            x = proc
        if do_yield:
            yield x

    # successful query completion
    if rr_pidv:
        pdbc._rr_query_observed_max = inf

# _pidlist returns list of pids from /proc that are > gt cutoff.
@func(ProcDBConn)
def _pidlist(pdbc, gt): # []pid↑ : pid > gt
    if gt == inf:
        return []
    pidv = []
    for _ in os.listdir(pdbc._prefix):
        try:
            pid = int(_)
        except ValueError:
            continue
        if pid > gt:
            pidv.append(pid)
    pidv.sort()
    return pidv



# ---- Mount ----

# lsof returns information about which processes use the mount and how.
@func(Mount)
def lsof(mnt, pdbc=None):  # -> i() of (proc, {}key -> path)   ; key = fd/X, mmap/Y, cwd, ...
    if pdbc is None:
        pdbc = ProcDB.open(isolation_level=ISOLATION_REPEATABLE_READ)
    assert isinstance(pdbc, ProcDBConn)

    # NOTE we must not access the filesystem on mnt at all or it might hang
    #      if filesystem server is deadlocked

    def _(proc):
        use  = {}
        for key in ('cwd', 'exe', 'root'):
            path = proc.get(key)
            # XXX better somehow to check via devid, but we can't stat link
            #     target because it will touch fs
            if path is not None  and  (path == mnt.point  or  path.startswith(mnt.point + "/")):
                use[key] = path

        for ifd in proc.fd.values():
            if ifd.mnt_id == mnt.id:
                use["fd/%d" % ifd.fd] = ifd.path

        for mmap in proc.mmaps.values():
            if mmap.dev == mnt.dev:
                use["mmap/%s" % mmap.addr] = mmap.path

        if len(use) > 0:
            return (proc, use)

        return False

    return pdbc.query(_, eperm="warn")


# ---- Proc/Task/... ----

# get retrieves process property with specified name.
#
# - ProcPermissionDenied is raised if OS denies access to the property;
#   with eperm="strerror" a descriptive error string is returned instead.
#
# - ProcGone is raised if the process becomes terminated and unavailable to inspect;
#   with gone="strerror" as descriptive error string is returned instead.
#
# get adheres to isolation level specified when opening ProcDBConn. See
# documentation for ProcDBConn and Proc for details.
@func(_Proc)
def get(proc, name, eperm="raise", gone="raise"):
    assert eperm in ("raise", "strerror"), eperm
    assert gone  in ("raise", "strerror"), gone

    def eraise(e):
        if isinstance(e, ProcPermissionDenied):
            if eperm == "strerror":
                return "(%s)" % e
        if isinstance(e, ProcGone):
            if gone == "strerror":
                return "(%s)" % e
        raise e

    if proc.pdbc.isolation_level in (ISOLATION_REPEATABLE_READ, ISOLATION_SNAPSHOT):
        v = proc._observed.get(name, _missing)
        if v is not _missing:
            if isinstance(v, Exception):
                v = eraise(v)
            return v
    f = None
    for cls in proc.__class__.__mro__:
        if not hasattr(cls, '_properties'): # e.g. object
            continue
        if name in cls._properties:
            f = cls._properties[name]
            break
    if f is None:
        raise AttributeError("%s has no property %s" % (proc.__class__, name))
    try:
        try:
            v = f(proc)
        except (OSError, IOError) as e:
            if e.errno == EACCES:
                raise ProcPermissionDenied(proc, name)
            if e.errno == ENOENT:
                for _ in range(2): # need to check for prefix again if status recheck failed
                    if not os.path.exists(proc._prefix):
                        raise ProcGone(proc)
                    # for zombie listdir yields the files, e.g. cwd, exe, root, but
                    # readlink'ing them returns ENOENT. So we need to explicitly
                    # check if maybe it is a zombie.
                    try:
                        st = proc._readkv("status")
                    except (OSError, IOError) as e:
                        # need to recheck proc's prefix again
                        continue
                    else:
                        if st.get("State", '').startswith("Z"):
                            raise ProcGone(proc)
            raise
    except Exception as e:
        v = e

    if proc.pdbc.isolation_level in (ISOLATION_REPEATABLE_READ, ISOLATION_SNAPSHOT):
        proc._observed[name] = v

    if isinstance(v, Exception):
        v = eraise(v)
    return v

@func(ProcPermissionDenied)
def __init__(e, proc, property):
    e.proc = proc
    e.property = property

@func(ProcGone)
def __init__(e, proc):
    e.proc = proc

# @_defproperty(Klass) defines property f on Klass.
#
# the property will be known to _Proc.get and adheres to ProcDBConn isolation level.
def _defproperty(cls):
    if '_properties' not in cls.__dict__:
        cls._properties = {}
    def _defproperty2(f):
        name = f.__name__
        cls._properties[name] = f
        def _(proc):
            return proc.get(name)
        setattr(cls, name, property(_))
        return _
    return _defproperty2


# user returns symbolic name of the user running the process.
@_defproperty(_Proc)
def user(proc): # -> str
    return pwd.getpwuid(proc.uid).pw_name


# uid returns numeric user ID of the user running the process.
@_defproperty(_Proc)
def uid(proc):
    _ = proc.status['Uid']
    uid, _, _, _ = _.split()
    return int(uid)


# comm returns "command name" string of the process.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_comm.5.html for details.
@_defproperty(_Proc)
def comm(proc): # -> str
    return proc._readfile("comm").rstrip('\n')

# argv returns vector of arguments with which the process is running.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_cmdline.5.html for details.
@_defproperty(Proc)
def argv(proc): # -> ()str
    _ = proc._readfile("cmdline")
    _ = _.split('\x00')
    assert _[-1] == '', _
    return tuple(_[:-1])

# exe returns path to executable ran by the process.
#
# For some processes, for example for kernel threads, there is no executable.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_exe.5.html for details.
@_defproperty(Proc)
def exe(proc):  # -> str | None
    try:
        exe = proc._readlink("exe")
    except (OSError, IOError) as e:
        if e.errno == ENOENT:   # e.g. for kthreadd even as root
            exe = None
        else:
            raise
    return exe

# cwd returns path to current working directory of the process.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_cwd.5.html for details.
@_defproperty(Proc)
def cwd(proc): # -> str
    return proc._readlink("cwd")

# root returns path to the root directory of the process.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_root.5.html for details.
@_defproperty(Proc)
def root(proc): # -> str
    return proc._readlink("root")


# status returns dict describing process status.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_status.5.html for details.
@_defproperty(_Proc)
def status(proc): # -> {} k->v
    return proc._readkv("status")

# fd returns information about process file descriptors.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_fd.5.html and
# https://www.man7.org/linux/man-pages/man5/proc_pid_fdinfo.5.html for details.
@_defproperty(Proc)
def fd(proc): # -> {} fd->FDInfo
    d = {}
    for fd in sorted([int(_) for _ in os.listdir("%s/fd" % (proc._prefix))]):
        ifd  = FDInfo()
        ifd.fd      = fd
        try:
            ifd.path    = proc._readlink("fd/%d" % fd)
            ifd.extra   = e = proc._readkv("fdinfo/%d" % fd)
        except (OSError, IOError) as e:
            if e.errno == ENOENT:
                continue    # fd was closed after listdir
        ifd.pos     = int(e.pop("pos"))
        ifd.flags   = int(e.pop("flags"), 8)
        ifd.mnt_id  = int(e.pop("mnt_id"))
        ifd.ino     = int(e.pop("ino"))
        d[fd] = ifd
    return d

# mmaps returns information about process memory mappings.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_maps.5.html for details.
@_defproperty(Proc)
def mmaps(proc): # -> {} addr->Map
    m = {}
    for line in proc._readfile("maps").splitlines():
        mmap = MMap._parse(line)
        if mmap.addr in m:
            raise ValueError("mmap entries with duplicate address:\n%s\n%s" % (mmap, m[mmap.addr]))
        m[mmap.addr] = mmap
    return m


# tasks returns information about process threads.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_task.5.html for details.
@_defproperty(Proc)
def tasks(proc): # -> {} tid->Task
    d = {}
    for tid in sorted([int(_) for _ in os.listdir("%s/task" % (proc._prefix))]):
        task = Task()
        task.pdbc = proc.pdbc
        task.proc = proc
        task.tid  = tid
        task._observed = {}
        d[tid] = task
    return d


# ktraceback returns kernel-level stacktrace of the process.
#
# See https://www.man7.org/linux/man-pages/man5/proc_pid_stack.5.html for details.
@_defproperty(Proc)
def ktraceback(proc):
    s = "%s" % proc
    taskv = list(proc.tasks.values())
    taskv.sort(key=lambda task: task.tid)
    for task in taskv:
        s += "\n"
        s += task.ktraceback
    return s

@_defproperty(Task)
def ktraceback(task): # -> str
    s = "%s\n" % task
    s += task._readfile("stack")
    return s


# _readkv returns parsed dict for a file composed with `key: value` lines.
@func(_Proc)
def _readkv(proc, name): # -> {} k->v
    _ = proc._readfile(name)
    kv = {}
    for line in _.splitlines():
        k, v = line.split(':', 1)
        k = k.strip()
        v = v.strip()
        kv[k] = v
    return kv

# _readfile returns content of named filed under proc's directory.
@func(_Proc)
def _readfile(proc, name):
    return readfile("%s/%s" % (proc._prefix, name))

# _readlink returns destination of a named symlink under proc's directory.
@func(_Proc)
def _readlink(proc, name):
    return os.readlink("%s/%s" % (proc._prefix, name))


# _prefix returns where proc's directory is on procfs.
@func(Proc)
@property
def _prefix(proc):
    return "%s/%d" % (proc.pdbc._prefix, proc.pid)

# _prefix returns where task's directory is on procfs.
@func(Task)
@property
def _prefix(task):
    return "%s/task/%d" % (task.proc._prefix, task.tid)


# ---- misc ----

# readfile reads file @ path.
def readfile(path):
    with open(path) as f:
        return f.read()

# writefile writes data to file @ path.
def writefile(path, data):
    with open(path, "w") as f:
        f.write(data)


_missing = object()
