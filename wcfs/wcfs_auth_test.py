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
"""wcfs_auth_test.py complements wcfs_test.py with tests that exercise
client authentication."""

from __future__ import print_function, absolute_import

from collections import namedtuple
import importlib
import os

from zodbtools.util import ashex

from wendelin.lib.zodb import zstor_2zurl
from wendelin.wcfs.internal import multiprocessing as xmp
from wendelin import wcfs

from golang import func, defer, context

from wendelin.wcfs.wcfs_test import tDB, tAt, \
    setup_module, teardown_module

from wendelin.conftest import authkey


# tSubProcess runs f(tClient, *argv, *kw) in subprocess.
# It's a small convenience wrapper over xmp.SubProcess - please see its documentation for details.
class tSubProcess(xmp.SubProcess):
    def __init__(fproc, t, f, *argv, **kw):
        kw.setdefault('zurl',         zstor_2zurl(t.root._p_jar.db().storage))
        kw.setdefault('zfile_oid',    t.zfile._p_oid)
        kw.setdefault('authenticate', False)  # default: don't auto-authenticate

        # Optional pintimeout for subclasses that need it
        if hasattr(t, 'pintimeout'):
            kw.setdefault('pintimeout', t.pintimeout)
            fproc._validate_timeout(t)

        for k,v in list(kw.items()):
            if isinstance(v, tAt):  # tAt is not picklable
                kw[k] = v.raw

        # Pass fully qualified name so subprocess can find the function
        funcpath = '%s.%s' % (f.__module__, f.__name__)
        super(tSubProcess, fproc).__init__(_tSubProcess_start, funcpath, *argv, **kw)
        assert fproc.cout.recv() == "f: start"

    # Override in subclasses to add timeout validation.
    def _validate_timeout(fproc, t):
        pass

@func
def _tSubProcess_start(cin, cout, funcname, authenticate, **kw):
    f = tClient()
    f.cin  = cin
    f.cout = cout
    f.zurl = kw.pop('zurl')
    f.zfile_oid = kw.pop('zfile_oid')
    f.pintimeout = kw.pop('pintimeout', None)  # Optional
    f.wc = wcfs.join(f.zurl, autostart=False);  defer(f.wc.close)
    ctx = context.background()
    f.cout.send("f: start")

    if authenticate:
        alink = wcfs.AuthLink(f.wc); defer(alink.close)
        reply = alink.sendReq(context.background(), "auth %s" % authkey)
        assert reply == b"ok", "authentication failed: %s" % reply

    # Look up test function in the module where it was defined
    # funcname format: "module.function"
    if '.' in funcname:
        modname, fname = funcname.rsplit('.', 1)
        mod = importlib.import_module(modname)
        testf = getattr(mod, fname)
    else:
        # Fallback: look in current module's globals
        testf = globals()[funcname]

    testf(ctx, f, **kw)

# tClient is placeholder for arguments + WCFS connection for running test
# function inside tSubProcess.
class tClient:
    # .cin
    # .cout
    # .zurl
    # .zfile_oid
    # .wc
    # .pintimeout  (optional)
    pass


# Perm encapsulates the permissions a user has to access a file.
class Perm(namedtuple('Perm', 'open read write')):

    # Perm.Flag represents the state of a single permission.
    # DENIED:   permission explicitly denied
    # ALLOWED:  permission explicitly allowed
    # SKIP:     permission type not applicable / not checked
    class Flag(int):
        @property
        def skip(self):
            return self is Perm.Flag.SKIP

        @property
        def allowed(self):
            return self is Perm.Flag.ALLOWED

        @property
        def denied(self):
            return self is Perm.Flag.DENIED

    Flag.DENIED = Flag(0)
    Flag.ALLOWED = Flag(1)
    Flag.SKIP = Flag(-1)  # if WCFS doesn't implement access type

    # Construct a Perm object from a string representation
    @classmethod
    def from_string(cls, string):
        s = (string + "----")[:len(cls._fields)]
        return cls(**dict(zip(cls._fields, (cls.CHAR_MAP[c] for c in s))))

    CHAR_MAP = {
        '+': Flag.ALLOWED,
        '-': Flag.DENIED,
        '.': Flag.SKIP,
    }

    # Construct a Perm object by probing the filesystem at the given path
    @classmethod
    def from_path(cls, path, **kwargs):
        def flag(val):
            if val is None:
                return cls.Flag.SKIP
            return cls.Flag.ALLOWED if val else cls.Flag.DENIED

        perms = _check_permissions(path, **kwargs)
        return cls(**{k: flag(v) for k, v in perms.items()})


# Probe the filesystem to determine open/read/write status
def _check_permissions(path, **kwargs):
    status = {}

    def probe(kind, func):
        v = None
        if kwargs.get("check_%s" % kind, False):
            try:
                with open(path, "r+b") as f:
                    func(f)
                v = True
            except (OSError, IOError):
                v = False
        status[kind] = v

    probe('open',  lambda f: None)
    probe('read',  lambda f: f.read(1))
    probe('write', lambda f: f.seek(0, os.SEEK_END) or f.flush())

    return status


# tPermission provides infrastructure to test if current process has access to WCFS
class tPermission:
    def __init__(self, mountpoint, oid):
        self.mountpoint = mountpoint
        self.oid = ashex(oid)

    # Returns error string if failed and None when succeeded
    def verify(self, authenticated):
        for path, perm_tuple in tPermission.PATH_MAP.items():
            perm_ok = perm_tuple[authenticated]
            perm = Perm.from_path(
                "%s%s" % (self.mountpoint, path.format(**{'oid': self.oid})),
                **{"check_%s" % k: not v.skip for k, v in perm_ok._asdict().items()}
            )
            assert perm == perm_ok, "when access '%s':\ngot\t%s\nwant\t%s" % (path, perm, perm_ok)

    PATH_MAP = {
        path: tuple(Perm.from_string(p) for p in perms)
        for path, perms in {
            "/.wcfs/zurl":          ("++.", "++."),
            "/.wcfs/stats":         ("--.", "++."),
            "/.wcfs/pintimeout":    ("--.", "++."),
            "/head/watch":          ("---", "+.."),  # XXX how to test authenticated->read/write ?
            "/head/at":             ("--.", "++."),
            "/head/bigfile/{oid}":  ("--.", "++."),
        }.items()
    }


# ---- tests ----


# Ensure that an authenticated client can access private WCFS files.
@func
def test_authenticated_user_access():
    t = tDB(multiproc=True); zf = t.zfile
    defer(t.close)
    perm = tPermission(t.wc.mountpoint, zf._p_oid)
    perm.verify(1)


# Ensure that an unauthenticated client cannot access private WCFS files.
@func
def test_unauthenticated_client_no_access():
    t = tDB(multiproc=True)
    defer(t.close)
    p = tSubProcess(t, _unauthenticated_access)
    p.join(t.ctx)
    assert p.exitcode == 0

def _unauthenticated_access(ctx, f):
    perm = tPermission(f.wc.mountpoint, f.zfile_oid)
    perm.verify(0)


# Ensure that a client providing an invalid authentication key is not authenticated.
@func
def test_bad_authkey():
    t = tDB(multiproc=True)
    defer(t.close)
    p = tSubProcess(t, _bad_authkey)
    p.join(t.ctx)
    assert p.exitcode == 0

@func
def _bad_authkey(ctx, f):
    alink = wcfs.AuthLink(f.wc); defer(alink.close)
    reply = alink.sendReq(context.background(), "auth badkey")
    assert reply == b"error invalid authentication key"
    perm = tPermission(f.wc.mountpoint, f.zfile_oid)
    perm.verify(0)


# Ensure that a client is deauthenticated when its authentication link is closed.
@func
def test_deauthenticate_on_close():
    t = tDB(multiproc=True)
    defer(t.close)
    p = tSubProcess(t, _deauthenticate_on_close)
    p.join(t.ctx)
    assert p.exitcode == 0

def _deauthenticate_on_close(ctx, f):
    alink = wcfs.AuthLink(f.wc)
    reply = alink.sendReq(ctx, "auth %s" % authkey)
    assert reply == b"ok"
    perm = tPermission(f.wc.mountpoint, f.zfile_oid)
    perm.verify(1)
    alink.close()
    perm.verify(0)