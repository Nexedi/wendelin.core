# Wendelin.core | pythonic package setup
# Copyright (C) 2014-2022  Nexedi SA and Contributors.
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
from golang.pyx.build import setup, DSO as _DSO, Extension as _PyGoExt, build_ext as _build_ext
from setuptools_dso import Extension, build_dso as _build_dso
from setuptools import Command, find_packages
from setuptools.command.build_py import build_py as _build_py
from pkg_resources import working_set, EntryPoint
from distutils.file_util import copy_file
from distutils.errors import DistutilsExecError
from subprocess import Popen, PIPE

import os
import sys
from errno import EEXIST

PY3 = (sys.version_info.major >= 3)


# tell cython to resolve `cimport wendelin.*` modules hierarchy starting at top-level.
# see wendelin.py for details.
from Cython.Compiler.Main import Context as CyContext
cy_search_inc_dirs = CyContext.search_include_directories
def wendelin_cy_searh_in_dirs(self, qualified_name, suffix, pos, include=False, sys_path=False):
    _ = qualified_name.split('.')
    if len(_) >= 1 and _[0] == "wendelin":
        qualified_name = '.'.join(_[1:])
    return cy_search_inc_dirs(self, qualified_name, suffix, pos, include, sys_path)
CyContext.search_include_directories = wendelin_cy_searh_in_dirs


# _with_defaults calls what(*argv, **kw) with kw amended with default build flags.
# e.g. _with_defaults(_DSO, *argv, **kw)
def _with_defaults(what, *argv, **kw):
    kw = kw.copy()
    kw['include_dirs'] = [
            '.',
            './include',
            './3rdparty/ccan',
            './3rdparty/include',
        ]

    ccdefault = []
    if kw.get('language') == 'c':
        ccdefault += [
            '-std=gnu99',           # declarations inside for-loop
            '-fplan9-extensions',   # anonymous-structs + simple inheritance

            # in C99 declaration after statement is ok, and we explicitly compile with -std=gnu99.
            # Python >= 3.4 however adds -Werror=declaration-after-statement even for extension
            # modules irregardless of their compilation flags:
            #
            #   https://bugs.python.org/issue21121
            #
            # ensure there is no warnings / errors for decl-after-statements.
            '-Wno-declaration-after-statement',
            '-Wno-error=declaration-after-statement',
        ]
    else:
        ccdefault.append('-std=gnu++11')    # not c++11 since we use typeof

    # DSOs are not yet annotated for visibility
    # XXX pyext besides _bigfile.so also cannot do this because PyMODINIT_FUNC
    # does not include export in it. TODO reenable for _bigfile.so
    """
    if what != _DSO:
        ccdefault.append('-fvisibility=hidden')  # by default symbols not visible outside DSO
    """

    _ = kw.get('extra_compile_args', [])[:]
    _[0:0] = ccdefault
    kw['extra_compile_args'] = _

    lddefault = []
    # python extensions cannot be built with -Wl,--no-undefined: at runtime
    # they link with either python (without libpython) or libpython. linking
    # with both libpython and python would be wrong.
    if what == _DSO:
        lddefault.append('-Wl,--no-undefined')  # check DSO for undefined symbols at link time

    _ = kw.get('extra_link_args', [])[:]
    _[0:0] = lddefault
    kw['extra_link_args'] = _

    return what(*argv, **kw)


def PyGoExt(*argv, **kw):   return _with_defaults(_PyGoExt, *argv, **kw)
def DSO(*argv, **kw):       return _with_defaults(_DSO, *argv, **kw)


# build_py that
# - prevents in-tree wendelin.py & setup.py to be installed
# - synthesizes wendelin/__init__.py on install
class build_py(_build_py):

    def find_package_modules(self, package, package_dir):
        modules = _build_py.find_package_modules(self, package, package_dir)
        try:
            modules.remove(('wendelin', 'wendelin', 'wendelin.py'))
            modules.remove(('wendelin', 'setup',    'setup.py'))
        except ValueError:
            pass    # was not there

        return modules

    def build_packages(self):
        _build_py.build_packages(self)
        # emit std namespacing mantra to wendelin/__init__.py
        self.initfile = self.get_module_outfile(self.build_lib, ('wendelin',), '__init__')
        with open(self.initfile, 'w') as f:
            f.write("# this is a namespace package (autogenerated)\n")
            f.write("__import__('pkg_resources').declare_namespace(__name__)\n")


    def get_outputs(self, include_bytecode=1):
        outputs = _build_py.get_outputs(self, include_bytecode)

        # add synthesized __init__.py to outputs, so that `pip uninstall`
        # works without leaving it
        outputs.append(self.initfile)
        if include_bytecode:
            if self.compile:
                outputs.append(self.initfile + 'c')
            if self.optimize:
                outputs.append(self.initfile + 'o')

        return outputs






# run `make <target>`
def runmake(target):
    # NOTE we care to propagate setuptools path to subpython because it could
    # be "inserted" to us by buildout. Propagating whole sys.path is more
    # risky, as e.g. it can break gdb which is using python3, while
    # building/testing under python2.
    pypathv = [working_set.by_key['setuptools'].location]
    pypath  = os.environ.get('PYTHONPATH')
    if pypath is not None:
        pypathv.append(pypath)

    err = os.system('make %s PYTHON="%s" PYTHONPATH="%s" INMAKE=1' % \
            (target, sys.executable, ':'.join(pypathv)))
    if err:
        raise DistutilsExecError('Failed to execute `make %s`' % target)


# create distutils command to "run `make <target>`"
def viamake(target, help_text):
    class run_viamake(Command):
        user_options = []
        def initialize_options(self):   pass
        def finalize_options(self):     pass

        description  = help_text
        def run(self):
            runmake(target)

    return run_viamake

# build_dso that
# - builds via Makefile  (and thus pre-builds ccan)  XXX hacky
class build_dso(_build_dso):

    def run(self):
        if 'INMAKE' not in os.environ:
            runmake('all')
        else:
            _build_dso.run(self)

# build_ext that
# - builds via Makefile  (and thus pre-builds ccan)  XXX hacky
# - integrates built wcfs/wcfs.go exe into wcfs/ python package
class build_ext(_build_ext):

    def run(self):
        runmake('all')
        _build_ext.run(self)

        # copy wcfs/wcfs built from wcfs.go by make into build/lib.linux-x86_64-2.7/...
        # so that py packaging tools see built wcfs.go as part of wendelin/wcfs package.
        dst = os.path.join(self.build_lib, 'wendelin/wcfs')
        try:
            os.makedirs(dst)
        except OSError as e:
            if e.errno != EEXIST:
                raise
        copy_file('wcfs/wcfs', dst, verbose=self.verbose, dry_run=self.dry_run)




# register `func` as entry point `entryname` in `groupname` in distribution `distname` on the fly
def register_as_entrypoint(func, entryname, groupname, distname):
    dist = working_set.by_key[distname]

    # register group if it is not yet registered
    # else dist.get_entry_map(groupname) returns just {} not connected to entry map
    entry_map = dist.get_entry_map()
    if groupname not in entry_map:
        entry_map[groupname] = {}

    entrypoint = EntryPoint(entryname, func.__module__, attrs=(func.__name__,),
                                    extras=(), dist=dist)
    group = dist.get_entry_map(groupname)
    assert entryname not in group
    group[entryname] = entrypoint

    # XXX hack to workaround ImportError in PEP517 mode: pip -> pep517 -> _in_process
    # sources, not imports, setup.py and so there, even though git_lsfiles.__module__=='__main__',
    # the module that is actually __main__ is pip. This leads to ImportError
    # when trying to resolve the entrypoint.
    mod = sys.modules[func.__module__]
    _ = getattr(mod, func.__name__, func)
    assert _ is func
    setattr(mod, func.__name__, func)


# like subprocess.check_output(), but properly report errors, if e.g. commands is not found
# check_output(['missing-command']) -> error: [Errno 2] No such file or directory
# runcmd      (['missing-command']) -> error: ['missing-command']: [Errno 2] No such file or directory
def runcmd(argv):
    try:
        process = Popen(argv, stdout=PIPE)
    except Exception as e:
        raise RuntimeError("%s: %s" % (argv, e))

    output, _err = process.communicate()
    retcode = process.poll()
    if retcode:
        raise RuntimeError("%s -> failed (status %s)" % (argv, retcode))

    if PY3:
        output = output.decode('utf-8')
    return output


def git_lsfiles(dirname):
    # FIXME dirname is currently ignored
    # XXX non-ascii names, etc
    for _ in runcmd(['git', 'ls-files']).splitlines():
        yield _
    # XXX recursive submodules
    for _ in runcmd(['git', 'submodule', 'foreach', '--quiet',  \
                r'git ls-files | sed "s|\(.*\)\$|$path/\1|g"']).splitlines():
        yield _

# if we are in git checkout - inject git_lsfiles to setuptools.file_finders
# entry-point on the fly.
#
# otherwise, for released sdist tarball we do not - that tarball already
# contains generated SOURCES.txt and sdist, if run from unpacked tarball, would
# reuse that information.
#
# (to setuptools - because we have to use some distribution and we already
#  depend on setuptools)
if os.path.exists('.git'):  # FIXME won't work if we are checked out as e.g. submodule
    register_as_entrypoint(git_lsfiles, 'git', 'setuptools.file_finders', 'setuptools')


# read file content
def readfile(path):
    with open(path, 'r') as f:
        return f.read()

libvirtmem_h = [
    '3rdparty/include/linux/list.h',
    'include/wendelin/bigfile/file.h',
    'include/wendelin/bigfile/pagemap.h',
    'include/wendelin/bigfile/ram.h',
    'include/wendelin/bigfile/types.h',
    'include/wendelin/bigfile/virtmem.h',
    'include/wendelin/bug.h',
    'include/wendelin/list.h',
    'include/wendelin/utils.h',
]

libwcfs_h = [
    'wcfs/client/wcfs.h',
    'wcfs/client/wcfs_misc.h',
    'wcfs/client/wcfs_watchlink.h',
]

setup(
    name        = 'wendelin.core',
    version     = '2.0.alpha2.post1',
    description = 'Out-of-core NumPy arrays',
    long_description = '%s\n----\n\n%s' % (
                            readfile('README.rst'), readfile('CHANGELOG.rst')),
    url         = 'https://lab.nexedi.com/nexedi/wendelin.core',
    license     = 'GPLv3+ with wide exception for Open-Source',
    author      = 'Kirill Smelkov',
    author_email= 'kirr@nexedi.com',

    keywords    = 'bigdata out-of-core numpy virtual-memory',

    x_dsos      = [DSO('wendelin.bigfile.libvirtmem',
                    ['bigfile/pagefault.c',
                     'bigfile/pagemap.c',
                     'bigfile/ram.c',
                     'bigfile/ram_shmfs.c',
                     'bigfile/ram_hugetlbfs.c',
                     'bigfile/virtmem.c',
                     'lib/bug.c',
                     'lib/utils.c'],
                    depends = libvirtmem_h,
                    define_macros       = [('_GNU_SOURCE',None)],
                    language = 'c'),

                   DSO('wendelin.wcfs.client.libwcfs',
                    ['wcfs/client/wcfs.cpp',
                     'wcfs/client/wcfs_watchlink.cpp',
                     'wcfs/client/wcfs_misc.cpp'],
                    depends = libvirtmem_h + libwcfs_h,
                    dsos = ['wendelin.bigfile.libvirtmem'])],

    ext_modules = [
                    PyGoExt('wendelin.bigfile._bigfile',
                        ['bigfile/_bigfile.c'],
                        depends = [
                         'bigfile/_bigfile.h',
                         'include/wendelin/compat_py2.h',
                        ] + libvirtmem_h,
                        define_macros   = [('_GNU_SOURCE',None)],
                        language        = 'c',
                        dsos = ['wendelin.bigfile.libvirtmem']),

                    PyGoExt('wendelin.bigfile._file_zodb',
                        ['bigfile/_file_zodb.pyx',
                         'bigfile/file_zodb.cpp'],
                        depends = [
                         'wcfs/client/_wcfs.pxd',
                         'wcfs/client/_wczsync.pxd',
                         'bigfile/_bigfile.h',
                        ] + libwcfs_h + libvirtmem_h,
                        dsos = ['wendelin.wcfs.client.libwcfs']),

                    PyGoExt('wendelin.wcfs.client._wcfs',
                        ['wcfs/client/_wcfs.pyx'],
                        depends = libwcfs_h + libvirtmem_h,
                        dsos = ['wendelin.wcfs.client.libwcfs']),

                    PyGoExt('wendelin.wcfs.client._wczsync',
                        ['wcfs/client/_wczsync.pyx'],
                        depends = [
                         'wcfs/client/_wcfs.pxd',
                        ] + libwcfs_h + libvirtmem_h,
                        dsos = ['wendelin.wcfs.client.libwcfs']),

                    PyGoExt('wendelin.wcfs.internal.wcfs_test',
                        ['wcfs/internal/wcfs_test.pyx']),

                    Extension('wendelin.wcfs.internal.io',
                        ['wcfs/internal/io.pyx']),

                    Extension('wendelin.wcfs.internal.mm',
                        ['wcfs/internal/mm.pyx']),
                  ],

    package_dir = {'wendelin': ''},
    packages    = ['wendelin'] + ['wendelin.%s' % _ for _ in
                        find_packages(exclude='3rdparty')],
    install_requires = [
                   'numpy',     # BigArray + its children

                   'ZODB >= 4', # for ZBigFile / ZBigArray
                   'zodbtools >= 0.0.0.dev8', # lib.zodb.dbstoropen + ...

                   'pygolang >= 0.1', # defer, sync.WaitGroup, pyx/nogil channels ...

                   'six',       # compat py2/py3

                   'psutil',    # demo_zbigarray
                  ],

    extras_require = {
                   'test': [
                     'pytest',
                     'scipy',
                     'ZEO',         # lib/tests/test_zodb.py
                    ],
    },

    cmdclass    = {'build_dso':     build_dso,
                   'build_ext':     build_ext,
                   'll_build_ext':  _build_ext, # original build_ext for Makefile
                   'build_py':      build_py,
                   'test':          viamake('test',     'run tests'),
                   'bench':         viamake('bench',    'run benchmarks'),
                  },

    entry_points= {'console_scripts': [
                        # start wcfs for zurl
                        'wcfs           = wendelin.wcfs:main',

                        # demo to test
                        'demo-zbigarray = wendelin.demo.demo_zbigarray:main',
                      ]
                  },

    classifiers = [_.strip() for _ in """\
        Development Status :: 5 - Production/Stable
        Intended Audience :: Developers
        Intended Audience :: Science/Research
        Operating System :: POSIX :: Linux
        Programming Language :: Python :: 2
        Programming Language :: Python :: 2.7
        Programming Language :: Python :: 3
        Programming Language :: Python :: 3.6
        Programming Language :: Python :: 3.7
        Programming Language :: Python :: Implementation :: CPython
        Topic :: Software Development :: Libraries :: Python Modules
        Framework :: ZODB\
    """.splitlines()]
)
