# Wendelin.core | pythonic package setup
# Copyright (C) 2014-2015  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Open Source Initiative approved licenses and Convey
# the resulting work. Corresponding source of such a combination shall include
# the source code for all other software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
from setuptools import setup, Extension, Command, find_packages
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.build_ext import build_ext as _build_ext
from distutils.errors import DistutilsExecError
from subprocess import Popen, PIPE

import os
import sys


_bigfile = Extension('wendelin.bigfile._bigfile',
            sources = [
                'bigfile/_bigfile.c',
                'bigfile/pagefault.c',
                'bigfile/pagemap.c',
                'bigfile/ram.c',
                'bigfile/ram_shmfs.c',
                'bigfile/ram_hugetlbfs.c',
                'bigfile/virtmem.c',
                'lib/bug.c',
                'lib/utils.c',
            ],
            include_dirs = [
                './include',
                './3rdparty/ccan',
                './3rdparty/include'
            ],
            define_macros       = [('_GNU_SOURCE',None)],
            extra_compile_args  = [
                '-std=gnu99',           # declarations inside for-loop
                '-fplan9-extensions',   # anonymous-structs + simple inheritance
                '-fvisibility=hidden',  # by default symbols not visible outside DSO

                # in C99 declaration after statement is ok, and we explicitly compile with -std=gnu99.
                # Python >= 3.4 however adds -Werror=declaration-after-statement even for extension
                # modules irregardless of their compilation flags:
                #
                #   https://bugs.python.org/issue21121
                #
                # ensure there is no warnings / errors for decl-after-statements.
                '-Wno-declaration-after-statement',
                '-Wno-error=declaration-after-statement',
            ],

            # can't - at runtime links with either python (without libpython) or libpython
            # linking with both libpython and python would be wrong
            #extra_link_args     = [
            #    '-Wl,--no-undefined',   # check DSO for undefined symbols at link time
            #]
            )


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
    err = os.system('make %s PYTHON="%s"' % (target, sys.executable))
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


# build_ext that
# - builds via Makefile  (and thus pre-builds ccan)  XXX hacky
class build_ext(_build_ext):

    def run(self):
        runmake('all')
        return _build_ext.run(self)



# register `func` as entry point `entryname` in `groupname` in distribution `distname` on the fly
def register_as_entrypoint(func, entryname, groupname, distname):
    from pkg_resources import working_set, EntryPoint
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

    return output


def git_lsfiles(dirname):
    # FIXME dirname is currently ignored
    # XXX non-ascii names, etc
    for _ in runcmd(['git', 'ls-files']).splitlines():
        yield _.decode('utf8')  # XXX utf8 hardcoded
    # XXX recursive submodules
    for _ in runcmd(['git', 'submodule', 'foreach', '--quiet',  \
                r'git ls-files | sed "s|\(.*\)\$|$path/\1|g"']).splitlines():
        yield _.decode('utf8')  # XXX utf8 hardcoded

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

setup(
    name        = 'wendelin.core',
    version     = '0.4',
    description = 'Out-of-core NumPy arrays',
    long_description = '%s\n----\n\n%s' % (
                            readfile('README.rst'), readfile('CHANGELOG')),
    url         = 'https://lab.nexedi.com/nexedi/wendelin.core',
    license     = 'GPLv3+ with wide exception for Open-Source',
    author      = 'Kirill Smelkov',
    author_email= 'kirr@nexedi.com',

    keywords    = 'bigdata out-of-core numpy virtual-memory',

    ext_modules = [_bigfile],

    package_dir = {'wendelin': ''},
    packages    = ['wendelin'] + ['wendelin.%s' % _ for _ in
                        find_packages(exclude='3rdparty')],
    install_requires = [
                   'numpy',     # BigArray + its children

                   # for ZBigFile / ZBigArray
                   # ( NOTE: ZODB3 3.11 just pulls in latest ZODB _4_, so this way
                   #   specifying ZODB _3_ we allow external requirements to
                   #   specify either to use e.g. ZODB3.10 or ZODB4 )
                   'ZODB3 >= 3.10',

                   'six',       # compat py2/py3

                   'psutil',    # demo_zbigarray
                  ],

    extras_require = {
                   'test': ['pytest'],
    },

    cmdclass    = {'build_ext':     build_ext,
                   'll_build_ext':  _build_ext, # original build_ext for Makefile
                   'build_py':      build_py,
                   'test':          viamake('test',     'run tests'),
                   'bench':         viamake('bench',    'run benchmarks'),
                  },

    entry_points= {'console_scripts': [
                        # demo to test
                        'demo-zbigarray = wendelin.demo.demo_zbigarray:main',
                      ]
                  },

    classifiers = [_.strip() for _ in """\
        Development Status :: 3 - Alpha
        Intended Audience :: Developers
        Intended Audience :: Science/Research
        Operating System :: POSIX :: Linux
        Programming Language :: Python :: 2
        Programming Language :: Python :: 2.7
        Programming Language :: Python :: 3
        Programming Language :: Python :: 3.4
        Programming Language :: Python :: Implementation :: CPython
        Topic :: Software Development :: Libraries :: Python Modules
        Framework :: ZODB\
    """.splitlines()]
)
