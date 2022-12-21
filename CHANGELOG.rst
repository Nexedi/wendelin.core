Wendelin.core change history
============================

- `2.0.alpha3`__ (2022-12-21)  More fixes discovered by on-field usage.

  __ https://lab.nexedi.com/nexedi/wendelin.core/compare/5e5ad598...7ce0978d

- `2.0.alpha2`__ (2022-01-27)  Fix several crashes discovered by first on-field usage.

  __ https://lab.nexedi.com/nexedi/wendelin.core/compare/49f826b1...a36cdcc3


2.0.alpha1 (2021-11-16)
-----------------------

This is a major pre-release that reduces wendelin.core RAM consumption
dramatically:

The project switches to be mainly using kernel virtual memory manager.
Bigfiles are now primarily accessed with plain OS-level mmap to files from
synthetic WCFS filesystem. This makes bigfile's cache (now it is the kernel's
pagecache) to be shared in between several processes.

In addition a custom coherency protocol is provided, which allows clients,
that want to receive isolation guarantee ("I" from ACID), to still use the shared
cache and at the same time get bigfile view isolated from other's changes.

By default wendelin.core python client continues to provide full ACID semantics as
before.

In addition to being significantly more efficient, WCFS also fixes
data-corruption bugs that were discovered__ in how Wendelin.core 1 handles
invalidations on BTree topology change.

__ https://lab.nexedi.com/nexedi/wendelin.core/commit/8c32c9f6

Please see wcfs.go__ for description of the new filesystem.

__ https://lab.nexedi.com/nexedi/wendelin.core/blob/master/wcfs/wcfs.go

Major steps: 1__, 2__, 3__, 4__, 5__, 6__, 7__, 8__, 9__, 10__, 11__, 12__,
13__, 14__, 15__, 16__, 17__, 18__, 19__, 20__, 21__.

__ https://lab.nexedi.com/nexedi/wendelin.core/commit/2c152d41?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/e3f2ee2d?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/0e829874?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/a8595565?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/b87edcfe?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/1f2cd49d?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/27df5a3b?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/80153aa5?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/2ab4be93?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/f980471f?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/4430de41?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/6f0cdaff?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/10f7153a?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/fae045cc?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/23362204?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/ceadfcc7?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/1dba3a9a?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/1f866c00?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/e11edc70?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/986cf86e?expanded=1
__ https://lab.nexedi.com/nexedi/wendelin.core/commit/c5e18c74?expanded=1


0.13 (2019-06-18)
-----------------

- Add support for Python 3.7 (commit__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/bca5f79e6f

- Add `RAMArray` class which is compatible to `ZBigArray` in API and semantic,
  but stores its data in RAM only (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/7365979b9d
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/fc9b69d8e1

- Add `lib.xnumpy.structured` - utility to create structured view of an array (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/6a5dfefaf8
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/32ca80e2d5

- Fix logic to keep `ZBigFileH` in sync with ZODB connection (commit__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/d9d6adec1b

- Fix crash on PyVMA deallocation (commit__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/d97641d2ba

- Move `py.bench` to pygolang__ so that it can be used not only in
  Wendelin.core (commit__).

  __ https://pypi.org/project/pygolang/
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/318efce0bf

- Enhance `t/qemu-runlinux` - utility that is used to work on combined
  kernel/user-space workloads (`commit 1`__, 2__, 3__, 4__, 5__, 6__).
  This was in particular useful to develop Linux kernel fixes that are needed
  for Wendelin.core 2.0 (`kernel commit 1`__, 2__, 3__, 4__, 5__, 6__, 7__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/fe541453f8
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/ccca055cfe
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/6ab952207e
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/a568d6d999
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/208aca62ae
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/89fb89929a

  __ https://git.kernel.org/linus/ad2ba64dd489
  __ https://git.kernel.org/linus/10dce8af3422
  __ https://git.kernel.org/linus/bbd84f33652f
  __ https://git.kernel.org/linus/c5bf68fe0c86
  __ https://git.kernel.org/linus/438ab720c675
  __ https://git.kernel.org/linus/7640682e67b3
  __ https://git.kernel.org/linus/d4b13963f217

- Various bugfixes.

0.12 (2018-04-16)
-----------------

- Update licensing to be in line with whole Nexedi stack (`commit`__). Please
  see https://www.nexedi.com/licensing for details, rationale and options.

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/f11386a4

- Add `ArrayRef` utility to find out for a NumPy array its top-level root
  parent and how to recreate the array as some view of the root;
  this builds the foundation for e.g. sending arrays as references without copy
  in CMFActivity joblib backend
  (`commit 1`__, 2__, 3__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/e9d61a89
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/d53371b6
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/450ad804


- Don't crash if during `loadblk()` garbage collection was run twice at tricky
  times (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/4228d8b6
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/3804cc39

- Don't crash on writeout if previously `storeblk()` resulted in error
  (`commit`__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/87bf4908



- Fix `py.bench` and rework it to produce output in Go benchmarking format
  (`commit 1`__, 2__, 3__, 4__, 5__); add benchmarks for handling pagefaults
  (`commit`__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/51f252d4
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/074ce24d
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/ed13c3f9
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/fc08766d
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/5a1ed45a
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/3cfc2728

- Use zodbtools/zodburi, if available, to open database by URL
  (`commit`__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/f785ac07

- Start to make sure it works with ZODB5 too (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/808b59b7
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/0dbf3c44

- Various bugfixes.

0.11 (2017-03-28)
-----------------

- Switch back to using ZBlk0 format by default (`commit`__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/0b68f178

0.10 (2017-03-16)
-----------------

- Tell the world `dtype=object` is not supported (`commit`__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/e44bd761

0.9 (2017-01-17)
----------------

- Avoid deadlocks via doing `storeblk()` calls with virtmem lock released
  (`commit 1`__, 2__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/8bb7f2f2
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/fb4bfb32

- Don't crash if in `loadblk()` implementation an exception is internally
  raised & caught
  (`commit 1`__, 2__, 3__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/9aa6a5d7
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/61b18a40
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/024c246c

0.8 (2016-09-28)
----------------

- Do not leak memory when loading data in ZBlk1 format (`commit`__).

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/542917d1

0.7 (2016-07-14)
------------------

- Add support for Python 3.5 (`commit 1`__, 2__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/20115391
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/e6beab19

- Fix bug in pagemap code which could lead to crashes and other issues (`commit`__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/ee9bcd00

- Various bugfixes

0.6 (2016-06-13)
----------------

- Add support for FORTRAN ordering (`commit 1`__, 2__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/ab9ca2df
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/2ca0f076


- Avoid deadlocks via doing `loadblk()` calls with virtmem lock released
  (`commit 1`__, 2__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/f49c11a3
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/0231a65d

- Various bugfixes

0.5 (2015-10-02)
----------------

- Introduce another storage format, which is optimized for small changes, and
  make it the default.
  (`commit 1`__, 2__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/13c0c17c
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/9ae42085

- Various bugfixes and documentation improvements


0.4 (2015-08-19)
----------------

- Add support for O(δ) in-place BigArray.append() (commit__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/1245acc9

- Implement proper multithreading support (commit__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/d53271b9

- Implement proper RAM pages invalidation when backing ZODB objects are changed
  from outside (`commit 1`__, 2__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/cb779c7b
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/92bfd03e

- Fix all kind of failures that could happen when ZODB connection changes
  worker thread in-between handling requests (`commit 1`__, 2__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/c7c01ce4
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/64d1f40b

- Tox tests now cover usage with FileStorage, ZEO and NEO ZODB storages
  (`commit 1`__, 2__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/010eeb35
  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/7fc4ec66

- Various bugfixes



0.3 (2015-06-12)
----------------

- Add support for automatic BigArray -> ndarray conversion, so that e.g. the
  following::

    A = BigArray(...)
    numpy.mean(A)       # passing BigArray to plain NumPy function

  either succeeds, or raises MemoryError if not enough address space is
  available to cover whole A. (current limitation is ~ 127TB on linux/amd64)

  (commit__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/00db08d6

- Various bugfixes (build-fixes, crashes, overflows, etc)


0.2 (2015-05-25)
----------------

- Add support for O(1) in-place BigArray.resize() (commit__)

  __ https://lab.nexedi.com/nexedi/wendelin.core/commit/ca064f75

- Various build bugfixes (older systems, non-std python, etc)


0.1 (2015-04-03)
----------------

- Initial release
