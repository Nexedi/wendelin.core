Wendelin.core change history
============================

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
