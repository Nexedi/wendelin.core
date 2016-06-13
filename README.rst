==========================================
 Wendelin.core - Out-of-core NumPy arrays
==========================================

Wendelin.core allows you to work with arrays bigger than RAM and local disk.
Bigarrays are persisted to storage, and can be changed in transactional manner.

In other words bigarrays are something like `numpy.memmap`_ for numpy.ndarray
and OS files, but support transactions and files bigger than disk. The whole
bigarray cannot generally be used as a drop-in replacement for numpy arrays,
but bigarray *slices* are real ndarrays and can be used everywhere ndarray can
be used, including in C/Cython/Fortran code. Slice size is limited by
virtual address-space size, which is ~ max 127TB on Linux/amd64.

The main class to work with is `ZBigArray` and is used like `ndarray` from
`NumPy`_:

1. create array::

    from wendelin.bigarray.array_zodb import ZBigArray
    import transaction

    # root is connected to opened database
    root['A'] = A = ZBigArray(shape=..., dtype=...)
    transaction.commit()

2. view array as a real ndarray::

    a = A[:]        # view which covers all array, if it fits into address-space
    b = A[10:100]

   data for views will be loaded lazily on memory access.

3. work with views, including using C/Cython/Fortran functions from NumPy
   and other libraries to read/modify data::

    a[2] = 1
    a[10:20] = numpy.arange(10)
    numpy.mean(a)

   | the amount of modifications in one transaction should be less than available RAM.
   | the amount of data read is limited only by virtual address-space size.

4. data can be appended to array in O(δ) time::

    values                  # ndarray to append of shape  (δ,)
    A.append(values)

   and array itself can be resized in O(1) time::

    A.resize(newshape)

5. changes to array data can be either discarded or saved back to DB::

    transaction.abort()     # discard all made changes
    transaction.commit()    # atomically save all changes



When using NEO_ or ZEO_ as a database, bigarrays can be simultaneously used by
several nodes in a cluster.


Please see `demo/demo_zbigarray.py`__ for a complete example.


.. _NumPy:          http://www.numpy.org/
.. _numpy.memmap:   http://docs.scipy.org/doc/numpy/reference/generated/numpy.memmap.html
.. _NEO:            http://www.neoppod.org/
.. _ZEO:            https://pypi.python.org/pypi/ZEO

__ demo/demo_zbigarray.py
