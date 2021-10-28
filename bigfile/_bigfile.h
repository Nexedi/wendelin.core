#ifndef _WENDELIN_BIGFILE__BIGFILE_H
#define _WENDELIN_BIGFILE__BIGFILE_H

/* Wendelin.bigfile | Python interface to memory/files
 * Copyright (C) 2014-2021  Nexedi SA and Contributors.
 *                          Kirill Smelkov <kirr@nexedi.com>
 *
 * This program is free software: you can Use, Study, Modify and Redistribute
 * it under the terms of the GNU General Public License version 3, or (at your
 * option) any later version, as published by the Free Software Foundation.
 *
 * You can also Link and Combine this program with other software covered by
 * the terms of any of the Free Software licenses or any of the Open Source
 * Initiative approved licenses and Convey the resulting work. Corresponding
 * source of such a combination shall include the source code for all other
 * software used.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * See COPYING file for full licensing terms.
 * See https://www.nexedi.com/licensing for rationale and options.
 */

/* Package _bigfile provides Python bindings to virtmem.
 *
 * - `BigFile` is base class that allows implementing BigFile backends in Python.
 *   Users can inherit from BigFile, implement loadblk/storeblk and this way
 *   provide access to data managed from Python to virtmem subsystem(*).
 * - `BigFileH` represents virtmem file handle for opened BigFile.
 *   It can be mmap'ed and provides writeout control.
 * - `VMA` represents mmap'ed part of a BigFileH.
 *   It provides buffer/memoryview interface for data access.
 *
 * (*) A subclass may additionally provide functionality to map file data into
 *     memory. Please see BigFile documentation for details.
 */

#include <Python.h>
#include <wendelin/bigfile/file.h>
#include <wendelin/bigfile/virtmem.h>

#ifdef  __cplusplus
extern "C" {
#endif

/*
 * python representation of VMA - exposes vma memory as python buffer
 *
 * also exposes:
 *
 *      .filerange()            to know which range in mmaped file this vma covers.
 *      .pagesize()             to know page size of underlying RAM.
 *
 * and:
 *
 *      .addr_start, .addr_stop to know offset of ndarray in VMA.
 *      .pyuser                 generic python-level attribute (see below).
 */
struct PyVMA {
    PyObject pyobj;
    PyObject *in_weakreflist;

    VMA vma;

    /* python-level user of this VMA.
     *
     * for example for ArrayRef to work, BigArray needs to find out VMA ->
     * top-level BigArray object for which this VMA was created.
     *
     * There is vma -> fileh -> file chain, but e.g. for a given ZBigFile there
     * can be several ZBigArrays created on top of it to view its data (e.g. via
     * BigArray.view()). So even if it can go from vma to -> zfile it does not
     * help to find out the top-level ZBigArray object itself.
     *
     * This way we allow BigArray python code to set vma.pyuser attribute
     * pointing to original BigArray object for which this VMA was created. */
    PyObject *pyuser;
};
typedef struct PyVMA PyVMA;


/*
 * python representation of BigFileH - exposes
 *
 *      .mmap()                 to create VMAs and
 *      .dirty_discard() and
 *      .dirty_writeout()       for storing changes back to file.
 *      .isdirty()              for knowing are there any changes at all
 */
struct PyBigFileH {
    PyObject pyobj;
    PyObject *in_weakreflist;

    BigFileH fileh;
};
typedef struct PyBigFileH PyBigFileH;


/*
 * BigFile that can be implemented in python
 *
 * Allows subclasses to implement .loadblk() and .storeblk() in python.
 * For users .fileh_open() is exposed to get to file handles.
 *
 * A subclass may additionally provide functionality to map file data into
 * memory: if subclass provides .blkmmapper attribute, it is treated as
 * pycapsule with type "wendelin.bigfile.IBlkMMapper" and C-level bigfile_ops
 * struct that provides .mmap_setup_read and other operations related to
 * mmapping data. To avoid deadlocks all mmap-related functionality must be
 * nogil and so cannot be implemented in Python.
 *
 * The primary user of .blkmmapper functionality is _ZBigFile which uses WCFS
 * and mmaps files from it to provide memory mappings for ZBigFile data.
 */
struct PyBigFile {
    PyObject pyobj;
    /* NOTE no explicit weakref support is needed - this is a base class and python
     * automatically adds support for weakrefs for in-python defined children   */

    BigFile file;

    /* blkmmapper is PyCapsule object with type.blkmmapper if BigFile subclass has it | NULL */
    PyObject     *blkmmapper;
    /* bigfile_ops extracted from ^^^ capsule | NULL */
    bigfile_ops  *blkmmap_ops;
};
typedef struct PyBigFile PyBigFile;


#ifdef  __cplusplus
}
#endif

#endif  // _WENDELIN_BIGFILE__BIGFILE_H
