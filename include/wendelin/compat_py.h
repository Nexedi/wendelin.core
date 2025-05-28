#ifndef _WENDELIN_COMPAT_PY_H
#define _WENDELIN_COMPAT_PY_H

/* Wendelin. Python compatibility
 * Copyright (C) 2014-2025  Nexedi SA and Contributors.
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

/* compatibility with older versions of python */
#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif


/* in python2 it was indicating support for memoryview */
#ifndef Py_TPFLAGS_HAVE_NEWBUFFER
# define Py_TPFLAGS_HAVE_NEWBUFFER  0L
#endif


/* PyMemoryView_FromMemory for python2
 * (taking PY2 PyMemoryView_FromBuffer, and
 *         PY3 PyMemoryView_FromMemory as templates)
 */
#if PY_MAJOR_VERSION < 3
static inline PyObject *
PyMemoryView_FromMemory(char *mem, Py_ssize_t size, int flags)
{
    PyMemoryViewObject *mview;
    int readonly;

    mview = (PyMemoryViewObject *)
        PyObject_GC_New(PyMemoryViewObject, &PyMemoryView_Type);
    if (mview == NULL)
        return NULL;
    mview->base = NULL;

    readonly = (flags == PyBUF_WRITE) ? 0 : 1;
    (void)PyBuffer_FillInfo(&mview->view, NULL, mem, size, readonly,
                            PyBUF_FULL_RO);

    _PyObject_GC_TRACK(mview);
    return (PyObject *)mview;
}
#endif


/* structure of PyBufferObject  (Objects/bufferobject.c) */
#if PY_MAJOR_VERSION < 3
typedef struct {
    PyObject_HEAD
    PyObject *b_base;
    void *b_ptr;
    Py_ssize_t b_size;
    Py_ssize_t b_offset;
    int b_readonly;
    long b_hash;
} PyBufferObject;
#endif


/* get current thread state without asserting it is !NULL
 * (PyThreadState_Get() does the assert) */
#if PY_MAJOR_VERSION < 3
static inline PyThreadState * _PyThreadState_UncheckedGet(void)
{
    return _PyThreadState_Current;
}
/* _PyThreadState_UncheckedGet() was added in CPython 3.5.2rc1
 * https://github.com/python/cpython/commit/df858591
 *
 * During CPython 3.5.0 - 3.5.rc1 there is a window when
 * - public access to pyatomics was removed, and
 * - _PyThreadState_UncheckedGet() was not added yet
 *
 * https://bugs.python.org/issue25150
 * https://bugs.python.org/issue26154 */
#elif PY_VERSION_HEX < 0x03050000
static inline PyThreadState * _PyThreadState_UncheckedGet(void)
{
    return (PyThreadState*)_Py_atomic_load_relaxed(&_PyThreadState_Current);
}
#elif PY_VERSION_HEX < 0x03050200
# error "You are using CPython 3.5.X series. Upgrade your CPython to >= 3.5.2 to get _PyThreadState_UncheckedGet() support."
#endif


/* PyThreadState_GetFrame for py < 3.9 */
#if PY_VERSION_HEX < 0x03090000
static inline PyFrameObject* PyThreadState_GetFrame(PyThreadState *tstate)
{
    PyFrameObject *frame = tstate->frame;
    Py_XINCREF(frame);
    return frame;
}
#endif


#ifdef __cplusplus
}
#endif

#endif
