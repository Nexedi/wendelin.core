#ifndef _WENDELIN_COMPAT_PY2_H
#define _WENDELIN_COMPAT_PY2_H

/* compatibility with python2 */
#include <Python.h>

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

#endif
