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


#endif
