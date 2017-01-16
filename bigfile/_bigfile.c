/* Wendelin.bigfile | Python interface to memory/files
 * Copyright (C) 2014-2015  Nexedi SA and Contributors.
 *                          Kirill Smelkov <kirr@nexedi.com>
 *
 * This program is free software: you can Use, Study, Modify and Redistribute
 * it under the terms of the GNU General Public License version 3, or (at your
 * option) any later version, as published by the Free Software Foundation.
 *
 * You can also Link and Combine this program with other software covered by
 * the terms of any of the Open Source Initiative approved licenses and Convey
 * the resulting work. Corresponding source of such a combination shall include
 * the source code for all other software used.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * See COPYING file for full licensing terms.
 *
 * ~~~~~~~~
 *
 * TODO big picture module description
 * - what maps to what, briefly
 *
 *   VMA with a buffer/memoryview interface
 *   BigFileH  with mmap (-> vma) and writeout control
 *   BigFile   base class (to allow implementing BigFile backends in python)
 */

#include <Python.h>
#include "structmember.h"
#include "frameobject.h"

#include <wendelin/bigfile/file.h>
#include <wendelin/bigfile/virtmem.h>
#include <wendelin/bigfile/ram.h>
#include <wendelin/bug.h>
#include <wendelin/compat_py2.h>

static PyObject *gcmodule;
static PyObject *pybuf_str;

/* whether to pass old buffer instead of memoryview to .loadblk() / .storeblk()
 *
 * on python2 < 2.7.10 memoreview object is not accepted in a lot of
 * places, see e.g. http://bugs.python.org/issue22113 for struct.pack_into()
 *
 * also on python 2.7.10, even latest numpy does not accept memoryview as
 * buffer for ndarray: https://github.com/numpy/numpy/issues/5935
 *
 * if we know memoryview won't be accepted - we pass old buffer
 *
 * TODO get rid of this and eventually use only memoryview  */
//#define BIGFILE_USE_OLD_BUFFER  (PY_VERSION_HEX < 0x02070AF0)
// waiting for numpy to start accept it on python2
#define BIGFILE_USE_OLD_BUFFER  (PY_VERSION_HEX < 0x03000000)


/*
 * python representation of VMA - exposes vma memory as python buffer
 */
struct PyVMA {
    PyObject;
    PyObject *in_weakreflist;

    VMA;
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
    PyObject;
    PyObject *in_weakreflist;

    BigFileH;
};
typedef struct PyBigFileH PyBigFileH;


/*
 * BigFile that can be implemented in python
 *
 * Allows subclasses to implement .loadblk() (& friends) in python.
 * For users .fileh_open() is exposed to get to file handles.   XXX <- should be not in this class?
 */
struct PyBigFile {
    PyObject;
    /* NOTE no explicit weakref support is needed - this is a base class and python
     * automatically adds support for weakrefs for in-python defined children   */

    BigFile;
};
typedef struct PyBigFile PyBigFile;


/* NOTE on refcounting: who holds who
 *
 *
 *  vma  ->  fileh  ->  file
 *   ^         |
 *   +---------+
 *   fileh->mmaps (kind of weak)
 */


/* like PyObject_New, but fully initializes instance (e.g. calls type ->tp_new) */
#define PyType_New(type, typeobj, args) \
    ( (type *)PyObject_CallObject((PyObject *)(typeobj), args) )

/* like PyErr_SetFromErrno(exc), but chooses exception type automatically */
static void XPyErr_SetFromErrno(void);

/* like PyErr_Clear but clears not only ->curexc_* but also ->exc_* and
 * everything else related to exception state */
static void XPyErr_FullClear(void);

/* get list of objects that refer to obj */
static PyObject* /* PyListObject* */ XPyObject_GetReferrers(PyObject *obj);

/* print objects that refer to obj */
static void XPyObject_PrintReferrers(PyObject *obj, FILE *fp);

/* check whether frame f is a callee of top */
static int XPyFrame_IsCalleeOf(PyFrameObject *f, PyFrameObject *top);

/* buffer utilities: unpin buffer from its memory - make it zero-length
 * pointing to NULL but staying a vailid python object */
#if PY_MAJOR_VERSION < 3
void XPyBufferObject_Unpin(PyBufferObject *bufo);
#endif
void XPyBuffer_Unpin(Py_buffer *view);


/************
 *  PyVMA   *
 ************/

#if PY_MAJOR_VERSION < 3
static Py_ssize_t
pyvma_getbuf(PyObject *pyvma0, Py_ssize_t segment, void **pptr)
{
    PyVMA *pyvma = upcast(PyVMA *, pyvma0);

    if (segment) {
        PyErr_SetString(PyExc_SystemError, "access to non-zero vma segment");
        return -1;
    }

    *pptr = (void *)pyvma->addr_start;
    return pyvma->addr_stop - pyvma->addr_start;
}


static Py_ssize_t
pyvma_getsegcount(PyObject *pyvma0, Py_ssize_t *lenp)
{
    PyVMA *pyvma = upcast(PyVMA *, pyvma0);

    if (lenp)
        *lenp = pyvma->addr_stop - pyvma->addr_start;
    return 1;
}
#endif


static int
pyvma_getbuffer(PyObject *pyvma0, Py_buffer *view, int flags)
{
    PyVMA *pyvma = upcast(PyVMA *, pyvma0);

    return PyBuffer_FillInfo(view, pyvma,
            (void *)pyvma->addr_start, pyvma->addr_stop - pyvma->addr_start,
            /*readonly=*/0, flags);
}


/* vma len in bytes */
static Py_ssize_t
pyvma_len(PyObject *pyvma0)
{
    PyVMA *pyvma = upcast(PyVMA *, pyvma0);

    return pyvma->addr_stop - pyvma->addr_start;
}


static void
pyvma_dealloc(PyObject *pyvma0)
{
    PyVMA       *pyvma = upcast(PyVMA *, pyvma0);
    BigFileH    *fileh = pyvma->fileh;

    if (pyvma->in_weakreflist)
        PyObject_ClearWeakRefs(pyvma);

    /* pyvma->fileh indicates whether vma was yet created (via fileh_mmap()) or not */
    if (fileh) {
        vma_unmap(pyvma);

        PyBigFileH *pyfileh = upcast(PyBigFileH *, fileh);
        Py_DECREF(pyfileh);
    }

    pyvma->ob_type->tp_free(pyvma);
}


static PyObject *
pyvma_new(PyTypeObject *type, PyObject *args, PyObject *kw)
{
    PyVMA *self;

    self = (PyVMA *)PyType_GenericNew(type, args, kw);
    if (!self)
        return NULL;
    self->in_weakreflist = NULL;

    return self;
}


static /*const*/ PyBufferProcs pyvma_as_buffer = {
#if PY_MAJOR_VERSION < 3
    /* for buffer() */
    .bf_getreadbuffer   = pyvma_getbuf,
    .bf_getwritebuffer  = pyvma_getbuf,
    .bf_getsegcount     = pyvma_getsegcount,
#endif

    /* for memoryview()
     * NOTE but ndarray() ctor does not get memoryview as buffer */ // XXX recheck
    .bf_getbuffer       = pyvma_getbuffer,
};


static /*const*/ PySequenceMethods pyvma_as_seq = {
    .sq_length          = pyvma_len,
};


static PyTypeObject PyVMA_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name            = "_bigfile.VMA",
    .tp_basicsize       = sizeof(PyVMA),
    .tp_flags           = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_NEWBUFFER,
    .tp_methods         = NULL, // TODO ?
    .tp_as_sequence     = &pyvma_as_seq,
    .tp_as_buffer       = &pyvma_as_buffer,
    .tp_dealloc         = pyvma_dealloc,
    .tp_new             = pyvma_new,
    .tp_weaklistoffset  = offsetof(PyVMA, in_weakreflist),
    .tp_doc             = "memory area representing one fileh mapping"
};



/****************
 *  PyBigFileH  *
 ****************/


#define PyFunc(FUNC, DOC)               \
static const char FUNC ##_doc[] = DOC;  \
static PyObject *FUNC

PyFunc(pyfileh_mmap, "mmap(pgoffset, pglen) - map fileh part into memory")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = upcast(PyBigFileH *, pyfileh0);
    Py_ssize_t  pgoffset, pglen;    // XXX Py_ssize_t vs pgoff_t ?
    PyVMA       *pyvma;
    int         err;

    if (!PyArg_ParseTuple(args, "nn", &pgoffset, &pglen))
        return NULL;

    pyvma = PyType_New(PyVMA, &PyVMA_Type, NULL);
    if (!pyvma)
        return NULL;

    Py_INCREF(pyfileh);
    err = fileh_mmap(pyvma, pyfileh, pgoffset, pglen);
    if (err) {
        Py_DECREF(pyfileh);
        Py_DECREF(pyvma);
        XPyErr_SetFromErrno();
        return NULL;
    }

    return pyvma;
}


PyFunc(pyfileh_dirty_writeout,
        "dirty_writeout(flags) - write changes made to fileh memory back to file")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = upcast(PyBigFileH *, pyfileh0);
    long flags;
    int err;

    if (!PyArg_ParseTuple(args, "l", &flags))
        return NULL;

    err = fileh_dirty_writeout(pyfileh, flags);
    if (err) {
        if (!PyErr_Occurred())
            // XXX not very informative
            PyErr_SetString(PyExc_RuntimeError, "fileh_dirty_writeout fail");
        return NULL;
    }

    Py_RETURN_NONE;
}


PyFunc(pyfileh_dirty_discard, "dirty_discard() - discard changes made to fileh memory")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = upcast(PyBigFileH *, pyfileh0);

    if (!PyArg_ParseTuple(args, ""))
        return NULL;

    fileh_dirty_discard(pyfileh);
    Py_RETURN_NONE;
}


PyFunc(pyfileh_isdirty, "isdirty() - are there any changes to fileh memory at all?")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = upcast(PyBigFileH *, pyfileh0);

    if (!PyArg_ParseTuple(args, ""))
        return NULL;

    /* NOTE not strictly neccessary to virt_lock() for reading ->dirty */
    return PyBool_FromLong(pyfileh->dirty);
}


PyFunc(pyfileh_invalidate_page, "invalidate_page(pgoffset) - invalidate fileh page")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = upcast(PyBigFileH *, pyfileh0);
    Py_ssize_t  pgoffset;   // XXX Py_ssize_t vs pgoff_t ?

    if (!PyArg_ParseTuple(args, "n", &pgoffset))
        return NULL;

    fileh_invalidate_page(pyfileh, pgoffset);

    Py_RETURN_NONE;
}


static void
pyfileh_dealloc(PyObject *pyfileh0)
{
    PyBigFileH  *pyfileh = upcast(PyBigFileH *, pyfileh0);
    BigFile     *file = pyfileh->file;
    PyBigFile   *pyfile;

    if (pyfileh->in_weakreflist)
        PyObject_ClearWeakRefs(pyfileh);

    /* pyfileh->file indicates whether fileh was yet opened (via fileh_open()) or not */
    if (file) {
        fileh_close(pyfileh);

        pyfile = upcast(PyBigFile  *, file);
        Py_DECREF(pyfile);
    }

    pyfileh->ob_type->tp_free(pyfileh);
}


static PyObject *
pyfileh_new(PyTypeObject *type, PyObject *args, PyObject *kw)
{
    PyBigFileH *self;

    self = (PyBigFileH *)PyType_GenericNew(type, args, kw);
    if (!self)
        return NULL;
    self->in_weakreflist = NULL;

    return self;
}


static /*const*/ PyMethodDef pyfileh_methods[] = {
    {"mmap",            pyfileh_mmap,           METH_VARARGS,   pyfileh_mmap_doc},
    {"dirty_writeout",  pyfileh_dirty_writeout, METH_VARARGS,   pyfileh_dirty_writeout_doc},
    {"dirty_discard",   pyfileh_dirty_discard,  METH_VARARGS,   pyfileh_dirty_discard_doc},
    {"isdirty",         pyfileh_isdirty,        METH_VARARGS,   pyfileh_isdirty_doc},
    {"invalidate_page", pyfileh_invalidate_page,METH_VARARGS,   pyfileh_invalidate_page_doc},
    {NULL}
};


static /*const*/ PyTypeObject PyBigFileH_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name            = "_bigfile.BigFileH",
    .tp_basicsize       = sizeof(PyBigFileH),
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_methods         = pyfileh_methods,
    .tp_dealloc         = pyfileh_dealloc,
    .tp_new             = pyfileh_new,
    .tp_weaklistoffset  = offsetof(PyBigFileH, in_weakreflist),
    .tp_doc             = "BigFile Handle"
};




/****************
 *  PyBigFile   *
 ****************/


/* like PySys_SetObject but
 *  1) aborts on failure and
 *  2) return set object
 */
static PyObject *XPySys_SetObject(char *name, PyObject *v)
{
    int err;
    err = PySys_SetObject(name, v);
    BUG_ON(err);
    return v;
}


static int pybigfile_loadblk(BigFile *file, blk_t blk, void *buf)
{
    PyBigFile *pyfile = upcast(PyBigFile *, file);
    PyObject  *pybuf = NULL;
    PyObject  *loadret = NULL;

    PyGILState_STATE gstate;
    PyThreadState *ts;
    PyFrameObject *ts_frame_orig;
    PyObject  *save_curexc_type, *save_curexc_value, *save_curexc_traceback;
    PyObject  *save_exc_type,    *save_exc_value,    *save_exc_traceback;
    PyObject  *save_async_exc;
    PyObject  *sys_exc_type,     *sys_exc_value,     *sys_exc_traceback;
    // XXX save/restore trash_delete_{nesting,later} ?


    /* ensure we hold the GIL
     * if not already taken - we'll acquire it; if already hold - no action.
     * as the result - _we_ are the thread which holds the GIL and can call
     * python capi. */
    // XXX assert PyGILState_GetThisThreadState() != NULL
    //      (i.e. pyton already knows this thread?)
    gstate = PyGILState_Ensure();

    /* TODO write why we setup completly new thread state which looks like
     * switching threads for python but stays at the same OS thread
     *
     * a) do not change current thread state in any way;
     * b) to completly clear ts after loadblk (ex. for pybuf->refcnf to go to exactly 1)
     */

    /* in python thread state - save what we'll possibly override
     *
     * to faulting code, it looks like loadblk() is just called as callback.
     * Only we have to care to restore exception states for caller (we can't
     * propagate exceptions through pagefaulting, and if there were series of
     * try/except, we too should restore original caller exception states.
     *
     * TODO better text.
     */
    ts = PyThreadState_GET();
    ts_frame_orig = ts->frame;  // just for checking

/* set ptr to NULL and return it's previous value */
#define set0(pptr)  ({ typeof(*(pptr)) p = *(pptr); *(pptr)=NULL; p; })

/* xincref that evaluate its argument only once */
#define XINC(arg)   do { typeof(arg) p = (arg); Py_XINCREF(p); } while (0)

/* like set0 for PySys_{Get,Set}Object, but also xincref original obj */
#define PySys_XInc_SetNone(name)    ({                                  \
    PyObject *p = PySys_GetObject(name);                                \
    /* incref here because otherwise on next line it can go away */     \
    Py_XINCREF(p);                                                      \
    PySys_SetObject(name, Py_None);                                     \
    p;                                                                  \
})

    XINC( save_curexc_type        = set0(&ts->curexc_type)        );
    XINC( save_curexc_value       = set0(&ts->curexc_value)       );
    XINC( save_curexc_traceback   = set0(&ts->curexc_traceback)   );
    XINC( save_exc_type           = set0(&ts->exc_type)           );
    XINC( save_exc_value          = set0(&ts->exc_value)          );
    XINC( save_exc_traceback      = set0(&ts->exc_traceback)      );
    XINC( save_async_exc          = set0(&ts->async_exc)          );

    /* before py3k python also stores exception in sys.exc_* variables (wrt
     * sys.exc_info()) for "backward compatibility" */
    sys_exc_type        = PySys_XInc_SetNone("exc_type");
    sys_exc_value       = PySys_XInc_SetNone("exc_value");
    sys_exc_traceback   = PySys_XInc_SetNone("exc_traceback");

#if BIGFILE_USE_OLD_BUFFER
    pybuf = PyBuffer_FromReadWriteMemory(buf, file->blksize);
#else
    pybuf = PyMemoryView_FromMemory(buf, file->blksize, PyBUF_WRITE /* = read-write */);
#endif
    if (!pybuf)
        goto err;

    /* NOTE K = unsigned long long */
    BUILD_ASSERT(sizeof(blk) == sizeof(unsigned long long));
    loadret = PyObject_CallMethod(pyfile, "loadblk", "KO", blk, pybuf);

    /* python should return to original frame */
    BUG_ON(ts != PyThreadState_GET());
    BUG_ON(ts->frame != ts_frame_orig);

    if (!loadret)
        goto err;

out:
    /* clear thread state to cleanup, and in particular so it does not hold
     * reference to pybuf - after return from loadblk, buf memory will be
     * unmapped, so the caller must not have left kept references to it. */

    /* we need to know only whether loadret != NULL, decref it now */
    Py_XDECREF(loadret);


    /* first clear exception states so it drop all references (and possibly in
     * called frame) to pybuf */
    XPyErr_FullClear();

    /* verify pybuf is not held - its memory will go away right after return */
    if (pybuf) {
        /* pybuf might be held e.g. due to an exception raised & caught
         * somewhere in loadblk implementation - so loadblk returns ok, but if
         *
         *   _, _, exc_traceback = sys.exc_info()
         *
         * was also used inside the following reference loop is created:
         *
         *   exc_traceback
         *     |        ^
         *     |        |
         *     v     .f_localsplus
         *    frame
         *
         * and some of the frames continue to hold pybuf reference.
         *
         * Do full GC to collect such, and possibly other, cycles this way
         * removing references to pybuf.
         */
        if (pybuf->ob_refcnt != 1) {
            PyGC_Collect();

            /* garbage collection could result in running arbitraty code
             * because of finalizers. Print problems (if any) and make sure
             * once again exception state is clear */
            if (PyErr_Occurred())
                PyErr_PrintEx(0);
            XPyErr_FullClear();
        }

        /* the story continues here - a traceback might be also explicitly
         * saved by code somewhere this way not going away after GC. Let's
         * find objects that refer to pybuf, and for frames called by loadblk()
         * change pybuf to another stub object (we know there we can do it safely) */
        if (pybuf->ob_refcnt != 1) {
            PyObject *pybuf_users = XPyObject_GetReferrers(pybuf);
            int i, j;

            for (i = 0; i < PyList_GET_SIZE(pybuf_users); i++) {
                PyObject *user = PyList_GET_ITEM(pybuf_users, i);
                PyObject **fastlocals;
                PyFrameObject *f;

                /* if it was the frame used for our calling to py loadblk() we
                 * can replace pybuf to "<pybuf>" there in loadblk arguments */
                if (PyFrame_Check(user)) {
                    f = (PyFrameObject *)user;
                    if (!XPyFrame_IsCalleeOf(f, ts->frame))
                        continue;

                    /* "fast" locals (->f_localsplus) */
                    fastlocals = f->f_localsplus;
                    for (j = f->f_code->co_nlocals; j >= 0; --j) {
                        if (fastlocals[j] == pybuf) {
                            Py_INCREF(pybuf_str);
                            fastlocals[j] = pybuf_str;
                            Py_DECREF(pybuf);
                        }
                    }

                    /* ->f_locals */
                    if (f->f_locals != NULL) {
                        TODO(!PyDict_CheckExact(f->f_locals));

                        PyObject *key, *value;
                        Py_ssize_t pos = 0;

                        while (PyDict_Next(f->f_locals, &pos, &key, &value)) {
                            if (value == pybuf) {
                                int err;
                                err = PyDict_SetItem(f->f_locals, key, pybuf_str);
                                BUG_ON(err == -1);
                            }
                        }
                    }
                }
            }

            Py_DECREF(pybuf_users);
        }

        /* now it is real bug if pybuf remains referenced from somewhere */
        if (pybuf->ob_refcnt != 1) {
            WARN("pybuf->ob_refcnt != 1 even after GC:");
            fprintf(stderr, "pybuf (ob_refcnt=%ld):\t", (long)pybuf->ob_refcnt);
            PyObject_Print(pybuf, stderr, 0);
            fprintf(stderr, "\npybuf referrers:\t");
            XPyObject_PrintReferrers(pybuf, stderr);
            fprintf(stderr, "\n");
            BUG();
        }
    }

    /* drop pybuf
     *
     * NOTE in theory decref could run arbitrary code, but buffer_dealloc() is
     * simply PyObject_DEL which does not lead to running python code. */
    Py_XDECREF(pybuf);
    BUG_ON(ts->curexc_type  || ts->curexc_value || ts->curexc_traceback);
    BUG_ON(ts->exc_type     || ts->exc_value    || ts->exc_traceback);
    BUG_ON(ts->async_exc);
    BUG_ON(PySys_GetObject("exc_type")      != Py_None ||
           PySys_GetObject("exc_value")     != Py_None ||
           PySys_GetObject("exc_traceback") != Py_None);

    /* now restore exception state to original */
    ts->curexc_type         = save_curexc_type;
    ts->curexc_value        = save_curexc_value;
    ts->curexc_traceback    = save_curexc_traceback;
    ts->exc_type            = save_exc_type;
    ts->exc_value           = save_exc_value;
    ts->exc_traceback       = save_exc_traceback;
    ts->async_exc           = save_async_exc;

    XPySys_SetObject("exc_type",       sys_exc_type);
    XPySys_SetObject("exc_value",      sys_exc_value);
    XPySys_SetObject("exc_traceback",  sys_exc_traceback);

    Py_XDECREF( save_curexc_type        );
    Py_XDECREF( save_curexc_value       );
    Py_XDECREF( save_curexc_traceback   );
    Py_XDECREF( save_exc_type           );
    Py_XDECREF( save_exc_value          );
    Py_XDECREF( save_exc_traceback      );
    Py_XDECREF( save_async_exc          );

    Py_XDECREF( sys_exc_type            );
    Py_XDECREF( sys_exc_value           );
    Py_XDECREF( sys_exc_traceback       );

    /* optionally release the GIL, if it was not ours initially */
    PyGILState_Release(gstate);

    /* loadblk() job done */
    return loadret ? 0 : -1;

err:
    /* error happened - dump traceback and return */
    PyErr_PrintEx(0);
    goto out;
}

#undef XINC


static int pybigfile_storeblk(BigFile *file, blk_t blk, const void *buf)
{
    PyBigFile *pyfile = upcast(PyBigFile *, file);
    PyObject  *pybuf;
    PyObject  *storeret = NULL;

    PyGILState_STATE gstate;
    int err;

    // XXX so far storeblk() is called only from py/user context (vs SIGSEGV
    //     context) - no need to save/restore interpreter state.
    // TODO -> if needed - do similar to pybigfile_loadblk().

    /* ensure we hold the GIL (and thus are _the_ python thread) */
    gstate = PyGILState_Ensure();

    // XXX readonly, but wants (void *) without const
#if BIGFILE_USE_OLD_BUFFER
    pybuf = PyBuffer_FromMemory((void *)buf, file->blksize);
#else
    pybuf = PyMemoryView_FromMemory((void *)buf, file->blksize, PyBUF_READ);
#endif
    if (!pybuf)
        goto out;

    /* NOTE K = unsigned long long */
    BUILD_ASSERT(sizeof(blk) == sizeof(unsigned long long));
    storeret = PyObject_CallMethod(pyfile, "storeblk", "KO", blk, pybuf);

    /* we need to know only whether storeret != NULL, decref it now */
    Py_XDECREF(storeret);

    /* FIXME the following is not strictly correct e.g. for:
     *   mbuf = memoryview(buf)
     * because mbuf.ptr will be a copy of buf.ptr and clearing buf does not
     * clear mbuf.
     *
     * -> TODO rework to call gc.collect() if pybuf->ob_refcnt != 1
     * like loadblk codepath does. */

    /* repoint pybuf to empty region - the original memory attached to it can
     * go away right after we return (if e.g. dirty page was not mapped in any
     * vma), but we need pybuf to stay not corrupt - for printing full
     * traceback in case of storeblk() error. */
#if BIGFILE_USE_OLD_BUFFER
    PyBufferObject *pybufo = (PyBufferObject *)pybuf;
    XPyBufferObject_Unpin(pybufo);
#else
    PyMemoryViewObject *pybufm = (PyMemoryViewObject *)pybuf;
    XPyBuffer_Unpin(&pybufm->view);
#endif

    /* verify that we actually tweaked pybuf ok */
    Py_buffer pybuf_view;
    err = PyObject_GetBuffer(pybuf, &pybuf_view, PyBUF_SIMPLE);
    BUG_ON(err);
    BUG_ON(pybuf_view.buf   != NULL);
    BUG_ON(pybuf_view.len   != 0);
    PyBuffer_Release(&pybuf_view);

    /* done with pybuf */
    Py_DECREF(pybuf);

out:
    PyGILState_Release(gstate);

    /* storeblk() job done */
    return storeret ? 0 : -1;
}


static const struct bigfile_ops pybigfile_ops = {
    .loadblk    = pybigfile_loadblk,
    .storeblk   = pybigfile_storeblk,
    //.release    =
};



static PyObject *
pyfileh_open(PyObject *pyfile0, PyObject *args)
{
    PyBigFile   *pyfile = upcast(PyBigFile *, pyfile0);
    PyBigFileH  *pyfileh;
    /* NOTE no virtmem lock needed - default RAM does not change */
    RAM *ram = ram_get_default(NULL);   // TODO get ram from args
    int err;


    if (!PyArg_ParseTuple(args, ""))
        return NULL;

    pyfileh = PyType_New(PyBigFileH, &PyBigFileH_Type, NULL);
    if (!pyfileh)
        return NULL;

    Py_INCREF(pyfile);
    err = fileh_open(pyfileh, pyfile, ram);
    if (err) {
        XPyErr_SetFromErrno();
        Py_DECREF(pyfile);
        Py_DECREF(pyfileh);
        return NULL;
    }

    return pyfileh;
}


static PyObject *
pyfile_new(PyTypeObject *type, PyObject *args, PyObject *kw)
{
    PyBigFile *self;

    self = (PyBigFile *)PyType_GenericNew(type, args, kw);
    if (!self)
        return NULL;

    // FIXME "k" = unsigned long - we need size_t
    static char *kw_list[] = {"blksize", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "k", kw_list, &self->blksize)) {
        Py_DECREF(self);
        return NULL;
    }

    self->file_ops = &pybigfile_ops;
    return self;
}


static PyMemberDef pyfile_members[] = {
    {"blksize", T_ULONG /* XXX vs typeof(blksize) = size_t ? */, offsetof(PyBigFile, blksize), READONLY, "block size"},
    {NULL}
};

static /*const*/ PyMethodDef pyfile_methods[] = {
    // XXX should be separate BigFileH ctor or fileh_open function?
    {"fileh_open",  pyfileh_open,   METH_VARARGS,   "fileh_open(ram=None) -> new file handle"},
    {NULL}
};

static PyTypeObject PyBigFile_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name            = "_bigfile.BigFile",
    .tp_basicsize       = sizeof(PyBigFile),
    .tp_flags           = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_methods         = pyfile_methods,
    .tp_members         = pyfile_members,
    .tp_dealloc         = NULL, // XXX
    .tp_new             = pyfile_new,
    .tp_doc             = "Base class for creating BigFile(s)\n\nTODO describe",    // XXX
};



PyFunc(pyram_reclaim, "ram_reclaim() -> reclaimed -- release some non-dirty ram back to OS")
    (PyObject *self, PyObject *args)
{
    /* NOTE no virtmem lock needed - default RAM does not change */
    RAM *ram = ram_get_default(NULL);   // TODO get ram from args
    int reclaimed;

    if (!PyArg_ParseTuple(args, ""))
        return NULL;

    reclaimed = ram_reclaim(ram);
    return PyLong_FromLong(reclaimed);
}


static /*const*/ PyMethodDef pybigfile_modulemeths[] = {
    {"ram_reclaim", pyram_reclaim,  METH_VARARGS,   pyram_reclaim_doc},
    {NULL}
};



/* GIL hooks for virtmem big lock */
static void *py_gil_ensure_unlocked(void)
{
    /* make sure we don't hold python GIL (not to deadlock, as GIL oscillates)
     *
     * NOTE it is ok to get _PyThreadState_Current even without holding py gil -
     *      we only need to check whether ts_current != ts_my, and thus if this
     *      thread don't hold the gil, _PyThreadState_Current will be != ts_my
     *      for sure.
     *
     * NOTE2 we don't call PyThreadState_Get() as that thinks it is a bug when
     *       _PyThreadState_Current == NULL */
    PyThreadState *ts_my        = PyGILState_GetThisThreadState();
    PyThreadState *ts_current   = _PyThreadState_UncheckedGet();
    PyThreadState *ts;

    if (ts_my && (ts_my == ts_current)) {
        ts = PyEval_SaveThread();
        BUG_ON(ts != ts_my);

        return ts_my;
    }

    return NULL;
}

static void py_gil_retake_if_waslocked(void *arg)
{
    PyThreadState *ts_my = (PyThreadState *)arg;

    /* retake GIL if we were holding it originally */
    PyEval_RestoreThread(ts_my);
}


static const VirtGilHooks py_virt_gil_hooks = {
    .gil_ensure_unlocked        = py_gil_ensure_unlocked,
    .gil_retake_if_waslocked    = py_gil_retake_if_waslocked,
};


/* module init */
#if PY_MAJOR_VERSION >= 3
static /*const*/ PyModuleDef pybigfile_moduledef = {
    PyModuleDef_HEAD_INIT,
    .m_name     = "_bigfile",
    .m_size     = -1,   /* = disable creating several instances of this module */
    .m_methods  = pybigfile_modulemeths,
};
#endif

static PyObject *
_init_bigfile(void)
{
    PyObject *m;
    int err;

    /* verify we copied struct PyBufferObject definition ok */
#if PY_MAJOR_VERSION < 3
    BUG_ON(sizeof(PyBufferObject) != PyBuffer_Type.tp_basicsize);
#endif

    /* setup virtmem gil hooks for python */
    virt_lock_hookgil(&py_virt_gil_hooks);

    /* setup pagefault handler right from the beginning - memory lazy-access
     * fundamentally depends on it */
    err = pagefault_init();
    if (err)
        Py_FatalError("bigfile: can't initialise pagefaulting");

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&pybigfile_moduledef);
#else
    m = Py_InitModule("_bigfile", pybigfile_modulemeths);
#endif
    if (!m)
        return NULL;

    /* NOTE we don't expose VMA & BigFileH to users via module namespace */
    if (PyType_Ready(&PyVMA_Type) < 0)
        return NULL;
    if (PyType_Ready(&PyBigFileH_Type) < 0)
        return NULL;

    if (PyType_Ready(&PyBigFile_Type) < 0)
        return NULL;
    Py_INCREF(&PyBigFile_Type);
    if (PyModule_AddObject(m, "BigFile", (PyObject *)&PyBigFile_Type))
        return NULL;

#define CSTi(name)  do {                            \
    if (PyModule_AddIntConstant(m, #name, name))    \
        return NULL;                                \
} while (0)

    CSTi(WRITEOUT_STORE);
    CSTi(WRITEOUT_MARKSTORED);

    /* import gc */
    gcmodule = PyImport_ImportModule("gc");
    if (!gcmodule)
        return NULL;

    pybuf_str = PyUnicode_FromString("<pybuf>");
    if (!pybuf_str)
        return NULL;

    return m;
}


#if PY_MAJOR_VERSION < 3
# define PyInit__bigfile init_bigfile
#endif
__attribute__((visibility("default")))  // XXX should be in PyMODINIT_FUNC
PyMODINIT_FUNC
PyInit__bigfile(void)
{
#if PY_MAJOR_VERSION >= 3
    return
#endif
        _init_bigfile();
}


static void
XPyErr_SetFromErrno(void)
{
    PyObject *exc;

    switch(errno) {
        case ENOMEM:    exc = PyExc_MemoryError; break;
        default:        exc = PyExc_RuntimeError;
    }

    PyErr_SetFromErrno(exc);
}


static void
XPyErr_FullClear(void)
{
    PyObject  *x_curexc_type,    *x_curexc_value,    *x_curexc_traceback;
    PyObject  *x_exc_type,       *x_exc_value,       *x_exc_traceback;
    PyObject  *x_async_exc;
    PyThreadState *ts;

    ts = PyThreadState_GET();

    x_curexc_type       = set0(&ts->curexc_type);
    x_curexc_value      = set0(&ts->curexc_value);
    x_curexc_traceback  = set0(&ts->curexc_traceback);
    x_exc_type          = set0(&ts->exc_type);
    x_exc_value         = set0(&ts->exc_value);
    x_exc_traceback     = set0(&ts->exc_traceback);
    x_async_exc         = set0(&ts->async_exc);

    Py_XDECREF(x_curexc_type);
    Py_XDECREF(x_curexc_value);
    Py_XDECREF(x_curexc_traceback);
    Py_XDECREF(x_exc_type);
    Py_XDECREF(x_exc_value);
    Py_XDECREF(x_exc_traceback);
    Py_XDECREF(x_async_exc);

    PySys_SetObject("exc_type",       Py_None);
    PySys_SetObject("exc_value",      Py_None);
    PySys_SetObject("exc_traceback",  Py_None);
}

static PyObject* /* PyListObject* */
XPyObject_GetReferrers(PyObject *obj)
{
    PyObject *obj_referrers = PyObject_CallMethod(gcmodule, "get_referrers", "O", obj);
    BUG_ON(!obj_referrers);
    BUG_ON(!PyList_CheckExact(obj_referrers));
    return /*(PyListObject *)*/obj_referrers;
}

static void
XPyObject_PrintReferrers(PyObject *obj, FILE *fp)
{
    PyObject *obj_referrers = XPyObject_GetReferrers(obj);
    PyObject_Print(obj_referrers, fp, 0);
    Py_DECREF(obj_referrers);
}

static int
XPyFrame_IsCalleeOf(PyFrameObject *f, PyFrameObject *top)
{
    for (; f; f = f->f_back)
        if (f == top)
            return 1;

    return 0;
}

#if PY_MAJOR_VERSION < 3
void
XPyBufferObject_Unpin(PyBufferObject *bufo)
{
    bufo->b_ptr    = NULL;
    bufo->b_size   = 0;
    bufo->b_offset = 0;
    bufo->b_hash   = -1;
    Py_CLEAR(bufo->b_base);
}
#endif

void
XPyBuffer_Unpin(Py_buffer *view)
{
    view->buf = NULL;
    view->len = 0;
    Py_CLEAR(view->obj);
}
