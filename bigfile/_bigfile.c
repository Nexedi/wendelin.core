/* Wendelin.bigfile | Python interface to memory/files
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

/* Package _bigfile provides Python bindings to virtmem.
 * See _bigfile.h for package overview. */

/* _bigfile organization
 *
 * NOTE on refcounting - who holds who:
 *
 *  vma  ->  fileh  ->  file(*)
 *   ^         |
 *   +---------+
 *   fileh->mmaps (kind of weak)
 *
 * (*) PyBigFile is intended to be used as subclass, whose children can add
 *     references from file to other objects. In particular ZBigFile keeps
 *     references to fileh that were created through it.
 *
 * NOTE virtmem/bigfile functions release/reacquire GIL (see virt_lock()) -
 * thus functions that use them cannot assume they run mutually exclusive to
 * other Python threads. See Py_CLEAR documentation which explain what could go
 * wrong if code is not careful to protect itself against concurrent GC:
 *
 * https://github.com/python/cpython/blob/v2.7.15-310-g112e4afd582/Include/object.h#L790-L798
 */

#include "bigfile/_bigfile.h"
#include "structmember.h"
#include "frameobject.h"

#if PY_VERSION_HEX >= 0x030B0000    // 3.11
# ifndef Py_BUILD_CORE
#  define Py_BUILD_CORE 1
# endif
# include "internal/pycore_frame.h"
#endif

#include <wendelin/bigfile/ram.h>
#include <wendelin/bug.h>
#include <wendelin/compat_py.h>
#include <ccan/container_of/container_of.h>

static PyObject *gcmodule;
static PyObject *pybuf_str;

/* whether to pass old buffer instead of memoryview to .loadblk() / .storeblk()
 *
 * on python2 < 2.7.10 memoryview object is not accepted in a lot of
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

/* whether to use PyThreadState->exc_state,exc_info instead of
 * PyThreadState->exc_type & friends.
 *
 * Starting from Python 3.7 the place to keep exception state was changed:
 * https://github.com/python/cpython/commit/ae3087c638  */
#define BIGFILE_USE_PYTS_EXC_INFO   (PY_VERSION_HEX >= 0x030700A3)

/* Starting from Python 3.11 exception state is kept only in exc_value without
 * exc_type and exc_traceback. https://github.com/python/cpython/commit/396b58345f81 */
#define BIGFILE_PYTS_EXC_INFO_ONLY_EXC_VALUE (PY_VERSION_HEX >= 0x030B0000)


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
void XPyObject_PrintReferrers(PyObject *obj, FILE *fp);

/* check whether frame f is a callee of top */
static int XPyFrame_IsCalleeOf(PyFrameObject *f, PyFrameObject *top);

/* buffer utilities: unpin buffer from its memory - make it zero-length
 * pointing to NULL but staying a valid python object */
#if PY_MAJOR_VERSION < 3
void XPyBufferObject_Unpin(PyBufferObject *bufo);
#endif
void XPyBuffer_Unpin(Py_buffer *view);


#define PyFunc(FUNC, DOC)               \
static const char FUNC ##_doc[] = DOC;  \
static PyObject *FUNC


/************
 *  PyVMA   *
 ************/

#if PY_MAJOR_VERSION < 3
static Py_ssize_t
pyvma_getbuf(PyObject *pyvma0, Py_ssize_t segment, void **pptr)
{
    PyVMA *pyvma = container_of(pyvma0, PyVMA, pyobj);
    VMA   *vma   = &pyvma->vma;

    if (segment) {
        PyErr_SetString(PyExc_SystemError, "access to non-zero vma segment");
        return -1;
    }

    *pptr = (void *)vma->addr_start;
    return vma->addr_stop - vma->addr_start;
}


static Py_ssize_t
pyvma_getsegcount(PyObject *pyvma0, Py_ssize_t *lenp)
{
    PyVMA *pyvma = container_of(pyvma0, PyVMA, pyobj);
    VMA   *vma   = &pyvma->vma;

    if (lenp)
        *lenp = vma->addr_stop - vma->addr_start;
    return 1;
}
#endif


static int
pyvma_getbuffer(PyObject *pyvma0, Py_buffer *view, int flags)
{
    PyVMA *pyvma = container_of(pyvma0, PyVMA, pyobj);
    VMA   *vma   = &pyvma->vma;

    return PyBuffer_FillInfo(view, &pyvma->pyobj,
            (void *)vma->addr_start, vma->addr_stop - vma->addr_start,
            /*readonly=*/0, flags);
}


/* vma len in bytes */
static Py_ssize_t
pyvma_len(PyObject *pyvma0)
{
    PyVMA *pyvma = container_of(pyvma0, PyVMA, pyobj);
    VMA   *vma   = &pyvma->vma;

    return vma->addr_stop - vma->addr_start;
}


PyFunc(pyvma_filerange, "filerange() -> (pgoffset, pglen) -- file range this vma covers")
    (PyObject *pyvma0, PyObject *args)
{
    PyVMA *pyvma = container_of(pyvma0, PyVMA, pyobj);
    VMA   *vma   = &pyvma->vma;
    Py_ssize_t pgoffset, pglen;     // XXX Py_ssize_t vs pgoff_t

    pgoffset = vma->f_pgoffset;
    pglen    = (vma->addr_stop - vma->addr_start) / vma->fileh->ramh->ram->pagesize;
    /* NOTE ^^^ addr_stop and addr_start must be page-aligned */

    return Py_BuildValue("(nn)", pgoffset, pglen);
}


PyFunc(pyvma_pagesize, "pagesize() -> pagesize -- page size of RAM underlying this VMA")
    (PyObject *pyvma0, PyObject *args)
{
    PyVMA *pyvma = container_of(pyvma0, PyVMA, pyobj);
    VMA   *vma   = &pyvma->vma;
    Py_ssize_t pagesize = vma->fileh->ramh->ram->pagesize;

    return Py_BuildValue("n", pagesize);
}


/* pyvma vs cyclic GC */
static int
pyvma_traverse(PyObject *pyvma0, visitproc visit, void *arg)
{
    PyVMA *pyvma = container_of(pyvma0, PyVMA, pyobj);

    /* NOTE don't traverse vma->fileh (see pyvma_clear for details) */

    Py_VISIT(pyvma->pyuser);
    return 0;
}

static int
pyvma_clear(PyObject *pyvma0)
{
    PyVMA *pyvma = container_of(pyvma0, PyVMA, pyobj);

    /* NOTE don't clear vma->fileh: we need vma to be released first - else -
     * if there would be vma <=> fileh cycle, it could be possible for Python
     * to release fileh first, and then fileh_close called by fileh release
     * would break with BUG asserting that there are no fileh mappings left.
     * Protect py-level users from that. */

    Py_CLEAR(pyvma->pyuser);
    return 0;
}


static void
pyvma_dealloc(PyObject *pyvma0)
{
    /* PyVMA supports cyclic GC - first, before destructing, remove it from GC
     * list to avoid segfaulting on double entry here - e.g. if GC is triggered
     * from a weakref callback, or concurrently from another thread.
     *
     * See https://bugs.python.org/issue31095 for details */
    PyObject_GC_UnTrack(pyvma0);

    PyVMA       *pyvma = container_of(pyvma0, PyVMA, pyobj);
    VMA         *vma   = &pyvma->vma;
    BigFileH    *fileh = vma->fileh;

    if (pyvma->in_weakreflist)
        PyObject_ClearWeakRefs(&pyvma->pyobj);

    /* pyvma->fileh indicates whether vma was yet created (via fileh_mmap()) or not */
    if (fileh) {
        vma_unmap(vma);

        PyBigFileH *pyfileh = container_of(fileh, PyBigFileH, fileh);
        Py_DECREF(pyfileh);
    }

    pyvma_clear(&pyvma->pyobj);
    pyvma->pyobj.ob_type->tp_free(&pyvma->pyobj);
}


static PyObject *
pyvma_new(PyTypeObject *type, PyObject *args, PyObject *kw)
{
    PyVMA *self;

    self = (PyVMA *)PyType_GenericNew(type, args, kw);
    if (!self)
        return NULL;
    self->in_weakreflist = NULL;

    return &self->pyobj;
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


static /*const*/ PyMethodDef pyvma_methods[] = {
    {"filerange",   pyvma_filerange,    METH_VARARGS,   pyvma_filerange_doc},
    {"pagesize",    pyvma_pagesize,     METH_VARARGS,   pyvma_pagesize_doc},
    {NULL}
};

// XXX vvv better switch on various possibilities and find appropriate type
// (e.g. on X32 uintptr_t will be 4 while long will be 8)
const int _ =
    BUILD_ASSERT_OR_ZERO(sizeof(uintptr_t) == sizeof(unsigned long));
#define T_UINTPTR   T_ULONG

static /*const*/ PyMemberDef pyvma_members[] = {
    {"addr_start",  T_UINTPTR,      offsetof(PyVMA, vma.addr_start),    READONLY, "vma's start addr"},
    {"addr_stop",   T_UINTPTR,      offsetof(PyVMA, vma.addr_stop),     READONLY, "vma's start addr"},
    // XXX pyuser: restrict to read-only access?
    {"pyuser",      T_OBJECT_EX,    offsetof(PyVMA, pyuser),            0,        "user of this vma"},
    {NULL}
};

static PyTypeObject PyVMA_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name            = "_bigfile.VMA",
    .tp_basicsize       = sizeof(PyVMA),
    .tp_flags           = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_NEWBUFFER | Py_TPFLAGS_HAVE_GC,
    .tp_traverse        = pyvma_traverse,
    .tp_clear           = pyvma_clear,
    .tp_methods         = pyvma_methods,
    .tp_members         = pyvma_members,
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


PyFunc(pyfileh_mmap, "mmap(pgoffset, pglen) - map fileh part into memory")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);
    Py_ssize_t  pgoffset, pglen;    // XXX Py_ssize_t vs pgoff_t ?
    PyVMA       *pyvma;
    int         err;

    if (!PyArg_ParseTuple(args, "nn", &pgoffset, &pglen))
        return NULL;

    pyvma = PyType_New(PyVMA, &PyVMA_Type, NULL);
    if (!pyvma)
        return NULL;

    Py_INCREF(pyfileh);
    err = fileh_mmap(&pyvma->vma, &pyfileh->fileh, pgoffset, pglen);
    if (err) {
        Py_DECREF(pyfileh);
        Py_DECREF(pyvma);
        XPyErr_SetFromErrno();
        return NULL;
    }

    return &pyvma->pyobj;
}


PyFunc(pyfileh_dirty_writeout,
        "dirty_writeout(flags) - write changes made to fileh memory back to file")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);
    BigFileH    *fileh   = &pyfileh->fileh;
    long flags;
    int err;

    if (!PyArg_ParseTuple(args, "l", &flags))
        return NULL;

    err = fileh_dirty_writeout(fileh, flags);
    if (err) {
        if (!PyErr_Occurred())
            // XXX not very informative
            PyErr_SetString(PyExc_RuntimeError, "fileh_dirty_writeout fail");
        return NULL;
    }

    Py_RETURN_NONE;
}


PyFunc(pyfileh_dirty_discard, "dirty_discard([discard_from_page]) - discard changes made to fileh memory")
(PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);
    BigFileH    *fileh   = &pyfileh->fileh;
    pgoff_t discard_from_page = 0;

    if (!PyArg_ParseTuple(args, "|K", &discard_from_page))
        return NULL;

    fileh_dirty_discard(fileh, discard_from_page);
    Py_RETURN_NONE;
}


PyFunc(pyfileh_isdirty, "isdirty() - are there any changes to fileh memory at all?")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);
    BigFileH    *fileh   = &pyfileh->fileh;

    if (!PyArg_ParseTuple(args, ""))
        return NULL;

    /* NOTE not strictly necessary to virt_lock() for checking ->dirty_pages not empty */
    return PyBool_FromLong(!list_empty(&fileh->dirty_pages));
}


PyFunc(pyfileh_invalidate_page, "invalidate_page(pgoffset) - invalidate fileh page")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);
    BigFileH    *fileh   = &pyfileh->fileh;
    Py_ssize_t  pgoffset;   // XXX Py_ssize_t vs pgoff_t ?

    if (!PyArg_ParseTuple(args, "n", &pgoffset))
        return NULL;

    fileh_invalidate_page(fileh, pgoffset);

    Py_RETURN_NONE;
}


PyFunc(pyfileh_uses_mmap_overlay, "uses_mmap_overlay() - whether base data for all VMAs"
                                  " of this fileh are taken as base-layer mmap")
    (PyObject *pyfileh0, PyObject *args)
{
    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);
    BigFileH    *fileh   = &pyfileh->fileh;

    if (!PyArg_ParseTuple(args, ""))
        return NULL;

    return PyBool_FromLong(fileh->mmap_overlay);
}


/* pyfileh vs cyclic GC */
static int
pyfileh_traverse(PyObject *pyfileh0, visitproc visit, void *arg)
{
    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);
    BigFileH    *fileh   = &pyfileh->fileh;
    BigFile     *file    = fileh->file;
    PyBigFile   *pyfile;

    if (file) {
        pyfile = container_of(file, PyBigFile, file);
        Py_VISIT(pyfile);
    }

    return 0;
}

static int
pyfileh_clear(PyObject *pyfileh0)
{
    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);
    BigFileH    *fileh   = &pyfileh->fileh;
    BigFile     *file    = fileh->file;
    PyBigFile   *pyfile;

    /* pyfileh->file indicates whether fileh was yet opened (via fileh_open()) or not */
    if (file) {
        pyfile = container_of(file, PyBigFile, file);

        /* NOTE calling fileh_close in tp_clear - it is a bit hacky but ok.
         * we have to call fileh_close now becuase we'll reset fileh->file=NULL next.
         * pyfileh_dealloc also calls pyfileh_clear. */
        fileh_close(fileh);

        Py_DECREF(pyfile);
    }

    return 0;
}


static void
pyfileh_dealloc(PyObject *pyfileh0)
{
    /* PyBigFileH supports cyclic GC - first, before destructing, remove it from GC
     * list to avoid segfaulting on double entry here - e.g. if GC is triggered
     * from a weakref callback, or concurrently from another thread.
     *
     * See https://bugs.python.org/issue31095 for details */
    PyObject_GC_UnTrack(pyfileh0);

    PyBigFileH  *pyfileh = container_of(pyfileh0, PyBigFileH, pyobj);

    if (pyfileh->in_weakreflist)
        PyObject_ClearWeakRefs(&pyfileh->pyobj);

    pyfileh_clear(&pyfileh->pyobj);
    pyfileh->pyobj.ob_type->tp_free(&pyfileh->pyobj);
}


static PyObject *
pyfileh_new(PyTypeObject *type, PyObject *args, PyObject *kw)
{
    PyBigFileH *self;

    self = (PyBigFileH *)PyType_GenericNew(type, args, kw);
    if (!self)
        return NULL;
    self->in_weakreflist = NULL;

    return &self->pyobj;
}


static /*const*/ PyMethodDef pyfileh_methods[] = {
    {"mmap",              pyfileh_mmap,              METH_VARARGS,  pyfileh_mmap_doc},
    {"dirty_writeout",    pyfileh_dirty_writeout,    METH_VARARGS,  pyfileh_dirty_writeout_doc},
    {"dirty_discard",     pyfileh_dirty_discard,     METH_VARARGS,  pyfileh_dirty_discard_doc},
    {"isdirty",           pyfileh_isdirty,           METH_VARARGS,  pyfileh_isdirty_doc},
    {"invalidate_page",   pyfileh_invalidate_page,   METH_VARARGS,  pyfileh_invalidate_page_doc},
    {"uses_mmap_overlay", pyfileh_uses_mmap_overlay, METH_VARARGS,  pyfileh_uses_mmap_overlay_doc},
    {NULL}
};


static /*const*/ PyTypeObject PyBigFileH_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name            = "_bigfile.BigFileH",
    .tp_basicsize       = sizeof(PyBigFileH),
    .tp_flags           = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse        = pyfileh_traverse,
    .tp_clear           = pyfileh_clear,
    .tp_methods         = pyfileh_methods,
    .tp_dealloc         = pyfileh_dealloc,
    .tp_new             = pyfileh_new,
    .tp_weaklistoffset  = offsetof(PyBigFileH, in_weakreflist),
    .tp_doc             = "BigFile Handle"
};




/****************
 *  PyBigFile   *
 ****************/


static int pybigfile_loadblk(BigFile *file, blk_t blk, void *buf)
{
    PyBigFile *pyfile = container_of(file, PyBigFile, file);
    PyObject  *pybuf = NULL;
    PyObject  *loadret = NULL;

    PyGILState_STATE gstate;
    PyThreadState *ts;
    PyFrameObject *ts_frame_orig = NULL, *ts_frame = NULL;
    PyObject  *exc_value,         *exc_type = NULL,      *exc_traceback = NULL;
    PyObject  *save_curexc_value, *save_curexc_type,     *save_curexc_traceback;
    PyObject  *save_exc_value,    *save_exc_type = NULL, *save_exc_traceback = NULL;
#if BIGFILE_USE_PYTS_EXC_INFO
    _PyErr_StackItem *save_exc_info;
#endif
    PyObject  *save_async_exc;
    // XXX save/restore trash_delete_{nesting,later} ?


    /* ensure we hold the GIL
     * if not already taken - we'll acquire it; if already hold - no action.
     * as the result - _we_ are the thread which holds the GIL and can call
     * python capi. */
    // XXX assert PyGILState_GetThisThreadState() != NULL
    //      (i.e. python already knows this thread?)
    gstate = PyGILState_Ensure();

    /* TODO write why we setup completely new thread state which looks like
     * switching threads for python but stays at the same OS thread
     *
     * a) do not change current thread state in any way;
     * b) to completely clear ts after loadblk (ex. for pybuf->refcnf to go to exactly 1)
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
    ts_frame_orig = PyThreadState_GetFrame(ts);  // just for checking

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

#if BIGFILE_USE_PYTS_EXC_INFO
    XINC( save_exc_value          = set0(&ts->exc_state.exc_value)      );
# if !BIGFILE_PYTS_EXC_INFO_ONLY_EXC_VALUE
    XINC( save_exc_type           = set0(&ts->exc_state.exc_type)       );
    XINC( save_exc_traceback      = set0(&ts->exc_state.exc_traceback)  );
# endif
    save_exc_info = ts->exc_info;
    ts->exc_info = &ts->exc_state;
    BUG_ON(ts->exc_state.previous_item != NULL);
#else
    XINC( save_exc_value          = set0(&ts->exc_value)          );
    XINC( save_exc_type           = set0(&ts->exc_type)           );
    XINC( save_exc_traceback      = set0(&ts->exc_traceback)      );
#endif

    XINC( save_async_exc          = set0(&ts->async_exc)          );

    /* before py3k python also stores exception in sys.exc_* variables (wrt
     * sys.exc_info()) for "backward compatibility", but we do not care about it
     * as sys.exc_* variables are not thread safe and from a thread point of view
     * can be changing at arbitrary times during while its python code runs. */

#if BIGFILE_USE_OLD_BUFFER
    pybuf = PyBuffer_FromReadWriteMemory(buf, file->blksize);
#else
    pybuf = PyMemoryView_FromMemory(buf, file->blksize, PyBUF_WRITE /* = read-write */);
#endif
    if (!pybuf)
        goto err;

    /* NOTE K = unsigned long long */
    BUILD_ASSERT(sizeof(blk) == sizeof(unsigned long long));
    loadret = PyObject_CallMethod(&pyfile->pyobj, "loadblk", "KO", blk, pybuf);

    /* python should return to original frame */
    BUG_ON(ts != PyThreadState_GET());
    ts_frame = PyThreadState_GetFrame(ts);
    BUG_ON(ts_frame != ts_frame_orig);

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
            /* NOTE this could be noop if GC was already started from another
             * thread, called some python code via decref and then while python
             * code there was executing - python thread switch happens to us to
             * come here with gc.collecting=1
             *
             * NOTE also: while collecting garbage even more garbage can be
             * created due to arbitrary code run from under __del__ of released
             * objects and weakref callbacks. This way after here GC collect
             * even a single allocation could trigger GC, and thus arbitrary
             * python code run, again */
            PyGC_Collect();

            /* garbage collection could result in running arbitrary code
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
                PyCodeObject  *f_code;
                PyObject      *f_locals;

                /* if it was the frame used for our calling to py loadblk() we
                 * can replace pybuf to "<pybuf>" there in loadblk arguments */
                if (PyFrame_Check(user)) {
                    f = (PyFrameObject *)user;
                    if (!XPyFrame_IsCalleeOf(f, ts_frame))
                        continue;

                    /* "fast" locals (->f_localsplus) */
#if PY_VERSION_HEX >= 0x030B0000  // 3.11
                    fastlocals = f->f_frame->localsplus;
#else
                    fastlocals = f->f_localsplus;
#endif

                    f_code = PyFrame_GetCode(f);
                    for (j = f_code->co_nlocals; j >= 0; --j) {
                        if (fastlocals[j] == pybuf) {
                            Py_INCREF(pybuf_str);
                            fastlocals[j] = pybuf_str;
                            Py_DECREF(pybuf);
                        }
                    }

                    /* ->f_locals */
#if PY_VERSION_HEX >= 0x030B0000  // 3.11
                    f_locals = f->f_frame->f_locals;
#else
                    f_locals = f->f_locals;
#endif
                    if (f_locals != NULL) {
                        TODO(!PyDict_CheckExact(f_locals));

                        PyObject *key, *value;
                        Py_ssize_t pos = 0;

                        while (PyDict_Next(f_locals, &pos, &key, &value)) {
                            if (value == pybuf) {
                                int err;
                                err = PyDict_SetItem(f_locals, key, pybuf_str);
                                BUG_ON(err == -1);
                            }
                        }
                    }

                    Py_DECREF(f_code);
                }
            }

            Py_DECREF(pybuf_users);

            /* see note ^^^ around PyGC_Collect() call that we can land here
             * with arbitrary python code ran again (because e.g.
             * XPyObject_GetReferrers() allocates at least a list and that
             * might trigger automatic GC again */
            if (PyErr_Occurred())
                PyErr_PrintEx(0);
            XPyErr_FullClear();
        }

        /* above attempts were "best effort" to unreference pybuf. However we
         * cannot completely do so. For example if
         *  1. f_locals dict reference pybuf
         *  2. f_locals contains only not-tracked by GC keys & values (and pybuf is not tracked)
         *  3. frame object for which f_locals was created is already garbage-collected
         *
         * the f_locals dict won't be even listed in pybuf referrers (python
         * dicts and tuples with all atomic types becomes not tracked by GC),
         * so we cannot even inspect it.
         *
         * if nothing helped, as a last resort, unpin pybuf from its original
         * memory and make it point to zero-sized NULL.
         *
         * In general this is not strictly correct to do as other buffers &
         * memoryview objects created from pybuf, copy its pointer on
         * initialization and thus pybuf unpinning won't adjust them.
         *
         * However we require BigFile implementations to make sure not to use
         * such-created objects, if any, after return from loadblk().
         */
        if (pybuf->ob_refcnt != 1) {
#if BIGFILE_USE_OLD_BUFFER
            PyBufferObject *pybufo = (PyBufferObject *)pybuf;
            XPyBufferObject_Unpin(pybufo);
#else
            PyMemoryViewObject *pybufm = (PyMemoryViewObject *)pybuf;
            XPyBuffer_Unpin(&pybufm->view);
#endif

            /* we released pybuf->base object which might run some code in __del__
             * clear exception state again */
            if (PyErr_Occurred())
                PyErr_PrintEx(0);
            XPyErr_FullClear();
        }

#if 0
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
#endif
    }

    /* drop pybuf
     *
     * NOTE in theory decref could run arbitrary code, but buffer_dealloc() is
     * simply PyObject_DEL which does not lead to running python code. */
    Py_XDECREF(pybuf);
    if (PyErr_Occurred()) {
        WARN("python thread-state found with exception set; but should not");
        WARN("I will dump the exception and then crash");
        PyErr_PrintEx(0);
    }
    BUG_ON(ts->curexc_type  || ts->curexc_value || ts->curexc_traceback);
#if BIGFILE_USE_PYTS_EXC_INFO
    if (ts->exc_info != &ts->exc_state) {
        WARN("python thread-state found with active generator after loadblk call");
        WARN("I will crash"); // TODO dump which generator is still running
        BUG();
    }
    exc_value       = ts->exc_state.exc_value;
# if !BIGFILE_PYTS_EXC_INFO_ONLY_EXC_VALUE
    exc_type        = ts->exc_state.exc_type;
    exc_traceback   = ts->exc_state.exc_traceback;
# endif
#else
    exc_value       = ts->exc_value;
    exc_type        = ts->exc_type;
    exc_traceback   = ts->exc_traceback;
#endif
    if (exc_value) {
        WARN("python thread-state found with handled but not cleared exception state");
        WARN("I will dump it and then crash");
        fprintf(stderr, "ts->exc_value:\t");        PyObject_Print(exc_value, stderr, 0);
#if !BIGFILE_PYTS_EXC_INFO_ONLY_EXC_VALUE
        fprintf(stderr, "\nts->exc_type:\t");       PyObject_Print(exc_type, stderr, 0);
        fprintf(stderr, "\nts->exc_traceback:\t");  PyObject_Print(exc_traceback, stderr, 0);
#endif
        fprintf(stderr, "\n");
        PyErr_Display(exc_type, exc_value, exc_traceback);
    }
    BUG_ON(exc_type     || exc_value    || exc_traceback);
    BUG_ON(ts->async_exc);

    /* release held frame. this can run arbitrary code in theory */
    Py_XDECREF(ts_frame_orig);
    Py_XDECREF(ts_frame);
    if (PyErr_Occurred())
        PyErr_PrintEx(0);

    /* now restore exception state to original */
    ts->curexc_type         = save_curexc_type;
    ts->curexc_value        = save_curexc_value;
    ts->curexc_traceback    = save_curexc_traceback;

#if BIGFILE_USE_PYTS_EXC_INFO
    ts->exc_state.exc_value     = save_exc_value;
# if !BIGFILE_PYTS_EXC_INFO_ONLY_EXC_VALUE
    ts->exc_state.exc_type      = save_exc_type;
    ts->exc_state.exc_traceback = save_exc_traceback;
# endif
    ts->exc_info = save_exc_info;
    BUG_ON(ts->exc_state.previous_item != NULL);
#else
    ts->exc_value           = save_exc_value;
    ts->exc_type            = save_exc_type;
    ts->exc_traceback       = save_exc_traceback;
#endif

    ts->async_exc           = save_async_exc;

    Py_XDECREF( save_curexc_type        );
    Py_XDECREF( save_curexc_value       );
    Py_XDECREF( save_curexc_traceback   );
    Py_XDECREF( save_exc_type           );
    Py_XDECREF( save_exc_value          );
    Py_XDECREF( save_exc_traceback      );
    Py_XDECREF( save_async_exc          );

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
    PyBigFile *pyfile = container_of(file, PyBigFile, file);
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
    storeret = PyObject_CallMethod(&pyfile->pyobj, "storeblk", "KO", blk, pybuf);

    /* we need to know only whether storeret != NULL, decref it now */
    Py_XDECREF(storeret);

    /* FIXME the following is not strictly correct e.g. for:
     *   mbuf = memoryview(buf)
     * because mbuf.ptr will be a copy of buf.ptr and clearing buf does not
     * clear mbuf.
     *
     * However we require BigFile implementations to make sure not to use
     * such-created objects, if any, after return from storeblk().
     *
     * See more details in loadblk() codepath */

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


/* PyBigFile: mmap methods.
 * They redirect op X to type.blkmmapper.X without going to Python level */

static int
pybigfile_mmap_setup_read(VMA *vma, BigFile *file0, blk_t blk, size_t blklen)
{
    PyBigFile *file = container_of(file0, PyBigFile, file);
    ASSERT(file->blkmmap_ops != NULL);
    return file->blkmmap_ops->mmap_setup_read(vma, file0, blk, blklen);
}

static int
pybigfile_remmap_blk_read(VMA *vma, BigFile *file0, blk_t blk)
{
    PyBigFile *file = container_of(file0, PyBigFile, file);
    ASSERT(file->blkmmap_ops != NULL);
    return file->blkmmap_ops->remmap_blk_read(vma, file0, blk);
}

static int
pybigfile_munmap(VMA *vma, BigFile *file0)
{
    PyBigFile *file = container_of(file0, PyBigFile, file);
    ASSERT(file->blkmmap_ops != NULL);
    return file->blkmmap_ops->munmap(vma, file0);
}


static const struct bigfile_ops pybigfile_ops = {
    .loadblk    = pybigfile_loadblk,
    .storeblk   = pybigfile_storeblk,
    //.release    =

    .mmap_setup_read    = pybigfile_mmap_setup_read,
    .remmap_blk_read    = pybigfile_remmap_blk_read,
    .munmap             = pybigfile_munmap,
};



static PyObject *
pyfileh_open(PyObject *pyfile0, PyObject *args)
{
    PyBigFile   *pyfile = container_of(pyfile0, PyBigFile, pyobj);
    PyBigFileH  *pyfileh;
    /* NOTE no virtmem lock needed - default RAM does not change */
    RAM *ram = ram_get_default(NULL);   // TODO get ram from args
    int err;

    int mmap_overlay = -1; /* -1 means None; https://bugs.python.org/issue14705 */
    if (!PyArg_ParseTuple(args, "|i", &mmap_overlay))
        return NULL;

    if (mmap_overlay == -1)
        mmap_overlay = (pyfile->blkmmap_ops != NULL ? 1 : 0);
    if (mmap_overlay && pyfile->blkmmap_ops == NULL)
        return PyErr_Format(PyExc_TypeError,
                "%s type does not provide blkmmapper", pyfile0->ob_type->tp_name);

    pyfileh = PyType_New(PyBigFileH, &PyBigFileH_Type, NULL);
    if (!pyfileh)
        return NULL;

    Py_INCREF(pyfile);
    err = fileh_open(&pyfileh->fileh, &pyfile->file, ram,
                        mmap_overlay ? MMAP_OVERLAY : DONT_MMAP_OVERLAY);
    if (err) {
        XPyErr_SetFromErrno();
        Py_DECREF(pyfile);
        Py_DECREF(pyfileh);
        return NULL;
    }

    return &pyfileh->pyobj;
}

/* pyfile vs cyclic GC */
static int
pyfile_traverse(PyObject *pyfile0, visitproc visit, void *arg)
{
    PyBigFile  *pyfile = container_of(pyfile0, PyBigFile, pyobj);

    Py_VISIT(pyfile->blkmmapper);
    return 0;
}

static int
pyfile_clear(PyObject *pyfile0)
{
    PyBigFile  *pyfile = container_of(pyfile0, PyBigFile, pyobj);

    Py_CLEAR(pyfile->blkmmapper);
    return 0;
}


static void
pyfile_dealloc(PyObject *pyfile0)
{
    /* PyBigFile supports cyclic GC - first, before destructing, remove it from GC
     * list to avoid segfaulting on double entry here - e.g. if GC is triggered
     * from a weakref callback, or concurrently from another thread.
     *
     * See https://bugs.python.org/issue31095 for details */
    PyObject_GC_UnTrack(pyfile0);

    PyBigFile  *pyfile = container_of(pyfile0, PyBigFile, pyobj);

    pyfile->blkmmap_ops = NULL;

    pyfile_clear(&pyfile->pyobj);
    pyfile->pyobj.ob_type->tp_free(&pyfile->pyobj);
}


static PyObject *
pyfile_new(PyTypeObject *type, PyObject *args, PyObject *kw)
{
    PyBigFile    *self;
    PyObject     *blkmmapper;
    bigfile_ops  *blkmmap_ops = NULL;

    /* try to get type.blkmmapper and verify it provides IBlkMMapper interface */
    blkmmapper = PyObject_GetAttrString((PyObject*)type, "blkmmapper");
    PyErr_Clear(); /* GetAttr raises exception if there is no attribute */
    if (blkmmapper) {
        if (!PyCapsule_IsValid(blkmmapper, "wendelin.bigfile.IBlkMMapper")) {
            Py_DECREF(blkmmapper);
            return PyErr_Format(PyExc_TypeError,
                "%s: .blkmmapper is not a valid pycapsule with mmap methods", type->tp_name);
        }

        blkmmap_ops = PyCapsule_GetPointer(blkmmapper, "wendelin.bigfile.IBlkMMapper");
        if (blkmmap_ops == NULL) { /* just in case - must not fail */
            Py_DECREF(blkmmapper);
            return NULL;
        }

        if (blkmmap_ops->loadblk ||
            blkmmap_ops->storeblk)
        {
            Py_DECREF(blkmmapper);
            return PyErr_Format(PyExc_TypeError,
                "%s: .blkmmapper: !mmap methods present", type->tp_name);
        }

        if (!(blkmmap_ops->mmap_setup_read &&
              blkmmap_ops->remmap_blk_read &&
              blkmmap_ops->munmap))
        {
            Py_DECREF(blkmmapper);
            return PyErr_Format(PyExc_TypeError,
                "%s: .blkmmapper: not all mmap methods present", type->tp_name);
        }
    }

    self = (PyBigFile *)PyType_GenericNew(type, args, kw);
    if (!self) {
        Py_XDECREF(blkmmapper);
        return NULL;
    }

    self->blkmmapper  = blkmmapper;
    self->blkmmap_ops = blkmmap_ops;

    // FIXME "k" = unsigned long - we need size_t
    static char *kw_list[] = {"blksize", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "k", kw_list, &self->file.blksize)) {
        Py_DECREF(self);
        return NULL;
    }

    self->file.file_ops = &pybigfile_ops;
    return &self->pyobj;
}


static PyMemberDef pyfile_members[] = {
    {"blksize", T_ULONG /* XXX vs typeof(blksize) = size_t ? */, offsetof(PyBigFile, file.blksize), READONLY, "block size"},
    {NULL}
};

static /*const*/ PyMethodDef pyfile_methods[] = {
    {"fileh_open",  pyfileh_open,   METH_VARARGS,   "fileh_open(ram=None, mmap_overlay=None) -> new file handle"},
    {NULL}
};

static PyTypeObject PyBigFile_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name            = "_bigfile.BigFile",
    .tp_basicsize       = sizeof(PyBigFile),
    .tp_flags           = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
    .tp_traverse        = pyfile_traverse,
    .tp_clear           = pyfile_clear,
    .tp_methods         = pyfile_methods,
    .tp_members         = pyfile_members,
    .tp_dealloc         = pyfile_dealloc,
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
    PyObject  *x_curexc_value,   *x_curexc_type,     *x_curexc_traceback;
    PyObject  *x_exc_value,      *x_exc_type = NULL, *x_exc_traceback = NULL;
    PyObject  *x_async_exc;
    PyThreadState *ts;

    ts = PyThreadState_GET();

    x_curexc_type       = set0(&ts->curexc_type);
    x_curexc_value      = set0(&ts->curexc_value);
    x_curexc_traceback  = set0(&ts->curexc_traceback);
#if BIGFILE_USE_PYTS_EXC_INFO
    /* NOTE clearing top-level exc_state; if there is an active generator
     * spawned - its exc state is preserved. */
    x_exc_value         = set0(&ts->exc_state.exc_value);
# if !BIGFILE_PYTS_EXC_INFO_ONLY_EXC_VALUE
    x_exc_type          = set0(&ts->exc_state.exc_type);
    x_exc_traceback     = set0(&ts->exc_state.exc_traceback);
# endif
#else
    x_exc_value         = set0(&ts->exc_value);
    x_exc_type          = set0(&ts->exc_type);
    x_exc_traceback     = set0(&ts->exc_traceback);
#endif
    x_async_exc         = set0(&ts->async_exc);

    Py_XDECREF(x_curexc_type);
    Py_XDECREF(x_curexc_value);
    Py_XDECREF(x_curexc_traceback);
    Py_XDECREF(x_exc_type);
    Py_XDECREF(x_exc_value);
    Py_XDECREF(x_exc_traceback);
    Py_XDECREF(x_async_exc);
}

static PyObject* /* PyListObject* */
XPyObject_GetReferrers(PyObject *obj)
{
    PyObject *obj_referrers = PyObject_CallMethod(gcmodule, "get_referrers", "O", obj);
    BUG_ON(!obj_referrers);
    BUG_ON(!PyList_CheckExact(obj_referrers));
    return /*(PyListObject *)*/obj_referrers;
}

void
XPyObject_PrintReferrers(PyObject *obj, FILE *fp)
{
    PyObject *obj_referrers = XPyObject_GetReferrers(obj);
    PyObject_Print(obj_referrers, fp, 0);
    Py_DECREF(obj_referrers);
}

static int
XPyFrame_IsCalleeOf(PyFrameObject *f, PyFrameObject *top)
{
    int ok = 0;
    PyFrameObject *f_back;

    Py_XINCREF(f);
    while (f) {
        if (f == top) {
            ok = 1;
            break;
        }
        f_back = PyFrame_GetBack(f);
        Py_XDECREF(f);
        f = f_back;
    }

    Py_XDECREF(f);
    return ok;
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
