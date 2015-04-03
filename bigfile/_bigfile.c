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
 */

#include <Python.h>


static /*const*/ PyMethodDef pybigfile_modulemeths[] = {
    {NULL}
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

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&pybigfile_moduledef);
#else
    m = Py_InitModule("_bigfile", pybigfile_modulemeths);
#endif
    if (!m)
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
