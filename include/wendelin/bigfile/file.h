#ifndef _WENDELIN_BIGFILE_FILE_H_
#define _WENDELIN_BIGFILE_FILE_H_

/* Wendelin.bigfile | Base file class
 * Copyright (C) 2014-2015  Nexedi SA and Contributors.
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
 *
 * ~~~~~~~~
 *
 * TODO description
 *
 * Clients usually work with bigfiles via mapping files to memory -
 * see wendelin/bigfile/virtmem.h and BigFileH for details. XXX
 */

#include <stddef.h>
#include <wendelin/bigfile/types.h>


/* BigFile base class
 *
 * BigFile is a file of fixed size blocks. It knows how to load/store blocks
 * to/from memory. Nothing else.
 *
 * Concrete file implementations subclass BigFile and define their file_ops.
 */
struct BigFile {
    const struct bigfile_ops *file_ops;

    size_t blksize;     /* size of block */
};
typedef struct BigFile BigFile;

struct bigfile_ops {
    /* load/store file block to/from memory.
     * NOTE len(buf) must be = file->blksize
     *
     * @return  0 - ok      !0 - fail
     */
    // XXX loadblk  - called from SIGSEGV context;
    //     storeblk - called from usual context
    //                (in the future maybe in SIGSEGV context too?)
    //
    // TODO maybe also/instead use ram-fd based interface and then
    // loadblk/storeblk would use splice(2) into/from that?
    int  (*loadblk)  (BigFile *file, blk_t blk, void *buf);
    int  (*storeblk) (BigFile *file, blk_t blk, const void *buf);

    /* release file */
    void (*release)  (BigFile *file);
};


#endif
