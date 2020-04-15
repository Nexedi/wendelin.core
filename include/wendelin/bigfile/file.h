#ifndef _WENDELIN_BIGFILE_FILE_H_
#define _WENDELIN_BIGFILE_FILE_H_

/* Wendelin.bigfile | Base file class
 * Copyright (C) 2014-2020  Nexedi SA and Contributors.
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

/* Header wendelin/bigfile/file.h provides BigFile and interfaces that
 * particular BigFile implementations must provide.
 *
 * The interfaces are described in `struct bigfile_ops`.
 * A particular BigFile implementation must provide loadblk/storeblk methods.
 *
 * Clients work with bigfiles via mapping files to memory - see
 * wendelin/bigfile/virtmem.h and BigFileH for client-level API details.
 */

#include <stddef.h>
#include <wendelin/bigfile/types.h>

#ifdef __cplusplus
extern "C" {
#endif


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

/* bigfile_ops defines interface that BigFile implementations must provide. */
struct bigfile_ops {
    /* loadblk is called to load file block into memory.
     *
     * NOTE loadblk is called from SIGSEGV signal handler context.
     *
     * len(buf) must be = file->blksize.
     * @return  0 - ok      !0 - fail
     */
    int  (*loadblk)  (BigFile *file, blk_t blk, void *buf);

    /* storeblk is called to store file block from memory.
     *
     * NOTE contrary to loadblk, storeblk is called from regular context.
     *
     * len(buf) must be = file->blksize.
     * @return  0 - ok      !0 - fail
     */
    int  (*storeblk) (BigFile *file, blk_t blk, const void *buf);

    /* release is called to release resources associated with file.
     *
     * The file is not otherwise used at the time of and past release call.
     */
    void (*release)  (BigFile *file);
};
typedef struct bigfile_ops bigfile_ops;

#ifdef __cplusplus
}
#endif

#endif
