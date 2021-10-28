#ifndef _WENDELIN_BIGFILE_FILE_H_
#define _WENDELIN_BIGFILE_FILE_H_

/* Wendelin.bigfile | Base file class
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

/* Header wendelin/bigfile/file.h provides BigFile and interfaces that
 * particular BigFile implementations must provide.
 *
 * The interfaces are described in `struct bigfile_ops`.
 * A particular BigFile implementation must provide loadblk/storeblk and
 * optionally mmap_* methods.
 *
 * Clients work with bigfiles via mapping files to memory - see
 * wendelin/bigfile/virtmem.h and BigFileH for client-level API details.
 */

#include <stddef.h>
#include <wendelin/bigfile/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct VMA VMA;


/* BigFile base class
 *
 * BigFile is a file of fixed size blocks. It knows how to load/store blocks
 * to/from memory. It can be also optionally mmaped into memory.
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


    /* Mmap overlaying
     *
     * Besides .loadblk and .storeblk a particular BigFile implementation can
     * also optionally provide functions to setup read-only memory mappings
     * with BigFile data. If such functions are provided, virtmem might use
     * them to organize read access to BigFile data through the mappings and
     * without allocating RAM for read pages. RAM will still be allocated for
     * dirtied pages that are layed over base data layer provided by the
     * mappings.
     *
     * The primary user of this functionality is wcfs - virtual filesystem that
     * provides access to ZBigFile data via OS-level files(*). The layering can
     * be schematically depicted as follows
     *
     *                  ┌──┐                      ┌──┐
     *                  │RW│                      │RW│      ← dirty pages
     *                  └──┘                      └──┘
     *                            +
     *      ─────────────────────────────────────────────   ← mmap'ed base data
     *
     * The functions to setup memory mappings are:
     *
     *   - mmap_setup_read(vma, file[blk +blklen))    setup initial read-only mmap to serve vma
     *   - remmap_blk_read(vma, file[blk])            remmap blk into vma again, after e.g.
     *                                                RW dirty page was discarded
     *   - munmap(vma)                                before VMA is unmapped
     *
     *
     * (*) see wcfs/client/wcfs.h and wcfs/wcfs.go
     */


    /* mmap_setup_read is called to setup new read-only mapping of file[blk +blklen).
     *
     * The mapping will be used as the base read-only layer for vma.
     *
     * After setup bigfile backend manages the mapping and can change it dynamically
     * e.g. due to changes to the file from outside. However before changing a page,
     * the backend must check if that page was already dirtied by virtmem and if
     * so don't change that page until virtmem calls .remmap_blk_read.
     *
     * The checking has to be done with virtmem lock held. A sketch of mapping
     * update sequence is as below:
     *
     *      // backend detects that block is changed from outside
     *      // fileh is vma->fileh - file handle with which the vma is associated
     *      virt_lock()
     *      for (pgoff : page_offsets_covered_by(blk))
     *          if (!__fileh_page_isdirty(fileh, pgoff)) {
     *              // update mappings for all fileh's vma that cover pgoff
     *          }
     *      virt_unlock()
     *
     * mmap_setup_read must set vma.addr_start and vma.addr_stop according to
     * created memory mapping.
     *
     * mmap_setup_read can use vma.mmap_overlay_server to associate vma with
     * object pointer specific to serving created mapping.
     *
     * Called under virtmem lock.       TODO easy to rework to call with !virt_lock
     *
     * NOTE blk and blklen are in blocks, not pages.
     *
     * @addr    NULL - mmap at anywhere,    !NULL - mmap exactly at addr.
     * @return  0 - ok      !0 - fail
     */
    int (*mmap_setup_read) (VMA *vma, BigFile *file, blk_t blk, size_t blklen);


    /* remmap_blk_read is called to remmap a block into vma again, after e.g.
     * RW dirty page was discarded.
     *
     * Called under virtmem lock.       XXX hard to rework to call with !virt_lock
     * Virtmem considers remmap_blk_read failure as fatal.
     */
    int (*remmap_blk_read) (VMA *vma, BigFile *file, blk_t blk);

    /* munmap is called when vma set up via mmap_setup_read is going to be unmapped.
     *
     * Called under virtmem lock.       TODO easy to rework to call with !virt_lock
     * Virtmem considers munmap failure as fatal.
     */
    int (*munmap) (VMA *vma, BigFile *file);
};
typedef struct bigfile_ops bigfile_ops;

#ifdef __cplusplus
}
#endif

#endif
