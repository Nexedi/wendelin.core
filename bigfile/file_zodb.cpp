// Copyright (C) 2019-2021  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

// File file_zodb.cpp provides blkmmapper functions for _ZBigFile.
// MMapping is implemented via wcfs client.

#include "wcfs/client/wcfs.h"
#include "wendelin/bigfile/file.h"
#include "wendelin/bigfile/virtmem.h"
#include "bigfile/_bigfile.h"
#include "bigfile/_file_zodb.h"
#include <ccan/container_of/container_of.h>

static int zfile_mmap_setup_read(VMA *vma, BigFile *file, blk_t blk, size_t blklen) {
    _ZBigFile* _zfile = container_of(file, _ZBigFile, __pyx_base.file);

    wcfs::FileH fileh = _zfile->wfileh;
    wcfs::Mapping mmap;
    error err;

    if (fileh == nil)
        panic("BUG: zfile_mmap_setup_read: ZBigFile.fileh_open did not set .wfileh");

    tie(mmap, err) = fileh->mmap(blk, blklen, vma);
    if (err != nil) {
        log::Errorf("%s", v(err)); // XXX no way to return error details to virtmem
        return -1;
    }

    return 0;
}

static int zfile_remmap_blk_read(VMA *vma, BigFile *file, blk_t blk) {
    wcfs::_Mapping* mmap   = static_cast<wcfs::_Mapping*>(vma->mmap_overlay_server);
    _ZBigFile*      _zfile = container_of(file, _ZBigFile, __pyx_base.file);

    if (mmap->fileh != _zfile->wfileh)
        panic("BUG: zfile_remmap_blk_read: vma and _zfile point to different wcfs::FileH");

    error err;
    err = mmap->remmap_blk(blk);
    if (err != nil) {
        log::Errorf("%s", v(err)); // XXX no way to return error details to virtmem
        return -1;
    }

    return 0;
}


static int zfile_munmap(VMA *vma, BigFile *file) {
    wcfs::_Mapping* mmap   = static_cast<wcfs::_Mapping*>(vma->mmap_overlay_server);
    _ZBigFile*      _zfile = container_of(file, _ZBigFile, __pyx_base.file);

    if (mmap->fileh != _zfile->wfileh)
        panic("BUG: zfile_remmap_blk_read: vma and _zfile point to different wcfs::FileH");

    error err;
    err = mmap->unmap();
    if (err != nil) {
        log::Errorf("%s", v(err)); // XXX no way to return error details to virtmem
        return -1;
    }

    return 0;
}


// NOTE reusing whole bigfile_ops for just .mmap* ops.
extern const bigfile_ops ZBigFile_mmap_ops;
static bigfile_ops _mkZBigFile_mmap_ops() {
    // workaround for "sorry, unimplemented: non-trivial designated initializers not supported"
    bigfile_ops _;
    _.mmap_setup_read   = zfile_mmap_setup_read;
    _.remmap_blk_read   = zfile_remmap_blk_read;
    _.munmap            = zfile_munmap;
    _.loadblk  = NULL;
    _.storeblk = NULL;
    return _;
};
const bigfile_ops ZBigFile_mmap_ops = _mkZBigFile_mmap_ops();
