// Copyright (C) 2018-2021  Nexedi SA and Contributors.
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

package main
// misc utilities

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"syscall"

	log "github.com/golang/glog"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/pkg/errors"
)

// ---- FUSE ----

// eInvalError is the error wrapper signifying that underlying error is about "invalid argument".
// err2LogStatus converts such errors into EINVAL return code + logs as warning.
type eInvalError struct {
	err error
}

func (e *eInvalError) Error() string {
	return "invalid argument: " + e.err.Error()
}

// don't propagate eInvalError.Cause -> e.err

func eINVAL(err error) *eInvalError {
	return &eInvalError{err}
}

func eINVALf(format string, argv ...interface{}) *eInvalError {
	return eINVAL(fmt.Errorf(format, argv...))
}

// err2LogStatus converts an error into FUSE status code and logs it appropriately.
//
// the error is logged because otherwise, if e.g. returning EINVAL or EIO
// codes, there is no more detail except the error code itself.
func err2LogStatus(err error) fuse.Status {
	// no error
	if err == nil {
		return fuse.OK
	}

	// direct usage of error code - don't log
	ecode, iscode := err.(syscall.Errno)
	if iscode {
		return fuse.Status(ecode)
	}

	// handling canceled -> EINTR, don't log
	e := errors.Cause(err)
	switch e {
	case context.Canceled:
		return fuse.EINTR
	case io.ErrClosedPipe:
		return fuse.Status(syscall.ECONNRESET)
	}

	// otherwise log as warnings EINVAL and as errors everything else
	switch e.(type) {
	case *eInvalError:
		log.WarningDepth(1, err)
		return fuse.EINVAL

	default:
		log.ErrorDepth(1, err)
		return fuse.EIO
	}
}


// fsNode should be used instead of nodefs.DefaultNode in wcfs.
//
// nodefs.DefaultNode.Open returns ENOSYS. This is convenient for filesystems
// that have no dynamic files at all. But for filesystems, where there are some
// dynamic files - i.e. nodes which do need to support Open, returning ENOSYS
// from any single node will make the kernel think that the filesystem does not
// support Open at all.
//
// In wcfs we have dynamic files (e.g. upcoming /head/watch) and this way we have to
// avoid returning ENOSYS on nodes, that do not need file handles.
//
// fsNode is like nodefs.defaultNode, but by default Open returns to kernel
// fh=0 and FOPEN_KEEP_CACHE - similarly how openless case is handled there.
//
// fsNode behaviour can be additionally controlled via fsOptions.
//
// fsNode should be created via newFSNode.
type fsNode struct {
	nodefs.Node

	opt *fsOptions

	// cache for path
	// we don't use hardlinks / don't want to pay locks + traversal price every time.
	xpath atomic.Value
}

func (n *fsNode) Open(flags uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	return &nodefs.WithFlags{
		File:      nil,
		FuseFlags: fuse.FOPEN_KEEP_CACHE,
	}, fuse.OK
}

// fsOptions allows to tune fsNode behaviour.
type fsOptions struct {
	// Sticky nodes are not removed from inode tree on FORGET.
	// Correspondingly OnForget is never called on a sticky node.
	Sticky bool
}

var fSticky = &fsOptions{Sticky: true} // frequently used shortcut

func (n *fsNode) Deletable() bool {
	return !n.opt.Sticky
}

func newFSNode(opt *fsOptions) fsNode { // NOTE not pointer
	return fsNode{
		Node: nodefs.NewDefaultNode(),
		opt:  opt,
	}
}

// path returns node path in its filesystem.
func (n *fsNode) path() string {
	xpath := n.xpath.Load()
	if xpath != nil {
		return xpath.(string)
	}

	// slow part - let's construct the path and remember it
	path := ""
	inode := n.Inode()
	for {
		var name string
		inode, name = inode.Parent()
		if inode == nil {
			break
		}

		path = "/" + name + path
	}

	n.xpath.Store(path)
	return path
}


// NewStaticFile creates nodefs.Node for file with static data.
//
// Created file is sticky.
func NewStaticFile(data []byte) *SmallFile {
	return newSmallFile(func(_ *fuse.Context) ([]byte, error) {
		return data, nil
	}, fuse.FOPEN_KEEP_CACHE /*see ^^^*/)
}

// SmallFile is a nodefs.Node for file with potentially dynamic, but always small, data.
type SmallFile struct {
	fsNode
	fuseFlags uint32 // fuse.FOPEN_*

	// readData gives whole file data
	readData func(fctx *fuse.Context) ([]byte, error)
}

func newSmallFile(readData func(*fuse.Context) ([]byte, error), fuseFlags uint32) *SmallFile {
	return &SmallFile{
		fsNode:    newFSNode(&fsOptions{Sticky: true}),
		fuseFlags: fuseFlags,
		readData:  readData,
	}
}

// NewSmallFile creates nodefs.Node for file with dynamic, but always small, data.
//
// Created file is sticky.
func NewSmallFile(readData func(*fuse.Context) ([]byte, error)) *SmallFile {
	return newSmallFile(readData, fuse.FOPEN_DIRECT_IO)
}

func (f *SmallFile) Open(flags uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	return &nodefs.WithFlags{
		File:      nil,
		FuseFlags: f.fuseFlags,
	}, fuse.OK
}

func (f *SmallFile) GetAttr(out *fuse.Attr, _ nodefs.File, fctx *fuse.Context) fuse.Status {
	data, err := f.readData(fctx)
	if err != nil {
		return err2LogStatus(err)
	}
	out.Size = uint64(len(data))
	out.Mode = fuse.S_IFREG | 0644
	return fuse.OK
}

func (f *SmallFile) Read(_ nodefs.File, dest []byte, off int64, fctx *fuse.Context) (fuse.ReadResult, fuse.Status) {
	data, err := f.readData(fctx)
	if err != nil {
		return nil, err2LogStatus(err)
	}
	l := int64(len(data))
	end := off + l
	if end > l {
		end = l
	}

	return fuse.ReadResultData(data[off:end]), fuse.OK
}


// mkdir adds child to parent as directory.
//
// Note: parent must be already in the filesystem tree - i.e. associated
// with Inode. if not - nodefs will panic in Inode.NewChild on nil dereference.
func mkdir(parent nodefs.Node, name string, child nodefs.Node) {
	parent.Inode().NewChild(name, true, child)
}

// mkfile adds child to parent as file.
//
// Note: parent must be already in the filesystem tree (see mkdir for details).
func mkfile(parent nodefs.Node, name string, child nodefs.Node) {
	parent.Inode().NewChild(name, false, child)
}


// mount is like nodefs.MountRoot but allows to pass in full fuse.MountOptions.
func mount(mntpt string, root nodefs.Node, opts *fuse.MountOptions) (*fuse.Server, *nodefs.FileSystemConnector, error) {
	nodefsOpts := nodefs.NewOptions()
	nodefsOpts.Debug = opts.Debug

	return nodefs.Mount(mntpt, root, opts, nodefsOpts)
}


// ---- make df happy (else it complains "function not supported") ----

func (root *Root) StatFs() *fuse.StatfsOut {
	return &fuse.StatfsOut{
		// filesystem sizes (don't try to estimate)
		Blocks: 0,
		Bfree:  0,
		Bavail: 0,

		// do we need to count files?
		Files: 0,
		Ffree: 0,

		// block size
		Bsize:	2*1024*1024, // "optimal transfer block size" XXX better get from root?
		Frsize: 2*1024*1024, // "fragment size"

		NameLen: 255, // XXX ok? /proc uses the same
	}
}

// ---- misc ----

func panicf(format string, argv ...interface{}) {
	panic(fmt.Sprintf(format, argv...))
}
