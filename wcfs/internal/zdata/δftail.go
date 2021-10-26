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

package zdata

// ΔFtail provides ZBigFile-level history tail.
//
// It translates ZODB object-level changes to information about which blocks of
// which ZBigFile were modified, and provides service to query that information.
// See ΔFtail class documentation for usage details.
//
//
// ΔFtail organization
//
// ΔFtail leverages:
//
//	- ΔBtail to track changes to ZBigFile.blktab BTree, and
//	- ΔZtail to track changes to ZBlk objects and to ZBigFile object itself.
//
// then every query merges ΔBtail and ΔZtail data on the fly to provide
// ZBigFile-level result.
//
// Merging on the fly, contrary to computing and maintaining vδF data, is done
// to avoid complexity of recomputing vδF when tracking set changes. Most of
// ΔFtail complexity is, thus, located in ΔBtail, which implements BTree diff
// and handles complexity of recomputing vδB when set of tracked blocks
// changes after new track requests.
//
// Changes to ZBigFile object indicate epochs. Epochs could be:
//
//	- file creation or deletion,
//	- change of ZBigFile.blksize,
//	- change of ZBigFile.blktab to point to another BTree.
//
// Epochs represent major changes to file history where file is assumed to
// change so dramatically, that practically it can be considered to be a
// "whole" change. In particular, WCFS, upon seeing a ZBigFile epoch,
// invalidates all data in corresponding OS-level cache for the file.
//
// The only historical data, that ΔFtail maintains by itself, is history of
// epochs. That history does not need to be recomputed when more blocks become
// tracked and is thus easy to maintain. It also can be maintained only in
// ΔFtail because ΔBtail and ΔZtail does not "know" anything about ZBigFile.
//
//
// Concurrency
//
// In order to allow multiple Track and queries requests to be served in
// parallel, ΔFtail bases its concurrency promise on ΔBtail guarantees +
// snapshot-style access for vδE and ztrackInBlk in queries:
//
// 1. Track calls ΔBtail.Track and quickly updates .byFile, .byRoot and
//    _RootTrack indices under a lock.
//
// 2. BlkRevAt queries ΔBtail.GetAt and then combines retrieved information
//    about zblk with vδE and δZ.
//
// 3. SliceByFileRev queries ΔBtail.SliceByRootRev and then merges retrieved
//    vδT data with vδZ, vδE and ztrackInBlk.
//
// 4. In queries vδE is retrieved/built in snapshot style similarly to how vδT
//    is built in ΔBtail. Note that vδE needs to be built only the first time,
//    and does not need to be further rebuilt, so the logic in ΔFtail is simpler
//    compared to ΔBtail.
//
// 5. for ztrackInBlk - that is used by SliceByFileRev query - an atomic
//    snapshot is retrieved for objects of interest. This allows to hold
//    δFtail.mu lock for relatively brief time without blocking other parallel
//    Track/queries requests for long.
//
// Combined this organization allows non-overlapping queries/track-requests
// to run simultaneously.
//
// See also "Concurrency" in ΔBtail organization for more details.

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/btree"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/set"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xtail"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xzodb"
)

type setI64 = set.I64
type setOid = set.Oid

// ΔFtail represents tail of revisional changes to files.
//
// It semantically consists of
//
//	[]δF			; rev ∈ (tail, head]
//
// where δF represents a change in files space
//
//	δF:
//		.rev↑
//		{} file ->  {}blk | EPOCH
//
// Only files and blocks explicitly requested to be tracked are guaranteed to
// be present. In particular a block that was not explicitly requested to be
// tracked, even if it was changed in δZ, is not guaranteed to be present in δF.
//
// After file epoch (file creation, deletion, or any other change to file
// object) previous track requests for that file become forgotten and have no
// further effect.
//
// ΔFtail provides the following operations:
//
//   .Track(file, blk, path, zblk)	- add file and block reached via BTree path to tracked set.
//
//   .Update(δZ) -> δF				- update files δ tail given raw ZODB changes
//   .ForgetPast(revCut)			- forget changes ≤ revCut
//   .SliceByRev(lo, hi) -> []δF		- query for all files changes with rev ∈ (lo, hi]
//   .SliceByFileRev(file, lo, hi) -> []δfile	- query for changes of a file with rev ∈ (lo, hi]
//   .BlkRevAt(file, #blk, at) -> blkrev	- query for what is last revision that changed
//						  file[#blk] as of @at database state.
//
// where δfile represents a change to one file
//
//	δfile:
//		.rev↑
//		{}blk | EPOCH
//
// See also zodb.ΔTail and xbtree.ΔBtail
//
//
// Concurrency
//
// ΔFtail is safe to use in single-writer / multiple-readers mode. That is at
// any time there should be either only sole writer, or, potentially several
// simultaneous readers. The table below classifies operations:
//
//	Writers:  Update, ForgetPast
//	Readers:  Track + all queries (SliceByRev, SliceByFileRev, BlkRevAt)
//
// Note that, in particular, it is correct to run multiple Track and queries
// requests simultaneously.
type ΔFtail struct {
	// ΔFtail merges ΔBtail with history of ZBlk
	δBtail      *xbtree.ΔBtail

	// mu protects ΔFtail data _and_ all _ΔFileTail/_RootTrack data for all files and roots.
	//
	// NOTE: even though this lock is global it is used only for brief periods of time. In
	// particular working with retrieved vδE and Zinblk snapshot does not need to hold the lock.
	mu sync.Mutex

	byFile map[zodb.Oid]*_ΔFileTail // file -> vδf tail
	byRoot map[zodb.Oid]*_RootTrack // tree-root -> ({foid}, Zinblk)   as of @head

	// set of files, which are newly tracked and for which byFile[foid].vδE was not yet rebuilt
	ftrackNew setOid // {}foid

	// set of tracked ZBlk objects mapped to tree roots  as of @head
	ztrackInRoot map[zodb.Oid]setOid // {} zblk -> {}root
}

// _RootTrack represents tracking information about one particular tree as of @head.
type _RootTrack struct {
	ftrackSet   setOid              // {}foid		which ZBigFiles refer to this tree
	ztrackInBlk map[zodb.Oid]setI64 // {} zblk -> {}blk	which blocks map to zblk
}

// _ΔFileTail represents tail of revisional changes to one file.
type _ΔFileTail struct {
	root zodb.Oid      // .blktab as of @head
	vδE  []_ΔFileEpoch // epochs (changes to ZBigFile object itself)  ; nil if not yet rebuilt

	rebuildJob *_RebuildJob // !nil if vδE rebuild is currently in-progress

	btrackReqSet setI64 // set of blocks explicitly requested to be tracked in this file
}

// _ΔFileEpoch represent a change to ZBigFile object.
type _ΔFileEpoch struct {
	Rev        zodb.Tid
	oldRoot    zodb.Oid // .blktab was pointing to oldRoot before   ; VDEL if ZBigFile deleted
	newRoot    zodb.Oid // .blktab was changed to point to newRoot  ; ----//----
	oldBlkSize int64    // .blksize was oldBlkSize                  ; -1   if ZBigFile deleted
	newBlkSize int64    // .blksize was changed to newBlkSize       ; ----//----

	// snapshot of ztrackInBlk for this file right before this epoch
	oldZinblk map[zodb.Oid]setI64 // {} zblk -> {}blk
}

// _RebuildJob represents currently in-progress vδE rebuilding job.
type _RebuildJob struct {
	ready chan struct{} // closed when job completes
	err   error
}


// ΔF represents a change in files space.
type ΔF struct {
	Rev    zodb.Tid
	ByFile map[zodb.Oid]*ΔFile // foid -> δfile
}

// ΔFile represents a change to one file.
type ΔFile struct {
	Rev    zodb.Tid
	Epoch  bool   // whether file changed completely
	Blocks setI64 // changed blocks
	Size   bool   // whether file size changed
}


// NewΔFtail creates new ΔFtail object.
//
// Initial tracked set is empty.
// Initial coverage of created ΔFtail is (at₀, at₀].
//
// db will be used by ΔFtail to open database connections to load data from
// ZODB when needed.
func NewΔFtail(at0 zodb.Tid, db *zodb.DB) *ΔFtail {
	return &ΔFtail{
		δBtail:       xbtree.NewΔBtail(at0, db),
		byFile:       map[zodb.Oid]*_ΔFileTail{},
		byRoot:       map[zodb.Oid]*_RootTrack{},
		ftrackNew:    setOid{},
		ztrackInRoot: map[zodb.Oid]setOid{},
	}
}

// (tail, head] coverage
func (δFtail *ΔFtail) Head() zodb.Tid { return δFtail.δBtail.Head() }
func (δFtail *ΔFtail) Tail() zodb.Tid { return δFtail.δBtail.Tail() }


// ---- Track/rebuild/Update/Forget ----

// Track associates file[blk]@head with zblk object and file[blkcov]@head with tree path.
//
// Path root becomes associated with the file, and the path and zblk object become tracked.
// One root can be associated with several files (each provided on different Track calls).
//
// zblk can be nil, which represents a hole.
// blk can be < 0, which requests not to establish file[blk] -> zblk
// association. zblk must be nil in this case.
//
// Objects in path and zblk must be with .PJar().At() == .head
func (δFtail *ΔFtail) Track(file *ZBigFile, blk int64, path []btree.LONode, blkcov btree.LKeyRange, zblk ZBlk) {
	head := δFtail.Head()

	fileAt := file.PJar().At()
	if fileAt != head {
		panicf("file.at (@%s)  !=  δFtail.head (@%s)", fileAt, head)
	}
	if zblk != nil {
		zblkAt := zblk.PJar().At()
		if zblkAt != head {
			panicf("zblk.at (@%s)  !=  δFtail.head (@%s)", zblkAt, head)
		}
	}
	// path.at == head is verified by ΔBtail.Track

	foid := file.POid()
	δFtail.δBtail.Track(path, blkcov)

	rootObj := path[0].(*btree.LOBTree)
	root    := rootObj.POid()

	δFtail.mu.Lock()
	defer δFtail.mu.Unlock()

	rt, ok := δFtail.byRoot[root]
	if !ok {
		rt = &_RootTrack{
			ftrackSet:   setOid{},
			ztrackInBlk: map[zodb.Oid]setI64{},
		}
		δFtail.byRoot[root] = rt
	}
	rt.ftrackSet.Add(foid)

	δftail, ok := δFtail.byFile[foid]
	if !ok {
		δftail = &_ΔFileTail{
			root:         root,
			vδE:          nil /*will need to be rebuilt to past till tail*/,
			btrackReqSet: setI64{},
		}
		δFtail.byFile[foid] = δftail
		δFtail.ftrackNew.Add(foid)
	}
	if δftail.root != root {
		// .root can change during epochs, but in between them it must be stable
		panicf("BUG: zfile<%s> root mutated from %s -> %s", foid, δftail.root, root)
	}
	if blk >= 0 {
		δftail.btrackReqSet.Add(blk)
	}


	// associate zblk with root, if it was not hole
	if zblk != nil {
		if blk < 0 {
			panicf("BUG: zfile<%s>: blk=%d, but zblk != nil", foid, blk)
		}
		zoid := zblk.POid()

		inroot, ok := δFtail.ztrackInRoot[zoid]
		if !ok {
			inroot = make(setOid, 1)
			δFtail.ztrackInRoot[zoid] = inroot
		}
		inroot.Add(root)

		inblk, ok := rt.ztrackInBlk[zoid]
		if !ok {
			inblk = make(setI64, 1)
			rt.ztrackInBlk[zoid] = inblk
		}
		inblk.Add(blk)
	}
}

// vδEForFile returns vδE and current root for specified file.
//
// It builds vδE for that file if there is such need.
// The only case when vδE actually needs to be built is when the file just started to be tracked.
//
// It also returns δftail for convenience.
// NOTE access to returned δftail must be protected via δFtail.mu.
func (δFtail *ΔFtail) vδEForFile(foid zodb.Oid) (vδE []_ΔFileEpoch, headRoot zodb.Oid, δftail *_ΔFileTail, err error) {
	δFtail.mu.Lock() // TODO verify that there is no in-progress writers
	defer δFtail.mu.Unlock()

	δftail = δFtail.byFile[foid]
	root := δftail.root
	vδE = δftail.vδE
	if vδE != nil {
		return vδE, root, δftail, nil
	}

	// vδE needs to be built
	job := δftail.rebuildJob

	// rebuild is currently in-progress -> wait for corresponding job to complete
	if job != nil {
		δFtail.mu.Unlock()
		<-job.ready
		if job.err == nil {
			δFtail.mu.Lock()
			vδE = δftail.vδE
		}
		return vδE, root, δftail, job.err
	}

	// we become responsible to build vδE
	// release the lock while building to allow simultaneous access to other files
	job = &_RebuildJob{ready: make(chan struct{})}
	δftail.rebuildJob = job
	δFtail.ftrackNew.Del(foid)
	δBtail := δFtail.δBtail

	δFtail.mu.Unlock()
	vδE, err = vδEBuild(foid, δBtail.ΔZtail(), δBtail.DB())
	δFtail.mu.Lock()

	if err == nil {
		δftail.vδE = vδE
	} else {
		δFtail.ftrackNew.Add(foid)
	}

	δftail.rebuildJob = nil
	job.err = err
	close(job.ready)

	return vδE, root, δftail, err
}

// _rebuildAll rebuilds vδE for all files from ftrackNew requests.
//
// must be called with δFtail.mu locked.
func (δFtail *ΔFtail) _rebuildAll() (err error) {
	defer xerr.Contextf(&err, "ΔFtail rebuildAll")

	δBtail := δFtail.δBtail
	δZtail := δBtail.ΔZtail()
	db     := δBtail.DB()
	for foid := range δFtail.ftrackNew {
		δFtail.ftrackNew.Del(foid)
		δftail := δFtail.byFile[foid]
		// no need to set δftail.rebuildJob - we are under lock
		δftail.vδE, err = vδEBuild(foid, δZtail, db)
		if err != nil {
			δFtail.ftrackNew.Add(foid)
			return err
		}
	}

	return nil
}

// Update updates δFtail given raw ZODB changes.
//
// It returns change in files space that corresponds to δZ.
//
// δZ should include all objects changed by ZODB transaction.
func (δFtail *ΔFtail) Update(δZ *zodb.EventCommit) (_ ΔF, err error) {
	headOld := δFtail.Head()
	defer xerr.Contextf(&err, "ΔFtail update %s -> %s", headOld, δZ.Tid)

	δFtail.mu.Lock()
	defer δFtail.mu.Unlock()
	// TODO verify that there is no in-progress readers/writers

	// rebuild vδE for newly tracked files
	err = δFtail._rebuildAll()
	if err != nil {
		return ΔF{}, err
	}

	δB, err := δFtail.δBtail.Update(δZ)
	if err != nil {
		return ΔF{}, err
	}

	δF := ΔF{Rev: δB.Rev, ByFile: make(map[zodb.Oid]*ΔFile)}

	// take ZBigFile changes into account
	δzfile := map[zodb.Oid]*_ΔZBigFile{} // which tracked ZBigFiles are changed
	for _, oid := range δZ.Changev {
		δftail, ok := δFtail.byFile[oid]
		if !ok {
			continue // not ZBigFile or file is not tracked
		}
		δ, err := zfilediff(δFtail.δBtail.DB(), oid, headOld, δZ.Tid)
		if err != nil {
			return ΔF{}, err
		}
		//fmt.Printf("zfile<%s> diff %s..%s   -> δ: %v\n", oid, headOld, δZ.Tid, δ)

		if δ != nil {
			δzfile[oid] = δ
			δE := _ΔFileEpoch{
				Rev:        δZ.Tid,
				oldRoot:    δ.blktabOld,
				newRoot:    δ.blktabNew,
				oldBlkSize: δ.blksizeOld,
				newBlkSize: δ.blksizeNew,
				oldZinblk:  map[zodb.Oid]setI64{},
			}

			rt, ok := δFtail.byRoot[δftail.root]
			if ok {
				for zoid, inblk := range rt.ztrackInBlk {
					δE.oldZinblk[zoid] = inblk.Clone()
					inroot, ok := δFtail.ztrackInRoot[zoid]
					if ok {
						inroot.Del(δftail.root)
						if len(inroot) == 0 {
							delete(δFtail.ztrackInRoot, zoid)
						}
					}
				}
			}

			δftail.root = δE.newRoot
			// NOTE no need to clone vδE: we are writer, vδE is never returned to
			// outside, append does not invalidate previous vδE retrievals.
			δftail.vδE  = append(δftail.vδE, δE)
			δftail.btrackReqSet = setI64{}
		}
	}

	// take btree changes into account
	//fmt.Printf("δB.ByRoot: %v\n", δB.ByRoot)
	for root, δt := range δB.ByRoot {
		//fmt.Printf("root: %v   δt: %v\n", root, δt)
		rt, ok := δFtail.byRoot[root]
		// NOTE rt might be nil e.g. if a zfile was tracked, then
		// deleted, but the tree referenced by zfile.blktab is still
		// not-deleted, remains tracked and is changed.
		if !ok {
			continue
		}

		for file := range rt.ftrackSet {
			δfile, ok := δF.ByFile[file]
			if !ok {
				δfile = &ΔFile{Rev: δF.Rev, Blocks: make(setI64)}
				δF.ByFile[file] = δfile
			}
			for blk /*, δzblk*/ := range δt {
				δfile.Blocks.Add(blk)
			}

			// TODO invalidate .size only if key >= maxkey was changed (size increase),
			// or if on the other hand maxkey was deleted (size decrease).
			//
			// XXX currently we invalidate size on any topology change.
			δfile.Size = true
		}

		// update ztrackInBlk according to btree changes
		for blk, δzblk := range δt {
			if δzblk.Old != xbtree.VDEL {
				inblk, ok := rt.ztrackInBlk[δzblk.Old]
				if ok {
					inblk.Del(blk)
					if len(inblk) == 0 {
						delete(rt.ztrackInBlk, δzblk.Old)
						inroot := δFtail.ztrackInRoot[δzblk.Old]
						inroot.Del(root)
						if len(inroot) == 0 {
							delete(δFtail.ztrackInRoot, δzblk.Old)
						}
					}
				}
			}

			if δzblk.New != xbtree.VDEL {
				inblk, ok := rt.ztrackInBlk[δzblk.New]
				if !ok {
					inblk = make(setI64, 1)
					rt.ztrackInBlk[δzblk.New] = inblk
					inroot, ok := δFtail.ztrackInRoot[δzblk.New]
					if !ok {
						inroot = make(setOid, 1)
						δFtail.ztrackInRoot[δzblk.New] = inroot
					}
					inroot.Add(root)
				}
				inblk.Add(blk)
			}
		}
	}

	// take zblk changes into account
	for _, oid := range δZ.Changev {
		inroot, ok := δFtail.ztrackInRoot[oid]
		if !ok {
			continue // not tracked
		}

		for root := range inroot {
			rt := δFtail.byRoot[root] // must be there
			inblk, ok := rt.ztrackInBlk[oid]
			if !ok || len(inblk) == 0 {
				continue
			}
			//fmt.Printf("root: %v   inblk: %v\n", root, inblk)
			for file := range rt.ftrackSet {
				δfile, ok := δF.ByFile[file]
				if !ok {
					δfile = &ΔFile{Rev: δF.Rev, Blocks: make(setI64)}
					δF.ByFile[file] = δfile
				}

				δfile.Blocks.Update(inblk)
			}
		}
	}

	// if ZBigFile object is changed - it starts new epoch for that file
	for foid, δ := range δzfile {
		δfile, ok := δF.ByFile[foid]
		if !ok {
			δfile = &ΔFile{Rev: δF.Rev}
			δF.ByFile[foid] = δfile
		}
		δfile.Epoch = true
		δfile.Blocks = nil
		δfile.Size = false

		//fmt.Printf("δZBigFile: %v\n", δ)

		// update .byRoot
		if δ.blktabOld != xbtree.VDEL {
			rt, ok := δFtail.byRoot[δ.blktabOld]
			if ok {
				rt.ftrackSet.Del(foid)
				if len(rt.ftrackSet) == 0 {
					delete(δFtail.byRoot, δ.blktabOld)
					// Zinroot -= δ.blktabNew
					for zoid := range rt.ztrackInBlk {
						inroot, ok := δFtail.ztrackInRoot[zoid]
						if ok {
							inroot.Del(δ.blktabOld)
							if len(inroot) == 0 {
								delete(δFtail.ztrackInRoot, zoid)
							}
						}
					}
				}
			}
		}
		if δ.blktabNew != xbtree.VDEL {
			rt, ok := δFtail.byRoot[δ.blktabNew]
			if !ok {
				rt = &_RootTrack{
					ftrackSet:   setOid{},
					ztrackInBlk: map[zodb.Oid]setI64{},
				}
				δFtail.byRoot[δ.blktabNew] = rt
			}
			rt.ftrackSet.Add(foid)
		}
	}

	//fmt.Printf("-> δF: %v\n", δF)
	return δF, nil
}


// ForgetPast discards all δFtail entries with rev ≤ revCut.
func (δFtail *ΔFtail) ForgetPast(revCut zodb.Tid) {
	δFtail.mu.Lock()
	defer δFtail.mu.Unlock()
	// TODO verify that there is no in-progress readers/writers

	δFtail.δBtail.ForgetPast(revCut)

	// TODO keep index which file changed epoch where (similarly to ΔBtail),
	// and, instead of scanning all files, trim vδE only on files that is really necessary.
	for _, δftail := range δFtail.byFile {
		δftail._forgetPast(revCut)
	}
}

func (δftail *_ΔFileTail) _forgetPast(revCut zodb.Tid) {
	icut := 0
	for ; icut < len(δftail.vδE); icut++ {
		if δftail.vδE[icut].Rev > revCut {
			break
		}
	}

	// vδE[:icut] should be forgotten
	if icut > 0 { // XXX workaround for ΔFtail.ForgetPast calling forgetPast on all files
		δftail.vδE = append([]_ΔFileEpoch{}, δftail.vδE[icut:]...)
	}
}

// ---- queries ----

// TODO if needed
// func (δFtail *ΔFtail) SliceByRev(lo, hi zodb.Tid) /*readonly*/ []ΔF

// _ZinblkOverlay is used by SliceByFileRev.
// It combines read-only Zinblk base with read-write adjustment.
// It provides the following operations:
//
//	- Get(zblk) -> {blk},
//	- AddBlk(zblk, blk),
//	- DelBlk(zblk, blk)
type _ZinblkOverlay struct {
	Base map[zodb.Oid]setI64 // taken from _RootTrack.ztrackInBlk or _ΔFileEpoch.oldZinblk
	Adj  map[zodb.Oid]setI64 // adjustment over base; blk<0 represents whiteout
}

// SliceByFileRev returns history of file changes in (lo, hi] range.
//
// it must be called with the following condition:
//
//	tail ≤ lo ≤ hi ≤ head
//
// the caller must not modify returned slice.
//
// Only tracked blocks are guaranteed to be present.
//
// Note: contrary to regular go slicing, low is exclusive while high is inclusive.
func (δFtail *ΔFtail) SliceByFileRev(zfile *ZBigFile, lo, hi zodb.Tid) (/*readonly*/[]*ΔFile, error) {
	return δFtail.SliceByFileRevEx(zfile, lo, hi, QueryOptions{})
}

// SliceByFileRevEx is extended version of SliceByFileRev with options.
func (δFtail *ΔFtail) SliceByFileRevEx(zfile *ZBigFile, lo, hi zodb.Tid, opt QueryOptions) (/*readonly*/[]*ΔFile, error) {
	foid := zfile.POid()
	//fmt.Printf("\nslice f<%s> (@%s,@%s]\n", foid, lo, hi)
	vδf, err := δFtail._SliceByFileRev(foid, lo, hi, opt)
	if err != nil {
		err = fmt.Errorf("slice f<%s> (@%s,@%s]: %e", foid, lo, hi, err)
	}
	return vδf, err
}

// QueryOptions represents options for SliceBy* queries.
type QueryOptions struct {
	// OnlyExplicitlyTracked requests that only blocks, that were
	// explicitly tracked, are included into result.
	//
	// By default SliceBy* return information about both blocks that
	// were explicitly tracked, and blocks that became tracked due to being
	// adjacent to a tracked block in BTree bucket.
	OnlyExplicitlyTracked bool
}

func (δFtail *ΔFtail) _SliceByFileRev(foid zodb.Oid, lo, hi zodb.Tid, opt QueryOptions) (/*readonly*/[]*ΔFile, error) {
	xtail.AssertSlice(δFtail, lo, hi)

	// query .δBtail.SliceByRootRev(file.blktab, lo, hi) +
	// merge δZBlk history with that.

	// merging tree (δT) and Zblk (δZblk) histories into file history (δFile):
	//
	//     δT	────────·──────────────·─────────────────·────────────
	//		        │              │
	//		        ↓              │
	//     δZblk₁	────────────────o───────────────────o─────────────────
	//                                     |
	//                                     ↓
	//     δZblk₂	────────────x────────────────x────────────────────────
	//
	//
	//     δFile	────────o───────o──────x─────x────────────────────────


	vδE, headRoot, δftail, err := δFtail.vδEForFile(foid)
	if err != nil {
		return nil, err
	}

	var vδf []*ΔFile
	// vδfTail returns or creates vδf entry for revision tail
	// tail must be <= all vδf revisions
	vδfTail := func(tail zodb.Tid) *ΔFile {
		if l := len(vδf); l > 0 {
			δfTail := vδf[l-1]
			if δfTail.Rev == tail {
				return δfTail
			}
			if !(tail < δfTail.Rev) {
				panic("BUG: tail not ↓")
			}
		}

		δfTail := &ΔFile{Rev: tail, Blocks: setI64{}}
		vδf = append(vδf, δfTail)
		return δfTail
	}

	vδZ := δFtail.δBtail.ΔZtail().SliceByRev(lo, hi)
	iz := len(vδZ) - 1

	// find epoch that covers hi
	le := len(vδE)
	ie := sort.Search(le, func(i int) bool {
		return hi < vδE[i].Rev
	})
	// vδE[ie]   is next epoch
	// vδE[ie-1] is epoch that covers hi

	// loop through all epochs from hi till lo
	for lastEpoch := false; !lastEpoch ; {
		// current epoch
		var epoch zodb.Tid
		ie--
		if ie < 0 {
			epoch = δFtail.Tail()
		} else {
			epoch = vδE[ie].Rev
		}

		if epoch <= lo {
			epoch = lo
			lastEpoch = true
		}

		var root zodb.Oid // root of blktab in current epoch
		var head zodb.Tid // head] of current epoch coverage
		// state of Zinblk as we are scanning ← current epoch
		// initially corresponds to head of the epoch (= @head for latest epoch)
		Zinblk := _ZinblkOverlay{}	// zblk -> which #blk refers to it
		var ZinblkAt zodb.Tid           // Zinblk covers [ZinblkAt,<next δT>)
		if ie+1 == le {
			// head
			root = headRoot
			head = δFtail.Head()

			// take atomic Zinblk snapshot that covers vδZ
			//
			// - the reason we take atomic snapshot is because simultaneous Track
			//   requests might change Zinblk concurrently, and without snapshotting
			//   this might result in changes to a block being not uniformly present in
			//   the returned vδf (some revision indicates change to that block, while
			//   another one - where the block is too actually changed - does not
			//   indicate change to that block).
			//
			// - the reason we limit snapshot to vδZ is to reduce amount of under-lock
			//   copying, because original Zinblk is potentially very large.
			//
			// NOTE the other approach could be to keep blocks in _RootTrack.Zinblk with
			// serial (!= zodb serial), and work with that _RootTrack.Zinblk snapshot by
			// ignoring all blocks with serial > serial of snapshot view. Do not kill
			// _ZinblkOverlay yet because we keep this  approach in mind for the future.
			ZinblkSnap := map[zodb.Oid]setI64{}
			δZAllOid := setOid{}
			for _, δZ := range vδZ {
				for _, oid := range δZ.Changev {
					δZAllOid.Add(oid)
				}
			}
			δFtail.mu.Lock()
			rt, ok := δFtail.byRoot[root]
			if ok {
				for oid := range δZAllOid {
					inblk, ok := rt.ztrackInBlk[oid]
					if ok {
						ZinblkSnap[oid] = inblk.Clone()
					}
				}
			}
			δFtail.mu.Unlock()

			Zinblk.Base = ZinblkSnap
		} else {
			δE := vδE[ie+1]
			root = δE.oldRoot
			head = δE.Rev - 1 // TODO better set to exact revision coming before δE.Rev
			Zinblk.Base = δE.oldZinblk
		}
		//fmt.Printf("Zinblk: %v\n", Zinblk)

		// vδT for current epoch
		var vδT []xbtree.ΔTree
		if root != xbtree.VDEL {
			vδT, err = δFtail.δBtail.SliceByRootRev(root, epoch, head) // NOTE @head, not hi
			if err != nil {
				return nil, err
			}
		}
		it := len(vδT) - 1
		if it >= 0 {
			ZinblkAt = vδT[it].Rev
		} else {
			ZinblkAt = epoch
		}

		// merge cumulative vδT(epoch,head] update to Zinblk, so that
		// changes to blocks that were not explicitly requested to be
		// tracked, are present in resulting slice uniformly.
		//
		// For example on
		//
		//	at1   T/B0:a,1:b,2:c   δDø		δ{0,1,2}
		//	at2   δT{0:d,1:e}      δD{c}		δ{0,1,2}
		//	at3   δTø              δD{c,d,e}	δ{0,1,2}
		//	at4   δTø              δD{c,e}		δ{  1,2}
		//
		// if tracked={0}, for (at1,at4] query, changes to 1 should be
		// also all present @at2, @at3 and @at4 - because @at2 both 0
		// and 1 are changed in the same tracked bucket. Note that
		// changes to 2 should not be present at all.
		Zinblk.Adj = map[zodb.Oid]setI64{}
		for _, δT := range vδT {
			for blk, δzblk := range δT.KV {
				if δzblk.Old != xbtree.VDEL {
					inblk, ok := Zinblk.Adj[δzblk.Old]
					if ok {
						inblk.Del(blk)
					}
				}
				if δzblk.New != xbtree.VDEL {
					inblk, ok := Zinblk.Adj[δzblk.New]
					if !ok {
						inblk = setI64{}
						Zinblk.Adj[δzblk.New] = inblk
					}
					inblk.Add(blk)
				}
			}
		}

		// merge vδZ and vδT of current epoch
		for ((iz >= 0 && vδZ[iz].Rev > epoch) || it >= 0) {
			// δZ that is covered by current Zinblk
			// -> update δf
			if iz >= 0 && vδZ[iz].Rev > epoch {
				δZ := vδZ[iz]
				if ZinblkAt <= δZ.Rev {
					//fmt.Printf("δZ @%s\n", δZ.Rev)
					for _, oid := range δZ.Changev {
						inblk, ok := Zinblk.Get_(oid)
						if ok && len(inblk) != 0 {
							δf := vδfTail(δZ.Rev)
							δf.Blocks.Update(inblk)
						}
					}
					iz--
					continue
				}
			}

			// δT -> adjust Zinblk + update δf
			if it >= 0 {
				δT := vδT[it]
				//fmt.Printf("δT @%s  %v\n", δT.Rev, δT.KV)
				for blk, δzblk := range δT.KV {
					// apply in reverse as we go ←
					if δzblk.New != xbtree.VDEL {
						Zinblk.DelBlk(δzblk.New, blk)
					}
					if δzblk.Old != xbtree.VDEL {
						Zinblk.AddBlk(δzblk.Old, blk)
					}

					if δT.Rev <= hi {
						δf := vδfTail(δT.Rev)
						δf.Blocks.Add(blk)
						δf.Size = true // see Update
					}
				}

				it--
				if it >= 0 {
					ZinblkAt = vδT[it].Rev
				} else {
					ZinblkAt = epoch
				}
			}
		}

		// emit epoch δf
		if ie >= 0 {
			epoch := vδE[ie].Rev
			if epoch > lo { // it could be <=
				δf := vδfTail(epoch)
				δf.Epoch = true
				δf.Blocks = nil // should be already nil
				δf.Size = false // should be already false
			}
		}
	}

	// vδf was built in reverse order
	// invert the order before returning
	for i,j := 0, len(vδf)-1; i<j; i,j = i+1,j-1 {
		vδf[i], vδf[j] = vδf[j], vδf[i]
	}

	// take opt.OnlyExplicitlyTracked into account
	// XXX epochs not handled (currently ok as epochs are rejected by wcfs)
	if opt.OnlyExplicitlyTracked {
		δblk := setI64{}
		for _, δf := range vδf {
			δblk.Update(δf.Blocks)
		}

		δFtail.mu.Lock()
		for blk := range δblk {
			if !δftail.btrackReqSet.Has(blk) {
				δblk.Del(blk)
			}
		}
		δFtail.mu.Unlock()

		for i := len(vδf)-1; i >= 0; i-- {
			δf := vδf[i]
			if δf.Epoch {
				continue
			}
			for blk := range δf.Blocks {
				if !δblk.Has(blk) {
					δf.Blocks.Del(blk)
				}
			}
			if len(δf.Blocks) == 0 {
				// delete @i
				copy(vδf[i:], vδf[i+1:])
				vδf = vδf[:len(vδf)-1]
			}
		}
	}

	return vδf, nil
}

// ZinblkOverlay

// Get_ returns set(blk) for o[zoid].
func (o *_ZinblkOverlay) Get_(zoid zodb.Oid) (inblk /*readonly*/setI64, ok bool) {
	base, bok := o.Base[zoid]
	adj, aok  := o.Adj[zoid]
	if !aok {
		return base, bok
	}

	// combine base + adj
	if bok {
		inblk = base.Clone()
	} else {
		inblk = make(setI64, len(adj))
	}
	for blk := range adj {
		if blk < 0 { // whiteout
			inblk.Del(flipsign(blk))
		} else {
			inblk.Add(blk)
		}
	}
	if len(inblk) == 0 {
		return nil, false
	}
	return inblk, true
}

// DelBlk removes blk from o[zoid].
func (o *_ZinblkOverlay) DelBlk(zoid zodb.Oid, blk int64) {
	if blk < 0 {
		panic("blk < 0")
	}
	o._AddBlk(zoid, flipsign(blk))
}

// AddBlk adds blk to o[zoid].
func (o *_ZinblkOverlay) AddBlk(zoid zodb.Oid, blk int64) {
	if blk < 0 {
		panic("blk < 0")
	}
	o._AddBlk(zoid, blk)
}

func (o *_ZinblkOverlay) _AddBlk(zoid zodb.Oid, blk int64) {
	adj, ok := o.Adj[zoid]
	if !ok {
		adj = make(setI64, 1)
		o.Adj[zoid] = adj
	}
	adj.Add(blk)
	adj.Del(flipsign(blk))
}

// flipsign returns x with sign bit flipped.
func flipsign(x int64) int64 {
	return int64(uint64(x) ^ (1<<63))
}


// BlkRevAt returns last revision that changed file[blk] as of @at database state.
//
// if exact=False - what is returned is only an upper bound for last block revision.
//
// zfile must be any checkout from (tail, head]
// at must ∈ (tail, head]
// blk must be tracked
func (δFtail *ΔFtail) BlkRevAt(ctx context.Context, zfile *ZBigFile, blk int64, at zodb.Tid) (_ zodb.Tid, exact bool, err error) {
	foid := zfile.POid()
	defer xerr.Contextf(&err, "blkrev f<%s> #%d @%s", foid, blk, at)

	//fmt.Printf("\nblkrev #%d @%s\n", blk, at)

	// assert at ∈ (tail, head]
	tail := δFtail.Tail()
	head := δFtail.Head()
	if !(tail < at && at <= head) {
		panicf("at out of bounds: at: @%s,  (tail, head] = (@%s, @%s]", at, tail, head)
	}

	// assert zfile.at ∈ (tail, head]
	zconn := zfile.PJar()
	zconnAt := zconn.At()
	if !(tail < zconnAt && zconnAt <= head) {
		panicf("zconn.at out of bounds: zconn.at: @%s,  (tail, head] = (@%s, @%s]", zconnAt, tail, head)
	}

	vδE, headRoot, _, err := δFtail.vδEForFile(foid)
	if err != nil {
		return zodb.InvalidTid, false, err
	}

	// find epoch that covers at and associated blktab root/object
	//fmt.Printf("  vδE: %v\n", vδE)
	l := len(vδE)
	i := sort.Search(l, func(i int) bool {
		return at < vδE[i].Rev
	})
	// vδE[i]   is next epoch
	// vδE[i-1] is epoch that covers at

	// root
	var root zodb.Oid
	if i == l {
		root = headRoot
	} else {
		root = vδE[i].oldRoot
	}

	// epoch
	var epoch zodb.Tid
	i--
	if i < 0 {
		// i<0 - first epoch (no explicit start) - use δFtail.tail as lo
		epoch = tail
	} else {
		epoch = vδE[i].Rev
	}

	//fmt.Printf("  epoch: @%s  root: %s\n", epoch, root)

	if root == xbtree.VDEL {
		return epoch, true, nil
	}

	zblk, tabRev, zblkExact, tabRevExact, err := δFtail.δBtail.GetAt(root, blk, at)

	//fmt.Printf("  GetAt  #%d @%s  ->  %s(%v), @%s(%v)\n", blk, at, zblk, zblkExact, tabRev, tabRevExact)
	if err != nil {
		return zodb.InvalidTid, false, err
	}

	if tabRev < epoch {
		tabRev = epoch
		tabRevExact = true
	}

	// if δBtail does not have entry that covers root[blk] - get it
	// through any zconn with .at ∈ (tail, head].
	if !zblkExact {
		xblktab, err := zconn.Get(ctx, root)
		if err != nil {
			return zodb.InvalidTid, false, err
		}
		blktab, err := vBlktab(xblktab)
		if err != nil {
			return zodb.InvalidTid, false, err
		}

		xzblkObj, ok, err := blktab.Get(ctx, blk)
		if err != nil {
			return zodb.InvalidTid, false, err
		}
		if !ok {
			zblk = xbtree.VDEL
		} else {
			zblkObj, err := vZBlk(xzblkObj)
			if err != nil {
				return zodb.InvalidTid, false, fmt.Errorf("blktab<%s>[#%d]: %s", root, blk, err)
			}
			zblk = zblkObj.POid()
		}
	}

	// block was removed
	if zblk == xbtree.VDEL {
		return tabRev, tabRevExact, nil
	}

	// blktab[blk] was changed to point to a zblk @tabRev.
	// blk revision is max rev and when zblk changed last in (rev, at] range.
	zblkRev, zblkRevExact := δFtail.δBtail.ΔZtail().LastRevOf(zblk, at)
	//fmt.Printf("  ZRevOf %s @%s -> @%s, %v\n", zblk, at, zblkRev, zblkRevExact)
	if zblkRev > tabRev {
		return zblkRev, zblkRevExact, nil
	} else {
		return tabRev, tabRevExact, nil
	}
}


// ---- vδEBuild (vδE rebuild core) ----

// vδEBuild builds vδE for file from vδZ.
func vδEBuild(foid zodb.Oid, δZtail *zodb.ΔTail, db *zodb.DB) (vδE []_ΔFileEpoch, err error) {
	defer xerr.Contextf(&err, "file<%s>: build vδE", foid)

	vδE  = []_ΔFileEpoch{}
	vδZ := δZtail.Data()
	atPrev := δZtail.Tail()
	for i := 0; i < len(vδZ); i++ {
		δZ := vδZ[i]

		fchanged := false
		for _, oid := range δZ.Changev {
			if oid == foid {
				fchanged = true
				break
			}
		}
		if !fchanged {
			continue
		}

		δ, err := zfilediff(db, foid, atPrev, δZ.Rev)
		if err != nil {
			return nil, err
		}

		if δ != nil {
			δE := _ΔFileEpoch{
				Rev:        δZ.Rev,
				oldRoot:    δ.blktabOld,
				newRoot:    δ.blktabNew,
				oldBlkSize: δ.blksizeOld,
				newBlkSize: δ.blksizeNew,
				oldZinblk:  nil, // nothing was tracked
			}
			vδE = append(vδE, δE)
		}

		atPrev = δZ.Rev
	}

	return vδE, nil
}

// zfilediff returns direct difference for ZBigFile<foid> old..new .
type _ΔZBigFile struct {
	blksizeOld, blksizeNew int64
	blktabOld,  blktabNew  zodb.Oid
}
func zfilediff(db *zodb.DB, foid zodb.Oid, old, new zodb.Tid) (δ *_ΔZBigFile, err error) {
	txn, ctx := transaction.New(context.TODO()) // TODO - merge in cancel via ctx arg
	defer txn.Abort()
	zconnOld, err := db.Open(ctx, &zodb.ConnOptions{At: old})
	if err != nil {
		return nil, err
	}
	zconnNew, err := db.Open(ctx, &zodb.ConnOptions{At: new})
	if err != nil {
		return nil, err
	}

	a, err1 := zgetFileOrNil(ctx, zconnOld, foid)
	b, err2 := zgetFileOrNil(ctx, zconnNew, foid)
	err = xerr.Merge(err1, err2)
	if err != nil {
		return nil, err
	}

	return diffF(ctx, a, b)
}

// diffF returns direct difference in between two ZBigFile objects.
func diffF(ctx context.Context, a, b *ZBigFile) (δ *_ΔZBigFile, err error) {
	defer xerr.Contextf(&err, "diffF %s %s", xzodb.XidOf(a), xzodb.XidOf(b))

	δ = &_ΔZBigFile{}
	if a == nil {
		δ.blksizeOld = -1
		δ.blktabOld  = xbtree.VDEL
	} else {
		err = a.PActivate(ctx);  if err != nil { return nil, err }
		defer a.PDeactivate()
		δ.blksizeOld = a.blksize
		δ.blktabOld  = a.blktab.POid()
	}

	if b == nil {
		δ.blksizeNew = -1
		δ.blktabNew  = xbtree.VDEL
	} else {
		err = b.PActivate(ctx);  if err != nil { return nil, err }
		defer b.PDeactivate()
		δ.blksizeNew = b.blksize
		δ.blktabNew  = b.blktab.POid()
	}

	// return δ=nil if no change
	if δ.blksizeOld == δ.blksizeNew && δ.blktabOld == δ.blktabNew {
		δ = nil
	}
	return δ, nil
}

// zgetFileOrNil returns ZBigFile corresponding to zconn.Get(oid) .
// if the file does not exist, (nil, ok) is returned.
func zgetFileOrNil(ctx context.Context, zconn *zodb.Connection, oid zodb.Oid) (zfile *ZBigFile, err error) {
	defer xerr.Contextf(&err, "getfile %s@%s", oid, zconn.At())
	xfile, err := xzodb.ZGetOrNil(ctx, zconn, oid)
	if xfile == nil || err != nil {
		return nil, err
	}

	zfile, ok := xfile.(*ZBigFile)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %s", zodb.ClassOf(xfile))
	}
	return zfile, nil
}
