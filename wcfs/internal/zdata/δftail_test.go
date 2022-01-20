// Copyright (C) 2019-2022  Nexedi SA and Contributors.
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
// tests for δftail.go
//
// This are the main tests for ΔFtail functionality. The primary testing
// concern is to verify how ΔFtail merges ΔBtail and ΔZtail histories on Update
// and queries.
//
// We assume that ΔBtail works correctly (this is covered by ΔBtail tests)
// -> no need to exercise many different topologies and tracking sets here.
//
// Since ΔFtail does not recompute anything by itself when tracking set
// changes, and only merges δBtail and δZtail histories on queries, there is no
// need to exercise many different tracking sets(*). Once again we assume that
// ΔBtail works correctly and verify δFtail only with track=[-∞,∞).
//
// There are 2 testing approaches:
//
//   a) transition a ZBigFile in ZODB through particular .blktab and ZBlk
//      states and feed ΔFtail through created database transactions.
//   b) transition a ZBigFile in ZODB through   random   .blktab and ZBlk
//      states and feed ΔFtail through created database transactions.
//
// TestΔFtail and TestΔFtailRandom implement approaches "a" and "b" correspondingly.
//
// (*) except one small place in SliceByFileRev which handles tracked vs
//     untracked set differences and is verified by TestΔFtailSliceUntrackedUniform.

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/set"
	"lab.nexedi.com/nexedi/wendelin.core/wcfs/internal/xbtree/xbtreetest"
)

type setStr = set.Str

const ø = "ø"


// T is environment for doing ΔFtail tests.
//
// it is based on xbtreetest.T .
type T struct {
	*xbtreetest.T

	foid zodb.Oid // oid of zfile
}


// ΔFTestEntry represents one entry in ΔFtail tests.
type ΔFTestEntry struct {
	δblkTab  map[int64]string  // changes in tree part	{} #blk -> ZBlk<name>
	δdataTab setStr            // changes to ZBlk objects   {ZBlk<name>}
}

// TestΔFtail runs ΔFtail tests on set of concrete prepared testcases.
func TestΔFtail(t *testing.T) {
	// δT is shorthand to create δblkTab.
	type δT = map[int64]string

	// δD is shorthand to create δdataTab.
	δD := func(zblkv ...string) setStr {
		δ := setStr{}
		for _, zblk := range zblkv {
			δ.Add(zblk)
		}
		return δ
	}

	const a,b,c,d,e,f,g,h,i,j = "a","b","c","d","e","f","g","h","i","j"

	testv := []ΔFTestEntry{
		{δT{1:a,2:b,3:ø},	δD(a)},
		{δT{},			δD(c)},
		{δT{2:c},		δD(a,b)},

		// clear the tree
		{δT{1:ø,2:ø},		δD()},

		// i is first associated with file, but later unlinked from it
		// then i is changed -> the file should no be in δF
		{δT{5:i},		δD()},
		{δT{5:e},		δD()},
		{δT{},			δD(i)},

		// delete the file
		{nil, nil},

		// ---- found by TestΔFtailRandom ----

		{δT{1:a,6:i,7:d,8:e},   δD(a,c,e,f,g,h,i,j)},

		// was including ≤ lo entries in SliceByFileRev
		{δT{0:b,2:j,3:i,5:f,6:b,7:i,8:d},	δD(a,b,c,d,e,g,i,j)},
		{δT{0:e,2:h,4:d,9:b},			δD(a,h,i)},
		{δT{0:j,1:i,3:g,5:a,6:e,7:j,8:f,9:d},	δD()},
		{δT{0:b,1:f,2:h,4:b,8:b},		δD(b,d,i)},
		{δT{1:a,3:d,6:j},			δD(b,c,d,f,g,h,i,j)},
		{δT{0:i,1:f,4:e,5:e,7:d,8:h},		δD(d,j)},
		{δT{},					δD(a,b,c,e,f,g,h,i,j)},

		// 0 was missing in δf
		{nil, nil},
		{δT{0:a},				δD()},
		{δT{2:i,3:c,5:d,9:c},			δD(a,b,c,d,e,f,g,h,i)},
		{δT{0:j,1:d,2:h,5:g,6:h,7:c,9:h},	δD(d,e,f,h,j)},
	}

	testq := make(chan ΔFTestEntry)
	go func() {
		defer close(testq)
		for _, test := range testv {
			testq <- test
		}
	}()
	testΔFtail(t, testq)
}

// TestΔFtailRandom runs ΔFtail tests on randomly-generated file changes.
func TestΔFtailRandom(t *testing.T) {
	n    := xbtreetest.N(1E3, 1E4, 1E5)
	nblk := xbtreetest.N(1E1, 2E1, 1E2) // keeps failures detail small on -short

	// random-number generator
	rng, seed := xbtreetest.NewRand()
	t.Logf("# n=%d seed=%d", n, seed)

	vv := "abcdefghij"
	randv := func() string {
		i := rng.Intn(len(vv))
		return vv[i:i+1]
	}

	testq := make(chan ΔFTestEntry)
	go func() {
		defer close(testq)
		for i := 0; i < n; i++ {
			nδblkTab  := rng.Intn(nblk)
			nδdataTab := rng.Intn(len(vv))

			δblkTab  := map[int64]string{}
			δdataTab := setStr{}

			blkv := rng.Perm(nblk)
			for j := 0; j < nδblkTab; j++ {
				blk  := blkv[j]
				zblk := randv()
				δblkTab[int64(blk)] = zblk
			}

			vv_ := rng.Perm(len(vv))
			for j := 0; j < nδdataTab; j++ {
				k := vv_[j]
				v := vv[k:k+1]
				δdataTab.Add(v)
			}

			testq <- ΔFTestEntry{δblkTab, δdataTab}
		}
	}()

	testΔFtail(t, testq)
}

// testΔFtail verifies ΔFtail on sequence of testcases coming from testq.
func testΔFtail(t_ *testing.T, testq chan ΔFTestEntry) {
	t := newT(t_)
	X := exc.Raiseif

	// data built via applying changes from testq
	epochv   := []zodb.Tid{}                      // rev↑
	vδf      := []*ΔFile{}                        // (rev↑, {}blk)
	vδE      := []_ΔFileEpoch{}                   // (rev↑, EPOCH)
	blkTab   := map[int64]string{}                // #blk -> ZBlk<name>
	Zinblk   := map[string]setI64{}               // ZBlk<name> -> which #blk refer to it
	blkRevAt := map[zodb.Tid]map[int64]zodb.Tid{} // {} at -> {} #blk -> rev

	// load dataTab
	dataTab  := map[string]string{}               // ZBlk<name> -> data
	for /*oid*/_, zblki := range t.Head().ZBlkTab {
		dataTab[zblki.Name] = zblki.Data
	}

	// start δFtail when zfile does not yet exists
	// this way we'll verify how ΔFtail rebuilds vδE for started-to-be-tracked file
	t0 := t.Commit("øf")
	t.Logf("#   @%s (%s)", t0.AtSymb(), t0.At)
	epochv = append(epochv, t0.At)
	δFtail := NewΔFtail(t.Head().At, t.DB)

	// create zfile, but do not track it yet
	// vδf + friends will be updated after "load zfile"
	δt1 := map[int64]string{0:"a"}
	t1 := t.Commit(fmt.Sprintf("t%s D%s", xbtreetest.KVTxt(δt1), dataTabTxt(dataTab)))
	δblk1 := setI64{}
	for blk := range δt1 {
		δblk1.Add(blk)
	}
	t.Logf("# → @%s (%s)  δT%s  δD{}  ; %s\tδ%s  *not-yet-tracked", t1.AtSymb(), t1.At, xbtreetest.KVTxt(δt1), t1.Tree, δblk1)
	δF, err := δFtail.Update(t1.ΔZ);  X(err)
	if !(δF.Rev == t1.At && len(δF.ByFile) == 0) {
		t.Errorf("wrong δF:\nhave {%s, %v}\nwant: {%s, ø}", δF.Rev, δF.ByFile, t1.At)
	}

	// load zfile via root['treegen/file']
	txn, ctx := transaction.New(context.Background())
	defer func() {
		txn.Abort()
	}()
	zconn, err := t.DB.Open(ctx, &zodb.ConnOptions{At: t.Head().At, NoPool: true});  X(err)
	zfile, blksize := t.XLoadZFile(ctx, zconn)
	foid := zfile.POid()

	// update vδf + co for t1
	vδf = append(vδf, &ΔFile{Rev: t1.At, Epoch: true})
	vδE = append(vδE, _ΔFileEpoch{
			Rev:        t1.At,
			oldRoot:    zodb.InvalidOid,
			newRoot:    t.Root(),
			oldBlkSize: -1,
			newBlkSize: blksize,
			oldZinblk:  nil,
	})
	epochv = append(epochv, t1.At)
	for blk, zblk := range δt1 {
		blkTab[blk] = zblk
		inblk, ok := Zinblk[zblk]
		if !ok {
			inblk = setI64{}
			Zinblk[zblk] = inblk
		}
		inblk.Add(blk)
	}


	// start tracking zfile[-∞,∞) from the beginning
	// this should make ΔFtail to see all zfile changes
	// ( later retrackAll should be called after new epoch to track zfile[-∞,∞) again )
	retrackAll := func() {
		for blk := range blkTab {
			_, path, blkcov, zblk, _, err := zfile.LoadBlk(ctx, blk);  X(err)
			δFtail.Track(zfile, blk, path, blkcov, zblk)
		}
	}
	retrackAll()


	i := 1 // matches t1
	delfilePrev := false
	for test := range testq {
		i++
		δblk := setI64{}
		δtree := false
		delfile := false

		// {nil,nil} commands to delete zfile
		if test.δblkTab == nil && test.δdataTab == nil {
			delfile = true
		}

		// new epoch starts when file is deleted or recreated
		newEpoch := delfile || (!delfile && delfile != delfilePrev)
		delfilePrev = delfile

		ZinblkPrev := map[string]setI64{}
		for zblk, inblk := range Zinblk {
			ZinblkPrev[zblk] = inblk.Clone()
		}

		// newEpoch -> reset
		if newEpoch {
			blkTab = map[int64]string{}
			Zinblk = map[string]setI64{}
			δblk   = nil
		} else {
			// rebuild blkTab/Zinblk
			for blk, zblk := range test.δblkTab {
				zprev, ok := blkTab[blk]
				if ok {
					inblk := Zinblk[zprev]
					inblk.Del(blk)
					if len(inblk) == 0 {
						delete(Zinblk, zprev)
					}
				} else {
					zprev = ø
				}

				if zblk != ø {
					blkTab[blk] = zblk
					inblk, ok := Zinblk[zblk]
					if !ok {
						inblk = setI64{}
						Zinblk[zblk] = inblk
					}
					inblk.Add(blk)
				} else {
					delete(blkTab, blk)
				}

				// update δblk due to change in blkTab
				if zblk != zprev {
					δblk.Add(blk)
					δtree = true
				}
			}

			// rebuild dataTab
			for zblk := range test.δdataTab {
				data, ok := dataTab[zblk]               // e.g. a -> a2
				if !ok {
					t.Fatalf("BUG: blk %s not in dataTab\ndataTab: %v", zblk, dataTab)
				}
				data = fmt.Sprintf("%s%d", data[:1], i) // e.g. a4
				dataTab[zblk] = data

				// update δblk due to change in ZBlk data
				for blk := range Zinblk[zblk] {
					δblk.Add(blk)
				}
			}
		}

		// commit updated zfile / blkTab + dataTab
		var req string
		if delfile {
			req = "øf"
		} else {
			req = fmt.Sprintf("t%s D%s", xbtreetest.KVTxt(blkTab), dataTabTxt(dataTab))
		}
		commit := t.Commit(req)
		if newEpoch {
			epochv = append(epochv, commit.At)
		}
		flags := ""
		if newEpoch {
			flags += "\tEPOCH"
		}
		t.Logf("# → @%s (%s)  δT%s  δD%s\t; %s\tδ%s%s", commit.AtSymb(), commit.At, xbtreetest.KVTxt(test.δblkTab), test.δdataTab, commit.Tree, δblk, flags)
		//t.Logf("#   vδf: %s", vδfstr(vδf))


		// update blkRevAt
		var blkRevPrev map[int64]zodb.Tid
		if i != 0 {
			blkRevPrev = blkRevAt[δFtail.Head()]
		}
		blkRev := map[int64]zodb.Tid{}
		for blk, rev := range blkRevPrev {
			if newEpoch {
				blkRev[blk] = commit.At
			} else {
				blkRev[blk] = rev
			}
		}
		for blk := range δblk {
			blkRev[blk] = commit.At
		}
		blkRevAt[commit.At] = blkRev
		/*
			fmt.Printf("blkRevAt[@%s]:\n", commit.AtSymb())
			blkv := []int64{}
			for blk := range blkRev {
				blkv = append(blkv, blk)
			}
			sort.Slice(blkv, func(i, j int) bool {
				return blkv[i] < blkv[j]
			})
			for _, blk := range blkv {
				fmt.Printf("  #%d:  %v\n", blk, blkRev[blk])
			}
		*/

		// update zfile
		txn.Abort()
		txn, ctx = transaction.New(context.Background())
		err = zconn.Resync(ctx, commit.At);  X(err)

		var δfok *ΔFile
		if newEpoch || len(δblk) != 0 {
			δfok = &ΔFile{
				Rev:    commit.At,
				Epoch:  newEpoch,
				Blocks: δblk,
				Size:   δtree, // not strictly ok, but matches current ΔFtail code
			}
			vδf = append(vδf, δfok)
		}
		if newEpoch {
			δE := _ΔFileEpoch{Rev: commit.At}
			if delfile {
				δE.oldRoot    = t.Root()
				δE.newRoot    = zodb.InvalidOid
				δE.oldBlkSize = blksize
				δE.newBlkSize = -1
			} else {
				δE.oldRoot    = zodb.InvalidOid
				δE.newRoot    = t.Root()
				δE.oldBlkSize = -1
				δE.newBlkSize = blksize
			}
			oldZinblk := map[zodb.Oid]setI64{}
			for zblk, inblk := range ZinblkPrev {
				oid, _ := commit.XGetBlkByName(zblk)
				oldZinblk[oid] = inblk
			}
			δE.oldZinblk = oldZinblk
			vδE = append(vδE, δE)
		}

		//fmt.Printf("Zinblk: %v\n", Zinblk)

		// update δFtail
		δF, err := δFtail.Update(commit.ΔZ);  X(err)

		// assert δF matches δfok
		t.assertΔF(δF, commit.At, δfok)

		// track whole zfile again if new epoch was started
		if newEpoch {
			retrackAll()
		}

		// verify byRoot
		trackRfiles  := map[zodb.Oid]setOid{}
		for root, rt := range δFtail.byRoot {
			trackRfiles[root] = rt.ftrackSet
		}
		filesOK := setOid{}
		if !delfile {
			filesOK.Add(foid)
		}
		RfilesOK := map[zodb.Oid]setOid{}
		if len(filesOK) != 0 {
			RfilesOK[t.Root()] = filesOK
		}
		if !reflect.DeepEqual(trackRfiles, RfilesOK) {
			t.Errorf("Rfiles:\nhave: %v\nwant: %v", trackRfiles, RfilesOK)
		}

		// verify Zinroot
		trackZinroot := map[string]setOid{}
		for zoid, inroot := range δFtail.ztrackInRoot {
			zblki := commit.ZBlkTab[zoid]
			trackZinroot[zblki.Name] = inroot
		}
		Zinroot := map[string]setOid{}
		for zblk := range Zinblk {
			inroot := setOid{}; inroot.Add(t.Root())
			Zinroot[zblk] = inroot
		}
		if !reflect.DeepEqual(trackZinroot, Zinroot) {
			t.Errorf("Zinroot:\nhave: %v\nwant: %v", trackZinroot, Zinroot)
		}

		// verify Zinblk
		trackZinblk := map[string]setI64{}
		switch {
		case len(δFtail.byRoot) == 0:
			// ok

		case len(δFtail.byRoot) == 1:
			rt, ok := δFtail.byRoot[t.Root()]
			if !ok {
				t.Errorf(".byRoot points to unexpected blktab")
			} else {
				for zoid, inblk := range rt.ztrackInBlk {
					zblki := commit.ZBlkTab[zoid]
					trackZinblk[zblki.Name] = inblk
				}
			}

		default:
			t.Errorf("len(.byRoot) != (0,1)  ; byRoot: %v", δFtail.byRoot)
		}
		if !reflect.DeepEqual(trackZinblk, Zinblk) {
			t.Errorf("Zinblk:\nhave: %v\nwant: %v", trackZinblk, Zinblk)
		}

		// ForgetPast configured threshold
		const ncut = 5
		if len(vδf) >= ncut {
			revcut := vδf[0].Rev
			t.Logf("# forget ≤ @%s", t.AtSymb(revcut))
			δFtail.ForgetPast(revcut)
			vδf = vδf[1:]
			//t.Logf("#   vδf: %s", vδfstr(vδf))
			//t.Logf("#   vδt: %s", vδfstr(δFtail.SliceByFileRev(zfile, δFtail.Tail(), δFtail.Head())))
			icut := 0;
			for ; icut < len(vδE); icut++ {
				if vδE[icut].Rev > revcut {
					break
				}
			}
			vδE = vδE[icut:]
		}

		// verify δftail.root
		δftail := δFtail.byFile[foid]
		rootOK := t.Root()
		if delfile {
			rootOK = zodb.InvalidOid
		}
		if δftail.root != rootOK {
			t.Errorf(".root: have %s  ; want %s", δftail.root, rootOK)
		}

		// verify vδE
		if !reflect.DeepEqual(δftail.vδE, vδE) {
			t.Errorf("vδE:\nhave: %v\nwant: %v", δftail.vδE, vδE)
		}


		// SliceByFileRev
		for j := 0; j < len(vδf); j++ {
			for k := j; k < len(vδf); k++ {
				var lo zodb.Tid
				if j == 0 {
					lo = vδf[0].Rev - 1
				} else {
					lo = vδf[j-1].Rev
				}
				hi := vδf[k].Rev

				vδf_ok := vδf[j:k+1] // [j,k]
				vδf_, err := δFtail.SliceByFileRev(zfile, lo, hi);  X(err)
				if !reflect.DeepEqual(vδf_, vδf_ok) {
					t.Errorf("slice (@%s,@%s]:\nhave: %v\nwant: %v", t.AtSymb(lo), t.AtSymb(hi), t.vδfstr(vδf_), t.vδfstr(vδf_ok))
				}
			}
		}


		// BlkRevAt
		blkv := []int64{} // all blocks
		if l := len(vδf); l > 0 {
			for blk := range blkRevAt[vδf[l-1].Rev] {
				blkv = append(blkv, blk)
			}
		}
		blkv = append(blkv, 1E4/*this block is always hole*/)
		sort.Slice(blkv, func(i, j int) bool {
			return blkv[i] < blkv[j]
		})

		for j := -1; j < len(vδf); j++ {
			var at     zodb.Tid
			var blkRev map[int64]zodb.Tid
			if j == -1 {
				at = δFtail.Tail()
				// blkRev remains ø
			} else {
				at = vδf[j].Rev
				blkRev = blkRevAt[at]
			}
			for _, blk := range blkv {
				rev, exact, err := δFtail.BlkRevAt(ctx, zfile, blk, at);  X(err)
				revOK, ok := blkRev[blk]
				if !ok {
					k := len(epochv) - 1
					for ; k >= 0; k-- {
						if epochv[k] <= at {
							break
						}
					}
					revOK = epochv[k]
				}
				exactOK := true
				if revOK <= δFtail.Tail() {
					revOK, exactOK = δFtail.Tail(), false
				}
				if !(rev == revOK && exact == exactOK) {
					t.Errorf("blkrev #%d @%s:\nhave: @%s, %v\nwant: @%s, %v", blk, t.AtSymb(at), t.AtSymb(rev), exact, t.AtSymb(revOK), exactOK)
				}
			}

		}
	}
}

// TestΔFtailSliceUntrackedUniform verifies that untracked blocks, if present, are present uniformly in returned slice.
//
// Some changes to untracked blocks, might be seen by ΔFtail, because those
// changes occur in the same BTree bucket that covers another change to a
// tracked block.
//
// Here we verify that if some change to such untracked block is ever present,
// SliceByFileRev returns all changes to that untracked block. In other words
// we verify that no change to untracked block is missed, if any change to that
// block is ever present in returned slice.
//
// This test also verifies handling of OnlyExplicitlyTracked query option.
func TestΔFtailSliceUntrackedUniform(t_ *testing.T) {
	t := newT(t_)
	X := exc.Raiseif

	at0 := t.Head().At
	δFtail := NewΔFtail(at0, t.DB)

	// commit t1. all 0, 1 and 2 are in the same bucket.
	t1 := t.Commit("T/B0:a,1:b,2:c")
	δF, err := δFtail.Update(t1.ΔZ);  X(err)
	t.assertΔF(δF, t1.At, nil) // δf empty

	t2 := t.Commit("t0:d,1:e,2:c Da:a,b:b,c:c2,d:d,e:e")   // 0:-a+d 1:-b+e δc₂
	δF, err = δFtail.Update(t2.ΔZ);  X(err)
	t.assertΔF(δF, t2.At, nil)

	t3 := t.Commit("t0:d,1:e,2:c Da:a,b:b,c:c3,d:d3,e:e3") // δc₃ δd₃ δe₃
	δF, err = δFtail.Update(t3.ΔZ);  X(err)
	t.assertΔF(δF, t3.At, nil)

	t4 := t.Commit("t0:d,1:e,2:c Da:a,b:b,c:c4,d:d3,e:e4") // δc₄ δe₄
	δF, err = δFtail.Update(t4.ΔZ);  X(err)
	t.assertΔF(δF, t4.At, nil)

	// load zfile via root['treegen/file']
	txn, ctx := transaction.New(context.Background())
	defer func() {
		txn.Abort()
	}()
	zconn, err := t.DB.Open(ctx, &zodb.ConnOptions{At: t.Head().At});  X(err)
	zfile, _ := t.XLoadZFile(ctx, zconn)

	xtrackBlk := func(blk int64) {
		_, path, blkcov, zblk, _, err := zfile.LoadBlk(ctx, blk);  X(err)
		δFtail.Track(zfile, blk, path, blkcov, zblk)
	}

	// track 0, but do not track 1 and 2.
	// blktab[1] becomes noticed by δBtail because both 0 and 1 are in the same bucket and both are changed @at2.
	// blktab[2] remains unnoticed because it is not changed past at1.
	xtrackBlk(0)

	// assertSliceByFileRev verifies result of SliceByFileRev and SliceByFileRevEx(OnlyExplicitlyTracked=y).
	assertSliceByFileRev := func(lo, hi zodb.Tid, vδf_ok, vδfT_ok []*ΔFile) {
		t.Helper()

		Tonly := QueryOptions{OnlyExplicitlyTracked: true}
		vδf,  err := δFtail.SliceByFileRev  (zfile, lo, hi);         X(err)
		vδfT, err := δFtail.SliceByFileRevEx(zfile, lo, hi, Tonly);  X(err)

		if !reflect.DeepEqual(vδf, vδf_ok) {
			t.Errorf("slice  (@%s,@%s]:\nhave: %v\nwant: %v", t.AtSymb(lo), t.AtSymb(hi), t.vδfstr(vδf), t.vδfstr(vδf_ok))
		}
		if !reflect.DeepEqual(vδfT, vδfT_ok) {
			t.Errorf("sliceT (@%s,@%s]:\nhave: %v\nwant: %v", t.AtSymb(lo), t.AtSymb(hi), t.vδfstr(vδfT), t.vδfstr(vδfT_ok))
		}
	}

	// (at1, at4]  -> changes to both 0 and 1, because they both are changed in the same bucket @at2
	assertSliceByFileRev(t1.At, t4.At,
		/*vδf*/  []*ΔFile{
			&ΔFile{Rev: t2.At, Blocks: b(0,1), Size: true},
			&ΔFile{Rev: t3.At, Blocks: b(0,1), Size: false},
			&ΔFile{Rev: t4.At, Blocks: b(  1), Size: false},
		},
		/*vδfT*/ []*ΔFile{
			&ΔFile{Rev: t2.At, Blocks: b(0  ), Size: true},
			&ΔFile{Rev: t3.At, Blocks: b(0  ), Size: false},
			// no change @at4
		})

	// (at2, at4]  -> changes to only 0, because there is no change to 2 via blktab
	assertSliceByFileRev(t2.At, t4.At,
		/*vδf*/  []*ΔFile{
			&ΔFile{Rev: t3.At, Blocks: b(0),   Size: false},
		},
		/*vδfT*/ []*ΔFile{
			&ΔFile{Rev: t3.At, Blocks: b(0),   Size: false},
		})

	// (at3, at4]  -> changes to only 0, ----/----
	assertSliceByFileRev(t3.At, t4.At,
		/*vδf*/  []*ΔFile(nil),
		/*vδfT*/ []*ΔFile(nil))
}


// newT creates new T.
func newT(t *testing.T) *T {
	t.Helper()
	tt := &T{xbtreetest.NewT(t), zodb.InvalidOid}

	// find out zfile's oid
	txn, ctx := transaction.New(context.Background())
	defer func() {
		txn.Abort()
	}()
	zconn, err := tt.DB.Open(ctx, &zodb.ConnOptions{At: tt.Head().At})
	if err != nil {
		tt.Fatal(err)
	}
	zfile, _ := tt.XLoadZFile(ctx, zconn)
	tt.foid = zfile.POid()

	return tt
}

// XLoadZFile loads zfile from root["treegen/file"]@head.
func (t *T) XLoadZFile(ctx context.Context, zconn *zodb.Connection) (zfile *ZBigFile, blksize int64) {
	t.Helper()
	X := exc.Raiseif
	xzroot, err := zconn.Get(ctx, 0);  X(err)
	zroot := xzroot.(*zodb.Map)
	err = zroot.PActivate(ctx);  X(err)
	zfile = zroot.Data["treegen/file"].(*ZBigFile)
	zroot.PDeactivate()
	err = zfile.PActivate(ctx);  X(err)
	blksize    = zfile.blksize
	blktabOid := zfile.blktab.POid()
	if blktabOid != t.Root() {
		t.Fatalf("BUG: zfile.blktab (%s)  !=  treeroot (%s)", blktabOid, t.Root())
	}
	zfile.PDeactivate()
	return zfile, blksize
}

// assertΔF asserts that δF has rev and δf as expected.
func (t *T) assertΔF(δF ΔF, rev zodb.Tid, δfok *ΔFile) {
	t.Helper()

	// assert δF points to zfile if δfok != ø
	if δF.Rev != rev {
		t.Errorf("wrong δF.Rev: have %s  ; want %s", δF.Rev, rev)
	}
	δfiles := setOid{}
	for δfile := range δF.ByFile {
		δfiles.Add(δfile)
	}
	δfilesOK := setOid{}
	if δfok != nil {
		δfilesOK.Add(t.foid)
	}
	if !δfiles.Equal(δfilesOK) {
		t.Errorf("wrong δF.ByFile:\nhave keys: %s\nwant keys: %s", δfiles, δfilesOK)
		return
	}

	// verify δf
	δf := δF.ByFile[t.foid]
	if !reflect.DeepEqual(δf, δfok) {
		t.Errorf("δf:\nhave: %v\nwant: %v", δf, δfok)
	}
}

// δfstr/vδfstr convert δf/vδf to string taking symbolic at into account.
func (t *T) δfstr(δf *ΔFile) string {
	s := fmt.Sprintf("@%s·%s", t.AtSymb(δf.Rev), δf.Blocks)
	if δf.Epoch {
		s += "E"
	}
	if δf.Size {
		s += "S"
	}
	return s
}
func (t *T) vδfstr(vδf []*ΔFile) string {
	var s []string
	for _, δf := range vδf {
		s = append(s, t.δfstr(δf))
	}
	return fmt.Sprintf("%s", s)
}


// dataTabTxt returns string representation of {} dataTab.
func dataTabTxt(dataTab map[string]string) string {
	// XXX dup wrt xbtreetest.KVTxt but uses string instead of Key for keys.
	if len(dataTab) == 0 {
		return "ø"
	}

	keyv := []string{}
	for k := range dataTab { keyv = append(keyv, k) }
	sort.Strings(keyv)

	sv := []string{}
	for _, k := range keyv {
		v := dataTab[k]
		if strings.ContainsAny(v, " \n\t,:") {
			panicf("[%v]=%q: invalid value", k, v)
		}
		sv = append(sv, fmt.Sprintf("%v:%s", k, v))
	}

	return strings.Join(sv, ",")
}


// b is shorthand to create setI64(blocks).
func b(blocks ...int64) setI64 {
	s := setI64{}
	for _, blk := range blocks {
		s.Add(blk)
	}
	return s
}
