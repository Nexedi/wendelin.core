// Copyright (C) 2020-2021  Nexedi SA and Contributors.
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

package xbtreetest
// treegen.go provides functionality:
//
//   - to commit a particular BTree topology into ZODB, and
//   - to generate set of random tree topologies that all correspond to particular {k->v} dict.
//
// treegen.py is used as helper for both tasks.

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"lab.nexedi.com/kirr/go123/my"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"
)


// TreeGenSrv represents connection to running `treegen ...` server.
type TreeGenSrv struct {
	argv      []string
	pysrv     *exec.Cmd      // spawned `treegen ...`
	pyin      io.WriteCloser // input to pysrv
	pyoutRaw  io.ReadCloser  // output from pysrv
	pyout     *bufio.Reader  // buffered ^^^
}

// TreeSrv represents connection to running `treegen trees` server.
//
// Create it with StartTreeSrv(zurl).
// - Commit(treeTopology) -> tid
type TreeSrv struct {
	*TreeGenSrv
	zurl	  string
	treeRoot  zodb.Oid       // oid of the tree treegen works on
	head      zodb.Tid       // last made commit
}

// AllStructsSrv represents connection to running `treegen allstructs` server.
//
// Create it with StartAllStructsSrv().
// - AllStructs(maxdepth, maxsplit, n, seed, kv1, kv2)
type AllStructsSrv struct {
	*TreeGenSrv
}

// startTreeGenSrv spawns `treegen ...` server.
func startTreeGenSrv(argv ...string) (_ *TreeGenSrv, hello string, err error) {
	defer xerr.Contextf(&err, "treesrv %v: start", argv)

	// spawn `treegen ...`
	tg := &TreeGenSrv{argv: argv}
	tg.pysrv = exec.Command(filepath.Dir(my.File())+"/treegen.py", argv...)
	tg.pyin, err = tg.pysrv.StdinPipe()
	if err != nil {
		return nil, "", err
	}
	tg.pyoutRaw, err = tg.pysrv.StdoutPipe()
	if err != nil {
		return nil, "", err
	}
	tg.pyout = bufio.NewReader(tg.pyoutRaw)
	tg.pysrv.Stderr = os.Stderr // no redirection

	err = tg.pysrv.Start()
	if err != nil {
		return nil, "", err
	}

	// wait for hello message and return it
	defer func() {
		if err != nil {
			tg.Close() // ignore error
		}
	}()
	defer xerr.Context(&err, "handshake")
	hello, err = tg.pyout.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, "", err
	}
	hello = strings.TrimSuffix(hello, "\n")

	return tg, hello, nil
}

// Close shutdowns treegen server.
func (tg *TreeGenSrv) Close() (err error) {
	defer xerr.Contextf(&err, "treegen %v: close", tg.argv)

	err1 := tg.pyin.Close()
	err2 := tg.pyoutRaw.Close()
	err3 := tg.pysrv.Wait()
	return xerr.Merge(err1, err2, err3)
}


// StartTreeSrv spawns `treegen trees` server.
func StartTreeSrv(zurl string) (_ *TreeSrv, err error) {
	defer xerr.Contextf(&err, "tree.srv %s: start", zurl)
	tgSrv, hello, err := startTreeGenSrv("trees", zurl)
	if err != nil {
		return nil, err
	}

	tg := &TreeSrv{TreeGenSrv: tgSrv, zurl: zurl}
	defer func() {
		if err != nil {
			tgSrv.Close() // ignore error
		}
	}()

	// tree.srv start @<at> tree=<root>
	defer xerr.Contextf(&err, "invalid hello %q", hello)
	startRe := regexp.MustCompile(`^tree.srv start @([^ ]+) root=([^ ]+)$`)
	m := startRe.FindStringSubmatch(hello)
	if m == nil {
		return nil, fmt.Errorf("unexpected format")
	}
	tg.head, err = zodb.ParseTid(m[1]) // <at>
	if err != nil {
		return nil, fmt.Errorf("tid: %s", err)
	}
	tg.treeRoot, err = zodb.ParseOid(m[2]) // <root>
	if err != nil {
		return nil, fmt.Errorf("root: %s", err)
	}

	return tg, nil
}

// StartAllStructsSrv spawns `treegen allstructs` server.
func StartAllStructsSrv() (_ *AllStructsSrv, err error) {
	defer xerr.Context(&err, "allstructs.srv: start")

	tgSrv, hello, err := startTreeGenSrv("allstructs")
	if err != nil {
		return nil, err
	}

	sg := &AllStructsSrv{TreeGenSrv: tgSrv}
	defer func() {
		if err != nil {
			tgSrv.Close() // ignore error
		}
	}()

	defer xerr.Contextf(&err, "invalid hello %q", hello)
	if hello != "# allstructs.srv start" {
		return nil, fmt.Errorf("unexpected format")
	}

	return sg, nil
}


// Commit creates new commit with underlying tree changed to specified tree topology.
//
// The following topologies are treated specially:
//
//	- "ø" requests deletion of the tree, and
//	- "øf" requests deletion of ZBigFile, that is referencing the tree.
//	- "t<kv> D<data-kv>" changes both tree natively, and ZBlk data objects.
//
// see treegen.py for details.
func (tg *TreeSrv) Commit(tree string) (_ zodb.Tid, err error) {
	defer xerr.Contextf(&err, "tree.srv %s: commit %s", tg.zurl, tree)

	_, err = io.WriteString(tg.pyin, tree + "\n")
	if err != nil {
		return zodb.InvalidTid, err
	}

	reply, err := tg.pyout.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return zodb.InvalidTid, err
	}
	reply = strings.TrimSuffix(reply, "\n")

	tid, err := zodb.ParseTid(reply)
	if err != nil {
		return zodb.InvalidTid, fmt.Errorf("invalid reply: %s", err)
	}
	tg.head = tid
	return tid, nil
}

// AllStructs returns response from `treegen allstructs`
func (tg *AllStructsSrv) AllStructs(kv map[Key]string, maxdepth, maxsplit, n int, seed int64) (_ []string, err error) {
	req := fmt.Sprintf("%d %d %d/%d %s", maxdepth, maxsplit, n, seed, KVTxt(kv))
	defer xerr.Contextf(&err, "allstructs.srv: %s ", req)

	_, err = io.WriteString(tg.pyin, req + "\n")
	if err != nil {
		return nil, err
	}

	structv := []string{}
	for {
		reply, err := tg.pyout.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		reply = strings.TrimSuffix(reply, "\n")

		if reply == "# ----" {
			return structv, nil // end of response
		}
		if strings.HasPrefix(reply, "#") {
			continue // comment
		}

		structv = append(structv, reply)
	}
}
