// Copyright (C) 2021  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package blib

import (
	"strings"
	"testing"

	"lab.nexedi.com/kirr/neo/go/zodb"
)

func TestPPTreeSubSetOps(t *testing.T) {
	const (
		a zodb.Oid = 0xa + iota
		b
		c
		d
		ø = zodb.InvalidOid
	)
	type S = PPTreeSubSet
	type testEntry struct {
		A, B       S
		Union      S
		Difference S
	}
	E := func(A, B, U, D S) testEntry {
		return testEntry{A, B, U, D}
	}

	testv := []testEntry{
		E(
			S{},	// A
			S{},	// B
				S{},	// U
				S{}),	// D

		E(
			S{a:{ø,0}},	// A
			S{a:{ø,0}},	// B
				S{a:{ø,0}},	// U
				S{}),		// D

		E(
			S{a:{ø,0}},	// A
			S{b:{ø,0}},	// B
				S{a:{ø,0}, b:{ø,0}},	// U
				S{a:{ø,0}}),		// D

		E(
			S{a:{ø,1}, b:{a,0}},	// A
			S{a:{ø,1}, c:{a,0}},	// B
				S{a:{ø,2}, b:{a,0}, c:{a,0}},	// U
				S{a:{ø,1}, b:{a,0}}),		// D

		E(
			S{a:{ø,1}, b:{a,1}, c:{b,0}},	// A
			S{a:{ø,1}, b:{a,1}, d:{b,0}},	// B
				S{a:{ø,1}, b:{a,2}, c:{b,0}, d:{b,0}},	// U
				S{a:{ø,1}, b:{a,1}, c:{b,0}}),		// D

		E(
			S{a:{ø,1}, b:{a,0}},	// A
			S{a:{ø,1}, b:{a,0}},	// B
				S{a:{ø,1}, b:{a,0}},	// U
				S{}),			// D

		E(
			S{a:{ø,1}, b:{a,1}, c:{b,0}},	// A
			S{a:{ø,1}, b:{a,1}, c:{b,0}},	// B (=A)
				S{a:{ø,1}, b:{a,1}, c:{b,0}},	// U (=A)
				S{}),				// D
	}

	// assert1 asserts that result of op(A,B) == resOK.
	assert1 := func(op string, A, B, res, resOK S) {
		t.Helper()
		if res.Equal(resOK) {
			return
		}
		op1 := op[0:1]
		t.Errorf("%s:\n  A:   %s\n  B:   %s\n  ->%s: %s\n  ok%s: %s\n",
			strings.Title(op), A, B, op1, res, strings.ToUpper(op1), resOK)
	}

	for _, tt := range testv {
		Uab := tt.A.Union(tt.B)
		Uba := tt.B.Union(tt.A)
		Dab := tt.A.Difference(tt.B)

		assert1("union",      tt.A, tt.B, Uab, tt.Union)
		assert1("union",      tt.B, tt.A, Uba, tt.Union)
		assert1("difference", tt.A, tt.B, Dab, tt.Difference)

		Uaa := tt.A.Union(tt.A)
		Ubb := tt.B.Union(tt.B)
		Daa := tt.A.Difference(tt.A)
		Dbb := tt.B.Difference(tt.B)

		assert1("union",      tt.A, tt.A, Uaa, tt.A)
		assert1("union",      tt.B, tt.B, Ubb, tt.B)
		assert1("difference", tt.A, tt.A, Daa, S{})
		assert1("difference", tt.B, tt.B, Dbb, S{})

		// TODO also verify U/D properties like (A+B)\B + (A+B)\A + (A^B) == (A+B) ?
	}
}
