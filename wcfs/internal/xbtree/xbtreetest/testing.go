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
// testing-related support

import (
	"flag"
	"math/rand"
	"testing"
	"time"
)

var (
	verylongFlag = flag.Bool("verylong", false, `switch tests to run in "very long" mode`)
	randseedFlag = flag.Int64("randseed", -1, `seed for random number generator`)
)

// VeryLong returns whether -verylong flag is in effect.
func VeryLong() bool {
	// -short takes priority over -verylong
	if testing.Short() {
		return false
	}
	return *verylongFlag
}

// N returns short, medium, or long depending on whether tests were ran with
// -short, -verylong, or  normally.
func N(short, medium, long int) int {
	// -short
	if testing.Short() {
		return short
	}
	// -verylong
	if *verylongFlag {
		return long
	}
	// default
	return medium
}

// NewRand returns new random-number generator and seed that was used to initialize it.
//
// The seed can be controlled via -randseed option.
func NewRand() (rng *rand.Rand, seed int64) {
	seed = *randseedFlag
	if seed == -1 {
		seed = time.Now().UnixNano()
	}
	rng = rand.New(rand.NewSource(seed))
	return rng, seed
}
