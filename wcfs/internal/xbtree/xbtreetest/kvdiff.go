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
// kvdiff + friends

import (
	"fmt"
	"sort"
	"strings"
)

// KVDiff returns difference in between kv1 and kv2.
const DEL = "ø" // DEL means deletion
type Δstring struct {
	Old string
	New string
}
func KVDiff(kv1, kv2 map[Key]string) map[Key]Δstring {
	delta := map[Key]Δstring{}
	keys := setKey{}
	for k := range kv1 { keys.Add(k) }
	for k := range kv2 { keys.Add(k) }

	for k := range keys {
		v1, ok := kv1[k]
		if !ok { v1 = DEL }
		v2, ok := kv2[k]
		if !ok { v2 = DEL }
		if v1 != v2 {
			delta[k] = Δstring{v1,v2}
		}
	}

	return delta
}

// KVTxt returns string representation of {} kv.
func KVTxt(kv map[Key]string) string {
	if len(kv) == 0 {
		return "ø"
	}

	keyv := []Key{}
	for k := range kv { keyv = append(keyv, k) }
	sort.Slice(keyv, func(i,j int) bool { return keyv[i] < keyv[j] })

	sv := []string{}
	for _, k := range keyv {
		v := kv[k]
		if strings.ContainsAny(v, " \n\t,:") {
			panicf("[%v]=%q: invalid value", k, v)
		}
		sv = append(sv, fmt.Sprintf("%v:%s", k, v))
	}

	return strings.Join(sv, ",")
}
