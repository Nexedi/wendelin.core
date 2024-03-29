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

import (
	"reflect"
	"testing"
)

func TestKVDiff(t *testing.T) {
	kv1 := map[Key]string{1:"a", 3:"c", 4:"d"}
	kv2 := map[Key]string{1:"b",        4:"d", 5:"e"}
	got  := KVDiff(kv1, kv2)
	want := map[Key]Δstring{1:{"a","b"}, 3:{"c",DEL}, 5:{DEL,"e"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("error:\ngot:  %v\nwant: %v", got, want)
	}
}

func TestKVTxt(t *testing.T) {
	kv := map[Key]string{3:"hello", 1:"zzz", 4:"world"}
	got  := KVTxt(kv)
	want := "1:zzz,3:hello,4:world"
	if got != want {
		t.Fatalf("error:\ngot:  %q\nwant: %q", got, want)
	}
}
