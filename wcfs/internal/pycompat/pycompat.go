// XXX dup from neo/go/zodb/internal/pickletools

package pycompat

import (
	"fmt"
	"math/big"
	pickle "github.com/kisielk/og-rek"
)

// Int64 tries to convert unpickled Python value to int64.
//
// (ogórek decodes python long as big.Int)
//
// XXX + support for float?
func Int64(xv interface{}) (v int64, ok bool) {
  switch v := xv.(type) {
  case int64:
    return v, true
  case *big.Int:
    if v.IsInt64() {
      return v.Int64(), true
    }
  }

  return 0, false
}

// Xstrbytes verifies and extacts str|bytes from unpickled value.
func Xstrbytes(x interface{}) (string, error) {
	var s string
	switch x := x.(type) {
	default:
		return "", fmt.Errorf("expect str|bytes; got %T", x)

	case string:
		s = x

	case pickle.Bytes:
		s = string(x)
	}

	return s, nil
}