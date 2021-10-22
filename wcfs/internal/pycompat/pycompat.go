// XXX dup from neo/go/zodb/internal/pickletools

package pycompat

import (
	"math/big"
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
