package arithflow

import (
	"fmt"
	"math"
)

// Returns the sum of two uint64 values. If the addition overflows, returns math.MaxUint64 and a non-nil error.
func AddUint64(a, b uint64) (result uint64, err error) {
	c := a + b
	if c >= a {
		return c, nil
	} else {
		return math.MaxUint64, fmt.Errorf("AddUint64(%d, %d) overflows", a, b)
	}
}
