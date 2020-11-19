package arithflow

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddUint64(t *testing.T) {
	assert := assert.New(t)

	// no overflow
	result, err := AddUint64(100, 200)
	assert.Equal(uint64(300), result)
	assert.NoError(err)

	// no overflow
	result, err = AddUint64(math.MaxUint64-200, 199)
	assert.Equal(uint64(math.MaxUint64-1), result)
	assert.NoError(err)

	// no overflow
	result, err = AddUint64(math.MaxUint64-200, 200)
	assert.Equal(uint64(math.MaxUint64), result)
	assert.NoError(err)

	// overflows by one
	result, err = AddUint64(math.MaxUint64-200, 201)
	assert.Equal(uint64(math.MaxUint64), result)
	assert.Error(err)
}

func BenchmarkAddUint64(b *testing.B) {
	x := uint64(math.MaxUint64 / 2)
	y := uint64(math.MaxUint64 / 4)
	for i := 0; i < b.N; i++ {
		AddUint64(x, y) // nolint: errcheck
	}
}
