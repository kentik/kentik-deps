package bloom

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/willf/bloom"
)

func TestWriteSecondaryIndexAddr(t *testing.T) {
	fs := &mapFS{m: testFiles()}
	createFile, openFile, existsFile = fs.Create, fs.Open, fs.Exists

	err := WriteSecondaryIndexAddr("/dir/1", "INET_SRC_ADDR")
	assert.NoError(t, err)
	fmt.Printf("fs: %+v\n", fs)
}

func testFiles() map[string][]byte {
	return map[string][]byte{
		"/dir/1/INET_SRC_ADDR": []byte{
			0x04, 0x7f, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0x04, 0x7f, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0x04, 0x7f, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0x04, 0x7f, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0x04, 0x7f, 0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		},
	}
}

func TestWriteRead(t *testing.T) {
	fs := &mapFS{m: testFiles()}
	createFile, openFile, existsFile = fs.Create, fs.Open, fs.Exists

	err := WriteSecondaryIndexAddr("/dir/1", "INET_SRC_ADDR")
	assert.NoError(t, err)

	dataDirs := []string{"/dir/"}
	partitions := []string{"1"}
	assert.False(t, parseKQLAndEvalBloomFilters("inet_src_addr = '127.0.0.3'", dataDirs, partitions))
	assert.True(t, parseKQLAndEvalBloomFilters("inet_src_addr = '127.0.0.1'", dataDirs, partitions))
}

func TestBFSize(t *testing.T) {
	// Bloom filters are linear in the size of their input.
	// This test demonstrates the exact size requirements.
	// 1 flow => 32 bytes
	// 1K flows => 1.2KB
	// 1M flows => 1.2MB

	assert := assert.New(t)

	var bf *bloom.BloomFilter
	var err error
	var bs *bytes.Buffer

	bf, err = getBloomFilterAddr([][addrLen]byte{{4, 127, 0, 0, 1}})
	assert.NoError(err)
	bs = &bytes.Buffer{}
	bf.WriteTo(bs)
	assert.Equal(32, bs.Len(), "a bloom filter with n=1 fits in 32 bytes")

	bf, err = getBloomFilterAddr(makeAddrValues(1024))
	assert.NoError(err)
	bs = &bytes.Buffer{}
	bf.WriteTo(bs)
	assert.Equal(1256, bs.Len(), "a bloom filter with n=2**10 fits in 1256 bytes")

	bf, err = getBloomFilterAddr(makeAddrValues(1024 * 1024))
	assert.NoError(err)
	bs = &bytes.Buffer{}
	bf.WriteTo(bs)
	assert.Equal(1256360, bs.Len(), "a bloom filter with n=2**20 fits in 1256360 bytes")
}

func makeAddrValues(n int) [][addrLen]byte {
	vs := make([][addrLen]byte, n)
	for i := 0; i < n; i++ {
		vs[i] = [addrLen]byte{4, byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
	}
	return vs
}
