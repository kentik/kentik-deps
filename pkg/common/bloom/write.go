package bloom

import (
	"errors"
	"math"
	"path/filepath"

	"github.com/willf/bloom"
)

func WriteSecondaryIndexAddr(directory, columnFilename string) error {
	fastbitFilename := filepath.Join(directory, columnFilename)
	bfFilename := fastbitFilename + ".bf"

	values, err := ReadDataAddr(fastbitFilename)
	if err != nil {
		return err
	}
	return writeSecondaryIndexAddrValues(bfFilename, values)
}

func writeSecondaryIndexAddrValues(filename string, values [][addrLen]byte) error {
	bf, err := getBloomFilterAddr(values)
	if err != nil {
		return err
	}
	return writeBloomFilter(bf, filename)
}

func getBloomFilterAddr(values [][addrLen]byte) (*bloom.BloomFilter, error) {
	// Create a new bloom filter assuming there may be as many as len(values)
	// distinct values. This will use a bit more space than necessary,
	// but is simple and we avoid deduping here.
	bf := newOptimalFilter(len(values))

	for i := range values {
		// Hashing the value's fastbit binary representation here.
		// Lookup code must test this same binary representation.
		bf.Add(values[i][0:17])
	}

	return bf, nil
}

var errUnknownAddrFamily = errors.New("unknown addr family")

// We're using github.com/willf/bloom, which is backed by murmur3.
func newOptimalFilter(n int) *bloom.BloomFilter {
	m := optimalM(bloomP, uint(n))
	k := optimalK(bloomP)
	return bloom.New(m, k)
}

// bloomP is our desired false positive rate for our bloom filter.
// We can pick this value however we like. Lower false positive rates
// require more space.
const bloomP = 0.01

// Calculate optimal values for m and k where optimal means smallest size
// while preserving the desired false positive rate.
// See the standard equations for this on https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
// optimal m/n = -(log_2(p)/ln(2))
// optimal k = -log_2(p)
func optimalMN(p float64) float64     { return -1 * (math.Log2(p) / math.Ln2) }
func optimalM(p float64, n uint) uint { return uint(math.Ceil(optimalMN(p) * float64(n))) }
func optimalK(p float64) uint         { return uint(-1 * math.Ceil(math.Log2(p))) }

// writeBloomFilter writes the bloom filter to disk, in this library's
// binary format.
// It looks like "m (uint64), k (uint64), l (uint64), bits ([]byte)".
func writeBloomFilter(bf *bloom.BloomFilter, filename string) error {
	file, err := createFile(filename)
	if err != nil {
		return err
	}
	_, err = bf.WriteTo(file)
	if err != nil {
		file.Close()
		return err
	}
	return file.Close()
}
