package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartitionForCompanyID(t *testing.T) {
	// no matter how many partitions, first company always on 0
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(1001, 1))
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(1001, 2))
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(1001, 3))

	// only have one partition - always partition #0
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(1002, 1))
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(1003, 1))
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(1004, 1))

	// position #103 (zero-based) with different partition counts
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(2237, 1))
	assert.Equal(t, int32(1), PartitionForCompanyIDAndPartitionCount(2237, 2))
	assert.Equal(t, int32(1), PartitionForCompanyIDAndPartitionCount(2237, 3))
	assert.Equal(t, int32(3), PartitionForCompanyIDAndPartitionCount(2237, 4))
	assert.Equal(t, int32(3), PartitionForCompanyIDAndPartitionCount(2237, 5))

	// roll across 5 nodes
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(1001, 5))
	assert.Equal(t, int32(1), PartitionForCompanyIDAndPartitionCount(1013, 5))
	assert.Equal(t, int32(2), PartitionForCompanyIDAndPartitionCount(1025, 5))
	assert.Equal(t, int32(3), PartitionForCompanyIDAndPartitionCount(1037, 5))
	assert.Equal(t, int32(4), PartitionForCompanyIDAndPartitionCount(1049, 5))
	assert.Equal(t, int32(0), PartitionForCompanyIDAndPartitionCount(1061, 5))
	assert.Equal(t, int32(1), PartitionForCompanyIDAndPartitionCount(1073, 5))
	assert.Equal(t, int32(2), PartitionForCompanyIDAndPartitionCount(1085, 5))
	assert.Equal(t, int32(3), PartitionForCompanyIDAndPartitionCount(1097, 5))
}
