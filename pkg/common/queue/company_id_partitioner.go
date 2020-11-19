package queue

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type companyPartitioner struct {
	topic string
}

// NewCompanyPartitioner returns a Partitioner which ensures equal
// distribution of Kentik companyIDs across our partition pool.
// "company_id" key must be set.
func NewCompanyPartitioner(topic string) sarama.Partitioner {
	if _partitionsPerTopic == nil {
		panic("Number of partitions per topic not configured - call InitializePartitionsPerTopic")
	}
	return &companyPartitioner{topic: topic}
}

// Partition by Kentik company ID, given that we start at 1001 and add 12 on each insert
// numPartitions is ignored
func (p *companyPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	companyID, found := GetUint32ProducerHeader("company_id", message)
	if !found {
		return -1, fmt.Errorf("Cannot partition - 'company_id' header not found")
	}

	return PartitionForCompanyID(companyID, message.Topic)
}

// RequiresConsistency indicates to the user of the partitioner whether the
// mapping of key->partition is consistent or not. Specifically, if a
// partitioner requires consistency then it must be allowed to choose from all
// partitions (even ones known to be unavailable), and its choice must be
// respected by the caller.
func (p *companyPartitioner) RequiresConsistency() bool {
	return true
}

// PartitionForCompanyID returns the partition number for a comapnyID,
// based on Kentik's mn_company.id sequence of:
// - starting point: 1001
// - incrementer: 12
func PartitionForCompanyID(companyID uint32, topic string) (int32, error) {
	partitionCount, found := _partitionsPerTopic[topic]
	if !found {
		return -1, fmt.Errorf("Cannot partition - number of partitions not configured for '%s'", topic)
	}
	if partitionCount <= 0 {
		return -1, fmt.Errorf("Cannot partition - number of partitions incorrectly configured to %d for '%s'", partitionCount, topic)
	}

	return PartitionForCompanyIDAndPartitionCount(companyID, partitionCount), nil
}

// PartitionForCompanyIDAndPartitionCount returns the partition number for a company with given partition count
func PartitionForCompanyIDAndPartitionCount(companyID uint32, partitionCount uint32) int32 {
	companyNumber := (int32(companyID) - int32(1001)) / int32(12)
	return int32(companyNumber % int32(partitionCount))
}
