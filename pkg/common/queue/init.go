package queue

import (
	"github.com/Shopify/sarama"
	"strings"
)

// the number of partitions per topic
var _partitionsPerTopic map[string]uint32

func init() {
	// Server with default configuration will reject message batches over 1MB.
	// A batch of small messages can trigger this, so we need to make sure
	// to keep the batches under the limit
	sarama.MaxRequestSize = 950000
}

// InitializePartitionsPerTopic initializes the number of partitions per topic.
// This can't change for an environment, as it'll lose messages that we expect to find.
func InitializePartitionsPerTopic(partitionsPerTopic map[string]uint32) {
	_partitionsPerTopic = make(map[string]uint32)

	for topic, partitions := range partitionsPerTopic {
		_partitionsPerTopic[strings.ToLower(topic)] = partitions
	}

	_partitionsPerTopic = partitionsPerTopic
}

// PartitionCount gets the number of partitions for the input topic
func PartitionCount(topic string) uint32 {
	return _partitionsPerTopic[strings.ToLower(topic)]
}

// PartitionsPerTopic returns the map of partitionName -> expected count
func PartitionsPerTopic() map[string]uint32 {
	ret := make(map[string]uint32)
	for k, v := range _partitionsPerTopic {
		ret[k] = v
	}
	return ret
}
