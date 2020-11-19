package queue

import (
	"github.com/Shopify/sarama"
)

// PartitionProcessor processes messages for a partition that its been assigned to by a Consumer
type PartitionProcessor interface {
	// ProcessMessage processes the input message, returning true if we should continue, else we shut down
	ProcessMessage(message sarama.ConsumerMessage) bool

	// Shutdown is called when the processor is finished handling messages for this partition
	Shutdown()
}

// NewPartitionProcessorFunc generates PartitionProcessor
type NewPartitionProcessorFunc func(partition int32) PartitionProcessor
