package queue

import (
	"time"
)

// ConsumerMessage encapsulates a Kafka message returned by the consumer.
type ConsumerMessage struct {
	Partition int32
	Offset    int64
	Timestamp time.Time
}
