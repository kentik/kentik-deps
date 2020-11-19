package sample

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
	"github.com/kentik/common/queues/topology/cloud/traffic"
)

// Consumer reads request flows from Kafka
type Consumer struct {
	log                    logging.Logger
	logPrefix              string
	multiPartitionConsumer *queue.MultiPartitionConsumer
}

// NewConsumer returns a new consumer - returns only new messages
func NewConsumer(log logging.Logger, logPrefix string, brokerList []string, hasReachedCurrentFunc func()) (*Consumer, error) {
	multiPartitionConsumer, err := queue.NewMultiPartitionConsumer(log, logPrefix, Topic, brokerList, queue.RelativeStartingOffsetOldest, hasReachedCurrentFunc)
	if err != nil {
		return nil, fmt.Errorf("Error building MultiPartitionConsumer for %s: %s", Topic, err)
	}

	return &Consumer{
		log:                    log,
		logPrefix:              fmt.Sprintf("%s(cloud sample consumer) ", logPrefix),
		multiPartitionConsumer: multiPartitionConsumer,
	}, nil
}

// Close shuts down the consumer
func (c *Consumer) Close() error {
	c.multiPartitionConsumer.Close()
	return nil
}

// Messages returns the message channel
// - if no more messages, it's because the client was closed
func (c *Consumer) Messages() (chan Message, error) {
	if err := c.multiPartitionConsumer.Consume(); err != nil {
		return nil, fmt.Errorf("Error consuming MultiPartitionConsumer: %s", err)
	}

	outChan := make(chan Message, 1000)
	go readMessages(c.log, c.logPrefix, c.multiPartitionConsumer.Messages(), outChan)
	return outChan, nil
}

// interpret the sarama messages as our typed Message
func readMessages(log logging.Logger, logPrefix string, sourceChan <-chan sarama.ConsumerMessage, destChan chan Message) {
	cleanupTicker := time.NewTicker(15 * time.Minute)
	defer cleanupTicker.Stop()

	deduper := NewDeduper()

	for {
		select {
		case <-cleanupTicker.C:
			cacheSize := deduper.Cleanup()
			log.Infof(logPrefix, "After cleaning, deduper cache size now has %d entries", cacheSize)

		case sourceMessage, ok := <-sourceChan:
			if !ok {
				// channel was closed
				log.Infof(logPrefix, "source message channel was closed")
				close(destChan)
				return
			}

			// found a message - deserialize it
			trafficSample := traffic.CompanyCloudTrafficSample{}
			if err := trafficSample.Unmarshal(sourceMessage.Value); err != nil {
				log.Errorf(logPrefix, "Error unmarshalling message from '%s'/0, offset %d to CloudSample - skipping message: %s", Topic, sourceMessage.Offset, err)
				continue // skip invalid message
			}

			// determine if this is likely a duplicate
			if deduper.LikelySeenMessage(trafficSample.CompanyID, trafficSample.RandInt, sourceMessage.Timestamp) {
				log.Infof(logPrefix, "Skipping likely duplicate message for companyID %d", trafficSample.CompanyID)
				continue
			}

			// determine deviceID and shardNumber from headers
			deviceID, _ := queue.GetUint32ConsumerHeader("device_id", &sourceMessage)
			shardNumber, _ := queue.GetUint32ConsumerHeader("shard_number", &sourceMessage)

			// success
			destChan <- Message{
				ConsumerMessage: queue.ConsumerMessage{
					Partition: sourceMessage.Partition,
					Offset:    sourceMessage.Offset,
					Timestamp: sourceMessage.Timestamp,
				},
				DeviceID:                  deviceID,
				ShardNumber:               shardNumber,
				CompanyCloudTrafficSample: trafficSample,
			}
		}
	}
}
