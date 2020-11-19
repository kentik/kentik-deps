package health

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Consumer reads health fact events from Kafka
type Consumer struct {
	log                    logging.Logger
	logPrefix              string
	multiPartitionConsumer *queue.MultiPartitionConsumer
}

// NewConsumer returns a new consumer that starts with the oldest messages
func NewConsumer(log logging.Logger, logPrefix string, brokerList []string, hasReachedCurrentFunc func()) (*Consumer, error) {
	multiPartitionConsumer, err := queue.NewMultiPartitionConsumer(log, logPrefix, Topic, brokerList, queue.RelativeStartingOffsetOldest, hasReachedCurrentFunc)
	if err != nil {
		return nil, fmt.Errorf("Error building MultiPartitionConsumer for %s: %s", Topic, err)
	}

	return &Consumer{
		log:                    log,
		logPrefix:              fmt.Sprintf("%s(health_fact consumer) ", logPrefix),
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
func (c *Consumer) Messages() (chan *DeviceHealth, error) {
	if err := c.multiPartitionConsumer.Consume(); err != nil {
		return nil, fmt.Errorf("Error consuming MultiPartitionConsumer: %s", err)
	}

	outChan := make(chan *DeviceHealth, 1000)
	go readMessages(c.log, c.logPrefix, c.multiPartitionConsumer.Messages(), outChan, 0)
	return outChan, nil
}

// interpret the sarama messages as DeviceHealth
func readMessages(log logging.Logger, logPrefix string, sourceChan <-chan sarama.ConsumerMessage, destChan chan *DeviceHealth, companyIDFilter uint32) {
	for sourceMessage := range sourceChan {
		// found a message - deserialize it

		if companyIDFilter > 0 {
			// we're filtering by companyID
			foundCompanyID, found := queue.GetUint32ConsumerHeader("company_id", &sourceMessage)
			if !found {
				continue // shouldn't happen
			}
			if foundCompanyID != companyIDFilter {
				// we're filtering on companyID and this isn't one we're looking for
				continue
			}
		}

		deviceHealth := DeviceHealth{}
		// insert/update
		if err := deviceHealth.Unmarshal(sourceMessage.Value); err != nil {
			log.Errorf(logPrefix, "Error unmarshalling message from '%s'/%d, offset %d to DeviceHealth - skipping message: %s",
				Topic, sourceMessage.Partition, sourceMessage.Offset, err)

			// skip the invalid message
			continue
		}

		// success
		destChan <- &deviceHealth
	}
	close(destChan)
}
