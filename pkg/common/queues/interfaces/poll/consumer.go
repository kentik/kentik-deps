package poll

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Consumer reads interface SNMP poll events from Kafka, published by chfclients
type Consumer struct {
	log                    logging.Logger
	logPrefix              string
	multiPartitionConsumer *queue.MultiPartitionConsumer
}

// NewConsumer returns a new consumer
func NewConsumer(log logging.Logger, logPrefix string, brokerList []string, hasReachedCurrentFunc func()) (*Consumer, error) {
	multiPartitionConsumer, err := queue.NewMultiPartitionConsumer(log, logPrefix, Topic, brokerList, queue.RelativeStartingOffsetOldest, hasReachedCurrentFunc)
	if err != nil {
		return nil, fmt.Errorf("Error building MultiPartitionConsumer for %s: %s", Topic, err)
	}

	return &Consumer{
		log:                    log,
		logPrefix:              fmt.Sprintf("%s(interface-poll consumer) ", logPrefix),
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
	go readMessages(c.log, c.logPrefix, c.multiPartitionConsumer.Messages(), outChan, 0, 0)
	return outChan, nil
}

// PastMessagesForCompanyID returns a message channel for a companyID that closes when we're caught up
// - returns a closeFunc() - make sure to call it when done
func PastMessagesForCompanyID(log logging.Logger, logPrefix string, companyID uint32, deviceID uint32, brokerList []string) (chan Message, func(), error) {
	partition := queue.PartitionForCompanyIDAndPartitionCount(companyID, PartitionCount)
	partitionConsumer, err := queue.NewPartitionConsumer(log, logPrefix, partition, Topic, brokerList, nil)
	if err != nil {
		return nil, func() {}, fmt.Errorf("Error creating partition consumer for topic '%s', partition %d: %s", Topic, partition, err)
	}
	go partitionConsumer.Consume(sarama.OffsetOldest)
	sourceChan, err := partitionConsumer.PastMessages()
	if err != nil {
		return nil, func() {}, fmt.Errorf("Error fetching message channel: %s", err)
	}

	// need to interpret, filter, convert messages
	destChan := make(chan Message)
	go readMessages(log, logPrefix, sourceChan, destChan, companyID, deviceID)
	return destChan, partitionConsumer.Close, nil
}

// interpret the sarama messages as our typed Message
func readMessages(log logging.Logger, logPrefix string, sourceChan <-chan sarama.ConsumerMessage, destChan chan Message, companyIDFilter uint32, deviceIDFilter uint32) {
	for sourceMessage := range sourceChan {
		if companyIDFilter > 0 && deviceIDFilter > 0 {
			// we're filtering by companyID
			if foundCompanyID, found := queue.GetUint32ConsumerHeader("company_id", &sourceMessage); !found || foundCompanyID != companyIDFilter {
				continue
			}
			if foundDeviceID, found := queue.GetUint32ConsumerHeader("device_id", &sourceMessage); !found || foundDeviceID != deviceIDFilter {
				continue
			}
		}

		if len(sourceMessage.Value) == 0 {
			// shouldn't happen - we don't do handle deletes in this topic
			continue
		}

		interfacePoll := InterfacePoll{}
		if err := interfacePoll.Unmarshal(sourceMessage.Value); err != nil {
			log.Errorf(logPrefix, "Error unmarshalling message from '%s'/%d, offset %d to InterfacePoll - skipping message: %s",
				Topic, sourceMessage.Partition, sourceMessage.Offset, err)

			// skip the invalid message
			continue
		}

		// success
		destChan <- Message{
			ConsumerMessage: queue.ConsumerMessage{
				Partition: sourceMessage.Partition,
				Offset:    sourceMessage.Offset,
				Timestamp: sourceMessage.Timestamp,
			},
			InterfacePoll: interfacePoll,
		}
	}
	close(destChan)
}
