package kflowfield

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Consumer reads cache kflow change events from Kafka
type Consumer struct {
	queue.PartitionConsumer
	log       logging.Logger
	logPrefix string
}

// NewConsumer returns a new consumer
func NewConsumer(log logging.Logger, logPrefix string, brokerList []string) (*Consumer, error) {
	// there's only one partition
	partitionConsumer, err := queue.NewPartitionConsumer(log, logPrefix, 0, Topic, brokerList, nil)
	if err != nil {
		return nil, fmt.Errorf("Error creating partition consumer for topic '%s', partition 0: %s", Topic, err)
	}

	return &Consumer{
		PartitionConsumer: partitionConsumer,
		log:               log,
		logPrefix:         fmt.Sprintf("%s(kflowfield consumer) ", logPrefix),
	}, nil
}

// Consume consumes all messages, starting with the oldest offset
// - no reason to consume from a particular offset, since this is a single partition, bounded, compacted topic.
//   The intent is that the caller will cache everything in memory.
func (c *Consumer) Consume() {
	c.PartitionConsumer.Consume(sarama.OffsetOldest)
}

// Messages returns the message channel
// - if no more messages, it's because the client was closed
func (c *Consumer) Messages() (chan Message, error) {
	sourceChan, err := c.PartitionConsumer.Messages()
	if err != nil {
		return nil, fmt.Errorf("Error fetching message channel: %s", err)
	}

	// need to interpret, filter, convert messages
	destChan := make(chan Message)
	go c.readMessages(sourceChan, destChan)
	return destChan, nil
}

// PastMessages returns a message channel that closes when we're caught up
func (c *Consumer) PastMessages() (chan Message, error) {
	sourceChan, err := c.PartitionConsumer.PastMessages()
	if err != nil {
		return nil, fmt.Errorf("Error fetching message channel: %s", err)
	}

	// need to interpret, filter, convert messages
	destChan := make(chan Message)
	go c.readMessages(sourceChan, destChan)
	return destChan, nil
}

// interpret the sarama messages as our typed Message
func (c *Consumer) readMessages(sourceChan <-chan sarama.ConsumerMessage, destChan chan Message) {
	for sourceMessage := range sourceChan {
		companyID, _ := queue.GetUint32ConsumerHeader("company_id", &sourceMessage)
		if companyID == 0 {
			c.log.Infof(c.logPrefix, "Skipping message at offset %d - missing 'company_id' header", sourceMessage.Offset)
			continue
		}

		action, _ := queue.GetStringConsumerHeader("action", &sourceMessage)
		switch action {
		case "kflowfield-upsert":
			field := KFlowField{}
			if err := field.Unmarshal(sourceMessage.Value); err != nil {
				c.log.Errorf(c.logPrefix, "Error unmarshalling 'kflowfield-upsert' message at offset %d - skipping message: %s", sourceMessage.Offset, err)

				// skip the invalid message
				continue
			}

			destChan <- Message{
				ConsumerMessage: queue.ConsumerMessage{
					Partition: sourceMessage.Partition,
					Offset:    sourceMessage.Offset,
					Timestamp: sourceMessage.Timestamp,
				},
				Field:  field,
				Action: KFlowFieldUpsertAction,
			}

		case "kflowfield-delete":
			var fieldID uint32
			var found bool
			if fieldID, found = queue.GetUint32ConsumerHeader("kflowfield_id", &sourceMessage); !found {
				c.log.Warnf(c.logPrefix, "Skipping 'kflowfield-delete' message at offset %d - missing 'kflowfield_id' header", sourceMessage.Offset)
				continue
			}
			destChan <- Message{
				ConsumerMessage: queue.ConsumerMessage{
					Partition: sourceMessage.Partition,
					Offset:    sourceMessage.Offset,
					Timestamp: sourceMessage.Timestamp,
				},
				Field: KFlowField{
					CompanyID: companyID,
					ID:        fieldID,
				},
				Action: KFlowFieldDeleteAction,
			}
		default:
			c.log.Infof(c.logPrefix, "Skipping message at offset %d - invalid 'action' header '%s'", sourceMessage.Offset, action)
		}

		// success
	}
	close(destChan)
}
