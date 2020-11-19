package device

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Consumer reads cache device change events from Kafka
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
		logPrefix:         fmt.Sprintf("%s(device consumer) ", logPrefix),
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
		case "device-upsert":
			device := Device{}
			if err := device.Unmarshal(sourceMessage.Value); err != nil {
				c.log.Errorf(c.logPrefix, "Error unmarshalling 'device-upsert' message at offset %d - skipping message: %s", sourceMessage.Offset, err)

				// skip the invalid message
				continue
			}

			destChan <- Message{
				ConsumerMessage: queue.ConsumerMessage{
					Partition: sourceMessage.Partition,
					Offset:    sourceMessage.Offset,
					Timestamp: sourceMessage.Timestamp,
				},
				Device: device,
				Action: DeviceUpsertAction,
			}

		case "device-delete":
			var deviceID uint32
			var found bool
			if deviceID, found = queue.GetUint32ConsumerHeader("device_id", &sourceMessage); !found {
				c.log.Warnf(c.logPrefix, "Skipping 'device-delete' message at offset %d - missing 'device_id' header", sourceMessage.Offset)
				continue
			}
			destChan <- Message{
				ConsumerMessage: queue.ConsumerMessage{
					Partition: sourceMessage.Partition,
					Offset:    sourceMessage.Offset,
					Timestamp: sourceMessage.Timestamp,
				},
				Device: Device{
					CompanyID: companyID,
					ID:        deviceID,
				},
				Action: DeviceDeleteAction,
			}

		case "plan-delete":
			var planID uint32
			var found bool
			if planID, found = queue.GetUint32ConsumerHeader("plan_id", &sourceMessage); !found {
				c.log.Warnf(c.logPrefix, "Skipping 'plan-delete' message at offset %d - missing 'plan_id' header", sourceMessage.Offset)
				continue
			}
			destChan <- Message{
				ConsumerMessage: queue.ConsumerMessage{
					Partition: sourceMessage.Partition,
					Offset:    sourceMessage.Offset,
					Timestamp: sourceMessage.Timestamp,
				},
				Device: Device{
					CompanyID: companyID,
					Plan: Plan{
						ID: planID,
					},
				},
				Action: PlanDeleteAction,
			}

		case "site-upsert":
			site := Site{}
			if err := site.Unmarshal(sourceMessage.Value); err != nil {
				c.log.Errorf(c.logPrefix, "Error unmarshalling 'site-upsert' message at offset %d - skipping message: %s", sourceMessage.Offset, err)

				// skip the invalid message
				continue
			}
			destChan <- Message{
				ConsumerMessage: queue.ConsumerMessage{
					Partition: sourceMessage.Partition,
					Offset:    sourceMessage.Offset,
					Timestamp: sourceMessage.Timestamp,
				},
				Device: Device{
					CompanyID: companyID,
					Site:      site,
				},
				Action: SiteUpsertAction,
			}

		case "site-delete":
			var siteID uint32
			var found bool
			if siteID, found = queue.GetUint32ConsumerHeader("site_id", &sourceMessage); !found {
				c.log.Warnf(c.logPrefix, "Skipping 'site-delete' message at offset %d - missing 'site_id' header", sourceMessage.Offset)
				continue
			}
			destChan <- Message{
				ConsumerMessage: queue.ConsumerMessage{
					Partition: sourceMessage.Partition,
					Offset:    sourceMessage.Offset,
					Timestamp: sourceMessage.Timestamp,
				},
				Device: Device{
					CompanyID: companyID,
					Site: Site{
						ID: siteID,
					},
				},
				Action: SiteDeleteAction,
			}

		default:
			c.log.Infof(c.logPrefix, "Skipping message at offset %d - invalid 'action' header '%s'", sourceMessage.Offset, action)
		}

		// success
	}
	close(destChan)
}
