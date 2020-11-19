package request

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Consumer reads request flows from Kafka
type Consumer struct {
	queue.PartitionConsumer // TODO: put this in a struct variable
	log                     logging.Logger
	logPrefix               string
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
		logPrefix:         logPrefix,
	}, nil
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

// interpret the sarama messages as our typed Message
func (c *Consumer) readMessages(sourceChan <-chan sarama.ConsumerMessage, destChan chan Message) {
	for sourceMessage := range sourceChan {
		// found a message - deserialize it
		flowRequest := FlowRequest{}
		if err := flowRequest.Unmarshal(sourceMessage.Value); err != nil {
			c.log.Errorf(c.logPrefix, "Error unmarshalling message from '%s'/0, offset %d to FlowRequest - skipping message: %s", Topic, sourceMessage.Offset, err)
			continue // skip the invalid message
		}

		flow, err := deserializeFlow(flowRequest.SerializedFlow)
		if err != nil {
			c.log.Errorf(c.logPrefix, "Error deserializing flow: %s", err)
			continue // skip the invalid message
		}

		// success
		destChan <- Message{
			ConsumerMessage: queue.ConsumerMessage{
				Partition: 0,
				Offset:    sourceMessage.Offset,
				Timestamp: sourceMessage.Timestamp,
			},
			CompanyID:          flowRequest.CompanyID,
			DeviceID:           flowRequest.DeviceID,
			ShardNumber:        flowRequest.ShardNumber,
			Host:               flowRequest.Host,
			ScenarioName:       flowRequest.ScenarioName,
			Flow:               flow,
			FieldID:            flowRequest.FieldID,
			ExpectedFieldValue: flowRequest.ExpectedFieldValue,
		}
	}
	close(destChan)
}
