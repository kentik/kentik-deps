package response

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Consumer reads request flows from Kafka
type Consumer struct {
	log                    logging.Logger
	logPrefix              string
	multiPartitionConsumer *queue.MultiPartitionConsumer
}

// NewConsumer returns a new consumer - returns only new messages
func NewConsumer(log logging.Logger, logPrefix string, brokerList []string) (*Consumer, error) {
	multiPartitionConsumer, err := queue.NewMultiPartitionConsumer(log, logPrefix, Topic, brokerList, queue.RelativeStartingOffsetNewest, nil)
	if err != nil {
		return nil, fmt.Errorf("Error building MultiPartitionConsumer for %s: %s", Topic, err)
	}

	return &Consumer{
		log:                    log,
		logPrefix:              fmt.Sprintf("%s(interface-changed consumer) ", logPrefix),
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
	for sourceMessage := range sourceChan {
		// found a message - deserialize it
		flowResponse := FlowResponse{}
		if err := flowResponse.Unmarshal(sourceMessage.Value); err != nil {
			log.Errorf(logPrefix, "Error unmarshalling message from '%s'/0, offset %d to FlowResponse - skipping message: %s", Topic, sourceMessage.Offset, err)
			continue // skip invalid message
		}

		// deserialize Flow
		flow, err := deserializeFlow(flowResponse.SerializedFlow)
		if err != nil {
			log.Errorf(logPrefix, "Error deserializing flow: %s", err)
			continue // skip invalid message
		}

		// success
		destChan <- Message{
			ConsumerMessage: queue.ConsumerMessage{
				Partition: 0,
				Offset:    sourceMessage.Offset,
				Timestamp: sourceMessage.Timestamp,
			},
			CompanyID:          flowResponse.CompanyID,
			DeviceID:           flowResponse.DeviceID,
			ShardNumber:        flowResponse.ShardNumber,
			Host:               flowResponse.Host,
			ScenarioName:       flowResponse.ScenarioName,
			Flow:               flow,
			FieldID:            flowResponse.FieldID,
			ExpectedFieldValue: flowResponse.ExpectedFieldValue,
		}
	}
	close(destChan)
}
