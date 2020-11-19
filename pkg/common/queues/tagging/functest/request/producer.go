package request

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Producer sends tag change events into Kafka
type Producer struct {
	log       logging.Logger
	logPrefix string
	producer  *queue.PartitionProducer
}

// NewProducer returns a new tag change event producer
func NewProducer(log logging.Logger, logPrefix string, kafkaBrokerList []string) (*Producer, error) {
	producer, err := queue.NewPartitionProducer(log, logPrefix, kafkaBrokerList, sarama.NewHashPartitioner, nil)
	if err != nil {
		return nil, fmt.Errorf("Could not create PartitionProducer: %s", err)
	}

	return &Producer{
		log:       log,
		logPrefix: logPrefix + fmt.Sprintf("[producer: %s] ", Topic),
		producer:  producer,
	}, nil
}

// SendFlowRequests sends flows to be processed by one or more devices
// - if companyID==0, will be handled by all companies
// - if deviceID==0, will be handled by all devices
// - if shardNumber==0, will be handled by all shards
// - if host=="", will be handled by all hosts
func (p *Producer) SendFlowRequests(batch *Batch) error {
	messages := batch.Messages()
	if err := p.producer.SendMessages(messages); err != nil {
		return fmt.Errorf("Error sending messages to '%s': %s", Topic, err)
	}

	p.log.Debugf(p.logPrefix, "Sent %d messages ", len(messages))

	return nil
}

// Close shuts down the producer
func (p *Producer) Close() {
	p.log.Infof(p.logPrefix, "Shutting down producer for '%s'", Topic)
	p.producer.Close()
}
