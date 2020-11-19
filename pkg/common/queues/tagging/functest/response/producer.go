package response

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

// SendFlowResponses sends a processed flow into Kafka
func (p *Producer) SendFlowResponses(responseBatch *Batch) error {
	messages := responseBatch.Messages()

	if err := p.producer.SendMessages(messages); err != nil {
		return fmt.Errorf("Error sending message to '%s': %s", Topic, err)
	}

	p.log.Debugf(p.logPrefix, "Sent %d messages", len(messages))
	return nil
}

// Close shuts down the producer
func (p *Producer) Close() {
	p.log.Infof(p.logPrefix, "Shutting down producer for '%s'", Topic)
	p.producer.Close()
}
