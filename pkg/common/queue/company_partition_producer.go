package queue

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
)

// CompanyPartitionProducer is a strongly consistent message producer that lets caller pick the partition to write to.
type CompanyPartitionProducer struct {
	log       logging.Logger
	logPrefix string
	producer  *PartitionProducer
}

// NewCompanyPartitionProducer returns a new Producer that allows us to choose which partition to submit to
func NewCompanyPartitionProducer(log logging.Logger, logPrefix string, brokerList []string, saramaConfigModFunc SaramaConfigModFunc) (*CompanyPartitionProducer, error) {
	producer, err := NewPartitionProducer(log, logPrefix, brokerList, NewCompanyPartitioner, saramaConfigModFunc)
	if err != nil {
		return nil, fmt.Errorf("Error creating partition producer for CompanyPartitionProducer: %s", err)
	}

	return &CompanyPartitionProducer{
		log:       log,
		logPrefix: logPrefix,
		producer:  producer,
	}, nil
}

// Close shuts down the producer, waiting on pending messages to finish first
func (p *CompanyPartitionProducer) Close() error {
	return p.producer.Close()
}

// SendMessage sends a Kafka message
// returns:
// - partition
// - offset
// - error
func (p *CompanyPartitionProducer) SendMessage(message *sarama.ProducerMessage) (int32, int64, error) {
	return p.producer.SendMessage(message)
}

// SendMessages produces a given set of messages, and returns only when all
// messages in the set have either succeeded or failed. Note that messages
// can succeed and fail individually; if some succeed and some fail,
// SendMessages will return an error.
func (p *CompanyPartitionProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return p.producer.SendMessages(msgs)
}
