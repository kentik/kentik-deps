package health

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Producer sends device health fact updates into Kafka
type Producer struct {
	log       logging.Logger
	logPrefix string
	producer  *queue.CompanyPartitionProducer
}

// NewProducer returns a new device health fact event producer
func NewProducer(log logging.Logger, logPrefix string, kafkaBrokerList []string) (*Producer, error) {
	producer, err := queue.NewCompanyPartitionProducer(log, logPrefix, kafkaBrokerList, nil)
	if err != nil {
		return nil, fmt.Errorf("Error building CompanyPartitionProducer: %s", err)
	}

	return &Producer{
		log:       log,
		logPrefix: fmt.Sprintf("%s(health_fact producer) ", logPrefix),
		producer:  producer,
	}, nil
}

// Close shuts down the processor, waiting for pending messages to stop
func (p *Producer) Close() error {
	p.log.Infof(p.logPrefix, "Shutting down")
	start := time.Now()
	if err := p.producer.Close(); err != nil {
		p.log.Errorf(p.logPrefix, "Error shutting down queue.Producer: %s", err)
	}
	p.log.Infof(p.logPrefix, "Shut down in %s", time.Since(start))
	return nil
}

// SendDeviceHealth sends an update containing some health facts about a device and its interfaces.
func (p *Producer) SendHealth(deviceHealth DeviceHealth) error {

	healthBytes, err := deviceHealth.Marshal()
	if err != nil {
		return fmt.Errorf("Error marshalling device health: %s", err)
	}

	message := &sarama.ProducerMessage{
		Topic: Topic,
		// Key is used for key/value compaction
		Key:   sarama.StringEncoder(fmt.Sprintf("%d-%d-%d", deviceHealth.CompanyID, deviceHealth.DeviceID, deviceHealth.TimeUnixNano)),
		Value: sarama.ByteEncoder(healthBytes),
	}
	queue.AddUintHeader("company_id", deviceHealth.CompanyID, message)
	queue.AddUintHeader("device_id", deviceHealth.DeviceID, message)

	if _, _, err := p.producer.SendMessage(message); err != nil {
		return fmt.Errorf("Error submitting a device health update to Kafka topic '%s': %s", Topic, err)
	}
	return nil
}
