package poll

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Producer sends interface poll events into Kafka
type Producer struct {
	log       logging.Logger
	logPrefix string
	producer  *queue.CompanyPartitionProducer
}

// NewProducer returns a new interface change event producer
func NewProducer(log logging.Logger, logPrefix string, kafkaBrokerList []string) (*Producer, error) {
	producer, err := queue.NewCompanyPartitionProducer(log, logPrefix, kafkaBrokerList, nil)
	if err != nil {
		return nil, fmt.Errorf("Error building CompanyPartitionProducer: %s", err)
	}

	return &Producer{
		log:       log,
		logPrefix: fmt.Sprintf("%s(interface-poll producer) ", logPrefix),
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

// SendInterfacePolls sends a batch of interfaces as read from an SNMP poll.
// - just logs validation errors, skipping those records
// - on error, some messages might have been successfully delivered
func (p *Producer) SendInterfacePolls(interfacePolls []InterfacePoll) error {
	messages := make([]*sarama.ProducerMessage, 0, len(interfacePolls))
	for _, interfacePoll := range interfacePolls {
		if interfacePoll.CompanyID == 0 {
			p.log.Infof(p.logPrefix, "Skipping record with companyID=0")
			continue
		}
		if interfacePoll.DeviceID == 0 {
			p.log.Infof(p.logPrefix, "Skipping record with deviceID=0")
			continue
		}

		deviceBytes, err := interfacePoll.Marshal()
		if err != nil {
			return fmt.Errorf("Error marshalling device interface: %s", err)
		}

		// if this is a history record, we include start date in the key. If it's current, we use "current" in the key
		message := &sarama.ProducerMessage{
			Topic: Topic,
			Value: sarama.ByteEncoder(deviceBytes),
		}
		queue.AddUintHeader("company_id", interfacePoll.CompanyID, message)
		queue.AddUintHeader("device_id", interfacePoll.DeviceID, message)
		queue.AddUintHeader("snmp_id", interfacePoll.SNMPID, message)

		messages = append(messages, message)
	}

	if err := p.producer.SendMessages(messages); err != nil {
		return fmt.Errorf("Error submitting a batch of %d interface polls to Kafka topic '%s': %s", len(interfacePolls), Topic, err)
	}
	return nil
}
