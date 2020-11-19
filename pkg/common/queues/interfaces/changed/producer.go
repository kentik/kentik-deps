package changed

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Producer sends interface change events into Kafka
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
		logPrefix: fmt.Sprintf("%s(interface-changed producer) ", logPrefix),
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

// SendInterfaces sends all the input device interfaces - just logs validation errors, skipping those records
// - on error, some messages might have been successfully delivered
func (p *Producer) SendInterfaces(deviceInterfaces []Interface) error {
	messages := make([]*sarama.ProducerMessage, 0, len(deviceInterfaces))

	for _, deviceInterface := range deviceInterfaces {
		if deviceInterface.CompanyID == 0 {
			p.log.Infof(p.logPrefix, "Skipping sending interface with companyID=0")
			continue
		}
		if deviceInterface.DeviceID == 0 {
			p.log.Infof(p.logPrefix, "Skipping sending interface with deviceID=0")
			continue
		}

		deviceBytes, err := deviceInterface.Marshal()
		if err != nil {
			return fmt.Errorf("Error marshalling device interface: %s", err)
		}

		// if this is a history record, we include start date in the key. If it's current, we use "current" in the key
		var key string
		if deviceInterface.EndTimeUnixNano > 0 {
			// history record
			key = fmt.Sprintf("%d-%d-%d-%d", deviceInterface.CompanyID, deviceInterface.DeviceID, deviceInterface.SNMPID, deviceInterface.StartTimeUnixNano)
		} else {
			// current
			key = fmt.Sprintf("%d-%d-%d-current", deviceInterface.CompanyID, deviceInterface.DeviceID, deviceInterface.SNMPID)
		}
		message := &sarama.ProducerMessage{
			Topic: Topic,
			// Key is used for key/value compaction
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(deviceBytes),
		}
		queue.AddUintHeader("company_id", deviceInterface.CompanyID, message)
		queue.AddUintHeader("device_id", deviceInterface.DeviceID, message)
		queue.AddUintHeader("snmp_id", deviceInterface.SNMPID, message)

		messages = append(messages, message)
	}

	if err := p.producer.SendMessages(messages); err != nil {
		return fmt.Errorf("Error submitting a batch of %d device interfaces to Kafka topic '%s': %s", len(deviceInterfaces), Topic, err)
	}
	return nil
}

// SendInterfacesDeleted sends an interface deletion message to Kafka
// - deletes the current value of these interfaces - does not create history message
// - logs and skips validation problems
// - on error, some messages might have been successfully delivered
func (p *Producer) SendInterfacesDeleted(deviceInterfaces []Interface) error {
	messages := make([]*sarama.ProducerMessage, 0, len(deviceInterfaces))
	for _, deviceInterface := range deviceInterfaces {
		if deviceInterface.CompanyID == 0 {
			p.log.Infof(p.logPrefix, "Skipping sending interface delete with companyID=0")
			continue
		}
		if deviceInterface.DeviceID == 0 {
			p.log.Infof(p.logPrefix, "Skipping sending interface delete with deviceID=0")
			continue
		}

		message := &sarama.ProducerMessage{
			Topic: Topic,
			// Key is used for key/value compaction
			Key:   sarama.StringEncoder(fmt.Sprintf("%d-%d-%d-current", deviceInterface.CompanyID, deviceInterface.DeviceID, deviceInterface.SNMPID)),
			Value: nil,
		}
		queue.AddUintHeader("company_id", deviceInterface.CompanyID, message)
		queue.AddUintHeader("device_id", deviceInterface.DeviceID, message)
		queue.AddUintHeader("snmp_id", deviceInterface.SNMPID, message)

		messages = append(messages, message)
	}

	if err := p.producer.SendMessages(messages); err != nil {
		return fmt.Errorf("Error submiting %d delete messages to Topic '%s': %s", len(deviceInterfaces), Topic, err)
	}
	return nil
}
