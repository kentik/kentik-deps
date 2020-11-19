package kflowfield

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Producer sends KFlowField change events into Kafka
type Producer struct {
	log       logging.Logger
	logPrefix string
	producer  *queue.PartitionProducer
}

// NewProducer returns a new kflow field change event producer
func NewProducer(log logging.Logger, logPrefix string, kafkaBrokerList []string) (*Producer, error) {
	producer, err := queue.NewPartitionProducer(log, logPrefix, kafkaBrokerList, sarama.NewRandomPartitioner, nil)
	if err != nil {
		return nil, fmt.Errorf("Error building CompanyPartitionProducer: %s", err)
	}

	return &Producer{
		log:       log,
		logPrefix: fmt.Sprintf("%s(kflowfield producer) ", logPrefix),
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

// SendKFlowField sends a kflow field to Kafka
// - returns false if the data has a validation problem - error should be used for logging in that case
// - return true if data passes validation - error describes what's most likely a temporary error in that case
func (p *Producer) SendKFlowField(kflowField KFlowField) (bool, error) {
	if kflowField.CompanyID == 0 {
		return false, fmt.Errorf("KFlowField.CompanyID is 0")
	}
	if kflowField.ID == 0 {
		return false, fmt.Errorf("KFlowField.ID is 0")
	}

	fieldBytes, err := kflowField.Marshal()
	if err != nil {
		return true, fmt.Errorf("Error marshalling KFlowField: %s", err)
	}

	message := &sarama.ProducerMessage{
		Topic: Topic,
		// Key is used for key/value compaction
		Key:   sarama.StringEncoder(fmt.Sprintf("kflowfield-%d-%d", kflowField.CompanyID, kflowField.ID)),
		Value: sarama.ByteEncoder(fieldBytes),
	}
	queue.AddStringHeader("action", "kflowfield-upsert", message)
	queue.AddUintHeader("company_id", kflowField.CompanyID, message)
	queue.AddUintHeader("kflowfiled_id", kflowField.ID, message)

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return true, fmt.Errorf("Error submiting kflowfield upsert message to '%s': %s", Topic, err)
	}
	p.log.Debugf(p.logPrefix, "Sent kflowfield upsert message - company_id: %d; kflowfield_id: %d; partition: %d; offset: %d; len(headers): %d; len(value): %d",
		kflowField.CompanyID, kflowField.ID, partition, offset, len(message.Headers), len(fieldBytes))

	return true, nil
}

// SendKFlowFieldDeleted sends a message that a kflowField has been deleted
// - returns false if the data has a validation problem - error should be used for logging in that case
// - return true if data passes validation - error describes what's most likely a temporary error in that case
func (p *Producer) SendKFlowFieldDeleted(companyID uint32, kflowFieldID uint32) (bool, error) {
	if companyID == 0 {
		return false, fmt.Errorf("companyID is 0")
	}
	if kflowFieldID == 0 {
		return false, fmt.Errorf("kflowField is 0")
	}

	message := &sarama.ProducerMessage{
		Topic: Topic,
		// Key is used for key/value compaction - must have same value for kflowfield upsert
		Key:   sarama.StringEncoder(fmt.Sprintf("kflowfield-%d-%d", companyID, kflowFieldID)),
		Value: nil,
	}
	queue.AddStringHeader("action", "kflowfield-delete", message)
	queue.AddUintHeader("company_id", companyID, message)
	queue.AddUintHeader("kflowfield_id", kflowFieldID, message)

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return true, fmt.Errorf("Error submiting kflowfield delete message to '%s': %s", Topic, err)
	}
	p.log.Debugf(p.logPrefix, "Sent kflowfield delete message - company_id: %d; kflowfield_id: %d; partition: %d; offset: %d; len(headers): %d; len(value): 0",
		companyID, kflowFieldID, partition, offset, len(message.Headers))

	return true, nil
}
