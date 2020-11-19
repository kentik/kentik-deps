package device

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
)

// Producer sends device change events into Kafka
type Producer struct {
	log       logging.Logger
	logPrefix string
	producer  *queue.PartitionProducer
}

// NewProducer returns a new device change event producer
func NewProducer(log logging.Logger, logPrefix string, kafkaBrokerList []string) (*Producer, error) {
	producer, err := queue.NewPartitionProducer(log, logPrefix, kafkaBrokerList, sarama.NewRandomPartitioner, nil)
	if err != nil {
		return nil, fmt.Errorf("Error building CompanyPartitionProducer: %s", err)
	}

	return &Producer{
		log:       log,
		logPrefix: fmt.Sprintf("%s(device producer) ", logPrefix),
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

// SendDevice sends a device to Kafka
// - returns false if the data has a validation problem - error should be used for logging in that case
// - return true if data passes validation - error describes what's most likely a temporary error in that case
func (p *Producer) SendDevice(device Device) (bool, error) {
	if device.CompanyID == 0 {
		return false, fmt.Errorf("Device.CompanyID is 0")
	}
	if device.ID == 0 {
		return false, fmt.Errorf("Device.ID is 0")
	}

	deviceBytes, err := device.Marshal()
	if err != nil {
		return true, fmt.Errorf("Error marshalling device: %s", err)
	}

	message := &sarama.ProducerMessage{
		Topic: Topic,
		// Key is used for key/value compaction
		Key:   sarama.StringEncoder(fmt.Sprintf("device-%d-%d", device.CompanyID, device.ID)),
		Value: sarama.ByteEncoder(deviceBytes),
	}
	queue.AddStringHeader("action", "device-upsert", message)
	queue.AddUintHeader("company_id", device.CompanyID, message)
	queue.AddUintHeader("device_id", device.ID, message)

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return true, fmt.Errorf("Error submiting device upsert message to '%s': %s", Topic, err)
	}
	p.log.Debugf(p.logPrefix, "Sent device upsert message - company_id: %d; device_id: %d; partition: %d; offset: %d; len(headers): %d; len(value): %d",
		device.CompanyID, device.ID, partition, offset, len(message.Headers), len(deviceBytes))

	return true, nil
}

// SendDeviceDeleted sends a message that a device has been deleted
// - returns false if the data has a validation problem - error should be used for logging in that case
// - return true if data passes validation - error describes what's most likely a temporary error in that case
func (p *Producer) SendDeviceDeleted(companyID uint32, deviceID uint32) (bool, error) {
	if companyID == 0 {
		return false, fmt.Errorf("companyID is 0")
	}
	if deviceID == 0 {
		return false, fmt.Errorf("deviceID is 0")
	}

	message := &sarama.ProducerMessage{
		Topic: Topic,
		// Key is used for key/value compaction - must have same value for device upsert
		Key:   sarama.StringEncoder(fmt.Sprintf("device-%d-%d", companyID, deviceID)),
		Value: nil,
	}
	queue.AddStringHeader("action", "device-delete", message)
	queue.AddUintHeader("company_id", companyID, message)
	queue.AddUintHeader("device_id", deviceID, message)

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return true, fmt.Errorf("Error submiting device delete message to '%s': %s", Topic, err)
	}
	p.log.Debugf(p.logPrefix, "Sent device delete message - company_id: %d; device_id: %d; partition: %d; offset: %d; len(headers): %d; len(value): 0",
		companyID, deviceID, partition, offset, len(message.Headers))

	return true, nil
}

// SendPlanDeleted sends a message that a plan has been deleted
// - returns false if the data has a validation problem - error should be used for logging in that case
// - return true if data passes validation - error describes what's most likely a temporary error in that case
func (p *Producer) SendPlanDeleted(companyID uint32, planID uint32) (bool, error) {
	if companyID == 0 {
		return false, fmt.Errorf("companyID is 0")
	}
	if planID == 0 {
		return false, fmt.Errorf("planID is 0")
	}

	message := &sarama.ProducerMessage{
		Topic: Topic,
		// Key is used for key/value compaction - must have same value for site upsert
		Key:   sarama.StringEncoder(fmt.Sprintf("plan-%d-%d", companyID, planID)),
		Value: nil,
	}
	queue.AddStringHeader("action", "plan-delete", message)
	queue.AddUintHeader("company_id", companyID, message)
	queue.AddUintHeader("plan_id", planID, message)

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return true, fmt.Errorf("Error submiting plan delete message to '%s': %s", Topic, err)
	}
	p.log.Debugf(p.logPrefix, "Sent plan delete message - company_id: %d; plan_id: %d; partition: %d; offset: %d; len(headers): %d; len(value): 0",
		companyID, planID, partition, offset, len(message.Headers))

	return true, nil
}

// SendSiteDeleted sends a message that a site has been deleted
// - returns false if the data has a validation problem - error should be used for logging in that case
// - return true if data passes validation - error describes what's most likely a temporary error in that case
func (p *Producer) SendSiteDeleted(companyID uint32, siteID uint32) (bool, error) {
	if companyID == 0 {
		return false, fmt.Errorf("companyID is 0")
	}
	if siteID == 0 {
		return false, fmt.Errorf("siteID is 0")
	}

	message := &sarama.ProducerMessage{
		Topic: Topic,
		// Key is used for key/value compaction - must have same value for site update
		Key:   sarama.StringEncoder(fmt.Sprintf("site-%d-%d", companyID, siteID)),
		Value: nil,
	}
	queue.AddStringHeader("action", "site-delete", message)
	queue.AddUintHeader("company_id", companyID, message)
	queue.AddUintHeader("site_id", siteID, message)

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return true, fmt.Errorf("Error submiting site delete message to '%s': %s", Topic, err)
	}
	p.log.Debugf(p.logPrefix, "Sent site delete message - company_id: %d; site_id: %d; partition: %d; offset: %d; len(headers): %d; len(value): 0",
		companyID, siteID, partition, offset, len(message.Headers))

	return true, nil
}

// SendSiteUpdated sends a message that a site has been updated
// - returns false if the data has a validation problem - error should be used for logging in that case
// - return true if data passes validation - error describes what's most likely a temporary error in that case
func (p *Producer) SendSiteUpdated(companyID uint32, site Site) (bool, error) {
	if companyID == 0 {
		return false, fmt.Errorf("companyID is 0")
	}

	siteBytes, err := site.Marshal()
	if err != nil {
		return true, fmt.Errorf("Error marshalling site: %s", err)
	}

	message := &sarama.ProducerMessage{
		Topic: Topic,
		// Key is used for key/value compaction - must have same value for site delete
		Key:   sarama.StringEncoder(fmt.Sprintf("site-%d-%d", companyID, site.ID)),
		Value: sarama.ByteEncoder(siteBytes),
	}
	queue.AddStringHeader("action", "site-upsert", message)
	queue.AddUintHeader("company_id", companyID, message)
	queue.AddUintHeader("site_id", site.ID, message)

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return true, fmt.Errorf("Error submiting site update message to '%s': %s", Topic, err)
	}
	p.log.Debugf(p.logPrefix, "Sent site update message - company_id: %d; site_id: %d; partition: %d; offset: %d; len(headers): %d; len(value): 0",
		companyID, site.ID, partition, offset, len(message.Headers))

	return true, nil
}
