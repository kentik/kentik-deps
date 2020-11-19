package summary

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
	"github.com/kentik/common/queues/topology/cloud"
	"github.com/kentik/common/queues/topology/cloud/traffic"
)

// Producer sends cloud sample events into Kafka
type Producer struct {
	log       logging.Logger
	logPrefix string
	producer  *queue.CompanyPartitionProducer
}

// NewProducer returns a new cloud sample event producer
func NewProducer(log logging.Logger, logPrefix string, kafkaBrokerList []string) (*Producer, error) {
	producer, err := queue.NewCompanyPartitionProducer(log, logPrefix, kafkaBrokerList, nil)
	if err != nil {
		return nil, fmt.Errorf("Error building CompanyPartitionProducer: %s", err)
	}

	return &Producer{
		log:       log,
		logPrefix: fmt.Sprintf("%s(cloudsummary producer) ", logPrefix),
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

// SendTrafficSummaries sends summaries out for a company/cloud
// Note: these are sent into a long-lived/permanent key/value Kafka topic
func (p *Producer) SendTrafficSummaries(companyID uint32, cloudType cloud.CloudType, samples []cloud.CloudTrafficSample) error {
	messages := make([]*sarama.ProducerMessage, 0, len(samples))

	// build the messages
	maxPayload := 0
	totalPayload := 0
	for _, sample := range samples {
		key, err := BuildTrafficMessageKey(companyID, cloudType, sample.CloudEntityType, sample.SourceEntity, sample.WindowStartUnixSeconds, sample.WindowEndUnixSeconds)
		if err != nil {
			return fmt.Errorf("Could not build message key: %s", err)
		}

		companyCloudTrafficSample := traffic.CompanyCloudTrafficSample{
			CompanyID:     companyID,
			CloudType:     cloudType,
			TrafficSample: sample,
		}

		sampleBytes, err := companyCloudTrafficSample.Marshal()
		if err != nil {
			return fmt.Errorf("Error marshalling CloudSample: %s", err)
		}
		if len(sampleBytes) > maxPayload {
			maxPayload = len(sampleBytes)
		}
		totalPayload += len(sampleBytes)

		msg := &sarama.ProducerMessage{
			Topic: Topic,
			Value: sarama.ByteEncoder(sampleBytes),
			Key:   sarama.StringEncoder(key),
		}
		queue.AddUintHeader("company_id", companyID, msg)
		messages = append(messages, msg)
	}

	// send the messages in bulk
	start := time.Now()
	if err := p.producer.SendMessages(messages); err != nil {
		return fmt.Errorf("Error submitting a batch of %d Cloud Traffic Summaries to Kafka topic '%s': %s", len(messages), Topic, err)
	}
	p.log.Infof(p.logPrefix, "Sent %d cloud traffic summaries with max size %d bytes, total size %d bytes to Kafka in %s", len(messages), maxPayload, totalPayload, time.Since(start))

	return nil
}
