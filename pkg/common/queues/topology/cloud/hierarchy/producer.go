package hierarchy

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
	"github.com/kentik/common/queues/topology/cloud"
)

// Producer sends cloud sample events into Kafka
type Producer struct {
	log         logging.Logger
	logPrefix   string
	producer    *queue.CompanyPartitionProducer
	companyID   uint32
	deviceID    uint32
	shardNumber uint32
	cloudType   cloud.CloudType
}

// NewProducer returns a new cloud sample event producer
func NewProducer(log logging.Logger, logPrefix string, kafkaBrokerList []string, companyID uint32, cloudType cloud.CloudType, deviceID uint32, shardNumber uint32) (*Producer, error) {
	producer, err := queue.NewCompanyPartitionProducer(log, logPrefix, kafkaBrokerList, nil)
	if err != nil {
		return nil, fmt.Errorf("Error building CompanyPartitionProducer: %s", err)
	}

	return &Producer{
		log:         log,
		logPrefix:   fmt.Sprintf("%s(cloudsample producer) ", logPrefix),
		producer:    producer,
		companyID:   companyID,
		deviceID:    deviceID,
		shardNumber: shardNumber,
		cloudType:   cloudType,
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

// SendCloudHierarchies sends hierarchy samples seen in flow
// - passed in as a map that caller kept to unique the samples
func (p *Producer) SendCloudHierarchies(hierarchies []cloud.CloudHierarchy) error {
	totalPayload := 0
	messages := make([]*sarama.ProducerMessage, 0, len(hierarchies))
	for _, hierarchy := range hierarchies {
		if hierarchy.Region == 0 && hierarchy.RegionStr == "" &&
			hierarchy.VPC == 0 && hierarchy.VPCStr == "" &&
			hierarchy.Subnet == 0 && hierarchy.SubnetStr == "" &&
			hierarchy.VMInstance == 0 && hierarchy.VMInstanceStr == "" {
			continue
		}

		msgHierarchy := CloudHierarchy{
			CompanyID: p.companyID,
			CloudType: p.cloudType,
			Hierarchy: hierarchy,
		}

		sampleBytes, err := msgHierarchy.Marshal()
		if err != nil {
			return fmt.Errorf("Error marshalling CloudSample: %s", err)
		}

		msg := &sarama.ProducerMessage{
			Topic: Topic,
			Value: sarama.ByteEncoder(sampleBytes),
		}

		// This topic has an expiration, but we also use key to clean up duplicates.
		cloudName := ""

		switch p.cloudType {
		case cloud.AWS:
			cloudName = "aws"
		case cloud.Azure:
			cloudName = "azure"
		case cloud.GCP:
			cloudName = "gcp"
		case cloud.IBM:
			cloudName = "ibm"
		default:
			// not handled yet
			continue
		}

		msg.Key = sarama.StringEncoder(fmt.Sprintf("cid:%d;cloud:%s;region:%d;vpc:%d;subnet:%d;vm:%d",
			p.companyID, cloudName, hierarchy.Region, hierarchy.VPC, hierarchy.Subnet, hierarchy.VMInstance))

		queue.AddUintHeader("company_id", p.companyID, msg)
		messages = append(messages, msg)

		totalPayload += len(sampleBytes)
	}

	// send the messages in bulk
	start := time.Now()
	if err := p.producer.SendMessages(messages); err != nil {
		return fmt.Errorf("Error submitting a batch of %d CloudTopologySamples to Kafka topic '%s': %s", len(messages), Topic, err)
	}
	p.log.Infof(p.logPrefix, "Sent %d CloudTopologySamples, total size %d bytes to Kafka in %s", len(messages), totalPayload, time.Since(start))

	return nil
}
