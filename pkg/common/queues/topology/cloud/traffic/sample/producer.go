package sample

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/queue"
	"github.com/kentik/common/queues/topology/cloud"
	"github.com/kentik/common/queues/topology/cloud/traffic"
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
		cloudType:   cloudType,
		deviceID:    deviceID,
		shardNumber: shardNumber,
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

// SendTrafficSamples sends the traffic samplings of region-to-region, vpc-to-vpc, and subnet-to-subnet
// - localLookups is used for clouds that deal with strings rather than uint32 populator lookups
func (p *Producer) SendTrafficSamples(startTime time.Time, endTime time.Time, regionToRegion cloud.EntityLookupPairingSample, vpcToVPC cloud.EntityLookupPairingSample, subnetToSubnet cloud.EntityLookupPairingSample, localLookups map[uint32]string) error {
	maxPayload := 0
	totalPayload := 0

	// break messages down to make sure they don't get too big - each sourc e
	messages := make([]*sarama.ProducerMessage, 0)

	addSample := func(sample cloud.CloudTrafficSample) error {
		trafficSample := traffic.CompanyCloudTrafficSample{
			CompanyID:     p.companyID,
			CloudType:     p.cloudType,
			TrafficSample: sample,
			RandInt:       rand.Uint64(), // used to help caller determine duplicate messages
		}

		if cloud.EntityFlowStorageMethodForCloud(p.cloudType) == cloud.StringUDRColumns {
			// string-based cloud - need to include local lookups
			msgLocalLookups := make(map[uint32]string)
			if strVal := localLookups[sample.SourceEntity]; strVal != "" {
				msgLocalLookups[sample.SourceEntity] = strVal
			}
			for lookupID := range sample.ByteCounts {
				if strVal := localLookups[lookupID]; strVal != "" {
					msgLocalLookups[lookupID] = strVal
				}
			}
			trafficSample.TrafficSample.LocalLookups = msgLocalLookups
		}

		sampleBytes, err := trafficSample.Marshal()
		if err != nil {
			return fmt.Errorf("Error marshalling CloudSample: %s", err)
		}

		msg := &sarama.ProducerMessage{
			Topic: Topic,
			Value: sarama.ByteEncoder(sampleBytes),
		}
		queue.AddUintHeader("company_id", p.companyID, msg)
		queue.AddUintHeader("device_id", p.deviceID, msg)
		queue.AddUintHeader("shard_number", p.shardNumber, msg)
		messages = append(messages, msg)

		if len(sampleBytes) > maxPayload {
			maxPayload = len(sampleBytes)
		}
		totalPayload += len(sampleBytes)

		return nil
	}

	// regions
	for sourceRegion := range regionToRegion {
		err := addSample(cloud.CloudTrafficSample{
			WindowStartUnixSeconds: startTime.Unix(),
			WindowEndUnixSeconds:   endTime.Unix(),
			CloudEntityType:        cloud.Region,
			SourceEntity:           sourceRegion,
			ByteCounts:             regionToRegion[sourceRegion],
		})
		if err != nil {
			return err
		}
	}

	// VPCs
	for sourceVPC := range vpcToVPC {
		err := addSample(cloud.CloudTrafficSample{
			WindowStartUnixSeconds: startTime.Unix(),
			WindowEndUnixSeconds:   endTime.Unix(),
			CloudEntityType:        cloud.VPC,
			SourceEntity:           sourceVPC,
			ByteCounts:             vpcToVPC[sourceVPC],
		})
		if err != nil {
			return err
		}
	}

	// subnets
	for sourceSubnet := range subnetToSubnet {
		err := addSample(cloud.CloudTrafficSample{
			WindowStartUnixSeconds: startTime.Unix(),
			WindowEndUnixSeconds:   endTime.Unix(),
			CloudEntityType:        cloud.Subnet,
			SourceEntity:           sourceSubnet,
			ByteCounts:             subnetToSubnet[sourceSubnet],
		})
		if err != nil {
			return err
		}
	}

	// send the messages in bulk
	start := time.Now()
	if err := p.producer.SendMessages(messages); err != nil {
		return fmt.Errorf("Error submitting a batch of %d %s CloudTrafficSamples with max size %d bytes, total size %d bytes to Kafka topic '%s': %s", len(messages), cloud.CloudType_name[int32(p.cloudType)], maxPayload, totalPayload, Topic, err)
	}
	p.log.Infof(p.logPrefix, "Sent %d %s CloudTrafficSamples with max size %d bytes, total size %d bytes to Kafka in %s", len(messages), cloud.CloudType_name[int32(p.cloudType)], maxPayload, totalPayload, time.Since(start))

	return nil
}
