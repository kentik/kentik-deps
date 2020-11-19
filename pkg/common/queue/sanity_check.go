package queue

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
)

// VerifyEnvironment makes sure everything is as we expect it with the environment
func VerifyEnvironment(log logging.Logger, logPrefix string, kafkaBrokerList []string) error {
	// connect to Kafka
	config := sarama.NewConfig()
	config.ClientID = "kentik.environment-verifier"
	client, err := sarama.NewClient(kafkaBrokerList, config)
	if err != nil {
		return fmt.Errorf("Error instantiating Sarama client: %s", err)
	}
	defer client.Close()

	// check registered partition counts
	for topic, expectedPartitionCount := range PartitionsPerTopic() {
		foundPartitions, err := client.Partitions(topic)
		if err != nil {
			return fmt.Errorf("Error checking partition count for topic '%s': %s", topic, err)
		}

		// check the count
		if uint32(len(foundPartitions)) != expectedPartitionCount {
			return fmt.Errorf("Expected %d partitions for topic '%s', found %d", expectedPartitionCount, topic, len(foundPartitions))
		}

		// make sure each partition number between 0-(count-1) exists - the collection is sorted
		for i := 0; i < len(foundPartitions); i++ {
			if foundPartitions[i] != int32(i) {
				return fmt.Errorf("topic %s: partition number at offset %d should be %d, but is %d", topic, i, i, foundPartitions[i])
			}
		}

		log.Infof(logPrefix, "Kafka check valid: topic '%s' has %d partitions, 0-%d", topic, expectedPartitionCount, expectedPartitionCount-1)
	}

	return nil
}
