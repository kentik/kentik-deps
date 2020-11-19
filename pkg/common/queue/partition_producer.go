package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/utility"
)

// PartitionProducer is a strongly consistent message producer that lets caller pick the partition to write to.
type PartitionProducer struct {
	log       logging.Logger
	logPrefix string
	producer  sarama.SyncProducer
	lock      *sync.Mutex
	isClosed  bool
	busyWG    *sync.WaitGroup
}

// NewPartitionProducer returns a new Producer that allows us to choose which partition to submit to
func NewPartitionProducer(log logging.Logger, logPrefix string, brokerList []string, partitionerConstructor sarama.PartitionerConstructor, saramaConfigModFunc SaramaConfigModFunc) (*PartitionProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Partitioner = partitionerConstructor
	config.Producer.Return.Successes = true
	// max request size set to 950kb, server configured to reject messages over 1MB. Constrain message size to 900kb
	config.Producer.MaxMessageBytes = 900000
	config.Version = sarama.V1_1_0_0
	config.ClientID = "kentik.partition-producer"

	// backoff for retrying a metadata request when the cluster is in the middle of a leader election
	config.Metadata.Retry.Max = 10000 // effectively forever - this'll keep retrying for over a day
	config.Metadata.Retry.BackoffFunc = func(retries int, maxRetries int) time.Duration {
		dur := utility.RandDurationSeconds(10, 20)
		log.Infof(logPrefix, "Need to retry fetching Kafka metadata (attempt #%d): backing off for %s", retries, dur)
		return dur
	}

	if saramaConfigModFunc != nil {
		saramaConfigModFunc(config)
	}

	// TODO: ?
	// tlsConfig := createTlsConfiguration()
	//if tlsConfig != nil {
	//    config.Net.TLS.Config = tlsConfig
	//    config.Net.TLS.Enable = true
	//}

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("Failed to start Sarama producer: %s", err)
	}

	return &PartitionProducer{
		log:       log,
		logPrefix: logPrefix,
		producer:  producer,
		lock:      &sync.Mutex{},
		busyWG:    &sync.WaitGroup{},
	}, nil
}

// Close waits for pending messages to send, then shuts down the producer
func (p *PartitionProducer) Close() error {
	// mark that we're closing in lock
	p.lock.Lock()
	p.isClosed = true
	p.lock.Unlock()

	start := time.Now()
	p.log.Infof(p.logPrefix, "Closing partition producer - waiting for messages to finish sending")
	p.busyWG.Wait()
	p.log.Infof(p.logPrefix, "Closed partition processor in %s", time.Since(start))

	if err := p.producer.Close(); err != nil {
		return fmt.Errorf("Error stopping the producer: %s", err)
	}
	return nil
}

// SendMessage sends a Kafka message
// returns:
// - partition
// - offset
// - error
func (p *PartitionProducer) SendMessage(message *sarama.ProducerMessage) (int32, int64, error) {
	// make sure we're not shutting down
	p.lock.Lock()
	if p.isClosed {
		p.lock.Unlock()
		return 0, 0, fmt.Errorf("Can't send message - producer has already been closed")
	}

	// increment busy WG while in lock
	p.busyWG.Add(1)
	p.lock.Unlock()
	defer p.busyWG.Done()

	return p.producer.SendMessage(message)
}

// SendMessages produces a given set of messages, and returns only when all
// messages in the set have either succeeded or failed. Note that messages
// can succeed and fail individually; if some succeed and some fail,
// SendMessages will return an error.
func (p *PartitionProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	// make sure we're not shutting down
	p.lock.Lock()
	if p.isClosed {
		p.lock.Unlock()
		return fmt.Errorf("Can't send messages - producer has already been closed")
	}

	// increment busy WG while in lock
	p.busyWG.Add(1)
	p.lock.Unlock()
	defer p.busyWG.Done()

	return p.producer.SendMessages(msgs)
}
