package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/utility"
)

// PartitionConsumer is a consumer that starts processing at a specific offset, and locally
// keeps track of it.
type PartitionConsumer struct {
	log                     logging.Logger
	logPrefix               string
	partition               int32
	topic                   string
	quitChan                chan interface{}
	startedConsuming        bool
	lock                    *sync.Mutex // protects startedConsuming
	finishedChan            chan interface{}
	initializingWG          *sync.WaitGroup // blocks while we're starting up - TODO: this might no longer be necessary
	initializationSuccess   bool
	messageChan             <-chan *sarama.ConsumerMessage
	saramaClient            sarama.Client
	startingOffset          int64
	partitionConsumer       sarama.PartitionConsumer
	hasFetchedHighWaterMark bool // whether we've fetched or the latest offset

	// if not nil, this function is called exactly once, once we're all caught up from where we began
	hasReachedCurrentFunc func()
}

// NewPartitionConsumer returns a new PartitionConsumer
func NewPartitionConsumer(log logging.Logger, logPrefix string, partition int32, topic string, brokerList []string, hasReachedCurrentFunc func()) (PartitionConsumer, error) {
	logPrefix = logPrefix + fmt.Sprintf("[%s:%d] ", topic, partition)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V1_1_0_0
	config.ClientID = "kentik.partition-consumer"

	// we won't use this consumer to produce, but this removes warnings about how this doesn't work with MaxRequestSize
	config.Producer.MaxMessageBytes = 900000

	// backoff for reconnect when we lose connection to a partition
	config.Consumer.Retry.BackoffFunc = func(retries int) time.Duration {
		dur := utility.RandDurationSeconds(10, 20)
		log.Infof(logPrefix, "Need to retry connecting to partition (attempt #%d): backing off for %s", retries, dur)
		return dur
	}

	// backoff for retrying a metadata request when the cluster is in the middle of a leader election
	config.Metadata.Retry.Max = 10000 // effectively forever - this'll keep retrying for over a day
	config.Metadata.Retry.BackoffFunc = func(retries int, maxRetries int) time.Duration {
		dur := utility.RandDurationSeconds(10, 20)
		log.Infof(logPrefix, "Need to retry fetching Kafka metadata (attempt #%d): backing off for %s", retries, dur)
		return dur
	}

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return PartitionConsumer{}, fmt.Errorf("Error creating sarama client: %s", err)
	}

	ret := PartitionConsumer{
		log:                   log,
		logPrefix:             logPrefix,
		lock:                  &sync.Mutex{},
		partition:             partition,
		topic:                 topic,
		saramaClient:          client,
		initializingWG:        &sync.WaitGroup{},
		quitChan:              make(chan interface{}, 0),
		finishedChan:          make(chan interface{}, 0),
		hasReachedCurrentFunc: hasReachedCurrentFunc,
	}
	ret.initializingWG.Add(1)

	return ret, nil
}

// Close shuts down everything
// - after you call this, you can't trust the messages channel - it'll be drained
func (c *PartitionConsumer) Close() {
	// signal quit, unless already quitting
	select {
	case <-c.quitChan:
	default:
		close(c.quitChan)
	}

	// check if we've started
	c.lock.Lock()
	didStart := c.startedConsuming
	c.lock.Unlock()

	if didStart {
		// wait for finish
		<-c.finishedChan
	}

	if err := c.saramaClient.Close(); err != nil {
		c.log.Errorf(c.logPrefix, "Error closing client: %s", err)
	}
}

// Partition returns the partition
func (c *PartitionConsumer) Partition() int32 {
	return c.partition
}

// Consume consumes messages after the input offset.
// - returns when done reading messages
// - pass sarama.OffsetNewest to start with newest message
// - pass sarama.OffsetOldest to start with oldest message
func (c *PartitionConsumer) Consume(startingOffset int64) {
	defer close(c.finishedChan)

	// record that we're starting
	shouldContinue := func() bool {
		c.lock.Lock()
		defer c.lock.Unlock()

		// see if we've already requested Close()
		select {
		case <-c.quitChan:
			// already done - nothing to do - exit
			return false

		default:
		}

		// haven't been signalled to quit
		c.startedConsuming = true
		return true
	}()
	if !shouldContinue {
		return
	}

	wg := sync.WaitGroup{}
	defer wg.Wait()

	if startingOffset < 0 && startingOffset != sarama.OffsetNewest && startingOffset != sarama.OffsetOldest {
		c.log.Errorf(c.logPrefix, "startingOffset (%d) is less than 0 and not: [-1: sarama.OffsetNewest, -2: sarama.OffsetOldest]", startingOffset)
		c.initializingWG.Done()
		return
	}

	consumer, err := sarama.NewConsumerFromClient(c.saramaClient)
	if err != nil {
		c.log.Errorf(c.logPrefix, "Error creating consumer from client: %s", err)
		c.initializingWG.Done()
		return
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			c.log.Errorf(c.logPrefix, "Error closing consumer: %s", err)
		}
	}()

	// make sure the offset we want is within the available bounds
	oldestOffset, err := c.saramaClient.GetOffset(c.topic, c.partition, sarama.OffsetOldest)
	if err != nil {
		c.log.Errorf(c.logPrefix, "Error querying oldest offset: %s", err)
		c.initializingWG.Done()
		return
	}

	// validate startingOffset
	if startingOffset == sarama.OffsetOldest {
		// oldest offset
		c.startingOffset = oldestOffset
	} else if startingOffset == sarama.OffsetNewest {
		// newest offset
		newestOffset, err := c.saramaClient.GetOffset(c.topic, c.partition, sarama.OffsetNewest)
		if err != nil {
			c.log.Errorf(c.logPrefix, "Error fetching latest offset: %s", err)
			c.initializingWG.Done()
			return
		}
		c.startingOffset = newestOffset
	} else {
		// caller picked an absolute offset
		if startingOffset < oldestOffset {
			// too old - walk it forward
			c.log.Warnf(c.logPrefix, "Requested starting offset %d, but earliest available is %d", startingOffset, oldestOffset)
			c.startingOffset = oldestOffset
		} else {
			newestOffset, err := c.saramaClient.GetOffset(c.topic, c.partition, sarama.OffsetNewest)
			if err != nil {
				c.log.Errorf(c.logPrefix, "Error fetching latest offset: %s", err)
				c.initializingWG.Done()
				return
			}
			if startingOffset > newestOffset {
				// too new - walk it back
				c.log.Warnf(c.logPrefix, "Requested starting offset %d, but latest offset is %d - using that", startingOffset, newestOffset)
				c.startingOffset = newestOffset
			} else {
				// just right
				c.startingOffset = startingOffset
			}
		}
	}

	partConsumer, err := consumer.ConsumePartition(c.topic, c.partition, c.startingOffset)
	if err != nil {
		c.log.Errorf(c.logPrefix, "Error consuming partition: %s", err)
		c.initializingWG.Done()
		return
	}
	defer func() {
		drainChan := make(chan interface{}, 0)
		go func() {
			for range partConsumer.Messages() {
				// drain
			}
			close(drainChan)
		}()
		if err := partConsumer.Close(); err != nil {
			c.log.Errorf(c.logPrefix, "Error closing partition consumer: %s", err)
		}
		<-drainChan
	}()
	c.partitionConsumer = partConsumer

	wg.Add(1)
	go func() {
		defer wg.Done()

		for err := range partConsumer.Errors() {
			c.log.Errorf(c.logPrefix, "Errors chan: %s", err)
		}
		c.log.Infof(c.logPrefix, "Finished consuming Err0rs channel")
	}()

	// set the message chan
	c.messageChan = partConsumer.Messages()

	// we're initialized now
	c.initializationSuccess = true
	c.initializingWG.Done()

	<-c.quitChan
	c.log.Debugf(c.logPrefix, "Leaving consume loop")
}

// Messages returns the message channel
// - if no more messages, it's because the client was closed
func (c *PartitionConsumer) Messages() (<-chan sarama.ConsumerMessage, error) {
	return c.messages(false)
}

// PastMessages returns a message channel that closes when we're caught up
func (c *PartitionConsumer) PastMessages() (<-chan sarama.ConsumerMessage, error) {
	return c.messages(true)
}

// Called when we've caught up to the point where we started.
// - call the callback function if set, then forget the function so we make sure to only call it once
func (c *PartitionConsumer) handleReachedCurrent() {
	if c.hasReachedCurrentFunc != nil {
		// notify the caller
		c.hasReachedCurrentFunc()

		// set nil, so we don't call it again
		c.hasReachedCurrentFunc = nil
	}
}

// return a messages channel that converts sarama ConsumerMessages to Message
func (c *PartitionConsumer) messages(stopWhenCurrent bool) (chan sarama.ConsumerMessage, error) {
	c.initializingWG.Wait()
	if !c.initializationSuccess {
		return nil, fmt.Errorf("Kafka client initialization was unsuccessful")
	}

	// we need to wrap our channel with another, so we can gatekeep
	ret := make(chan sarama.ConsumerMessage)

	highWaterMark, err := c.highWaterMark()
	if err != nil {
		return nil, fmt.Errorf("Could not fetch high water mark for Kafka topic: %s", err)
	}

	c.log.Debugf(c.logPrefix, "Starting offset: %d, high water mark: %d", c.startingOffset, highWaterMark)
	if c.startingOffset >= highWaterMark {
		// we're all caught up
		c.handleReachedCurrent()
		if stopWhenCurrent {
			// nothing to fetch
			close(ret)
			return ret, nil
		}
	}

	// there are messages to fetch - gatekeep until current
	go func() {
		defer close(ret)

		for {
			select {
			case saramaMessage, ok := <-c.messageChan:
				if !ok {
					c.log.Debugf(c.logPrefix, "Message channel for '%s' seems to be shutting down - breaking out of loop", c.topic)
					return
				}

				c.log.Debugf(c.logPrefix, "Received message with offset %d", saramaMessage.Offset)
				ret <- *saramaMessage
				if saramaMessage.Offset+1 >= highWaterMark {
					// we're all caught up
					c.handleReachedCurrent()
					if stopWhenCurrent {
						// reached end of channel
						return
					}
				}

			case <-c.quitChan:
				// told to stop early
				return
			}
		}
	}()
	return ret, nil
}

// WaitForInitialization waits for initialization to be complete, returning whether it did
func (c *PartitionConsumer) WaitForInitialization() bool {
	c.initializingWG.Wait()
	return c.initializationSuccess
}

// OffsetNearTime returns the nearest offset by the input time
// - returns -1 if there's nothing in the queue
func (c *PartitionConsumer) OffsetNearTime(msgTime time.Time) (int64, error) {
	return c.saramaClient.GetOffset(c.topic, c.partition, msgTime.Unix()*1000)
}

// return the high water mark known as of the last message fetch, or get it if we haven't yet fetched a message
// - returns -1 if there's nothing in the queue
func (c *PartitionConsumer) highWaterMark() (int64, error) {
	if !c.hasFetchedHighWaterMark {
		ret, err := c.saramaClient.GetOffset(c.topic, c.partition, sarama.OffsetNewest)
		if err != nil {
			return -1, fmt.Errorf("Error fetching latest offset: %s", err)
		}
		c.hasFetchedHighWaterMark = true
		c.log.Debugf(c.logPrefix, "Fetched partition info - found that high water mark is %d", ret)
		return ret, nil
	}

	// partition consumer updates its knowledge of the high water mark on every message received
	return c.partitionConsumer.HighWaterMarkOffset(), nil
}
