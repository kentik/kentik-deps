package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/utility"
)

// MultiPartitionConsumer consumes all partitions for a single topic, muxing all messages
// into a single unbuffered channel, either from newest or oldest offet.
type MultiPartitionConsumer struct {
	log                    logging.Logger
	logPrefix              string
	topic                  string
	quitChan               chan interface{}
	messageChan            chan sarama.ConsumerMessage
	saramaClient           sarama.Client
	consumer               sarama.Consumer
	partitionConsumers     []sarama.PartitionConsumer
	hasReachedCurrentFunc  func()
	relativeStartingOffset RelativeStartingOffset
}

// RelativeStartingOffset is a type for determining the relative starting offset for multi-partition consumers
// - matches up with Sarama's OffsetNewest/OffsetOldest
type RelativeStartingOffset int64

// RelativeStartingOffsetNewest tells the multi-partition consumer to start with the newest messages
var RelativeStartingOffsetNewest RelativeStartingOffset = -1

// RelativeStartingOffsetOldest tells the multi-partition consumer to start with the oldest messages
var RelativeStartingOffsetOldest RelativeStartingOffset = -2

// NewMultiPartitionConsumer returns a new MultiPartitionConsumer
// - if hasReachedCurrentFunc is non-nil, it'll be called once we've read all messages before when we started consuming
func NewMultiPartitionConsumer(log logging.Logger, logPrefix string, topic string, brokerList []string,
	relativeStartingOffset RelativeStartingOffset, hasReachedCurrentFunc func()) (*MultiPartitionConsumer, error) {

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V1_1_0_0
	config.ClientID = "kentik.multi-partition-consumer"

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

	saramaClient, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("Error creating sarama client: %s", err)
	}

	consumer, err := sarama.NewConsumerFromClient(saramaClient)
	if err != nil {
		saramaClient.Close()
		return nil, fmt.Errorf("Error creating consumer from client: %s", err)
	}

	ret := &MultiPartitionConsumer{
		log:       log,
		logPrefix: logPrefix + fmt.Sprintf("[%s:<multi>] ", topic),
		topic:     topic,
		quitChan:  make(chan interface{}),

		// try an unbuffered channel - sarama buffers for us, and this makes it easier to tell when we've caught up
		messageChan:            make(chan sarama.ConsumerMessage, 0),
		saramaClient:           saramaClient,
		consumer:               consumer,
		partitionConsumers:     make([]sarama.PartitionConsumer, 0),
		hasReachedCurrentFunc:  hasReachedCurrentFunc,
		relativeStartingOffset: relativeStartingOffset,
	}

	return ret, nil
}

// Close shuts down everything
// - only call this after Consume() has returned with no error
// - after you call this, you can't trust the messages channel - it'll be drained
func (c *MultiPartitionConsumer) Close() {
	select {
	case <-c.quitChan:
		// already quit
		return
	default:
	}
	close(c.quitChan)

	c.log.Infof(c.logPrefix, "Close() called - cleaning up")
	start := time.Now()

	// need to close all partition consumers first
	for _, partitionConsumer := range c.partitionConsumers {
		if partitionConsumer != nil {
			partitionConsumer.AsyncClose()
		}
	}

	if err := c.consumer.Close(); err != nil {
		c.log.Infof(c.logPrefix, "Error closing consumer: %s", err)
	}

	if err := c.saramaClient.Close(); err != nil {
		c.log.Infof(c.logPrefix, "Error closing sarama client: %s", err)
	}
	c.log.Infof(c.logPrefix, "Finished Close() in %s", time.Since(start))
}

// Messages returns the message channel, where all messages found in every partition are written
// - if no more messages, it's because the client was closed
func (c *MultiPartitionConsumer) Messages() <-chan sarama.ConsumerMessage {
	return c.messageChan
}

// Consume consumes all messages from the partitions we're interested in
// - returns after all goroutines have been launched, or on error while setting up, in which case we'll have called Close()
// - if not nil, the intput caughtUpFunc will be called when we're all caught up. Caller should handle a timeout
func (c *MultiPartitionConsumer) Consume() error {
	// when all partition consumers have finshed, we can then close the message channel, signaling there's nothing left to process/drain
	finishedWg := &sync.WaitGroup{}
	defer func() {
		go func() {
			finishedWg.Wait()
			c.log.Infof(c.logPrefix, "Finished consuming %d partitions", len(c.partitionConsumers))

			// no more messages will be written to messageChan, and drain has been initiated - close the message channel
			close(c.messageChan)
		}()
	}()

	// watch for quitChan so we can drain the message channel
	go func() {
		<-c.quitChan

		c.log.Infof(c.logPrefix, "quitChan closed: draining messages")
		for range c.messageChan {
			// do nothing - channel will be closed when all partition consumers are closed
		}
		c.log.Infof(c.logPrefix, "finished draining messages")
	}()

	// figure out how many partitions
	partitions, err := c.saramaClient.Partitions(c.topic)
	if err != nil {
		c.Close()
		return fmt.Errorf("Error determining partition count")
	}
	c.log.Infof(c.logPrefix, "Found %d partitions", len(partitions))

	if len(partitions) == 0 {
		c.log.Warnf(c.logPrefix, "Couldn't find any partitions")
		c.hasReachedCurrentFunc()
	}

	start := time.Now()
	partitionsNotYetCaughtUp := int32(len(partitions))
	totalMessagesRead := uint64(0)
	handleCaughtUp := func(partitionNumber int32, msgCount uint64) {
		c.log.Debugf(c.logPrefix, "Handled all past messages (%d) for partition %d", msgCount, partitionNumber)
		newTotal := atomic.AddUint64(&totalMessagesRead, msgCount)
		if atomic.AddInt32(&partitionsNotYetCaughtUp, -1) == 0 {
			c.log.Infof(c.logPrefix, "Found %d past messages for all %d partitions in %s", newTotal, len(partitions), time.Since(start))
			if c.hasReachedCurrentFunc != nil {
				c.hasReachedCurrentFunc()
			}
		}
	}

	// waitgroup to wait until all partition consumers have finished initializing
	initializationWg := &sync.WaitGroup{}
	partitionConsumerErrCount := uint32(0) // protect with sync/atomic
	initializationStart := time.Now()

	// we pre-size this slice, then each goroutine will write in the appropriate spot
	// - who knows if partitions are always [0,partitionCount)? - Make sure
	maxPartitionNum := int32(0)
	for _, partition := range partitions {
		if partition > maxPartitionNum {
			maxPartitionNum = partition
		}
	}
	c.partitionConsumers = make([]sarama.PartitionConsumer, maxPartitionNum+1)

	// start each partition consumer in a goroutine, then wait for them to initialize
	for _, loopPartitionNumber := range partitions {
		partitionNumber := loopPartitionNumber

		initializationWg.Add(1)
		finishedWg.Add(1)
		go func() {
			defer finishedWg.Done()

			startingOffset, err := c.saramaClient.GetOffset(c.topic, partitionNumber, int64(c.relativeStartingOffset))
			if err != nil {
				c.log.Errorf(c.logPrefix, "Error fetching starting offset for partition %d: %s", partitionNumber, err)
				atomic.AddUint32(&partitionConsumerErrCount, 1)
				initializationWg.Done()
				return
			}

			partitionConsumer, err := c.consumer.ConsumePartition(c.topic, partitionNumber, int64(c.relativeStartingOffset))
			if err != nil {
				c.log.Errorf(c.logPrefix, "Error fetching partition consumer for partition %d: %s", partitionNumber, err)
				atomic.AddUint32(&partitionConsumerErrCount, 1)
				initializationWg.Done()
				return
			}
			c.partitionConsumers[partitionNumber] = partitionConsumer

			// get the high water mark via GetOffset, rather than HighWaterMarkOffset(), since that's only updated on each message
			highWaterMarkOffset, err := c.saramaClient.GetOffset(c.topic, partitionNumber, sarama.OffsetNewest)
			if err != nil {
				c.log.Errorf(c.logPrefix, "Error fetching high water mark offset for partition %d: %s", partitionNumber, err)
				atomic.AddUint32(&partitionConsumerErrCount, 1)
				initializationWg.Done()
				return
			}

			c.log.Debugf(c.logPrefix, "Consuming messages for partition %d starting with offset %d, high water mark offset %d", partitionNumber, startingOffset, highWaterMarkOffset)

			// this bool makes sure we only decrement the pending partition count once for this partition
			thisPartitionCaughtUp := false

			if startingOffset >= highWaterMarkOffset {
				// nothing to read
				thisPartitionCaughtUp = true
				handleCaughtUp(partitionNumber, 0)
			}

			// watch Errors channel
			go func() {
				for err := range partitionConsumer.Errors() {
					c.log.Warnf(c.logPrefix, "sarama err0r for partition %d: %s", partitionNumber, err)
				}
			}()

			initializationWg.Done()
			msgCount := uint64(0)
			for msg := range partitionConsumer.Messages() {
				c.messageChan <- *msg
				msgCount++

				// see if this message catches us up
				if !thisPartitionCaughtUp && msg.Offset+1 >= highWaterMarkOffset {
					thisPartitionCaughtUp = true
					handleCaughtUp(partitionNumber, msgCount)
				}
			}
			c.log.Infof(c.logPrefix, "Finished watching partition %d for messages, found %d", partitionNumber, msgCount)
		}()
	}

	// wait for all partition consumers to initialize
	c.log.Infof(c.logPrefix, "Waiting for %d partition consumers to initialize", len(partitions))
	initializationWg.Wait()
	c.log.Infof(c.logPrefix, "All %d partition consumers have been initialized in %s", len(partitions), time.Since(initializationStart))
	if partitionConsumerErrCount > 0 {
		c.Close()
		return fmt.Errorf("%d error(s) occurred while initializing partition consumers (already logged)", partitionConsumerErrCount)
	}

	return nil
}
