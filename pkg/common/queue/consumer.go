package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
	"github.com/kentik/common/utility"
)

// Consumer is a Kafka consumer that belongs to a consumer group and relies on Kafka
// to pick its partitions and manage its offset. Each partition that a Consumer handles
// is given a separate goroutine to process messages for it.
//
// Note: It's possible that a message will be delivered multiple times during a partition
//       rebalancing, so either make sure that messages are okay to re-process, or set up
//       your own external message offset checking.
type Consumer struct {
	log                    logging.Logger
	logPrefix              string
	topic                  string
	brokerList             []string
	consumerGroupName      string
	quitChan               chan interface{}
	startedConsuming       bool
	lock                   *sync.Mutex // protects startedConsuming
	finishedChan           chan interface{}
	processorGeneratorFunc NewPartitionProcessorFunc
	saramaConfigModFunc    SaramaConfigModFunc
}

// NewConsumer returns a new Consumer
func NewConsumer(log logging.Logger, logPrefix string, topic string, consumerGroupName string, brokerList []string, processorGeneratorFunc NewPartitionProcessorFunc, saramaConfigModFunc SaramaConfigModFunc) (Consumer, error) {
	ret := Consumer{
		log:                    log,
		logPrefix:              fmt.Sprintf("%s(%s) ", logPrefix, topic),
		lock:                   &sync.Mutex{},
		topic:                  topic,
		brokerList:             brokerList,
		consumerGroupName:      consumerGroupName,
		processorGeneratorFunc: processorGeneratorFunc,
		quitChan:               make(chan interface{}, 0),
		finishedChan:           make(chan interface{}, 0),
		saramaConfigModFunc:    saramaConfigModFunc,
	}

	return ret, nil
}

// Close shuts down processing, blocking until done
func (c *Consumer) Close() {
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
}

// Consume consumes messages
// - returns when closed, leaves and rejoins consumer group on failures
func (c *Consumer) Consume() error {
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
		return nil
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V1_1_0_0
	config.ClientID = "kentik.group-consumer"

	// we won't use this consumer to produce, but this removes warnings about how this doesn't work with MaxRequestSize
	config.Producer.MaxMessageBytes = 900000

	// backoff for reconnect when we lose connection to a partition
	config.Consumer.Retry.BackoffFunc = func(retries int) time.Duration {
		dur := utility.RandDurationSeconds(10, 20)
		c.log.Infof(c.logPrefix, "Need to retry connecting to partition (attempt #%d): backing off for %s", retries, dur)
		return dur
	}

	// backoff for retrying a metadata request when the cluster is in the middle of a leader election
	config.Metadata.Retry.Max = 10000 // effectively forever - this'll keep retrying for over a day
	config.Metadata.Retry.BackoffFunc = func(retries int, maxRetries int) time.Duration {
		dur := utility.RandDurationSeconds(10, 20)
		c.log.Infof(c.logPrefix, "Need to retry fetching Kafka metadata (attempt #%d): backing off for %s", retries, dur)
		return dur
	}

	if c.saramaConfigModFunc != nil {
		c.saramaConfigModFunc(config)
	}

	// create the consumer group - keep trying on error, in case Kafka is down on startup
	var consumerGroup sarama.ConsumerGroup
	for {
		c.log.Infof(c.logPrefix, "Attempting to create ConsumerGroup")
		var err error
		consumerGroup, err = sarama.NewConsumerGroup(c.brokerList, c.consumerGroupName, config)
		if err != nil {
			sleepTime := utility.RandDurationSeconds(15, 30)
			c.log.Errorf(c.logPrefix, "Error creating consumer group - waiting %s, then trying again: %s", sleepTime, err)
			time.Sleep(sleepTime)
			continue
		}

		// success
		c.log.Infof(c.logPrefix, "Created ConsumerGroup")
		break
	}

	wg := sync.WaitGroup{}
	defer wg.Wait()

	// close ConsumerGroup on quitChan
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-c.quitChan:
		}

		if err := consumerGroup.Close(); err != nil {
			c.log.Errorf(c.logPrefix, "Error closing consumer group: %s", err)
		}
		c.log.Infof(c.logPrefix, "Closed consumer group")
	}()

	// consume errors
	wg.Add(1)
	go func() {
		defer wg.Done()

		for err := range consumerGroup.Errors() {
			c.log.Errorf(c.logPrefix, "Errors chan: %s", err)
		}
		c.log.Infof(c.logPrefix, "Finished consuming Err0rs channel")
	}()

	// consume retry loop
	for {
		start := time.Now()
		c.log.Infof(c.logPrefix, "Attempting to Consume()")

		// Consume returns when partitions are rebalanced. We're supposed to keep calling this in an infinite loop,
		// so all consumers are active at the same time during rebalancing.
		if err := consumerGroup.Consume(context.Background(), []string{c.topic}, c); err != nil {
			c.log.Errorf(c.logPrefix, "Error consuming: %s", err)
			// don't exit or break
		}
		c.log.Infof(c.logPrefix, "Stopped consuming after %s", time.Since(start))

		// see if we're all done
		select {
		case <-c.quitChan:
			c.log.Infof(c.logPrefix, "Exiting Consume() loop due to closed quitChan")
			return nil

		default:
			// try again
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
// ** ConsumerGroupHandler method
func (c *Consumer) Setup(consumerGroupSession sarama.ConsumerGroupSession) error {
	c.log.Infof(c.logPrefix, "Setup: Beginning consumer group session with memberID: %s, generation: %d, partitions: %v",
		consumerGroupSession.MemberID(), consumerGroupSession.GenerationID(), consumerGroupSession.Claims()[c.topic])
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
// ** ConsumerGroupHandler method
func (c *Consumer) Cleanup(consumerGroupSession sarama.ConsumerGroupSession) error {
	c.log.Infof(c.logPrefix, "Cleanup: Exiting consumer group session with partitions: %v", consumerGroupSession.Claims()[c.topic])
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
// ** ConsumerGroupHandler method
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	processor := c.processorGeneratorFunc(claim.Partition())

	c.log.Infof(c.logPrefix, "ConsumeClaim: Beginning processing for topic '%s', partition %d, initial/high-water offsets: %d/%d",
		c.topic, claim.Partition(), claim.InitialOffset(), claim.HighWaterMarkOffset())
	func() { // outer function for returns
		for {
			select {
			case msg, ok := <-claim.Messages():
				if !ok {
					// Messages() channel closed - maybe by a rebalance?
					c.log.Infof(c.logPrefix, "Finished processing for topic '%s', partition %d - messages channel closed", c.topic, claim.Partition())
					return
				}

				if !processor.ProcessMessage(*msg) {
					c.log.Infof(c.logPrefix, "Finished processing for topic '%s', partition %d - processor returned false", c.topic, claim.Partition())
					return
				}

				// success: mark the message as processed
				session.MarkMessage(msg, "")

			case <-c.quitChan:
				c.log.Infof(c.logPrefix, "quit: Shutting down message channel for topic '%s', partition #%d", c.topic, claim.Partition())
				return
			}
		}
	}()

	// shut down our processor
	processor.Shutdown()

	// need to drain messages while shutting down the PartitionConsumer, or its busy goroutines could hang, trying to write to it
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range claim.Messages() {
			// drain
		}
	}()

	// wait for drain goroutine to finish
	wg.Wait()
	c.log.Infof(c.logPrefix, "Cleaned up after processing for topic '%s', partition %d", c.topic, claim.Partition())

	return nil
}
