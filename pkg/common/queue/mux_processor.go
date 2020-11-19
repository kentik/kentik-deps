package queue

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
)

// MuxProcessor that reads from any partition given to it, feeding every message into a single channel
type MuxProcessor struct {
	log       logging.Logger
	logPrefix string
	outChan   chan sarama.ConsumerMessage
	quitChan  chan interface{}
}

// NewMuxProcessor returns a new MuxProcessor
func NewMuxProcessor(log logging.Logger, logPrefix string, topic string, outChan chan sarama.ConsumerMessage) *MuxProcessor {
	return &MuxProcessor{
		log:       log,
		logPrefix: fmt.Sprintf("%s[mux-processor %s] ", logPrefix, topic),
		outChan:   outChan,
		quitChan:  make(chan interface{}, 0),
	}
}

// Close shuts down the consumer
func (m *MuxProcessor) Close() {
	close(m.outChan)
	close(m.quitChan)
}

// ProcessMessage receives a message, and submits it to the out channel
func (m *MuxProcessor) ProcessMessage(consumerMessage sarama.ConsumerMessage) bool {
	select {
	case m.outChan <- consumerMessage:
		return true

	case <-m.quitChan:
		return false
	}
}

// Shutdown is called when we lose a partition
func (m *MuxProcessor) Shutdown() {}
