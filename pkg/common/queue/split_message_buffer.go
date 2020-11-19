package queue

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/logging"
)

// SplitMessageBuffer buffers queue messages received for a channel, storing partial
// messages until it has the full part, then combines them back together.
// This allows us to send bigger models thorugh Kafka than it's configured to handle.
type SplitMessageBuffer struct {
	log            logging.Logger
	logPrefix      string
	topic          string
	companyBuffers map[uint32]*CompanyMessageBuffer
}

// NewSplitMessageBuffer returns a new SplitMessageBuffer
func NewSplitMessageBuffer(log logging.Logger, logPrefix string, topic string) *SplitMessageBuffer {
	return &SplitMessageBuffer{
		log:            log,
		logPrefix:      logPrefix + "[msg-buffer] ",
		topic:          topic,
		companyBuffers: make(map[uint32]*CompanyMessageBuffer),
	}
}

// NewCompanyMessageBuffer returns a new CompanyMessageBuffer
func NewCompanyMessageBuffer() *CompanyMessageBuffer {
	return &CompanyMessageBuffer{
		partsByGUID: make(map[string]*MessageParts),
	}
}

// NewMessageParts return a new MessageParts
func NewMessageParts() *MessageParts {
	return &MessageParts{
		partByNumber: make(map[uint32]*MessagePart),
	}
}

// ReceiveMessage receives the input message, expected to be of type queue.MessagePart,
// returns the serialized bytes of the original pre-split message, if it's now complete,
// else, returns nil
func (b *SplitMessageBuffer) ReceiveMessage(consumerMessage sarama.ConsumerMessage) ([]byte, *ConsumerMessage) {
	part := MessagePart{}
	if err := part.Unmarshal(consumerMessage.Value); err != nil {
		// 2018-07-20: mute this for now, while old non-MessagePart messages are still in the queue
		// b.log.Warnf(b.logPrefix, "Ignoring message from %s/%d, offset %d that is not of type MessagePart: %s", consumerMessage.Topic, consumerMessage.Partition, consumerMessage.Offset, err)
		return nil, nil
	}

	if part.PartCount == 1 {
		return part.Data, &ConsumerMessage{
			Partition: consumerMessage.Partition,
			Offset:    consumerMessage.Offset,
			Timestamp: consumerMessage.Timestamp,
		}
	}

	companyMessageBuffer, ok := b.companyBuffers[part.CompanyID]
	if !ok {
		companyMessageBuffer = NewCompanyMessageBuffer()
		b.companyBuffers[part.CompanyID] = companyMessageBuffer
	}

	ret, msg, err := companyMessageBuffer.Receive(&part, consumerMessage)
	if err != nil {
		// we just log this - don't make caller worry about it
		b.log.Errorf(b.logPrefix, "Ignoring consumer message: %s", err)
		return nil, nil
	}
	return ret, msg
}

// CompanyMessageBuffer buffers messages for a specific company
type CompanyMessageBuffer struct {
	partsByGUID map[string]*MessageParts
}

// Receive receives a message part, returning the data byte array if it completed the message.
// If it did not yet complete the message, then it caches this part and returns nil.
// - assume the PartCount has already been checked
func (b *CompanyMessageBuffer) Receive(messagePart *MessagePart, consumerMessage sarama.ConsumerMessage) ([]byte, *ConsumerMessage, error) {
	messageParts, ok := b.partsByGUID[messagePart.GUID]
	if !ok {
		messageParts = NewMessageParts()
		b.partsByGUID[messagePart.GUID] = messageParts
	}

	ret, msg, err := messageParts.Receive(messagePart, consumerMessage)
	if err != nil {
		return nil, nil, err
	}
	if ret != nil {
		// all done with this part
		delete(b.partsByGUID, messagePart.GUID)
	}
	return ret, msg, nil
}

// MessageParts holds the pending message parts for a single message
type MessageParts struct {
	partByNumber         map[uint32]*MessagePart
	firstOffset          int64
	firstTimestamp       time.Time
	lastPartRecievedDate time.Time
}

// Receive receives a message part, returning the data byte array if it completed the message.
// If it did not yet complete the message, then it caches this part and returns nil.
// - assume the PartCount has already been checked
func (p *MessageParts) Receive(messagePart *MessagePart, consumerMessage sarama.ConsumerMessage) ([]byte, *ConsumerMessage, error) {
	p.lastPartRecievedDate = time.Now()
	p.partByNumber[messagePart.PartNumber] = messagePart

	if p.firstOffset == 0 {
		p.firstOffset = consumerMessage.Offset
		p.firstTimestamp = consumerMessage.Timestamp
	}

	if uint32(len(p.partByNumber)) >= messagePart.PartCount {
		// allocate once
		retSize := 0
		for _, part := range p.partByNumber {
			retSize += len(part.Data)
		}
		ret := make([]byte, 0, retSize)

		for partNumber := uint32(1); partNumber <= messagePart.PartCount; partNumber++ {
			part, ok := p.partByNumber[partNumber]
			if !ok {
				return nil, nil, fmt.Errorf("Message part should be finished, but can't find part %d of %d for companyID %d, message GUID %s", partNumber, messagePart.PartCount, messagePart.CompanyID, messagePart.GUID)
			}

			ret = append(ret, part.Data...)
		}

		return ret, &ConsumerMessage{
			Partition: consumerMessage.Partition,
			Offset:    p.firstOffset,
			Timestamp: p.firstTimestamp,
		}, nil
	}

	return nil, nil, nil
}
