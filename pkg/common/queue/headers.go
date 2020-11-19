package queue

import (
	"encoding/binary"

	"github.com/Shopify/sarama"
)

// AddStringHeader adds a string key/value header to the message
func AddStringHeader(key string, value string, message *sarama.ProducerMessage) {
	if message.Headers == nil {
		message.Headers = make([]sarama.RecordHeader, 0, 1)
	}

	message.Headers = append(message.Headers, sarama.RecordHeader{Key: []byte(key), Value: []byte(value)})
}

// GetStringConsumerHeader gets the value for a string header
func GetStringConsumerHeader(key string, message *sarama.ConsumerMessage) (string, bool) {
	for _, header := range message.Headers {
		if string(header.Key) == key {
			return string(header.Value), true
		}
	}
	return "", false
}

// GetStringProducerHeader gets the value for a string header
func GetStringProducerHeader(key string, message *sarama.ProducerMessage) (string, bool) {
	for _, header := range message.Headers {
		if string(header.Key) == key {
			return string(header.Value), true
		}
	}
	return "", false
}

// AddUintHeader adds a string key, uint32 value to the message
func AddUintHeader(key string, value uint32, message *sarama.ProducerMessage) {
	if message.Headers == nil {
		message.Headers = make([]sarama.RecordHeader, 0, 1)
	}

	valueBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(valueBytes, value)

	header := sarama.RecordHeader{Key: []byte(key), Value: valueBytes}
	message.Headers = append(message.Headers, header)
}

// GetUint32ConsumerHeader returns the uint32 header key
func GetUint32ConsumerHeader(key string, message *sarama.ConsumerMessage) (uint32, bool) {
	for _, header := range message.Headers {
		if string(header.Key) == key {
			return binary.BigEndian.Uint32(header.Value), true
		}
	}
	return 0, false
}

// GetUint32ProducerHeader returns the uint32 header key
func GetUint32ProducerHeader(key string, message *sarama.ProducerMessage) (uint32, bool) {
	for _, header := range message.Headers {
		if string(header.Key) == key {
			return binary.BigEndian.Uint32(header.Value), true
		}
	}
	return 0, false
}
