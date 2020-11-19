package request

import (
	"fmt"

	"github.com/Shopify/sarama"
	chf "github.com/kentik/proto/kflow"
)

// Batch holds a batch of requests
type Batch struct {
	messages []*sarama.ProducerMessage
}

// NewBatch returns a new Batch
func NewBatch() *Batch {
	return &Batch{
		messages: make([]*sarama.ProducerMessage, 0),
	}
}

// AddRequest adds a request to the batch
func (b *Batch) AddRequest(scenarioName string, companyID uint32, deviceID uint32, shardNumber int32, hostName string, flow chf.CHF, fieldID uint32, expectedFieldValue string) error {
	serializedFlow, err := serializeFlow(flow)
	if err != nil {
		return fmt.Errorf("Could not serialize flow: %s", err)
	}

	req := FlowRequest{
		CompanyID:          companyID,
		DeviceID:           deviceID,
		ShardNumber:        shardNumber,
		Host:               hostName,
		ScenarioName:       scenarioName,
		SerializedFlow:     serializedFlow,
		FieldID:            fieldID,
		ExpectedFieldValue: expectedFieldValue,
	}

	serializedReq, err := req.Marshal()
	if err != nil {
		return fmt.Errorf("Couldn't serialize FlowRequest: %s", err)
	}

	b.messages = append(b.messages, &sarama.ProducerMessage{
		Topic: Topic,
		Value: sarama.ByteEncoder(serializedReq),
	})

	return nil
}

// Messages returns the sarama producer messages
func (b *Batch) Messages() []*sarama.ProducerMessage {
	return b.messages
}
