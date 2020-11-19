package response

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kentik/common/queues/tagging/functest/request"
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

// AddResponse adds a request to the batch
func (b *Batch) AddResponse(companyID uint32, deviceID uint32, shardNumber uint32, host string, request request.Message, flow chf.CHF) error {
	serializedFlow, err := serializeFlow(flow)
	if err != nil {
		return fmt.Errorf("Could not serialize flow: %s", err)
	}

	resp := FlowResponse{
		CompanyID:          companyID,
		DeviceID:           deviceID,
		ShardNumber:        shardNumber,
		Host:               host,
		ScenarioName:       request.ScenarioName,
		SerializedFlow:     serializedFlow,
		FieldID:            request.FieldID,
		ExpectedFieldValue: request.ExpectedFieldValue,
	}

	serializedResp, err := resp.Marshal()
	if err != nil {
		return fmt.Errorf("Couldn't serialize FlowResponse: %s", err)
	}

	b.messages = append(b.messages, &sarama.ProducerMessage{
		Topic: Topic,
		Value: sarama.ByteEncoder(serializedResp),
	})

	return nil
}

// Messages returns the sarama producer messages
func (b *Batch) Messages() []*sarama.ProducerMessage {
	return b.messages
}
