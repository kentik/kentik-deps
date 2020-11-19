package request

import (
	"github.com/kentik/common/queue"
	chf "github.com/kentik/proto/kflow"
)

// Message represents a TagIngestBatchPartInternal message received from queue
type Message struct {
	queue.ConsumerMessage

	// CompanyID that should process this message, or 0 for all
	CompanyID uint32

	// DeviceID that should process this message, or 0 for all
	DeviceID uint32

	// ShardNumber that should process this message, or -1 for all
	// - devices with multiple shards have shard numbers (1,N)
	// - devices with one shard have shard number 0
	ShardNumber int32

	// Host that should process this message, or "" for all
	Host string

	// ScenarioName holds the test scenario name
	ScenarioName string

	// Flow record
	Flow chf.CHF

	// FieldID to match
	FieldID uint32

	// Expected value in this field (converted to string if necessary)
	ExpectedFieldValue string
}

// ShouldBeHandled returns whether the client with the input companyID, deviceID,
// and hostName should process this message
func (m *Message) ShouldBeHandled(companyID uint32, deviceID uint32, shardNumber uint32, hostName string) bool {
	if m.CompanyID != 0 && m.CompanyID != companyID {
		// meant for another company
		return false
	}

	if m.DeviceID != 0 && m.DeviceID != deviceID {
		// meant for another device
		return false
	}

	if m.Host != "" && m.Host != hostName {
		// meant for another host
		return false
	}

	if m.ShardNumber >= 0 && uint32(m.ShardNumber) != shardNumber {
		// meant for another shard
		return false
	}

	return true
}
