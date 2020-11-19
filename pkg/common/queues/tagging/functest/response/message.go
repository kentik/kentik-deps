package response

import (
	"github.com/kentik/common/queue"
	chf "github.com/kentik/proto/kflow"
)

// Message represents a TagIngestBatchPartInternal message received from queue
type Message struct {
	queue.ConsumerMessage

	// CompanyID that processed the message
	CompanyID uint32

	// DeviceID that processed this message
	DeviceID uint32

	// ShardNumber of the device that processed the message
	ShardNumber uint32

	// Host that processed this flow
	Host string

	// ScenarioName holds the test scenario name
	ScenarioName string

	// processed flow record
	Flow chf.CHF

	// Field field we're watching (echoed from request)
	FieldID uint32

	// Expected value in this field (converted to string if necessary) (echoed from request)
	ExpectedFieldValue string
}
