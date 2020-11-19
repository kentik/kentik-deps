package changed

import (
	"github.com/kentik/common/queue"
)

// Message represents an Interface changed message received from queue
type Message struct {
	queue.ConsumerMessage
	Interface Interface

	// if this signals a delete, CompanyID and DeviceID will still be in Interface
	IsDeleted bool
}
