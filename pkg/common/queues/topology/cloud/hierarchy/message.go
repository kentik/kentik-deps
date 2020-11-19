package hierarchy

import (
	"github.com/kentik/common/queue"
)

// Message represents a new/updated topology hierarchy entity
type Message struct {
	queue.ConsumerMessage
	CloudHierarchy CloudHierarchy
}
