package poll

import (
	"github.com/kentik/common/queue"
)

// Message represents an Interface changed message received from queue
type Message struct {
	queue.ConsumerMessage
	InterfacePoll InterfacePoll
}
