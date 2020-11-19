package kflowfield

import (
	"github.com/kentik/common/queue"
)

// MessageAction represents a message action
// - these exist in code-only
type MessageAction int

var (
	KFlowFieldUpsertAction MessageAction = 1
	KFlowFieldDeleteAction MessageAction = 2
)

// Message represents a KFlowField changed message received from queue
type Message struct {
	queue.ConsumerMessage
	Field KFlowField

	Action MessageAction
}
