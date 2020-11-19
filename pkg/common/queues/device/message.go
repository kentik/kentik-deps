package device

import (
	"github.com/kentik/common/queue"
)

// MessageAction represents a message action
// - these exist in code-only
type MessageAction int

var (
	DeviceUpsertAction MessageAction = 1
	DeviceDeleteAction MessageAction = 2
	SiteUpsertAction   MessageAction = 3
	SiteDeleteAction   MessageAction = 4
	PlanDeleteAction   MessageAction = 5
)

// Message represents a Device changed message received from queue
type Message struct {
	queue.ConsumerMessage
	Device Device

	// for device/plan/site deletes, CompanyID will be present, as will the appropriate DeviceID/PlanID/SiteID
	Action MessageAction
}
