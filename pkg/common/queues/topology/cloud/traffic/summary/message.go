package summary

import (
	"github.com/kentik/common/queue"
	"github.com/kentik/common/queues/topology/cloud/traffic"
)

// Message represents a new/updated topology hierarchy entity
type Message struct {
	queue.ConsumerMessage
	CompanyCloudTrafficSample traffic.CompanyCloudTrafficSample
}
