package summary

import (
	"fmt"
	"github.com/kentik/common/queues/topology/cloud"
)

// BuildTrafficMessageKey returns a key to use to store cloud traffic summary data in Kafka
func BuildTrafficMessageKey(companyID uint32, cloudType cloud.CloudType, entityType cloud.CloudEntityType, sourceEntity uint32, startTimeUnixSeconds int64, endTimeUnixSeconds int64) (string, error) {
	cloudStr := ""
	switch cloudType {
	case cloud.AWS:
		cloudStr = "aws"
	case cloud.Azure:
		cloudStr = "azure"
	case cloud.GCP:
		cloudStr = "gcp"
	case cloud.IBM:
		cloudStr = "ibm"
	default:
		return "", fmt.Errorf("Unknown/unhandled cloud type: %v", cloudType)
	}

	entityTypeStr := ""
	switch entityType {
	case cloud.Region:
		entityTypeStr = "region"
	case cloud.VPC:
		entityTypeStr = "vpc"
	case cloud.Subnet:
		entityTypeStr = "subnet"
	case cloud.VM:
		entityTypeStr = "vm"
	default:
		return "", fmt.Errorf("Error determining entity type: %v", entityType)
	}
	return fmt.Sprintf("type:traffic;cid:%d;cloud:%s;type:%s;src:%d;start:%d;end:%d;", companyID, cloudStr, entityTypeStr, sourceEntity, startTimeUnixSeconds, endTimeUnixSeconds), nil
}
