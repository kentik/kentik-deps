package cloud

import (
	"strings"
)

// EntityLookupPairingSample defines a map of source entity names (as tagging lookup) to multiple destination entity names,
// along with their sampled byte counts.
type EntityLookupPairingSample map[uint32]map[uint32]ByteCount

// EntityFlowStorageMethod describes how a cloud stores entities in a flow records
type EntityFlowStorageMethod uint8

// UnknownEntityFlowStorageMethod is the default value for CloudEntityFlowStorageMethod
var UnknownEntityFlowStorageMethod EntityFlowStorageMethod = 0

// LookupPopulators describes the cloud entity flow storage method where the entities are stored
// in string populators as uint32s written to custom columns, to be looked up by the tagging lookup client, via tag-api
var LookupPopulators EntityFlowStorageMethod = 1

// StringUDRColumns descirbes the cloud entity flow storage method where entities are stored as strings
// directly in UDR/flex columns, STR**
var StringUDRColumns EntityFlowStorageMethod = 2

// EntityFlowStorageMethodForCloud returns the entity flow storage method for the input cloud
func EntityFlowStorageMethodForCloud(cloudType CloudType) EntityFlowStorageMethod {
	switch cloudType {
	case AWS, Azure:
		return LookupPopulators
	case GCP, IBM:
		return StringUDRColumns
	default:
		return UnknownEntityFlowStorageMethod
	}
}

// CloudTypeFromString returns the cloud type based on the input string
func CloudTypeFromString(cloudType string) CloudType {
	switch strings.ToUpper(cloudType) {
	case "AWS":
		return AWS
	case "AZURE":
		return Azure
	case "IBM":
		return IBM
	case "GCP":
		return GCP
	default:
		return UnknownCloudType
	}
}

// CloudEntityTypeFromString returns the cloud entity type based on the input string
func CloudEntityTypeFromString(cloudEntityType string) CloudEntityType {
	switch strings.ToUpper(cloudEntityType) {
	case "REGION":
		return Region
	case "VPC":
		return VPC
	case "SUBNET":
		return Subnet
	case "VM":
		return VM
	default:
		return UnknownEntityType
	}
}
