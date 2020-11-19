package kt

import "time"

type MitigationEventType string

const (
	ReCheckSyncTimeout                  = 60 * time.Second
	ReCheckAfterErrorSyncTimeout        = 5 * time.Minute
	TimeoutAfterA10SessionLimit         = 70 * time.Minute
	QuiesceDurationAfterMutationRequest = 10 * time.Second
	RadwareDefaultAttackStopGracePeriod = 1 * time.Hour

	MaxMitigationsPerPlatform = 1024

	MitigationsAckReqTimeout = 15 * 24 * time.Hour // drop mitigations been in ack req for too long
)

const (
	START_MITIGATION = "start_mitigation"
	STOP_MITIGATION  = "stop_mitigation"

	CLIENT_KEEP_ALIVE = 60 * time.Second

	WHITE_LIST_SPLIT = ","

	FALLBACK = "BASELINE_FALLBACK"

	A10_MODE_STATIC       = "static"
	A10_MODE_DYNAMIC      = "dynamic"
	A10_BGP_ANNOUNCE_NO   = "no"
	A10_BGP_ANNOUNCE_CIDR = "alerted_cidr"
	A10_BGP_ANNOUNCE_24   = "convert_to_24"

	METHOD_TCP   = "tcp"
	METHOD_UDP   = "udp"
	METHOD_ICMP  = "icmp"
	METHOD_OTHER = "other"

	METHOD_TCP_RADWARE   = "TCP"
	METHOD_UDP_RADWARE   = "UDP"
	METHOD_ICMP_RADWARE  = "ICMP"
	METHOD_OTHER_RADWARE = "OTHER"

	RTBH_LOCALPREF_DEFAULT     = 100
	FLOWSPEC_LOCALPREF_DEFAULT = 100

	MANUAL_MITIGATION_DUMMY_IP = "127.42.42.42" // from portal
)
