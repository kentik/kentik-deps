package kt

import (
	"time"
)

const (
	ChanSlackSmall = 100
	ChanSlack      = 10000
	ChanSlackLarge = 50000

	LocalIpAddr = "127.0.0.1"

	AllDeviceID = DeviceID(100)

	// TODO(tjonak): unify under kt.PolicyStatus type
	ActiveStatus       = "A"
	AlertStatusRemoved = "R"

	KeyJoinToken        = ":"
	DimensionKeyJoin    = ":"
	DimensionKeyReToken = "__$_kntq$__"

	HttpHealthCheckPath      = "/check"
	HttpAlertInboundPath     = "/chf"
	HttpGetBlPath            = "/bl"
	HttpGetLastTopPath       = "/last"
	HttpIntrospectPolicyPath = "/introspect/policy"
	HttpPolicyID             = "aid"
	HttpCompanyID            = "sid"
	HttpAlertKey             = "key"
	HttpAlertLimit           = "limit"

	DefaultTimeClear   = 20
	DefaultDashboardID = 49

	BaselineDuration                = 1 * time.Hour
	BaselineMaxValues               = 60
	BaselineFrontfill               = "frontfill"
	BaselineBackfill                = "backfill"
	BaselineFrontfillPrio           = 100
	BaselineStoreNumberFudgeFactor  = 1.2 // Store a little more baseline than the user requests.
	BaselineGaugeMetricUpdatePeriod = 10 * time.Second
	BaselineSwapDuration            = 60 * time.Minute
	BaselineCheckDuration           = 10 * time.Second

	EnvChalertRO = "DB_CHALERT_RO"
	EnvChalertRW = "DB_CHALERT_RW"
	EnvChwwwRO   = "DB_CHWWW_RO"
	EnvChwwwRW   = "DB_CHWWW_RW"

	EnvCidWhitelist               = "CID_WHITELIST"
	EnvCidBlacklist               = "CID_BLACKLIST"
	EnvConductorListenAddr        = "KENTIK_CONDUCTOR_LISTEN_ADDR"
	EnvConductorListenAddrDefault = "0.0.0.0:9460"
	EnvChnodeSystemUserEmail      = "CHNODE_SYSTEM_USER_EMAIL"
	EnvChnodeUrl                  = "CHNODE_URL"
	EnvNotifyAPIGRPCAddress       = "K_NOTIFY_API_GRPC_ADDRESS"
	EnvNotifyAPIGRPCInsecure      = "K_NOTIFY_API_GRPC_INSECURE"
	EnvZKServers                  = "K_ZK_SERVERS"
	EnvZKSessionTimeout           = "K_ZK_SESSION_TIMEOUT"
	EnvZKRootPath                 = "K_ZK_ROOT_PATH"
	EnvZKRetryDuration            = "K_ZK_RETRY_DURATION"
	EnvBackfillNumNodesDefault    = "K_BACKFILL_NUM_NODES_DEFAULT"
	EnvKafkaBrokers               = "K_KAFKA_BROKERS"
	EnvKafkaVersion               = "K_KAFKA_VERSION"
	EnvKafkaClientID              = "K_KAFKA_CLIENT_ID"
	EnvKafkaProducerTimeout       = "K_KAFKA_PRODUCER_TIMEOUT"
	EnvKafkaProducerRetries       = "K_KAFKA_PRODUCER_RETRIES"
	EnvKafkaProducerBackoff       = "K_KAFKA_PRODUCER_BACKOFF"
	EnvBackfillConsumerGroupID    = "K_BACKFILL_CONSUMER_GROUP_ID"
	EnvBackfillSlotJobTopic       = "K_BACKFILL_SLOT_JOB_TOPIC"
	EnvBackfillPrio               = "K_BACKFILL_PRIO"
	EnvBackfillMaxParallelism     = "K_BACKFILL_MAX_PARALLELISM"
	EnvBackfillKafkaRetryDuration = "K_BACKFILL_KAFKA_RETRY_DURATION"
	EnvQueryRelayBaseURL          = "K_QUERY_RELAY_URL"
	EnvQueryRelayOverallTimeout   = "K_QRC_OVERALL_TIMEOUT"
	EnvBackfillNoZKRun            = "K_BACKFILL_NO_ZK_RUN"
	EnvSynServiceAddr             = "K_SYNTHETICS_GRPC_ADDRESS"
	EnvSynServicePEMFile          = "K_SYNTHETICS_GRPC_PEM"

	KafkaAlertAPIClientID          = "kentik-alert-api"
	KafkaBaselineClientID          = "kentik-chfbaseline"
	BackfillConsumerGroupIDDefault = "kentik.alert.baseline.backfill.cg"
	BackfillSlotJobTopicDefault    = "kentik.alert.baseline.backfill.slotjobs"

	ZKSessionTimeoutDefault = 10 * time.Second

	EnableLightStepTracingEnvVar  = "K_TRACING_LIGHTSTEP_ENABLED"
	EnableLightStepTracingDefault = false
	LightStepTracingTokenEnvVar   = "K_TRACING_LIGHTSTEP_TOKEN"
	LightStepTracingTokenDefault  = ""
	LightstepTracingHostEnvVar    = "K_TRACING_LIGHTSTEP_HOST"
	LightstepTracingHostDefault   = ""
	LightstepTracingPortEnvVar    = "K_TRACING_LIGHTSTEP_PORT"
	LightstepTracingPortDefault   = 0
)
