package kt

import (
	"github.com/kentik/eggs/pkg/properties"
)

const (
	PROPERTY_PLATFORM_NEXT_DOWNLOAD         = "mitigate.platform.nextdownloadtimeout"
	PROPERTY_PLATFORM_NEXT_DOWNLOAD_ERR     = "mitigate.platform.nextdownloadtimeouterror"
	PROPERTY_NOTIFICATIONS_DEBUGGING_TARGET = "notifications.debugging.target"

	// Alarms and mitigations
	PropertyLimitActiveAlarmsTotal      = "activate.limits.active_alarms_total"
	PropertyLimitActiveAlarmsForCompany = "activate.limits.active_alarms_for_company"
	PropertyLimitMitigationsTotal       = "mitigate.limits.mitigations_total"
	PropertyLimitMitigationsThisRun     = "mitigate.limits.mitigations_this_run"

	// mit extended key rollout
	PropertyMitigateExtendedKeyEnabledAboveCid = "mitigate.extendedkey.enabled_above_cid"

	// baseline
	PropertyBaselineFrontfillSavePeriod = "baseline.frontfill.save_period"

	PropertyBulkChunkSizeForNodeDevicesBulkRequest = "streaming.node_client.devices_chunk_size"

	PropertyConfigDigestRefreshPeriodMinutes = "streaming.config_digest.refresh_period_minutes"
)

// Cleaner properties
const (
	// global retention settings (activate, mitigate) in days
	PropertyCleanerActivateLiveRetentionDays    = "streaming.cleaner.retention.live_days"
	PropertyCleanerActivateHistoryRetentionDays = "streaming.cleaner.retention.history_days"

	PropertyCleanerActivateAlertMatchHistoryRetentionDays = "streaming.cleaner.retention.alert_match_history_days"
	PropertyCleanerActivateAlertAlarmRetentionDays        = "streaming.cleaner.retention.alert_alarm_days"
	PropertyCleanerActivateAlertAlarmHistoryRetentionDays = "streaming.cleaner.retention.alert_alarm_history_days"
	PropertyCleanerActivateBaselineRetentionDays          = "streaming.cleaner.retention.baseline_days"
	PropertyCleanerActivateBaselineJobRetentionDays       = "streaming.cleaner.retention.baseline_job_days"

	PropertyCleanerIntervalMs = "streaming.cleaner.interval_ms"
	PropertyCleanerPauseMs    = "streaming.cleaner.pause_ms"

	// limit auto adjust tunables
	PropertyCleanerAdjTargetPauseMs = "streaming.cleaner.adj.target_pause_ms"
	PropertyCleanerAdjUp            = "streaming.cleaner.adj.up"
	PropertyCleanerAdjDown          = "streaming.cleaner.adj.down"

	// initial row deletion targets
	PropertyCleanerRowsActivateAlarmInit             = "streaming.cleaner.rows.alarm.init"
	PropertyCleanerRowsActivateBaselineInit          = "streaming.cleaner.rows.baseline.init"
	PropertyCleanerRowsActivateAlarmHistoryInit      = "streaming.cleaner.rows.alarm_history.init"
	PropertyCleanerRowsActivateAlarmMatchHistoryInit = "streaming.cleaner.rows.alarm_match_history.init"
	PropertyCleanerRowsMitigateMachineInit           = "streaming.cleaner.rows.mitigation_machine.init"
	PropertyCleanerRowsMitigateMachineLogInit        = "streaming.cleaner.rows.mitigation_machine_log.init"
)

// Default properties for the alerting backend (can be overridden by env vars or filesystem entries).
var DefaultAlertingProperties properties.PropertyBacking

// Initialize DefaultAlertingProperties
func init() {
	defaultProps := make(map[string]string)

	// features that are globally enabled by default
	for _, featureName := range FeaturesEnabledByDefault {
		defaultProps["features."+featureName+"-global"] = "true"
	}

	DefaultAlertingProperties = properties.NewStaticMapPropertyBacking(defaultProps)
}
