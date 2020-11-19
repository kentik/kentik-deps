package kt

import (
	"encoding/json"
	"strings"
	"time"
)

// An AlertMatchHistoryRow is a row in chalert/mn_alert_match_history.
// This struct is used for writing, so we don't make the fields nullable,
// even though they are nullable.
type AlertMatchHistoryRow struct {
	ID              int       `db:"id"`
	AlertID         PolicyID  `db:"alert_id"`         // nullable
	CompanyID       Cid       `db:"company_id"`       // nullable
	ThresholdID     Tid       `db:"threshold_id"`     // nullable
	AlarmID         AlarmID   `db:"alarm_id"`         // nullable
	AlertKey        string    `db:"alert_key"`        // nullable
	AlertDimension  string    `db:"alert_dimension"`  // nullable
	AlertMetric     string    `db:"alert_metric"`     // nullable
	AlertValue      float64   `db:"alert_value"`      // nullable
	AlertValue2nd   float64   `db:"alert_value2nd"`   // nullable
	AlertValue3rd   float64   `db:"alert_value3rd"`   // nullable
	AlertBaseline   float64   `db:"alert_baseline"`   // nullable
	BaselineUsed    int       `db:"baseline_used"`    // nullable
	LearningMode    bool      `db:"learning_mode"`    // nullable, actually tinyint(1) not bool
	DebugMode       int       `db:"debug_mode"`       // nullable
	Ctime           time.Time `db:"ctime"`            // default current_timestamp()
	CurrentPosition int       `db:"current_position"` // nullable
	HistoryPosition int       `db:"history_position"` // nullable, default 0
}

// An AlertAlarmRow is a row in chalert/mn_alert_alarm.
// This struct is used for writing, so we don't make the fields nullable,
// even though they are nullable.
type AlertAlarmRow struct {
	ID               AlarmID      `db:"id"`
	AlarmState       string       `db:"alarm_state"`
	AlertID          PolicyID     `db:"alert_id"`           // nullable
	MitigationID     MitigationID `db:"mitigation_id"`      // nullable
	CompanyID        Cid          `db:"company_id"`         // nullable
	ThresholdID      Tid          `db:"threshold_id"`       // nullable
	AlertKey         string       `db:"alert_key"`          // nullable
	AlertDimension   string       `db:"alert_dimension"`    // nullable
	AlertMetric      string       `db:"alert_metric"`       // nullable
	AlertValue       float64      `db:"alert_value"`        // nullable
	AlertValue2nd    float64      `db:"alert_value2nd"`     // nullable
	AlertValue3rd    float64      `db:"alert_value3rd"`     // nullable
	AlertMatchCount  int          `db:"alert_match_count"`  // nullable
	AlertBaseline    float64      `db:"alert_baseline"`     // nullable
	AlertSeverity    string       `db:"alert_severity"`     // nullable
	BaselineUsed     int          `db:"baseline_used"`      // nullable
	LearningMode     bool         `db:"learning_mode"`      // nullable, actually tinyint(1) not bool
	DebugMode        int          `db:"debug_mode"`         // nullable
	Ctime            time.Time    `db:"ctime"`              // default current_timestamp()
	AlarmStart       time.Time    `db:"alarm_start"`        // default 0000-00-00 00:00:00
	AlarmEnd         time.Time    `db:"alarm_end"`          // default 0000-00-00 00:00:00
	NotifyStart      time.Time    `db:"notify_start"`       // default 0000-00-00 00:00:00
	NotifyEnd        time.Time    `db:"notify_end"`         // default 0000-00-00 00:00:00
	AlarmLastComment string       `db:"alarm_last_comment"` // nullable
	LastMatchTime    time.Time    `db:"last_match_time"`    // default 0000-00-00 00:00:00
	MitigateStart    time.Time    `db:"mitigate_start"`     // default 0000-00-00 00:00:00
	MitigateEnd      time.Time    `db:"mitigate_end"`       // default 0000-00-00 00:00:00
}

// Possible enum values for AlertAlarmRow.AlarmState.
const (
	AlarmStateClear  = "CLEAR"
	AlarmStateAckReq = "ACK_REQ"
	AlarmStateAlarm  = "ALARM"
)

// An AlertAlarmHistoryRow is a row in chalert/mn_alert_alarm_history.
// This struct is used for writing, so we don't make the fields nullable,
// even though they are nullable.
type AlertAlarmHistoryRow struct {
	ID               int          `db:"id"`
	AlarmID          AlarmID      `db:"alarm_id"`           // nullable
	AlarmHistoryType string       `db:"alarm_history_type"` // nullable
	OldAlarmState    string       `db:"old_alarm_state"`
	NewAlarmState    string       `db:"new_alarm_state"`
	AlertID          PolicyID     `db:"alert_id"`          // nullable
	MitigationID     MitigationID `db:"mitigation_id"`     // nullable
	CompanyID        Cid          `db:"company_id"`        // nullable
	ThresholdID      Tid          `db:"threshold_id"`      // nullable
	AlertKey         string       `db:"alert_key"`         // nullable
	AlertDimension   string       `db:"alert_dimension"`   // nullable
	AlertMetric      string       `db:"alert_metric"`      // nullable
	AlertValue       float64      `db:"alert_value"`       // nullable
	AlertValue2nd    float64      `db:"alert_value2nd"`    // nullable
	AlertValue3rd    float64      `db:"alert_value3rd"`    // nullable
	AlertMatchCount  int          `db:"alert_match_count"` // nullable
	AlertBaseline    float64      `db:"alert_baseline"`    // nullable
	AlertSeverity    string       `db:"alert_severity"`    // nullable
	BaselineUsed     int          `db:"baseline_used"`     // nullable
	LearningMode     bool         `db:"learning_mode"`     // nullable, actually tinyint(1) not bool
	DebugMode        int          `db:"debug_mode"`        // nullable
	Ctime            time.Time    `db:"ctime"`             // default current_timestamp()
	Comment          string       `db:"comment"`           // nullable
	AlarmStartTime   time.Time    `db:"alarm_start_time"`  // default 0000-00-00 00:00:00
}

// AlarmHistoryEvent denotes reason for alarm state change
type AlarmHistoryEvent string

// Possible enum values for AlertAlarmHistoryRow.AlarmHistoryType.
const (
	AlarmHistoryStart      = "START"
	AlarmHistoryEscalation = "ESCALATION"
)

// AlarmEvent is used for sending info to chnode about notifications.
// It's very large and re-used in activate and mitigate.
// FIXME: Consider breaking this up or simplifying the number of fields sent.
// For example, just send a mn_alert_alarm_history row or id.
// The JSON tags are for serialization to chnode, and the DB tags
// are for loading from chalertv3.mn_alert_alarm. Careful with the DB
// tags, as many of these columns are actually nullable.
type AlarmEvent struct {
	AlarmID              AlarmID              `db:"id" json:"AlarmID"`
	AlarmState           string               `db:"alarm_state" json:"AlarmState"`
	PolicyID             PolicyID             `db:"alert_id" json:"PolicyID"`
	CompanyID            Cid                  `db:"company_id" json:"CompanyID"`
	ThresholdID          Tid                  `db:"threshold_id" json:"ThresholdID"`
	AlertKey             string               `db:"alert_key" json:"AlertKey"`
	AlertDimension       string               `db:"alert_dimension" json:"AlertDimension"`
	AlertMetric          string               `db:"alert_metric" json:"AlertMetric"`
	AlertValue           float64              `db:"alert_value" json:"AlertValue"`
	AlertValueSecond     float64              `db:"alert_value2nd" json:"AlertValueSecond"`
	AlertValueThird      float64              `db:"alert_value3rd" json:"AlertValueThird"`
	AlertActivateCount   int                  `db:"alert_match_count" json:"AlertActivateCount"`
	AlertBaseline        float64              `db:"alert_baseline" json:"AlertBaseline"`
	ActivateSeverity     string               `db:"alert_severity" json:"ActivateSeverity"`
	BaselineUsedFallback int                  `db:"baseline_used" json:"BaselineUsedFallback"`
	LearningMode         bool                 `db:"learning_mode" json:"LearningMode"`
	DebugMode            bool                 `db:"debug_mode" json:"DebugMode"`
	NotifyStart          time.Time            `db:"notify_start" json:"NotifyStart"`
	NotifyEnd            time.Time            `db:"notify_end" json:"NotifyEnd"`
	AlarmStart           time.Time            `db:"alarm_start" json:"AlarmStart"`
	AlarmEnd             time.Time            `db:"alarm_end" json:"AlarmEnd"`
	LastActivate         time.Time            `db:"last_match_time" json:"LastActivate"`
	AlertFullKey         string               `json:"AlertFullKey"`
	MitigationID         MitigationID         `db:"mitigation_id" json:"MitigationID"`
	MitigationState      string               `json:"MitigationState"`
	PolicyName           string               `json:"PolicyName"`
	Comment              string               `db:"comment" json:"comment"`
	MitigationMethodID   MitigationMethodID   `json:"MitigationMethodID"`
	MitigationPlatformID MitigationPlatformID `json:"MitigationPlatformID"`
	MitigationPolicyID   MitigationPlatformID `json:"MitigationPolicyID"` // sic: "PolicyID" -- chnode APIs receiving this object expect this typo. Send both MitigationPlatformID and MitigationPolicyID.

	IsShadowPolicy bool `db:"-" json:"-"`
}

func (ae *AlarmEvent) GetDimensionMap() map[string]string {
	m := make(map[string]string)
	keys := strings.Split(ae.AlertDimension, KeyJoinToken)
	values := strings.Split(ae.AlertKey, KeyJoinToken)
	if len(keys) != len(values) {
		return m
	}
	for i := 0; i < len(keys); i++ {
		m[keys[i]] = values[i]
	}
	return m
}

func (ae *AlarmEvent) GetMetric(targetMetric string) float64 {
	var mets []string

	if err := json.Unmarshal([]byte(ae.AlertMetric), &mets); err != nil {
		return 0
	}

	if len(mets) == 0 {
		return 0
	}

	for i, met := range mets {
		if met == targetMetric {
			switch i {
			case 0:
				return ae.AlertValue
			case 1:
				return ae.AlertValueSecond
			case 2:
				return ae.AlertValueThird
			}
		}
	}

	return 0
}

// An AlertAlarmSmall is a row from mn_alert_alarm, with only the most critical
// columns.
type AlertAlarmSmall struct {
	ID            AlarmID   `db:"id"`
	AlarmState    string    `db:"alarm_state"`
	AlertKey      string    `db:"alert_key"`
	ThresholdID   Tid       `db:"threshold_id"`
	AlarmStart    time.Time `db:"alarm_start"`
	LastMatchTime time.Time `db:"last_match_time"`
}

type DeleteClearAlarmArgs struct {
	CompanyID   Cid      `db:"company_id"`
	PolicyID    PolicyID `db:"alert_id"`
	ThresholdID Tid      `db:"threshold_id"`
	ID          AlarmID  `db:"id"`
}

type UpdateAlarmLastMatchTimeArgs AlertAlarmRow
type UpdateAlarmForEscalationArgs AlertAlarmRow
type UpdateAlarmEndingArgs AlertAlarmRow
type InsertAlarmArgs AlertAlarmRow

type InsertAlarmHistoryArgs AlertAlarmHistoryRow
type InsertAlarmHistoryWithLastIDArgs AlertAlarmHistoryRow
type InsertAlarmHistoryCopyingStartArgs AlertAlarmHistoryRow

func StringSet(ss ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		set[s] = struct{}{}
	}
	return set
}

// AlarmStateTransition represents state transition of an alarm (i.e 'clear'->'alarm')
type AlarmStateTransition struct {
	State       string             `db:"new_alarm_state"`
	Timestamp   time.Time          `db:"ctime"`
	HistoryType AlarmHistoryEvent  `db:"alarm_history_type"`
	Severity    AlarmSeverityLevel `db:"alert_severity"`
	ThresholdID Tid                `db:"threshold_id"`
}

type TimeRange struct {
	Start time.Time
	End   time.Time
}

func (tr TimeRange) IsZero() bool {
	return tr.Start.IsZero() && tr.End.IsZero()
}

func (tr TimeRange) GetStart() time.Time {
	return tr.Start
}

var theDistantFuture = time.Date(9999, 12, 1, 23, 59, 0, 0, time.UTC)

func (tr TimeRange) GetEnd() time.Time {
	if tr.End.IsZero() {
		return theDistantFuture
	}
	return tr.End
}

// GetAlarmsFilter encapsulates params used to narrow result of FetchCompanyAlarms
type GetAlarmsFilter struct {
	CompanyID            Cid
	ExcludeUserDefined   bool
	IncludeInsightAlarms bool

	Alarms          []AlarmID
	States          []string
	Severities      []string
	PolicyIDs       []PolicyID
	IgnorePolicyIDs []PolicyID
	MitigationIDs   []MitigationID

	DeprecatedStart time.Time
	DeprecatedEnd   time.Time
	CreationRange   TimeRange
	StartRange      TimeRange
	EndRange        TimeRange

	DimensionFiltersDNF []map[string]string
}
