package kt

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AlertPolicy is the top level configuration for an alert policy.
// This is a data struct loaded from ch_www/mn_alert_policy.
// As an overall object it is not serialized/deserialized,
// but we give it JSON tags matching its db tags for the benefit of
// validation/introspection.
// TODO(tjonak): check whether it's worth it to split AlertPolicy into db and computed part
type AlertPolicy struct {
	PolicyID       PolicyID `db:"id" json:"id"`
	CompanyID      Cid      `db:"company_id" json:"company_id"`
	UserID         uint64   `db:"user_id" json:"user_id"`
	DashboardID    int64    `db:"dashboard_id" json:"dashboard_id"`
	PolicyName     string   `db:"policy_name" json:"policy_name"`
	PolicyWindow   int      `db:"policy_window" json:"policy_window"`
	IsTop          bool     `db:"is_top" json:"is_top"`
	IsAutoCalc     bool     `db:"is_auto_calc" json:"is_auto_calc"`
	LimitNumber    int      `db:"limit_num" json:"limit_num"`
	StoreNumber    int      `db:"store_num" json:"store_num"`
	DimensionsJSON string   `db:"dimensions" json:"dimensions"` // text, JSON array of strings
	MetricJSON     string   `db:"metric" json:"metric"`         // text, JSON array of strings
	FiltersJSON    *string  `db:"filters" json:"filters"`       // json nullable, JSON
	MinTraffic     float64  `db:"min_traffic" json:"min_traffic"`
	// TODO(tjonak): this is deriveable from learn_mode_expire_time, check whether we can remove this field
	IsLearn bool `db:"learning_mode" json:"learning_mode"`
	// policy_kvs -- not currently used
	Status       string `db:"status" json:"status"`
	BaselineJSON string `db:"baseline" json:"baseline"` // JSON
	// TODO(tjonak): mode has no meaning in our code afaik, check whether we can remove that
	Mode                         string    `db:"mode" json:"mode"`
	Cdate                        time.Time `db:"cdate" json:"cdate"`
	Edate                        time.Time `db:"edate" json:"edate"`
	LearnExpire                  time.Time `db:"learning_mode_expire_date" json:"learning_mode_expire_date"`
	Description                  string    `db:"policy_description" json:"policy_description"`
	DimensionGroupingOptionsJSON string    `db:"primary_dimension" json:"primary_dimension"`
	DeviceSelectorJSON           string    `db:"selected_devices" json:"selected_devices"`
	// template_id -- not used in alerting backend
	// is_advanced_mode -- not used in alerting backend
	// template_parent_ids -- not used in alerting backend

	// Additional fields
	SelectedDevices *DeviceIDSet `db:"-" json:"-"`
	IsShadowPolicy  bool         `db:"-" json:"-"`
	ShadowCompanyID Cid          `db:"-" json:"-"`

	// Computed fields
	UnsortedDimensions []string    `db:"-" json:"-"`
	SortedDimensions   []string    `db:"-" json:"-"`
	Metric             []string    `db:"-" json:"-"`
	FilterRoot         *FilterRoot `db:"-" json:"-"`

	Baseline PolicyBaselineSettings `db:"-" json:"-"`

	DimensionGroupingOptions *DimensionGroupingOptions `db:"-" json:"-"`
	DeviceSelector           *DeviceSelector           `db:"-" json:"-"`
}

// GetDevices returns list of devices this policy is applicable to
func (ap *AlertPolicy) GetDevices() *DeviceIDSet {
	return ap.SelectedDevices
}

// GetDevicesMap returns mapping of policies this policy is applicable to
func (ap *AlertPolicy) GetDevicesMap() map[DeviceID]bool {
	result := map[DeviceID]bool{}

	for _, deviceID := range ap.GetDevices().Items() {
		result[deviceID] = true
	}

	return result
}

// Boolean check to see if this policy is a synthetic one or not.
func (ap *AlertPolicy) IsSynthetic() bool {
	return ap.DeviceSelector != nil && ap.DeviceSelector.Syn != nil
}

// ShadowPolicyBitMarker is a bit we set for Cids internally to indicate
// that we're talking about a shadow policy. This way when chnode
// directly queries the chalert database for normal alarms, these are not picked up.
// This concept should remain internal to alerting.
// We'll use the 30th bit because in the "chalert" database in mariadb,
// company ids are often stored as a 32bit signed integer. Setting the
// 30th bit will leave us with a big positive integer.
const ShadowPolicyBitMarker = 1 << 30

func (p *AlertPolicy) CompanyIDWithShadowBit() Cid {
	return CompanyIDWithShadowBit(p.CompanyID, p.IsShadowPolicy)
}

func CompanyIDWithShadowBit(cid Cid, isShadowPolicy bool) Cid {
	if isShadowPolicy {
		return cid | ShadowPolicyBitMarker
	}
	return cid & (^ShadowPolicyBitMarker)
}

// CompanyIDWithoutShadowBit strips shadow bit from cid if one is present
func CompanyIDWithoutShadowBit(cid Cid) Cid {
	if !CompanyIDHasShadowBit(cid) {
		return cid
	}
	return cid & (^ShadowPolicyBitMarker)
}

func CompanyIDHasShadowBit(cid Cid) bool { return cid&ShadowPolicyBitMarker == ShadowPolicyBitMarker }

// MinimumPolicyWindow is the min value for AlertPolicy.PolicyWindow.
const (
	MinimumPolicyWindowSec = 30
)

// FilterRoot is the JSON object stored in mn_alert_policy.filters that
// represents a set of filters to be applied to flow.
type FilterRoot struct {
	Connector    string        `json:"connector"`
	Custom       bool          `json:"custom"`
	FilterString string        `json:"filterString"`
	FilterGroups []FilterGroup `json:"filterGroups"`

	IsNot bool `json:"-"` // not part of json; stored in a separate column and added here.
}

// FilterGroup is a subsection of a FilterRoot.
type FilterGroup struct {
	Connector    string                 `json:"connector"`
	FilterString string                 `json:"filterString"`
	Filters      []Filter               `json:"filters"`
	SavedFilters []SavedFilterReference `json:"saved_filters"`
	FilterGroups []FilterGroup          `json:"filterGroups"`
	IsNot        bool                   `json:"not"`
}

// Filter is a subsection of a FilterGroup.
type Filter struct {
	FilterField string `json:"filterField"`
	Operator    string `json:"operator"`
	FilterValue string `json:"filterValue"`
}

// SavedFilterReference is a subsection of FilterGroup
type SavedFilterReference struct {
	FilterID SavedFilterID `json:"filter_id"`
	IsNot    bool          `json:"is_not"`
}

// SavedFilter represents entry form mn_saved_filter table
type SavedFilter struct {
	ID             SavedFilterID `db:"id"`
	Name           string        `db:"filter_name"`
	Description    string        `db:"filter_description"`
	FilterRootJSON string        `db:"filters"`
	CDate          time.Time     `db:"cdate"`
	EDate          time.Time     `db:"edate"`
}

// PolicyBaselineSettings is the JSON object stored in mn_alert_policy.baseline
// that defines settings for how we calculate baselines.
type PolicyBaselineSettings struct {
	HourAgg         string `json:"hour_agg"`
	LookbackSec     int    `json:"lookback_sec"`
	LookbackStepSec int    `json:"lookback_step_sec"`
	PointWidthSec   int    `json:"point_width_sec"`
	Point2ndAgg     string `json:"point_2nd_agg"`
	PointFinalAgg   string `json:"point_final_agg"`
	StartStep       int    `json:"start_step"`
	MinLookbackSec  int    `json:"min_lookback_sec"`
	WeekendAware    bool   `json:"weekend_aware"`
	// start_step_override? set by frontend but not used in backend
}

// Possible values for PolicyBaselineSettings.HourAgg/Point2ndAgg/PointFinalAgg
const (
	BaselineAggregateP05   = "REL_P05"
	BaselineAggregateP10   = "REL_P10"
	BaselineAggregateP25   = "REL_P25"
	BaselineAggregateP50   = "REL_P50"
	BaselineAggregateP75   = "REL_P75"
	BaselineAggregateP80   = "REL_P80"
	BaselineAggregateP90   = "REL_P90"
	BaselineAggregateP95   = "REL_P95"
	BaselineAggregateP98   = "REL_P98"
	BaselineAggregateP99   = "REL_P99"
	BaselineAggregateMax   = "REL_MAX"
	BaselineAggregateMin   = "REL_MIN"
	BaselineAggregateNone  = ""
	BaselineAggregateNone2 = "None"
)

// DefaultMinLookbackSec is used for PolicyBaselineSettings.MinLookback if none is set.
const DefaultMinLookbackSec = 60 * 60 * 24 * 4 // 4 days

// DimensionGroupingOptions is the JSON object stored in mn_alert_policy.primary_dimension
// that defines settings for grouping top keys.
type DimensionGroupingOptions struct {
	MaxPerGroup           int      `json:"MaxPerGroup"`
	GroupingDimensions    []string `json:"GroupingDimensions"`
	NumGroupingDimensions int      `json:"NumGroupingDimensions"` // Don't use this; use GroupingDimensions.
}

// DeviceSelector is the JSON object stored in mn_alert_policy.selected_devices
// defines set of devices monitored by policy
// Overloaded now to also carry some syn data.
type DeviceSelector struct {
	All    bool     `json:"all_devices"`
	Names  []string `json:"device_name"`
	Labels []int64  `json:"device_labels"`
	Sites  []int64  `json:"device_sites"`
	Types  []string `json:"device_types"`
	Syn    *SynTest `json:"device_synthetic"`
}

// This defines which synthetic tests are part of this policy.
type SynTest struct {
	Id       SynTestID `json:"id"`
	AgentIds []uint64  `json:"agent_ids"`
}

// FindPoliciesFilter set of criteria for AlertDataSource.FindPoliciesFunction
type FindPoliciesFilter struct {
	CompanyID      Cid
	IDs            []PolicyID
	CreateTimeLow  *time.Time
	CreateTimeHigh *time.Time
	ModifyTimeLow  *time.Time
	ModifyTimeHigh *time.Time
	Thresholds     []Tid
	Users          []UserID
	Dimensions     []string
	SynTests       []SynTestID

	Limit  uint64
	Offset uint64
}

type InactiveAlertPolicy struct {
	PolicyID PolicyID
	Edate    time.Time
}

func IAPsToString(as []InactiveAlertPolicy) string {
	ss := make([]string, len(as))
	for i := range as {
		ss[i] = strconv.Itoa(int(as[i].PolicyID))
	}
	return strings.Join(ss, ", ")
}

type InactiveAlertPolicies []InactiveAlertPolicy // Only provided for its String method. Otherwise just use []InactiveAlertPolicy.

func (as InactiveAlertPolicies) String() string { return IAPsToString([]InactiveAlertPolicy(as)) }

type CompanyFilterBase struct {
	ID     SavedFilterID
	Filter string
	IsNot  bool
	Edate  time.Time
}

// ThresholdData is the data needed in order to execute a Threshold.
type ThresholdData struct {
	Cid
	*AlertPolicy
	*Threshold
	*DeviceLabelsForCompany
	*InterfaceGroupsForCompany
	DebugKeys       []AlertingDebugKey
	ActivationTimes map[string][]int64
	Interfaces      []*Interface
	Devices
}

// NotificationChannelID denotes id of notification channel
type NotificationChannelID = uint64

// A Threshold is part of an Alert Policy that specifies conditions for which
// that policy creates alarms of a certain severity.
// This is a data struct loaded from the ch_www/mn_alert_threshold table.
// As an overall object it is not serialized/deserialized by chfactivate,
// but we give it JSON tags matching its db tags for the benefit
// of validation/introspection.
type Threshold struct {
	ThresholdID    Tid                `db:"id" json:"id"`
	CompanyID      Cid                `db:"company_id" json:"company_id"`
	PolicyID       PolicyID           `db:"policy_id" json:"policy_id"`
	Severity       string             `db:"severity" json:"severity"`     // string enum
	ConditionsJSON string             `db:"conditions" json:"conditions"` // JSON array
	ActivateJSON   string             `db:"activate" json:"activate"`     // JSON object
	CompareJSON    string             `db:"compare" json:"compare"`       // JSON object or "null"
	Direction      ThresholdDirection `db:"direction" json:"direction"`   // string enum
	Mode           string             `db:"mode" json:"mode"`             // string enum
	FallbackJSON   string             `db:"fallback" json:"fallback"`     // JSON
	Status         string             `db:"status" json:"status"`
	// Kvs interface{} `db:"kvs" json:"kvs"` // kvs hstore field, unused.
	Cdate       time.Time `db:"cdate" json:"cdate"`
	Edate       time.Time `db:"edate" json:"edate"`
	AckReq      bool      `db:"threshold_ack_required" json:"threshold_ack_required"`
	Description string    `db:"threshold_description" json:"threshold_description"` // Not used in backend
	FiltersJSON string    `db:"filters" json:"filters"`                             // JSON object or "null"
	Runbook     string    `db:"runbook" json:"runbook"`                             // Not used in backend

	// Computed fields
	Activate   ThresholdActivate    `db:"-" json:"-"`
	Compare    ThresholdCompare     `db:"-" json:"-"`
	Conditions []ThresholdCondition `db:"-" json:"-"`
	Fallback   ThresholdFallback    `db:"-" json:"-"`
	Filters    ThresholdFilter      `db:"-" json:"-"`

	NotificationChannels []NotificationChannelID `db:"-" json:"-"` // only used on create/update, not loaded
}

// ThresholdStatus is a status of a threshold
// TODO(tjonak): change field type in Threshold struct
type ThresholdStatus string

const (
	// ThresholdStatusActive denotes active threshold
	// To be fair we currently either have active threshold or it gets deleted
	ThresholdStatusActive = ThresholdStatus('A')
)

// ThresholdMode represents enum strings used in db.mn_alert_threshold.mode column
type ThresholdMode string

const (
	// ThresholdModeTopset represents threshold which includes topset condition
	ThresholdModeTopset = ThresholdMode("exist")
	// ThresholdModeBaseline represents threshold with baseline conditions
	ThresholdModeBaseline = ThresholdMode("baseline")
	// ThresholdModeStatic represents threshold with just static conditions
	ThresholdModeStatic = ThresholdMode("static")
)

// AlarmSeverityLevel denotes alarm severity level
type AlarmSeverityLevel string

// Severity Levels are the valid values for Threshold.Severity (mn_alert_threshold.severity).
const (
	SeverityLevelLowest   = "minor" // internal use only
	SeverityLevelMinor    = "minor"
	SeverityLevelMinor2   = "minor2"
	SeverityLevelMajor    = "major"
	SeverityLevelMajor2   = "major2"
	SeverityLevelCritical = "critical"
)

var (
	// SeverityLevels lists all severity levels by decreasing severity.
	SeverityLevels = []string{
		SeverityLevelCritical,
		SeverityLevelMajor2,
		SeverityLevelMajor,
		SeverityLevelMinor2,
		SeverityLevelMinor,
	}

	// SeverityLevelToInt maps severity levels such that
	// "higher" severity is numerically higher.
	SeverityLevelToInt = map[string]int{
		SeverityLevelMinor:    1,
		SeverityLevelMinor2:   2,
		SeverityLevelMajor:    3,
		SeverityLevelMajor2:   4,
		SeverityLevelCritical: 5,
	}

	// SeverityLevelFromInt maps severity levels such that
	// "higher" severity is numerically higher.
	SeverityLevelFromInt = map[int]string{
		1: SeverityLevelMinor,
		2: SeverityLevelMinor2,
		3: SeverityLevelMajor,
		4: SeverityLevelMajor2,
		5: SeverityLevelCritical,
	}
)

// ThresholdDirection denotes direction on comparision between baseline (history) and current values
type ThresholdDirection string

// Directions are the valid values for Threshold.Direction, indicating how we
// compare current data with baseline data.
const (
	DirectionCurrentToHistory = ThresholdDirection("current_to_history")
	DirectionHistoryToCurrent = ThresholdDirection("history_to_current")
)

// ThresholdActivate is JSON loaded from ch_www/mn_alert_threshold.activate.
// It specifies how many times a threshold has to match before generating
// an alarm.
type ThresholdActivate struct {
	Operator   string `json:"operator"`
	TimeClear  int    `json:"gracePeriod"`
	TimeUnit   string `json:"timeUnit"`
	TimeWindow int    `json:"timeWindow"`
	Times      int    `json:"times"`
}

// ThresholdCompare is JSON loaded from ch_www/mn_alert_threshold.compare.
// It specifies how many keys are stored in the baseline and how many
// keys are looked at each tick.
type ThresholdCompare struct {
	AutoCalc       bool `json:"auto_calc"`
	ThresholdStore int  `json:"threshold_store"`
	ThresholdTop   int  `json:"threshold_top"`
}

// ThresholdCondition is an element of the JSON array loaded
// from ch_www/mn_alert_threshold.condition.
// It specifies a condition which will trigger a match for this threshold.
type ThresholdCondition struct {
	Metric      string  `json:"metric"`
	Operator    string  `json:"operator"`
	Type        string  `json:"type"`
	Value       float64 `json:"value"`
	ValueSelect string  `json:"value_select"`
	ValueType   string  `json:"value_type"`
}

// GetMetric returns metric of this threshold (if applicable)
func (tc *ThresholdCondition) GetMetric(metrics ...string) (r Metric) {
	if len(tc.Metric) > 0 {
		return Metric(tc.Metric)
	}

	getChecked := func(i int) Metric {
		if len(metrics) < i+1 {
			return Metric("")
		}
		return Metric(metrics[i])
	}

	switch MetricSelector(tc.ValueSelect) {
	case MetricSelector0:
		r = getChecked(0)
	case MetricSelector1:
		r = getChecked(1)
	case MetricSelector2:
		r = getChecked(2)
	}

	return
}

// Possible values for ThresholdCondition.Type.
const (
	ActivateTypeBasic          = "basic"
	ActivateTypeAdvanced       = "advanced"
	ActivateTypeInterfaceMixed = "interface_capacity"
)

// ActivateTypePriority is the relative priority of each type of condition.
// Conditions will all be sorted based on this priority.
var ActivateTypePriority = map[string]int{
	ActivateTypeBasic:          0,
	ActivateTypeInterfaceMixed: 10,
	ActivateTypeAdvanced:       100,
}

// Possible values for ThresholdCondition.ValueType.
const (
	ActivateValueTypeNone           = ""
	ActivateValueTypeUnit           = "unit"
	ActivateValueTypePercent        = "percent"
	ActivateValueTypeKeyNotInTopset = "key_not_in_topset"
	ActivateValueTypeTopsetNotInKey = "topset_not_in_key"
	ActivateValueTypeDirect         = "direct"
)

// ActivateValueTypes is a set of all the possible values for ThresholdCondition.ValueType.
var ActivateValueTypes = StringSet(
	ActivateValueTypeUnit,
	ActivateValueTypePercent,
	ActivateValueTypeKeyNotInTopset,
	ActivateValueTypeTopsetNotInKey,
	ActivateValueTypeDirect,
)

// ThresholdFallback is JSON loaded from ch_www/mn_alert_threshold.fallback.
// It specifies what to do when baseline data is missing.
type ThresholdFallback struct {
	MissOp  string `json:"miss_op"`
	MissVal uint64 `json:"miss_val"`
}

// Possible values for ThresholdFallback.MissOp.
const (
	BaselineMissingSkip     = "skip"
	BaselineMissingTrigger  = "trigger"
	BaselineMissingLowest   = "lowest"
	BaselineMissingHighest  = "highest"
	BaselineMissingUseValue = "use_this_value"
	BaselineMissingUnset    = ""
)

// BaselineMissingValues is the set of possible values for ThresholdFallback.MissOp.
var BaselineMissingValues = StringSet(
	BaselineMissingSkip,
	BaselineMissingTrigger,
	BaselineMissingLowest,
	BaselineMissingHighest,
	BaselineMissingUseValue,
	BaselineMissingUnset,
)

// ThresholdFilter is JSON loaded from ch_www/mn_alert_threshold.filters.
// It specifies threshold-level key filtering settings.
type ThresholdFilter struct {
	EntrySets []ThresholdFilterConjunction `json:"entry_sets"`
	IsInclude bool                         `json:"is_include"`
}

// ThresholdFilterConjunction is part of a ThresholdFilter.
type ThresholdFilterConjunction struct {
	Entries []ThresholdFilterTerm `json:"entries"`
}

// ThresholdFilterTerm is part of a ThresholdFilterConjunction.
type ThresholdFilterTerm struct {
	Dimension string      `json:"dimension"`
	Value     interface{} `json:"value"`         // Must be string, int, []string, or []int
	Not       bool        `json:"not,omitempty"` // inverts the condition if true.
}

func (term *ThresholdFilterTerm) UnmarshalJSON(b []byte) error {
	type alias ThresholdFilterTerm
	var t alias
	err := json.Unmarshal(b, &t)
	if err != nil {
		return err
	}
	*term = ThresholdFilterTerm(t)

	// We want to convert []interface{} => []int, []string.
	switch v := term.Value.(type) {
	// Types as in encoding.json.Unmarshal docs: bool, float64, string, []interface{}, map[string]interface{}, nil
	case bool:
		return fmt.Errorf("ThresholdFilterTerm.UnmarshalJSON: unexpected value type bool")
	case float64:
		term.Value = int(v) // We are expecting integers only.
	case string: // OK
	case []interface{}:
		if len(v) <= 0 {
			return fmt.Errorf("ThresholdFilterTerm.UnmarshalJSON: len(Value) <= 0")
		}
		switch v[0].(type) {
		case int:
			arr := make([]int, len(v))
			for i, elI := range v {
				el, ok := elI.(int)
				if !ok {
					return fmt.Errorf("ThresholdFilterTerm.UnmarshalJSON: unexpected heterogeneous value array")
				}
				arr[i] = el
			}
			term.Value = arr
		case string:
			arr := make([]string, len(v))
			for i, elI := range v {
				el, ok := elI.(string)
				if !ok {
					return fmt.Errorf("ThresholdFilterTerm.UnmarshalJSON: unexpected heterogeneous value array")
				}
				arr[i] = el
			}
			term.Value = arr
		default:
			return fmt.Errorf("ThresholdFilterTerm.UnmarshalJSON: unexpected value array type")
		}
	case map[string]interface{}:
		return fmt.Errorf("ThresholdFilterTerm.UnmarshalJSON: unexpected value type map")
	default:
		return fmt.Errorf("ThresholdFilterTerm.UnmarshalJSON: unexpected value type: %v", v)
	}
	return nil
}

// An AlertingDebugKey is a row in mn_alert_debug_key.
type AlertingDebugKey struct {
	CompanyID   Cid       `db:"company_id"`
	PolicyID    *PolicyID `db:"policy_id"`    // nullable
	ThresholdID *Tid      `db:"threshold_id"` // nullable
	Type        string    `db:"alert_type"`
	KeyJSON     string    `db:"alert_key"` // JSON, []AlertingDebugTableEntry

	// Computed fields
	Key []AlertingDebugTableEntry
}

// Possible values for AlertingDebugKey.Type
const (
	DebugKeyTypeDimensionFull    = "full"
	DebugKeyTypeDimensionPartial = "partial"
)

// An AlertingDebugTableEntry is a Dimension/Value pair.
type AlertingDebugTableEntry struct {
	Dimension string `json:"dimension"`
	Value     string `json:"value"`
}

// Metric is an alias type to discriminate single metrics
type Metric string

// Metrics represents set of metrics used by given policy
// This list is ordered, first metrics is the primary one
// TODO(tjonak): validate using parser/metric package
type Metrics []Metric

// MetricSelector is used to pick metric in positional fashion from kflow
type MetricSelector string

const (
	MetricSelector0 = MetricSelector(METRIC_FIRST)
	MetricSelector1 = MetricSelector(METRIC_SECOND)
	MetricSelector2 = MetricSelector(METRIC_THIRD)
)

// NewMetrics constructs metrics instance
func NewMetrics(metrics []string) (res Metrics, err error) {
	res = NewMetricsWithoutValidation(metrics)
	err = res.Validate()

	return
}

// NewMetricsWithoutValidation constructs metrics but doesn't touch internal state
// usefull for fetching stuff from db
func NewMetricsWithoutValidation(metrics []string) (res Metrics) {
	for _, metric := range metrics {
		res = append(res, Metric(metric))
	}
	return
}

// Validate checks whether preconditions for Metrics are fulfilled
func (m Metrics) Validate() error {
	if len(m) > 3 {
		return fmt.Errorf("at most 3 metrics are supported")
	}

	if len(m) < 1 {
		return fmt.Errorf("At least one metric is required")
	}

	return nil
}

// Primary returns primary metric
func (m Metrics) Primary() (Metric, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}

	return m[0], nil
}

var metricSelectorList = []MetricSelector{METRIC_FIRST, METRIC_SECOND, METRIC_THIRD}

// ToValueSelectLookup constructs MetricSelector lookup based on metric set
func (m Metrics) ToValueSelectLookup() map[Metric]MetricSelector {
	result := make(map[Metric]MetricSelector, len(m))

	for i := range m {
		if i >= len(metricSelectorList) {
			break
		}
		result[m[i]] = metricSelectorList[i]
	}

	return result
}

// MetricType denotes metric identificator
type MetricType string

// AppProtocolMetric descirbes entry in chww.mn_lookup_app_protocol_cols for purpose of enabling metrics
type AppProtocolMetric struct {
	Protocol            string     `db:"app_protocol"`
	ProtocolDisplayName string     `db:"display_name"`
	ColumnName          string     `db:"custom_column"`
	MetricType          MetricType `db:"metric_type"`
	Description         string     `db:"dimension_label"`
}

// SQLColumn denotes column which gonna get updated by this term
type SQLColumn string

// UpdateTuple stores parts of update statement and it's args, like ("policy_name","new name")
type UpdateTuple struct {
	Column SQLColumn
	Arg    interface{}
}

// PolicyUpdateTuple typed UpdateTuple for policies
type PolicyUpdateTuple UpdateTuple

// PolicyUpdateBundle typed PolicyUpdateTuple list
type PolicyUpdateBundle []*PolicyUpdateTuple

// SavedFilterUpdateTuple typed UpdateTuple for policies
type SavedFilterUpdateTuple UpdateTuple

// SavedFilterUpdateBundle typed SavedFilterUpdateTuple list
type SavedFilterUpdateBundle []*SavedFilterUpdateTuple

// DeviceIDGetter knows how to retrieve device name from flow
type DeviceIDGetter func(*Flow) DeviceID

// InterfaceGetter knows how to retrieve interface id from flow
type InterfaceGetter func(*Flow) IfaceID

// SNMPBundle holds data about columns used by SNMP based calculations
type SNMPBundle struct {
	DeviceNameColumn string `json:"device"`
	InterfaceColumn  string `json:"interface"`
	CapacityColumn   string `json:"interface_capacity"`
}

// FindSavedFiltersCriteria is a set of criteria for AlertDS.FindSavedFilters
type FindSavedFiltersCriteria struct {
	IDs               []SavedFilterID
	Names             []string
	CreateStart       time.Time
	CreateEnd         time.Time
	ModifyStart       time.Time
	ModifyEnd         time.Time
	IncludePredefined bool
	Limit             uint64
	Offset            uint64
}
