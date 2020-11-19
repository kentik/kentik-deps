package kt

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kentik/chf-alert/pkg/eggs/limits"
	"github.com/kentik/chf-alert/pkg/eggs/state"
)

const (
	AutoMitigation   = "auto"
	ManualMitigation = "manual"
)

// A MitigationPlatform is our configuration entity representing a
// remote device (e.g. a Radware router, or a set of routers we peer with over bgp).
// It corresponds to a row in mn_mitigation_platform.
// This model isn't intended to be serialized/deserialized, but it does have
// JSON struct tags for its database fields, for the benefit of validation.
type MitigationPlatform struct {
	// Database fields
	PlatformID          MitigationPlatformID `db:"id" json:"id"`                                                               // mn_mitigation_platform.id
	PlatformName        string               `db:"platform_name" json:"platform_name"`                                         // mn_mitigation_platform.platform_name
	PlatformDescription string               `db:"platform_description" json:"platform_description"`                           // mn_mitigation_platform.platform_description
	MitigationType      MitigationType       `db:"platform_mitigation_device_type" json:"platform_mitigation_device_type"`     // mn_mitigation_platform.platform_mitigation_device_type
	RawOptions          string               `db:"platform_mitigation_device_detail" json:"platform_mitigation_device_detail"` // mn_mitigation_platform.platform_mitigation_device_detail
	Edate               time.Time            `db:"edate" json:"edate"`                                                         // mn_mitigation_platform.edate
	Cdate               time.Time            `db:"cdate" json:"cdate"`                                                         // mn_mitigation_platform.cdate
	// TODO(tjonak): there's also a status field, which we prolly ignore right now

	// Computed fields
	Options interface{} `json:"-"` // RawOptions deserialized
}

type A10PlatformOptions struct {
	IPAddress                     string `json:"ipAddress"`
	APILogin                      string `json:"apiLogin"`
	APIPassword                   string `json:"apiPassword"`
	DeleteFromRemoteIfNotInKentik bool   `json:"deleteFromA10IfNotInKentik"`
}

type RadwarePlatformOptions struct {
	IPAddress                     string `json:"ipAddress"`
	APILogin                      string `json:"apiLogin"`
	APIPassword                   string `json:"apiPassword"`
	DeleteFromRemoteIfNotInKentik bool   `json:"deleteFromRadwareIfNotInKentik"`
}

type CFMTPlatformOptions struct {
	AccountID   string `json:"accountID"`
	APILogin    string `json:"apiLogin"`
	APIPassword string `json:"apiPassword"`
}

type RTBHPlatformOptions struct {
	Devices *DeviceIDSet `json:"devices"`
}

// MitigationType denotes particular type of mitigation used
type MitigationType string

const (
	// MitigationTypeFlowspec denotes Flowspec mitigation type
	MitigationTypeFlowspec = MitigationType("Flowspec")
	// MitigationTypeRTBH denotes RTBH mitigation type
	MitigationTypeRTBH = MitigationType("RTBH")
	// MitigationTypeRadware denotes Radware mitigation type
	MitigationTypeRadware = MitigationType("Radware")
	// MitigationTypeA10 denotes A10 mitigation type
	MitigationTypeA10 = MitigationType("A10")
	// MitigationTypeCFMT denotes cloudflare magictransit mitigation type
	MitigationTypeCFMT = MitigationType("CFMT")
	// MitigationTypeDummy denotes dummy mitigation type
	MitigationTypeDummy = MitigationType("Dummy")
)

// BGPMitigationTypes is an array of BGP based mitigation types
var BGPMitigationTypes = []MitigationType{MitigationTypeRTBH, MitigationTypeFlowspec}

// A MitigationMethod is our configuration entity representing a way
// to run mitigations on a MitigationPlatform.
// It corresponds to a row in mn_mitigation_method.
// This model isn't intended to be serialized/deserialized, but it does have
// JSON struct tags for its database fields, for the benefit of validation.
type MitigationMethod struct {
	// Database fields
	MethodID          MitigationMethodID `db:"id" json:"id"`                                                           // mn_mitigation_method.id
	MethodName        string             `db:"method_name" json:"method_name"`                                         // mn_mitigation_method.method_name
	MethodDescription string             `db:"method_description" json:"method_description"`                           // mn_mitigation_method.method_description
	MethodType        MitigationType     `db:"method_mitigation_device_type" json:"method_mitigation_device_type"`     // mn_mitigation_method.method_mitigation_device_type
	RawOptions        string             `db:"method_mitigation_device_detail" json:"method_mitigation_device_detail"` // mn_mitigation_method.method_mitigation_device_detail
	RawWhiteList      string             `db:"white_list" json:"white_list"`                                           // mn_mitigation_method.white_list
	RawGracePeriod    int                `db:"grace_period" json:"grace_period"`                                       // mn_mitigation_method.grace_period
	AckRequired       bool               `db:"ack_required" json:"ack_required"`                                       // mn_mitigation_method.ack_required
	Edate             time.Time          `db:"edate" json:"edate"`                                                     // mn_mitigation_method.edate
	Cdate             time.Time          `db:"cdate" json:"cdate"`                                                     // mn_mitigation_method.cdate

	// Computed fields
	GracePeriod time.Duration `json:"-"` // RawGracePeriod interpreted as minutes
	Options     interface{}   `json:"-"` // RawOptions deserialized
	WhiteList   []*net.IPNet  `json:"-"` // RawWhiteList deserialized
}

// MitigationMethodWithNotifChans bundles mitigation method data with its notification channels
type MitigationMethodWithNotifChans struct {
	*MitigationMethod
	NotificationChannels []NotificationChannelID
}

// MitMethodNotifChannelTuple bundles up method and channel id
type MitMethodNotifChannelTuple struct {
	MethodID       MitigationMethodID    `db:"mitigation_method_id"`
	NotifChannelID NotificationChannelID `db:"notification_channel_id"`
}

type A10MethodOptions struct {
	AnnounceViaBgp string                  `json:"announceViaBGP"`
	Priority       int                     `json:"priority"`
	StaticConfig   A10MethodOptionsStatic  `json:"staticConfig"`
	DynamicConfig  A10MethodOptionsDynamic `json:"dynamicConfig"`
	Mode           string                  `json:"mode"`
}

type A10MethodOptionsDynamic struct {
	AnnounceViaBgp string  `json:"announceViaBGP"`
	ZoneName       string  `json:"zoneName"`
	ZoneDestIP     string  `json:"zoneDestIP"`
	AdvertiseBGP   bool    `json:"advertiseBGP"`
	Macros         []Macro `json:"macros"`
}

type A10MethodOptionsStatic struct {
	AnnounceViaBgp string `json:"announceViaBGP"`
}

type Macro struct {
	Type          string                   `json:"type"`
	MacroKey      JSONStringOrNumberString `json:"macroKey"`
	MacroValue    JSONStringOrNumberString `json:"macroValue"`
	PrimaryMetric string                   `json:"primaryMetric"`
	Multiplier    FlexInt                  `json:"multiplier"`
	Offset        FlexInt                  `json:"offset"`
	PolicyID      PolicyID                 `json:"alertId"`
	Fallback      FlexInt                  `json:"fallback"`
}

const (
	A10MacroTypeStatic          = "static"
	A10MacroTypeInitialBaseline = "initial_alert_baseline"
	A10MacroTypeOtherBaseline   = "other_alert_baseline"
)

type RadwareMethodOptions struct {
	ProtectedObjectName string `json:"protectedObjectName"`
	Protocol            string `json:"protocol"`
	CallType            string `json:"callType"`
	IsConvertIPTo24     bool   `json:"isConvertIPTo24"`
	UseProtocolFromDim  bool   `json:"isUseProtocolFromAlertDimension"`

	CurrentBPSFallback int `json:"bpsBaselineValue"`
	CurrentPPSFallback int `json:"ppsBaselineValue"`

	BaselineIcmpPPSPolicyID PolicyID `json:"baselineIcmpPacketsPerSecond"`
	BaselineIcmpBPSPolicyID PolicyID `json:"baselineIcmpBytesPerSecond"`

	BaselineTcpPPSPolicyID PolicyID `json:"baselineTcpPacketsPerSecond"`
	BaselineTcpBPSPolicyID PolicyID `json:"baselineTcpBytesPerSecond"`

	BaselineUdpPPSPolicyID PolicyID `json:"baselineUdpPacketsPerSecond"`
	BaselineUdpBPSPolicyID PolicyID `json:"baselineUdpBytesPerSecond"`
}

type RTBHMethodOptions struct {
	Community       [][2]ASNumber `json:"community"`
	NextHopIP       string        `json:"nexthop"`
	Localpref       uint32        `json:"localpref"`
	IsConvertIPTo24 bool          `json:"isConvertIPTo24"`
	SrcIPv4         string        `json:"srcIpV4"`
	SrcIPv6         string        `json:"srcIpV6"`

	NextHopIPv4 string `json:"nexthopV4"`
	NextHopIPv6 string `json:"nexthopV6"`
}

type CFMTMethodOptions struct {
	// No configuration needed for now, add here as FRs come in
}

type MitigationPriority struct {
	PolicyID     PolicyID
	Priority     int
	MitigationID MitigationID
	AlarmID      AlarmID
	ThresholdID  Tid
}

type BaselineData struct {
	Data     map[string]int
	Fallback int
	Metric   string
}

// ThresholdMitigation corresponds to a row in mn_threshold_mitigation,
// plus related entities and calculated fields.
type ThresholdMitigation struct {
	// Database fields
	ThresholdMitigationID ThresholdMitigationID `json:"id"`               // mn_threshold_mitigation.id
	ThresholdID           Tid                   `json:"activate_id"`      // mn_threshold_mitigation.threshold_id
	MitigationApplyType   string                `json:"apply_type"`       // mn_threshold_mitigation.mitigation_apply_type
	MitigationClearType   string                `json:"clear_type"`       // mn_threshold_mitigation.mitigation_clear_type
	AckPeriod             time.Duration         `json:"ack_period"`       // mn_threshold_mitigation.mitigation_apply_timer
	AckPeriodClear        time.Duration         `json:"ack_period_clear"` // mn_threshold_mitigation.mitigation_clear_timer
	Edate                 time.Time             // mn_threshold_mitigation.edate
	Cdate                 time.Time             // mn_threshold_mitigation.cdate

	// Linked entities/fields

	// Linked by mn_threshold_mitigation.pairing_id=mn_associated_mitigation_platform_method.id->
	//   mn_associated_mitigation_platform_method.mitigation_{platform,method}_id
	Platform *MitigationPlatform `json:"policy" validate:"not_nil"`
	Method   *MitigationMethod   `json:"method" validate:"not_nil"`

	CompanyID       Cid      `json:"company_id"`
	PolicyID        PolicyID `json:"alert_id"` // From the associated threshold->policy
	AlertPolicyName string   // From the associated threshold->policy->mn_alert_policy.policy_name
	RawDimensions   string   // From the associated threshold->policy->mn_alert_policy.dimensions
	// NOTE: there is also mn_alert_threshold.threshold_ack_required
	MethodAckReq bool   `json:"ackreq"`   // mn_mitigation_method.ack_required
	Severity     string `json:"severity"` // mn_alert_threshold.severity

	// Calculated fields
	Dimensions       []string // Calculated from RawDimensions.
	IPCidrDimIndex   int      // Calculated from Dimensions.
	ProtocolDimIndex int      // Calculated from Dimensions.
}

// ThresholdMitigationShort represents row from mn_threshold_mitigation without any joins.
type ThresholdMitigationShort struct {
	ID                  ThresholdMitigationID          `db:"id" json:"id"`
	CompanyID           Cid                            `db:"company_id" json:"company_id"`
	PairingID           MitigationPlatformMethodPairID `db:"pairing_id" json:"pairing_id"`
	ThresholdID         Tid                            `db:"threshold_id" json:"activate_id"`
	MitigationApplyType string                         `db:"mitigation_apply_type" json:"apply_type"`
	MitigationClearType string                         `db:"mitigation_clear_type" json:"clear_type"`
	// both in minutes in db
	AckPeriodRaw      int64  `db:"mitigation_apply_timer" json:"ack_period"`
	AckPeriodClearRaw int64  `db:"mitigation_clear_timer" json:"ack_period_clear"`
	Status            string `db:"status" json:"status"`

	// Left out fields, unused in alerting. V3 ui doesn't seem to expose those to user either.
	// is_platform_overridable
	// is_method_overridable

	Edate time.Time `db:"cdate"`
	Cdate time.Time `db:"edate"`
}

type ThresholdMitigationWithIDs struct {
	ThresholdMitigationShort
	PlatformID MitigationPlatformID `db:"mitigation_platform_id"`
	MethodID   MitigationMethodID   `db:"mitigation_method_id"`
}

// Enum values for MitigationApplyType and MitigationClearType
const (
	UserAckUnlessTimer = "user_ack_unless_timer"
	UserAck            = "user_ack"
	Immediate          = "immediate"
)

// PlatMethodIDTuple bundles platform and method ids
type PlatMethodIDTuple struct {
	Platform MitigationPlatformID
	Method   MitigationMethodID
}

// ErrUnsupportedPlatMethodIDTuple denotes lack of support for given platform/method combination
type ErrUnsupportedPlatMethodIDTuple PlatMethodIDTuple

func (pmt ErrUnsupportedPlatMethodIDTuple) Error() string {
	return fmt.Sprintf("Unsupported platform/method pair %d:%d", pmt.Platform, pmt.Method)
}

// MitigationPlatformMethodPair represents row from mn_associated_mitigation_platform_method junction table
type MitigationPlatformMethodPair struct {
	ID         MitigationPlatformMethodPairID `db:"id"`
	CompanyID  Cid                            `db:"company_id"`
	PlatformID MitigationPlatformID           `db:"mitigation_platform_id"`
	MethodID   MitigationMethodID             `db:"mitigation_method_id"`
	Status     Status                         `db:"status"`

	// Edate time.Time `db:"cdate"`
	// Cdate time.Time `db:"edate"`
}

// RTBHCall corresponds to a row in mn_current_rtbh_call,
// which is an indication that a specific `device_id` should be sent a bgp update
// based on `args` for the ipCidr `alert_cidr` when chfribd starts.
// Said another way, this is what we think we are actively mitigating via RTBH
// right now.
type RTBHCall struct {
	// Database fields
	CompanyID    Cid                  `json:"companyID" db:"company_id"`
	DeviceID     DeviceID             `json:"deviceID" db:"device_id"`
	AlertCidr    string               `json:"alertCidr" db:"alert_cidr"`
	Args         string               `json:"args" db:"args"`
	Ctime        time.Time            `json:"ctime" db:"ctime"`
	PlatformID   MitigationPlatformID `json:"platformID" db:"platform_id"`
	MitigationID MitigationID         `json:"mitigationID" db:"mitigation_id"`
}

func (c *RTBHCall) String() string {
	return fmt.Sprintf("{%d, %d, `%s`, `%s`, time.Time{}, %d, %d}", c.CompanyID, c.DeviceID, c.AlertCidr, c.Args, c.PlatformID, c.MitigationID)
}

// MitigationMachineParams are the params used in the mitigate v2 state machine.
type MitigationMachineParams struct {
	MitigationID `json:"mitigationID"`

	// possibly -1 in case of manual mitigation
	ThresholdMitigationID `json:"thresholdMitigationID"`
	AlarmID               `json:"alarmID"`

	// Would be duplicate data, except this is all we have in the case of
	// manual mitigations.
	IPCidr               string `json:"ipCidr"`
	MitigationPlatformID `json:"mitigationPlatformID"`
	MitigationMethodID   `json:"mitigationMethodID"`

	// For manual mitigations only, an extra param determining when
	// we will stop the mitigation. (i.e. "manual mitigation TTL")
	MinutesBeforeAutoStopManual int `json:"minutesBeforeAutoStopManual"`

	// Would be duplicate data from an alarm, but the alarm can be
	// deleted when it clears, so we need to copy this here, as the
	// mitigation may outlive the alarm. Don't check this in the mitigation
	// engine, but return these values from the API.
	// A manual mitigation won't have these values.
	PolicyID    PolicyID   `json:"policyID"`
	ThresholdID Tid        `json:"thresholdID"`
	AlertKey    string     `json:"alertKey"`
	AlarmEvent  AlarmEvent `json:"alarmEvent"`
}

// NewMitigationParams constructs a MitigationMachineParams.
func NewMitigationParams(
	mitID MitigationID,
	tmid ThresholdMitigationID,
	alarmID AlarmID,
	ipCidr string,
	pid MitigationPlatformID,
	mid MitigationMethodID,
	policyID PolicyID,
	thresholdID Tid,
	alarmEvent AlarmEvent,
	minutesBeforeAutoStopManual int,
) *MitigationMachineParams {
	return &MitigationMachineParams{
		MitigationID: mitID,

		ThresholdMitigationID: tmid,
		AlarmID:               alarmID,

		IPCidr:               ipCidr,
		MitigationPlatformID: pid,
		MitigationMethodID:   mid,

		MinutesBeforeAutoStopManual: minutesBeforeAutoStopManual,

		PolicyID:    policyID,
		ThresholdID: thresholdID,
		AlertKey:    alarmEvent.AlertKey,
		AlarmEvent:  alarmEvent,
	}
}

// Validate implements state.MachineParams for MitigationMachineParams.
func (params *MitigationMachineParams) Validate() error {
	return nil
}

// MitigationInput is used as the shared pipeline.Input object for mitigate2.
// You can get one when you have a pipeline.Scope by scope.GetInput().(*MitigationInput).
type MitigationInput struct {
	state.MachineServiceInputBase
	CompanyID  Cid
	Config     *ConfigServiceInput
	Remote     *RemoteServiceInput
	Mitigation *MitigationServiceInput
	Limits     *MitigationLimits
}

// ConfigServiceInput is what mitigate2's configService adds to the MitigationInput.
type ConfigServiceInput struct {
	// These things are really "config":
	Thresholds                map[Tid]*Threshold
	ThresholdMitigationsByTid map[Tid][]*ThresholdMitigation
	ThresholdMitigations      map[ThresholdMitigationID]*ThresholdMitigation
	Platforms                 map[MitigationPlatformID]*MitigationPlatform
	Methods                   map[MitigationMethodID]*MitigationMethod

	// These things are more like state:
	Alarms               map[AlarmID]*AlarmEvent
	AlarmsByTid          map[Tid][]*AlarmEvent
	BaselineDataByPolicy map[PolicyID]*BaselineData

	V3Enabled bool
}

// RemoteServiceInput is what remoteService.Input adds to the
// pipeline's input. It is intended to be used by mitigationService.
type RemoteServiceInput struct {
	PlatformStates map[MitigationPlatformID]interface{} // map[MitigationPlatformID]platform.State

	// To be used only be Output methods.
	PlatformServicePool interface{} // *platform.Pool do not serialize
}

// MitigationServiceInput is what mitigationService.Input adds to the
// pipeline's input. It shouldn't be accessed outside of
// mitigationService.
type MitigationServiceInput struct {
	NextMitigationID MitigationID `json:"nextMitigationID"`
}

type MitigationLimits struct {
	TotalMitigations      limits.LimitedResource
	NewMitigationsThisRun limits.LimitedResource
}

type MitigationUserActionRequest struct {
	MitigationID
	EventName string
}

type MitigationCommentRequest struct {
	MitigationID
	Comment string
}

// IPCidr represents block of ip addresses
type IPCidr = string

// AlreadyMitigatedError is returned for a ManualMitigationRequest on
// an IPCidr that is already being mitigated.
type AlreadyMitigatedError struct {
	IPCidr string
}

func (e AlreadyMitigatedError) Error() string {
	return fmt.Sprintf("%s is already being mitigated.", e.IPCidr)
}

// NotFoundError is returned when a resource cannot be found
type NotFoundError struct {
	Resource string
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("Resource not found: %s", e.Resource)
}

// Deserialization

func DimensionsFromRaw(dimsRaw []byte) ([]string, error) {
	var dims []string

	if err := json.Unmarshal(dimsRaw, &dims); err != nil {
		return nil, err
	}

	if len(dims) == 0 {
		return nil, fmt.Errorf("No dimensions set")
	}

	sort.Strings(dims)

	return dims, nil
}

func A10PlatformOptionsFromRaw(rawPlatform []byte) (*A10PlatformOptions, error) {
	var platformOptions A10PlatformOptions
	if err := json.Unmarshal(rawPlatform, &platformOptions); err != nil {
		return nil, fmt.Errorf("Invalid A10 platform options: %v", err)
	}
	if platformOptions.IPAddress == "" {
		return nil, fmt.Errorf("Invalid A10 platform options: no IPAddress set")
	}
	return &platformOptions, nil
}

func A10MethodOptionsFromRaw(rawMethod []byte) (*A10MethodOptions, error) {
	var methodOptions A10MethodOptions
	if err := json.Unmarshal(rawMethod, &methodOptions); err != nil {
		return nil, fmt.Errorf("Invalid A10 method options: %v", err)
	}

	if methodOptions.Mode == A10_MODE_DYNAMIC {
		if methodOptions.DynamicConfig.ZoneName == "" {
			return nil, fmt.Errorf("Invalid A10 method options: no DynamicConfig.ZoneName set")
		}
		methodOptions.AnnounceViaBgp = methodOptions.DynamicConfig.AnnounceViaBgp
	} else {
		methodOptions.AnnounceViaBgp = methodOptions.StaticConfig.AnnounceViaBgp
	}

	if methodOptions.AnnounceViaBgp == "" {
		methodOptions.AnnounceViaBgp = A10_BGP_ANNOUNCE_CIDR
	}
	return &methodOptions, nil
}

func RadwarePlatformOptionsFromRaw(rawPlatform []byte) (*RadwarePlatformOptions, error) {
	var platformOptions RadwarePlatformOptions
	if err := json.Unmarshal(rawPlatform, &platformOptions); err != nil {
		return nil, fmt.Errorf("Invalid Radware platform options: %v", err)
	}

	if platformOptions.IPAddress == "" {
		return nil, fmt.Errorf("Invalid Radware platform options: no IPAddress set")
	}

	return &platformOptions, nil
}

func RadwareMethodOptionsFromRaw(rawMethod []byte) (*RadwareMethodOptions, error) {
	var methodOptions RadwareMethodOptions
	if err := json.Unmarshal(rawMethod, &methodOptions); err != nil {
		return nil, fmt.Errorf("Invalid Radware method options: %v", err)
	}

	switch methodOptions.Protocol {
	case METHOD_TCP:
		methodOptions.Protocol = METHOD_TCP_RADWARE
	case METHOD_UDP:
		methodOptions.Protocol = METHOD_UDP_RADWARE
	case METHOD_ICMP:
		methodOptions.Protocol = METHOD_ICMP_RADWARE
	case METHOD_OTHER:
		methodOptions.Protocol = METHOD_OTHER_RADWARE
	default:
		return nil, fmt.Errorf("Invalid Radware method options: unknown protocol '%s'", methodOptions.Protocol)
	}

	return &methodOptions, nil
}

func CFMTPlatformOptionsFromRaw(rawPlatform []byte) (*CFMTPlatformOptions, error) {
	var platformOptions CFMTPlatformOptions
	if err := json.Unmarshal(rawPlatform, &platformOptions); err != nil {
		return nil, fmt.Errorf("invalid CFMT platform options: %v", err)
	}
	if platformOptions.AccountID == "" {
		return nil, fmt.Errorf("invalid CFMT platform options: no IPAddress set")
	}
	if platformOptions.APILogin == "" {
		return nil, fmt.Errorf("invalid CFMT platform options: no APILogin set")
	}
	if platformOptions.APIPassword == "" {
		return nil, fmt.Errorf("invalid CFMT platform options: no APIPassword set")
	}
	return &platformOptions, nil
}

func CFMTMethodOptionsFromRaw(rawMethod []byte) (*CFMTMethodOptions, error) {
	var methodOptions CFMTMethodOptions
	if err := json.Unmarshal(rawMethod, &methodOptions); err != nil {
		return nil, fmt.Errorf("invalid CFMT method options: %v", err)
	}
	return &methodOptions, nil
}

func RTBHPlatformOptionsFromRaw(rawPlatform []byte) (*RTBHPlatformOptions, error) {
	var platformOptions RTBHPlatformOptions
	if err := json.Unmarshal(rawPlatform, &platformOptions); err != nil {
		return nil, fmt.Errorf("Invalid RTBH platform options: %v", err)
	}
	return &platformOptions, nil
}

func sPick(firstOption string, moreOptions ...string) string {
	if firstOption != "" {
		return firstOption
	}
	for _, s := range moreOptions {
		if s != "" {
			return s
		}
	}
	return ""
}

func RTBHMethodOptionsFromRaw(rawMethod []byte) (*RTBHMethodOptions, error) {
	methodOptions := RTBHMethodOptions{}
	methodOptions.Localpref = RTBH_LOCALPREF_DEFAULT // In case it is missing from the JSON
	if err := json.Unmarshal(rawMethod, &methodOptions); err != nil {
		return nil, fmt.Errorf("Invalid RTBH method options: %v", err)
	}

	nextHop := sPick(methodOptions.NextHopIPv4, methodOptions.NextHopIPv6, methodOptions.NextHopIP)
	ip := net.ParseIP(nextHop)
	if ip == nil {
		return nil, fmt.Errorf("Invalid RTBH method options: cannot parse nexthop '%s'", methodOptions.NextHopIP)
	}

	if len(methodOptions.Community) == 0 {
		return nil, fmt.Errorf("Invalid RTBH method options: no community set")
	}

	for _, com := range methodOptions.Community {
		if len(com) != 2 {
			return nil, fmt.Errorf("Invalid RTBH method options: invalid community '%v'", com)
		}
	}
	return &methodOptions, nil
}

// MitigationRepr bundles data about mitigation as expected by alert-api related code
type MitigationRepr struct {
	MitigationID         MitigationID         `db:"mitigation_id"`
	PolicyID             PolicyID             `db:"policy_id"`
	ThresholdID          Tid                  `db:"threshold_id"`
	AlarmID              AlarmID              `db:"alarm_id"`
	IPCidr               string               `db:"ip_cidr"`
	Target               string               `db:"target"`
	MitigationPlatformID MitigationPlatformID `db:"platform_id"`
	MitigationMethodID   MitigationMethodID   `db:"method_id"`
	State                string               `db:"to_state_name"`
	StateTransitionsRaw  []byte               `db:"state_transitions"`
	StartTime            time.Time            `db:"initial_state_time"`
	CurTime              time.Time            `db:"current_state_time"`
	AutoStopTTLMinutes   uint64               `db:"autostop_ttl_minutes"`

	StateTransitions []*MitigationTransition
}

// MitigationTransition denotes single state to which mitigation state machine transitioned
// no db encoding as this gets decoded from MitigationRepr.StateTransitionsRaw field
type MitigationTransition struct {
	State string    `json:"state"`
	Event string    `json:"event"`
	Stamp time.Time `json:"stamp"`
}

// ErrMachineNotFound is thrown when interaction is requested with nonexistent mitigation machine
type ErrMachineNotFound struct {
	ID MitigationID
}

func (emnf *ErrMachineNotFound) Error() string {
	return fmt.Sprintf("no machine found for mitigation id: %d", emnf.ID)
}

// Is implements interface required by errors.Is
func (emnf *ErrMachineNotFound) Is(target error) bool {
	_, ok := target.(*ErrMachineNotFound)
	return ok
}

// PlatformUpdateTuple typed UpdateTuple for mitigation platforms
type PlatformUpdateTuple UpdateTuple

// PlatformUpdateBundle typed PolicyUpdateTuple slice
type PlatformUpdateBundle []*PlatformUpdateTuple

// MethodUpdateTuple typed UpdateTuple for mitigation methods
type MethodUpdateTuple UpdateTuple

// MethodUpdateBundle typed MethodUpdateTuple slice
type MethodUpdateBundle []*MethodUpdateTuple

// ErrInvalidDevices denotes invalid devices
var ErrInvalidDevices = fmt.Errorf("one or more of devices is undefined, unsupported or already used by BGP based platform %v", BGPMitigationTypes)

// ASNumber represents part of bgp community type as defined in rfc1997
// It's able to handle asdot notation as well as extended communities
type ASNumber uint32

// UnmarshalJSON fulfills json.Unmarshaller interface
func (asn *ASNumber) UnmarshalJSON(b []byte) error {
	if b[0] == '"' {
		val, err := maybeasnToUint32(string(b[1 : len(b)-1]))
		if err != nil {
			return err
		}
		*asn = ASNumber(val)
		return nil
	}

	if strings.IndexByte(string(b), '.') > 0 {
		val, err := asdotToUint32(string(b))
		if err != nil {
			return err
		}
		*asn = ASNumber(val)
		return nil
	}

	var interim uint32
	err := json.Unmarshal(b, &interim)
	if err != nil {
		return err
	}
	*asn = ASNumber(interim)

	return nil
}

func maybeasnToUint32(s string) (uint32, error) {
	decVal, err := strconv.ParseUint(s, 10, 32)
	if err == nil {
		return uint32(decVal), nil
	}

	hexVal, err := strconv.ParseUint(s, 16, 32)
	if err == nil {
		return uint32(hexVal), nil
	}

	return asdotToUint32(s)
}

func asdotToUint32(s string) (uint32, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("Unexpected amount of parts in asdot notation: %s", s)
	}

	h, l := parts[0], parts[1]
	vh, err := strconv.ParseUint(h, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("Couldn't parse high bytes in asdot notation: %w", err)
	}

	vl, err := strconv.ParseUint(l, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("Couldn't parse low bytes in asdot notation: %w", err)
	}

	return uint32(vh)<<16 + uint32(vl), nil
}

// NotifChannelsWrapped poor man's option type to discriminate lack of entries from empty list
type NotifChannelsWrapped struct {
	Contains               bool
	NotificationChannelIDs []NotificationChannelID
}

func (ncw *NotifChannelsWrapped) Append(chans []NotificationChannelID) {
	ncw.Contains = true
	ncw.NotificationChannelIDs = append(ncw.NotificationChannelIDs, chans...)
}

// Merge joins contents of two NotifChannelsWrapped
func (ncw *NotifChannelsWrapped) Merge(other *NotifChannelsWrapped) {
	if other == nil {
		return
	}

	ncw.Contains = ncw.Contains || other.Contains
	ncw.NotificationChannelIDs = append(ncw.NotificationChannelIDs, other.NotificationChannelIDs...)
}

// FindMitigationPlatformsFilter set of criteria for MitgateDS.FindMitigationPlatforms
type FindMitigationPlatformsFilter struct {
	CompanyID   Cid
	PlatformIDs []MitigationPlatformID
	Names       []string
	Types       []MitigationType
	CreateStart time.Time
	CreateEnd   time.Time
	ModifyStart time.Time
	ModifyEnd   time.Time
	Limit       uint64
	Offset      uint64
}

// FindMitigationMethodsFilter set of criteria for MitgateDS.FindMitigationMethods
type FindMitigationMethodsFilter struct {
	CompanyID   Cid
	MethodIDs   []MitigationMethodID
	Names       []string
	Types       []MitigationType
	CreateStart time.Time
	CreateEnd   time.Time
	ModifyStart time.Time
	ModifyEnd   time.Time
	Limit       uint64
	Offset      uint64
}

// MitigationSource wrapper type for auto/manual mitigation types
type MitigationSource string

const (
	// MitigationSourceAuto denotes automatic mitigation (based on alarm)
	MitigationSourceAuto = "auto"
	// MitigationSourceManual denotes manual mitigation (raised by user)
	MitigationSourceManual = "manual"
)

// FindMitigationsFilter set of criteria for MitigationDataSource.FindMitigations function
type FindMitigationsFilter struct {
	CompanyID     Cid
	MitigationIDs []MitigationID
	PolicyIDs     []PolicyID
	AlarmIDs      []AlarmID
	ThresholdIDs  []Tid
	StateNames    []string
	PlatformIDs   []MitigationPlatformID
	MethodIDs     []MitigationMethodID
	IPCidrs       []string
	Sources       []MitigationSource
	Types         []MitigationType

	Limit  uint64
	Offset uint64
}
