package kt

import (
	"time"

	"github.com/kentik/chf-alert/pkg/eggs/state"
	chfhttp "github.com/kentik/eggs/pkg/http"
)

// -------------------------------------------

// CurrentMitigationsRequest is the JSON body expected by
// the /v2/company/{cid}/mitigations endpoint.
type CurrentMitigationsRequest struct {
	FilterBy  string    `json:"filterBy"`
	FilterVal string    `json:"filterVal"`
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
	SortBy    string    `json:"sortBy"`
}

// Possible values for CurrentMitigationsRequest.FilterBy
const (
	CMFilterByMitigationID    = "mitigationID"
	CMFilterByIPCidr          = "ipCidr"
	CMFilterByIPCidrSubstring = "ipCidrSubstring"
	CMFilterByState           = "state"
	CMFilterByStateCategory   = "stateCategory"
)

// Possible FilterVal values when CurrentMitigationsRequest.FilterBy is CMFilterByStateCategory
const (
	CMFilterByStateCategoryActive = "active"
)

// Possible values for CurrentMitigationsRequest.SortBy
const (
	CMSortByMitigationID = "mitigationID"
	CMSortByIPCidr       = "ipCidr"
	CMSortByState        = "state"
	CMSortByPlatformID   = "platformID"
	CMSortByPolicyID     = "policyID"
	CMSortByAlarmID      = "alarmID"
	CMSortByStartTime    = "startTime"
	CMSortByEndTime      = "endTime"
)

// CurrentMitigationsResponse is the JSON body returned by
// the /v2/company/{cid}/mitigations endpoint.
type CurrentMitigationsResponse struct {
	CurrentMitigationsSummary
	Mitigations []CurrentMitigation `json:"mitigations"`
}

// CurrentMitigationsSummary is a sub-section of a CurrentMitigationsResponse.
type CurrentMitigationsSummary struct {
	ActiveCount      int `json:"activeCount"` // i.e. mitigations not in CLEAR/ACK_REQ
	AckRequiredCount int `json:"ackReqCount"` // i.e. mitigations in ACK_REQ
}

// A CurrentMitigation is a single result row in a CurrentMitigationsResponse.
type CurrentMitigation struct {
	MitigationID MitigationID          `json:"mitigationID"`
	IPCidr       *string               `json:"ipCidr"`
	Key          *string               `json:"key"`
	State        *string               `json:"state"`
	PlatformID   *MitigationPlatformID `json:"platformID"`
	MethodID     *MitigationMethodID   `json:"methodID"`
	PolicyID     *PolicyID             `json:"policyID"`
	ThresholdID  *Tid                  `json:"thresholdID"`
	AlarmID      *AlarmID              `json:"alarmID"`
	StartTime    time.Time             `json:"startTime"`
	EndTime      *time.Time            `json:"endTime"` // not nullable in db, but can be mysql zero time
	Comment      *string               `json:"comment"`
	Args         *string               `json:"args"`
	Type         string                `json:"type"`

	MinutesBeforeAutoStop int        `json:"minutesBeforeAutoStop,omitempty"`
	AutoStopTime          *time.Time `json:"autoStopTime,omitempty"`
}

// -------------------------------------------

// MitigationHistoryRequest is the JSON body expected by the
// /v2/company/{cid}/mitigations/history endpoint.
type MitigationHistoryRequest struct {
	FilterBy  string     `json:"filterBy"`
	FilterVal string     `json:"filterVal"`
	StartTime *time.Time `json:"startTime"`
	EndTime   *time.Time `json:"endTime"`
	SortBy    string     `json:"sortBy"`
	Limit     uint64     `json:"limit"`
}

// Possible values for MitigationHistoryRequest.FilterBy
const (
	MHFilterByMitigationID    = "mitigationID"
	MHFilterByIPCidr          = "ipCidr"
	MHFilterByIPCidrSubstring = "ipCidrSubstring"
	MHFilterByOldState        = "oldState"
	MHFilterByNewState        = "newState"
	MHFilterByAnyState        = "anyState" // i.e. either oldState or newState
)

// MHFilterBys is a list of all possible MitigationHistoryRequest.FilterBy values.
var MHFilterBys = []string{MHFilterByMitigationID, MHFilterByIPCidr, MHFilterByIPCidrSubstring, MHFilterByOldState, MHFilterByNewState, MHFilterByAnyState}

// Possible values for MitigationHistoryRequest.SortBy
const (
	MHSortByMitigationID = "mitigationID"
	MHSortByIPCidr       = "ipCidr"
	MHSortByState        = "state"
	MHSortByPlatformID   = "platformID" // in chnode_v2, sort by severity
	MHSortByPolicyID     = "policyID"   // aka alert_id
	MHSortByAlarmID      = "alarmID"
	MHSortByCtime        = "ctime" // The default: "ctime desc". Otherwise sort by this second.
)

// MHSortBys is a list of all possible MitigationHistoryRequest.SortBy values.
var MHSortBys = []string{MHSortByMitigationID, MHSortByIPCidr, MHSortByState, MHSortByPlatformID, MHSortByPolicyID, MHSortByAlarmID, MHSortByCtime}

// MitigationHistoryResponse is the JSON body returned by
// the /v2/company/{cid}/mitigations/history endpoint.
type MitigationHistoryResponse struct {
	MitigationHistorySummary
	HistoryEvents []MitigationHistoryEvent `json:"historyEvents"`
}

// MitigationHistorySummary is a sub-section of a MitigationHistoryResponse.
type MitigationHistorySummary struct {
	MitigatingCount  int `json:"mitigatingCount"` // i.e. transitions to MITIGATING
	AckRequiredCount int `json:"ackReqCount"`     // i.e. transitions to ACK_REQ
}

// A MitigationHistoryEvent is a single result row in a MitigationHistoryResponse.
type MitigationHistoryEvent struct {
	MitigationID *MitigationID         `json:"mitigationID"`
	IPCidr       *string               `json:"ipCidr"`
	Key          *string               `json:"key"`
	OldState     *string               `json:"oldState"`
	NewState     *string               `json:"newState"`
	PlatformID   *MitigationPlatformID `json:"platformID"`
	MethodID     *MitigationMethodID   `json:"methodID"`
	PolicyID     *PolicyID             `json:"policyID"`
	ThresholdID  *Tid                  `json:"thresholdID"`
	AlarmID      *AlarmID              `json:"alarmID"`
	Ctime        time.Time             `json:"ctime"`
	Comment      *string               `json:"comment"`
	Args         *string               `json:"args"`
	Type         string                `json:"type"`
	EventName    string                `json:"eventName"`
}

// -------------------------------------------

// An AckMitigationsRequest is the JSON body expected by
// the /v2/company/{cid}/mitigations/ack endpoint.
// It takes a list of ids instead of a single one so that
// you can ACK in bulk.
// If you don't need to ack in bulk,
// /v2/company/{cid}/mitigations/{mitID}/ack exists as well.
type AckMitigationsRequest struct {
	MitigationIDs []MitigationID `json:"mitigationIDs"`
	Comment       string         `json:"comment"`
}

// -------------------------------------------

// A CommentMitigationsRequest is the JSON body expected by
// the /v2/company/{cid}/mitigations/comment endpoint.
// It takes a list of ids instead of a single one so that
// you can comment in bulk.
// If you don't need to comment in bulk,
// /v2/company/{cid}/mitigations/{mitID}/comment exists as well.
type CommentMitigationsRequest struct {
	MitigationIDs []MitigationID `json:"mitigationIDs"`
	Comment       string         `json:"comment"`
}

// -------------------------------------------

// A CommentMitigationRequest is the JSON body expected by
// the /v2/company/{cid}/mitigations/{mitID}/comment endpoint.
type CommentMitigationRequest struct {
	Comment string `json:"comment"`
}

// -------------------------------------------

// A ManualMitigationRequest is the JSON body expected by
// the POST /v2/company/{cid}/mitigations endpoint.
type ManualMitigationRequest struct {
	IPCidr                string `json:"ipCidr"`
	Comment               string `json:"comment"`
	MitigationPlatformID  `json:"platformID"`
	MitigationMethodID    `json:"methodID"`
	MinutesBeforeAutoStop int `json:"minutesBeforeAutoStop"`
}

// ManualMitigationResponse is the JSON body returned by
// the POST /v2/company/{cid}/mitigations endpoint.
type ManualMitigationResponse struct {
	chfhttp.OKResponse
	MitigationID MitigationID `json:"mitigationID"`
}

// -------------------------------------------

// A CompaniesWithActiveMitigationsResponse is the JSON body
// returned by the GET /v2/global/companiesWithActiveMitigations endpoint.
type CompaniesWithActiveMitigationsResponse struct {
	CompanyIDs []Cid `json:"companyIDs"`
}

// -------------------------------------------

// A MitigationStateMachineMetadataResponse is the JSON body
// returned by the GET /v2/global/stateMachine/mitigation endpoint.
type MitigationStateMachineMetadataResponse struct {
	MachineDefinition state.MachineClassDefinition `json:"machineDefinition"`
}

// -------------------------------------------

// A MitigationStateMachineActionsResponse is the JSON body
// returned by the GET /v2/global/stateMachine/mitigation/actions endpoint.
type MitigationStateMachineActionsResponse struct {
	ActionsForState map[string][]MitigationSMActionDetails `json:"actionsForState"`
}

// MitigationSMActionDetails is a sub-section of MitigationStateMachineActionsResponse.
type MitigationSMActionDetails struct {
	state.MachineActionDetails
	ActionURL string `json:"actionURL"`
}
