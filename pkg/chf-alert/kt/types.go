package kt

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/kentik/eggs/pkg/logger"
	model "github.com/kentik/proto/kflow2"
)

func NewFlowBatch(cid Cid, n int) *FlowBatch {
	return &FlowBatch{
		CompanyID: cid,
		Flows:     make([]Flow, 0, n),
	}
}

type FlowBatch struct {
	CompanyID Cid
	Flows     []Flow
}

// AddRawFlow adds a raw flow to this batch and initializes it.
// Assumption: cap(b.Flows) has already been set properly, so the reslicing is safe.
// Thread safety: this function is not threadsafe.
func (b *FlowBatch) AddRawFlow(msg model.CHF) {
	b.Flows = b.Flows[0 : len(b.Flows)+1] // extend the slice by one
	b.Flows[len(b.Flows)-1] = Flow{       // append
		CHF:         msg,
		CustomCache: CustomCache{},
	}
	b.Flows[len(b.Flows)-1].Init() // and init the just appeneded flow
}

type Flow struct {
	model.CHF
	CustomCache
}

type FlowPredicateKeyed struct {
	Predicate FlowPredicate
	Key       string
}

type FlowPredicate func(v *Flow) bool

type CustomCache struct {
	customListReadOnly model.Custom_List
	customByIDReadOnly map[uint32]model.Custom
}

// for tests
func (flow *Flow) ResetCustomCache() { flow.Init() }

// Init initializes this flow by pulling some info out of the capnproto format.
// Thread safety: not thread safe. Must be called exactly once.
func (flow *Flow) Init() {
	cl, _ := flow.Custom()
	m := make(map[uint32]model.Custom, cl.Len())
	for i := 0; i < cl.Len(); i++ {
		c := cl.At(i)
		m[c.Id()] = c
	}
	flow.customListReadOnly = cl
	flow.customByIDReadOnly = m
}

func (flow *Flow) GetCustomList() model.Custom_List { return flow.customListReadOnly }

// GetCustomColumnWithID returns custom column from customColumn list.
func (flow *Flow) GetCustomColumnWithID(id uint32) (model.Custom, bool) {
	c, ok := flow.customByIDReadOnly[id]
	if ok {
		return c, true
	}
	return model.Custom{}, false
}

// SetCustomValue sets custom value in flow, panics on unsupported types
// Thread safety: not thread safe! Should only be used in tests.
func (flow *Flow) SetCustomValue(position, id uint32, value interface{}) {
	customs, err := flow.Custom()
	if err != nil {
		panic(err)
	}
	cval := customs.At(int(position))
	cval.SetId(id)
	switch v := value.(type) {
	case string:
		cval.Value().SetStrVal(v)
	case uint32:
		cval.Value().SetUint32Val(v)
	default:
		panic(value)
	}
	flow.Init()
}

type IntId int64

type Cid IntId                      // company id -- TODO: switch to uint64
func (id Cid) Itoa() string         { return strconv.Itoa(int(id)) }
func AtoiCid(s string) (Cid, error) { id, err := strconv.Atoi(s); return Cid(id), err }

type Cids []Cid               // Only provided for its String method. Otherwise just use []Cid.
func (t Cids) String() string { return CidsToString([]Cid(t)) }
func CidsToString(as []Cid) string {
	ss := make([]string, len(as))
	for i := range as {
		ss[i] = strconv.Itoa(int(as[i]))
	}
	return strings.Join(ss, ", ")
}

func ContainsCid(cids []Cid, cid Cid) bool {
	for _, c := range cids {
		if cid == c {
			return true
		}
	}
	return false
}

// RequestID tracks requests going through services
type RequestID string

type PolicyID IntId                           // policy id aka alert id
func (id PolicyID) Itoa() string              { return strconv.Itoa(int(id)) }
func AtoiPolicyID(s string) (PolicyID, error) { id, err := strconv.Atoi(s); return PolicyID(id), err }

type PolicyIDs []PolicyID          // Only provided for its String method. Otherwise just use []PolicyID.
func (t PolicyIDs) String() string { return PolicyIDsToString([]PolicyID(t)) }
func PolicyIDsToString(as []PolicyID) string {
	ss := make([]string, len(as))
	for i := range as {
		ss[i] = strconv.Itoa(int(as[i]))
	}
	return strings.Join(ss, ", ")
}

// PolicyIDsFromInt64Slice poor man's typecast from []T to []alias(T)
func PolicyIDsFromInt64Slice(arr []int64) (res []PolicyID) {
	for _, id := range arr {
		res = append(res, PolicyID(id))
	}
	return
}

// PolicyStatus describes policy status
type PolicyStatus string

const (
	// PolicyStatusActive denotes active policy
	PolicyStatusActive = PolicyStatus("A")
	// PolicyStatusDisabled denotes disabled policy
	PolicyStatusDisabled = PolicyStatus("D")
	// PolicyStatusError denotes broken policy
	PolicyStatusError = PolicyStatus("ERR")
)

var validPolicyStatuses = map[PolicyStatus]struct{}{
	PolicyStatusActive:   struct{}{},
	PolicyStatusDisabled: struct{}{},
	PolicyStatusError:    struct{}{},
}

// ValidPolicyStatus checks whether policy status passed is valid
func ValidPolicyStatus(st PolicyStatus) bool {
	_, ok := validPolicyStatuses[st]
	return ok
}

type CidPidTuple struct {
	Cid
	PolicyID
}

func (t CidPidTuple) String() string               { return fmt.Sprintf("(cid=%d,polid=%d)", t.Cid, t.PolicyID) }
func (t CidPidTuple) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

func CidPidTuplesToSets(ts []CidPidTuple) map[Cid]map[PolicyID]struct{} {
	s := make(map[Cid]map[PolicyID]struct{})
	for _, t := range ts {
		pids, ok := s[t.Cid]
		if !ok {
			pids = make(map[PolicyID]struct{})
			s[t.Cid] = pids
		}
		pids[t.PolicyID] = struct{}{}
	}
	return s
}

type CidPidTuples []CidPidTuple       // Only provided for its String method. Otherwise just use []CidPidTuple.
func (t CidPidTuples) String() string { return CidPidTuplesToString([]CidPidTuple(t)) }
func CidPidTuplesToString(as []CidPidTuple) string {
	ss := make([]string, len(as))
	for i := range as {
		ss[i] = as[i].String()
	}
	return strings.Join(ss, ", ")
}

// DeviceID denotes id from mn_device table
type DeviceID int64

// Itoa returns string formated device id
func (id DeviceID) Itoa() string {
	return strconv.FormatInt(int64(id), 10)
}

func (id *DeviceID) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		did, err := AtoiDevid(v)
		if err != nil {
			return fmt.Errorf("DeviceID Scan: %w", err)
		}
		*id = did
	case []byte:
		did, err := AtoiDevid(string(v))
		if err != nil {
			return fmt.Errorf("DeviceID Scan: %w", err)
		}
		*id = did
	case int:
		*id = DeviceID(v)
	case int64:
		*id = DeviceID(v)
	case uint:
		*id = DeviceID(v)
	case uint64:
		*id = DeviceID(v)
	default:
		return fmt.Errorf("DeviceID Scan: incompatible type for DeviceID %+v", src)
	}

	return nil
}

// AtoiDevid translates string repr into device id
// Unsigned integer fitting on 32 bit required
func AtoiDevid(s string) (DeviceID, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	return DeviceID(id), err
}

// DeviceIDSlice provides sort for DeviceID
type DeviceIDSlice []DeviceID

func (a DeviceIDSlice) Len() int           { return len(a) }
func (a DeviceIDSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DeviceIDSlice) Less(i, j int) bool { return a[i] < a[j] }

// IfaceID denotes interfce id
// note this is not mn_interface.id but snmp_id, {input,output}_port in flow
type IfaceID int

// Itoa returns string repr of interface id
func (id IfaceID) Itoa() string { return strconv.Itoa(int(id)) }

// AtoiIfaceID parses interface id from string
func AtoiIfaceID(s string) (IfaceID, error) {
	id, err := strconv.Atoi(strings.TrimSpace(s))
	return IfaceID(id), err
}

type IfaceGroupID int

type DeviceIDSNMPIDPair struct {
	DeviceID
	SNMPID string
}

// UserID denotes id of an user
type UserID int64

// Itoa converts UserID to string
func (id UserID) Itoa() string { return strconv.FormatInt(int64(id), 10) }

// AtoiUserID converts string to UserID
func AtoiUserID(s string) (UserID, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	return UserID(id), err
}

// VRFEntryID denotes index from mn_vrf table
type VRFEntryID int64

// Tid is a threshold ID (previously known as "activate id" or "activation id")
type Tid IntId

// Itoa converts Tid to string
func (id Tid) Itoa() string { return strconv.Itoa(int(id)) }

// AtoiTid converts string to Tid
func AtoiTid(s string) (Tid, error) { id, err := strconv.Atoi(s); return Tid(id), err }

// ThresholdIDsFromInt64Slice poor man's typecast from []T to []alias(T)
func ThresholdIDsFromInt64Slice(arr []int64) (res []Tid) {
	for _, id := range arr {
		res = append(res, Tid(id))
	}
	return
}

// MitigationID id of mitigation
type MitigationID int64

// Itoa converts MitigationID to string
func (id MitigationID) Itoa() string { return strconv.FormatInt(int64(id), 10) }

// AtoiMitigationID converts string to MitigationID
func AtoiMitigationID(s string) (MitigationID, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	return MitigationID(id), err
}

// MitigationIDsFromInt64Slice poor man's typecast from []T to []alias(T)
func MitigationIDsFromInt64Slice(arr []int64) (res []MitigationID) {
	for _, id := range arr {
		res = append(res, MitigationID(id))
	}
	return
}

// SavedFilterID denotes filter id from mn_saved_filter table
type SavedFilterID uint64

// SavedFiltersIDsFromInt64Slice poor man's typecast from []T to []alias(T)
func SavedFiltersIDsFromInt64Slice(arr []int64) (res []SavedFilterID) {
	for _, id := range arr {
		res = append(res, SavedFilterID(id))
	}
	return
}

// ThresholdMitigationID denotes junction table entry which links platform/method pair with threshold.
// (platform,method) association is stored in another table, foreign key = pairing_id
type ThresholdMitigationID int

// MitigationPlatformMethodPairID is an id of junction table betwen platform and method.
// Used to link platform/method pair with threshold.
type MitigationPlatformMethodPairID int

// MitigationPlatformID denotes id of a mitigation platform
type MitigationPlatformID int

// AtoiMitigationPlatformID constructs MitigationPlatformID from string
func AtoiMitigationPlatformID(s string) (MitigationPlatformID, error) {
	id, err := strconv.Atoi(s)
	return MitigationPlatformID(id), err
}

// Scan implements sql.Scanner interface
func (id *MitigationPlatformID) Scan(src interface{}) error {
	switch t := src.(type) {
	case int64:
		*id = MitigationPlatformID(t)
	case string:
		v, err := AtoiMitigationPlatformID(t)
		if err != nil {
			return err
		}
		*id = v
	case []byte:
		v, err := AtoiMitigationPlatformID(string(t))
		if err != nil {
			return err
		}
		*id = v
	default:
		return fmt.Errorf("Unsupported type: %v", src)
	}
	return nil
}

// MitigationMethodID denotes id of a mitigation method
type MitigationMethodID int

// AtoiMitigationMethodID constructs MitigationMethodID from string
func AtoiMitigationMethodID(s string) (MitigationMethodID, error) {
	id, err := strconv.Atoi(s)
	return MitigationMethodID(id), err
}

// Scan implements sql.Scanner interface
func (id *MitigationMethodID) Scan(src interface{}) error {
	switch t := src.(type) {
	case int64:
		*id = MitigationMethodID(t)
	case string:
		v, err := AtoiMitigationMethodID(t)
		if err != nil {
			return err
		}
		*id = v
	case []byte:
		v, err := AtoiMitigationMethodID(string(t))
		if err != nil {
			return err
		}
		*id = v
	default:
		return fmt.Errorf("Unsupported type: %v", src)
	}
	return nil
}

// MitigationPlatformIDsFromInt64Slice poor man's typecast from []T to []alias(T)
func MitigationPlatformIDsFromInt64Slice(arr []int64) (res []MitigationPlatformID) {
	for _, id := range arr {
		res = append(res, MitigationPlatformID(id))
	}
	return
}

// MitigationMethodIDsFromInt64Slice poor man's typecast from []T to []alias(T)
func MitigationMethodIDsFromInt64Slice(arr []int64) (res []MitigationMethodID) {
	for _, id := range arr {
		res = append(res, MitigationMethodID(id))
	}
	return
}

type AlarmID int

func AtoiAlarmID(s string) (AlarmID, error) { id, err := strconv.Atoi(s); return AlarmID(id), err }

func AlarmIDsFromInt64Slice(arr []int64) (res []AlarmID) {
	for _, id := range arr {
		res = append(res, AlarmID(id))
	}
	return
}

type EndType int

type BaselineID IntId

func (id BaselineID) Itoa() string { return strconv.Itoa(int(id)) }
func AtoiBaselineID(s string) (BaselineID, error) {
	id, err := strconv.Atoi(s)
	return BaselineID(id), err
}

const (
	EndTypeUpdate EndType = iota
	EndTypeHalting
	EndTypeDelete
)

var endTypeToString = map[EndType]string{
	EndTypeUpdate:  "EndTypeUpdate",
	EndTypeHalting: "EndTypeHalting",
	EndTypeDelete:  "EndTypeDelete",
}

func (e EndType) String() string { return endTypeToString[e] }

// MakeCidMapOrPanic parses a comma separated string of cids from e.g. an env var.
func MakeCidMapOrPanic(log logger.ContextL, mapName string, rawCidMap string) map[Cid]bool {
	cidMap, err := makeCidMap(rawCidMap)
	if err != nil {
		log.Errorf("could not parse %s, panicing: %s", mapName, err.Error())
		// This misconfiguration could be very bad, we don't want to process incorrect cids.
		// So, we panic.
		panic(err)
	}
	if cidMap == nil {
		log.Infof("cidMaps %s is nil", mapName)
	} else {
		log.Infof("cidMaps %s: %+v", mapName, cidMap)
	}
	return cidMap
}

func makeCidMap(rawCidMap string) (map[Cid]bool, error) {
	if rawCidMap == "" {
		return nil, nil
	}

	cidMap := make(map[Cid]bool)
	for _, s := range strings.Split(rawCidMap, ",") {
		cid, err := AtoiCid(s)
		if err != nil {
			return nil, err
		}
		cidMap[cid] = true
	}
	return cidMap, nil
}

func (fi *PolicyID) UnmarshalJSON(b []byte) error {
	if b[0] != '"' {
		var i int
		if err := json.Unmarshal(b, &i); err != nil {
			return err
		}
		*fi = PolicyID(i)
		return nil
	}

	if len(b) <= 2 { // empty string
		*fi = 0
		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*fi = PolicyID(i)
	return nil
}

// AppProtoID denotes id of app protocol
type AppProtoID uint32

// AppProtoIDSlice provides sort for AppProtoID
type AppProtoIDSlice []AppProtoID

func (a AppProtoIDSlice) Len() int           { return len(a) }
func (a AppProtoIDSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a AppProtoIDSlice) Less(i, j int) bool { return a[i] < a[j] }

// CustomColumnID denotes id of custom column in a flow
type CustomColumnID uint32

// CustomColumnIDSlice provides sort for CustomColumnID
type CustomColumnIDSlice []CustomColumnID

func (a CustomColumnIDSlice) Len() int           { return len(a) }
func (a CustomColumnIDSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a CustomColumnIDSlice) Less(i, j int) bool { return a[i] < a[j] }

// Status denotes status of given entity
type Status string

const (
	// StatusActive entity is active
	StatusActive = Status("A")
	// StatusDisabled entity is disabled
	StatusDisabled = Status("D")
	// StatusError entity encountered error
	StatusError = Status("ERR")
)

// SynTestId -- Synthetic test id
type SynTestID IntId
