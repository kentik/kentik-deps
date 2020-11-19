package kt

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// DeviceIDSet helps avoid duplicate device IDs
type DeviceIDSet struct {
	unique map[DeviceID]struct{}
	items  []DeviceID
}

func NewDeviceIDSet(dids ...DeviceID) *DeviceIDSet {
	didSet := &DeviceIDSet{}
	didSet.Add(dids...)
	return didSet
}

func (didSet *DeviceIDSet) String() string {
	return fmt.Sprintf("didSet(%v)", didSet.items)
}

func (didSet *DeviceIDSet) Add(dids ...DeviceID) {
	if didSet == nil || len(dids) == 0 {
		return
	}
	if didSet.items == nil || didSet.unique == nil {
		didSet.items = make([]DeviceID, 0, len(dids))
		didSet.unique = make(map[DeviceID]struct{}, len(dids))
	}
	for _, did := range dids {
		if _, seen := didSet.unique[did]; seen {
			continue
		}
		didSet.unique[did] = struct{}{}
		didSet.items = append(didSet.items, did)
	}
}

// All returns whether all specified devices are present in DeviceIDSet
func (didSet *DeviceIDSet) All(dids ...DeviceID) bool {
	for _, did := range dids {
		_, ok := didSet.unique[did]
		if !ok {
			return false
		}
	}

	return true
}

func (didSet *DeviceIDSet) Len() int {
	if didSet == nil || didSet.items == nil {
		return 0
	}
	return len(didSet.items)
}

func (didSet *DeviceIDSet) Items() []DeviceID {
	if didSet == nil || didSet.items == nil {
		return []DeviceID{}
	}
	return didSet.items
}

func (didSet *DeviceIDSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(didSet.items)
}

func (didSet *DeviceIDSet) UnmarshalJSON(b []byte) error {
	var devices []DeviceID
	err := json.Unmarshal(b, &devices)
	if err != nil {
		return err
	}
	didSet.Add(devices...)
	return nil
}

// Devices is a map of device ids to devices for a company.
type Devices map[DeviceID]Device

// GetDeviceInterface returns a specific interface by id from a device.
func (d Devices) GetDeviceInterface(did DeviceID, id IfaceID) (Interface, bool) {
	if device, ok := d[did]; ok {
		i, ok := device.Interfaces[id]
		return i, ok
	}
	return Interface{}, false
}

// A Device represents a device, corresponding to a row in mn_device.
// It also has all its interfaces attached.
type Device struct {
	ID         DeviceID
	Name       string
	Interfaces map[IfaceID]Interface
}

// A DeviceShortDetail is a few fields from a row in mn_device.
type DeviceShortDetail struct {
	ID   DeviceID `db:"id"`          // not null, serial primary key
	Name string   `db:"device_name"` // text not null
	Type string   `db:"device_type"` // text not null
}

// A CustomColumn corresponds a row in mn_kflow_field, which represents
// a field in the "Custom" field of a kflow record. This could either be
// a field we add to most kflow (e.g. i_ult_exit_network_bndry_name), or
// a "custom dimension" for a company (e.g. c_customer, c_kentik_services, etc.).
// The CustomMapValues field is filled in with data from mn_flow_tag_kv if
// it exists, but in a post HSCD (hyper-scale custom dimensions aka hippo tagging)
// world, these generally won't be there.
type CustomColumn struct {
	ID              uint32
	Name            string
	Type            string // kt.FORMAT_UINT32 or kt.FORMAT_STRING or kt.FORMAT_ADDR
	CustomMapValues map[uint32]string
}

// InterfaceCapacityBPS denotes capacity of interface in bits per second
type InterfaceCapacityBPS = uint64

// An Interface is everything we know about a device's interfaces.
// It corresponds to a row in mn_interface, joined with information
// from mn_device and mn_site.
type Interface struct {
	ID int64 `db:"id"`

	DeviceID   DeviceID `db:"device_id"`
	DeviceName string   `db:"device_name"`
	DeviceType string   `db:"device_type"`
	SiteID     int      `db:"site_id"`

	SnmpID               string        `db:"snmp_id"`
	SnmpSpeedMbps        int64         `db:"snmp_speed"` // unit? TODO: switch to uint64, rename to SnmpSpeedMbps
	SnmpType             int           `db:"snmp_type"`
	SnmpAlias            string        `db:"snmp_alias"`
	InterfaceIP          string        `db:"interface_ip"`
	InterfaceDescription string        `db:"interface_description"`
	Provider             string        `db:"provider"`
	VrfID                sql.NullInt64 `db:"vrf_id"`

	SiteTitle   string `db:"site_title"`
	SiteCountry string `db:"site_country"`
}

type InterfacePartial struct {
	Speed    int64
	Provider string
}

// SNMPSpeed returns interface speed in bits
func (i *Interface) SNMPSpeed() (InterfaceCapacityBPS, bool) {
	if i.SnmpSpeedMbps <= 0 || i.SnmpSpeedMbps >= 1<<31-1 {
		return 0, false
	}
	return uint64(i.SnmpSpeedMbps * 1e6), true
}

// SNMPID returns snmp id as integer
func (i *Interface) SNMPID() (IfaceID, error) {
	return AtoiIfaceID(i.SnmpID)
}

// A Site corresponds to a row in mn_site. It represents what we
// know about a location where devices are, e.g. a datacenter or office.
type Site struct {
	// Most of these fields are nullable, but we'll represent null
	// with the zero value for convenience.
	ID        int     `db:"id"`         // not nullable
	CompanyID Cid     `db:"company_id"` // nullable
	Title     string  `db:"title"`      // not nullable
	Latitude  float64 `db:"lat"`        // nullable
	Longitude float64 `db:"lon"`        // nullable
	Address   string  `db:"address"`    // nullable
	City      string  `db:"city"`       // nullable
	Region    string  `db:"region"`     // nullable
	Postal    string  `db:"postal"`     // nullable
	Country   string  `db:"country"`    // nullable
}

// A DeviceLabelsForCompany contains a map of devices to device labels
// and vice-versa.
// The backing data in the db is stored in mn_device_label + mn_device_label_device.
type DeviceLabelsForCompany struct {
	CompanyID        Cid
	DeviceToLabelSet map[DeviceID]map[IntId]bool
	LabelToDeviceSet map[IntId]map[DeviceID]bool
}

// A DeviceLabelRow corresponds to a row in mn_device_label.
type DeviceLabelRow struct {
	ID        IntId      `db:"id"`         // not null
	Name      string     `db:"name"`       // not null
	Edate     *time.Time `db:"edate"`      // nullable, default now
	Cdate     *time.Time `db:"cdate"`      // nullable, default now
	CompanyID Cid        `db:"company_id"` // not null

	// Fields we won't need for alerting.
	//Description *string    `db:"description"` // nullable
	//UserID      *IntId     `db:"user_id"`     // nullable
	//Color       *string    `db:"color"`       // nullable, with default
	//Order       int        `db:"order"`       // nullable
}

// A DeviceLabelDeviceRow corresponds to a row in mn_device_label_device.
// It is a join between mn_device and mn_device_label.
type DeviceLabelDeviceRow struct {
	ID       int      `db:"id"`        // not null
	DeviceID DeviceID `db:"device_id"` // not null, ref to mn_device
	LabelID  IntId    `db:"label_id"`  // not null, ref to mn_device_label
}

type DeviceInterfaceTuple struct {
	DeviceID    DeviceID
	InterfaceID IfaceID
}

func DevIf(did int, ifaceID int) DeviceInterfaceTuple {
	return DeviceInterfaceTuple{
		DeviceID: DeviceID(did), InterfaceID: IfaceID(ifaceID),
	}
}

type InterfaceGroupsForCompany struct {
	CompanyID           Cid
	GroupToInterfaceSet map[IfaceGroupID]map[DeviceInterfaceTuple]bool
}

// A InterfaceGroupInterfaceRow corresponds to a row in mn_interface_group_interface.
type InterfaceGroupInterfaceRow struct {
	DeviceID    DeviceID     `db:"device_id"` // not null, ref to mn_device
	InterfaceID IfaceID      `db:"snmp_id"`   // not null
	GroupID     IfaceGroupID `db:"group_id"`  // not null, ref to mn_interface_group
}

// VRFEntry virtual router entry for given device, as represented in chwww.mn_vrf
type VRFEntry struct {
	// TODO(tjonak): type(id) = int in mn_vrf, but bigint in mn_interface foreign key
	ID                    VRFEntryID `db:"id"`
	DeviceID              DeviceID   `db:"device_id"`
	Name                  string     `db:"name"`
	ExtRouteDistinguisher int64      `db:"ext_route_distinguisher"`
	RouteDistinguisher    string     `db:"route_distinguisher"`
	RouteTarget           string     `db:"route_target"`
	EDate                 time.Time  `db:"edate"`
}

// DeviceSubtypeMapping represents association between device_subtype and devices which belong to it
type DeviceSubtypeMapping struct {
	DeviceSubtype string `db:"device_subtype"`
	// DeviceIDs type should be kt.DeviceID but pq driver doesn't derive type properly
	DeviceIDs []int64 `db:"device_ids"`
}

// FlexMetric describes entries in chwww.mn_device_subtype_cols
type FlexMetric struct {
	SubtypeName       string     `db:"device_subtype"`
	CustomColumnName  string     `db:"custom_column"`
	MetricType        MetricType `db:"metric_type"`
	MetricDescription string     `db:"dimension_label"`
}

// DeviceSubtypeTuple represents matching between subtype and custom column
type UDRTuple struct {
	DeviceSubtype    string
	CustomColumnName string
}

// DeviceProxyBGPPair encapsulates an optional pair of device proxy addresses
type DeviceProxyBGPPair struct {
	V4 string
	V6 string
}

func (d DeviceProxyBGPPair) IP(v6 bool) string {
	if v6 {
		return d.V6
	} else {
		return d.V4
	}
}

// Returns the list of non empty IPs in this pair, if any
func (d DeviceProxyBGPPair) IPs() []string {
	res := make([]string, 0)
	if d.V4 != "" {
		res = append(res, d.V4)
	}
	if d.V6 != "" {
		res = append(res, d.V6)
	}
	return res
}

// ASN corresponds to a row in mn_asn.
type ASN struct {
	ID          int64  `db:"id"`          // not null, ASNs are in fact uint32 but postgres doesn't support unsigned, thus int64
	Description string `db:"description"` // nullable (coalesce to "")
}
