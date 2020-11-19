package ktds

import (
	"context"
	"fmt"
	"text/template"
	"time"

	"github.com/kentik/chf-alert/pkg/kt"
	"github.com/kentik/chf-alert/pkg/metric"

	"github.com/lib/pq"

	"github.com/jmoiron/sqlx"
	"github.com/kentik/eggs/pkg/database"
	"github.com/kentik/eggs/pkg/logger"
)

type deviceDataSource struct {
	logger.ContextL
	metrics metric.DBStore

	db *RoRwPairDBPair

	stmtLoadInterfaces                *sqlx.NamedStmt
	stmtLoadSites                     *sqlx.NamedStmt
	stmtLoadKFlowFields               *sqlx.NamedStmt
	stmtLoadTagFields                 *sqlx.NamedStmt
	stmtLoadTagValues                 *sqlx.NamedStmt
	stmtLoadDeviceLabels              *sqlx.NamedStmt
	stmtLoadDeviceLabelDevices        *sqlx.NamedStmt
	stmtLoadInterfaceGroupsInterfaces *sqlx.NamedStmt

	stmtLoadVRFEntries              *sqlx.Stmt
	stmtLoadDeviceSubtypeMapping    *sqlx.Stmt
	stmtLoadDeviceSubtypeMappingAll *sqlx.Stmt
	stmtLoadFlexDimensions          *sqlx.Stmt
	stmtLoadFlexMetrics             *sqlx.Stmt

	stmtLoadAvailableBGPFilteringEnabledDevices *sqlx.Stmt

	stmtFetchDeviceShortDetails         *sqlx.Stmt
	stmtFetchSitesByName                *sqlx.Stmt
	stmtFetchInterfacesByDeviceIDSNMPID *sqlx.Stmt
	stmtFetchInterfacesByDeviceID       *sqlx.Stmt
	stmtFetchASNs                       *sqlx.Stmt
	stmtIDsForNames                     *sqlx.Stmt

	stmtLoadConfigDigests    *sqlx.Stmt
	stmtRefreshConfigDigests *sqlx.Stmt

	stmtLoadCompanySpecificDimensions *sqlx.Stmt
}

// NewDeviceDataSourceContextL constructs valid instance of DeviceDataSource
func NewDeviceDataSourceContextL(db *RoRwPairDBPair, log logger.ContextL) (DeviceDataSource, error) {
	return newDeviceDataSourceInternal(db, logger.NewSubContextL(logger.SContext{S: "[deviceDS]"}, log))
}

// NewDeviceDataSource constructs valid instance of DeviceDataSource
func NewDeviceDataSource(db *RoRwPairDBPair, log logger.Underlying) (DeviceDataSource, error) {
	return newDeviceDataSourceInternal(db, logger.NewContextLFromUnderlying(logger.SContext{S: "deviceDS"}, log))
}

func newDeviceDataSourceInternal(db *RoRwPairDBPair, log logger.ContextL) (dds DeviceDataSource, retErr error) {
	defer func() {
		err := recover()
		if err != nil {
			retErr = fmt.Errorf("caught panic while initializing: %s", err)
		}
	}()
	var err error

	var ds = &deviceDataSource{
		ContextL: log,
		metrics:  metric.NewDB("DeviceDS"),

		db: db,

		stmtFetchDeviceShortDetails:         prepxOrPanic(db.Rox, stmtFetchDeviceShortDetailsSQL),
		stmtFetchSitesByName:                prepxOrPanic(db.Rox, stmtFetchSitesByNameSQL),
		stmtFetchInterfacesByDeviceIDSNMPID: prepxOrPanic(db.Rox, stmtFetchInterfacesByDeviceIDSNMPIDSQLExample),
		stmtFetchInterfacesByDeviceID:       prepxOrPanic(db.Rox, stmtFetchInterfacesByDeviceIDSQL),
		stmtFetchASNs:                       prepxOrPanic(db.Rox, stmtFetchASNsSQL),
		stmtLoadConfigDigests:               prepxOrPanic(db.Rox, stmtLoadConfigDigestsSQL),
		stmtRefreshConfigDigests:            prepxOrPanic(db.Rwx, stmtRefreshConfigDigestsSQL),
	}

	ds.stmtLoadInterfaces, err = db.Rox.PrepareNamed(`
		SELECT
			-- device fields
			d.id AS device_id,
			d.device_name,
			d.device_type,
			COALESCE(d.site_id, 0) AS site_id,

			-- interface fields
			COALESCE(i.snmp_id, '') AS snmp_id,
			COALESCE(i.snmp_speed, 0) AS snmp_speed,
			COALESCE(i.snmp_type, 0) AS snmp_type,
			COALESCE(i.snmp_alias, '') AS snmp_alias,
			COALESCE(i.interface_ip, '127.0.0.1') AS interface_ip,
			COALESCE(i.interface_description, '') AS interface_description,
			COALESCE(i.provider, '') AS provider,
			i.vrf_id as vrf_id,

			-- site fields
			COALESCE(s.title, '') AS site_title,
			COALESCE(s.country, '') AS site_country
		FROM mn_device AS d
		LEFT JOIN mn_interface AS i ON (d.id = i.device_id) AND (d.company_id = i.company_id)
		LEFT JOIN mn_site AS s ON (d.site_id = s.id) AND (d.company_id = s.company_id)
		WHERE d.company_id = :company_id
`)
	if err != nil {
		return nil, err
	}
	ds.stmtLoadSites, err = db.Rox.PrepareNamed(`
		SELECT
			id,
			COALESCE(company_id, 0) AS company_id,
			COALESCE(title, '') AS title,
			COALESCE(lat, 0) AS lat,
			COALESCE(lon, 0) AS lon,
			COALESCE(address, '') AS address,
			COALESCE(city, '') AS city,
			COALESCE(region, '') AS region,
			COALESCE(postal, '') AS postal,
			COALESCE(country, '') AS country
		FROM mn_site
		WHERE company_id = :company_id
`)
	if err != nil {
		return nil, err
	}
	ds.stmtLoadKFlowFields, err = db.Rox.PrepareNamed(`
		SELECT id, col_name, col_type
		FROM mn_kflow_field
		WHERE status = 'A' AND usage = 'kflow'
`)
	if err != nil {
		return nil, err
	}
	ds.stmtLoadTagFields, err = db.Rox.PrepareNamed(`
		SELECT id, col_name, col_type
		FROM mn_kflow_field
		WHERE status = 'A'
			AND usage = 'tag'
			AND company_id = :company_id
`)
	if err != nil {
		return nil, err
	}
	ds.stmtLoadTagValues, err = db.Rox.PrepareNamed(`
		SELECT
			ftk.field_id,
			ftk.flow_tag_id,
			ftk.field_value
		FROM mn_kflow_field kf
		JOIN mn_flow_tag_kv ftk ON ftk.field_id = kf.id
		WHERE kf.status = 'A'
			AND kf.usage = 'tag'
			AND kf.company_id = :company_id
`)
	if err != nil {
		return nil, err
	}
	ds.stmtLoadDeviceLabels, err = db.Rox.PrepareNamed(`
		SELECT id, name, edate, cdate, company_id
		FROM mn_device_label
		WHERE company_id = :company_id
`)
	if err != nil {
		return nil, err
	}
	ds.stmtLoadDeviceLabelDevices, err = db.Rox.PrepareNamed(`
		SELECT dld.id, dld.device_id, dld.label_id
		FROM mn_device_label_device dld
		INNER JOIN mn_device_label dl ON dld.label_id = dl.id
		WHERE dl.company_id = :company_id
`)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadInterfaceGroupsInterfaces, err = db.Rox.PrepareNamed(`
		SELECT group_id, device_id, snmp_id
		FROM mn_interface_group_interface
		WHERE company_id = :company_id AND end_time IS NULL
`)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadVRFEntries, err = db.Rox.Preparex(`
	SELECT id, device_id, name, ext_route_distinguisher, route_distinguisher, route_target, edate
	FROM mn_vrf
	WHERE company_id = $1
	`)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadDeviceSubtypeMapping, err = db.Rox.Preparex(`
	SELECT dsc.device_subtype, array_agg(DISTINCT company_device.id) FILTER (WHERE company_device IS NOT NULL) AS device_ids
	FROM mn_device_subtype_cols dsc
	LEFT JOIN
	(
		SELECT mn_device.id AS id, mn_device.device_subtype AS subtype
		FROM mn_device
		WHERE mn_device.company_id = $1
	) company_device ON dsc.device_subtype = company_device.subtype
	LEFT JOIN mn_device_subtypes ON dsc.device_subtype = mn_device_subtypes.device_subtype
	WHERE mn_device_subtypes.enabled
	GROUP BY dsc.device_subtype
	`)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadDeviceSubtypeMappingAll, err = db.Rox.Preparex(`
	SELECT d.company_id,  d.device_subtype, array_agg(d.id)
	FROM mn_device d JOIN mn_device_subtypes ds ON ds.device_subtype = d.device_subtype AND ds.enabled
	GROUP BY d.company_id, d.device_subtype
	`)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadFlexDimensions, err = db.Rox.Preparex(`
	SELECT 'ktsubtype__' || dsc.device_subtype || '__' || dsc.custom_column AS column_name
	FROM mn_device_subtype_cols dsc
	LEFT JOIN mn_device_subtypes ON dsc.device_subtype = mn_device_subtypes.device_subtype
	WHERE status = 'A' AND mn_device_subtypes.enabled
	ORDER BY dsc.device_subtype, custom_column
	`)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadFlexMetrics, err = db.Rox.Preparex(`
	SELECT dsc.device_subtype, dsc.custom_column, dsc.metadata->>'metric_type' AS metric_type, dsc.dimension_label
	FROM mn_device_subtype_cols dsc
	LEFT JOIN mn_device_subtypes ON dsc.device_subtype = mn_device_subtypes.device_subtype
	WHERE dsc.metadata->>'is_metric' = 'true' AND dsc.status = 'A' AND mn_device_subtypes.enabled
	ORDER BY dsc.device_subtype, custom_column, metric_type
	`)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadCompanySpecificDimensions, err = db.Rox.Preparex(stmtLoadCompanySpecificDimensionsSQL)
	if err != nil {
		return nil, err
	}

	ds.stmtIDsForNames, err = db.Rox.Preparex(stmtIDsForNamesSQL)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadAvailableBGPFilteringEnabledDevices, err = db.Rox.Preparex(stmtLoadAvailableBGPFilteringEnabledDevicesSQL)
	if err != nil {
		return nil, err
	}

	return ds, nil
}

func (ds *deviceDataSource) Close() {
	database.CloseStatements(ds)
}

const stmtRefreshConfigDigestsSQL = `SELECT FROM refresh_mn_alert_config_digest($1)`

func (ds *deviceDataSource) RefreshConfigDigests(maxAgeMinutes int) (err error) {
	defer func(start time.Time) { ds.metrics.Get("RefreshConfigDigests", 0).Done(start, err) }(time.Now())

	rows, err := ds.stmtRefreshConfigDigests.Queryx(maxAgeMinutes)
	if err != nil {
		return
	}

	err = rows.Close()
	return
}

const stmtLoadConfigDigestsSQL = `SELECT company_id, digest FROM alert_config_digest($1)`

// return a map of cid -> configuration digest
// this should covers all the things we want to keep fresh: policies, thresholds, devices, interfaces, labels, ...
func (ds *deviceDataSource) LoadConfigDigests(maxAgeMinutes int) (map[kt.Cid]string, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("LoadConfigDigests", 0).Done(start, err) }(time.Now())

	rows, err := ds.stmtLoadConfigDigests.Queryx(maxAgeMinutes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[kt.Cid]string)

	entry := &struct {
		Cid    kt.Cid `db:"company_id"`
		Digest string `db:"digest"`
	}{}
	for rows.Next() {
		err = rows.StructScan(entry)
		if err != nil {
			return nil, err
		}
		result[entry.Cid] = entry.Digest
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (ds *deviceDataSource) LoadInterfaces(cid kt.Cid) (interfaces []*kt.Interface, err error) {
	defer func(start time.Time) { ds.metrics.Get("LoadInterfaces", cid).Done(start, err) }(time.Now())

	err = ds.stmtLoadInterfaces.Select(&interfaces, qCid{cid})
	if err != nil {
		return nil, fmt.Errorf("stmtLoadInterfaces.Select: %v", err)
	}

	return
}

func (ds *deviceDataSource) DevicesFromInterfaces(cid kt.Cid, ifs []*kt.Interface) (kt.Devices, error) {
	devices := make(kt.Devices, len(ifs))

	for _, i := range ifs {
		device, ok := devices[i.DeviceID]
		if !ok {
			device = kt.Device{
				ID:         i.DeviceID,
				Name:       i.DeviceName,
				Interfaces: make(map[kt.IfaceID]kt.Interface),
			}
			devices[i.DeviceID] = device
		}

		if i.SnmpID == "" {
			continue
		}

		id, err := i.SNMPID()
		if err != nil {
			ds.Errorf("Could not parse SnmpID '%s' as integer for interface %d", i.SnmpID, i.DeviceID)
			continue
		}

		device.Interfaces[id] = *i
	}

	return devices, nil
}

func (ds *deviceDataSource) LoadSites(cid kt.Cid) (map[int]*kt.Site, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("LoadSites", cid).Done(start, err) }(time.Now())

	var sites []*kt.Site
	err = ds.stmtLoadSites.Select(&sites, qCid{cid})
	if err != nil {
		return nil, fmt.Errorf("stmtLoadSites.Select: %v", err)
	}
	sitesMap := make(map[int]*kt.Site, len(sites))
	for _, site := range sites {
		sitesMap[site.ID] = site
	}

	return sitesMap, nil
}

func (ds *deviceDataSource) LoadCustomColumns(cid kt.Cid) (map[uint32]*kt.CustomColumn, error) {
	type kflowFieldRow struct { // from mn_kflow_field
		ID   uint32 `db:"id"`
		Name string `db:"col_name"`
		Type string `db:"col_type"`
	}
	mtTagFields := ds.metrics.Get("stmtLoadTagFields", cid)
	start := time.Now()

	var tagFieldRows []kflowFieldRow
	err := ds.stmtLoadTagFields.Select(&tagFieldRows, qCid{cid})
	if err != nil {
		mtTagFields.Error(start)
		return nil, fmt.Errorf("stmtLoadTagFields.Select: %v", err)
	}

	mtTagFields.Success(start)

	var tagValueRows []struct { // from mn_flow_tag_kv
		FieldID    uint32 `db:"field_id"`
		FlowTagID  uint32 `db:"flow_tag_id"`
		FieldValue string `db:"field_value"`
	}

	mtTagValues := ds.metrics.Get("stmtLoadTagValues", cid)
	start = time.Now()

	err = ds.stmtLoadTagValues.Select(&tagValueRows, qCid{cid})
	if err != nil {
		mtTagValues.Error(start)
		return nil, fmt.Errorf("stmtLoadTagValues.Select: %v", err)
	}
	mtTagValues.Success(start)

	mtKflowFields := ds.metrics.Get("stmtLoadKFlowFields", cid)
	start = time.Now()

	var kflowFieldRows []kflowFieldRow
	err = ds.stmtLoadKFlowFields.Select(&kflowFieldRows, struct{}{})
	if err != nil {
		mtKflowFields.Error(start)
		return nil, fmt.Errorf("stmtLoadKFlowFields.Select: %v", err)
	}
	mtKflowFields.Success(start)

	customs := make(map[uint32]*kt.CustomColumn)
	for i := range tagFieldRows {
		customs[tagFieldRows[i].ID] = &kt.CustomColumn{
			ID:              tagFieldRows[i].ID,
			Name:            tagFieldRows[i].Name,
			Type:            tagFieldRows[i].Type,
			CustomMapValues: make(map[uint32]string),
		}
	}
	for i := range tagValueRows {
		col, ok := customs[tagValueRows[i].FieldID]
		if !ok { // We didn't see this field definition, although we just queried it. Unexpected.
			continue
		}
		col.CustomMapValues[tagValueRows[i].FlowTagID] = tagValueRows[i].FieldValue
	}
	for i := range kflowFieldRows {
		customs[kflowFieldRows[i].ID] = &kt.CustomColumn{
			ID:              kflowFieldRows[i].ID,
			Name:            kflowFieldRows[i].Name,
			Type:            kflowFieldRows[i].Type,
			CustomMapValues: nil,
		}
	}

	ds.Debugf("Got customs from DB for cid=%d: %d", cid, len(customs))

	return customs, nil
}

func (ds *deviceDataSource) LoadDeviceLabels(companyID kt.Cid) (*kt.DeviceLabelsForCompany, error) {
	_, deviceLabelDeviceRows, err := ds.loadDeviceLabelsRows(companyID)
	if err != nil {
		return nil, err
	}

	return deviceLabelsForCompanyFromRows(companyID, deviceLabelDeviceRows), nil
}

func (ds *deviceDataSource) loadDeviceLabelsRows(companyID kt.Cid) ([]*kt.DeviceLabelRow, []*kt.DeviceLabelDeviceRow, error) {
	mt := ds.metrics.Get("stmtLoadDeviceLabels", companyID)
	start := time.Now()

	var deviceLabelRows []*kt.DeviceLabelRow
	err := ds.stmtLoadDeviceLabels.Select(&deviceLabelRows, qCid{companyID})
	if err != nil {
		mt.Error(start)
		return nil, nil, fmt.Errorf("LoadDeviceLabels: stmtLoadDeviceLabels: %v", err)
	}
	mt.Success(start)

	mt2 := ds.metrics.Get("stmtLoadDeviceLabelDevices", companyID)
	start = time.Now()

	var deviceLabelDeviceRows []*kt.DeviceLabelDeviceRow
	err = ds.stmtLoadDeviceLabelDevices.Select(&deviceLabelDeviceRows, qCid{companyID})
	if err != nil {
		mt2.Error(start)
		return nil, nil, fmt.Errorf("LoadDeviceLabels: stmtLoadDeviceLabelDevices: %v", err)
	}
	mt2.Success(start)

	return deviceLabelRows, deviceLabelDeviceRows, nil
}

func deviceLabelsForCompanyFromRows(companyID kt.Cid, deviceLabelDeviceRows []*kt.DeviceLabelDeviceRow) *kt.DeviceLabelsForCompany {
	deviceToLabelSet := make(map[kt.DeviceID]map[kt.IntId]bool)
	labelToDeviceSet := make(map[kt.IntId]map[kt.DeviceID]bool)
	for _, labelDevice := range deviceLabelDeviceRows {
		did := labelDevice.DeviceID
		lid := labelDevice.LabelID

		if deviceToLabelSet[did] == nil {
			deviceToLabelSet[did] = make(map[kt.IntId]bool)
		}
		deviceToLabelSet[did][lid] = true

		if labelToDeviceSet[lid] == nil {
			labelToDeviceSet[lid] = make(map[kt.DeviceID]bool)
		}
		labelToDeviceSet[lid][did] = true
	}

	return &kt.DeviceLabelsForCompany{
		CompanyID:        companyID,
		DeviceToLabelSet: deviceToLabelSet,
		LabelToDeviceSet: labelToDeviceSet,
	}
}

func (ds *deviceDataSource) LoadInterfaceGroups(companyID kt.Cid) (*kt.InterfaceGroupsForCompany, error) {
	rows, err := ds.loadInterfaceGroupsInterfaces(companyID)
	if err != nil {
		return nil, err
	}
	res := interfaceGroupForCompanyFromRows(companyID, rows)
	ds.Debugf("LoadInterfaceGroups: %+v", res)
	return res, nil
}

func (ds *deviceDataSource) loadInterfaceGroupsInterfaces(companyID kt.Cid) (rows []*kt.InterfaceGroupInterfaceRow, err error) {
	defer func(start time.Time) { ds.metrics.Get("stmtLoadInterfaceGroupsInterfaces", companyID).Done(start, err) }(time.Now())

	err = ds.stmtLoadInterfaceGroupsInterfaces.Select(&rows, qCid{companyID})
	if err != nil {
		return nil, fmt.Errorf("LoadInterfaceGroups: stmtLoadInterfaceGroupsInterfaces: %v", err)
	}
	return rows, nil
}

func interfaceGroupForCompanyFromRows(companyID kt.Cid, rows []*kt.InterfaceGroupInterfaceRow) *kt.InterfaceGroupsForCompany {
	groupToInterfaceSet := make(map[kt.IfaceGroupID]map[kt.DeviceInterfaceTuple]bool)

	for _, row := range rows {
		dif := kt.DeviceInterfaceTuple{
			DeviceID:    row.DeviceID,
			InterfaceID: row.InterfaceID,
		}
		gid := row.GroupID

		ifSet := groupToInterfaceSet[gid]
		if ifSet == nil {
			ifSet = map[kt.DeviceInterfaceTuple]bool{}
			groupToInterfaceSet[gid] = ifSet
		}
		ifSet[dif] = true
	}

	return &kt.InterfaceGroupsForCompany{
		CompanyID:           companyID,
		GroupToInterfaceSet: groupToInterfaceSet,
	}
}

// LoadVRFData fetches information about virtual routers for given subset of companies
func (ds *deviceDataSource) LoadVRFData(cid kt.Cid) (map[kt.DeviceID][]*kt.VRFEntry, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("LoadVRFData", cid).Done(start, err) }(time.Now())

	vrfEntries := map[kt.DeviceID][]*kt.VRFEntry{}

	rows, err := ds.stmtLoadVRFEntries.Queryx(cid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		entry := &kt.VRFEntry{}
		err = rows.StructScan(entry)
		if err != nil {
			break
		}

		vrfEntries[entry.DeviceID] = append(vrfEntries[entry.DeviceID], entry)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return vrfEntries, nil
}

// LoadDeviceSubtypeColumns pulls latest data about devices of given subtype for given company
func (ds *deviceDataSource) LoadDeviceSubtypeColumns(cid kt.Cid) (
	perSubtypeDeviceMapping map[string]map[kt.DeviceID]bool,
	err error,
) {
	defer func(start time.Time) { ds.metrics.Get("LoadDeviceSubtypeColumns", cid).Done(start, err) }(time.Now())

	rows, err := ds.stmtLoadDeviceSubtypeMapping.Queryx(cid)
	if err != nil {
		return
	}
	defer rows.Close()

	perSubtypeDeviceMapping = map[string]map[kt.DeviceID]bool{}

	for rows.Next() {
		subtypeMapping := &kt.DeviceSubtypeMapping{}

		// Due to limited type inference, pq driver cannot deduce that []kt.DeviceID is equal to []int64
		// this thing can be mitigated using unsafe package following way
		//
		// reinterpretPointer := (*[]int64)(unsafe.Pointer(&subtypeMapping.DeviceIDs))
		// err = rows.Scan(&subtypeMapping.DeviceSubtype, pq.Array(reinterpretPointer))
		// But on the other hand we could just use int64 here, which sounds reasonable
		err = rows.Scan(&subtypeMapping.DeviceSubtype, pq.Array(&subtypeMapping.DeviceIDs))
		if err != nil {
			break
		}

		destSubtypeMapping := map[kt.DeviceID]bool{}
		for _, id := range subtypeMapping.DeviceIDs {
			destSubtypeMapping[kt.DeviceID(id)] = true
		}

		perSubtypeDeviceMapping[subtypeMapping.DeviceSubtype] = destSubtypeMapping
	}

	err = rows.Err()

	return
}

func (ds *deviceDataSource) LoadDeviceSubtypeMappings(ctx context.Context) (
	mapping map[kt.Cid]map[string][]kt.DeviceID,
	err error,
) {
	defer func(start time.Time) { ds.metrics.Get("LoadDeviceSubtypeMappings", 0).Done(start, err) }(time.Now())

	rows, err := ds.stmtLoadDeviceSubtypeMappingAll.QueryContext(context.Background())
	if err != nil {
		err = fmt.Errorf("from LoadDeviceSubtypeMappings: %w", err)
		return
	}
	defer rows.Close()

	mapping = map[kt.Cid]map[string][]kt.DeviceID{}
	for rows.Next() {
		var (
			cid       kt.Cid
			subtype   string
			deviceIDs []kt.DeviceID
		)

		err = rows.Scan(&cid, &subtype, pq.Array(&deviceIDs))
		if err != nil {
			err = fmt.Errorf("from LoadDeviceSubtypeMappings: %w", err)
			return
		}

		devicesBySubtype, ok := mapping[cid]
		if !ok {
			devicesBySubtype = map[string][]kt.DeviceID{}
			mapping[cid] = devicesBySubtype
		}
		devicesBySubtype[subtype] = deviceIDs
	}

	err = rows.Err()
	return
}

func (ds *deviceDataSource) LoadFlexDimensions(ctx context.Context) (flexDims []string, err error) {
	defer func(start time.Time) { ds.metrics.Get("LoadFlexDimensions", 0).Done(start, err) }(time.Now())

	err = ds.stmtLoadFlexDimensions.SelectContext(ctx, &flexDims)
	return
}

func (ds *deviceDataSource) LoadFlexMetrics(ctx context.Context) (result []kt.FlexMetric, err error) {
	defer func(start time.Time) { ds.metrics.Get("LoadFlexMetrics", 0).Done(start, err) }(time.Now())

	err = ds.stmtLoadFlexMetrics.SelectContext(ctx, &result)
	return
}

const stmtFetchDeviceShortDetailsSQL = `
SELECT id, device_name, device_type
FROM mn_device
WHERE id = ANY($1)
`

func (ds *deviceDataSource) FetchDeviceShortDetails(ctx context.Context, deviceIDs *kt.DeviceIDSet) (map[kt.DeviceID]kt.DeviceShortDetail, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchDeviceShortDetails", 0).Done(start, err) }(time.Now())

	var deviceDetails []kt.DeviceShortDetail
	err = ds.stmtFetchDeviceShortDetails.SelectContext(ctx, &deviceDetails, pq.Array(deviceIDs.Items()))
	if err != nil {
		return nil, err
	}

	m := make(map[kt.DeviceID]kt.DeviceShortDetail)
	for i := range deviceDetails {
		m[deviceDetails[i].ID] = deviceDetails[i]
	}

	return m, nil
}

const stmtFetchSitesByNameSQL = `
SELECT id, title
FROM mn_site
WHERE company_id = $1 AND title = ANY($2)
`

func (ds *deviceDataSource) FetchSitesByName(ctx context.Context, cid kt.Cid, siteNames []string) (map[string]kt.Site, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchSitesByName", cid).Done(start, err) }(time.Now())

	var sites []kt.Site
	err = ds.stmtFetchSitesByName.SelectContext(ctx, &sites, cid, pq.Array(siteNames))
	if err != nil {
		return nil, err
	}

	m := make(map[string]kt.Site)
	for i := range sites {
		m[sites[i].Title] = sites[i]
	}

	return m, nil
}

const stmtFetchInterfacesByDeviceIDSNMPIDSQLTemplateString = `
SELECT
  id,
  device_id,
  snmp_id,
  coalesce(interface_description, '') AS interface_description,
  coalesce(snmp_speed, 0) AS snmp_speed
FROM mn_interface
WHERE company_id = $1
  AND (
{{- range $ix, $val := .Pairs -}}
  {{- if (gt $ix 0)}} OR {{end -}}
  (device_id = ${{$val.DeviceIDArg}} AND snmp_id = ${{$val.SNMPIDArg}})
{{- end -}}
  )
`

var stmtFetchInterfacesByDeviceIDSNMPIDSQLTemplate = template.Must(template.New("").Parse(stmtFetchInterfacesByDeviceIDSNMPIDSQLTemplateString))

var stmtFetchInterfacesByDeviceIDSNMPIDSQLExample = mustRender(stmtFetchInterfacesByDeviceIDSNMPIDSQLTemplate, didSNMPIDTemplateArgs{
	Pairs: []argPair{{DeviceIDArg: 2, SNMPIDArg: 3}, {DeviceIDArg: 4, SNMPIDArg: 5}},
})

type didSNMPIDTemplateArgs struct {
	Pairs []argPair
}

type argPair struct {
	DeviceIDArg int
	SNMPIDArg   int
}

func (ds *deviceDataSource) FetchInterfacesByDeviceIDSNMPID(
	ctx context.Context,
	cid kt.Cid,
	didSNMPIDs []kt.DeviceIDSNMPIDPair,
) (map[kt.DeviceIDSNMPIDPair]kt.Interface, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchInterfacesByDeviceIDSNMPID", cid).Done(start, err) }(time.Now())

	// FIXME: would be way simpler to do this with a static sql statement and
	// some kind of multidimensional array, or tuples, but nothing quite works here
	// that also lets us use the db indexes on (company_id, device_id, snmp_id).
	argPairs := make([]argPair, len(didSNMPIDs))
	args := make([]interface{}, 0, len(didSNMPIDs)*2+1)
	args = append(args, cid)
	for i, pair := range didSNMPIDs {
		argPairs[i] = argPair{DeviceIDArg: 2 * (i + 1), SNMPIDArg: 2*(i+1) + 1}
		args = append(args, pair.DeviceID)
		args = append(args, pair.SNMPID)
	}

	sql, err := render(stmtFetchInterfacesByDeviceIDSNMPIDSQLTemplate, didSNMPIDTemplateArgs{Pairs: argPairs})
	if err != nil {
		return nil, err
	}

	var interfaces []kt.Interface
	err = ds.db.Rox.SelectContext(ctx, &interfaces, sql, args...)
	if err != nil {
		return nil, err
	}

	m := make(map[kt.DeviceIDSNMPIDPair]kt.Interface)
	for i := range interfaces {
		m[kt.DeviceIDSNMPIDPair{
			DeviceID: interfaces[i].DeviceID,
			SNMPID:   interfaces[i].SnmpID,
		}] = interfaces[i]
	}
	return m, nil
}

const stmtFetchInterfacesByDeviceIDSQL = `
SELECT id, snmp_id
FROM mn_interface
WHERE device_id = ANY($1)
`

func (ds *deviceDataSource) FetchInterfacesByDeviceID(ctx context.Context, dids *kt.DeviceIDSet) (map[string]kt.Interface, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchInterfacesByDeviceID", 0).Done(start, err) }(time.Now())

	var interfaces []kt.Interface
	err = ds.stmtFetchInterfacesByDeviceID.SelectContext(ctx, &interfaces, pq.Array(dids.Items()))
	if err != nil {
		return nil, err
	}

	m := make(map[string]kt.Interface)
	for i := range interfaces {
		m[interfaces[i].SnmpID] = interfaces[i]
	}

	return m, nil
}

const stmtFetchASNsSQL = `
SELECT id, coalesce(description, '') AS description
FROM mn_asn
WHERE id = ANY($1)
`

func (ds *deviceDataSource) FetchASNs(ctx context.Context, asns []int64) (map[int64]kt.ASN, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchASNs", 0).Done(start, err) }(time.Now())

	var asnDetails []kt.ASN
	err = ds.stmtFetchASNs.SelectContext(ctx, &asnDetails, pq.Array(asns))
	if err != nil {
		return nil, err
	}

	m := make(map[int64]kt.ASN)
	for i := range asnDetails {
		m[asnDetails[i].ID] = asnDetails[i]
	}

	return m, nil
}

// TODO(tjonak): add other tag based dimensions, check whether we can just use all or there are some constraints to that
const stmtLoadCompanySpecificDimensionsSQL = `
SELECT col_name FROM mn_kflow_field
WHERE status = 'A'
  AND col_name ILIKE 'kt_%'
  AND company_id = $1
`

func (ds *deviceDataSource) LoadCompanySpecificDimensions(
	ctx context.Context,
	companyID kt.Cid,
) (companyDimensions []string, err error) {
	defer func(start time.Time) { ds.metrics.Get("LoadCompanySpecificDimensions", companyID).Done(start, err) }(time.Now())

	err = ds.stmtLoadCompanySpecificDimensions.SelectContext(ctx, &companyDimensions, companyID)

	return
}

const stmtIDsForNamesSQL = `SELECT id, device_name FROM mn_device WHERE company_id = $1 AND device_name = ANY($2)`

func (ds *deviceDataSource) IDsForDeviceNames(ctx context.Context, companyID kt.Cid, deviceNames []string) (result map[string]kt.DeviceID, err error) {
	result = map[string]kt.DeviceID{}
	defer func(start time.Time) { ds.metrics.Get("IDsForNames", companyID).Done(start, err) }(time.Now())

	var didNames []struct {
		ID   kt.DeviceID `db:"id"`
		Name string      `db:"device_name"`
	}

	err = ds.stmtIDsForNames.SelectContext(ctx, &didNames, companyID, pq.Array(deviceNames))
	if err != nil {
		return nil, fmt.Errorf("IDsForDeviceNames: %v", err)
	}

	for _, tpl := range didNames {
		result[tpl.Name] = tpl.ID
	}

	return
}

const stmtLoadAvailableBGPFilteringEnabledDevicesSQL = `
SELECT id
FROM mn_device
WHERE company_id=$1
  AND (device_kvs->'device_bgp_flowspec')::boolean
  AND id::text NOT IN (
	  SELECT json_array_elements((platform_mitigation_device_detail::json->>'devices')::json)::text
	  FROM mn_mitigation_platform
	  WHERE company_id = $1
	    AND platform_mitigation_device_type = ANY($2)
  )
`

// LoadBGPEnabledDevices loads device ids of devices capable of rtbh/flowspec filtering
// ui/database uses device_bgp_flowspec tag, but this doesn't capture whole scope i guess
func (ds *deviceDataSource) LoadAvailableBGPFilteringEnabledDevices(
	ctx context.Context,
	companyID kt.Cid,
) (didSet *kt.DeviceIDSet, err error) {
	defer func(start time.Time) {
		ds.metrics.Get("LoadAvailableBGPFilteringEnabledDevices", companyID).Done(start, err)
	}(time.Now())

	devices := []kt.DeviceID{}
	err = ds.stmtLoadAvailableBGPFilteringEnabledDevices.SelectContext(ctx, &devices, companyID, pq.Array(kt.BGPMitigationTypes))
	if err != nil {
		return nil, fmt.Errorf("stmtLoadAvailableBGPFilteringEnabledDevices: %w", err)
	}

	return kt.NewDeviceIDSet(devices...), nil
}
