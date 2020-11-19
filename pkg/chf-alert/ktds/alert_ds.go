package ktds

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/kentik/chf-alert/pkg/alert/appproto"
	"github.com/kentik/chf-alert/pkg/kt"
	"github.com/kentik/chf-alert/pkg/metric"

	"github.com/kentik/eggs/pkg/database"
	"github.com/kentik/eggs/pkg/logger"
	"github.com/kentik/eggs/pkg/preconditions"
	"github.com/pkg/errors"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq" // Load postgres driver
)

type alertDataSource struct {
	logger.ContextL

	dbrwx *sqlx.DB

	metrics metric.DBStore

	stmtLoadAlertPolicies     *sqlx.Stmt
	stmtLoadAlertPoliciesDone *sql.Stmt
	stmtSavedFiltersOld       *sql.Stmt
	stmtPolicyBad             *sql.Stmt
	stmtListCompanies         *sqlx.Stmt // Get a list of all companies regardless of policies defined
	stmtSavedFilters          *sqlx.Stmt
	stmtOidFromUserID         *sql.Stmt

	stmtGetPolicy             *sqlx.Stmt
	stmtFindPolicies          *sqlx.NamedStmt
	stmtFindPoliciesCount     *sqlx.NamedStmt
	stmtFindPoliciesWithLimit *sqlx.NamedStmt
	stmtDeletePolicy          *sql.Stmt
	stmtCreatePolicy          *sqlx.NamedStmt
	tmplUpdatePolicy          *template.Template
	stmtModifyPolicyStatus    *sql.Stmt
	stmtMutePolicy            *sql.Stmt
	stmtUnmutePolicy          *sql.Stmt

	stmtAllPoliciesExist *sqlx.Stmt

	stmtLoadAppProtocolMappings   *sql.Stmt
	stmtLoadAppProtocolMetrics    *sqlx.Stmt
	stmtLoadAppProtocolDimensions *sqlx.Stmt
	stmtLoadNonSTDAppProtocols    *sqlx.Stmt
	stmtLoadAppProtocolSNMPBundle *sqlx.Stmt

	stmtFetchUserIDAndHydraForCompany *sqlx.Stmt

	*devicesByCidStore
	stmtFetchSelectedDevicesForCompany *sqlx.Stmt

	stmtGetSavedFilter        *sqlx.Stmt
	stmtDeleteSavedFilter     *sqlx.Stmt
	tmplCreateSavedFilter     *template.Template
	tmplUpdateSavedFilter     *template.Template
	stmtFindSavedFilters      *sqlx.Stmt
	stmtFindSavedFiltersCount *sqlx.Stmt

	permittedSavedFilterColumns map[kt.SQLColumn]struct{}
	permittedPolicyColumns      map[kt.SQLColumn]struct{}

	cachedAppProtoMetrics *cachedValue
	cachedAppProtoDims    *cachedValue
	cachedAppProtoMapping *cachedValue
}

const cachedAppProtoTTL = 1 * time.Hour

// NewAlertDataSource constructs alert data source instance
func NewAlertDataSource(db *RoRwPairDBPair, log logger.Underlying) (AlertDataSource, error) {
	var err error

	var ds = &alertDataSource{
		ContextL: logger.NewContextLFromUnderlying(logger.SContext{S: "alertDS"}, log),
		metrics:  metric.NewDB("PolicyDS"),
		dbrwx:    db.Rwx,
	}

	ds.stmtLoadAlertPolicies, err = db.Rox.Preparex(`
	SELECT
		id,
		company_id,
		COALESCE(user_id, 0) AS user_id,
		policy_name,
		policy_window,
		is_top,
		is_auto_calc,
		limit_num,
		store_num,
		dimensions,
		metric,
		filters,
		min_traffic,
		learning_mode,
		status,
		COALESCE(baseline, '{}') AS baseline,
		mode,
		cdate,
		edate,
		COALESCE(learning_mode_expire_date, now()-interval'1 day') AS learning_mode_expire_date,
		COALESCE(policy_description, '') AS policy_description,
		COALESCE(primary_dimension, '{}') AS primary_dimension,
		COALESCE(selected_devices, '{}') AS selected_devices
	FROM mn_alert_policy
	WHERE company_id = COALESCE($1, company_id)
		AND edate > COALESCE($2, '1970-01-01'::timestamp)
		AND id = COALESCE($3, id)
		AND status = $4`)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadAlertPoliciesDone, err = db.Ro.Prepare(`
	SELECT id, edate, status
	FROM mn_alert_policy
	WHERE company_id = COALESCE($1,company_id) AND edate > COALESCE($2, '1970-01-01'::timestamp) AND id = COALESCE($3, id) AND status <> $4
	UNION
	SELECT id, archived_on AS edate, 'R'
	FROM mn_archived_alert_policy
	WHERE company_id = COALESCE($5, company_id) AND archived_on > COALESCE($6, now()) AND id = COALESCE($7, id)`)
	if err != nil {
		return nil, err
	}

	ds.stmtSavedFiltersOld, err = db.Ro.Prepare(`
	SELECT id, policy_id, FILTERS, COALESCE(is_not, false), edate
	FROM chv_alert_policy_saved_filter
	WHERE company_id = $1 OR (filter_level = 'global' and policy_id IN (SELECT id FROM mn_alert_policy WHERE company_id = $2))`)
	if err != nil {
		return nil, err
	}

	ds.stmtPolicyBad, err = db.Rw.Prepare(`UPDATE mn_alert_policy SET status = 'ERR', edate=now() WHERE id = $1`)
	if err != nil {
		return nil, err
	}

	ds.stmtListCompanies, err = db.Rox.Preparex(`SELECT id FROM mn_company WHERE company_status = 'V'`)
	if err != nil {
		return nil, err
	}

	ds.stmtSavedFilters, err = db.Rox.Preparex(`
	SELECT id, filters, cdate, edate
	FROM mn_saved_filter
	WHERE (company_id = $1 OR filter_level='global')
	`)
	if err != nil {
		return nil, err
	}
	ds.stmtOidFromUserID, err = db.Ro.Prepare(`
	SELECT oid FROM mn_user
	INNER JOIN pg_roles ON rolname = regexp_replace(user_hydra_connection, 'postgres://([^:]+).*', '\1')
	WHERE mn_user.id = $1
	`)
	if err != nil {
		return nil, err
	}

	ds.stmtFindPolicies, err = db.Rox.PrepareNamed(stmtFindPoliciesSQL)
	if err != nil {
		return nil, err
	}

	ds.stmtFindPoliciesCount, err = db.Rox.PrepareNamed(stmtFindPoliciesCountSQL)
	if err != nil {
		return nil, err
	}

	ds.stmtFindPoliciesWithLimit, err = db.Rox.PrepareNamed(stmtFindPoliciesSQLWithLimitSQL)
	if err != nil {
		return nil, err
	}

	ds.stmtDeletePolicy, err = db.Rwx.Prepare(stmtDeletePolicySQL)
	if err != nil {
		return nil, err
	}

	ds.stmtCreatePolicy, err = db.Rwx.PrepareNamed(stmtCreatePolicySQL)
	if err != nil {
		return nil, err
	}

	ds.stmtModifyPolicyStatus, err = db.Rwx.Prepare(stmtModifyPolicyStatusSQL)
	if err != nil {
		return nil, err
	}

	ds.stmtMutePolicy, err = db.Rwx.Prepare(stmtMutePolicySQL)
	if err != nil {
		return nil, err
	}

	ds.stmtUnmutePolicy, err = db.Rwx.Prepare(stmtUnmutePolicySQL)
	if err != nil {
		return nil, err
	}

	ds.stmtLoadAppProtocolMappings, err = db.Rox.Prepare(stmtLoadAppProtocolMappingsSQL)
	if err != nil {
		return nil, err
	}

	pgSqlxRoStmts := []struct {
		dbx     *sqlx.DB
		stmtPtr **sqlx.Stmt
		stmt    string
	}{
		{db.Rox, &ds.stmtGetPolicy, stmtGetPolicySQL},
		{db.Rox, &ds.stmtLoadAppProtocolMetrics, stmtLoadAppProtocolMetricsSQL},
		{db.Rox, &ds.stmtLoadAppProtocolDimensions, stmtLoadAppProtocolDimensionsSQL},
		{db.Rox, &ds.stmtFetchSelectedDevicesForCompany, stmtFetchSelectedDevicesForCompanySQL},
		{db.Rox, &ds.stmtLoadNonSTDAppProtocols, stmtLoadNonSTDAppProtocolsSQL},
		{db.Rox, &ds.stmtLoadAppProtocolSNMPBundle, stmtLoadAppProtocolSNMPBundleSQL},
		{db.Rox, &ds.stmtFetchUserIDAndHydraForCompany, fetchUserIDAndHydraForCompanySQL},
		{db.Rox, &ds.stmtAllPoliciesExist, stmtAllPoliciesExistSQL},
		{db.Rox, &ds.stmtGetSavedFilter, stmtGetSavedFilterSQL},
		{db.Rox, &ds.stmtFindSavedFilters, stmtFindSavedFiltersSQL},
		{db.Rox, &ds.stmtFindSavedFiltersCount, stmtFindSavedFiltersCountSQL},

		{db.Rwx, &ds.stmtDeleteSavedFilter, stmtDeleteSavedFilterSQL},
	}
	for _, data := range pgSqlxRoStmts {
		*data.stmtPtr, err = data.dbx.Preparex(data.stmt)
		if err != nil {
			return nil, err
		}
	}

	ds.tmplUpdatePolicy = template.Must(
		template.New("stmtUpdatePolicySQLTemplate").
			Funcs(template.FuncMap{"offset": func(i int) int { return i + stmtUpdatePolicySQLArgOffset }}).
			Parse(stmtUpdatePolicySQLTemplate),
	)

	ds.tmplCreateSavedFilter = template.Must(
		template.New("stmtCreateSavedFilter").
			Funcs(template.FuncMap{"offset": func(i int) int { return i + stmtCreateSavedFilterSQLArgOffset }}).
			Parse(stmtCreateSavedFilterSQL),
	)

	ds.tmplUpdateSavedFilter = template.Must(
		template.New("stmtUpdatePolicySQLTemplate").
			Funcs(template.FuncMap{"offset": func(i int) int { return i + stmtUpdateSavedFilterSQLArgOffset }}).
			Parse(stmtUpdateSavedFilterSQL),
	)

	ds.devicesByCidStore = newDevicesByCidStore(ds, devicesByCidTTLDefault) // tying knot

	ds.permittedSavedFilterColumns = entityPermittedColumns(kt.SavedFilter{})
	ds.permittedPolicyColumns = entityPermittedColumns(kt.AlertPolicy{})

	ds.cachedAppProtoMetrics = newCachedValue()
	ds.cachedAppProtoDims = newCachedValue()
	ds.cachedAppProtoMapping = newCachedValue()

	preconditions.ValidateStruct(ds, preconditions.NoNilPointers)
	return ds, nil
}

func (ds *alertDataSource) Close() {
	database.CloseStatements(ds)
}

func (ds *alertDataSource) GetAlertPoliciesMap(cid *kt.Cid, policyID *kt.PolicyID, lastCheck time.Time) (map[kt.PolicyID]*kt.AlertPolicy, error) {
	policies, err := ds.GetAlertPolicies(cid, &lastCheck, policyID)
	if err != nil {
		return nil, err
	}

	policyMap := make(map[kt.PolicyID]*kt.AlertPolicy)
	for i := range policies {
		policyMap[policies[i].PolicyID] = &policies[i]
	}
	return policyMap, nil
}

func (ds *alertDataSource) GetAlertPoliciesBulk(cidList ...kt.Cid) (map[kt.Cid]map[kt.PolicyID]*kt.AlertPolicy, error) {
	// get all policies the specified cids
	policiesByCid := make(map[kt.Cid][]*kt.AlertPolicy)
	errs := make(map[kt.Cid]error)
	for _, cid := range cidList {
		start := time.Now()
		mt := ds.metrics.Get("GetAlertPolicies", cid)

		policies := []*kt.AlertPolicy{}
		err := ds.stmtLoadAlertPolicies.Select(&policies, cid, nil, nil, kt.ActiveStatus)
		if err != nil {
			mt.Error(start)
			errs[cid] = err
			continue
		} else {
			mt.Success(start)
		}
		policiesByCid[cid] = policies
	}
	return doGetAlertPoliciesBulk(ds.ContextL, policiesByCid)
}

func doGetAlertPoliciesBulk(log logger.ContextL, policiesByCid map[kt.Cid][]*kt.AlertPolicy) (map[kt.Cid]map[kt.PolicyID]*kt.AlertPolicy, error) {

	// map to per cid and policy; validate, fixup and keep a flat list of policy ids in the process
	policiesByCidPid := make(map[kt.Cid]map[kt.PolicyID]*kt.AlertPolicy)
	now := time.Now()
	allPolicyIDs := make([]kt.PolicyID, 0)
	for cid, policies := range policiesByCid {
		policiesByPid := make(map[kt.PolicyID]*kt.AlertPolicy)
		for _, policy := range policies {
			if policy.PolicyWindow < kt.MinimumPolicyWindowSec {
				log.Warnf("Policy(%d) has policy_window=%d, which is less than the minimum of %d. Defaulting to minimum. Frontend should never set a value this low.", policy.PolicyID, policy.PolicyWindow, kt.MinimumPolicyWindowSec)
			}
			err := FixupPolicy(policy, now)
			if err != nil {
				log.Errorf("Skipping policy with invalid JSON (cid=%d, policyid=%d): %v", cid, policy.PolicyID, err)
				continue
			}
			policiesByPid[policy.PolicyID] = policy
			allPolicyIDs = append(allPolicyIDs, policy.PolicyID)
		}
		policiesByCidPid[cid] = policiesByPid
	}

	return policiesByCidPid, nil
}

func (ds *alertDataSource) GetAlertPolicies(cid *kt.Cid, lastCheck *time.Time, policyID *kt.PolicyID) ([]kt.AlertPolicy, error) {
	start := time.Now()
	ctx := context.TODO()

	cid2 := kt.Cid(0)
	if cid != nil {
		cid2 = *cid
	}
	mt := ds.metrics.Get("GetAlertPolicies", cid2)

	dbPolicies := []*kt.AlertPolicy{}
	err := ds.stmtLoadAlertPolicies.SelectContext(ctx, &dbPolicies, cid, lastCheck, policyID, kt.ActiveStatus)
	mt.Done(start, err)
	if err != nil {
		return nil, fmt.Errorf("GetAlertPolicies Select: %v", err)
	}

	policies := make([]kt.AlertPolicy, 0, len(dbPolicies))
	for _, policy := range dbPolicies {
		if policy.PolicyWindow < kt.MinimumPolicyWindowSec {
			ds.Warnf("Policy(%d) has policy_window=%d, which is less than the minimum of %d. Defaulting to minimum. Frontend should never set a value this low.", policy.PolicyID, policy.PolicyWindow, kt.MinimumPolicyWindowSec)
		}

		err = FixupPolicy(policy, start)
		if err != nil {
			ds.Errorf("Skipping policy with invalid JSON (cid=%d, policyid=%d): %v", *cid, policy.PolicyID, err)
			continue
		}
		policies = append(policies, *policy)
	}

	return policies, nil
}

// FixupPolicy sets computed fields on the alert policy by e.g. unmarshalling JSON.
func FixupPolicy(policy *kt.AlertPolicy, now time.Time) error {
	if policy.PolicyWindow < kt.MinimumPolicyWindowSec {
		policy.PolicyWindow = kt.MinimumPolicyWindowSec
	}

	if policy.IsLearn && policy.LearnExpire.Before(now) {
		// FIXME: It doesn't make sense to have both IsLearn and LearnExpire
		// in the DB, especially if they can get out of sync. There is even
		// a chnode job to update these values. Perhaps we delete
		// IsLearn and always check LearnExpire?
		policy.IsLearn = false
	}

	dimensionsJSON := policy.DimensionsJSON
	if dimensionsJSON == "" {
		dimensionsJSON = "[]"
	}
	err := json.Unmarshal([]byte(dimensionsJSON), &policy.UnsortedDimensions)
	if err != nil {
		return err
	}
	policy.SortedDimensions = make([]string, len(policy.UnsortedDimensions))
	copy(policy.SortedDimensions, policy.UnsortedDimensions)
	sort.Strings(policy.SortedDimensions)

	metricJSON := policy.MetricJSON
	if metricJSON == "" {
		metricJSON = "[]"
	}
	err = json.Unmarshal([]byte(metricJSON), &policy.Metric)
	if err != nil {
		return err
	}

	filtersJSON := "{}"
	if policy.FiltersJSON != nil && *policy.FiltersJSON != "" {
		filtersJSON = *policy.FiltersJSON
	}
	err = json.Unmarshal([]byte(filtersJSON), &policy.FilterRoot)
	if err != nil {
		return err
	}

	baselineJSON := policy.BaselineJSON
	if baselineJSON == "" {
		baselineJSON = "{}"
	}
	err = json.Unmarshal([]byte(baselineJSON), &policy.Baseline)
	if err != nil {
		return err
	}

	policy.DimensionGroupingOptions, err = unmarshalDimensionGroupingOptions(policy)
	if err != nil {
		return err
	}

	deviceSelectorJSON := "{}"
	if policy.DeviceSelectorJSON != "" {
		deviceSelectorJSON = policy.DeviceSelectorJSON
	}
	err = json.Unmarshal([]byte(deviceSelectorJSON), &policy.DeviceSelector)
	if err != nil {
		return err
	}

	return nil
}

func unmarshalDimensionGroupingOptions(policy *kt.AlertPolicy) (*kt.DimensionGroupingOptions, error) {
	if policy.DimensionGroupingOptionsJSON == "" || policy.DimensionGroupingOptionsJSON == "{}" {
		return nil, nil
	}

	var dgo kt.DimensionGroupingOptions
	err := json.Unmarshal([]byte(policy.DimensionGroupingOptionsJSON), &dgo)
	if err != nil {
		return nil, err
	}

	if dgo.GroupingDimensions == nil &&
		dgo.NumGroupingDimensions > 0 &&
		len(policy.UnsortedDimensions) >= dgo.NumGroupingDimensions {
		dgo.GroupingDimensions = policy.UnsortedDimensions[0:dgo.NumGroupingDimensions]
	}

	if len(dgo.GroupingDimensions) == 0 {
		return nil, nil
	}

	return &dgo, nil
}

func (ds *alertDataSource) GetAlertPoliciesDone(
	cid kt.Cid,
	lastCheck time.Time,
	policyID kt.PolicyID,
) (deleted []kt.InactiveAlertPolicy, paused []kt.InactiveAlertPolicy, err error) {
	var (
		cidp      *kt.Cid
		policyIDp *kt.PolicyID
	)

	defer func(start time.Time) { ds.metrics.Get("GetAlertPoliciesDone", cid).Done(start, err) }(time.Now())

	if cid > 0 {
		cidp = &cid
	}

	if policyID > 0 {
		policyIDp = &policyID
	}

	rows, err := ds.stmtLoadAlertPoliciesDone.Query(cidp, lastCheck, policyIDp, kt.ActiveStatus, cidp, lastCheck, policyIDp)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var policyID kt.PolicyID
		var edate time.Time
		var status string

		if err = rows.Scan(&policyID, &edate, &status); err != nil {
			ds.Errorf("Error reading alert policy done rows: %v", err)
			continue
		}

		policy := kt.InactiveAlertPolicy{
			PolicyID: policyID,
			Edate:    edate,
		}

		if status == kt.AlertStatusRemoved {
			deleted = append(deleted, policy)
		} else {
			paused = append(paused, policy)
		}
	}

	ds.Debugf("Alerts Done: %s", kt.InactiveAlertPolicies(deleted))
	ds.Debugf("Alerts Paused: %s", kt.InactiveAlertPolicies(paused))

	err = rows.Err()

	return deleted, paused, err
}

// FetchSavedFiltersFlat returns saved filters in old format
// They used to be added in "flat" fashion at root level to overall filter hierarchy
// if chv_alert_policy_saved_filter has no entries for given PolicyId then its nested format
func (ds *alertDataSource) FetchSavedFiltersFlat(cid kt.Cid) (map[kt.PolicyID][]*kt.CompanyFilterBase, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchSavedFiltersFlat", cid).Done(start, err) }(time.Now())

	rows, err := ds.stmtSavedFiltersOld.Query(cid, cid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	filters := make(map[kt.PolicyID][]*kt.CompanyFilterBase)

	for rows.Next() {
		var cf kt.CompanyFilterBase
		var policyID kt.PolicyID
		if err = rows.Scan(&cf.ID, &policyID, &cf.Filter, &cf.IsNot, &cf.Edate); err != nil {
			ds.Errorf("Error reading company filters (cid: %d, err: %v)", cid, err)
			break
		}

		filters[policyID] = append(filters[policyID], &cf)
	}

	err = rows.Err()

	return filters, err
}

// FetchNestedSavedFilters fetches saved filters
func (ds *alertDataSource) FetchSavedFilters(ctx context.Context, cid kt.Cid) (_ map[kt.SavedFilterID]*kt.SavedFilter, err error) {
	defer func(start time.Time) { ds.metrics.Get("FetchSavedFilters", cid).Done(start, err) }(time.Now())

	rows, err := ds.stmtSavedFilters.QueryxContext(ctx, cid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[kt.SavedFilterID]*kt.SavedFilter{}
	for rows.Next() {
		savedFilter := &kt.SavedFilter{}
		if err = rows.StructScan(&savedFilter); err != nil {
			break
		}
		result[savedFilter.ID] = savedFilter
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (ds *alertDataSource) MarkPolicyBad(policyID kt.PolicyID) (err error) {
	defer func(start time.Time) { ds.metrics.Get("MarkPolicyBad", 0).Done(start, err) }(time.Now())

	_, err = ds.stmtPolicyBad.Exec(policyID)

	return
}

func (ds *alertDataSource) GetLocalCompanies(
	gna GetNodeAddress,
	isAllLocal bool,
	cidWhitelist map[kt.Cid]bool,
	cidBlacklist map[kt.Cid]bool,
) (localCids []kt.Cid, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetLocalCompanies", 0).Done(start, err) }(time.Now())

	localIPs, err := GetLocalIPAddresses()
	if err != nil {
		return nil, err
	}

	var allCids []kt.Cid
	err = ds.stmtListCompanies.Select(&allCids)
	if err != nil {
		return nil, err
	}

	// If the cid maps to a local address, and is whitelisted/not-blacklisted,
	// it's a local company.
	for _, cid := range allCids {
		ipAddr, err := gna.GetNodeAddress(kt.IntId(cid))
		if err != nil {
			ds.Errorf("from GetNodeAddress(%d): %v", cid, err)
			continue // e.g. dns error looking up the associated host; unexpected.
		}

		isLocal := isAllLocal || localIPs[ipAddr]
		whitelisted := cidWhitelist == nil || cidWhitelist[cid]
		blacklisted := cidBlacklist != nil && cidBlacklist[cid]
		if isLocal && whitelisted && !blacklisted {
			localCids = append(localCids, cid)
		}
	}

	return localCids, nil
}

// TODO(tjonak): this func/query looks unused, delete
func (ds *alertDataSource) GetOidFromUserId(userId uint64) (uint64, error) {
	var oid uint64
	err := ds.stmtOidFromUserID.QueryRow(userId).Scan(&oid)
	if err != nil {
		return 0, err
	}

	return oid, nil
}

// GetLocalIPAddresses returns set of ip adresses belonging to this host
// in form of formatted strings (i.e "127.0.0.1")
// TODO(tjonak): move to some other package
func GetLocalIPAddresses() (map[string]bool, error) {
	localIPs := map[string]bool{kt.LocalIpAddr: true}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		localIPs[ip.String()] = true
	}

	return localIPs, nil
}

const stmtFindPoliciesSQL = `
SELECT
	ap.id,
	ap.company_id,
	COALESCE(ap.user_id, 0) AS user_id,
	ap.dashboard_id,
	ap.policy_name,
	ap.policy_window,
	ap.is_top,
	ap.is_auto_calc,
	ap.limit_num,
	ap.store_num,
	ap.dimensions,
	ap.metric,
	ap.filters,
	ap.min_traffic,
	ap.learning_mode,
	ap.status,
	COALESCE(ap.baseline, '{}') AS baseline,
	ap.mode,
	ap.cdate,
	ap.edate,
	COALESCE(ap.learning_mode_expire_date, now()-interval'1 day') AS learning_mode_expire_date,
	COALESCE(ap.policy_description, '') AS policy_description,
	COALESCE(ap.primary_dimension, '{}') AS primary_dimension,
	COALESCE(ap.selected_devices, '{}') AS selected_devices
FROM mn_alert_policy ap
LEFT JOIN mn_alert_threshold at ON ap.id = at.policy_id
WHERE ap.company_id = :company_id
  AND (ap.id = ANY(:policy_ids) OR ARRAY_LENGTH(CAST(:policy_ids AS bigint[]), 1) IS NULL)
  AND ap.cdate BETWEEN COALESCE(:create_low, CAST('-infinity' AS timestamp without time zone)) AND COALESCE(:create_high, CAST('infinity' AS timestamp without time zone))
  AND ap.edate BETWEEN COALESCE(:modify_low, CAST('-infinity' AS timestamp without time zone)) AND COALESCE(:modify_high, CAST('infinity' AS timestamp without time zone))
  AND (ap.user_id = ANY(:user_ids) OR ARRAY_LENGTH(CAST(:user_ids AS bigint[]), 1) IS NULL)
  AND (at.id = ANY(:threshold_ids) OR ARRAY_LENGTH(CAST(:threshold_ids AS bigint[]), 1) IS NULL)
  AND (CAST(dimensions AS jsonb) ?& CAST(:dimensions AS text[]) OR ARRAY_LENGTH(CAST(:dimensions AS text[]), 1) IS NULL)
  AND (CAST(COALESCE(selected_devices->'device_synthetic'->>'id', '0') AS bigint) = ANY(:synth_tests_ids) OR ARRAY_LENGTH(CAST(:synth_tests_ids AS bigint[]), 1) IS NULL)
GROUP BY ap.id
ORDER BY ap.id DESC
`

const stmtFindPoliciesSQLWithLimitSQL = stmtFindPoliciesSQL + `
LIMIT :limit
OFFSET :offset
`

const stmtFindPoliciesCountSQL = `
SELECT COUNT(1) FROM (` + stmtFindPoliciesSQL + `) a`

type findPoliciesFilter struct {
	CompanyID      kt.Cid         `db:"company_id"`
	IDs            pq.Int64Array  `db:"policy_ids"`
	CreateTimeLow  *time.Time     `db:"create_low"`
	CreateTimeHigh *time.Time     `db:"create_high"`
	ModifyTimeLow  *time.Time     `db:"modify_low"`
	ModifyTimeHigh *time.Time     `db:"modify_high"`
	Thresholds     pq.Int64Array  `db:"threshold_ids"`
	Users          pq.Int64Array  `db:"user_ids"`
	Dimensions     pq.StringArray `db:"dimensions"`
	SynthTests     pq.Int64Array  `db:"synth_tests_ids"`

	Limit  uint64 `db:"limit"`
	Offset uint64 `db:"offset"`
}

func toInternalFilter(filter *kt.FindPoliciesFilter) *findPoliciesFilter {
	result := &findPoliciesFilter{
		CompanyID:      filter.CompanyID,
		CreateTimeLow:  filter.CreateTimeLow,
		CreateTimeHigh: filter.CreateTimeHigh,
		ModifyTimeLow:  filter.ModifyTimeLow,
		ModifyTimeHigh: filter.ModifyTimeHigh,
		Dimensions:     filter.Dimensions,
		Limit:          filter.Limit,
		Offset:         filter.Offset,
	}

	for _, policyID := range filter.IDs {
		result.IDs = append(result.IDs, int64(policyID))
	}

	for _, thresholdID := range filter.Thresholds {
		result.Thresholds = append(result.Thresholds, int64(thresholdID))
	}

	for _, userID := range filter.Users {
		result.Users = append(result.Users, int64(userID))
	}

	for _, testID := range filter.SynTests {
		result.SynthTests = append(result.SynthTests, int64(testID))
	}
	return result
}

func (ds *alertDataSource) FindPolicies(
	ctx context.Context,
	filter *kt.FindPoliciesFilter,
) (_ map[kt.PolicyID]*kt.AlertPolicy, err error) {
	internalFilter := toInternalFilter(filter)
	return ds.findPolicies(ctx, internalFilter)
}

// FindPolicies queries policies by given set of criteria
func (ds *alertDataSource) findPolicies(
	ctx context.Context,
	filter *findPoliciesFilter,
) (_ map[kt.PolicyID]*kt.AlertPolicy, err error) {
	defer func(start time.Time) { ds.metrics.Get("findPolicies", filter.CompanyID).Done(start, err) }(time.Now())

	policies := []*kt.AlertPolicy{}

	stmt := ds.stmtFindPoliciesWithLimit
	if filter.Limit+filter.Offset == 0 {
		stmt = ds.stmtFindPolicies
	}

	err = stmt.SelectContext(ctx, &policies, filter)
	if err != nil {
		return
	}

	now := time.Now()
	policyIDs := make([]kt.PolicyID, len(policies))
	policyMap := map[kt.PolicyID]*kt.AlertPolicy{}
	for i, policy := range policies {
		policyIDs[i] = policy.PolicyID
		err := FixupPolicy(policy, now)
		if err != nil {
			ds.Errorf("Couldn't fixup policy (pid: %d), skipping: %v", policy.PolicyID, err)
			continue
		}
		policyMap[policy.PolicyID] = policy
	}

	policyToDevices, err := ds.GetSelectedDevicesForCompanyFromStore(ctx, filter.CompanyID)
	if err != nil {
		return nil, err
	}

	for pid, dids := range policyToDevices {
		policy, ok := policyMap[pid]
		if !ok {
			continue
		}
		policy.SelectedDevices = dids
	}
	// TODO: warn on missing devices fields?

	return policyMap, nil
}

// FindPoliciesWithCount works as FindPolicies and also returns count of results before applying limit/offset
func (ds *alertDataSource) FindPoliciesWithCount(
	ctx context.Context,
	filter *kt.FindPoliciesFilter,
) (_ map[kt.PolicyID]*kt.AlertPolicy, _ uint64, err error) {

	internalFilter := toInternalFilter(filter)
	policies, err := ds.findPolicies(ctx, internalFilter)
	if err != nil {
		return nil, 0, err
	}

	defer func(start time.Time) { ds.metrics.Get("FindPoliciesCount", filter.CompanyID).Done(start, err) }(time.Now())

	row := ds.stmtFindPoliciesCount.QueryRowxContext(ctx, internalFilter)
	if row == nil {
		err = ErrNilRow
		return nil, 0, fmt.Errorf("stmtFindPoliciesCount returned nil row")
	}

	if err := row.Err(); err != nil {
		return nil, 0, fmt.Errorf("stmtFindPoliciesCount: %v", err)
	}

	var count uint64
	err = row.Scan(&count)
	if err != nil {
		return nil, 0, fmt.Errorf("Couldn't extract count from stmtFindPoliciesCount response: %v", err)
	}

	return policies, count, nil
}

const stmtDeletePolicySQL = `
DELETE FROM mn_alert_policy WHERE company_id = $1 AND id = $2
`

func (ds *alertDataSource) DeletePolicy(ctx context.Context, companyID kt.Cid, policyID kt.PolicyID) (uint64, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("DeletePolicy", companyID).Done(start, err) }(time.Now())

	result, err := ds.stmtDeletePolicy.ExecContext(ctx, companyID, policyID)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return uint64(rowsAffected), err
}

const stmtGetPolicySQL = `
SELECT
	ap.id,
	ap.company_id,
	COALESCE(ap.user_id, 0) AS user_id,
	ap.dashboard_id,
	ap.policy_name,
	ap.policy_window,
	ap.is_top,
	ap.is_auto_calc,
	ap.limit_num,
	ap.store_num,
	ap.dimensions,
	ap.metric,
	ap.filters,
	ap.min_traffic,
	ap.learning_mode,
	ap.status,
	COALESCE(ap.baseline, '{}') AS baseline,
	ap.mode,
	ap.cdate,
	ap.edate,
	COALESCE(ap.learning_mode_expire_date, now()-interval'1 day') AS learning_mode_expire_date,
	COALESCE(ap.policy_description, '') AS policy_description,
	COALESCE(ap.primary_dimension, '{}') AS primary_dimension,
	COALESCE(ap.selected_devices, '{}') AS selected_devices
FROM mn_alert_policy ap
WHERE ap.company_id = $1 AND ap.id = $2
`

func (ds *alertDataSource) GetPolicy(ctx context.Context, companyID kt.Cid, policyID kt.PolicyID) (*kt.AlertPolicy, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetPolicy", companyID).Done(start, err) }(time.Now())

	result := &kt.AlertPolicy{}

	row := ds.stmtGetPolicy.QueryRowxContext(ctx, companyID, policyID)
	if row == nil { // FIXME: does this ever happen?
		err = ErrNilRow
		return nil, fmt.Errorf("stmtGetPolicy returned nil row")
	}

	if err = row.Err(); err != nil {
		return nil, fmt.Errorf("stmtGetPolicy: %v", err)
	}

	if err = row.StructScan(result); err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, fmt.Errorf("Couldn't extract policy from stmtGetPolicy response: %v", err)
	}

	err = FixupPolicy(result, time.Now())
	if err != nil {
		return nil, fmt.Errorf("Couldn't compute policy fields: %v", err)
	}

	return result, nil
}

func (ds *alertDataSource) NewChwwwTransaction() (kt.ChwwwTransaction, error) {
	tx, err := ds.dbrwx.Beginx()
	return kt.ChwwwTransaction{Tx: tx, ContextL: ds.ContextL}, err
}

const stmtCreatePolicySQL = `
INSERT INTO mn_alert_policy(company_id, user_id, dashboard_id, status, policy_name, policy_description, policy_window, dimensions, metric, filters, baseline, selected_devices, devices, is_top, is_auto_calc, min_traffic, learning_mode_expire_date, limit_num, store_num, primary_dimension)
VALUES (:company_id, :user_id, :dashboard_id, :status, :policy_name, :policy_description, :policy_window, :dimensions, :metric, :filters, :baseline, :selected_devices, '{}', :is_top, :is_auto_calc, :min_traffic, :learning_mode_expire_date, :limit_num, :store_num, :primary_dimension)
RETURNING id, company_id, user_id, dashboard_id, status, policy_name, COALESCE(policy_description, '') AS policy_description, policy_window, dimensions, metric, filters, baseline, COALESCE(selected_devices, '{}') AS selected_devices, is_top, is_auto_calc, min_traffic, learning_mode_expire_date, limit_num, store_num, coalesce(primary_dimension, '{}') as primary_dimension, cdate, edate
`

func (ds *alertDataSource) CreatePolicy(ctx context.Context, policy *kt.AlertPolicy) (*kt.AlertPolicy, error) {
	return ds.createPolicy(ctx, ds.stmtCreatePolicy, policy)
}

func (ds *alertDataSource) CreatePolicyTx(ctx context.Context, tx kt.ChwwwTransaction, policy *kt.AlertPolicy) (*kt.AlertPolicy, error) {
	return ds.createPolicy(ctx, tx.NamedStmtContext(ctx, ds.stmtCreatePolicy), policy)
}

func (ds *alertDataSource) createPolicy(ctx context.Context, stmt *sqlx.NamedStmt, policy *kt.AlertPolicy) (*kt.AlertPolicy, error) {
	if policy == nil {
		return nil, fmt.Errorf("CreatePolicy called with nil policy")
	}

	var err error
	defer func(start time.Time) { ds.metrics.Get("CreatePolicy", policy.CompanyID).Done(start, err) }(time.Now())

	row := stmt.QueryRowxContext(ctx, policy)
	if row == nil {
		err = ErrNilRow
		return nil, fmt.Errorf("Couldn't retrieve policy create response from db")
	}

	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("stmtCreatePolicy execution failed: %v", err)
	}

	result := &kt.AlertPolicy{}
	err = row.StructScan(result)
	if err != nil {
		return nil, fmt.Errorf("stmtCreatePolicy scan failed: %v", err)
	}

	// TODO(tjonak): remove policy if this thing fails
	_ = FixupPolicy(result, time.Now())

	return result, nil
}

func (ds *alertDataSource) EnablePolicy(ctx context.Context, companyID kt.Cid, policyID kt.PolicyID) error {
	return ds.modifyPolicyStatus(ctx, companyID, policyID, kt.PolicyStatusActive)
}

func (ds *alertDataSource) DisablePolicy(ctx context.Context, companyID kt.Cid, policyID kt.PolicyID) error {
	return ds.modifyPolicyStatus(ctx, companyID, policyID, kt.PolicyStatusDisabled)
}

const stmtModifyPolicyStatusSQL = `
UPDATE mn_alert_policy
SET status = $3
WHERE company_id = $1 AND id = $2`

func (ds *alertDataSource) modifyPolicyStatus(
	ctx context.Context,
	companyID kt.Cid,
	policyID kt.PolicyID,
	status kt.PolicyStatus,
) (err error) {
	defer func(start time.Time) { ds.metrics.Get("modifyPolicyStatus", companyID).Done(start, err) }(time.Now())

	_, err = ds.stmtModifyPolicyStatus.ExecContext(ctx, companyID, policyID, status)
	return
}

const stmtMutePolicySQL = `
UPDATE mn_alert_policy
SET learning_mode=true, learning_mode_expire_date=$3
WHERE company_id = $1 AND id = $2`

func (ds *alertDataSource) MutePolicy(ctx context.Context, companyID kt.Cid, policyID kt.PolicyID, end time.Time) (err error) {
	defer func(start time.Time) { ds.metrics.Get("MutePolicy", companyID).Done(start, err) }(time.Now())

	_, err = ds.stmtMutePolicy.ExecContext(ctx, companyID, policyID, end)
	return
}

const stmtUnmutePolicySQL = `
UPDATE mn_alert_policy
SET learning_mode = false
WHERE company_id = $1 AND id = $2`

func (ds *alertDataSource) UnmutePolicy(ctx context.Context, companyID kt.Cid, policyID kt.PolicyID) (err error) {
	defer func(start time.Time) { ds.metrics.Get("MutePolicy", companyID).Done(start, err) }(time.Now())

	_, err = ds.stmtUnmutePolicy.ExecContext(ctx, companyID, policyID)
	return
}

const stmtLoadAppProtocolMappingsSQL = `SELECT id, app_protocol FROM mn_lookup_app_protocol WHERE status = 'A'`

func (ds *alertDataSource) LoadAppProtocolMappings(ctx context.Context) (map[string]uint64, error) {
	var result map[string]uint64
	var err error

	if data, valid := ds.cachedAppProtoMapping.Get(); valid {
		return data.(map[string]uint64), nil
	}

	defer func(start time.Time) { ds.metrics.Get("LoadAppProtocolMappings", 0).Done(start, err) }(time.Now())
	rows, err := ds.stmtLoadAppProtocolMappings.QueryContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("Error querying stmtLoadAppProtocolMappings: %v", err)
	}
	defer rows.Close()

	result = map[string]uint64{}
	for rows.Next() {
		var protocol string
		var id uint64
		err = rows.Scan(&id, &protocol)
		if err != nil {
			break
		}

		result[strings.ToLower(protocol)] = id
	}

	err = rows.Err()

	// Cache if no error.
	if err == nil {
		ds.Infof("Caching AppProtoMapping")
		ds.cachedAppProtoMapping.Set(result, cachedAppProtoTTL)
	}

	return result, err
}

const stmtLoadAppProtocolMetricsSQL = `
SELECT
	ap.app_protocol,
	ap.display_name,
	apc.custom_column,
	apc.metadata->>'metric_type' AS metric_type,
	apc.dimension_label
FROM mn_lookup_app_protocol_cols apc
	JOIN mn_lookup_app_protocol ap ON apc.app_protocol_id = ap.id
WHERE ap.status = 'A' AND apc.status = 'A' AND apc.metadata->>'is_metric' = 'true';
`

func (ds *alertDataSource) LoadAppProtocolMetrics(ctx context.Context) ([]appproto.Metric, error) {
	var metrics []appproto.Metric
	var err error

	if data, valid := ds.cachedAppProtoMetrics.Get(); valid {
		return data.([]appproto.Metric), nil
	}

	defer func(start time.Time) { ds.metrics.Get("LoadAppProtocolMetrics", 0).Done(start, err) }(time.Now())
	err = ds.stmtLoadAppProtocolMetrics.SelectContext(ctx, &metrics)
	if err == nil {
		ds.Infof("Caching AppProtoMetrics")
		ds.cachedAppProtoMetrics.Set(metrics, cachedAppProtoTTL)
	}

	return metrics, err
}

const stmtLoadAppProtocolDimensionsSQL = `
SELECT
	ap.app_protocol,
	apc.custom_column
FROM mn_lookup_app_protocol_cols apc
	JOIN mn_lookup_app_protocol ap ON apc.app_protocol_id = ap.id
WHERE ap.status = 'A'
  AND apc.status = 'A'
  AND NOT COALESCE(apc.metadata->>'is_metric', 'false')::bool
`

func (ds *alertDataSource) LoadAppProtocolDimensions(ctx context.Context) ([]appproto.Tuple, error) {
	var dimensions []appproto.Tuple
	var err error

	if data, valid := ds.cachedAppProtoDims.Get(); valid {
		return data.([]appproto.Tuple), nil
	}

	defer func(start time.Time) { ds.metrics.Get("LoadAppProtocolDimensions", 0).Done(start, err) }(time.Now())
	err = ds.stmtLoadAppProtocolDimensions.SelectContext(ctx, &dimensions)
	if err == nil {
		ds.Infof("Caching AppProtoDims")
		ds.cachedAppProtoDims.Set(dimensions, cachedAppProtoTTL)
	}

	return dimensions, err
}

const stmtLoadNonSTDAppProtocolsSQL = `SELECT id FROM mn_lookup_app_protocol WHERE NOT add_to_standard_fields`

func (ds *alertDataSource) LoadNonSTDAppProtocols(ctx context.Context) (protoIDs []kt.AppProtoID, err error) {
	defer func(start time.Time) { ds.metrics.Get("LoadNonSTDAppProtocols", 0).Done(start, err) }(time.Now())

	err = ds.stmtLoadNonSTDAppProtocols.SelectContext(ctx, &protoIDs)

	return
}

const stmtLoadAppProtocolSNMPBundleSQL = `
SELECT
	json_object(array_agg(replace(a.snmp_role::text, '"', '')), array_agg(a.custom_column)) AS snmp_bundle
FROM
(
	SELECT apc.custom_column, apc.metadata->'snmp' AS snmp_role
	FROM mn_lookup_app_protocol_cols apc
	JOIN mn_lookup_app_protocol ap ON apc.app_protocol_id = ap.id AND lower(ap.app_protocol)='snmp'
	WHERE (apc.metadata->>'snmp') IS NOT NULL AND (apc.metadata->>'snmp') != 'metric'
) a
`

func (ds *alertDataSource) LoadAppProtocolSNMPBundle(ctx context.Context) (bundle *kt.SNMPBundle, err error) {
	defer func(start time.Time) { ds.metrics.Get("LoadAppProtocolSNMPBundle", 0).Done(start, err) }(time.Now())

	result := []byte{}
	ds.stmtLoadAppProtocolSNMPBundle.GetContext(ctx, &result)

	bundle = &kt.SNMPBundle{}
	err = json.Unmarshal(result, bundle)

	return
}

const fetchUserIDAndHydraForCompanySQL = `
SELECT id, coalesce(user_hydra_connection, '') AS user_hydra_connection
FROM mn_user
WHERE company_id = $1 AND user_status = 'V'
LIMIT 1
`

func (ds *alertDataSource) FetchUserIDAndHydraForCompany(ctx context.Context, cid kt.Cid) (uid kt.UserID, hydra string, err error) {
	defer func(start time.Time) { ds.metrics.Get("FetchUserIDAndHydraForCompany", 0).Done(start, err) }(time.Now())

	result := struct {
		ID                  int64  `db:"id"`
		UserHydraConnection string `db:"user_hydra_connection"`
	}{}
	err = ds.stmtFetchUserIDAndHydraForCompany.GetContext(ctx, &result, cid)
	if err != nil {
		return 0, "", err
	}
	return kt.UserID(result.ID), result.UserHydraConnection, nil
}

const stmtUpdatePolicySQLArgOffset = 4
const stmtUpdatePolicySQLTemplate = `
UPDATE mn_alert_policy
   SET user_id = $3,
       edate = now() {{- range $id, $col := . -}},
       {{$col}} = ${{offset $id}}
{{- end}}
WHERE company_id = $1 AND id = $2
RETURNING id,company_id,user_id,dashboard_id,status,policy_name,COALESCE(policy_description, '') AS policy_description,policy_window,dimensions,metric,filters,baseline,COALESCE(selected_devices, '{}') AS selected_devices,cdate,edate,is_top,is_auto_calc,min_traffic,learning_mode_expire_date,limit_num,store_num,coalesce(primary_dimension, '{}') AS primary_dimension
`

func (ds *alertDataSource) UpdatePolicy(
	ctx context.Context,
	cid kt.Cid,
	pid kt.PolicyID,
	uid kt.UserID,
	pub kt.PolicyUpdateBundle,
) (resultPolicy *kt.AlertPolicy, err error) {
	executor := func(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
		return ds.dbrwx.QueryRowxContext(ctx, query, args...)
	}
	return ds.updatePolicy(ctx, executor, cid, pid, uid, pub)
}

func (ds *alertDataSource) UpdatePolicyTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	cid kt.Cid,
	pid kt.PolicyID,
	uid kt.UserID,
	pub kt.PolicyUpdateBundle,
) (resultPolicy *kt.AlertPolicy, err error) {
	executor := func(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
		return tx.QueryRowxContext(ctx, query, args...)
	}
	return ds.updatePolicy(ctx, executor, cid, pid, uid, pub)
}

func (ds *alertDataSource) updatePolicy(
	ctx context.Context,
	executor func(ctx context.Context, query string, args ...interface{}) *sqlx.Row,
	cid kt.Cid,
	pid kt.PolicyID,
	uid kt.UserID,
	pub kt.PolicyUpdateBundle,
) (resultPolicy *kt.AlertPolicy, err error) {
	defer func(start time.Time) { ds.metrics.Get("UpdatePolicy", cid).Done(start, err) }(time.Now())

	errWithMessage := func(err error) (*kt.AlertPolicy, error) { return nil, errors.WithMessage(err, "UpdatePolicy") }

	cols, args := []kt.SQLColumn{}, []interface{}{cid, pid, uid}

	for _, tpl := range pub {
		_, ok := ds.permittedPolicyColumns[tpl.Column]
		if !ok {
			return errWithMessage(fmt.Errorf("unsupported column: %s %v", tpl.Column, tpl.Arg))
		}

		cols = append(cols, tpl.Column)
		args = append(args, tpl.Arg)
	}

	stmt, err := render(ds.tmplUpdatePolicy, cols)
	if err != nil {
		return errWithMessage(err)
	}

	ds.Debugf("UpdatePolicy Stmt: %s", stmt)

	row := executor(ctx, stmt, args...)

	resultPolicy = &kt.AlertPolicy{}
	if err = row.StructScan(resultPolicy); err != nil {
		return errWithMessage(err)
	}

	FixupPolicy(resultPolicy, time.Now())

	return
}

const stmtAllPoliciesExistSQL = `
SELECT array_agg(id) @> $2 FROM mn_alert_policy WHERE company_id = $1 AND id = ANY($2)`

func (ds *alertDataSource) AllPoliciesExist(ctx context.Context, companyID kt.Cid, policyIDs []kt.PolicyID) (ok bool, err error) {
	if len(policyIDs) == 0 {
		return true, nil
	}

	defer func(start time.Time) { ds.metrics.Get("AllPoliciesExist", companyID).Done(start, err) }(time.Now())

	row := ds.stmtAllPoliciesExist.QueryRowxContext(ctx, companyID, pq.Array(policyIDs))
	if err = row.Err(); err != nil {
		return
	}

	err = row.Scan(&ok)

	return
}

const stmtSelectSavedFilterSQL = `
SELECT
	id,
	filter_name,
	COALESCE(filter_description, '') AS filter_description,
	COALESCE(filters,'{}') AS filters,
	cdate,
	edate
`

const stmtGetSavedFilterSQL = stmtSelectSavedFilterSQL + `
FROM mn_saved_filter
WHERE (company_id = $1 OR filter_level='global') AND id = $2
`

func (ds *alertDataSource) GetSavedFilter(ctx context.Context, companyID kt.Cid, filterID kt.SavedFilterID) (filter *kt.SavedFilter, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetSavedFilter", companyID).Done(start, err) }(time.Now())

	filter = &kt.SavedFilter{}
	err = ds.stmtGetSavedFilter.GetContext(ctx, filter, companyID, filterID)
	if err != nil {
		err = fmt.Errorf("from GetSavedFilter: %w", err)
	}

	return
}

const stmtDeleteSavedFilterSQL = `
DELETE FROM mn_saved_filter WHERE company_id = $1 AND id = $2`

func (ds *alertDataSource) DeleteSavedFilter(ctx context.Context, companyID kt.Cid, filterID kt.SavedFilterID) (err error) {
	defer func(start time.Time) { ds.metrics.Get("DeleteSavedFilter", companyID).Done(start, err) }(time.Now())
	defer func() {
		if err != nil {
			err = fmt.Errorf("from DeleteSavedFilter: %w", err)
		}
	}()

	row, err := ds.stmtDeleteSavedFilter.ExecContext(ctx, companyID, filterID)
	if err != nil {
		return
	}

	affected, err := row.RowsAffected()
	if err != nil {
		return
	}

	if affected < 1 {
		err = sql.ErrNoRows
		return
	}

	return
}

const stmtSavedFilterReturnDefault = `RETURNING id, filter_name, filter_description, filters, cdate, edate`

const stmtCreateSavedFilterSQLArgOffset = 2
const stmtCreateSavedFilterSQL = `
INSERT INTO mn_saved_filter(company_id, filter_level {{- range $_, $col := . -}}, {{$col}} {{- end -}} )
VALUES ($1, 'COMPANY' {{- range $id, $_ := . -}}, ${{offset $id}} {{- end -}})
` + stmtSavedFilterReturnDefault

func (ds *alertDataSource) CreateSavedFilter(
	ctx context.Context,
	companyID kt.Cid,
	bundle kt.SavedFilterUpdateBundle,
) (sf *kt.SavedFilter, err error) {
	defer func(start time.Time) { ds.metrics.Get("CreateSavedFilter", companyID).Done(start, err) }(time.Now())

	errWithMessage := func(err error) (*kt.SavedFilter, error) { return nil, fmt.Errorf("CreateSavedFilter: %w", err) }

	cols, args := []kt.SQLColumn{}, []interface{}{companyID}
	for _, tpl := range bundle {
		_, ok := ds.permittedSavedFilterColumns[tpl.Column]
		if !ok {
			return errWithMessage(fmt.Errorf("unsupported column: %s %v", tpl.Column, tpl.Arg))
		}

		cols = append(cols, tpl.Column)
		args = append(args, tpl.Arg)
	}

	stmt, err := render(ds.tmplCreateSavedFilter, cols)
	if err != nil {
		return errWithMessage(err)
	}

	ds.Debugf("CreateSavedFilter stmt: %s", stmt)

	sf = &kt.SavedFilter{}
	if err = ds.dbrwx.GetContext(ctx, sf, stmt, args...); err != nil {
		return errWithMessage(err)
	}

	return
}

const stmtUpdateSavedFilterSQLArgOffset = 3
const stmtUpdateSavedFilterSQL = `
UPDATE mn_saved_filter
   SET edate = now() {{- range $id, $col := . -}},
       {{$col}} = ${{offset $id}}
{{- end}}
WHERE company_id = $1 AND id = $2
` + stmtSavedFilterReturnDefault

func (ds *alertDataSource) UpdateSavedFilter(
	ctx context.Context,
	companyID kt.Cid,
	savedFilterID kt.SavedFilterID,
	bundle kt.SavedFilterUpdateBundle,
) (sf *kt.SavedFilter, err error) {
	defer func(start time.Time) { ds.metrics.Get("UpdateSavedFilter", companyID).Done(start, err) }(time.Now())

	errWithMessage := func(err error) (*kt.SavedFilter, error) { return nil, fmt.Errorf("UpdateSavedFilter: %w", err) }

	cols, args := []kt.SQLColumn{}, []interface{}{companyID, savedFilterID}
	for _, tpl := range bundle {
		_, ok := ds.permittedSavedFilterColumns[tpl.Column]
		if !ok {
			return errWithMessage(fmt.Errorf("unsupported column: %s %v", tpl.Column, tpl.Arg))
		}

		cols = append(cols, tpl.Column)
		args = append(args, tpl.Arg)
	}

	stmt, err := render(ds.tmplUpdateSavedFilter, cols)
	if err != nil {
		return errWithMessage(err)
	}

	ds.Debugf("UpdateSavedFilter stmt: %s", stmt)

	sf = &kt.SavedFilter{}
	if err = ds.dbrwx.GetContext(ctx, sf, stmt, args...); err != nil {
		return errWithMessage(err)
	}

	return
}

const stmtFindSavedFiltersSQL = stmtSelectSavedFilterSQL + stmtFindSavedFiltersBaselSQL + `
LIMIT $9
OFFSET $10`

const stmtFindSavedFiltersBaselSQL = `
FROM mn_saved_filter
WHERE (company_id = $1 OR ($8 AND filter_level='global'))
  AND (id = ANY($2) OR ARRAY_LENGTH(CAST($2 AS bigint[]), 1) IS NULL)
  AND (filter_name = ANY($3) OR ARRAY_LENGTH(CAST($3 AS text[]), 1) IS NULL)
  AND cdate BETWEEN COALESCE($4, CAST('-infinity' AS timestamp without time zone)) AND COALESCE($5, CAST('infinity' AS timestamp without time zone))
  AND edate BETWEEN COALESCE($6, CAST('-infinity' AS timestamp without time zone)) AND COALESCE($7, CAST('infinity' AS timestamp without time zone))
`

const stmtFindSavedFiltersCountSQL = `SELECT count(1)` + stmtFindSavedFiltersBaselSQL

func (ds *alertDataSource) FindSavedFiltersWithCount(
	ctx context.Context,
	companyID kt.Cid,
	criteria *kt.FindSavedFiltersCriteria,
) (filters []*kt.SavedFilter, count uint64, err error) {
	if criteria == nil {
		return
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("FindSavedFiltersWithCount: %w", err)
		}
	}()

	args := []interface{}{
		companyID,
		pq.Array(criteria.IDs),
		pq.Array(criteria.Names),
		boxTime(criteria.CreateStart),
		boxTime(criteria.CreateEnd),
		boxTime(criteria.ModifyStart),
		boxTime(criteria.ModifyEnd),
		criteria.IncludePredefined,
	}

	{
		defer func(start time.Time) {
			ds.metrics.Get("stmtFindSavedFiltersCount", companyID).Done(start, err)
		}(time.Now())

		if err = ds.stmtFindSavedFiltersCount.GetContext(ctx, &count, args...); err != nil {
			return
		}

		if count == 0 {
			return
		}
	}

	args = append(args, criteria.Limit, criteria.Offset)

	defer func(start time.Time) { ds.metrics.Get("stmtFindSavedFilters", companyID).Done(start, err) }(time.Now())

	err = ds.stmtFindSavedFilters.SelectContext(ctx, &filters, args...)

	return
}
