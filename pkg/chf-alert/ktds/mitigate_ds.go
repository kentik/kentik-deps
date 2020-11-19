package ktds

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/kentik/chf-alert/pkg/alert/util"
	"github.com/kentik/chf-alert/pkg/eggs/pipeline"
	"github.com/kentik/chf-alert/pkg/eggs/state"
	"github.com/kentik/chf-alert/pkg/flowspec"
	"github.com/kentik/chf-alert/pkg/kt"
	"github.com/kentik/chf-alert/pkg/metric"
	"github.com/kentik/chf-alert/pkg/services/mitigate/machinev2"
	"github.com/kentik/chf-alert/pkg/services/mitigate/machinev3"

	"github.com/kentik/eggs/pkg/database"
	"github.com/kentik/eggs/pkg/logger"
	"github.com/kentik/eggs/pkg/preconditions"

	"github.com/jmoiron/sqlx"
	_ "github.com/kentik/go-mysql-fork/mysql" // Load mysql driver
	"github.com/lib/pq"                       // Load postgres driver
)

type mitigateDataSource struct {
	logger.ContextL
	metrics metric.DBStore

	testingMode bool

	stmtGetThresholdMitigationsForThreshold *sqlx.Stmt `validate:"not_nil"`
	stmtLoadThresholdMitigationBulk         *sqlx.Stmt `validate:"not_nil"`
	stmtGetPlatformMethodPairsBulk          *sqlx.Stmt `validate:"not_nil"`
	stmtGetForDeviceBGP                     *sqlx.Stmt `validate:"not_nil"`

	tmplCreateThresholdMitigationsBulk *template.Template `validate:"not_nil"`
	stmtGetThresholdMitigations        *sqlx.Stmt         `validate:"not_nil"`

	stmtGetCurrentRTBHCalls           *sqlx.NamedStmt `validate:"not_nil"`
	stmtMarkRTBHAnnounceForDevice     *sqlx.NamedStmt `validate:"not_nil"`
	stmtMarkRTBHWithdrawForDevice     *sqlx.NamedStmt `validate:"not_nil"`
	stmtGetCurrentFlowspecCalls       *sqlx.NamedStmt `validate:"not_nil"`
	stmtMarkFlowspecAnnounceForDevice *sqlx.NamedStmt `validate:"not_nil"`
	stmtMarkFlowspecWithdrawForDevice *sqlx.NamedStmt `validate:"not_nil"`
	stmtLoadStateMachines             *sqlx.NamedStmt `validate:"not_nil"`
	stmtSaveStateMachinesLog          *sqlx.NamedStmt `validate:"not_nil"`
	stmtSaveStateMachinesCurrent      *sqlx.NamedStmt `validate:"not_nil"`
	stmtDeleteStateMachinesCurrent    *sqlx.NamedStmt `validate:"not_nil"`
	stmtLoadNextMitigationID          *sqlx.NamedStmt `validate:"not_nil"`
	stmtSaveNextMitigationID          *sqlx.NamedStmt `validate:"not_nil"`

	stmtGetMitigation              *sqlx.Stmt   `validate:"not_nil"`
	stmtGetMitigationBulk          *inStmt      `validate:"not_nil"`
	stmtFindMitigationsCount       *namedInStmt `validate:"not_nil"`
	stmtFindMitigationsLimitOffset *namedInStmt `validate:"not_nil"`

	stmtGetAllMitigationPlatforms    *sqlx.Stmt         `validate:"not_nil"`
	stmtGetMitigationPlatform        *sqlx.Stmt         `validate:"not_nil"`
	stmtGetMitigationPlatforms       *sqlx.Stmt         `validate:"not_nil"`
	stmtDeleteMitigationPlatform     *sqlx.Stmt         `validate:"not_nil"`
	tmplCreateMitigationPlatform     *template.Template `validate:"not_nil"`
	tmplUpdateMitigationPlatform     *template.Template `validate:"not_nil"`
	stmtFindMitigationPlatforms      *sqlx.Stmt         `validate:"not_nil"`
	stmtFindMitigationPlatformsCount *sqlx.Stmt         `validate:"not_nil"`

	tmplAssociateMethodsWithPlatform *template.Template `validate:"not_nil"`
	stmtGetAssociatedMethods         *sqlx.Stmt         `validate:"not_nil"`
	stmtValidatePlatformMethodPair   *sqlx.Stmt         `validate:"not_nil"`

	stmtGetAllMitigationMethods    *sqlx.Stmt         `validate:"not_nil"`
	stmtGetMitigationMethods       *sqlx.Stmt         `validate:"not_nil"`
	stmtGetMitigationMethod        *sqlx.Stmt         `validate:"not_nil"`
	stmtDeleteMitigationMethod     *sqlx.Stmt         `validate:"not_nil"`
	tmplCreateMitigationMethod     *template.Template `validate:"not_nil"`
	tmplUpdateMitigationMethod     *template.Template `validate:"not_nil"`
	stmtFindMitigationMethods      *sqlx.Stmt         `validate:"not_nil"`
	stmtFindMitigationMethodsCount *sqlx.Stmt         `validate:"not_nil"`

	stmtGetNotificationChannels                 *sqlx.Stmt         `validate:"not_nil"`
	stmtGetNotificationChannelsBulk             *sqlx.Stmt         `validate:"not_nil"`
	stmtValidateNotificationChannels            *sqlx.Stmt         `validate:"not_nil"`
	tmplAssociateNotificationChannelsWithMethod *template.Template `validate:"not_nil"`
	stmtDeleteNotifChannelsToMethodAssociation  *sqlx.Stmt         `validate:"not_nil"`

	chwww   *RoRwPairDBPair
	chalert *RoRwPairDBPair

	permittedPlatformColumns map[kt.SQLColumn]struct{}
	permittedMethodColumns   map[kt.SQLColumn]struct{}
}

// NewMitigateDataSource constructs alert data source instance
func NewMitigateDataSource(chwww *RoRwPairDBPair, chalert *RoRwPairDBPair, log logger.Underlying, testingMode bool) (MitigateDataSource, error) {

	var ds = &mitigateDataSource{
		ContextL:    logger.NewContextLFromUnderlying(logger.SContext{S: "mitigateDS"}, log),
		testingMode: testingMode,
		metrics:     metric.NewDB("MitigateDS"),
		chwww:       chwww,
		chalert:     chalert,
	}

	// chwww
	pgStmts := []struct {
		dbx  *sqlx.DB
		stmt **sqlx.Stmt
		s    string
	}{
		{chwww.Rox, &ds.stmtGetThresholdMitigationsForThreshold, stmtGetThresholdMitigationsForThresholdSQL},
		{chwww.Rox, &ds.stmtLoadThresholdMitigationBulk, stmtLoadThresholdMitigationBulkSQL},
		{chwww.Rox, &ds.stmtGetPlatformMethodPairsBulk, stmtGetPlatformMethodPairsBulkSQL},
		{chwww.Rox, &ds.stmtGetAllMitigationMethods, stmtGetAllMitigationMethodsSQL},
		{chwww.Rox, &ds.stmtGetMitigationMethod, stmtGetMitigationMethodSQL},
		{chwww.Rox, &ds.stmtGetMitigationMethods, stmtGetMitigationMethodsSQL},
		{chwww.Rwx, &ds.stmtDeleteMitigationMethod, stmtDeleteMitigationMethodSQL},
		{chwww.Rox, &ds.stmtGetForDeviceBGP, `
	SELECT id, device_proxy_bgp, device_proxy_bgp6
	FROM mn_device
	WHERE device_status = 'V'
		AND device_bgp_type = 'device'
		AND company_id = $1`},
		{chwww.Rox, &ds.stmtGetMitigationPlatforms, stmtGetMitigationPlatformsSQL},
		{chwww.Rox, &ds.stmtGetAllMitigationPlatforms, stmtGetAllMitigationPlatformsSQL},
		{chwww.Rox, &ds.stmtGetMitigationPlatform, stmtGetMitigationPlatformSQL},
		{chwww.Rwx, &ds.stmtDeleteMitigationPlatform, stmtDeleteMitigationPlatformSQL},
		{chwww.Rox, &ds.stmtGetThresholdMitigations, stmtGetThresholdMitigationsSQL},
		{chwww.Rox, &ds.stmtGetNotificationChannels, stmtGetNotificationChannelsSQL},
		{chwww.Rox, &ds.stmtGetNotificationChannelsBulk, stmtGetNotificationChannelsBulkSQL},
		{chwww.Rox, &ds.stmtValidateNotificationChannels, stmtValidateNotificationChannelsSQL},
		{chwww.Rwx, &ds.stmtDeleteNotifChannelsToMethodAssociation, stmtDeleteNotifChannelsToMethodAssociationSQL},
		{chwww.Rox, &ds.stmtFindMitigationPlatforms, stmtFindMitigationPlatformsSQL},
		{chwww.Rox, &ds.stmtFindMitigationPlatformsCount, stmtFindMitigationPlatformsCountSQL},
		{chwww.Rox, &ds.stmtFindMitigationMethods, stmtFindMitigationMethodsSQL},
		{chwww.Rox, &ds.stmtFindMitigationMethodsCount, stmtFindMitigationMethodsCountSQL},
		{chwww.Rox, &ds.stmtGetAssociatedMethods, stmtGetAssociatedMethodsSQL},
		{chwww.Rox, &ds.stmtValidatePlatformMethodPair, stmtValidatePlatformMethodPairSQL},
	}
	for _, s := range pgStmts {
		stmt, err := s.dbx.Preparex(s.s)
		*s.stmt = stmt
		if err != nil {
			return nil, fmt.Errorf("while preparing \"%s\": %s", s.s, err)
		}
	}

	pgStmtsNamed := []struct {
		dbx  *sqlx.DB
		stmt **sqlx.NamedStmt
		s    string
	}{
		{chwww.Rox, &ds.stmtGetCurrentRTBHCalls, `
	SELECT company_id, device_id, alert_cidr, args, ctime, platform_id, mitigation_id
	FROM mn_current_rtbh_call
	WHERE company_id = :company_id`},
		{chwww.Rwx, &ds.stmtMarkRTBHAnnounceForDevice, `
	INSERT INTO mn_current_rtbh_call (company_id, device_id, alert_cidr, args, platform_id, mitigation_id)
	VALUES (:company_id, :device_id, (:alert_cidr)::::INET, :args, :platform_id, :mitigation_id)`},
		{chwww.Rwx, &ds.stmtMarkRTBHWithdrawForDevice, `
	DELETE FROM mn_current_rtbh_call
	WHERE company_id = :company_id
		AND device_id = :device_id
		AND alert_cidr = (:alert_cidr)::::INET
		AND platform_id = :platform_id
		AND mitigation_id = :mitigation_id`},
		{chwww.Rox, &ds.stmtGetCurrentFlowspecCalls, `
	SELECT company_id, device_id, alert_cidr, args, ctime, platform_id, mitigation_id
	FROM mn_current_flowspec_call
	WHERE company_id = :company_id`},
		{chwww.Rwx, &ds.stmtMarkFlowspecAnnounceForDevice, `
	INSERT INTO mn_current_flowspec_call (company_id, device_id, alert_cidr, args, platform_id, mitigation_id)
	VALUES (:company_id, :device_id, (:alert_cidr)::::INET, :args, :platform_id, :mitigation_id)`},
		{chwww.Rwx, &ds.stmtMarkFlowspecWithdrawForDevice, `
	DELETE FROM mn_current_flowspec_call
	WHERE company_id = :company_id
		AND device_id = :device_id
		AND alert_cidr = (:alert_cidr)::::INET
		AND platform_id = :platform_id
		AND mitigation_id = :mitigation_id
		AND args = :args`},
	}
	for _, s := range pgStmtsNamed {
		stmt, err := s.dbx.PrepareNamed(s.s)
		*s.stmt = stmt
		if err != nil {
			return nil, fmt.Errorf("while preparing \"%s\": %s", s.s, err)
		}
	}

	// chalert
	mysqlStmt := []struct {
		dbx  *sqlx.DB
		stmt **sqlx.Stmt
		s    string
	}{
		{chalert.Rox, &ds.stmtGetMitigation, stmtGetMitigationSQL},
	}
	for _, s := range mysqlStmt {
		stmt, err := s.dbx.Preparex(s.s)
		if err != nil {
			return nil, err
		}
		*s.stmt = stmt
	}

	mysqlStmtsNamed := []struct {
		dbx  *sqlx.DB
		stmt **sqlx.NamedStmt
		s    string
	}{
		{chalert.Rox, &ds.stmtLoadStateMachines, `
	SELECT * FROM mn_mit2_machines WHERE company_id = :company_id`},
		{chalert.Rwx, &ds.stmtSaveStateMachinesCurrent, `
	INSERT INTO mn_mit2_machines (company_id, machine_class, machine_id, state_name, params, state_object, metadata)
	VALUES (:company_id, :machine_class, :machine_id, :state_name, :params, :state_object, :metadata)
	ON DUPLICATE KEY UPDATE state_name = :state_name,
		params = :params,
		state_object = :state_object,
		metadata = :metadata`},
		{chalert.Rwx, &ds.stmtSaveStateMachinesLog, `
	INSERT IGNORE INTO mn_mit2_machines_log (company_id, machine_class, machine_id, states_leaving, states_entering, from_state_name, to_state_name, event_name, params, state_object, metadata)
	VALUES (:company_id, :machine_class, :machine_id, :states_leaving, :states_entering, :from_state_name, :to_state_name, :event_name, :params, :state_object, :metadata)`},
		{chalert.Rwx, &ds.stmtDeleteStateMachinesCurrent, `
	DELETE FROM mn_mit2_machines
	WHERE company_id = :company_id
		AND machine_class = :machine_class
		AND machine_id = :machine_id`},
		{chalert.Rwx, &ds.stmtSaveNextMitigationID, `
	INSERT INTO mn_mit2_mitigation_id (company_id, mitigation_id)
	VALUES (:company_id, :mitigation_id)
	ON DUPLICATE KEY UPDATE mitigation_id = :mitigation_id`},
		{chalert.Rox, &ds.stmtLoadNextMitigationID, `
	SELECT mitigation_id FROM mn_mit2_mitigation_id WHERE company_id = :company_id`},
	}
	for _, s := range mysqlStmtsNamed {
		stmt, err := s.dbx.PrepareNamed(s.s)
		*s.stmt = stmt
		if err != nil {
			return nil, err
		}
	}

	ds.stmtGetMitigationBulk = newInStmt(chalert.Rox, stmtGetMitigationBulkSQL)

	namedInStmts := []struct {
		dbx   *sqlx.DB
		stmt  **namedInStmt
		query string
	}{
		{chalert.Rox, &ds.stmtFindMitigationsCount, stmtFindMitigationsCountSQL},
		{chalert.Rox, &ds.stmtFindMitigationsLimitOffset, stmtFindMitigationsLimitOffsetSQL},
	}
	for _, s := range namedInStmts {
		*s.stmt = newNamedInStmt(s.dbx, s.query)
	}

	ds.tmplCreateThresholdMitigationsBulk = template.Must(
		template.New("").Parse(tmplCreateThresholdMitigationsBulkTemplate))

	ds.permittedPlatformColumns = entityPermittedColumns(kt.MitigationPlatform{})
	ds.permittedMethodColumns = entityPermittedColumns(kt.MitigationMethod{})

	ds.tmplCreateMitigationPlatform = template.Must(
		template.New("tmplCreateMitigationPlatform").
			Funcs(template.FuncMap{"offset": func(i int) int { return i + stmtCreateMitigationPlatformSQLArgOffset }}).
			Parse(stmtCreateMitigationPlatformSQLTemplate),
	)

	ds.tmplUpdateMitigationPlatform = template.Must(
		template.New("tmplUpdateMitigationPlatform").
			Funcs(template.FuncMap{"offset": func(i int) int { return i + stmtUpdateMitigationPlatformSQLArgOffset }}).
			Parse(stmtUpdateMitigationPlatformSQLTemplate),
	)

	ds.tmplAssociateMethodsWithPlatform = template.Must(
		template.New("tmplAssociateMethodsWithPlatform").
			Parse(stmtAssociateMehtodsWithPlatformSQLTemplate),
	)

	ds.tmplCreateMitigationMethod = template.Must(
		template.New("tmplCreateMitigationMethod").
			Funcs(template.FuncMap{"offset": func(i int) int { return i + stmtCreateMitigationMethodSQLArgOffset }}).
			Parse(stmtCreateMitigationMethodSQLTemplate),
	)

	ds.tmplUpdateMitigationMethod = template.Must(
		template.New("tmplUpdateMitigationMethod").
			Funcs(template.FuncMap{"offset": func(i int) int { return i + stmtUpdateMitigationMethodSQLArgOffset }}).
			Parse(stmtUpdateMitigationMethodSQLTemplate),
	)

	ds.tmplAssociateNotificationChannelsWithMethod = template.Must(
		template.New("AssociateNotificationChannelWithMethod").
			Parse(stmtAssociateNotificationChannelsWithMethodSQLTemplate),
	)

	preconditions.ValidateStruct(ds)
	return ds, nil
}

func (ds *mitigateDataSource) Close() {
	database.CloseStatements(ds)
}

const stmtLoadThresholdMitigationsBaseSQL = `
SELECT
      TM.id,
      TM.threshold_id,
      TM.mitigation_apply_type,
      TM.mitigation_clear_type,
      TM.mitigation_apply_timer,
      TM.mitigation_clear_timer,
      GREATEST(TM.edate, MP.edate, MM.edate),
      TM.cdate,

      MP.id,
      MP.platform_name,
      COALESCE(MP.platform_description, ''),
      MP.platform_mitigation_device_type,
      MP.platform_mitigation_device_detail,
      MP.edate,
      MP.cdate,

      MM.id,
      COALESCE(MM.method_name, ''),
      COALESCE(MM.method_description, ''),
      MM.method_mitigation_device_type,
      MM.method_mitigation_device_detail,
      COALESCE(MM.white_list, ''),
      MM.grace_period,
      MM.ack_required,
      MM.edate,
      MM.cdate,

      AP.company_id,
      AP.id,
      AP.policy_name,
      AP.dimensions,
      COALESCE(MM.ack_required, false),
      AT.severity
    FROM mn_mitigation_platform MP
     JOIN mn_associated_mitigation_platform_method AMPM ON MP.id = AMPM.mitigation_platform_id
     JOIN mn_mitigation_method MM ON AMPM.mitigation_method_id = MM.id
     JOIN mn_threshold_mitigation TM ON (AMPM.id = TM.pairing_id)
     JOIN mn_alert_threshold AT ON (AT.id = TM.threshold_id)
	  JOIN mn_alert_policy AP ON (AP.id = AT.policy_id)
`

const stmtGetThresholdMitigationsForThresholdSQL = stmtLoadThresholdMitigationsBaseSQL + `
WHERE MM.company_id = $1
	AND AT.company_id = $1
	AND AP.company_id = $1
	AND MP.company_id = $1
	AND TM.threshold_id = $2
`

func (ds *mitigateDataSource) populateThresholdMitigations(rows *sql.Rows) (tms []*kt.ThresholdMitigation, err error) {
	for rows.Next() {
		platform := &kt.MitigationPlatform{}
		method := &kt.MitigationMethod{}
		tm := kt.ThresholdMitigation{
			Platform: platform,
			Method:   method,
		}

		var ackPeriodMins int
		var ackPeriodClearMins int
		if err := rows.Scan(
			&tm.ThresholdMitigationID,
			&tm.ThresholdID,
			&tm.MitigationApplyType,
			&tm.MitigationClearType,
			&ackPeriodMins,      // &tm.AckPeriod,
			&ackPeriodClearMins, // &tm.AckPeriodClear,
			&tm.Edate,
			&tm.Cdate,

			&platform.PlatformID,
			&platform.PlatformName,
			&platform.PlatformDescription,
			&platform.MitigationType,
			&platform.RawOptions,
			&platform.Edate,
			&platform.Cdate,

			&method.MethodID,
			&method.MethodName,
			&method.MethodDescription,
			&method.MethodType,
			&method.RawOptions,
			&method.RawWhiteList,
			&method.RawGracePeriod,
			&method.AckRequired,
			&method.Edate,
			&method.Cdate,

			&tm.CompanyID,
			&tm.PolicyID,
			&tm.AlertPolicyName,
			&tm.RawDimensions,
			&tm.MethodAckReq, // TODO: use the one directly on the MitigationMethod
			&tm.Severity,
		); err != nil {
			ds.Errorf("%d:%d:%d skipping, error when reading alert mitigate rows: %v", tm.CompanyID, tm.PolicyID, tm.ThresholdID, err)
			continue
		}
		ds.Debugf("Starting to load threshold mitigation row %d:%d:%d:%d", tm.CompanyID, tm.PolicyID, tm.ThresholdID, tm.ThresholdMitigationID)

		if err = FixupThresholdMitigation(&tm, ackPeriodMins, ackPeriodClearMins); err != nil {
			ds.Errorf("%d:%d:%d skipping, error when fixing up threshold mitigation: %v", tm.CompanyID, tm.PolicyID, tm.ThresholdID, err)
			continue
		}

		if err = FixupPlatform(tm.Platform); err != nil {
			ds.Errorf("%d:%d:%d skipping, error when fixing up platform: %v", tm.CompanyID, tm.PolicyID, tm.ThresholdID, err)
			continue
		}

		if err = FixupMethod(tm.Method); err != nil {
			ds.Errorf("%d:%d:%d skipping, error when fixing up method: %v", tm.CompanyID, tm.PolicyID, tm.ThresholdID, err)
			continue
		}

		preconditions.ValidateStruct(tm)

		tms = append(tms, &tm)
	}

	if err = rows.Err(); err != nil {
		ds.Errorf("When closing alert mitigation info: %v", err)
	}

	return
}

// Pull any mitigation if any set for this threshold
func (ds *mitigateDataSource) Load(cid kt.Cid, tid kt.Tid) (tms []*kt.ThresholdMitigation, err error) {
	defer func(start time.Time) { ds.metrics.Get("Load", cid).Done(start, err) }(time.Now())

	rows, err := ds.stmtGetThresholdMitigationsForThreshold.Query(cid, tid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tms, err = ds.populateThresholdMitigations(rows)

	return
}

const stmtLoadThresholdMitigationBulkSQL = stmtLoadThresholdMitigationsBaseSQL + `
WHERE MM.company_id = $1
	AND AT.company_id = $1
	AND AP.company_id = $1
	AND MP.company_id = $1
	AND TM.threshold_id = ANY($2)
`

func (ds *mitigateDataSource) LoadBulk(cid kt.Cid, tids []kt.Tid) (tms []*kt.ThresholdMitigation, err error) {
	if len(tids) == 0 {
		return
	}

	defer func(start time.Time) { ds.metrics.Get("LoadBulk", cid).Done(start, err) }(time.Now())

	rows, err := ds.stmtLoadThresholdMitigationBulk.Query(cid, pq.Array(tids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tms, err = ds.populateThresholdMitigations(rows)

	return
}

func FixupThresholdMitigation(thresholdMitigation *kt.ThresholdMitigation, ackPeriodMins, ackPeriodClearMins int) error {
	thresholdMitigation.AckPeriod = time.Minute * time.Duration(ackPeriodMins)
	thresholdMitigation.AckPeriodClear = time.Minute * time.Duration(ackPeriodClearMins)

	dims, err := kt.DimensionsFromRaw([]byte(thresholdMitigation.RawDimensions))
	if err != nil {
		return err
	}
	thresholdMitigation.Dimensions = dims

	ipCidrDimIndex, err := kt.MitigateIPDimIndex(thresholdMitigation.Dimensions)
	if err != nil {
		return err
	}
	thresholdMitigation.IPCidrDimIndex = ipCidrDimIndex
	thresholdMitigation.ProtocolDimIndex = kt.MitigateProtocolDimIndex(thresholdMitigation.Dimensions)

	switch thresholdMitigation.MitigationApplyType {
	case kt.UserAckUnlessTimer, kt.UserAck, kt.Immediate:
	default:
		return fmt.Errorf("Invalid Mitigation Apply Type: %s", thresholdMitigation.MitigationApplyType)
	}

	switch thresholdMitigation.MitigationClearType {
	case kt.UserAckUnlessTimer, kt.UserAck, kt.Immediate:
	default:
		return fmt.Errorf("Invalid Mitigation Clear Type: %s", thresholdMitigation.MitigationClearType)
	}
	return nil
}

func FixupPlatform(platform *kt.MitigationPlatform) error {
	{
		var err error
		switch platform.MitigationType {
		case kt.MitigationTypeA10:
			platform.Options, err = kt.A10PlatformOptionsFromRaw([]byte(platform.RawOptions))
			if err != nil {
				return err
			}
		case kt.MitigationTypeRadware:
			platform.Options, err = kt.RadwarePlatformOptionsFromRaw([]byte(platform.RawOptions))
			if err != nil {
				return err
			}
		case kt.MitigationTypeRTBH:
			platform.Options, err = kt.RTBHPlatformOptionsFromRaw([]byte(platform.RawOptions))
			if err != nil {
				return err
			}
		case kt.MitigationTypeFlowspec:
			platform.Options, err = flowspec.PlatformOptionsFromRaw([]byte(platform.RawOptions))
			if err != nil {
				return err
			}
		case kt.MitigationTypeCFMT:
			platform.Options, err = kt.CFMTPlatformOptionsFromRaw([]byte(platform.RawOptions))
			if err != nil {
				return err
			}
		case kt.MitigationTypeDummy:
			platform.Options = nil
		default:
			return fmt.Errorf("Unknown MitigationType when deserializing: %s", platform.MitigationType)
		}
	}

	return nil
}

func FixupMethod(method *kt.MitigationMethod) (err error) {
	method.GracePeriod = time.Minute * time.Duration(method.RawGracePeriod)

	method.WhiteList, err = util.WhiteListFromRaw(method.RawWhiteList)
	if err != nil {
		return fmt.Errorf("WhiteList parsing error: %w", err)
	}

	switch method.MethodType {
	case kt.MitigationTypeA10:
		method.Options, err = kt.A10MethodOptionsFromRaw([]byte(method.RawOptions))
	case kt.MitigationTypeRadware:
		method.Options, err = kt.RadwareMethodOptionsFromRaw([]byte(method.RawOptions))
	case kt.MitigationTypeRTBH:
		method.Options, err = kt.RTBHMethodOptionsFromRaw([]byte(method.RawOptions))
	case kt.MitigationTypeFlowspec:
		method.Options, err = flowspec.NewMethodOptions([]byte(method.RawOptions))
	case kt.MitigationTypeCFMT:
		method.Options, err = kt.CFMTMethodOptionsFromRaw([]byte(method.RawOptions))
	case kt.MitigationTypeDummy:
		method.Options = nil
	default:
		err = fmt.Errorf("Unknown MitigationType when deserializing: %s", method.MethodType)
	}

	return err
}

const stmtGetAllMitigationMethodsSQL = `
SELECT
	A.id,
	COALESCE(A.method_name,''),
	COALESCE(A.method_description,''),
	A.method_mitigation_device_type,
	A.method_mitigation_device_detail,
	COALESCE(A.white_list,''),
	A.grace_period,
	COALESCE(A.ack_required, false),
	A.method_mitigation_scope_type,
	A.cdate,
	A.edate
FROM mn_mitigation_method AS A
WHERE A.company_id = $1`

func (ds *mitigateDataSource) GetAllMethods(cid kt.Cid) (methods []*kt.MitigationMethod, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetAllMethods", cid).Done(start, err) }(time.Now())

	rows, err := ds.stmtGetAllMitigationMethods.Query(cid)
	if err != nil {
		return nil, fmt.Errorf("While loading all methods: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		method := &kt.MitigationMethod{}
		var cdate, edate time.Time
		var scope string // ignored
		if err = rows.Scan(
			&method.MethodID,
			&method.MethodName,
			&method.MethodDescription,
			&method.MethodType,
			&method.RawOptions,
			&method.RawWhiteList,
			&method.RawGracePeriod,
			&method.AckRequired,
			&scope,
			&cdate,
			&edate,
		); err != nil {
			return nil, err
		}
		if err = FixupMethod(method); err != nil {
			// Method is invalid. Just skip, don't fail to load every other
			// method because of this.
			ds.Warnf("Skipping method %d: %v", method.MethodID, err)
			continue
		}
		methods = append(methods, method)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return methods, nil
}

const stmtGetMitigationMethodsSQL = `
SELECT
	A.id,
	COALESCE(A.method_name,'') as method_name,
	COALESCE(A.method_description,'') as method_description,
	A.method_mitigation_device_type,
	A.method_mitigation_device_detail,
	COALESCE(A.white_list,'') as white_list,
	A.grace_period,
	COALESCE(A.ack_required, false) as ack_required,
	A.cdate,
	A.edate
FROM mn_mitigation_method AS A
WHERE company_id = $1 AND id = ANY($2)
`

func (ds *mitigateDataSource) GetMitigationMethods(
	ctx context.Context,
	cid kt.Cid,
	methodIDs []kt.MitigationMethodID,
) (map[kt.MitigationMethodID]*kt.MitigationMethod, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetMitigationMethods", cid).Done(start, err) }(time.Now())

	methods := []*kt.MitigationMethod{}
	err = ds.stmtGetMitigationMethods.SelectContext(ctx, &methods, cid, pq.Array(methodIDs))
	if err != nil {
		return nil, err
	}

	res := map[kt.MitigationMethodID]*kt.MitigationMethod{}

	for _, method := range methods {
		res[method.MethodID] = method
	}

	return res, nil
}

const stmtGetMitigationPlatformsSQL = `
SELECT
		A.id,
		A.platform_name,
		COALESCE(A.platform_description, '') as platform_description,
		A.platform_mitigation_device_type,
		A.platform_mitigation_device_detail,
		A.cdate,
		A.edate
FROM mn_mitigation_platform AS A
WHERE A.company_id = $1 and A.id = ANY($2)
`

func (ds *mitigateDataSource) GetMitigationPlatforms(
	ctx context.Context,
	cid kt.Cid,
	platformIDs []kt.MitigationPlatformID,
) (map[kt.MitigationPlatformID]*kt.MitigationPlatform, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetMitigationPlatforms", cid).Done(start, err) }(time.Now())

	platforms := []*kt.MitigationPlatform{}
	err = ds.stmtGetMitigationPlatforms.SelectContext(ctx, &platforms, cid, pq.Array(platformIDs))
	if err != nil {
		return nil, err
	}

	res := map[kt.MitigationPlatformID]*kt.MitigationPlatform{}
	for _, platform := range platforms {
		res[platform.PlatformID] = platform
	}

	return res, nil
}

func (ds *mitigateDataSource) GetDevicesAndBGPAddrs(cid kt.Cid) (map[kt.DeviceID]kt.DeviceProxyBGPPair, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetDevicesAndBGPAddrs", cid).Done(start, err) }(time.Now())

	var rows []struct {
		DeviceID        kt.DeviceID    `db:"id"`
		DeviceProxyBGP  sql.NullString `db:"device_proxy_bgp"`
		DeviceProxyBGP6 sql.NullString `db:"device_proxy_bgp6"`
	}

	err = ds.stmtGetForDeviceBGP.Select(&rows, cid)
	if err != nil {
		return nil, err
	}

	devices := map[kt.DeviceID]kt.DeviceProxyBGPPair{}
	for _, row := range rows {
		devices[row.DeviceID] = kt.DeviceProxyBGPPair{V4: row.DeviceProxyBGP.String, V6: row.DeviceProxyBGP6.String}
	}

	return devices, nil
}

func (ds *mitigateDataSource) MarkRTBHAnnounceForDevices(cid kt.Cid, dids *kt.DeviceIDSet, cidr string, args []byte, platformID kt.MitigationPlatformID, mitigationID kt.MitigationID) error {
	mt := ds.metrics.Get("MarkRTBHAnnounceForDevice", cid)
	for _, did := range dids.Items() {
		start := time.Now()

		_, err := ds.stmtMarkRTBHAnnounceForDevice.Exec(&kt.RTBHCall{
			CompanyID:    cid,
			DeviceID:     kt.DeviceID(did),
			AlertCidr:    cidr,
			Args:         string(args),
			PlatformID:   platformID,
			MitigationID: mitigationID,
		})
		if err != nil {
			mt.Error(start)
			ds.Warnf( // Just log as warning. Maybe there was already a row for this did.
				"Could not write rtbh announce cid=%d platform=%d mitid=%d did=%d cidr=%s: %s",
				cid, platformID, mitigationID, did, cidr, err)
		} else {
			mt.Success(start)
		}
	}
	return nil
}

func (ds *mitigateDataSource) MarkRTBHWithdrawForDevices(cid kt.Cid, dids *kt.DeviceIDSet, cidr string, args []byte, platformID kt.MitigationPlatformID, mitigationID kt.MitigationID) error {
	mt := ds.metrics.Get("MarkRTBHWithdrawForDevice", cid)
	for _, did := range dids.Items() {
		start := time.Now()
		_, err := ds.stmtMarkRTBHWithdrawForDevice.Exec(&kt.RTBHCall{
			CompanyID:    cid,
			DeviceID:     kt.DeviceID(did),
			AlertCidr:    cidr,
			PlatformID:   platformID,
			MitigationID: mitigationID,
		})
		if err != nil {
			mt.Error(start)
			return err
		}
		mt.Success(start)
	}
	return nil
}

func (ds *mitigateDataSource) GetCurrentRTBHCalls(cid kt.Cid) (rtbhCalls []*kt.RTBHCall, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetCurrentRTBHCalls", cid).Done(start, err) }(time.Now())

	err = ds.stmtGetCurrentRTBHCalls.Select(&rtbhCalls, qCid{cid})
	if err != nil {
		return nil, fmt.Errorf("GetCurrentRTBHCalls: %v", err)
	}

	return
}

func (ds *mitigateDataSource) MarkFlowspecAnnounceForDevices(cid kt.Cid, dids *kt.DeviceIDSet, cidr string, args []byte, platformID kt.MitigationPlatformID, mitigationID kt.MitigationID) error {
	var errCount int
	var retErr error
	mt := ds.metrics.Get("MarkFlowspecAnnounceForDevice", cid)

	for _, did := range dids.Items() {
		start := time.Now()
		_, err := ds.stmtMarkFlowspecAnnounceForDevice.Exec(&flowspec.Call{
			CompanyID:    cid,
			DeviceID:     did,
			AlertCidr:    cidr,
			Args:         string(args),
			PlatformID:   platformID,
			MitigationID: mitigationID,
		})
		if err != nil {
			errCount++
			mt.Error(start)
			retErr = fmt.Errorf("could not write flowspec announce cid=%d platform=%d mitid=%d did=%d args=%s err=%s", cid, platformID, mitigationID, did, args, err)
			ds.Warnf(retErr.Error()) // Just log as warning. Maybe there was already a row for this did.
		} else {
			mt.Success(start)
		}
	}
	if errCount >= dids.Len() {
		return fmt.Errorf("MarkFlowspecAnnounceForDevices failed for all devices, Last error: %s", retErr)
	}
	return nil
}

func (ds *mitigateDataSource) MarkFlowspecWithdrawForDevices(cid kt.Cid, dids *kt.DeviceIDSet, cidr string, args []byte, platformID kt.MitigationPlatformID, mitigationID kt.MitigationID) error {
	mt := ds.metrics.Get("MarkFlowspecWithdrawForDevice", cid)

	for _, did := range dids.Items() {
		start := time.Now()
		call := &flowspec.Call{
			CompanyID:    cid,
			DeviceID:     did,
			AlertCidr:    cidr,
			PlatformID:   platformID,
			MitigationID: mitigationID,
			Args:         string(args),
		}
		res, err := ds.stmtMarkFlowspecWithdrawForDevice.Exec(call)
		if err != nil {
			mt.Error(start)
			return err
		}
		count, err := res.RowsAffected()
		if err != nil {
			mt.Error(start)
			return err
		}
		if count != 1 {
			mt.Error(start)
			return fmt.Errorf("MarkFlowspecWithdrawForDevices did not delete any rows for %+v", call)
		}
		mt.Success(start)
	}

	return nil
}

func (ds *mitigateDataSource) GetCurrentFlowspecCalls(cid kt.Cid) (flowspecCalls []*flowspec.Call, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetCurrentFlowspecCalls", cid).Done(start, err) }(time.Now())

	err = ds.stmtGetCurrentFlowspecCalls.Select(&flowspecCalls, qCid{cid})
	if err != nil {
		return nil, fmt.Errorf("GetCurrentFlowspecCalls: %v", err)
	}

	return flowspecCalls, nil
}

func (ds *mitigateDataSource) GetNextMitigationID(cid kt.Cid) (kt.MitigationID, error) {
	mt := ds.metrics.Get("GetNextMitigationID", cid)
	start := time.Now()

	var nextMitID kt.MitigationID
	err := ds.stmtLoadNextMitigationID.QueryRowx(&mnMit2MitigationIDRow{
		CompanyID: cid,
	}).Scan(&nextMitID)

	if err == sql.ErrNoRows {
		mt.Success(start)
		return 10000, nil
	} else if err != nil {
		mt.Error(start)
		return 0, err
	}

	mt.Success(start)
	return nextMitID, nil
}

func (ds *mitigateDataSource) SetNextMitigationID(cid kt.Cid, mitID kt.MitigationID) (err error) {
	defer func(start time.Time) { ds.metrics.Get("SetNextMitigationID", cid).Done(start, err) }(time.Now())

	_, err = ds.stmtSaveNextMitigationID.Exec(&mnMit2MitigationIDRow{
		CompanyID:    cid,
		MitigationID: mitID,
	})

	return
}

type qCid struct { // Helper struct for named queries
	Cid kt.Cid `db:"company_id"`
}

// GetStateMachineStore returns a version of the mitigateDataSource that chf/eggs/state
// can use. It's scoped to a single Cid so state doesn't have to know about that.
func (ds *mitigateDataSource) GetStateMachineStore(cid kt.Cid) state.MachineStoreAndEventListener {
	return &cidScopedMachineStore{
		mitigateDataSource: ds,
		cid:                cid,
	}
}

type cidScopedMachineStore struct {
	*mitigateDataSource
	cid kt.Cid
}

// mnMit2MachinesRow corresponds directly to a row from mn_mit2_machines.
type mnMit2MachinesRow struct {
	CompanyID    kt.Cid    `db:"company_id"`
	MachineClass string    `db:"machine_class"`
	MachineID    string    `db:"machine_id"`
	StateName    string    `db:"state_name"`
	Params       string    `db:"params"`       // JSON
	StateObject  string    `db:"state_object"` // JSON
	Metadata     string    `db:"metadata"`     // JSON
	Cdate        time.Time `db:"cdate"`
	Edate        time.Time `db:"edate"`
}

// mnMit2MachinesLogRow corresponds directly to a row from mn_mit2_machines_log.
type mnMit2MachinesLogRow struct {
	CompanyID      kt.Cid    `db:"company_id"`
	MachineClass   string    `db:"machine_class"`
	MachineID      string    `db:"machine_id"`
	StatesLeaving  string    `db:"states_leaving"`  // comma separated list
	StatesEntering string    `db:"states_entering"` // comma separated list
	FromStateName  string    `db:"from_state_name"`
	ToStateName    string    `db:"to_state_name"`
	EventName      string    `db:"event_name"`
	Params         string    `db:"params"`       // JSON
	StateObject    string    `db:"state_object"` // JSON
	Metadata       string    `db:"metadata"`     // JSON
	Stamp          time.Time `db:"stamp"`
}

// mnMit2MachinesLogRowJSONFields contains rows extracted from
// JSON blobs in mn_mit2_machines_log.
type mnMit2MachinesLogRowJSONFields struct {
	// JSON keys => all nullable.
	MitigationID         *kt.MitigationID         `db:"mitigation_id"` // from params
	IPCidr               *string                  `db:"ip_cidr"`       // from params
	MitigationPlatformID *kt.MitigationPlatformID `db:"platform_id"`   // from params
	MitigationMethodID   *kt.MitigationMethodID   `db:"method_id"`     // from params
	AlarmID              *kt.AlarmID              `db:"alarm_id"`      // from params
	Comment              *string                  `db:"comment"`       // from metadata
}

type mnMit2MachinesLogRowWithExtraFields struct {
	mnMit2MachinesLogRow
	mnMit2MachinesLogRowJSONFields

	// LEFT JOIN keys => all nullable.
	PolicyID    *kt.PolicyID `db:"alert_id"`     // mn_alert_alarm.alert_id
	ThresholdID *kt.Tid      `db:"threshold_id"` // mn_alert_alarm.threshold_id
	AlertKey    *string      `db:"alert_key"`    // mn_alert_alarm.alert_key
}

// mnMit2MitigationIDRow corresponds directly to a row from mn_mit2_mitigation_id.
type mnMit2MitigationIDRow struct {
	CompanyID    kt.Cid          `db:"company_id"`
	MitigationID kt.MitigationID `db:"mitigation_id"`
}

func (cds *cidScopedMachineStore) LoadSerializedMachines() (*state.SerializedMachines, error) {
	var err error
	defer func(start time.Time) { cds.metrics.Get("LoadSerializedMachines", cds.cid).Done(start, err) }(time.Now())

	var rows []*mnMit2MachinesRow
	err = cds.stmtLoadStateMachines.Select(&rows, &qCid{cds.cid})
	if err != nil {
		return nil, err
	}

	result := &state.SerializedMachines{
		Machines: make([]*state.SerializedMachine, 0, len(rows)),
	}
	for _, row := range rows {
		result.Machines = append(result.Machines, &state.SerializedMachine{
			StateName: row.StateName,
			Key: state.MachineKey{
				Class: row.MachineClass,
				ID:    row.MachineID,
			},
			Params:      row.Params,
			StateObject: row.StateObject,
			Metadata:    row.Metadata,

			Deleted: false,
			Dirty:   false,
		})
	}

	return result, nil
}

func (cds *cidScopedMachineStore) SaveSerializedMachines(machines *state.SerializedMachines) (err error) {
	// TODO(tjonak): should this take tx.Rollback error into account
	defer func(start time.Time) { cds.metrics.Get("SaveSerializedMachines", cds.cid).Done(start, err) }(time.Now())

	tx, err := cds.chalert.Rwx.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck
	txDel := tx.NamedStmt(cds.stmtDeleteStateMachinesCurrent)
	txUpsert := tx.NamedStmt(cds.stmtSaveStateMachinesCurrent)

	for _, machine := range machines.Machines {
		if !machine.Dirty {
			continue // Only save dirty machines.
		}

		if machine.Deleted {
			if _, err = txDel.Exec(map[string]interface{}{
				"company_id":    cds.cid,
				"machine_class": machine.Key.Class,
				"machine_id":    machine.Key.ID,
			}); err != nil {
				return err
			}
		} else {
			if _, err = txUpsert.Exec(&mnMit2MachinesRow{
				CompanyID:    cds.cid,
				MachineClass: machine.Key.Class,
				MachineID:    machine.Key.ID,
				StateName:    machine.StateName,
				Params:       machine.Params,
				StateObject:  machine.StateObject,
				Metadata:     machine.Metadata,
				// Cdate default CURRENT_TIMESTAMP
				// Edate default CURRENT_TIMESTAMP
			}); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (cds *cidScopedMachineStore) HandleEvents(scope pipeline.Scope, history *state.MachineEvents) error {
	if history == nil {
		return nil
	}

	mt := cds.metrics.Get("HandleEvent", cds.cid)

	// TODO(tjonak): bulk this if possible
	for _, evt := range history.Events {
		start := time.Now()
		if _, err := cds.stmtSaveStateMachinesLog.Exec(&mnMit2MachinesLogRow{
			CompanyID:      cds.cid,
			MachineClass:   evt.Class,
			MachineID:      evt.ID,
			StatesLeaving:  strings.Join(evt.StatesLeaving, ","),
			StatesEntering: strings.Join(evt.StatesEntering, ","),
			FromStateName:  evt.FromStateName,
			ToStateName:    evt.ToStateName,
			EventName:      evt.EventName,
			Params:         evt.CurrentParams,
			StateObject:    evt.CurrentStateObject,
			Metadata:       evt.CurrentMetadata,
			// Stamp default CURRENT_TIMESTAMP
		}); err != nil {
			mt.Error(start)
			cds.Warnf( // Not critical, just log as warning.
				"Could not save machine to log cid:%v id:%v event:%v",
				cds.cid, evt.ID, evt.EventName)
		} else {
			mt.Success(start)
		}
	}

	return nil
}

func (ds *mitigateDataSource) GetMitigationHistoryV2Count(cid kt.Cid, request *kt.MitigationHistoryRequest) (c int, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetMitigationHistoryV2Count", cid).Done(start, err) }(time.Now())

	filterExprs, params := mitigationHistoryV2FilterSQL(cid, request)
	filterExprs = append(filterExprs, "l.to_state_name IN (:state_name_in)")
	params["state_name_in"] = []string{machinev2.Mitigating, machinev3.Mitigating}
	/*
		Using virtual columns instead of these computed values:
		JSON_VALUE(l.params, '$.mitigationID') AS 'mitigation_id',
	*/
	err = namedInRebindGet(ds.chalert.Rox, &c, `SELECT count(*)
		FROM mn_mit2_machines_log l
		WHERE `+strings.Join(filterExprs, " AND "),
		params)

	return
}

func (ds *mitigateDataSource) GetMitigationHistoryV2EndWaitCount(cid kt.Cid, request *kt.MitigationHistoryRequest) (c int, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetMitigationHistoryV2EndWaitCount", cid).Done(start, err) }(time.Now())

	filterExprs, params := mitigationHistoryV2FilterSQL(cid, request)
	filterExprs = append(filterExprs, "l.to_state_name IN (:state_name_in)")
	params["state_name_in"] = []string{machinev2.EndWait, machinev2.EndWait}
	/*
		Using virtual columns instead of these computed values:
		JSON_VALUE(l.params, '$.mitigationID') AS 'mitigation_id',
	*/

	err = namedInRebindGet(ds.chalert.Rox, &c, `SELECT count(*)
		FROM mn_mit2_machines_log l
		WHERE `+strings.Join(filterExprs, " AND "),
		params)

	return
}

func (ds *mitigateDataSource) GetMitigationHistoryV2(cid kt.Cid, request *kt.MitigationHistoryRequest) ([]kt.MitigationHistoryEvent, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetMitigationHistoryV2", cid).Done(start, err) }(time.Now())

	filterExprs, params := mitigationHistoryV2FilterSQL(cid, request)
	sortExpr := mitigationHistoryV2SortSQL(request)

	limitExpr, limitParams := mitigationHistoryV2LimitSQL(request)
	for k, v := range limitParams {
		params[k] = v
	}

	/*
		Using virtual columns instead of these computed values:
		JSON_VALUE(l.params, '$.mitigationID') AS 'mitigation_id',
		JSON_VALUE(l.params, '$.ipCidr') AS 'ip_cidr',
		JSON_VALUE(l.params, '$.mitigationPlatformID') AS 'platform_id',
		JSON_VALUE(l.params, '$.mitigationMethodID') AS 'method_id',
		JSON_VALUE(l.params, '$.alarmID') AS 'alarm_id',
		JSON_VALUE(l.params, '$.policyID') AS 'alert_id',
		JSON_VALUE(l.params, '$.thresholdID') AS 'threshold_id',
		JSON_VALUE(l.params, '$.alertKey') AS 'alert_key'
	*/
	baseQuery := `
	SELECT
		l.company_id,
		l.machine_class,
		l.machine_id,
		l.states_leaving,
		l.states_entering,
		l.from_state_name,
		l.to_state_name,
		l.event_name,
		l.params,
		l.state_object,
		l.metadata,
		l.stamp,
		l.mitigation_id,
		l.ip_cidr,
		l.platform_id,
		l.method_id,
		l.alarm_id,
		l.policy_id AS 'alert_id',
		l.threshold_id,
		l.alert_key
	FROM mn_mit2_machines_log l
	WHERE
	`
	fullQuery := fmt.Sprintf("%s %s %s %s", baseQuery, strings.Join(filterExprs, " AND "), sortExpr, limitExpr)

	var rows []*mnMit2MachinesLogRowWithExtraFields
	err = namedInRebindSelect(ds.chalert.Rox, &rows, fullQuery, params)
	if err != nil {
		return nil, err
	}

	var result []kt.MitigationHistoryEvent
	for _, row := range rows {
		evt := kt.MitigationHistoryEvent{
			MitigationID: row.MitigationID,
			IPCidr:       row.IPCidr,
			Key:          row.AlertKey,
			OldState:     &row.FromStateName,
			NewState:     &row.ToStateName,
			PlatformID:   row.MitigationPlatformID,
			MethodID:     row.MitigationMethodID,
			PolicyID:     row.PolicyID,
			ThresholdID:  row.ThresholdID,
			AlarmID:      row.AlarmID,
			Ctime:        row.Stamp,
			Comment:      row.Comment,
			Args:         nil,               // Not applicable in mitigate v2
			Type:         kt.AutoMitigation, // overridden below when needed
			EventName:    row.EventName,
		}
		if evt.PolicyID == nil || *evt.PolicyID == -1 { // manual mitigation
			evt.Type = kt.ManualMitigation
			if evt.IPCidr != nil {
				keyPart := strings.Replace(*evt.IPCidr, ":", "_", -1) // GetKeyPartFromIPMaybePrefix
				evt.Key = &keyPart
			}
			evt.PolicyID = nil
			evt.ThresholdID = nil
			evt.AlarmID = nil
		}
		result = append(result, evt)
	}

	return result, nil
}

func mitigationHistoryV2FilterSQL(cid kt.Cid, request *kt.MitigationHistoryRequest) ([]string, map[string]interface{}) {
	exprs := make([]string, 0, 4)
	params := map[string]interface{}{}

	exprs = append(exprs, "l.company_id = :company_id")
	params["company_id"] = cid
	exprs = append(exprs, "l.machine_class IN (:mit2_classes)")
	params["mit2_classes"] = mitigate2MachineClasses

	switch request.FilterBy {
	case kt.MHFilterByMitigationID:
		//exprs = append(exprs, "JSON_VALUE(l.params, '$.mitigationID') = :filter")
		exprs = append(exprs, "l.mitigation_id = :filter")
		params["filter"] = request.FilterVal
	case kt.MHFilterByIPCidr:
		//exprs = append(exprs, "JSON_VALUE(l.params, '$.ipCidr') = :filter")
		exprs = append(exprs, "l.ip_cidr = :filter")
		params["filter"] = request.FilterVal
	case kt.MHFilterByIPCidrSubstring:
		//exprs = append(exprs, "JSON_VALUE(l.params, '$.ipCidr') LIKE concat('%', :filter, '%')")
		exprs = append(exprs, "l.ip_cidr LIKE concat('%', :filter, '%')")
		params["filter"] = request.FilterVal
	case kt.MHFilterByOldState:
		exprs = append(exprs, "l.from_state_name = :filter")
		params["filter"] = request.FilterVal
	case kt.MHFilterByNewState:
		exprs = append(exprs, "l.to_state_name = :filter")
		params["filter"] = request.FilterVal
	case kt.MHFilterByAnyState:
		exprs = append(exprs, "l.from_state_name = :filter OR l.to_state_name = :filter")
		params["filter"] = request.FilterVal
	case "": // Nothing
	default: // Unknown FilterBy!
	}

	if request.StartTime != nil {
		exprs = append(exprs, "l.stamp >= :start")
		params["start"] = *request.StartTime
	}

	if request.EndTime != nil {
		exprs = append(exprs, "l.stamp < :end")
		params["end"] = *request.EndTime
	}

	return exprs, params
}

func mitigationHistoryV2SortSQL(request *kt.MitigationHistoryRequest) string {
	switch request.SortBy {
	case kt.MHSortByMitigationID:
		//return "ORDER BY JSON_VALUE(l.params, '$.mitigationID'), l.id DESC"
		return "ORDER BY l.mitigation_id, l.id DESC"
	case kt.MHSortByIPCidr:
		//return "ORDER BY JSON_VALUE(l.params, '$.ipCidr'), l.id DESC"
		return "ORDER BY l.ip_cidr, l.id DESC"
	case kt.MHSortByState:
		return "ORDER BY to_state_name ASC, l.id DESC"
	case kt.MHSortByPlatformID:
		//return "ORDER BY JSON_VALUE(l.params, '$.mitigationPlatformID'), l.id DESC"
		return "ORDER BY l.platform_id, l.id DESC"
	case kt.MHSortByPolicyID:
		//return "ORDER BY JSON_VALUE(l.params, '$.alertKey'), l.id DESC"
		return "ORDER BY l.alert_key, l.id DESC"
	case kt.MHSortByAlarmID:
		//return "ORDER BY JSON_VALUE(l.params, '$.alarmID'), l.id DESC"
		return "ORDER BY l.alarm_id, l.id DESC"
	case kt.MHSortByCtime, "": // id by default
		return "ORDER BY l.id DESC"
	default:
		return "ORDER BY l.id DESC"
	}
}

// MitigationHistoryMaxRows upper bound on history size
const MitigationHistoryMaxRows = 1000

// mitigationHistoryV2LimitSQL returns tag and value for limit requested from api, defaults to MitigationHistoryMaxRows
func mitigationHistoryV2LimitSQL(request *kt.MitigationHistoryRequest) (string, map[string]interface{}) {
	limit := request.Limit
	if limit == 0 || limit > MitigationHistoryMaxRows {
		limit = MitigationHistoryMaxRows
	}

	return "LIMIT :limit", map[string]interface{}{"limit": int64(limit)}
}

var (
	mitigate2MachineClasses = []string{machinev2.MitigationMachine, machinev3.MitigationMachine}
	mitigate2ActiveStates   = func() []string {
		ss := make([]string, 0, len(machinev2.APIActiveStates)+len(machinev3.APIActiveStates))
		ss = append(ss, machinev2.APIActiveStates...)
		ss = append(ss, machinev3.APIActiveStates...)
		return ss
	}()
)

func (ds *mitigateDataSource) GetCompaniesWithActiveMitigations(ctx context.Context) (result []kt.Cid, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetCompaniesWithActiveMitigations", 0).Done(start, err) }(time.Now())

	err = namedInRebindSelectContext(ctx, ds.chalert.Rox, &result, `
	(SELECT DISTINCT company_id
		FROM mn_mit2_machines
		WHERE machine_class IN (:mit2_classes)
			AND state_name IN (:mit2_states))`, map[string]interface{}{
		"mit2_classes": mitigate2MachineClasses,
		"mit2_states":  mitigate2ActiveStates,
	})

	return
}

func (ds *mitigateDataSource) GetTotalMitigations() (count int, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetTotalMitigations", 0).Done(start, err) }(time.Now())

	err = namedInRebindGet(ds.chalert.Rox, &count, `
SELECT COUNT(*)
FROM mn_mit2_machines
WHERE machine_class IN (:mit2_classes)`, map[string]interface{}{
		"mit2_classes": mitigate2MachineClasses,
	})

	return
}

const stmtGetMitigationSelectSQL = `
SELECT
	COALESCE(ml.mitigation_id, 0) AS mitigation_id,
	COALESCE(ml.threshold_id, 0) AS threshold_id,
	COALESCE(ml.alarm_id, 0) AS alarm_id,
	COALESCE(ml.ip_cidr, '') AS ip_cidr,
	COALESCE(JSON_EXTRACT(ml.params, '$.target'), '') AS target,
	COALESCE(ml.policy_id, 0) AS policy_id,
	COALESCE(JSON_EXTRACT(ml.params, '$.minutesBeforeAutoStopManual'), 0) AS autostop_ttl_minutes,
	COALESCE(ml.platform_id, 0) AS platform_id,
	COALESCE(ml.method_id, 0) AS method_id,
	CONVERT(COALESCE(initial_state_time, 0), DATE) AS initial_state_time,
	CONVERT(COALESCE(current_state_time, 0), DATE) AS current_state_time
`

// TODO(tjonak): group_concat has limit of 1024 characters, we may hit that and return illformed json
const stmtGetMitigationSQL = stmtGetMitigationSelectSQL + `,
	CONCAT('[', GROUP_CONCAT(JSON_OBJECT('stamp', date_format(ml.current_state_time,'%Y-%m-%dT%H:%i:%SZ'), 'state', ml.to_state_name, 'event', ml.event_name) ORDER BY id ASC),']') AS state_transitions
FROM mn_mit2_machines_log ml
WHERE ml.company_id = ? AND ml.mitigation_id = ?
GROUP BY ml.mitigation_id;
`

func (ds *mitigateDataSource) GetMitigation(
	ctx context.Context,
	companyID kt.Cid,
	mitigationID kt.MitigationID,
) (*kt.MitigationRepr, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetMitigation", companyID).Done(start, err) }(time.Now())

	result := &kt.MitigationRepr{}
	err = ds.stmtGetMitigation.GetContext(ctx, result, companyID, mitigationID)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(result.StateTransitionsRaw, &result.StateTransitions)
	if err != nil {
		return nil, fmt.Errorf("Couldn't unmarshall state_transitions: %v", err)
	}

	return result, nil
}

const stmtGetMitigationBulkSQL = stmtGetMitigationSelectSQL + `,
	CONCAT('[', GROUP_CONCAT(JSON_OBJECT('stamp', date_format(ml.current_state_time,'%Y-%m-%dT%H:%i:%SZ'), 'state', ml.to_state_name, 'event', ml.event_name)),']') AS state_transitions
FROM mn_mit2_machines_log ml
WHERE ml.company_id = ? AND ml.mitigation_id IN (?)
GROUP BY ml.mitigation_id;
`

func (ds *mitigateDataSource) GetMitigationBulk(
	ctx context.Context,
	companyID kt.Cid,
	mitigationIDs []kt.MitigationID,
) (_ map[kt.MitigationID]*kt.MitigationRepr, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetMitigationBulk", companyID).Done(start, err) }(time.Now())

	mitigations := []*kt.MitigationRepr{}

	result := map[kt.MitigationID]*kt.MitigationRepr{}
	if len(mitigationIDs) == 0 {
		return result, nil
	}

	err = ds.stmtGetMitigationBulk.SelectContext(ctx, &mitigations, companyID, mitigationIDs)
	if err != nil {
		return
	}

	for _, mitigation := range mitigations {
		result[mitigation.MitigationID] = mitigation
	}

	return result, nil
}

const stmtGetPlatformMethodPairsBulkSQL = `
SELECT
	id,
	company_id,
	mitigation_platform_id,
	mitigation_method_id
FROM mn_associated_mitigation_platform_method
WHERE company_id = $1
  AND mitigation_platform_id = ANY($2)
  AND mitigation_method_id = ANY($3)
  AND status = 'A'
`

// GetPlatformMethodPairsBulk fetches PlatformMethod pairs for given ids
func (ds *mitigateDataSource) GetPlatformMethodPairsBulk(
	ctx context.Context,
	companyID kt.Cid,
	platformIDs []kt.MitigationPlatformID,
	methodIDs []kt.MitigationMethodID,
) (res map[kt.PlatMethodIDTuple]*kt.MitigationPlatformMethodPair, err error) {
	res = map[kt.PlatMethodIDTuple]*kt.MitigationPlatformMethodPair{}
	if len(platformIDs) == 0 || len(methodIDs) == 0 {
		return
	}

	defer func(start time.Time) { ds.metrics.Get("GetPlatformMethodPairsBulk", companyID).Done(start, err) }(time.Now())

	interim := []*kt.MitigationPlatformMethodPair{}
	err = ds.stmtGetPlatformMethodPairsBulk.SelectContext(
		ctx,
		&interim,
		companyID,
		pq.Array(platformIDs),
		pq.Array(methodIDs),
	)

	if err != nil {
		return
	}

	for _, pm := range interim {
		res[kt.PlatMethodIDTuple{Platform: pm.PlatformID, Method: pm.MethodID}] = pm
	}

	return
}

// tmplCreateThresholdMitigationsBulkTemplate serves as poor man's multiple value insert
// right now none of those parameters is user defined so it's safe to just concatenate query
// if situation ever changes then well, do something else.
const tmplCreateThresholdMitigationsBulkTemplate = `
INSERT INTO mn_threshold_mitigation(company_id, pairing_id, threshold_id, mitigation_apply_type, mitigation_clear_type, mitigation_apply_timer, mitigation_clear_timer)
VALUES {{- range $ix, $tm := . }}
	{{- if (gt $ix 0) -}} , {{- end}}
	({{$tm.CompanyID}},{{$tm.PairingID}},{{$tm.ThresholdID}},'{{$tm.MitigationApplyType}}','{{$tm.MitigationClearType}}',{{$tm.AckPeriodRaw}},{{$tm.AckPeriodClearRaw}})
{{- end}};
`

func (ds *mitigateDataSource) CreateThresholdMitigations(
	ctx context.Context,
	companyID kt.Cid,
	tms []*kt.ThresholdMitigationShort,
) (err error) {
	executor := func(ctx context.Context, stmt string) (sql.Result, error) { return ds.chwww.Rwx.ExecContext(ctx, stmt) }
	return ds.createThresholdMitigations(ctx, executor, companyID, tms)
}

func (ds *mitigateDataSource) CreateThresholdMitigationsTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	tms []*kt.ThresholdMitigationShort,
) (err error) {
	executor := func(ctx context.Context, stmt string) (sql.Result, error) { return tx.ExecContext(ctx, stmt) }
	return ds.createThresholdMitigations(ctx, executor, companyID, tms)
}

func (ds *mitigateDataSource) createThresholdMitigations(
	ctx context.Context,
	executor func(context.Context, string) (sql.Result, error),
	companyID kt.Cid,
	tms []*kt.ThresholdMitigationShort,
) (err error) {
	if len(tms) == 0 {
		return nil
	}
	wrapErr := func(err error) error { return fmt.Errorf("CreateThresholdMitigations: %w", err) }

	defer func(start time.Time) { ds.metrics.Get("CreateThresholdMitigations", companyID).Done(start, err) }(time.Now())

	stmt, err := render(ds.tmplCreateThresholdMitigationsBulk, tms)
	if err != nil {
		return wrapErr(err)
	}

	res, err := executor(ctx, stmt)
	if err != nil {
		return wrapErr(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return wrapErr(err)
	}

	if rows < int64(len(tms)) {
		return fmt.Errorf("CreateThresholdMitigations: insert has less rows than expected (expected: %d, affected: %d)",
			len(tms), rows)
	}

	return
}

const stmtGetThresholdMitigationsSQL = `
SELECT
	tm.threshold_id,
	mpm.mitigation_platform_id,
	mpm.mitigation_method_id,
	tm.mitigation_apply_type,
	tm.mitigation_clear_type,
	tm.mitigation_apply_timer,
	tm.mitigation_clear_timer
FROM mn_threshold_mitigation tm
JOIN mn_associated_mitigation_platform_method mpm
  ON mpm.id = tm.pairing_id
 AND mpm.company_id = tm.company_id
WHERE tm.company_id = $1
  AND tm.threshold_id = ANY($2)
`

func (ds *mitigateDataSource) GetThresholdMitigations(
	ctx context.Context,
	companyID kt.Cid,
	thresholdIDs []kt.Tid,
) (result map[kt.Tid][]*kt.ThresholdMitigationWithIDs, err error) {
	if len(thresholdIDs) == 0 {
		return nil, nil
	}
	wrapErr := func(err error) error { return fmt.Errorf("GetThresholdMitigations: %w", err) }

	result = map[kt.Tid][]*kt.ThresholdMitigationWithIDs{}
	defer func(start time.Time) { ds.metrics.Get("GetThresholdMitigations", companyID).Done(start, err) }(time.Now())

	tms := []*kt.ThresholdMitigationWithIDs{}
	err = ds.stmtGetThresholdMitigations.SelectContext(ctx, &tms, companyID, pq.Array(thresholdIDs))
	if err != nil {
		return nil, wrapErr(err)
	}

	for _, tm := range tms {
		result[tm.ThresholdID] = append(result[tm.ThresholdID], tm)
	}

	return
}

const stmtGetMitigationPlatformSQL = `
SELECT
	A.id,
	A.platform_name,
	COALESCE(A.platform_description, ''),
	A.platform_mitigation_device_type,
	A.platform_mitigation_device_detail,
	A.cdate,
	A.edate
FROM mn_mitigation_platform AS A
WHERE A.company_id = $1 AND A.id = $2
`

func (ds *mitigateDataSource) GetPlatform(
	ctx context.Context,
	cid kt.Cid,
	platformID kt.MitigationPlatformID,
) (_ *kt.MitigationPlatform, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetPlatform", cid).Done(start, err) }(time.Now())

	platform := &kt.MitigationPlatform{}
	if err = ds.stmtGetMitigationPlatform.QueryRow(cid, platformID).Scan(
		&platform.PlatformID,
		&platform.PlatformName,
		&platform.PlatformDescription,
		&platform.MitigationType,
		&platform.RawOptions,
		&platform.Cdate,
		&platform.Edate,
	); err != nil {
		return nil, err
	}

	if err = FixupPlatform(platform); err != nil {
		return nil, err
	}

	return platform, nil
}

const stmtGetAllMitigationPlatformsSQL = `
SELECT
	A.id,
	A.platform_name,
	COALESCE(A.platform_description, ''),
	A.platform_mitigation_device_type,
	A.platform_mitigation_device_detail,
	A.cdate,
	A.edate
FROM mn_mitigation_platform AS A
WHERE A.company_id = $1`

func (ds *mitigateDataSource) GetAllPlatforms(cid kt.Cid) (platforms []*kt.MitigationPlatform, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetAllPlatforms", cid).Done(start, err) }(time.Now())

	rows, err := ds.stmtGetAllMitigationPlatforms.Query(cid)
	if err != nil {
		return nil, fmt.Errorf("While loading all platforms: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		platform := &kt.MitigationPlatform{}
		var cdate time.Time
		if err = rows.Scan(
			&platform.PlatformID,
			&platform.PlatformName,
			&platform.PlatformDescription,
			&platform.MitigationType,
			&platform.RawOptions,
			&cdate,
			&platform.Edate,
		); err != nil {
			return nil, err
		}
		if err = FixupPlatform(platform); err != nil {
			// Platform is invalid. Just skip, don't fail to load every other
			// platform because of this.
			ds.Warnf("Skipping platform %d: %v", platform.PlatformID, err)
			continue
		}
		platforms = append(platforms, platform)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return platforms, nil
}

const stmtGetMitigationMethodSQL = `
SELECT
	A.id,
	COALESCE(A.method_name,'') AS method_name,
	COALESCE(A.method_description,'') AS method_description,
	A.method_mitigation_device_type,
	COALESCE(A.method_mitigation_device_detail,'') AS method_mitigation_device_detail,
	COALESCE(A.white_list,'') AS white_list,
	A.grace_period,
	COALESCE(A.ack_required, false) AS ack_required,
	A.cdate,
	A.edate
FROM mn_mitigation_method AS A
WHERE A.company_id = $1 AND A.id = $2`

func (ds *mitigateDataSource) GetMethod(
	ctx context.Context,
	companyID kt.Cid,
	methodID kt.MitigationMethodID,
) (_ *kt.MitigationMethodWithNotifChans, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetMethod", companyID).Done(start, err) }(time.Now())
	wrapErr := func(err error) error { return fmt.Errorf("GetMethod: %w", err) }

	row := ds.stmtGetMitigationMethod.QueryRowxContext(ctx, companyID, methodID)

	if err = row.Err(); err != nil {
		return nil, wrapErr(err)
	}

	method := &kt.MitigationMethod{}
	if err = row.StructScan(method); err != nil {
		return nil, wrapErr(err)
	}

	if err = FixupMethod(method); err != nil {
		return nil, wrapErr(err)
	}

	notifChannelIDs, err := ds.getNotificationChannels(ctx, companyID, methodID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		ds.Warnf("Couldn't retrieve notification channels (err: %v)", err)
	}

	return &kt.MitigationMethodWithNotifChans{
		MitigationMethod:     method,
		NotificationChannels: notifChannelIDs,
	}, nil
}

const stmtDeleteMitigationMethodSQL = `DELETE FROM mn_mitigation_method WHERE company_id = $1 AND id = $2`

func (ds *mitigateDataSource) DeleteMethod(ctx context.Context, companyID kt.Cid, methodID kt.MitigationMethodID) (err error) {
	defer func(start time.Time) { ds.metrics.Get("DeleteMethod", companyID).Done(start, err) }(time.Now())
	const entity = "mitigationMethod"

	res, err := ds.stmtDeleteMitigationMethod.ExecContext(ctx, companyID, methodID)
	if err != nil {
		return
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return
	}

	if rows < 1 {
		return ErrNoRows{entity: entity, entityID: uint64(methodID), companyID: companyID}
	}

	if rows > 1 {
		// TODO(tjonak): extract it into error struct
		ds.Warnf(moreThanOneRowLog(companyID, entity, uint64(methodID)))
	}

	return
}

const stmtDeleteMitigationPlatformSQL = `DELETE FROM mn_mitigation_platform WHERE company_id = $1 AND id = $2`

func (ds *mitigateDataSource) DeletePlatform(ctx context.Context, companyID kt.Cid, platformID kt.MitigationPlatformID) (err error) {
	defer func(start time.Time) { ds.metrics.Get("DeletePlatform", companyID).Done(start, err) }(time.Now())
	const entity = "mitigationPlatform"

	res, err := ds.stmtDeleteMitigationPlatform.ExecContext(ctx, companyID, platformID)
	if err != nil {
		return
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return
	}

	if rows < 1 {
		return ErrNoRows{entity: entity, entityID: uint64(platformID), companyID: companyID}
	}

	if rows > 1 {
		// TODO(tjonak): extract it into error struct
		ds.Warnf(moreThanOneRowLog(companyID, entity, uint64(platformID)))
	}

	return
}

const stmtGetNotificationChannelsSQL = `
SELECT DISTINCT notification_channel_id
FROM mn_mitigation_method_notification_channel
WHERE company_id = $1 AND mitigation_method_id = $2
`

func (ds *mitigateDataSource) getNotificationChannels(
	ctx context.Context,
	companyID kt.Cid,
	methodID kt.MitigationMethodID,
) (notifChannelIDs []kt.NotificationChannelID, err error) {
	defer func(start time.Time) { ds.metrics.Get("getNotificationChannels", companyID).Done(start, err) }(time.Now())

	err = ds.stmtGetNotificationChannels.SelectContext(ctx, &notifChannelIDs, companyID, methodID)

	return
}

const stmtGetNotificationChannelsBulkSQL = `
SELECT mitigation_method_id, notification_channel_id
FROM mn_mitigation_method_notification_channel
WHERE company_id = $1 AND mitigation_method_id = ANY($2)
`

func (ds *mitigateDataSource) getNotificationChannelsBulk(
	ctx context.Context,
	companyID kt.Cid,
	methodIDs []kt.MitigationMethodID,
) (res map[kt.MitigationMethodID][]kt.NotificationChannelID, err error) {
	defer func(start time.Time) { ds.metrics.Get("getNotificationChannelsBulk", companyID).Done(start, err) }(time.Now())

	notifChannelList := []kt.MitMethodNotifChannelTuple{}
	err = ds.stmtGetNotificationChannelsBulk.SelectContext(ctx, &notifChannelList, companyID, pq.Array(methodIDs))
	if err != nil {
		return nil, fmt.Errorf("from getNotificationChannelsBulk: %w", err)
	}

	res = make(map[kt.MitigationMethodID][]kt.NotificationChannelID)
	for _, tuple := range notifChannelList {
		res[tuple.MethodID] = append(res[tuple.MethodID], tuple.NotifChannelID)
	}

	return
}

const stmtUpdateMitigationPlatformSQLArgOffset = 3
const stmtUpdateMitigationPlatformSQLTemplate = `
UPDATE mn_mitigation_platform
   SET company_id = $1,
       edate = now() {{- range $id, $col := . -}},
       {{$col}} = ${{offset $id}}
{{- end}}
WHERE company_id = $1 AND id = $2
` + stmtMitPlatformReturnDefault

func (ds *mitigateDataSource) UpdatePlatformTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	platformID kt.MitigationPlatformID,
	bundle kt.PlatformUpdateBundle,
) (plat *kt.MitigationPlatform, err error) {
	errWithMessage := func(err error) (*kt.MitigationPlatform, error) { return nil, fmt.Errorf("UpdatePlatform: %w", err) }

	if len(bundle) == 0 {
		return
	}

	cols, args := []kt.SQLColumn{}, []interface{}{companyID, platformID}
	for _, tpl := range bundle {

		_, ok := ds.permittedPlatformColumns[tpl.Column]
		if !ok {
			return errWithMessage(fmt.Errorf("Unsupported column: %s %v", tpl.Column, tpl.Arg))
		}

		cols = append(cols, tpl.Column)
		args = append(args, tpl.Arg)
	}

	stmt, err := render(ds.tmplUpdateMitigationPlatform, cols)
	if err != nil {
		return errWithMessage(err)
	}

	// just measure execution
	defer func(start time.Time) { ds.metrics.Get("UpdatePlatform", companyID).Done(start, err) }(time.Now())

	row := tx.QueryRowxContext(ctx, stmt, args...)

	if err = row.Err(); err != nil {
		return errWithMessage(err)
	}

	plat = &kt.MitigationPlatform{}
	if err = row.StructScan(plat); err != nil {
		return errWithMessage(err)
	}

	if err = FixupPlatform(plat); err != nil {
		return errWithMessage(err)
	}

	return
}

const stmtMitPlatformReturnDefault = `
RETURNING
	id,
	platform_name,
	COALESCE(platform_description, '') AS platform_description,
	platform_mitigation_device_type,
	COALESCE(platform_mitigation_device_detail,'') AS platform_mitigation_device_detail,
	cdate,
	edate
`

const stmtCreateMitigationPlatformSQLArgOffset = 2
const stmtCreateMitigationPlatformSQLTemplate = `
INSERT INTO mn_mitigation_platform(company_id {{- range $_, $col := . -}}, {{$col}} {{- end -}} )
VALUES ($1 {{- range $id, $_ := . -}}, ${{offset $id}} {{- end -}})
` + stmtMitPlatformReturnDefault

func (ds *mitigateDataSource) CreatePlatformTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	bundle kt.PlatformUpdateBundle,
) (plat *kt.MitigationPlatform, err error) {
	errWithMessage := func(err error) (*kt.MitigationPlatform, error) { return nil, fmt.Errorf("CreatePlatform: %w", err) }

	if len(bundle) == 0 {
		return
	}

	cols, args := []kt.SQLColumn{}, []interface{}{companyID}
	for _, tpl := range bundle {
		_, ok := ds.permittedPlatformColumns[tpl.Column]
		if !ok {
			return errWithMessage(fmt.Errorf("Unsupported column: %s %v", tpl.Column, tpl.Arg))
		}

		cols = append(cols, tpl.Column)
		args = append(args, tpl.Arg)
	}

	stmt, err := render(ds.tmplCreateMitigationPlatform, cols)
	if err != nil {
		return errWithMessage(err)
	}

	// just measure execution
	defer func(start time.Time) { ds.metrics.Get("CreatePlatform", companyID).Done(start, err) }(time.Now())

	row := tx.QueryRowxContext(ctx, stmt, args...)

	if err = row.Err(); err != nil {
		return errWithMessage(err)
	}

	plat = &kt.MitigationPlatform{}
	if err = row.StructScan(plat); err != nil {
		return errWithMessage(err)
	}

	if err = FixupPlatform(plat); err != nil {
		return errWithMessage(err)
	}

	return
}

const stmtAssociateMehtodsWithPlatformSQLTemplate = `
INSERT INTO mn_associated_mitigation_platform_method(company_id,mitigation_platform_id,mitigation_method_id)
VALUES {{- range  $ix, $methodID := . }}
   {{- if (gt $ix 0) -}} , {{- end}}
   ($1,$2,{{$methodID}})
{{- end}};
`

func (ds *mitigateDataSource) AssociateMethodsWithPlatformTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	platformID kt.MitigationPlatformID,
	methodIDs []kt.MitigationMethodID,
) (err error) {
	const entity = `associatedMitigationPlatforms`

	if len(methodIDs) == 0 {
		return
	}

	stmt, err := render(ds.tmplAssociateMethodsWithPlatform, methodIDs)
	if err != nil {
		return err
	}

	defer func(start time.Time) { ds.metrics.Get("AssociateMethodsWithPlatform", companyID).Done(start, err) }(time.Now())

	res, err := tx.ExecContext(ctx, stmt, companyID, platformID)
	if err != nil {
		return
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return
	}

	if rows < 1 {
		return ErrNoRows{entity: entity, entityID: uint64(platformID), companyID: companyID}
	}

	if rows > 1 {
		ds.Warnf(moreThanOneRowLog(companyID, entity, uint64(platformID)))
	}

	return
}

const stmtGetAssociatedMethodsSQL = `
SELECT mitigation_platform_id, array_agg(mitigation_method_id) AS mitigation_method_ids
FROM mn_associated_mitigation_platform_method
WHERE company_id = $1 AND mitigation_platform_id = ANY($2)
GROUP BY mitigation_platform_id
`

func (ds *mitigateDataSource) GetAssociatedMethods(
	ctx context.Context,
	companyID kt.Cid,
	platformIDs []kt.MitigationPlatformID,
) (associatedMethods map[kt.MitigationPlatformID][]kt.MitigationMethodID, err error) {
	associatedMethods = make(map[kt.MitigationPlatformID][]kt.MitigationMethodID)

	if len(platformIDs) == 0 {
		return
	}

	defer func(start time.Time) { ds.metrics.Get("GetAssociatedMethods", companyID).Done(start, err) }(time.Now())

	rows, err := ds.stmtGetAssociatedMethods.QueryxContext(ctx, companyID, pq.Array(platformIDs))
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if err != nil {
		return nil, fmt.Errorf("from GetAssociatedMethods: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var platformID kt.MitigationPlatformID
		methodIDs := []kt.MitigationMethodID{}
		if err := rows.Scan(&platformID, pq.Array(&methodIDs)); err != nil {
			return nil, fmt.Errorf("from GetAssociatedMethods: %w", err)
		}
		associatedMethods[platformID] = methodIDs
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("from GetAssociatedMethods: %w", err)
	}

	return
}

const stmtValidateNotificationChannelsSQL = `
SELECT array_agg(id) @> $2 FROM mn_alert_notification_channel WHERE company_id = $1
`

func (ds *mitigateDataSource) ValidateNotificationChannels(
	ctx context.Context,
	companyID kt.Cid,
	notifChannelIDs []kt.NotificationChannelID,
) (ok bool, err error) {
	const entity = `notificationChannel`

	if len(notifChannelIDs) == 0 {
		return true, nil
	}

	defer func(start time.Time) { ds.metrics.Get("AssociateMehtodsWithPlatform", companyID).Done(start, err) }(time.Now())

	row := ds.stmtValidateNotificationChannels.QueryRowxContext(ctx, companyID, pq.Array(notifChannelIDs))
	if err = row.Err(); err != nil {
		return
	}

	err = row.Scan(&ok)

	return
}

const stmtMitMethodReturnDefault = `
RETURNING
	id,
	method_name,
	COALESCE(method_description,'') AS method_description,
	method_mitigation_device_type,
	method_mitigation_device_detail,
	COALESCE(white_list,'') AS white_list,
	COALESCE(grace_period,0) AS grace_period,
	ack_required,
	cdate,
	edate
`

const stmtCreateMitigationMethodSQLArgOffset = 2
const stmtCreateMitigationMethodSQLTemplate = `
INSERT INTO mn_mitigation_method(company_id, method_mitigation_scope_type {{- range $_, $col := . -}}, {{$col}} {{- end -}} )
VALUES ($1, 'COMPANY' {{- range $id, $_ := . -}}, ${{offset $id}} {{- end -}})
` + stmtMitMethodReturnDefault

func (ds *mitigateDataSource) CreateMethodTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	bundle kt.MethodUpdateBundle,
) (meth *kt.MitigationMethod, err error) {
	errWithMessage := func(err error) (*kt.MitigationMethod, error) { return nil, fmt.Errorf("CreateMethod: %w", err) }

	if len(bundle) == 0 {
		return
	}

	cols, args := []kt.SQLColumn{}, []interface{}{companyID}
	for _, tpl := range bundle {
		_, ok := ds.permittedMethodColumns[tpl.Column]
		if !ok {
			return errWithMessage(fmt.Errorf("Unsupported column: %s %v", tpl.Column, tpl.Arg))
		}

		cols = append(cols, tpl.Column)
		args = append(args, tpl.Arg)
	}

	stmt, err := render(ds.tmplCreateMitigationMethod, cols)
	if err != nil {
		return errWithMessage(err)
	}

	// just measure execution
	defer func(start time.Time) { ds.metrics.Get("CreateMethod", companyID).Done(start, err) }(time.Now())

	row := tx.QueryRowxContext(ctx, stmt, args...)

	if err = row.Err(); err != nil {
		return errWithMessage(err)
	}

	meth = &kt.MitigationMethod{}
	if err = row.StructScan(meth); err != nil {
		return errWithMessage(err)
	}

	if err = FixupMethod(meth); err != nil {
		return errWithMessage(err)
	}

	return
}

const stmtUpdateMitigationMethodSQLArgOffset = 3
const stmtUpdateMitigationMethodSQLTemplate = `
UPDATE mn_mitigation_method
   SET company_id = $1,
       edate = now() {{- range $id, $col := . -}},
       {{$col}} = ${{offset $id}}
{{- end}}
WHERE company_id = $1 AND id = $2
` + stmtMitMethodReturnDefault

func (ds *mitigateDataSource) UpdateMethodTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	platformID kt.MitigationMethodID,
	bundle kt.MethodUpdateBundle,
) (meth *kt.MitigationMethod, err error) {
	errWithMessage := func(err error) (*kt.MitigationMethod, error) { return nil, fmt.Errorf("UpdateMethod: %w", err) }

	if len(bundle) == 0 {
		return
	}

	cols, args := []kt.SQLColumn{}, []interface{}{companyID, platformID}
	for _, tpl := range bundle {

		_, ok := ds.permittedMethodColumns[tpl.Column]
		if !ok {
			return errWithMessage(fmt.Errorf("Unsupported column: %s %v", tpl.Column, tpl.Arg))
		}

		cols = append(cols, tpl.Column)
		args = append(args, tpl.Arg)
	}

	stmt, err := render(ds.tmplUpdateMitigationMethod, cols)
	if err != nil {
		return errWithMessage(err)
	}

	// just measure execution
	defer func(start time.Time) { ds.metrics.Get("UpdateMethod", companyID).Done(start, err) }(time.Now())

	row := tx.QueryRowxContext(ctx, stmt, args...)

	if err = row.Err(); err != nil {
		return errWithMessage(err)
	}

	meth = &kt.MitigationMethod{}
	if err = row.StructScan(meth); err != nil {
		return errWithMessage(err)
	}

	if err = FixupMethod(meth); err != nil {
		return errWithMessage(err)
	}

	return
}

func (ds *mitigateDataSource) UpdateNotificationChannelsToMethodAssociationTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	methodID kt.MitigationMethodID,
	oldChannelSet []kt.NotificationChannelID,
	newChannelSet kt.NotifChannelsWrapped,
) (err error) {
	if !newChannelSet.Contains {
		return
	}

	// TODO(tjonak): this new/old notion happens in various places, establish order and use consistently
	added, deleted := notifChannelChangedEntries(newChannelSet.NotificationChannelIDs, oldChannelSet)

	err = ds.associateNotificationChannelsWithMethodTx(ctx, tx, companyID, methodID, added)
	if err != nil {
		return
	}

	err = ds.deleteNotifChannelsToMethodAssociationTx(ctx, tx, companyID, methodID, deleted)

	return
}

const stmtAssociateNotificationChannelsWithMethodSQLTemplate = `
INSERT INTO mn_mitigation_method_notification_channel(company_id,mitigation_method_id,notification_channel_id)
VALUES {{- range  $ix, $notifChanID := . }}
   {{- if (gt $ix 0) -}} , {{- end}}
   ($1,$2,{{$notifChanID}})
{{- end}};
`

func (ds *mitigateDataSource) associateNotificationChannelsWithMethodTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	methodID kt.MitigationMethodID,
	channels []kt.NotificationChannelID,
) (err error) {
	const entity = `associatedNotificationChannels`

	if len(channels) == 0 {
		return
	}

	stmt, err := render(ds.tmplAssociateMethodsWithPlatform, channels)
	if err != nil {
		return err
	}

	defer func(start time.Time) {
		ds.metrics.Get("associateNotificationChannelsWithMethod", companyID).Done(start, err)
	}(time.Now())

	res, err := tx.ExecContext(ctx, stmt, companyID, methodID)

	rows, err := res.RowsAffected()
	if err != nil {
		return
	}

	if rows < 1 {
		return ErrNoRows{entity: entity, entityID: uint64(methodID), companyID: companyID}
	}

	if rows > 1 {
		ds.Warnf(moreThanOneRowLog(companyID, entity, uint64(methodID)))
	}

	return
}

const stmtDeleteNotifChannelsToMethodAssociationSQL = `
DELETE FROM mn_mitigation_method_notification_channel
WHERE company_id = $1
  AND mitigation_method_id = $2
  AND notification_channel_id = ANY($3)
`

func (ds *mitigateDataSource) deleteNotifChannelsToMethodAssociationTx(
	ctx context.Context,
	tx kt.ChwwwTransaction,
	companyID kt.Cid,
	methodID kt.MitigationMethodID,
	channels []kt.NotificationChannelID,
) (err error) {
	cLen := int64(len(channels))
	if cLen == 0 {
		return
	}

	defer func(start time.Time) {
		ds.metrics.Get("deleteNotifChannelsToMethodAssociation", companyID).Done(start, err)
	}(time.Now())

	stmt := tx.StmtxContext(ctx, ds.stmtDeleteNotifChannelsToMethodAssociation)

	res, err := stmt.ExecContext(ctx, companyID, methodID, pq.Array(channels))
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != cLen {
		ds.Warnf("Amount of rows deleted differs from expected, maybe channel was deleted in meantime (expected: %d, got: %d)",
			rows, cLen)
	}

	return
}

func notifChannelChangedEntries(new, old []kt.NotificationChannelID) (added, deleted []kt.NotificationChannelID) {
	allKeys := make(map[kt.NotificationChannelID]struct{}, len(new)+len(old))
	oldLookup := make(map[kt.NotificationChannelID]struct{}, len(old))
	newLookup := make(map[kt.NotificationChannelID]struct{}, len(new))

	for _, k := range new {
		allKeys[k] = struct{}{}
		oldLookup[k] = struct{}{}
	}

	for _, k := range old {
		allKeys[k] = struct{}{}
		newLookup[k] = struct{}{}
	}

	for k := range allKeys {
		_, inNew := newLookup[k]
		_, inOld := oldLookup[k]

		if inNew && inOld {
			continue
		}

		if inOld {
			deleted = append(deleted, k)
		} else {
			added = append(added, k)
		}
	}

	return
}

const stmtFindMitigationPlatformsSQL = `
SELECT id,
		 platform_name,
		 COALESCE(platform_description,'') AS platform_description,
		 platform_mitigation_device_type,
		 COALESCE(platform_mitigation_device_detail,'') AS platform_mitigation_device_detail,
		 cdate,
		 edate
` + stmtFindMitigationPlatformsBaseSQL + `
LIMIT $9
OFFSET $10
`

const stmtFindMitigationPlatformsBaseSQL = `
FROM mn_mitigation_platform
WHERE company_id = $1
  AND (id = ANY($2) OR ARRAY_LENGTH(CAST($2 AS bigint[]), 1) IS NULL)
  AND (platform_name = ANY($3) OR ARRAY_LENGTH(CAST($3 AS text[]), 1) IS NULL)
  AND (platform_mitigation_device_type = ANY($4) OR ARRAY_LENGTH(CAST($4 AS text[]), 1) IS NULL)
  AND cdate BETWEEN COALESCE($5, CAST('-infinity' AS timestamp without time zone)) AND COALESCE($6, CAST('infinity' AS timestamp without time zone))
  AND edate BETWEEN COALESCE($7, CAST('-infinity' AS timestamp without time zone)) AND COALESCE($8, CAST('infinity' AS timestamp without time zone))
`

const stmtFindMitigationPlatformsCountSQL = `
SELECT count(1)
` + stmtFindMitigationPlatformsBaseSQL

func (ds *mitigateDataSource) FindPlatformsWithCount(
	ctx context.Context,
	filter *kt.FindMitigationPlatformsFilter,
) (platforms []*kt.MitigationPlatform, count uint64, err error) {
	if filter == nil {
		return
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("from FindPlatformsWithCount: %w", err)
		}
	}()

	args := []interface{}{
		filter.CompanyID,
		pq.Array(filter.PlatformIDs),
		pq.Array(filter.Names),
		pq.Array(filter.Types),
		boxTime(filter.CreateStart),
		boxTime(filter.CreateEnd),
		boxTime(filter.ModifyStart),
		boxTime(filter.ModifyEnd),
	}

	{
		defer func(start time.Time) {
			ds.metrics.Get("FindMitigationPlatformsCount", filter.CompanyID).Done(start, err)
		}(time.Now())

		row := ds.stmtFindMitigationPlatformsCount.QueryRowxContext(ctx, args...)
		if err = row.Err(); err != nil {
			return
		}

		if err = row.Scan(&count); err != nil {
			return
		}

		if count == 0 {
			return
		}
	}

	args = append(args, filter.Limit, filter.Offset)

	interim := []*kt.MitigationPlatform{}
	{
		defer func(start time.Time) {
			ds.metrics.Get("FindMitigationPlatforms", filter.CompanyID).Done(start, err)
		}(time.Now())

		err = ds.stmtFindMitigationPlatforms.SelectContext(ctx, &interim, args...)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return
			}
			err = nil
		}
	}

	platforms = make([]*kt.MitigationPlatform, 0, len(interim))
	for _, platform := range interim {
		err := FixupPlatform(platform)
		if err != nil {
			ds.Errorf("couldn't fixup platform, skipping (id: %d, err: %v)", platform.PlatformID, err)
			continue
		}
		platforms = append(platforms, platform)
	}

	return
}

const stmtFindMitigationMethodsSQL = `
SELECT
	id,
	method_name,
	COALESCE(method_description, '') AS method_description,
	method_mitigation_device_type,
	COALESCE(method_mitigation_device_detail, '') AS method_mitigation_device_detail,
	COALESCE(white_list, '') AS white_list,
	COALESCE(grace_period, 0) AS grace_period,
	ack_required,
	cdate,
	edate
` + stmtFindMitigationMethodsBaseSQL + `
LIMIT $9
OFFSET $10
`

const stmtFindMitigationMethodsBaseSQL = `
FROM mn_mitigation_method
WHERE company_id = $1
  AND (id = ANY($2) OR ARRAY_LENGTH(CAST($2 AS bigint[]), 1) IS NULL)
  AND (method_name = ANY($3) OR ARRAY_LENGTH(CAST($3 AS text[]), 1) IS NULL)
  AND (method_mitigation_device_type = ANY($4) OR ARRAY_LENGTH(CAST($4 AS text[]), 1) IS NULL)
  AND cdate BETWEEN COALESCE($5, CAST('-infinity' AS timestamp without time zone)) AND COALESCE($6, CAST('infinity' AS timestamp without time zone))
  AND edate BETWEEN COALESCE($7, CAST('-infinity' AS timestamp without time zone)) AND COALESCE($8, CAST('infinity' AS timestamp without time zone))
`

const stmtFindMitigationMethodsCountSQL = `
SELECT count(1)
` + stmtFindMitigationMethodsBaseSQL

func (ds *mitigateDataSource) FindMethodsWithCount(
	ctx context.Context,
	filter *kt.FindMitigationMethodsFilter,
) (methods []*kt.MitigationMethodWithNotifChans, count uint64, err error) {
	if filter == nil {
		return
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("from FindMethodsWithCount: %w", err)
		}
	}()

	boxTime := func(t time.Time) *time.Time {
		if t.IsZero() {
			return nil
		}
		return &t
	}

	args := []interface{}{
		filter.CompanyID,
		pq.Array(filter.MethodIDs),
		pq.Array(filter.Names),
		pq.Array(filter.Types),
		boxTime(filter.CreateStart),
		boxTime(filter.CreateEnd),
		boxTime(filter.ModifyStart),
		boxTime(filter.ModifyEnd),
	}

	{
		defer func(start time.Time) {
			ds.metrics.Get("FindMitigationMethodsCount", filter.CompanyID).Done(start, err)
		}(time.Now())

		row := ds.stmtFindMitigationMethodsCount.QueryRowxContext(ctx, args...)
		if err = row.Err(); err != nil {
			return
		}

		if err = row.Scan(&count); err != nil {
			return
		}

		if count == 0 {
			return
		}
	}

	args = append(args, filter.Limit, filter.Offset)

	interim := []*kt.MitigationMethod{}
	{
		defer func(start time.Time) {
			ds.metrics.Get("FindMitigationMethods", filter.CompanyID).Done(start, err)
		}(time.Now())

		err = ds.stmtFindMitigationMethods.SelectContext(ctx, &interim, args...)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return
			}
			err = nil
		}
	}

	methodIDs := make([]kt.MitigationMethodID, len(interim))
	for i, method := range interim {
		methodIDs[i] = method.MethodID
	}

	notifChannelIDs, err := ds.getNotificationChannelsBulk(ctx, filter.CompanyID, methodIDs)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			ds.Warnf("Couldn't retrieve notification channels (err: %v)", err)
			return
		}
		err = nil
	}

	methods = make([]*kt.MitigationMethodWithNotifChans, 0, len(interim))
	for _, method := range interim {
		err := FixupMethod(method)
		if err != nil {
			ds.Errorf("couldn't fixup method, skipping (id: %d, err: %v)", method.MethodID, err)
			continue
		}
		methods = append(methods, &kt.MitigationMethodWithNotifChans{
			MitigationMethod:     method,
			NotificationChannels: notifChannelIDs[method.MethodID],
		})
	}

	return
}

// TODO(tjonak): mitigation sideeffects end at ('ACK_REQ','MAUAL_CLEAR') states, if we ever want to include mitigation
// stop time, those values should be somehow preserved, maybe in additional column

const stmtFindMitigationsSQL = stmtGetMitigationSelectSQL + `,
	COALESCE(ml.to_state_name, '') AS to_state_name
FROM mn_mit2_machines_log ml
WHERE id IN (
	SELECT max(id)
	FROM mn_mit2_machines_log
	WHERE company_id = :company_id
	  AND (mitigation_id IN (:mitigation_ids) OR :all_mitigations)
	  AND (policy_id IN (:policy_ids) OR :all_policies)
	  AND (alarm_id IN (:alarm_ids) OR :all_alarms)
	  AND (threshold_id IN (:threshold_ids) OR :all_thresholds)
	  AND (platform_id IN (:platform_ids) OR :all_platforms)
	  AND (method_id IN (:method_ids) OR :all_methods)
	  AND (ip_cidr IN (:ip_cidrs) OR :all_ipcidrs)
	  AND ((policy_id = -1 XOR :type_auto) OR :all_types)
	GROUP BY mitigation_id
) AND (ml.to_state_name IN (:state_names) OR :all_states)
`

const stmtFindMitigationsCountSQL = `
SELECT count(1) FROM ( ` + stmtFindMitigationsSQL + `  ) a
`

const stmtFindMitigationsLimitOffsetSQL = stmtFindMitigationsSQL + `
LIMIT :limit
OFFSET :offset`

func (ds *mitigateDataSource) FindMitigationsWithCount(
	ctx context.Context,
	params *kt.FindMitigationsFilter,
) (mitigations []*kt.MitigationRepr, count uint64, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("from FindMitigationsWithCount: %w", err)
		}
	}()

	args := prepareFindMitigationsArgs(params)

	count, err = ds.findMitigationsCount(ctx, params.CompanyID, args)
	if err != nil {
		return
	}

	if count == 0 {
		// there's no reason for another query
		return
	}

	mitigations, err = ds.findMitigationsLimitOffset(ctx, params.CompanyID, args)

	return
}

func (ds *mitigateDataSource) findMitigationsCount(ctx context.Context, cid kt.Cid, args map[string]interface{}) (count uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("findMitigationsCount", cid).Done(start, err) }(time.Now())

	row, err := ds.stmtFindMitigationsCount.QueryRowxContext(ctx, args)
	if err != nil {
		err = fmt.Errorf("from stmtFindMitigationsCount.QueryRowxContext: %w", err)
		return
	}

	if err = row.Err(); err != nil {
		err = fmt.Errorf("from row.Err: %w", err)
		return
	}

	if err = row.Scan(&count); err != nil {
		err = fmt.Errorf("from row.Scan: %w", err)
		return
	}

	return
}

func (ds *mitigateDataSource) findMitigationsLimitOffset(ctx context.Context, cid kt.Cid, args map[string]interface{}) (mitigations []*kt.MitigationRepr, err error) {
	defer func(start time.Time) { ds.metrics.Get("findMitigationsLimitOffset", cid).Done(start, err) }(time.Now())

	mitigations = []*kt.MitigationRepr{}
	err = ds.stmtFindMitigationsLimitOffset.SelectContext(ctx, &mitigations, args)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if err != nil {
		err = fmt.Errorf("from stmtFindMitigationsLimitOffset.SelectContext: %w", err)
	}

	return
}

func prepareFindMitigationsArgs(params *kt.FindMitigationsFilter) map[string]interface{} {
	allMitigations := false
	if len(params.MitigationIDs) == 0 {
		params.MitigationIDs, allMitigations = []kt.MitigationID{kt.MitigationID(-2)}, true
	}

	allPolicies := false
	if len(params.PolicyIDs) == 0 {
		params.PolicyIDs, allPolicies = []kt.PolicyID{kt.PolicyID(-2)}, true
	}

	allAlarms := false
	if len(params.AlarmIDs) == 0 {
		params.AlarmIDs, allAlarms = []kt.AlarmID{kt.AlarmID(-2)}, true
	}

	allThresholds := false
	if len(params.ThresholdIDs) == 0 {
		params.ThresholdIDs, allThresholds = []kt.Tid{kt.Tid(-2)}, true
	}

	allStates := false
	if len(params.StateNames) == 0 {
		params.StateNames, allStates = []string{""}, true
	}

	allPlatforms := false
	if len(params.PlatformIDs) == 0 {
		params.PlatformIDs, allPlatforms = []kt.MitigationPlatformID{kt.MitigationPlatformID(-2)}, true
	}

	allMethods := false
	if len(params.MethodIDs) == 0 {
		params.MethodIDs, allMethods = []kt.MitigationMethodID{kt.MitigationMethodID(-2)}, true
	}

	allIPCidrs := false
	if len(params.IPCidrs) == 0 {
		params.IPCidrs, allIPCidrs = []string{""}, true
	}

	typeAuto := false
	allTypes := false

	typeDict := map[kt.MitigationType]struct{}{}

	for _, t := range params.Types {
		typeDict[t] = struct{}{}
	}

	if len(typeDict) == 0 || len(typeDict) > 1 {
		allTypes = true
	} else {
		typeAuto = params.Types[0] == kt.MitigationSourceAuto
	}

	// TODO(tjonak): time parameters

	return map[string]interface{}{
		"company_id":      params.CompanyID,
		"mitigation_ids":  params.MitigationIDs,
		"all_mitigations": allMitigations,
		"policy_ids":      params.PolicyIDs,
		"all_policies":    allPolicies,
		"alarm_ids":       params.AlarmIDs,
		"all_alarms":      allAlarms,
		"threshold_ids":   params.ThresholdIDs,
		"all_thresholds":  allThresholds,
		"state_names":     params.StateNames,
		"all_states":      allStates,
		"platform_ids":    params.PlatformIDs,
		"all_platforms":   allPlatforms,
		"method_ids":      params.MethodIDs,
		"all_methods":     allMethods,
		"ip_cidrs":        params.IPCidrs,
		"all_ipcidrs":     allIPCidrs,
		"type_auto":       typeAuto,
		"all_types":       allTypes,
		"limit":           int64(params.Limit),
		"offset":          int64(params.Offset),
	}
}

const stmtValidatePlatformMethodPairSQL = `
SELECT EXISTS (
	SELECT mitigation_method_id
	FROM mn_associated_mitigation_platform_method
	WHERE company_id = $1 AND mitigation_platform_id = $2 AND mitigation_method_id = $3
)`

func (ds *mitigateDataSource) ValidatePlatformMethodPair(
	ctx context.Context,
	companyID kt.Cid,
	platformID kt.MitigationPlatformID,
	methodID kt.MitigationMethodID,
) (valid bool, err error) {
	defer func(start time.Time) { ds.metrics.Get("ValidatePlatformMethodPair", companyID).Done(start, err) }(time.Now())

	err = ds.stmtValidatePlatformMethodPair.GetContext(ctx, &valid, companyID, platformID, methodID)

	return
}
