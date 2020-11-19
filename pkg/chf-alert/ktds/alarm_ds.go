package ktds

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"text/template"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/kentik/chf-alert/pkg/kt"
	"github.com/kentik/chf-alert/pkg/metric"

	"github.com/kentik/eggs/pkg/database"
	"github.com/kentik/eggs/pkg/logger"
	"github.com/kentik/eggs/pkg/preconditions"

	"github.com/jmoiron/sqlx"
	_ "github.com/kentik/go-mysql-fork/mysql" // Load mysql driver
)

type alarmDataSource struct {
	logger.ContextL

	db      *RoRwPairDBPair
	metrics metric.DBStore

	stmtGetAll              *sqlx.Stmt
	stmtMitigationToStart   *sqlx.Stmt
	stmtMitigationToStop    *sqlx.Stmt
	stmtMarkMitigationStart *sqlx.Stmt
	stmtMarkMitigationStop  *sqlx.Stmt

	stmtAssociateAlarmWithMitigation        *sqlx.NamedStmt
	stmtAssociateAlarmHistoryWithMitigation *sqlx.NamedStmt
	stmtGetTotalActiveAlarms                *sqlx.NamedStmt
	stmtGetTotalActiveShadowAlarms          *sqlx.NamedStmt
	stmtGetTotalActiveAlarmsIncludingShadow *sqlx.NamedStmt
	stmtGetTotalActiveAlarmsForCompany      *sqlx.NamedStmt

	stmtRemoveMachinesOld *sqlx.Stmt

	stmtGetAllCompanyInsights    *sqlx.NamedStmt
	stmtGetCompanyActiveInsight  *sqlx.Stmt
	stmtGetCompanyInsightHistory *sqlx.Stmt

	stmtGetAlarm                 *namedInStmt
	stmtGetAlarmFromHistory      *sqlx.Stmt
	stmtGetAlarmFromCurrent      *sqlx.Stmt
	stmtGetAlarmsForThresholds   *inStmt
	stmtFindAlarmsExample        *namedInStmt
	stmtGetAlarmStateTransitions *sqlx.Stmt
	stmtGetPolicyIDs             *namedInStmt
	stmtGetOccurrencesForKey     *sqlx.Stmt
	stmtGetAlarmsRelatedByKey    *sqlx.Stmt

	stmtAckAlarmCurrent *sqlx.Stmt
	stmtAckAlarmHistory *sqlx.NamedStmt
}

// NewAlarmDataSource constructs activator data source instance
func NewAlarmDataSource(db *RoRwPairDBPair, log logger.Underlying) (ads AlarmDataSource, retErr error) {
	defer func() {
		err := recover()
		if err != nil {
			retErr = fmt.Errorf("caught panic while initializing: %s", err)
		}
	}()

	ds := &alarmDataSource{
		ContextL: logger.NewContextLFromUnderlying(logger.SContext{S: "alarmDS"}, log),
		db:       db,
		metrics:  metric.NewDB("AlarmDS"),

		stmtGetAll:              prepxOrPanic(db.Rox, stmtGetAllSQL),
		stmtMitigationToStart:   prepxOrPanic(db.Rox, stmtMitigationToStartSQL),
		stmtMitigationToStop:    prepxOrPanic(db.Rox, stmtMitigationToStopSQL),
		stmtMarkMitigationStart: prepxOrPanic(db.Rwx, stmtMarkMitigationStartSQL),
		stmtMarkMitigationStop:  prepxOrPanic(db.Rwx, stmtMarkMitigationStopSQL),

		stmtAssociateAlarmWithMitigation:        prepnOrPanic(db.Rwx, stmtAssociateAlarmWithMitigationSQL),
		stmtAssociateAlarmHistoryWithMitigation: prepnOrPanic(db.Rwx, stmtAssociateAlarmHistoryWithMitigationSQL),
		stmtGetTotalActiveAlarms:                prepnOrPanic(db.Rox, stmtGetTotalActiveAlarmsSQL),
		stmtGetTotalActiveShadowAlarms:          prepnOrPanic(db.Rox, stmtGetTotalActiveShadowAlarmsSQL),
		stmtGetTotalActiveAlarmsIncludingShadow: prepnOrPanic(db.Rox, stmtGetTotalActiveAlarmsIncludingShadowSQL),
		stmtGetTotalActiveAlarmsForCompany:      prepnOrPanic(db.Rox, stmtGetTotalActiveAlarmsForCompanySQL),

		stmtRemoveMachinesOld: prepxOrPanic(db.Rwx, stmtRemoveMachinesOldSQL),

		stmtGetAllCompanyInsights:    prepnOrPanic(db.Rox, stmtGetAllCompanyInsightsSQLExample),
		stmtGetCompanyActiveInsight:  prepxOrPanic(db.Rox, stmtGetCompanyActiveInsightSQL),
		stmtGetCompanyInsightHistory: prepxOrPanic(db.Rox, stmtGetCompanyInsightHistorySQL),

		stmtGetAlarm:                 newNamedInStmt(db.Rox, stmtGetAlarmSQL),
		stmtGetAlarmFromHistory:      prepxOrPanic(db.Rox, stmtGetAlarmFromHistorySQL),
		stmtGetAlarmFromCurrent:      prepxOrPanic(db.Rox, stmtGetAlarmFromCurrentSQL),
		stmtFindAlarmsExample:        newNamedInStmt(db.Rox, stmtFindAlarmsSQLExample),
		stmtGetAlarmsForThresholds:   newInStmt(db.Rwx, stmtGetAlarmsForThresholdsSQL), // use leader here
		stmtGetAlarmStateTransitions: prepxOrPanic(db.Rox, stmtGetAlarmStateTransitionsSQL),
		stmtGetPolicyIDs:             newNamedInStmt(db.Rox, stmtGetPolicyIDs),
		stmtGetOccurrencesForKey:     prepxOrPanic(db.Rox, stmtGetOccurrencesForKeySQL),
		stmtGetAlarmsRelatedByKey:    prepxOrPanic(db.Rox, stmtGetAlarmsRelatedByKeySQL),

		stmtAckAlarmCurrent: prepxOrPanic(db.Rwx, stmtAckAlarmCurrentSQL),
		stmtAckAlarmHistory: prepnOrPanic(db.Rwx, stmtAckAlarmHistorySQL),
	}
	preconditions.ValidateStruct(ds, preconditions.NoNilPointers)
	return ds, nil
}

func (ds *alarmDataSource) Close() {
	if ds != nil {
		database.CloseStatements(ds)
	}
}

const selectAlarmBaseSQL = `
SELECT
	id,
	alarm_state,
	alert_id,
	coalesce(mitigation_id, 0) AS mitigation_id,
	company_id,
	threshold_id,
	alert_key,
	alert_dimension,
	alert_metric,
	alert_value,
	alert_value2nd,
	alert_value3rd,
	alert_match_count,
	alert_baseline,
	alert_severity,
	baseline_used,
	learning_mode,
	debug_mode,
	notify_start,
	notify_end,
	alarm_start,
	alarm_end,
	last_match_time
FROM mn_alert_alarm `

const stmtGetAllSQL = selectAlarmBaseSQL + `
WHERE company_id = ?
  AND alert_id = ?
  AND threshold_id = ?
`

func (ds *alarmDataSource) GetAllAlarms(cid kt.Cid, policyID kt.PolicyID, tid kt.Tid) (ret []*kt.AlarmEvent, err error) {
	preconditions.AssertNonNil(ds, "ds")

	defer func(start time.Time) { ds.metrics.Get("GetAllAlarms", cid).Done(start, err) }(time.Now())

	ret, err = ds.queryAlarmEvents(ds.stmtGetAll, cid, policyID, tid)

	return
}

const stmtGetAlarmsForThresholdsSQL = selectAlarmBaseSQL + `
WHERE company_id = ?
  AND alert_id IN (?)
  AND threshold_id IN (?)
`

func (ds *alarmDataSource) GetAlarmsForThresholds(ctx context.Context, cid kt.Cid, policyIDs []kt.PolicyID, tids []kt.Tid) (ret []*kt.AlarmEvent, err error) {
	if len(tids) == 0 {
		return
	}

	if len(policyIDs) == 0 {
		return
	}

	defer func(start time.Time) { ds.metrics.Get("GetAlarmsForThresholds", cid).Done(start, err) }(time.Now())

	err = ds.stmtGetAlarmsForThresholds.SelectContext(ctx, &ret, cid, policyIDs, tids)
	return
}

func (ds *alarmDataSource) queryAlarmEvents(stmt *sqlx.Stmt, cid kt.Cid, policyID kt.PolicyID, tid kt.Tid) ([]*kt.AlarmEvent, error) {
	preconditions.AssertNonNil(ds, "ds")
	rows, err := stmt.Query(cid, policyID, tid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []*kt.AlarmEvent{}
	for rows.Next() {
		var ae kt.AlarmEvent
		if err := rows.Scan(&ae.AlarmID, &ae.AlarmState, &ae.PolicyID, &ae.MitigationID, &ae.CompanyID, &ae.ThresholdID, &ae.AlertKey, &ae.AlertDimension, &ae.AlertMetric, &ae.AlertValue, &ae.AlertValueSecond, &ae.AlertValueThird, &ae.AlertActivateCount, &ae.AlertBaseline, &ae.ActivateSeverity, &ae.BaselineUsedFallback, &ae.LearningMode, &ae.DebugMode, &ae.NotifyStart, &ae.NotifyEnd, &ae.AlarmStart, &ae.AlarmEnd, &ae.LastActivate); err != nil {
			break
		}
		ae.AlertKey = kt.AlertKeyFixupForGeoNulBytes(ae.AlertKey)
		events = append(events, &ae)
	}

	return events, rows.Err()
}

const stmtMitigationToStartSQL = selectAlarmBaseSQL + `
WHERE company_id = ?
  AND alert_id = ?
  AND threshold_id = ?
  AND notify_start != '0000-00-00 00:00:00'
  AND alarm_state = 'ALARM'
  AND mitigate_start = '0000-00-00 00:00:00'
`

func (ds *alarmDataSource) GetAlarmsForMitigationStart(cid kt.Cid, policyID kt.PolicyID, tid kt.Tid) (ret []*kt.AlarmEvent, err error) {
	preconditions.AssertNonNil(ds, "ds")

	defer func(start time.Time) { ds.metrics.Get("GetAlarmsForMitigationStart", cid).Done(start, err) }(time.Now())

	ret, err = ds.queryAlarmEvents(ds.stmtMitigationToStart, cid, policyID, tid)

	return
}

const stmtMitigationToStopSQL = selectAlarmBaseSQL + `
WHERE company_id = ?
  AND alert_id = ?
  AND threshold_id = ?
  AND notify_end != '0000-00-00 00:00:00'
  AND (alarm_state = 'ACK_REQ' OR alarm_state = 'CLEAR')
  AND mitigate_start != '0000-00-00 00:00:00'
  AND mitigate_end = '0000-00-00 00:00:00'
`

func (ds *alarmDataSource) GetAlarmsForMitigationStop(cid kt.Cid, policyID kt.PolicyID, tid kt.Tid) (ret []*kt.AlarmEvent, err error) {
	preconditions.AssertNonNil(ds, "ds")

	defer func(start time.Time) { ds.metrics.Get("GetAlarmsForMitigationStop", cid).Done(start, err) }(time.Now())

	ret, err = ds.queryAlarmEvents(ds.stmtMitigationToStop, cid, policyID, tid)

	return
}

const stmtMarkMitigationStartSQL = `
UPDATE mn_alert_alarm
SET mitigate_start = NOW()
WHERE id = ?
  AND mitigate_start = '0000-00-00 00:00:00'
`

func (ds *alarmDataSource) MarkMitigationStart(alarmID kt.AlarmID) (err error) {
	preconditions.AssertNonNil(ds, "ds")

	defer func(start time.Time) { ds.metrics.Get("MarkMitigationStart", 0).Done(start, err) }(time.Now())

	_, err = ds.stmtMarkMitigationStart.Exec(alarmID)

	return
}

const stmtMarkMitigationStopSQL = `
UPDATE mn_alert_alarm
SET mitigate_end = NOW()
WHERE id = ?
  AND mitigate_end = '0000-00-00 00:00:00'
`

func (ds *alarmDataSource) MarkMitigationStop(alarmID kt.AlarmID) (err error) {
	preconditions.AssertNonNil(ds, "ds")

	defer func(start time.Time) { ds.metrics.Get("MarkMitigationStop", 0).Done(start, err) }(time.Now())

	_, err = ds.stmtMarkMitigationStop.Exec(alarmID)

	return err
}

const stmtAssociateAlarmWithMitigationSQL = `
	UPDATE mn_alert_alarm
	SET mitigation_id = :mitigation_id
	WHERE id = :alarm_id AND company_id = :company_id
`

const stmtAssociateAlarmHistoryWithMitigationSQL = `
	UPDATE mn_alert_alarm_history
	SET mitigation_id = :mitigation_id
	WHERE alarm_id = :alarm_id AND company_id = :company_id
`

// AssociateAlarmWithMitigation sets the alarm's mitigation_id.
func (ds *alarmDataSource) AssociateAlarmWithMitigation(companyID kt.Cid, alarmID kt.AlarmID, mitigationID kt.MitigationID) (err error) {
	preconditions.AssertNonNil(ds, "ds")

	defer func(start time.Time) { ds.metrics.Get("AssociateAlarmWithMitigation", companyID).Done(start, err) }(time.Now())

	tx, err := ds.db.Rwx.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck

	args := map[string]interface{}{
		"company_id":    companyID,
		"alarm_id":      alarmID,
		"mitigation_id": mitigationID,
	}
	_, err = tx.NamedStmt(ds.stmtAssociateAlarmWithMitigation).Exec(args)
	if err != nil {
		return err
	}
	_, err = tx.NamedStmt(ds.stmtAssociateAlarmHistoryWithMitigation).Exec(args)
	if err != nil {
		return err
	}
	return tx.Commit()
}

const stmtGetTotalActiveAlarmsSQL = `
	SELECT COUNT(*) FROM mn_alert_alarm
	WHERE alarm_state = 'ALARM'
		AND company_id < :company_id_shadow_cutoff
`

func (ds *alarmDataSource) GetTotalActiveAlarms() (cnt int, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetTotalActiveAlarms", 0).Done(start, err) }(time.Now())

	cnt, err = getCountN(ds.stmtGetTotalActiveAlarms, &struct {
		CompanyIDShadowCutoff int `db:"company_id_shadow_cutoff"`
	}{
		CompanyIDShadowCutoff: kt.ShadowPolicyBitMarker,
	})

	return
}

const stmtGetTotalActiveShadowAlarmsSQL = `
	SELECT COUNT(*) FROM mn_alert_alarm
	WHERE alarm_state = 'ALARM'
		AND company_id >= :company_id_shadow_cutoff
`

func (ds *alarmDataSource) GetTotalActiveShadowAlarms() (cnt int, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetTotalActiveShadowAlarms", 0).Done(start, err) }(time.Now())

	cnt, err = getCountN(ds.stmtGetTotalActiveShadowAlarms, &struct {
		CompanyIDShadowCutoff int `db:"company_id_shadow_cutoff"`
	}{
		CompanyIDShadowCutoff: kt.ShadowPolicyBitMarker,
	})

	return
}

const stmtGetTotalActiveAlarmsIncludingShadowSQL = `
	SELECT COUNT(*) FROM mn_alert_alarm
	WHERE alarm_state = 'ALARM'
`

func (ds *alarmDataSource) GetTotalActiveAlarmsIncludingShadow() (cnt int, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetTotalActiveAlarmsIncludingShadow", 0).Done(start, err) }(time.Now())

	cnt, err = getCountN(ds.stmtGetTotalActiveAlarmsIncludingShadow, struct{}{})

	return
}

const stmtGetTotalActiveAlarmsForCompanySQL = `
	SELECT COUNT(*) FROM mn_alert_alarm
	WHERE alarm_state = 'ALARM' AND company_id = :company_id
`

func (ds *alarmDataSource) GetTotalActiveAlarmsForCompany(cid kt.Cid) (count int, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetTotalActiveAlarmsForCompany", cid).Done(start, err) }(time.Now())

	err = ds.stmtGetTotalActiveAlarmsForCompany.Get(&count, qCid{cid})

	return
}

// RemoveAlarmMatchHistoricalEntries clears entries older than retention period from mn_alert_match_history
func (ds *alarmDataSource) RemoveAlarmMatchHistoricalEntries(retention time.Duration, rowsThreshold uint64) (cnt uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("RemoveAlarmMatchHistoricalEntries", 0).Done(start, err) }(time.Now())

	cnt, err = ds.fancyDelete("mn_alert_match_history", "id", "ctime", retention, rowsThreshold, "")

	return
}

// RemoveOldClearedAlarms clears entries older than retention period from mn_alert_alarm
func (ds *alarmDataSource) RemoveClearedAlarmOldEntries(retention time.Duration, rowsThreshold uint64) (cnt uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("RemoveClearedAlarmOldEntries", 0).Done(start, err) }(time.Now())

	cnt, err = ds.fancyDelete("mn_alert_alarm", "id", "alarm_end", retention, rowsThreshold,
		"alarm_state = 'CLEAR' AND alarm_end != '0000-00-00 00:00:00'")

	return
}

// RemoveAlarmHistoryOldEntries clears entries older than retention period from mn_alert_alarm_history
func (ds *alarmDataSource) RemoveAlarmHistoryOldEntries(retention time.Duration, rowsThreshold uint64) (cnt uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("RemoveAlarmHistoryOldEntries", 0).Done(start, err) }(time.Now())

	cnt, err = ds.fancyDelete("mn_alert_alarm_history", "id", "ctime", retention, rowsThreshold, "")

	return
}

// RemoveBaselineOldEntries clears entries older than retention period from mn_alert_baseline
func (ds *alarmDataSource) RemoveBaselineOldEntries(retention time.Duration, rowsThreshold uint64) (cnt uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("RemoveBaselineOldEntries", 0).Done(start, err) }(time.Now())

	cnt, err = ds.fancyDelete("mn_alert_baseline", "id", "start_time", retention, rowsThreshold, "")

	// TODO(stefan): remove this once we're fully off mn_alert_long_set in all environments
	ds.fancyDelete("mn_alert_long_set", "id", "ctime", retention, rowsThreshold, "")

	return
}

// RemoveBaselineOldEntries clears entries older than retention period from mn_alert_baseline
func (ds *alarmDataSource) RemoveBaselineBackfillJobsOldEntries(retention time.Duration, rowsThreshold uint64) (cnt uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("RemoveBaselineBackfillJobsOldEntries", 0).Done(start, err) }(time.Now())

	cnt, err = ds.fancyDelete("mn_alert_baseline_job", "id", "ctime", retention, rowsThreshold, "")

	return
}

// RemoveMachineLogOldEntries clears entries older than retention period from mn_mit2_machines_log
func (ds *alarmDataSource) RemoveMachineLogOldEntries(retention time.Duration, rowsThreshold uint64) (cnt uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("RemoveMachineLogOldEntries", 0).Done(start, err) }(time.Now())

	cnt, err = ds.fancyDelete("mn_mit2_machines_log", "id", "stamp", retention, rowsThreshold, "")

	return
}

const stmtRemoveMachinesOldSQL = `
	DELETE FROM mn_mit2_machines
	WHERE edate < now() - INTERVAL ? SECOND
	LIMIT ?
`

// RemoveMachinesOldEntries clears entries older than retention period from mn_mit2_machines
func (ds *alarmDataSource) RemoveMachinesOldEntries(retention time.Duration, rowsThreshold uint64) (cnt uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("RemoveMachinesOldEntries", 0).Done(start, err) }(time.Now())

	// can't use genericDelete b/c mn_mit2_machines has no id field
	cnt, err = execDelete(ds.stmtRemoveMachinesOld, retention, rowsThreshold)

	return
}

// Note in the SQL below that literal colons are double-escaped.
const whereActiveInsightsSQLTemplateString = `
WHERE (company_id = :shadow_company_id OR company_id = :original_company_id OR :original_company_id = 0)
	AND ((NOT :do_alarm_ids) OR id IN (:alarm_ids))
	AND ((NOT :do_policy_ids) OR alert_id IN (:policy_ids))
	AND ((NOT :do_ignore_policy_ids) OR alert_id NOT IN (:ignore_policy_ids))
	AND ((NOT :do_threshold_ids) OR threshold_id IN (:threshold_ids))
	AND ((NOT :do_mitigation_ids) OR mitigation_id IN (:mitigation_ids))
	AND ((NOT :do_states) OR alarm_state IN (:states))
{{if (gt (len .DimensionFiltersDNFArray) 0)}}
	AND (
	{{range $ix, $val := .DimensionFiltersDNFArray}}
	{{if (gt $ix 0)}} OR {{end}}
		(
		{{range $cix, $cval := $val}}
		{{if (gt $cix 0)}} AND {{end}}
		(substring_index(substring_index(alert_key, '::',
				find_in_set(:dnf_dimension_{{$ix}}_{{$cix}}, regexp_replace(alert_dimension, '::', ','))
			), '::', -1) = :dnf_dimension_value_{{$ix}}_{{$cix}})
		{{end}}
		)
	{{end}}
	)
{{end}}
	AND alarm_state = 'ALARM'
	AND :include_active
ORDER BY id DESC `

// Matching columns with selectAlarmBaseSQL so we can UNION.
const selectHistoricalInsightsSQL = `
SELECT
	coalesce(alarm_id, 0) AS id,
	'CLEAR' AS alarm_state,
	coalesce(alert_id, 0) AS alert_id,
	coalesce(mitigation_id, 0) AS mitigation_id,
	coalesce(company_id, 0) AS company_id,
	coalesce(threshold_id, 0) AS threshold_id,
	coalesce(alert_key, "") AS alert_key,
	coalesce(alert_dimension, "") AS alert_dimension,
	coalesce(alert_metric, "") AS alert_metric,
	coalesce(alert_value, 0) AS alert_value,
	coalesce(alert_value2nd, 0) AS alert_value2nd,
	coalesce(alert_value3rd, 0) AS alert_value3rd,
	coalesce(alert_match_count, 0) AS alert_match_count,
	coalesce(alert_baseline, 0) AS alert_baseline,
	coalesce(alert_severity, "") AS alert_severity,
	coalesce(baseline_used, 0) AS baseline_used,
	coalesce(learning_mode, 0) AS learning_mode,
	coalesce(debug_mode, 0) AS debug_mode,
	STR_TO_DATE('0000-00-00 000000', '%Y-%m-%d %H%i%s') AS notify_start,
	STR_TO_DATE('0000-00-00 000000', '%Y-%m-%d %H%i%s') AS notify_end,
	alarm_start_time AS alarm_start,
	ctime AS alarm_end,
	STR_TO_DATE('0000-00-00 000000', '%Y-%m-%d %H%i%s') AS last_match_time
FROM mn_alert_alarm_history `

// Note in the SQL below that literal colons are double-escaped.
const whereHistoricalInsightsSQLTemplateString = `
WHERE (company_id = :shadow_company_id OR company_id = :original_company_id OR :original_company_id = 0)
	AND ((NOT :do_alarm_ids) OR alarm_id IN (:alarm_ids))
	AND ((NOT :do_policy_ids) OR alert_id IN (:policy_ids))
	AND ((NOT :do_ignore_policy_ids) OR alert_id NOT IN (:ignore_policy_ids))
	AND ((NOT :do_threshold_ids) OR threshold_id IN (:threshold_ids))
	AND ((NOT :do_mitigation_ids) OR mitigation_id IN (:mitigation_ids))
	AND ((NOT :do_states) OR new_alarm_state IN (:states))
{{if (gt (len .DimensionFiltersDNFArray) 0)}}
	AND (
	{{range $ix, $val := .DimensionFiltersDNFArray}}
	{{if (gt $ix 0)}} OR {{end}}
		(
		{{range $cix, $cval := $val}}
		{{if (gt $cix 0)}} AND {{end}}
		(substring_index(substring_index(alert_key, '::',
				find_in_set(:dnf_dimension_{{$ix}}_{{$cix}}, regexp_replace(alert_dimension, '::', ','))
			), '::', -1) = :dnf_dimension_value_{{$ix}}_{{$cix}})
		{{end}}
		)
	{{end}}
	)
{{end}}
	AND alarm_start_time >= :start
	AND ctime < :end
	AND old_alarm_state = 'ALARM'
	AND new_alarm_state IN ('CLEAR', 'ACK_REQ')
	AND :include_history
ORDER BY id DESC `

const stmtGetAllCompanyInsightsSQLTemplateString = `
SELECT * FROM (
(` + selectAlarmBaseSQL + whereActiveInsightsSQLTemplateString + `)
UNION
(` + selectHistoricalInsightsSQL + whereHistoricalInsightsSQLTemplateString + `)
) sq
ORDER BY id DESC
LIMIT :limit
OFFSET :offset`

var stmtGetAllCompanyInsightsSQLTemplate = template.Must(template.New("").Parse(stmtGetAllCompanyInsightsSQLTemplateString))

var stmtGetAllCompanyInsightsSQLExample = mustRender(stmtGetAllCompanyInsightsSQLTemplate, &kt.FetchCompanyInsightsFilter{
	DimensionFiltersDNFArray: [][]kt.StringPair{
		{{}, {}},
		{{}, {}},
	},
})

func (ds *alarmDataSource) FetchCompanyInsights(filter *kt.FetchCompanyInsightsFilter) ([]*kt.AlarmEvent, error) {
	// If parameters are the zero value, don't filter on them.
	originalCid := kt.CompanyIDWithShadowBit(filter.Cid, false)
	if filter.ExcludeUserDefined {
		originalCid = -1
	}

	shadowCid := filter.Cid
	if shadowCid != 0 {
		shadowCid = kt.CompanyIDWithShadowBit(shadowCid, true)
	}

	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchCompanyInsights", filter.Cid).Done(start, err) }(time.Now())

	alarmIDs := filter.AlarmIDs
	if len(alarmIDs) == 0 {
		alarmIDs = []kt.AlarmID{-1}
	}
	policyIDs := filter.PolicyIDs
	if len(policyIDs) == 0 {
		policyIDs = []kt.PolicyID{-1}
	}
	ignorePolicyIDs := filter.IgnorePolicyIDs
	if len(ignorePolicyIDs) == 0 {
		ignorePolicyIDs = []kt.PolicyID{-1}
	}
	thresholdIDs := filter.ThresholdIDs
	if len(thresholdIDs) == 0 {
		thresholdIDs = []kt.Tid{-1}
	}
	mitigationIDs := filter.MitigationIDs
	if len(mitigationIDs) == 0 {
		mitigationIDs = []kt.MitigationID{-1}
	}
	states := filter.States
	if len(states) == 0 {
		states = []string{"invalid"}
	}
	filter.DimensionFiltersDNFArray = make([][]kt.StringPair, 0, len(filter.DimensionFiltersDNF))
	for _, conj := range filter.DimensionFiltersDNF {
		filter.DimensionFiltersDNFArray = append(
			filter.DimensionFiltersDNFArray,
			kt.StringMapToArray(conj))
	}

	sql, err := render(stmtGetAllCompanyInsightsSQLTemplate, filter)
	if err != nil {
		return nil, err
	}

	args := map[string]interface{}{
		"original_company_id":  originalCid,
		"shadow_company_id":    shadowCid,
		"do_alarm_ids":         filter.DoAlarmIDs,
		"alarm_ids":            alarmIDs,
		"do_policy_ids":        filter.DoPolicyIDs,
		"policy_ids":           policyIDs,
		"do_ignore_policy_ids": filter.DoIgnorePolicyIDs,
		"ignore_policy_ids":    ignorePolicyIDs,
		"do_threshold_ids":     filter.DoThresholdIDs,
		"threshold_ids":        thresholdIDs,
		"do_mitigation_ids":    filter.DoMitigationIDs,
		"mitigation_ids":       mitigationIDs,
		"do_states":            filter.DoStates,
		"states":               states,
		"include_active":       filter.IncludeActive,
		"include_history":      filter.IncludeHistory,
		"start":                filter.Start,
		"end":                  filter.End,
		"limit":                filter.Limit,
		"offset":               filter.Offset,
	}
	for i, conj := range filter.DimensionFiltersDNFArray {
		for ci, sp := range conj {
			args[fmt.Sprintf("dnf_dimension_%d_%d", i, ci)] = sp.Key
			args[fmt.Sprintf("dnf_dimension_value_%d_%d", i, ci)] = sp.Value
		}
	}

	var alarms []*kt.AlarmEvent
	err = namedInRebindSelect(ds.db.Rox, &alarms, sql, args)
	if err != nil {
		return nil, err
	}
	for i := range alarms {
		alarms[i].CompanyID = kt.CompanyIDWithShadowBit(alarms[i].CompanyID, false)
	}
	return alarms, nil
}

const stmtGetCompanyInsightHistorySQL = selectHistoricalInsightsSQL + `
WHERE (company_id = ? OR company_id = ? OR 0 = ?)
	AND alarm_id = ?
	AND old_alarm_state = 'ALARM'
	AND new_alarm_state = 'CLEAR'
ORDER BY id DESC
`

const stmtGetCompanyActiveInsightSQL = selectAlarmBaseSQL + `
WHERE (company_id = ? OR company_id = ? OR 0 = ?)
	AND id = ?
	AND alarm_state = 'ALARM'
ORDER BY id DESC
`

func (ds *alarmDataSource) GetCompanyInsightByID(originalCid kt.Cid, insightID kt.AlarmID, excludeUserDefined bool) (*kt.AlarmEvent, error) {
	mt := ds.metrics.Get("stmtGetCompanyInsightHistory", originalCid)
	start := time.Now()

	cidWithShadow := kt.CompanyIDWithShadowBit(originalCid, true)
	cidNoShadow := kt.CompanyIDWithShadowBit(originalCid, false)
	if excludeUserDefined {
		cidNoShadow = -1
	}

	var alarms []*kt.AlarmEvent
	err := ds.stmtGetCompanyInsightHistory.Select(
		&alarms,
		cidWithShadow,
		cidNoShadow,
		originalCid,
		insightID)
	if err != nil {
		mt.Error(start)
		return nil, err
	}
	if len(alarms) >= 1 {
		// We found it in history.
		alarms[0].CompanyID = kt.CompanyIDWithShadowBit(alarms[0].CompanyID, false)
		return alarms[0], nil
	}
	mt.Success(start)

	defer func(start time.Time) { ds.metrics.Get("stmtGetCompanyActiveInsight", originalCid).Done(start, err) }(time.Now())

	var activeAlarms []*kt.AlarmEvent
	err = ds.stmtGetCompanyActiveInsight.Select(
		&activeAlarms,
		cidWithShadow,
		cidNoShadow,
		originalCid,
		insightID)
	if err != nil {
		return nil, err
	}

	if len(activeAlarms) >= 1 {
		// We found it in active alarms.
		activeAlarms[0].CompanyID = kt.CompanyIDWithShadowBit(activeAlarms[0].CompanyID, false)
		return activeAlarms[0], nil
	}

	// We didn't find it.
	return nil, nil
}

const stmtFindAlarmsInHistorySelectSQL = `
SELECT
	coalesce(alarm_id, 0) AS id,
	coalesce(company_id, 0) AS company_id,
	coalesce(alert_id, 0) AS alert_id,
	coalesce(threshold_id, 0) AS threshold_id,
	coalesce(mitigation_id, 0) AS mitigation_id,
	new_alarm_state AS alarm_state,
	coalesce(alert_dimension, '') AS alert_dimension,
	coalesce(alert_key, '') AS alert_key,
	coalesce(alert_metric, '') AS alert_metric,
	coalesce(alert_value, 0) AS alert_value,
	coalesce(alert_value2nd, 0) AS alert_value2nd,
	coalesce(alert_value3rd, 0) AS alert_value3rd,
	coalesce(alert_baseline, 0) AS alert_baseline,
	coalesce(alert_match_count, 0) AS alert_match_count,
	coalesce(alert_severity, '') AS alert_severity,
	coalesce(baseline_used, 0) AS baseline_used,
	alarm_start_time AS alarm_start,
	ctime AS alarm_end
`

const stmtFindAlarmsInHistorySQLTemplateString = stmtFindAlarmsInHistorySelectSQL + `
`

const stmtFindAlarmsSQLTemplateString = `
SELECT
	id,
	company_id,
	alert_id,
	threshold_id,
	mitigation_id,
	alarm_state,
	alert_dimension,
	alert_key,
	alert_metric,
	alert_value,
	alert_value2nd,
	alert_value3rd,
	alert_baseline,
	alert_severity,
	baseline_used,
	alarm_start,
	alarm_end
FROM (
` + stmtFindAlarmsInHistorySelectSQL + `
FROM mn_alert_alarm_history
WHERE id IN (
	SELECT max(id) AS id
	FROM mn_alert_alarm_history
	WHERE
		(company_id IN (:company_ids) OR 0 IN (:company_ids))
		AND (:any_create_ts OR (alarm_start_time BETWEEN :creation_start AND :creation_end))
		AND (:any_start_ts OR (alarm_start_time BETWEEN :start_start AND :start_end))
		AND (:any_end_ts OR (old_alarm_state = 'ALARM' AND new_alarm_state = 'CLEAR' AND ctime BETWEEN :end_start AND :end_end))
		AND (:any_deprecated_ts OR
			(COALESCE(:deprecated_start, ctime) <= ctime AND COALESCE(:deprecated_end, alarm_start_time) >= alarm_start_time)
		)
		AND (alarm_id IN (:alarm_ids) OR :all_alarms)
		AND (mitigation_id IN (:mitigation_ids) OR :all_mitigations)
		AND (alert_id IN (:policy_ids) OR :all_policies)
		AND (alert_id NOT IN (:ignore_policy_ids) OR (NOT :use_ignore_policies))
	{{if (gt (len .DimensionFiltersDNFArray) 0)}}
		AND (
		{{range $ix, $val := .DimensionFiltersDNFArray}}
		{{if (gt $ix 0)}} OR {{end}}
			(
			{{range $cix, $cval := $val}}
			{{if (gt $cix 0)}} AND {{end}}
			(substring_index(substring_index(alert_key, '::',
					find_in_set(:dnf_dimension_{{$ix}}_{{$cix}}, regexp_replace(alert_dimension, '::', ','))
				), '::', -1) = :dnf_dimension_value_{{$ix}}_{{$cix}})
			{{end}}
			)
		{{end}}
		)
	{{end}}
	GROUP BY alarm_id
)
) sq
WHERE
	(:all_states OR alarm_state IN (:alarm_states))
	AND (:all_severities OR alert_severity IN (:alarm_severities))
ORDER BY alarm_start DESC
`

var stmtFindAlarmsSQLTemplate = template.Must(template.New("").Parse(stmtFindAlarmsSQLTemplateString))

type findAlarmsTemplateData struct {
	DimensionFiltersDNFArray [][]kt.StringPair
}

var stmtFindAlarmsSQLExample = mustRender(stmtFindAlarmsSQLTemplate, findAlarmsTemplateData{
	DimensionFiltersDNFArray: [][]kt.StringPair{
		{{}, {}},
		{{}, {}},
	},
})

func (ds *alarmDataSource) FindAlarms(
	ctx context.Context,
	filter *kt.GetAlarmsFilter,
) (alarms []*kt.AlarmEvent, err error) {
	// FIXME: at this time this query isn't reported automatically (by the instrumented sql driver)
	// so do this manually (also see below)
	var span opentracing.Span
	if opentracing.IsGlobalTracerRegistered() {
		span, ctx = opentracing.StartSpanFromContext(ctx, "alarmDataSource.FindAlarms")
		defer span.Finish()
	}

	defer func(start time.Time) { ds.metrics.Get("FindAlarms", filter.CompanyID).Done(start, err) }(time.Now())

	var companyIDs []kt.Cid
	if !filter.ExcludeUserDefined {
		companyIDs = append(companyIDs, filter.CompanyID)
	}
	if filter.IncludeInsightAlarms {
		companyIDs = append(companyIDs, kt.CompanyIDWithShadowBit(filter.CompanyID, true))
	}
	if len(companyIDs) == 0 {
		companyIDs = []kt.Cid{-1}
	}

	allStates := false
	if len(filter.States) == 0 {
		filter.States, allStates = []string{"invalid"}, true
	}

	allSeverities := false
	if len(filter.Severities) == 0 {
		filter.Severities, allSeverities = []string{"invalid"}, true
	}

	allAlarms := false
	if len(filter.Alarms) == 0 {
		filter.Alarms, allAlarms = []kt.AlarmID{kt.AlarmID(-1)}, true
	}

	allMits := false
	if len(filter.MitigationIDs) == 0 {
		// In query can't handle empty list, thus imposible value is added
		filter.MitigationIDs, allMits = []kt.MitigationID{kt.MitigationID(-1)}, true
	}

	allPolicies := false
	if len(filter.PolicyIDs) == 0 {
		filter.PolicyIDs, allPolicies = []kt.PolicyID{kt.PolicyID(-1)}, true
	}

	useIgnorePolicies := true
	if len(filter.IgnorePolicyIDs) == 0 {
		filter.IgnorePolicyIDs, useIgnorePolicies = []kt.PolicyID{kt.PolicyID(-1)}, false
	}

	dimensionFiltersDNFArray := make([][]kt.StringPair, 0, len(filter.DimensionFiltersDNF))
	for _, conj := range filter.DimensionFiltersDNF {
		dimensionFiltersDNFArray = append(
			dimensionFiltersDNFArray,
			kt.StringMapToArray(conj))
	}

	sql, err := render(stmtFindAlarmsSQLTemplate, findAlarmsTemplateData{
		DimensionFiltersDNFArray: dimensionFiltersDNFArray,
	})
	if err != nil {
		return nil, err
	}

	args := map[string]interface{}{
		"company_ids":      companyIDs,
		"alarm_ids":        filter.Alarms,
		"all_alarms":       allAlarms,
		"all_states":       allStates,
		"alarm_states":     filter.States,
		"all_severities":   allSeverities,
		"alarm_severities": filter.Severities,

		"any_create_ts":     filter.CreationRange.IsZero(),
		"creation_start":    filter.CreationRange.GetStart(),
		"creation_end":      filter.CreationRange.GetEnd(),
		"any_start_ts":      filter.StartRange.IsZero(),
		"start_start":       filter.StartRange.GetStart(),
		"start_end":         filter.StartRange.GetEnd(),
		"any_end_ts":        filter.EndRange.IsZero(),
		"end_start":         filter.EndRange.GetStart(),
		"end_end":           filter.EndRange.GetEnd(),
		"any_deprecated_ts": filter.DeprecatedStart.IsZero() && filter.DeprecatedEnd.IsZero(),
		"deprecated_start":  filter.DeprecatedStart,
		"deprecated_end":    filter.DeprecatedEnd,

		"mitigation_ids":      filter.MitigationIDs,
		"all_mitigations":     allMits,
		"policy_ids":          filter.PolicyIDs,
		"all_policies":        allPolicies,
		"ignore_policy_ids":   filter.IgnorePolicyIDs,
		"use_ignore_policies": useIgnorePolicies,
	}
	for i, conj := range dimensionFiltersDNFArray {
		for ci, sp := range conj {
			args[fmt.Sprintf("dnf_dimension_%d_%d", i, ci)] = sp.Key
			args[fmt.Sprintf("dnf_dimension_value_%d_%d", i, ci)] = sp.Value
		}
	}

	// FIXME: at this time this query isn't reported automatically (by the instrumented sql driver)
	// so do this manually (also see above)
	if opentracing.IsGlobalTracerRegistered() {
		span.SetTag("sql", sql)
		span.SetTag("args", args)
	}

	err = namedInRebindSelect(ds.db.Rox, &alarms, sql, args)
	if err != nil {
		return nil, err
	}
	for i := range alarms {
		alarms[i].IsShadowPolicy = kt.CompanyIDHasShadowBit(alarms[i].CompanyID)
		alarms[i].CompanyID = kt.CompanyIDWithShadowBit(alarms[i].CompanyID, false)
	}
	return alarms, nil
}

const stmtFindAlarmsSelectSQL = `
SELECT
	coalesce(id, 0) AS id,
	coalesce(company_id, 0) AS company_id,
	coalesce(alert_id, 0) AS alert_id,
	coalesce(threshold_id, 0) AS threshold_id,
	coalesce(mitigation_id, 0) AS mitigation_id,
	coalesce(alarm_state, 'CLEAR') AS alarm_state,
	coalesce(alert_dimension, '') AS alert_dimension,
	coalesce(alert_key, '') AS alert_key,
	coalesce(alert_metric, '') AS alert_metric,
	coalesce(alert_value, 0) AS alert_value,
	coalesce(alert_value2nd, 0) AS alert_value2nd,
	coalesce(alert_value3rd, 0) AS alert_value3rd,
	coalesce(alert_baseline, 0) AS alert_baseline,
	coalesce(alert_match_count, 0) AS alert_match_count,
	coalesce(alert_severity, '') AS alert_severity,
	coalesce(baseline_used, 0) AS baseline_used,
	alarm_start,
	alarm_end
`

const stmtGetAlarmSQL = `
(
` + stmtFindAlarmsSelectSQL + `
FROM mn_alert_alarm
WHERE company_id = :company_id AND id = :alarm_id
)
UNION ALL
(
` + stmtFindAlarmsInHistorySelectSQL + `
FROM mn_alert_alarm_history
WHERE company_id = :company_id AND alarm_id = :alarm_id
ORDER BY id DESC
)
LIMIT 1`

// GetAlarm returns single alarm
func (ds *alarmDataSource) GetAlarm(
	ctx context.Context,
	companyID kt.Cid,
	alarmID kt.AlarmID,
) (*kt.AlarmEvent, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetAlarm", companyID).Done(start, err) }(time.Now())

	result := &kt.AlarmEvent{}
	err = ds.stmtGetAlarm.GetContext(ctx, result, map[string]interface{}{
		"company_id": companyID,
		"alarm_id":   alarmID,
	})

	return result, err
}

const stmtGetAlarmFromCurrentSQL = stmtFindAlarmsSelectSQL + `
FROM mn_alert_alarm
WHERE company_id = ? AND id = ?
`

func (ds *alarmDataSource) GetAlarmFromCurrent(
	ctx context.Context,
	companyID kt.Cid,
	alarmID kt.AlarmID,
) (*kt.AlarmEvent, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetAlarmFromCurrent", companyID).Done(start, err) }(time.Now())

	row := ds.stmtGetAlarmFromCurrent.QueryRowx(companyID, alarmID)
	if row == nil {
		err = ErrNilRow
		return nil, err
	}

	event := &kt.AlarmEvent{}
	if err = row.StructScan(event); err != nil {
		return nil, err
	}

	return event, nil
}

const stmtGetAlarmFromHistorySQL = stmtFindAlarmsInHistorySelectSQL + `
FROM mn_alert_alarm_history
WHERE company_id = ? AND alarm_id = ?
ORDER BY id DESC
LIMIT 1
`

func (ds *alarmDataSource) GetAlarmFromHistory(
	ctx context.Context,
	companyID kt.Cid,
	alarmID kt.AlarmID,
) (*kt.AlarmEvent, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetAlarmFromHistory", companyID).Done(start, err) }(time.Now())

	row := ds.stmtGetAlarmFromHistory.QueryRowx(companyID, alarmID)
	if row == nil {
		err = ErrNilRow
		return nil, err
	}

	event := &kt.AlarmEvent{}
	if err = row.StructScan(event); err != nil {
		return nil, err
	}

	return event, nil
}

// TODO(tjonak): mn_alert_alarm_history.alarm_history_type should have event FINISH instead of null

const stmtGetAlarmStateTransitionsSQL = `
SELECT
	new_alarm_state,
	ctime,
	COALESCE(alarm_history_type,'') AS alarm_history_type,
	alert_severity,
	threshold_id
FROM mn_alert_alarm_history
WHERE company_id = ? AND alarm_id = ?
ORDER BY ctime DESC`

// GetAlarmStateTransitions returns list of state transitions for given alarm, sorted by time descending
func (ds *alarmDataSource) GetAlarmStateTransitions(
	ctx context.Context,
	cid kt.Cid,
	alarmID kt.AlarmID,
) ([]*kt.AlarmStateTransition, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetAlarmStateTransitions", cid).Done(start, err) }(time.Now())

	result := []*kt.AlarmStateTransition{}
	err = ds.stmtGetAlarmStateTransitions.SelectContext(ctx, &result, cid, alarmID)

	return result, err
}

const stmtGetPolicyIDs = `
SELECT DISTINCT alert_id FROM mn_alert_alarm_history
WHERE alarm_id IN (:alarm_ids) AND company_id = :company_id
`

func (ds *alarmDataSource) FindAssociatedPolicies(
	ctx context.Context,
	companyID kt.Cid,
	alarmIDs []kt.AlarmID,
) (policyIDs []kt.PolicyID, err error) {
	if len(alarmIDs) == 0 {
		return nil, nil
	}

	defer func(start time.Time) { ds.metrics.Get("FindAssociatedPolicies", companyID).Done(start, err) }(time.Now())

	err = ds.stmtGetPolicyIDs.SelectContext(ctx, &policyIDs, map[string]interface{}{
		"alarm_ids":  alarmIDs,
		"company_id": companyID,
	})

	return policyIDs, err
}

const stmtGetOccurrencesForKeySQL = `
SELECT count(alert_key) as count
FROM mn_alert_alarm_history
WHERE company_id = ?
	AND alert_id = ?
	AND alert_dimension = ?
	AND alert_key = ?
GROUP BY alert_key
`

func (ds *alarmDataSource) GetOccurrencesForKey(
	ctx context.Context,
	companyID kt.Cid,
	policyID kt.PolicyID,
	dim string,
	key string,
) (count uint64, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetOccurrencesForKey", companyID).Done(start, err) }(time.Now())

	row := ds.stmtGetOccurrencesForKey.QueryRowContext(ctx, companyID, policyID, dim, key)
	if row == nil {
		err = ErrNilRow
		return 0, fmt.Errorf("stmtGetOccurrencesForKey returned nil row")
	}

	err = row.Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("Couldn't scan count value: %w", err)
	}

	return
}

const stmtGetAlarmsRelatedByKeySQL = `
SELECT DISTINCT alarm_id
FROM mn_alert_alarm_history
WHERE company_id = ?
	AND alert_id = ?
	AND alert_dimension = ?
	AND alert_key = ?
ORDER BY ctime DESC
LIMIT ?
`

const relatedAlarmsLimit = 100

func (ds *alarmDataSource) FindAlarmsRelatedByKey(
	ctx context.Context,
	companyID kt.Cid,
	policyID kt.PolicyID,
	dimension string,
	key string,
) (result []kt.AlarmID, err error) {
	defer func(start time.Time) { ds.metrics.Get("FindAlarmsRelatedByKey", companyID).Done(start, err) }(time.Now())

	err = ds.stmtGetAlarmsRelatedByKey.Select(&result, companyID, policyID, dimension, key, relatedAlarmsLimit)
	if err != nil {
		return nil, fmt.Errorf("Error selecting stmtGetAlarmsRelatedByKey: %v", err)
	}

	return
}

// fancyDelete efficiently removes rows from large tables that have an indexed id field and a non-indexed stamp field.
func (ds *alarmDataSource) fancyDelete(table, id, stamp string, retention time.Duration, chunkSize uint64, whereClause string) (uint64, error) {
	startTime := time.Now()

	if whereClause == "" {
		whereClause = "(1=1)"
	}

	// first, query to find the it to delete up to. we look for the Nth oldest row by id (with N=chunkSize) that matches
	// whereClause and is older than retention
	var idWatermark uint64
	getIdQuery := `SELECT id
		FROM (SELECT ` + id + ` AS id, ` + stamp + ` AS stamp
				FROM ` + table + ` WHERE ` + whereClause + ` ORDER BY ` + id + ` ASC LIMIT 1 OFFSET ` + strconv.FormatUint(chunkSize, 10) + `) t
		WHERE t.stamp < NOW() - INTERVAL ` + strconv.Itoa(int(retention.Seconds())) + ` SECOND`

	ds.Debugf("Executing %s", getIdQuery)
	// using rwdb here: we're going to be deleting immediately based on what we find, and reading from the rw node
	// lets us avoid any replication/lag related issues
	row := ds.db.Rwx.QueryRow(getIdQuery)
	if err := row.Scan(&idWatermark); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	// second, actually delete anything matching whereClause and under the id we found
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id < %d AND %s", table, idWatermark, whereClause)
	ds.Debugf("Executing %s", deleteQuery)
	res, err := ds.db.Rwx.Exec(deleteQuery)
	if err != nil {
		return 0, err
	}

	c, _ := res.RowsAffected() // nolint: errcheck
	ds.Debugf("%s: deleted %d rows in %v with `%s`", c, time.Since(startTime), deleteQuery)
	return uint64(c), nil
}

func execDelete(stmt *sqlx.Stmt, retention time.Duration, rowLimit uint64) (uint64, error) {
	if retention == 0 {
		return 0, fmt.Errorf("retention cannot be zero")
	}

	result, err := stmt.Exec(retention.Seconds(), int64(rowLimit))
	if err != nil {
		return 0, err
	}

	rows, err := result.RowsAffected()
	return uint64(rows), err
}

const stmtAckAlarmCurrentSQL = `
UPDATE mn_alert_alarm
SET alarm_state='CLEAR'
WHERE company_id = ? AND id = ?`

const stmtAckAlarmHistorySQL = `
INSERT INTO mn_alert_alarm_history(
	company_id,
	alert_id,
	alarm_id,
	threshold_id,
	mitigation_id,
	alert_severity,
	alert_dimension,
	alert_metric,
	alert_key,
	alert_value,
	alert_value2nd,
	alert_value3rd,
	alert_match_count,
	alert_baseline,
	baseline_used,
	learning_mode,
	debug_mode,
	old_alarm_state,
	new_alarm_state,
	alarm_history_type,
	comment,
	alarm_start_time
)
VALUES(
	:company_id,
	:alert_id,
	:id,
	:threshold_id,
	:mitigation_id,
	:alert_severity,
	:alert_dimension,
	:alert_metric,
	:alert_key,
	:alert_value,
	:alert_value2nd,
	:alert_value3rd,
	:alert_match_count,
	:alert_baseline,
	:baseline_used,
	:learning_mode,
	:debug_mode,
	:alarm_state,
	'CLEAR',
	'ACKNOWLEDGEMENT',
	:comment,
	:alarm_start
)
`

// TODO(tjonak): move hardcoded states into query params

func (ds *alarmDataSource) AckAlarmCurrent(ctx context.Context, alarm *kt.AlarmEvent) (err error) {
	if alarm == nil {
		err = fmt.Errorf("AckAlarmCurrent: nil alarm passed")
	}

	defer func(start time.Time) { ds.metrics.Get("AckAlarmCurrent", alarm.CompanyID).Done(start, err) }(time.Now())

	tx, err := ds.db.Rwx.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck

	_, err = tx.ExecContext(ctx, stmtAckAlarmCurrentSQL, alarm.CompanyID, alarm.AlarmID)
	if err != nil {
		return
	}

	_, err = tx.NamedExecContext(ctx, stmtAckAlarmHistorySQL, alarm)
	if err != nil {
		return
	}

	err = tx.Commit()

	return
}

func (ds *alarmDataSource) AckAlarmHistory(ctx context.Context, alarm *kt.AlarmEvent) (err error) {
	if alarm == nil {
		err = fmt.Errorf("AckAlarmHistory: nil alarm passed")
	}

	defer func(start time.Time) { ds.metrics.Get("AckAlarmHistory", alarm.CompanyID).Done(start, err) }(time.Now())

	_, err = ds.stmtAckAlarmHistory.ExecContext(ctx, alarm)

	return
}
