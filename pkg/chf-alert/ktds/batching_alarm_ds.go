package ktds

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/jmoiron/sqlx"
	"github.com/kentik/eggs/pkg/logger"
)

func NewBatchingAlarmDataSource(ctx context.Context, rodsn string, rwdsn string, maxSQLConns int, log logger.Underlying) (BatchingAlarmDataSource, error) {
	// We need MultiStatements and Interpolate params to function correctly.
	// See RunAlarmStatementsBatch for more details.
	dsn, err := mysqlDSNEnsureMultiInterpolate(rwdsn)
	if err != nil {
		return nil, err
	}

	db, err := OpenMysqlDB(ctx, dsn, maxSQLConns)
	if err != nil {
		return nil, err
	}
	mysqldbxMultiInterpolate := sqlx.NewDb(db, "mysql")

	rodb, err := OpenMysqlDB(ctx, rodsn, maxSQLConns)
	if err != nil {
		return nil, err
	}
	mysqlrodbx := sqlx.NewDb(rodb, "mysql")

	stmtGetAlarmsSmallForPolicy, err := rodb.Prepare(`
	SELECT id, alert_key, alarm_state, threshold_id, alarm_start, last_match_time
	FROM mn_alert_alarm
	WHERE company_id = ?
		AND alert_id = ?
	`)
	if err != nil {
		return nil, err
	}

	selectAlarmEvent := `SELECT
		id, alarm_state, alert_id, coalesce(mitigation_id, 0) AS mitigation_id, company_id, threshold_id,
		alert_key, alert_dimension, alert_metric, alert_value, alert_value2nd, alert_value3rd,
		alert_match_count, alert_baseline, alert_severity, baseline_used, learning_mode, debug_mode,
		notify_start, notify_end, alarm_start, alarm_end, last_match_time
	FROM mn_alert_alarm `
	stmtGetNotifyStartAlarmEvents, err := mysqlrodbx.PrepareNamed(
		selectAlarmEvent + `
	WHERE company_id = :company_id
		AND alert_id = :alert_id
		AND notify_start = '0000-00-00 00::00::00'
		AND alarm_state = 'ALARM'
	`)
	if err != nil {
		return nil, err
	}
	stmtGetNotifyEscalateAlarmEvents, err := mysqlrodbx.PrepareNamed(`
	SELECT
		id, alarm_state, alert_id, coalesce(mitigation_id, 0) AS mitigation_id, company_id, threshold_id,
		alert_key, alert_dimension, alert_metric, alert_value, alert_value2nd, alert_value3rd,
		alert_match_count, alert_baseline, alert_severity, baseline_used, learning_mode, debug_mode,
		notify_start, notify_end, alarm_start, alarm_end, last_match_time
	FROM (
	SELECT aa.*
	FROM mn_alert_alarm aa
	INNER JOIN mn_alert_alarm_history ah
	ON aa.id = ah.alarm_id
	WHERE aa.company_id = :company_id
		AND aa.alert_id = :alert_id
		AND ah.alarm_history_type = 'ESCALATION'
		AND aa.notify_start != '0000-00-00 00::00::00'
		AND aa.notify_start < ah.ctime
		AND aa.alarm_state = 'ALARM'
	) sq
	`)
	if err != nil {
		return nil, err
	}
	stmtGetNotifyEndAlarmEvents, err := mysqlrodbx.PrepareNamed(
		selectAlarmEvent + `
	WHERE company_id = :company_id
		AND alert_id = :alert_id
		AND notify_end = '0000-00-00 00::00::00'
		AND (alarm_state = 'ACK_REQ' OR alarm_state = 'CLEAR')
	`)
	if err != nil {
		return nil, err
	}

	return &batchingAlarmDataSource{
		mysqldbxMultiInterpolate: mysqldbxMultiInterpolate,
		mysqlrodbx:               mysqlrodbx,
		ContextL: logger.NewContextLFromUnderlying(
			logger.SContext{S: "batchingAlarmDS"},
			log),

		stmtGetAlarmsSmallForPolicy:      stmtGetAlarmsSmallForPolicy,
		stmtGetNotifyStartAlarmEvents:    stmtGetNotifyStartAlarmEvents,
		stmtGetNotifyEscalateAlarmEvents: stmtGetNotifyEscalateAlarmEvents,
		stmtGetNotifyEndAlarmEvents:      stmtGetNotifyEndAlarmEvents,
	}, nil
}

type batchingAlarmDataSource struct {
	mysqldbxMultiInterpolate *sqlx.DB
	mysqlrodbx               *sqlx.DB
	logger.ContextL

	stmtGetAlarmsSmallForPolicy      *sql.Stmt
	stmtGetNotifyStartAlarmEvents    *sqlx.NamedStmt
	stmtGetNotifyEscalateAlarmEvents *sqlx.NamedStmt
	stmtGetNotifyEndAlarmEvents      *sqlx.NamedStmt
}

func (ds *batchingAlarmDataSource) Close() {
	if err := ds.mysqldbxMultiInterpolate.Close(); err != nil {
		ds.Errorf("while closing: %v", err)
	}
	if err := ds.mysqlrodbx.Close(); err != nil {
		ds.Errorf("while closing: %v", err)
	}
}

func (ds *batchingAlarmDataSource) InsertAlertMatchHistoryRows(rows []kt.AlertMatchHistoryRow) error {
	sql, args, err := insertMultipleRowsSQL(
		`INSERT INTO mn_alert_match_history
(alert_id, company_id, threshold_id, alert_key, alert_dimension, alert_metric, alert_value, alert_value2nd, alert_value3rd, alert_baseline, baseline_used, learning_mode, debug_mode, current_position, history_position)
VALUES `,
		"(:alert_id, :company_id, :threshold_id, :alert_key, :alert_dimension, :alert_metric, :alert_value, :alert_value2nd, :alert_value3rd, :alert_baseline, :baseline_used, :learning_mode, :debug_mode, :current_position, :history_position)",
		rows)
	if err != nil {
		return err
	}
	_, err = ds.mysqldbxMultiInterpolate.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil

}

func insertMultipleRowsSQL(prefix string, namedSQL string, rows []kt.AlertMatchHistoryRow) (string, []interface{}, error) {
	if len(rows) <= 0 {
		return "", nil, fmt.Errorf("insertMultipleRowsSQL: must have at least one row")
	}
	rowTexts := make([]string, len(rows))
	args := make([]interface{}, 0, len(rows)*strings.Count(namedSQL, ":"))
	for i := range rows {
		var err error
		var rowArgs []interface{}
		rowTexts[i], rowArgs, err = sqlx.Named(namedSQL, rows[i])
		if err != nil {
			return "", nil, err
		}
		args = append(args, rowArgs...)
	}
	return prefix + strings.Join(rowTexts, ","), args, nil
}

func (ds *batchingAlarmDataSource) GetAlarmsSmallForPolicy(cid kt.Cid, policyID kt.PolicyID) ([]kt.AlertAlarmSmall, error) {
	rows, err := ds.stmtGetAlarmsSmallForPolicy.Query(cid, policyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]kt.AlertAlarmSmall, 0, 16)
	for rows.Next() {
		var item kt.AlertAlarmSmall
		if err := rows.Scan(
			&item.ID,
			&item.AlertKey,
			&item.AlarmState,
			&item.ThresholdID,
			&item.AlarmStart,
			&item.LastMatchTime,
		); err != nil {
			return nil, err
		}
		item.AlertKey = kt.AlertKeyFixupForGeoNulBytes(item.AlertKey)
		result = append(result, item)
	}
	return result, err
}

func (ds *batchingAlarmDataSource) GetAlarmEventsStarting(cid kt.Cid, policyID kt.PolicyID) ([]*kt.AlarmEvent, error) {
	var aes []*kt.AlarmEvent
	err := ds.stmtGetNotifyStartAlarmEvents.Select(&aes, map[string]interface{}{
		"company_id": cid,
		"alert_id":   policyID,
	})
	return aes, err
}

func (ds *batchingAlarmDataSource) GetAlarmEventsEscalating(cid kt.Cid, policyID kt.PolicyID) ([]*kt.AlarmEvent, error) {
	var aes []*kt.AlarmEvent
	err := ds.stmtGetNotifyEscalateAlarmEvents.Select(&aes, map[string]interface{}{
		"company_id": cid,
		"alert_id":   policyID,
	})
	return aes, err
}

func (ds *batchingAlarmDataSource) GetAlarmEventsStopping(cid kt.Cid, policyID kt.PolicyID) ([]*kt.AlarmEvent, error) {
	var aes []*kt.AlarmEvent
	err := ds.stmtGetNotifyEndAlarmEvents.Select(&aes, map[string]interface{}{
		"company_id": cid,
		"alert_id":   policyID,
	})
	return aes, err
}

func (ds *batchingAlarmDataSource) UpdateAlarmsNotifyStart(notifyStart time.Time, cid kt.Cid, policyID kt.PolicyID, alarmIDs ...kt.AlarmID) error {
	return namedInRebindExec(ds.mysqldbxMultiInterpolate, `
UPDATE mn_alert_alarm
SET notify_start = :notify_start
WHERE company_id = :company_id
  AND alert_id = :alert_id
  AND id IN (:alarm_ids)
`, map[string]interface{}{
		"notify_start": notifyStart,
		"company_id":   cid,
		"alert_id":     policyID,
		"alarm_ids":    alarmIDs,
	})
}

func (ds *batchingAlarmDataSource) UpdateAlarmsNotifyEnd(notifyEnd time.Time, cid kt.Cid, policyID kt.PolicyID, alarmIDs ...kt.AlarmID) error {
	return namedInRebindExec(ds.mysqldbxMultiInterpolate, `
UPDATE mn_alert_alarm
SET notify_end = :notify_end
WHERE company_id = :company_id
  AND alert_id = :alert_id
  AND id IN (:alarm_ids)
`, map[string]interface{}{
		"notify_end": notifyEnd,
		"company_id": cid,
		"alert_id":   policyID,
		"alarm_ids":  alarmIDs,
	})
}

// RunAlarmStatementsBatch takes a list of statement types/args and executes them
// as a single mysql query. The intent is to batch many related inserts/updates/deletes.
// NOTE: for this to work the mysql driver MUST have multi statements and interpolation
// turned on.
// The mysql "CLIENT_MULTI_STATEMENTS" connection flag must be set to process multiple
// statements in one query. The go-sql-driver/mysql driver sets this when you pass
// "multiStatements=true" in the DSN: https://github.com/go-sql-driver/mysql#multistatements
// The go-sql-driver/mysql driver has a driver level option to interpolate params:
// https://github.com/kentik/go-mysql-fork/mysql#interpolateparams
// interpolateParams must be set for RunAlarmStatementsBatch to work, because
// mysql does not allow parameter placeholders in multistatement queries after
// the first statement. In our queries, we want to use parameters in every query.
// See also mysqlDSNEnsureMultiStatements, mysqlDSNEnsureInterpolateParams.
func (ds *batchingAlarmDataSource) RunAlarmStatementsBatch(statements []interface{}) error {
	if len(statements) <= 0 {
		return nil
	}

	sqls := make([]string, len(statements))
	args := make([]interface{}, 0, len(statements)*15)
	for i := range statements {
		var statementSQLRaw string
		switch statements[i].(type) {
		case kt.DeleteClearAlarmArgs:
			statementSQLRaw = `
DELETE FROM mn_alert_alarm
WHERE company_id = :company_id
  AND alert_id = :alert_id
  AND threshold_id = :threshold_id
  AND id = :id
  AND (alarm_state = 'CLEAR' OR alarm_state = 'ACK_REQ')
`
		case kt.UpdateAlarmLastMatchTimeArgs:
			statementSQLRaw = `
UPDATE mn_alert_alarm
SET alert_match_count = alert_match_count + :alert_match_count,
    last_match_time = :last_match_time
WHERE company_id = :company_id
  AND alert_id = :alert_id
  AND threshold_id = :threshold_id
  AND id = :id
`
		case kt.UpdateAlarmForEscalationArgs:
			statementSQLRaw = `
UPDATE mn_alert_alarm
SET threshold_id = :threshold_id,
  alert_key = :alert_key,
  alert_dimension = :alert_dimension,
  alert_metric = :alert_metric,
  alert_value = :alert_value,
  alert_value2nd = :alert_value2nd,
  alert_value3rd = :alert_value3rd,
  alert_match_count = alert_match_count + :alert_match_count,
  alert_baseline = :alert_baseline,
  baseline_used = :baseline_used,
  learning_mode = :learning_mode,
  debug_mode = :debug_mode,
  -- not updating alarm_start
  last_match_time = :last_match_time,
  alert_severity = :alert_severity
WHERE company_id = :company_id
  AND alert_id = :alert_id
  AND id = :id
`
			// TODO: Tracef
			// ds.Debugf("UpdateAlarmForEscalationArgs company_id = %d alert_id = %d id = %d", z.CompanyID, z.AlertID, z.ID)
		case kt.UpdateAlarmEndingArgs:
			statementSQLRaw = `
UPDATE mn_alert_alarm
SET alarm_end = :alarm_end, alarm_state = :alarm_state
WHERE company_id = :company_id
  AND alert_id = :alert_id
  AND threshold_id = :threshold_id
  AND id = :id
`
		case kt.InsertAlarmArgs:
			statementSQLRaw = `
INSERT INTO mn_alert_alarm (alarm_state, company_id, alert_id, threshold_id, alert_key, alert_dimension, alert_metric, alert_value, alert_value2nd, alert_value3rd, alert_match_count, alert_baseline, baseline_used, learning_mode, debug_mode, alarm_start, last_match_time, alert_severity)
VALUES (:alarm_state, :company_id, :alert_id, :threshold_id, :alert_key, :alert_dimension, :alert_metric, :alert_value, :alert_value2nd, :alert_value3rd, :alert_match_count, :alert_baseline, :baseline_used, :learning_mode, :debug_mode, :alarm_start, :last_match_time, :alert_severity)
`
		case kt.InsertAlarmHistoryArgs:
			statementSQLRaw = `
INSERT INTO mn_alert_alarm_history (alarm_history_type, alarm_id, old_alarm_state, new_alarm_state, company_id, alert_id, threshold_id, alert_key, alert_dimension, alert_metric, alert_value, alert_value2nd, alert_value3rd, alert_match_count, alert_baseline, baseline_used, learning_mode, debug_mode, alert_severity, alarm_start_time)
VALUES (:alarm_history_type, :alarm_id, :old_alarm_state, :new_alarm_state, :company_id, :alert_id, :threshold_id, :alert_key, :alert_dimension, :alert_metric, :alert_value, :alert_value2nd, :alert_value3rd, :alert_match_count, :alert_baseline, :baseline_used, :learning_mode, :debug_mode, :alert_severity, :alarm_start_time)
`
		case kt.InsertAlarmHistoryWithLastIDArgs:
			statementSQLRaw = `
INSERT INTO mn_alert_alarm_history (alarm_history_type, alarm_id, old_alarm_state, new_alarm_state, company_id, alert_id, threshold_id, alert_key, alert_dimension, alert_metric, alert_value, alert_value2nd, alert_value3rd, alert_match_count, alert_baseline, baseline_used, learning_mode, debug_mode, alert_severity, alarm_start_time)
VALUES (:alarm_history_type, LAST_INSERT_ID(), :old_alarm_state, :new_alarm_state, :company_id, :alert_id, :threshold_id, :alert_key, :alert_dimension, :alert_metric, :alert_value, :alert_value2nd, :alert_value3rd, :alert_match_count, :alert_baseline, :baseline_used, :learning_mode, :debug_mode, :alert_severity, :alarm_start_time)
`
		case kt.InsertAlarmHistoryCopyingStartArgs:
			statementSQLRaw = `
INSERT INTO mn_alert_alarm_history (alarm_id, old_alarm_state, new_alarm_state, alert_id, company_id, threshold_id, alert_key, alert_dimension, alert_metric, alert_value, alert_value2nd, alert_value3rd, alert_match_count, alert_baseline, baseline_used, learning_mode, debug_mode, alert_severity, mitigation_id, alarm_start_time)
SELECT alarm_id, :old_alarm_state, :new_alarm_state, alert_id, company_id, threshold_id, alert_key, alert_dimension, alert_metric, alert_value, alert_value2nd, alert_value3rd, alert_match_count, alert_baseline, baseline_used, learning_mode, debug_mode, alert_severity, mitigation_id, alarm_start_time
FROM mn_alert_alarm_history
WHERE alarm_id = :alarm_id
  AND alarm_history_type = 'START'
LIMIT 1
`
		default:
			return fmt.Errorf("unknown statement type: %T", statements[i])
		}
		var err error
		var rowArgs []interface{}
		sqls[i], rowArgs, err = sqlx.Named(statementSQLRaw, statements[i])
		if err != nil {
			return err
		}
		// TODO: Tracef
		// ds.Debugf("RunAlarmStatementsBatch args: %+v", rowArgs)
		args = append(args, rowArgs...)
	}

	sql := strings.Join(sqls, ";\n")
	ds.Debugf("RunAlarmStatementsBatch combination sql len(sqls)=%d len(sql)=%d", len(sqls), len(sql))
	// TODO: Tracef
	// ds.Debugf("RunAlarmStatementsBatch combination sql statement: %s", strings.Replace(sql, "\n", "  ", -1))
	_, err := ds.mysqldbxMultiInterpolate.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}
