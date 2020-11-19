package ktds

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kentik/chf-alert/pkg/kt"
	"github.com/kentik/chf-alert/pkg/metric"

	"github.com/jmoiron/sqlx"
	"github.com/kentik/eggs/pkg/database"
	"github.com/kentik/eggs/pkg/logger"
	"github.com/kentik/eggs/pkg/olly"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type thresholdDataSource struct {
	logger.ContextL
	metrics metric.DBStore

	stmtCompanyThreshold                 *sqlx.Stmt
	stmtLoadThresholdsBulk               *sqlx.Stmt
	stmtGetThresholdsForPolicies         *sqlx.Stmt
	stmtActiveCompanyThresholds          *sqlx.Stmt
	stmtGetThresholdsCurrentAndArchieved *sqlx.Stmt
	stmtCreateThreshold                  *sqlx.NamedStmt
	stmtDeleteThresholds                 *sqlx.Stmt
	stmtCreateThresholdNotifications     *sqlx.Stmt
	stmtLoadNotificationChannels         *sqlx.Stmt

	pgrwx        *sqlx.DB
	eventBuilder *olly.Builder
}

// NewThresholdDataSource construct thresholds data source
func NewThresholdDataSource(
	db *RoRwPairDBPair,
	log logger.ContextL,
	eventBuilder *olly.Builder,
) (ds ThresholdDataSource, err error) {

	ds = &thresholdDataSource{
		ContextL:                             logger.NewContextL(logger.SContext{S: "activatorDS"}, log),
		metrics:                              metric.NewDB("ThresholdDS"),
		stmtCompanyThreshold:                 prepxOrPanic(db.Rox, stmtCompanyThresholdSQL),
		stmtLoadThresholdsBulk:               prepxOrPanic(db.Rox, stmtLoadThresholdsBulkSQL),
		stmtActiveCompanyThresholds:          prepxOrPanic(db.Rox, stmtActiveCompanyThresholdsSQL),
		stmtGetThresholdsCurrentAndArchieved: prepxOrPanic(db.Rox, stmtGetThresholdsCurrentAndArchievedSQL),
		stmtGetThresholdsForPolicies:         prepxOrPanic(db.Rox, stmtGetThresholdsForPoliciesSQL),
		stmtCreateThreshold:                  prepnOrPanic(db.Rwx, stmtCreateThresholdSQL),
		stmtDeleteThresholds:                 prepxOrPanic(db.Rwx, stmtDeleteThresholdsSQL),

		stmtCreateThresholdNotifications: prepxOrPanic(db.Rwx, stmtCreateThresholdNotificationSQL),
		stmtLoadNotificationChannels:     prepxOrPanic(db.Rox, stmtLoadNotificationChannelsSQL),
		pgrwx:                            db.Rwx,
	}
	return
}

func (ds *thresholdDataSource) Close() {
	database.CloseStatements(ds)
}

const thresholdSelectSQL = `
SELECT
	id,
	company_id,
	policy_id,
	COALESCE(severity, '') AS severity,
	COALESCE(conditions, '') AS conditions,
	COALESCE(activate, '') AS activate,
	COALESCE(compare, '') AS compare,
	COALESCE(direction, '') AS direction,
	COALESCE(mode, '') AS mode,
	COALESCE(fallback, '') AS fallback,
	COALESCE(status, '') AS status,
	cdate,
	edate,
	COALESCE(threshold_ack_required, false) AS threshold_ack_required,
	COALESCE(filters, '{}') AS filters
`

const thresholdSelectBaseSQL = thresholdSelectSQL + `
FROM mn_alert_threshold
`

const stmtCompanyThresholdSQL = thresholdSelectBaseSQL + `
WHERE company_id = $1
	AND edate > COALESCE($2, '1970-01-01'::timestamp)
	AND policy_id = $3
	AND status = $4
ORDER BY id ASC`

func (ds *thresholdDataSource) LoadCompanyThreshold(
	cid kt.Cid,
	lastCheck *time.Time,
	policyID kt.PolicyID,
	status string,
) (_ []*kt.Threshold, err error) {
	defer func(start time.Time) { ds.metrics.Get("LoadCompanyThreshold", cid).Done(start, err) }(time.Now())

	var dbThresholds []*kt.Threshold
	if err = ds.stmtCompanyThreshold.Select(&dbThresholds, cid, lastCheck, policyID, status); err != nil {
		return nil, fmt.Errorf("LoadCompanyThreshold Select: %v", err)
	}

	dbThresholds = ds.fixupThresholdsWarnBroken(dbThresholds)
	if _, err = ds.loadNotificationChannels(context.TODO(), dbThresholds...); err != nil {
		return nil, fmt.Errorf("LoadCompanyThreshold loadNotificationChannels: %v", err)
	}
	return dbThresholds, nil
}

const stmtLoadThresholdsBulkSQL = thresholdSelectBaseSQL + `
WHERE company_id = $1
  AND status = ANY($2)
  AND policy_id = ANY($3)`

func (ds *thresholdDataSource) LoadThresholdsBulk(ctx context.Context, cid kt.Cid, statuses []kt.ThresholdStatus, policyIDs []kt.PolicyID) (result []*kt.Threshold, err error) {
	if len(policyIDs) == 0 || len(statuses) == 0 {
		return
	}

	defer func(start time.Time) { ds.metrics.Get("LoadThresholdsBulk", cid).Done(start, err) }(time.Now())

	var dbThresholds []*kt.Threshold
	if err = ds.stmtLoadThresholdsBulk.SelectContext(ctx, &dbThresholds, cid, pq.Array(statuses), pq.Array(policyIDs)); err != nil {
		return nil, err
	}
	dbThresholds = ds.fixupThresholdsWarnBroken(dbThresholds)
	if _, err = ds.loadNotificationChannels(ctx, dbThresholds...); err != nil {
		return nil, err
	}
	return dbThresholds, nil
}

const stmtActiveCompanyThresholdsSQL = thresholdSelectBaseSQL + `
WHERE company_id = $1
	AND status = 'A'
ORDER BY id ASC`

func (ds *thresholdDataSource) LoadAllActiveThresholdsForCompany(cid kt.Cid) (map[kt.Tid]*kt.Threshold, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("LoadAllActiveThresholdsForCompany", cid).Done(start, err) }(time.Now())

	var dbThresholds []*kt.Threshold
	err = ds.stmtActiveCompanyThresholds.Select(&dbThresholds, cid)
	if err != nil {
		return nil, fmt.Errorf("LoadAllActiveThresholdsForCompany Select: %v", err)
	}

	thresholdsArray := ds.fixupThresholdsWarnBroken(dbThresholds)
	thresholdsMap, err := ds.loadNotificationChannels(context.TODO(), thresholdsArray...)
	if err != nil {
		return nil, fmt.Errorf("LoadAllActiveThresholdsForCompany loadNotificationChannels: %v", err)
	}

	return thresholdsMap, nil
}

const stmtLoadNotificationChannelsSQL = `SELECT threshold_id, channel_id FROM mn_alert_notification_channel_threshold WHERE company_id = $1 AND threshold_id = ANY($2)`

func (ds *thresholdDataSource) loadNotificationChannels(ctx context.Context, thresholds ...*kt.Threshold) (map[kt.Tid]*kt.Threshold, error) {
	tmap := make(map[kt.Tid]*kt.Threshold, len(thresholds))
	if len(thresholds) < 1 {
		return tmap, nil
	}
	cid := thresholds[0].CompanyID
	tids := []kt.Tid{}
	for _, threshold := range thresholds {
		if threshold.CompanyID != cid {
			return nil, fmt.Errorf("all thresholds must belong to the same cid")
		}
		threshold.NotificationChannels = make([]kt.NotificationChannelID, 0)
		tmap[threshold.ThresholdID] = threshold
		tids = append(tids, threshold.ThresholdID)
	}

	ds.Debugf("loadNotificationChannels: getting channels for tids %v", tids)
	rows, err := ds.stmtLoadNotificationChannels.QueryContext(ctx, cid, pq.Array(tids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tid kt.Tid
		var channelID kt.NotificationChannelID
		if err := rows.Scan(&tid, &channelID); err != nil {
			break
		}
		tmap[tid].NotificationChannels = append(tmap[tid].NotificationChannels, channelID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tmap, nil
}

func (ds *thresholdDataSource) fixupThresholdsWarnBroken(dbThresholds []*kt.Threshold) []*kt.Threshold {
	thresholds := make([]*kt.Threshold, 0, len(dbThresholds))
	for _, threshold := range dbThresholds {
		err := FixupThreshold(threshold)
		if err != nil {
			ds.Warnf("Skipping threshold with invalid JSON (cid=%d,policyid=%d,tid=%d): %v",
				threshold.CompanyID,
				threshold.PolicyID,
				threshold.ThresholdID,
				err)
			continue
		}
		thresholds = append(thresholds, threshold)
	}
	return thresholds
}

// FixupThreshold computes fields after database data is loaded,
// by unmarshalling JSON, etc.
func FixupThreshold(threshold *kt.Threshold) error {
	activateJSON := threshold.ActivateJSON
	if activateJSON == "" {
		activateJSON = "null"
	}
	var activate *kt.ThresholdActivate // possibly "null"
	err := json.Unmarshal([]byte(activateJSON), &activate)
	if err != nil {
		return fmt.Errorf("ActivateJSON: %v", err)
	}
	if activate != nil {
		threshold.Activate = *activate
	}

	compareJSON := threshold.CompareJSON
	if compareJSON == "" {
		compareJSON = "null"
	}
	var compare *kt.ThresholdCompare // possibly "null"
	err = json.Unmarshal([]byte(compareJSON), &compare)
	if err != nil {
		return fmt.Errorf("CompareJSON: %v", err)
	}
	if compare != nil {
		threshold.Compare = *compare
	}

	conditionsJSON := threshold.ConditionsJSON
	if conditionsJSON == "" {
		conditionsJSON = "[]"
	}
	err = json.Unmarshal([]byte(conditionsJSON), &threshold.Conditions)
	if err != nil {
		return fmt.Errorf("ConditionsJSON: %v", err)
	}

	fallbackJSON := threshold.FallbackJSON
	if fallbackJSON == "" {
		fallbackJSON = "null"
	}
	var fallback *kt.ThresholdFallback // possibly "null"
	err = json.Unmarshal([]byte(fallbackJSON), &fallback)
	if err != nil {
		return fmt.Errorf("FallbackJSON: %v", err)
	}
	if fallback != nil {
		threshold.Fallback = *fallback
	}

	filtersJSON := threshold.FiltersJSON
	if filtersJSON == "" || filtersJSON == "{}" {
		filtersJSON = "null"
	}
	var filters *kt.ThresholdFilter // possibly "null"
	err = json.Unmarshal([]byte(filtersJSON), &filters)
	if err != nil {
		return fmt.Errorf("FiltersJSON: %v", err)
	}
	if filters != nil {
		threshold.Filters = *filters
	}

	return nil
}

const stmtGetThresholdsForPoliciesSQL = thresholdSelectBaseSQL + `
WHERE
	company_id = $1
	AND policy_id = ANY($2)
	AND status = 'A'
`

func (ds *thresholdDataSource) GetThresholdsForPolicies(
	ctx context.Context,
	companyID kt.Cid,
	policyIDs []kt.PolicyID,
) (map[kt.PolicyID][]*kt.Threshold, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetThresholdsForPolicies", companyID).Done(start, err) }(time.Now())

	thresholds := []*kt.Threshold{}
	err = ds.stmtGetThresholdsForPolicies.SelectContext(ctx, &thresholds, companyID, pq.Array(policyIDs))
	if err != nil {
		return nil, fmt.Errorf("Couldn't execute stmtGetThresholdsForPolicies: %v", err)
	}

	thresholds = ds.fixupThresholdsWarnBroken(thresholds)
	if _, err := ds.loadNotificationChannels(ctx, thresholds...); err != nil {
		return nil, fmt.Errorf("GetThresholdsForPolicies loadNotificationChannels: %v", err)
	}

	result := map[kt.PolicyID][]*kt.Threshold{}
	for _, threshold := range thresholds {
		result[threshold.PolicyID] = append(result[threshold.PolicyID], threshold)
	}

	return result, nil
}

const stmtCreateThresholdSQL = `
INSERT INTO mn_alert_threshold(company_id, policy_id, severity, conditions, mode, activate, compare, direction, fallback, status, threshold_ack_required, threshold_description, filters)
VALUES (:company_id, :policy_id, :severity, :conditions, :mode, :activate, :compare, :direction, :fallback, :status, :threshold_ack_required, :threshold_description, :filters)
RETURNING id, company_id, policy_id, severity, conditions, mode, activate, compare, direction, fallback, status, threshold_ack_required, threshold_description, filters
`

const stmtCreateThresholdNotificationSQL = `
INSERT INTO mn_alert_notification_channel_threshold(company_id, threshold_id, channel_id) VALUES ($1, $2, $3)
`

func (ds *thresholdDataSource) CreateThreshold(ctx context.Context, threshold *kt.Threshold) (*kt.Threshold, error) {
	return ds.createThreshold(ctx, ds.stmtCreateThreshold, ds.stmtCreateThresholdNotifications, threshold)
}

func (ds *thresholdDataSource) CreateThresholdTx(ctx context.Context, tx kt.ChwwwTransaction, threshold *kt.Threshold) (*kt.Threshold, error) {
	return ds.createThreshold(ctx, tx.NamedStmtContext(ctx, ds.stmtCreateThreshold), tx.StmtxContext(ctx, ds.stmtCreateThresholdNotifications), threshold)
}

func (ds *thresholdDataSource) createThreshold(
	ctx context.Context,
	createStmt *sqlx.NamedStmt,
	createNotifStmt *sqlx.Stmt,
	threshold *kt.Threshold,
) (*kt.Threshold, error) {
	if threshold == nil {
		return nil, fmt.Errorf("CreateThreshold called with nil threshold")
	}

	var err error
	defer func(start time.Time) { ds.metrics.Get("CreateThreshold", threshold.CompanyID).Done(start, err) }(time.Now())

	row := createStmt.QueryRowxContext(ctx, threshold)
	if row == nil {
		err = ErrNilRow
		return nil, fmt.Errorf("couldn't retrieve threshold create response from db")
	}

	if err = row.Err(); err != nil {
		return nil, fmt.Errorf("stmtCreateThreshold execution failed: %v", err)
	}

	result := &kt.Threshold{}
	err = row.StructScan(result)
	if err != nil {
		return nil, fmt.Errorf("stmtCreateThreshold scan failed: %v", err)
	}

	// TODO(tjonak): remove threshold if this thing fails
	if err = FixupThreshold(result); err != nil {
		return nil, fmt.Errorf("CreateThreshold: FixupThreshold: %v", err)
	}

	result.NotificationChannels = make([]kt.NotificationChannelID, 0)
	for _, channelID := range threshold.NotificationChannels {
		if _, err := createNotifStmt.ExecContext(ctx, result.CompanyID, result.ThresholdID, channelID); err != nil {
			return nil, fmt.Errorf("stmtCreateThresholdNotifications(%v, %v, %v) failed: %w", result.CompanyID, result.ThresholdID, channelID, err)
		}
		result.NotificationChannels = append(result.NotificationChannels, channelID)
	}

	return result, nil
}

const stmtDeleteThresholdsSQL = `
DELETE FROM mn_alert_threshold WHERE company_id = $1 AND policy_id = $2 AND id = ANY($3)`

func (ds *thresholdDataSource) UpdateThresholds(ctx context.Context, cid kt.Cid, pid kt.PolicyID, oldThresholds, newThresholds []*kt.Threshold) (thresholds []*kt.Threshold, err error) {
	tx, err := ds.pgrwx.Beginx()
	if err != nil {
		return nil, fmt.Errorf("UpdateThresholds: %v", err)
	}
	defer tx.Rollback()

	thresholds, err = ds.updateThresholds(ctx, tx, cid, pid, oldThresholds, newThresholds)
	if err != nil {
		return nil, err
	}

	tx.Commit()

	return
}

func (ds *thresholdDataSource) UpdateThresholdsTx(ctx context.Context, tx kt.ChwwwTransaction, cid kt.Cid, pid kt.PolicyID, oldThresholds, newThresholds []*kt.Threshold) (_ []*kt.Threshold, err error) {
	return ds.updateThresholds(ctx, tx.Tx, cid, pid, oldThresholds, newThresholds)
}

func (ds *thresholdDataSource) updateThresholds(ctx context.Context, tx *sqlx.Tx, cid kt.Cid, pid kt.PolicyID, oldThresholds, newThresholds []*kt.Threshold) (_ []*kt.Threshold, err error) {
	if len(newThresholds) == 0 && len(oldThresholds) == 0 {
		return nil, nil
	}

	defer func(start time.Time) { ds.metrics.Get("UpdateThresholds", cid).Done(start, err) }(time.Now())

	stmtCreate := tx.NamedStmtContext(ctx, ds.stmtCreateThreshold)
	stmtCreateNotification := tx.StmtxContext(ctx, ds.stmtCreateThresholdNotifications)

	resultThresholds := make([]*kt.Threshold, len(newThresholds))
	for i, threshold := range newThresholds {
		row := stmtCreate.QueryRowxContext(ctx, threshold)
		if row.Err() != nil {
			return nil, errors.WithMessage(err, "UpdateThresholds.stmtCreate")
		}

		resultThresholds[i] = &kt.Threshold{}
		err = row.StructScan(resultThresholds[i])
		if err != nil {
			return nil, errors.WithMessage(err, "UpdateThresholds.stmtCreate.StructScan")
		}

		// TODO(tjonak): bulk that
		for _, channelID := range threshold.NotificationChannels {
			if _, err := stmtCreateNotification.ExecContext(ctx, resultThresholds[i].CompanyID, resultThresholds[i].ThresholdID, channelID); err != nil {
				return nil, fmt.Errorf("stmtCreateThresholdNotifications(%v, %v, %v) failed: %w", threshold.CompanyID, threshold.ThresholdID, channelID, err)
			}
		}

		resultThresholds[i].NotificationChannels = threshold.NotificationChannels
	}

	if len(oldThresholds) > 0 {
		tids := []kt.Tid{}
		for _, threshold := range oldThresholds {
			tids = append(tids, threshold.ThresholdID)
		}

		stmtDeleteThresholds := tx.StmtxContext(ctx, ds.stmtDeleteThresholds)

		res, err := stmtDeleteThresholds.ExecContext(ctx, cid, pid, pq.Array(tids))
		if err != nil {
			return nil, errors.WithMessage(err, "UpdateThresholds.stmtDeleteThresholds")
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return nil, errors.WithMessage(err, "UpdateThresholds.stmtDeleteThresholds.RowsAffected")
		}

		if rows < 1 {
			return nil, fmt.Errorf("0 rows affected by UpdateThresholds.stmtDeleteThresholds")
		}
	}

	return resultThresholds, nil
}

const stmtGetThresholdsCurrentAndArchievedSQL = thresholdSelectSQL + `
FROM mn_alert_threshold
WHERE company_id = $1 AND id = ANY($2)
UNION ALL
` + thresholdSelectSQL + `
FROM mn_archived_alert_threshold
WHERE company_id = $1 AND id = ANY($2)
`

func (ds *thresholdDataSource) GetThresholdsWithArchived(ctx context.Context, companyID kt.Cid, thresholdIDs []kt.Tid) (_ map[kt.Tid]*kt.Threshold, err error) {
	if len(thresholdIDs) < 1 {
		return map[kt.Tid]*kt.Threshold{}, nil
	}

	defer func(start time.Time) { ds.metrics.Get("GetThresholdsWithArchived", companyID).Done(start, err) }(time.Now())

	thresholds := []*kt.Threshold{}
	err = ds.stmtGetThresholdsCurrentAndArchieved.SelectContext(ctx, &thresholds, companyID, pq.Array(thresholdIDs))
	if err != nil {
		return nil, err
	}

	thresholds = ds.fixupThresholdsWarnBroken(thresholds)

	return ds.loadNotificationChannels(ctx, thresholds...)
}
