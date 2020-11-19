package ktds

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kentik/chf-alert/pkg/alert/baseline"
	"github.com/kentik/chf-alert/pkg/metric"

	"github.com/kentik/eggs/pkg/baseserver"

	"github.com/kentik/go-mysql-fork/mysql"

	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/kentik/eggs/pkg/database"
	"github.com/kentik/eggs/pkg/logger"
	"github.com/kentik/eggs/pkg/olly"
	"github.com/kentik/eggs/pkg/preconditions"
	"github.com/kentik/eggs/pkg/timing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Load postgres driver
)

type activatorDataSource struct {
	logger.ContextL
	metrics metric.DBStore
	ThresholdDataSource

	stmtKeyDebug         *sqlx.Stmt
	stmtSelectShortItems *sqlx.Stmt
	stmtActivateLong     *sqlx.Stmt
	stmtSelectBaseline   *sqlx.Stmt

	stmtGetBaselinesWithCompleteBackfills *sqlx.Stmt

	blLegacyCustomQuery *mysql.CustomQuery
	blCustomQuery       *mysql.CustomQuery

	chalertdbrox *sqlx.DB

	eventBuilder *olly.Builder
}

// NewActivatorDataSource constructs activator data source instance
func NewActivatorDataSource(
	chwww *RoRwPairDBPair,
	chalert *RoRwPairDBPair,
	myreaddsn string,
	log logger.Underlying,
	evtB *olly.Builder,
) (ads ActivatorDataSource, retErr error) {
	defer func() {
		err := recover()
		if err != nil {
			retErr = fmt.Errorf("caught panic while initializing: %s", err)
		}
	}()

	blCustomQuery, err := mysql.PrepareCustomQuery(myreaddsn, baselineCustomQuery, baselineCustomQueryColumns)
	if err != nil {
		return nil, err
	}

	blLegacyCustomQuery, err := mysql.PrepareCustomQuery(myreaddsn, stmtSelectLongItemsCustomQuery, stmtSelectLongItemsCustomQueryColumns)
	if err != nil {
		return nil, err
	}

	clogger := logger.NewContextLFromUnderlying(logger.SContext{S: "activatorDS"}, log)

	tds, err := NewThresholdDataSource(chwww, clogger, evtB.Clone())
	if err != nil {
		return nil, err
	}

	ds := &activatorDataSource{
		ContextL:            clogger,
		metrics:             metric.NewDB("ActivatorDS"),
		ThresholdDataSource: tds,

		stmtKeyDebug: prepxOrPanic(chwww.Rox, stmtKeyDebugSQL),

		stmtSelectShortItems: prepxOrPanic(chalert.Rox, stmtSelectShortItemsSQL),
		stmtActivateLong:     prepxOrPanic(chalert.Rwx, stmtActivateLongSQL),
		stmtSelectBaseline:   prepxOrPanic(chalert.Rox, stmtSelectBaselineSQL),

		stmtGetBaselinesWithCompleteBackfills: prepxOrPanic(chalert.Rox, getBaselinesWithCompleteBackfillsSQL),

		blCustomQuery:       blCustomQuery,
		blLegacyCustomQuery: blLegacyCustomQuery,

		chalertdbrox: chalert.Rox,

		eventBuilder: evtB,
	}

	preconditions.ValidateStruct(ds, preconditions.NoNilPointers)
	return ds, nil
}

func (ds *activatorDataSource) Close() {
	database.CloseStatements(ds)
	ds.ThresholdDataSource.Close()
}

func (ds *activatorDataSource) OllyBuilder() *olly.Builder {
	return ds.eventBuilder
}

const stmtSelectShortItemsSQL = `
SELECT threshold_id, alert_key, ctime
FROM mn_alert_match_history
WHERE company_id = ? AND alert_id = ? AND ctime > ?
`

func (ds *activatorDataSource) LoadCachedActivationTimes(cid kt.Cid, policyID kt.PolicyID, lastCheck time.Time) (map[kt.Tid]map[string][]int64, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("LoadCachedActivationTimes", cid).Done(start, err) }(time.Now())

	rows, err := ds.stmtSelectShortItems.Query(cid, policyID, lastCheck)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	found := 0
	actTimes := make(map[kt.Tid]map[string][]int64)
	for rows.Next() {
		var tid kt.Tid
		var key string
		var ts time.Time
		if err := rows.Scan(&tid, &key, &ts); err != nil {
			ds.Errorf("Error reading alert activated times: %v", err)
		} else {
			if _, ok := actTimes[tid]; !ok {
				actTimes[tid] = make(map[string][]int64)
			}

			if _, ok := actTimes[tid][key]; !ok {
				actTimes[tid][key] = make([]int64, 0)
			}

			actTimes[tid][key] = append(actTimes[tid][key], ts.Unix())
			found++
		}
	}

	if err = rows.Err(); err != nil {
		ds.Errorf("Error closing alert activated times: %v", err)
	}

	ds.Debugf("%d:%d Loaded %d Activation events from cache.", cid, policyID, found)
	return actTimes, nil
}

func (ds *activatorDataSource) TestBaseline(method string, cid kt.Cid, policyID kt.PolicyID, startTime time.Time) ([]*kt.BaselineValue, bool, error) {
	switch method {
	case "legacy":
		vs, err := ds.DownloadBaselineLegacy(cid, policyID, startTime)
		return vs, false, err
	case "custom":
		return ds.DownloadBaselineCustomQuery(cid, policyID, startTime)
	default:
		return nil, false, fmt.Errorf("unknown method %s", method)
	}
}

func (ds *activatorDataSource) DownloadBaseline(cid kt.Cid, policyID kt.PolicyID, startTime time.Time) ([]*kt.BaselineValue, bool, error) {
	if baseserver.GetGlobalFeatureService().Enabled(kt.FeatureBaselineActivateLegacyDownload, "cid", cid, "pid", policyID) {
		vs, err := ds.DownloadBaselineLegacy(cid, policyID, startTime)
		return vs, false, err
	} else {
		return ds.DownloadBaselineCustomQuery(cid, policyID, startTime)
	}
}

const baselineCustomQueryChunkSize = 64
const baselineCustomQueryColumns = 12
const baselineCustomQuery = `
SELECT
	baseline_id,
	alert_key,
	alert_dimension,
	alert_metric,
	alert_value_min,
	alert_value_max,
	alert_value_count,
	alert_value_98,
	alert_value_95,
	alert_value_50,
	alert_value_25,
	alert_value_05
FROM mn_alert_baseline_values
WHERE baseline_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

func (ds *activatorDataSource) DownloadBaselineCustomQuery(cid kt.Cid, policyID kt.PolicyID, startTime time.Time) (result []*kt.BaselineValue, notReady bool, err error) {
	t := timing.StartChrono()

	mt := ds.metrics.Get("DownloadBaselineCustomQuery", cid)
	start := time.Now()

	queries := 1

	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("DownloadBaselineCustomQuery: panic: %v", r)
		}
		ds.Infof("DownloadBaselineCustomQuery cid=%d pid=%d startTime=%s rows=%d queries=%d duration=%v", cid, policyID, startTime, len(result), queries, t.FinishDur())
		olly.QuickC(ds, olly.Op("baseline.download"),
			"baseline.download.duration_ms", t.Finish(),
			"baseline.download.rows", len(result),
			"baseline.download.cid", cid, "baseline.download.pid", policyID,
			"baseline.download.queries", queries,
		)

		mt.Done(start, err)
	}()

	// create zero-filled chunks of baseline IDs
	// return as int64 to we can pass these items directly to blCustomQuery.QueryWithInt64Args
	chunk := func(data map[kt.BaselineID]time.Time, chunkSize int) [][]int64 {
		i := 0
		items := make([]int64, len(data))
		for k := range data {
			items[i] = int64(k)
			i++
		}
		var chunks [][]int64
		zeroes := make([]int64, chunkSize)
		for i := 0; i < len(items); i += chunkSize {
			end := i + chunkSize
			if end > len(items) {
				end = len(items)
			}
			c := items[i:end]
			if len(c) < chunkSize {
				c = append(c, zeroes[0:chunkSize-len(c)]...)
			}
			chunks = append(chunks, c)
		}
		return chunks
	}

	baselineIds, notReady, err := ds.getBaselineIDsAndTimes(cid, policyID, startTime)
	if err != nil {
		// FIXME
		return nil, false, err
	}

	result = make([]*kt.BaselineValue, 0, 8*1024)
	for _, ids := range chunk(baselineIds, baselineCustomQueryChunkSize) {
		queries++
		err = ds.blCustomQuery.QueryWithInt64Args(func(row []driver.Value) {
			bl := new(kt.BaselineValue)
			bl.PolicyID = policyID // avoid reading/parsing cost
			bl.CompanyID = cid     // avoid reading/parsing cost

			bl.TimeStart = baselineIds[kt.BaselineID(row[0].(int64))]
			bl.TimeEnd = bl.TimeStart.Add(1 * time.Hour) // avoid reading/parsing cost

			bl.AlertKey = string(row[1].([]uint8))
			bl.AlertDimension = string(row[2].([]uint8))
			bl.AlertMetric = string(row[3].([]uint8))
			bl.AlertValueMin = float64(row[4].(float32))
			bl.AlertValueMax = float64(row[5].(float32))
			bl.AlertValueCount = int(row[6].(int64))
			bl.AlertValue98 = float64(row[7].(float32))
			bl.AlertValue95 = float64(row[8].(float32))
			bl.AlertValue50 = float64(row[9].(float32))
			bl.AlertValue25 = float64(row[10].(float32))
			bl.AlertValue05 = float64(row[11].(float32))

			result = append(result, bl)
		}, ids...)
	}

	return
}

// this is carefully crafted to be fast (fully indexed) while guaranteeing an ordering we can easily work with (also using the index, without further sorting)
// |    1 | SIMPLE      | mn_alert_baseline | range | cidpidstartprio_idx    | cidpidstartprio_idx | 12      | NULL |    1 |   0.00 |   100.00 |     100.00 | Using index condition |
const stmtSelectBaselineSQL = `
SELECT id,
  unix_timestamp(start_time),
  COALESCE(JSON_EXTRACT(metadata, '$.waitingToBeBackfilled') = true, false)
FROM mn_alert_baseline
WHERE company_id = ? AND alert_id = ? AND start_time >= ? AND prio > 0
ORDER BY start_time, prio ASC
`

func (ds *activatorDataSource) getBaselineIDsAndTimes(cid kt.Cid, policyID kt.PolicyID, startTime time.Time) (map[kt.BaselineID]time.Time, bool, error) {
	res := make(map[kt.BaselineID]time.Time, 16)
	rows, err := ds.stmtSelectBaseline.Query(cid, policyID, startTime)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var id, prevId kt.BaselineID
	var start, prevStart int64
	var baselineSlotIsPendingBackfill, aSlotIsNotReady bool
	for rows.Next() {
		if err := rows.Scan(&id, &start, &baselineSlotIsPendingBackfill); err != nil {
			// FIXME
			continue
		}
		res[id] = time.Unix(start, 0)
		if start == prevStart {
			// there are multiple baseline choices for this cid+pid+time
			// since we order by start_time, prio in the stmtSelectBaseline query, we'll get rows for the same
			// slot one after the other, with the highest prio coming last. this means we just have to keep track
			// if the previous start time we saw; if we see it again, just overwrite the last id we added
			delete(res, prevId)
		}
		prevId = id
		prevStart = start

		// If this baseline slot is pending backfill,
		// then this entire set of baseline values is "not ready".
		// FIXME: perhaps if there is valid frontfill data in the same slot we don't care
		// that this slot is not ready? Marking not ready is conservative and safe.
		aSlotIsNotReady = aSlotIsNotReady || baselineSlotIsPendingBackfill
	}
	return res, aSlotIsNotReady, nil
}

const getBaselinesWithCompleteBackfillsSQL = `
SELECT * FROM (

SELECT
  alert_id,
  MAX(ctime) AS max_ctime,
  BIT_OR(COALESCE(JSON_EXTRACT(metadata, '$.waitingToBeBackfilled') = true, false)) != 0 AS waiting_for_backfill
FROM mn_alert_baseline
WHERE company_id = ?
  AND start_time >= ?
  AND prio > 0
  AND source = 'backfill'
GROUP BY alert_id

) sq1
WHERE NOT waiting_for_backfill
`

func (ds *activatorDataSource) FetchBackfillCompletionTimes(cid kt.Cid, startTime time.Time) (map[kt.PolicyID]time.Time, error) {
	rows, err := ds.stmtGetBaselinesWithCompleteBackfills.Query(cid, startTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[kt.PolicyID]time.Time)
	for rows.Next() {
		var alertID kt.PolicyID
		var maxCtime time.Time
		var b bool

		if err := rows.Scan(&alertID, &maxCtime, &b); err != nil {
			ds.Warnf("while scanning in FetchBackfillCompletionTimes: err=%s", err)
			continue
		}

		m[alertID] = maxCtime
	}

	return m, nil
}

const stmtSelectLongItemsCustomQueryColumns = 12
const stmtSelectLongItemsCustomQuery = `
SELECT
	unix_timestamp(time_start),
	alert_key,
	alert_dimension,
	alert_metric,
	alert_value_min,
	alert_value_max,
	alert_value_count,
	alert_value_98,
	alert_value_95,
	alert_value_50,
	alert_value_25,
	alert_value_05
FROM mn_alert_long_set
WHERE company_id = ? AND alert_id = ? AND ctime >= now() - interval ` + baseline.MaxLookbackDaysStr + ` day AND time_start >= ?
`

func (ds *activatorDataSource) DownloadBaselineLegacy(cid kt.Cid, policyID kt.PolicyID, startTime time.Time) (result []*kt.BaselineValue, err error) {
	t := timing.StartChrono()

	mt := ds.metrics.Get("DownloadBaselineLegacy", cid)
	start := time.Now()

	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("DownloadBaselineLegacy: panic: %v", r)
		}
		ds.Infof("DownloadBaselineLegacy cid=%d pid=%d startTime=%s rows=%d duration=%s", cid, policyID, startTime, len(result), t.FinishDur())
		olly.QuickC(ds, olly.Op("baseline.download"),
			"baseline.download.duration_ms", t.Finish(),
			"baseline.download.rows", len(result),
			"baseline.download.cid", cid, "baseline.download.pid", policyID,
		)

		mt.Done(start, err)
	}()
	result = make([]*kt.BaselineValue, 0, 8*1024)
	err = ds.blLegacyCustomQuery.Query(func(row []driver.Value) {
		bl := new(kt.BaselineValue)
		bl.PolicyID = policyID // avoid reading/parsing cost
		bl.CompanyID = cid     // avoid reading/parsing cost

		bl.TimeStart = time.Unix(row[0].(int64), 0)
		bl.AlertKey = string(row[1].([]uint8))
		bl.AlertDimension = string(row[2].([]uint8))
		bl.AlertMetric = string(row[3].([]uint8))
		bl.AlertValueMin = float64(row[4].(float32))
		bl.AlertValueMax = float64(row[5].(float32))
		bl.AlertValueCount = int(row[6].(int64))
		bl.AlertValue98 = float64(row[7].(float32))
		bl.AlertValue95 = float64(row[8].(float32))
		bl.AlertValue50 = float64(row[9].(float32))
		bl.AlertValue25 = float64(row[10].(float32))
		bl.AlertValue05 = float64(row[11].(float32))

		bl.TimeEnd = bl.TimeStart.Add(1 * time.Hour) // avoid reading/parsing cost

		result = append(result, bl)
	}, int64(cid), int64(policyID), startTime)
	return
}

const stmtActivateLongSQL = `
INSERT INTO mn_alert_long_set (alert_id, company_id, time_start, time_end, alert_key, alert_dimension, alert_metric, alert_value_min, alert_value_max, alert_value_count, alert_value_98, alert_value_95, alert_value_50, alert_value_25, alert_value_05, alert_position)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE alert_value_count = alert_value_count + VALUES(alert_value_count)
`

func (ds *activatorDataSource) UploadBaseline(policyID kt.PolicyID, cid kt.Cid, time_start time.Time, time_end time.Time, alert_key string, alert_dimension string, alert_metric string, alert_value_min float64, alert_value_max float64, alert_value_count int64, alert_value_98 float64, alert_value_95 float64, alert_value_50 float64, alert_value_25 float64, alert_value_05 float64, alert_position int) error {
	var err error
	defer func(start time.Time) { ds.metrics.Get("UploadBaseline", cid).Done(start, err) }(time.Now())

	_, err = ds.stmtActivateLong.Exec(policyID, cid, time_start, time_end, alert_key, alert_dimension, alert_metric, alert_value_min, alert_value_max, alert_value_count, alert_value_98, alert_value_95, alert_value_50, alert_value_25, alert_value_05, alert_position)
	return err
}

const stmtKeyDebugSQL = `
SELECT company_id, policy_id, threshold_id, alert_type, alert_key
FROM mn_alert_debug_key
WHERE company_id = $1
	AND COALESCE(expire, now()+interval'1 day') > now()
	AND (policy_id IS NULL OR policy_id = COALESCE($2, policy_id))
	AND (threshold_id IS NULL OR threshold_id = COALESCE($3, threshold_id))
`

func (ds *activatorDataSource) GetDebugKeys(
	cid kt.Cid,
	policyID *kt.PolicyID,
	tid *kt.Tid,
) (debugKeys []kt.AlertingDebugKey, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetDebugKeys", cid).Done(start, err) }(time.Now())

	err = ds.stmtKeyDebug.Select(&debugKeys, cid, policyID, tid)
	if err != nil {
		return nil, fmt.Errorf("GetDebugKeys Select: %v", err)
	}

	for i := range debugKeys {
		err := json.Unmarshal([]byte(debugKeys[i].KeyJSON), &debugKeys[i].Key)
		if err != nil {
			ds.Warnf("Skipping bad debug entry (cid=%d,policyID=%d,tid=%d): %v", cid, policyID, tid, err)
			debugKeys[i].Key = nil
		}
	}

	return debugKeys, nil
}

func (ds *activatorDataSource) GetBaselineTableInfo() (kt.TableInfo, error) {
	return ds.GetTableInfo("chalert", "mn_alert_baseline_values")
}

func (ds *activatorDataSource) GetTableInfo(schema, table string) (kt.TableInfo, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("GetTableInfo", 0).Done(start, err) }(time.Now())

	var ti kt.TableInfo
	// Note: information_schema.tables data is cached, typically updated once every
	// 24 hours. In production, this will always be an estimate.
	// You can force an update by running 'analyze table <table_name>', but it doesn't
	// seem appropriate to do that for every query.
	err = ds.chalertdbrox.Get(&ti, `
      select table_rows, avg_row_length, data_length, index_length
      from information_schema.tables
      where table_schema = ? and table_name = ?
		limit 1`, schema, table)

	return ti, err
}

func (ds *activatorDataSource) GetBaselineTableRowsByCompany() (ccs []kt.CidWithCount, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetBaselineTableRowsByCompany", 0).Done(start, err) }(time.Now())

	err = ds.chalertdbrox.Select(&ccs, `
      select company_id, sum(entries) as count
      from mn_alert_baseline
      group by company_id
		order by count desc`)

	return
}

func (ds *activatorDataSource) GetBaselineTableRows(cid kt.Cid, policyID kt.PolicyID) (c int, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetBaselineTableRows", cid).Done(start, err) }(time.Now())

	err = ds.chalertdbrox.Get(&c, `
      select sum(entries)
      from mn_alert_baseline
		where company_id = ? and alert_id = ?`, cid, policyID)

	return
}

func (ds *activatorDataSource) FetchMariaDBTableInfos() (map[string]kt.TableInfo, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchMariaDBTableInfos", 0).Done(start, err) }(time.Now())

	var tis []kt.TableInfo
	// Note: information_schema.tables data is cached, typically updated once every
	// 24 hours. In production, this will always be an estimate.
	// You can force an update by running 'analyze table <table_name>', but it doesn't
	// seem appropriate to do that for every query.
	err = ds.chalertdbrox.Select(&tis, `
		select table_name, table_rows, avg_row_length, data_length, index_length
		from information_schema.tables
		where table_schema = ?
		`, "chalert")
	if err != nil {
		return nil, err
	}

	m := make(map[string]kt.TableInfo)
	for _, ti := range tis {
		m[ti.TableName] = ti
	}
	return m, nil
}

func (ds *activatorDataSource) FetchMariaDBTableOldestRowAges() (map[string]time.Duration, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchMariaDBTableOldestRowAges", 0).Done(start, err) }(time.Now())

	var ages []struct {
		TableName  string `db:"table_name"`
		AgeSeconds int    `db:"age_seconds"`
	}
	err = ds.chalertdbrox.Select(&ages, `
		select 'mn_alert_alarm' AS table_name, coalesce(to_seconds(now()) - to_seconds(ctime), 0) AS 'age_seconds' from mn_alert_alarm where id = (select min(id) from mn_alert_alarm)
		UNION
		select 'mn_alert_alarm_history' AS table_name, coalesce(to_seconds(now()) - to_seconds(ctime), 0) AS 'age_seconds' from mn_alert_alarm_history where id = (select min(id) from mn_alert_alarm_history)
		UNION
		select 'mn_alert_baseline' AS table_name, coalesce(to_seconds(now()) - to_seconds(ctime), 0) AS 'age_seconds' from mn_alert_baseline where id = (select min(id) from mn_alert_baseline)
		UNION
		select 'mn_alert_match_history' AS table_name, coalesce(to_seconds(now()) - to_seconds(ctime), 0) AS 'age_seconds' from mn_alert_match_history where id = (select min(id) from mn_alert_match_history)
		UNION
		select 'mn_mit2_machines' AS table_name, coalesce(to_seconds(now()) - to_seconds(min(cdate)), 0) AS 'age_seconds' from mn_mit2_machines
		UNION
		select 'mn_mit2_machines_log' AS table_name, coalesce(to_seconds(now()) - to_seconds(stamp), 0) AS 'age_seconds' from mn_mit2_machines_log where id = (select min(id) from mn_mit2_machines_log)
	`)
	if err != nil {
		return nil, err
	}

	m := make(map[string]time.Duration)
	for _, age := range ages {
		m[age.TableName] = time.Duration(age.AgeSeconds) * time.Second
	}
	return m, nil
}
