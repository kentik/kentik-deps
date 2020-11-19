package ktds

import (
	"fmt"
	"time"

	"github.com/kentik/chf-alert/pkg/eggs/conductor"
	"github.com/kentik/chf-alert/pkg/kt"
	"github.com/kentik/chf-alert/pkg/metric"

	"github.com/jmoiron/sqlx"
	"github.com/kentik/eggs/pkg/database"
	"github.com/kentik/eggs/pkg/logger"
	"github.com/kentik/eggs/pkg/preconditions"

	"github.com/lib/pq"
)

func NewConductorDataSource(db *RoRwPairDBPair, log logger.Underlying) (cds ConductorDataSource, retErr error) {
	defer func() {
		err := recover()
		if err != nil {
			retErr = fmt.Errorf("caught panic while initializing: %s", err)
		}
	}()

	ds := &conductorDS{
		ContextL: logger.NewContextLFromUnderlying(logger.SContext{S: "conductorDS"}, log),
		metrics:  metric.NewDB("ConductorDS"),

		stmtListCompanies:        prepxOrPanic(db.Rox, stmtListCompaniesSQL),
		stmtFetchShadowCompanyID: prepxOrPanic(db.Rox, stmtFetchShadowCompanyIDSQL),
		stmtFetchDeviceAlertRows: prepxOrPanic(db.Rox, stmtFetchDeviceAlertRowsSQL),
		stmtUpdateDeviceAlertRow: prepnOrPanic(db.Rwx, stmtUpdateDeviceAlertRowSQL),
		stmtFetchHostPoolEntries: prepxOrPanic(db.Rox, stmtFetchHostPoolEntriesSQL),
		stmtInsertHostPoolEntry:  prepnOrPanic(db.Rwx, stmtInsertHostPoolEntrySQL),
		stmtFetchPipelines:       prepxOrPanic(db.Rox, stmtFetchPipelinesSQL),
		stmtInsertPipeline:       prepnOrPanic(db.Rwx, stmtInsertPipelineSQL),
		stmtFetchAllServices:     prepxOrPanic(db.Rox, stmtFetchAllServicesSQL),
		stmtFetchServicesForHost: prepxOrPanic(db.Rox, stmtFetchServicesForHostSQL),
		stmtInsertClusterService: prepnOrPanic(db.Rwx, stmtInsertClusterServiceSQL),
	}
	preconditions.ValidateStruct(ds, preconditions.NoNilPointers)
	return ds, nil
}

type conductorDS struct {
	logger.ContextL
	metrics metric.DBStore

	stmtListCompanies        *sqlx.Stmt // Get a list of all companies regardless of policies defined.
	stmtFetchShadowCompanyID *sqlx.Stmt
	stmtFetchDeviceAlertRows *sqlx.Stmt
	stmtUpdateDeviceAlertRow *sqlx.NamedStmt
	stmtFetchHostPoolEntries *sqlx.Stmt
	stmtInsertHostPoolEntry  *sqlx.NamedStmt
	stmtFetchPipelines       *sqlx.Stmt
	stmtInsertPipeline       *sqlx.NamedStmt
	stmtFetchAllServices     *sqlx.Stmt
	stmtFetchServicesForHost *sqlx.Stmt
	stmtInsertClusterService *sqlx.NamedStmt

	shadowCompanyID kt.Cid
}

func (ds *conductorDS) Close() { database.CloseStatements(ds) }

const stmtListCompaniesSQL = `
	SELECT id FROM mn_company WHERE company_status = 'V'
`

func (ds *conductorDS) FetchAllActiveCompanies() ([]kt.Cid, error) {
	mt := ds.metrics.Get("FetchAllActiveCompanies", 0)
	start := time.Now()

	var allCids []kt.Cid
	err := ds.stmtListCompanies.Select(&allCids)

	mt.Done(start, err)
	return allCids, err
}

const stmtFetchShadowCompanyIDSQL = `
	SELECT id FROM mn_company
	WHERE company_status = 'V'
		AND company_name = 'kentikshadowpolicies'
`

func (ds *conductorDS) FetchShadowCompanyID() (cid kt.Cid, err error) {
	if ds.shadowCompanyID != 0 {
		return ds.shadowCompanyID, nil
	}
	defer func(start time.Time) { ds.metrics.Get("FetchShadowCompanyID", 0).Done(start, err) }(time.Now())
	err = ds.stmtFetchShadowCompanyID.Get(&ds.shadowCompanyID)
	return ds.shadowCompanyID, err
}

const stmtFetchDeviceAlertRowsSQL = `
	SELECT id, company_id, device_alert, edate
	FROM mn_device
	WHERE company_id = ANY($1)
`

func (ds *conductorDS) FetchDeviceAlertRows(cids []kt.Cid) (dars []*conductor.DeviceAlertRow, err error) {
	defer func(start time.Time) { ds.metrics.Get("FetchDeviceAlertRows", 0).Done(start, err) }(time.Now())

	err = ds.stmtFetchDeviceAlertRows.Select(&dars, pq.Array(cids))

	return dars, err
}

const stmtUpdateDeviceAlertRowSQL = `
	UPDATE mn_device
	SET device_alert = :device_alert, edate = :edate
	WHERE id = :id AND company_id = :company_id
`

func (ds *conductorDS) UpdateDeviceAlertRows(dars []*conductor.DeviceAlertRow) (err error) {
	mt := ds.metrics.Get("stmtUpdateDeviceAlertRow", 0)

	for _, dar := range dars {
		start := time.Now()
		// FIXME: This is serial. Perhaps update in a batch. However, not expecting this often.
		_, err = ds.stmtUpdateDeviceAlertRow.Exec(dar)
		if err != nil {
			mt.Error(start)
			return err
		}
		mt.Success(start)
	}

	return nil
}

const stmtFetchHostPoolEntriesSQL = `
	SELECT revision_id, cdate, host, status, attributes
	FROM mn_alerting_pool
	WHERE revision_id IN (
		SELECT MAX(revision_id)
		FROM mn_alerting_pool ap
		GROUP BY (ap.host)
	)
`

func (ds *conductorDS) GetHostPool() (hpes []conductor.HostPoolEntry, err error) {
	defer func(start time.Time) { ds.metrics.Get("GetHostPool", 0).Done(start, err) }(time.Now())

	err = ds.stmtFetchHostPoolEntries.Select(&hpes)
	if err != nil {
		return nil, err
	}
	for i := range hpes {
		err = conductor.FixupHostPoolEntry(&hpes[i])
		if err != nil {
			return nil, err
		}
	}
	return
}

const stmtInsertHostPoolEntrySQL = `
	INSERT INTO mn_alerting_pool
	(host, status, attributes)
	VALUES
	(:host, :status, :attributes)
`

func (ds *conductorDS) AppendHostPoolEntry(hpe *conductor.HostPoolEntry) (err error) {
	defer func(start time.Time) { ds.metrics.Get("AppendHostPoolEntry", 0).Done(start, err) }(time.Now())

	err = conductor.SerializeHostPoolEntryFields(hpe)
	if err != nil {
		return
	}
	_, err = ds.stmtInsertHostPoolEntry.Exec(hpe)

	return
}

const stmtFetchPipelinesSQL = `
	SELECT revision_id, cdate, pipeline_type, pipeline_id, pipeline_hosts, status, attributes
	FROM mn_alerting_cluster_pipeline
	WHERE revision_id IN (
		SELECT MAX(revision_id)
		FROM mn_alerting_cluster_pipeline acp
		GROUP BY (acp.pipeline_type, acp.pipeline_id)
	)
`

func (ds *conductorDS) FetchPipelines() (uint64, []*conductor.PipelineRow, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchPipelines", 0).Done(start, err) }(time.Now())

	var pipelines []*conductor.PipelineRow
	err = ds.stmtFetchPipelines.Select(&pipelines)
	if err != nil {
		return 0, nil, err
	}
	if len(pipelines) == 0 {
		return 0, nil, nil
	}

	for _, p := range pipelines {
		err := conductor.FixupPipelineRow(p)
		if err != nil {
			return 0, pipelines, err
		}
	}

	maxID := pipelines[0].RevisionID
	for i := 1; i < len(pipelines); i++ {
		if pipelines[i].RevisionID > maxID {
			maxID = pipelines[i].RevisionID
		}
	}
	return maxID, pipelines, nil
}

const stmtInsertPipelineSQL = `
	INSERT INTO mn_alerting_cluster_pipeline
	(pipeline_type, pipeline_id, pipeline_hosts, status, attributes)
	VALUES
	(:pipeline_type, :pipeline_id, :pipeline_hosts, :status, :attributes)
`

func (ds *conductorDS) AppendPipelines(pipelines []*conductor.PipelineRow) error {
	// Populate JSON columns.
	for _, p := range pipelines {
		err := conductor.SerializePipelineRowFields(p)
		if err != nil {
			return err
		}
	}

	mt := ds.metrics.Get("stmtInsertPipeline", 0)

	for _, p := range pipelines {
		start := time.Now()
		// FIXME: could do in single sql statement. But this way is easy.
		_, err := ds.stmtInsertPipeline.Exec(p)
		if err != nil {
			mt.Error(start)
			return err
		}
		mt.Success(start)
	}

	return nil
}

const stmtFetchAllServicesSQL = `
	SELECT revision_id, cdate, host, service_type, service_id, status, attributes
	FROM mn_alerting_cluster_service
	WHERE revision_id IN (
		SELECT MAX(revision_id)
		FROM mn_alerting_cluster_service acs
		GROUP BY (acs.service_type, acs.service_id)
	)
`

func (ds *conductorDS) FetchAllServices() (uint64, []*conductor.ClusterServiceRow, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchAllServices", 0).Done(start, err) }(time.Now())

	var csrs []*conductor.ClusterServiceRow
	err = ds.stmtFetchAllServices.Select(&csrs)
	if err != nil {
		return 0, nil, err
	}
	if len(csrs) == 0 {
		return 0, nil, nil
	}
	for _, csr := range csrs {
		err = conductor.FixupClusterServiceRow(csr)
		if err != nil {
			return 0, nil, err
		}
	}

	return getMaxIDNonEmptyArray(csrs), csrs, nil
}

const stmtInsertClusterServiceSQL = `
	INSERT INTO mn_alerting_cluster_service
	(host, service_type, service_id, status, attributes)
	VALUES
	(:host, :service_type, :service_id, :status, :attributes)
`

func (ds *conductorDS) AppendServices(csrs []*conductor.ClusterServiceRow) error {
	// Populate JSON columns.
	for _, csr := range csrs {
		err := conductor.SerializeClusterServiceRowFields(csr)
		if err != nil {
			return err
		}
	}

	mt := ds.metrics.Get("stmtInsertClusterService", 0)

	for _, csr := range csrs {
		start := time.Now()
		// FIXME: could do in single sql statement. But this way is easy.
		_, err := ds.stmtInsertClusterService.Exec(csr)
		if err != nil {
			mt.Error(start)
			return err
		}
		mt.Success(start)
	}

	return nil
}

const stmtFetchServicesForHostSQL = `
	SELECT revision_id, cdate, host, service_type, service_id, status, attributes
	FROM mn_alerting_cluster_service
	WHERE revision_id IN (
		SELECT MAX(revision_id)
		FROM mn_alerting_cluster_service acs
		GROUP BY (acs.service_type, acs.service_id)
	)
		AND host = $1
`

func (ds *conductorDS) FetchServicesForHost(hostname string) (uint64, []*conductor.ClusterServiceRow, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("FetchServicesForHost", 0).Done(start, err) }(time.Now())

	var csrs []*conductor.ClusterServiceRow
	err = ds.stmtFetchServicesForHost.Select(&csrs, hostname)
	if err != nil {
		return 0, nil, err
	}
	if len(csrs) == 0 {
		return 0, nil, nil
	}
	for _, csr := range csrs {
		err = conductor.FixupClusterServiceRow(csr)
		if err != nil {
			return 0, nil, err
		}
	}
	return getMaxIDNonEmptyArray(csrs), csrs, nil
}

func getMaxIDNonEmptyArray(csrs []*conductor.ClusterServiceRow) uint64 {
	// precondition: array is not empty
	maxID := csrs[0].RevisionID
	for i := 1; i < len(csrs); i++ {
		if csrs[i].RevisionID > maxID {
			maxID = csrs[i].RevisionID
		}
	}
	return maxID
}
