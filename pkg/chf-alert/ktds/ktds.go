// Package ktds is the Kentik data layer for the streaming codebase.
package ktds

import (
	"context"
	"time"

	"github.com/kentik/chf-alert/pkg/alert/appproto"
	"github.com/kentik/chf-alert/pkg/eggs/conductor"
	"github.com/kentik/chf-alert/pkg/eggs/state"
	"github.com/kentik/chf-alert/pkg/kt"
	"github.com/kentik/chf-alert/pkg/services/mitigate/platform"
)

// go get github.com/golang/mock/mockgen
//go:generate mockgen -package=mocks -destination=mocks/$GOFILE github.com/kentik/chf-alert/pkg/ktds AlarmDataSource,AlertDataSource,ActivatorDataSource,BatchingAlarmDataSource,MitigateDataSource,ThresholdDataSource,DataSource

type DataSource interface {
	Close()
	MitigateDataSourceNoClose
	ActivatorDataSourceNoClose
	AlarmDataSourceNoClose
	AlertDataSourceNoClose
	BatchingAlarmDataSourceNoClose
	DeviceDataSourceNoClose
	CredentialsDataSourceNoClose
	ConductorDataSourceNoClose
}

type MitigateDataSource interface {
	Close()
	MitigateDataSourceNoClose
}

type MitigateDataSourceNoClose interface {
	Load(cid kt.Cid, tid kt.Tid) ([]*kt.ThresholdMitigation, error)
	LoadBulk(kt.Cid, []kt.Tid) ([]*kt.ThresholdMitigation, error)

	GetTotalMitigations() (int, error)
	GetMitigation(context.Context, kt.Cid, kt.MitigationID) (*kt.MitigationRepr, error)
	GetMitigationBulk(context.Context, kt.Cid, []kt.MitigationID) (map[kt.MitigationID]*kt.MitigationRepr, error)
	GetMitigationPlatforms(context.Context, kt.Cid, []kt.MitigationPlatformID) (map[kt.MitigationPlatformID]*kt.MitigationPlatform, error)
	GetMitigationMethods(context.Context, kt.Cid, []kt.MitigationMethodID) (map[kt.MitigationMethodID]*kt.MitigationMethod, error)
	FindMitigationsWithCount(context.Context, *kt.FindMitigationsFilter) ([]*kt.MitigationRepr, uint64, error)

	// rtbh and flowspec
	platform.BGPDataSource

	// Mitigate V2
	GetNextMitigationID(kt.Cid) (kt.MitigationID, error)
	SetNextMitigationID(kt.Cid, kt.MitigationID) error
	GetStateMachineStore(kt.Cid) state.MachineStoreAndEventListener

	GetMitigationHistoryV2Count(cid kt.Cid, request *kt.MitigationHistoryRequest) (int, error)
	GetMitigationHistoryV2EndWaitCount(cid kt.Cid, request *kt.MitigationHistoryRequest) (int, error)
	GetMitigationHistoryV2(cid kt.Cid, request *kt.MitigationHistoryRequest) ([]kt.MitigationHistoryEvent, error)

	GetCompaniesWithActiveMitigations(context.Context) ([]kt.Cid, error)

	GetPlatformMethodPairsBulk(context.Context, kt.Cid, []kt.MitigationPlatformID, []kt.MitigationMethodID) (map[kt.PlatMethodIDTuple]*kt.MitigationPlatformMethodPair, error)
	CreateThresholdMitigations(context.Context, kt.Cid, []*kt.ThresholdMitigationShort) error
	CreateThresholdMitigationsTx(context.Context, kt.ChwwwTransaction, kt.Cid, []*kt.ThresholdMitigationShort) error
	GetThresholdMitigations(context.Context, kt.Cid, []kt.Tid) (map[kt.Tid][]*kt.ThresholdMitigationWithIDs, error)

	CreatePlatformTx(context.Context, kt.ChwwwTransaction, kt.Cid, kt.PlatformUpdateBundle) (*kt.MitigationPlatform, error)
	GetPlatform(context.Context, kt.Cid, kt.MitigationPlatformID) (*kt.MitigationPlatform, error)
	DeletePlatform(context.Context, kt.Cid, kt.MitigationPlatformID) error
	UpdatePlatformTx(context.Context, kt.ChwwwTransaction, kt.Cid, kt.MitigationPlatformID, kt.PlatformUpdateBundle) (*kt.MitigationPlatform, error)
	FindPlatformsWithCount(context.Context, *kt.FindMitigationPlatformsFilter) ([]*kt.MitigationPlatform, uint64, error)
	GetAllPlatforms(cid kt.Cid) ([]*kt.MitigationPlatform, error)

	AssociateMethodsWithPlatformTx(context.Context, kt.ChwwwTransaction, kt.Cid, kt.MitigationPlatformID, []kt.MitigationMethodID) error
	GetAssociatedMethods(context.Context, kt.Cid, []kt.MitigationPlatformID) (map[kt.MitigationPlatformID][]kt.MitigationMethodID, error)
	ValidatePlatformMethodPair(context.Context, kt.Cid, kt.MitigationPlatformID, kt.MitigationMethodID) (bool, error)

	CreateMethodTx(context.Context, kt.ChwwwTransaction, kt.Cid, kt.MethodUpdateBundle) (*kt.MitigationMethod, error)
	GetMethod(context.Context, kt.Cid, kt.MitigationMethodID) (*kt.MitigationMethodWithNotifChans, error)
	DeleteMethod(context.Context, kt.Cid, kt.MitigationMethodID) error
	UpdateMethodTx(context.Context, kt.ChwwwTransaction, kt.Cid, kt.MitigationMethodID, kt.MethodUpdateBundle) (*kt.MitigationMethod, error)
	FindMethodsWithCount(context.Context, *kt.FindMitigationMethodsFilter) ([]*kt.MitigationMethodWithNotifChans, uint64, error)
	GetAllMethods(cid kt.Cid) ([]*kt.MitigationMethod, error)

	ValidateNotificationChannels(context.Context, kt.Cid, []kt.NotificationChannelID) (bool, error)
	UpdateNotificationChannelsToMethodAssociationTx(context.Context, kt.ChwwwTransaction, kt.Cid, kt.MitigationMethodID, []kt.NotificationChannelID, kt.NotifChannelsWrapped) error
}

type ActivatorDataSource interface {
	Close()
	ActivatorDataSourceNoClose
}

type ActivatorDataSourceNoClose interface {
	ThresholdDataSourceNoClose
	LoadCachedActivationTimes(cid kt.Cid, policyID kt.PolicyID, lastCheck time.Time) (map[kt.Tid]map[string][]int64, error)
	DownloadBaseline(cid kt.Cid, policyID kt.PolicyID, startTime time.Time) ([]*kt.BaselineValue, bool, error)
	TestBaseline(method string, cid kt.Cid, policyID kt.PolicyID, startTime time.Time) ([]*kt.BaselineValue, bool, error)
	UploadBaseline(policyID kt.PolicyID, cid kt.Cid, time_start time.Time, time_end time.Time, alert_key string, alert_dimension string, alert_metric string, alert_value_min float64, alert_value_max float64, alert_value_count int64, alert_value_98 float64, alert_value_95 float64, alert_value_50 float64, alert_value_25 float64, alert_value_05 float64, alert_position int) error
	FetchBackfillCompletionTimes(kt.Cid, time.Time) (map[kt.PolicyID]time.Time, error)
	GetBaselineTableInfo() (kt.TableInfo, error)
	GetBaselineTableRowsByCompany() ([]kt.CidWithCount, error)
	GetBaselineTableRows(kt.Cid, kt.PolicyID) (int, error)

	FetchMariaDBTableInfos() (map[string]kt.TableInfo, error)
	FetchMariaDBTableOldestRowAges() (map[string]time.Duration, error)

	ActivatorDebugKeysDataSource
}

type ThresholdDataSource interface {
	Close()
	ThresholdDataSourceNoClose
}

type ThresholdDataSourceNoClose interface {
	LoadCompanyThreshold(cid kt.Cid, lastCheck *time.Time, policyID kt.PolicyID, status string) ([]*kt.Threshold, error)
	LoadThresholdsBulk(context.Context, kt.Cid, []kt.ThresholdStatus, []kt.PolicyID) ([]*kt.Threshold, error)
	LoadAllActiveThresholdsForCompany(kt.Cid) (map[kt.Tid]*kt.Threshold, error)
	GetThresholdsWithArchived(context.Context, kt.Cid, []kt.Tid) (map[kt.Tid]*kt.Threshold, error)
	GetThresholdsForPolicies(context.Context, kt.Cid, []kt.PolicyID) (map[kt.PolicyID][]*kt.Threshold, error)
	CreateThreshold(context.Context, *kt.Threshold) (*kt.Threshold, error)
	CreateThresholdTx(context.Context, kt.ChwwwTransaction, *kt.Threshold) (*kt.Threshold, error)
	UpdateThresholds(context.Context, kt.Cid, kt.PolicyID, []*kt.Threshold, []*kt.Threshold) ([]*kt.Threshold, error)
	UpdateThresholdsTx(context.Context, kt.ChwwwTransaction, kt.Cid, kt.PolicyID, []*kt.Threshold, []*kt.Threshold) ([]*kt.Threshold, error)
}

type ActivatorDebugKeysDataSource interface {
	GetDebugKeys(kt.Cid, *kt.PolicyID, *kt.Tid) ([]kt.AlertingDebugKey, error)
}

type AlarmDataSource interface {
	Close()
	AlarmDataSourceNoClose
}

type AlarmDataSourceNoClose interface {
	GetAllAlarms(cid kt.Cid, policyID kt.PolicyID, tid kt.Tid) ([]*kt.AlarmEvent, error)
	GetAlarmsForMitigationStart(cid kt.Cid, policyID kt.PolicyID, tid kt.Tid) ([]*kt.AlarmEvent, error)
	GetAlarmsForMitigationStop(cid kt.Cid, policyID kt.PolicyID, tid kt.Tid) ([]*kt.AlarmEvent, error)
	MarkMitigationStart(alarmID kt.AlarmID) error
	MarkMitigationStop(alarmID kt.AlarmID) error

	AssociateAlarmWithMitigation(kt.Cid, kt.AlarmID, kt.MitigationID) error

	GetTotalActiveAlarms() (int, error)
	GetTotalActiveShadowAlarms() (int, error)
	GetTotalActiveAlarmsIncludingShadow() (int, error)
	GetTotalActiveAlarmsForCompany(kt.Cid) (int, error)

	RemoveAlarmMatchHistoricalEntries(retention time.Duration, rowsThreshold uint64) (uint64, error)
	RemoveClearedAlarmOldEntries(retention time.Duration, rowsThreshold uint64) (uint64, error)
	RemoveAlarmHistoryOldEntries(retention time.Duration, rowsThreshold uint64) (uint64, error)
	RemoveBaselineOldEntries(retention time.Duration, rowsThreshold uint64) (uint64, error)
	RemoveBaselineBackfillJobsOldEntries(retention time.Duration, rowsThreshold uint64) (uint64, error)
	RemoveMachineLogOldEntries(retention time.Duration, rowsThreshold uint64) (uint64, error)
	RemoveMachinesOldEntries(retention time.Duration, rowsThreshold uint64) (uint64, error)

	FetchCompanyInsights(*kt.FetchCompanyInsightsFilter) ([]*kt.AlarmEvent, error)
	GetCompanyInsightByID(kt.Cid, kt.AlarmID, bool) (*kt.AlarmEvent, error)

	GetAlarm(context.Context, kt.Cid, kt.AlarmID) (*kt.AlarmEvent, error)
	GetAlarmFromCurrent(context.Context, kt.Cid, kt.AlarmID) (*kt.AlarmEvent, error)
	GetAlarmFromHistory(context.Context, kt.Cid, kt.AlarmID) (*kt.AlarmEvent, error)
	GetAlarmsForThresholds(context.Context, kt.Cid, []kt.PolicyID, []kt.Tid) ([]*kt.AlarmEvent, error)
	FindAlarms(context.Context, *kt.GetAlarmsFilter) ([]*kt.AlarmEvent, error)
	GetAlarmStateTransitions(context.Context, kt.Cid, kt.AlarmID) ([]*kt.AlarmStateTransition, error)
	FindAssociatedPolicies(context.Context, kt.Cid, []kt.AlarmID) ([]kt.PolicyID, error)

	AckAlarmHistory(context.Context, *kt.AlarmEvent) error
	AckAlarmCurrent(context.Context, *kt.AlarmEvent) error

	FindAlarmsRelatedByKey(context.Context, kt.Cid, kt.PolicyID, string, string) ([]kt.AlarmID, error)
	GetOccurrencesForKey(context.Context, kt.Cid, kt.PolicyID, string, string) (uint64, error)
}

type BatchingAlarmDataSource interface {
	Close()
	BatchingAlarmDataSourceNoClose
}

type BatchingAlarmDataSourceNoClose interface {
	InsertAlertMatchHistoryRows([]kt.AlertMatchHistoryRow) error
	GetAlarmsSmallForPolicy(kt.Cid, kt.PolicyID) ([]kt.AlertAlarmSmall, error)
	GetAlarmEventsStarting(kt.Cid, kt.PolicyID) ([]*kt.AlarmEvent, error)
	GetAlarmEventsEscalating(kt.Cid, kt.PolicyID) ([]*kt.AlarmEvent, error)
	GetAlarmEventsStopping(kt.Cid, kt.PolicyID) ([]*kt.AlarmEvent, error)
	UpdateAlarmsNotifyStart(time.Time, kt.Cid, kt.PolicyID, ...kt.AlarmID) error
	UpdateAlarmsNotifyEnd(time.Time, kt.Cid, kt.PolicyID, ...kt.AlarmID) error
	RunAlarmStatementsBatch(statements []interface{}) error
}

type AlertDataSource interface {
	Close()
	AlertDataSourceNoClose
}

type AlertDataSourceNoClose interface {
	NewChwwwTransaction() (kt.ChwwwTransaction, error)

	CreatePolicy(context.Context, *kt.AlertPolicy) (*kt.AlertPolicy, error)
	CreatePolicyTx(context.Context, kt.ChwwwTransaction, *kt.AlertPolicy) (*kt.AlertPolicy, error)
	UpdatePolicy(context.Context, kt.Cid, kt.PolicyID, kt.UserID, kt.PolicyUpdateBundle) (*kt.AlertPolicy, error)
	UpdatePolicyTx(context.Context, kt.ChwwwTransaction, kt.Cid, kt.PolicyID, kt.UserID, kt.PolicyUpdateBundle) (*kt.AlertPolicy, error)
	GetAlertPoliciesMap(cid *kt.Cid, policyID *kt.PolicyID, lastCheck time.Time) (map[kt.PolicyID]*kt.AlertPolicy, error)
	GetAlertPolicies(cid *kt.Cid, lastCheck *time.Time, policyID *kt.PolicyID) ([]kt.AlertPolicy, error)
	GetAlertPoliciesDone(cid kt.Cid, lastCheck time.Time, policyID kt.PolicyID) ([]kt.InactiveAlertPolicy, []kt.InactiveAlertPolicy, error)
	GetAlertPoliciesBulk(cidList ...kt.Cid) (map[kt.Cid]map[kt.PolicyID]*kt.AlertPolicy, error)
	FetchSavedFiltersFlat(cid kt.Cid) (map[kt.PolicyID][]*kt.CompanyFilterBase, error)
	FetchSavedFilters(ctx context.Context, cid kt.Cid) (map[kt.SavedFilterID]*kt.SavedFilter, error)

	// TODO(tjonak): bulk load all of that data and derive results in application layer
	LoadAppProtocolMappings(context.Context) (map[string]uint64, error)
	LoadAppProtocolMetrics(context.Context) ([]appproto.Metric, error)
	LoadAppProtocolDimensions(context.Context) ([]appproto.Tuple, error)
	LoadNonSTDAppProtocols(context.Context) ([]kt.AppProtoID, error)
	LoadAppProtocolSNMPBundle(context.Context) (*kt.SNMPBundle, error)

	MarkPolicyBad(policyID kt.PolicyID) error
	GetLocalCompanies(gna GetNodeAddress, isAllLocal bool, cidWhitelist map[kt.Cid]bool, cidBlacklist map[kt.Cid]bool) ([]kt.Cid, error)
	GetOidFromUserId(userId uint64) (uint64, error)
	GetPolicy(context.Context, kt.Cid, kt.PolicyID) (*kt.AlertPolicy, error)

	FindPolicies(context.Context, *kt.FindPoliciesFilter) (map[kt.PolicyID]*kt.AlertPolicy, error)
	FindPoliciesWithCount(context.Context, *kt.FindPoliciesFilter) (map[kt.PolicyID]*kt.AlertPolicy, uint64, error)

	DeletePolicy(context.Context, kt.Cid, kt.PolicyID) (uint64, error)
	EnablePolicy(context.Context, kt.Cid, kt.PolicyID) error
	DisablePolicy(context.Context, kt.Cid, kt.PolicyID) error

	MutePolicy(context.Context, kt.Cid, kt.PolicyID, time.Time) error
	UnmutePolicy(context.Context, kt.Cid, kt.PolicyID) error

	AllPoliciesExist(context.Context, kt.Cid, []kt.PolicyID) (ok bool, err error)

	AddSelectedDevicesToPolicies(context.Context, kt.Cid, map[kt.PolicyID]*kt.AlertPolicy) error
	GetSelectedDevicesForCompanyFromStore(context.Context, kt.Cid) (map[kt.PolicyID]*kt.DeviceIDSet, error)

	FetchUserIDAndHydraForCompany(context.Context, kt.Cid) (kt.UserID, string, error)

	GetSavedFilter(context.Context, kt.Cid, kt.SavedFilterID) (*kt.SavedFilter, error)
	DeleteSavedFilter(context.Context, kt.Cid, kt.SavedFilterID) error
	CreateSavedFilter(context.Context, kt.Cid, kt.SavedFilterUpdateBundle) (*kt.SavedFilter, error)
	UpdateSavedFilter(context.Context, kt.Cid, kt.SavedFilterID, kt.SavedFilterUpdateBundle) (*kt.SavedFilter, error)
	FindSavedFiltersWithCount(context.Context, kt.Cid, *kt.FindSavedFiltersCriteria) ([]*kt.SavedFilter, uint64, error)
}

type ConductorDataSource interface {
	Close()
	ConductorDataSourceNoClose
}

type ConductorDataSourceNoClose interface {
	FetchAllActiveCompanies() ([]kt.Cid, error)
	FetchShadowCompanyID() (kt.Cid, error)
	FetchDeviceAlertRows([]kt.Cid) ([]*conductor.DeviceAlertRow, error)
	UpdateDeviceAlertRows([]*conductor.DeviceAlertRow) error
	GetHostPool() ([]conductor.HostPoolEntry, error)
	AppendHostPoolEntry(*conductor.HostPoolEntry) error
	FetchPipelines() (uint64, []*conductor.PipelineRow, error)
	AppendPipelines([]*conductor.PipelineRow) error
	FetchAllServices() (uint64, []*conductor.ClusterServiceRow, error)
	AppendServices([]*conductor.ClusterServiceRow) error
	FetchServicesForHost(string) (uint64, []*conductor.ClusterServiceRow, error)
}

// implemented by i.e. shipper.ServiceHash
type GetNodeAddress interface {
	GetNodeAddress(kt.IntId) (string, error)
}

type DeviceDataSource interface {
	Close()
	DeviceDataSourceNoClose
}

type DeviceDataSourceNoClose interface {
	RefreshConfigDigests(int) error
	LoadConfigDigests(int) (map[kt.Cid]string, error)
	LoadInterfaces(kt.Cid) ([]*kt.Interface, error)
	DevicesFromInterfaces(kt.Cid, []*kt.Interface) (kt.Devices, error)
	LoadSites(kt.Cid) (map[int]*kt.Site, error)
	LoadCustomColumns(kt.Cid) (map[uint32]*kt.CustomColumn, error)
	LoadDeviceLabels(kt.Cid) (*kt.DeviceLabelsForCompany, error)
	LoadInterfaceGroups(kt.Cid) (*kt.InterfaceGroupsForCompany, error)
	LoadVRFData(kt.Cid) (map[kt.DeviceID][]*kt.VRFEntry, error)
	LoadDeviceSubtypeColumns(cid kt.Cid) (map[string]map[kt.DeviceID]bool, error)
	LoadDeviceSubtypeMappings(ctx context.Context) (map[kt.Cid]map[string][]kt.DeviceID, error)
	LoadFlexDimensions(ctx context.Context) ([]string, error)
	LoadFlexMetrics(ctx context.Context) ([]kt.FlexMetric, error)
	LoadAvailableBGPFilteringEnabledDevices(context.Context, kt.Cid) (*kt.DeviceIDSet, error)

	FetchDeviceShortDetails(context.Context, *kt.DeviceIDSet) (map[kt.DeviceID]kt.DeviceShortDetail, error)
	FetchSitesByName(context.Context, kt.Cid, []string) (map[string]kt.Site, error)
	FetchInterfacesByDeviceIDSNMPID(context.Context, kt.Cid, []kt.DeviceIDSNMPIDPair) (map[kt.DeviceIDSNMPIDPair]kt.Interface, error)
	FetchInterfacesByDeviceID(context.Context, *kt.DeviceIDSet) (map[string]kt.Interface, error)
	FetchASNs(context.Context, []int64) (map[int64]kt.ASN, error)

	LoadCompanySpecificDimensions(context.Context, kt.Cid) ([]string, error)
	IDsForDeviceNames(context.Context, kt.Cid, []string) (map[string]kt.DeviceID, error)
}

type CredentialsDataSource interface {
	Close()
	CredentialsDataSourceNoClose
}

type CredentialsDataSourceNoClose interface {
	GetAPICredentials(cid kt.Cid, userEmail string) (string, string, error)
}
