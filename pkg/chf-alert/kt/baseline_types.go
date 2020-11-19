package kt

import (
	"fmt"
	"time"
)

// A BaselineValue corresponds to a row in mn_alert_baseline_values
// It represents a single data point in the baseline for its
// policy and company.
type BaselineValue struct {
	// database fields
	PolicyID        PolicyID  // mn_alert_baseline.alert_id
	CompanyID       Cid       // mn_alert_baseline.company_id
	TimeStart       time.Time // mn_alert_baseline.time_start
	TimeEnd         time.Time // mn_alert_baseline.time_start + mn_alert_baseline.width_seconds
	AlertKey        string    // mn_alert_baseline_values.alert_key
	AlertDimension  string    // mn_alert_baseline_values.alert_dimension
	AlertMetric     string    // mn_alert_baseline_values.alert_metric
	AlertValueMin   float64   // mn_alert_baseline_values.alert_value_min
	AlertValueMax   float64   // mn_alert_baseline_values.alert_value_max
	AlertValueCount int       // mn_alert_baseline_values.alert_value_count
	AlertValue05    float64   // mn_alert_baseline_values.alert_value_05
	AlertValue25    float64   // mn_alert_baseline_values.alert_value_25
	AlertValue50    float64   // mn_alert_baseline_values.alert_value_50
	AlertValue95    float64   // mn_alert_baseline_values.alert_value_95
	AlertValue98    float64   // mn_alert_baseline_values.alert_value_98

	// stateful fields, initially empty
	Policy         string
	Debug          bool
	SortedPosition int
}

func (b *BaselineValue) String() string {
	return fmt.Sprintf("bl ts=%s cid=%d pid=%d dim=%s met==%s, key=%s, p95=%f", b.TimeStart, b.CompanyID, b.PolicyID, b.AlertDimension, b.AlertMetric, b.AlertKey, b.AlertValue95)
}

type CidWithCount struct {
	Cid   Cid `db:"company_id"`
	Count int `db:"count"`
}

type TableInfo struct {
	TableName    string `db:"table_name"`
	TableRows    int    `db:"table_rows"`
	AvgRowLength int    `db:"avg_row_length"`
	DataLength   int    `db:"data_length"`
	IndexLength  int    `db:"index_length"`
}

const (
	// BaselineSeasonalityHourlySeconds a
	BaselineSeasonalityHourlySeconds = 60 * 60
	// BaselineSeasonalityDailySeconds b
	BaselineSeasonalityDailySeconds = BaselineSeasonalityHourlySeconds * 24
	// BaselineSeasonalityWeeklySeconds c
	BaselineSeasonalityWeeklySeconds = BaselineSeasonalityDailySeconds * 7
)
