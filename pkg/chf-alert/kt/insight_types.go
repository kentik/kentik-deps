package kt

import (
	"time"
)

type FetchCompanyInsightsFilter struct {
	Cid

	ExcludeUserDefined bool

	DoAlarmIDs bool
	AlarmIDs   []AlarmID

	DoPolicyIDs       bool
	PolicyIDs         []PolicyID
	DoIgnorePolicyIDs bool
	IgnorePolicyIDs   []PolicyID

	DoThresholdIDs bool
	ThresholdIDs   []Tid

	DoMitigationIDs bool
	MitigationIDs   []MitigationID

	DoStates bool
	States   []string

	DimensionFiltersDNF []map[string]string

	IncludeActive  bool
	IncludeHistory bool
	Start          time.Time
	End            time.Time
	Limit          int
	Offset         int

	// calculated fields
	DimensionFiltersDNFArray [][]StringPair
}

type StringPair struct{ Key, Value string }

func StringMapToArray(m map[string]string) []StringPair {
	a := make([]StringPair, 0, len(m))
	for k, v := range m {
		a = append(a, StringPair{Key: k, Value: v})
	}
	return a
}
