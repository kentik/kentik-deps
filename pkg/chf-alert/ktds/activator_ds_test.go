package ktds

import (
	"fmt"
	"testing"

	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/stretchr/testify/assert"

	"github.com/xo/dburl"
)

func TestFixupThresholdCondition(t *testing.T) {
	tcs := []struct {
		conditionsJSON     string
		expectedOK         bool
		expectedFirstValue float64
	}{
		{`[{"value": 99}]`, true, 99},
		{`[{"value": 99.5}]`, true, 99.5},
		{`[{"value": 99.999}]`, true, 99.999},
		{`[{"value": 99.0}]`, true, 99.0},
		{`[{"value": 20.0}]`, true, 20.0},
		{`[{"value": 0.001}]`, true, 0.001},
		{`[{"value": "99"}]`, false, 0}, // Please no.
	}

	for _, tc := range tcs {
		threshold := &kt.Threshold{ConditionsJSON: tc.conditionsJSON}
		err := FixupThreshold(threshold)
		if tc.expectedOK {
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedFirstValue, threshold.Conditions[0].Value)
		} else {
			assert.Error(t, err)
		}
	}
}

func TestLol(t *testing.T) {
	u, _ := dburl.Parse("postgresql://user:pass@localhost/mydatabase/?sslmode=disable")
	fmt.Println(u.Driver, u.Host, u.Path)
}
