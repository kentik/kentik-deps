// +build integration_test

package ktds

import (
	"fmt"
	"testing"

	"github.com/kentik/chf-alert/pkg/kt"
)

func TestIntegrationAlertDS(t *testing.T) {
	l := newLogForTesting(t)
	chwwwDB := newCHWWWSQLDBForTesting(t)
	ds := newAlertDSForTesting(t, l, chwwwDB)
	cid := kt.Cid(1013)

	policies, err := ds.GetAlertPolicies(&cid, nil, nil)
	if err != nil {
		t.Fatalf("GetAlertPolicies: %v", err)
	}

	for _, policy := range policies {
		fmt.Printf("%+v\n", policy)
	}
}
