// +build integration_test

package ktds

import (
	"context"
	"fmt"
	"testing"

	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/kentik/eggs/pkg/olly"

	"github.com/kentik/eggs/pkg/logger"
	ltesting "github.com/kentik/eggs/pkg/logger/testing"

	_ "github.com/kentik/go-mysql-fork/mysql" // Load mysql driver
	_ "github.com/lib/pq"                     // Load postgres driver
)

func getActivatorDSForTesting(t *testing.T) (*activatorDataSource, logger.Underlying) {

	pg, err := OpenPostgresDBPair(context.TODO(), pgConnForTesting, pgConnForTesting, DefaultMaxOpenConns)
	if err != nil {
		t.Fatalf("pg err %v", err)
	}

	mysql, err := OpenMysqlDBPair(context.TODO(), alertConnForTesting, alertConnForTesting, DefaultMaxOpenConns)
	if err != nil {
		t.Fatalf("mysql err %v", err)
	}

	log := &ltesting.Test{T: t}
	activatorDS, err := NewActivatorDataSource(pg, mysql, alertConnForTesting, log, olly.NewBuilder())
	if err != nil {
		t.Fatalf("activatorDS err %v", err)
	}

	return activatorDS.(*activatorDataSource), log
}

func TestIntegrationActivatorDS(t *testing.T) {
	ds, _ := getActivatorDSForTesting(t)
	cid := kt.Cid(1013)
	policyID := kt.PolicyID(4)

	thresholds, err := ds.LoadCompanyThreshold(cid, nil, policyID, kt.ActiveStatus)
	if err != nil {
		t.Fatalf("LoadCompanyThreshold: %v", err)
	}

	for _, threshold := range thresholds {
		fmt.Printf("%+v\n", threshold)
	}
}

func TestIntegrationGetDebugKeys(t *testing.T) {
	ds, _ := getActivatorDSForTesting(t)
	cid := kt.Cid(1013)
	policyID := kt.PolicyID(4380)
	thresholdID := kt.Tid(7435)

	debugKeys, err := ds.GetDebugKeys(cid, &policyID, &thresholdID)
	if err != nil {
		t.Fatalf("GetDebugKeys: %v", err)
	}

	t.Logf("GetDebugKeys len=%d", len(debugKeys))

	debugKeys, err = ds.GetDebugKeys(cid, &policyID, nil)
	if err != nil {
		t.Fatalf("GetDebugKeys: %v", err)
	}

	t.Logf("GetDebugKeys (nil threshold) len=%d", len(debugKeys))
}

func TestIntegrationFetchMariaDBTableInfos(t *testing.T) {
	ds, _ := getActivatorDSForTesting(t)

	infos, err := ds.FetchMariaDBTableInfos()
	if err != nil {
		t.Fatalf("FetchMariaDBTableInfos: %v", err)
	}

	t.Logf("infos: %+v", infos)
}

func TestIntegrationFetchMariaDBTableOldestRowAges(t *testing.T) {
	ds, _ := getActivatorDSForTesting(t)

	ages, err := ds.FetchMariaDBTableOldestRowAges()
	if err != nil {
		t.Fatalf("FetchMariaDBTableOldestRowAges: %v", err)
	}

	t.Logf("ages: %+v", ages)
}
