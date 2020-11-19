// +build integration_test

package ktds

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kentik/chf-alert/pkg/eggs/state"
	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/kentik/eggs/pkg/logger"
	ltesting "github.com/kentik/eggs/pkg/logger/testing"

	_ "github.com/kentik/go-mysql-fork/mysql" // Load mysql driver
	_ "github.com/lib/pq"                     // Load postgres driver
	"github.com/stretchr/testify/assert"
)

func getThings(t *testing.T) (MitigateDataSource, logger.Underlying) {
	pgConn := "user=www_user dbname=ch_www sslmode=disable host=127.0.0.1 port=5433 password=portalpassword"
	alertConn := "alert_user:chfpassword@(127.0.0.1:3313)/chalert?parseTime=true"

	pg, err := OpenPostgresDBPair(context.TODO(), pgConn, pgConn, DefaultMaxOpenConns)
	if err != nil {
		t.Fatalf("pg err %v", err)
	}

	mysql, err := OpenMysqlDBPair(context.TODO(), alertConn, alertConn, DefaultMaxOpenConns)
	if err != nil {
		t.Fatalf("mysql err %v", err)
	}

	log := &ltesting.Test{T: t}
	mitigateDS, err := NewMitigateDataSource(pg, mysql, log, false)
	if err != nil {
		t.Fatalf("NewMitigateDataSource err: %v", err)
	}

	return mitigateDS, log
}

func TestGetCurrentRTBHCalls(t *testing.T) {
	ds, _ := getThings(t)
	assert := assert.New(t)
	cid := kt.Cid(1013)
	var calls []*kt.RTBHCall
	var err error

	getPrintCalls := func() {
		calls, err = ds.GetCurrentRTBHCalls(cid)
		assert.NoError(err)
		fmt.Printf("calls:\n")
		for _, c := range calls {
			fmt.Printf("%+v\n", c)
		}
	}

	getPrintCalls()

	dids := kt.NewDeviceIDSet(1001)
	cidr := "127.0.0.1/31"
	args := []byte("{}")
	assert.NoError(ds.MarkRTBHAnnounceForDevices(cid, dids, cidr, args, 1, 10001))

	getPrintCalls()

	assert.NoError(ds.MarkRTBHWithdrawForDevices(cid, dids, cidr, args, 1, 10001))

	getPrintCalls()
}

func TestGetDevicesAndBGPAddrs(t *testing.T) {
	ds, _ := getThings(t)
	_, err := ds.GetDevicesAndBGPAddrs(kt.Cid(1013))
	assert.NoError(t, err)
}

func TestMit2DBCalls(t *testing.T) {
	// Testing that the DB calls will execute in integration with the db.
	// Not testing that they return the right thing, etc.
	// This test may have db side effects.
	ds, _ := getThings(t)
	cid := kt.Cid(1013)
	assert := assert.New(t)
	var err error

	mid, err := ds.GetNextMitigationID(cid)
	assert.NoError(err)

	assert.NoError(ds.SetNextMitigationID(cid, mid))

	_, err = ds.GetCompaniesWithActiveMitigations(context.Background())
	assert.NoError(err)
	_, err = ds.GetTotalMitigations()
	assert.NoError(err)

	sms := ds.GetStateMachineStore(cid)
	machines, err := sms.LoadSerializedMachines()
	assert.NoError(err)
	assert.NoError(sms.SaveSerializedMachines(machines))

	assert.NoError(sms.HandleEvents(nil, &state.MachineEvents{Events: []*state.MachineTransitionEvent{&state.MachineTransitionEvent{}}}))

	histAssertNoErr := func(mhr *kt.MitigationHistoryRequest) {
		_, err = ds.GetMitigationHistoryV2Count(cid, mhr)
		assert.NoError(err)
		_, err = ds.GetMitigationHistoryV2EndWaitCount(cid, mhr)
		assert.NoError(err)
		_, err = ds.GetMitigationHistoryV2(cid, mhr)
		assert.NoError(err)
	}

	histAssertNoErr(&kt.MitigationHistoryRequest{})
	now := time.Now()
	histAssertNoErr(&kt.MitigationHistoryRequest{StartTime: &now})
	histAssertNoErr(&kt.MitigationHistoryRequest{EndTime: &now})

	for _, fb := range kt.MHFilterBys {
		histAssertNoErr(&kt.MitigationHistoryRequest{
			FilterBy:  fb,
			FilterVal: "asdf",
		})
	}

	for _, sb := range kt.MHSortBys {
		histAssertNoErr(&kt.MitigationHistoryRequest{
			SortBy: sb,
		})
	}

	for _, fb := range kt.MHFilterBys {
		for _, sb := range kt.MHSortBys {
			histAssertNoErr(&kt.MitigationHistoryRequest{
				FilterBy:  fb,
				FilterVal: "asdf",
				SortBy:    sb,
			})
		}
	}
}
