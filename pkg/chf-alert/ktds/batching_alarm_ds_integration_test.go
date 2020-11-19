// +build integration_test

package ktds

import (
	"context"
	"testing"
	"time"

	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/kentik/eggs/pkg/logger"
	ltesting "github.com/kentik/eggs/pkg/logger/testing"

	_ "github.com/kentik/go-mysql-fork/mysql" // Load mysql driver
	_ "github.com/lib/pq"                     // Load postgres driver
	"github.com/stretchr/testify/assert"
)

func getBatchingAlarmDSForTesting(t *testing.T) (*batchingAlarmDataSource, logger.Underlying) {
	log := &ltesting.Test{T: t}
	ds, err := NewBatchingAlarmDataSource(context.TODO(), alertConnForTesting, alertConnForTesting, 10, log)
	if err != nil {
		t.Fatalf("batchingAlarmDS err %v", err)
	}

	return ds.(*batchingAlarmDataSource), log
}

func TestIntegrationBatchingAlarmDS(t *testing.T) {
	// This test has db side effects!
	// This test doesn't check everything it needs to by itself;
	// it's useful for running manually.

	ds, _ := getBatchingAlarmDSForTesting(t)
	assert := assert.New(t)
	var err error

	cid := kt.Cid(1013)
	policyID := kt.PolicyID(1)
	thresholdID := kt.Tid(12345)
	alarmID := kt.AlarmID(100000)
	value := 123456.0
	now := time.Now()

	actions := []interface{}{
		kt.DeleteClearAlarmArgs{
			CompanyID:   cid,
			PolicyID:    policyID,
			ThresholdID: thresholdID,
			ID:          alarmID,
		},
		kt.UpdateAlarmLastMatchTimeArgs{
			AlertMatchCount: 1,
			LastMatchTime:   now,
			CompanyID:       cid,
			AlertID:         policyID,
			ThresholdID:     thresholdID,
			ID:              alarmID,
		},
		kt.UpdateAlarmForEscalationArgs{
			// Args left out here.
			CompanyID:   cid,
			AlertID:     policyID,
			ThresholdID: thresholdID,
			ID:          alarmID,
		},
		kt.UpdateAlarmEndingArgs{
			AlarmEnd:    now,
			AlarmState:  kt.AlarmStateClear,
			CompanyID:   cid,
			AlertID:     policyID,
			ThresholdID: thresholdID,
			ID:          alarmID,
		},
		kt.InsertAlarmArgs{
			// Args left out here.
			AlarmState:  kt.AlarmStateAlarm,
			AlertValue:  value,
			CompanyID:   cid,
			AlertID:     policyID,
			ThresholdID: thresholdID,
		},
		kt.InsertAlarmHistoryWithLastIDArgs{
			AlarmHistoryType: kt.AlarmHistoryStart,
			OldAlarmState:    kt.AlarmStateClear,
			NewAlarmState:    kt.AlarmStateAlarm,
		},
		kt.InsertAlarmHistoryArgs{
			CompanyID:   cid,
			AlertID:     policyID,
			ThresholdID: thresholdID,
			AlarmID:     alarmID,
		},
		kt.InsertAlarmHistoryCopyingStartArgs{
			AlarmID:       alarmID,
			OldAlarmState: kt.AlarmStateClear,
			NewAlarmState: kt.AlarmStateAlarm,
		},
	}
	err = ds.RunAlarmStatementsBatch(actions)
	assert.NoError(err)
}

/*
// Same thing, but as an executable.

package main

import (
	"fmt"

	"github.com/kentik/chf-alert/pkg/kt"
	"github.com/kentik/chf-alert/pkg/ktds"

	"github.com/kentik/golog/logger"

	_ "github.com/kentik/go-mysql-fork/mysql" // Load mysql driver
	_ "github.com/lib/pq"              // Load postgres driver
)

const alertConnForTesting = "alert_user:chfpassword@(127.0.0.1:3313)/chalert?multiStatements=true&interpolateParams=true"

func main() {
	ds := getBatchingAlarmDSForTesting()
	var err error

	actions := []interface{}{
		kt.InsertAlarmArgs{
			AlarmState: "ALARMTEST",
			AlertValue: 1234,
		},
		kt.InsertAlarmHistoryWithLastIDArgs{
			AlarmHistoryType: "TEST",
			OldAlarmState:    "TEST1",
			NewAlarmState:    "TEST2",
		},
	}
	fmt.Printf("running alarm statements batch\n")
	err = ds.RunAlarmStatementsBatch(actions)
	fmt.Printf("err: %+v\n", err)

	//user := "alert_user"
	//pass := "chfpassword"
	//addr := "127.0.0.1:3313"
	//dbname := "chalert"

	//db := mysql.New("tcp", "", addr, user, pass, dbname)

	//err = db.Connect()
	//if err != nil {
		//panic(err)
	//}

	//_, err = db.Prepare("INSERT INTO mn_alert_alarm (alarm_state) VALUES (?)")
	//if err != nil {
		//panic(err)
	//}
	//_, err = db.Prepare("INSERT INTO mn_alert_alarm_history (alarm_history_type, alarm_id, old_alarm_state, new_alarm_state) VALUES (?, LAST_INSERT_ID(), ?, ?)")
	//if err != nil {
		//panic(err)
	//}
	//stmt, err := db.Prepare("INSERT INTO mn_alert_alarm (alarm_state) VALUES (?) ; INSERT INTO mn_alert_alarm_history (alarm_history_type, alarm_id, old_alarm_state, new_alarm_state) VALUES (?, LAST_INSERT_ID(), ?, ?)")
	//if err != nil {
		//panic(err)
	//}
	//res, err := stmt.Run("foo", "test", "test1", "test2")
	//if err != nil {
		//panic(err)
	//}
	//fmt.Printf("res: %+v\n", res)

	logger.Drain()
}

func getBatchingAlarmDSForTesting() ktds.BatchingAlarmDataSource {
	fmt.Printf("new alarm ds\n")
	log := logger.New(logger.Levels.Debug)
	logger.SetStdOut()
	alarmDS, err := ktds.NewBatchingAlarmDataSource(alertConnForTesting, 10, log)
	if err != nil {
		panic(err)
	}

	fmt.Printf("got alarm ds\n")
	return alarmDS
}
*/
