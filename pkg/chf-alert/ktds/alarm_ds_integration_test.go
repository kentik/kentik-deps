// +build integration_test

package ktds

import (
	"context"
	"testing"

	"github.com/kentik/eggs/pkg/logger"
	ltesting "github.com/kentik/eggs/pkg/logger/testing"

	_ "github.com/kentik/go-mysql-fork/mysql" // Load mysql driver
	_ "github.com/lib/pq"                     // Load postgres driver
)

const pgConnForTesting = "user=www_user dbname=ch_www sslmode=disable host=127.0.0.1 port=5433 password=portalpassword"
const alertConnForTesting = "alert_user:chfpassword@(127.0.0.1:3313)/chalert?parseTime=true"

func getAlarmDSForTesting(t *testing.T) (*alarmDataSource, logger.Underlying) {
	mysql, err := OpenMysqlDBPair(context.TODO(), alertConnForTesting, alertConnForTesting, DefaultMaxOpenConns)
	if err != nil {
		t.Fatalf("mysql err %v", err)
	}

	log := &ltesting.Test{T: t}
	alarmDS, err := NewAlarmDataSource(mysql, log)
	if err != nil {
		t.Fatalf("alarmDS err %v", err)
	}

	return alarmDS.(*alarmDataSource), log
}
