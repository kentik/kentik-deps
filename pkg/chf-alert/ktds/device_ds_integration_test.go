// +build integration_test

package ktds

import (
	"context"
	"testing"

	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/kentik/eggs/pkg/logger"
	ltesting "github.com/kentik/eggs/pkg/logger/testing"

	_ "github.com/lib/pq" // Load postgres driver
	"github.com/stretchr/testify/assert"
)

func TestLoadInterfaces(t *testing.T) {
	assert := assert.New(t)
	ds, log := getDeviceDS(t)
	cid := kt.Cid(1013)

	ints, err := ds.LoadInterfaces(cid)
	assert.NoError(err)
	for _, iface := range ints {
		log.Infof("", "iface %s: %+v", iface.SnmpID, iface)
	}
}

func TestLoadSites(t *testing.T) {
	assert := assert.New(t)
	ds, log := getDeviceDS(t)
	cid := kt.Cid(1013)

	sites, err := ds.LoadSites(cid)
	assert.NoError(err)
	for _, site := range sites {
		log.Infof("", "site %d: %+v", site.ID, site)
	}
}

func TestLoadCustomColumns(t *testing.T) {
	assert := assert.New(t)
	ds, log := getDeviceDS(t)
	cid := kt.Cid(1013)

	cols, err := ds.LoadCustomColumns(cid)
	assert.NoError(err)
	for id, col := range cols {
		log.Infof("", "col %d: %+v", id, col)
	}
}

func getDeviceDS(t *testing.T) (DeviceDataSource, logger.Underlying) {
	pgConn := "user=www_user dbname=ch_www sslmode=disable host=127.0.0.1 port=5433 password=portalpassword"

	pg, err := OpenPostgresDBPair(context.TODO(), pgConn, pgConn, DefaultMaxOpenConns)
	if err != nil {
		t.Fatalf("pg err %v", err)
	}

	log := &ltesting.Test{T: t}
	deviceDS, err := NewDeviceDataSource(pg, log)
	if err != nil {
		t.Fatalf("NewDeviceDataSource err: %v", err)
	}

	return deviceDS, log
}
