// +build integration_test

package ktds

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/kentik/chf-alert/pkg/chnodeapi"
	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/kentik/eggs/pkg/logger"
	ltesting "github.com/kentik/eggs/pkg/logger/testing"
)

// Usage:
// go test github.com/kentik/chf-alert/pkg/ktds -c -tags integration_test && scp ktds.test vpn.our1.kentik.com:
// set up env vars in ktds.test.sh, and run it:
/*
#!/bin/bash
# testing run script for ktds.test
# Jack Bearheart

KENTIK_PG_PASS="$(sudo bash -c "source /etc/profile.d/kentik-env.sh && echo \${KENTIK_PG_PASS}")"
HAPROXYBE_HOST="$(head -n1 /data/hostlists/postgres_portal_read)"
export PG_CONNECTION="user=www_user dbname=ch_www sslmode=disable host=${HAPROXYBE_HOST} port=5434 password=${KENTIK_PG_PASS}"
K_ENV="$(sudo bash -c "source /etc/profile.d/kentik-env.sh && echo \${PUPPET_ENV}")"
export CHNODE_URL="https://api.${K_ENV}.kentik.com"
[[ "$K_ENV" == "production" ]] && export CHNODE_URL="https://api.kentik.com"
[[ "$K_ENV" == "fra1" ]] && export CHNODE_URL="https://api.kentik.eu"
export CHNODE_SYSTEM_USER_EMAIL="system@kentik.com"

./ktds.test \
  -test.run TestIntegrationFetchSelectedDevicesForCompany \
  -test.count=1 \
  -test.v
*/
func TestIntegrationFetchSelectedDevicesForCompany(t *testing.T) {
	l := newLogForTesting(t)
	chwwwDB := newCHWWWSQLDBForTesting(t)
	chwwwDBx := newCHWWWSQLXDBForTesting(chwwwDB.Rw)
	credDS := newCredentialsDataSourceForTesting(t, l, chwwwDB.Rox)
	cds := newConductorDataSourceForTesting(t, l, chwwwDB)
	apiEmail, apiToken := newAPICredsForTesting(t, credDS)
	ads := newAlertDSForTesting(t, l, chwwwDB)
	nodeClient := newCHNodeAPIForTesting(t, l, apiEmail, apiToken)
	ctx := context.TODO()
	cids := newCidsForTesting(ctx, t, chwwwDBx)
	sleepDuration := newSleepDurationForTesting()
	shadowCompanyID := newShadowCidForTesting(t, cds)

	for _, cid := range cids {
		l.Infof("", "processing cid=%d time=%s", cid, time.Now())

		var pids []kt.PolicyID
		err := chwwwDBx.SelectContext(ctx, &pids, `
select id from mn_alert_policy
where company_id = ANY($1)
`, pq.Array([]kt.Cid{cid, shadowCompanyID}))
		if err != nil {
			t.Fatal(err)
		}

		var policyToDevicesFromSQL map[kt.PolicyID]*kt.DeviceIDSet
		if os.Getenv("K_NO_SQL_QUERY") != "true" {
			policyToDevicesFromSQL, err = ads.fetchSelectedDevicesForCompany(ctx, cid)
			if err != nil {
				t.Fatal(err)
			}
		}

		var policyToDevicesFromNode map[kt.PolicyID]*kt.DeviceIDSet
		if os.Getenv("K_NO_NODE_QUERY") != "true" {
			policyToDevicesFromNode, err = nodeClient.GetDeviceIDsForPolicies(ctx, pids)
			if err != nil {
				t.Fatal(err)
			}
		}

		diffDevices(t, cid, policyToDevicesFromSQL, policyToDevicesFromNode)

		time.Sleep(sleepDuration) // Don't hammer chnode too hard.
	}
}

func diffDevices(t *testing.T, cid kt.Cid, policyToDevicesFromSQL, policyToDevicesFromNode map[kt.PolicyID]*kt.DeviceIDSet) {
	for pid := range policyToDevicesFromSQL {
		s, n := policyToDevicesFromSQL[pid].Items(), policyToDevicesFromNode[pid].Items()
		sort.Sort(byIDAsc(s))
		sort.Sort(byIDAsc(n))
		if !reflect.DeepEqual(s, n) {
			t.Fail()
			t.Logf("cid=%d pid=%d dir=sql->node not equal: sql=%+v node=%+v", cid, pid, s, n)
		}
	}
	for pid := range policyToDevicesFromNode {
		s, n := policyToDevicesFromSQL[pid].Items(), policyToDevicesFromNode[pid].Items()
		sort.Sort(byIDAsc(s))
		sort.Sort(byIDAsc(n))
		if !reflect.DeepEqual(s, n) {
			t.Fail()
			t.Logf("cid=%d pid=%d dir=node->sql not equal: sql=%+v node=%+v", cid, pid, s, n)
		}
	}
}

// byIDAsc implements sort.Interface for []kt.DeviceID
type byIDAsc []kt.DeviceID

func (a byIDAsc) Len() int           { return len(a) }
func (a byIDAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byIDAsc) Less(i, j int) bool { return a[i] < a[j] }

func newLogForTesting(t *testing.T) logger.Underlying { return &ltesting.Test{T: t} }

func newCHWWWSQLDBForTesting(t *testing.T) *RoRwPairDBPair {
	dsn := sOrDef(os.Getenv(kt.EnvChwwwRW), pgConnForTesting)
	pg, err := OpenPostgresDBPair(context.TODO(), dsn, dsn, DefaultMaxOpenConns)
	if err != nil {
		t.Fatalf("pg err %v", err)
	}
	return pg
}

func newCHWWWSQLXDBForTesting(chwwwDB *sql.DB) *sqlx.DB { return sqlx.NewDb(chwwwDB, "postgres") }

func newCredentialsDataSourceForTesting(t *testing.T, l logger.Underlying, chwwwDB *sqlx.DB) CredentialsDataSource {
	cds, err := NewCredentialsDataSource(chwwwDB, l)
	if err != nil {
		t.Fatal(err)
	}
	return cds
}

func newConductorDataSourceForTesting(t *testing.T, l logger.Underlying, chwwwDB *RoRwPairDBPair) ConductorDataSource {
	cds, err := NewConductorDataSource(chwwwDB, l)
	if err != nil {
		t.Fatal(err)
	}
	return cds
}

func newAPICredsForTesting(t *testing.T, cds CredentialsDataSource) (string, string) {
	chnodeAPIEmail := sOrDef(os.Getenv(kt.EnvChnodeSystemUserEmail), "system@kentik.com")
	apiEmail, apiToken, err := cds.GetAPICredentials(0, chnodeAPIEmail)
	if err != nil {
		t.Fatal(err)
	}
	return apiEmail, apiToken
}

func newAlertDSForTesting(t *testing.T, l logger.Underlying, chwwwDB *RoRwPairDBPair) *alertDataSource {
	alertDS, err := NewAlertDataSource(chwwwDB, l)
	if err != nil {
		t.Fatal(err)
	}
	return alertDS.(*alertDataSource)
}

func newCHNodeAPIForTesting(t *testing.T, l logger.Underlying, apiEmail, apiToken string) chnodeapi.Client {
	url := sOrDef(os.Getenv(kt.EnvChnodeUrl), "https://api.our1.kentik.com")
	return chnodeapi.NewClient(apiEmail, apiToken, url, nil, l)
}

func newCidsForTesting(ctx context.Context, t *testing.T, chwwwDBx *sqlx.DB) []kt.Cid {
	var cids []kt.Cid
	cidFromEnv, _ := kt.AtoiCid(os.Getenv("K_CID"))
	if cidFromEnv != 0 {
		cids = []kt.Cid{cidFromEnv}
	} else {
		err := chwwwDBx.SelectContext(ctx, &cids, `select id from mn_company where company_status = 'V'`)
		if err != nil {
			t.Fatal(err)
		}
		minCid, _ := kt.AtoiCid(os.Getenv("K_CID_MIN"))
		maxCid, _ := kt.AtoiCid(os.Getenv("K_CID_MAX"))
		filteredCids := make([]kt.Cid, 0, len(cids))
		for _, c := range cids {
			if minCid != 0 && c < minCid {
				continue
			}
			if maxCid != 0 && c > maxCid {
				continue
			}
			filteredCids = append(filteredCids, c)
		}
		cids = filteredCids
		sort.Sort(byCidAsc(cids))
	}
	return cids
}

// byIDAsc implements sort.Interface for []kt.Cid
type byCidAsc []kt.Cid

func (a byCidAsc) Len() int           { return len(a) }
func (a byCidAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byCidAsc) Less(i, j int) bool { return a[i] < a[j] }

func newSleepDurationForTesting() time.Duration {
	durationFromEnv, _ := time.ParseDuration(os.Getenv("K_SLEEP_DURATION"))
	if durationFromEnv > 0 {
		return durationFromEnv
	}
	return 50 * time.Millisecond
}

func newShadowCidForTesting(t *testing.T, cds ConductorDataSource) kt.Cid {
	shadowCompanyID, err := cds.FetchShadowCompanyID()
	if err != nil {
		t.Logf("warning: no shadowCompanyID: %s", err)
		return 0
	}
	return shadowCompanyID
}

func sOrDef(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
