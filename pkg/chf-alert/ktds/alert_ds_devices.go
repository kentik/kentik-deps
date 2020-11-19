package ktds

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/kentik/chf-alert/pkg/kt"
)

// Note: this query lines up with the logic in
// chnode's getDevicesForPolicies.js:getDevicesForPolicy and companyServices.js:getUniqueDeviceNames
// A while back we decided to experiment to see if alerting could call chnode's api
// so this business logic could live and grow over there, without duplication.
// That is a good goal but as of 2019-08-07 this often doesn't work well because
// alerting hammers chnode asking for this data. It would be nice to remove the
// duplication with a better api structure, streaming updates, another service, etc.,
// but it is tremendously easier to just shove this logic in here.
// Since this logic hasn't changed in 2019 so far, it isn't that big of a deal.
//
// Future us: feel free to abstract this logic into a device service or something
// when it makes sense.
const stmtFetchSelectedDevicesForCompanySQL = `
WITH consts (device_company) AS (VALUES ($1 :: bigint)),
policy_companies AS (
  (SELECT device_company AS company_id FROM consts)
  UNION ALL
  -- (SELECT id AS company_id FROM (+stmtFetchShadowCompanyIDSQL+) pcsq1)
  (SELECT id AS company_id FROM mn_company WHERE company_status = 'V' AND company_name = 'kentikshadowpolicies')
),
policy_info AS (
  SELECT
    id,
    company_id,
    devices,
    selected_devices,
    (selected_devices->>'all_devices') :: bool AS all_devices,

    (select array_agg(x) from (
      select json_array_elements_text(
        COALESCE(NULLIF(
          p.selected_devices->>'device_types', 'null'
        )::json, '[]'::json)
      ) as x
      ) sq1) AS device_types,

    (select array_agg(x :: bigint) from (
      select json_array_elements_text(
        COALESCE(NULLIF(
          p.selected_devices->>'device_labels', 'null'
        )::json, '[]'::json)
      ) as x
      ) sq2) AS device_labels,

    (select array_agg(x :: bigint) from (
      select json_array_elements_text(
        COALESCE(NULLIF(
          p.selected_devices->>'device_sites', 'null'
        )::json, '[]'::json)
      ) as x
      ) sq3) AS device_sites,

    (select array_agg(x :: text) from (
      select json_array_elements_text(
        COALESCE(NULLIF(
          p.selected_devices->>'device_name', 'null'
        )::json, '[]'::json)
      ) as x
      ) sq4) AS device_name
  FROM mn_alert_policy p
  WHERE company_id IN (select company_id FROM policy_companies)
),
policy_devices AS (
  SELECT
    pi.id,
    pi.company_id,
    pi.devices,
    pi.selected_devices,

    (CASE WHEN pi.all_devices THEN ARRAY[100] ELSE ARRAY[]::integer[] END) AS all_devices_devices,

    (select array_agg(id) from (
      SELECT d.id FROM mn_device d
      WHERE
        (d.device_subtype IN (SELECT unnest(pi.device_types)))
        AND
        ((d.device_subtype != 'kprobe') OR
         -- ignore kprobe devices with cloud_export_id NULL
         (d.device_subtype = 'kprobe' AND cloud_export_id IS NULL))
        AND d.company_id = device_company
    ) psq1) AS device_types_devices,

    (select array_agg(id) from (
      SELECT d.id FROM mn_device d
      LEFT OUTER JOIN mn_device_label_device dld
      ON d.id = dld.device_id
      WHERE dld.label_id IN (SELECT unnest(pi.device_labels))
        AND d.company_id = device_company
    ) psq2) AS device_labels_devices,

    (select array_agg(id) from (
      SELECT id FROM mn_device d
      WHERE d.site_id IN (SELECT unnest(pi.device_sites))
        AND d.company_id = device_company
    ) psq3) AS device_sites_devices,

    (select array_agg(id) from (
      SELECT id FROM mn_device d
      WHERE d.device_name IN (SELECT unnest(pi.device_name))
        AND d.company_id = device_company
    ) psq4) AS device_name_devices
  FROM policy_info pi, consts
),
policy_device_ids AS (
  SELECT
    pd.id,
    pd.company_id,
    (CASE
    WHEN pd.selected_devices IS NULL OR pd.selected_devices::text = 'null' THEN pd.devices
    WHEN pd.all_devices_devices = ARRAY[100] THEN ARRAY[100]
    ELSE
     (select array_agg(distinct did) from (
       select unnest(
         array_cat(pd.device_types_devices,
          array_cat(pd.device_labels_devices,
            array_cat(pd.device_sites_devices,
             pd.device_name_devices)))) as did ) pdisq)
    END) AS device_ids
  FROM policy_devices pd
)
select
  id,
  company_id,
  device_ids
from policy_device_ids
where policy_device_ids.company_id IN (select company_id from policy_companies)
`

func (ds *alertDataSource) fetchSelectedDevicesForCompany(ctx context.Context, cid kt.Cid) (map[kt.PolicyID]*kt.DeviceIDSet, error) {
	var err error
	defer func(start time.Time) { ds.metrics.Get("fetchSelectedDevicesForCompany", cid).Done(start, err) }(time.Now())

	var rows []struct {
		ID        kt.PolicyID   `db:"id"`
		CompanyID kt.Cid        `db:"company_id"`
		DeviceIDs pq.Int64Array `db:"device_ids"`
	}
	err = ds.stmtFetchSelectedDevicesForCompany.SelectContext(ctx, &rows, cid)
	if err != nil {
		return nil, err
	}

	m := make(map[kt.PolicyID]*kt.DeviceIDSet, len(rows))
	for i := range rows {
		m[rows[i].ID] = kt.NewDeviceIDSet()
		for j := range rows[i].DeviceIDs {
			m[rows[i].ID].Add(kt.DeviceID(rows[i].DeviceIDs[j]))
		}
	}
	return m, err
}

const devicesByCidTTLDefault = 30 * time.Minute

func newDevicesByCidStore(ds *alertDataSource, ttl time.Duration) *devicesByCidStore {
	return &devicesByCidStore{
		fetchSelectedDevicesForCompany: ds.fetchSelectedDevicesForCompany,
		now:                            time.Now,
		ttl:                            ttl,

		RWMutex: sync.RWMutex{},
		cache:   make(map[kt.Cid]devicesByCidStoreEntry),
	}
}

type devicesByCidStore struct {
	fetchSelectedDevicesForCompany func(context.Context, kt.Cid) (map[kt.PolicyID]*kt.DeviceIDSet, error)
	now                            func() time.Time
	ttl                            time.Duration

	sync.RWMutex
	cache map[kt.Cid]devicesByCidStoreEntry
}

type devicesByCidStoreEntry struct {
	time time.Time
	v    map[kt.PolicyID]*kt.DeviceIDSet
}

func (s *devicesByCidStore) AddSelectedDevicesToPolicies(ctx context.Context, cid kt.Cid, ps map[kt.PolicyID]*kt.AlertPolicy) error {
	for _, p := range ps {
		err := s.addSelectedDevicesToPolicy(ctx, cid, p)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *devicesByCidStore) addSelectedDevicesToPolicy(ctx context.Context, cid kt.Cid, p *kt.AlertPolicy) error {
	dids, err := s.getForPolicy(ctx, cid, p.PolicyID)
	if err != nil {
		return err
	}
	p.SelectedDevices = dids
	return nil
}

func (s *devicesByCidStore) getForPolicy(ctx context.Context, cid kt.Cid, policyID kt.PolicyID) (*kt.DeviceIDSet, error) {
	policyToDevices, haveDevices := s.getFromCache(ctx, cid)
	_, haveThisPolicy := policyToDevices[policyID]
	if !haveDevices || !haveThisPolicy {
		var err error
		policyToDevices, err = s.getForce(ctx, cid)
		if err != nil {
			return nil, err
		}
	}

	dids, ok := policyToDevices[policyID]
	if !ok {
		return nil, fmt.Errorf("no devices for policy")
	}
	return dids, nil
}

func (s *devicesByCidStore) GetSelectedDevicesForCompanyFromStore(ctx context.Context, k kt.Cid) (map[kt.PolicyID]*kt.DeviceIDSet, error) {
	v, ok := s.getFromCache(ctx, k)
	if ok {
		return v, nil
	}
	return s.getForce(ctx, k)
}

func (s *devicesByCidStore) getFromCache(ctx context.Context, k kt.Cid) (map[kt.PolicyID]*kt.DeviceIDSet, bool) {
	now := s.now()
	s.RLock()
	entry, ok := s.cache[k]
	s.RUnlock()
	return entry.v, ok && entry.time.Add(s.ttl).After(now)
}

func (s *devicesByCidStore) getForce(ctx context.Context, k kt.Cid) (map[kt.PolicyID]*kt.DeviceIDSet, error) {
	v, err := s.fetchSelectedDevicesForCompany(ctx, k)
	if err != nil {
		return nil, err
	}
	now := s.now()
	s.Lock()
	s.cache[k] = devicesByCidStoreEntry{time: now, v: v}
	s.Unlock()
	return v, nil
}
