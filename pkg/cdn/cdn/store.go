package cdn

// Patterned on asn/store.go.
//
// This package is used in two places: in the report command local to this
// repository, and in runner.
//
// In report, we read the REs (PopulateFromREs) and then update the DB
// (SyncWithDB).  In runner, we only ever read the DB (UpdateFromDB).

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kentik/cdn-attribution/pkg/common"
)

const (
	dbMaxRetries    = 5
	dbRetryInterval = 500 * time.Millisecond
	dbSoftError     = "40P01"
)

// internal interface to wrap SQL Rows for testing
type cdn2NameRowScanner interface {
	Next() bool
	Scan(dest ...interface{}) error
}

// Logger is used for log messages
type Logger interface {
	Infof(prefix, format string, v ...interface{})
	Errorf(prefix, format string, v ...interface{})
}

// Hold error from db
// From https://github.com/lib/pq/blob/master/error.go
type pgError struct {
	Severity         string
	Code             ErrorCode
	Message          string
	Detail           string
	Hint             string
	Position         string
	InternalPosition string
	InternalQuery    string
	Where            string
	Schema           string
	Table            string
	Column           string
	DataTypeName     string
	Constraint       string
	File             string
	Line             string
	Routine          string
}

var (
	// Variable table names for testing
	mn_cdn                  = "mn_cdn"
	mn_dataset_version      = "mn_dataset_version"
	version_format          = "20060102150405"
	pseudoMostRecentVersion = time.Now().Format(version_format)
)

func (err pgError) Error() string {
	return "pq: " + err.Message
}

type Metadata struct {
	idInt                  int
	ID                     string     `json:"id"`
	Name                   string     `json:"name"`
	CdnType                []string   `json:"cdn_type"`
	Embedded               bool       `json:"embedded"`
	IsStatic               bool       `json:"has_statically_defined_cidrs"`
	EmbeddedUrl            string     `json:"embedded_program_url"`
	PeeringdbUrl           string     `json:"peeringdb_url"`
	NoiseFilterOttProvider string     `json:"noise_filter_ott_provider"`
	CDN2ASN                *CDN2ASN   `json:"cdn2asn"`
	StaticCDN              *StaticCDN `json:"statics"`
}

type CDN2ASN struct {
	Name string   `json:"name"`
	Asns []uint32 `json:"asns"`
}

type StaticCDN struct {
	Name  string `json:"name"`
	Cidrs []Cidr `json:"cidrs"`
}

type Cidr struct {
	Protocol string   `json:"protocol"`
	Cidr     []string `json:"cidr"`
	Asn      uint32   `json:"asn"`
	Comment  string   `json:"comment"`
}

type ErrorCode string

// Store manages fetching, storing, and updating CDN data from Postgres.
type Store struct {
	log                Logger
	logPrefix          string
	lock               sync.RWMutex // used when swapping
	pgDB               *sql.DB
	cdn2Name           map[int]string // CDN -> name mapping; protected by lock
	name2CDN           map[string]int // CDN name -> ids; protected by lock
	currentDataVersion string         // version of the currently-loaded data; protected by lock
	sort               bool
	cdn2MDS            map[int]Metadata // CDN -> MDS mapping; protected by lock
}

// NewStore creates a new CDN store.
func NewStore(log Logger, logPrefix string, pgDB *sql.DB) *Store {
	return &Store{
		log:       log,
		logPrefix: logPrefix,
		lock:      sync.RWMutex{},
		pgDB:      pgDB,
		name2CDN:  make(map[string]int),
		cdn2Name:  make(map[int]string),
		cdn2MDS:   make(map[int]Metadata),
	}
}

func NewTestStore(name2CDN map[string]int) *Store {
	cdn2Name := map[int]string{}
	for k, v := range name2CDN {
		cdn2Name[v] = k
	}
	return &Store{
		name2CDN: name2CDN,
		cdn2Name: cdn2Name,
		cdn2MDS:  make(map[int]Metadata),
		sort:     true,
	}
}

func (s *Store) fetchCDNData() (*sql.Rows, error) {
	// fetch the updated records outside transaction
	s.log.Infof(s.logPrefix, "Checked database for updated CDN data. New data is available - fetching it.")
	rows, err := s.pgDB.Query("SELECT id, name FROM " + mn_cdn)
	return rows, err
}

func (s *Store) GetCDNData() (*sql.Rows, error) {
	for retries := 0; ; retries++ {
		rows, err := s.fetchCDNData()
		if err != nil {
			if pgerr, ok := err.(*pgError); ok {
				if pgerr.Code != dbSoftError {
					return nil, fmt.Errorf("Error fetching CDN information from database: %s", err)
				}
			} else {
				return nil, fmt.Errorf("Error fetching CDN information from database, not not get type: %s", err)
			}
		} else {
			return rows, err
		}
		if retries >= dbMaxRetries {
			return nil, fmt.Errorf("Error fetching CDN information from database (max retries exceeded): %s", err)
		}
		time.Sleep(dbRetryInterval * time.Duration(retries+1))
	}
}

// UpdateFromDB updates the internal store from the database.
// This is threadsafe, and will block until done.
func (s *Store) UpdateFromDB() error {
	// fetch the version in a read-lock
	s.lock.RLock()
	currentDataVersion := s.currentDataVersion
	s.lock.RUnlock()

	start := time.Now()

	// check the database to get the latest import version
	newestVersion, err := s.fetchLatestVersion()
	if err != nil {
		return fmt.Errorf("Error fetching latest CDN dataset version: %s", err)
	}
	if currentDataVersion == newestVersion {
		s.log.Infof(s.logPrefix, "Checked database for updated CDN data. Nothing to do: local cache is already current.")
		return nil
	}

	rows, err := s.GetCDNData()
	if err != nil {
		return fmt.Errorf("Error fetching CDN information from database: %s", err)
	} else if rows != nil {
		defer func() {
			if err := rows.Close(); err != nil {
				s.log.Errorf(s.logPrefix, "Error closing rows after querying mn_cdn.id|name")
			}
		}()
	}

	recordCount, err := s.consumeCDNRows(newestVersion, rows)
	if err != nil {
		return err
	}

	s.log.Infof(s.logPrefix, "Imported/Updated %d CDN records from database in %s", recordCount, time.Now().Sub(start))
	return nil
}

func (s *Store) PopulateFromREs(REs []common.RE) error {
	newCDN2Name := make(map[int]string, len(REs))
	newName2CDN := make(map[string]int, len(REs))
	for _, re := range REs {
		// add ID -> name lookup
		if _, present := newCDN2Name[re.ID]; present {
			return fmt.Errorf("Duplicate ID in CDN list: %d", re.ID)
		}
		newCDN2Name[re.ID] = re.CDN

		// name -> ID lookup
		if _, present := newName2CDN[re.CDN]; present {
			return fmt.Errorf("Duplicate Name in CDN list: %s", re.CDN)
		}
		newName2CDN[re.CDN] = re.ID
	}

	// swap in the new data in a write-lock
	s.lock.Lock()
	s.currentDataVersion = time.Now().Format(version_format)
	s.cdn2Name = newCDN2Name
	s.name2CDN = newName2CDN
	s.lock.Unlock()

	return nil
}

// fetch the most recent version
func (s *Store) fetchLatestVersion() (string, error) {
	// check the database to get the latest version
	rows, err := s.pgDB.Query("SELECT MAX(edate) FROM " + mn_dataset_version + " WHERE dataset_name='cdn'")
	if err != nil {
		return "", fmt.Errorf("Error querying for most recent CDN import: %s", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			s.log.Errorf(s.logPrefix, "Error closing rows after querying mn_dataset_version.edate")
		}
	}()

	var mostRecentVersion sql.NullString
	if rows.Next() {
		err = rows.Scan(&mostRecentVersion)
		if err != nil {
			return "", fmt.Errorf("Error scanning edate from mn_dataset_version: %s", err)
		}
	}

	// Will only be valid if a row was returned, and the column is not null.
	if mostRecentVersion.Valid {
		return mostRecentVersion.String, nil
	}
	// If no rows, or column is null, return the same fake date each time.  This
	// should cause one update on startup, and no more after that, until
	// something is added to mn_dataset_version.
	s.log.Infof(s.logPrefix, "No rows from %s or no edate, using %s for latestVersion",
		mn_dataset_version, pseudoMostRecentVersion)
	return pseudoMostRecentVersion, nil
}

// consume the results of the (cdnID, cdnName) SQL query, updating the Store's state
// when done. It is this method's responsibility to handle locking the internal state.
func (s *Store) consumeCDNRows(version string, rows cdn2NameRowScanner) (int, error) {
	newCDN2Name := make(map[int]string)
	newName2CDN := make(map[string]int)
	cdnID := int(0)
	cdnName := ""
	recordCount := 0
	for rows.Next() {
		if err := rows.Scan(&cdnID, &cdnName); err != nil {
			return 0, fmt.Errorf("Error scanning a row of CDN data: %s", err)
		}

		// add ID -> name lookup
		newCDN2Name[cdnID] = cdnName

		// name -> ID lookup
		newName2CDN[cdnName] = cdnID

		recordCount++
	}

	// Note: since we didn't use a transaction, it's possible that we've just loaded new data with
	// the old version. This isn't a big deal - data will just be stale till next poll.

	// swap in the new data in a write-lock
	s.lock.Lock()
	s.currentDataVersion = version
	s.cdn2Name = newCDN2Name
	s.name2CDN = newName2CDN
	s.lock.Unlock()

	return recordCount, nil
}

// AllData returns all Name->CDN data. The returned map is never modified,
// except when being sync'ed with the DB during cdn-attribution.
func (s *Store) AllData() map[string]int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.name2CDN
}

// CDNByName returns a CDN ID for the input name.
func (s *Store) CDNByName(name string) (int, bool) {
	// get working instances of the maps so we can let go of the lock ASAP
	s.lock.RLock()
	name2CDN := s.name2CDN
	s.lock.RUnlock()

	id, ok := name2CDN[name]
	return id, ok
}

// CDNsByName returns a slice of CDN IDs for the given regular expression
func (s *Store) CDNsByName(re *regexp.Regexp) []int {
	// get working instances of the maps so we can let go of the lock ASAP
	s.lock.RLock()
	name2CDN := s.name2CDN
	s.lock.RUnlock()

	ids := []int{}
	for name, id := range name2CDN {
		if re.MatchString(name) {
			ids = append(ids, id)
		}
	}

	// Sort output, so that the values are in a predictable order, for test cases.
	if s.sort {
		sort.Ints(ids)
	}

	return ids
}

// NameByCDN looks up a CDN name for an ID.
func (s *Store) NameByCDN(cdn int) (string, bool) {
	// get working instances of the map so we can let go of the lock ASAP
	s.lock.RLock()
	cdn2Name := s.cdn2Name
	s.lock.RUnlock()

	name, ok := cdn2Name[cdn]
	return name, ok
}

// NamesByCDNs looks up CDN names for an array of IDs. Return value
// must line up with the request, with empty values for those that could
// not be found.
func (s *Store) NamesByCDNs(cdns []uint32) []string {
	// get working instances of the map so we can let go of the lock ASAP
	s.lock.RLock()
	cdn2Name := s.cdn2Name
	s.lock.RUnlock()

	ret := make([]string, len(cdns))
	for i := 0; i < len(cdns); i++ {
		ret[i] = cdn2Name[int(cdns[i])]
	}
	return ret
}

// SyncWithDB takes the current store and syncs it with the db table.  The
// current store is assumed to be populated from the cdns.csv file, which is
// canonical, and so it overrides the DB.  All changes are logged.  If there
// are any additions or updates, we update mn_dataset_version.
//
// Any returned errors are fatal and the caller should abort.
func (s *Store) SyncWithDB() (numChanges int, e error) {
	l, pref := s.log, s.logPrefix
	s.lock.Lock()
	defer s.lock.Unlock()

	tx, err := s.pgDB.Begin()
	if err != nil {
		return 0, err
	}

	// Commit or rollback when done
	defer func() {
		if e == nil {
			err := tx.Commit()
			if err != nil {
				l.Errorf(pref, "Error committing transaction: %v", err)
				numChanges, e = 0, err
				return
			}
		} else {
			numChanges = 0 // just to make sure
			err := tx.Rollback()
			if err != nil {
				l.Errorf(pref, "Error rolling back transaction: %v", err)
				return
			}
		}
	}()

	c2n_db := map[int]string{}

	// Read the DB
	rows, err := tx.Query(`
		SELECT id, name
		FROM ` + mn_cdn)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			l.Errorf(pref, "Error reading a row out of mn_cdn: %v", err)
			return 0, err
		}

		c2n_db[id] = name

		// If the ID is not in the local list, add it
		if _, present := s.cdn2Name[id]; !present {
			if reID, present := s.name2CDN[name]; present {
				l.Errorf(pref, "Conflict between mn_cdn and RE: db has '%s' -> %d but REs already have '%s' -> %d; db ignored",
					name, id, name, reID)
			}

			l.Infof(pref, "Adding %d -> '%s' to cdn store from DB", id, name)
			s.cdn2Name[id] = name
			s.name2CDN[name] = id
		}
	}
	if err := rows.Err(); err != nil {
		l.Errorf(pref, "Query error reading mn_cdn: %v", err)
		return 0, err
	}

	// If a local ID is not in the db, add it
	insStmt, err := tx.Prepare(`INSERT INTO ` + mn_cdn + ` (id, name, metadata) VALUES ($1, $2, $3)`)
	if err != nil {
		return 0, err
	}
	for reID, reName := range s.cdn2Name {
		if _, present := c2n_db[reID]; !present {
			mds := "{}"
			if _, ok := s.cdn2MDS[reID]; ok {
				md, _ := json.Marshal(s.cdn2MDS[reID])
				mds = string(md)
			}
			l.Infof(pref, "Adding %d -> '%s' (%s) to db", reID, reName, mds)
			_, err := insStmt.Exec(reID, reName, mds)
			if err != nil {
				return 0, err
			}
			numChanges++
		}
	}
	err = insStmt.Close()
	if err != nil {
		return 0, err
	}

	// If a local name is different in the db, update it in the db
	updStmt, err := tx.Prepare(`UPDATE ` + mn_cdn + ` SET name = $1, edate = now(), metadata = $3 WHERE id = $2`)
	if err != nil {
		return 0, err
	}
	for reID, reName := range s.cdn2Name {
		if dbName, present := c2n_db[reID]; present {
			if reName != dbName {
				mds := "{}"
				if _, ok := s.cdn2MDS[reID]; ok {
					md, _ := json.Marshal(s.cdn2MDS[reID])
					mds = string(md)
				}
				l.Infof(pref, "Updating db from %d -> '%s' to '%s' [%s]", reID, dbName, reName, mds)
				_, err := updStmt.Exec(reName, reID, mds)
				if err != nil {
					return 0, err
				}
				numChanges++
			}
		}
	}
	updStmt.Close()
	if err != nil {
		return 0, err
	}

	// If any changes, update mn_dataset_version
	if numChanges > 0 {
		version := time.Now().Format(version_format)
		l.Infof(pref, "Updating mn_dataset_version with version %s", version)
		result, err := tx.Exec(`
			UPDATE `+mn_dataset_version+`
			SET dataset_version = $1, edate = now(), status = $2
			WHERE dataset_name = $3
		`, version, "OK", "cdn")
		if err != nil {
			return 0, err
		}
		ra, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		if ra != 1 {
			result, err := tx.Exec(`
				INSERT INTO `+mn_dataset_version+` (dataset_name, dataset_version)
				VALUES ($1, $2)
			`, "cdn", version)
			if err != nil {
				return 0, err
			}
			ra, err := result.RowsAffected()
			if err != nil {
				return 0, err
			}
			if ra != 1 {
				return 0, fmt.Errorf("Error inserting into mn_dataset_version: RowsAffected = %d; exected 1", ra)
			}
		}
	}

	return numChanges, nil
}

// Populate the MDS array if one is set.
func (s *Store) LoadMDS(metadataFile string, metaCDN string, metaStatic string) (int, error) {
	mds := []Metadata{}
	asns := []CDN2ASN{}
	statics := []StaticCDN{}

	byteValue, err := ioutil.ReadFile(metadataFile)
	if err != nil {
		return 0, err
	}

	err = json.Unmarshal(byteValue, &mds)
	if err != nil {
		return 0, err
	}

	// Now, try to load other files.
	if metaCDN != "" {
		byteValueMeta, err := ioutil.ReadFile(metaCDN)
		if err != nil {
			return 0, err
		}
		err = json.Unmarshal(byteValueMeta, &asns)
		if err != nil {
			return 0, err
		}
	}

	if metaStatic != "" {
		byteValueStatic, err := ioutil.ReadFile(metaStatic)
		if err != nil {
			return 0, err
		}
		err = json.Unmarshal(byteValueStatic, &statics)
		if err != nil {
			return 0, err
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	for _, md := range mds {
		id, err := strconv.Atoi(md.ID)
		if err == nil {
			md.idInt = id

			// Loop in the other stuff here
			for _, asn := range asns {
				if strings.ToLower(md.Name) == strings.ToLower(asn.Name) {
					md.CDN2ASN = &asn
					break
				}
			}
			for _, static := range statics {
				if strings.ToLower(md.Name) == strings.ToLower(static.Name) {
					md.StaticCDN = &static
					break
				}
			}

			s.cdn2MDS[id] = md
		}
	}

	return len(s.cdn2MDS), nil
}
