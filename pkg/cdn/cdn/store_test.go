package cdn

// Patterned on asn/store_test.go

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	_ "github.com/bmizerany/pq"
	"github.com/kentik/cdn-attribution/pkg/common"
	"github.com/kentik/cdn-attribution/pkg/pgpass"
	"github.com/kentik/golog/logger"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

// implementation of cdn2NameRowScanner for testing
type fakeRows struct {
	cdn2Name     map[int]string
	keys         []int
	currentIndex int
}

var (
	l                         = logger.New(logger.Levels.Debug)
	selectVersion, selectName string
)

func init() {
	// logger.SetStdOut()
	mn_cdn = "mn_cdn_test"
	mn_dataset_version = "mn_dataset_version_test"
	selectVersion = "SELECT dataset_version FROM " + mn_dataset_version + " WHERE dataset_name = 'cdn'"
	selectName = "SELECT name FROM " + mn_cdn + " WHERE id = $1"
}

// create a new cdn2NameRowScanner for testing
func newFakeRows(cdn2Name map[int]string) cdn2NameRowScanner {
	keys := make([]int, 0, len(cdn2Name))
	for k := range cdn2Name {
		keys = append(keys, k)
	}
	return &fakeRows{
		cdn2Name:     cdn2Name,
		keys:         keys,
		currentIndex: -1,
	}
}

// Next advances the pointer, returning whether there's a new record
func (r *fakeRows) Next() bool {
	if len(r.cdn2Name) == 0 || r.currentIndex >= len(r.cdn2Name)-1 {
		return false
	}
	r.currentIndex++
	return true
}

// Scan returns the cdnID and description into dest
func (r *fakeRows) Scan(dest ...interface{}) error {
	if len(dest) != 2 {
		// no reason to return error here for testing
		panic("Expected 2 records in dest: [cdnID, description]")
	}
	if r.currentIndex < 0 {
		// no reason to return error here for testing
		panic("looks like Next() hcdn't been called")
	}
	if r.currentIndex >= len(r.cdn2Name) {
		// no reason to return error here for testing
		panic("Can't scan - no more records")
	}
	d0 := dest[0].(*int)
	*d0 = r.keys[r.currentIndex]

	d1 := dest[1].(*string)
	*d1 = r.cdn2Name[r.keys[r.currentIndex]]
	return nil
}

// Test CDNByName
func TestCDNByName(t *testing.T) {
	cdn2Name := map[int]string{
		1:  "abc",
		2:  "def",
		3:  "ghi",
		4:  "klm",
		77: "GHi",
		5:  "1ABcDefGhij",
	}
	rows := newFakeRows(cdn2Name)

	// system under test - we're going to skip calling UpdateFromDB, since that's where the SQL is.
	// instead, we'll consume the fake Rows object
	sut := NewStore(nil, "", nil)
	recordCount, err := sut.consumeCDNRows("version1", rows)
	assert.NoError(t, err)
	assert.Equal(t, 6, recordCount)
	assert.Equal(t, "version1", sut.currentDataVersion)

	// Test #1: match with results
	expected, eok := 3, true
	results, rok := sut.CDNByName("ghi")
	assert.Equal(t, expected, results)
	assert.Equal(t, eok, rok)

	// Test #2: match with results, different only by case
	expected, eok = 77, true
	results, rok = sut.CDNByName("GHi")
	assert.Equal(t, expected, results)
	assert.Equal(t, eok, rok)

	// Test #2: query without results
	expected, eok = 0, false
	results, rok = sut.CDNByName("GHI")
	assert.Equal(t, expected, results)
	assert.Equal(t, eok, rok)
}

func TestMDS(t *testing.T) {
	content := []byte(`[{
   "id": "47",
   "name": "Comcast CDN",
   "cdn_type": ["Commercial", "Telco"],
   "embedded": false,
   "has_statically_defined_cidrs": false
 }
]`)
	tmpfile, err := ioutil.TempFile("", "testFile")
	assert.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up

	tmpfile.Write(content)
	assert.NoError(t, err)

	err = tmpfile.Close()
	assert.NoError(t, err)

	sut := NewStore(nil, "", nil)
	nums, err := sut.LoadMDS(tmpfile.Name(), "", "")
	assert.NoError(t, err)

	assert.Equal(t, 1, nums)
	assert.Equal(t, "Comcast CDN", sut.cdn2MDS[47].Name)
}

// Test NamesByCDNs
func TestNamesByCDNs(t *testing.T) {
	cdn2Name := map[int]string{
		1:  "abc",
		2:  "def",
		3:  "ghi",
		4:  "klm",
		77: "GHi",
		5:  "1ABcDefGhij",
		99: "1ABcDefGHij", // capital H
		66: "ghi",
	}
	rows := newFakeRows(cdn2Name)

	// system under test - we're going to skip calling UpdateFromDB, since that's where the SQL is.
	// instead, we'll consume the fake Rows object
	sut := NewStore(nil, "", nil)
	recordCount, err := sut.consumeCDNRows("version1", rows)
	assert.NoError(t, err)
	assert.Equal(t, 8, recordCount)
	assert.Equal(t, "version1", sut.currentDataVersion)

	query := []uint32{77, 5, 100, 33, 66, 0, 999}
	expected := []string{"GHi", "1ABcDefGhij", "", "", "ghi", "", ""}
	actual := sut.NamesByCDNs(query)
	assert.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i], actual[i])
	}
}

func TestSyncWithDB(t *testing.T) {
	Convey("Given a DB connection", t, func() {
		db, err := getDBConn("127.0.0.1", "5432")
		if err != nil {
			l.Errorf("", "Could not connect to PG: %v", err)
			panic(err)
		}
		defer db.Close()

		// Create tables.  Tried using temporary tables, but you can't use them
		// in a transaction (which seems really wierd) -- or at least I got
		// errors, and with real tables I didn't -- so switched to real tables.
		_, err = db.Exec(`
			DROP TABLE IF EXISTS ` + mn_cdn + `;
			CREATE TABLE ` + mn_cdn + ` (
				id int PRIMARY KEY,
				name text NOT NULL,
				metadata JSONB,
				cdate timestamp without time zone default now(),
				edate timestamp
			);
			DROP TABLE IF EXISTS ` + mn_dataset_version + `;
			CREATE TABLE ` + mn_dataset_version + ` (
				 dataset_name text NOT NULL,
				 dataset_version text NOT NULL,
				 cdate timestamp without time zone DEFAULT now(),
				 edate timestamp without time zone DEFAULT now(),
				 status text DEFAULT 'NEW'::text NOT NULL
			);
		`)
		if err != nil {
			l.Errorf("", "Could not create tmp table %s: %v", mn_cdn, err)
			panic(err)
		}
		defer func() {
			db.Exec(`
				DROP TABLE IF EXISTS ` + mn_cdn + `;
				DROP TABLE IF EXISTS ` + mn_dataset_version + `;`)
		}()

		store := NewStore(l, "", db)

		Convey("Given an empty store", func() {
			Convey("When you populate from a list of REs", func() {
				REs := []common.RE{
					{ID: 0, CDN: "zero"},
				}
				store.PopulateFromREs(REs)

				Convey("The maps should be populated", func() {
					So(len(store.cdn2Name), ShouldEqual, len(REs))
					So(len(store.name2CDN), ShouldEqual, len(REs))

					name, ok := store.NameByCDN(0)
					So(ok, ShouldBeTrue)
					So(name, ShouldEqual, "zero")

					id, ok := store.CDNByName("zero")
					So(ok, ShouldBeTrue)
					So(id, ShouldEqual, 0)

					name, ok = store.NameByCDN(1)
					So(ok, ShouldBeFalse)
				})
			})
		})

		Convey("Given empty tables", func() {
			REs := []common.RE{
				{ID: 0, CDN: "zero"},
			}
			store.PopulateFromREs(REs)

			// Verity table's empty
			rows, err := db.Query("SELECT COUNT(*) FROM " + mn_cdn)
			So(err, ShouldBeNil)
			So(rows, ShouldNotBeNil)
			defer rows.Close()
			So(rows.Next(), ShouldEqual, true)
			var count int = -1
			err = rows.Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 0)

			Convey("When you sync the DB", func() {
				// No version string for cdns yet
				rows, err := db.Query(selectVersion)
				So(err, ShouldBeNil)
				So(rows, ShouldNotBeNil)
				So(rows.Next(), ShouldEqual, false)
				// rows.Next() => false automatically closes rows, so no need to
				// defer rows.Close() here.

				// Do the update
				numChanges, err := store.SyncWithDB()
				So(err, ShouldBeNil)
				So(numChanges, ShouldEqual, 1)

				Convey("The DB should be populated", func() {
					name := getName(db, 0)
					So(name, ShouldEqual, "zero")
				})

				Convey("The version should be inserted", func() {
					// Will fail if version not inserted correctly
					getVersion(db)
				})
			})
		})

		Convey("Given a populated DB", func() {
			REs := []common.RE{
				{ID: 0, CDN: "zero"},
				{ID: 1, CDN: "one"},
			}
			store.PopulateFromREs(REs)
			numChanges, err := store.SyncWithDB()
			So(err, ShouldBeNil)
			So(numChanges, ShouldEqual, 2)

			// Has its own assertions; will fail if no version
			getVersion(db)

			Convey("When an ID is not in the local list, add it", func() {
				REs = []common.RE{
					{ID: 0, CDN: "zero"},
				}
				store.PopulateFromREs(REs)
				So(len(store.cdn2Name), ShouldEqual, 1)

				numChanges, err = store.SyncWithDB()

				So(err, ShouldBeNil)
				So(numChanges, ShouldEqual, 0)
				So(len(store.cdn2Name), ShouldEqual, 2)
			})

			// Convey("When a conflicting ID is in the local list, log it", func() {
			// })

			Convey("When a local ID is not in the DB, add it", func() {
				beforeVersion := getVersion(db)

				REs = append(REs, common.RE{ID: 2, CDN: "two"})
				store.PopulateFromREs(REs)
				So(len(store.cdn2Name), ShouldEqual, 3)

				// Sleep until the second changes to make the versions different.
				sleepTillNextSecond()

				numChanges, err = store.SyncWithDB()

				So(err, ShouldBeNil)
				So(numChanges, ShouldEqual, 1)

				Convey("When a changes occurs, the version is updated", func() {
					afterVersion := getVersion(db)
					So(beforeVersion, ShouldNotEqual, afterVersion)
				})
			})

			Convey("When a local name is different in the DB, update it", func() {
				name := getName(db, 1)
				So(name, ShouldEqual, "one")

				REs := []common.RE{
					{ID: 0, CDN: "zero"},
					{ID: 1, CDN: "new one"},
				}
				store.PopulateFromREs(REs)

				numChanges, err := store.SyncWithDB()
				So(err, ShouldBeNil)
				So(numChanges, ShouldEqual, 1)

				name = getName(db, 1)
				So(name, ShouldEqual, "new one")
			})
		})
	})
}

func TestFetchLatestVersion(t *testing.T) {
	Convey("Given a DB connection", t, func() {
		db, err := getDBConn("127.0.0.1", "5432")
		if err != nil {
			l.Errorf("", "Could not connect to PG: %v", err)
			panic(err)
		}
		defer db.Close()

		// Create tables.  Tried using temporary tables, but you can't use them
		// in a transaction (which seems really wierd) -- or at least I got
		// errors, and with real tables I didn't -- so switched to real tables.
		_, err = db.Exec(`
			DROP TABLE IF EXISTS ` + mn_cdn + `;
			CREATE TABLE ` + mn_cdn + ` (
				id int PRIMARY KEY,
				name text NOT NULL,
				cdate timestamp without time zone default now(),
				edate timestamp
			);
			DROP TABLE IF EXISTS ` + mn_dataset_version + `;
			CREATE TABLE ` + mn_dataset_version + ` (
				 dataset_name text NOT NULL,
				 dataset_version text NOT NULL,
				 cdate timestamp without time zone DEFAULT now(),
				 edate timestamp without time zone DEFAULT now(),
				 status text DEFAULT 'NEW'::text NOT NULL
			);
		`)
		if err != nil {
			l.Errorf("", "Could not create tmp table %s: %v", mn_cdn, err)
			panic(err)
		}
		defer func() {
			db.Exec(`
				DROP TABLE IF EXISTS ` + mn_cdn + `;
				DROP TABLE IF EXISTS ` + mn_dataset_version + `;`)
		}()

		store := NewStore(l, "", db)

		Convey("Given an empty DB", func() {
			Convey("When you call fetchLatestVersion() repeatedly", func() {
				ver1, err1 := store.fetchLatestVersion()
				sleepTillNextSecond()
				ver2, err2 := store.fetchLatestVersion()
				sleepTillNextSecond()
				ver3, err3 := store.fetchLatestVersion()

				Convey("It should return the same date", func() {
					So(err1, ShouldBeNil)
					So(err2, ShouldBeNil)
					So(err3, ShouldBeNil)
					So(ver1, ShouldEqual, ver2)
					So(ver1, ShouldEqual, ver3)
				})
			})
		})
	})
}

////////////////////////////////////////////////////////////////////////////////
// Utility functions

// Like common.GetMainDBConn except uses user "postgres" (which can create
// tables) instead of "www_user" (which can't).
func getDBConn(server, port string) (*sql.DB, error) {
	pw := os.Getenv("KENTIK_PG_PASS")
	if pw == "" {
		return nil, errors.New("KENTIK_PG_PASS is not set, cannot connect to Postgres")
	}

	connStr := pgpass.ConnStr(server, port, "ch_www", "postgres", pw)

	var db *sql.DB
	var err error
	if db, err = sql.Open("postgres", connStr); err != nil {
		return nil, err
	}
	return db, nil
}

func getVersion(db *sql.DB) string {
	rows, err := db.Query(selectVersion)
	So(err, ShouldBeNil)
	So(rows, ShouldNotBeNil)
	defer rows.Close()
	So(rows.Next(), ShouldEqual, true)
	var version string
	err = rows.Scan(&version)
	So(err, ShouldBeNil)
	So(len(version), ShouldEqual, 14)
	return version
}

func getName(db *sql.DB, id int) string {
	rows, err := db.Query(selectName, id)
	So(err, ShouldBeNil)
	So(rows, ShouldNotBeNil)
	defer rows.Close()
	So(rows.Next(), ShouldEqual, true)
	var name string
	err = rows.Scan(&name)
	So(err, ShouldBeNil)
	return name
}

// Could just sleep for a second, but hey, this'll be faster.  :)
func sleepTillNextSecond() {
	time.Sleep(time.Second - time.Duration(time.Now().Nanosecond()))
}
