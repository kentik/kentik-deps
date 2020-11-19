package database

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestDataSource struct {
	Stmt          *sql.Stmt
	notAStatement string
}

func (ds *TestDataSource) Close() {
	CloseStatements(ds)
}

func TestCloseStatements(t *testing.T) {
	drv := &MockDriver{closed: map[string]bool{}}
	sql.Register("mock", drv)

	db, err := sql.Open("mock", "")
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ := db.Prepare("stmt")
	ds := TestDataSource{Stmt: stmt}
	ds.Close()

	assert.True(t, drv.closed["stmt"], "*sql.Stmt should be closed")
}

type (
	MockDriver struct {
		closed map[string]bool
	}

	MockConn struct {
		d *MockDriver
		n string
	}

	MockStmt struct {
		d *MockDriver
		n string
	}
)

func (d *MockDriver) Open(name string) (driver.Conn, error)         { return &MockConn{d: d, n: name}, nil }
func (c *MockConn) Prepare(query string) (driver.Stmt, error)       { return &MockStmt{d: c.d, n: query}, nil }
func (c *MockConn) Close() error                                    { c.d.closed[c.n] = true; return nil }
func (c *MockConn) Begin() (driver.Tx, error)                       { return nil, nil }
func (s *MockStmt) Close() error                                    { s.d.closed[s.n] = true; return nil }
func (s *MockStmt) NumInput() int                                   { return 0 }
func (s *MockStmt) Exec(args []driver.Value) (driver.Result, error) { return nil, nil }
func (s *MockStmt) Query(args []driver.Value) (driver.Rows, error)  { return nil, nil }
