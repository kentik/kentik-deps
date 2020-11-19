package ktds

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/kentik/chf-alert/pkg/metric"
	"os"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/kentik/go-mysql-fork/mysql"

	"github.com/xo/dburl"
)

const (
	// DefaultMaxOpenConns is the default value our services use for
	// database/sql.SetMaxOpenConns, the connection pool maximum size.
	DefaultMaxOpenConns = 16

	// DefaultConnMaxLifetime is the default value our services use for
	// database/sql.SetConnMaxLifetime, the duration after which a connection
	// in the connection pool will be closed.
	DefaultConnMaxLifetime = 1 * time.Minute

	// connMaxLifetimeEnvVar is an env var to override DefaultConnMaxLifetime.
	// FIXME: pass this value down as a parameter like maxOpenConns, maybe config struct.
	connMaxLifetimeEnvVar = "KENTIK_CONN_MAX_LIFETIME"
)

type RoRwPairDBPair struct {
	Ro  *sql.DB
	Rox *sqlx.DB
	Rw  *sql.DB
	Rwx *sqlx.DB
}

func openPair(ctx context.Context, ro, rw string, maxConn int, driverName string, opener func(ctx context.Context, ds string, maxConn int) (*sql.DB, error)) (*RoRwPairDBPair, error) {
	if ro == "" {
		ro = rw
	}
	if rw == "" {
		return nil, fmt.Errorf("empty rw connection string")
	}
	var err error
	pair := &RoRwPairDBPair{}
	pair.Ro, err = opener(ctx, ro, maxConn)
	if err != nil {
		return nil, err
	}
	pair.Rw, err = opener(ctx, rw, maxConn)
	if err != nil {
		return nil, err
	}

	// finalize
	pair.Rox = sqlx.NewDb(pair.Ro, driverName)
	pair.Rwx = sqlx.NewDb(pair.Rw, driverName)
	return pair, nil
}

func OpenPostgresDBPair(ctx context.Context, ro, rw string, maxConn int) (*RoRwPairDBPair, error) {
	return openPair(ctx, ro, rw, maxConn, "postgres", OpenPostgresDB)
}

func OpenMysqlDBPair(ctx context.Context, ro, rw string, maxConn int) (*RoRwPairDBPair, error) {
	return openPair(ctx, ro, rw, maxConn, "mysql", OpenMysqlDB)
}

func (p *RoRwPairDBPair) Close() error {
	e1 := p.Ro.Close()
	e2 := p.Rw.Close()
	if e1 != nil {
		return e1
	}
	return e2
}

// OpenPostgresDB connects to a postgres DB, and ensures settings like
// connection pool size are set.
func OpenPostgresDB(ctx context.Context, dataSourceName string, maxOpenConns int) (*sql.DB, error) {
	return openDB(ctx, GetDriverNameForTracing(postgresDriverName), dataSourceName, maxOpenConns)
}

// OpenMysqlDB connects to a mysql DB, and ensures settings like
// connection pool size are set.
func OpenMysqlDB(ctx context.Context, dataSourceName string, maxOpenConns int) (*sql.DB, error) {
	return openDB(ctx, GetDriverNameForTracing(mysqlDriverName), dataSourceName, maxOpenConns)
}

const (
	postgresDriverName = "postgres"
	mysqlDriverName    = "mysql"
)

func reportMetrics(ctx context.Context, dataSourceName string, db *sql.DB) {
	url, err := dburl.Parse(dataSourceName)
	if err != nil {
		return // no metrics then
	}
	m := metric.NewDatabasePoolMetrics(url.Driver, url.Host, url.Port(), url.Path)
	for ctx.Err() == nil {
		stats := db.Stats()
		m.OpenConnections.Update(int64(stats.OpenConnections))
		time.Sleep(1 * time.Minute)
	}
}

func openDB(ctx context.Context, driverName, dataSourceName string, maxOpenConns int) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetConnMaxLifetime(getConnMaxLifetime())

	go reportMetrics(ctx, dataSourceName, db)

	return db, nil
}

func getConnMaxLifetime() time.Duration {
	os.Getenv(connMaxLifetimeEnvVar)
	d, err := time.ParseDuration(os.Getenv(connMaxLifetimeEnvVar))
	if err != nil {
		return DefaultConnMaxLifetime
	}
	return d
}

func mysqlDSNEnsureMultiInterpolate(dsn string) (string, error) {
	var err error
	dsn, err = mysqlDSNEnsureMultiStatements(dsn)
	if err != nil {
		return "", err
	}
	return mysqlDSNEnsureInterpolateParams(dsn)
}

func mysqlDSNEnsureMultiStatements(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	cfg.MultiStatements = true
	return cfg.FormatDSN(), nil
}

func mysqlDSNEnsureInterpolateParams(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	cfg.InterpolateParams = true
	return cfg.FormatDSN(), nil
}
