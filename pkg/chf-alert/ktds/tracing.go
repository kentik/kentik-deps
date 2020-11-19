package ktds

import (
	"database/sql"
	"sync"

	"github.com/kentik/go-mysql-fork/mysql"
	"github.com/lib/pq"
	"github.com/luna-duclos/instrumentedsql"
	instrumentedsql_opentracing "github.com/luna-duclos/instrumentedsql/opentracing"
	"github.com/opentracing/opentracing-go"
)

func installTracingInstrumentedDrivers() {
	mysqlDriver := instrumentedsql.WrapDriver(mysql.MySQLDriver{},
		instrumentedsql.WithTracer(instrumentedsql_opentracing.NewTracer(true)),
		instrumentedsql.WithOpsExcluded(
			// Disabling this as they seem verbose but heh
			instrumentedsql.OpSQLRowsNext,
			instrumentedsql.OpSQLStmtClose,
		),
	)
	sql.Register("instrumented-mysql", mysqlDriver)

	postgresDriver := instrumentedsql.WrapDriver(&pq.Driver{},
		instrumentedsql.WithTracer(instrumentedsql_opentracing.NewTracer(true)),
		instrumentedsql.WithOpsExcluded(
			// Disabling this as they seem verbose but heh
			instrumentedsql.OpSQLRowsNext,
			instrumentedsql.OpSQLStmtClose,
		),
	)
	sql.Register("instrumented-postgres", postgresDriver)
}

var installTracingInstrumentedDriversOnce sync.Once

func GetDriverNameForTracing(name string) string {
	if !opentracing.IsGlobalTracerRegistered() {
		return name
	}
	installTracingInstrumentedDriversOnce.Do(installTracingInstrumentedDrivers)
	switch name {
	case Mysql:
		return "instrumented-mysql"
	case Postgres:
		return "instrumented-postgres"
	default:
		return name
	}
}

type DriverName = string

const (
	Postgres = DriverName("postgres")
	Mysql    = DriverName("mysql")
)
