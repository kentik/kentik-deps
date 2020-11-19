package ktds

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kentik/chf-alert/pkg/kt"
)

// ErrNilRow represents nil row returned by query
var ErrNilRow = errors.New("Nil row in response")

// EntityData holds some basic info about given entity, basis for errros
type EntityData struct {
	entity    string
	entityID  uint64
	companyID kt.Cid
}

// ErrNoRows represents no rows affected by exec query
type ErrNoRows EntityData

func (enr ErrNoRows) Error() string {
	return fmt.Sprintf("No rows affected by query (cid: %d, entity: %s, id: %d)", enr.companyID, enr.entity, enr.entityID)
}

func moreThanOneRowLog(cid kt.Cid, entity string, entityID uint64) string {
	return fmt.Sprintf("More than one row affected by exec query (company_id: %d, entity: %s,entity id: %d)",
		cid, entity, entityID)
}

func getCountX(stmt *sqlx.Stmt, args ...interface{}) (int, error) {
	var result int
	err := stmt.Get(&result, args...)
	if err != nil {
		return 0, err
	}
	return result, nil
}

func getCountN(stmt *sqlx.NamedStmt, args interface{}) (int, error) {
	var result int
	err := stmt.Get(&result, args)
	if err != nil {
		return 0, err
	}
	return result, nil
}

// TODO(tjonak): use namedInStmt instead of this

func namedInRebindSelect(db *sqlx.DB, dest interface{}, query string, arg interface{}) error {
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return db.Select(dest, db.Rebind(query), args...)
}

func namedInRebindGet(db *sqlx.DB, dest interface{}, query string, arg interface{}) error {
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return db.Get(dest, db.Rebind(query), args...)
}

func namedInRebindSelectContext(ctx context.Context, db *sqlx.DB, dest interface{}, query string, arg interface{}) error {
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return db.SelectContext(ctx, dest, db.Rebind(query), args...)
}

func namedInRebindExec(db *sqlx.DB, query string, arg interface{}) error {
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}
	_, err = db.Exec(db.Rebind(query), args...)
	return err
}

func prepxOrPanic(db *sqlx.DB, query string) *sqlx.Stmt {
	stmt, err := db.Preparex(query)
	if err != nil {
		panic(fmt.Sprintf("prepxOrPanic: %s", err))
	}
	return stmt
}

func prepnOrPanic(db *sqlx.DB, query string) *sqlx.NamedStmt {
	stmt, err := db.PrepareNamed(query)
	if err != nil {
		panic(fmt.Sprintf("prepnOrPanic: %s", err))
	}
	return stmt
}

func newInStmt(db *sqlx.DB, query string) *inStmt {
	return &inStmt{db: db, query: query}
}

// inStmt is used to execute sql stmts with IN clause, handy when working with mysql
type inStmt struct {
	db    *sqlx.DB
	query string
}

func (is *inStmt) Select(dest interface{}, args ...interface{}) error {
	query, args, err := sqlx.In(string(is.query), args...)
	if err != nil {
		return err
	}

	return is.db.Select(dest, is.db.Rebind(query), args...)
}

func (is *inStmt) SelectContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	query, args, err := sqlx.In(string(is.query), args...)
	if err != nil {
		return err
	}

	return is.db.SelectContext(ctx, dest, is.db.Rebind(query), args...)
}

func newNamedInStmt(db *sqlx.DB, query string) *namedInStmt {
	return &namedInStmt{db: db, query: query}
}

type namedInStmt struct {
	db    *sqlx.DB
	query string
}

func (nis *namedInStmt) Select(dest interface{}, arg interface{}) error {
	query, args, err := sqlx.Named(nis.query, arg)
	if err != nil {
		return err
	}

	query, args, err = sqlx.In(string(query), args...)
	if err != nil {
		return err
	}

	return nis.db.Select(dest, nis.db.Rebind(query), args...)
}

func (nis *namedInStmt) SelectContext(ctx context.Context, dest interface{}, arg interface{}) error {
	query, args, err := sqlx.Named(nis.query, arg)
	if err != nil {
		return err
	}

	query, args, err = sqlx.In(string(query), args...)
	if err != nil {
		return err
	}

	return nis.db.SelectContext(ctx, dest, nis.db.Rebind(query), args...)
}

func (nis *namedInStmt) GetContext(ctx context.Context, dest interface{}, arg interface{}) error {
	query, args, err := sqlx.Named(nis.query, arg)
	if err != nil {
		return err
	}

	query, args, err = sqlx.In(string(query), args...)
	if err != nil {
		return err
	}

	return nis.db.GetContext(ctx, dest, nis.db.Rebind(query), args...)
}

func (nis *namedInStmt) QueryRowxContext(ctx context.Context, arg interface{}) (*sqlx.Row, error) {
	query, args, err := sqlx.Named(nis.query, arg)
	if err != nil {
		return nil, err
	}

	query, args, err = sqlx.In(string(query), args...)
	if err != nil {
		return nil, err
	}

	return nis.db.QueryRowxContext(ctx, nis.db.Rebind(query), args...), nil
}

func render(tmpl *template.Template, data interface{}) (string, error) {
	var b strings.Builder
	err := tmpl.Execute(&b, data)
	return b.String(), err
}

func mustRender(tmpl *template.Template, data interface{}) string {
	s, err := render(tmpl, data)
	if err != nil {
		panic(err)
	}
	return s
}

func entityPermittedColumns(entity interface{}) (res map[kt.SQLColumn]struct{}) {
	t := reflect.TypeOf(entity)

	res = make(map[kt.SQLColumn]struct{}, t.NumField())
	for i := 0; i < t.NumField(); i++ {

		dbTag := t.Field(i).Tag.Get("db")
		if len(dbTag) > 0 {
			res[kt.SQLColumn(dbTag)] = struct{}{}
		}
	}

	return
}

func boxTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
