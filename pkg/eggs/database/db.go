package database

import (
	"database/sql"
	"reflect"
)

func CloseStatements(ds interface{}) {
	v := reflect.ValueOf(ds)
	t := v.Type()

	if t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = v.Type()
	}

	if t.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < t.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() != reflect.Ptr || f.IsNil() || !f.CanInterface() {
			continue
		}
		if stmt, ok := f.Interface().(*sql.Stmt); ok {
			stmt.Close() // nolint: errcheck
		}
	}
}
