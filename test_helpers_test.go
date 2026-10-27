package dalgo2libsql

import (
	"errors"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// requireNativeValue compares one cell, telling a typed nil from SQL NULL.
func requireNativeValue(t *testing.T, label string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %#v (%T), want %#v (%T)", label, got, got, want, want)
	}
}

func readAllRecordValues(t *testing.T, reader dal.RecordsReader) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for {
		rec, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			return rows
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		data, ok := rec.Data().(map[string]any)
		if !ok {
			t.Fatalf("record data = %T, want a map", rec.Data())
		}
		rows = append(rows, data)
	}
}

func readAllRecordsetValues(t *testing.T, reader dal.RecordsetReader) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for {
		row, rs, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			return rows
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		values := make(map[string]any, len(rs.Columns()))
		for _, column := range rs.Columns() {
			value, err := row.GetValueByName(column.Name(), rs)
			if err != nil {
				t.Fatalf("GetValueByName(%q): %v", column.Name(), err)
			}
			values[column.Name()] = value
		}
		rows = append(rows, values)
	}
}

func requireGroups(t *testing.T, label string, got, want []map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %d rows, want %d: %#v", label, len(got), len(want), got)
	}
	for i := range want {
		for name, wantValue := range want[i] {
			requireNativeValue(t, label+" row "+name, got[i][name], wantValue)
		}
	}
}
