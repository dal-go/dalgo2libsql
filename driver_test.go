package dalgo2libsql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo2libsql/internal/hrana"
)

func newPipelineServer(t *testing.T, result map[string]any, calls *int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			*calls++
		}
		body, _ := json.Marshal(map[string]any{
			"baton": nil,
			"results": []any{
				map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": result}},
				map[string]any{"type": "ok", "response": map[string]any{"type": "close"}},
			},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server
}

func newTestConnector(t *testing.T, serverURL string) *connector {
	t.Helper()
	endpoint, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	client, err := hrana.New(hrana.Config{Endpoint: endpoint})
	if err != nil {
		t.Fatalf("hrana.New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &connector{client: client}
}

func TestSQLDriverScansTypedRows(t *testing.T) {
	server := newPipelineServer(t, map[string]any{
		"cols": []any{
			map[string]any{"name": "n", "decltype": "INTEGER"},
			map[string]any{"name": "f", "decltype": "REAL"},
			map[string]any{"name": "t", "decltype": "TEXT"},
			map[string]any{"name": "b", "decltype": "BLOB"},
			map[string]any{"name": "z", "decltype": ""},
			map[string]any{"name": "nullable", "decltype": "TEXT"},
		},
		"rows": [][]any{[]any{
			map[string]any{"type": "integer", "value": "-9223372036854775808"},
			map[string]any{"type": "float", "value": "1.25"},
			map[string]any{"type": "text", "value": "héllo"},
			map[string]any{"type": "blob", "value": base64.StdEncoding.EncodeToString([]byte{7, 8})},
			map[string]any{"type": "null"},
			map[string]any{"type": "null"},
		}},
	}, nil)
	connector := newTestConnector(t, server.URL)
	pool := sql.OpenDB(connector)
	defer pool.Close()

	var n int64
	var f float64
	var text string
	var blob []byte
	var expression, nullable any
	rows, err := pool.QueryContext(context.Background(), "SELECT n, f, t, b, 1 AS z, nullable FROM t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("column types: %v", err)
	}
	if len(types) != 6 {
		t.Fatalf("column count = %d", len(types))
	}
	if got := types[0].DatabaseTypeName(); got != "INTEGER" {
		t.Errorf("database type name = %q", got)
	}
	if got := types[0].ScanType(); got != reflect.TypeOf(int64(0)) {
		t.Errorf("integer scan type = %v", got)
	}
	if got := types[1].ScanType(); got != reflect.TypeOf(float64(0)) {
		t.Errorf("real scan type = %v", got)
	}
	if got := types[2].ScanType(); got != reflect.TypeOf("") {
		t.Errorf("text scan type = %v", got)
	}
	if got := types[3].ScanType(); got != reflect.TypeOf([]byte(nil)) {
		t.Errorf("blob scan type = %v", got)
	}
	if got := types[4].ScanType(); got != reflect.TypeOf((*any)(nil)).Elem() {
		t.Errorf("expression scan type = %v", got)
	}
	if !rows.Next() {
		t.Fatalf("no row: %v", rows.Err())
	}
	if err := rows.Scan(&n, &f, &text, &blob, &expression, &nullable); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != -9223372036854775808 || f != 1.25 || text != "héllo" || string(blob) != "\a\b" {
		t.Fatalf("values = %d %v %q %v", n, f, text, blob)
	}
	if expression != nil || nullable != nil {
		t.Fatalf("nulls = %v %v", expression, nullable)
	}
	if rows.Next() {
		t.Fatal("unexpected second row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
}

func TestSQLDriverRefusesUnsupportedSurfaces(t *testing.T) {
	var calls int
	server := newPipelineServer(t, map[string]any{"cols": []any{}, "rows": []any{}}, &calls)
	connector := newTestConnector(t, server.URL)
	pool := sql.OpenDB(connector)
	defer pool.Close()
	ctx := context.Background()

	if _, err := pool.PrepareContext(ctx, "SELECT 1"); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("prepare error = %v", err)
	}
	if _, err := pool.ExecContext(ctx, "DELETE FROM t"); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("exec error = %v", err)
	}
	tx, err := pool.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("a read-only transaction was started")
	}
	if !errors.Is(err, ErrNotSupported) {
		t.Fatalf("begin error = %v", err)
	}
	if _, err := connector.Driver().Open("libsql://example.com"); err == nil {
		t.Fatal("the unregistered driver accepted a DSN")
	}
	if calls != 0 {
		t.Fatalf("the unsupported paths made %d requests", calls)
	}
	if err := pool.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if calls != 1 {
		t.Fatalf("ping made %d requests", calls)
	}
}

func TestSQLDriverRefusesNamedAndUnsupportedArguments(t *testing.T) {
	server := newPipelineServer(t, map[string]any{"cols": []any{}, "rows": []any{}}, nil)
	connector := newTestConnector(t, server.URL)
	pool := sql.OpenDB(connector)
	defer pool.Close()
	ctx := context.Background()

	if _, err := pool.QueryContext(ctx, "SELECT ?", sql.Named("n", 1)); err == nil {
		t.Fatal("a named argument was accepted")
	}
	if _, err := pool.QueryContext(ctx, "SELECT ?", struct{ X int }{1}); err == nil {
		t.Fatal("an unsupported argument type was accepted")
	}
}

func TestSQLDriverRowsCloseIsIdempotent(t *testing.T) {
	server := newPipelineServer(t, map[string]any{
		"cols": []any{map[string]any{"name": "n", "decltype": "INTEGER"}},
		"rows": [][]any{},
	}, nil)
	connector := newTestConnector(t, server.URL)
	pool := sql.OpenDB(connector)
	defer pool.Close()
	rows, err := pool.QueryContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
