package dalgo2libsql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

// sqliteHrana is a test-only Hrana 3 endpoint backed by an in-process SQLite
// database. It is not a libSQL server and is not a claim about hosted Turso;
// it lets the unit suite exercise the real dalgo2sql SQLite compiler and the
// real schema reader against native SQLite semantics without an account.
//
// The wire it speaks is the official Hrana 3 JSON Value shape:
//
//	{ "type": "integer", "value": "42" }        // decimal string
//	{ "type": "float",   "value": 1.5 }         // JSON number
//	{ "type": "text",    "value": "hi" }
//	{ "type": "blob",    "base64": "AAEC" }
//	{ "type": "null" }
type sqliteHrana struct {
	db  *sql.DB
	url string

	mu         sync.Mutex
	statements []string
	requests   int
}

func newSQLiteHrana(t *testing.T) (*sqliteHrana, *httptest.Server) {
	t.Helper()
	dsn := fmt.Sprintf("file:hrana-test-%p?mode=memory&cache=shared", t)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	server := &sqliteHrana{db: db}
	httpServer := httptest.NewServer(http.HandlerFunc(server.handle))
	server.url = httpServer.URL
	t.Cleanup(httpServer.Close)
	return server, httpServer
}

func (s *sqliteHrana) exec(t *testing.T, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
}

func (s *sqliteHrana) sawStatement(fragment string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, statement := range s.statements {
		if strings.Contains(statement, fragment) {
			return true
		}
	}
	return false
}

func (s *sqliteHrana) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

func (s *sqliteHrana) resetRequests() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = 0
}

type wirePipeline struct {
	Baton    *string `json:"baton"`
	Requests []struct {
		Type string `json:"type"`
		Stmt *struct {
			SQL  string      `json:"sql"`
			Args []wireValue `json:"args"`
		} `json:"stmt"`
	} `json:"requests"`
}

// wireValue decodes the official Value shape and also refuses the shapes the
// old mock emitted (a float as a string, a blob in "value"), so the test
// double cannot silently validate a non-conforming client.
type wireValue struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value"`
	Base64 *string         `json:"base64"`
}

func (s *sqliteHrana) handle(w http.ResponseWriter, r *http.Request) {
	var pipeline wirePipeline
	if err := json.NewDecoder(r.Body).Decode(&pipeline); err != nil {
		writeWireError(w, "BAD_REQUEST")
		return
	}
	results := make([]any, 0, len(pipeline.Requests))
	for _, request := range pipeline.Requests {
		switch request.Type {
		case "close":
			results = append(results, map[string]any{
				"type":     "ok",
				"response": map[string]any{"type": "close"},
			})
		case "execute":
			if request.Stmt == nil {
				writeWireError(w, "BAD_REQUEST")
				return
			}
			s.mu.Lock()
			s.statements = append(s.statements, request.Stmt.SQL)
			s.mu.Unlock()
			result, err := s.execute(r.Context(), request.Stmt.SQL, request.Stmt.Args)
			if err != nil {
				results = append(results, map[string]any{
					"type":  "error",
					"error": map[string]any{"message": "statement failed", "code": "SQLITE_ERROR"},
				})
				continue
			}
			results = append(results, map[string]any{
				"type":     "ok",
				"response": map[string]any{"type": "execute", "result": result},
			})
		default:
			writeWireError(w, "BAD_REQUEST")
			return
		}
	}
	s.mu.Lock()
	s.requests++
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"baton": nil, "results": results})
}

func writeWireError(w http.ResponseWriter, code string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"baton": nil,
		"results": []any{
			map[string]any{"type": "error", "error": map[string]any{"message": "bad request", "code": code}},
		},
	})
}

func (s *sqliteHrana) execute(ctx context.Context, statement string, args []wireValue) (map[string]any, error) {
	values := make([]any, len(args))
	for i, a := range args {
		value, err := decodeWireValue(a)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	rows, err := s.db.QueryContext(ctx, statement, values...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	cols := make([]any, len(columnTypes))
	for i, column := range columnTypes {
		cols[i] = map[string]any{"name": column.Name(), "decltype": column.DatabaseTypeName()}
	}
	outRows := make([]any, 0, 16)
	dest := make([]any, len(columnTypes))
	refs := make([]any, len(columnTypes))
	for i := range dest {
		refs[i] = &dest[i]
	}
	for rows.Next() {
		if err := rows.Scan(refs...); err != nil {
			return nil, err
		}
		cells := make([]any, len(dest))
		for i, value := range dest {
			cells[i] = wireCell(value)
		}
		outRows = append(outRows, cells)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{
		"cols":               cols,
		"rows":               outRows,
		"affected_row_count": 0,
		"last_insert_rowid":  nil,
		"rows_read":          0,
		"rows_written":       0,
		"query_duration_ms":  0,
	}, nil
}

func decodeWireValue(v wireValue) (any, error) {
	switch v.Type {
	case "null":
		if len(v.Value) != 0 || v.Base64 != nil {
			return nil, fmt.Errorf("null value carries a payload")
		}
		return nil, nil
	case "integer":
		var s string
		if err := json.Unmarshal(v.Value, &s); err != nil {
			return nil, fmt.Errorf("integer value is not a JSON string: %w", err)
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	case "float":
		var f float64
		if err := json.Unmarshal(v.Value, &f); err != nil {
			return nil, fmt.Errorf("float value is not a JSON number: %w", err)
		}
		return f, nil
	case "text":
		var s string
		if err := json.Unmarshal(v.Value, &s); err != nil {
			return nil, err
		}
		return s, nil
	case "blob":
		if v.Base64 == nil || len(v.Value) != 0 {
			return nil, fmt.Errorf("blob value is not carried in base64")
		}
		return base64.StdEncoding.DecodeString(*v.Base64)
	default:
		return nil, fmt.Errorf("unknown argument type %q", v.Type)
	}
}

func wireCell(value any) map[string]any {
	switch x := value.(type) {
	case nil:
		return map[string]any{"type": "null"}
	case int64:
		return map[string]any{"type": "integer", "value": strconv.FormatInt(x, 10)}
	case float64:
		return map[string]any{"type": "float", "value": x}
	case bool:
		if x {
			return map[string]any{"type": "integer", "value": "1"}
		}
		return map[string]any{"type": "integer", "value": "0"}
	case string:
		return map[string]any{"type": "text", "value": x}
	case []byte:
		return map[string]any{"type": "blob", "base64": base64.StdEncoding.EncodeToString(x)}
	default:
		return map[string]any{"type": "text", "value": fmt.Sprint(x)}
	}
}
