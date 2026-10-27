package hrana

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t *testing.T, handler http.HandlerFunc, mutate func(*Config)) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	u := mustParse(t, server.URL)
	cfg := Config{Endpoint: u, Limits: Limits{Timeout: int64(5 * time.Second)}}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

// stmtResult builds a complete, conforming StmtResult: every mandatory field
// is present, and unknown fields are absent.
func stmtResult(cols []any, rows ...[]any) map[string]any {
	if cols == nil {
		cols = []any{}
	}
	if rows == nil {
		rows = [][]any{}
	}
	return map[string]any{
		"cols":               cols,
		"rows":               rows,
		"affected_row_count": 0,
		"last_insert_rowid":  nil,
		"rows_read":          0,
		"rows_written":       0,
		"query_duration_ms":  0,
	}
}

func pipelineOK(result map[string]any) []byte {
	body := map[string]any{
		"baton": nil,
		"results": []any{
			map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": result}},
			map[string]any{"type": "ok", "response": map[string]any{"type": "close"}},
		},
	}
	raw, _ := json.Marshal(body)
	return raw
}

func writeBody(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func TestExecuteDecodesAndEncodesTheOfficialValueShape(t *testing.T) {
	var sawBody map[string]any
	var calls atomic.Int32
	var headerCalls atomic.Int32
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer t" {
			t.Errorf("authorization = %q", got)
		}
		if got := r.URL.Path; got != "/v3/pipeline" {
			t.Errorf("path = %q", got)
		}
		_ = json.NewDecoder(r.Body).Decode(&sawBody)
		writeBody(w, pipelineOK(stmtResult(
			[]any{
				map[string]any{"name": "n", "decltype": "INTEGER"},
				map[string]any{"name": "f", "decltype": "REAL"},
				map[string]any{"name": "t", "decltype": "TEXT"},
				map[string]any{"name": "b", "decltype": "BLOB"},
				map[string]any{"name": "z", "decltype": nil},
			},
			[]any{
				map[string]any{"type": "integer", "value": "9223372036854775807"},
				map[string]any{"type": "float", "value": 0.5},
				map[string]any{"type": "text", "value": "hi"},
				map[string]any{"type": "blob", "base64": base64.StdEncoding.EncodeToString([]byte{0, 1, 2})},
				map[string]any{"type": "null"},
			},
		)))
	}, func(c *Config) {
		c.Headers = func(ctx context.Context) (map[string]string, error) {
			headerCalls.Add(1)
			return map[string]string{"Authorization": "Bearer t"}, nil
		}
	})

	blob := []byte{9, 8}
	result, err := client.Execute(context.Background(), "SELECT ?, ?, ?", []any{int64(7), 1.25, blob})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if calls.Load() != 1 || headerCalls.Load() != 1 {
		t.Fatalf("calls = %d, header calls = %d", calls.Load(), headerCalls.Load())
	}
	if len(result.Columns) != 5 || result.Columns[0].Name != "n" || result.Columns[0].DeclType != "INTEGER" {
		t.Fatalf("columns = %#v", result.Columns)
	}
	row := result.Rows[0]
	if row[0].Kind != Integer || row[0].Int != 9223372036854775807 {
		t.Errorf("integer = %#v", row[0])
	}
	if row[1].Kind != Float || row[1].Float != 0.5 {
		t.Errorf("float = %#v", row[1])
	}
	if row[2].Kind != Text || row[2].Text != "hi" {
		t.Errorf("text = %#v", row[2])
	}
	if row[3].Kind != Blob || string(row[3].Blob) != "\x00\x01\x02" {
		t.Errorf("blob = %#v", row[3])
	}
	if row[4].Kind != Null {
		t.Errorf("null = %#v", row[4])
	}

	// The request must use the official argument shapes: an integer is a
	// decimal string, a float is a JSON number, a blob lives in "base64" and
	// null carries no value field at all.
	requests, _ := sawBody["requests"].([]any)
	if len(requests) != 2 {
		t.Fatalf("requests = %#v", sawBody["requests"])
	}
	exec := requests[0].(map[string]any)
	if exec["type"] != "execute" || requests[1].(map[string]any)["type"] != "close" {
		t.Fatalf("unexpected pipeline: %#v", requests)
	}
	if sawBody["baton"] != nil {
		t.Fatalf("a stateless request must not carry a baton: %#v", sawBody["baton"])
	}
	stmtBody, _ := exec["stmt"].(map[string]any)
	args, _ := stmtBody["args"].([]any)
	if len(args) != 3 {
		t.Fatalf("args = %#v", args)
	}
	integer, _ := args[0].(map[string]any)
	if integer["type"] != "integer" || integer["value"] != "7" {
		t.Errorf("integer argument = %#v", integer)
	}
	floatArg, _ := args[1].(map[string]any)
	if floatArg["type"] != "float" || floatArg["value"] != 1.25 {
		t.Errorf("float argument = %#v", floatArg)
	}
	blobArg, _ := args[2].(map[string]any)
	if blobArg["type"] != "blob" || blobArg["base64"] != base64.StdEncoding.EncodeToString(blob) {
		t.Errorf("blob argument = %#v", blobArg)
	}
	if _, present := blobArg["value"]; present {
		t.Errorf("a blob argument carried a value field: %#v", blobArg)
	}
}

func TestExecuteEncodesNullAndTextArguments(t *testing.T) {
	var sawBody map[string]any
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&sawBody)
		writeBody(w, pipelineOK(stmtResult(nil)))
	}, nil)
	if _, err := client.Execute(context.Background(), "SELECT ?, ?", []any{nil, ""}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	requests := sawBody["requests"].([]any)
	stmtBody := requests[0].(map[string]any)["stmt"].(map[string]any)
	args := stmtBody["args"].([]any)
	nullArg := args[0].(map[string]any)
	if nullArg["type"] != "null" {
		t.Errorf("null argument = %#v", nullArg)
	}
	if _, present := nullArg["value"]; present {
		t.Errorf("a null argument carried a value field: %#v", nullArg)
	}
	textArg := args[1].(map[string]any)
	if textArg["type"] != "text" || textArg["value"] != "" {
		t.Errorf("text argument = %#v", textArg)
	}
}

func TestExecuteRefusesErrorInCloseResult(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"baton": nil,
		"results": []any{
			map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": stmtResult(nil)}},
			map[string]any{"type": "error", "error": map[string]any{"message": "secret detail", "code": "CLOSE_FAILED"}},
		},
	})
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeBody(w, body)
	}, nil)
	_, err := client.Execute(context.Background(), "SELECT 1", nil)
	if err == nil {
		t.Fatal("expected an error from the close result")
	}
	if strings.Contains(err.Error(), "secret detail") || strings.Contains(err.Error(), "CLOSE_FAILED") {
		t.Fatalf("server text leaked: %v", err)
	}
	var protocolErr *Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != "" {
		t.Fatalf("error = %#v", err)
	}
}

func TestExecuteKeepsAllowlistedServerCode(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"baton": nil,
		"results": []any{
			map[string]any{"type": "error", "error": map[string]any{"message": "m", "code": "SQLITE_BUSY"}},
			map[string]any{"type": "ok", "response": map[string]any{"type": "close"}},
		},
	})
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeBody(w, body)
	}, nil)
	_, err := client.Execute(context.Background(), "SELECT 1", nil)
	var protocolErr *Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != "SQLITE_BUSY" {
		t.Fatalf("error = %#v", err)
	}
}

func TestExecuteRefusesRedirectAndBaseURLChange(t *testing.T) {
	t.Run("redirect", func(t *testing.T) {
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			targetCalls.Add(1)
		}))
		defer target.Close()
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL+"/v3/pipeline", http.StatusTemporaryRedirect)
		}, nil)
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); err == nil {
			t.Fatal("expected a refusal")
		}
		if targetCalls.Load() != 0 {
			t.Fatal("a redirect was followed")
		}
	})
	t.Run("base_url", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{
			"baton":    nil,
			"base_url": "https://evil.example.com/v3/pipeline",
			"results": []any{
				map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": stmtResult(nil)}},
				map[string]any{"type": "ok", "response": map[string]any{"type": "close"}},
			},
		})
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			writeBody(w, body)
		}, nil)
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); err == nil {
			t.Fatal("expected a base_url refusal")
		}
	})
}

func TestExecuteRefusesMalformedAndOversizedResponses(t *testing.T) {
	resultWith := func(mutate func(map[string]any)) map[string]any {
		result := stmtResult(nil)
		mutate(result)
		return result
	}
	cases := map[string][]byte{
		"baton": []byte(`{"baton":"x","results":[]}`),
		"negative affected": pipelineOK(resultWith(func(r map[string]any) {
			r["affected_row_count"] = -1
		})),
		"missing affected": pipelineOK(resultWith(func(r map[string]any) { delete(r, "affected_row_count") })),
		"missing cols":     pipelineOK(resultWith(func(r map[string]any) { delete(r, "cols") })),
		"null cols":        pipelineOK(resultWith(func(r map[string]any) { r["cols"] = nil })),
		"missing rows":     pipelineOK(resultWith(func(r map[string]any) { delete(r, "rows") })),
		"null rows":        pipelineOK(resultWith(func(r map[string]any) { r["rows"] = nil })),
		"missing last_insert_rowid": pipelineOK(resultWith(func(r map[string]any) {
			delete(r, "last_insert_rowid")
		})),
		"numeric last_insert_rowid": pipelineOK(resultWith(func(r map[string]any) {
			r["last_insert_rowid"] = 1
		})),
		"missing rows_read":    pipelineOK(resultWith(func(r map[string]any) { delete(r, "rows_read") })),
		"missing rows_written": pipelineOK(resultWith(func(r map[string]any) { delete(r, "rows_written") })),
		"missing query_duration_ms": pipelineOK(resultWith(func(r map[string]any) {
			delete(r, "query_duration_ms")
		})),
		"integer overflow": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "n", "decltype": "INTEGER"}},
			[]any{map[string]any{"type": "integer", "value": "9223372036854775808"}},
		)),
		"integer as number": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "n", "decltype": "INTEGER"}},
			[]any{map[string]any{"type": "integer", "value": 1}},
		)),
		"integer with base64": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "n", "decltype": "INTEGER"}},
			[]any{map[string]any{"type": "integer", "value": "1", "base64": "AA=="}},
		)),
		"float as string": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "f", "decltype": "REAL"}},
			[]any{map[string]any{"type": "float", "value": "0.5"}},
		)),
		"float out of range": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "f", "decltype": "REAL"}},
			[]any{map[string]any{"type": "float", "value": json.RawMessage("1e400")}},
		)),
		"blob in value": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "b", "decltype": "BLOB"}},
			[]any{map[string]any{"type": "blob", "value": "AA=="}},
		)),
		"blob without base64": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "b", "decltype": "BLOB"}},
			[]any{map[string]any{"type": "blob"}},
		)),
		"blob invalid base64": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "b", "decltype": "BLOB"}},
			[]any{map[string]any{"type": "blob", "base64": "!!!"}},
		)),
		"null with value": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "z", "decltype": nil}},
			[]any{map[string]any{"type": "null", "value": "x"}},
		)),
		"both value and base64": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "t", "decltype": "TEXT"}},
			[]any{map[string]any{"type": "text", "value": "x", "base64": "AA=="}},
		)),
		"row width": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "a", "decltype": nil}},
			[]any{},
		)),
		"null row": pipelineOK(resultWith(func(r map[string]any) {
			r["cols"] = []any{map[string]any{"name": "a", "decltype": nil}}
			r["rows"] = []any{nil}
		})),
		"unknown kind": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "a", "decltype": nil}},
			[]any{map[string]any{"type": "timestamp", "value": "1"}},
		)),
		"column name number": pipelineOK(stmtResult(
			[]any{map[string]any{"name": 7, "decltype": nil}},
		)),
		"missing decltype": pipelineOK(stmtResult(
			[]any{map[string]any{"name": "a"}},
		)),
		"error with response": []byte(`{"baton":null,"results":[` +
			`{"type":"error","response":{"type":"close"},"error":{"message":"m"}},` +
			`{"type":"ok","response":{"type":"close"}}]}`),
		"ok with error": []byte(`{"baton":null,"results":[` +
			`{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"rows_read":0,"rows_written":0,"query_duration_ms":0}},` +
			`{"type":"ok","error":{"message":"m"},"response":{"type":"close"}}]}`),
		"error without error": []byte(`{"baton":null,"results":[` +
			`{"type":"error"},{"type":"ok","response":{"type":"close"}}]}`),
		"error without message": []byte(`{"baton":null,"results":[` +
			`{"type":"error","error":{"code":"SQLITE_BUSY"}},{"type":"ok","response":{"type":"close"}}]}`),
		"wrong response type": []byte(`{"baton":null,"results":[` +
			`{"type":"ok","response":{"type":"close"}},{"type":"ok","response":{"type":"close"}}]}`),
		"extra close payload": []byte(`{"baton":null,"results":[` +
			`{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"rows_read":0,"rows_written":0,"query_duration_ms":0}}},` +
			`{"type":"ok","response":{"type":"close","result":{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"rows_read":0,"rows_written":0,"query_duration_ms":0}}}]}`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				writeBody(w, body)
			}, nil)
			if _, err := client.Execute(context.Background(), "SELECT 1", nil); err == nil {
				t.Fatal("expected a refusal")
			}
		})
	}
}

func TestExecuteAcceptsUnpaddedBase64Blob(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeBody(w, pipelineOK(stmtResult(
			[]any{map[string]any{"name": "b", "decltype": "BLOB"}},
			[]any{map[string]any{"type": "blob", "base64": "AQ"}},
		)))
	}, nil)
	result, err := client.Execute(context.Background(), "SELECT x'01'", nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Rows[0][0].Kind != Blob || len(result.Rows[0][0].Blob) != 1 || result.Rows[0][0].Blob[0] != 1 {
		t.Fatalf("blob = %#v", result.Rows[0][0])
	}
}

func TestExecuteAcceptsZeroRowsAndColumns(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeBody(w, pipelineOK(stmtResult([]any{})))
	}, nil)
	result, err := client.Execute(context.Background(), "SELECT 1 WHERE 0", nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Columns) != 0 || len(result.Rows) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestExecuteRefusesUnsafeResponseFraming(t *testing.T) {
	t.Run("status", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"token":"secret"}`))
		}, nil)
		_, err := client.Execute(context.Background(), "SELECT 1", nil)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("error = %v", err)
		}
		var protocolErr *Error
		if !errors.As(err, &protocolErr) || protocolErr.Status != http.StatusUnauthorized {
			t.Fatalf("error = %#v", err)
		}
	})
	t.Run("content type", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write(pipelineOK(stmtResult(nil)))
		}, nil)
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); err == nil {
			t.Fatal("expected a content-type refusal")
		}
	})
	t.Run("response bytes", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			writeBody(w, []byte(`{"baton":null,"results":[]}`+strings.Repeat(" ", 4096)))
		}, func(c *Config) { c.Limits.MaxResponseBytes = 512 })
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); !errors.Is(err, ErrLimit) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestExecuteBounds(t *testing.T) {
	t.Run("statement", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			t.Error("no request expected")
		}, func(c *Config) { c.Limits.MaxStatementBytes = 4 })
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); !errors.Is(err, ErrLimit) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("arguments", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			t.Error("no request expected")
		}, func(c *Config) { c.Limits.MaxArgs = 1 })
		if _, err := client.Execute(context.Background(), "SELECT ?, ?", []any{1, 2}); !errors.Is(err, ErrLimit) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("rows", func(t *testing.T) {
		body := pipelineOK(stmtResult(
			[]any{map[string]any{"name": "n", "decltype": "INTEGER"}},
			[]any{map[string]any{"type": "integer", "value": "1"}},
			[]any{map[string]any{"type": "integer", "value": "2"}},
		))
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			writeBody(w, body)
		}, func(c *Config) { c.Limits.MaxRows = 1 })
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); !errors.Is(err, ErrLimit) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("operation rows across calls", func(t *testing.T) {
		body := pipelineOK(stmtResult(
			[]any{map[string]any{"name": "n", "decltype": "INTEGER"}},
			[]any{map[string]any{"type": "integer", "value": "1"}},
		))
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			writeBody(w, body)
		}, func(c *Config) {
			c.Limits.MaxOperationRows = 1
			c.Limits.MaxOperationRequests = 4
			c.Limits.MaxRows = 10
		})
		ctx := WithBudget(context.Background(), NewBudget(Limits{MaxOperationRows: 1, MaxOperationRequests: 4, MaxRows: 10}))
		if _, err := client.Execute(ctx, "SELECT 1", nil); err != nil {
			t.Fatalf("first call: %v", err)
		}
		if _, err := client.Execute(ctx, "SELECT 1", nil); !errors.Is(err, ErrLimit) {
			t.Fatalf("second call error = %v", err)
		}
	})
	t.Run("operation requests across calls", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			writeBody(w, pipelineOK(stmtResult(nil)))
		}, nil)
		ctx := WithBudget(context.Background(), NewBudget(Limits{MaxOperationRequests: 1}))
		if _, err := client.Execute(ctx, "SELECT 1", nil); err != nil {
			t.Fatalf("first call: %v", err)
		}
		if _, err := client.Execute(ctx, "SELECT 1", nil); !errors.Is(err, ErrLimit) {
			t.Fatalf("second call error = %v", err)
		}
	})
}

func TestExecuteRefusesHeaderProblems(t *testing.T) {
	t.Run("newline", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			t.Error("no request expected")
		}, func(c *Config) {
			c.Headers = func(context.Context) (map[string]string, error) {
				return map[string]string{"X-Test": "a\r\nX-Injected: b"}, nil
			}
		})
		_, err := client.Execute(context.Background(), "SELECT 1", nil)
		if err == nil || strings.Contains(err.Error(), "Injected") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("protocol override", func(t *testing.T) {
		for _, name := range []string{"Host", "Content-Type", "Content-Length", "Connection"} {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				t.Error("no request expected")
			}, func(c *Config) {
				c.Headers = func(context.Context) (map[string]string, error) {
					return map[string]string{name: "x"}, nil
				}
			})
			if _, err := client.Execute(context.Background(), "SELECT 1", nil); err == nil {
				t.Fatalf("%s was accepted", name)
			}
		}
	})
	t.Run("provider error is sanitized", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			t.Error("no request expected")
		}, func(c *Config) {
			c.Headers = func(context.Context) (map[string]string, error) {
				return nil, errors.New("token=super-secret")
			}
		})
		_, err := client.Execute(context.Background(), "SELECT 1", nil)
		if err == nil || strings.Contains(err.Error(), "super-secret") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("provider gets the request deadline", func(t *testing.T) {
		var deadlineOK atomic.Bool
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			t.Error("no request expected")
		}, func(c *Config) {
			c.Limits.Timeout = int64(50 * time.Millisecond)
			c.Headers = func(ctx context.Context) (map[string]string, error) {
				_, hasDeadline := ctx.Deadline()
				deadlineOK.Store(hasDeadline)
				<-ctx.Done()
				return nil, ctx.Err()
			}
		})
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v", err)
		}
		if !deadlineOK.Load() {
			t.Fatal("the header provider did not receive a deadline")
		}
	})
	t.Run("ignoring provider leaks no goroutine", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			writeBody(w, pipelineOK(stmtResult(nil)))
		}, func(c *Config) {
			c.Headers = func(context.Context) (map[string]string, error) {
				return map[string]string{"X-Test": "v"}, nil
			}
		})
		before := runtime.NumGoroutine()
		for i := 0; i < 100; i++ {
			if _, err := client.Execute(context.Background(), "SELECT 1", nil); err != nil {
				t.Fatalf("Execute: %v", err)
			}
		}
		runtime.Gosched()
		if grown := runtime.NumGoroutine() - before; grown > 10 {
			t.Fatalf("goroutines grew by %d", grown)
		}
	})
}

func TestExecuteKeepsContextIdentity(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		release := make(chan struct{})
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			<-release
		}, nil)
		defer close(release)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		if _, err := client.Execute(ctx, "SELECT 1", nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}, func(c *Config) { c.Limits.Timeout = int64(20 * time.Millisecond) })
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("deadline while reading the body", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}, func(c *Config) { c.Limits.Timeout = int64(20 * time.Millisecond) })
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestNewRejectsUnsafeEndpoint(t *testing.T) {
	base := mustParse(t, "https://db.example.com")
	with := func(mutate func(*url.URL)) *url.URL {
		u := *base
		mutate(&u)
		return &u
	}
	for name, endpoint := range map[string]*url.URL{
		"userinfo": with(func(u *url.URL) { u.User = url.UserPassword("u", "p") }),
		"query":    with(func(u *url.URL) { u.RawQuery = "x=1" }),
		"fragment": with(func(u *url.URL) { u.Fragment = "f" }),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(Config{Endpoint: endpoint}); err == nil {
				t.Fatal("expected a refusal")
			}
		})
	}
}

func TestReadBoundedStopsEarly(t *testing.T) {
	reader := strings.NewReader(strings.Repeat("x", 1000))
	if _, err := readBounded(io.Reader(reader), 10); !errors.Is(err, ErrLimit) {
		t.Fatalf("error = %v", err)
	}
}
