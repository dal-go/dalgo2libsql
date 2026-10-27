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

func TestExecuteDecodesEveryStorageClass(t *testing.T) {
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
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(pipelineOK(map[string]any{
			"cols": []any{
				map[string]any{"name": "n", "decltype": "INTEGER"},
				map[string]any{"name": "f", "decltype": "REAL"},
				map[string]any{"name": "t", "decltype": "TEXT"},
				map[string]any{"name": "b", "decltype": "BLOB"},
				map[string]any{"name": "z", "decltype": ""},
			},
			"rows": [][]any{{
				map[string]any{"type": "integer", "value": "9223372036854775807"},
				map[string]any{"type": "float", "value": "0.5"},
				map[string]any{"type": "text", "value": "hi"},
				map[string]any{"type": "blob", "value": base64.StdEncoding.EncodeToString([]byte{0, 1, 2})},
				map[string]any{"type": "null"},
			}},
			"affected_row_count": 0,
		}))
	}, func(c *Config) {
		c.Headers = func(ctx context.Context) (map[string]string, error) {
			headerCalls.Add(1)
			return map[string]string{"Authorization": "Bearer t"}, nil
		}
	})

	result, err := client.Execute(context.Background(), "SELECT 1", nil)
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
}

func TestExecuteRefusesErrorInCloseResult(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"baton": nil,
		"results": []any{
			map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": map[string]any{
				"cols": []any{}, "rows": []any{}, "affected_row_count": 0,
			}}},
			map[string]any{"type": "error", "error": map[string]any{"message": "secret detail", "code": "CLOSE_FAILED"}},
		},
	})
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}, nil)
	_, err := client.Execute(context.Background(), "SELECT 1", nil)
	if err == nil {
		t.Fatal("expected an error from the close result")
	}
	if strings.Contains(err.Error(), "secret detail") {
		t.Fatalf("server text leaked: %v", err)
	}
	var protocolErr *Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != "CLOSE_FAILED" {
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
		_, err := client.Execute(context.Background(), "SELECT 1", nil)
		if err == nil {
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
				map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": map[string]any{"cols": []any{}, "rows": []any{}}}},
				map[string]any{"type": "ok", "response": map[string]any{"type": "close"}},
			},
		})
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}, nil)
		_, err := client.Execute(context.Background(), "SELECT 1", nil)
		if err == nil {
			t.Fatal("expected a base_url refusal")
		}
	})
}

func TestExecuteRefusesMalformedAndOversizedResponses(t *testing.T) {
	cases := map[string][]byte{
		"baton": []byte(`{"baton":"x","results":[]}`),
		"negative affected": pipelineOK(map[string]any{
			"cols": []any{}, "rows": []any{}, "affected_row_count": -1,
		}),
		"integer overflow": pipelineOK(map[string]any{
			"cols": []any{map[string]any{"name": "n", "decltype": "INTEGER"}},
			"rows": [][]any{[]any{map[string]any{"type": "integer", "value": "9223372036854775808"}}},
		}),
		"malformed float": pipelineOK(map[string]any{
			"cols": []any{map[string]any{"name": "f", "decltype": "REAL"}},
			"rows": [][]any{[]any{map[string]any{"type": "float", "value": "NaN"}}},
		}),
		"blob base64": pipelineOK(map[string]any{
			"cols": []any{map[string]any{"name": "b", "decltype": "BLOB"}},
			"rows": [][]any{[]any{map[string]any{"type": "blob", "value": "!!!"}}},
		}),
		"row width": pipelineOK(map[string]any{
			"cols": []any{map[string]any{"name": "a", "decltype": ""}},
			"rows": [][]any{[]any{}},
		}),
		"unknown kind": pipelineOK(map[string]any{
			"cols": []any{map[string]any{"name": "a", "decltype": ""}},
			"rows": [][]any{[]any{map[string]any{"type": "timestamp", "value": "1"}}},
		}),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(body)
			}, nil)
			if _, err := client.Execute(context.Background(), "SELECT 1", nil); err == nil {
				t.Fatal("expected a refusal")
			}
		})
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
			_, _ = w.Write(pipelineOK(map[string]any{"cols": []any{}, "rows": []any{}}))
		}, nil)
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); err == nil {
			t.Fatal("expected a content-type refusal")
		}
	})
	t.Run("response bytes", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"baton":null,"results":[]}` + strings.Repeat(" ", 4096)))
		}, func(c *Config) { c.Limits.MaxResponseBytes = 512 })
		_, err := client.Execute(context.Background(), "SELECT 1", nil)
		if !errors.Is(err, ErrLimit) {
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
		body := pipelineOK(map[string]any{
			"cols": []any{map[string]any{"name": "n", "decltype": "INTEGER"}},
			"rows": [][]any{
				{map[string]any{"type": "integer", "value": "1"}},
				{map[string]any{"type": "integer", "value": "2"}},
			},
		})
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}, func(c *Config) { c.Limits.MaxRows = 1 })
		if _, err := client.Execute(context.Background(), "SELECT 1", nil); !errors.Is(err, ErrLimit) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("operation rows across calls", func(t *testing.T) {
		body := pipelineOK(map[string]any{
			"cols": []any{map[string]any{"name": "n", "decltype": "INTEGER"}},
			"rows": [][]any{[]any{map[string]any{"type": "integer", "value": "1"}}},
		})
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
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
