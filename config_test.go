package dalgo2libsql

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
)

func TestNormalizeURL(t *testing.T) {
	ok := map[string]string{
		"https://db.example.com":           "https://db.example.com",
		"libsql://db.example.com":          "https://db.example.com",
		"https://db.example.com/base":      "https://db.example.com/base",
		"HTTPS://db.example.com:8443":      "https://db.example.com:8443",
		"https://db.example.com/namespace": "https://db.example.com/namespace",
	}
	for raw, want := range ok {
		t.Run("ok/"+raw, func(t *testing.T) {
			got, err := normalizeURL(raw, false)
			if err != nil {
				t.Fatalf("normalizeURL: %v", err)
			}
			if got.String() != want {
				t.Fatalf("got %q, want %q", got.String(), want)
			}
		})
	}
	refused := []string{
		"",
		" ftp://db.example.com",
		"ftp://db.example.com",
		"https://user:pass@db.example.com",
		"https://db.example.com?x=1",
		"https://db.example.com#frag",
		"https://db.example.com/v3/pipeline",
		"https://db.example.com/v2",
		"https://db.example.com/health",
		"https://db.example.com/a/../b",
		"https://db.example.com//a",
		"https:///nohost",
		"http://db.example.com",
		"libsql://",
	}
	for _, raw := range refused {
		t.Run("refused/"+raw, func(t *testing.T) {
			if _, err := normalizeURL(raw, false); err == nil {
				t.Fatalf("%q was accepted", raw)
			}
		})
	}
	if _, err := normalizeURL("http://127.0.0.1:8080", true); err != nil {
		t.Fatalf("loopback HTTP: %v", err)
	}
	if _, err := normalizeURL("http://[::1]:8080", true); err != nil {
		t.Fatalf("loopback IPv6 HTTP: %v", err)
	}
	if _, err := normalizeURL("http://127.0.0.1:8080/x", false); err == nil {
		t.Fatal("loopback HTTP without the opt-in was accepted")
	}
}

func closeDB(t *testing.T, db dal.DB) {
	t.Helper()
	closer, ok := dal.As[io.Closer](db)
	if !ok {
		t.Fatal("the database is not closable")
	}
	if err := closer.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestNewDBPerformsNoIO(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	db, err := NewDB(context.Background(), Config{
		URL: server.URL, AllowLoopbackHTTP: true,
		Headers: func(context.Context) (map[string]string, error) {
			return nil, errors.New("must not be called during construction")
		},
		HTTPClient: &http.Client{Timeout: time.Second},
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("construction made %d requests", calls.Load())
	}
	closer, ok := dal.As[io.Closer](db)
	if !ok {
		t.Fatal("the database is not closable")
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestNewDBRefusesInvalidConfig(t *testing.T) {
	cases := map[string]Config{
		"empty URL":    {},
		"userinfo":     {URL: "https://u:p@db.example.com"},
		"bad scheme":   {URL: "ftp://db.example.com"},
		"max open":     {URL: "https://db.example.com", MaxOpenConns: 99},
		"timeout":      {URL: "https://db.example.com", Timeout: time.Hour},
		"request size": {URL: "https://db.example.com", MaxRequestBytes: 1 << 30},
		"recordset":    {URL: "https://db.example.com", Recordsets: map[string]string{"bad name": "id"}},
		"pk name":      {URL: "https://db.example.com", Recordsets: map[string]string{"t": "bad-name"}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewDB(context.Background(), cfg); err == nil {
				t.Fatal("expected a refusal")
			}
		})
	}
}

func TestCopiedHTTPClientAndRedirectRefusal(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer server.Close()
	supplied := &http.Client{Timeout: 5 * time.Second}
	db, err := NewDB(context.Background(), Config{URL: server.URL, AllowLoopbackHTTP: true, HTTPClient: supplied})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer closeDB(t, db)
	if supplied.CheckRedirect != nil {
		t.Fatal("the caller's client was mutated")
	}
	if _, ok := dal.As[dbschema.SchemaReader](db); !ok {
		t.Fatal("the backend does not expose the borrowed schema reader")
	}
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("the backend does not expose the borrowed schema reader")
	}
	if _, err := reader.ListCollections(context.Background(), nil); err == nil {
		t.Fatal("a redirect was accepted")
	}
	if targetCalls.Load() != 0 {
		t.Fatal("redirect was followed")
	}
}

func TestNewDBRefusesUnsafeHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request expected")
	}))
	defer server.Close()
	db, err := NewDB(context.Background(), Config{
		URL: server.URL, AllowLoopbackHTTP: true,
		Headers: func(context.Context) (map[string]string, error) {
			return map[string]string{"X-Test": "a\r\nInjected: 1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer closeDB(t, db)
	// A metadata read is the first request; it must refuse the unsafe header
	// before the HTTP call.
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("the backend does not expose the borrowed schema reader")
	}
	if _, err := reader.ListCollections(context.Background(), nil); err == nil {
		t.Fatal("an unsafe header was accepted")
	}
}
