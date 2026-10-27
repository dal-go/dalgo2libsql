package dalgo2libsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
	"github.com/dal-go/dalgo2sqlite"

	"github.com/dal-go/dalgo2libsql/internal/hrana"
)

// Version is the adapter version reported by Adapter().
const Version = "0.1.0"

// Package-level failures. They are safe to test with errors.Is; their text
// never contains a URL, a credential, SQL text or a response body.
var (
	// ErrNotSupported is returned by an operation this read-only checkpoint
	// does not implement. It is always returned before any I/O.
	ErrNotSupported = errors.New("dalgo2libsql: operation not supported by this read-only adapter")
	// ErrInvalidConfig is returned by NewDB for a configuration it refuses.
	ErrInvalidConfig = errors.New("dalgo2libsql: invalid configuration")
	// ErrUnsafeTarget is returned when a database URL is not an HTTPS (or
	// explicitly allowed loopback HTTP) base URL.
	ErrUnsafeTarget = errors.New("dalgo2libsql: unsafe database URL")
	// ErrUnsupportedResponse is returned when the endpoint answers in a way
	// the adapter refuses to interpret.
	ErrUnsupportedResponse = errors.New("dalgo2libsql: unsupported endpoint response")
)

// HeaderProvider returns request headers for one request. It is called with
// the operation's context and may refresh a token there. Names and values
// must be CR/LF-safe; protocol headers are refused.
type HeaderProvider func(ctx context.Context) (map[string]string, error)

// Config configures one adapter instance for one existing libSQL database.
// The adapter opens a private HTTPS connection pool; it does not create
// tables, run migrations, or use an account or a management API.
type Config struct {
	// URL is the base URL of the libSQL server, for example
	// https://db.example.com or libsql://db.example.com. A libsql:// URL is
	// normalized to https://. A path, when present, is kept as the server's
	// base path. Credentials, a query, a fragment, and an API suffix such as
	// /v3/pipeline are refused. Plain HTTP is accepted only for a loopback
	// host and only when AllowLoopbackHTTP is true.
	URL string

	// ID is the identifier ID() reports. It defaults to the host and path of
	// URL, without credentials (URL cannot carry any).
	ID string

	// Headers supplies credentials per request. The provider is called for
	// every request with the operation's context, before the per-request
	// deadline is applied, so it can refresh an expiring token.
	Headers HeaderProvider

	// HTTPClient optionally supplies the client. NewDB copies it, refuses
	// redirects on the copy and clones an *http.Transport; the caller's
	// client is never mutated.
	HTTPClient *http.Client

	// AllowLoopbackHTTP permits plain HTTP for localhost, 127.0.0.0/8 and
	// ::1 only. It exists for local development against a fixture.
	AllowLoopbackHTTP bool

	// Recordsets declares the recordset (table) name and its single primary
	// key column, for keyed reads. A key is never assumed to be "id": a
	// collection that is not declared here refuses keyed reads, and a
	// declaration is checked against the live schema before it is used.
	// Structured reads discover their key from the SQLite catalog themselves
	// and do not need a declaration.
	//
	// A keyed read target must be map record data: the delegate preserves the
	// native SQLite value of every column, and a struct target is refused
	// before a statement because a field type could force a conversion.
	Recordsets map[string]string

	// MaxOpenConns bounds the private pool. Default 1: schema probes and the
	// statement they inform then share one connection, which is what makes
	// per-connection SQLite settings consistent.
	MaxOpenConns int

	// Timeout bounds one HTTP request and, as one operation deadline, the
	// whole public call: every internal catalog probe, header refresh and data
	// statement of one Get or reader shares it. Default 30s.
	Timeout time.Duration

	// MaxRequestBytes bounds one encoded request body. Default 1 MiB.
	MaxRequestBytes int64
	// MaxResponseBytes bounds one encoded response body. Default 4 MiB.
	MaxResponseBytes int64
	// MaxOperationBytes bounds the response bytes of one DALgo operation,
	// internal metadata probes included. Default 16 MiB.
	MaxOperationBytes int64
	// MaxOperationRequests bounds the pipeline calls of one operation.
	// Default 32.
	MaxOperationRequests int
	// MaxRows bounds the rows of one statement result. Default 10000.
	MaxRows int
	// MaxOperationRows bounds the rows decoded across one operation.
	// Default 50000.
	MaxOperationRows int
	// MaxColumns bounds the columns of one result. Default 256.
	MaxColumns int
	// MaxValueBytes bounds one text or blob value. Default 1 MiB.
	MaxValueBytes int
	// MaxArgs bounds the bound arguments of one statement. Default 256.
	MaxArgs int
}

const (
	defaultMaxOpenConns  = 1
	absoluteMaxOpenConns = 16
	defaultTimeout       = 30 * time.Second
	absoluteMaxTimeout   = 2 * time.Minute
)

// NewDB validates cfg, normalizes the database URL, and returns a read-only
// DALgo database over a private libSQL pool. It performs no I/O: the first
// read (or a schema read) is the connection test, and it runs under that
// caller's context.
//
// A native libsql:// URL is normalized to https://. A URL with credentials,
// a query, a fragment, or an API suffix is refused; so is plain HTTP for a
// non-loopback host. Redirects are never followed.
func NewDB(ctx context.Context, cfg Config) (dal.DB, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	endpoint, err := normalizeURL(cfg.URL, cfg.AllowLoopbackHTTP)
	if err != nil {
		return nil, err
	}
	maxOpenConns := cfg.MaxOpenConns
	if maxOpenConns == 0 {
		maxOpenConns = defaultMaxOpenConns
	}
	if maxOpenConns < 0 || maxOpenConns > absoluteMaxOpenConns {
		return nil, fmt.Errorf("%w: MaxOpenConns must be between 0 and %d", ErrInvalidConfig, absoluteMaxOpenConns)
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	if timeout < 0 || timeout > absoluteMaxTimeout {
		return nil, fmt.Errorf("%w: Timeout must be between 0 and %s", ErrInvalidConfig, absoluteMaxTimeout)
	}
	limits, err := hrana.NormalizeLimits(hrana.Limits{
		MaxRequestBytes:      cfg.MaxRequestBytes,
		MaxResponseBytes:     cfg.MaxResponseBytes,
		MaxOperationBytes:    cfg.MaxOperationBytes,
		MaxOperationRequests: cfg.MaxOperationRequests,
		MaxRows:              cfg.MaxRows,
		MaxOperationRows:     cfg.MaxOperationRows,
		MaxColumns:           cfg.MaxColumns,
		MaxValueBytes:        cfg.MaxValueBytes,
		MaxArgs:              cfg.MaxArgs,
		Timeout:              int64(timeout),
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidConfig, err)
	}

	recordsets, declared, err := buildRecordsets(cfg.Recordsets)
	if err != nil {
		return nil, err
	}

	client, err := hrana.New(hrana.Config{
		Endpoint:   endpoint,
		HTTPClient: cfg.HTTPClient,
		Headers:    hrana.HeaderProvider(cfg.Headers),
		Limits:     limits,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidConfig, err)
	}

	connector := &connector{client: client, limits: limits}
	pool := sql.OpenDB(connector)
	pool.SetMaxOpenConns(maxOpenConns)
	pool.SetMaxIdleConns(maxOpenConns)
	pool.SetConnMaxLifetime(0)

	reader, err := dalgo2sqlite.NewSchemaReader(pool)
	if err != nil {
		_ = pool.Close()
		_ = client.Close()
		return nil, fmt.Errorf("%w: schema reader: %s", ErrInvalidConfig, err)
	}

	// PreserveNativeValues keeps the value libSQL delivers for each result
	// column: int64, float64, string, []byte (already copied by the delegate) or
	// nil for NULL, with no affinity or text normalization. One recordset column
	// may therefore hold any of those classes in different rows, and an aggregate
	// or expression column has a value without needing a declared type.
	//
	// Keyed reads (Get, GetMulti) and structured records readers into map data
	// keep those values. A struct target is refused by the delegate before a
	// statement: a field type could force the very conversion the option exists
	// to avoid, so this adapter documents map record data for keyed reads.
	delegate := dalgo2sql.NewDatabase(pool, dal.NewSchema(nil, nil), dalgo2sql.DbOptions{
		ID:                     cfg.ID,
		Recordsets:             recordsets,
		StructuredQueryDialect: dialectSQLite,
		PreserveNativeValues:   true,
	})

	id := cfg.ID
	if id == "" {
		id = "libsql:" + endpoint.Host + endpoint.Path
	}
	return dal.NewDB(&database{
		ConcurrencyAvailable: dal.ConcurrencyAvailable{},
		id:                   id,
		schema:               dal.NewSchema(nil, nil),
		delegate:             delegate,
		reader:               &schemaReader{reader: reader, limits: limits},
		pool:                 pool,
		client:               client,
		limits:               limits,
		timeout:              timeout,
		declared:             declared,
		recordsets:           recordsets,
	}), nil
}

const dialectSQLite = "sqlite"

func buildRecordsets(declared map[string]string) (map[string]*dalgo2sql.Recordset, map[string]string, error) {
	if len(declared) == 0 {
		return nil, nil, nil
	}
	if len(declared) > 64 {
		return nil, nil, fmt.Errorf("%w: at most 64 recordsets may be declared", ErrInvalidConfig)
	}
	recordsets := make(map[string]*dalgo2sql.Recordset, len(declared))
	copied := make(map[string]string, len(declared))
	for name, key := range declared {
		if !validIdentifier(name) || !validIdentifier(key) {
			return nil, nil, fmt.Errorf("%w: a recordset or key name is not a plain identifier", ErrInvalidConfig)
		}
		copied[name] = key
		recordsets[name] = dalgo2sql.NewRecordset(name, dalgo2sql.Table, []dal.FieldRef{dal.NewFieldRef(name, key)})
	}
	return recordsets, copied, nil
}

func validIdentifier(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// normalizeURL validates and normalizes one database base URL.
func normalizeURL(raw string, allowLoopbackHTTP bool) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, fmt.Errorf("%w: a URL is required", ErrInvalidConfig)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: the URL cannot be parsed", ErrUnsafeTarget)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "libsql":
		parsed.Scheme = "https"
	case "https":
	case "http":
		if !allowLoopbackHTTP || !isLoopbackHost(parsed.Hostname()) {
			return nil, fmt.Errorf("%w: plain HTTP is allowed only for an explicitly permitted loopback host", ErrUnsafeTarget)
		}
	default:
		return nil, fmt.Errorf("%w: only libsql:// and https:// are accepted", ErrUnsafeTarget)
	}
	if parsed.Host == "" || parsed.Hostname() == "" {
		return nil, fmt.Errorf("%w: the URL has no host", ErrUnsafeTarget)
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("%w: the URL must not carry credentials", ErrUnsafeTarget)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return nil, fmt.Errorf("%w: the URL must not carry a query", ErrUnsafeTarget)
	}
	if parsed.Fragment != "" || parsed.RawFragment != "" {
		return nil, fmt.Errorf("%w: the URL must not carry a fragment", ErrUnsafeTarget)
	}
	if parsed.Opaque != "" {
		return nil, fmt.Errorf("%w: the URL must not be opaque", ErrUnsafeTarget)
	}
	path := parsed.Path
	if path != "" {
		if path == "/" {
			path = ""
		}
		if err := validateBasePath(path); err != nil {
			return nil, err
		}
	}
	parsed.Path = path
	parsed.RawPath = ""
	return parsed, nil
}

func validateBasePath(path string) error {
	if !strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") {
		return fmt.Errorf("%w: the URL path must be a clean base path", ErrUnsafeTarget)
	}
	for _, segment := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("%w: the URL path must be a clean base path", ErrUnsafeTarget)
		}
		switch strings.ToLower(segment) {
		case "v2", "v3", "pipeline", "health", "version":
			return fmt.Errorf("%w: the URL must be a base URL, not an API endpoint", ErrUnsafeTarget)
		}
		if !isASCII(segment) || len(segment) > 255 {
			return fmt.Errorf("%w: the URL path contains an unsupported segment", ErrUnsafeTarget)
		}
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}
