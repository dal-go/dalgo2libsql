// Package hrana is a bounded client for the official libSQL Hrana 3
// pipeline protocol. It is private to the dalgo2libsql adapter.
//
// One call sends a stateless pipeline — an execute request and a close
// request, with no baton — and decodes every result before returning any
// value, so a server that answers a later request with an error can never
// look like a successful read. Nothing here exposes a baton, a raw endpoint
// URL, credentials, or a request body to a caller's error chain.
package hrana

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Version identifies the protocol this client speaks.
const Version = "hrana3"

// Error is a protocol, transport or limit failure. Its text is safe to
// return to a caller: it never contains the endpoint, credentials, SQL text,
// bound values, a response body, or an underlying transport error.
type Error struct {
	Op     string
	Status int
	Code   string
	Detail string
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("hrana: ")
	if e.Op != "" {
		b.WriteString(e.Op)
		b.WriteString(": ")
	}
	if e.Status != 0 {
		fmt.Fprintf(&b, "status %d: ", e.Status)
	}
	b.WriteString(e.Detail)
	if e.Code != "" {
		b.WriteString(" (code ")
		b.WriteString(e.Code)
		b.WriteByte(')')
	}
	return b.String()
}

// ErrLimit is wrapped by every bound failure.
var ErrLimit = errors.New("hrana: limit exceeded")

// ErrProtocol is wrapped by every malformed or unexpected response.
var ErrProtocol = errors.New("hrana: protocol violation")

// Limits bounds one operation. A zero field selects the package default;
// values above the corresponding absolute maximum are rejected by
// NormalizeLimits.
type Limits struct {
	// MaxRequestBytes bounds one encoded request body.
	MaxRequestBytes int64
	// MaxResponseBytes bounds one encoded response body.
	MaxResponseBytes int64
	// MaxOperationBytes bounds the response bytes of one operation,
	// including every internal metadata query it makes.
	MaxOperationBytes int64
	// MaxOperationRequests bounds the pipeline calls of one operation.
	MaxOperationRequests int
	// MaxRows bounds the rows of one execute result.
	MaxRows int
	// MaxOperationRows bounds the rows decoded across one operation.
	MaxOperationRows int
	// MaxColumns bounds the columns of one execute result.
	MaxColumns int
	// MaxValueBytes bounds one text or blob cell.
	MaxValueBytes int
	// MaxArgs bounds the bound arguments of one statement.
	MaxArgs int
	// MaxStatementBytes bounds one SQL statement string.
	MaxStatementBytes int
	// Timeout bounds one HTTP request.
	Timeout int64 // nanoseconds; kept as int64 so Limits stays comparable
}

// Default and absolute limits. The defaults are deliberately modest: an
// existing-table read adapter should not buffer an unbounded result.
const (
	DefaultMaxRequestBytes   = 1 << 20  // 1 MiB
	DefaultMaxResponseBytes  = 4 << 20  // 4 MiB
	DefaultMaxOperationBytes = 16 << 20 // 16 MiB
	DefaultMaxOperationReqs  = 32
	DefaultMaxRows           = 10_000
	DefaultMaxOperationRows  = 50_000
	DefaultMaxColumns        = 256
	DefaultMaxValueBytes     = 1 << 20 // 1 MiB
	DefaultMaxArgs           = 256
	DefaultMaxStatementBytes = 1 << 20

	AbsoluteMaxRequestBytes   = 64 << 20
	AbsoluteMaxResponseBytes  = 64 << 20
	AbsoluteMaxOperationBytes = 64 << 20
	AbsoluteMaxOperationReqs  = 256
	AbsoluteMaxRows           = 100_000
	AbsoluteMaxOperationRows  = 500_000
	AbsoluteMaxColumns        = 2_048
	AbsoluteMaxValueBytes     = 16 << 20
	AbsoluteMaxArgs           = 4_096
	AbsoluteMaxStatementBytes = 16 << 20
)

// NormalizeLimits returns l with package defaults filled in. It rejects a
// negative value and a value above the corresponding hard maximum.
func NormalizeLimits(l Limits) (Limits, error) {
	var errs []error
	bound := func(name string, value *int64, def, max int64) {
		if *value == 0 {
			*value = def
		}
		if *value < 0 {
			errs = append(errs, fmt.Errorf("%s must not be negative", name))
		} else if *value > max {
			errs = append(errs, fmt.Errorf("%s exceeds the hard maximum %d", name, max))
		}
	}
	boundInt := func(name string, value *int, def, max int) {
		if *value == 0 {
			*value = def
		}
		if *value < 0 {
			errs = append(errs, fmt.Errorf("%s must not be negative", name))
		} else if *value > max {
			errs = append(errs, fmt.Errorf("%s exceeds the hard maximum %d", name, max))
		}
	}
	bound("MaxRequestBytes", &l.MaxRequestBytes, DefaultMaxRequestBytes, AbsoluteMaxRequestBytes)
	bound("MaxResponseBytes", &l.MaxResponseBytes, DefaultMaxResponseBytes, AbsoluteMaxResponseBytes)
	bound("MaxOperationBytes", &l.MaxOperationBytes, DefaultMaxOperationBytes, AbsoluteMaxOperationBytes)
	boundInt("MaxOperationRequests", &l.MaxOperationRequests, DefaultMaxOperationReqs, AbsoluteMaxOperationReqs)
	boundInt("MaxRows", &l.MaxRows, DefaultMaxRows, AbsoluteMaxRows)
	boundInt("MaxOperationRows", &l.MaxOperationRows, DefaultMaxOperationRows, AbsoluteMaxOperationRows)
	boundInt("MaxColumns", &l.MaxColumns, DefaultMaxColumns, AbsoluteMaxColumns)
	boundInt("MaxValueBytes", &l.MaxValueBytes, DefaultMaxValueBytes, AbsoluteMaxValueBytes)
	boundInt("MaxArgs", &l.MaxArgs, DefaultMaxArgs, AbsoluteMaxArgs)
	boundInt("MaxStatementBytes", &l.MaxStatementBytes, DefaultMaxStatementBytes, AbsoluteMaxStatementBytes)
	if l.Timeout < 0 {
		errs = append(errs, errors.New("timeout must not be negative"))
	}
	if len(errs) > 0 {
		return Limits{}, fmt.Errorf("hrana: invalid limits: %w", errors.Join(errs...))
	}
	return l, nil
}

// Budget is the cumulative accounting of one DALgo operation. The facade
// attaches one to the context of every read, so the metadata probes the SQL
// compiler makes and the data statement that follows share one bound.
type Budget struct {
	limits Limits

	mu       sync.Mutex
	requests int
	rows     int
	bytes    int64
}

// NewBudget returns a budget for one operation. Zero limits select the
// package defaults; NewDB normalizes them once, and this keeps a directly
// constructed budget from meaning "zero bytes allowed".
func NewBudget(l Limits) *Budget {
	normalized, err := NormalizeLimits(l)
	if err != nil {
		normalized, _ = NormalizeLimits(Limits{})
	}
	return &Budget{limits: normalized}
}

type budgetKey struct{}

// WithBudget returns a context carrying b. A nil b is a no-op.
func WithBudget(ctx context.Context, b *Budget) context.Context {
	if b == nil {
		return ctx
	}
	return context.WithValue(ctx, budgetKey{}, b)
}

// BudgetFrom returns the budget carried by ctx, or nil.
func BudgetFrom(ctx context.Context) *Budget {
	b, _ := ctx.Value(budgetKey{}).(*Budget)
	return b
}

// AddRequest records one pipeline call.
func (b *Budget) AddRequest() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.requests++
	if b.requests > b.limits.MaxOperationRequests {
		return fmt.Errorf("%w: more than %d requests in one operation", ErrLimit, b.limits.MaxOperationRequests)
	}
	return nil
}

// AddResponse records response bytes and rows of one decoded result.
func (b *Budget) AddResponse(bytes int64, rows int) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bytes += bytes
	b.rows += rows
	if b.bytes > b.limits.MaxOperationBytes {
		return fmt.Errorf("%w: more than %d response bytes in one operation", ErrLimit, b.limits.MaxOperationBytes)
	}
	if b.rows > b.limits.MaxOperationRows {
		return fmt.Errorf("%w: more than %d rows in one operation", ErrLimit, b.limits.MaxOperationRows)
	}
	return nil
}

// HeaderProvider returns headers for one request. It is called with the
// request context, which carries the operation's deadline and the
// per-request cap, so a provider that honours its context cannot outlive the
// operation.
type HeaderProvider func(ctx context.Context) (map[string]string, error)

// Client is one bounded Hrana 3 pipeline endpoint.
type Client struct {
	endpoint *url.URL // parsed target URL without userinfo/query/fragment
	pipeline string   // absolute pipeline URL
	origin   string   // scheme://host[:port] of the configured target
	http     *http.Client
	ownHTTP  bool
	headers  HeaderProvider
	limits   Limits
}

// Config is the input of New.
type Config struct {
	// Endpoint is the normalized base URL: https://host[/path]. The client
	// appends /v3/pipeline.
	Endpoint *url.URL
	// HTTPClient is the client to use. New copies the value and, when its
	// transport is an *http.Transport, clones that too; the caller's client
	// and transport are never mutated. A nil client uses a new default.
	HTTPClient *http.Client
	// Headers supplies per-request headers, credentials included.
	Headers HeaderProvider
	// Limits bounds the client and its operations.
	Limits Limits
}

// New returns a client for one endpoint. It performs no I/O.
func New(cfg Config) (*Client, error) {
	if cfg.Endpoint == nil {
		return nil, errors.New("hrana: nil endpoint")
	}
	if cfg.Endpoint.User != nil || cfg.Endpoint.RawQuery != "" || cfg.Endpoint.Fragment != "" {
		return nil, errors.New("hrana: endpoint must not carry credentials, a query or a fragment")
	}
	limits, err := NormalizeLimits(cfg.Limits)
	if err != nil {
		return nil, err
	}
	base := *cfg.Endpoint
	base.Path = strings.TrimSuffix(base.Path, "/")
	origin := base.Scheme + "://" + base.Host
	pipeline := *cfg.Endpoint
	pipeline.Path = strings.TrimSuffix(pipeline.Path, "/") + "/v3/pipeline"
	pipeline.RawPath = ""

	var httpClient *http.Client
	own := false
	if cfg.HTTPClient != nil {
		cloned := *cfg.HTTPClient
		httpClient = &cloned
		if t, ok := httpClient.Transport.(*http.Transport); ok {
			httpClient.Transport = t.Clone()
			own = true
		}
	} else {
		httpClient = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
		own = true
	}
	// Never follow a redirect: the target of a redirect is not the configured
	// endpoint, and a second request could carry credentials elsewhere.
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if limits.Timeout > 0 {
		httpClient.Timeout = 0 // per request deadline, not the whole client
	}
	return &Client{
		endpoint: &base,
		pipeline: pipeline.String(),
		origin:   origin,
		http:     httpClient,
		ownHTTP:  own,
		headers:  cfg.Headers,
		limits:   limits,
	}, nil
}

// Origin returns the configured scheme://host[:port] without credentials.
func (c *Client) Origin() string { return c.origin }

// Close releases idle connections of a transport this client owns. It never
// closes a transport the caller supplied, and it is idempotent.
func (c *Client) Close() error {
	if c.ownHTTP {
		if t, ok := c.http.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
	}
	return nil
}

// Column is one result column. DeclType is SQLite's declared type; it is
// empty for an expression that has none.
type Column struct {
	Name     string
	DeclType string
}

// ValueKind identifies the Hrana storage class of one cell.
type ValueKind uint8

// Storage classes.
const (
	Null ValueKind = iota
	Integer
	Float
	Text
	Blob
)

// Value is one decoded cell.
type Value struct {
	Kind  ValueKind
	Int   int64
	Float float64
	Text  string
	Blob  []byte
}

// Result is one fully decoded execute result.
type Result struct {
	Columns  []Column
	Rows     [][]Value
	Affected int64
}

// Execute sends one statement in a stateless pipeline and returns its
// complete result. The close request is validated like the execute request;
// a failure in either is an error, never a partial success.
func (c *Client) Execute(ctx context.Context, statement string, args []any) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(statement) == 0 {
		return nil, &Error{Op: "execute", Detail: "empty statement"}
	}
	if len(statement) > c.limits.MaxStatementBytes {
		return nil, fmt.Errorf("%w: statement exceeds %d bytes", ErrLimit, c.limits.MaxStatementBytes)
	}
	if len(args) > c.limits.MaxArgs {
		return nil, fmt.Errorf("%w: more than %d bound arguments", ErrLimit, c.limits.MaxArgs)
	}
	encodedArgs := make([]wireValue, len(args))
	for i, a := range args {
		v, err := encodeArg(a, c.limits.MaxValueBytes)
		if err != nil {
			return nil, err
		}
		encodedArgs[i] = v
	}

	body, err := json.Marshal(pipelineRequest{
		Baton: nil,
		Requests: []request{
			{Type: "execute", Stmt: &stmt{
				SQL:      statement,
				Args:     encodedArgs,
				WantRows: true,
			}},
			{Type: "close"},
		},
	})
	if err != nil {
		return nil, &Error{Op: "encode", Detail: "cannot encode the request"}
	}
	if int64(len(body)) > c.limits.MaxRequestBytes {
		return nil, fmt.Errorf("%w: request exceeds %d bytes", ErrLimit, c.limits.MaxRequestBytes)
	}

	budget := BudgetFrom(ctx)
	if err := budget.AddRequest(); err != nil {
		return nil, err
	}

	// The request context carries the caller's operation deadline and, when
	// configured, the per-request cap. The header provider is called with it,
	// so a provider that honours its context cannot outlive the request, while
	// the operation's own deadline and budget survive the nested timeout.
	reqCtx := ctx
	var cancel context.CancelFunc
	if c.limits.Timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, time.Duration(c.limits.Timeout))
		defer cancel()
	}
	if err := reqCtx.Err(); err != nil {
		return nil, err
	}

	headers := http.Header{"Content-Type": {"application/json"}, "Accept": {"application/json"}}
	if c.headers != nil {
		extra, err := c.headers(reqCtx)
		if err != nil {
			return nil, headerError(reqCtx, err)
		}
		if err := mergeHeaders(headers, extra); err != nil {
			return nil, err
		}
	}
	if err := reqCtx.Err(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.pipeline, bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Op: "request", Detail: "cannot construct the request"}
	}
	req.Header = headers

	resp, err := c.http.Do(req)
	if err != nil {
		if ctxErr := reqCtx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, &Error{Op: "transport", Detail: "the request failed"}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, &Error{Op: "transport", Status: resp.StatusCode, Detail: "the endpoint answered with a non-200 status"}
	}
	if mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
		return nil, &Error{Op: "transport", Detail: "the endpoint did not answer with application/json"}
	}

	raw, err := readBounded(resp.Body, c.limits.MaxResponseBytes)
	if err != nil {
		if ctxErr := reqCtx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	if err := reqCtx.Err(); err != nil {
		return nil, err
	}

	var decoded pipelineResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, &Error{Op: "decode", Detail: "the response is not valid JSON"}
	}
	if decoded.Baton != nil {
		return nil, &Error{Op: "decode", Detail: "the endpoint returned a baton for a stateless request"}
	}
	if decoded.BaseURL != nil {
		if !sameOriginAndPath(*decoded.BaseURL, c.endpoint) {
			return nil, &Error{Op: "redirect", Detail: "the endpoint returned a different base URL"}
		}
	}
	if len(decoded.Results) != 2 {
		return nil, &Error{Op: "decode", Detail: "the response does not carry exactly the two requested results"}
	}
	if err := decoded.Results[0].validate("execute"); err != nil {
		return nil, err
	}
	if err := decoded.Results[1].validate("close"); err != nil {
		return nil, err
	}

	out, err := decodeExecuteResult(decoded.Results[0].Response.Result, c.limits)
	if err != nil {
		return nil, err
	}
	if err := budget.AddResponse(int64(len(raw)), len(out.Rows)); err != nil {
		return nil, err
	}
	return out, nil
}

// headerError turns a header-provider failure into a safe error. A provider
// that returns the context's error keeps that identity, so a caller can tell a
// cancellation from a token problem; any other cause is replaced, never
// echoed, because it can carry a token or a URL.
func headerError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return &Error{Op: "headers", Detail: "the header provider failed"}
}

func sameOriginAndPath(raw string, configured *url.URL) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Scheme, configured.Scheme) &&
		strings.EqualFold(u.Host, configured.Host) &&
		strings.TrimSuffix(u.Path, "/") == strings.TrimSuffix(configured.Path, "/")
}

func mergeHeaders(dst http.Header, extra map[string]string) error {
	for name, value := range extra {
		if err := validateHeader(name, value); err != nil {
			return err
		}
		dst.Set(name, value)
	}
	return nil
}

func validateHeader(name, value string) error {
	if name == "" || !utf8.ValidString(name) || !utf8.ValidString(value) {
		return &Error{Op: "headers", Detail: "a header name or value is invalid"}
	}
	if strings.ContainsAny(name, " \t\r\n:") || strings.ContainsAny(value, "\r\n") {
		return &Error{Op: "headers", Detail: "a header name or value contains a forbidden character"}
	}
	switch strings.ToLower(name) {
	case "host", "content-length", "transfer-encoding", "connection", "upgrade",
		"keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
		"content-type", "accept", "accept-encoding":
		return &Error{Op: "headers", Detail: "a header name would override the protocol"}
	}
	return nil
}

func readBounded(r io.Reader, max int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, &Error{Op: "transport", Detail: "the response body could not be read"}
	}
	if int64(len(raw)) > max {
		return nil, fmt.Errorf("%w: response exceeds %d bytes", ErrLimit, max)
	}
	return raw, nil
}

// --- Values ---------------------------------------------------------------
//
// HRANA_3_SPEC.md defines the JSON Value structure as:
//
//	{ "type": "null" }
//	{ "type": "integer", "value": string }
//	{ "type": "float",   "value": number }
//	{ "type": "text",    "value": string }
//	{ "type": "blob",    "base64": string }
//
// An integer is a decimal string to keep full int64 precision, a float is a
// JSON number, and a blob lives in "base64", never in "value". Encoding or
// decoding any other shape is a protocol violation, not a lossy conversion.

// wireValue is one encoded argument. A blob is carried in Base64 and never in
// Value; a float is a JSON number; an integer and text are JSON strings.
type wireValue struct {
	Type   string  `json:"type"`
	Value  any     `json:"value,omitempty"`
	Base64 *string `json:"base64,omitempty"`
}

func encodeArg(v any, maxValue int) (wireValue, error) {
	switch x := v.(type) {
	case nil:
		return wireValue{Type: "null"}, nil
	case int:
		return wireValue{Type: "integer", Value: strconv.FormatInt(int64(x), 10)}, nil
	case int64:
		return wireValue{Type: "integer", Value: strconv.FormatInt(x, 10)}, nil
	case bool:
		if x {
			return wireValue{Type: "integer", Value: "1"}, nil
		}
		return wireValue{Type: "integer", Value: "0"}, nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return wireValue{}, &Error{Op: "encode", Detail: "a bound float is not finite"}
		}
		return wireValue{Type: "float", Value: x}, nil
	case string:
		if len(x) > maxValue {
			return wireValue{}, fmt.Errorf("%w: a bound text value exceeds %d bytes", ErrLimit, maxValue)
		}
		return wireValue{Type: "text", Value: x}, nil
	case []byte:
		if len(x) > maxValue {
			return wireValue{}, fmt.Errorf("%w: a bound blob value exceeds %d bytes", ErrLimit, maxValue)
		}
		encoded := base64.StdEncoding.EncodeToString(x)
		return wireValue{Type: "blob", Base64: &encoded}, nil
	default:
		return wireValue{}, &Error{Op: "encode", Detail: "a bound argument type is not supported"}
	}
}

// cell is one raw JSON cell. Value and Base64 are kept raw so an absent field
// is distinguishable from null, a string from a number, and a cross-tagged
// field is refused instead of silently ignored.
type cell struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value"`
	Base64 json.RawMessage `json:"base64"`
}

func (c cell) decode(maxValue int) (Value, error) {
	hasValue := len(bytes.TrimSpace(c.Value)) > 0
	hasBase64 := len(bytes.TrimSpace(c.Base64)) > 0
	switch c.Type {
	case "null":
		if hasValue || hasBase64 {
			return Value{}, &Error{Op: "decode", Detail: "a null cell carries a value"}
		}
		return Value{Kind: Null}, nil
	case "integer":
		if !hasValue || hasBase64 {
			return Value{}, &Error{Op: "decode", Detail: "an integer cell must carry only a string value"}
		}
		s, err := jsonString(c.Value, "an integer cell")
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return Value{}, &Error{Op: "decode", Detail: "an integer cell is malformed or out of range"}
		}
		return Value{Kind: Integer, Int: n}, nil
	case "float":
		if !hasValue || hasBase64 {
			return Value{}, &Error{Op: "decode", Detail: "a float cell must carry only a numeric value"}
		}
		f, err := jsonFloat(c.Value, "a float cell")
		if err != nil {
			return Value{}, err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return Value{}, &Error{Op: "decode", Detail: "a float cell is not finite"}
		}
		return Value{Kind: Float, Float: f}, nil
	case "text":
		if !hasValue || hasBase64 {
			return Value{}, &Error{Op: "decode", Detail: "a text cell must carry only a string value"}
		}
		s, err := jsonString(c.Value, "a text cell")
		if err != nil {
			return Value{}, err
		}
		if len(s) > maxValue {
			return Value{}, fmt.Errorf("%w: a text cell exceeds %d bytes", ErrLimit, maxValue)
		}
		if !utf8.ValidString(s) {
			return Value{}, &Error{Op: "decode", Detail: "a text cell is not valid UTF-8"}
		}
		return Value{Kind: Text, Text: s}, nil
	case "blob":
		if hasValue || !hasBase64 {
			return Value{}, &Error{Op: "decode", Detail: "a blob cell must carry only a base64 field"}
		}
		s, err := jsonString(c.Base64, "a blob cell")
		if err != nil {
			return Value{}, err
		}
		if base64.StdEncoding.DecodedLen(len(s)) > maxValue {
			return Value{}, fmt.Errorf("%w: a blob cell exceeds %d bytes", ErrLimit, maxValue)
		}
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return Value{}, &Error{Op: "decode", Detail: "a blob cell is not valid base64"}
		}
		if len(raw) > maxValue {
			return Value{}, fmt.Errorf("%w: a blob cell exceeds %d bytes", ErrLimit, maxValue)
		}
		return Value{Kind: Blob, Blob: raw}, nil
	default:
		return Value{}, &Error{Op: "decode", Detail: "a cell has an unknown storage class"}
	}
}

func jsonString(raw json.RawMessage, what string) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", &Error{Op: "decode", Detail: what + " must be a JSON string"}
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err != nil {
		return "", &Error{Op: "decode", Detail: what + " must be a JSON string"}
	}
	return s, nil
}

func jsonFloat(raw json.RawMessage, what string) (float64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] == '"' || trimmed[0] == '{' || trimmed[0] == '[' ||
		trimmed[0] == 't' || trimmed[0] == 'f' || bytes.Equal(trimmed, []byte("null")) {
		return 0, &Error{Op: "decode", Detail: what + " must be a JSON number"}
	}
	var f float64
	if err := json.Unmarshal(trimmed, &f); err != nil {
		return 0, &Error{Op: "decode", Detail: what + " must be a finite JSON number"}
	}
	return f, nil
}

func jsonUint(raw json.RawMessage, name string) (uint64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return 0, &Error{Op: "decode", Detail: name + " is missing"}
	}
	if trimmed[0] == '"' || trimmed[0] == '-' || trimmed[0] == '{' || trimmed[0] == '[' ||
		trimmed[0] == 't' || trimmed[0] == 'f' || bytes.Equal(trimmed, []byte("null")) {
		return 0, &Error{Op: "decode", Detail: name + " must be a non-negative JSON integer"}
	}
	var n uint64
	if err := json.Unmarshal(trimmed, &n); err != nil {
		return 0, &Error{Op: "decode", Detail: name + " must be a non-negative JSON integer"}
	}
	return n, nil
}

// --- Results --------------------------------------------------------------

// decodeExecuteResult validates every mandatory field of a StmtResult before
// any value is returned. An absent or null field, a wrong JSON type, or a row
// whose width does not match the columns is an error, never a successful zero
// result. Unknown fields are ignored, as the protocol requires.
func decodeExecuteResult(wire *executeResult, limits Limits) (*Result, error) {
	if wire == nil {
		return nil, &Error{Op: "decode", Detail: "the execute result is missing"}
	}
	columns, err := decodeColumns(wire.Cols, limits.MaxColumns)
	if err != nil {
		return nil, err
	}
	if len(columns) > limits.MaxColumns {
		return nil, fmt.Errorf("%w: more than %d columns", ErrLimit, limits.MaxColumns)
	}
	rows, err := decodeRows(wire.Rows, len(columns), limits)
	if err != nil {
		return nil, err
	}
	affected, err := jsonUint(wire.AffectedRowCount, "affected_row_count")
	if err != nil {
		return nil, err
	}
	if affected > math.MaxInt64 {
		return nil, &Error{Op: "decode", Detail: "affected_row_count is out of range"}
	}
	if err := decodeLastInsertRowID(wire.LastInsertRowID); err != nil {
		return nil, err
	}
	if _, err := jsonUint(wire.RowsRead, "rows_read"); err != nil {
		return nil, err
	}
	if _, err := jsonUint(wire.RowsWritten, "rows_written"); err != nil {
		return nil, err
	}
	if _, err := jsonFloat(wire.QueryDurationMS, "query_duration_ms"); err != nil {
		return nil, err
	}
	return &Result{Columns: columns, Rows: rows, Affected: int64(affected)}, nil
}

func decodeColumns(raw json.RawMessage, max int) ([]Column, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, &Error{Op: "decode", Detail: "cols is missing"}
	}
	if trimmed[0] != '[' {
		return nil, &Error{Op: "decode", Detail: "cols must be an array"}
	}
	var entries []wireColumn
	if err := json.Unmarshal(trimmed, &entries); err != nil {
		return nil, &Error{Op: "decode", Detail: "cols is malformed"}
	}
	if len(entries) > max {
		return nil, fmt.Errorf("%w: more than %d columns", ErrLimit, max)
	}
	out := make([]Column, len(entries))
	for i, entry := range entries {
		name, err := jsonNullableString(entry.Name, "a column name")
		if err != nil {
			return nil, err
		}
		if len(name) > 1024 || !utf8.ValidString(name) {
			return nil, &Error{Op: "decode", Detail: "a column name is invalid"}
		}
		declType, err := jsonNullableString(entry.DeclType, "a declared type")
		if err != nil {
			return nil, err
		}
		if len(declType) > 255 || !utf8.ValidString(declType) {
			return nil, &Error{Op: "decode", Detail: "a declared type is invalid"}
		}
		out[i] = Column{Name: name, DeclType: declType}
	}
	return out, nil
}

func jsonNullableString(raw json.RawMessage, what string) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return "", &Error{Op: "decode", Detail: what + " is missing"}
	}
	if bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}
	return jsonString(trimmed, what)
}

func decodeRows(raw json.RawMessage, columns int, limits Limits) ([][]Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, &Error{Op: "decode", Detail: "rows is missing"}
	}
	if trimmed[0] != '[' {
		return nil, &Error{Op: "decode", Detail: "rows must be an array"}
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(trimmed, &entries); err != nil {
		return nil, &Error{Op: "decode", Detail: "rows is malformed"}
	}
	if len(entries) > limits.MaxRows {
		return nil, fmt.Errorf("%w: more than %d rows", ErrLimit, limits.MaxRows)
	}
	out := make([][]Value, len(entries))
	for r, entry := range entries {
		body := bytes.TrimSpace(entry)
		if len(body) == 0 || body[0] != '[' {
			return nil, &Error{Op: "decode", Detail: "a row must be an array"}
		}
		var cells []cell
		if err := json.Unmarshal(body, &cells); err != nil {
			return nil, &Error{Op: "decode", Detail: "a row is malformed"}
		}
		if len(cells) != columns {
			return nil, &Error{Op: "decode", Detail: "a row does not match the column count"}
		}
		values := make([]Value, len(cells))
		for i, c := range cells {
			v, err := c.decode(limits.MaxValueBytes)
			if err != nil {
				return nil, err
			}
			values[i] = v
		}
		out[r] = values
	}
	return out, nil
}

// decodeLastInsertRowID validates the mandatory last_insert_rowid field: it
// must be present, and either null or a decimal string holding a signed int64.
func decodeLastInsertRowID(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return &Error{Op: "decode", Detail: "last_insert_rowid is missing"}
	}
	if bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	s, err := jsonString(trimmed, "last_insert_rowid")
	if err != nil {
		return err
	}
	if _, err := strconv.ParseInt(s, 10, 64); err != nil {
		return &Error{Op: "decode", Detail: "last_insert_rowid is not a signed 64-bit string"}
	}
	return nil
}

// --- Wire structures ------------------------------------------------------

type pipelineRequest struct {
	Baton    *string   `json:"baton"`
	Requests []request `json:"requests"`
}

type request struct {
	Type string `json:"type"`
	Stmt *stmt  `json:"stmt,omitempty"`
}

type stmt struct {
	SQL      string      `json:"sql"`
	Args     []wireValue `json:"args"`
	WantRows bool        `json:"want_rows"`
}

type pipelineResponse struct {
	Baton   *string         `json:"baton"`
	BaseURL *string         `json:"base_url"`
	Results []resultOrError `json:"results"`
}

type resultOrError struct {
	Type     string     `json:"type"`
	Error    *wireError `json:"error,omitempty"`
	Response *response  `json:"response,omitempty"`
}

func (r resultOrError) validate(want string) error {
	switch r.Type {
	case "error":
		if r.Response != nil {
			return &Error{Op: "decode", Detail: "an error result also carries a response"}
		}
		if r.Error == nil {
			return &Error{Op: "decode", Detail: "an error result carries no error"}
		}
		if r.Error.Message == nil {
			return &Error{Op: "decode", Detail: "a server error carries no message"}
		}
		code := ""
		if r.Error.Code != nil {
			code = safeServerCode(*r.Error.Code)
		}
		return &Error{Op: "server", Code: code, Detail: "the endpoint reported an error for the " + want + " request"}
	case "ok":
		if r.Error != nil {
			return &Error{Op: "decode", Detail: "an ok result also carries an error"}
		}
		if r.Response == nil {
			return &Error{Op: "decode", Detail: "an ok result carries no response"}
		}
	default:
		return &Error{Op: "decode", Detail: "a result is neither ok nor an error"}
	}
	if want == "execute" {
		if r.Response.Type != "execute" || r.Response.Result == nil {
			return &Error{Op: "decode", Detail: "the execute result is missing"}
		}
		return nil
	}
	if r.Response.Type != "close" {
		return &Error{Op: "decode", Detail: "the close result is missing"}
	}
	if r.Response.Result != nil {
		return &Error{Op: "decode", Detail: "the close result carries an unexpected execute payload"}
	}
	return nil
}

// safeServerCode returns code only when it is one of the stable libSQL/SQLite
// codes this adapter knows. An arbitrary code is server text: it can carry a
// token, a query or a path, so it is omitted rather than echoed.
func safeServerCode(code string) string {
	if _, ok := allowedServerCodes[code]; ok {
		return code
	}
	return ""
}

var allowedServerCodes = map[string]struct{}{
	"SQLITE_OK": {}, "SQLITE_ERROR": {}, "SQLITE_INTERNAL": {}, "SQLITE_PERM": {},
	"SQLITE_ABORT": {}, "SQLITE_BUSY": {}, "SQLITE_LOCKED": {}, "SQLITE_NOMEM": {},
	"SQLITE_READONLY": {}, "SQLITE_INTERRUPT": {}, "SQLITE_IOERR": {}, "SQLITE_CORRUPT": {},
	"SQLITE_NOTFOUND": {}, "SQLITE_FULL": {}, "SQLITE_CANTOPEN": {}, "SQLITE_PROTOCOL": {},
	"SQLITE_EMPTY": {}, "SQLITE_SCHEMA": {}, "SQLITE_TOOBIG": {}, "SQLITE_CONSTRAINT": {},
	"SQLITE_MISMATCH": {}, "SQLITE_MISUSE": {}, "SQLITE_NOLFS": {}, "SQLITE_AUTH": {},
	"SQLITE_FORMAT": {}, "SQLITE_RANGE": {}, "SQLITE_NOTADB": {}, "SQLITE_NOTICE": {},
	"SQLITE_WARNING": {}, "SQLITE_ROW": {}, "SQLITE_DONE": {},
	"SQLITE_CONSTRAINT_PRIMARYKEY": {}, "SQLITE_CONSTRAINT_UNIQUE": {},
	"SQLITE_CONSTRAINT_NOTNULL": {}, "SQLITE_CONSTRAINT_FOREIGNKEY": {},
	"SQLITE_CONSTRAINT_CHECK": {},
	"HRANA_PROTO_ERROR":       {}, "HRANA_CLOSED": {}, "HRANA_SYNC_TIMEOUT": {},
	"HRANA_SYNC_INTERRUPTED": {}, "HRANA_SYNC_ABORTED": {},
}

type wireError struct {
	Message *string `json:"message"`
	Code    *string `json:"code"`
}

type response struct {
	Type   string         `json:"type"`
	Result *executeResult `json:"result,omitempty"`
}

type executeResult struct {
	Cols             json.RawMessage `json:"cols"`
	Rows             json.RawMessage `json:"rows"`
	AffectedRowCount json.RawMessage `json:"affected_row_count"`
	LastInsertRowID  json.RawMessage `json:"last_insert_rowid"`
	RowsRead         json.RawMessage `json:"rows_read"`
	RowsWritten      json.RawMessage `json:"rows_written"`
	QueryDurationMS  json.RawMessage `json:"query_duration_ms"`
}

type wireColumn struct {
	Name     json.RawMessage `json:"name"`
	DeclType json.RawMessage `json:"decltype"`
}
