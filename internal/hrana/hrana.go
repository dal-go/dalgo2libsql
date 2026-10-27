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
		errs = append(errs, errors.New("Timeout must not be negative"))
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
// operation's context, before a per-request deadline is applied, so a token
// refresh is not cut short by the transport timeout.
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
	encodedArgs := make([]arg, len(args))
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

	headers := http.Header{"Content-Type": {"application/json"}, "Accept": {"application/json"}}
	if c.headers != nil {
		extra, err := c.headers(ctx)
		if err != nil {
			return nil, &Error{Op: "headers", Detail: "the header provider failed"}
		}
		if err := mergeHeaders(headers, extra); err != nil {
			return nil, err
		}
	}

	reqCtx := ctx
	var cancel context.CancelFunc
	if c.limits.Timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, time.Duration(c.limits.Timeout))
		defer cancel()
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.pipeline, bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Op: "request", Detail: "cannot construct the request"}
	}
	req.Header = headers

	resp, err := c.http.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
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
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
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

	result := decoded.Results[0].Response.Result
	out := &Result{Columns: make([]Column, len(result.Cols)), Rows: make([][]Value, len(result.Rows))}
	if len(result.Cols) > c.limits.MaxColumns {
		return nil, fmt.Errorf("%w: more than %d columns", ErrLimit, c.limits.MaxColumns)
	}
	if len(result.Rows) > c.limits.MaxRows {
		return nil, fmt.Errorf("%w: more than %d rows", ErrLimit, c.limits.MaxRows)
	}
	for i, col := range result.Cols {
		if col.Name == "" || !utf8.ValidString(col.Name) || len(col.Name) > 1024 {
			return nil, &Error{Op: "decode", Detail: "a column name is empty or invalid"}
		}
		if len(col.DeclType) > 255 || !utf8.ValidString(col.DeclType) {
			return nil, &Error{Op: "decode", Detail: "a declared type is invalid"}
		}
		out.Columns[i] = Column{Name: col.Name, DeclType: col.DeclType}
	}
	for r, row := range result.Rows {
		if len(row) != len(result.Cols) {
			return nil, &Error{Op: "decode", Detail: "a row does not match the column count"}
		}
		cells := make([]Value, len(row))
		for i, cell := range row {
			v, err := cell.decode(c.limits.MaxValueBytes)
			if err != nil {
				return nil, err
			}
			cells[i] = v
		}
		out.Rows[r] = cells
	}
	if result.AffectedRowCount < 0 {
		return nil, &Error{Op: "decode", Detail: "a negative affected row count"}
	}
	out.Affected = result.AffectedRowCount
	if err := budget.AddResponse(int64(len(raw)), len(out.Rows)); err != nil {
		return nil, err
	}
	return out, nil
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

type arg struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func encodeArg(v any, maxValue int) (arg, error) {
	switch x := v.(type) {
	case nil:
		return arg{Type: "null"}, nil
	case int:
		return arg{Type: "integer", Value: fmt.Sprintf("%d", int64(x))}, nil
	case int64:
		return arg{Type: "integer", Value: fmt.Sprintf("%d", x)}, nil
	case bool:
		if x {
			return arg{Type: "integer", Value: "1"}, nil
		}
		return arg{Type: "integer", Value: "0"}, nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return arg{}, &Error{Op: "encode", Detail: "a bound float is not finite"}
		}
		return arg{Type: "float", Value: fmt.Sprintf("%v", x)}, nil
	case string:
		if len(x) > maxValue {
			return arg{}, fmt.Errorf("%w: a bound text value exceeds %d bytes", ErrLimit, maxValue)
		}
		return arg{Type: "text", Value: x}, nil
	case []byte:
		if len(x) > maxValue {
			return arg{}, fmt.Errorf("%w: a bound blob value exceeds %d bytes", ErrLimit, maxValue)
		}
		return arg{Type: "blob", Value: base64.StdEncoding.EncodeToString(x)}, nil
	default:
		return arg{}, &Error{Op: "encode", Detail: "a bound argument type is not supported"}
	}
}

func (c cell) decode(maxValue int) (Value, error) {
	switch c.Type {
	case "null":
		if c.Value != nil && *c.Value != "" {
			return Value{}, &Error{Op: "decode", Detail: "a null cell carries a value"}
		}
		return Value{Kind: Null}, nil
	case "integer":
		if c.Value == nil {
			return Value{}, &Error{Op: "decode", Detail: "an integer cell has no value"}
		}
		var n int64
		if err := json.Unmarshal([]byte(*c.Value), &n); err != nil {
			return Value{}, &Error{Op: "decode", Detail: "an integer cell is malformed or out of range"}
		}
		return Value{Kind: Integer, Int: n}, nil
	case "float":
		if c.Value == nil {
			return Value{}, &Error{Op: "decode", Detail: "a float cell has no value"}
		}
		var f float64
		if err := json.Unmarshal([]byte(*c.Value), &f); err != nil {
			return Value{}, &Error{Op: "decode", Detail: "a float cell is malformed"}
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return Value{}, &Error{Op: "decode", Detail: "a float cell is not finite"}
		}
		return Value{Kind: Float, Float: f}, nil
	case "text":
		if c.Value == nil {
			return Value{}, &Error{Op: "decode", Detail: "a text cell has no value"}
		}
		if len(*c.Value) > maxValue {
			return Value{}, fmt.Errorf("%w: a text cell exceeds %d bytes", ErrLimit, maxValue)
		}
		if !utf8.ValidString(*c.Value) {
			return Value{}, &Error{Op: "decode", Detail: "a text cell is not valid UTF-8"}
		}
		return Value{Kind: Text, Text: *c.Value}, nil
	case "blob":
		if c.Value == nil {
			return Value{}, &Error{Op: "decode", Detail: "a blob cell has no value"}
		}
		if base64.StdEncoding.DecodedLen(len(*c.Value)) > maxValue {
			return Value{}, fmt.Errorf("%w: a blob cell exceeds %d bytes", ErrLimit, maxValue)
		}
		raw, err := base64.StdEncoding.DecodeString(*c.Value)
		if err != nil {
			return Value{}, &Error{Op: "decode", Detail: "a blob cell is not valid base64"}
		}
		return Value{Kind: Blob, Blob: raw}, nil
	default:
		return Value{}, &Error{Op: "decode", Detail: "a cell has an unknown storage class"}
	}
}

// The wire types below follow HRANA_3_SPEC.md.

type pipelineRequest struct {
	Baton    *string   `json:"baton"`
	Requests []request `json:"requests"`
}

type request struct {
	Type string `json:"type"`
	Stmt *stmt  `json:"stmt,omitempty"`
}

type stmt struct {
	SQL      string `json:"sql"`
	Args     []arg  `json:"args"`
	WantRows bool   `json:"want_rows"`
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
	if r.Type == "error" {
		code := ""
		if r.Error != nil {
			code = r.Error.Code
		}
		return &Error{Op: "server", Code: code, Detail: "the endpoint reported an error for the " + want + " request"}
	}
	if r.Type != "ok" || r.Response == nil {
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

type wireError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

type response struct {
	Type   string         `json:"type"`
	Result *executeResult `json:"result,omitempty"`
}

type executeResult struct {
	Cols             []wireColumn `json:"cols"`
	Rows             [][]cell     `json:"rows"`
	AffectedRowCount int64        `json:"affected_row_count"`
	LastInsertRowID  *string      `json:"last_insert_rowid"`
}

type wireColumn struct {
	Name     string `json:"name"`
	DeclType string `json:"decltype"`
}

type cell struct {
	Type  string  `json:"type"`
	Value *string `json:"value"`
}
