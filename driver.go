package dalgo2libsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	"github.com/dal-go/dalgo2libsql/internal/hrana"
)

// This file implements a private database/sql driver.Connector over the
// Hrana 3 pipeline. It is deliberately narrow: connections can query and
// ping, and every other driver surface fails with an explicit error. There is
// no driver registration, no DSN parsing, no global state and no fallback
// that could turn an unsupported write path into a successful request.
//
// database/sql is used for its pool and its Rows contract, which are what the
// dalgo2sql SQLite compiler expects. A result is decoded completely and within
// bounds before QueryContext returns, so a malformed or truncated response is
// an error, never a short result.

var (
	errDriverPrepare = fmt.Errorf("%w: prepared statements are not implemented; use a context query", ErrNotSupported)
	errDriverExec    = fmt.Errorf("%w: the libSQL read-only driver does not execute statements", ErrNotSupported)
	errDriverBegin   = fmt.Errorf("%w: transactions are not implemented in this read-only checkpoint", ErrNotSupported)
	errDriverOpen    = fmt.Errorf("%w: the driver is opened only through NewDB", ErrNotSupported)
	errNamedValue    = errors.New("dalgo2libsql: unsupported bound argument")
)

const driverName = "dalgo2libsql"

// connector implements driver.Connector. A *sql.DB created with sql.OpenDB
// calls Connect; Open is never used because the driver is not registered.
type connector struct {
	client *hrana.Client
	limits hrana.Limits
}

var _ driver.Connector = (*connector)(nil)

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &conn{client: c.client, limits: c.limits}, nil
}

func (c *connector) Driver() driver.Driver { return libsqlDriver{} }

// libsqlDriver exists only because driver.Connector requires it. Open refuses
// a DSN so no unvalidated configuration can produce a connection.
type libsqlDriver struct{}

var _ driver.Driver = libsqlDriver{}

func (libsqlDriver) Open(string) (driver.Conn, error) { return nil, errDriverOpen }

type conn struct {
	client *hrana.Client
	limits hrana.Limits
}

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
)

func (c *conn) Prepare(string) (driver.Stmt, error) { return nil, errDriverPrepare }

func (c *conn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return nil, errDriverPrepare
}

func (c *conn) Close() error { return nil }

func (c *conn) Begin() (driver.Tx, error) { return nil, errDriverBegin }

func (c *conn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, errDriverBegin
}

func (c *conn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errDriverExec
}

func (c *conn) Ping(ctx context.Context) error {
	_, err := c.QueryContext(ctx, "SELECT 1", nil)
	return err
}

func (c *conn) QueryContext(ctx context.Context, query string, named []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	args := make([]any, len(named))
	for i, nv := range named {
		if nv.Name != "" || nv.Ordinal != i+1 {
			return nil, fmt.Errorf("%w: only ordered positional arguments are accepted", errNamedValue)
		}
		value, ok := driverValue(nv.Value)
		if !ok {
			return nil, fmt.Errorf("%w: argument %d has an unsupported type", errNamedValue, i+1)
		}
		args[i] = value
	}
	result, err := c.client.Execute(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &rows{columns: result.Columns, data: result.Rows}, nil
}

// driverValue narrows a database/sql argument to the storage classes Hrana
// carries. A time.Time has no lossless Hrana representation here, so it is
// refused rather than silently formatted as text.
func driverValue(v driver.Value) (any, bool) {
	switch x := v.(type) {
	case nil, int64, float64, bool, string, []byte:
		if f, ok := x.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			return nil, false
		}
		return x, true
	default:
		return nil, false
	}
}

// rows implements driver.Rows over one fully decoded result.
type rows struct {
	columns []hrana.Column
	data    [][]hrana.Value
	next    int
	closed  bool
}

var (
	_ driver.Rows                           = (*rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*rows)(nil)
)

func (r *rows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, column := range r.columns {
		names[i] = column.Name
	}
	return names
}

func (r *rows) Close() error {
	r.closed = true
	r.data = nil
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed {
		return errors.New("dalgo2libsql: rows are closed")
	}
	if r.next >= len(r.data) {
		return io.EOF
	}
	if len(dest) != len(r.columns) {
		return errors.New("dalgo2libsql: destination does not match the column count")
	}
	row := r.data[r.next]
	r.next++
	for i, cell := range row {
		switch cell.Kind {
		case hrana.Null:
			dest[i] = nil
		case hrana.Integer:
			dest[i] = cell.Int
		case hrana.Float:
			dest[i] = cell.Float
		case hrana.Text:
			dest[i] = cell.Text
		case hrana.Blob:
			blob := make([]byte, len(cell.Blob))
			copy(blob, cell.Blob)
			dest[i] = blob
		default:
			return errors.New("dalgo2libsql: a cell has an unknown storage class")
		}
	}
	return nil
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return r.columns[index].DeclType
}

// ColumnTypeScanType reports the Go type Scan should target. A declared type
// decides it; an expression has none and is reported as any.
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return nil
	}
	switch strings.ToUpper(strings.TrimSpace(r.columns[index].DeclType)) {
	case "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "BOOLEAN":
		return reflect.TypeOf(int64(0))
	case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "NUMERIC", "DECIMAL":
		return reflect.TypeOf(float64(0))
	case "BLOB":
		return reflect.TypeOf([]byte(nil))
	case "TEXT", "VARCHAR", "CHAR", "CLOB", "DATE", "DATETIME", "TIMESTAMP":
		return reflect.TypeOf("")
	default:
		return reflect.TypeOf((*any)(nil)).Elem()
	}
}

var _ = driverName
