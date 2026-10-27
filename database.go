package dalgo2libsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/dalgo2sql"
	dalrecord "github.com/dal-go/record"

	"github.com/dal-go/dalgo2libsql/internal/hrana"
)

// database is the read-only DALgo backend. It embeds no SQL handle and
// exposes no write, raw-SQL, or handle accessor: the dalgo2sql database and
// the *sql.DB are private fields, so a caller cannot unwrap the adapter into
// a writable surface.
//
// Reads that this checkpoint supports are Get, Exists, GetMulti, structured
// records readers, structured recordset readers, and the five borrowed
// schema-reader methods. Writes, transactions, joins and text SQL are refused
// before any I/O.
type database struct {
	dal.ConcurrencyAvailable

	id       string
	schema   dal.Schema
	delegate dal.DB
	reader   dbschema.SchemaReader
	pool     *sql.DB
	client   *hrana.Client
	limits   hrana.Limits
	timeout  time.Duration

	recordsets map[string]*dalgo2sql.Recordset
	declared   map[string]string

	validateMu sync.Mutex
	validated  map[string]bool

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

var (
	_ dal.Backend                   = (*database)(nil)
	_ dal.QueryCapabilitiesProvider = (*database)(nil)
	_ dbschema.SchemaReader         = (*database)(nil)
	_ io.Closer                     = (*database)(nil)
)

// ID is the identifier supplied (or derived) at construction.
func (d *database) ID() string { return d.id }

// Adapter names this adapter and its version.
func (d *database) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2libsql", Version) }

// Schema returns the key-mapping schema. The adapter does not invent one.
func (d *database) Schema() dal.Schema { return d.schema }

// QueryCapabilities explicitly advertises only what this backend runs
// natively. SQLite's GROUP BY, HAVING, ORDER BY and the aggregate subset are
// compiled to SQL and verified by the server; joins are not advertised and
// are refused, because their preflight needs a stateful transaction this
// checkpoint does not implement. No stable row order and no aggregate-local
// ORDER BY is claimed.
func (d *database) QueryCapabilities() dal.QueryCapabilities {
	return dal.QueryCapabilities{
		GroupBy: true,
		Having:  true,
		OrderBy: true,
		Aggregate: dal.AggregateCapabilities{
			Count: true, CountDistinct: true,
			Sum: true, SumDistinct: true,
			Avg: true, AvgDistinct: true,
			Min: true, Max: true,
		},
	}
}

// Get reads one record through its declared primary key. A collection whose
// key is not declared, or whose declaration does not match the live schema,
// is refused before any statement is sent.
func (d *database) Get(ctx context.Context, rec dalrecord.Record) error {
	if rec == nil {
		return fmt.Errorf("%w: nil record", ErrInvalidConfig)
	}
	op, err := d.begin(ctx)
	if err != nil {
		rec.SetError(err)
		return err
	}
	defer op.done()
	if err := d.validateRecordset(op.ctx, rec.Key().Collection()); err != nil {
		rec.SetError(err)
		return err
	}
	if err := d.delegate.Get(op.ctx, rec); err != nil {
		safe := sanitizeReadError(err)
		rec.SetError(safe)
		return safe
	}
	return nil
}

// Exists reports whether a record with the key exists, through the same
// declared and validated key.
func (d *database) Exists(ctx context.Context, key *dalrecord.Key) (bool, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return false, err
	}
	defer op.done()
	if key == nil {
		return false, fmt.Errorf("%w: nil key", ErrInvalidConfig)
	}
	if err := d.validateRecordset(op.ctx, key.Collection()); err != nil {
		return false, err
	}
	exists, err := d.delegate.Exists(op.ctx, key)
	if err != nil {
		return false, sanitizeReadError(err)
	}
	return exists, nil
}

// GetMulti reads several records by key. Every target collection must have a
// validated declaration; the check happens before the first statement, and
// every catalog probe shares the operation's deadline and budget.
func (d *database) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	op, err := d.begin(ctx)
	if err != nil {
		return err
	}
	defer op.done()
	for _, rec := range records {
		if rec == nil {
			return fmt.Errorf("%w: a nil record", ErrInvalidConfig)
		}
		if err := d.validateRecordset(op.ctx, rec.Key().Collection()); err != nil {
			return err
		}
	}
	if err := d.delegate.GetMulti(op.ctx, records); err != nil {
		return sanitizeReadError(err)
	}
	return nil
}

// ExecuteQueryToRecordsReader compiles and runs one structured read. The
// operation's deadline and budget stay alive until the returned reader is
// closed, exhausted, or the caller's context ends: the reader is not returned
// with a cancelled context.
func (d *database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	if err := d.checkQuery(query); err != nil {
		op.done()
		return nil, err
	}
	if err := d.validateDeclaredRecordsets(op.ctx); err != nil {
		op.done()
		return nil, sanitizeReadError(err)
	}
	reader, err := d.delegate.ExecuteQueryToRecordsReader(op.ctx, query)
	if err != nil {
		op.done()
		return nil, sanitizeReadError(err)
	}
	return &boundedRecordsReader{RecordsReader: reader, op: op}, nil
}

// ExecuteQueryToRecordsetReader is the recordset counterpart of
// ExecuteQueryToRecordsReader.
func (d *database) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	if err := d.checkQuery(query); err != nil {
		op.done()
		return nil, err
	}
	if err := d.validateDeclaredRecordsets(op.ctx); err != nil {
		op.done()
		return nil, sanitizeReadError(err)
	}
	reader, err := d.delegate.ExecuteQueryToRecordsetReader(op.ctx, query, options...)
	if err != nil {
		op.done()
		return nil, sanitizeReadError(err)
	}
	return &boundedRecordsetReader{RecordsetReader: reader, op: op}, nil
}

// boundedRecordsReader keeps the operation's deadline and budget alive until
// the reader is closed or reaches its end.
type boundedRecordsReader struct {
	dal.RecordsReader
	op *operation
}

func (r *boundedRecordsReader) Next() (dalrecord.Record, error) {
	rec, err := r.RecordsReader.Next()
	if err != nil {
		r.op.done()
	}
	return rec, err
}

func (r *boundedRecordsReader) Close() error {
	err := r.RecordsReader.Close()
	r.op.done()
	return err
}

// boundedRecordsetReader is the recordset counterpart.
type boundedRecordsetReader struct {
	dal.RecordsetReader
	op *operation
}

func (r *boundedRecordsetReader) Next() (recordset.Row, recordset.Recordset, error) {
	row, rs, err := r.RecordsetReader.Next()
	if err != nil {
		r.op.done()
	}
	return row, rs, err
}

func (r *boundedRecordsetReader) Close() error {
	err := r.RecordsetReader.Close()
	r.op.done()
	return err
}

// RunReadonlyTransaction is not implemented: a real read-only transaction
// needs a persistent Hrana stream with a baton and a server-side snapshot.
// The baton is not implemented, so no transaction is promised.
func (d *database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: read-only transactions need a stateful Hrana stream, which this checkpoint does not implement", ErrNotSupported)
}

// RunReadwriteTransaction refuses before any I/O: this adapter never writes.
func (d *database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: the adapter is read-only", ErrNotSupported)
}

// Close releases the private pool and this adapter's own idle connections.
// It is idempotent, and it never touches a client or transport supplied by
// the caller.
func (d *database) Close() error {
	d.closeOnce.Do(func() {
		d.closed.Store(true)
		var errs []error
		if err := d.pool.Close(); err != nil {
			errs = append(errs, errors.New("dalgo2libsql: closing the private pool failed"))
		}
		if err := d.client.Close(); err != nil {
			errs = append(errs, errors.New("dalgo2libsql: closing the transport failed"))
		}
		if len(errs) > 0 {
			d.closeErr = errors.Join(errs...)
		}
	})
	return d.closeErr
}

// operation is the single bounded scope of one public call. Every catalog,
// schema and data statement made for that call shares its deadline and its
// cumulative counters.
type operation struct {
	ctx    context.Context
	cancel context.CancelFunc
	budget *hrana.Budget
}

func (o *operation) done() {
	if o != nil && o.cancel != nil {
		o.cancel()
	}
}

// begin establishes the operation's deadline and budget before any preflight,
// header provider call or statement. A context that already carries a budget
// is a nested part of an open operation: it is reused as it is, so a reader
// keeps the deadline and counters it was created with.
func (d *database) begin(ctx context.Context) (*operation, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	if d.closed.Load() {
		return nil, fmt.Errorf("%w: the database is closed", ErrNotSupported)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if budget := hrana.BudgetFrom(ctx); budget != nil {
		return &operation{ctx: ctx, budget: budget}, nil
	}
	budget := hrana.NewBudget(d.limits)
	opCtx := hrana.WithBudget(ctx, budget)
	var cancel context.CancelFunc
	if d.timeout > 0 {
		opCtx, cancel = context.WithTimeout(opCtx, d.timeout)
	}
	return &operation{ctx: opCtx, budget: budget, cancel: cancel}, nil
}

// checkQuery allows a structured read and refuses text/native SQL and joins
// before any statement is compiled or sent.
func (d *database) checkQuery(query dal.Query) error {
	if query == nil {
		return fmt.Errorf("%w: nil query", ErrNotSupported)
	}
	if _, text := query.(dal.TextQuery); text {
		return fmt.Errorf("%w: text and native SQL queries are not exposed; use a structured read-only query", ErrNotSupported)
	}
	structured, ok := query.(dal.StructuredQuery)
	if !ok {
		return fmt.Errorf("%w: only structured read-only queries are supported", ErrNotSupported)
	}
	from := structured.From()
	if from != nil && len(from.Joins()) > 0 {
		return fmt.Errorf("%w: joins need a transaction-local key preflight that this checkpoint does not implement", ErrNotSupported)
	}
	return nil
}

func (d *database) validateRecordset(ctx context.Context, name string) error {
	key, declared := d.declared[name]
	if !declared {
		return fmt.Errorf("%w: collection %q has no declared primary key; declare it in Config.Recordsets or use a structured read", ErrNotSupported, name)
	}
	d.validateMu.Lock()
	already := d.validated[name]
	d.validateMu.Unlock()
	if already {
		return nil
	}
	ref := dal.NewRootCollectionRef(name, "")
	def, err := d.reader.DescribeCollection(ctx, &ref)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if safe := sanitizeReadError(err); safe != ErrBackend {
			return safe
		}
		return fmt.Errorf("%w: cannot validate the declared key of %q against the live schema", ErrInvalidConfig, name)
	}
	if len(def.PrimaryKey) != 1 || string(def.PrimaryKey[0]) != key {
		return fmt.Errorf("%w: %q does not have the declared single-column primary key", ErrInvalidConfig, name)
	}
	var found *dbschema.FieldDef
	for i := range def.Fields {
		if string(def.Fields[i].Name) == key {
			found = &def.Fields[i]
			break
		}
	}
	if found == nil {
		return fmt.Errorf("%w: the declared key column is not described by the schema", ErrInvalidConfig)
	}
	if found.Nullable {
		return fmt.Errorf("%w: the declared key column is nullable", ErrInvalidConfig)
	}
	if found.Type != dbschema.Int && found.Type != dbschema.String {
		return fmt.Errorf("%w: the declared key column is neither INTEGER nor TEXT", ErrInvalidConfig)
	}
	d.validateMu.Lock()
	if d.validated == nil {
		d.validated = map[string]bool{}
	}
	d.validated[name] = true
	d.validateMu.Unlock()
	return nil
}

func (d *database) validateDeclaredRecordsets(ctx context.Context) error {
	for name := range d.declared {
		if err := d.validateRecordset(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// schemaReader methods are the five borrowed dbschema.SchemaReader methods,
// each run in the operation's shared deadline and budget.
func (d *database) ListCollections(ctx context.Context, parent *dalrecord.Key) ([]dal.CollectionRef, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done()
	return d.reader.ListCollections(op.ctx, parent)
}

func (d *database) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done()
	return d.reader.DescribeCollection(op.ctx, ref)
}

func (d *database) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done()
	return d.reader.ListIndexes(op.ctx, ref)
}

func (d *database) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done()
	return d.reader.ListConstraints(op.ctx, ref)
}

func (d *database) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done()
	return d.reader.ListReferrers(op.ctx, ref)
}

// ErrBackend reports a read that failed for a reason this adapter does not
// classify. It deliberately carries no server text, SQL, URL or transport
// detail.
var ErrBackend = errors.New("dalgo2libsql: the read failed")

// sanitizeReadError keeps context identity and the adapter's own classified
// errors, and replaces anything else with ErrBackend so a server message, SQL
// text, a column of a broken stream or a transport detail never reaches the
// caller.
func sanitizeReadError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, dalrecord.ErrRecordNotFound):
		return fmt.Errorf("%w: %w", ErrBackend, err)
	case errors.Is(err, dal.ErrNotSupported), errors.Is(err, dal.ErrNotImplementedYet), errors.Is(err, ErrNotSupported):
		return fmt.Errorf("%w: %w", ErrBackend, err)
	case errors.Is(err, hrana.ErrLimit), errors.Is(err, hrana.ErrProtocol):
		return err
	case errors.Is(err, ErrInvalidConfig), errors.Is(err, ErrUnsafeTarget), errors.Is(err, ErrUnsupportedResponse):
		return err
	}
	var protocolErr *hrana.Error
	if errors.As(err, &protocolErr) {
		return protocolErr
	}
	return ErrBackend
}
