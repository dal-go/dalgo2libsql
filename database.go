package dalgo2libsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

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
	if err := d.ready(ctx, rec); err != nil {
		if rec != nil {
			rec.SetError(err)
		}
		return err
	}
	if err := d.validateRecordset(ctx, rec.Key().Collection()); err != nil {
		rec.SetError(err)
		return err
	}
	err := d.delegate.Get(d.withBudget(ctx), rec)
	if err != nil {
		safe := sanitizeReadError(err)
		rec.SetError(safe)
		return safe
	}
	return nil
}

// Exists reports whether a record with the key exists, through the same
// declared and validated key.
func (d *database) Exists(ctx context.Context, key *dalrecord.Key) (bool, error) {
	if err := d.ready(ctx, nil); err != nil {
		return false, err
	}
	if key == nil {
		return false, fmt.Errorf("%w: nil key", ErrInvalidConfig)
	}
	if err := d.validateRecordset(ctx, key.Collection()); err != nil {
		return false, err
	}
	exists, err := d.delegate.Exists(d.withBudget(ctx), key)
	if err != nil {
		return false, sanitizeReadError(err)
	}
	return exists, nil
}

// GetMulti reads several records by key. Every target collection must have a
// validated declaration; the check happens before the first statement.
func (d *database) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	if err := d.ready(ctx, nil); err != nil {
		return err
	}
	for _, rec := range records {
		if rec == nil {
			return fmt.Errorf("%w: a nil record", ErrInvalidConfig)
		}
		if err := d.validateRecordset(ctx, rec.Key().Collection()); err != nil {
			return err
		}
	}
	if err := d.delegate.GetMulti(d.withBudget(ctx), records); err != nil {
		return sanitizeReadError(err)
	}
	return nil
}

func (d *database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	if err := d.ready(ctx, nil); err != nil {
		return nil, err
	}
	if err := d.checkQuery(query); err != nil {
		return nil, err
	}
	if err := d.validateDeclaredRecordsets(ctx); err != nil {
		return nil, err
	}
	return d.delegate.ExecuteQueryToRecordsReader(d.withBudget(ctx), query)
}

func (d *database) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	if err := d.ready(ctx, nil); err != nil {
		return nil, err
	}
	if err := d.checkQuery(query); err != nil {
		return nil, err
	}
	if err := d.validateDeclaredRecordsets(ctx); err != nil {
		return nil, err
	}
	return d.delegate.ExecuteQueryToRecordsetReader(d.withBudget(ctx), query, options...)
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

func (d *database) ready(ctx context.Context, _ dalrecord.Record) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	if d.closed.Load() {
		return fmt.Errorf("%w: the database is closed", ErrNotSupported)
	}
	return ctx.Err()
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

func (d *database) withBudget(ctx context.Context) context.Context {
	if hrana.BudgetFrom(ctx) != nil {
		return ctx
	}
	return hrana.WithBudget(ctx, hrana.NewBudget(d.limits))
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
	def, err := d.reader.DescribeCollection(d.withBudget(ctx), &ref)
	if err != nil {
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
// each run under the operation's shared budget.
func (d *database) ListCollections(ctx context.Context, parent *dalrecord.Key) ([]dal.CollectionRef, error) {
	if err := d.ready(ctx, nil); err != nil {
		return nil, err
	}
	return d.reader.ListCollections(d.withBudget(ctx), parent)
}

func (d *database) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	if err := d.ready(ctx, nil); err != nil {
		return nil, err
	}
	return d.reader.DescribeCollection(d.withBudget(ctx), ref)
}

func (d *database) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	if err := d.ready(ctx, nil); err != nil {
		return nil, err
	}
	return d.reader.ListIndexes(d.withBudget(ctx), ref)
}

func (d *database) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	if err := d.ready(ctx, nil); err != nil {
		return nil, err
	}
	return d.reader.ListConstraints(d.withBudget(ctx), ref)
}

func (d *database) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	if err := d.ready(ctx, nil); err != nil {
		return nil, err
	}
	return d.reader.ListReferrers(d.withBudget(ctx), ref)
}

// ErrBackend reports a read that failed for a reason this adapter does not
// classify. It deliberately carries no server text, SQL, URL or transport
// detail.
var ErrBackend = errors.New("dalgo2libsql: the read failed")

// sanitizeReadError keeps context identity and known DALgo sentinels, and
// replaces anything else with ErrBackend so a server message, SQL text or a
// transport detail never reaches the caller.
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
	default:
		return ErrBackend
	}
}
