package dalgo2libsql

import (
	"context"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	dalrecord "github.com/dal-go/record"

	"github.com/dal-go/dalgo2libsql/internal/hrana"
)

// schemaReader is the budget wrapper around dalgo2sqlite's borrowed
// dbschema.SchemaReader. It is the only schema surface the backend exposes:
// no DDL, row, or Close method of that package's Database is reachable, and
// the pool it borrows is owned and closed by this adapter.
type schemaReader struct {
	reader dbschema.SchemaReader
	limits hrana.Limits
}

var _ dbschema.SchemaReader = (*schemaReader)(nil)

// scope returns an operation-scoped context. A context that already carries
// the operation's budget is used as it is, so a probe made during a keyed read
// or inside a reader shares that operation's deadline and counters. A direct
// call gets its own deadline and budget, cancelled when the call returns.
func (r *schemaReader) scope(ctx context.Context) (context.Context, context.CancelFunc) {
	if hrana.BudgetFrom(ctx) != nil {
		return ctx, func() {}
	}
	scoped := hrana.WithBudget(ctx, hrana.NewBudget(r.limits))
	if r.limits.Timeout <= 0 {
		return scoped, func() {}
	}
	return context.WithTimeout(scoped, time.Duration(r.limits.Timeout))
}

func (r *schemaReader) ListCollections(ctx context.Context, parent *dalrecord.Key) ([]dal.CollectionRef, error) {
	scoped, cancel := r.scope(ctx)
	defer cancel()
	return r.reader.ListCollections(scoped, parent)
}

func (r *schemaReader) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	scoped, cancel := r.scope(ctx)
	defer cancel()
	return r.reader.DescribeCollection(scoped, ref)
}

func (r *schemaReader) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	scoped, cancel := r.scope(ctx)
	defer cancel()
	return r.reader.ListIndexes(scoped, ref)
}

func (r *schemaReader) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	scoped, cancel := r.scope(ctx)
	defer cancel()
	return r.reader.ListConstraints(scoped, ref)
}

func (r *schemaReader) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	scoped, cancel := r.scope(ctx)
	defer cancel()
	return r.reader.ListReferrers(scoped, ref)
}
