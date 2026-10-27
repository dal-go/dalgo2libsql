package dalgo2libsql

import (
	"context"

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

func (r *schemaReader) ctx(ctx context.Context) context.Context {
	if hrana.BudgetFrom(ctx) != nil {
		return ctx
	}
	return hrana.WithBudget(ctx, hrana.NewBudget(r.limits))
}

func (r *schemaReader) ListCollections(ctx context.Context, parent *dalrecord.Key) ([]dal.CollectionRef, error) {
	return r.reader.ListCollections(r.ctx(ctx), parent)
}

func (r *schemaReader) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	return r.reader.DescribeCollection(r.ctx(ctx), ref)
}

func (r *schemaReader) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	return r.reader.ListIndexes(r.ctx(ctx), ref)
}

func (r *schemaReader) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	return r.reader.ListConstraints(r.ctx(ctx), ref)
}

func (r *schemaReader) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	return r.reader.ListReferrers(r.ctx(ctx), ref)
}
