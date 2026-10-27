package dalgo2libsql

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/record"

	"github.com/dal-go/dalgo2libsql/internal/hrana"
)

func seedSQLite(t *testing.T) *sqliteHrana {
	t.Helper()
	server, _ := newSQLiteHrana(t)
	server.exec(t,
		`CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			qty INTEGER,
			price REAL,
			payload BLOB,
			note TEXT
		)`,
		`INSERT INTO items (id, name, qty, price, payload, note) VALUES (1, 'one', 'not-a-number', 1.5, x'000102', NULL)`,
		`INSERT INTO items (id, name, qty, price, payload, note) VALUES (2, 'two', 42, 2.25, NULL, 'second')`,
		`INSERT INTO items (id, name, qty, price, payload, note) VALUES (3, 'three', NULL, NULL, NULL, 'third')`,
		`CREATE TABLE composite (a INTEGER NOT NULL, b TEXT NOT NULL, v TEXT, PRIMARY KEY (a, b))`,
		`CREATE TABLE nullable_key (code TEXT, v TEXT)`,
		`CREATE UNIQUE INDEX nullable_key_code ON nullable_key (code)`,
		`CREATE TABLE pk_desc (id INTEGER PRIMARY KEY DESC, v TEXT)`,
		`INSERT INTO pk_desc (id, v) VALUES (NULL, 'null-first'), (1, 'one')`,
		`CREATE TABLE unique_key (code TEXT NOT NULL UNIQUE, v TEXT)`,
		`CREATE TABLE other_items (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO other_items (id, v) VALUES (10, 'ten')`,
		`CREATE VIEW item_names AS SELECT id, name FROM items`,
	)
	return server
}

func openSeedBackend(t *testing.T, server *sqliteHrana, recordsets map[string]string) dal.DB {
	t.Helper()
	db, err := NewDB(context.Background(), Config{
		URL:               server.url,
		AllowLoopbackHTTP: true,
		Recordsets:        recordsets,
		MaxOpenConns:      1,
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { closeDB(t, db) })
	return db
}

func TestBackendGetExistsMultiAndSchema(t *testing.T) {
	server := seedSQLite(t)
	db := openSeedBackend(t, server, map[string]string{"items": "id"})

	rec := record.NewRecordWithData(record.NewKeyWithID("items", 2), make(map[string]any))
	if err := db.Get(context.Background(), rec); err != nil {
		t.Fatalf("Get: %v", err)
	}
	data := rec.Data().(map[string]any)
	if !reflect.DeepEqual(data["name"], "two") || !reflect.DeepEqual(data["qty"], int64(42)) || !reflect.DeepEqual(data["note"], "second") {
		t.Fatalf("data = %#v", data)
	}

	// SQLite is dynamically typed: a TEXT value in an INTEGER column comes
	// back as text, a blob as bytes. Neither is coerced to the declared type.
	rec = record.NewRecordWithData(record.NewKeyWithID("items", 1), make(map[string]any))
	if err := db.Get(context.Background(), rec); err != nil {
		t.Fatalf("Get: %v", err)
	}
	data = rec.Data().(map[string]any)
	if !reflect.DeepEqual(data["qty"], "not-a-number") {
		t.Fatalf("qty = %#v", data["qty"])
	}
	if !reflect.DeepEqual(data["payload"], []byte{0, 1, 2}) {
		t.Fatalf("payload = %#v", data["payload"])
	}

	exists, err := db.Exists(context.Background(), record.NewKeyWithID("items", 3))
	if err != nil || !exists {
		t.Fatalf("Exists = %v, %v", exists, err)
	}
	exists, err = db.Exists(context.Background(), record.NewKeyWithID("items", 99))
	if err != nil || exists {
		t.Fatalf("Exists(missing) = %v, %v", exists, err)
	}

	multi := []record.Record{
		record.NewRecordWithData(record.NewKeyWithID("items", 2), make(map[string]any)),
		record.NewRecordWithData(record.NewKeyWithID("items", 3), make(map[string]any)),
	}
	if err := db.GetMulti(context.Background(), multi); err != nil {
		t.Fatalf("GetMulti: %v", err)
	}
	if got := multi[1].Data().(map[string]any)["name"]; got != "three" {
		t.Fatalf("GetMulti name = %#v", got)
	}

	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("the backend does not expose the borrowed schema reader")
	}
	collections, err := reader.ListCollections(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	found := false
	for _, ref := range collections {
		if ref.Name() == "items" {
			found = true
		}
	}
	if !found {
		t.Fatalf("collections = %#v", collections)
	}
	ref := dal.NewRootCollectionRef("items", "")
	def, err := reader.DescribeCollection(context.Background(), &ref)
	if err != nil {
		t.Fatalf("DescribeCollection: %v", err)
	}
	if len(def.PrimaryKey) != 1 || string(def.PrimaryKey[0]) != "id" {
		t.Fatalf("primary key = %#v", def.PrimaryKey)
	}
}

func TestBackendKeyedReadsRefuseUnvalidatedKeys(t *testing.T) {
	server := seedSQLite(t)
	cases := map[string]struct {
		collection string
		recordsets map[string]string
	}{
		"undeclared":             {collection: "items"},
		"wrong key":              {collection: "items", recordsets: map[string]string{"items": "name"}},
		"composite":              {collection: "composite", recordsets: map[string]string{"composite": "a"}},
		"nullable key":           {collection: "nullable_key", recordsets: map[string]string{"nullable_key": "code"}},
		"non-integer key":        {collection: "items", recordsets: map[string]string{"items": "price"}},
		"descending integer key": {collection: "pk_desc", recordsets: map[string]string{"pk_desc": "id"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			db := openSeedBackend(t, server, tc.recordsets)
			rec := record.NewRecordWithData(record.NewKeyWithID(tc.collection, 1), make(map[string]any))
			err := db.Get(context.Background(), rec)
			if err == nil {
				t.Fatal("the keyed read was accepted")
			}
			if !errors.Is(err, ErrInvalidConfig) && !errors.Is(err, ErrNotSupported) {
				t.Fatalf("error = %v", err)
			}
			if rec.Error() == nil {
				t.Fatal("the record does not carry the refusal")
			}
		})
	}
}

func TestBackendStructuredReadsLimitOffsetAndOrder(t *testing.T) {
	server := seedSQLite(t)
	db := openSeedBackend(t, server, map[string]string{"items": "id"})

	collection := dal.NewRootCollectionRef("items", "")
	query := dal.NewQueryBuilder(dal.From(&collection)).
		OrderBy(dal.Ascending(dal.Field("id"))).
		Limit(2).
		Offset(1).
		SelectIntoRecordset()

	first := readIDs(t, db, query)
	second := readIDs(t, db, query)
	if !reflect.DeepEqual(first, []int64{2, 3}) {
		t.Fatalf("ids = %#v", first)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("a repeated explicit-order read changed: %#v vs %#v", first, second)
	}
	if !server.sawStatement("LIMIT") || !server.sawStatement("OFFSET") {
		t.Fatalf("the server did not receive LIMIT/OFFSET")
	}

	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
	}
	if cursor, err := reader.Cursor(); err == nil || cursor != "" {
		t.Fatalf("an opaque cursor was exposed: %q, %v", cursor, err)
	}
	_ = reader.Close()

	rsReader, err := db.ExecuteQueryToRecordsetReader(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
	}
	row, rs, err := rsReader.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	for name, want := range map[string]any{
		"id":      int64(2),
		"name":    "two",
		"qty":     int64(42),
		"note":    "second",
		"price":   2.25,
		"payload": nil,
	} {
		value, err := row.GetValueByName(name, rs)
		if err != nil || !reflect.DeepEqual(value, want) {
			t.Fatalf("recordset %s = %#v, %v (want %#v)", name, value, err, want)
		}
	}
	// The recordset holds the connection until it is done: close it before
	// opening another reader on the single-connection pool.
	if err := rsReader.Close(); err != nil {
		t.Fatalf("close recordset reader: %v", err)
	}

	// A float and a blob survive through a recordset read as the values the
	// driver delivered, not as declared-type coercions.
	scalar := dal.NewQueryBuilder(dal.From(&collection)).
		WhereField("id", dal.Equal, 1).
		SelectColumns(
			dal.Column{Expression: dal.Field("id")},
			dal.Column{Expression: dal.Field("price")},
			dal.Column{Expression: dal.Field("payload")},
		)
	sReader, err := db.ExecuteQueryToRecordsetReader(context.Background(), scalar)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(scalar): %v", err)
	}
	defer func() { _ = sReader.Close() }()
	scalarRow, scalarSet, err := sReader.Next()
	if err != nil {
		t.Fatalf("Next(scalar): %v", err)
	}
	price, err := scalarRow.GetValueByName("price", scalarSet)
	if err != nil || !reflect.DeepEqual(price, 1.5) {
		t.Fatalf("recordset price = %#v, %v", price, err)
	}
	payload, err := scalarRow.GetValueByName("payload", scalarSet)
	if err != nil || !reflect.DeepEqual(payload, []byte{0, 1, 2}) {
		t.Fatalf("recordset payload = %#v, %v", payload, err)
	}
}

// TestDriverPreservesMixedStorageClasses holds the driver, not the delegate,
// to SQLite's dynamic typing: the declared affinity never changes a value.
func TestDriverPreservesMixedStorageClasses(t *testing.T) {
	server := seedSQLite(t)
	pool := sql.OpenDB(newTestConnector(t, server.url))
	defer func() { _ = pool.Close() }()

	rows, err := pool.QueryContext(context.Background(),
		"SELECT id, qty, price, payload, note FROM items WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("no row: %v", rows.Err())
	}
	var id, qty, price, payload, note any
	if err := rows.Scan(&id, &qty, &price, &payload, &note); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if id != int64(1) {
		t.Errorf("id = %#v", id)
	}
	if qty != "not-a-number" {
		t.Errorf("qty = %#v, want the stored TEXT", qty)
	}
	if price != 1.5 {
		t.Errorf("price = %#v", price)
	}
	if !reflect.DeepEqual(payload, []byte{0, 1, 2}) {
		t.Errorf("payload = %#v", payload)
	}
	if note != nil {
		t.Errorf("note = %#v, want NULL", note)
	}
}

func readIDs(t *testing.T, db dal.DB, query dal.Query) []int64 {
	t.Helper()
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var ids []int64
	for {
		rec, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			return ids
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		data := rec.Data().(map[string]any)
		switch id := data["id"].(type) {
		case int64:
			ids = append(ids, id)
		case int:
			ids = append(ids, int64(id))
		default:
			t.Fatalf("id = %#v", data["id"])
		}
	}
}

func TestBackendRefusesUnsupportedOperationsBeforeIO(t *testing.T) {
	server := seedSQLite(t)
	db := openSeedBackend(t, server, nil)
	if _, ok := any(dal.BackendOf(db)).(dal.WriteSession); ok {
		t.Fatal("the backend exposes writes")
	}

	ctx := context.Background()
	if err := db.RunReadonlyTransaction(ctx, func(context.Context, dal.ReadTransaction) error {
		t.Error("the worker must not run")
		return nil
	}); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("read-only transaction error = %v", err)
	}
	if err := db.RunReadwriteTransaction(ctx, func(context.Context, dal.ReadwriteTransaction) error {
		t.Error("the worker must not run")
		return nil
	}); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("read-write transaction error = %v", err)
	}
	if _, err := db.ExecuteQueryToRecordsReader(ctx, dal.NewTextQuery("SELECT 1", nil)); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("text query error = %v", err)
	}

	items := dal.NewRootCollectionRef("items", "i")
	other := dal.NewRootCollectionRef("items", "o")
	on := dal.NewComparison(dal.NewFieldRef("i", "id"), dal.Equal, dal.NewFieldRef("o", "id"))
	joined := dal.From(&items).Join(dal.NewJoinedSource(other, dal.JoinInner, on))
	// The adapter itself refuses a native join before compiling SQL. DALgo's
	// wrapper may still plan a join locally from plain structured reads; that
	// is the framework's plan, not a native-join claim by this adapter.
	if _, err := dal.BackendOf(db).ExecuteQueryToRecordsReader(ctx, joined.NewQuery().SelectIntoRecordset()); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("native join error = %v", err)
	}

	if _, ok := dal.As[io.Closer](db); !ok {
		t.Fatal("the database is not closable")
	}
}

func TestBackendOperationBudgetIsCumulative(t *testing.T) {
	server := seedSQLite(t)
	first, err := NewDB(context.Background(), Config{
		URL:               server.url,
		AllowLoopbackHTTP: true,
		Recordsets:        map[string]string{"items": "id"},
		MaxOpenConns:      1,
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer closeDB(t, first)
	rec := record.NewRecordWithData(record.NewKeyWithID("items", 2), make(map[string]any))
	if err := first.Get(context.Background(), rec); err != nil {
		t.Fatalf("Get: %v", err)
	}
	used := server.requestCount()
	if used < 2 {
		t.Fatalf("a keyed read used %d requests; the test needs validation plus data", used)
	}

	// One budget less than the whole operation needs: validation alone would
	// fit, so a fresh budget for the data statement would wrongly succeed.
	server.resetRequests()
	second, err := NewDB(context.Background(), Config{
		URL:                  server.url,
		AllowLoopbackHTTP:    true,
		Recordsets:           map[string]string{"items": "id"},
		MaxOpenConns:         1,
		MaxOperationRequests: used - 1,
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer closeDB(t, second)
	rec = record.NewRecordWithData(record.NewKeyWithID("items", 2), make(map[string]any))
	if err := second.Get(context.Background(), rec); !errors.Is(err, hrana.ErrLimit) {
		t.Fatalf("error = %v, want the cumulative request limit", err)
	}
	if got := server.requestCount(); got > used-1 {
		t.Fatalf("the operation made %d requests, above its %d budget", got, used-1)
	}
}

func TestBackendMappedCollectionsShareOneBudget(t *testing.T) {
	server := seedSQLite(t)
	db, err := NewDB(context.Background(), Config{
		URL:                  server.url,
		AllowLoopbackHTTP:    true,
		Recordsets:           map[string]string{"items": "id", "other_items": "id"},
		MaxOpenConns:         1,
		MaxOperationRequests: 2,
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer closeDB(t, db)
	records := []record.Record{
		record.NewRecordWithData(record.NewKeyWithID("items", 2), make(map[string]any)),
		record.NewRecordWithData(record.NewKeyWithID("other_items", 10), make(map[string]any)),
	}
	// Validating both declarations already uses the two allowed requests, so
	// a per-collection (or per-statement) budget would let the read through.
	if err := db.GetMulti(context.Background(), records); !errors.Is(err, hrana.ErrLimit) {
		t.Fatalf("error = %v, want the cumulative request limit", err)
	}
}

func TestBackendDeadlineCoversPreflightAndHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer server.Close()
	var sawDeadline atomic.Bool
	db, err := NewDB(context.Background(), Config{
		URL:               server.URL,
		AllowLoopbackHTTP: true,
		Timeout:           50 * time.Millisecond,
		Headers: func(ctx context.Context) (map[string]string, error) {
			_, hasDeadline := ctx.Deadline()
			sawDeadline.Store(hasDeadline)
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer closeDB(t, db)
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("the backend does not expose the schema reader")
	}
	if _, err := reader.ListCollections(context.Background(), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want the operation deadline", err)
	}
	if !sawDeadline.Load() {
		t.Fatal("the header provider did not receive the operation deadline")
	}
}

func TestBackendReaderRetainsTheOperationDeadline(t *testing.T) {
	server := seedSQLite(t)
	db, err := NewDB(context.Background(), Config{
		URL:               server.url,
		AllowLoopbackHTTP: true,
		MaxOpenConns:      1,
		Timeout:           60 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer closeDB(t, db)
	collection := dal.NewRootCollectionRef("items", "")
	query := dal.From(&collection).NewQuery().SelectIntoRecordset()
	reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	time.Sleep(100 * time.Millisecond)
	if _, _, err := reader.Next(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next error = %v, want the operation deadline", err)
	}
}

func TestBackendKeepsCancellationIdentity(t *testing.T) {
	server := seedSQLite(t)
	db := openSeedBackend(t, server, map[string]string{"items": "id"})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	rec := record.NewRecordWithData(record.NewKeyWithID("items", 2), make(map[string]any))
	if err := db.Get(cancelled, rec); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get error = %v", err)
	}
	if !errors.Is(rec.Error(), context.Canceled) {
		t.Fatalf("record error = %v", rec.Error())
	}
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("the backend does not expose the schema reader")
	}
	if _, err := reader.ListCollections(cancelled, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListCollections error = %v", err)
	}
}

func TestBackendDoesNotEchoServerText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"token":"super-secret","query":"SELECT secret FROM t"}`))
	}))
	defer server.Close()
	db, err := NewDB(context.Background(), Config{URL: server.URL, AllowLoopbackHTTP: true})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer closeDB(t, db)
	rec := record.NewRecordWithData(record.NewKeyWithID("items", 1), make(map[string]any))
	err = db.Get(context.Background(), rec)
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, secret := range []string{"super-secret", "SELECT secret", "token"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error echoes %q: %v", secret, err)
		}
	}
}

func TestBackendAdvertisesOnlyCarriedCapabilities(t *testing.T) {
	caps := (&database{}).QueryCapabilities()
	want := dal.QueryCapabilities{
		GroupBy: true,
		Having:  true,
		OrderBy: true,
		Aggregate: dal.AggregateCapabilities{
			Count:         true,
			CountDistinct: true,
			Sum:           true,
			SumDistinct:   true,
			Avg:           true,
			AvgDistinct:   true,
			Min:           true,
			Max:           true,
		},
	}
	if !reflect.DeepEqual(caps, want) {
		t.Fatalf("capabilities = %#v, want exactly %#v", caps, want)
	}
	// The claim selects DALgo's native plan, not its local fallback.
	items := dal.NewRootCollectionRef("items", "")
	grouped := dal.From(&items).NewQuery().
		GroupBy(dal.Field("qty")).
		Having(dal.NewComparison(dal.Count().Expression, dal.GreaterThen, dal.NewConstant(int64(0)))).
		SelectColumns(
			dal.Column{Expression: dal.Field("qty")},
			dal.CountAs(dal.Star(), "n"),
		)
	plan, err := dal.PlanAggregation(grouped, caps)
	if err != nil {
		t.Fatalf("PlanAggregation: %v", err)
	}
	if plan.Strategy != dal.AggregationNative {
		t.Fatalf("strategy = %q (%s), want native", plan.Strategy, plan.Reason)
	}
}

// TestBackendAggregatesAndGroupingReachTheServer proves the capability claim
// with the real dalgo2sql delegate and the real SQLite compiler: the grouped
// and aggregate SQL reaches the server, through both readers, rather than
// being computed by DALgo's local fallback.
func TestBackendAggregatesAndGroupingReachTheServer(t *testing.T) {
	server := seedSQLite(t)
	db := openSeedBackend(t, server, map[string]string{"items": "id"})
	ctx := context.Background()
	items := dal.NewRootCollectionRef("items", "")

	scalar := dal.From(&items).NewQuery().SelectColumns(
		dal.CountAs(dal.Star(), "n"),
		dal.SumAs(dal.Field("price"), "sum"),
		dal.AverageAs(dal.Field("price"), "avg"),
		dal.MinAs(dal.Field("price"), "min"),
		dal.MaxAs(dal.Field("price"), "max"),
		dal.CountDistinctAs(dal.Field("price"), "n_distinct"),
		dal.SumDistinctAs(dal.Field("price"), "sum_distinct"),
		dal.AverageDistinctAs(dal.Field("price"), "avg_distinct"),
	)
	reader, err := db.ExecuteQueryToRecordsReader(ctx, scalar)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(scalar): %v", err)
	}
	rows := readAllRecordValues(t, reader)
	if err := reader.Close(); err != nil {
		t.Fatalf("close scalar reader: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("scalar rows = %d, want 1", len(rows))
	}
	for name, want := range map[string]any{
		"n":            int64(3),
		"sum":          float64(3.75),
		"avg":          1.875,
		"min":          1.5,
		"max":          2.25,
		"n_distinct":   int64(2),
		"sum_distinct": float64(3.75),
		"avg_distinct": 1.875,
	} {
		requireNativeValue(t, "scalar "+name, rows[0][name], want)
	}
	for _, fragment := range []string{"SUM(", "AVG(", "COUNT(", "MIN(", "MAX("} {
		if !server.sawStatement(fragment) {
			t.Errorf("the aggregate %q never reached the server", fragment)
		}
	}

	grouped := dal.From(&items).NewQuery().
		GroupBy(dal.Field("qty")).
		OrderBy(dal.Ascending(dal.Field("qty"))).
		SelectColumns(
			dal.Column{Expression: dal.Field("qty")},
			dal.CountAs(dal.Star(), "n"),
			dal.SumAs(dal.Field("price"), "sum"),
		)
	server.resetStatements()
	reader, err = db.ExecuteQueryToRecordsReader(ctx, grouped)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(grouped): %v", err)
	}
	groups := readAllRecordValues(t, reader)
	if err := reader.Close(); err != nil {
		t.Fatalf("close grouped reader: %v", err)
	}
	if !server.sawStatement("GROUP BY") {
		t.Fatal("the grouped read did not reach the server as SQL")
	}
	requireGroups(t, "grouped records", groups, []map[string]any{
		{"qty": nil, "n": int64(1), "sum": nil},
		{"qty": float64(42), "n": int64(1), "sum": 2.25},
		{"qty": "not-a-number", "n": int64(1), "sum": 1.5},
	})

	having := dal.From(&items).NewQuery().
		GroupBy(dal.Field("qty")).
		Having(dal.NewComparison(dal.Count().Expression, dal.GreaterOrEqual, dal.NewConstant(int64(1)))).
		OrderBy(dal.Ascending(dal.Field("qty"))).
		SelectColumns(
			dal.Column{Expression: dal.Field("qty")},
			dal.CountAs(dal.Star(), "n"),
		)
	server.resetStatements()
	reader, err = db.ExecuteQueryToRecordsReader(ctx, having)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(having): %v", err)
	}
	havingRows := readAllRecordValues(t, reader)
	if err := reader.Close(); err != nil {
		t.Fatalf("close having reader: %v", err)
	}
	if !server.sawStatement("HAVING") {
		t.Fatal("the HAVING read did not reach the server as SQL")
	}
	if len(havingRows) != 3 {
		t.Fatalf("having rows = %#v, want 3 groups", havingRows)
	}

	// The recordset reader runs the same native SQL and holds the aggregate
	// results in any-typed columns.
	server.resetStatements()
	rsReader, err := db.ExecuteQueryToRecordsetReader(ctx, grouped)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(grouped): %v", err)
	}
	groupSets := readAllRecordsetValues(t, rsReader)
	if err := rsReader.Close(); err != nil {
		t.Fatalf("close grouped recordset reader: %v", err)
	}
	if !server.sawStatement("GROUP BY") {
		t.Fatal("the grouped recordset read did not reach the server as SQL")
	}
	requireGroups(t, "grouped recordset", groupSets, []map[string]any{
		{"qty": nil, "n": int64(1), "sum": nil},
		{"qty": float64(42), "n": int64(1), "sum": 2.25},
		{"qty": "not-a-number", "n": int64(1), "sum": 1.5},
	})
}
