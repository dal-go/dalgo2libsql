package dalgo2libsql

// The integration suite runs against the real, commit-pinned libSQL server
// started by scripts/libsql-fixture.mjs. It is opt-in: without
// LIBSQL_FIXTURE_URL every test skips, and the script sets
// LIBSQL_REQUIRE_FIXTURE=1 so a skipped test fails the scripted run.

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/record"
)

func fixtureURL(t *testing.T) (string, string) {
	t.Helper()
	url := os.Getenv("LIBSQL_FIXTURE_URL")
	if url == "" {
		if os.Getenv("LIBSQL_REQUIRE_FIXTURE") == "1" {
			t.Fatal("LIBSQL_FIXTURE_URL is required by LIBSQL_REQUIRE_FIXTURE=1")
		}
		t.Skip("set LIBSQL_FIXTURE_URL (or run scripts/libsql-fixture.mjs -- go test ...) to run the real libSQL integration suite")
	}
	token := os.Getenv("LIBSQL_RO_TOKEN")
	if token == "" {
		t.Fatal("LIBSQL_RO_TOKEN is required with LIBSQL_FIXTURE_URL")
	}
	return url, token
}

// openFixtureDB opens the adapter with the fixture's read-only token. The
// writer token is used only by the fixture script to seed; no test below has
// access to it.
func openFixtureDB(t *testing.T) dal.DB {
	t.Helper()
	url, token := fixtureURL(t)
	db, err := NewDB(context.Background(), Config{
		URL:               url,
		AllowLoopbackHTTP: strings.HasPrefix(url, "http://"),
		Headers: func(context.Context) (map[string]string, error) {
			return map[string]string{"Authorization": "Bearer " + token}, nil
		},
		Recordsets: map[string]string{
			"items":   "id",
			"bigints": "id",
			"blobs":   "id",
			"mixed":   "id",
		},
		MaxOpenConns: 1,
	})
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { closeDB(t, db) })
	return db
}

// fixtureMixedRows is what each row of the fixture's mixed table holds, in its
// native class. declared_integer holds all five SQLite storage classes in
// different rows.
var fixtureMixedRows = []struct {
	id              int64
	declaredInteger any
	declaredText    any
}{
	{1, "text-in-integer", []byte{0x00, 0x01, 0x02}},
	{2, int64(42), "plain"},
	{3, 2.5, nil},
	{4, []byte{0x0a, 0xff}, "seven"},
	{5, nil, "last"},
}

func TestIntegrationReadsNativeValues(t *testing.T) {
	db := openFixtureDB(t)
	ctx := context.Background()

	// Get through the declared key keeps every native class.
	rec := record.NewRecordWithData(record.NewKeyWithID("items", 1), make(map[string]any))
	if err := db.Get(ctx, rec); err != nil {
		t.Fatalf("Get: %v", err)
	}
	data := rec.Data().(map[string]any)
	for name, want := range map[string]any{
		"id":      int64(1),
		"title":   "Alpha",
		"rank":    int64(2),
		"score":   9.5,
		"done":    int64(0),
		"code":    "a",
		"payload": []byte{0x00, 0xff, 0x10},
	} {
		requireNativeValue(t, name, data[name], want)
	}

	// NULL stays nil: no zero value stands in for it.
	rec = record.NewRecordWithData(record.NewKeyWithID("items", 2), make(map[string]any))
	if err := db.Get(ctx, rec); err != nil {
		t.Fatalf("Get(item 2): %v", err)
	}
	data = rec.Data().(map[string]any)
	if data["score"] != nil || data["payload"] != nil {
		t.Errorf("NULLs = %#v, %#v", data["score"], data["payload"])
	}

	// The full int64 range and a value above 2^53 survive the string encoding
	// of an integer, through the key and through both reader types.
	for _, id := range []int64{math.MaxInt64, math.MinInt64, 9007199254740993} {
		rec = record.NewRecordWithData(record.NewKeyWithID("bigints", id), make(map[string]any))
		if err := db.Get(ctx, rec); err != nil {
			t.Fatalf("Get(bigints %d): %v", id, err)
		}
		requireNativeValue(t, "bigints id", rec.Data().(map[string]any)["id"], id)
	}
	bigints := dal.NewRootCollectionRef("bigints", "")
	bigintQuery := dal.From(&bigints).NewQuery().
		OrderBy(dal.Ascending(dal.Field("id"))).
		SelectColumns(dal.Column{Expression: dal.Field("id")})
	wantBigints := []int64{math.MinInt64, 9007199254740993, math.MaxInt64}
	recordsReader, err := db.ExecuteQueryToRecordsReader(ctx, bigintQuery)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(bigints): %v", err)
	}
	for i, row := range readAllRecordValues(t, recordsReader) {
		requireNativeValue(t, "bigints records id", row["id"], wantBigints[i])
	}
	if err := recordsReader.Close(); err != nil {
		t.Fatalf("close bigints records reader: %v", err)
	}
	recordsetReader, err := db.ExecuteQueryToRecordsetReader(ctx, bigintQuery)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(bigints): %v", err)
	}
	for i, row := range readAllRecordsetValues(t, recordsetReader) {
		requireNativeValue(t, "bigints recordset id", row["id"], wantBigints[i])
	}
	if err := recordsetReader.Close(); err != nil {
		t.Fatalf("close bigints recordset reader: %v", err)
	}

	// GetMulti keeps the same classes for every row.
	records := make([]record.Record, len(fixtureMixedRows))
	for i, row := range fixtureMixedRows {
		records[i] = record.NewRecordWithData(record.NewKeyWithID("mixed", row.id), make(map[string]any))
	}
	if err := db.GetMulti(ctx, records); err != nil {
		t.Fatalf("GetMulti: %v", err)
	}
	for i, row := range fixtureMixedRows {
		data = records[i].Data().(map[string]any)
		requireNativeValue(t, "mixed id", data["id"], row.id)
		requireNativeValue(t, "mixed declared_integer", data["declared_integer"], row.declaredInteger)
		requireNativeValue(t, "mixed declared_text", data["declared_text"], row.declaredText)
		if !records[i].Exists() {
			t.Errorf("mixed row %d is not marked as read", row.id)
		}
	}

	// A returned []byte is the caller's: mutating it changes neither another
	// read nor the stored value. dalgo2sql copies the driver's buffer; this
	// holds the adapter to that ownership at its own boundary.
	blob := records[3].Data().(map[string]any)["declared_integer"].([]byte)
	blob[0] = 0x7f
	rec = record.NewRecordWithData(record.NewKeyWithID("mixed", 4), make(map[string]any))
	if err := db.Get(ctx, rec); err != nil {
		t.Fatalf("Get(mixed 4) after mutation: %v", err)
	}
	requireNativeValue(t, "copied blob", rec.Data().(map[string]any)["declared_integer"], []byte{0x0a, 0xff})

	mixed := dal.NewRootCollectionRef("mixed", "")
	mixedQuery := dal.From(&mixed).NewQuery().
		OrderBy(dal.Ascending(dal.Field("id"))).
		SelectIntoRecordset()

	// The records reader: one column, five storage classes.
	recordsReader, err = db.ExecuteQueryToRecordsReader(ctx, mixedQuery)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(mixed): %v", err)
	}
	mixedRecords := readAllRecordValues(t, recordsReader)
	if err := recordsReader.Close(); err != nil {
		t.Fatalf("close mixed records reader: %v", err)
	}
	if len(mixedRecords) != len(fixtureMixedRows) {
		t.Fatalf("mixed records = %d rows, want %d", len(mixedRecords), len(fixtureMixedRows))
	}
	for i, row := range fixtureMixedRows {
		requireNativeValue(t, "mixed records declared_integer", mixedRecords[i]["declared_integer"], row.declaredInteger)
		requireNativeValue(t, "mixed records declared_text", mixedRecords[i]["declared_text"], row.declaredText)
	}

	// The recordset reader: the same heterogeneous column, still any-typed.
	recordsetReader, err = db.ExecuteQueryToRecordsetReader(ctx, mixedQuery)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(mixed): %v", err)
	}
	mixedRows := readAllRecordsetValues(t, recordsetReader)
	if err := recordsetReader.Close(); err != nil {
		t.Fatalf("close mixed recordset reader: %v", err)
	}
	if len(mixedRows) != len(fixtureMixedRows) {
		t.Fatalf("mixed recordset = %d rows, want %d", len(mixedRows), len(fixtureMixedRows))
	}
	for i, row := range fixtureMixedRows {
		requireNativeValue(t, "mixed recordset declared_integer", mixedRows[i]["declared_integer"], row.declaredInteger)
		requireNativeValue(t, "mixed recordset declared_text", mixedRows[i]["declared_text"], row.declaredText)
	}

	// SQLite orders NULL first in ascending order. The first row's column is
	// nil and does not fix the column's type for the rows that follow.
	nullsFirst := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().
		OrderBy(dal.Ascending(dal.Field("score"))).
		SelectColumns(
			dal.Column{Expression: dal.Field("id")},
			dal.Column{Expression: dal.Field("score")},
		)
	recordsReader, err = db.ExecuteQueryToRecordsReader(ctx, nullsFirst)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(nulls first): %v", err)
	}
	first, err := recordsReader.Next()
	if err != nil {
		t.Fatalf("Next(nulls first): %v", err)
	}
	if first.Data().(map[string]any)["score"] != nil {
		t.Fatalf("first record score = %#v, want NULL", first.Data().(map[string]any)["score"])
	}
	if err := recordsReader.Close(); err != nil {
		t.Fatalf("close nulls-first records reader: %v", err)
	}
	recordsetReader, err = db.ExecuteQueryToRecordsetReader(ctx, nullsFirst)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(nulls first): %v", err)
	}
	firstRow, firstSet, err := recordsetReader.Next()
	if err != nil {
		t.Fatalf("Next(nulls first recordset): %v", err)
	}
	score, err := firstRow.GetValueByName("score", firstSet)
	if err != nil || score != nil {
		t.Fatalf("first recordset score = %#v, %v, want NULL", score, err)
	}
	if err := recordsetReader.Close(); err != nil {
		t.Fatalf("close nulls-first recordset reader: %v", err)
	}
}

func TestIntegrationMissingKeyNotFound(t *testing.T) {
	db := openFixtureDB(t)
	rec := record.NewRecordWithData(record.NewKeyWithID("items", 999_999), make(map[string]any))
	err := db.Get(context.Background(), rec)
	if !errors.Is(err, record.ErrRecordNotFound) || !record.IsNotFound(err) {
		t.Fatalf("Get(missing) error = %v, want ErrRecordNotFound", err)
	}
	// A not-found record is a state, not an error: record.Error is nil and its
	// data stays readable, as DALgo defines it.
	if rec.Error() != nil {
		t.Fatalf("record error = %v", rec.Error())
	}
	exists, err := db.Exists(context.Background(), record.NewKeyWithID("items", 1))
	if err != nil || !exists {
		t.Fatalf("Exists = %v, %v", exists, err)
	}
	exists, err = db.Exists(context.Background(), record.NewKeyWithID("items", 999_999))
	if err != nil || exists {
		t.Fatalf("Exists(missing) = %v, %v", exists, err)
	}
}

func TestIntegrationStructuredReadsStayStable(t *testing.T) {
	db := openFixtureDB(t)
	collection := dal.NewRootCollectionRef("items", "")
	query := dal.NewQueryBuilder(dal.From(&collection)).
		WhereField("rank", dal.Equal, int64(1)).
		OrderBy(dal.Ascending(dal.Field("id"))).
		Limit(1).
		Offset(1).
		SelectColumns(
			dal.Column{Expression: dal.Field("id")},
			dal.Column{Expression: dal.Field("title")},
		)

	read := func() {
		t.Helper()
		reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), query)
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
		}
		defer func() { _ = reader.Close() }()
		row, rs, err := reader.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		id, err := row.GetValueByName("id", rs)
		if err != nil || id != int64(6) {
			t.Fatalf("id = %#v, %v", id, err)
		}
		title, err := row.GetValueByName("title", rs)
		if err != nil || title != "Zeta" {
			t.Fatalf("title = %#v, %v", title, err)
		}
		if _, _, err := reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
			t.Fatalf("second Next error = %v", err)
		}
	}
	read()
	read()

	// A float and a blob survive through a recordset read of the real server,
	// not only through a record.
	scalar := dal.NewQueryBuilder(dal.From(&collection)).
		WhereField("id", dal.Equal, int64(1)).
		SelectColumns(
			dal.Column{Expression: dal.Field("id")},
			dal.Column{Expression: dal.Field("score")},
			dal.Column{Expression: dal.Field("payload")},
		)
	scalarReader, err := db.ExecuteQueryToRecordsetReader(context.Background(), scalar)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(scalar): %v", err)
	}
	scalarRow, scalarSet, err := scalarReader.Next()
	if err != nil {
		t.Fatalf("Next(scalar): %v", err)
	}
	if score, err := scalarRow.GetValueByName("score", scalarSet); err != nil || score != 9.5 {
		t.Fatalf("recordset score = %#v, %v", score, err)
	}
	if payload, err := scalarRow.GetValueByName("payload", scalarSet); err != nil || !reflect.DeepEqual(payload, []byte{0x00, 0xff, 0x10}) {
		t.Fatalf("recordset payload = %#v, %v", payload, err)
	}
	if err := scalarReader.Close(); err != nil {
		t.Fatalf("close scalar reader: %v", err)
	}

	// Fractions and their order survive a full scan of a REAL primary key.
	metrics := dal.NewRootCollectionRef("metrics", "")
	ratioQuery := dal.NewQueryBuilder(dal.From(&metrics)).
		OrderBy(dal.Ascending(dal.Field("ratio"))).
		SelectIntoRecordset()
	reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), ratioQuery)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(metrics): %v", err)
	}
	defer func() { _ = reader.Close() }()
	var ratios []float64
	for {
		row, rs, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatalf("Next(metrics): %v", err)
		}
		value, err := row.GetValueByName("ratio", rs)
		if err != nil {
			t.Fatalf("ratio: %v", err)
		}
		ratio, ok := value.(float64)
		if !ok {
			t.Fatalf("ratio = %#v", value)
		}
		ratios = append(ratios, ratio)
	}
	want := []float64{0.1, 0.2, 0.30000000000000004, 1.5}
	if !reflect.DeepEqual(ratios, want) {
		t.Fatalf("ratios = %#v, want %#v", ratios, want)
	}
}

// TestIntegrationBorrowedSchema runs all five schema-reader methods with the
// fixture's a:"ro" token. The foreign-key assertion is real: it reads a table
// that declares a foreign key, so the reader's enforcement query and its
// foreign-key metadata path both actually run. A read-only token is refused a
// bare PRAGMA with 403, so a passing DescribeCollection also proves the reader
// used the table-valued function.
func TestIntegrationBorrowedSchema(t *testing.T) {
	db := openFixtureDB(t)
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("the backend does not expose the borrowed schema reader")
	}
	ctx := context.Background()
	collections, err := reader.ListCollections(ctx, nil)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	found := map[string]bool{}
	for _, collection := range collections {
		found[collection.Name()] = true
	}
	for _, name := range []string{"items", "labels", "composite", "metrics", "bigints", "fk_parent", "fk_child"} {
		if !found[name] {
			t.Errorf("collection %q is missing from %#v", name, found)
		}
	}

	itemsRef := dal.NewRootCollectionRef("items", "")
	def, err := reader.DescribeCollection(ctx, &itemsRef)
	if err != nil {
		t.Fatalf("DescribeCollection(items): %v", err)
	}
	if len(def.PrimaryKey) != 1 || string(def.PrimaryKey[0]) != "id" {
		t.Fatalf("primary key = %#v", def.PrimaryKey)
	}

	childRef := dal.NewRootCollectionRef("fk_child", "")
	child, err := reader.DescribeCollection(ctx, &childRef)
	if err != nil {
		t.Fatalf("DescribeCollection(fk_child): %v", err)
	}
	if len(child.ForeignKeys) != 1 {
		t.Fatalf("foreign keys = %+v", child.ForeignKeys)
	}
	fk := child.ForeignKeys[0]
	if fk.ReferencedCollection != "fk_parent" || len(fk.Fields) != 1 || fk.Fields[0] != "parent_id" ||
		len(fk.ReferencedFields) != 1 || fk.ReferencedFields[0] != "id" {
		t.Fatalf("foreign key = %+v", fk)
	}
	if fk.OnUpdate != "CASCADE" || fk.OnDelete != "RESTRICT" {
		t.Fatalf("referential actions = %+v", fk)
	}
	// The pinned libSQL server enables foreign-key enforcement by default.
	// The value is what the reader's pragma_foreign_keys query returned; a
	// bare PRAGMA would have been refused for this token.
	if fk.Enforcement != dbschema.ForeignKeyEnforcementEnabled {
		t.Fatalf("enforcement = %q, want the pinned server's enabled state", fk.Enforcement)
	}

	labelsRef := dal.NewRootCollectionRef("labels", "")
	indexes, err := reader.ListIndexes(ctx, &labelsRef)
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	unique := false
	for _, index := range indexes {
		if index.Name == "labels_label_u" && index.Unique {
			unique = true
		}
	}
	if !unique {
		t.Fatalf("the unique index is missing from %#v", indexes)
	}

	constraints, err := reader.ListConstraints(ctx, &childRef)
	if err != nil {
		t.Fatalf("ListConstraints(fk_child): %v", err)
	}
	types := map[string]bool{}
	for _, constraint := range constraints {
		types[constraint.Type] = true
	}
	if !types["primary-key"] || !types["foreign-key"] {
		t.Fatalf("constraints = %+v", constraints)
	}
	parentRef := dal.NewRootCollectionRef("fk_parent", "")
	referrers, err := reader.ListReferrers(ctx, &parentRef)
	if err != nil {
		t.Fatalf("ListReferrers(fk_parent): %v", err)
	}
	foundReferrer := false
	for _, referrer := range referrers {
		if referrer.Collection.Name() == "fk_child" && len(referrer.Fields) == 1 && referrer.Fields[0] == "parent_id" {
			foundReferrer = true
		}
	}
	if !foundReferrer {
		t.Fatalf("referrers = %+v", referrers)
	}
}

func TestIntegrationRefusesWritesAndTextSQL(t *testing.T) {
	db := openFixtureDB(t)
	ctx := context.Background()
	if err := db.RunReadwriteTransaction(ctx, func(context.Context, dal.ReadwriteTransaction) error {
		t.Error("the write worker must not run")
		return nil
	}); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("read-write transaction error = %v", err)
	}
	if _, err := db.ExecuteQueryToRecordsReader(ctx, dal.NewTextQuery("DELETE FROM items", nil)); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("text query error = %v", err)
	}
	// Nothing was written: all 12 seeded rows are still there.
	items := dal.NewRootCollectionRef("items", "")
	query := dal.NewQueryBuilder(dal.From(&items)).
		OrderBy(dal.Ascending(dal.Field("id"))).
		SelectKeysOnly(reflect.Int64)
	reader, err := db.ExecuteQueryToRecordsReader(ctx, query)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(keys): %v", err)
	}
	defer func() { _ = reader.Close() }()
	count := 0
	for {
		_, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		count++
	}
	if count != 12 {
		t.Fatalf("items = %d, want the 12 seeded rows", count)
	}
}

func TestIntegrationReaderTokenCannotWrite(t *testing.T) {
	url, token := fixtureURL(t)
	body := `{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"CREATE TABLE ro_probe (id INTEGER)","want_rows":false}},{"type":"close"}]}`
	req, err := http.NewRequest(http.MethodPost, url+"/v3/pipeline", strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		var payload json.RawMessage
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		t.Fatalf("reader token write status = %d, want 403", resp.StatusCode)
	}
}

// TestIntegrationAggregatesAndGroupingRunNatively holds the adapter's
// capability claim to the real server: the same aggregate, grouped and HAVING
// queries run through both readers, and DALgo plans each of them natively.
func TestIntegrationAggregatesAndGroupingRunNatively(t *testing.T) {
	db := openFixtureDB(t)
	ctx := context.Background()
	items := dal.NewRootCollectionRef("items", "")

	provider, ok := dal.BackendOf(db).(dal.QueryCapabilitiesProvider)
	if !ok {
		t.Fatal("the backend does not advertise query capabilities")
	}
	caps := provider.QueryCapabilities()

	scalar := dal.From(&items).NewQuery().SelectColumns(
		dal.CountAs(dal.Star(), "n"),
		dal.SumAs(dal.Field("score"), "sum"),
		dal.AverageAs(dal.Field("score"), "avg"),
		dal.MinAs(dal.Field("score"), "min"),
		dal.MaxAs(dal.Field("score"), "max"),
		dal.CountDistinctAs(dal.Field("score"), "n_distinct"),
		dal.SumDistinctAs(dal.Field("score"), "sum_distinct"),
		dal.AverageDistinctAs(dal.Field("score"), "avg_distinct"),
	)
	grouped := dal.From(&items).NewQuery().
		GroupBy(dal.Field("rank")).
		OrderBy(dal.Ascending(dal.Field("rank"))).
		SelectColumns(
			dal.Column{Expression: dal.Field("rank")},
			dal.CountAs(dal.Star(), "n"),
			dal.SumAs(dal.Field("score"), "sum"),
			dal.AverageAs(dal.Field("score"), "avg"),
			dal.MinAs(dal.Field("score"), "min"),
			dal.MaxAs(dal.Field("score"), "max"),
		)
	having := dal.From(&items).NewQuery().
		GroupBy(dal.Field("rank")).
		Having(dal.NewComparison(dal.Count().Expression, dal.GreaterOrEqual, dal.NewConstant(int64(3)))).
		OrderBy(dal.Ascending(dal.Field("rank"))).
		SelectColumns(
			dal.Column{Expression: dal.Field("rank")},
			dal.CountAs(dal.Star(), "n"),
		)

	for name, query := range map[string]dal.Query{"scalar": scalar, "grouped": grouped, "having": having} {
		plan, err := dal.PlanAggregation(query.(dal.StructuredQuery), caps)
		if err != nil {
			t.Fatalf("%s: PlanAggregation: %v", name, err)
		}
		if plan.Strategy != dal.AggregationNative {
			t.Fatalf("%s: strategy = %q (%s), want native", name, plan.Strategy, plan.Reason)
		}
	}

	wantScalar := map[string]any{
		"n":            int64(12),
		"sum":          float64(28.5),
		"avg":          28.5 / 7,
		"min":          0.5,
		"max":          9.5,
		"n_distinct":   int64(7),
		"sum_distinct": float64(28.5),
		"avg_distinct": 28.5 / 7,
	}
	recordsReader, err := db.ExecuteQueryToRecordsReader(ctx, scalar)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(scalar): %v", err)
	}
	scalarRows := readAllRecordValues(t, recordsReader)
	if err := recordsReader.Close(); err != nil {
		t.Fatalf("close scalar records reader: %v", err)
	}
	if len(scalarRows) != 1 {
		t.Fatalf("scalar records = %d rows, want 1", len(scalarRows))
	}
	for name, want := range wantScalar {
		requireNativeValue(t, "scalar records "+name, scalarRows[0][name], want)
	}
	recordsetReader, err := db.ExecuteQueryToRecordsetReader(ctx, scalar)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(scalar): %v", err)
	}
	scalarSets := readAllRecordsetValues(t, recordsetReader)
	if err := recordsetReader.Close(); err != nil {
		t.Fatalf("close scalar recordset reader: %v", err)
	}
	if len(scalarSets) != 1 {
		t.Fatalf("scalar recordset = %d rows, want 1", len(scalarSets))
	}
	for name, want := range wantScalar {
		requireNativeValue(t, "scalar recordset "+name, scalarSets[0][name], want)
	}

	// The NULL rank is its own group and sorts first; MIN/MAX ignore the NULL
	// scores inside each group, as SQL defines them.
	wantGroups := []map[string]any{
		{"rank": nil, "n": int64(2), "sum": 0.5, "avg": 0.5, "min": 0.5, "max": 0.5},
		{"rank": float64(1), "n": int64(3), "sum": 10.5, "avg": 5.25, "min": 3.5, "max": 7.0},
		{"rank": float64(2), "n": int64(4), "sum": 12.0, "avg": 6.0, "min": 2.5, "max": 9.5},
		{"rank": float64(3), "n": int64(3), "sum": 5.5, "avg": 2.75, "min": 1.25, "max": 4.25},
	}
	recordsReader, err = db.ExecuteQueryToRecordsReader(ctx, grouped)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(grouped): %v", err)
	}
	groupRows := readAllRecordValues(t, recordsReader)
	if err := recordsReader.Close(); err != nil {
		t.Fatalf("close grouped records reader: %v", err)
	}
	requireGroups(t, "grouped records", groupRows, wantGroups)
	recordsetReader, err = db.ExecuteQueryToRecordsetReader(ctx, grouped)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(grouped): %v", err)
	}
	groupSets := readAllRecordsetValues(t, recordsetReader)
	if err := recordsetReader.Close(); err != nil {
		t.Fatalf("close grouped recordset reader: %v", err)
	}
	requireGroups(t, "grouped recordset", groupSets, wantGroups)

	// HAVING drops the two-row NULL group: only ranks with three or more rows
	// remain, through both readers.
	wantHaving := []map[string]any{
		{"rank": float64(1), "n": int64(3)},
		{"rank": float64(2), "n": int64(4)},
		{"rank": float64(3), "n": int64(3)},
	}
	recordsReader, err = db.ExecuteQueryToRecordsReader(ctx, having)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(having): %v", err)
	}
	havingRows := readAllRecordValues(t, recordsReader)
	if err := recordsReader.Close(); err != nil {
		t.Fatalf("close having records reader: %v", err)
	}
	requireGroups(t, "having records", havingRows, wantHaving)
	recordsetReader, err = db.ExecuteQueryToRecordsetReader(ctx, having)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(having): %v", err)
	}
	havingSets := readAllRecordsetValues(t, recordsetReader)
	if err := recordsetReader.Close(); err != nil {
		t.Fatalf("close having recordset reader: %v", err)
	}
	requireGroups(t, "having recordset", havingSets, wantHaving)
}
