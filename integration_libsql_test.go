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

func TestIntegrationReadsNativeValues(t *testing.T) {
	db := openFixtureDB(t)
	ctx := context.Background()

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
		if !reflect.DeepEqual(data[name], want) {
			t.Errorf("%s = %#v, want %#v", name, data[name], want)
		}
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

	// The full int64 range survives the string encoding of an integer.
	for _, id := range []int64{math.MaxInt64, math.MinInt64} {
		rec = record.NewRecordWithData(record.NewKeyWithID("bigints", id), make(map[string]any))
		if err := db.Get(ctx, rec); err != nil {
			t.Fatalf("Get(bigints %d): %v", id, err)
		}
		if got := rec.Data().(map[string]any)["id"]; got != id {
			t.Errorf("bigints id = %#v, want %d", got, id)
		}
	}

	// SQLite's dynamic typing: a TEXT value in an INTEGER-declared column is
	// read as text, not coerced to an integer.
	rec = record.NewRecordWithData(record.NewKeyWithID("mixed", 1), make(map[string]any))
	if err := db.Get(ctx, rec); err != nil {
		t.Fatalf("Get(mixed): %v", err)
	}
	if got := rec.Data().(map[string]any)["declared_integer"]; got != "text-in-integer" {
		t.Errorf("mixed.declared_integer = %#v, want the stored TEXT", got)
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

func TestIntegrationBorrowedSchema(t *testing.T) {
	db := openFixtureDB(t)
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("the backend does not expose the borrowed schema reader")
	}
	collections, err := reader.ListCollections(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	found := map[string]bool{}
	for _, collection := range collections {
		found[collection.Name()] = true
	}
	for _, name := range []string{"items", "labels", "composite", "metrics", "bigints"} {
		if !found[name] {
			t.Errorf("collection %q is missing from %#v", name, found)
		}
	}
	// dalgo2sqlite v0.5.0's DescribeCollection issues a bare `PRAGMA
	// foreign_keys`, and libsql-server 0.24.33 classifies a bare PRAGMA as a
	// write and refuses it for an `a:"ro"` token (403). The data path below
	// still uses the read-only token; this one metadata call uses the writer
	// token until dalgo2sqlite reads `pragma_foreign_keys` instead.
	rwDB, err := NewDB(context.Background(), Config{
		URL:               os.Getenv("LIBSQL_FIXTURE_URL"),
		AllowLoopbackHTTP: true,
		Headers: func(context.Context) (map[string]string, error) {
			return map[string]string{"Authorization": "Bearer " + os.Getenv("LIBSQL_RW_TOKEN")}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewDB(rw): %v", err)
	}
	defer closeDB(t, rwDB)
	rwReader, ok := dal.As[dbschema.SchemaReader](rwDB)
	if !ok {
		t.Fatal("the backend does not expose the schema reader")
	}
	ref := dal.NewRootCollectionRef("items", "")
	def, err := rwReader.DescribeCollection(context.Background(), &ref)
	if err != nil {
		t.Fatalf("DescribeCollection: %v", err)
	}
	if len(def.PrimaryKey) != 1 || string(def.PrimaryKey[0]) != "id" {
		t.Fatalf("primary key = %#v", def.PrimaryKey)
	}
	labelsRef := dal.NewRootCollectionRef("labels", "")
	indexes, err := reader.ListIndexes(context.Background(), &labelsRef)
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

func TestIntegrationAggregatesUseTheLocalPlan(t *testing.T) {
	db := openFixtureDB(t)
	items := dal.NewRootCollectionRef("items", "")
	query := dal.From(&items).NewQuery().SelectColumns(dal.Count())

	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader(count): %v", err)
	}
	rec, err := reader.Next()
	if err != nil {
		t.Fatalf("Next(count): %v", err)
	}
	data := rec.Data().(map[string]any)
	if data["COUNT(*)"] != 12 && data["COUNT(*)"] != int64(12) {
		t.Fatalf("record count = %#v", data)
	}
	_ = reader.Close()

	rsReader, err := db.ExecuteQueryToRecordsetReader(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader(count): %v", err)
	}
	defer func() { _ = rsReader.Close() }()
	row, rs, err := rsReader.Next()
	if err != nil {
		t.Fatalf("Next(count recordset): %v", err)
	}
	name := rs.Columns()[0].Name()
	count, err := row.GetValueByName(name, rs)
	if err != nil {
		t.Fatalf("count value: %v", err)
	}
	if count != 12 && count != int64(12) {
		t.Fatalf("recordset count = %#v", count)
	}
}
