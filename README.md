# dalgo2libsql

A read-only [DALgo](https://github.com/dal-go/dalgo) adapter for an **existing**
libSQL database, speaking the official [Hrana 3](https://github.com/tursodatabase/libsql/blob/d6c75af6353bb1c34985399608e37cd272a35aa1/docs/HRANA_3_SPEC.md)
HTTP pipeline directly. No account, no management API, no migrations.

## What it supports

- `Get`, `Exists`, `GetMulti` through a primary key declared in
  `Config.Recordsets`; the declaration is validated against the live catalog
  before a statement is sent. The key is never assumed to be `id`. A keyed read
  needs **map record data** (`record.NewRecordWithData(key, map[string]any{})`),
  because the delegate keeps native values and a struct target is refused
  before a statement.
- Structured read-only queries, via the published `dalgo2sql` SQLite compiler:
  projection, filter, `ORDER BY`, `LIMIT`/`OFFSET`, `GROUP BY`, `HAVING`,
  `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` and their `DISTINCT` forms, through record and
  recordset readers. `QueryCapabilities` advertises exactly this set, so DALgo
  plans those reads natively instead of using its local hash fallback.
  `FIRST`/`LAST` and ordered aggregates are refused by the SQLite compiler and
  are not advertised.
- Native SQLite values through **both** readers and through `Get`/`GetMulti`:
  `int64`, `float64`, `string`, `[]byte` (copied before the driver reuses its
  buffer) and `nil` for `NULL`, with no affinity conversion. One column may hold
  different classes in different rows, and a first row that is `NULL` does not
  fix the column's type. The full `int64` range, including values above 2^53,
  survives the decimal string encoding.
- The five borrowed `dbschema.SchemaReader` methods from the published
  `dalgo2sqlite` package, with a read-only token. That includes real
  foreign-key metadata and enforcement state: the reader queries
  `pragma_foreign_keys` (a read-only token is refused a bare `PRAGMA` with 403).
- `Close`, idempotently.

Writes, raw text SQL, joins, opaque cursors, and read/write transactions are
refused explicitly, before any I/O. `RunReadonlyTransaction` is not
implemented yet: a real read-only transaction needs a stateful Hrana stream,
which this checkpoint does not have. Nothing pretends otherwise:
`QueryCapabilities` advertises only what the SQLite compiler actually runs and
what the reader tests prove.

## Pinned prerequisites

This checkpoint relies on two published modules, with no `replace` and no
`go.work`:

- `github.com/dal-go/dalgo2sql` **v0.31.0**, whose `DbOptions.PreserveNativeValues`
  is SQLite-only and keeps driver values as they are. It also makes an
  aggregate or expression column holdable as `any`.
- `github.com/dal-go/dalgo2sqlite` **v0.5.1**, whose schema reader reads
  foreign-key enforcement through `SELECT foreign_keys FROM pragma_foreign_keys`.

Earlier versions had the two limitations this checkpoint closes: affinity-based
recordset typing and a reader that needed a writer token for
`DescribeCollection`.

## Quick start

```go
db, err := dalgo2libsql.NewDB(ctx, dalgo2libsql.Config{
    URL: "libsql://db.example.com", // normalized to https://
    Headers: func(ctx context.Context) (map[string]string, error) {
        return map[string]string{"Authorization": "Bearer " + token}, nil
    },
    Recordsets: map[string]string{"items": "id"},
})
```

`NewDB` performs no I/O. The first read is the connection test and runs under
the caller's context. A `libsql://` URL is normalized to HTTPS; plain HTTP is
accepted only for loopback hosts with `AllowLoopbackHTTP: true`. Credentials,
queries, fragments, and API suffixes in the URL are refused.

A keyed read target is map-backed, so native values survive:

```go
rec := record.NewRecordWithData(record.NewKeyWithID("items", id), map[string]any{})
if err := db.Get(ctx, rec); err != nil { /* ... */ }
data := rec.Data().(map[string]any) // int64, float64, string, []byte or nil
```

See [`examples/readonly`](examples/readonly/main.go) for a full program.

`LIMIT`/`OFFSET` are sent to the server. A structured read can run more than
one statement (a catalog probe, then the data statement) on one connection,
but it is **not** a snapshot: two calls can see different data if the database
changes between them. Ordering is stable across calls only when the query names
an explicit `ORDER BY`; the adapter never promises a stable order without one.

## Bounds and safety

Each public call gets **one** operation deadline (`Config.Timeout`, default
30 s) and **one** cumulative budget before any preflight, header refresh, or
statement. Internal catalog probes, schema reads, and the data statement share
them. A returned reader keeps its deadline and counters until it is closed,
exhausted, or its context ends. Request and response bytes, rows, columns, value
size, arguments, statements, and the number of requests are all bounded.

Responses are decoded completely and validated before a row is returned. The
Hrana `Value` shape is enforced exactly: an integer is a decimal string, a
float is a finite JSON number, a blob is standard base64 (padded or unpadded),
`null` carries no value. A malformed result, a missing mandatory field, a
wrong-width row, or an `ok`/`error` mismatch is an error, never a silent empty
result. Server error codes are replaced unless they are on a fixed allowlist;
server messages, SQL, bound values, URLs, tokens, and transport causes are never
echoed. `context.Canceled` and `context.DeadlineExceeded` keep their identity
through every phase.

## Remaining limitations

- No writes, raw text SQL, joins, opaque cursors, or read-write transactions.
  `RunReadonlyTransaction` still needs a stateful Hrana stream.
- Keyed reads require map record data. A struct target is refused before a
  statement, because a field type could force the conversion that native-value
  preservation exists to avoid.
- Grouping follows DALgo's normalized representation: a numeric group key is
  returned as `float64`, while text and `NULL` keep their class. `SUM` and
  `AVG` return `float64`; `COUNT` returns `int64`. `MIN`/`MAX` keep the value's
  class. `FIRST`/`LAST` and ordered aggregates are not available on SQLite.
- The borrowed schema reader is the strict `dalgo2sqlite` variant: a schema a
  lossless export cannot carry is refused, not silently simplified.
  `ListConstraints` reports primary-key, foreign-key and inline UNIQUE
  constraints; an explicitly created unique index appears in `ListIndexes`.
- A structured read is not a snapshot, and order is stable only when the query
  names an `ORDER BY`.
- Reader `Next`/`Close` errors are sanitized: `io.EOF`, `dal.ErrNoMoreRecords`,
  `context.Canceled` and `context.DeadlineExceeded` keep their identity, and
  anything else becomes `ErrBackend` without server text, SQL, tokens or a
  transport cause. A broken stream is never reported as a shorter, successful
  result.
- This adapter checkpoint is not evidence for CLI/web consumer support or for
  hosted Turso. The integration suite runs the pinned local server only.

## Real libSQL fixture

The integration suite runs against one disposable container of the
commit-pinned image
`ghcr.io/tursodatabase/libsql-server@sha256:6dd3eb27…` (source
`d6c75af6353bb1c34985399608e37cd272a35aa1`, ARM64 and AMD64 manifests checked
before start). Per run it generates an ephemeral Ed25519 key pair and separate
`a:"ro"` and `a:"rw"` JWTs, starts a unique container on a random loopback
port, proves the reader token is refused a write with 403, and seeds native
fixtures: integer and text primary keys, a unique-only index, a nullable
descending integer key, a composite key, a view, fractions, blobs (including
empty), NULLs, the full `int64` range, values above 2^53, mixed storage classes
in one column, and a declared foreign key with referential actions. The child
test process receives only the reader token; the writer token seeds and stays
in the fixture script.

```sh
# whole suite against the real server
node scripts/libsql-fixture.mjs -- go test ./... -run TestIntegration -v -count=1

# start it and print the URL and tokens
node scripts/libsql-fixture.mjs

# verify the pinned multi-arch manifest without starting anything
node scripts/libsql-fixture.mjs --check-pins
```

Teardown records the exact mounts of its own container, removes only that
container and those volumes with `docker rm -f -v`, verifies they are gone, and
fails the run if anything it owns remains. Nothing is pruned globally and no
Docker configuration is touched. Local native CORS behaviour of this server is
not evidence about hosted Turso.

The Go tests skip without `LIBSQL_FIXTURE_URL`; the script sets
`LIBSQL_REQUIRE_FIXTURE=1`, so a skipped test fails a scripted run.

## Development

```sh
go test ./...
go vet ./...
golangci-lint run ./...
```
