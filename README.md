# dalgo2libsql

A read-only [DALgo](https://github.com/dal-go/dalgo) adapter for an **existing**
libSQL database, speaking the official [Hrana 3](https://github.com/tursodatabase/libsql/blob/d6c75af6353bb1c34985399608e37cd272a35aa1/docs/HRANA_3_SPEC.md)
HTTP pipeline directly. No account, no management API, no migrations.

## What it supports

- `Get`, `Exists`, `GetMulti` through a primary key declared in
  `Config.Recordsets`; the declaration is validated against the live catalog
  before a statement is sent. The key is never assumed to be `id`.
- Structured read-only queries, via the published `dalgo2sql` SQLite compiler:
  projection, filter, `ORDER BY`, `LIMIT`/`OFFSET`, and record and recordset
  readers. Grouping and aggregates are planned by DALgo's local strategy:
  the adapter does not claim native aggregation, because an aggregate result
  column has no declared type and the current recordset reader cannot hold it
  (see the gaps below).
- The five borrowed `dbschema.SchemaReader` methods from the published
  `dalgo2sqlite` package.
- `Close`, idempotently.

Writes, raw text SQL, joins, opaque cursors, and read/write transactions are
refused explicitly, before any I/O. `RunReadonlyTransaction` is not
implemented yet: a real read-only transaction needs a stateful Hrana stream,
which this checkpoint does not have. Nothing pretends otherwise:
`QueryCapabilities` advertises only what the SQLite compiler actually runs.

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

## Known gaps in this checkpoint

These are prerequisite changes in the published dependencies, not silent
limitations. The adapter does not claim the affected behaviour until they land.

1. **Storage classes through records and recordsets.** `dalgo2sql` v0.30.5
   builds recordset columns from `driver.RowsColumnTypeScanType`, so a column's
   declared affinity decides the Go type a row accepts. SQLite stores any
   class in any column, so the delegate either refuses the row (TEXT in an
   INTEGER-declared column) or coerces it (a blob in a TEXT-declared column
   becomes a string). The driver itself preserves the native `int64`, `float64`,
   `string`, `[]byte`, and `nil` (`TestDriverPreservesMixedStorageClasses`).
   A storage-preserving delegate needs one additive `dalgo2sql` option, for
   example `DbOptions.PreserveNativeStorageClasses`, that (a) stops the records
   reader's `[]byte`→`string` and NUMERIC normalisation, (b) makes the recordset
   reader create an `any`-typed column instead of one derived from `ScanType`
   (this also covers aggregate and expression columns, whose declared type is
   null), and (c) applies the same rule to the keyed `Get` map path. Until it
   exists, `QueryCapabilities` does not claim native grouping or aggregation;
   DALgo computes them locally and the results are correct, just not pushed
   down.

2. **`DescribeCollection` with a read-only token.** `dalgo2sqlite` v0.5.0
   checks foreign-key enforcement with a bare `PRAGMA foreign_keys`.
   `libsql-server` 0.24.33 classifies a bare `PRAGMA` as a write and answers
   403 for an `a:"ro"` JWT. The adapter's own key validation already uses the
   table-valued `pragma_table_info` / `pragma_index_list` functions, which
   read-only tokens may run, so keyed reads work with a reader token. The
   borrowed `DescribeCollection` needs the upstream reader to use
   `SELECT foreign_keys FROM pragma_foreign_keys` instead.

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
empty), NULLs, the full `int64` range, and mixed storage classes.

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
