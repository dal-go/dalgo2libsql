// Command readonly reads one record from an existing libSQL table through the
// dalgo2libsql adapter. It performs no writes and needs no account or
// management API: only the database's HTTPS address and a read-only token.
//
//	go run ./examples/readonly \
//	  -url https://db.example.com \
//	  -table items -key 1
//
// The token is read from LIBSQL_TOKEN, never from an argument, so it does not
// land in the shell history.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"

	dalgo2libsql "github.com/dal-go/dalgo2libsql"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "readonly:", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", os.Getenv("LIBSQL_URL"), "libSQL base URL (libsql:// or https://)")
	table := flag.String("table", "items", "table to read")
	key := flag.String("key", "1", "primary key value")
	flag.Parse()

	token := os.Getenv("LIBSQL_TOKEN")
	if token == "" {
		return fmt.Errorf("LIBSQL_TOKEN is required")
	}
	if *url == "" {
		return fmt.Errorf("-url or LIBSQL_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := dalgo2libsql.NewDB(ctx, dalgo2libsql.Config{
		URL: *url,
		Headers: func(context.Context) (map[string]string, error) {
			return map[string]string{"Authorization": "Bearer " + token}, nil
		},
		// A keyed read needs the table's single non-nullable primary key
		// column declared here; the adapter validates it against the live
		// catalog before sending a statement.
		Recordsets: map[string]string{*table: "id"},
	})
	if err != nil {
		return err
	}
	closer, ok := dal.As[io.Closer](db)
	if !ok {
		return fmt.Errorf("the database is not closable")
	}
	defer func() { _ = closer.Close() }()

	rec := record.NewRecordWithData(record.NewKeyWithID(*table, *key), make(map[string]any))
	if err := db.Get(ctx, rec); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rec.Data(), "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
