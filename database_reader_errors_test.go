package dalgo2libsql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	dalrecord "github.com/dal-go/record"
)

// stubDelegate satisfies dal.DB by embedding the interface; only the two
// reader constructors are reached, and every other method would panic if a
// test path tried to write or read through it.
type stubDelegate struct {
	dal.DB

	recordsReader   dal.RecordsReader
	recordsetReader dal.RecordsetReader
	err             error

	readCtx context.Context
}

func (s *stubDelegate) ExecuteQueryToRecordsReader(ctx context.Context, _ dal.Query) (dal.RecordsReader, error) {
	s.readCtx = ctx
	return s.recordsReader, s.err
}

func (s *stubDelegate) ExecuteQueryToRecordsetReader(ctx context.Context, _ dal.Query, _ ...recordset.Option) (dal.RecordsetReader, error) {
	s.readCtx = ctx
	return s.recordsetReader, s.err
}

type stubRecordsReader struct {
	next     func() (dalrecord.Record, error)
	closeErr error
}

func (r *stubRecordsReader) Cursor() (string, error) { return "", nil }
func (r *stubRecordsReader) Close() error            { return r.closeErr }
func (r *stubRecordsReader) Next() (dalrecord.Record, error) {
	return r.next()
}

type stubRecordsetReader struct {
	next     func() (recordset.Row, recordset.Recordset, error)
	closeErr error
}

func (r *stubRecordsetReader) Cursor() (string, error) { return "", nil }
func (r *stubRecordsetReader) Recordset() recordset.Recordset {
	return nil
}
func (r *stubRecordsetReader) Close() error { return r.closeErr }
func (r *stubRecordsetReader) Next() (recordset.Row, recordset.Recordset, error) {
	return r.next()
}

func stubQuery() dal.Query {
	items := dal.NewRootCollectionRef("items", "")
	return dal.From(&items).NewQuery().SelectIntoRecordset()
}

func newStubDatabase(t *testing.T, delegate dal.DB) *database {
	t.Helper()
	return &database{
		delegate: delegate,
		timeout:  time.Minute,
	}
}

func requireOperationEnded(t *testing.T, delegate *stubDelegate) {
	t.Helper()
	if delegate.readCtx == nil {
		t.Fatal("the delegate never received the operation context")
	}
	select {
	case <-delegate.readCtx.Done():
	default:
		t.Fatal("the operation's deadline was not released")
	}
}

func requireSanitizedFailure(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("a failed read or close was reported as success")
	}
	if !errors.Is(err, ErrBackend) {
		t.Fatalf("error = %v, want ErrBackend", err)
	}
	for _, secret := range []string{"super-secret", "SELECT secret", "Bearer", "https://db.example.com"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error echoes %q: %v", secret, err)
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("a failed read looks like exhaustion: %v", err)
	}
}

func TestBoundedRecordsReaderNormalizesFailures(t *testing.T) {
	external := errors.New(`HTTP 500: SELECT secret FROM tokens; Authorization: Bearer super-secret at https://db.example.com`)

	t.Run("Next sanitizes an external error and releases the operation", func(t *testing.T) {
		delegate := &stubDelegate{recordsReader: &stubRecordsReader{
			next: func() (dalrecord.Record, error) { return nil, external },
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
		}
		_, nextErr := reader.Next()
		requireSanitizedFailure(t, nextErr)
		requireOperationEnded(t, delegate)
		if err := reader.Close(); err != nil {
			t.Fatalf("Close after a failed Next = %v, want nil", err)
		}
	})

	t.Run("Next keeps the end of the result", func(t *testing.T) {
		for name, end := range map[string]error{
			"dal.ErrNoMoreRecords": dal.ErrNoMoreRecords,
			"io.EOF":               io.EOF,
		} {
			t.Run(name, func(t *testing.T) {
				delegate := &stubDelegate{recordsReader: &stubRecordsReader{
					next: func() (dalrecord.Record, error) { return nil, end },
				}}
				db := newStubDatabase(t, delegate)
				reader, err := db.ExecuteQueryToRecordsReader(context.Background(), stubQuery())
				if err != nil {
					t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
				}
				_, nextErr := reader.Next()
				if name == "dal.ErrNoMoreRecords" {
					if !errors.Is(nextErr, dal.ErrNoMoreRecords) {
						t.Fatalf("error = %v, want ErrNoMoreRecords", nextErr)
					}
				} else if nextErr != io.EOF {
					t.Fatalf("error = %v, want the exact io.EOF", nextErr)
				}
				if errors.Is(nextErr, ErrBackend) {
					t.Fatalf("the end of a result was replaced: %v", nextErr)
				}
				requireOperationEnded(t, delegate)
				_ = reader.Close()
			})
		}
	})

	t.Run("a wrapped io.EOF failure is not exhaustion", func(t *testing.T) {
		delegate := &stubDelegate{recordsReader: &stubRecordsReader{
			next: func() (dalrecord.Record, error) {
				return nil, fmt.Errorf("read: %w", io.EOF)
			},
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
		}
		if _, nextErr := reader.Next(); !errors.Is(nextErr, ErrBackend) {
			t.Fatalf("error = %v, want ErrBackend, not exhaustion", nextErr)
		}
		requireOperationEnded(t, delegate)
		_ = reader.Close()
	})

	t.Run("dal.ErrLimitReached is not exhaustion", func(t *testing.T) {
		delegate := &stubDelegate{recordsReader: &stubRecordsReader{
			next: func() (dalrecord.Record, error) { return nil, dal.ErrLimitReached },
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
		}
		if _, nextErr := reader.Next(); !errors.Is(nextErr, ErrBackend) {
			t.Fatalf("error = %v, want ErrBackend, not exhaustion", nextErr)
		}
		requireOperationEnded(t, delegate)
		_ = reader.Close()
	})

	t.Run("Next keeps context identity and releases the operation", func(t *testing.T) {
		for name, contextErr := range map[string]error{
			"canceled":          context.Canceled,
			"deadline exceeded": context.DeadlineExceeded,
		} {
			t.Run(name, func(t *testing.T) {
				delegate := &stubDelegate{recordsReader: &stubRecordsReader{
					next: func() (dalrecord.Record, error) { return nil, contextErr },
				}}
				db := newStubDatabase(t, delegate)
				reader, err := db.ExecuteQueryToRecordsReader(context.Background(), stubQuery())
				if err != nil {
					t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
				}
				if _, nextErr := reader.Next(); !errors.Is(nextErr, contextErr) {
					t.Fatalf("error = %v, want %v", nextErr, contextErr)
				}
				requireOperationEnded(t, delegate)
				_ = reader.Close()
			})
		}
	})

	t.Run("Close sanitizes an external error and releases the operation", func(t *testing.T) {
		delegate := &stubDelegate{recordsReader: &stubRecordsReader{
			next:     func() (dalrecord.Record, error) { return nil, dal.ErrNoMoreRecords },
			closeErr: external,
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
		}
		requireSanitizedFailure(t, reader.Close())
		requireOperationEnded(t, delegate)
	})

	t.Run("Close keeps context identity", func(t *testing.T) {
		delegate := &stubDelegate{recordsReader: &stubRecordsReader{
			next:     func() (dalrecord.Record, error) { return nil, dal.ErrNoMoreRecords },
			closeErr: context.DeadlineExceeded,
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
		}
		if closeErr := reader.Close(); !errors.Is(closeErr, context.DeadlineExceeded) {
			t.Fatalf("Close error = %v, want the deadline identity", closeErr)
		}
		requireOperationEnded(t, delegate)
	})
}

func TestBoundedRecordsetReaderNormalizesFailures(t *testing.T) {
	external := errors.New(`HTTP 500: SELECT secret FROM tokens; Authorization: Bearer super-secret at https://db.example.com`)

	t.Run("Next sanitizes an external error and releases the operation", func(t *testing.T) {
		delegate := &stubDelegate{recordsetReader: &stubRecordsetReader{
			next: func() (recordset.Row, recordset.Recordset, error) { return nil, nil, external },
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
		}
		_, _, nextErr := reader.Next()
		requireSanitizedFailure(t, nextErr)
		requireOperationEnded(t, delegate)
		if err := reader.Close(); err != nil {
			t.Fatalf("Close after a failed Next = %v, want nil", err)
		}
	})

	t.Run("Next keeps the end of the result", func(t *testing.T) {
		for name, end := range map[string]error{
			"dal.ErrNoMoreRecords": dal.ErrNoMoreRecords,
			"io.EOF":               io.EOF,
		} {
			t.Run(name, func(t *testing.T) {
				delegate := &stubDelegate{recordsetReader: &stubRecordsetReader{
					next: func() (recordset.Row, recordset.Recordset, error) { return nil, nil, end },
				}}
				db := newStubDatabase(t, delegate)
				reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), stubQuery())
				if err != nil {
					t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
				}
				_, _, nextErr := reader.Next()
				if name == "dal.ErrNoMoreRecords" {
					if !errors.Is(nextErr, dal.ErrNoMoreRecords) {
						t.Fatalf("error = %v, want ErrNoMoreRecords", nextErr)
					}
				} else if nextErr != io.EOF {
					t.Fatalf("error = %v, want the exact io.EOF", nextErr)
				}
				if errors.Is(nextErr, ErrBackend) {
					t.Fatalf("the end of a result was replaced: %v", nextErr)
				}
				requireOperationEnded(t, delegate)
				_ = reader.Close()
			})
		}
	})

	t.Run("a wrapped io.EOF failure is not exhaustion", func(t *testing.T) {
		delegate := &stubDelegate{recordsetReader: &stubRecordsetReader{
			next: func() (recordset.Row, recordset.Recordset, error) {
				return nil, nil, fmt.Errorf("stream: %w", io.EOF)
			},
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
		}
		if _, _, nextErr := reader.Next(); !errors.Is(nextErr, ErrBackend) {
			t.Fatalf("error = %v, want ErrBackend, not exhaustion", nextErr)
		}
		requireOperationEnded(t, delegate)
		_ = reader.Close()
	})

	t.Run("dal.ErrLimitReached is not exhaustion", func(t *testing.T) {
		delegate := &stubDelegate{recordsetReader: &stubRecordsetReader{
			next: func() (recordset.Row, recordset.Recordset, error) { return nil, nil, dal.ErrLimitReached },
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
		}
		if _, _, nextErr := reader.Next(); !errors.Is(nextErr, ErrBackend) {
			t.Fatalf("error = %v, want ErrBackend, not exhaustion", nextErr)
		}
		requireOperationEnded(t, delegate)
		_ = reader.Close()
	})

	t.Run("Next keeps context identity and releases the operation", func(t *testing.T) {
		for name, contextErr := range map[string]error{
			"canceled":          context.Canceled,
			"deadline exceeded": context.DeadlineExceeded,
		} {
			t.Run(name, func(t *testing.T) {
				delegate := &stubDelegate{recordsetReader: &stubRecordsetReader{
					next: func() (recordset.Row, recordset.Recordset, error) { return nil, nil, contextErr },
				}}
				db := newStubDatabase(t, delegate)
				reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), stubQuery())
				if err != nil {
					t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
				}
				if _, _, nextErr := reader.Next(); !errors.Is(nextErr, contextErr) {
					t.Fatalf("error = %v, want %v", nextErr, contextErr)
				}
				requireOperationEnded(t, delegate)
				_ = reader.Close()
			})
		}
	})

	t.Run("Close sanitizes an external error and releases the operation", func(t *testing.T) {
		delegate := &stubDelegate{recordsetReader: &stubRecordsetReader{
			next:     func() (recordset.Row, recordset.Recordset, error) { return nil, nil, dal.ErrNoMoreRecords },
			closeErr: external,
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
		}
		requireSanitizedFailure(t, reader.Close())
		requireOperationEnded(t, delegate)
	})

	t.Run("Close keeps context identity", func(t *testing.T) {
		delegate := &stubDelegate{recordsetReader: &stubRecordsetReader{
			next:     func() (recordset.Row, recordset.Recordset, error) { return nil, nil, dal.ErrNoMoreRecords },
			closeErr: context.Canceled,
		}}
		db := newStubDatabase(t, delegate)
		reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), stubQuery())
		if err != nil {
			t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
		}
		if closeErr := reader.Close(); !errors.Is(closeErr, context.Canceled) {
			t.Fatalf("Close error = %v, want the cancellation identity", closeErr)
		}
		requireOperationEnded(t, delegate)
	})
}

func TestOperationDoneIsOnce(t *testing.T) {
	calls := 0
	op := &operation{ctx: context.Background(), cancel: func() { calls++ }}
	op.done()
	op.done()
	if calls != 1 {
		t.Fatalf("cancel calls = %d, want exactly 1", calls)
	}
	var nilOperation *operation
	nilOperation.done()
}
