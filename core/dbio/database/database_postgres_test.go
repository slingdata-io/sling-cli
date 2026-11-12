package database

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"iter"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenPgxTunnelVerifyFull checks verify-full through a tunnel on 127.0.0.1,
// with a certificate that has only a DNS SAN (GitHub issue #733).
func TestOpenPgxTunnelVerifyFull(t *testing.T) {
	cert, certPEM, _ := newTestCert(t, "pgtls")
	rootCert := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(rootCert, certPEM, 0600))
	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}}

	// handshake accepts one connection, answers the SSLRequest and gives the TLS result
	handshake := func(ln net.Listener) (serverName string, err error) {
		c, err := ln.Accept()
		if err != nil {
			return "", err
		}
		defer c.Close()
		if _, err = io.ReadFull(c, make([]byte, 8)); err != nil {
			return "", err
		}
		if _, err = c.Write([]byte("S")); err != nil {
			return "", err
		}
		tc := tls.Server(c, serverTLS)
		err = tc.Handshake()
		return tc.ConnectionState().ServerName, err
	}

	for _, tt := range []struct {
		name    string
		tlsHost string
		ok      bool
	}{
		{"original host", "pgtls", true},
		{"tunnel address", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer ln.Close()

			type result struct {
				serverName string
				err        error
			}
			done := make(chan result, 1)
			go func() {
				name, err := handshake(ln)
				done <- result{name, err}
			}()

			connURL := "postgresql://u:p@" + ln.Addr().String() + "/db?sslmode=verify-full&connect_timeout=5&sslrootcert=" + rootCert
			db, err := openPgx(connURL, tt.tlsHost)
			require.NoError(t, err)
			defer db.Close()
			_ = db.Ping() // the fake server stops after the handshake

			res := <-done
			if tt.ok {
				assert.NoError(t, res.err)
				assert.Equal(t, "pgtls", res.serverName)
			} else {
				assert.Error(t, res.err, "client must reject a certificate without the 127.0.0.1 IP SAN")
			}
		})
	}
}

func TestPgTimeInRange(t *testing.T) {
	cases := map[string]bool{
		"00:00:00":        true,
		"23:59:59.999999": true,
		"24:00:00":        true,
		"24:00:00.000":    true,
		"24:00:01":        false,
		"100:00:00":       false,
		"838:59:59":       false,
		"-01:00:00":       false,
		"12:30":           true,
		"noon":            true, // pgx and Postgres validate other formats
	}
	for val, want := range cases {
		assert.Equal(t, want, pgTimeInRange(val), val)
	}
}

func TestPgCopyConverter(t *testing.T) {
	cols := []string{"bin", "flag", "txt", "vc", "js", "tm", "num"}
	oids := []uint32{pgtype.ByteaOID, pgtype.BoolOID, pgtype.TextOID, pgtype.VarcharOID, pgtype.JSONBOID, pgtype.TimeOID, pgtype.Int8OID}

	cv := newPgCopyConverter(cols, oids)
	row := []any{"ab", int64(1), "a\x00b", "c\x00d", `{"k":"v"}`, "30:00:00", int64(7)}
	require.NoError(t, cv.Convert(row))
	assert.Equal(t, []any{[]byte("ab"), true, "ab", "cd", `{"k":"v"}`, nil, int64(7)}, row)

	row = []any{[]byte("x"), "false", "e\x00", nil, nil, "12:00:00", "8"}
	require.NoError(t, cv.Convert(row))
	assert.Equal(t, []any{[]byte("x"), false, "e", nil, nil, "12:00:00", "8"}, row)

	assert.Equal(t, 2, cv.nulRemoved[2])
	assert.Equal(t, 1, cv.nulRemoved[3])
	assert.Equal(t, 1, cv.timeNulled[5])

	require.NoError(t, cv.Convert([]any{"short"}), "a short row")

	err := cv.Convert([]any{nil, "abc"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column flag to bool")
}

func TestPgCopySource(t *testing.T) {
	rows := func() [][]any { return [][]any{{"1", int64(1)}, {"2", int64(0)}, {"3", "abc"}, {"4", true}} }
	conv := func() *pgCopyConverter {
		return newPgCopyConverter([]string{"id", "flag"}, []uint32{pgtype.TextOID, pgtype.BoolOID})
	}

	t.Run("all rows in order", func(t *testing.T) {
		src := newPgCopySource(context.Background(), slices.Values(rows()[:2]), nil)
		src.conv = conv()
		defer src.Stop()
		require.True(t, src.Peek())
		var got [][]any
		for src.Next() {
			row, err := src.Values()
			require.NoError(t, err)
			got = append(got, row)
		}
		require.NoError(t, src.Err())
		assert.Equal(t, [][]any{{"1", true}, {"2", false}}, got, "the peeked row comes first")
	})

	t.Run("empty", func(t *testing.T) {
		src := newPgCopySource(context.Background(), slices.Values([][]any{}), nil)
		defer src.Stop()
		assert.False(t, src.Peek())
		assert.NoError(t, src.Err())
	})

	t.Run("conversion error", func(t *testing.T) {
		src := newPgCopySource(context.Background(), slices.Values(rows()), nil)
		src.conv = conv()
		defer src.Stop()
		var err error
		for src.Next() {
			if _, err = src.Values(); err != nil {
				break
			}
		}
		require.Error(t, err)
		assert.Equal(t, err, src.Err())
		assert.False(t, src.Next())
	})

	t.Run("cancel while the pull waits", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		release := make(chan struct{})
		var aborted atomic.Int32
		blocking := func(yield func([]any) bool) {
			if yield([]any{"1"}) {
				<-release // a producer that sends no more rows
			}
		}
		src := newPgCopySource(ctx, blocking, func() { aborted.Add(1); close(release) })
		require.True(t, src.Next())

		done := make(chan bool)
		go func() { done <- src.Next() }()
		cancel()
		select {
		case ok := <-done:
			assert.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("Next did not return after the cancel")
		}
		assert.ErrorIs(t, src.Err(), context.Canceled)
		src.Stop()
		assert.EqualValues(t, 1, aborted.Load(), "abort runs once")
	})

	t.Run("stop before and after the end", func(t *testing.T) {
		var aborted int
		stopped := false
		seq := func(yield func([]any) bool) {
			for _, row := range rows() {
				if !yield(row) {
					stopped = true
					return
				}
			}
		}
		src := newPgCopySource(context.Background(), seq, func() { aborted++ })
		require.True(t, src.Next())
		src.Stop()
		assert.True(t, stopped, "the seq sees yield false")
		assert.Equal(t, 1, aborted)

		aborted = 0
		src = newPgCopySource(context.Background(), seq, func() { aborted++ })
		for src.Next() {
		}
		src.Stop()
		assert.Equal(t, 0, aborted, "no abort after the end")
	})

	t.Run("panic in the iterator", func(t *testing.T) {
		src := newPgCopySource(context.Background(), func(yield func([]any) bool) { panic("boom") }, nil)
		defer src.Stop()
		assert.False(t, src.Next())
		require.Error(t, src.Err())
		assert.Contains(t, src.Err().Error(), "panic in the row iterator")
	})
}

func TestPgCopyTextWriter(t *testing.T) {
	cols := []string{"id", "txt", "missing", "bin", "tz", "empty", "flag"}
	oids := []uint32{pgtype.Int8OID, pgtype.TextOID, pgtype.TextOID, pgtype.ByteaOID, 1266, pgtype.TextOID, pgtype.BoolOID}
	rows := [][]any{
		{int64(1), "a\tb\nc\\d\re", nil, []byte{0xde, 0xad}, "12:34:56+02", "", int64(1)},
	}

	src := newPgCopySource(context.Background(), slices.Values(rows), nil)
	src.conv = newPgCopyConverter(cols, oids)
	var buf bytes.Buffer
	require.NoError(t, src.WriteText(&buf, pgtype.NewMap()))
	want := "1\t" + `a\tb\nc\\d\re` + "\t" + `\N` + "\t" + `\\xdead` + "\t12:34:56+02\t\tt\n"
	assert.Equal(t, want, buf.String(), "an empty string is not NULL")

	src = newPgCopySource(context.Background(), slices.Values([][]any{{struct{ X int }{1}}}), nil)
	src.conv = newPgCopyConverter([]string{"num"}, []uint32{pgtype.Int4OID})
	err := src.WriteText(io.Discard, pgtype.NewMap())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column num")
}

// TestPgCopyStreamMemory checks that the memory of a COPY does not grow with the row count.
func TestPgCopyStreamMemory(t *testing.T) {
	const total, sample = 2_000_000, 100_000
	cols := []string{"id", "a", "b", "flag", "tm", "bin"}
	oids := []uint32{pgtype.Int8OID, pgtype.TextOID, pgtype.VarcharOID, pgtype.BoolOID, pgtype.TimeOID, pgtype.ByteaOID}

	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc

	var peak uint64
	gen := func(yield func([]any) bool) {
		for i := range total {
			if i%sample == 0 {
				runtime.ReadMemStats(&ms)
				peak = max(peak, ms.HeapAlloc)
			}
			s := fmt.Sprintf("%064d", i)
			row := []any{int64(i), s, s + "\x00", int64(i % 2), "12:00:00", s[:16]}
			if !yield(row) {
				return
			}
		}
	}
	const limit = 64 << 20 // about 300 MB if all rows stay in memory

	t.Run("binary", func(t *testing.T) {
		peak = 0
		src := newPgCopySource(context.Background(), iter.Seq[[]any](gen), nil)
		src.conv = newPgCopyConverter(cols, oids)
		defer src.Stop()
		n := 0
		for src.Next() {
			_, err := src.Values()
			require.NoError(t, err)
			n++
		}
		require.NoError(t, src.Err())
		assert.Equal(t, total, n)
		assert.Less(t, peak-min(peak, base), uint64(limit), "heap grows with the row count")
	})

	t.Run("text", func(t *testing.T) {
		peak = 0
		src := newPgCopySource(context.Background(), iter.Seq[[]any](gen), nil)
		src.conv = newPgCopyConverter(cols, oids)
		defer src.Stop()
		var cw countWriter
		require.NoError(t, src.WriteText(&cw, pgtype.NewMap()))
		assert.Equal(t, total, cw.lines)
		assert.Less(t, peak-min(peak, base), uint64(limit), "heap grows with the row count")
	})
}

type countWriter struct{ lines int }

func (w *countWriter) Write(p []byte) (int, error) {
	w.lines += bytes.Count(p, []byte{'\n'})
	return len(p), nil
}

// TestPgCopySourceBatchAbort checks that a cancel ends a pull that waits on a real batch,
// while the consumer holds the stream mutex, as bulkImportStreamPgx does during the COPY.
func TestPgCopySourceBatchAbort(t *testing.T) {
	const fed = 1000
	feed := make(chan []any)
	nextFunc := func(it *iop.Iterator) bool {
		row, ok := <-feed
		it.Row = row
		return ok
	}
	ds := iop.NewDatastreamIt(context.Background(), iop.NewColumnsFromFields("id"), nextFunc)
	ds.Inferred = true
	go func() {
		for i := range fed {
			feed <- []any{int64(i)}
		}
	}()
	require.NoError(t, ds.Start())
	batch := <-ds.BatchChan

	ctx, cancel := context.WithCancel(context.Background())
	ds.Context.Mux.Lock()
	src := newPgCopySource(ctx, batch.Rows(), batch.Close)

	var got atomic.Int64
	pulled := make(chan struct{})
	go func() {
		defer close(pulled)
		for src.Next() {
			got.Add(1)
		}
	}()

	// the feed stops, so the pull waits for the producer
	require.Eventually(t, func() bool { return got.Load() == fed }, 5*time.Second, 10*time.Millisecond)
	cancel()

	select {
	case <-pulled:
	case <-time.After(5 * time.Second):
		t.Fatal("the pull did not end after the cancel")
	}
	src.Stop()
	ds.Context.Mux.Unlock()
	close(feed)
	assert.ErrorIs(t, src.Err(), context.Canceled)
	assert.True(t, batch.Closed())
}
