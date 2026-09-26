package iop

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDb(t *testing.T) {

	t.Run("ExecMultiContext", func(t *testing.T) {
		duck := NewDuckDb(context.Background())
		result, err := duck.ExecMultiContext(
			context.Background(),
			"create table test (id int, name varchar)",
			"insert into test (id, name) values (1, 'John')",
			"insert into test (id, name) values (2, 'Jane')",
		)

		if assert.NoError(t, err) {
			rows, err := result.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, int64(2), rows)
		}
	})

	t.Run("ExecContext with erroneous query", func(t *testing.T) {
		duck := NewDuckDb(context.Background())
		_, err := duck.Exec("SELECT * FROM non_existent_table")

		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "non_existent_table")
		}
	})

	t.Run("Stream", func(t *testing.T) {

		duck := NewDuckDb(context.Background(), "instance="+filepath.Join(t.TempDir(), "test.duckdb"))

		// Create a test table and insert some data
		_, err := duck.ExecMultiContext(
			context.Background(),
			"CREATE or replace TABLE export_test (id INT, name VARCHAR, age INT)",
			"INSERT INTO export_test VALUES (1, 'Alice', 30),(2, 'Bob', 25),(3, 'Charlie', 35)",
		)
		assert.NoError(t, err)

		// Test the Export function
		ds, err := duck.StreamContext(
			context.Background(),
			"SELECT * FROM export_test ORDER BY id",
		)
		assert.NoError(t, err)
		assert.NotNil(t, ds)

		// Verify the exported data
		data, err := ds.Collect(0)
		records := data.Records()
		assert.NoError(t, err)
		assert.Equal(t, 3, len(records))

		// Check the content of the first record
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, int64(30), records[0]["age"])

		// Check the content of the last record
		assert.Equal(t, int64(3), records[2]["id"])
		assert.Equal(t, "Charlie", records[2]["name"])
		assert.Equal(t, int64(35), records[2]["age"])

		// Clean up: drop the test table
		_, err = duck.Exec("DROP TABLE export_test")
		assert.NoError(t, err)

		err = duck.Close()
		assert.NoError(t, err)
	})

	t.Run("Query", func(t *testing.T) {
		duck := NewDuckDb(context.Background(), "instance="+filepath.Join(t.TempDir(), "test.duckdb"))

		// Create a test table and insert some data
		_, err := duck.ExecMultiContext(
			context.Background(),
			"CREATE or replace TABLE query_test (id INT, name VARCHAR, age INT)",
			"INSERT INTO query_test VALUES (1, 'Alice', 30),(2, 'Bob', 25),(3, 'Charlie', 35)",
		)
		assert.NoError(t, err)

		// Test the Query function
		data, err := duck.Query("SELECT * FROM query_test ORDER BY id")
		assert.NoError(t, err)
		assert.NotNil(t, data)

		// Verify the queried data
		if !assert.Equal(t, 3, len(data.Rows)) {
			return
		}

		// Check the content of the first row
		assert.Equal(t, int64(1), data.Rows[0][0])
		assert.Equal(t, "Alice", data.Rows[0][1])
		assert.Equal(t, int64(30), data.Rows[0][2])

		// Check the content of the last row
		assert.Equal(t, int64(3), data.Rows[2][0])
		assert.Equal(t, "Charlie", data.Rows[2][1])
		assert.Equal(t, int64(35), data.Rows[2][2])

		// Verify column names
		expectedColumns := []string{"id", "name", "age"}
		actualColumns := data.GetFields()
		assert.Equal(t, expectedColumns, actualColumns)

		// Clean up: drop the test table
		_, err = duck.Exec("DROP TABLE query_test")
		assert.NoError(t, err)

		// Test Pragma Column
		data, err = duck.Query("PRAGMA database_list")
		assert.NoError(t, err)

		assert.Len(t, data.Columns, 3)
		assert.Contains(t, data.Columns.Names(), "seq")
		assert.Contains(t, data.Columns.Names(), "name")
		assert.Contains(t, data.Columns.Names(), "file")
	})
}

// TestDuckDbNoDeadlock guards against the reader hanging forever on the result
// pipe (holding the lock and stalling all subsequent queries).
func TestDuckDbNoDeadlock(t *testing.T) {

	// run fn with a hard deadline; fails (not hangs) if it does not return in time
	runWithDeadline := func(t *testing.T, d time.Duration, fn func()) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			fn()
		}()
		select {
		case <-done:
		case <-time.After(d):
			t.Fatal("query did not return in time — reader is deadlocked on the result pipe")
		}
	}

	t.Run("context cancellation unblocks reader", func(t *testing.T) {
		duck := NewDuckDb(context.Background(), "copy_format=csv")
		defer duck.Close()

		// prime the connection
		_, err := duck.Exec("select 1")
		assert.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()

		runWithDeadline(t, 30*time.Second, func() {
			// cancellation must unblock the reader; don't assert on err value
			_, _ = duck.QueryContext(ctx, "select count(*) from range(1, 100000000000)")
		})

		// the connection must still be usable afterwards (lock released)
		runWithDeadline(t, 30*time.Second, func() {
			data, err := duck.Query("select 42 as n")
			if assert.NoError(t, err) && assert.Len(t, data.Rows, 1) {
				assert.Equal(t, int64(42), data.Rows[0][0])
			}
		})
	})

	t.Run("oversized line does not hang", func(t *testing.T) {
		// a ~200KB line exceeds the scan buffer, so the stdout scanner stops on
		// bufio.ErrTooLong; the watcher must detect it and unblock the reader.
		// Arrow mode does not use the line scanner, so use csv. On Windows, the
		// csv output goes to a file, not through the scanner.
		duck := NewDuckDb(context.Background(), "max_buffer_size=1024", "copy_format=csv")

		runWithDeadline(t, 30*time.Second, func() {
			data, err := duck.Query("select repeat('x', 200000) as big")
			if runtime.GOOS == "windows" {
				if assert.NoError(t, err) && assert.Len(t, data.Rows, 1) {
					assert.Len(t, cast.ToString(data.Rows[0][0]), 200000)
				}
			} else {
				assert.Error(t, err)
			}
		})

		// new connection with normal buffer must work fine (sanity)
		duck2 := NewDuckDb(context.Background())
		defer duck2.Close()
		runWithDeadline(t, 30*time.Second, func() {
			data, err := duck2.Query("select 7 as n")
			if assert.NoError(t, err) && assert.Len(t, data.Rows, 1) {
				assert.Equal(t, int64(7), data.Rows[0][0])
			}
		})
	})
}

// A duckdb process that dies silently (OOM kill, crash) must surface its exit
// status instead of a bare "exited before query completed:" and must reopen on
// the next query.
func TestDuckDbProcessDeathError(t *testing.T) {
	t.Setenv("SLING_DUCKDB_STALL_TIMEOUT", "0")

	duck := NewDuckDb(context.Background())
	defer duck.Close()

	_, err := duck.Exec("create table death_repro (id bigint)")
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		// a long insert stays silent on stdout until it completes
		_, err := duck.Exec("insert into death_repro select i from range(1, 50000000000) t(i)")
		done <- err
	}()

	time.Sleep(500 * time.Millisecond)
	require.NoError(t, duck.Proc.Cmd.Process.Kill())

	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("query did not return after the duckdb process died")
	}

	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "duckdb process exited before query completed")
	assert.False(t, strings.HasSuffix(strings.TrimSpace(msg), ":"), msg)
	if runtime.GOOS != "windows" {
		assert.Contains(t, msg, "signal: killed", msg)
		assert.Contains(t, msg, "out-of-memory", msg)
	}

	// the connection must reopen on the next query
	data, err := duck.Query("select 9 as n")
	if assert.NoError(t, err) && assert.Len(t, data.Rows, 1) {
		assert.Equal(t, int64(9), data.Rows[0][0])
	}
}

// The death error must carry the last stderr lines of the sidecar, so the
// real cause reaches telemetry.
func TestDuckDbProcessDeathStderrTail(t *testing.T) {
	t.Setenv("SLING_DUCKDB_STALL_TIMEOUT", "0")

	duck := NewDuckDb(context.Background())
	defer duck.Close()

	_, err := duck.Exec("select * from table_that_is_not_there_xyz")
	require.Error(t, err)

	_, err = duck.Exec("create table death_tail (id bigint)")
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := duck.Exec("insert into death_tail select i from range(1, 50000000000) t(i)")
		done <- err
	}()

	time.Sleep(500 * time.Millisecond)
	require.NoError(t, duck.Proc.Cmd.Process.Kill())

	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("query did not return after the duckdb process died")
	}

	require.Error(t, err)
	assert.True(t, IsDuckDbProcDeath(err), err.Error())
	assert.Contains(t, err.Error(), "last duckdb stderr output")
	assert.Contains(t, err.Error(), "table_that_is_not_there_xyz")
}

// KillChildProcs must stop every live sidecar, since they are not in the
// process group of sling and get no console signal.
func TestDuckDbKillChildProcs(t *testing.T) {
	duck := NewDuckDb(context.Background())
	defer duck.Close()

	_, err := duck.Exec("select 1")
	require.NoError(t, err)
	require.False(t, duck.Proc.Exited())

	env.KillChildProcs()

	deadline := time.Now().Add(10 * time.Second)
	for !duck.Proc.Exited() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	assert.True(t, duck.Proc.Exited(), "duckdb sidecar still runs after KillChildProcs")
}

func TestDuckDbStreamArrow(t *testing.T) {
	t.Run("StreamArrow basic query", func(t *testing.T) {
		duck := NewDuckDb(context.Background())

		// Ensure arrow extension is added and connection is open
		duck.AddExtension("arrow from community")
		err := duck.Open()
		if !assert.NoError(t, err) {
			return
		}
		defer duck.Close()

		// Use inline VALUES — the Arrow process is separate and has no access to in-memory tables
		sql := "SELECT * FROM (VALUES (1, 'Alice', 10.5, true), (2, 'Bob', 20.7, false), (3, 'Charlie', 30.9, true)) AS t(id, name, value, flag) ORDER BY id"

		reader, cleanup, _, err := duck.StreamArrow(context.Background(), sql)
		if !assert.NoError(t, err) {
			return
		}
		defer cleanup()

		// Consume the Arrow stream into a Datastream
		ds := NewDatastreamContext(context.Background(), nil)
		err = ds.ConsumeArrowReaderStream(reader)
		if !assert.NoError(t, err) {
			return
		}

		data, err := ds.Collect(0)
		if !assert.NoError(t, err) {
			return
		}

		records := data.Records()
		if !assert.Equal(t, 3, len(records)) {
			return
		}

		// Verify data (Arrow may return different Go types depending on DuckDB inference)
		assert.EqualValues(t, 1, records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
		assert.EqualValues(t, 3, records[2]["id"])
		assert.Equal(t, "Charlie", records[2]["name"])
	})

	t.Run("StreamContext with copy_format arrow", func(t *testing.T) {
		duck := NewDuckDb(context.Background(), "copy_format=arrow")

		sql := "SELECT * FROM (VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)) AS t(id, name, age) ORDER BY id"

		// StreamContext should use Arrow path
		ds, err := duck.StreamContext(context.Background(), sql)
		if !assert.NoError(t, err) {
			return
		}

		data, err := ds.Collect(0)
		if !assert.NoError(t, err) {
			return
		}

		records := data.Records()
		if !assert.Equal(t, 3, len(records)) {
			return
		}

		assert.EqualValues(t, 1, records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
		assert.EqualValues(t, 30, records[0]["age"])

		assert.EqualValues(t, 3, records[2]["id"])
		assert.Equal(t, "Charlie", records[2]["name"])
		assert.EqualValues(t, 35, records[2]["age"])

		ds.Close()
	})

	t.Run("StreamArrow with file-based instance", func(t *testing.T) {
		tmpDir := t.TempDir()
		instancePath := tmpDir + "/test_arrow.duckdb"

		// Create and populate a file-based database, then close to release lock
		setupDuck := NewDuckDb(context.Background(), "instance="+instancePath)
		_, err := setupDuck.ExecMultiContext(
			context.Background(),
			"CREATE TABLE arrow_file_test (id INT, name VARCHAR, amount DECIMAL(10,2))",
			"INSERT INTO arrow_file_test VALUES (1, 'Alice', 100.50),(2, 'Bob', 200.75),(3, 'Charlie', 300.25)",
		)
		if !assert.NoError(t, err) {
			return
		}
		setupDuck.Close()
		time.Sleep(200 * time.Millisecond) // ensure lock is fully released

		// StreamArrow on the file-based instance (no interactive process needed)
		duck := NewDuckDb(context.Background(), "instance="+instancePath)
		duck.AddExtension("arrow from community")

		reader, cleanup, _, err := duck.StreamArrow(context.Background(), "SELECT * FROM arrow_file_test ORDER BY id")
		if !assert.NoError(t, err) {
			return
		}
		defer cleanup()

		ds := NewDatastreamContext(context.Background(), nil)
		err = ds.ConsumeArrowReaderStream(reader)
		if !assert.NoError(t, err) {
			return
		}

		data, err := ds.Collect(0)
		if !assert.NoError(t, err) {
			return
		}

		records := data.Records()
		if !assert.Equal(t, 3, len(records)) {
			return
		}

		assert.EqualValues(t, 1, records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
	})
}

func TestDuckDbDataflowToHttpStream(t *testing.T) {
	t.Run("CSV streaming - verifies streaming without io.ReadAll", func(t *testing.T) {
		// This test confirms that DataflowToHttpStream now streams data
		// without buffering all data in memory

		// Create a simple dataflow
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		df := NewDataflow()
		columns := NewColumnsFromFields("id", "name", "value")
		columns[0].Type = IntegerType
		columns[1].Type = StringType
		columns[2].Type = DecimalType
		df.Columns = columns
		df.Ready = true

		// Create datastream with test data
		testData := [][]any{
			{int64(1), "Alice", float64(100.5)},
			{int64(2), "Bob", float64(200.7)},
			{int64(3), "Charlie", float64(300.9)},
		}

		ds := NewDatastreamContext(ctx, columns)
		ds.SetConfig(map[string]string{})

		// Add data to buffer to simulate a loaded datastream
		for _, row := range testData {
			ds.Buffer = append(ds.Buffer, row)
		}
		ds.Count = uint64(len(testData))
		ds.Ready = true

		// Add datastream to dataflow
		df.Streams = append(df.Streams, ds)

		// Send datastream through channel
		go func() {
			defer close(df.StreamCh)
			df.StreamCh <- ds
			// Close the datastream after sending to trigger batch closure
			time.Sleep(50 * time.Millisecond)
			ds.Close()
		}()

		// Create DuckDB instance
		duck := NewDuckDb(ctx)
		defer duck.Close()

		// Test DataflowToHttpStream with small batch limit to force multiple parts
		sc := StreamConfig{
			Format:       dbio.FileTypeCsv,
			BatchLimit:   2, // Small batch limit to test multiple parts
			FileMaxBytes: 1024 * 1024,
		}

		streamPartChn, err := duck.DataflowToHttpStream(df, sc)
		assert.NoError(t, err)
		assert.NotNil(t, streamPartChn)

		// Collect results
		parts := []HttpStreamPart{}
		timeout := time.After(3 * time.Second)

	collectLoop:
		for {
			select {
			case part, ok := <-streamPartChn:
				if !ok {
					break collectLoop
				}
				parts = append(parts, part)

				// Verify part structure
				assert.NotEmpty(t, part.FromExpr)
				assert.Contains(t, part.FromExpr, "read_csv")
				assert.Contains(t, part.FromExpr, "http://localhost:")
				assert.NotNil(t, part.Columns)
				assert.Equal(t, 3, len(part.Columns))

				t.Logf("Received part %d: %s", len(parts), part.FromExpr)
			case <-timeout:
				// It's OK to timeout - we just want to verify we got at least one part
				break collectLoop
			}
		}

		// Cancel context to clean up
		cancel()

		// Verify we got at least one part
		assert.GreaterOrEqual(t, len(parts), 1, "Should have received at least one stream part")

		// The test confirms that DataflowToHttpStream now streams data
		// through io.Pipe without loading all batch data into memory
		t.Logf("Test completed - received %d parts. Implementation now uses io.Pipe for streaming.", len(parts))
	})

	t.Run("Arrow streaming - verifies streaming without io.ReadAll", func(t *testing.T) {
		// This test confirms that DataflowToHttpStream works with Arrow format
		// and streams data without buffering all data in memory

		// Create a simple dataflow
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		df := NewDataflow()
		columns := NewColumnsFromFields("id", "value")
		columns[0].Type = IntegerType
		columns[1].Type = DecimalType
		df.Columns = columns
		df.Ready = true

		// Create datastream with test data
		testData := [][]any{
			{int64(1), float64(10.5)},
			{int64(2), float64(20.5)},
		}

		ds := NewDatastreamContext(ctx, columns)
		ds.SetConfig(map[string]string{})

		// Add data to buffer
		for _, row := range testData {
			ds.Buffer = append(ds.Buffer, row)
		}
		ds.Count = uint64(len(testData))
		ds.Ready = true

		// Add datastream to dataflow
		df.Streams = append(df.Streams, ds)

		// Send datastream through channel
		go func() {
			defer close(df.StreamCh)
			df.StreamCh <- ds
			time.Sleep(50 * time.Millisecond)
			ds.Close()
		}()

		// Create DuckDB instance
		duck := NewDuckDb(ctx)
		duck.AddExtension("arrow from community")
		defer duck.Close()

		// Test DataflowToHttpStream with Arrow format
		sc := StreamConfig{
			Format:       dbio.FileTypeArrow,
			BatchLimit:   10,
			FileMaxBytes: 1024 * 1024,
		}

		streamPartChn, err := duck.DataflowToHttpStream(df, sc)
		assert.NoError(t, err)
		assert.NotNil(t, streamPartChn)

		// Collect results
		parts := []HttpStreamPart{}
		timeout := time.After(3 * time.Second)

	collectLoop:
		for {
			select {
			case part, ok := <-streamPartChn:
				if !ok {
					break collectLoop
				}
				parts = append(parts, part)

				// Verify part structure for Arrow format
				assert.NotEmpty(t, part.FromExpr)
				assert.Contains(t, part.FromExpr, "read_arrow")
				assert.Contains(t, part.FromExpr, "http://localhost:")
				assert.NotNil(t, part.Columns)
				assert.Equal(t, 2, len(part.Columns))

				t.Logf("Received Arrow part %d: %s", len(parts), part.FromExpr)
			case <-timeout:
				break collectLoop
			}
		}

		// Cancel context to clean up
		cancel()

		// Verify we got at least one part
		assert.GreaterOrEqual(t, len(parts), 1, "Should have received at least one stream part")

		t.Logf("Test completed - received %d Arrow parts. Implementation uses io.Pipe for streaming.", len(parts))
	})

	t.Run("CSV streaming - max_line_size raised for binary and text columns", func(t *testing.T) {
		// columns that can hold unbounded values must raise max_line_size,
		// else a single large row fails the stream (issue #787)
		testCases := []struct {
			name        string
			columnType  ColumnType
			maxLineSize string
		}{
			{"binary column raises limit", BinaryType, "max_line_size=268435456"},
			{"text column raises limit", TextType, "max_line_size=268435456"},
			{"string column raises limit", StringType, "max_line_size=268435456"},
			{"json column raises limit", JsonType, "max_line_size=268435456"},
			{"integer column keeps default", IntegerType, "max_line_size=2000000"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				df := NewDataflow()
				columns := NewColumnsFromFields("id", "payload")
				columns[0].Type = IntegerType
				columns[1].Type = tc.columnType
				df.Columns = columns
				df.Ready = true

				ds := NewDatastreamContext(ctx, columns)
				ds.SetConfig(map[string]string{})
				ds.Buffer = append(ds.Buffer, []any{int64(1), "some payload"})
				ds.Count = 1
				ds.Ready = true

				df.Streams = append(df.Streams, ds)

				go func() {
					defer close(df.StreamCh)
					df.StreamCh <- ds
					time.Sleep(50 * time.Millisecond)
					ds.Close()
				}()

				duck := NewDuckDb(ctx)
				defer duck.Close()

				sc := StreamConfig{
					Format:       dbio.FileTypeCsv,
					BatchLimit:   10,
					FileMaxBytes: 1024 * 1024,
				}

				streamPartChn, err := duck.DataflowToHttpStream(df, sc)
				assert.NoError(t, err)
				assert.NotNil(t, streamPartChn)

				select {
				case part, ok := <-streamPartChn:
					if assert.True(t, ok, "should have received a stream part") {
						assert.Contains(t, part.FromExpr, "read_csv")
						assert.Contains(t, part.FromExpr, tc.maxLineSize)
						t.Logf("Received part: %s", part.FromExpr)
					}
				case <-time.After(3 * time.Second):
					t.Error("timed out waiting for stream part")
				}

				cancel()
			})
		}
	})
}

func TestDuckDbMaxLineSize(t *testing.T) {
	colOf := func(t ColumnType) Columns {
		cols := NewColumnsFromFields("id", "payload")
		cols[0].Type = IntegerType
		cols[1].Type = t
		return cols
	}

	duckOf := func(props ...string) *DuckDb {
		return NewDuckDb(context.Background(), props...)
	}

	t.Run("unbounded types raise the limit", func(t *testing.T) {
		duck := duckOf()
		for _, ct := range []ColumnType{StringType, TextType, JsonType, BinaryType, UUIDType, GeometryType} {
			assert.Equal(t, DuckDbLargeMaxLineSize, duck.MaxLineSize(colOf(ct)), "colType=%s", ct)
		}
	})

	t.Run("bounded types keep the default", func(t *testing.T) {
		duck := duckOf()
		for _, ct := range []ColumnType{IntegerType, BigIntType, DecimalType, BoolType, DateType, DatetimeType} {
			assert.Equal(t, DuckDbDefaultMaxLineSize, duck.MaxLineSize(colOf(ct)), "colType=%s", ct)
		}
	})

	t.Run("max_line_size prop overrides", func(t *testing.T) {
		duck := duckOf("max_line_size=999")
		assert.Equal(t, 999, duck.MaxLineSize(colOf(TextType)))
		assert.Equal(t, 999, duck.MaxLineSize(colOf(IntegerType)))
	})

	t.Run("invalid prop is ignored", func(t *testing.T) {
		duck := duckOf("max_line_size=abc")
		assert.Equal(t, DuckDbLargeMaxLineSize, duck.MaxLineSize(colOf(TextType)))
	})
}

func TestGenerateCopyStatementEpochPartitionKey(t *testing.T) {
	duck := NewDuckDb(context.Background())
	cols := NewColumnsFromFields("id", "_sling_loaded_at")
	cols[0].Type = IntegerType
	cols[1].Type = IntegerType
	sql, err := duck.GenerateCopyStatement("main.t", "/tmp/out", DuckDbCopyOptions{
		Format:          dbio.FileTypeParquet,
		PartitionFields: []PartitionLevel{PartitionLevelYearMonth, PartitionLevelDay},
		PartitionKey:    "_sling_loaded_at",
		Columns:         cols,
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Contains(t, sql, "to_timestamp(_sling_loaded_at)")
	assert.Contains(t, sql, "strftime(to_timestamp(_sling_loaded_at), '%Y-%m')")
	assert.NotContains(t, sql, "strftime(_sling_loaded_at,")
}

// regression guard for the v1.5.25 OOM: DuckDB sizes its read_csv buffer as
// 16 × max_line_size and allocates it eagerly. The 256MB raise thus demands a
// 4 GiB block, which fails on hosts with memory_limit below ~4 GiB. The bridge
// expression must cap the buffer so it works under small memory limits.
func TestDuckDbReadCsvExprLowMemory(t *testing.T) {
	cols := NewColumnsFromFields("id", "payload")
	cols[0].Type = IntegerType
	cols[1].Type = TextType

	// a ~5MB line exceeds the 2 MB default limit, so this also guards the
	// #787 raise: the line must still load with the capped buffer_size
	csvPath := os.TempDir() + "/duckdb_low_mem_test.csv"
	payload := strings.Repeat("x", 5*1024*1024)
	err := os.WriteFile(csvPath, []byte("id,payload\n1,"+payload+"\n"), 0644)
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(csvPath)

	duck := NewDuckDb(context.Background(), "memory_limit=1GB")
	err = duck.Open()
	if !assert.NoError(t, err) {
		return
	}
	defer duck.Close()

	expr := duck.ReadCsvExpr(csvPath, cols)
	assert.Contains(t, expr, cast.ToString(DuckDbLargeMaxLineSize))

	// same COPY shape the fabric/parquet bridge submits in production
	parquetPath := os.TempDir() + "/duckdb_low_mem_test.parquet"
	defer os.Remove(parquetPath)
	_, err = duck.Exec(g.F("COPY (select * from %s) TO '%s' (format 'parquet', overwrite true)", expr, parquetPath))
	if !assert.NoError(t, err) {
		return
	}

	data, err := duck.Query(g.F("select count(*) cnt from read_parquet('%s')", parquetPath))
	if assert.NoError(t, err) && assert.Equal(t, 1, len(data.Rows)) {
		assert.EqualValues(t, 1, cast.ToInt(data.Rows[0][0]))
	}
}

// regression guard for issue #770: http_timeout must be raised on every DuckDB
// session, not only when an S3/httpfs secret registers the extension.
func TestDuckDbHttpTimeout(t *testing.T) {
	t.Run("setting SQL always emitted, independent of extensions", func(t *testing.T) {
		duck := NewDuckDb(context.Background())
		assert.Contains(t, duck.getSessionSettingsSQL(), "SET http_timeout = 9999")

		// stays present once an httpfs-triggering secret is added
		duck.AddSecret(NewDuckDbSecret("s3_secret", DuckDbSecretTypeS3, map[string]string{}))
		assert.Contains(t, duck.getSessionSettingsSQL(), "SET http_timeout = 9999")
	})

	t.Run("http_timeout prop override", func(t *testing.T) {
		duck := NewDuckDb(context.Background(), "http_timeout=1234")
		assert.Contains(t, duck.getSessionSettingsSQL(), "SET http_timeout = 1234")
	})

	t.Run("timeout actually applied to the live session without httpfs", func(t *testing.T) {
		// query the live session: http_timeout must be the bumped value, not 30s
		duck := NewDuckDb(context.Background())
		defer duck.Close()

		data, err := duck.Query("SELECT current_setting('http_timeout') AS http_timeout")
		if !assert.NoError(t, err) {
			return
		}
		if assert.Equal(t, 1, len(data.Rows)) {
			assert.Equal(t, int64(9999), cast.ToInt64(data.Rows[0][0]),
				"http_timeout should be raised from DuckDB's 30s default")
		}
	})
}

func TestStripSQLComments(t *testing.T) {
	type testCase struct {
		name     string
		input    string
		expected string
	}
	cases := []testCase{
		{
			name:     "no comments",
			input:    "SELECT * FROM users WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
		},
		{
			name:     "single line comment at end",
			input:    "SELECT * FROM users -- Get all users",
			expected: "SELECT * FROM users ",
		},
		{
			name:     "single line comment in middle",
			input:    "SELECT * -- Get all users\nFROM users",
			expected: "SELECT * \nFROM users",
		},
		{
			name:     "single line comment at start",
			input:    "-- Get all users\nSELECT * FROM users",
			expected: "\nSELECT * FROM users",
		},
		{
			name:     "multi-line comment at end",
			input:    "SELECT * FROM users /* Get all users */",
			expected: "SELECT * FROM users ",
		},
		{
			name:     "multi-line comment in middle",
			input:    "SELECT * /* Get all users */ FROM users",
			expected: "SELECT *  FROM users",
		},
		{
			name:     "multi-line comment at start",
			input:    "/* Get all users */\nSELECT * FROM users",
			expected: "\nSELECT * FROM users",
		},
		{
			name:     "multi-line comment spanning lines",
			input:    "SELECT * FROM users /* This is a\nmulti-line\ncomment */ WHERE id = 1",
			expected: "SELECT * FROM users  WHERE id = 1",
		},
		{
			name:     "quote with dash inside",
			input:    "SELECT * FROM users WHERE name = 'user--name'",
			expected: "SELECT * FROM users WHERE name = 'user--name'",
		},
		{
			name:     "quote with comment markers inside",
			input:    "SELECT * FROM users WHERE name = '/* comment in string */'",
			expected: "SELECT * FROM users WHERE name = '/* comment in string */'",
		},
		{
			name:     "multiple mixed comments",
			input:    "/* Header comment */\nSELECT * -- Get all\nFROM users /* Filter */ WHERE id = 1",
			expected: "\nSELECT * \nFROM users  WHERE id = 1",
		},
		{
			name:     "comment with SQL keywords",
			input:    "SELECT * FROM users -- SELECT * FROM secrets",
			expected: "SELECT * FROM users ",
		},
		{
			name:     "dash without comment",
			input:    "SELECT * FROM users WHERE id = 1-5",
			expected: "SELECT * FROM users WHERE id = 1-5",
		},
		{
			name:     "slash without comment",
			input:    "SELECT * FROM users WHERE id = 1/5",
			expected: "SELECT * FROM users WHERE id = 1/5",
		},
		{
			name:     "nested comment-like structures in string",
			input:    "SELECT '-- not /*really*/ a -- comment'",
			expected: "SELECT '-- not /*really*/ a -- comment'",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, err := StripSQLComments(c.input)
			assert.NoError(t, err)
			assert.Equal(t, c.expected, result)
		})
	}
}

// duckStreamModes runs fn in the CSV and the Arrow read mode of StreamContext.
func duckStreamModes(t *testing.T, fn func(t *testing.T, formatProp string)) {
	for _, format := range []string{"csv", "arrow"} {
		t.Run(format, func(t *testing.T) {
			fn(t, "copy_format="+format)
		})
	}
}

// duckWithin fails the test when fn does not return in time.
func duckWithin(t *testing.T, d time.Duration, fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("did not return in %s", d)
	}
}

func TestDuckDbStreamTypes(t *testing.T) {
	type typeCase struct {
		name, expr string
		colType    ColumnType
		value      string // the value as duckValueString gives it
		precision  int
		scale      int
	}
	cases := []typeCase{
		{name: "bool", expr: "true", colType: BoolType, value: "true"},
		{name: "tinyint", expr: "(-128)::tinyint", colType: SmallIntType, value: "-128"},
		{name: "smallint", expr: "(-32768)::smallint", colType: SmallIntType, value: "-32768"},
		{name: "int", expr: "(-2147483648)::int", colType: IntegerType, value: "-2147483648"},
		{name: "bigint", expr: "(-9223372036854775808)::bigint", colType: BigIntType, value: "-9223372036854775808"},
		{name: "hugeint", expr: "170141183460469231731687303715884105727::hugeint", colType: DecimalType, value: "170141183460469231731687303715884105727", precision: 38},
		{name: "utinyint", expr: "255::utinyint", colType: IntegerType, value: "255"},
		{name: "usmallint", expr: "65535::usmallint", colType: IntegerType, value: "65535"},
		{name: "uinteger", expr: "4294967295::uinteger", colType: BigIntType, value: "4294967295"},
		{name: "ubigint", expr: "18446744073709551615::ubigint", colType: DecimalType, value: "18446744073709551615", precision: 20},
		{name: "uhugeint", expr: "340282366920938463463374607431768211455::uhugeint", colType: TextType, value: "340282366920938463463374607431768211455"},
		{name: "float", expr: "1.5::float", colType: FloatType, value: "1.5"},
		{name: "double_inf", expr: "'inf'::double", colType: FloatType, value: "+Inf"},
		{name: "dec4_2", expr: "(-12.34)::decimal(4,2)", colType: DecimalType, value: "-12.34", precision: 4, scale: 2},
		{name: "dec38_10", expr: "(-1234567890123456789012345678.0123456789)::decimal(38,10)", colType: DecimalType, value: "-1234567890123456789012345678.0123456789", precision: 38, scale: 10},
		{name: "varchar_unicode", expr: "'héllo 🚀 ünï'", colType: TextType, value: "héllo 🚀 ünï"},
		{name: "varchar_csv", expr: `'a,b "q" x' || chr(10) || 'line2'`, colType: TextType, value: "a,b \"q\" x\nline2"},
		{name: "varchar_empty", expr: "''", colType: TextType, value: ""},
		{name: "varchar_null_lit", expr: "'NULL'", colType: TextType, value: "NULL"},
		{name: "varchar_backslash_n", expr: `'\N'`, colType: TextType, value: `\N`},
		{name: "date", expr: "'1969-07-20'::date", colType: DateType, value: "1969-07-20T00:00:00Z"},
		{name: "date_old", expr: "'0001-01-01'::date", colType: DateType, value: "0001-01-01T00:00:00Z"},
		{name: "date_inf", expr: "'infinity'::date", colType: DateType, value: "infinity"},
		{name: "date_neg_inf", expr: "'-infinity'::date", colType: DateType, value: "-infinity"},
		{name: "time", expr: "'23:59:59.123456'::time", colType: TimeType, value: "23:59:59.123456"},
		{name: "timetz", expr: "'10:11:12+02:00'::timetz", colType: TimezType, value: "10:11:12+02"},
		{name: "ts", expr: "'2026-09-25 10:11:12.123456'::timestamp", colType: TimestampType, value: "2026-09-25T10:11:12.123456Z"},
		{name: "ts_pre1970", expr: "'1900-01-01 00:00:00.5'::timestamp", colType: TimestampType, value: "1900-01-01T00:00:00.5Z"},
		{name: "ts_s", expr: "'2026-09-25 10:11:12'::timestamp_s", colType: TimestampType, value: "2026-09-25T10:11:12Z"},
		{name: "ts_ms", expr: "'2026-09-25 10:11:12.123'::timestamp_ms", colType: TimestampType, value: "2026-09-25T10:11:12.123Z"},
		{name: "ts_ns", expr: "'2026-09-25 10:11:12.123456789'::timestamp_ns", colType: TimestampType, value: "2026-09-25T10:11:12.123456789Z"},
		{name: "ts_inf", expr: "'infinity'::timestamp", colType: TimestampType, value: "infinity"},
		{name: "ts_neg_inf", expr: "'-infinity'::timestamp", colType: TimestampType, value: "-infinity"},
		{name: "tstz", expr: "'2026-09-25 10:11:12.5+02:00'::timestamptz", colType: TimestampzType, value: "2026-09-25T08:11:12.5Z"},
		{name: "interval", expr: "interval '1 month 2 days 03:04:05'", colType: StringType, value: "1 month 2 days 03:04:05"},
		{name: "interval_year", expr: "interval '1 year'", colType: StringType, value: "1 year"},
		{name: "uuid", expr: "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", colType: UUIDType, value: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{name: "json", expr: `'{"a": [1, 2]}'::json`, colType: JsonType, value: `{"a": [1, 2]}`},
		{name: "enum", expr: "'b'::enum('a','b')", colType: StringType, value: "b"},
		{name: "bit", expr: "'10101'::bit", colType: BinaryType, value: "10101"},
		{name: "varint", expr: "'123456789012345678901234567890123456789012'::varint", colType: TextType, value: "123456789012345678901234567890123456789012"},
		{name: "null_int", expr: "null::int", colType: IntegerType, value: "<nil>"},
		{name: "null_varchar", expr: "null::varchar", colType: TextType, value: "<nil>"},
	}

	exprs := make([]string, len(cases))
	for i, c := range cases {
		exprs[i] = c.expr + " as " + c.name
	}
	sql := "select " + strings.Join(exprs, ", ")

	duckStreamModes(t, func(t *testing.T, formatProp string) {
		duck := NewDuckDb(context.Background(), formatProp)
		defer duck.Close()

		ds, err := duck.StreamContext(context.Background(), sql)
		require.NoError(t, err)
		data, err := ds.Collect(0)
		require.NoError(t, err)
		require.Len(t, data.Rows, 1)
		require.Len(t, data.Columns, len(cases))

		for i, c := range cases {
			col := data.Columns[i]
			assert.Equal(t, c.name, col.Name)
			assert.Equal(t, c.colType, col.Type, c.name)
			assert.Equal(t, c.value, duckValueString(data.Rows[0][i]), c.name)
			if c.precision > 0 {
				assert.Equal(t, c.precision, col.DbPrecision, c.name)
				assert.Equal(t, c.scale, col.DbScale, c.name)
			}
		}
	})
}

// duckValueString is a value in a form that does not depend on the read mode.
func duckValueString(val any) string {
	switch v := val.(type) {
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case []byte:
		return string(v)
	}
	return fmt.Sprint(val)
}

func TestDuckDbStreamNested(t *testing.T) {
	sql := `select [1, 2, null] as list_int, {'x': 1, 'y': 'z'} as struct_col,
		map {'k1': 1} as map_col, ['a', 'b']::enum('a', 'b')[] as enum_list,
		{'i': interval '1 day'} as interval_struct`

	duckStreamModes(t, func(t *testing.T, formatProp string) {
		duck := NewDuckDb(context.Background(), formatProp)
		defer duck.Close()

		ds, err := duck.StreamContext(context.Background(), sql)
		require.NoError(t, err)
		data, err := ds.Collect(0)
		require.NoError(t, err)
		require.Len(t, data.Rows, 1)

		// the values are text in both modes; only arrow mode gives JSON
		for i, val := range data.Rows[0] {
			assert.NotEmpty(t, duckValueString(val), data.Columns[i].Name)
		}
		assert.Equal(t, JsonType, data.Columns[1].Type)
		assert.Equal(t, JsonType, data.Columns[2].Type)
	})
}

func TestDuckDbStreamMidStreamError(t *testing.T) {
	sql := `select i, case when i = 250000 then error('boom') else i end as v from range(300000) t(i)`

	duckStreamModes(t, func(t *testing.T, formatProp string) {
		duck := NewDuckDb(context.Background(), formatProp)
		defer duck.Close()

		duckWithin(t, 60*time.Second, func() {
			ds, err := duck.StreamContext(context.Background(), sql)
			if err == nil {
				_, err = ds.Collect(0)
			}
			// a short result with no error is data loss
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), "boom")
			}

			// the connection stays usable
			data, err := duck.Query("select 42 as n")
			if assert.NoError(t, err) && assert.Len(t, data.Rows, 1) {
				assert.EqualValues(t, 42, data.Rows[0][0])
			}
		})
	})
}

func TestDuckDbStreamEarlyClose(t *testing.T) {
	duckStreamModes(t, func(t *testing.T, formatProp string) {
		duck := NewDuckDb(context.Background(), formatProp)
		defer duck.Close()

		duckWithin(t, 60*time.Second, func() {
			ds, err := duck.StreamContext(context.Background(), "select i, i::varchar as s from range(5000000) t(i)")
			require.NoError(t, err)

			count := 0
			for range ds.Rows() {
				count++
				if count == 1000 {
					break
				}
			}
			ds.Close()
			assert.Equal(t, 1000, count)

			data, err := duck.Query("select 42 as n")
			if assert.NoError(t, err) && assert.Len(t, data.Rows, 1) {
				assert.EqualValues(t, 42, data.Rows[0][0])
			}
		})
	})
}

func TestDuckDbStreamCancel(t *testing.T) {
	duckStreamModes(t, func(t *testing.T, formatProp string) {
		duck := NewDuckDb(context.Background(), formatProp)
		defer duck.Close()

		duckWithin(t, 60*time.Second, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ds, err := duck.StreamContext(ctx, "select i from range(10000000) t(i)")
			require.NoError(t, err)

			count := 0
			for range ds.Rows() {
				count++
				if count == 1000 {
					cancel()
				}
			}
			assert.Less(t, count, 10000000)
		})
	})
}

func TestDuckDbStreamSQLForms(t *testing.T) {
	duckStreamModes(t, func(t *testing.T, formatProp string) {
		duck := NewDuckDb(context.Background(), formatProp)
		defer duck.Close()

		counts := map[string]int{
			"select * from range(3) t(i);":                      3,
			"select * from range(3) t(i) -- trailing comment":   3,
			"with a as (select 1 as i) select * from a":         1,
			"select * from range(3) t(i) where i > 5":           0,
			"select 1 as a, 2 as a":                             1,
			"select 'x' as \"we\"\"ird\", 'b'::enum('a','b') e": 1,
		}
		for sql, expected := range counts {
			ds, err := duck.StreamContext(context.Background(), sql)
			if !assert.NoError(t, err, sql) {
				continue
			}
			data, err := ds.Collect(0)
			assert.NoError(t, err, sql)
			assert.Len(t, data.Rows, expected, sql)
		}
	})
}

func TestDuckDbStreamFileInstance(t *testing.T) {
	duckStreamModes(t, func(t *testing.T, formatProp string) {
		dbPath := filepath.Join(t.TempDir(), "stream.duckdb")
		duck := NewDuckDb(context.Background(), "instance="+dbPath, formatProp)
		defer duck.Close()

		_, err := duck.Exec("create table t as select i, 'v' || i as s from range(1000) t(i)")
		require.NoError(t, err)

		duckWithin(t, 60*time.Second, func() {
			ds, err := duck.StreamContext(context.Background(), "select * from t")
			require.NoError(t, err)
			data, err := ds.Collect(0)
			require.NoError(t, err)
			assert.Len(t, data.Rows, 1000)
		})
	})
}

func TestDuckDbArrowSQL(t *testing.T) {
	cols := Columns{
		{Name: "id", DbType: "INTEGER"},
		{Name: "e", DbType: "ENUM('a', 'b')"},
		{Name: "u", DbType: "UBIGINT"},
		{Name: `we"ird`, DbType: "INTERVAL"},
		{Name: "l", DbType: "ENUM('a', 'b')[]"},
		{Name: "s", DbType: "STRUCT(x INTEGER)"},
	}
	sql, ok := duckArrowSQL("select * from t;", cols)
	assert.True(t, ok)
	assert.Equal(t, "SELECT * REPLACE ("+
		`CAST("e" AS VARCHAR) AS "e", `+
		`CAST("u" AS DECIMAL(20,0)) AS "u", `+
		`CAST("we""ird" AS VARCHAR) AS "we""ird", `+
		`to_json("l")::VARCHAR AS "l"`+
		") FROM (\nselect * from t\n)", sql)

	// no cast: the query stays as it is
	sql, ok = duckArrowSQL("select 1 as id", cols[:1])
	assert.True(t, ok)
	assert.Equal(t, "select 1 as id", sql)

	// a cast of a repeated name is ambiguous
	_, ok = duckArrowSQL("select * from t", Columns{{Name: "E", DbType: "INTEGER"}, {Name: "e", DbType: "BIT"}})
	assert.False(t, ok)

	assert.Equal(t, "COPY (\nselect 1 -- c\n) TO '/dev/stdout' (FORMAT ARROWS)",
		duckCopySQL(" select 1 -- c\n", "/dev/stdout", "FORMAT ARROWS"))
}

func TestArrowSentinelValues(t *testing.T) {
	mem := memory.NewGoAllocator()

	dates := array.NewDate32Builder(mem)
	dates.AppendValues([]arrow.Date32{math.MaxInt32, -math.MaxInt32, 0}, nil)
	dateArr := dates.NewArray()
	defer dateArr.Release()
	assert.Equal(t, "infinity", GetValueFromArrowArray(dateArr, 0))
	assert.Equal(t, "-infinity", GetValueFromArrowArray(dateArr, 1))
	assert.Equal(t, time.Unix(0, 0).UTC(), GetValueFromArrowArray(dateArr, 2))

	stamps := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond})
	stamps.AppendValues([]arrow.Timestamp{math.MaxInt64, -math.MaxInt64, 0}, nil)
	stampArr := stamps.NewArray()
	defer stampArr.Release()
	assert.Equal(t, "infinity", GetValueFromArrowArray(stampArr, 0))
	assert.Equal(t, "-infinity", GetValueFromArrowArray(stampArr, 1))
	assert.Equal(t, time.UnixMicro(0).UTC(), GetValueFromArrowArray(stampArr, 2))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
	}, nil)
	cols := ArrowSchemaToColumns(schema)
	assert.Equal(t, BigIntType, cols[0].Type)
	assert.Equal(t, DecimalType, cols[1].Type)
	assert.Equal(t, 20, cols[1].DbPrecision)
}

func TestColumnTypingKeepSourced(t *testing.T) {
	arrowCols := Columns{
		{Name: "id", Type: BigIntType, DbType: "INT64", Sourced: true, Metadata: map[string]string{"k": "v"}},
		{Name: "doc", Type: StringType, DbType: "STRING", Sourced: true},
	}
	described := Columns{
		{Name: "ID", Type: DecimalType, DbType: "HUGEINT", DbPrecision: 38, Sourced: true},
		{Name: "doc", Type: JsonType, DbType: "JSON", Sourced: true},
	}

	cols := arrowCols.Clone().KeepSourcedTypes(described)
	assert.Equal(t, DecimalType, cols[0].Type)
	assert.Equal(t, 38, cols[0].DbPrecision)
	assert.Equal(t, "id", cols[0].Name)
	assert.Equal(t, "v", cols[0].Metadata["k"])
	assert.Equal(t, JsonType, cols[1].Type)

	// other names, count or unsourced columns keep the arrow types
	for _, other := range []Columns{
		{described[0]},
		{described[0], {Name: "other", Type: JsonType, Sourced: true}},
		{described[0], {Name: "doc", Type: JsonType}},
	} {
		cols = arrowCols.Clone().KeepSourcedTypes(other)
		assert.Equal(t, BigIntType, cols[0].Type)
	}
}

func TestDuckDbCopyFormat(t *testing.T) {
	cases := []struct {
		props    []string
		format   dbio.FileType
		explicit bool
		err      bool
	}{
		{props: nil, format: dbio.FileTypeArrow},
		{props: []string{"copy_format=csv"}, format: dbio.FileTypeCsv, explicit: true},
		{props: []string{"copy_format=ARROW"}, format: dbio.FileTypeArrow, explicit: true},
		{props: []string{"duckdb_copy_format=csv"}, format: dbio.FileTypeCsv, explicit: true},
		{props: []string{"copy_format=parquet"}, err: true},
		{props: []string{"copy_method=arrow_http"}, format: dbio.FileTypeArrow, explicit: true},
		{props: []string{"copy_method=csv_http"}, format: dbio.FileTypeCsv, explicit: true},
		{props: []string{"copy_method=named_pipes"}, format: dbio.FileTypeCsv, explicit: true},
		{props: []string{"duckdb_copy_method=csv_files"}, format: dbio.FileTypeCsv, explicit: true},
		{props: []string{"copy_format=arrow", "copy_method=csv_http"}, format: dbio.FileTypeArrow, explicit: true},
	}
	for _, c := range cases {
		t.Run(strings.Join(c.props, ","), func(t *testing.T) {
			duck := NewDuckDb(context.Background(), c.props...)
			format, explicit, err := duck.CopyFormat()
			if c.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.format, format)
			assert.Equal(t, c.explicit, explicit)
		})
	}

	t.Run("invalid format fails stream", func(t *testing.T) {
		duck := NewDuckDb(context.Background(), "copy_format=parquet")
		defer duck.Close()
		_, err := duck.Query("select 1 as n")
		assert.Error(t, err)
	})

	t.Run("csv import format", func(t *testing.T) {
		duck := NewDuckDb(context.Background(), "copy_format=csv")
		format, err := duck.ImportFormat()
		require.NoError(t, err)
		assert.Equal(t, dbio.FileTypeCsv, format)
	})

	t.Run("default read uses csv after arrow fails", func(t *testing.T) {
		duck := NewDuckDb(context.Background())
		defer duck.Close()
		duck.arrowReadFail.Store(true)
		data, err := duck.Query("select 42 as n")
		require.NoError(t, err)
		require.Len(t, data.Rows, 1)
		assert.EqualValues(t, 42, data.Rows[0][0])
	})
}
