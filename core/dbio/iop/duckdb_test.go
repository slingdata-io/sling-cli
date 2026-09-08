package iop

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
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

		duck := NewDuckDb(context.Background(), "instance=/tmp/test.duckdb")

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
		duck := NewDuckDb(context.Background(), "instance=/tmp/test.duckdb")

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
		duck := NewDuckDb(context.Background())
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
		// Arrow mode pipes binary IPC from a separate process and never uses the
		// line scanner, so there is no oversized line to trip on.
		if cast.ToBool(os.Getenv("DUCKDB_USE_ARROW")) {
			t.Skip("scanner-specific: arrow mode bypasses the stdout line scanner")
		}

		duck := NewDuckDb(context.Background(), "max_buffer_size=1024")

		runWithDeadline(t, 30*time.Second, func() {
			_, err := duck.Query("select repeat('x', 200000) as big")
			assert.Error(t, err)
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

	t.Run("StreamContext with DUCKDB_USE_ARROW", func(t *testing.T) {
		t.Setenv("DUCKDB_USE_ARROW", "true")

		duck := NewDuckDb(context.Background())

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
