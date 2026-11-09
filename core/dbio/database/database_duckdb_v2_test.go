package database

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `~` does a full match before DuckDB 2.0 and a partial match after it.
// regexp_matches gives the same count in both versions.
func TestDuckDbConnAnalysisChars(t *testing.T) {
	for _, version := range []string{"1.5.5", "2.0.0"} {
		t.Run(version, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "chars.duckdb")
			conn, err := NewConn("duckdb://"+dbPath, "duckdb_version="+version)
			require.NoError(t, err)
			require.NoError(t, conn.Connect())
			defer conn.Close()

			_, err = conn.Exec("create table main.t as select 'a' || chr(10) || 'b' as v union all select 'c'")
			require.NoError(t, err)

			data, err := conn.RunAnalysis("chars", map[string]any{"schema": "main", "table": "t", "field": "v"})
			require.NoError(t, err)
			require.Len(t, data.Rows, 1)
			assert.Equal(t, int64(1), cast.ToInt64(data.Records()[0]["cnt_nline"]))
		})
	}
}

// DuckDB 2.0 gives a NULL-type column for CTAS of SELECT NULL.
func TestDuckDbConnNullAndVariantTypes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "types.duckdb")
	conn, err := NewConn("duckdb://" + dbPath)
	require.NoError(t, err)
	require.NoError(t, conn.Connect())
	defer conn.Close()

	_, err = conn.Exec(`create table main.t as select NULL as n, '{"a": 1}'::JSON::VARIANT as v`)
	require.NoError(t, err)

	cols, err := conn.GetColumns("main.t")
	require.NoError(t, err)
	require.Len(t, cols, 2)
	assert.Equal(t, iop.StringType, cols[0].Type)
	assert.Equal(t, iop.JsonType, cols[1].Type)
}

func TestDuckLakeConnAttachErrorRedacted(t *testing.T) {
	connStr := "host=127.0.0.1 port=1 user=sling password=secretpw_313 dbname=lake"
	conn, err := NewConn("ducklake://",
		"catalog_type=postgres",
		"catalog_conn_string="+connStr,
		"data_path="+t.TempDir(),
	)
	require.NoError(t, err)
	defer conn.Close()

	err = conn.Connect()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secretpw_313")
}

func TestDuckDbConnQuackVersionGate(t *testing.T) {
	conn, err := NewConn("duckdb://quack:localhost:9", "instance=quack:localhost:9", "duckdb_version=1.5.5")
	require.NoError(t, err)
	defer conn.Close()

	err = conn.Connect()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quack connections need DuckDB 2.0 or later (duckdb_version is 1.5.5)")
}

// startQuackServer starts a DuckDB 2.0 Quack server on a free local port. It
// returns the instance URI. The server stops at the end of the test.
func startQuackServer(t *testing.T, token string) string {
	t.Helper()
	bin, err := iop.NewDuckDb(context.Background()).EnsureBinDuckDB("2.0.0")
	require.NoError(t, err)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cmd := exec.Command(bin, "-no-agent", filepath.Join(t.TempDir(), "server.duckdb"))
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		stdin.Close()
		cmd.Process.Kill()
		cmd.Wait()
	})

	uri := fmt.Sprintf("quack:localhost:%d", port)
	_, err = io.WriteString(stdin, fmt.Sprintf("CALL quack_serve('%s', token = '%s');\n", uri, token))
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port)); err == nil {
			c.Close()
			return uri
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("quack server did not start on %s", uri)
	return ""
}

func newQuackConn(t *testing.T, uri, token string) Connection {
	t.Helper()
	conn, err := NewConn("duckdb://"+uri, "instance="+uri, "token="+token, "duckdb_version=2.0.0")
	require.NoError(t, err)
	require.NoError(t, conn.Connect())
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestQuackLive(t *testing.T) {
	uri := startQuackServer(t, "t313")

	// a wrong token fails at connect time, with no token in the error
	bad, err := NewConn("duckdb://"+uri, "instance="+uri, "token=wrong_tok", "duckdb_version=2.0.0")
	require.NoError(t, err)
	err = bad.Connect()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Authentication failed")
	assert.NotContains(t, err.Error(), "wrong_tok")
	bad.Close()

	conn := newQuackConn(t, uri, "t313")

	_, err = conn.Exec("create table main.t1 (id integer, name varchar)")
	require.NoError(t, err)

	data := iop.NewDataset(iop.NewColumnsFromFields("id", "name"))
	data.Columns[0].Type = iop.IntegerType
	data.Columns[1].Type = iop.StringType
	for i := 0; i < 500; i++ {
		data.Append([]any{i, fmt.Sprintf("n%d", i)})
	}
	df, err := iop.MakeDataFlow(data.Stream())
	require.NoError(t, err)
	count, err := conn.BulkImportFlow("main.t1", df)
	require.NoError(t, err)
	assert.EqualValues(t, 500, count)

	ds, err := conn.StreamRows("select * from main.t1 order by id")
	require.NoError(t, err)
	rows, err := ds.Collect(0)
	require.NoError(t, err)
	require.Len(t, rows.Rows, 500)
	assert.Equal(t, "n499", rows.Rows[499][1])

	schemata, err := conn.GetSchemata(SchemataLevelColumn, "main", "t1")
	require.NoError(t, err)
	assert.Len(t, schemata.Databases, 1, "only the remote catalog")
	tables := schemata.Tables()
	require.Len(t, tables, 1)
	for _, table := range tables {
		assert.Equal(t, "t1", strings.ToLower(table.Name))
		assert.Len(t, table.Columns, 2)
	}

	// a second client sees the data on the server
	other := newQuackConn(t, uri, "t313")
	res, err := other.Query("select count(*) as cnt from main.t1")
	require.NoError(t, err)
	assert.EqualValues(t, 500, cast.ToInt(res.Rows[0][0]))
}

func TestQuackLiveConcurrentWriters(t *testing.T) {
	uri := startQuackServer(t, "t313")

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := NewConn("duckdb://"+uri, "instance="+uri, "token=t313", "duckdb_version=2.0.0")
			if err == nil {
				err = conn.Connect()
			}
			if err != nil {
				errs[i] = err
				return
			}
			defer conn.Close()
			_, errs[i] = conn.Exec(fmt.Sprintf("create table main.w%d as select range as i from range(1000)", i))
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}

	// Quack 2.0.0 allows one remote scan per query
	conn := newQuackConn(t, uri, "t313")
	for _, table := range []string{"main.w0", "main.w1"} {
		res, err := conn.Query("select count(*) as cnt from " + table)
		require.NoError(t, err)
		assert.EqualValues(t, 1000, cast.ToInt(res.Rows[0][0]), table)
	}
}

func TestQuackLiveReconnect(t *testing.T) {
	uri := startQuackServer(t, "t313")
	conn := newQuackConn(t, uri, "t313")

	_, err := conn.Exec("create table main.r as select 7 as v")
	require.NoError(t, err)

	duck := conn.(*DuckDbConn).DuckDb()
	// the same steps as a query cancel: the next query opens a new sidecar
	duck.SetProp("connected", "false")
	require.NoError(t, duck.Proc.Cmd.Process.Kill())
	duck.Proc.Cmd.Wait()

	res, err := conn.Query("select v from main.r")
	require.NoError(t, err, "the session SQL must attach the server again")
	assert.EqualValues(t, 7, cast.ToInt(res.Rows[0][0]))
}
