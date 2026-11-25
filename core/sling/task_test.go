package sling

import (
	"sync"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
)

func TestErrorHelper(t *testing.T) {
	maxLineSizeErr := g.Error("Invalid Input Error: CSV Error on Line: 2\nMaximum line size of 2000000 bytes exceeded. Actual Size:5022477 bytes.")
	csvErr := g.Error("Invalid Input Error: CSV Error on Line: 2\nsome other serialization error")

	t.Run("max_line_size exceeded gets specific help, not copy_format: arrow", func(t *testing.T) {
		helpString := ErrorHelper(maxLineSizeErr, dbio.TypeDbSQLServer, dbio.TypeFileS3)
		assert.Contains(t, helpString, "max_line_size")
		assert.Contains(t, helpString, "max_line_size` property")
		assert.NotContains(t, helpString, "copy_format: arrow")
	})

	t.Run("csv error suggests copy_format: arrow for duckdb-class connections", func(t *testing.T) {
		for _, connType := range []dbio.Type{dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck, dbio.TypeDbDuckLake} {
			helpString := ErrorHelper(csvErr, dbio.TypeDbPostgres, connType)
			assert.Contains(t, helpString, "copy_format: arrow", "connType=%s", connType)
		}
	})

	t.Run("csv error does not suggest copy_format: arrow for non-duckdb connections", func(t *testing.T) {
		helpString := ErrorHelper(csvErr, dbio.TypeDbSQLServer, dbio.TypeFileS3)
		assert.NotContains(t, helpString, "copy_format: arrow")
	})

	t.Run("sql browser timeout explains named instance port", func(t *testing.T) {
		err := g.Error("unable to get instances from Sql Server Browser on host THEHOST: read udp 127.0.0.1:50533->192.168.0.1:1434: i/o timeout")
		helpString := ErrorHelper(err, dbio.TypeDbSQLServer)
		assert.Contains(t, helpString, "UDP 1434")
		assert.Contains(t, helpString, "instance TCP port")
	})

	t.Run("duckdb 2.0 hints", func(t *testing.T) {
		cases := map[string]string{
			"IO Error: Trying to read a database file with version number 999, but we can only read versions between 64 and 68.\nThe database file was created with an newer version of DuckDB.": "storage_version",
			"Binder Error: Deprecated lambda arrow (->) detected. Please transition to the new lambda syntax":                                                                                    "lambda x: x + 1",
			"Invalid Input Error: MERGE INTO command cannot affect the same target row more than once.":                                                                                          "duplicate primary key",
			"Not implemented Error: Only DuckLake versions 0.1 to 1.0 are supported":                                                                                                             "migrate the DuckLake catalog",
			`Error opening "c.ducklake": file was written with a storage version greater than the latest version supported by this DuckDB instance.`:                                             "writes to a catalog that is a DuckDB file",
		}
		for msg, want := range cases {
			assert.Contains(t, ErrorHelper(g.Error(msg), dbio.TypeDbDuckDb), want, msg)
		}
	})
}

func TestTaskDfConcurrentAccess(t *testing.T) {
	task := &TaskExecution{df: iop.NewDataflow(), StartTime: g.Ptr(time.Now())}
	last := iop.NewDataflow()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 499; i++ {
			task.setDf(iop.NewDataflow())
		}
		task.setDf(last)
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_ = task.Df()
			_ = task.GetCount()
			_, _ = task.GetBytes()
		}
	}()
	wg.Wait()

	assert.Same(t, last, task.Df())
}

// GetRate runs on the ticker and the task goroutines while the task ends.
// Run with -race.
func TestTaskStatusSnapshotGetRate(t *testing.T) {
	task := &TaskExecution{df: iop.NewDataflow(), StartTime: g.Ptr(time.Now().Add(-time.Minute))}

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				_, _ = task.GetRate(1)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 500 {
			task.setEndTime(time.Now())
		}
	}()
	wg.Wait()

	st := task.StatusSnapshot()
	assert.NotNil(t, st.EndTime)
	assert.Same(t, task.StartTime, st.StartTime)
}

func TestIsFullRefreshCDC(t *testing.T) {
	newTask := func(mode Mode, srcType dbio.Type, opts *CDCOptions) *TaskExecution {
		cfg := &Config{Mode: mode, ReplicationStream: &ReplicationStreamConfig{CDCOptions: opts}}
		cfg.SrcConn.Type = srcType
		return &TaskExecution{Config: cfg}
	}
	set := &CDCOptions{RunMaxEvents: g.Ptr(10)}

	t.Setenv("SLING_STATE", "LOCAL/state")
	assert.False(t, newTask(FullRefreshMode, dbio.TypeDbSQLite, &CDCOptions{}).isFullRefreshCDC())
	assert.False(t, newTask(FullRefreshMode, dbio.TypeDbPostgres, &CDCOptions{}).isFullRefreshCDC())
	assert.False(t, newTask(FullRefreshMode, dbio.TypeDbPostgres, nil).isFullRefreshCDC())
	assert.True(t, newTask(FullRefreshMode, dbio.TypeDbPostgres, set).isFullRefreshCDC())
	assert.False(t, newTask(FullRefreshMode, dbio.TypeDbSQLite, set).isFullRefreshCDC())
	assert.False(t, newTask(IncrementalMode, dbio.TypeDbPostgres, set).isFullRefreshCDC())

	t.Setenv("SLING_STATE", "")
	assert.False(t, newTask(FullRefreshMode, dbio.TypeDbPostgres, set).isFullRefreshCDC())
}
