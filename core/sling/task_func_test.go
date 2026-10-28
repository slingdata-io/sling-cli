package sling

import (
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTableConn is a target with a gap between the existence check and the
// create, as on ClickHouse Cloud. With hidden, the catalog never shows a
// table.
type createTableConn struct {
	database.Connection
	mu      sync.Mutex
	tables  map[string]bool
	creates int
	gap     time.Duration
	hidden  bool
}

func (c *createTableConn) GetType() dbio.Type { return dbio.TypeDbClickhouse }

func (c *createTableConn) GetSchemas() (iop.Dataset, error) {
	data := iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	data.Append([]any{"main"})
	return data, nil
}

func (c *createTableConn) GenerateDDL(table database.Table, data iop.Dataset, temporary bool) (string, error) {
	return "create table " + table.FullName(), nil
}

func (c *createTableConn) GetTemplateValue(path string) string { return "already exists" }

func (c *createTableConn) TableExists(table database.Table) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tables[table.FullName()] && !c.hidden, nil
}

func (c *createTableConn) ExecMulti(sqls ...string) (sql.Result, error) {
	time.Sleep(c.gap)
	name := strings.TrimPrefix(sqls[0], "create table ")

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tables[name] {
		return nil, g.Error("Code: 57. DB::Exception: Table %s already exists. (TABLE_ALREADY_EXISTS)", name)
	}
	c.tables[name] = true
	c.creates++
	return nil, nil
}

// createInChunks runs createTableIfNotExists from n chunks at the same time.
func createInChunks(t *testing.T, conn *createTableConn, n int) (created int, errs []error) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			table := database.Table{Name: "big_table", Schema: "main", Dialect: dbio.TypeDbClickhouse}
			<-start
			ok, err := createTableIfNotExists(conn, iop.Dataset{}, &table, false)

			mu.Lock()
			defer mu.Unlock()
			if ok {
				created++
			}
			if err != nil {
				errs = append(errs, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	return
}

func TestCreateTableIfNotExists_Chunks(t *testing.T) {
	t.Run("the chunks of a run create the table once", func(t *testing.T) {
		conn := &createTableConn{tables: map[string]bool{}, gap: 20 * time.Millisecond}
		created, errs := createInChunks(t, conn, 4)
		require.Empty(t, errs)
		assert.Equal(t, 1, created)
		assert.Equal(t, 1, conn.creates)
	})

	t.Run("a catalog that does not show the new table yet", func(t *testing.T) {
		conn := &createTableConn{tables: map[string]bool{}, gap: 50 * time.Millisecond, hidden: true}
		created, errs := createInChunks(t, conn, 4)
		require.Empty(t, errs)
		assert.Equal(t, 1, created)
	})

	t.Run("a table that a different process created is an error", func(t *testing.T) {
		table := database.Table{Name: "big_table", Schema: "main", Dialect: dbio.TypeDbClickhouse}
		conn := &createTableConn{tables: map[string]bool{table.FullName(): true}, hidden: true}
		_, err := createTableIfNotExists(conn, iop.Dataset{}, &table, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "A concurrent process created it")
		assert.ErrorAs(t, err, new(tableExistsError), "createTable accepts it for a chunk")
	})

	t.Run("a chunk accepts a table that a different process created", func(t *testing.T) {
		table := database.Table{Name: "big_table", Schema: "main", Dialect: dbio.TypeDbClickhouse}
		conn := &createTableConn{tables: map[string]bool{table.FullName(): true}, hidden: true}

		chunk := &TaskExecution{Config: &Config{}}
		chunk.Config.Source.Options = &SourceOptions{ChunkCount: g.Ptr(4)}
		created, err := createTable(chunk, conn, table, iop.Dataset{}, false)
		require.NoError(t, err)
		assert.False(t, created)

		_, err = createTable(&TaskExecution{Config: &Config{Source: Source{Options: &SourceOptions{}}}}, conn, table, iop.Dataset{}, false)
		require.Error(t, err, "a stream without chunks keeps the error")
	})

	assert.Empty(t, tableCreates.entries, "an entry stays after its last holder")
}
