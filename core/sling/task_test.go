package sling

import (
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/stretchr/testify/assert"
)

func TestErrorHelper(t *testing.T) {
	maxLineSizeErr := g.Error("Invalid Input Error: CSV Error on Line: 2\nMaximum line size of 2000000 bytes exceeded. Actual Size:5022477 bytes.")
	csvErr := g.Error("Invalid Input Error: CSV Error on Line: 2\nsome other serialization error")

	t.Run("max_line_size exceeded gets specific help, not arrow_http", func(t *testing.T) {
		helpString := ErrorHelper(maxLineSizeErr, dbio.TypeDbSQLServer, dbio.TypeFileS3)
		assert.Contains(t, helpString, "max_line_size")
		assert.Contains(t, helpString, "max_line_size` property")
		assert.NotContains(t, helpString, "arrow_http")
	})

	t.Run("csv error suggests arrow_http for duckdb-class connections", func(t *testing.T) {
		for _, connType := range []dbio.Type{dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck, dbio.TypeDbDuckLake} {
			helpString := ErrorHelper(csvErr, dbio.TypeDbPostgres, connType)
			assert.Contains(t, helpString, "arrow_http", "connType=%s", connType)
		}
	})

	t.Run("csv error does not suggest arrow_http for non-duckdb connections", func(t *testing.T) {
		helpString := ErrorHelper(csvErr, dbio.TypeDbSQLServer, dbio.TypeFileS3)
		assert.NotContains(t, helpString, "arrow_http")
	})

	t.Run("sql browser timeout explains named instance port", func(t *testing.T) {
		err := g.Error("unable to get instances from Sql Server Browser on host THEHOST: read udp 127.0.0.1:50533->192.168.0.1:1434: i/o timeout")
		helpString := ErrorHelper(err, dbio.TypeDbSQLServer)
		assert.Contains(t, helpString, "UDP 1434")
		assert.Contains(t, helpString, "instance TCP port")
	})
}
