package database

import (
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
)

func newTestSQLServerConn() *MsSQLServerConn {
	conn := &MsSQLServerConn{}
	conn.BaseConn.Type = dbio.TypeDbSQLServer
	conn.template, _ = dbio.TypeDbSQLServer.Template()
	return conn
}

func TestSQLServerChangeTrackingSnapshotSQL(t *testing.T) {
	conn := newTestSQLServerConn()

	table := Table{
		Name:   "customers",
		Schema: "dbo",
		Columns: iop.Columns{
			{Name: "id", Type: iop.IntegerType},
			{Name: "name", Type: iop.StringType},
			{Name: "amount", Type: iop.DecimalType},
		},
		Dialect: dbio.TypeDbSQLServer,
	}

	// 1. All columns snapshot
	sql := conn.BuildChangeTrackingSnapshotSQL(table, []string{"id"}, []string{"*"}, 100, "")
	assert.Contains(t, sql, "SELECT t.\"id\" AS \"id\", t.\"name\" AS \"name\", t.\"amount\" AS \"amount\", 'I' AS [_sling_synced_op], CAST(100 AS BIGINT) AS [_sling_cdc_seq], SYSUTCDATETIME() AS [_sling_synced_at]")
	assert.Contains(t, sql, "FROM \"dbo\".\"customers\" AS t")
	assert.NotContains(t, sql, "WHERE")

	// 2. Snapshot with where clause
	sqlWhere := conn.BuildChangeTrackingSnapshotSQL(table, []string{"id"}, []string{"*"}, 105, "amount > 50")
	assert.Contains(t, sqlWhere, "WHERE amount > 50")

	// 3. Snapshot with explicit select fields and aliases
	sqlSelect := conn.BuildChangeTrackingSnapshotSQL(table, []string{"id"}, []string{"id", "name as full_name"}, 110, "")
	assert.Contains(t, sqlSelect, "t.\"id\" AS \"id\", t.\"name\" AS \"full_name\"")

	// 4. Snapshot with select omitting PK: PK should be automatically included
	sqlOmitPK := conn.BuildChangeTrackingSnapshotSQL(table, []string{"id"}, []string{"name"}, 120, "")
	assert.Contains(t, sqlOmitPK, "t.\"id\" AS \"id\"")
	assert.Contains(t, sqlOmitPK, "t.\"name\" AS \"name\"")

	// 5. Snapshot with PK aliasing: original PK must also be included under canonical name
	sqlAliasPK := conn.BuildChangeTrackingSnapshotSQL(table, []string{"id"}, []string{"id as custom_id", "name"}, 125, "")
	assert.Contains(t, sqlAliasPK, "t.\"id\" AS \"id\"")
	assert.Contains(t, sqlAliasPK, "t.\"id\" AS \"custom_id\"")

	// 6. Snapshot with empty table.Columns and wildcard: does not duplicate PK
	emptyTable := Table{Name: "customers", Schema: "dbo", Dialect: dbio.TypeDbSQLServer}
	sqlEmptyCols := conn.BuildChangeTrackingSnapshotSQL(emptyTable, []string{"id"}, []string{"*"}, 130, "")
	assert.Contains(t, sqlEmptyCols, "SELECT t.*, 'I' AS [_sling_synced_op]")
	assert.NotContains(t, sqlEmptyCols, "t.\"id\" AS \"id\", t.*")
}

func TestSQLServerChangeTrackingSelectSQL(t *testing.T) {
	conn := newTestSQLServerConn()

	table := Table{
		Name:   "orders",
		Schema: "sales",
		Columns: iop.Columns{
			{Name: "order_id", Type: iop.IntegerType},
			{Name: "customer_id", Type: iop.IntegerType},
			{Name: "total", Type: iop.DecimalType},
		},
		Dialect: dbio.TypeDbSQLServer,
	}

	// 1. Single PK, all columns
	sql := conn.BuildChangeTrackingSelectSQL(table, []string{"order_id"}, []string{"*"}, 50, 75, "")
	assert.Contains(t, sql, "FROM CHANGETABLE(CHANGES \"sales\".\"orders\", 50) AS ct")
	assert.Contains(t, sql, "LEFT OUTER JOIN \"sales\".\"orders\" AS t ON ct.\"order_id\" = t.\"order_id\"")
	assert.Contains(t, sql, "ct.\"order_id\" AS \"order_id\"")
	assert.Contains(t, sql, "t.\"customer_id\" AS \"customer_id\"")
	assert.Contains(t, sql, "t.\"total\" AS \"total\"")
	assert.Contains(t, sql, "CASE WHEN t.\"order_id\" IS NULL THEN 'D' ELSE ct.SYS_CHANGE_OPERATION END AS [_sling_synced_op]")
	assert.Contains(t, sql, "ct.SYS_CHANGE_VERSION AS [_sling_cdc_seq]")
	assert.Contains(t, sql, "SYSUTCDATETIME() AS [_sling_synced_at]")
	assert.Contains(t, sql, "WHERE ct.SYS_CHANGE_VERSION <= 75")
	assert.Contains(t, sql, "ORDER BY ct.SYS_CHANGE_VERSION")
	assert.NotContains(t, sql, "ct.SYS_CHANGE_OPERATION = 'D'")

	// 2. Composite PKs
	compositeTable := Table{
		Name:   "line_items",
		Schema: "sales",
		Columns: iop.Columns{
			{Name: "order_id", Type: iop.IntegerType},
			{Name: "item_id", Type: iop.IntegerType},
			{Name: "qty", Type: iop.IntegerType},
		},
		Dialect: dbio.TypeDbSQLServer,
	}
	compSQL := conn.BuildChangeTrackingSelectSQL(compositeTable, []string{"order_id", "item_id"}, []string{"*"}, 100, 150, "")
	assert.Contains(t, compSQL, "ct.\"order_id\" = t.\"order_id\" AND ct.\"item_id\" = t.\"item_id\"")
	assert.Contains(t, compSQL, "ct.\"order_id\" AS \"order_id\"")
	assert.Contains(t, compSQL, "ct.\"item_id\" AS \"item_id\"")
	assert.Contains(t, compSQL, "t.\"qty\" AS \"qty\"")
	assert.Contains(t, compSQL, "ORDER BY ct.SYS_CHANGE_VERSION")

	// 3. User WHERE clause: must preserve deletes via OR (ct.SYS_CHANGE_OPERATION = 'D' OR (where))
	sqlWithWhere := conn.BuildChangeTrackingSelectSQL(table, []string{"order_id"}, []string{"*"}, 50, 75, "t.customer_id = 42")
	assert.Contains(t, sqlWithWhere, "WHERE ct.SYS_CHANGE_VERSION <= 75 AND (ct.SYS_CHANGE_OPERATION = 'D' OR (t.customer_id = 42))")
	assert.Contains(t, sqlWithWhere, "ORDER BY ct.SYS_CHANGE_VERSION")

	// 4. Custom select: PK is automatically included from ct even if omitted from select
	sqlCustomSelect := conn.BuildChangeTrackingSelectSQL(table, []string{"order_id"}, []string{"total"}, 50, 75, "")
	assert.Contains(t, sqlCustomSelect, "ct.\"order_id\" AS \"order_id\"")
	assert.Contains(t, sqlCustomSelect, "t.\"total\" AS \"total\"")
	assert.False(t, strings.Contains(sqlCustomSelect, "t.\"customer_id\""), "customer_id should not be selected")

	// 5. Select with PK aliased: canonical PK must also be included
	sqlAliasPK := conn.BuildChangeTrackingSelectSQL(table, []string{"order_id"}, []string{"order_id as custom_order_id", "total"}, 50, 75, "")
	assert.Contains(t, sqlAliasPK, "ct.\"order_id\" AS \"order_id\"")
	assert.Contains(t, sqlAliasPK, "ct.\"order_id\" AS \"custom_order_id\"")

	// 6. Empty table.Columns with wildcard: fallback includes t.*
	emptyTable := Table{Name: "orders", Schema: "sales", Dialect: dbio.TypeDbSQLServer}
	sqlEmptyCols := conn.BuildChangeTrackingSelectSQL(emptyTable, []string{"order_id"}, []string{"*"}, 50, 75, "")
	assert.Contains(t, sqlEmptyCols, "ct.\"order_id\" AS \"order_id\"")
	assert.Contains(t, sqlEmptyCols, "t.*")
}
