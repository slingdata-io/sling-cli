package database

import (
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStarRocksSchemaMigrationDDL(t *testing.T) {
	t.Setenv("SLING_SCHEMA_MIGRATION", "all")

	conn, err := NewConn("starrocks://root:@localhost:9030/sys")
	require.NoError(t, err)

	col := func(name string, colType iop.ColumnType, meta map[string]string) iop.Column {
		return iop.Column{Name: name, Type: colType, Metadata: meta}
	}
	columns := iop.Columns{
		col("name", iop.StringType, map[string]string{"nullable": "false", "default_value": "'x'", "unique": "true", "description": "the name"}),
		col("id", iop.BigIntType, map[string]string{"is_primary_key": "true", "auto_increment": "true", "nullable": "false"}),
		col("qty", iop.IntegerType, map[string]string{"default_value": "0"}),
		col("small", iop.SmallIntType, map[string]string{"auto_increment": "true"}),
		col("active", iop.BoolType, map[string]string{"default_value": "true"}),
		col("ts", iop.TimestampType, map[string]string{"nullable": "false", "default_value": "current_timestamp"}),
		col("d", iop.DateType, map[string]string{"default_value": "current_date"}),
		col("uid", iop.UUIDType, map[string]string{"default_value": "uuid()"}),
	}
	require.NoError(t, columns.SetKeys(iop.PrimaryKey, "id"))

	data := iop.NewDataset(columns)
	data.Inferred = true
	table := Table{Schema: "public", Name: "t1", Dialect: conn.GetType()}

	ddl, err := conn.GenerateDDL(table, data, false)
	require.NoError(t, err)

	// keys first, NOT NULL before AUTO_INCREMENT / DEFAULT
	assert.Contains(t, ddl, "(`id` bigint NOT NULL AUTO_INCREMENT,")
	assert.Contains(t, ddl, "`name` varchar(65533) NOT NULL DEFAULT 'x' COMMENT 'the name'")
	assert.Contains(t, ddl, "`qty` bigint DEFAULT '0'")
	assert.Contains(t, ddl, "`active` boolean DEFAULT '1'")
	assert.Contains(t, ddl, "`ts` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP")
	assert.Contains(t, ddl, "`uid` varchar(36) DEFAULT (uuid())")
	assert.Contains(t, ddl, "primary key(`id`) distributed by hash(`id`)")

	// unsupported by StarRocks: AUTO_INCREMENT on non-BIGINT, CURRENT_DATE default, UNIQUE, inline PRIMARY KEY
	assert.Contains(t, ddl, "`small` smallint,")
	assert.Contains(t, ddl, "`d` date,")
	assert.NotContains(t, ddl, "UNIQUE")
	assert.NotContains(t, ddl, "PRIMARY KEY (")
}

func TestStarRocksForeignKeysDDL(t *testing.T) {
	t.Setenv("SLING_SCHEMA_MIGRATION", "foreign_key")

	conn, err := NewConn("starrocks://root:@localhost:9030/sys")
	require.NoError(t, err)

	columns := iop.Columns{
		{Name: "order_id", Type: iop.BigIntType, Metadata: map[string]string{
			"foreign_key": `{"constraint_name":"fk1","column_name":"order_id","referenced_schema":"public","referenced_table":"sm_orders","referenced_column":"order_id"}`,
		}},
		{Name: "product_id", Type: iop.BigIntType, Metadata: map[string]string{
			"foreign_key": `{"constraint_name":"fk2","column_name":"product_id","referenced_schema":"public","referenced_table":"sm_products","referenced_column":"product_id"}`,
		}},
	}
	table := Table{Schema: "public", Name: "sm_order_items", Dialect: conn.GetType()}

	sm := NewSchemaMigrator(nil).(*SchemaMigratorBase)
	statements := sm.GenerateForeignKeysDDL(conn, table, columns)

	// all foreign keys in one table property, with unquoted names
	require.Len(t, statements, 1)
	assert.Equal(t,
		"alter table `public`.`sm_order_items` set ('foreign_key_constraints' = '(order_id) REFERENCES public.sm_orders(order_id);(product_id) REFERENCES public.sm_products(product_id)')",
		statements[0],
	)
}
