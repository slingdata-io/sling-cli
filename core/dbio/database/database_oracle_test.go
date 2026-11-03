package database

import (
	"testing"

	go_ora "github.com/sijms/go-ora/v2"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessOracleInsertRow(t *testing.T) {
	cols := iop.Columns{
		{Name: "v", DbType: "VARCHAR2"},
		{Name: "nv", DbType: "NVARCHAR2"},
		{Name: "nc", DbType: "NCHAR"},
		{Name: "ncl", DbType: "nclob"},
		{Name: "nnull", DbType: "NVARCHAR2"},
		{Name: "nnum", DbType: "NVARCHAR2"},
	}
	row := processOracleInsertRow(cols, []any{"ł", "ł", "ł", "ł", nil, 5, "extra"})

	assert.Equal(t, "ł", row[0])
	assert.Equal(t, go_ora.NVarChar("ł"), row[1])
	assert.Equal(t, go_ora.NVarChar("ł"), row[2])
	assert.Equal(t, go_ora.NVarChar("ł"), row[3])
	assert.Nil(t, row[4])
	assert.Equal(t, 5, row[5])
	assert.Equal(t, "extra", row[6])
}

func TestOracleNationalTypes(t *testing.T) {
	conn, err := NewConn("oracle://u:p@localhost:1521/FREEPDB1")
	require.NoError(t, err)

	nativeType := func(col iop.Column) string {
		nt, err := conn.GetNativeType(col)
		require.NoError(t, err)
		return nt
	}

	str := iop.Column{Name: "s", Type: iop.StringType, DbPrecision: 40, Sourced: true}
	assert.Equal(t, "varchar(40)", nativeType(str))

	// go-ora reports VARCHAR2 result columns as NCHAR: no national type
	str.DbType = "NCHAR"
	assert.Equal(t, "varchar(40)", nativeType(str))

	str.DbType = "NVARCHAR2"
	assert.Equal(t, "nvarchar2(40)", nativeType(str))

	str.DbPrecision = 3000
	assert.Equal(t, "nclob", nativeType(str))

	text := iop.Column{Name: "t", Type: iop.TextType, DbType: "NCLOB"}
	assert.Equal(t, "nclob", nativeType(text))

	// a CLOB temp column goes into an NCLOB target without a varchar2 cast
	src := iop.Column{Name: "c", Type: iop.TextType, DbType: "CLOB"}
	assert.Equal(t, `"c"`, conn.CastColumnForSelect(src, iop.Column{Name: "c", Type: iop.TextType, DbType: "NCLOB"}))
	assert.Equal(t, `cast("c" as varchar2(100))`, conn.CastColumnForSelect(src, iop.Column{Name: "c", Type: iop.StringType, DbType: "VARCHAR2", DbPrecision: 100}))
}
