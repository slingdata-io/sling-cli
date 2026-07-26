package database

import (
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file consolidates the index-related tests for the database package:
//   - TableKeys.ParseIndexes parsing (the table_keys.index list forms)
//   - TableIndex.CreateDDL rendering across every index-capable engine
//
// The cross-engine DDL matrix is hard-coded inline (see indexMatrixCases) rather
// than kept as a tree of checked-in golden .sql files, so the expected output
// lives next to the cases that produce it.

// ---------------------------------------------------------------------------
// ParseIndexes
// ---------------------------------------------------------------------------

// tkFromYAML parses a table_keys YAML block into TableKeys (exercising the
// custom UnmarshalJSON via the YAML->JSON round-trip used by the config loader).
func tkFromYAML(t *testing.T, yamlStr string) TableKeys {
	t.Helper()
	var raw map[string]any
	require.NoError(t, g.UnmarshalYAML(yamlStr, &raw))

	var tk TableKeys
	require.NoError(t, g.JSONUnmarshal([]byte(g.Marshal(raw["table_keys"])), &tk))
	return tk
}

var idxKnownCols = iop.Columns{
	{Name: "col1"}, {Name: "col2"}, {Name: "col3"},
	{Name: "search_col"}, {Name: "org_id"}, {Name: "project_id"},
	{Name: "created_at"}, {Name: "status"}, {Name: "name"},
}

func TestParseIndexesStringEntry(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  index:
    - search_col
`)
	defs, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "idx_mytable_search_col", defs[0].Name)
	require.Len(t, defs[0].Columns, 1)
	assert.Equal(t, "search_col", defs[0].Columns[0].Name)
}

func TestParseIndexesCompositeAnonymous(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  index:
    - [col1, col2]
`)
	defs, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	require.Len(t, defs[0].Columns, 2)
	assert.Equal(t, "col1", defs[0].Columns[0].Name)
	assert.Equal(t, "col2", defs[0].Columns[1].Name)
	assert.Equal(t, "idx_mytable_col1_col2", defs[0].Name)
}

func TestParseIndexesNamedSimple(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  index:
    - idx_search: [col1, col2]
`)
	defs, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "idx_search", defs[0].Name)
	require.Len(t, defs[0].Columns, 2)
}

func TestParseIndexesNamedComplex(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  index:
    - idx_lower_name:
        expression: LOWER(name)
        type: btree
    - idx_org_active:
        columns: [org_id, project_id]
        where: deleted_at IS NULL
        unique: true
    - idx_sorted:
        columns:
          - { name: created_at, sort: desc }
          - { name: project_id }
        type: btree
        include: [status]
`)
	defs, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.NoError(t, err)
	require.Len(t, defs, 3)

	byName := map[string]iop.IndexDef{}
	for _, d := range defs {
		byName[d.Name] = d
	}

	lower := byName["idx_lower_name"]
	assert.Equal(t, "LOWER(name)", lower.Expression)
	assert.Equal(t, "btree", lower.Type)
	assert.Empty(t, lower.Columns)

	active := byName["idx_org_active"]
	assert.True(t, active.Unique)
	assert.Equal(t, "deleted_at IS NULL", active.Where)
	require.Len(t, active.Columns, 2)

	sorted := byName["idx_sorted"]
	require.Len(t, sorted.Columns, 2)
	assert.Equal(t, "created_at", sorted.Columns[0].Name)
	assert.Equal(t, "desc", sorted.Columns[0].Sort)
	assert.Equal(t, []string{"status"}, sorted.Include)
}

func TestParseIndexesMixed(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  index:
    - search_col
    - [col1, col2]
    - idx_named: [col3]
    - idx_complex:
        columns: [org_id]
        unique: true
`)
	defs, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.NoError(t, err)
	require.Len(t, defs, 4)
}

func TestParseIndexesExpressionAnonymous(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  index:
    - [col1, "LOWER(col2)"]
`)
	defs, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	require.Len(t, defs[0].Columns, 2)
	assert.Equal(t, "col1", defs[0].Columns[0].Name)
	assert.Equal(t, "LOWER(col2)", defs[0].Columns[1].Name)
}

func TestParseIndexesUnknownColumn(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  index:
    - nonexistent_col
`)
	_, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestParseIndexesOldFlatForm(t *testing.T) {
	// existing single-key index: [col1, col2] -> two separate single-col indexes
	tk := tkFromYAML(t, `
table_keys:
  index: [col1, col2]
`)
	defs, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.NoError(t, err)
	require.Len(t, defs, 2)
	assert.Equal(t, "idx_mytable_col1", defs[0].Name)
	assert.Equal(t, "idx_mytable_col2", defs[1].Name)
}

func TestParseIndexesOtherKeysUnaffected(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  primary: [col1]
  unique: [col2]
  index:
    - idx_x:
        columns: [col3]
`)
	assert.Equal(t, []string{"col1"}, []string(tk[iop.PrimaryKey]))
	assert.Equal(t, []string{"col2"}, []string(tk[iop.UniqueKey]))

	defs, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "idx_x", defs[0].Name)
}

func TestTableKeysPrimaryUniqueComposite(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  primary: [[user_id, brand_id]]
  unique: [[org_id, project_id]]
`)
	assert.Equal(t, []string{"user_id", "brand_id"}, []string(tk[iop.PrimaryKey]))
	assert.Equal(t, []string{"org_id", "project_id"}, tk.UniqueColumnNames())
	require.Len(t, tk.UniqueGroups(), 1)
	assert.Equal(t, []string{"org_id", "project_id"}, tk.UniqueGroups()[0])

	tkFlat := tkFromYAML(t, `
table_keys:
  primary: [user_id, brand_id]
  unique: [org_id, project_id]
`)
	assert.Equal(t, []string(tk[iop.PrimaryKey]), []string(tkFlat[iop.PrimaryKey]))
	assert.Equal(t, []string{"org_id", "project_id"}, tkFlat.UniqueColumnNames())
	require.Len(t, tkFlat.UniqueGroups(), 2)
	assert.Equal(t, []string{"org_id"}, tkFlat.UniqueGroups()[0])
	assert.Equal(t, []string{"project_id"}, tkFlat.UniqueGroups()[1])

	tkScalar := tkFromYAML(t, `
table_keys:
  primary: user_id
`)
	assert.Equal(t, []string{"user_id"}, []string(tkScalar[iop.PrimaryKey]))

	tkNested := tkFromYAML(t, `
table_keys:
  primary: [[user_id], [brand_id]]
`)
	assert.Equal(t, []string{"user_id", "brand_id"}, []string(tkNested[iop.PrimaryKey]))

	tkMixed := tkFromYAML(t, `
table_keys:
  unique: [email, [org_id, project_id]]
`)
	require.Len(t, tkMixed.UniqueGroups(), 2)
	assert.Equal(t, []string{"email"}, tkMixed.UniqueGroups()[0])
	assert.Equal(t, []string{"org_id", "project_id"}, tkMixed.UniqueGroups()[1])
	assert.Equal(t, []string{"email", "org_id", "project_id"}, tkMixed.UniqueColumnNames())
}

func TestTableKeysCompositeSetKeys(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  primary: [[user_id, brand_id]]
  unique: [[org_id, project_id]]
`)

	cols := iop.Columns{
		{Name: "id"}, {Name: "user_id"}, {Name: "brand_id"},
		{Name: "org_id"}, {Name: "project_id"},
	}

	tbl := &Table{Name: "mytable", Schema: "public", Dialect: dbio.TypeDbPostgres, Columns: cols}
	require.NoError(t, tbl.SetKeys(nil, "", tk))

	assert.True(t, tbl.Columns.GetColumn("user_id").IsKeyType(iop.PrimaryKey))
	assert.True(t, tbl.Columns.GetColumn("brand_id").IsKeyType(iop.PrimaryKey))
	assert.True(t, tbl.Columns.GetColumn("org_id").IsKeyType(iop.UniqueKey))
	assert.True(t, tbl.Columns.GetColumn("project_id").IsKeyType(iop.UniqueKey))
	assert.False(t, tbl.Columns.GetColumn("id").IsKeyType(iop.PrimaryKey))
}

func TestTableKeysUniqueCompositeIndexes(t *testing.T) {
	cols := iop.Columns{
		{Name: "id"}, {Name: "org_id"}, {Name: "project_id"}, {Name: "email"},
	}

	tkComp := tkFromYAML(t, `
table_keys:
  unique: [[org_id, project_id]]
`)
	tbl := &Table{Name: "mytable", Schema: "public", Dialect: dbio.TypeDbPostgres, Columns: cols, Keys: tkComp}
	require.NoError(t, tbl.SetKeys(nil, "", tkComp))
	indexes := tbl.Indexes(tbl.Columns)
	require.Len(t, indexes, 1)
	assert.True(t, indexes[0].Def.Unique)
	require.Len(t, indexes[0].Def.Columns, 2)
	assert.Equal(t, "org_id", indexes[0].Def.Columns[0].Name)
	assert.Equal(t, "project_id", indexes[0].Def.Columns[1].Name)
	assert.Equal(t, "idx_mytable_org_id_project_id", indexes[0].Def.Name)

	tkFlat := tkFromYAML(t, `
table_keys:
  unique: [org_id, project_id]
`)
	tblFlat := &Table{Name: "mytable", Schema: "public", Dialect: dbio.TypeDbPostgres, Columns: cols, Keys: tkFlat}
	require.NoError(t, tblFlat.SetKeys(nil, "", tkFlat))
	indexesFlat := tblFlat.Indexes(tblFlat.Columns)
	require.Len(t, indexesFlat, 2)
	assert.True(t, indexesFlat[0].Def.Unique)
	assert.True(t, indexesFlat[1].Def.Unique)
	require.Len(t, indexesFlat[0].Def.Columns, 1)
	require.Len(t, indexesFlat[1].Def.Columns, 1)
	assert.Equal(t, "org_id", indexesFlat[0].Def.Columns[0].Name)
	assert.Equal(t, "project_id", indexesFlat[1].Def.Columns[0].Name)

	tkMixed := tkFromYAML(t, `
table_keys:
  unique: [email, [org_id, project_id]]
`)
	tblMixed := &Table{Name: "mytable", Schema: "public", Dialect: dbio.TypeDbPostgres, Columns: cols, Keys: tkMixed}
	require.NoError(t, tblMixed.SetKeys(nil, "", tkMixed))
	indexesMixed := tblMixed.Indexes(tblMixed.Columns)
	require.Len(t, indexesMixed, 2)
	assert.Equal(t, []string{"email"}, indexesMixed[0].Def.ColumnNames())
	assert.Equal(t, []string{"org_id", "project_id"}, indexesMixed[1].Def.ColumnNames())
	assert.True(t, indexesMixed[0].Def.Unique)
	assert.True(t, indexesMixed[1].Def.Unique)
}

func TestParseIndexesMutualExclusion(t *testing.T) {
	tk := tkFromYAML(t, `
table_keys:
  index:
    - idx_bad:
        columns: [col1]
        expression: LOWER(col2)
`)
	_, err := tk.ParseIndexes("mytable", idxKnownCols, 63)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

// ---------------------------------------------------------------------------
// CreateDDL matrix
// ---------------------------------------------------------------------------

// indexMatrixCase pairs an IndexDef with the expected CreateDDL output per
// engine. It renders through the real template system and needs no live database.
type indexMatrixCase struct {
	name string
	def  iop.IndexDef
	want map[dbio.Type]string
}

var indexMatrixCases = []indexMatrixCase{
	{
		name: "single_col",
		def:  iop.IndexDef{Name: "idx_single", Columns: []iop.IndexColumn{{Name: "col1"}}},
		want: map[dbio.Type]string{
			dbio.TypeDbPostgres:  `create index if not exists "idx_single" on "public"."mytable" ("col1")`,
			dbio.TypeDbMySQL:     "create index `idx_single` on `public`.`mytable` (`col1`)",
			dbio.TypeDbMariaDB:   "create index `idx_single` on `public`.`mytable` (`col1`)",
			dbio.TypeDbSQLServer: `create index "idx_single" on "public"."mytable" ("col1")`,
			dbio.TypeDbOracle:    `create index "idx_single" on "public"."mytable" ("col1")`,
			dbio.TypeDbDuckDb:    `create index "idx_single" on "public"."mytable" ("col1")`,
			dbio.TypeDbSQLite:    `create index if not exists "idx_single" on "public"."mytable" ("col1")`,
		},
	},
	{
		name: "composite_sorted",
		def:  iop.IndexDef{Name: "idx_comp", Columns: []iop.IndexColumn{{Name: "col1"}, {Name: "col2", Sort: "desc"}}},
		want: map[dbio.Type]string{
			dbio.TypeDbPostgres:  `create index if not exists "idx_comp" on "public"."mytable" ("col1", "col2" DESC)`,
			dbio.TypeDbMySQL:     "create index `idx_comp` on `public`.`mytable` (`col1`, `col2` DESC)",
			dbio.TypeDbMariaDB:   "create index `idx_comp` on `public`.`mytable` (`col1`, `col2` DESC)",
			dbio.TypeDbSQLServer: `create index "idx_comp" on "public"."mytable" ("col1", "col2" DESC)`,
			dbio.TypeDbOracle:    `create index "idx_comp" on "public"."mytable" ("col1", "col2" DESC)`,
			dbio.TypeDbDuckDb:    `create index "idx_comp" on "public"."mytable" ("col1", "col2" DESC)`,
			dbio.TypeDbSQLite:    `create index if not exists "idx_comp" on "public"."mytable" ("col1", "col2" DESC)`,
		},
	},
	{
		name: "unique",
		def:  iop.IndexDef{Name: "idx_uniq", Columns: []iop.IndexColumn{{Name: "email"}}, Unique: true},
		want: map[dbio.Type]string{
			dbio.TypeDbPostgres:  `create unique index if not exists "idx_uniq" on "public"."mytable" ("email")`,
			dbio.TypeDbMySQL:     "create unique index `idx_uniq` on `public`.`mytable` (`email`)",
			dbio.TypeDbMariaDB:   "create unique index `idx_uniq` on `public`.`mytable` (`email`)",
			dbio.TypeDbSQLServer: `create unique index "idx_uniq" on "public"."mytable" ("email")`,
			dbio.TypeDbOracle:    `create unique index "idx_uniq" on "public"."mytable" ("email")`,
			dbio.TypeDbDuckDb:    `create unique index "idx_uniq" on "public"."mytable" ("email")`,
			dbio.TypeDbSQLite:    `create unique index if not exists "idx_uniq" on "public"."mytable" ("email")`,
		},
	},
	{
		name: "partial_where",
		def:  iop.IndexDef{Name: "idx_partial", Columns: []iop.IndexColumn{{Name: "org_id"}}, Where: "deleted_at IS NULL"},
		want: map[dbio.Type]string{
			// engines without partial-index support drop the where clause (with a warning)
			dbio.TypeDbPostgres:  `create index if not exists "idx_partial" on "public"."mytable" ("org_id") where deleted_at IS NULL`,
			dbio.TypeDbMySQL:     "create index `idx_partial` on `public`.`mytable` (`org_id`)",
			dbio.TypeDbMariaDB:   "create index `idx_partial` on `public`.`mytable` (`org_id`)",
			dbio.TypeDbSQLServer: `create index "idx_partial" on "public"."mytable" ("org_id") where deleted_at IS NULL`,
			dbio.TypeDbOracle:    `create index "idx_partial" on "public"."mytable" ("org_id")`,
			dbio.TypeDbDuckDb:    `create index "idx_partial" on "public"."mytable" ("org_id")`,
			dbio.TypeDbSQLite:    `create index if not exists "idx_partial" on "public"."mytable" ("org_id") where deleted_at IS NULL`,
		},
	},
	{
		name: "covering_include",
		def:  iop.IndexDef{Name: "idx_cover", Columns: []iop.IndexColumn{{Name: "col1"}}, Include: []string{"status", "name"}},
		want: map[dbio.Type]string{
			// engines without covering-index support drop the include clause (with a warning)
			dbio.TypeDbPostgres:  `create index if not exists "idx_cover" on "public"."mytable" ("col1") include ("status", "name")`,
			dbio.TypeDbMySQL:     "create index `idx_cover` on `public`.`mytable` (`col1`)",
			dbio.TypeDbMariaDB:   "create index `idx_cover` on `public`.`mytable` (`col1`)",
			dbio.TypeDbSQLServer: `create index "idx_cover" on "public"."mytable" ("col1") include ("status", "name")`,
			dbio.TypeDbOracle:    `create index "idx_cover" on "public"."mytable" ("col1")`,
			dbio.TypeDbDuckDb:    `create index "idx_cover" on "public"."mytable" ("col1")`,
			dbio.TypeDbSQLite:    `create index if not exists "idx_cover" on "public"."mytable" ("col1")`,
		},
	},
	{
		name: "expression",
		def:  iop.IndexDef{Name: "idx_expr", Expression: "LOWER(name)"},
		want: map[dbio.Type]string{
			dbio.TypeDbPostgres:  `create index if not exists "idx_expr" on "public"."mytable" (LOWER(name))`,
			dbio.TypeDbMySQL:     "create index `idx_expr` on `public`.`mytable` (LOWER(name))",
			dbio.TypeDbMariaDB:   "create index `idx_expr` on `public`.`mytable` (LOWER(name))",
			dbio.TypeDbSQLServer: `create index "idx_expr" on "public"."mytable" (LOWER(name))`,
			dbio.TypeDbOracle:    `create index "idx_expr" on "public"."mytable" (LOWER(name))`,
			dbio.TypeDbDuckDb:    `create index "idx_expr" on "public"."mytable" (LOWER(name))`,
			dbio.TypeDbSQLite:    `create index if not exists "idx_expr" on "public"."mytable" (LOWER(name))`,
		},
	},
}

// indexMatrixEngines: engines that support secondary indexes and render real DDL.
var indexMatrixEngines = []dbio.Type{
	dbio.TypeDbPostgres,
	dbio.TypeDbMySQL,
	dbio.TypeDbMariaDB,
	dbio.TypeDbSQLServer,
	dbio.TypeDbOracle,
	dbio.TypeDbDuckDb,
	dbio.TypeDbSQLite,
}

func TestIndexDDLMatrix(t *testing.T) {
	for _, engine := range indexMatrixEngines {
		for _, c := range indexMatrixCases {
			t.Run(string(engine)+"/"+c.name, func(t *testing.T) {
				want, ok := c.want[engine]
				require.True(t, ok, "missing expected DDL for %s/%s", engine, c.name)

				tbl := &Table{Name: "mytable", Schema: "public", Dialect: engine}
				ti := TableIndex{Def: c.def, Table: tbl}
				got := ti.CreateDDL()

				assert.Equal(t, strings.TrimSpace(want), strings.TrimSpace(got))
			})
		}
	}
}

// TestIndexDDLMatrixCoverage ensures every index-capable engine has expected
// output for every case, so adding a connector forces adding its matrix coverage.
func TestIndexDDLMatrixCoverage(t *testing.T) {
	for _, engine := range indexMatrixEngines {
		for _, c := range indexMatrixCases {
			_, ok := c.want[engine]
			assert.True(t, ok, "missing matrix coverage for %s/%s", engine, c.name)
		}
	}
}

// TestIndexDDLClosedSetError verifies a closed-set type violation errors out
// (returns empty DDL after warning) on SQL Server, but passes through on
// passthrough engines like Postgres.
func TestIndexDDLClosedSetError(t *testing.T) {
	// SQL Server: unknown type -> rejected (empty DDL)
	tbl := &Table{Name: "t", Schema: "dbo", Dialect: dbio.TypeDbSQLServer}
	ti := TableIndex{Def: iop.IndexDef{Name: "idx", Columns: []iop.IndexColumn{{Name: "c"}}, Type: "gin"}, Table: tbl}
	assert.Empty(t, ti.CreateDDL(), "sqlserver should reject unknown closed-set type")

	// SQL Server: valid clustered -> renders
	ti = TableIndex{Def: iop.IndexDef{Name: "idx", Columns: []iop.IndexColumn{{Name: "c"}}, Type: "clustered"}, Table: tbl}
	got := ti.CreateDDL()
	assert.Contains(t, strings.ToUpper(got), "CLUSTERED")

	// Postgres: passthrough type -> renders with USING
	tbl = &Table{Name: "t", Schema: "public", Dialect: dbio.TypeDbPostgres}
	ti = TableIndex{Def: iop.IndexDef{Name: "idx", Columns: []iop.IndexColumn{{Name: "c"}}, Type: "gin"}, Table: tbl}
	got = ti.CreateDDL()
	assert.Contains(t, got, "using gin")
}

// TestIndexDDLNoIndexEngines verifies no-index engines render nothing.
func TestIndexDDLNoIndexEngines(t *testing.T) {
	for _, engine := range []dbio.Type{dbio.TypeDbRedshift, dbio.TypeDbSnowflake} {
		tbl := &Table{Name: "t", Schema: "public", Dialect: engine}
		ti := TableIndex{Def: iop.IndexDef{Name: "idx", Columns: []iop.IndexColumn{{Name: "c"}}}, Table: tbl}
		assert.Empty(t, ti.CreateDDL(), "%s should render no index DDL", engine)
	}
}
