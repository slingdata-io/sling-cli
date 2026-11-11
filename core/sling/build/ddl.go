package build

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
)

// quoteFullTableName parses schema.table and returns a dialect-quoted FDQN.
func (e *Executor) quoteFullTableName(fullName string) (string, error) {
	table, err := database.ParseTableName(fullName, e.DbConn.GetType())
	if err != nil {
		return "", g.Error(err, "could not parse table name '%s'", fullName)
	}
	return table.FDQN(), nil
}

// supportsDropCascade reports whether the dialect accepts CASCADE on DROP.
func supportsDropCascade(t dbio.Type) bool {
	return g.In(t,
		dbio.TypeDbPostgres, dbio.TypeDbRedshift,
		dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck, dbio.TypeDbDuckLake, dbio.TypeDbLanceDB,
		dbio.TypeDbSnowflake, // accepted (no-op-ish) but harmless
	)
}

// supportsCreateOrReplaceTable reports dialects with atomic CREATE OR REPLACE TABLE.
// LanceDB is excluded: the lance extension serves a stale projection after a
// replace that changes the schema, so the table must be dropped first.
func supportsCreateOrReplaceTable(t dbio.Type) bool {
	return g.In(t,
		dbio.TypeDbSnowflake,
		dbio.TypeDbBigQuery,
		dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck, dbio.TypeDbDuckLake,
		dbio.TypeDbDatabricks,
	)
}

// supportsRenameTable reports dialects with a rename_table template.
func (e *Executor) supportsRenameTable() bool {
	return strings.TrimSpace(e.DbConn.GetTemplateValue("core.rename_table")) != ""
}

// isSQLServerFamily reports SQL Server / Azure SQL / Fabric dialects.
func isSQLServerFamily(t dbio.Type) bool {
	return g.In(t, dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH, dbio.TypeDbFabric)
}

// wantCascade returns whether CASCADE should be used for this model.
func (e *Executor) wantCascade(model *Model) bool {
	if model != nil && model.Config.DropCascade != nil {
		return *model.Config.DropCascade
	}
	if e.Build != nil && e.Build.Project != nil && e.Build.Project.Config != nil {
		if e.Build.Project.Config.Defaults.DropCascade != nil {
			return *e.Build.Project.Config.Defaults.DropCascade
		}
	}
	return false // safe default: no CASCADE
}

// dropTable drops a table using the dialect template. When cascade is requested
// and the dialect supports it, CASCADE is appended. On failure due to dependents,
// the error message hints at drop_cascade: true.
func (e *Executor) dropTable(fullName string, cascade bool) error {
	quoted, err := e.quoteFullTableName(fullName)
	if err != nil {
		return err
	}

	sql := g.R(e.DbConn.GetTemplateValue("core.drop_table"), "table", quoted)
	if cascade && supportsDropCascade(e.DbConn.GetType()) {
		// Avoid double CASCADE if template already has it
		if !strings.Contains(strings.ToUpper(sql), "CASCADE") {
			sql = strings.TrimRight(sql, "; \t\n") + " CASCADE"
		}
	}

	if _, err := e.DbConn.Exec(sql); err != nil {
		errLower := strings.ToLower(err.Error())
		// Ignore "does not exist" style errors
		ignore := e.DbConn.Template().Variable["error_ignore_drop_table"]
		if ignore != "" && strings.Contains(errLower, strings.ToLower(ignore)) {
			return nil
		}
		if strings.Contains(errLower, "does not exist") ||
			strings.Contains(errLower, "unknown table") ||
			strings.Contains(errLower, "cannot find") ||
			strings.Contains(errLower, "not found") {
			return nil
		}
		// Dependent objects often surface as "depends on" / "dependent" / FK errors
		if !cascade && (strings.Contains(errLower, "depend") ||
			strings.Contains(errLower, "referenced by") ||
			strings.Contains(errLower, "foreign key")) {
			return g.Error(err, "could not drop table %s (dependent objects may exist; set drop_cascade: true to force)", fullName)
		}
		return g.Error(err, "could not drop table %s", fullName)
	}
	return nil
}

// dropView drops a view using the dialect template.
func (e *Executor) dropView(fullName string, cascade bool) error {
	quoted, err := e.quoteFullTableName(fullName)
	if err != nil {
		return err
	}

	sql := g.R(e.DbConn.GetTemplateValue("core.drop_view"), "view", quoted)
	if cascade && supportsDropCascade(e.DbConn.GetType()) {
		if !strings.Contains(strings.ToUpper(sql), "CASCADE") {
			sql = strings.TrimRight(sql, "; \t\n") + " CASCADE"
		}
	}

	if _, err := e.DbConn.Exec(sql); err != nil {
		errLower := strings.ToLower(err.Error())
		ignore := e.DbConn.Template().Variable["error_ignore_drop_view"]
		if ignore != "" && strings.Contains(errLower, strings.ToLower(ignore)) {
			return nil
		}
		if strings.Contains(errLower, "does not exist") ||
			strings.Contains(errLower, "unknown") ||
			strings.Contains(errLower, "cannot find") ||
			strings.Contains(errLower, "not found") {
			return nil
		}
		if !cascade && (strings.Contains(errLower, "depend") ||
			strings.Contains(errLower, "referenced by")) {
			return g.Error(err, "could not drop view %s (dependent objects may exist; set drop_cascade: true to force)", fullName)
		}
		return g.Error(err, "could not drop view %s", fullName)
	}
	return nil
}

// truncateTable truncates via the dialect template.
func (e *Executor) truncateTable(fullName string) error {
	quoted, err := e.quoteFullTableName(fullName)
	if err != nil {
		return err
	}
	sql := g.R(e.DbConn.GetTemplateValue("core.truncate_table"), "table", quoted)
	if strings.TrimSpace(sql) == "" {
		sql = g.F("TRUNCATE TABLE %s", quoted)
	}
	_, err = e.DbConn.Exec(sql)
	return err
}

// insertSelect inserts the result of a SELECT into an existing table.
// ClickHouse rejects parens around the SELECT.
func (e *Executor) insertSelect(model *Model, selectSQL string) (uint64, error) {
	quoted, err := e.quoteFullTableName(model.FullTableName)
	if err != nil {
		return 0, err
	}
	if e.isClickHouse() {
		return rowsFromExec(e.execModel(model, g.F("INSERT INTO %s %s", quoted, selectSQL)))
	}
	return rowsFromExec(e.execModel(model, g.F("INSERT INTO %s (%s)", quoted, selectSQL)))
}

// createTableAs creates a table from a SELECT, dialect-aware.
// model may be nil for temp tables that don't need ClickHouse engine config.
func (e *Executor) createTableAs(fullName, selectSQL string, model *Model) (uint64, error) {
	quoted, err := e.quoteFullTableName(fullName)
	if err != nil {
		return 0, err
	}
	dbType := e.DbConn.GetType()

	if e.isClickHouse() {
		table := newCHTable(model, e.DbConn.Quote, dbType == dbio.TypeDbClickhouse)
		return rowsFromExec(e.execModel(model, g.F("CREATE TABLE %s %s AS (%s)", quoted, table.clause(), selectSQL)))
	}

	if isSQLServerFamily(dbType) {
		// SQL Server has no CTAS; use SELECT INTO
		return rowsFromExec(e.DbConn.Exec(g.F("SELECT * INTO %s FROM (%s) AS _sling_src", quoted, selectSQL)))
	}

	// Standard CTAS (Postgres, MySQL, Snowflake, BigQuery, DuckDB, …)
	return rowsFromExec(e.DbConn.Exec(g.F("CREATE TABLE %s AS (%s)", quoted, selectSQL)))
}

// createOrReplaceTableAs atomically rebuilds a table from a SELECT when the
// dialect supports CREATE OR REPLACE TABLE.
func (e *Executor) createOrReplaceTableAs(fullName, selectSQL string, model *Model) (uint64, error) {
	quoted, err := e.quoteFullTableName(fullName)
	if err != nil {
		return 0, err
	}
	if e.isClickHouse() {
		// ClickHouse: drop + create (atomic path uses rename elsewhere)
		return e.createTableAs(fullName, selectSQL, model)
	}
	return rowsFromExec(e.DbConn.Exec(g.F("CREATE OR REPLACE TABLE %s AS (%s)", quoted, selectSQL)))
}

// createOrReplaceView creates/replaces a view, dialect-aware.
func (e *Executor) createOrReplaceView(fullName, selectSQL string) (uint64, error) {
	quoted, err := e.quoteFullTableName(fullName)
	if err != nil {
		return 0, err
	}
	dbType := e.DbConn.GetType()

	if isSQLServerFamily(dbType) {
		// SQL Server 2016+: CREATE OR ALTER VIEW
		return rowsFromExec(e.DbConn.Exec(g.F("CREATE OR ALTER VIEW %s AS %s", quoted, selectSQL)))
	}

	return rowsFromExec(e.DbConn.Exec(g.F("CREATE OR REPLACE VIEW %s AS (%s)", quoted, selectSQL)))
}

// renameTable renames oldFull → newFull using the dialect template.
// newFull may be schema-qualified; for dialects that want a bare name in
// RENAME TO, only the name component is used.
func (e *Executor) renameTable(oldFull, newFull string) error {
	oldQuoted, err := e.quoteFullTableName(oldFull)
	if err != nil {
		return err
	}
	newTable, err := database.ParseTableName(newFull, e.DbConn.GetType())
	if err != nil {
		return g.Error(err, "could not parse table name '%s'", newFull)
	}

	tpl := e.DbConn.GetTemplateValue("core.rename_table")
	if strings.TrimSpace(tpl) == "" {
		return g.Error("database %s does not support table rename", e.DbConn.GetType())
	}

	// ClickHouse RENAME TABLE a TO b wants fully-qualified both sides.
	// Postgres ALTER TABLE a RENAME TO b wants bare new name.
	var newRef string
	if e.isClickHouse() {
		newRef = newTable.FDQN()
	} else {
		newRef = e.DbConn.Quote(newTable.Name)
	}

	sql := g.R(tpl, "table", oldQuoted, "new_table", newRef)
	_, err = e.DbConn.Exec(sql)
	return err
}

// sql renders the settings as "k1 = v1, k2 = v2", sorted by key.
func (v SettingValues) sql() string {
	parts := make([]string, 0, len(v))
	for _, key := range sortedKeys(v) {
		val := v[key]
		if s, ok := val.(string); ok {
			val = "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
		}
		parts = append(parts, key+" = "+settingScalar(val))
	}
	return strings.Join(parts, ", ")
}

// wire returns the values that the driver sends, as strings.
// It returns a new map on each call.
func (v SettingValues) wire() map[string]string {
	out := make(map[string]string, len(v))
	for key, val := range v {
		out[key] = settingScalar(val)
	}
	return out
}

// settingScalar renders a string, number or bool value without quotes.
func settingScalar(val any) string {
	switch x := val.(type) {
	case string:
		return x
	case bool:
		if x {
			return "1"
		}
		return "0"
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return fmt.Sprint(val)
}

// chTable holds the ClickHouse clauses between the table name and AS.
type chTable struct {
	engine      string // without "ENGINE = "
	partitionBy string
	primaryKey  string
	orderBy     string // expression without "ORDER BY "
	ttl         string
	settings    SettingValues
}

// newCHTable builds the clauses of the final model table. keys is false on
// Proton: only the engine and the unique key apply.
func newCHTable(model *Model, quote func(string) string, keys bool) chTable {
	t := chTable{engine: "MergeTree()", orderBy: "tuple()"}
	if model == nil {
		return t
	}
	c := model.Config
	if c.Engine != "" {
		t.engine = c.Engine
	}
	if keys {
		t.partitionBy, t.primaryKey, t.ttl = c.PartitionBy.SQL(), c.PrimaryKey, c.TTL
		if len(c.PartitionBy) > 1 {
			t.partitionBy = "(" + t.partitionBy + ")"
		}
		t.settings = c.Settings.Table.merge(nil)
	}

	if keys && c.OrderBy != "" {
		t.orderBy = c.OrderBy
	} else if uniqueKeys := getUniqueKeys(model); len(uniqueKeys) > 0 {
		quoted := make([]string, len(uniqueKeys))
		for i, k := range uniqueKeys {
			quoted[i] = k
			if quote != nil {
				quoted[i] = quote(k)
			}
		}
		t.orderBy = "(" + strings.Join(quoted, ", ") + ")"
	}

	// MergeTree sorting keys cannot be nullable. allow_nullable_key lets
	// incremental unique_key columns work without wrapping every SELECT
	// in assumeNotNull
	if !strings.Contains(strings.ToUpper(t.orderBy), "TUPLE()") {
		if _, set := t.settings["allow_nullable_key"]; !set {
			if t.settings == nil {
				t.settings = SettingValues{}
			}
			t.settings["allow_nullable_key"] = 1
		}
	}
	return t
}

// newCHTempTable builds the clauses of a staging table. keys is false on Proton.
func newCHTempTable(model *Model, keys bool) chTable {
	t := chTable{engine: "MergeTree()", orderBy: "tuple()"}
	if model != nil && keys {
		t.settings = model.Config.Settings.Table.merge(nil)
	}
	return t
}

// clause renders the clauses in the order that ClickHouse requires.
func (t chTable) clause() string {
	parts := []string{"ENGINE = " + t.engine}
	if t.partitionBy != "" {
		parts = append(parts, "PARTITION BY "+t.partitionBy)
	}
	if t.primaryKey != "" {
		parts = append(parts, "PRIMARY KEY "+t.primaryKey)
	}
	parts = append(parts, "ORDER BY "+t.orderBy)
	if t.ttl != "" {
		parts = append(parts, "TTL "+t.ttl)
	}
	if len(t.settings) > 0 {
		parts = append(parts, "SETTINGS "+t.settings.sql())
	}
	return strings.Join(parts, " ")
}

// querySettingsConn is a target conn that sends query settings with each statement.
type querySettingsConn interface {
	QuerySettingsContext(ctx context.Context, settings map[string]string) context.Context
}

// querySettings returns the conn and the settings to send with a statement of
// the model. ok is false when the statement must run with a plain call.
func (e *Executor) querySettings(model *Model) (conn querySettingsConn, settings SettingValues, ok bool) {
	if model == nil || len(model.Config.Settings.Query) == 0 {
		return nil, nil, false
	}
	conn, ok = e.DbConn.(querySettingsConn)
	return conn, model.Config.Settings.Query, ok
}

// execModel runs one statement of the model.
func (e *Executor) execModel(model *Model, q string) (sql.Result, error) {
	conn, settings, ok := e.querySettings(model)
	if !ok {
		return e.DbConn.Exec(q)
	}
	ctx := conn.QuerySettingsContext(e.DbConn.Context().Ctx, settings.wire())
	result, err := e.DbConn.ExecContext(ctx, q)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return result, err
}

// execModelMulti runs a multi-statement string (merge SQL) of the model.
// Each statement gets the settings.
func (e *Executor) execModelMulti(model *Model, q string) (sql.Result, error) {
	conn, settings, ok := e.querySettings(model)
	if !ok {
		return e.DbConn.ExecMulti(q)
	}
	ctx := conn.QuerySettingsContext(e.DbConn.Context().Ctx, settings.wire())
	result, err := e.DbConn.ExecMultiContext(ctx, q)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return result, err
}

// ctasDialect reads the CTAS templates of one dialect.
type ctasDialect struct {
	t dbio.Type
}

func newCTASDialect(t dbio.Type) ctasDialect {
	return ctasDialect{t: t}
}

// has reports whether the merged template has a non-empty value for key.
func (d ctasDialect) has(key string) bool {
	return strings.TrimSpace(d.t.GetTemplateValue(key)) != ""
}

// unsupported returns the keys that c sets and the dialect ignores.
func (d ctasDialect) unsupported(c ModelConfig) []string {
	clickhouse := d.t == dbio.TypeDbClickhouse
	keys := []struct {
		name      string
		set       bool
		supported bool
	}{
		{"order_by", c.OrderBy != "", clickhouse},
		{"partition_by", len(c.PartitionBy) > 0, clickhouse || d.has("core.ctas_partition_by")},
		{"cluster_by", len(c.ClusterBy) > 0, d.has("core.ctas_cluster_by")},
		{"primary_key", c.PrimaryKey != "", clickhouse},
		{"ttl", c.TTL != "", clickhouse},
		{"transient", c.Transient != nil, d.has("core.ctas_transient")},
		{"unlogged", c.Unlogged != nil, d.has("core.ctas_unlogged")},
		{"dist", c.Dist != "", d.has("core.ctas_dist_style")},
		{"sort", len(c.Sort) > 0, d.has("core.ctas_sort")},
		{"settings.table", len(c.Settings.Table) > 0, clickhouse || d.has("core.ctas_options")},
		{"settings.query", len(c.Settings.Query) > 0, clickhouse || d.has("variable.query_settings_scope")},
	}
	var out []string
	for _, k := range keys {
		if k.set && !k.supported {
			out = append(out, k.name)
		}
	}
	return out
}
