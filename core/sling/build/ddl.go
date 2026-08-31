package build

import (
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

// quotedName returns just the quoted bare table name (no schema).
func (e *Executor) quotedName(name string) string {
	return e.DbConn.Quote(name)
}

// supportsDropCascade reports whether the dialect accepts CASCADE on DROP.
func supportsDropCascade(t dbio.Type) bool {
	return g.In(t,
		dbio.TypeDbPostgres, dbio.TypeDbRedshift,
		dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck, dbio.TypeDbDuckLake,
		dbio.TypeDbSnowflake, // accepted (no-op-ish) but harmless
	)
}

// supportsCreateOrReplaceTable reports dialects with atomic CREATE OR REPLACE TABLE.
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
func (e *Executor) insertSelect(fullName, selectSQL string) (uint64, error) {
	quoted, err := e.quoteFullTableName(fullName)
	if err != nil {
		return 0, err
	}
	return rowsFromExec(e.DbConn.Exec(g.F("INSERT INTO %s (%s)", quoted, selectSQL)))
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
		engineClause := "ENGINE = Memory"
		orderByClause := "ORDER BY tuple()"
		settings := ""
		if model != nil {
			engineClause = e.getEngineClause(model)
			orderByClause = e.getOrderByClause(model)
			// MergeTree sorting keys cannot be nullable. allow_nullable_key
			// lets incremental unique_key columns work without wrapping every
			// SELECT in assumeNotNull
			if !strings.Contains(strings.ToUpper(orderByClause), "TUPLE()") {
				settings = " SETTINGS allow_nullable_key = 1"
			}
		}
		return rowsFromExec(e.DbConn.Exec(g.F("CREATE TABLE %s %s %s%s AS (%s)", quoted, engineClause, orderByClause, settings, selectSQL)))
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

// bareTableName extracts the unqualified table name from schema.table.
func bareTableName(fullName string) string {
	if idx := strings.LastIndex(fullName, "."); idx >= 0 {
		return fullName[idx+1:]
	}
	return fullName
}

// schemaOf extracts the schema from schema.table.
func schemaOf(fullName string) string {
	if idx := strings.LastIndex(fullName, "."); idx >= 0 {
		return fullName[:idx]
	}
	return ""
}
