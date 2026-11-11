package build

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
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
// Athena has the template, but its Hive rename fails ("Database does not exist").
func (e *Executor) supportsRenameTable() bool {
	return strings.TrimSpace(e.DbConn.GetTemplateValue("core.rename_table")) != "" &&
		e.DbConn.GetType() != dbio.TypeDbAthena
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
// model may be nil. role tells whether the table gets the layout of the model.
func (e *Executor) createTableAs(fullName, selectSQL string, model *Model, role tableRole) (uint64, error) {
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

	target, err := database.ParseTableName(fullName, dbType)
	if err != nil {
		return 0, g.Error(err, "could not parse table name '%s'", fullName)
	}
	d := newCTASDialect(dbType, e.DbConn.Props()).withSalt()
	t, err := newCTASTable(model, d, role, target)
	if err != nil {
		return 0, err
	}
	return rowsFromExec(e.execModel(model, t.render(d.t.GetTemplateValue("core.create_table_as"), d.tableName(target, quoted), selectSQL)))
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
		return e.createTableAs(fullName, selectSQL, model, tableFinal)
	}
	target, err := database.ParseTableName(fullName, e.DbConn.GetType())
	if err != nil {
		return 0, g.Error(err, "could not parse table name '%s'", fullName)
	}
	d := newCTASDialect(e.DbConn.GetType(), e.DbConn.Props()).withSalt()
	t, err := newCTASTable(model, d, tableFinal, target)
	if err != nil {
		return 0, err
	}
	return rowsFromExec(e.execModel(model, t.render(d.t.GetTemplateValue("core.create_or_replace_table_as"), d.tableName(target, quoted), selectSQL)))
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
// Only ClickHouse sends settings through the driver context.
func (e *Executor) querySettings(model *Model) (conn querySettingsConn, settings SettingValues, ok bool) {
	if model == nil || len(model.Config.Settings.Query) == 0 {
		return nil, nil, false
	}
	if !e.isClickHouse() {
		return nil, nil, false
	}
	conn, ok = e.DbConn.(querySettingsConn)
	return conn, model.Config.Settings.Query, ok
}

// execModel runs one statement of the model. ClickHouse sends the settings
// with the driver context. Other dialects with a scope run the statement in
// one session or transaction.
func (e *Executor) execModel(model *Model, q string) (sql.Result, error) {
	if conn, settings, ok := e.querySettings(model); ok {
		ctx := conn.QuerySettingsContext(e.DbConn.Context().Ctx, settings.wire())
		result, err := e.DbConn.ExecContext(ctx, q)
		if err != nil {
			err = g.Error(err, "Could not execute SQL")
		}
		return result, err
	}
	if scope := e.queryScope(model); scope != "" {
		return e.execScoped(model, scope, []string{q})
	}
	return e.DbConn.Exec(q)
}

// execModelMulti runs a multi-statement string (merge SQL) of the model.
// All statements get the same settings.
func (e *Executor) execModelMulti(model *Model, q string) (sql.Result, error) {
	if conn, settings, ok := e.querySettings(model); ok {
		ctx := conn.QuerySettingsContext(e.DbConn.Context().Ctx, settings.wire())
		result, err := e.DbConn.ExecMultiContext(ctx, q)
		if err != nil {
			err = g.Error(err, "Could not execute SQL")
		}
		return result, err
	}
	if scope := e.queryScope(model); scope != "" {
		return e.execScoped(model, scope, database.ParseSQLMultiStatements(q, e.DbConn.GetType()))
	}
	return e.DbConn.ExecMulti(q)
}

// queryScope returns the settings.query scope of the model on this target,
// or "" when the statement must run with a plain call.
func (e *Executor) queryScope(model *Model) string {
	if model == nil || len(model.Config.Settings.Query) == 0 {
		return ""
	}
	return newCTASDialect(e.DbConn.GetType(), e.DbConn.Props()).querySettingsScope()
}

// execScoped runs the statements in one session or transaction, after the
// settings. A session scope resets the settings at the end. A failed scope
// drops its session.
func (e *Executor) execScoped(model *Model, scope string, stmts []string) (result sql.Result, err error) {
	if e.openSession == nil {
		return nil, g.Error("Could not execute SQL: settings.query needs a database/sql connection")
	}
	d := newCTASDialect(e.DbConn.GetType(), e.DbConn.Props())
	vals := model.Config.Settings.Query
	sets, err := d.setStatements(vals)
	if err != nil {
		return nil, g.Error(err, "Could not execute SQL")
	}

	session, err := e.openSession(e.DbConn.Context().Ctx, scope)
	if err != nil {
		return nil, g.Error(err, "Could not execute SQL")
	}
	ctx := e.DbConn.Context().Ctx

	var rows int64
	for _, q := range sets {
		if _, err = session.ExecContext(ctx, q); err != nil {
			break
		}
	}
	if err == nil {
		for _, q := range stmts {
			if strings.TrimSpace(q) == "" {
				continue
			}
			var res sql.Result
			if res, err = session.ExecContext(ctx, q); err != nil {
				break
			}
			if n, rerr := res.RowsAffected(); rerr == nil {
				rows += n
			}
		}
	}
	if err == nil && scope == "session" {
		for _, q := range d.resetStatements(vals) {
			if _, err = session.ExecContext(ctx, q); err != nil {
				break
			}
		}
	}

	if cerr := session.Close(err == nil); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, g.Error(err, "Could not execute SQL")
	}
	return sessionResult(rows), nil
}

// modelSession runs the statements of one call in one scope.
type modelSession interface {
	ExecContext(ctx context.Context, query string) (sql.Result, error)
	Close(ok bool) error
}

// sqlSession is a modelSession on a database/sql pool.
type sqlSession struct {
	conn  *sqlx.Conn
	tx    *sqlx.Tx
	props map[string]string
}

// openSQLSession pins one pool conn (scope session) or starts one local
// transaction (scope transaction).
func openSQLSession(ctx context.Context, db *sqlx.DB, props map[string]string, scope string) (*sqlSession, error) {
	if db == nil {
		return nil, g.Error("settings.query needs a database/sql connection")
	}
	s := &sqlSession{props: props}
	var err error
	switch scope {
	case "transaction":
		if s.tx, err = db.BeginTxx(ctx, nil); err != nil {
			return nil, g.Error(err, "could not begin transaction")
		}
	case "session":
		if s.conn, err = db.Connx(ctx); err != nil {
			return nil, g.Error(err, "could not get a connection")
		}
	default:
		return nil, g.Error("unknown query_settings_scope '%s'", scope)
	}
	return s, nil
}

func (s *sqlSession) ExecContext(ctx context.Context, query string) (sql.Result, error) {
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	env.LogSQL(s.props, query)
	if s.tx != nil {
		return s.tx.ExecContext(ctx, query)
	}
	return s.conn.ExecContext(ctx, query)
}

// Close ends the scope. A failed session conn is discarded, so a value that
// was not reset cannot reach another model.
func (s *sqlSession) Close(ok bool) error {
	if s.tx != nil {
		if ok {
			return s.tx.Commit()
		}
		return s.tx.Rollback()
	}
	if !ok {
		_ = s.conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	if err := s.conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		return err
	}
	return nil
}

// sessionResult is the sum of the rows of the statements of one scope.
type sessionResult int64

func (r sessionResult) LastInsertId() (int64, error) { return 0, nil }
func (r sessionResult) RowsAffected() (int64, error) { return int64(r), nil }

// ctasDialect reads the CTAS templates of one dialect.
type ctasDialect struct {
	t     dbio.Type
	props map[string]string // connection properties; nil when unknown
	salt  string            // path part that makes a default location new on each run
}

func newCTASDialect(t dbio.Type, props map[string]string) ctasDialect {
	return ctasDialect{t: t, props: props}
}

// withSalt returns d with a new salt. A dropped Hive table on Athena keeps its
// data, so a default location that is reused makes the next CTAS fail.
func (d ctasDialect) withSalt() ctasDialect {
	d.salt = time.Now().UTC().Format("20060102T150405") + g.RandString(g.AlphaRunesLower, 4)
	return d
}

// tableName returns the table name for a CTAS. A dialect with
// variable.ctas_quote_char quotes the name with it (Athena DML takes
// double quotes, not the backticks of its DDL).
func (d ctasDialect) tableName(t database.Table, quoted string) string {
	q := d.t.GetTemplateValue("variable.ctas_quote_char")
	if q == "" {
		return quoted
	}
	parts := make([]string, 0, 3)
	for _, p := range []string{t.Database, t.Schema, t.Name} {
		if p != "" {
			parts = append(parts, q+p+q)
		}
	}
	return strings.Join(parts, ".")
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

// tableRole tells which table a CTAS makes.
type tableRole int

const (
	tableFinal tableRole = iota // the model table, or the Path B tmp table that becomes it
	tableStage                  // a staging _sling_build_tmp_* table
)

// ctasTable holds the rendered CTAS parts of one table.
type ctasTable struct {
	Kind, PartitionBy, ClusterBy, Dist, Sort, Options string
}

// newCTASTable renders the CTAS parts of the model for the target table.
// A nil model or a staging table gets no user part, only the default
// options of the dialect.
func newCTASTable(model *Model, d ctasDialect, role tableRole, target database.Table) (t ctasTable, err error) {
	var user SettingValues
	if model != nil && role == tableFinal {
		user = model.Config.Settings.Table
		t.layout(model, d)
	}

	extra := d.defaultOptions(user, target)
	if t.Options, err = d.options(user, extra); err != nil {
		return t, g.Error("model '%s': %s", model.Name, err.Error())
	}
	return t, nil
}

// layout renders the kind, layout and flag parts of the model.
func (t *ctasTable) layout(model *Model, d ctasDialect) {
	c := model.Config
	part := func(key string, kv ...string) string {
		tmpl := d.t.GetTemplateValue(key)
		if strings.TrimSpace(tmpl) == "" {
			return ""
		}
		return g.R(tmpl, kv...)
	}

	var kinds []string
	if c.Transient != nil && *c.Transient {
		kinds = append(kinds, part("core.ctas_transient"))
	}
	if c.Unlogged != nil && *c.Unlogged {
		kinds = append(kinds, part("core.ctas_unlogged"))
	}
	t.Kind = strings.Join(lo.Compact(kinds), " ")

	if len(c.PartitionBy) > 0 {
		t.PartitionBy = part("core.ctas_partition_by", "expr", c.PartitionBy.SQL())
	}
	if len(c.ClusterBy) > 0 {
		t.ClusterBy = part("core.ctas_cluster_by", "expr", c.ClusterBy.SQL())
	}
	if len(c.Sort) > 0 {
		t.Sort = part("core.ctas_sort", "expr", c.Sort.SQL())
	}
	if dist := strings.TrimSpace(c.Dist); dist != "" {
		switch strings.ToLower(dist) {
		case "even", "all", "auto":
			t.Dist = part("core.ctas_dist_style", "style", strings.ToUpper(dist))
		default:
			t.Dist = part("core.ctas_dist_key", "expr", dist)
		}
	}
}

// render fills the template. Only the skeleton collapses: a part that is
// empty drops its token, and the values keep their spaces.
func (t ctasTable) render(tmpl, table, selectSQL string) string {
	tokens := strings.Fields(tmpl)
	out := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		tok = g.R(tok,
			"kind", t.Kind, "partition_by", t.PartitionBy, "cluster_by", t.ClusterBy,
			"dist", t.Dist, "sort", t.Sort, "options", t.Options,
			"table", table, "sql", selectSQL,
		)
		if tok != "" {
			out = append(out, tok)
		}
	}
	return strings.Join(out, " ")
}

// clause returns the layout and option parts of the template that are not
// empty. It leaves out the kind, the table and the SQL.
func (t ctasTable) clause(tmpl string) string {
	var out []string
	for _, tok := range strings.Fields(tmpl) {
		if !strings.ContainsAny(tok, "{") || strings.Contains(tok, "{kind}") ||
			strings.Contains(tok, "{table}") || strings.Contains(tok, "{sql}") {
			continue
		}
		tok = g.R(tok,
			"partition_by", t.PartitionBy, "cluster_by", t.ClusterBy,
			"dist", t.Dist, "sort", t.Sort, "options", t.Options,
		)
		if tok != "" {
			out = append(out, tok)
		}
	}
	return strings.Join(out, " ")
}

// literal renders a setting value as SQL. option is true for a value in
// the options of the table.
func (d ctasDialect) literal(v any, option bool) (string, error) {
	quoteAll := cast.ToBool(d.t.GetTemplateValue("variable.ctas_option_quote_all"))
	switch x := v.(type) {
	case string:
		return d.quote(x), nil
	case bool, int, int64, uint64, float32, float64:
		s := settingScalar(x)
		if b, ok := x.(bool); ok {
			s = strconv.FormatBool(b)
		}
		if option && quoteAll {
			return d.quote(s), nil
		}
		return s, nil
	case []any:
		arr := d.t.GetTemplateValue("core.ctas_array")
		if strings.TrimSpace(arr) == "" {
			return "", fmt.Errorf("lists are not supported on %s", d.t)
		}
		items := make([]string, len(x))
		for i, item := range x {
			s, err := d.literal(item, false)
			if err != nil {
				return "", err
			}
			items[i] = s
		}
		return g.R(arr, "values", strings.Join(items, ", ")), nil
	case map[string]any:
		arr := d.t.GetTemplateValue("core.ctas_array")
		entry := d.t.GetTemplateValue("core.ctas_map_entry")
		if strings.TrimSpace(arr) == "" || strings.TrimSpace(entry) == "" {
			return "", fmt.Errorf("maps are not supported on %s", d.t)
		}
		items := make([]string, 0, len(x))
		for _, key := range sortedKeys(x) {
			val, err := d.literal(x[key], false)
			if err != nil {
				return "", err
			}
			items = append(items, g.R(entry, "key", d.quote(key), "value", val))
		}
		return g.R(arr, "values", strings.Join(items, ", ")), nil
	}
	return "", fmt.Errorf("value type %T is not supported", v)
}

// quote renders a string literal with the quote and escape of the dialect.
func (d ctasDialect) quote(s string) string {
	q := d.t.GetTemplateValue("variable.literal_quote")
	esc := d.t.GetTemplateValue("variable.literal_escape")
	pairs := []string{q, esc}
	if strings.HasPrefix(esc, `\`) {
		pairs = append([]string{`\`, `\\`}, pairs...)
	}
	return q + strings.NewReplacer(pairs...).Replace(s) + q
}

// options renders the table options of the dialect, or "" when there is
// nothing to render.
func (d ctasDialect) options(vals SettingValues, extra string) (string, error) {
	tmpl := d.t.GetTemplateValue("core.ctas_options")
	if (len(vals) == 0 && extra == "") || strings.TrimSpace(tmpl) == "" {
		return "", nil
	}
	optTmpl := d.t.GetTemplateValue("variable.ctas_option")
	pairs := make([]string, 0, len(vals))
	for _, key := range sortedKeys(vals) {
		val, err := d.literal(vals[key], true)
		if err != nil {
			return "", fmt.Errorf("settings.table.%s: %s", key, err)
		}
		pairs = append(pairs, g.R(optTmpl, "key", key, "value", val))
	}
	if extra != "" {
		pairs = append(pairs, extra)
	}
	return g.R(tmpl, "options", strings.Join(pairs, d.t.GetTemplateValue("variable.ctas_option_sep"))), nil
}

// defaultOptions renders the options that Sling adds when the user did not
// set them: the location of the table, from the connection data_location.
// It returns "" when the dialect or the connection has no default.
func (d ctasDialect) defaultOptions(vals SettingValues, target database.Table) string {
	base := strings.TrimSuffix(d.props["data_location"], "/")
	if base == "" {
		return ""
	}
	for key := range vals {
		if g.In(strings.ToLower(key), "location", "external_location") {
			return ""
		}
	}

	const prefix = "variable.ctas_default_options"
	key := prefix
	if tt := strings.ToLower(cast.ToString(vals["table_type"])); tt != "" && d.has(prefix+"_"+tt) {
		key = prefix + "_" + tt
	}
	tmpl := d.t.GetTemplateValue(key)
	if strings.TrimSpace(tmpl) == "" {
		return ""
	}
	path := base + "/" + target.Schema + "/" + target.Name + "/"
	if d.salt != "" {
		path += d.salt + "/"
	}
	loc, _ := d.literal(path, false)
	return g.R(tmpl, "location", loc)
}

// querySettingsScope returns the settings.query scope of the dialect:
// "session", "transaction" or "" when it has none.
func (d ctasDialect) querySettingsScope() string {
	return strings.TrimSpace(d.t.GetTemplateValue("variable.query_settings_scope"))
}

// setStatements renders the statements that set the values, sorted by key.
func (d ctasDialect) setStatements(vals SettingValues) ([]string, error) {
	tmpl := d.t.GetTemplateValue("core.set_query_setting")
	out := make([]string, 0, len(vals))
	for _, key := range sortedKeys(vals) {
		val, err := d.literal(vals[key], false)
		if err != nil {
			return nil, fmt.Errorf("settings.query.%s: %s", key, err)
		}
		out = append(out, g.R(tmpl, "key", key, "value", val))
	}
	return out, nil
}

// resetStatements renders the statements that reset the values. It returns
// nil when the dialect has no reset template (transaction scope).
func (d ctasDialect) resetStatements(vals SettingValues) []string {
	tmpl := d.t.GetTemplateValue("core.reset_query_setting")
	if strings.TrimSpace(tmpl) == "" {
		return nil
	}
	out := make([]string, 0, len(vals))
	for _, key := range sortedKeys(vals) {
		out = append(out, g.R(tmpl, "key", key))
	}
	return out
}
