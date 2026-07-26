package database

import (
	"strings"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

type TableKeys map[iop.KeyType][]string

// indexEntryPrefix marks an index entry whose original YAML/JSON value was a
// composite list or a complex map (rather than a plain column name). The
// original value is JSON-encoded after the prefix so it survives the
// []string-typed TableKeys map and can be decoded by ParseIndexes.
const indexEntryPrefix = "\x00idx:"

// uniqueEntryPrefix marks a multi-column unique group (unique: [[a, b]]).
const uniqueEntryPrefix = "\x00uq:"

// UnmarshalJSON allows the `index` key to accept rich entry shapes (bare column
// names, composite lists, named simple maps, named complex maps) while keeping
// every other key as a plain []string. Complex index entries are JSON-encoded
// behind indexEntryPrefix so the map stays []string-typed for all existing code.
// Nested unique lists are preserved as composite groups (unique: [[a, b]]).
// Other keys (primary, partition, ...) flatten nested lists so primary: [[a, b]]
// is treated identically to primary: [a, b].
func (tk *TableKeys) UnmarshalJSON(data []byte) error {
	raw := map[iop.KeyType]any{}
	if err := g.JSONUnmarshal(data, &raw); err != nil {
		return g.Error(err, "could not unmarshal table_keys")
	}

	out := TableKeys{}
	for kt, val := range raw {
		if kt == iop.IndexKey {
			entries, ok := val.([]any)
			if !ok {
				// allow a single non-list value
				entries = []any{val}
			}
			encoded := []string{}
			for _, entry := range entries {
				switch e := entry.(type) {
				case string:
					encoded = append(encoded, e)
				default:
					// composite list or complex map -> encode behind prefix
					encoded = append(encoded, indexEntryPrefix+g.Marshal(e))
				}
			}
			out[kt] = encoded
			continue
		}

		if kt == iop.UniqueKey {
			out[kt] = encodeUniqueEntries(val)
			continue
		}

		// primary/partition/...: flatten nested lists (primary: [[a, b]] == [a, b])
		switch v := val.(type) {
		case []any:
			out[kt] = flattenToStrings(v)
		case nil:
			out[kt] = []string{}
		default:
			out[kt] = []string{cast.ToString(v)}
		}
	}

	*tk = out
	return nil
}

func encodeUniqueEntries(val any) []string {
	entries, ok := val.([]any)
	if !ok {
		if val == nil {
			return []string{}
		}
		entries = []any{val}
	}

	encoded := []string{}
	for _, entry := range entries {
		switch e := entry.(type) {
		case string:
			s := strings.TrimSpace(e)
			if s != "" {
				encoded = append(encoded, s)
			}
		case []any:
			cols := flattenToStrings(e)
			switch len(cols) {
			case 0:
			case 1:
				encoded = append(encoded, cols[0])
			default:
				encoded = append(encoded, uniqueEntryPrefix+g.Marshal(cols))
			}
		default:
			s := strings.TrimSpace(cast.ToString(e))
			if s != "" {
				encoded = append(encoded, s)
			}
		}
	}
	return encoded
}

// flattenToStrings turns an arbitrarily-nested list value into a flat []string.
// This lets the composite/wrapped list form (primary: [[user_id, brand_id]])
// be accepted for non-index keys and behave the same as a flat list. Empty
// tokens are dropped so a trailing separator does not create a phantom column.
func flattenToStrings(items []any) (out []string) {
	for _, item := range items {
		switch v := item.(type) {
		case []any:
			out = append(out, flattenToStrings(v)...)
		default:
			s := strings.TrimSpace(cast.ToString(v))
			if s != "" {
				out = append(out, s)
			}
		}
	}
	return
}

// UnmarshalYAML delegates to UnmarshalJSON by round-tripping through JSON, so
// the rich `index` entry shapes are handled identically on the YAML config path.
func (tk *TableKeys) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw map[string]any
	if err := unmarshal(&raw); err != nil {
		return g.Error(err, "could not unmarshal table_keys (yaml)")
	}
	// normalize any map[interface{}]interface{} from yaml.v2 to JSON-friendly maps
	return tk.UnmarshalJSON([]byte(g.Marshal(raw)))
}

// IndexColumnNames returns the plain column-name index entries (used for
// key-metadata validation), skipping any complex/encoded entries.
func (tk TableKeys) IndexColumnNames() (names []string) {
	for _, entry := range tk[iop.IndexKey] {
		if !strings.HasPrefix(entry, indexEntryPrefix) {
			names = append(names, entry)
		}
	}
	return
}

// UniqueColumnNames expands unique groups to a flat column list for SetKeys.
func (tk TableKeys) UniqueColumnNames() (names []string) {
	for _, group := range tk.UniqueGroups() {
		names = append(names, group...)
	}
	return
}

// UniqueGroups returns unique key groups (one unique index per group).
func (tk TableKeys) UniqueGroups() (groups [][]string) {
	for _, entry := range tk[iop.UniqueKey] {
		if strings.HasPrefix(entry, uniqueEntryPrefix) {
			var cols []string
			if err := g.Unmarshal(strings.TrimPrefix(entry, uniqueEntryPrefix), &cols); err != nil {
				continue
			}
			if len(cols) > 0 {
				groups = append(groups, cols)
			}
			continue
		}
		if s := strings.TrimSpace(entry); s != "" {
			groups = append(groups, []string{s})
		}
	}
	return
}

// isIndexExpression returns true when the entry is not a plain identifier and
// should be treated as a raw SQL expression (no column-name validation).
func isIndexExpression(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	for _, r := range s {
		if r == '_' || r == '$' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		return true // contains '(', '|', whitespace, etc. -> expression
	}
	return false
}

// indexColumnEntry turns a single column/expression token into an IndexColumn.
// Plain identifiers are validated against knownColumns (unless empty); anything
// else is passed through verbatim as an expression.
func indexColumnEntry(token string, knownColumns iop.Columns) (col iop.IndexColumn, isExpr bool, err error) {
	token = strings.TrimSpace(token)
	if isIndexExpression(token) {
		return iop.IndexColumn{Name: token}, true, nil
	}
	if len(knownColumns) > 0 {
		c := knownColumns.GetColumn(token)
		if c == nil {
			return col, false, g.Error("index references unknown column %q\navailable: %s", token, g.Marshal(knownColumns.Names()))
		}
		// use the resolved column's actual casing (e.g. Oracle folds to upper)
		token = c.Name
	}
	return iop.IndexColumn{Name: token}, false, nil
}

// ParseIndexes normalizes the table_keys.index entries (in all accepted shapes)
// into []iop.IndexDef. tableName is used for auto-naming; knownColumns (when
// provided) validates plain-identifier column references. Expression entries
// (containing non-identifier characters) bypass validation.
func (tk TableKeys) ParseIndexes(tableName string, knownColumns iop.Columns, maxLength int) (defs []iop.IndexDef, err error) {
	for _, entry := range tk[iop.IndexKey] {
		var raw any
		if strings.HasPrefix(entry, indexEntryPrefix) {
			if err = g.Unmarshal(strings.TrimPrefix(entry, indexEntryPrefix), &raw); err != nil {
				return nil, g.Error(err, "could not decode index entry")
			}
		} else {
			raw = entry
		}

		def, err := parseIndexEntry(raw, tableName, knownColumns, maxLength)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, nil
}

// parseIndexEntry parses one normalized index entry value into an IndexDef.
func parseIndexEntry(raw any, tableName string, knownColumns iop.Columns, maxLength int) (def iop.IndexDef, err error) {
	switch v := raw.(type) {
	case string:
		// 1. anonymous single-column (or expression)
		ic, _, err := indexColumnEntry(v, knownColumns)
		if err != nil {
			return def, err
		}
		def.Columns = []iop.IndexColumn{ic}

	case []any:
		// 2./3. anonymous composite (strings and/or expressions)
		for _, item := range v {
			ic, _, err := indexColumnEntry(cast.ToString(item), knownColumns)
			if err != nil {
				return def, err
			}
			def.Columns = append(def.Columns, ic)
		}

	case map[string]any:
		// 4./5./6. named entry (single key)
		if len(v) != 1 {
			return def, g.Error("named index entry must have exactly one key (the index name), got %d", len(v))
		}
		for name, body := range v {
			def.Name = name
			switch b := body.(type) {
			case []any:
				// named simple: value is a column list
				for _, item := range b {
					ic, _, err := indexColumnEntry(cast.ToString(item), knownColumns)
					if err != nil {
						return def, err
					}
					def.Columns = append(def.Columns, ic)
				}
			case map[string]any:
				// named complex
				if err := applyComplexIndexBody(&def, b, knownColumns); err != nil {
					return def, err
				}
			default:
				return def, g.Error("named index %q has unsupported value type %T", name, body)
			}
		}

	default:
		return def, g.Error("unsupported index entry type %T", raw)
	}

	if def.Name == "" {
		parts := def.ColumnNames()
		if def.Expression != "" {
			parts = []string{def.Expression}
		}
		def.Name = iop.MakeIndexName(tableName, parts, maxLength)
	}

	return def, nil
}

// applyComplexIndexBody fills an IndexDef from the complex-form map body.
func applyComplexIndexBody(def *iop.IndexDef, body map[string]any, knownColumns iop.Columns) error {
	for k, val := range body {
		switch strings.ToLower(k) {
		case "columns":
			items, ok := val.([]any)
			if !ok {
				return g.Error("index %q: columns must be a list", def.Name)
			}
			for _, item := range items {
				switch c := item.(type) {
				case string:
					ic, _, err := indexColumnEntry(c, knownColumns)
					if err != nil {
						return err
					}
					def.Columns = append(def.Columns, ic)
				case map[string]any:
					// { name: created_at, sort: desc }
					ic := iop.IndexColumn{
						Name: cast.ToString(c["name"]),
						Sort: strings.ToLower(cast.ToString(c["sort"])),
					}
					if ic.Sort != "" && ic.Sort != "asc" && ic.Sort != "desc" {
						return g.Error("index %q: invalid sort %q (expected asc/desc)", def.Name, ic.Sort)
					}
					// validate plain identifier names only
					if !isIndexExpression(ic.Name) && len(knownColumns) > 0 {
						if col := knownColumns.GetColumn(ic.Name); col == nil {
							return g.Error("index %q references unknown column %q", def.Name, ic.Name)
						}
					}
					def.Columns = append(def.Columns, ic)
				default:
					return g.Error("index %q: unsupported column entry type %T", def.Name, item)
				}
			}
		case "expression":
			def.Expression = cast.ToString(val)
		case "where":
			def.Where = cast.ToString(val)
		case "unique":
			def.Unique = cast.ToBool(val)
		case "type", "using":
			def.Type = strings.ToLower(cast.ToString(val))
		case "include":
			items, ok := val.([]any)
			if !ok {
				return g.Error("index %q: include must be a list", def.Name)
			}
			for _, item := range items {
				def.Include = append(def.Include, cast.ToString(item))
			}
		default:
			return g.Error("index %q: unknown key %q", def.Name, k)
		}
	}

	if def.Expression != "" && len(def.Columns) > 0 {
		return g.Error("index %q: 'columns' and 'expression' are mutually exclusive", def.Name)
	}
	if def.Expression == "" && len(def.Columns) == 0 {
		return g.Error("index %q: must declare either 'columns' or 'expression'", def.Name)
	}

	return nil
}

type TableIndex struct {
	Def   iop.IndexDef
	Table *Table

	// Deprecated convenience accessors retained for back-compat with callers
	// that constructed TableIndex directly; prefer Def.
	Name    string
	Columns iop.Columns
	Unique  bool
}

// indexDef returns the effective IndexDef for this TableIndex, normalizing the
// legacy Name/Columns/Unique fields into Def when Def is empty.
func (ti *TableIndex) indexDef() iop.IndexDef {
	def := ti.Def
	if def.Name == "" && ti.Name != "" {
		def.Name = ti.Name
	}
	if len(def.Columns) == 0 && len(ti.Columns) > 0 {
		for _, c := range ti.Columns {
			def.Columns = append(def.Columns, iop.IndexColumn{Name: c.Name})
		}
	}
	if !def.Unique && ti.Unique {
		def.Unique = true
	}
	return def
}

// CreateDDL renders the CREATE INDEX statement for the dialect; unsupported
// attributes are dropped with a warning, a closed-set type violation returns "".
func (ti *TableIndex) CreateDDL() string {
	dialect := ti.Table.Dialect
	def := ti.indexDef()

	// validate + warn for dropped attributes
	warnings, err := validateIndexForDialect(def, dialect)
	for _, w := range warnings {
		g.Warn(w)
	}
	if err != nil {
		g.Warn(err.Error())
		return ""
	}

	cap := indexCapabilityFor(dialect)
	if cap.noIndexes {
		return "" // engine has no index concept; warning already emitted
	}

	cols := renderIndexColumns(def, dialect)

	// build the optional tokens for create_index_full
	uniqueTok := ""
	if def.Unique && cap.supportsUnique {
		uniqueTok = "unique"
	}

	usingTok := ""
	if def.Type != "" && cap.supportsType {
		// SQL Server maps clustered/nonclustered to a keyword before INDEX, not USING
		if dialect == dbio.TypeDbSQLServer {
			uniqueTok = strings.TrimSpace(uniqueTok + " " + strings.ToUpper(def.Type))
		} else if dialect == dbio.TypeDbOracle && strings.EqualFold(def.Type, "bitmap") {
			uniqueTok = strings.TrimSpace(uniqueTok + " bitmap")
		} else if dialect == dbio.TypeDbBigQuery && strings.EqualFold(def.Type, "search") {
			uniqueTok = strings.TrimSpace(uniqueTok + " search")
		} else {
			usingTok = "using " + def.Type
		}
	}

	includeTok := ""
	if len(def.Include) > 0 && cap.supportsInclude {
		includeTok = g.F("include (%s)", strings.Join(dialect.QuoteNames(def.Include...), ", "))
	}

	whereTok := ""
	if def.Where != "" && cap.supportsWhere {
		whereTok = "where " + def.Where
	}

	// prefer the richer template when the dialect (or base) defines it
	tmpl := dialect.GetTemplateValue("core.create_index_full")
	if tmpl != "" {
		ddl := g.R(
			tmpl,
			"unique", uniqueTok,
			"index", dialect.Quote(def.Name),
			"table", ti.Table.FDQN(),
			"using", usingTok,
			"cols", cols,
			"include", includeTok,
			"where", whereTok,
		)
		return collapseSpaces(ddl)
	}

	// legacy fallback (no extras)
	key := "core.create_index"
	if def.Unique {
		key = "core.create_unique_index"
	}
	return g.R(
		dialect.GetTemplateValue(key),
		"index", dialect.Quote(def.Name),
		"table", ti.Table.FDQN(),
		"cols", cols,
	)
}

// collapseSpaces removes the extra whitespace left by empty interpolation tokens.
func collapseSpaces(s string) string {
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	s = strings.ReplaceAll(s, " (", " (")
	return strings.TrimSpace(s)
}

func (ti *TableIndex) DropDDL() string {
	dialect := ti.Table.Dialect
	def := ti.indexDef()

	return g.R(
		dialect.GetTemplateValue("core.drop_index"),
		"index", dialect.Quote(def.Name),
		"name", def.Name,
		"table", ti.Table.FDQN(),
		"schema", ti.Table.SchemaQ(),
	)
}

// Indexes collects all index definitions for the table from both sources:
//   - inline column modifiers (columns: ... index(...)) on the given columns
//   - target_options.table_keys.index (t.Keys[iop.IndexKey])
//   - legacy single-column index/unique keys marked on columns
//
// Definitions are deduped by name (table_keys wins over inline on conflict).
func (t *Table) Indexes(columns iop.Columns) (indexes []TableIndex) {
	maxLength := cast.ToInt(t.Dialect.GetTemplateValue("variable.max_column_length"))

	seen := map[string]bool{}
	add := func(def iop.IndexDef) {
		key := strings.ToLower(def.Name)
		if seen[key] {
			return
		}
		seen[key] = true
		indexes = append(indexes, TableIndex{Def: def, Table: t})
	}

	// 1. table_keys.index (rich forms)
	if len(t.Keys[iop.IndexKey]) > 0 {
		defs, err := t.Keys.ParseIndexes(t.Name, columns, maxLength)
		if err != nil {
			g.Warn("could not parse table_keys.index: %s", err.Error())
		}
		for _, def := range defs {
			add(def)
		}
	}

	// 2. inline column-modifier indexes
	if inlineDefs, err := columns.CollectInlineIndexes(t.Name, maxLength); err != nil {
		g.Warn("could not collect inline column indexes: %s", err.Error())
	} else {
		for _, def := range inlineDefs {
			add(def)
		}
	}

	// 3. table_keys.unique groups (or UniqueKey column metadata fallback)
	if groups := t.Keys.UniqueGroups(); len(groups) > 0 {
		for _, group := range groups {
			idxCols := make([]iop.IndexColumn, 0, len(group))
			names := make([]string, 0, len(group))
			for _, name := range group {
				if c := columns.GetColumn(name); c != nil {
					name = c.Name
				}
				idxCols = append(idxCols, iop.IndexColumn{Name: name})
				names = append(names, name)
			}
			if len(idxCols) == 0 {
				continue
			}
			add(iop.IndexDef{
				Name:    iop.MakeIndexName(t.Name, names, maxLength),
				Columns: idxCols,
				Unique:  true,
			})
		}
	} else {
		for _, col := range columns.GetKeys(iop.UniqueKey) {
			add(iop.IndexDef{
				Name:    iop.MakeIndexName(t.Name, []string{col.Name}, maxLength),
				Columns: []iop.IndexColumn{{Name: col.Name}},
				Unique:  true,
			})
		}
	}

	return
}

// indexCapability describes, per engine, which index attributes are rendered,
// dropped (with a warning), or rejected.
type indexCapability struct {
	noIndexes bool // no standalone CREATE INDEX; the whole index is a no-op

	supportsWhere   bool // partial index predicate
	supportsInclude bool // covering-index columns
	supportsUnique  bool // UNIQUE keyword

	// supportsType: engine accepts a type/using clause. When typeClosedSet is
	// non-nil, an unknown value is a hard error; else passed through verbatim.
	supportsType  bool
	typeClosedSet []string // recognized type values (lowercased)
}

// Engines absent from this map use indexCapDefault (basic CREATE INDEX).
var indexCapabilities = map[dbio.Type]indexCapability{
	dbio.TypeDbPostgres: {supportsWhere: true, supportsInclude: true, supportsUnique: true, supportsType: true},
	dbio.TypeDbRedshift: {noIndexes: true},
	dbio.TypeDbMySQL:    {supportsUnique: true, supportsType: true, typeClosedSet: []string{"btree", "hash"}},
	dbio.TypeDbMariaDB:  {supportsUnique: true, supportsType: true, typeClosedSet: []string{"btree", "hash"}},
	dbio.TypeDbSQLServer: {
		supportsWhere: true, supportsInclude: true, supportsUnique: true,
		supportsType: true, typeClosedSet: []string{"clustered", "nonclustered"},
	},
	// ClickHouse data-skipping indexes are declared inline in CREATE TABLE
	// (see injectInlineIndexes); standalone CREATE INDEX is unsupported, so the
	// standalone path is a no-op here.
	dbio.TypeDbClickhouse: {noIndexes: true},
	dbio.TypeDbProton:     {noIndexes: true},
	dbio.TypeDbSnowflake:  {noIndexes: true},
	// BigQuery has no plain secondary index (only CREATE SEARCH/VECTOR INDEX,
	// not generated here); treat table_keys.index as a no-op.
	dbio.TypeDbBigQuery: {noIndexes: true},
	// StarRocks indexes (BITMAP/inverted) only apply to specific column/table
	// models and don't fit the generic CREATE INDEX form; no-op for now.
	dbio.TypeDbStarRocks: {noIndexes: true},
	dbio.TypeDbDuckDb:     {supportsUnique: true},
	dbio.TypeDbMotherDuck: {supportsUnique: true},
	dbio.TypeDbDuckLake:   {noIndexes: true}, // DuckLake does not support indexes
	dbio.TypeDbOracle:     {supportsUnique: true, supportsType: true, typeClosedSet: []string{"bitmap"}},
	dbio.TypeDbSQLite:     {supportsWhere: true, supportsUnique: true},
}

var indexCapDefault = indexCapability{supportsUnique: true}

func indexCapabilityFor(dialect dbio.Type) indexCapability {
	if cap, ok := indexCapabilities[dialect]; ok {
		return cap
	}
	return indexCapDefault
}

// validateIndexForDialect checks the IndexDef's attributes against the engine
// capabilities. It returns a (possibly empty) list of one-line warnings for
// dropped attributes, and a hard error only for a closed-set `type` violation.
func validateIndexForDialect(def iop.IndexDef, dialect dbio.Type) (warnings []string, err error) {
	cap := indexCapabilityFor(dialect)

	if cap.noIndexes {
		warnings = append(warnings, g.F("index %q: %s has no secondary indexes; index is ignored", def.Name, dialect))
		return warnings, nil
	}

	if def.Where != "" && !cap.supportsWhere {
		warnings = append(warnings, g.F("index %q: %s does not support partial-index 'where'; clause ignored", def.Name, dialect))
	}
	if len(def.Include) > 0 && !cap.supportsInclude {
		warnings = append(warnings, g.F("index %q: %s does not support covering-index 'include'; clause ignored", def.Name, dialect))
	}
	if def.Unique && !cap.supportsUnique {
		warnings = append(warnings, g.F("index %q: %s does not support unique indexes; treated as non-unique", def.Name, dialect))
	}
	if def.Type != "" {
		if !cap.supportsType {
			warnings = append(warnings, g.F("index %q: %s does not support index 'type'; clause ignored", def.Name, dialect))
		} else if len(cap.typeClosedSet) > 0 && !g.In(strings.ToLower(def.Type), cap.typeClosedSet...) {
			return warnings, g.Error("index %q: %s does not support index type %q (allowed: %s)", def.Name, dialect, def.Type, strings.Join(cap.typeClosedSet, ", "))
		}
	}

	return warnings, nil
}

// renderIndexColumns produces the quoted, sorted column/expression list for an
// index. Plain identifiers are quoted; expressions and explicit sort orders are
// passed through. For an expression index (no Columns), the raw expression is
// returned verbatim.
func renderIndexColumns(def iop.IndexDef, dialect dbio.Type) string {
	if len(def.Columns) == 0 && def.Expression != "" {
		return def.Expression
	}
	parts := []string{}
	for _, c := range def.Columns {
		name := c.Name
		// quote plain identifiers only; leave expressions untouched
		if !isIndexExpression(name) {
			name = dialect.Quote(name)
		}
		if c.Sort != "" {
			name += " " + strings.ToUpper(c.Sort)
		}
		parts = append(parts, name)
	}
	return strings.Join(parts, ", ")
}

// appendIndexesAndComments appends CREATE INDEX and column-comment statements to
// a CREATE TABLE ddl (";\n"-joined). CreateDDL/ColumnCommentsDDL return "" when
// the dialect lacks the capability. Skipped for temporary tables.
func appendIndexesAndComments(ddl string, conn Connection, table Table, data iop.Dataset, temporary bool) string {
	if temporary {
		return ddl
	}

	for _, index := range table.Indexes(data.Columns) {
		if idxDDL := index.CreateDDL(); idxDDL != "" {
			ddl = ddl + ";\n" + idxDDL
		}
	}

	for _, stmt := range table.ColumnCommentsDDL(conn, data.Columns) {
		ddl = ddl + ";\n" + stmt
	}

	return ddl
}

// appendColumnComments appends only column-comment statements (";\n"-joined) to a
// CREATE TABLE ddl, for dialects that support descriptions but not CREATE INDEX.
func appendColumnComments(ddl string, conn Connection, table Table, data iop.Dataset, temporary bool) string {
	if temporary {
		return ddl
	}
	for _, stmt := range table.ColumnCommentsDDL(conn, data.Columns) {
		ddl = ddl + ";\n" + stmt
	}
	return ddl
}
