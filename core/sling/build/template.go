package build

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/flarco/g"
	"github.com/nikolalohinski/gonja/v2/builtins"
	"github.com/nikolalohinski/gonja/v2/config"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/nikolalohinski/gonja/v2/loaders"
	"github.com/spf13/cast"
)

// =============================================================================
// Macros
// =============================================================================

// MacroFile represents a .macros.sql file containing Jinja macro definitions.
type MacroFile struct {
	FilePath string // absolute path
	Dir      string // directory relative to project root ("" for root, "staging", "marts/core")
	RawSQL   string // raw file content with {% macro %} definitions
}

// macroNameRegex extracts macro names from {% macro name(...) %} blocks.
var macroNameRegex = regexp.MustCompile(`\{%-?\s*macro\s+(\w+)\s*\(`)

// collectMacro reads a .macros.sql file and adds it to the project.
func collectMacro(project *BuildProject, absPath string, relDir string) error {
	rawSQL, err := os.ReadFile(absPath)
	if err != nil {
		return g.Error(err, "could not read macro file: %s", absPath)
	}

	// Normalize relDir: use forward slashes, "" for root
	relDir = filepath.ToSlash(relDir)
	if relDir == "." {
		relDir = ""
	}

	project.Macros = append(project.Macros, &MacroFile{
		FilePath: absPath,
		Dir:      relDir,
		RawSQL:   string(rawSQL),
	})

	return nil
}

// GetMacrosForModel returns the concatenated macro SQL applicable to a model.
// Macros are ordered root-first (outermost to innermost scope).
func GetMacrosForModel(project *BuildProject, model *Model) string {
	if len(project.Macros) == 0 {
		return ""
	}

	// Get model's relative directory
	modelDir := filepath.ToSlash(filepath.Dir(model.RelPath))
	if modelDir == "." {
		modelDir = ""
	}

	// Filter applicable macros
	var applicable []*MacroFile
	for _, m := range project.Macros {
		if macroApplies(m.Dir, modelDir) {
			applicable = append(applicable, m)
		}
	}

	if len(applicable) == 0 {
		return ""
	}

	// Sort: by dir depth ascending (root first), then dir name, then filename
	sort.Slice(applicable, func(i, j int) bool {
		di := dirDepth(applicable[i].Dir)
		dj := dirDepth(applicable[j].Dir)
		if di != dj {
			return di < dj
		}
		if applicable[i].Dir != applicable[j].Dir {
			return applicable[i].Dir < applicable[j].Dir
		}
		return filepath.Base(applicable[i].FilePath) < filepath.Base(applicable[j].FilePath)
	})

	// Concatenate macro content
	var parts []string
	for _, m := range applicable {
		parts = append(parts, m.RawSQL)
	}

	return strings.Join(parts, "\n")
}

// macroApplies returns true if a macro defined in macroDir is accessible from modelDir.
func macroApplies(macroDir, modelDir string) bool {
	// Root-level macros are global
	if macroDir == "" {
		return true
	}
	// Same directory
	if macroDir == modelDir {
		return true
	}
	// Model is in a child directory of the macro
	return strings.HasPrefix(modelDir, macroDir+"/")
}

// dirDepth returns the number of path segments (0 for root).
func dirDepth(dir string) int {
	if dir == "" {
		return 0
	}
	return len(strings.Split(dir, "/"))
}

// warnMacroShadows warns when the same macro name is defined at multiple scope levels.
// Always emits (not debug-only) — macro shadowing is a correctness smell.
func warnMacroShadows(project *BuildProject) {
	if len(project.Macros) == 0 {
		return
	}

	// Extract macro names from each file with their directory scope
	type macroLoc struct {
		name     string
		dir      string
		filePath string
	}

	var locs []macroLoc
	for _, m := range project.Macros {
		matches := macroNameRegex.FindAllStringSubmatch(m.RawSQL, -1)
		for _, match := range matches {
			locs = append(locs, macroLoc{
				name:     match[1],
				dir:      m.Dir,
				filePath: m.FilePath,
			})
		}
	}

	// Group by name first (O(n) groups, then O(k²) within each name)
	byName := make(map[string][]macroLoc)
	for _, loc := range locs {
		byName[loc.name] = append(byName[loc.name], loc)
	}

	for name, group := range byName {
		if len(group) < 2 {
			continue
		}
		for i, a := range group {
			for _, b := range group[i+1:] {
				if macroApplies(a.dir, b.dir) && a.dir != b.dir {
					g.Warn("macro '%s' in %s shadows definition in %s", name, b.filePath, a.filePath)
				} else if macroApplies(b.dir, a.dir) && a.dir != b.dir {
					g.Warn("macro '%s' in %s shadows definition in %s", name, a.filePath, b.filePath)
				}
			}
		}
	}
}

// =============================================================================
// SQL Parser
// =============================================================================

// tableRefRegex matches FROM/JOIN followed by a table reference.
// Captures: optional schema (word + dot) + table name.
// Handles optional quoting with double quotes or backticks.
// Ignores subqueries (parentheses after FROM/JOIN).
var tableRefRegex = regexp.MustCompile(
	`(?i)(?:FROM|JOIN)\s+` + // FROM or JOIN keyword
		`(?:(?:LATERAL|NATURAL|LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+)*` + // optional join qualifiers
		`(?:(?:OUTER|INNER)\s+)?` + // another optional qualifier
		`(?:JOIN\s+)?` + // optional repeated JOIN after qualifiers like LEFT OUTER JOIN
		"(" + // start capture group
		`(?:` +
		`(?:` + quotePattern(`"`) + `|` + quotePattern("`") + `|\w+)` + // db/schema/table name (quoted or unquoted)
		`\.` + // dot separator
		`){0,2}` + // 0-2 prefix parts (database.schema.)
		`(?:` + quotePattern(`"`) + `|` + quotePattern("`") + `|\w+)` + // final table name
		")" + // end capture group
		`(?:\s|$|,|\))`, // followed by whitespace, end, comma, or close paren
)

// quotePattern returns a regex pattern matching a quoted identifier with the given quote char.
func quotePattern(q string) string {
	return q + `[^` + q + `]+` + q
}

// cteNameRegex matches WITH ... AS or , name AS patterns to identify CTE names.
var cteNameRegex = regexp.MustCompile(`(?i)(?:WITH|,)\s+(\w+)\s+AS\s*\(`)

// ExtractTableReferences extracts table references from FROM/JOIN clauses in SQL.
// It returns unique table references (schema.table or just table) found in the SQL,
// excluding CTEs and Jinja template expressions.
func ExtractTableReferences(sql string) []string {
	// First, collect CTE names so we can exclude them
	cteNames := make(map[string]bool)
	for _, match := range cteNameRegex.FindAllStringSubmatch(sql, -1) {
		if len(match) > 1 {
			cteNames[strings.ToLower(match[1])] = true
		}
	}

	// Strip Jinja expressions to avoid matching template variables
	cleaned := stripJinjaExpressions(sql)

	// Find all table references
	seen := make(map[string]bool)
	var results []string

	for _, match := range tableRefRegex.FindAllStringSubmatch(cleaned, -1) {
		if len(match) < 2 {
			continue
		}

		ref := strings.TrimSpace(match[1])
		if ref == "" {
			continue
		}

		// Skip if it looks like a subquery or keyword
		refLower := strings.ToLower(ref)
		if isReservedWord(refLower) {
			continue
		}

		// Skip Jinja placeholder
		if strings.Contains(ref, "__JINJA__") {
			continue
		}

		// Skip CTE references
		namePart := refLower
		if idx := strings.LastIndex(namePart, "."); idx >= 0 {
			namePart = namePart[idx+1:]
		}
		// Unquote for comparison
		namePart = unquoteIdentifier(namePart)
		if cteNames[namePart] {
			continue
		}

		// Normalize: unquote identifiers
		ref = normalizeTableRef(ref)
		if ref == "" {
			continue
		}

		if !seen[ref] {
			seen[ref] = true
			results = append(results, ref)
		}
	}

	return results
}

// stripJinjaExpressions removes {{ ... }} and {% ... %} blocks from SQL
// so they don't interfere with table reference detection.
func stripJinjaExpressions(sql string) string {
	// Remove {{ ... }}
	result := regexp.MustCompile(`\{\{.*?\}\}`).ReplaceAllString(sql, "__JINJA__")
	// Remove {% ... %}
	result = regexp.MustCompile(`\{%.*?%\}`).ReplaceAllString(result, "")
	return result
}

// normalizeTableRef removes quotes from a table reference like "schema"."table" -> schema.table.
func normalizeTableRef(ref string) string {
	parts := strings.Split(ref, ".")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = unquoteIdentifier(part)
		if part == "" {
			return ""
		}
		normalized = append(normalized, part)
	}
	return strings.Join(normalized, ".")
}

// detectQuoteStyle returns the quote character used in a table reference (", `, or empty).
func detectQuoteStyle(ref string) string {
	for _, c := range ref {
		if c == '"' {
			return `"`
		}
		if c == '`' {
			return "`"
		}
		if c == '.' {
			continue
		}
	}
	return ""
}

// requoteTableRef applies the quoting style from the original reference to a new table name.
// E.g., if original was `"staging"."stg_orders"` and replacement is `dev_fritz.staging_stg_orders`,
// returns `"dev_fritz"."staging_stg_orders"`.
func requoteTableRef(original, replacement string) string {
	q := detectQuoteStyle(original)
	if q == "" {
		return replacement
	}
	parts := strings.Split(replacement, ".")
	for i, part := range parts {
		parts[i] = q + part + q
	}
	return strings.Join(parts, ".")
}

// unquoteIdentifier removes surrounding double quotes or backticks.
func unquoteIdentifier(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '`' && s[len(s)-1] == '`') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// isReservedWord checks if the string is a SQL keyword that shouldn't be treated as a table.
func isReservedWord(s string) bool {
	reserved := map[string]bool{
		"select": true, "from": true, "where": true, "and": true, "or": true,
		"not": true, "in": true, "exists": true, "between": true, "like": true,
		"is": true, "null": true, "true": true, "false": true, "as": true,
		"on": true, "using": true, "case": true, "when": true, "then": true,
		"else": true, "end": true, "group": true, "order": true, "by": true,
		"having": true, "limit": true, "offset": true, "union": true, "all": true,
		"intersect": true, "except": true, "values": true, "set": true,
		"insert": true, "update": true, "delete": true, "into": true,
		"lateral": true, "unnest": true, "generate_series": true,
	}
	return reserved[s]
}

// =============================================================================
// SQL Table Reference Rewriting
// =============================================================================

// protectLiterals replaces string literals, comments, and Jinja expressions with numbered
// placeholders so they are not affected by table reference rewriting.
func protectLiterals(sql string) (string, []string) {
	var placeholders []string
	var result strings.Builder
	i := 0
	n := len(sql)

	for i < n {
		// Jinja {{ ... }}
		if i+1 < n && sql[i] == '{' && sql[i+1] == '{' {
			end := strings.Index(sql[i+2:], "}}")
			if end >= 0 {
				end += i + 2 + 2
				placeholders = append(placeholders, sql[i:end])
				result.WriteString(g.F("__PROTECTED_%d__", len(placeholders)-1))
				i = end
				continue
			}
		}

		// Jinja {% ... %}
		if i+1 < n && sql[i] == '{' && sql[i+1] == '%' {
			end := strings.Index(sql[i+2:], "%}")
			if end >= 0 {
				end += i + 2 + 2
				placeholders = append(placeholders, sql[i:end])
				result.WriteString(g.F("__PROTECTED_%d__", len(placeholders)-1))
				i = end
				continue
			}
		}

		// Single-line comment --
		if i+1 < n && sql[i] == '-' && sql[i+1] == '-' {
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				end = n - i
			}
			placeholders = append(placeholders, sql[i:i+end])
			result.WriteString(g.F("__PROTECTED_%d__", len(placeholders)-1))
			i += end
			continue
		}

		// Block comment /* ... */
		if i+1 < n && sql[i] == '/' && sql[i+1] == '*' {
			end := strings.Index(sql[i+2:], "*/")
			if end >= 0 {
				end += i + 2 + 2
				placeholders = append(placeholders, sql[i:end])
				result.WriteString(g.F("__PROTECTED_%d__", len(placeholders)-1))
				i = end
				continue
			}
		}

		// Single-quoted string literal '...' (with '' escape)
		if sql[i] == '\'' {
			j := i + 1
			for j < n {
				if sql[j] == '\'' {
					if j+1 < n && sql[j+1] == '\'' {
						j += 2 // escaped quote
						continue
					}
					j++ // closing quote
					break
				}
				j++
			}
			placeholders = append(placeholders, sql[i:j])
			result.WriteString(g.F("__PROTECTED_%d__", len(placeholders)-1))
			i = j
			continue
		}

		result.WriteByte(sql[i])
		i++
	}

	return result.String(), placeholders
}

// restoreLiterals replaces placeholders back with their original content.
func restoreLiterals(sql string, placeholders []string) string {
	for i, p := range placeholders {
		sql = strings.Replace(sql, g.F("__PROTECTED_%d__", i), p, 1)
	}
	return sql
}

// RewriteTableReferences scans compiled SQL for table references matching prod-mode names
// of known models/seeds and rewrites them to current-mode FullTableNames.
// Returns the rewritten SQL and matched model/seed names (for DependsOn).
// An ambiguous bare name is an error.
func RewriteTableReferences(sql string, project *BuildProject, selfName string) (string, []string, error) {
	// Protect literals so we don't rewrite inside strings/comments
	protected, placeholders := protectLiterals(sql)

	// Collect CTE names to exclude
	cteNames := make(map[string]bool)
	for _, match := range cteNameRegex.FindAllStringSubmatch(protected, -1) {
		if len(match) > 1 {
			cteNames[strings.ToLower(match[1])] = true
		}
	}

	// Find all table reference positions
	matches := tableRefRegex.FindAllStringSubmatchIndex(protected, -1)

	var deps []string
	var result strings.Builder
	lastEnd := 0

	for _, loc := range matches {
		if len(loc) < 4 {
			continue
		}
		// loc[2]:loc[3] is capture group 1 (the table reference)
		refStart, refEnd := loc[2], loc[3]
		ref := protected[refStart:refEnd]

		// Normalize for lookup
		normalized := normalizeTableRef(ref)
		if normalized == "" {
			continue
		}
		normalizedLower := strings.ToLower(normalized)

		// Skip reserved words
		if isReservedWord(normalizedLower) {
			continue
		}

		// Skip CTE names
		namePart := normalizedLower
		if idx := strings.LastIndex(namePart, "."); idx >= 0 {
			namePart = namePart[idx+1:]
		}
		if cteNames[namePart] {
			continue
		}

		resolved, err := project.ResolveName(normalized)
		if err != nil {
			continue
		}
		if resolved.Name == selfName {
			continue
		}

		// Replace the table ref with current-mode FullTableName,
		// preserving the original quoting style (double quotes, backticks, or none)
		result.WriteString(protected[lastEnd:refStart])
		result.WriteString(requoteTableRef(ref, resolved.FullTableName))
		lastEnd = refEnd

		if !containsStr(deps, resolved.Name) {
			deps = append(deps, resolved.Name)
		}
	}

	result.WriteString(protected[lastEnd:])

	// Restore protected regions
	final := restoreLiterals(result.String(), placeholders)
	return final, deps, nil
}

// =============================================================================
// @model_name Preprocessing
// =============================================================================

// atRefRegex matches @identifier but not @@identifier (MySQL system variables).
// Captures the identifier after @.
var atRefRegex = regexp.MustCompile(`(?:^|[^@])@([a-zA-Z_]\w*)`)

// preprocessAtRefs replaces @model_name references with the current-mode FullTableName.
// Must be called before Jinja rendering. Populates model.DependsOn for each resolved reference.
func (te *TemplateEngine) preprocessAtRefs(sql string, model *Model) (string, error) {
	out := atRefRegex.ReplaceAllStringFunc(sql, func(match string) string {
		atIdx := strings.Index(match, "@")
		prefix := match[:atIdx]
		name := match[atIdx+1:]

		ref, err := te.project.ResolveName(name)
		if err != nil {
			return match
		}
		if ref.Name == model.Name {
			return match
		}
		if !containsStr(model.DependsOn, ref.Name) {
			model.DependsOn = append(model.DependsOn, ref.Name)
		}
		return prefix + ref.FullTableName
	})
	return out, nil
}

// =============================================================================
// Incremental style detection
// =============================================================================

// Style identifies which incremental pattern a model uses.
type Style int

const (
	// StyleDbt is the dbt-compatible pattern: models use is_incremental() and {{ this }}
	// to write their own WHERE clauses. This is the zero value and the harmless default
	// for non-incremental models.
	StyleDbt Style = iota
	// StyleSling is the sling-native pattern: models use incremental_where_cond() and/or
	// incremental_value() Jinja functions, and sling owns the WHERE clause / watermark.
	StyleSling
)

// detectModelStyle scans raw model SQL (before Jinja rendering) to determine which
// incremental pattern the model uses. Returns an error if both patterns appear in
// the same file — the user must pick one.
func detectModelStyle(rawSQL string) (Style, error) {
	hasSling := strings.Contains(rawSQL, "incremental_where_cond(") ||
		strings.Contains(rawSQL, "incremental_value(")
	hasDbt := strings.Contains(rawSQL, "is_incremental(")
	hasLegacy := strings.Contains(rawSQL, "{incremental_where_cond}") ||
		strings.Contains(rawSQL, "{incremental_value}")

	if hasLegacy && !hasSling {
		return StyleDbt, g.Error("use {{ incremental_where_cond() }} or {{ incremental_value() }} instead of {incremental_where_cond} / {incremental_value}")
	}
	if hasSling && hasDbt {
		return StyleDbt, g.Error("cannot mix is_incremental() and incremental_where_cond() in the same model. Choose one pattern: either dbt-compatible (is_incremental() + {{ this }}) or sling-native ({{ incremental_where_cond() }})")
	}
	if hasSling {
		return StyleSling, nil
	}
	// Only dbt pattern, or neither pattern, → StyleDbt (harmless default;
	// is_incremental() is registered but never called for models that don't use it)
	return StyleDbt, nil
}

// =============================================================================
// Incremental Context
// =============================================================================

// IncrementalContext carries values used by CompileModel to resolve incremental
// functions and flags. One type serves both styles:
//
//   - Style A (dbt):    CompileModel reads IsIncremental only and passes it to
//     the is_incremental() Jinja function. WhereCond/Value
//     are ignored because the SQL does not call
//     incremental_where_cond() / incremental_value().
//
//   - Style B (sling):  CompileModel reads WhereCond/Value and exposes them as
//     the incremental_where_cond() / incremental_value() Jinja
//     functions. is_incremental() is still registered but
//     returns IsIncremental (typically false for sling-style).
//
// A nil *IncrementalContext is equivalent to DefaultIncrementalContext():
// first-run semantics (WhereCond=1=1, Value=null, IsIncremental=false).
type IncrementalContext struct {
	WhereCond     string // e.g., `"created_at" > '2024-01-01'` or "1=1"
	Value         string // e.g., `'2024-01-01'` or "null"
	IsIncremental bool   // drives is_incremental() for dbt-style models
}

// DefaultIncrementalContext returns a first-run context: Jinja functions resolve
// to "1=1"/"null" and is_incremental() returns false.
func DefaultIncrementalContext() *IncrementalContext {
	return &IncrementalContext{
		WhereCond:     "1=1",
		Value:         "null",
		IsIncremental: false,
	}
}

// =============================================================================
// Template Engine
// =============================================================================

// configBlockRegex matches {%- config(...) -%} or {% config(...) %} blocks
// and rewrites them to {{ config(...) }} so gonja treats them as expressions.
var configBlockRegex = regexp.MustCompile(`\{%-?\s*config\(([^)]*)\)\s*-?%\}`)

// TemplateEngine compiles SQL model templates using Jinja-like syntax.
type TemplateEngine struct {
	project *BuildProject
	vars    map[string]any
}

// NewTemplateEngine creates a new template engine for the given project.
func NewTemplateEngine(project *BuildProject, vars map[string]any) *TemplateEngine {
	if vars == nil {
		vars = make(map[string]any)
	}
	return &TemplateEngine{
		project: project,
		vars:    vars,
	}
}

// CompileModel compiles a model's SQL template, extracting config and resolving references.
// The incCtx parameter carries incremental-pattern values:
//   - Style A (dbt) models read incCtx.IsIncremental via the is_incremental() Jinja function.
//   - Style B (sling) models read incCtx.WhereCond / incCtx.Value via
//     incremental_where_cond() and incremental_value() Jinja functions.
//
// A nil incCtx is equivalent to DefaultIncrementalContext() — first-run semantics.
func (te *TemplateEngine) CompileModel(model *Model, incCtx *IncrementalContext) (string, error) {
	if incCtx == nil {
		incCtx = DefaultIncrementalContext()
	}

	// Build the gonja environment
	envCtx := builtins.GlobalFunctions.Inherit().Update(builtins.GlobalVariables)

	// Register config() function — captures kwargs into model.Config, returns empty string.
	// If YAML frontmatter was used, config() is a no-op (frontmatter is canonical).
	envCtx.Set("config", func(_ *exec.Evaluator, params *exec.VarArgs) (string, error) {
		if !model.HasFrontmatter {
			if err := te.applyConfig(model, params); err != nil {
				return "", err
			}
		}
		return "", nil
	})

	// Register ref() function — resolves model/seed name to full table name
	envCtx.Set("ref", func(_ *exec.Evaluator, params *exec.VarArgs) (string, error) {
		if len(params.Args) == 0 {
			return "", g.Error("ref() requires a model name argument")
		}
		name := params.Args[0].String()

		ref, err := te.project.ResolveName(name)
		if err != nil {
			return "", err
		}

		// Record dependency. Refs keep the literal; DependsOn uses the identity.
		if !containsStr(model.Refs, name) {
			model.Refs = append(model.Refs, name)
		}
		if !containsStr(model.DependsOn, ref.Name) {
			model.DependsOn = append(model.DependsOn, ref.Name)
		}

		return ref.FullTableName, nil
	})

	// Register src()/source() functions — passthrough, records as source
	// Accepts src('schema.table') or source('schema', 'table')
	srcFunc := func(_ *exec.Evaluator, params *exec.VarArgs) (string, error) {
		if len(params.Args) == 0 {
			return "", g.Error("src()/source() requires a table name argument")
		}
		var tableName string
		if len(params.Args) >= 2 {
			tableName = params.Args[0].String() + "." + params.Args[1].String()
		} else {
			tableName = params.Args[0].String()
		}

		if !containsStr(model.Sources, tableName) {
			model.Sources = append(model.Sources, tableName)
		}

		return tableName, nil
	}
	envCtx.Set("src", srcFunc)
	envCtx.Set("source", srcFunc)

	// Register this — resolves to model's own full table name
	envCtx.Set("this", model.FullTableName)

	// Register is_incremental() function — drives dbt-style {% if is_incremental() %} blocks
	envCtx.Set("is_incremental", func(params *exec.VarArgs) bool {
		return incCtx.IsIncremental
	})

	envCtx.Set("incremental_where_cond", func(_ *exec.Evaluator, params *exec.VarArgs) (string, error) {
		return incCtx.WhereCond, nil
	})
	envCtx.Set("incremental_value", func(_ *exec.Evaluator, params *exec.VarArgs) (string, error) {
		return incCtx.Value, nil
	})

	// Register user vars
	for k, v := range te.vars {
		envCtx.Set(k, v)
	}

	env := &exec.Environment{
		Context:           envCtx,
		Filters:           builtins.Filters,
		Tests:             builtins.Tests,
		ControlStructures: builtins.ControlStructures,
		Methods:           builtins.Methods,
	}

	// Pre-process: convert {%- config(...) -%} to {{ config(...) }}
	processedSQL := preprocessSQL(model.RawSQL)

	// Prepend applicable macros
	macroSQL := GetMacrosForModel(te.project, model)
	if macroSQL != "" {
		processedSQL = macroSQL + "\n" + processedSQL
	}

	// Resolve @model_name references before Jinja rendering
	var atErr error
	processedSQL, atErr = te.preprocessAtRefs(processedSQL, model)
	if atErr != nil {
		return "", atErr
	}

	// Create template from model SQL
	templateID := "/" + model.Name
	loader := loaders.MustNewMemoryLoader(map[string]string{
		templateID: processedSQL,
	})

	tpl, err := exec.NewTemplate(templateID, config.New(), loader, env)
	if err != nil {
		return "", g.Error(err, "could not parse template for model '%s'", model.Name)
	}

	// Execute template
	result, err := tpl.ExecuteToString(exec.EmptyContext())
	if err != nil {
		return "", g.Error(err, "could not compile template for model '%s'", model.Name)
	}

	compiled := strings.TrimSpace(result)
	model.CompiledSQL = compiled

	return compiled, nil
}

// CompileAll compiles all models in the project using the provided incremental
// context. A nil incCtx is equivalent to DefaultIncrementalContext().
func (te *TemplateEngine) CompileAll(incCtx *IncrementalContext) error {
	for _, model := range te.project.Models {
		if _, err := te.CompileModel(model, incCtx); err != nil {
			return err
		}
	}
	return nil
}

// applyConfig extracts config kwargs into the model's ModelConfig.
func (te *TemplateEngine) applyConfig(model *Model, params *exec.VarArgs) error {
	for key, val := range params.KwArgs {
		strVal := val.String()
		switch key {
		case "mode":
			canonical, warn := normalizeMode(strVal)
			if warn != "" {
				g.Warn("model '%s': %s", model.Name, warn)
			}
			if canonical == "ephemeral" {
				return g.Error("model '%s': ephemeral models are not supported; use view or table", model.Name)
			}
			if err := validateMode(canonical, model.Name); err != nil {
				return err
			}
			model.Config.Mode = canonical
		case "materialized":
			// dbt alias — only applied when mode is unset
			mapped, err := mapMaterialized(strVal)
			if err != nil {
				return g.Error(err, "model '%s'", model.Name)
			}
			if model.Config.Mode == "" {
				model.Config.Mode = mapped
			}
		case "unique_key":
			// Can be a string or list
			if val.IsList() {
				model.Config.UniqueKey = toStringSlice(val)
			} else {
				model.Config.UniqueKey = strVal
			}
		case "merge_strategy":
			model.Config.MergeStrategy = strVal
		case "update_key":
			model.Config.UpdateKey = strVal
		case "tags":
			model.Config.Tags = toStringSlice(val)
		case "pre_hook", "post_hook":
			return g.Error("pre_hook/post_hook are not supported in sling build config(). Use YAML frontmatter with hooks.start/hooks.end instead.\nSee https://docs.slingdata.io/concepts/sling-build for details")
		case "schema":
			model.Config.Schema = strVal
		case "database":
			model.Config.Database = strVal
		case "enabled":
			enabled := cast.ToBool(strVal)
			model.Config.Enabled = &enabled
		case "engine":
			model.Config.Engine = strVal
		case "drop_cascade":
			dc := cast.ToBool(strVal)
			model.Config.DropCascade = &dc
		case "rewrite":
			rw := cast.ToBool(strVal)
			model.Config.Rewrite = &rw
		default:
			g.Warn("model '%s': unrecognized config key '%s' (ignored)", model.Name, key)
		}
	}
	return nil
}

// toStringSlice converts a gonja Value to a string slice.
func toStringSlice(val *exec.Value) []string {
	if val == nil {
		return nil
	}
	iface := val.Interface()
	switch v := iface.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			result = append(result, cast.ToString(item))
		}
		return result
	default:
		return []string{cast.ToString(iface)}
	}
}

// containsStr checks if a string is in a slice.
func containsStr(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// preprocessSQL converts dbt-style {%- config(...) -%} blocks to {{ config(...) }}
// so gonja treats them as expressions rather than control structures.
func preprocessSQL(sql string) string {
	return configBlockRegex.ReplaceAllString(sql, "{{ config($1) }}")
}
