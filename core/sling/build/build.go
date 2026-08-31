package build

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/flarco/g"
	"github.com/slingdata-io/golyglot"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/env"
)

// Build is the main orchestrator for sling build.
// It loads a project, compiles templates, builds the DAG,
// applies selectors, and provides compile output.
type Build struct {
	Project     *BuildProject
	DAG         *DAG
	Engine      *TemplateEngine
	Options     BuildOptions
	Selected    []string               // selected node names after selector filtering
	SubBuilds   []*Build               // compiled sub-projects (for multi-target compile mode)
	connEntries connection.ConnEntries // pre-resolved connection entries for parallel execution
	ExecRows    uint64                 // sum of model/seed rows after Execute
	ExecBytes   uint64
}

// NewBuild creates a new Build from the given project directory and options.
func NewBuild(dir string, opts BuildOptions) (*Build, error) {
	project, err := LoadProject(dir, opts)
	if err != nil {
		return nil, g.Error(err, "could not load project from %s", dir)
	}

	b := &Build{
		Project: project,
		Options: opts,
	}

	return b, nil
}

// Compile loads the project, compiles all model templates, builds the DAG,
// and applies selectors. After Compile(), the Build is ready for execution
// or compile output.
func (b *Build) Compile() error {
	// For sub-projects (independent builds), compile each one
	if len(b.Project.SubProjects) > 0 {
		if !b.Options.Compile {
			return nil // sub-projects are compiled individually during Execute
		}
		for _, subProject := range b.Project.SubProjects {
			subBuild := &Build{Project: subProject, Options: b.Options}
			if subBuild.Options.Target == "" && subProject.Config != nil {
				subBuild.Options.Target = subProject.Config.Target
			}
			if err := subBuild.Compile(); err != nil {
				return g.Error(err, "could not compile sub-project %s", subProject.Dir)
			}
			b.SubBuilds = append(b.SubBuilds, subBuild)
		}
		return nil
	}

	// Resolve target
	target := ""
	if b.Project.Config != nil {
		target = b.Project.Config.Target
	}
	if b.Options.Target != "" {
		target = b.Options.Target
	}
	if target == "" {
		return g.Error("No target specified. Use '--target <conn>' or set target in sling_build.yml.")
	}

	// Get vars
	vars := make(map[string]any)
	if b.Project.Config != nil {
		for k, v := range b.Project.Config.Vars {
			vars[k] = v
		}
	}
	for k, v := range b.Options.Vars {
		vars[k] = v
	}

	// Compile templates
	b.Engine = NewTemplateEngine(b.Project, vars)
	if err := b.Engine.CompileAll(DefaultIncrementalContext()); err != nil {
		return g.Error(err, "could not compile models")
	}

	// Rewrite table references: match prod-mode names in SQL and replace with current-mode names.
	// This also populates DependsOn for matched references.
	// Models with rewrite: false skip bare-name rewriting (ref()/src() still resolve).
	for _, model := range b.Project.Models {
		if model.Config.Rewrite != nil && !*model.Config.Rewrite {
			continue
		}
		rewritten, deps := RewriteTableReferences(model.CompiledSQL, b.Project, model.Name)
		model.CompiledSQL = rewritten
		for _, dep := range deps {
			if !containsStr(model.DependsOn, dep) {
				model.DependsOn = append(model.DependsOn, dep)
			}
		}
	}

	// Split multi-statement models into pre-statements, model query, and post-statements
	if err := b.splitMultiStatementModels(); err != nil {
		return g.Error(err, "could not parse multi-statement models")
	}

	// Auto-detect SQL references as a safety net for DependsOn (catches edge cases
	// the rewrite step might miss, e.g., three-part names or unusual patterns)
	for _, model := range b.Project.Models {
		autoRefs := ExtractTableReferences(model.CompiledSQL)
		for _, ref := range autoRefs {
			// Check if this matches a project model by full table name
			for name, m := range b.Project.Models {
				if m.FullTableName == ref && name != model.Name {
					if !containsStr(model.DependsOn, name) {
						model.DependsOn = append(model.DependsOn, name)
					}
				}
			}
			// Check if this matches a project seed by full table name
			for name, s := range b.Project.Seeds {
				if s.FullTableName == ref {
					if !containsStr(model.DependsOn, name) {
						model.DependsOn = append(model.DependsOn, name)
					}
				}
			}
		}
	}

	// Build DAG
	dag, err := BuildDAG(b.Project)
	if err != nil {
		return g.Error(err, "could not build dependency graph")
	}
	b.DAG = dag

	// Apply selectors
	selector := NewSelector(b.Options.Select, b.Options.Exclude)
	selected, err := selector.Apply(b.DAG)
	if err != nil {
		return g.Error(err, "could not apply selectors")
	}
	b.Selected = selected

	return nil
}

// GetTarget returns the resolved target connection name.
func (b *Build) GetTarget() string {
	if b.Options.Target != "" {
		return b.Options.Target
	}
	if b.Project.Config != nil {
		return b.Project.Config.Target
	}
	return ""
}

// GetModelMode returns the effective mode for a model, considering:
// 1. CLI --full-refresh flag overrides everything
// 2. Model config() block mode
// 3. Project defaults.mode
// 4. Default: full-refresh
func (b *Build) GetModelMode(model *Model) string {
	if b.Options.FullRefresh {
		return "full-refresh"
	}
	mode := model.Config.Mode
	if mode == "" && b.Project.Config != nil {
		mode = b.Project.Config.Defaults.Mode
	}
	if mode == "" {
		return "full-refresh"
	}
	canonical, _ := normalizeMode(mode)
	if canonical == "" {
		return "full-refresh"
	}
	return canonical
}

// splitMultiStatementModels parses each model's CompiledSQL and splits
// multi-statement files into pre-statements, the model query, and post-statements.
// This runs after CompileAll but before ExtractTableReferences so that
// auto-ref detection only scans the model query, not pre/post DDL.
func (b *Build) splitMultiStatementModels() error {
	dbType := b.resolveDbType()
	for _, model := range b.Project.Models {
		result, err := MakeModelSQL(model.CompiledSQL, dbType)
		if err != nil {
			return g.Error(err, "model '%s'", model.Name)
		}
		model.PreStatements = result.PreStatements
		model.CompiledSQL = result.ModelQuery
		model.PostStatements = result.PostStatements
	}
	return nil
}

// resolveDbType determines the database type from the target connection
// without opening a connection (uses local connection entries).
func (b *Build) resolveDbType() dbio.Type {
	entries := connection.GetLocalConns()
	entry := entries.Get(b.GetTarget())
	if entry.Name != "" {
		return entry.Connection.Type
	}
	g.Warn("target connection '%s' not found locally; multi-statement splitting will use generic dialect", b.GetTarget())
	return dbio.TypeUnknown
}

// Execute runs the compiled build against the target database.
// Must be called after Compile().
func (b *Build) Execute() error {
	// Handle sub-projects (independent builds) — run with thread limit
	if len(b.Project.SubProjects) > 0 {
		// Pre-resolve connections once to avoid concurrent access to GetLocalConns
		entries := connection.GetLocalConns()

		threads := b.Options.Threads
		if threads < 1 {
			threads = 1
		}
		if b.resolveDbType().IsSingleWriterDB() && threads > 1 {
			g.Debug("capping sub-project threads to 1 for DuckDB-family target (single writer)")
			threads = 1
		}

		var wg sync.WaitGroup
		sem := make(chan struct{}, threads)
		errCh := make(chan error, len(b.Project.SubProjects))

		for _, subProject := range b.Project.SubProjects {
			sem <- struct{}{} // acquire semaphore
			wg.Add(1)
			go func(sp *BuildProject) {
				defer wg.Done()
				defer func() { <-sem }() // release semaphore
				subBuild := &Build{
					Project:     sp,
					Options:     b.Options,
					connEntries: entries,
				}
				if err := subBuild.Compile(); err != nil {
					errCh <- g.Error(err, "could not compile sub-project %s", sp.Dir)
					return
				}
				if err := subBuild.Execute(); err != nil {
					errCh <- g.Error(err, "could not execute sub-project %s", sp.Dir)
				}
			}(subProject)
		}

		wg.Wait()
		close(errCh)

		var errs []string
		for err := range errCh {
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			return g.Error(strings.Join(errs, "; "))
		}
		return nil
	}

	executor, err := NewExecutor(b)
	if err != nil {
		return err
	}

	err = executor.Execute()
	for _, r := range executor.Results {
		b.ExecRows += r.Rows
		b.ExecBytes += r.Bytes
	}
	return err
}

// PrintListOutput prints the selected models/seeds and exits.
func (b *Build) PrintListOutput() {
	if len(b.SubBuilds) > 0 {
		for _, subBuild := range b.SubBuilds {
			fmt.Printf("=== Sub-project: %s (target: %s) ===\n", filepath.Base(subBuild.Project.Dir), subBuild.GetTarget())
			subBuild.PrintListOutput()
			fmt.Println()
		}
		return
	}

	if b.DAG == nil || len(b.Selected) == 0 {
		fmt.Println("No models selected.")
		return
	}

	for _, name := range b.Selected {
		node := b.DAG.Nodes[name]
		if b.Options.NoSeeds && node.Seed != nil {
			continue
		}
		nodeType := ""
		if node.Seed != nil {
			nodeType = "seed"
		} else if node.Model != nil {
			nodeType = b.GetModelMode(node.Model)
		}
		fmt.Printf("%s (%s)\n", name, nodeType)
	}
}

// PrintListJSON prints selected nodes as JSON.
func (b *Build) PrintListJSON() {
	type item struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	var items []item
	for _, name := range b.Selected {
		node := b.DAG.Nodes[name]
		if node == nil {
			continue
		}
		if b.Options.NoSeeds && node.Seed != nil {
			continue
		}
		it := item{Name: name}
		if node.Seed != nil {
			it.Type = "seed"
		} else if node.Model != nil {
			it.Type = b.GetModelMode(node.Model)
		}
		items = append(items, it)
	}
	fmt.Println(g.Marshal(items))
}

// PrintCompileOutput prints the compile output in YAML format for each selected node.
func (b *Build) PrintCompileOutput() {
	if len(b.SubBuilds) > 0 {
		for i, subBuild := range b.SubBuilds {
			if i > 0 {
				fmt.Println()
			}
			fmt.Printf("# === Sub-project: %s (target: %s) ===\n", filepath.Base(subBuild.Project.Dir), subBuild.GetTarget())
			subBuild.PrintCompileOutput()
		}
		return
	}

	if b.DAG == nil || len(b.Selected) == 0 {
		fmt.Println("# No models selected.")
		return
	}

	// Print DAG execution order
	fmt.Println("DAG Execution Order:")
	for _, name := range b.Selected {
		node := b.DAG.Nodes[name]
		nodeType := ""
		if node.Seed != nil {
			nodeType = "seed"
		} else if node.Model != nil {
			nodeType = b.GetModelMode(node.Model)
		}
		fmt.Printf("  %s (%s)\n", name, nodeType)
	}
	fmt.Println()

	for i, name := range b.Selected {
		node := b.DAG.Nodes[name]
		if i > 0 {
			fmt.Println()
		}
		if node.Seed != nil {
			b.printSeedYAML(name, node)
		} else if node.Model != nil {
			b.printModelYAML(name, node)
		}
	}
}

// PrintCompileJSON prints compile output as JSON (models, deps, SQL).
func (b *Build) PrintCompileJSON() {
	if len(b.SubBuilds) > 0 {
		var all []map[string]any
		for _, sub := range b.SubBuilds {
			all = append(all, sub.compileJSONPayload())
		}
		fmt.Println(g.Marshal(all))
		return
	}
	fmt.Println(g.Marshal(b.compileJSONPayload()))
}

// CompileJSONPayload returns the --compile --json object.
// Safe when Compile did not finish (e.g. a cycle): order/nodes stay empty.
func (b *Build) CompileJSONPayload() map[string]any {
	if b == nil {
		return map[string]any{"order": []string{}, "nodes": []map[string]any{}, "target": ""}
	}
	if b.DAG == nil {
		return map[string]any{"order": []string{}, "nodes": []map[string]any{}, "target": b.GetTarget()}
	}
	return b.compileJSONPayload()
}

func (b *Build) compileJSONPayload() map[string]any {
	nodes := make([]map[string]any, 0, len(b.Selected))
	for _, name := range b.Selected {
		node := b.DAG.Nodes[name]
		if node == nil {
			continue
		}
		m := map[string]any{"name": name}
		if node.Seed != nil {
			m["type"] = "seed"
			m["table"] = node.Seed.FullTableName
			m["file"] = filepath.ToSlash(node.Seed.RelPath)
		} else if node.Model != nil {
			m["type"] = "model"
			m["table"] = node.Model.FullTableName
			m["file"] = filepath.ToSlash(node.Model.RelPath)
			m["mode"] = b.GetModelMode(node.Model)
			m["dependencies"] = node.Dependencies
			m["sql"] = node.Model.CompiledSQL
			if len(node.Model.Config.Tests) > 0 {
				m["tests"] = node.Model.Config.Tests
			}
		}
		nodes = append(nodes, m)
	}
	return map[string]any{
		"order":  append([]string{}, b.Selected...),
		"nodes":  nodes,
		"target": b.GetTarget(),
	}
}

// printSeedYAML prints a seed node in YAML format.
func (b *Build) printSeedYAML(name string, node *DAGNode) {
	seed := node.Seed
	fmt.Printf("%s:\n", name)
	fmt.Printf("  type: seed\n")
	fmt.Printf("  table: %s\n", seed.FullTableName)
	fmt.Printf("  file: %s\n", filepath.ToSlash(seed.RelPath))
	fmt.Printf("  format: %s\n", seed.Format)
}

// printModelYAML prints a model node in YAML format.
func (b *Build) printModelYAML(name string, node *DAGNode) {
	model := node.Model
	mode := b.GetModelMode(model)

	fmt.Printf("%s:\n", name)
	fmt.Printf("  type: model\n")
	fmt.Printf("  table: %s\n", model.FullTableName)
	fmt.Printf("  file: %s\n", filepath.ToSlash(model.RelPath))
	fmt.Printf("  mode: %s\n", mode)

	// Incremental details
	if model.Config.UniqueKey != nil {
		fmt.Printf("  unique_key: %s\n", formatYAMLValue(model.Config.UniqueKey))
	}
	if model.Config.MergeStrategy != "" {
		fmt.Printf("  merge_strategy: %s\n", model.Config.MergeStrategy)
	}
	if model.Config.UpdateKey != "" {
		fmt.Printf("  update_key: %s\n", model.Config.UpdateKey)
	}

	// Tags
	if len(model.Config.Tags) > 0 {
		fmt.Printf("  tags: %s\n", formatYAMLValue(model.Config.Tags))
	}

	// Dependencies
	fmt.Printf("  dependencies: %s\n", formatYAMLValue(node.Dependencies))

	// Hooks
	if !model.Config.Hooks.IsEmpty() {
		if len(model.Config.Hooks.Start) > 0 {
			fmt.Printf("  start_hooks:\n")
			for _, h := range model.Config.Hooks.Start {
				fmt.Printf("    - %s\n", g.Marshal(h))
			}
		}
		if len(model.Config.Hooks.End) > 0 {
			fmt.Printf("  end_hooks:\n")
			for _, h := range model.Config.Hooks.End {
				fmt.Printf("    - %s\n", g.Marshal(h))
			}
		}
	}

	// Pre-statements
	if len(model.PreStatements) > 0 {
		fmt.Printf("  pre_statements:\n")
		for _, stmt := range model.PreStatements {
			fmt.Printf("    - |\n")
			for _, line := range strings.Split(strings.TrimRight(stmt, "\n"), "\n") {
				fmt.Printf("        %s\n", line)
			}
		}
	}

	// Compiled SQL
	fmt.Printf("  sql: |\n")
	for _, line := range strings.Split(strings.TrimRight(model.CompiledSQL, "\n"), "\n") {
		fmt.Printf("    %s\n", line)
	}

	// Post-statements
	if len(model.PostStatements) > 0 {
		fmt.Printf("  post_statements:\n")
		for _, stmt := range model.PostStatements {
			fmt.Printf("    - |\n")
			for _, line := range strings.Split(strings.TrimRight(stmt, "\n"), "\n") {
				fmt.Printf("        %s\n", line)
			}
		}
	}
}

// formatYAMLValue formats a value as inline YAML.
func formatYAMLValue(v any) string {
	switch val := v.(type) {
	case []string:
		if len(val) == 0 {
			return "[]"
		}
		items := make([]string, len(val))
		for i, s := range val {
			items[i] = s
		}
		return "[" + strings.Join(items, ", ") + "]"
	case string:
		return val
	default:
		return g.Marshal(v)
	}
}

/////////////////////////////////// golyglot

func init() {
	os.Setenv("GOLYGLOT_LIBRARY_FOLDER", filepath.Join(env.HomeDir, "lib", "golyglot"))
}

// ModelSQL holds the split result of a multi-statement SQL model file.
type ModelSQL struct {
	PreStatements  []string
	ModelQuery     string
	PostStatements []string
}

// MakeModelSQL splits a SQL model file into pre-statements, the model query,
// and post-statements. The model query is the single SELECT/WITH/UNION statement.
// All other statements are classified as pre (before) or post (after) the query.
func MakeModelSQL(sql string, dbType dbio.Type) (*ModelSQL, error) {
	dialect := mapDialect(dbType)
	pre, model, post, err := SplitModelSQL(sql, dialect)
	if err != nil {
		return nil, err
	}
	return &ModelSQL{
		PreStatements:  pre,
		ModelQuery:     model,
		PostStatements: post,
	}, nil
}

// mapDialect converts a dbio.Type to a polyglot dialect string.
func mapDialect(dbType dbio.Type) string {
	switch dbType {
	case dbio.TypeDbPostgres:
		return "postgresql"
	case dbio.TypeDbRedshift:
		return "redshift"
	case dbio.TypeDbMySQL, dbio.TypeDbMariaDB, dbio.TypeDbStarRocks:
		return "mysql"
	case dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH, dbio.TypeDbFabric:
		return "tsql"
	case dbio.TypeDbClickhouse, dbio.TypeDbProton:
		return "clickhouse"
	case dbio.TypeDbBigQuery:
		return "bigquery"
	case dbio.TypeDbSnowflake:
		return "snowflake"
	case dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck, dbio.TypeDbDuckLake:
		return "duckdb"
	case dbio.TypeDbDatabricks:
		return "databricks"
	case dbio.TypeDbSQLite, dbio.TypeDbD1:
		return "sqlite"
	case dbio.TypeDbTrino, dbio.TypeDbAthena, dbio.TypeDbIceberg:
		return "trino"
	default:
		return "generic"
	}
}

// SplitModelSQL splits multi-statement SQL into pre-statements, the model query,
// and post-statements. The model query is the single SELECT/WITH/UNION statement.
//
// Returns an error if zero or more than one query statement is found.
func SplitModelSQL(sql, dialect string) (preStatements []string, modelQuery string, postStatements []string, err error) {
	// Fast path: if no semicolons outside quotes/comments, it's a single statement
	if !containsSemicolon(sql) {
		return nil, strings.TrimSpace(sql), nil, nil
	}

	stmts, classifyErr := golyglot.ClassifyStatements(sql, dialect)
	if classifyErr != nil {
		return nil, "", nil, classifyErr
	}

	if len(stmts) == 0 {
		return nil, "", nil, fmt.Errorf("no SQL statements found")
	}

	// Single statement: must be the model
	if len(stmts) == 1 {
		if stmts[0].Type != golyglot.StmtQuery {
			return nil, "", nil, fmt.Errorf("model file contains a single %s statement (%s), expected a SELECT query", stmts[0].Type, stmts[0].TypeKey)
		}
		return nil, stmts[0].SQL, nil, nil
	}

	// Multiple statements: find the query
	queryIdx := -1
	queryCount := 0
	for i, stmt := range stmts {
		if stmt.Type == golyglot.StmtQuery {
			queryIdx = i
			queryCount++
		}
	}

	if queryCount == 0 {
		return nil, "", nil, fmt.Errorf("no SELECT query found in model file; found %d statements but none are queries", len(stmts))
	}
	if queryCount > 1 {
		return nil, "", nil, fmt.Errorf("found %d SELECT queries in model file; expected exactly 1", queryCount)
	}

	// Split around the query
	for _, stmt := range stmts[:queryIdx] {
		preStatements = append(preStatements, stmt.SQL)
	}
	modelQuery = stmts[queryIdx].SQL
	for _, stmt := range stmts[queryIdx+1:] {
		postStatements = append(postStatements, stmt.SQL)
	}

	return preStatements, modelQuery, postStatements, nil
}

// containsSemicolon checks if SQL contains a semicolon outside of quotes, comments,
// and Postgres-style dollar-quoting ($tag$ ... $tag$).
// This is a quick heuristic to skip WASM/FFI for simple single-statement files.
func containsSemicolon(sql string) bool {
	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false
	// dollarTag is non-empty when inside $tag$...$tag$ (Postgres dollar quotes)
	dollarTag := ""

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		if dollarTag != "" {
			// Look for closing tag
			if c == '$' && i+len(dollarTag) <= len(sql) && sql[i:i+len(dollarTag)] == dollarTag {
				i += len(dollarTag) - 1
				dollarTag = ""
			}
			continue
		}

		if inLineComment {
			if c == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if c == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inSingleQuote {
			if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++ // escaped quote
				} else {
					inSingleQuote = false
				}
			}
			continue
		}
		if inDoubleQuote {
			if c == '"' {
				inDoubleQuote = false
			}
			continue
		}

		switch c {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case '$':
			// Start of dollar quote: $tag$ or $$
			j := i + 1
			for j < len(sql) && ((sql[j] >= 'a' && sql[j] <= 'z') ||
				(sql[j] >= 'A' && sql[j] <= 'Z') ||
				(sql[j] >= '0' && sql[j] <= '9') || sql[j] == '_') {
				j++
			}
			if j < len(sql) && sql[j] == '$' {
				dollarTag = sql[i : j+1]
				i = j
			}
		case '-':
			if i+1 < len(sql) && sql[i+1] == '-' {
				inLineComment = true
				i++
			}
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				inBlockComment = true
				i++
			}
		case ';':
			return true
		}
	}
	return false
}
