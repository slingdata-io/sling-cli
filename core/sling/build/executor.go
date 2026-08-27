package build

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
)

// Executor runs a compiled Build against a target database.
type Executor struct {
	Build       *Build
	ConnName    string                 // resolved target connection name
	DbConn      database.Connection    // database connection (nil until Connect)
	Results     []ExecutionResult      // per-node results
	RunID       string                 // unique per Execute() for temp table isolation
	connEntries connection.ConnEntries // pre-resolved connection entries (optional)
	ctx         *g.Context
	failedSet   map[string]bool // tracks failed nodes for skipping downstream
	stopped     bool            // set when fail-fast triggers; prevents new dispatches
}

// ExecutionResult holds the outcome of executing one node.
type ExecutionResult struct {
	Name     string
	NodeType string // "seed" or "model"
	Mode     string // "full-refresh", "view", "truncate", "incremental", "append"
	Duration time.Duration
	Err      error
	Skipped  bool
}

// BuildState implements sling.RuntimeState for build model hooks.
type BuildState struct {
	State     map[string]map[string]any `json:"state,omitempty"`
	Store     map[string]any            `json:"store,omitempty"`
	Env       map[string]any            `json:"env,omitempty"`
	Timestamp sling.DateTimeState       `json:"timestamp,omitempty"`
	Model     BuildModelState           `json:"model,omitempty"`
	Target    BuildTargetState          `json:"target,omitempty"`
}

// BuildModelState holds model metadata available in hooks.
type BuildModelState struct {
	Name     string `json:"name,omitempty"`
	Schema   string `json:"schema,omitempty"`
	FullName string `json:"full_name,omitempty"`
	Mode     string `json:"mode,omitempty"`
}

// BuildTargetState holds target connection metadata available in hooks.
type BuildTargetState struct {
	Name string `json:"name,omitempty"`
}

func (bs *BuildState) GetStore() map[string]any { return bs.Store }
func (bs *BuildState) SetStoreData(key string, value any, del bool) {
	if del {
		delete(bs.Store, key)
	} else {
		bs.Store[key] = value
	}
}
func (bs *BuildState) SetStateData(id string, data map[string]any) { bs.State[id] = data }
func (bs *BuildState) SetStateKeyValue(id, key string, value any) {
	if bs.State[id] == nil {
		bs.State[id] = map[string]any{}
	}
	bs.State[id][key] = value
}
func (bs *BuildState) Marshall() string                            { return g.Marshal(bs) }
func (bs *BuildState) TaskExecution() *sling.TaskExecution         { return nil }
func (bs *BuildState) StepExecution() *sling.PipelineStepExecution { return nil }

// NewExecutor creates an Executor from a compiled Build.
func NewExecutor(b *Build) (*Executor, error) {
	connName := b.GetTarget()
	if connName == "" {
		return nil, g.Error("no target connection specified")
	}
	return &Executor{
		Build:       b,
		ConnName:    connName,
		RunID:       g.RandString(g.AlphaRunesLower, 6),
		connEntries: b.connEntries,
		failedSet:   make(map[string]bool),
		ctx:         g.NewContext(context.Background()),
	}, nil
}

// Connect establishes a database connection to the target.
func (e *Executor) Connect() error {
	entries := e.connEntries
	if entries == nil {
		entries = connection.GetLocalConns()
	}
	entry := entries.Get(e.ConnName)
	if entry.Name == "" {
		return g.Error("connection '%s' not found", e.ConnName)
	}

	dbConn, err := entry.Connection.AsDatabase()
	if err != nil {
		return g.Error(err, "could not create database connection for '%s'", e.ConnName)
	}

	if err := dbConn.Connect(); err != nil {
		return g.Error(err, "could not connect to '%s'", e.ConnName)
	}

	e.DbConn = dbConn
	return nil
}

// Close closes the database connection.
func (e *Executor) Close() {
	if e.DbConn != nil {
		e.DbConn.Close()
	}
}

// CreateSchemas creates all unique schemas needed by the selected nodes.
func (e *Executor) CreateSchemas() error {
	schemas := make(map[string]bool)
	for _, name := range e.Build.Selected {
		node := e.Build.DAG.Nodes[name]
		if node.Model != nil {
			schemas[node.Model.Schema] = true
		}
		if node.Seed != nil {
			schemas[node.Seed.Schema] = true
		}
	}

	for schema := range schemas {
		sql := g.R(
			e.DbConn.Template().Value("core.create_schema"),
			"schema", e.DbConn.Quote(schema),
		)
		if _, err := e.DbConn.Exec(sql); err != nil {
			// Ignore "already exists" errors for databases without IF NOT EXISTS
			errLower := strings.ToLower(err.Error())
			if !strings.Contains(errLower, "already exists") && !strings.Contains(errLower, "duplicate") {
				return g.Error(err, "could not create schema '%s'", schema)
			}
		}
	}
	return nil
}

// Execute runs all selected nodes with a ready-queue scheduler.
// A node is dispatched as soon as its selected dependencies complete (no level barriers).
func (e *Executor) Execute() error {
	if err := e.Connect(); err != nil {
		return err
	}
	defer e.Close()

	if err := e.CreateSchemas(); err != nil {
		return err
	}

	total := len(e.Build.Selected)
	if total == 0 {
		fmt.Println("No models selected.")
		return nil
	}

	threads := e.Build.Options.Threads
	if threads < 1 {
		threads = 1
	}

	if e.DbConn != nil && e.DbConn.GetType().IsSingleWriterDB() && threads > 1 {
		g.Debug("capping build threads to 1 for %s target (single writer)", e.DbConn.GetType())
		threads = 1
	}

	selectedSet := make(map[string]bool, total)
	for _, name := range e.Build.Selected {
		selectedSet[name] = true
	}

	// remaining[name] = count of selected deps not yet finished
	remaining := make(map[string]int, total)
	dependents := make(map[string][]string, total)
	for _, name := range e.Build.Selected {
		node := e.Build.DAG.Nodes[name]
		if node == nil {
			continue
		}
		count := 0
		for _, dep := range node.Dependencies {
			if selectedSet[dep] {
				count++
				dependents[dep] = append(dependents[dep], name)
			}
		}
		remaining[name] = count
	}

	var (
		mu        sync.Mutex
		ready     []string
		started   = make(map[string]bool, total)
		inFlight  int
		completed int
		idx       int
		failFast  = e.Build.Options.FailFast
		cond      = sync.NewCond(&mu)
	)

	for _, name := range e.Build.Selected {
		if remaining[name] == 0 {
			ready = append(ready, name)
		}
	}

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for {
			mu.Lock()
			for len(ready) == 0 && !e.stopped && completed+inFlight < total {
				cond.Wait()
			}
			// Exit when nothing left to do
			if len(ready) == 0 {
				mu.Unlock()
				return
			}
			if e.stopped {
				// Don't start new work under fail-fast
				mu.Unlock()
				return
			}

			name := ready[0]
			ready = ready[1:]
			if started[name] {
				mu.Unlock()
				continue
			}
			started[name] = true
			idx++
			nodeIdx := idx
			inFlight++
			mu.Unlock()

			result := e.runNode(name, nodeIdx, total)

			mu.Lock()
			e.Results = append(e.Results, result)
			seedSkipped := result.Skipped && e.Build.Options.NoSeeds &&
				e.Build.DAG.Nodes[name] != nil && e.Build.DAG.Nodes[name].Seed != nil
			// failedSet is also read under e.ctx.Mux in runNode — hold both
			e.ctx.Mux.Lock()
			if result.Err != nil || (result.Skipped && !seedSkipped) {
				e.failedSet[name] = true
			}
			if result.Err != nil && failFast {
				e.stopped = true
			}
			e.ctx.Mux.Unlock()
			for _, depName := range dependents[name] {
				remaining[depName]--
				if remaining[depName] == 0 && !started[depName] {
					ready = append(ready, depName)
				}
			}
			inFlight--
			completed++
			cond.Broadcast()
			mu.Unlock()

			e.printProgress(nodeIdx, total, result, false)
		}
	}

	nWorkers := threads
	if nWorkers > total {
		nWorkers = total
	}
	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		go worker()
	}
	wg.Wait()

	// Mark any never-started nodes as skipped (fail-fast or dep failure cascade)
	for _, name := range e.Build.Selected {
		mu.Lock()
		wasStarted := started[name]
		mu.Unlock()
		if wasStarted {
			continue
		}
		result := ExecutionResult{Name: name, Skipped: true}
		if node := e.Build.DAG.Nodes[name]; node != nil {
			if node.Seed != nil {
				result.NodeType = "seed"
				result.Mode = "full-refresh"
				result.Name = node.Seed.FullTableName
			} else if node.Model != nil {
				result.NodeType = "model"
				result.Mode = e.Build.GetModelMode(node.Model)
				result.Name = node.Model.FullTableName
			}
		}
		e.Results = append(e.Results, result)
		e.ctx.Mux.Lock()
		e.failedSet[name] = true
		e.ctx.Mux.Unlock()
	}

	fmt.Println()
	e.printSummary()

	var errs []error
	for _, r := range e.Results {
		if r.Err != nil {
			errs = append(errs, g.Error(r.Err, "%s", r.Name))
		}
	}
	if len(errs) > 0 {
		msgs := make([]string, len(errs))
		for i, err := range errs {
			msgs[i] = err.Error()
		}
		return g.Error("build completed with %d error(s): %s", len(errs), strings.Join(msgs, "; "))
	}
	return nil
}

// runNode executes a single DAG node and returns its result.
func (e *Executor) runNode(nodeName string, nodeIndex, total int) ExecutionResult {
	start := time.Now()
	node := e.Build.DAG.Nodes[nodeName]

	var result ExecutionResult
	result.Name = nodeName
	if node == nil {
		result.Err = g.Error("node '%s' not found in DAG", nodeName)
		result.Duration = time.Since(start)
		return result
	}

	e.ctx.Mux.Lock()
	skipped := e.isDownstreamOfFailed(nodeName)
	e.ctx.Mux.Unlock()

	seedSkipped := !skipped && e.Build.Options.NoSeeds && node.Seed != nil
	testOnly := e.Build.Options.Test
	if testOnly && node.Seed != nil {
		seedSkipped = true
	}

	if skipped || seedSkipped {
		result.Skipped = true
		result.Duration = time.Since(start)
		if node.Seed != nil {
			result.NodeType = "seed"
			result.Mode = "full-refresh"
			result.Name = node.Seed.FullTableName
		} else if node.Model != nil {
			result.NodeType = "model"
			result.Mode = e.Build.GetModelMode(node.Model)
			result.Name = node.Model.FullTableName
		}
		return result
	}

	if node.Seed != nil {
		result.NodeType = "seed"
		result.Mode = "full-refresh"
		result.Name = node.Seed.FullTableName
		e.printProgress(nodeIndex, total, result, true)
		result.Err = e.executeSeed(node.Seed)
		result.Duration = time.Since(start)
		return result
	}

	if node.Model != nil {
		mode := e.Build.GetModelMode(node.Model)
		result.NodeType = "model"
		result.Mode = mode
		if testOnly {
			result.Mode = "test"
		}
		result.Name = node.Model.FullTableName
		e.printProgress(nodeIndex, total, result, true)
		if testOnly {
			result.Err = e.executeModelTests(node.Model)
		} else {
			result.Err = e.executeModel(node.Model, mode)
		}
		result.Duration = time.Since(start)
	}
	return result
}

// isDownstreamOfFailed checks if any upstream dependency of the node has failed.
// Must be called under e.mu lock.
func (e *Executor) isDownstreamOfFailed(name string) bool {
	node := e.Build.DAG.Nodes[name]
	if node == nil {
		return false
	}
	for _, dep := range node.Dependencies {
		if e.failedSet[dep] {
			return true
		}
	}
	return false
}

// executeSeed loads a seed file using the sling task infrastructure.
func (e *Executor) executeSeed(seed *Seed) error {
	// LoadSeed opens its own target connection. DuckDB-family files allow
	// one writer, so release the executor handle first.
	if e.DbConn != nil && e.DbConn.GetType().IsSingleWriterDB() {
		e.Close()
		defer func() { _ = e.Connect() }()
	}
	return LoadSeed(seed, e.ConnName, true)
}

// parseModelHooks parses model hooks from the config into executable Hook objects.
func (e *Executor) parseModelHooks(model *Model, mode string) error {
	if model.Config.Hooks.IsEmpty() {
		return nil
	}

	state := &BuildState{
		State: map[string]map[string]any{},
		Store: map[string]any{},
		Env:   map[string]any{},
		Model: BuildModelState{
			Name:     model.Name,
			Schema:   model.Schema,
			FullName: model.FullTableName,
			Mode:     mode,
		},
		Target: BuildTargetState{
			Name: e.ConnName,
		},
	}
	state.Timestamp.Update()

	// populate env from Build vars if available
	if e.Build != nil && e.Build.Project != nil && e.Build.Project.Config != nil {
		for k, v := range e.Build.Project.Config.Vars {
			state.Env[k] = v
		}
	}

	ctx := g.NewContext(context.Background())

	// parse start hooks
	for i, hookRaw := range model.Config.Hooks.Start {
		opts := sling.NewParseOptions(sling.HookStageStart, sling.HookKindHook, i, state, ctx)
		hook, err := sling.ParseHook(hookRaw, opts)
		if err != nil {
			return g.Error(err, "error parsing start hook %d for model '%s'", i+1, model.Name)
		}
		if hook != nil {
			model.startHooks = append(model.startHooks, hook)
		}
	}

	// parse end hooks
	for i, hookRaw := range model.Config.Hooks.End {
		opts := sling.NewParseOptions(sling.HookStageEnd, sling.HookKindHook, i, state, ctx)
		hook, err := sling.ParseHook(hookRaw, opts)
		if err != nil {
			return g.Error(err, "error parsing end hook %d for model '%s'", i+1, model.Name)
		}
		if hook != nil {
			model.endHooks = append(model.endHooks, hook)
		}
	}

	return nil
}

// executeModel executes a single model based on its mode.
func (e *Executor) executeModel(model *Model, mode string) error {
	// Parse and execute start hooks
	if err := e.parseModelHooks(model, mode); err != nil {
		return g.Error(err, "could not parse hooks for model '%s'", model.Name)
	}

	if len(model.startHooks) > 0 {
		g.Debug("running start hooks for %s", model.Name)
		if err := model.startHooks.Execute(); err != nil {
			return g.Error(err, "start hook failed for model '%s'", model.Name)
		}
	}

	// Pre-statements (from multi-statement SQL file)
	for i, stmt := range model.PreStatements {
		g.Debug("running pre-statement %d/%d for %s", i+1, len(model.PreStatements), model.Name)
		if _, err := e.DbConn.Exec(stmt); err != nil {
			return g.Error(err, "pre-statement %d failed for model '%s'", i+1, model.Name)
		}
	}

	var err error
	switch mode {
	case "full-refresh":
		err = e.executeFullRefresh(model)
	case "view":
		err = e.executeView(model)
	case "truncate":
		err = e.executeTruncate(model)
	case "incremental":
		err = e.executeIncremental(model)
	case "append", "snapshot": // snapshot is deprecated alias
		err = e.executeAppend(model)
	default:
		err = g.Error("unknown mode '%s' for model '%s'", mode, model.Name)
	}

	if err != nil {
		return err
	}

	// Post-statements (from multi-statement SQL file)
	for i, stmt := range model.PostStatements {
		g.Debug("running post-statement %d/%d for %s", i+1, len(model.PostStatements), model.Name)
		if _, err := e.DbConn.Exec(stmt); err != nil {
			return g.Error(err, "post-statement %d failed for model '%s'", i+1, model.Name)
		}
	}

	// Declarative data tests
	if len(model.Config.Tests) > 0 {
		if err := e.executeModelTests(model); err != nil {
			return err
		}
	}

	// End hooks
	if len(model.endHooks) > 0 {
		g.Debug("running end hooks for %s", model.Name)
		if err := model.endHooks.Execute(); err != nil {
			return g.Error(err, "end hook failed for model '%s'", model.Name)
		}
	}

	return nil
}

// executeFullRefresh rebuilds the table. Prefer atomic swap (tmp + rename) or
// CREATE OR REPLACE TABLE where supported so the target is never missing mid-build.
func (e *Executor) executeFullRefresh(model *Model) error {
	sql := model.CompiledSQL
	cascade := e.wantCascade(model)

	// Drop any existing view first (model may previously have been a view)
	_ = e.dropView(model.FullTableName, cascade)

	// Path A: CREATE OR REPLACE TABLE (Snowflake, BigQuery, DuckDB, Databricks)
	if supportsCreateOrReplaceTable(e.DbConn.GetType()) && !e.isClickHouse() {
		return e.createOrReplaceTableAs(model.FullTableName, sql, model)
	}

	// Path B: atomic temp + rename swap when rename is available
	if e.supportsRenameTable() {
		return e.executeFullRefreshAtomic(model, sql, cascade)
	}

	// Path C: fallback — drop then CTAS (brief downtime window)
	if err := e.dropTable(model.FullTableName, cascade); err != nil {
		return err
	}
	return e.createTableAs(model.FullTableName, sql, model)
}

// executeFullRefreshAtomic creates into a temp table, then renames into place.
func (e *Executor) executeFullRefreshAtomic(model *Model, sql string, cascade bool) error {
	tmpFull := e.getTempTableName(model)
	// Ensure temp is clean
	_ = e.dropTable(tmpFull, false)

	if err := e.createTableAs(tmpFull, sql, model); err != nil {
		_ = e.dropTable(tmpFull, false)
		return g.Error(err, "could not create temp table for full-refresh of '%s'", model.Name)
	}

	// Drop target (or rename aside if we want even less downtime — drop is fine
	// once data is ready in tmp; window is only drop+rename, not CTAS duration)
	if err := e.dropTable(model.FullTableName, cascade); err != nil {
		_ = e.dropTable(tmpFull, false)
		return err
	}

	if err := e.renameTable(tmpFull, model.FullTableName); err != nil {
		// Best-effort: leave tmp so data isn't lost
		return g.Error(err, "could not rename temp table to '%s' (temp left at %s)", model.FullTableName, tmpFull)
	}
	return nil
}

// executeView creates or replaces a view.
func (e *Executor) executeView(model *Model) error {
	cascade := e.wantCascade(model)

	// Drop any existing table first (dirty fixtures plant tables that
	// staging models then replace with views). CREATE OR REPLACE VIEW
	// fails on Postgres when the name is already a table.
	if err := e.dropTable(model.FullTableName, cascade); err != nil {
		g.Debug("drop table before view creation for %s: %s", model.Name, err)
	}
	if err := e.dropView(model.FullTableName, cascade); err != nil {
		g.Debug("drop view before view creation for %s: %s", model.Name, err)
	}

	return e.createOrReplaceView(model.FullTableName, model.CompiledSQL)
}

// executeTruncate creates the table on first run, truncates + inserts on subsequent runs.
func (e *Executor) executeTruncate(model *Model) error {
	sql := model.CompiledSQL

	exists, err := e.tableExists(model)
	if err != nil {
		return err
	}

	if !exists {
		return e.createTableAs(model.FullTableName, sql, model)
	}

	if err := e.truncateTable(model.FullTableName); err != nil {
		return err
	}
	return e.insertSelect(model.FullTableName, sql)
}

// executeIncremental dispatches to the correct incremental strategy based on the
// model's detected style. dbt-style models use executeLegacyIncremental (the
// original is_incremental() path); sling-style models use resolveRange + executeRange.
func (e *Executor) executeIncremental(model *Model) error {
	uniqueKeys := getUniqueKeys(model)
	if len(uniqueKeys) == 0 {
		return g.Error("model '%s' uses incremental mode but has no unique_key defined in config()", model.Name)
	}

	exists, err := e.tableExists(model)
	if err != nil {
		return err
	}
	if !exists || e.Build.Options.FullRefresh {
		return e.executeFullRefresh(model)
	}

	switch model.Style {
	case StyleDbt:
		if e.Build.Options.Range != nil {
			return g.Error("model '%s' uses is_incremental() (dbt style); --range requires {incremental_where_cond} (sling style)", model.Name)
		}
		return e.executeLegacyIncremental(model)
	case StyleSling:
		r, err := e.resolveRange(model)
		if err != nil {
			return err
		}
		return e.executeRange(model, r)
	default:
		return g.Error("model '%s': unknown incremental style %d", model.Name, model.Style)
	}
}

// executeLegacyIncremental is the original dbt-style incremental path, preserved
// byte-for-byte. Recompiles with is_incremental()=true, stages into a temp table,
// and merges using the configured strategy.
func (e *Executor) executeLegacyIncremental(model *Model) error {
	t := model.FullTableName

	// Subsequent run: recompile with is_incremental()=true
	_, err := e.Build.Engine.CompileModel(model, &IncrementalContext{IsIncremental: true})
	if err != nil {
		return g.Error(err, "could not compile incremental SQL for '%s'", model.Name)
	}

	// Re-apply table reference rewriting after incremental recompilation
	if model.Config.Rewrite == nil || *model.Config.Rewrite {
		rewritten, _ := RewriteTableReferences(model.CompiledSQL, e.Build.Project, model.Name)
		model.CompiledSQL = rewritten
	}

	// Re-split after recompilation since CompiledSQL changed
	result, splitErr := MakeModelSQL(model.CompiledSQL, e.DbConn.GetType())
	if splitErr != nil {
		return g.Error(splitErr, "could not parse incremental SQL for '%s'", model.Name)
	}
	model.CompiledSQL = result.ModelQuery
	incrementalSQL := model.CompiledSQL

	// Get merge strategy
	strategy := e.getMergeStrategy(model)

	// Create temp table with incremental results
	tempTable := e.getTempTableName(model)
	defer func() {
		if dropErr := e.dropTable(tempTable, false); dropErr != nil {
			g.Debug("could not drop temp table %s: %s", tempTable, dropErr)
		}
	}()

	// ClickHouse temp staging uses Memory engine (model=nil → Memory default in createTableAs)
	if e.isClickHouse() {
		quoted, qErr := e.quoteFullTableName(tempTable)
		if qErr != nil {
			return qErr
		}
		_, err = e.DbConn.Exec(g.F("CREATE TABLE %s ENGINE = Memory AS (%s)", quoted, incrementalSQL))
	} else {
		err = e.createTableAs(tempTable, incrementalSQL, nil)
	}
	if err != nil {
		return g.Error(err, "could not create temp table for incremental merge on '%s'", model.Name)
	}

	// Generate and execute merge SQL using sling's merge infrastructure
	uniqueKeys := getUniqueKeys(model)
	// Quote target/temp for merge; GenerateMergeSQL expects usable identifiers
	tgtQuoted, err := e.quoteFullTableName(t)
	if err != nil {
		return err
	}
	tmpQuoted, err := e.quoteFullTableName(tempTable)
	if err != nil {
		return err
	}
	mergeSQL, err := e.DbConn.GenerateMergeSQLWithStrategy(tmpQuoted, tgtQuoted, uniqueKeys, &strategy)
	if err != nil {
		return g.Error(err, "could not generate merge SQL for '%s'", model.Name)
	}

	_, err = e.DbConn.ExecMulti(mergeSQL)
	if err != nil {
		return g.Error(err, "could not execute incremental merge for '%s'", model.Name)
	}

	return nil
}

// executeAppend handles append-only mode (formerly "snapshot").
// First run: CTAS. Subsequent: INSERT.
func (e *Executor) executeAppend(model *Model) error {
	sql := model.CompiledSQL

	exists, err := e.tableExists(model)
	if err != nil {
		return err
	}

	if !exists {
		return e.createTableAs(model.FullTableName, sql, model)
	}

	return e.insertSelect(model.FullTableName, sql)
}

// tableExists checks whether the model's target table exists.
func (e *Executor) tableExists(model *Model) (bool, error) {
	table, err := database.ParseTableName(model.FullTableName, e.DbConn.GetType())
	if err != nil {
		return false, g.Error(err, "could not parse table name '%s'", model.FullTableName)
	}
	return e.DbConn.TableExists(table)
}

// isClickHouse returns true if the target database is ClickHouse or Proton.
func (e *Executor) isClickHouse() bool {
	return g.In(e.DbConn.GetType(), dbio.TypeDbClickhouse, dbio.TypeDbProton)
}

// getEngineClause returns the ClickHouse ENGINE clause for a model.
func getEngineClause(model *Model) string {
	engine := model.Config.Engine
	if engine == "" {
		engine = "MergeTree()"
	}
	return g.F("ENGINE = %s", engine)
}

// getOrderByClause returns the ClickHouse ORDER BY clause for a model.
// quote, when non-nil, is applied to each key.
func getOrderByClause(model *Model, quote func(string) string) string {
	keys := getUniqueKeys(model)
	if len(keys) == 0 {
		return "ORDER BY tuple()"
	}
	quoted := make([]string, len(keys))
	for i, k := range keys {
		if quote != nil {
			quoted[i] = quote(k)
		} else {
			quoted[i] = k
		}
	}
	return g.F("ORDER BY (%s)", strings.Join(quoted, ", "))
}

// getUniqueKeys extracts the unique key(s) from the model config.
func getUniqueKeys(model *Model) []string {
	if model.Config.UniqueKey == nil {
		return nil
	}
	switch v := model.Config.UniqueKey.(type) {
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	case []string:
		return v
	case []interface{}:
		keys := make([]string, 0, len(v))
		for _, k := range v {
			keys = append(keys, fmt.Sprint(k))
		}
		return keys
	}
	return nil
}

// method wrappers that delegate to package-level functions
func (e *Executor) getEngineClause(model *Model) string { return getEngineClause(model) }
func (e *Executor) getOrderByClause(model *Model) string {
	return getOrderByClause(model, e.DbConn.Quote)
}

// getMergeStrategy maps the user-facing merge_strategy string to a database.MergeStrategy constant.
// For ClickHouse, it forces delete+insert since ClickHouse doesn't support UPDATE/MERGE.
func (e *Executor) getMergeStrategy(model *Model) database.MergeStrategy {
	return getMergeStrategy(model, e.isClickHouse())
}

// getMergeStrategy is the package-level implementation for merge strategy resolution.
func getMergeStrategy(model *Model, isClickHouse bool) database.MergeStrategy {
	userStrategy := model.Config.MergeStrategy

	// ClickHouse: force delete+insert
	if isClickHouse {
		if userStrategy != "" && userStrategy != "delete+insert" {
			g.Warn("ClickHouse does not support '%s' merge strategy; using delete+insert instead", userStrategy)
		}
		return database.MergeStrategyDeleteInsert
	}

	switch userStrategy {
	case "delete+insert":
		return database.MergeStrategyDeleteInsert
	case "update+insert":
		return database.MergeStrategyUpdateInsert
	case "insert":
		return database.MergeStrategyInsert
	default:
		return database.MergeStrategyDeleteInsert
	}
}

// getTempTableName returns a unique schema-qualified temp table for this run.
// The run ID prevents collisions across concurrent builds of the same model.
func (e *Executor) getTempTableName(model *Model) string {
	return getTempTableName(model, e.RunID)
}

// getTempTableName is the package-level implementation.
// Uses a schema-qualified name so that GetColumns() can find the table for merge SQL generation.
func getTempTableName(model *Model, runID string) string {
	if runID == "" {
		runID = "x"
	}
	// Keep identifier safe: alphanumeric + underscore only
	safeName := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, model.Name)
	tempName := g.F("_sling_build_tmp_%s_%s", safeName, runID)
	return g.F("%s.%s", model.Schema, tempName)
}

// printProgress prints a single line of execution progress.
// When started is true, prints the "START" line (no duration).
// When started is false, prints the final status with duration.
func (e *Executor) printProgress(index, total int, result ExecutionResult, started bool) {
	nodeType := result.Mode
	if result.NodeType == "seed" {
		nodeType = "seed"
	}

	// Format: [1/8] staging.country_codes (seed) ........... START
	// Format: [1/8] staging.country_codes (seed) ........... OK (0.2s)
	prefix := g.F("[%d/%d] %s (%s) ", index, total, result.Name, env.DarkGrayString(nodeType))

	dotsLen := 70 - len(prefix)
	if dotsLen < 3 {
		dotsLen = 3
	}
	dots := strings.Repeat(".", dotsLen)

	if started {
		e.ctx.Info("%s%s %s", prefix, dots, "START")
		return
	}

	if result.Skipped {
		e.ctx.Info("%s%s %s", prefix, dots, env.YellowString("SKIP"))
		return
	}

	durationStr := formatDuration(result.Duration)
	status := env.GreenString("OK")
	if result.Err != nil {
		status = env.RedString("FAIL")
	}
	e.ctx.Info("%s%s %s (%s)", prefix, dots, status, durationStr)
}

// formatDuration formats a duration as a human-readable string.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return g.F("%dms", d.Milliseconds())
	}
	return g.F("%.1fs", d.Seconds())
}

// formatChunkProgressLine returns the indented sub-line for a single range chunk.
// Used for DEBUG-level per-chunk progress during multi-chunk --range backfills.
// Pure function — returns a string; caller decides log level and writer.
func formatChunkProgressLine(idx, total int, chunk RangeChunk, updateKey string, dur time.Duration, failed bool) string {
	status := env.GreenString("OK")
	if failed {
		status = env.RedString("FAIL")
	}
	return fmt.Sprintf("         chunk %d/%d  %s   %s (%s)",
		idx, total, chunk.Describe(updateKey), status, formatDuration(dur))
}

// formatResumeCommand returns the --range value for the resume hint.
// The failed chunk's lower bound becomes the new start; the original
// last chunk's upper bound remains the end.
func formatResumeCommand(failed, last RangeChunk, step string) string {
	raw := failed.LowerRaw + "," + last.UpperRaw
	if step != "" {
		raw = raw + "," + step
	}
	return raw
}

// printSummary prints the final execution summary.
func (e *Executor) printSummary() {
	var successes, failures, skipped int
	var totalDuration time.Duration
	var failedResults []ExecutionResult
	for _, r := range e.Results {
		totalDuration += r.Duration
		if r.Skipped {
			skipped++
		} else if r.Err != nil {
			failures++
			failedResults = append(failedResults, r)
		} else {
			successes++
		}
	}

	successStr := env.GreenString(g.F("%d Successes", successes))
	failureStr := g.F("%d Failures", failures)
	if failures > 0 {
		failureStr = env.RedString(failureStr)
	} else {
		failureStr = env.GreenString(failureStr)
	}
	skippedStr := ""
	if skipped > 0 {
		skippedStr = g.F(" | %s", env.YellowString(g.F("%d Skipped", skipped)))
	}

	g.Info("Build Completed in %s | %s | %s%s\n", g.DurationString(totalDuration), successStr, failureStr, skippedStr)

	// Print errors section
	if len(failedResults) > 0 {
		fmt.Println(env.RedString("Errors:"))
		for _, r := range failedResults {
			errMsg := strings.ReplaceAll(strings.TrimSpace(g.ErrMsgSimple(r.Err)), "\n", "\n      ")
			fmt.Printf("  - %s:\n      %s\n", r.Name, env.RedString(errMsg))
		}
	}
}

// Range is the resolved set of chunks for a single build model execution.
type Range struct {
	Chunks      []RangeChunk // ordered; 0 chunks = no-op
	UpdateState bool         // advance SLING_STATE after last chunk succeeds
	FromCLI     bool         // came from --range (print resume hint on failure)
	CLIRaw      string       // original raw --range value (for resume hint)
	Step        string       // parsed step for resume hint, may be ""
}

// RangeChunk is a single [lower, upper) window for one merge pass.
type RangeChunk struct {
	Lower          string         // already-quoted SQL literal, or "" for unbounded
	Upper          string         // already-quoted SQL literal, or "" for unbounded
	LowerInclusive bool           // true → use >= for lower
	ColType        iop.ColumnType // for state writes
	LowerRaw       string         // raw display form for logs/resume hint
	UpperRaw       string         // raw display form for logs/resume hint
}

// WhereCond returns the WHERE clause body for this chunk.
func (c RangeChunk) WhereCond(updateKey string, quote func(string) string) string {
	qKey := quote(updateKey)
	hasLower := c.Lower != "" && c.Lower != "null"
	hasUpper := c.Upper != "" && c.Upper != "null"
	switch {
	case !hasLower && !hasUpper:
		return "1=1"
	case !hasLower:
		return fmt.Sprintf("%s < %s", qKey, c.Upper)
	case !hasUpper:
		if c.LowerInclusive {
			return fmt.Sprintf("%s >= %s", qKey, c.Lower)
		}
		return fmt.Sprintf("%s > %s", qKey, c.Lower)
	default:
		lowerOp := ">"
		if c.LowerInclusive {
			lowerOp = ">="
		}
		return fmt.Sprintf("%s %s %s AND %s < %s", qKey, lowerOp, c.Lower, qKey, c.Upper)
	}
}

// Describe is used for log lines.
func (c RangeChunk) Describe(updateKey string) string {
	lower := c.LowerRaw
	if lower == "" {
		lower = "∅"
	}
	upper := c.UpperRaw
	if upper == "" {
		upper = "now"
	}
	bracket := "("
	if c.LowerInclusive {
		bracket = "["
	}
	return fmt.Sprintf("%s=%s%s, %s)", updateKey, bracket, lower, upper)
}

// parseValueAsTime turns a state value into time.Time. Only valid for
// date/datetime column types (enforced by caller).
func parseValueAsTime(value string, colType iop.ColumnType) (time.Time, error) {
	if value == "" {
		return time.Time{}, g.Error("empty state value cannot be parsed as time")
	}
	if colType != "" && !colType.IsDatetime() && !colType.IsDate() {
		return time.Time{}, g.Error("range.advance/lookback requires datetime/date update_key, got %q", string(colType))
	}
	t, err := cast.ToTimeE(value)
	if err != nil {
		return time.Time{}, g.Error(err, "could not parse value %q as time", value)
	}
	return t, nil
}

// quoteValue formats a value as a SQL literal for the given DB.
// Delegates to iop.FormatValue (core/dbio/iop/datatype.go:1643).
func quoteValue(value any, colType iop.ColumnType, dbType dbio.Type) string {
	if value == nil {
		return "null"
	}
	if t, ok := value.(time.Time); ok {
		if colType == "" {
			colType = iop.TimestampType
		}
		return iop.FormatValue(t, colType, dbType)
	}
	if colType == "" {
		colType = iop.StringType
	}
	return iop.FormatValue(value, colType, dbType)
}

// parseCLIRange parses --range "start,end[,step]".
func parseCLIRange(raw string) (start, end, step string, hasStep bool, err error) {
	parts := strings.Split(raw, ",")
	if len(parts) < 2 || len(parts) > 3 {
		return "", "", "", false, g.Error("invalid --range %q: expected 'start,end' or 'start,end,step'", raw)
	}
	start = strings.TrimSpace(parts[0])
	end = strings.TrimSpace(parts[1])
	if start == "" || end == "" {
		return "", "", "", false, g.Error("invalid --range %q: start and end must be non-empty", raw)
	}
	if len(parts) == 3 {
		step = strings.TrimSpace(parts[2])
		if step != "" {
			if _, err := parseBuildDuration(step); err != nil {
				return "", "", "", false, g.Error(err, "invalid --range step")
			}
			hasStep = true
		}
	}
	return
}

// splitCLIRange produces chunks from a raw --range string. colType=""
// makes quoteValue treat values as string literals (ISO dates implicit-cast
// correctly on every dialect).
func splitCLIRange(raw string, dbType dbio.Type, colType iop.ColumnType) (*Range, error) {
	start, end, stepStr, hasStep, err := parseCLIRange(raw)
	if err != nil {
		return nil, err
	}
	r := &Range{UpdateState: false, FromCLI: true, CLIRaw: raw, Step: stepStr}
	if !hasStep {
		r.Chunks = []RangeChunk{{
			Lower:          quoteValue(start, colType, dbType),
			Upper:          quoteValue(end, colType, dbType),
			LowerInclusive: true, // CLI backfills are inclusive-start
			ColType:        colType,
			LowerRaw:       start,
			UpperRaw:       end,
		}}
		return r, nil
	}
	step, _ := parseBuildDuration(stepStr)
	startT, err := cast.ToTimeE(start)
	if err != nil {
		return nil, g.Error(err, "--range with step requires ISO date/timestamp start")
	}
	endT, err := cast.ToTimeE(end)
	if err != nil {
		return nil, g.Error(err, "--range with step requires ISO date/timestamp end")
	}
	for cur := startT; cur.Before(endT); cur = cur.Add(step) {
		upper := cur.Add(step)
		if upper.After(endT) {
			upper = endT
		}
		r.Chunks = append(r.Chunks, RangeChunk{
			Lower:          quoteValue(cur, colType, dbType),
			Upper:          quoteValue(upper, colType, dbType),
			LowerInclusive: true,
			ColType:        colType,
			LowerRaw:       cur.Format(time.RFC3339),
			UpperRaw:       upper.Format(time.RFC3339),
		})
	}
	return r, nil
}

// resolveRange decides which range-resolution strategy to use and returns the
// ordered set of chunks to execute. Returns a Range with 0 chunks as a no-op.
func (e *Executor) resolveRange(model *Model) (*Range, error) {
	// --range CLI flag wins over all automatic resolution
	if e.Build.Options.Range != nil {
		return e.resolveCLIRange(model)
	}

	rc := model.Config.Range
	if rc != nil && rc.HasAdvance() {
		return e.resolveAdvanceRange(model)
	}

	return e.resolveIncrementalRange(model)
}

// resolveCLIRange parses the --range flag and produces one or more chunks.
// State is never advanced for CLI backfills.
func (e *Executor) resolveCLIRange(model *Model) (*Range, error) {
	raw := *e.Build.Options.Range
	dbType := e.DbConn.GetType()
	return splitCLIRange(raw, dbType, "")
}

// resolveIncrementalRange resolves the watermark via tier A/B/C and applies
// optional lookback. This is used when there is no step (plain incremental).
//
//   - Tier A: SLING_STATE is configured → read from state store
//   - Tier B: target table has rows → SELECT MAX(update_key)
//   - Tier C: first run → unbounded lower (full-refresh semantics)
func (e *Executor) resolveIncrementalRange(model *Model) (*Range, error) {
	updateKey := model.Config.UpdateKey
	if updateKey == "" {
		return nil, g.Error("model '%s': sling-style incremental requires update_key in config()", model.Name)
	}

	dbType := e.DbConn.GetType()

	var lowerRaw string
	var colType iop.ColumnType
	lowerInclusive := false

	// Tier A: state store
	if sling.IsStateConfigured() {
		rec, err := sling.ReadState(model.Name, model.FullTableName)
		if err != nil {
			return nil, g.Error(err, "could not read SLING_STATE for model '%s'", model.Name)
		}
		if rec != nil && rec.IsValid() {
			lowerRaw = rec.Value
			colType = rec.ColumnType
			g.Debug("build[%s]: tier A — state value %q", model.Name, lowerRaw)
		}
	}

	// Tier B: query target table max
	if lowerRaw == "" {
		maxVal, maxType, err := e.queryTargetMax(model, updateKey)
		if err != nil {
			g.Warn("build[%s]: tier B probe failed (%s); falling through to first-run", model.Name, err)
		} else if maxVal != "" {
			lowerRaw = maxVal
			colType = maxType
			g.Debug("build[%s]: tier B — target MAX %q", model.Name, lowerRaw)
		}
	}

	// Tier C: first run — unbounded lower
	if lowerRaw == "" {
		g.Debug("build[%s]: tier C — first run, no lower bound", model.Name)
		r := &Range{
			UpdateState: true,
			Chunks: []RangeChunk{{
				Lower:          "",
				Upper:          "",
				LowerInclusive: false,
				ColType:        colType,
			}},
		}
		return r, nil
	}

	// Apply lookback
	rc := model.Config.Range
	if rc != nil && rc.HasLookback() {
		dur, err := parseBuildDuration(rc.Lookback)
		if err != nil {
			return nil, g.Error(err, "model '%s': invalid range.lookback", model.Name)
		}
		t, err := parseValueAsTime(lowerRaw, colType)
		if err != nil {
			return nil, g.Error(err, "model '%s': could not apply lookback to state value", model.Name)
		}
		lowerRaw = t.Add(-dur).Format(time.RFC3339)
		lowerInclusive = true
	}

	lower := quoteValue(lowerRaw, colType, dbType)
	return &Range{
		UpdateState: true,
		Chunks: []RangeChunk{{
			Lower:          lower,
			Upper:          "",
			LowerInclusive: lowerInclusive,
			ColType:        colType,
			LowerRaw:       lowerRaw,
		}},
	}, nil
}

// resolveAdvanceRange resolves a range that moves forward one "advance" window
// per run. Requires SLING_STATE. On first run, probes source for MIN(update_key)
// (or uses range.start). On subsequent runs, advances by one advance-window from
// the last state value.
func (e *Executor) resolveAdvanceRange(model *Model) (*Range, error) {
	if !sling.IsStateConfigured() {
		return nil, g.Error("model '%s': range.advance requires SLING_STATE to be configured", model.Name)
	}

	rc := model.Config.Range
	advance, err := parseBuildDuration(rc.Advance)
	if err != nil {
		return nil, g.Error(err, "model '%s': invalid range.advance", model.Name)
	}

	updateKey := model.Config.UpdateKey
	if updateKey == "" {
		return nil, g.Error("model '%s': range.advance requires update_key in config()", model.Name)
	}

	dbType := e.DbConn.GetType()
	var colType iop.ColumnType

	// Read existing state
	rec, err := sling.ReadState(model.Name, model.FullTableName)
	if err != nil {
		return nil, g.Error(err, "could not read SLING_STATE for model '%s'", model.Name)
	}

	now := time.Now().UTC()

	if rec != nil && rec.IsValid() {
		// Subsequent run: advance from last state
		stateT, err := parseValueAsTime(rec.Value, rec.ColumnType)
		if err != nil {
			return nil, g.Error(err, "model '%s': could not parse state value for advance range", model.Name)
		}

		lower := stateT
		if rc.HasLookback() {
			lb, err := parseBuildDuration(rc.Lookback)
			if err != nil {
				return nil, g.Error(err, "model '%s': invalid range.lookback", model.Name)
			}
			lower = stateT.Add(-lb)
		}
		upper := stateT.Add(advance)
		if upper.After(now) {
			upper = now
		}

		if !lower.Before(upper) {
			g.Debug("build[%s]: advance range caught up (lower >= upper), no-op", model.Name)
			return &Range{UpdateState: false, Chunks: nil}, nil
		}

		lowerInclusive := rc.HasLookback()
		colType = rec.ColumnType
		return &Range{
			UpdateState: true,
			Step:        rc.Advance,
			Chunks: []RangeChunk{{
				Lower:          quoteValue(lower, colType, dbType),
				Upper:          quoteValue(upper, colType, dbType),
				LowerInclusive: lowerInclusive,
				ColType:        colType,
				LowerRaw:       lower.Format(time.RFC3339),
				UpperRaw:       upper.Format(time.RFC3339),
			}},
		}, nil
	}

	// First run: resolve origin
	var originT time.Time
	if rc.Start != "" {
		originT, err = parseValueAsTime(rc.Start, "")
		if err != nil {
			return nil, g.Error(err, "model '%s': could not parse range.start", model.Name)
		}
		colType = iop.TimestampType
	} else {
		// Probe source for MIN(update_key)
		minVal, minType, err := e.probeSourceMin(model, updateKey)
		if err != nil {
			return nil, g.Error(err, "model '%s': could not probe source for range origin", model.Name)
		}
		if minVal == "" {
			g.Debug("build[%s]: advance first-run probe returned empty source; no-op", model.Name)
			return &Range{UpdateState: false, Chunks: nil}, nil
		}
		colType = minType
		originT, err = parseValueAsTime(minVal, colType)
		if err != nil {
			return nil, g.Error(err, "model '%s': could not parse probed origin", model.Name)
		}
	}

	// Cache origin in state immediately (idempotent on crash/retry)
	if err := sling.WriteState(model.Name, model.FullTableName, originT.Format(time.RFC3339), colType); err != nil {
		g.Warn("build[%s]: could not cache advance origin in SLING_STATE: %s", model.Name, err)
	}

	upper := originT.Add(advance)
	if upper.After(now) {
		upper = now
	}
	if !originT.Before(upper) {
		return &Range{UpdateState: false, Chunks: nil}, nil
	}

	return &Range{
		UpdateState: true,
		Step:        rc.Advance,
		Chunks: []RangeChunk{{
			Lower:          quoteValue(originT, colType, dbType),
			Upper:          quoteValue(upper, colType, dbType),
			LowerInclusive: true, // first-run: inclusive of origin
			ColType:        colType,
			LowerRaw:       originT.Format(time.RFC3339),
			UpperRaw:       upper.Format(time.RFC3339),
		}},
	}, nil
}

// probeSourceMin compiles the model with default context, rewrites refs, builds
// the model SQL, and runs SELECT MIN(update_key) FROM (<sql>) __sling_probe.
func (e *Executor) probeSourceMin(model *Model, updateKey string) (string, iop.ColumnType, error) {
	// Save and restore CompiledSQL so the probe doesn't pollute model state
	savedSQL := model.CompiledSQL

	_, err := e.Build.Engine.CompileModel(model, DefaultIncrementalContext())
	if err != nil {
		model.CompiledSQL = savedSQL
		return "", "", g.Error(err, "probeSourceMin: could not compile model '%s'", model.Name)
	}

	rewritten, _ := RewriteTableReferences(model.CompiledSQL, e.Build.Project, model.Name)
	result, err := MakeModelSQL(rewritten, e.DbConn.GetType())
	model.CompiledSQL = savedSQL // always restore

	if err != nil {
		return "", "", g.Error(err, "probeSourceMin: could not parse SQL for model '%s'", model.Name)
	}

	probeSQL := fmt.Sprintf("SELECT MIN(%s) FROM (%s) __sling_probe",
		e.DbConn.Quote(updateKey), result.ModelQuery)

	data, err := e.DbConn.Query(probeSQL)
	if err != nil {
		return "", "", g.Error(err, "probeSourceMin: query failed for model '%s'", model.Name)
	}
	if len(data.Rows) == 0 || len(data.Rows[0]) == 0 || data.Rows[0][0] == nil {
		return "", "", nil
	}

	var colType iop.ColumnType
	if len(data.Columns) > 0 {
		colType = data.Columns[0].Type
	}
	if colType == "" {
		colType = iop.TimestampType
	}

	return fmt.Sprint(data.Rows[0][0]), colType, nil
}

// queryTargetMax runs SELECT MAX(update_key) FROM full_table_name.
func (e *Executor) queryTargetMax(model *Model, updateKey string) (string, iop.ColumnType, error) {
	quoted, err := e.quoteFullTableName(model.FullTableName)
	if err != nil {
		return "", "", err
	}
	sql := fmt.Sprintf("SELECT MAX(%s) FROM %s",
		e.DbConn.Quote(updateKey), quoted)

	data, err := e.DbConn.Query(sql)
	if err != nil {
		return "", "", err
	}
	if len(data.Rows) == 0 || len(data.Rows[0]) == 0 || data.Rows[0][0] == nil {
		return "", "", nil
	}

	var colType iop.ColumnType
	if len(data.Columns) > 0 {
		colType = data.Columns[0].Type
	}
	if colType == "" {
		colType = iop.TimestampType
	}

	return fmt.Sprint(data.Rows[0][0]), colType, nil
}

// executeRange iterates over the chunks in r and runs a merge for each one.
// On any chunk failure it calls handleChunkError and returns.
// On all-success it conditionally advances SLING_STATE.
func (e *Executor) executeRange(model *Model, r *Range) error {
	if len(r.Chunks) == 0 {
		g.Debug("build[%s]: range resolved to 0 chunks; skipping", model.Name)
		return nil
	}

	updateKey := model.Config.UpdateKey
	if updateKey == "" {
		return g.Error("model '%s': sling-style incremental requires update_key in config()", model.Name)
	}

	multi := len(r.Chunks) > 1
	for i, chunk := range r.Chunks {
		whereCond := chunk.WhereCond(updateKey, e.DbConn.Quote)
		valueLit := chunk.Lower
		if valueLit == "" || valueLit == "null" {
			valueLit = "null"
		}

		incCtx := &IncrementalContext{
			IsIncremental: true,
			WhereCond:     whereCond,
			Value:         valueLit,
		}

		chunkStart := time.Now()
		chunkErr := e.runMergeForChunk(model, incCtx)
		chunkDur := time.Since(chunkStart)

		if multi {
			g.Debug("%s", formatChunkProgressLine(i+1, len(r.Chunks), chunk, updateKey, chunkDur, chunkErr != nil))
		} else {
			g.Debug("build[%s]: chunk %d/%d %s (%s)",
				model.Name, i+1, len(r.Chunks), chunk.Describe(updateKey), formatDuration(chunkDur))
		}

		if chunkErr != nil {
			return e.handleChunkError(model, r, i, chunkErr)
		}
	}

	// Advance state after successful run
	if r.UpdateState && sling.IsStateConfigured() {
		if err := e.advanceStateAfterRange(model, r, updateKey); err != nil {
			g.Warn("build[%s]: could not advance SLING_STATE: %s", model.Name, err)
		}
	}

	return nil
}

// runMergeForChunk compiles the model with incCtx, rewrites refs, and executes
// the temp-table + merge strategy. This is factored from executeLegacyIncremental.
func (e *Executor) runMergeForChunk(model *Model, incCtx *IncrementalContext) error {
	t := model.FullTableName
	uniqueKeys := getUniqueKeys(model)

	_, err := e.Build.Engine.CompileModel(model, incCtx)
	if err != nil {
		return g.Error(err, "could not compile incremental SQL for '%s'", model.Name)
	}

	// Honor rewrite: false
	if model.Config.Rewrite == nil || *model.Config.Rewrite {
		rewritten, _ := RewriteTableReferences(model.CompiledSQL, e.Build.Project, model.Name)
		model.CompiledSQL = rewritten
	}

	result, splitErr := MakeModelSQL(model.CompiledSQL, e.DbConn.GetType())
	if splitErr != nil {
		return g.Error(splitErr, "could not parse incremental SQL for '%s'", model.Name)
	}
	model.CompiledSQL = result.ModelQuery
	incrementalSQL := model.CompiledSQL

	strategy := e.getMergeStrategy(model)
	tempTable := e.getTempTableName(model)
	defer func() {
		if dropErr := e.dropTable(tempTable, false); dropErr != nil {
			g.Debug("could not drop temp table %s: %s", tempTable, dropErr)
		}
	}()

	if e.isClickHouse() {
		quoted, qErr := e.quoteFullTableName(tempTable)
		if qErr != nil {
			return qErr
		}
		_, err = e.DbConn.Exec(g.F("CREATE TABLE %s ENGINE = Memory AS (%s)", quoted, incrementalSQL))
	} else {
		err = e.createTableAs(tempTable, incrementalSQL, nil)
	}
	if err != nil {
		return g.Error(err, "could not create temp table for incremental merge on '%s'", model.Name)
	}

	tgtQuoted, err := e.quoteFullTableName(t)
	if err != nil {
		return err
	}
	tmpQuoted, err := e.quoteFullTableName(tempTable)
	if err != nil {
		return err
	}
	mergeSQL, err := e.DbConn.GenerateMergeSQLWithStrategy(tmpQuoted, tgtQuoted, uniqueKeys, &strategy)
	if err != nil {
		return g.Error(err, "could not generate merge SQL for '%s'", model.Name)
	}

	_, err = e.DbConn.ExecMulti(mergeSQL)
	if err != nil {
		return g.Error(err, "could not execute incremental merge for '%s'", model.Name)
	}

	return nil
}

// advanceStateAfterRange writes the final watermark to SLING_STATE.
// For bounded upper (paged): writes last chunk's UpperRaw.
// For unbounded upper (plain incremental): queries target MAX post-merge.
func (e *Executor) advanceStateAfterRange(model *Model, r *Range, updateKey string) error {
	last := r.Chunks[len(r.Chunks)-1]

	if last.Upper != "" && last.Upper != "null" {
		// Paged / bounded — advance to upper bound
		return sling.WriteState(model.Name, model.FullTableName, last.UpperRaw, last.ColType)
	}

	// Unbounded upper — query actual max from target
	maxVal, maxType, err := e.queryTargetMax(model, updateKey)
	if err != nil {
		return g.Error(err, "advanceStateAfterRange: queryTargetMax failed")
	}
	if maxVal == "" {
		g.Warn("build[%s]: target table appears empty after merge; leaving state unchanged", model.Name)
		return nil
	}

	colType := last.ColType
	if colType == "" {
		colType = maxType
	}
	return sling.WriteState(model.Name, model.FullTableName, maxVal, colType)
}

// handleChunkError formats an error from a failed chunk and optionally prints
// a resume hint when the range came from --range.
func (e *Executor) handleChunkError(model *Model, r *Range, failedIdx int, err error) error {
	chunk := r.Chunks[failedIdx]
	if r.FromCLI {
		e.printResumeHint(model, r, failedIdx, chunk)
	}
	return g.Error(err, "build[%s]: chunk %d failed (%s)",
		model.Name, failedIdx+1, chunk.Describe(model.Config.UpdateKey))
}

// printResumeHint prints a sling build --range command the user can re-run to
// resume from the failed chunk. Emits at INFO level (regardless of --debug) so
// operators can always see the recovery command in a wall of logs.
func (e *Executor) printResumeHint(model *Model, r *Range, failedIdx int, failed RangeChunk) {
	last := r.Chunks[len(r.Chunks)-1]
	raw := formatResumeCommand(failed, last, r.Step)
	e.ctx.Info("%s chunk %d/%d failed — resume with:",
		env.YellowString("▶"), failedIdx+1, len(r.Chunks))
	e.ctx.Info("  sling build --range '%s' -s %s", raw, model.Name)
}
