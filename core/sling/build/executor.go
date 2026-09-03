package build

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/slingdata-io/sling-cli/core/store"
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

	prevLogSink func(*g.LogLine)
	startTime   *time.Time

	// Per-model log buffers. env.LogSink is process-global
	modelLogs   map[string]g.LogLines
	modelLogsMu sync.Mutex
	gidModels   sync.Map // uint64 goroutine id → model name
}

// ExecutionResult holds the outcome of executing one node.
type ExecutionResult struct {
	Name      string
	NodeType  string // "seed", "model", or "test"
	Mode      string // "full-refresh", "view", "truncate", "incremental", "append"
	Duration  time.Duration
	Err       error
	Skipped   bool
	StartTime *time.Time
	// Rows is sql.Result.RowsAffected, or COUNT(*) on column-store
	// full-refresh / truncate / first-run append. Incremental and subsequent
	// append count the staging temp table. CREATE VIEW reports 0.
	Rows  uint64
	Bytes uint64
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
	Database string `json:"database,omitempty"`
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
		modelLogs:   make(map[string]g.LogLines),
	}, nil
}

// rowsFromResult returns RowsAffected clamped to 0.
// CREATE VIEW reports 0. Some drivers return -1.
func rowsFromResult(result sql.Result) uint64 {
	if result == nil {
		return 0
	}
	n, err := result.RowsAffected()
	if err != nil || n < 0 {
		return 0
	}
	return uint64(n)
}

func rowsFromExec(result sql.Result, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	return rowsFromResult(result), nil
}

// countIfColumnStore runs COUNT(*) on columnar engines. CTAS/INSERT/MERGE often
// report RowsAffected 0. Call on the target after full-refresh/truncate, or on
// the staging temp table after incremental/append.
func (e *Executor) countIfColumnStore(fullName string, rows uint64) uint64 {
	if e.DbConn == nil || !e.DbConn.GetType().IsColumnStore() {
		return rows
	}
	quoted, err := e.quoteFullTableName(fullName)
	if err != nil {
		g.Debug("skip count of %s: %s", fullName, err)
		return rows
	}
	n, err := e.DbConn.GetCount(quoted)
	if err != nil || n < 0 {
		g.Debug("count of %s failed: %s", quoted, err)
		return rows
	}
	return uint64(n)
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
	type schemaKey struct{ database, schema string }
	schemas := make(map[schemaKey]bool)
	for _, name := range e.Build.Selected {
		node := e.Build.DAG.Nodes[name]
		if node.Model != nil {
			schemas[schemaKey{node.Model.Database, node.Model.Schema}] = true
		}
		if node.Seed != nil {
			schemas[schemaKey{node.Seed.Database, node.Seed.Schema}] = true
		}
	}

	dbType := e.DbConn.GetType()
	for key := range schemas {
		// SQL Server family cannot CREATE SCHEMA in another database.
		if key.database != "" && g.In(dbType, dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH, dbio.TypeDbFabric) {
			if !strings.EqualFold(key.database, e.DbConn.GetProp("database")) {
				g.Warn("skipping schema creation for '%s.%s': %s cannot create a schema in another database", key.database, key.schema, dbType)
				continue
			}
		}

		qualifier, err := e.quoteSchemaQualifier(key.database, key.schema)
		if err != nil {
			return err
		}
		sql := g.R(
			e.DbConn.Template().Value("core.create_schema"),
			"schema", qualifier,
		)
		if _, err := e.DbConn.Exec(sql); err != nil {
			// Ignore "already exists" errors for databases without IF NOT EXISTS
			errLower := strings.ToLower(err.Error())
			if !strings.Contains(errLower, "already exists") && !strings.Contains(errLower, "duplicate") {
				return g.Error(err, "could not create schema '%s'", key.schema)
			}
		}
	}
	return nil
}

// quoteSchemaQualifier renders [database.]schema quoted for the dialect. It
// goes through ParseTableName so the case normalization matches the one
// applied to the model's own table name.
func (e *Executor) quoteSchemaQualifier(db, schema string) (string, error) {
	name := schema
	if db != "" {
		name = db + "." + schema
	}
	// ParseTableName needs a table part; add a placeholder and drop it after.
	table, err := database.ParseTableName(name+"._", e.DbConn.GetType())
	if err != nil {
		return "", g.Error(err, "could not parse schema name '%s'", name)
	}
	table.Name = ""
	return table.FDQN(), nil
}

// Execute runs all selected nodes with a ready-queue scheduler.
// A node is dispatched as soon as its selected dependencies complete (no level barriers).
func (e *Executor) Execute() error {
	start := time.Now()
	e.startTime = &start
	e.attachLogSink()
	defer e.detachLogSink()

	if err := e.Connect(); err != nil {
		if !sling.IsPipelineRunMode() {
			e.syncBuildStatus(sling.ExecStatusError, err)
		}
		return err
	}
	defer e.Close()

	if err := e.CreateSchemas(); err != nil {
		if !sling.IsPipelineRunMode() {
			e.syncBuildStatus(sling.ExecStatusError, err)
		}
		return err
	}

	total := len(e.Build.Selected)
	if total == 0 {
		fmt.Println("No models selected.")
		if !sling.IsPipelineRunMode() {
			e.syncBuildStatus(sling.ExecStatusSuccess, nil)
		}
		return nil
	}

	stopHB := func() {}
	if !sling.IsPipelineRunMode() {
		e.syncBuildStatus(sling.ExecStatusRunning, nil)
		stopHB = e.startBuildHeartbeat()
		defer stopHB()
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
			seedSkipped := result.Skipped && e.Build.Options.NoSeeds &&
				e.Build.DAG.Nodes[name] != nil && e.Build.DAG.Nodes[name].Seed != nil
			// failedSet is also read under e.ctx.Mux in runNode — hold both
			e.ctx.Mux.Lock()
			e.Results = append(e.Results, result)
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
		result := ExecutionResult{Name: name, Skipped: true, StartTime: g.Ptr(time.Now())}
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
		e.ctx.Mux.Lock()
		e.Results = append(e.Results, result)
		e.failedSet[name] = true
		e.ctx.Mux.Unlock()
		e.syncModelStatus(name, result, sling.ExecStatusSkipped)
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
		err := g.Error("build completed with %d error(s): %s", len(errs), strings.Join(msgs, "; "))
		stopHB()
		if !sling.IsPipelineRunMode() {
			e.syncBuildStatus(sling.ExecStatusError, err)
		}
		return err
	}
	stopHB()
	if !sling.IsPipelineRunMode() {
		e.syncBuildStatus(sling.ExecStatusSuccess, nil)
	}
	return nil
}

// runNode executes a single DAG node and returns its result.
func (e *Executor) runNode(nodeName string, nodeIndex, total int) ExecutionResult {
	unbind := e.bindModel(nodeName)
	defer unbind()

	start := time.Now()
	node := e.Build.DAG.Nodes[nodeName]

	var result ExecutionResult
	result.Name = nodeName
	result.StartTime = g.Ptr(start)
	if node == nil {
		result.Err = g.Error("node '%s' not found in DAG", nodeName)
		result.Duration = time.Since(start)
		e.printProgress(nodeIndex, total, result, false)
		e.syncModelStatus(nodeName, result, sling.ExecStatusError)
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
		e.printProgress(nodeIndex, total, result, false)
		e.syncModelStatus(nodeName, result, sling.ExecStatusSkipped)
		return result
	}

	stopHB := func() {}
	if node.Seed != nil {
		result.NodeType = "seed"
		result.Mode = "full-refresh"
		result.Name = node.Seed.FullTableName
		e.printProgress(nodeIndex, total, result, true)
		e.syncModelStatus(nodeName, result, sling.ExecStatusRunning)
		stopHB = e.startModelHeartbeat(nodeName, &result)
		rows, bytes, err := e.executeSeed(node.Seed)
		e.ctx.Lock()
		result.Rows, result.Bytes, result.Err = rows, bytes, err
		result.Duration = time.Since(start)
		snap := result
		e.ctx.Unlock()
		stopHB()
		e.printProgress(nodeIndex, total, snap, false)
		e.syncModelStatus(nodeName, snap, resultStatus(snap))
		return snap
	}

	if node.Model != nil {
		mode := e.Build.GetModelMode(node.Model)
		result.NodeType = "model"
		result.Mode = mode
		if testOnly {
			result.NodeType = "test"
			result.Mode = "test"
		}
		result.Name = node.Model.FullTableName
		e.printProgress(nodeIndex, total, result, true)
		e.syncModelStatus(nodeName, result, sling.ExecStatusRunning)
		stopHB = e.startModelHeartbeat(nodeName, &result)
		var rows uint64
		var err error
		if testOnly {
			err = e.executeModelTests(node.Model)
		} else {
			rows, err = e.executeModel(node.Model, mode)
		}
		e.ctx.Lock()
		result.Rows, result.Err = rows, err
		result.Duration = time.Since(start)
		snap := result
		e.ctx.Unlock()
		stopHB()
		e.printProgress(nodeIndex, total, snap, false)
		e.syncModelStatus(nodeName, snap, resultStatus(snap))
		return snap
	}
	return result
}

func resultStatus(result ExecutionResult) sling.ExecStatus {
	if result.Skipped {
		return sling.ExecStatusSkipped
	}
	if result.Err != nil {
		return sling.ExecStatusError
	}
	return sling.ExecStatusSuccess
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
func (e *Executor) executeSeed(seed *Seed) (rows, bytes uint64, err error) {
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
			Database: model.Database,
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
func (e *Executor) executeModel(model *Model, mode string) (rows uint64, err error) {
	// Parse and execute start hooks
	if err := e.parseModelHooks(model, mode); err != nil {
		return 0, g.Error(err, "could not parse hooks for model '%s'", model.Name)
	}

	if len(model.startHooks) > 0 {
		g.Debug("running start hooks for %s", model.Name)
		if err := model.startHooks.Execute(); err != nil {
			return 0, g.Error(err, "start hook failed for model '%s'", model.Name)
		}
	}

	// Pre-statements (from multi-statement SQL file)
	for i, stmt := range model.PreStatements {
		g.Debug("running pre-statement %d/%d for %s", i+1, len(model.PreStatements), model.Name)
		if _, err := e.DbConn.Exec(stmt); err != nil {
			return 0, g.Error(err, "pre-statement %d failed for model '%s'", i+1, model.Name)
		}
	}

	switch mode {
	case "full-refresh":
		rows, err = e.executeFullRefresh(model)
	case "view":
		rows, err = e.executeView(model)
	case "truncate":
		rows, err = e.executeTruncate(model)
	case "incremental":
		rows, err = e.executeIncremental(model)
	case "append", "snapshot": // snapshot is deprecated alias
		rows, err = e.executeAppend(model)
	default:
		err = g.Error("unknown mode '%s' for model '%s'", mode, model.Name)
	}

	if err != nil {
		return rows, err
	}

	// Post-statements (from multi-statement SQL file)
	for i, stmt := range model.PostStatements {
		g.Debug("running post-statement %d/%d for %s", i+1, len(model.PostStatements), model.Name)
		if _, err := e.DbConn.Exec(stmt); err != nil {
			return rows, g.Error(err, "post-statement %d failed for model '%s'", i+1, model.Name)
		}
	}

	// Declarative data tests
	if len(model.Config.Tests) > 0 {
		if err := e.executeModelTests(model); err != nil {
			return rows, err
		}
	}

	// End hooks
	if len(model.endHooks) > 0 {
		g.Debug("running end hooks for %s", model.Name)
		if err := model.endHooks.Execute(); err != nil {
			return rows, g.Error(err, "end hook failed for model '%s'", model.Name)
		}
	}

	return rows, nil
}

// executeFullRefresh rebuilds the table. Prefer atomic swap (tmp + rename) or
// CREATE OR REPLACE TABLE where supported so the target is never missing mid-build.
func (e *Executor) executeFullRefresh(model *Model) (uint64, error) {
	sql := model.CompiledSQL
	cascade := e.wantCascade(model)

	// Drop any existing view first (model may previously have been a view)
	_ = e.dropView(model.FullTableName, cascade)

	var rows uint64
	var err error

	// Path A: CREATE OR REPLACE TABLE (Snowflake, BigQuery, DuckDB, Databricks)
	if supportsCreateOrReplaceTable(e.DbConn.GetType()) && !e.isClickHouse() {
		rows, err = e.createOrReplaceTableAs(model.FullTableName, sql, model)
	} else if e.supportsRenameTable() {
		// Path B: atomic temp + rename swap when rename is available
		rows, err = e.executeFullRefreshAtomic(model, sql, cascade)
	} else {
		// Path C: fallback — drop then CTAS (brief downtime window)
		if err = e.dropTable(model.FullTableName, cascade); err != nil {
			return 0, err
		}
		rows, err = e.createTableAs(model.FullTableName, sql, model)
	}
	if err != nil {
		return rows, err
	}
	return e.countIfColumnStore(model.FullTableName, rows), nil
}

// executeFullRefreshAtomic creates into a temp table, then renames into place.
func (e *Executor) executeFullRefreshAtomic(model *Model, sql string, cascade bool) (uint64, error) {
	tmpFull := e.getTempTableName(model)
	// Ensure temp is clean
	_ = e.dropTable(tmpFull, false)

	rows, err := e.createTableAs(tmpFull, sql, model)
	if err != nil {
		_ = e.dropTable(tmpFull, false)
		return 0, g.Error(err, "could not create temp table for full-refresh of '%s'", model.Name)
	}

	// Drop target (or rename aside if we want even less downtime — drop is fine
	// once data is ready in tmp; window is only drop+rename, not CTAS duration)
	if err := e.dropTable(model.FullTableName, cascade); err != nil {
		_ = e.dropTable(tmpFull, false)
		return rows, err
	}

	if err := e.renameTable(tmpFull, model.FullTableName); err != nil {
		// Best-effort: leave tmp so data isn't lost
		return rows, g.Error(err, "could not rename temp table to '%s' (temp left at %s)", model.FullTableName, tmpFull)
	}
	return rows, nil
}

// executeView creates or replaces a view.
func (e *Executor) executeView(model *Model) (uint64, error) {
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
func (e *Executor) executeTruncate(model *Model) (uint64, error) {
	sql := model.CompiledSQL

	exists, err := e.tableExists(model)
	if err != nil {
		return 0, err
	}

	if !exists {
		rows, err := e.createTableAs(model.FullTableName, sql, model)
		if err != nil {
			return 0, err
		}
		return e.countIfColumnStore(model.FullTableName, rows), nil
	}

	if err := e.truncateTable(model.FullTableName); err != nil {
		return 0, err
	}
	rows, err := e.insertSelect(model.FullTableName, sql)
	if err != nil {
		return 0, err
	}
	return e.countIfColumnStore(model.FullTableName, rows), nil
}

// executeIncremental dispatches to the correct incremental strategy based on the
// model's detected style. dbt-style models use executeLegacyIncremental (the
// original is_incremental() path); sling-style models use resolveRange + executeRange.
func (e *Executor) executeIncremental(model *Model) (uint64, error) {
	uniqueKeys := getUniqueKeys(model)
	if len(uniqueKeys) == 0 {
		return 0, g.Error("model '%s' uses incremental mode but has no unique_key defined in config()", model.Name)
	}

	exists, err := e.tableExists(model)
	if err != nil {
		return 0, err
	}
	if !exists || e.Build.Options.FullRefresh {
		return e.executeFullRefresh(model)
	}

	switch model.Style {
	case StyleDbt:
		if e.Build.Options.Range != nil {
			return 0, g.Error("model '%s' uses is_incremental() (dbt style); --range requires incremental_where_cond() (sling style)", model.Name)
		}
		return e.executeLegacyIncremental(model)
	case StyleSling:
		r, err := e.resolveRange(model)
		if err != nil {
			return 0, err
		}
		return e.executeRange(model, r)
	default:
		return 0, g.Error("model '%s': unknown incremental style %d", model.Name, model.Style)
	}
}

// executeLegacyIncremental is the original dbt-style incremental path, preserved
// byte-for-byte. Recompiles with is_incremental()=true, stages into a temp table,
// and merges using the configured strategy.
func (e *Executor) executeLegacyIncremental(model *Model) (uint64, error) {
	t := model.FullTableName

	// Subsequent run: recompile with is_incremental()=true
	_, err := e.Build.Engine.CompileModel(model, &IncrementalContext{IsIncremental: true})
	if err != nil {
		return 0, g.Error(err, "could not compile incremental SQL for '%s'", model.Name)
	}

	// Re-apply table reference rewriting after incremental recompilation
	if model.Config.Rewrite == nil || *model.Config.Rewrite {
		rewritten, _, err := RewriteTableReferences(model.CompiledSQL, e.Build.Project, model.Name)
		if err != nil {
			return 0, err
		}
		model.CompiledSQL = rewritten
	}

	// Re-split after recompilation since CompiledSQL changed
	result, splitErr := MakeModelSQL(model.CompiledSQL, e.DbConn.GetType())
	if splitErr != nil {
		return 0, g.Error(splitErr, "could not parse incremental SQL for '%s'", model.Name)
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

	if _, err = e.createTempTable(tempTable, incrementalSQL); err != nil {
		return 0, g.Error(err, "could not create temp table for incremental merge on '%s'", model.Name)
	}

	// Generate and execute merge SQL using sling's merge infrastructure
	uniqueKeys := getUniqueKeys(model)
	// Quote target/temp for merge; GenerateMergeSQL expects usable identifiers
	tgtQuoted, err := e.quoteFullTableName(t)
	if err != nil {
		return 0, err
	}
	tmpQuoted, err := e.quoteFullTableName(tempTable)
	if err != nil {
		return 0, err
	}
	mergeSQL, err := e.DbConn.GenerateMergeSQLWithStrategy(tmpQuoted, tgtQuoted, uniqueKeys, &strategy)
	if err != nil {
		return 0, g.Error(err, "could not generate merge SQL for '%s'", model.Name)
	}

	rows, err := rowsFromExec(e.DbConn.ExecMulti(mergeSQL))
	if err != nil {
		return 0, g.Error(err, "could not execute incremental merge for '%s'", model.Name)
	}
	return e.countIfColumnStore(tempTable, rows), nil
}

// executeAppend handles append-only mode (formerly "snapshot").
// First run: CTAS. Subsequent: INSERT. Column stores stage into a temp table
// and COUNT(*) that table, because INSERT RowsAffected is often 0.
func (e *Executor) executeAppend(model *Model) (uint64, error) {
	sql := model.CompiledSQL

	exists, err := e.tableExists(model)
	if err != nil {
		return 0, err
	}

	if !exists {
		rows, err := e.createTableAs(model.FullTableName, sql, model)
		if err != nil {
			return 0, err
		}
		return e.countIfColumnStore(model.FullTableName, rows), nil
	}

	if e.DbConn != nil && e.DbConn.GetType().IsColumnStore() {
		tempTable := e.getTempTableName(model)
		defer func() {
			if dropErr := e.dropTable(tempTable, false); dropErr != nil {
				g.Debug("could not drop temp table %s: %s", tempTable, dropErr)
			}
		}()

		rows, err := e.createTempTable(tempTable, sql)
		if err != nil {
			return 0, g.Error(err, "could not create temp table for append of '%s'", model.Name)
		}
		rows = e.countIfColumnStore(tempTable, rows)

		tgtQuoted, err := e.quoteFullTableName(model.FullTableName)
		if err != nil {
			return rows, err
		}
		tmpQuoted, err := e.quoteFullTableName(tempTable)
		if err != nil {
			return rows, err
		}
		// No wrapping parens: ClickHouse rejects INSERT INTO t (SELECT * FROM tmp)
		if _, err = e.DbConn.Exec(g.F("INSERT INTO %s SELECT * FROM %s", tgtQuoted, tmpQuoted)); err != nil {
			return rows, g.Error(err, "could not append from temp table into '%s'", model.Name)
		}
		return rows, nil
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

// createTempTable stages SQL into a temp table. ClickHouse uses Memory engine
// (no ORDER BY). Other dialects use CREATE TABLE AS.
func (e *Executor) createTempTable(tempTable, sql string) (uint64, error) {
	if e.isClickHouse() {
		quoted, err := e.quoteFullTableName(tempTable)
		if err != nil {
			return 0, err
		}
		return rowsFromExec(e.DbConn.Exec(g.F("CREATE TABLE %s ENGINE = Memory AS (%s)", quoted, sql)))
	}
	return e.createTableAs(tempTable, sql, nil)
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
	return TableIdentity{Name: tempName, Schema: model.Schema, Database: model.Database}.FullName()
}

func (e *Executor) attachLogSink() {
	prev := env.LogSink
	e.prevLogSink = prev
	env.LogSink = func(ll *g.LogLine) {
		name := e.modelForGID()
		if name != "" {
			ll.Group = g.F("%s,%s", env.ExecID, name)
		}
		if prev != nil {
			prev(ll)
		}
		e.appendLog(name, ll)
	}
}

func (e *Executor) detachLogSink() {
	env.LogSink = e.prevLogSink
}

const maxModelLogBuf = 5000

func (e *Executor) bindModel(modelName string) func() {
	gid := goroutineID()
	e.gidModels.Store(gid, modelName)
	return func() {
		e.gidModels.Delete(gid)
	}
}

func (e *Executor) modelForGID() string {
	v, ok := e.gidModels.Load(goroutineID())
	if !ok {
		return ""
	}
	name, _ := v.(string)
	return name
}

func (e *Executor) appendLog(modelName string, ll *g.LogLine) {
	if ll == nil {
		return
	}
	e.modelLogsMu.Lock()
	defer e.modelLogsMu.Unlock()
	if e.modelLogs == nil {
		e.modelLogs = make(map[string]g.LogLines)
	}
	if len(e.modelLogs[modelName]) >= maxModelLogBuf {
		return
	}
	e.modelLogs[modelName] = append(e.modelLogs[modelName], *ll)
}

func (e *Executor) drainLogs(modelName string) g.LogLines {
	e.modelLogsMu.Lock()
	defer e.modelLogsMu.Unlock()
	lines := e.modelLogs[modelName]
	e.modelLogs[modelName] = nil
	return lines
}

// goroutineID returns the current goroutine's id from the stack prefix.
// Used to route process-global LogSink lines to the in-flight model.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// "goroutine 123 [running]..."
	id := uint64(0)
	for i := 10; i < n; i++ {
		c := buf[i]
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + uint64(c-'0')
	}
	return id
}

func (e *Executor) fileName() *string {
	if v := os.Getenv("SLING_FILE_NAME"); v != "" && IsConfigFileName(filepath.Base(v)) {
		return g.Ptr(v)
	}
	if e.Build != nil && e.Build.Project != nil {
		if p, ok := FindConfigFile(e.Build.Project.Dir); ok {
			if rel, err := filepath.Rel(e.Build.Project.Dir, p); err == nil {
				return g.Ptr(rel)
			}
			return g.Ptr(p)
		}
	}
	return nil
}

func (e *Executor) selectedOrder(name string) int {
	if e.Build == nil {
		return 0
	}
	for i, n := range e.Build.Selected {
		if n == name {
			return i + 1
		}
	}
	return 0
}

func (e *Executor) tryNumber() int {
	n := cast.ToInt(os.Getenv("SLING_TRY_NUMBER"))
	return lo.Ternary(n == 0, 1, n)
}

func (e *Executor) syncModelStatus(modelName string, result ExecutionResult, status sling.ExecStatus) {
	ms := e.makeModelStatus(modelName, result, status)
	_ = sling.StoreSet(ms)
}

func (e *Executor) makeModelStatus(modelName string, result ExecutionResult, status sling.ExecStatus) *store.BuildModelStatus {
	ms := &store.BuildModelStatus{
		ProjectID:  g.String(os.Getenv("SLING_PROJECT_ID")),
		JobID:      os.Getenv("SLING_JOB_ID"),
		ExecID:     os.Getenv("SLING_EXEC_ID"),
		FileName:   e.fileName(),
		Target:     e.ConnName,
		ModelName:  modelName,
		NodeType:   result.NodeType,
		ObjectName: result.Name,
		Mode:       result.Mode,
		Status:     status,
		Rows:       result.Rows,
		Bytes:      result.Bytes,
		Order:      e.selectedOrder(modelName),
		Tries:      e.tryNumber(),
		TryNumber:  e.tryNumber(),
		NewLines:   e.drainLogs(modelName),
		TimeNs:     time.Now().UnixNano(),
		AgentID:    g.Getenv("SLING_RUNNER_ID", os.Getenv("SLING_AGENT_ID")),
	}
	ms.Hostname, _ = os.Hostname()
	if result.Err != nil {
		ms.Error = g.Ptr(cast.ToString(result.Err))
	}
	if result.StartTime != nil {
		ms.StartTimeNs = g.Int64(result.StartTime.UnixNano())
	}
	if status != sling.ExecStatusRunning {
		ms.EndTimeNs = g.Int64(time.Now().UnixNano())
	}
	return ms
}

func (e *Executor) startModelHeartbeat(modelName string, result *ExecutionResult) func() {
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	var wg sync.WaitGroup
	var once sync.Once
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.ctx.Lock()
				snap := *result
				e.ctx.Unlock()
				e.syncModelStatus(modelName, snap, sling.ExecStatusRunning)
			case <-done:
				return
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
			wg.Wait()
		})
	}
}

func (e *Executor) startBuildHeartbeat() func() {
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	var wg sync.WaitGroup
	var once sync.Once
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.syncBuildStatus(sling.ExecStatusRunning, nil)
			case <-done:
				return
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
			wg.Wait()
		})
	}
}

func (e *Executor) syncBuildStatus(status sling.ExecStatus, runErr error) {
	_ = sling.StoreSet(e.makeBuildStatus(status, runErr))
}

func (e *Executor) makeBuildStatus(status sling.ExecStatus, runErr error) *store.BuildStatus {
	e.ctx.Lock()
	results := append([]ExecutionResult(nil), e.Results...)
	e.ctx.Unlock()

	bs := &store.BuildStatus{
		ProjectID:  g.String(os.Getenv("SLING_PROJECT_ID")),
		JobID:      os.Getenv("SLING_JOB_ID"),
		ExecID:     os.Getenv("SLING_EXEC_ID"),
		FileName:   e.fileName(),
		Target:     e.ConnName,
		Status:     status,
		ModelCount: 0,
		Tries:      e.tryNumber(),
		TryNumber:  e.tryNumber(),
		NewLines:   e.drainLogs(""),
		TimeNs:     time.Now().UnixNano(),
		AgentID:    g.Getenv("SLING_RUNNER_ID", os.Getenv("SLING_AGENT_ID")),
	}
	if e.Build != nil {
		bs.Select = e.Build.Options.Select
		bs.Exclude = e.Build.Options.Exclude
		bs.FullRefresh = e.Build.Options.FullRefresh
		bs.ModelCount = len(e.Build.Selected)
	}
	bs.Hostname, _ = os.Hostname()
	if e.startTime != nil {
		bs.StartTimeNs = g.Int64(e.startTime.UnixNano())
	}
	if status != sling.ExecStatusRunning {
		bs.EndTimeNs = g.Int64(time.Now().UnixNano())
		if status == sling.ExecStatusError {
			bs.ExitCode = 1
		}
	}
	if runErr != nil {
		bs.Error = g.Ptr(cast.ToString(runErr))
	}
	for _, r := range results {
		bs.Rows += r.Rows
		bs.Bytes += r.Bytes
		switch {
		case r.Skipped:
			bs.SkippedCount++
		case r.Err != nil:
			bs.FailedCount++
		default:
			bs.OkCount++
		}
	}
	return bs
}

// printProgress prints a single line of execution progress.
// When started is true, prints the "START" line (no duration).
// When started is false, prints the final status with duration.
func (e *Executor) printProgress(index, total int, result ExecutionResult, started bool) {
	if e.Build != nil && e.Build.Options.JSON {
		return
	}
	nodeType := result.Mode
	if result.NodeType == "seed" {
		nodeType = "seed"
	}

	// Format: [1/8] staging.country_codes (seed) ........... START
	// Format: [1/8] staging.country_codes (seed) ........... OK (0.2s)
	prefix := progressPrefix(index, total, result.Name, nodeType)

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

// progressPrefix is the left side of a progress line, including the dim
// "(mode)" so both parentheses share the same color as the mode name.
func progressPrefix(index, total int, name, nodeType string) string {
	return g.F("[%d/%d] %s %s ", index, total, name, env.DarkGrayString("("+nodeType+")"))
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

var (
	resolveAdvanceRange = func(e *Executor, model *Model) (*Range, error) {
		g.Warn("use the official release of sling-cli to use sling state")
		return nil, nil
	}

	advanceStateAfterRange = func(e *Executor, model *Model, r *Range, updateKey string) error {
		g.Warn("use the official release of sling-cli to use sling state")
		return nil
	}

	readState = func(string, string) (string, iop.ColumnType, error) {
		g.Warn("use the official release of sling-cli to use sling state")
		return "", "", nil
	}
)

// resolveIncrementalRange resolves the watermark via tier A/B/C and applies
// optional lookback. This is used when there is no step (plain incremental).
//
//   - Tier A: SLING_STATE is configured → read from state store
//   - Tier B: target table has rows → SELECT MAX(update_key)
//   - Tier C: first run → unbounded lower (full-refresh semantics)
func resolveIncrementalRange(e *Executor, model *Model) (*Range, error) {
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
		var err error
		lowerRaw, colType, err = readState(model.Name, model.FullTableName)
		if err != nil {
			return nil, g.Error(err, "could not read SLING_STATE for model '%s'", model.Name)
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

// resolveRange decides which range-resolution strategy to use and returns the
// ordered set of chunks to execute. Returns a Range with 0 chunks as a no-op.
func (e *Executor) resolveRange(model *Model) (*Range, error) {
	// --range CLI flag wins over all automatic resolution
	if e.Build.Options.Range != nil {
		raw := *e.Build.Options.Range
		dbType := e.DbConn.GetType()
		return splitCLIRange(raw, dbType, "")
	}

	rc := model.Config.Range
	if rc != nil && rc.HasAdvance() {
		if !sling.IsStateConfigured() {
			return nil, g.Error("model '%s': range.advance requires SLING_STATE to be configured", model.Name)
		}
		return resolveAdvanceRange(e, model)
	}

	return resolveIncrementalRange(e, model)
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

	rewritten, _, rwErr := RewriteTableReferences(model.CompiledSQL, e.Build.Project, model.Name)
	if rwErr != nil {
		model.CompiledSQL = savedSQL
		return "", "", rwErr
	}
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
func (e *Executor) executeRange(model *Model, r *Range) (uint64, error) {
	if len(r.Chunks) == 0 {
		g.Debug("build[%s]: range resolved to 0 chunks; skipping", model.Name)
		return 0, nil
	}

	updateKey := model.Config.UpdateKey
	if updateKey == "" {
		return 0, g.Error("model '%s': sling-style incremental requires update_key in config()", model.Name)
	}

	var rows uint64
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
		chunkRows, chunkErr := e.runMergeForChunk(model, incCtx)
		chunkDur := time.Since(chunkStart)
		rows += chunkRows

		if multi {
			g.Debug("%s", formatChunkProgressLine(i+1, len(r.Chunks), chunk, updateKey, chunkDur, chunkErr != nil))
		} else {
			g.Debug("build[%s]: chunk %d/%d %s (%s)",
				model.Name, i+1, len(r.Chunks), chunk.Describe(updateKey), formatDuration(chunkDur))
		}

		if chunkErr != nil {
			return rows, e.handleChunkError(model, r, i, chunkErr)
		}
	}

	// Advance state after successful run
	if r.UpdateState && sling.IsStateConfigured() {
		if err := advanceStateAfterRange(e, model, r, updateKey); err != nil {
			g.Warn("build[%s]: could not advance SLING_STATE: %s", model.Name, err)
		}
	}

	return rows, nil
}

// runMergeForChunk compiles the model with incCtx, rewrites refs, and executes
// the temp-table + merge strategy. This is factored from executeLegacyIncremental.
func (e *Executor) runMergeForChunk(model *Model, incCtx *IncrementalContext) (uint64, error) {
	t := model.FullTableName
	uniqueKeys := getUniqueKeys(model)

	_, err := e.Build.Engine.CompileModel(model, incCtx)
	if err != nil {
		return 0, g.Error(err, "could not compile incremental SQL for '%s'", model.Name)
	}

	// Honor rewrite: false
	if model.Config.Rewrite == nil || *model.Config.Rewrite {
		rewritten, _, err := RewriteTableReferences(model.CompiledSQL, e.Build.Project, model.Name)
		if err != nil {
			return 0, err
		}
		model.CompiledSQL = rewritten
	}

	result, splitErr := MakeModelSQL(model.CompiledSQL, e.DbConn.GetType())
	if splitErr != nil {
		return 0, g.Error(splitErr, "could not parse incremental SQL for '%s'", model.Name)
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

	if _, err = e.createTempTable(tempTable, incrementalSQL); err != nil {
		return 0, g.Error(err, "could not create temp table for incremental merge on '%s'", model.Name)
	}

	tgtQuoted, err := e.quoteFullTableName(t)
	if err != nil {
		return 0, err
	}
	tmpQuoted, err := e.quoteFullTableName(tempTable)
	if err != nil {
		return 0, err
	}
	mergeSQL, err := e.DbConn.GenerateMergeSQLWithStrategy(tmpQuoted, tgtQuoted, uniqueKeys, &strategy)
	if err != nil {
		return 0, g.Error(err, "could not generate merge SQL for '%s'", model.Name)
	}

	rows, err := rowsFromExec(e.DbConn.ExecMulti(mergeSQL))
	if err != nil {
		return 0, g.Error(err, "could not execute incremental merge for '%s'", model.Name)
	}
	return e.countIfColumnStore(tempTable, rows), nil
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
