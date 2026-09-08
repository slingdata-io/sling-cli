package build

import (
	"strings"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/sling"
)

func init() {
	// Register so type: build steps work without an import cycle
	// (build imports sling; sling never imports build).
	sling.HookRunBuild = RunForHook
}

// RunForHook compiles and executes a build project for a pipeline/replication hook.
// Returns a state map with per-node results for state.<step_id>.results.
func RunForHook(path string, opts sling.HookBuildRunOptions) (map[string]any, error) {
	buildOpts := BuildOptions{
		Target:      opts.Target,
		Select:      opts.Select,
		Exclude:     opts.Exclude,
		Vars:        opts.Vars,
		FailFast:    opts.FailFast,
		FullRefresh: opts.FullRefresh,
		Threads:     opts.Threads,
		Schema:      opts.Schema,
		Prod:        opts.Prod,
		NoSeeds:     opts.NoSeeds,
		Recursive:   opts.Recursive,
		Test:        opts.Test,
		Compile:     opts.Compile,
		List:        opts.List,
	}
	if buildOpts.Threads < 1 {
		buildOpts.Threads = DefaultThreads
	}
	if opts.Range != "" {
		buildOpts.Range = g.String(opts.Range)
	}

	b, err := NewBuild(path, buildOpts)
	if err != nil {
		return nil, g.Error(err, "could not load build project from %s", path)
	}

	if err := b.Compile(); err != nil {
		return nil, g.Error(err, "could not compile build project")
	}

	if opts.List {
		return listToState(path, b), nil
	}
	if opts.Compile {
		return compileToState(path, b), nil
	}

	// Multi-target / recursive sub-projects use Build.Execute (no per-node results).
	if len(b.Project.SubProjects) > 0 {
		if err := b.Execute(); err != nil {
			return g.M(
				"path", path,
				"target", b.GetTarget(),
				"sub_projects", len(b.Project.SubProjects),
			), g.Error(err, "build failed")
		}
		return g.M(
			"path", path,
			"target", b.GetTarget(),
			"sub_projects", len(b.Project.SubProjects),
			"results", []map[string]any{},
			"total", 0,
			"ok", 0,
			"failed", 0,
			"skipped", 0,
		), nil
	}

	executor, err := NewExecutor(b)
	if err != nil {
		return nil, err
	}

	runErr := executor.Execute()
	data := resultsToState(path, b.GetTarget(), executor.Results)
	if runErr != nil {
		return data, g.Error(runErr, "build failed")
	}
	return data, nil
}

func resultsToState(path, target string, results []ExecutionResult) map[string]any {
	rows := make([]map[string]any, 0, len(results))
	ok, failed, skipped := 0, 0, 0
	var totalRows, totalBytes uint64
	for _, r := range results {
		status := "success"
		errMsg := ""
		switch {
		case r.Skipped:
			status = "skipped"
			skipped++
		case r.Err != nil:
			status = "error"
			errMsg = r.Err.Error()
			failed++
		default:
			ok++
		}
		totalRows += r.Rows
		totalBytes += r.Bytes
		rows = append(rows, g.M(
			"name", r.Name,
			"type", r.NodeType,
			"mode", r.Mode,
			"duration", r.Duration.Seconds(),
			"status", status,
			"error", errMsg,
			"rows", r.Rows,
			"bytes", r.Bytes,
		))
	}

	// Stable order for downstream checks (execution order can be parallel)
	// — keep as-is; callers can count by status.

	return g.M(
		"path", path,
		"target", target,
		"results", rows,
		"total", len(rows),
		"ok", ok,
		"failed", failed,
		"skipped", skipped,
		"rows", totalRows,
		"bytes", totalBytes,
		// Convenience: comma-joined names of successful models/seeds
		"ok_names", joinResultNames(results, "success"),
	)
}

func joinResultNames(results []ExecutionResult, wantStatus string) string {
	var names []string
	for _, r := range results {
		status := "success"
		if r.Skipped {
			status = "skipped"
		} else if r.Err != nil {
			status = "error"
		}
		if status == wantStatus {
			names = append(names, r.Name)
		}
	}
	return strings.Join(names, ",")
}

func compileToState(path string, b *Build) map[string]any {
	if len(b.SubBuilds) > 0 {
		var all []map[string]any
		total := 0
		for _, sub := range b.SubBuilds {
			p := sub.CompileJSONPayload()
			all = append(all, p)
			if order, ok := p["order"].([]string); ok {
				total += len(order)
			}
		}
		return g.M(
			"path", path,
			"command", "compile",
			"sub_projects", all,
			"results", []map[string]any{},
			"total", total,
			"ok", total,
			"failed", 0,
			"skipped", 0,
		)
	}
	payload := b.CompileJSONPayload()
	order, _ := payload["order"].([]string)
	nodes, _ := payload["nodes"].([]map[string]any)
	return g.M(
		"path", path,
		"target", b.GetTarget(),
		"command", "compile",
		"order", payload["order"],
		"nodes", payload["nodes"],
		"results", nodes,
		"total", len(order),
		"ok", len(order),
		"failed", 0,
		"skipped", 0,
	)
}

func listToState(path string, b *Build) map[string]any {
	showTable := b.GetTarget() != ""
	var rows []map[string]any
	if b.DAG != nil {
		for _, name := range b.Selected {
			node := b.DAG.Nodes[name]
			if node == nil {
				continue
			}
			if b.Options.NoSeeds && node.Seed != nil {
				continue
			}
			item := g.M("name", name)
			if node.Seed != nil {
				item["type"] = "seed"
				item["file"] = node.Seed.RelPath
				if showTable {
					item["table"] = node.Seed.FullTableName
				}
			} else if node.Model != nil {
				item["type"] = "model"
				item["mode"] = b.GetModelMode(node.Model)
				item["file"] = node.Model.RelPath
				if showTable {
					item["table"] = node.Model.FullTableName
				}
			}
			rows = append(rows, item)
		}
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	return g.M(
		"path", path,
		"target", b.GetTarget(),
		"command", "list",
		"results", rows,
		"total", len(rows),
		"ok", len(rows),
		"failed", 0,
		"skipped", 0,
	)
}
