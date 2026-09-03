package build

import (
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/flarco/g"
	"github.com/gobwas/glob"
	"github.com/samber/lo"
)

// =============================================================================
// DAG
// =============================================================================

// DAG represents a directed acyclic graph of models and seeds.
type DAG struct {
	Nodes map[string]*DAGNode
	Order []string // topological sort result
}

// DAGNode represents a node in the dependency graph.
type DAGNode struct {
	Name         string
	Model        *Model // nil for seeds
	Seed         *Seed  // nil for models
	Dependencies []string
	Dependents   []string
	Depth        int
}

// BuildDAG constructs a dependency graph from a project's models and seeds.
// Models must have been compiled first (DependsOn populated).
func BuildDAG(project *BuildProject) (*DAG, error) {
	dag := &DAG{
		Nodes: make(map[string]*DAGNode),
	}

	// Add seed nodes (depth 0, no dependencies)
	for name, seed := range project.Seeds {
		dag.Nodes[name] = &DAGNode{
			Name: name,
			Seed: seed,
		}
	}

	// isDisabled returns true for models that are excluded from DAG execution.
	isDisabled := func(name string) bool {
		m, ok := project.Models[name]
		if !ok {
			return false
		}
		return m.Config.Enabled != nil && !*m.Config.Enabled
	}

	// Add model nodes with dependencies
	for name, model := range project.Models {
		// Skip disabled models — they stay in project.Models (for compile/list/
		// template) but are excluded from DAG-based execution. Dependents of a
		// disabled model silently drop the edge (matching the existing unresolved
		// -ref behavior) and will fail later with a missing-table error if they
		// actually query it.
		if isDisabled(name) {
			continue
		}

		// Filter dependencies to only include known models/seeds.
		// Disabled models are silently dropped so the DAG stays consistent.
		deps := make([]string, 0)
		for _, dep := range model.DependsOn {
			if _, ok := project.Models[dep]; ok {
				if isDisabled(dep) {
					continue
				}
				deps = append(deps, dep)
			} else if _, ok := project.Seeds[dep]; ok {
				deps = append(deps, dep)
			}
			// Unknown deps (from src() or external tables) are silently skipped
		}

		dag.Nodes[name] = &DAGNode{
			Name:         name,
			Model:        model,
			Dependencies: deps,
		}
	}

	// Build reverse edges (dependents)
	for name, node := range dag.Nodes {
		for _, dep := range node.Dependencies {
			if depNode, ok := dag.Nodes[dep]; ok {
				depNode.Dependents = append(depNode.Dependents, name)
			}
		}
	}

	// Topological sort
	order, err := dag.TopologicalSort()
	if err != nil {
		return nil, err
	}
	dag.Order = order

	// Compute depths
	dag.computeDepths()

	return dag, nil
}

// TopologicalSort performs Kahn's algorithm to produce a topological ordering.
// Returns an error if a cycle is detected.
func (dag *DAG) TopologicalSort() ([]string, error) {
	// Count incoming edges for each node
	inDegree := make(map[string]int)
	for name := range dag.Nodes {
		inDegree[name] = len(dag.Nodes[name].Dependencies)
	}

	// Find all nodes with in-degree 0
	queue := make([]string, 0)
	for name, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, name)
		}
	}
	// Sort for deterministic output
	sort.Strings(queue)

	var order []string
	for len(queue) > 0 {
		// Pop from queue
		name := queue[0]
		queue = queue[1:]
		order = append(order, name)

		// Reduce in-degree of dependents
		node := dag.Nodes[name]
		nextBatch := make([]string, 0)
		for _, dependent := range node.Dependents {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				nextBatch = append(nextBatch, dependent)
			}
		}
		// Sort for deterministic ordering within same level
		sort.Strings(nextBatch)
		queue = append(queue, nextBatch...)
	}

	if len(order) != len(dag.Nodes) {
		// Cycle detected
		cycles := dag.DetectCycles()
		cycleStrs := make([]string, 0, len(cycles))
		for _, cycle := range cycles {
			cycleStrs = append(cycleStrs, strings.Join(cycle, " -> "))
		}
		return nil, g.Error("cycle detected in model dependencies: %s", strings.Join(cycleStrs, "; "))
	}

	return order, nil
}

// DetectCycles finds cycles in the graph using DFS.
func (dag *DAG) DetectCycles() [][]string {
	var cycles [][]string
	visited := make(map[string]int) // 0=unvisited, 1=in-progress, 2=done
	path := make([]string, 0)

	var dfs func(name string)
	dfs = func(name string) {
		if visited[name] == 2 {
			return
		}
		if visited[name] == 1 {
			// Found a cycle — extract it from path
			cycleStart := -1
			for i, p := range path {
				if p == name {
					cycleStart = i
					break
				}
			}
			if cycleStart >= 0 {
				cycle := make([]string, 0)
				cycle = append(cycle, path[cycleStart:]...)
				cycle = append(cycle, name) // close the cycle
				cycles = append(cycles, cycle)
			}
			return
		}

		visited[name] = 1
		path = append(path, name)

		node := dag.Nodes[name]
		if node != nil {
			for _, dep := range node.Dependencies {
				dfs(dep)
			}
		}

		path = path[:len(path)-1]
		visited[name] = 2
	}

	// Sort names for deterministic output
	names := make([]string, 0, len(dag.Nodes))
	for name := range dag.Nodes {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		dfs(name)
	}

	return cycles
}

// GetUpstream returns all transitive upstream dependencies of the given node.
func (dag *DAG) GetUpstream(name string) []string {
	visited := make(map[string]bool)
	var result []string

	var walk func(n string)
	walk = func(n string) {
		node, ok := dag.Nodes[n]
		if !ok {
			return
		}
		for _, dep := range node.Dependencies {
			if !visited[dep] {
				visited[dep] = true
				result = append(result, dep)
				walk(dep)
			}
		}
	}

	walk(name)

	// Return in topological order
	orderMap := make(map[string]int)
	for i, n := range dag.Order {
		orderMap[n] = i
	}
	sort.Slice(result, func(i, j int) bool {
		return orderMap[result[i]] < orderMap[result[j]]
	})

	return result
}

// GetUpstreamN returns upstream dependencies up to N degrees from the given node.
func (dag *DAG) GetUpstreamN(name string, n int) []string {
	visited := make(map[string]bool)
	var result []string

	current := []string{name}
	for depth := 0; depth < n && len(current) > 0; depth++ {
		var next []string
		for _, nodeName := range current {
			node, ok := dag.Nodes[nodeName]
			if !ok {
				continue
			}
			for _, dep := range node.Dependencies {
				if !visited[dep] {
					visited[dep] = true
					result = append(result, dep)
					next = append(next, dep)
				}
			}
		}
		current = next
	}

	// Return in topological order
	orderMap := make(map[string]int)
	for i, n := range dag.Order {
		orderMap[n] = i
	}
	sort.Slice(result, func(i, j int) bool {
		return orderMap[result[i]] < orderMap[result[j]]
	})

	return result
}

// GetDownstreamN returns downstream dependents up to N degrees from the given node.
func (dag *DAG) GetDownstreamN(name string, n int) []string {
	visited := make(map[string]bool)
	var result []string

	current := []string{name}
	for depth := 0; depth < n && len(current) > 0; depth++ {
		var next []string
		for _, nodeName := range current {
			node, ok := dag.Nodes[nodeName]
			if !ok {
				continue
			}
			for _, dep := range node.Dependents {
				if !visited[dep] {
					visited[dep] = true
					result = append(result, dep)
					next = append(next, dep)
				}
			}
		}
		current = next
	}
	return result
}

// GetDownstream returns all transitive downstream dependents of the given node.
func (dag *DAG) GetDownstream(name string) []string {
	visited := make(map[string]bool)
	var result []string

	var walk func(n string)
	walk = func(n string) {
		node, ok := dag.Nodes[n]
		if !ok {
			return
		}
		for _, dep := range node.Dependents {
			if !visited[dep] {
				visited[dep] = true
				result = append(result, dep)
				walk(dep)
			}
		}
	}

	walk(name)
	return result
}

// GetExecutionLevels groups nodes by depth for parallel execution.
// Each level contains nodes that can be executed concurrently.
func (dag *DAG) GetExecutionLevels() [][]string {
	if len(dag.Order) == 0 {
		return nil
	}

	maxDepth := 0
	for _, node := range dag.Nodes {
		if node.Depth > maxDepth {
			maxDepth = node.Depth
		}
	}

	levels := make([][]string, maxDepth+1)
	for _, name := range dag.Order {
		node := dag.Nodes[name]
		levels[node.Depth] = append(levels[node.Depth], name)
	}

	return levels
}

// computeDepths calculates the depth of each node based on dependencies.
func (dag *DAG) computeDepths() {
	for _, name := range dag.Order {
		node := dag.Nodes[name]
		maxDepth := -1
		for _, dep := range node.Dependencies {
			if depNode, ok := dag.Nodes[dep]; ok {
				if depNode.Depth > maxDepth {
					maxDepth = depNode.Depth
				}
			}
		}
		node.Depth = maxDepth + 1
	}
}

// =============================================================================
// Selector
// =============================================================================

// Selector filters DAG nodes based on include/exclude patterns.
type Selector struct {
	Includes []string
	Excludes []string
	Project  *BuildProject
}

// NewSelector creates a new selector from include and exclude patterns.
func NewSelector(includes, excludes []string) *Selector {
	return &Selector{
		Includes: includes,
		Excludes: excludes,
	}
}

// Apply filters DAG nodes based on the selector patterns, returning names in DAG order.
func (s *Selector) Apply(dag *DAG) ([]string, error) {
	selected := make(map[string]bool)

	if len(s.Includes) == 0 {
		// No includes = select all
		for _, name := range dag.Order {
			selected[name] = true
		}
	} else {
		// Apply each include pattern
		for _, pattern := range s.Includes {
			matched, err := matchPattern(pattern, dag, s.Project)
			if err != nil {
				return nil, err
			}
			for _, name := range matched {
				selected[name] = true
			}
		}
	}

	// Apply excludes
	for _, pattern := range s.Excludes {
		matched, err := matchPattern(pattern, dag, s.Project)
		if err != nil {
			return nil, err
		}
		for _, name := range matched {
			delete(selected, name)
		}
	}

	// Return in DAG order
	result := make([]string, 0, len(selected))
	for _, name := range dag.Order {
		if selected[name] {
			result = append(result, name)
		}
	}

	return result, nil
}

// matchPattern matches a single pattern against DAG nodes.
// Supported forms:
//   - model_name          exact/glob match
//   - +model              all upstream + model (inclusive)
//   - model+              model + all downstream (inclusive)
//   - +model+             all upstream + model + all downstream
//   - N+model             N degrees upstream + model
//   - model+N             model + N degrees downstream
//   - modelA-modelB       slice: all nodes between A and B (inclusive)
//   - tag:xxx             match by tag
//   - path/pattern        match by file path
func matchPattern(pattern string, dag *DAG, project *BuildProject) ([]string, error) {
	// Tag selector: tag:xxx (check early, before + parsing)
	if strings.HasPrefix(pattern, "tag:") {
		tag := strings.TrimPrefix(pattern, "tag:")
		return matchByTag(tag, dag), nil
	}

	// Graph traversal selectors (contains "+")
	if strings.Contains(pattern, "+") {
		return matchGraphSelector(pattern, dag, project)
	}

	// Slice selector: modelA-modelB (all nodes between A and B inclusive)
	// Only match if both sides resolve to known nodes (avoid matching glob patterns or paths with hyphens)
	if idx := strings.Index(pattern, "-"); idx > 0 && idx < len(pattern)-1 && !strings.Contains(pattern, "/") {
		left := pattern[:idx]
		right := pattern[idx+1:]
		leftName, rightName := left, right
		if project != nil {
			if lr, err := project.ResolveName(left); err == nil {
				leftName = lr.Name
			}
			if rr, err := project.ResolveName(right); err == nil {
				rightName = rr.Name
			}
		}
		_, leftOk := dag.Nodes[leftName]
		_, rightOk := dag.Nodes[rightName]
		if leftOk && rightOk {
			return matchSlice(leftName, rightName, dag)
		}
	}

	// Path selector: contains "/" -> match against relative file paths
	if strings.Contains(pattern, "/") {
		return matchByPath(pattern, dag)
	}

	if isGlob(pattern) {
		return matchByGlob(pattern, dag)
	}
	return resolveNodes(pattern, dag, project, pattern)
}

// matchGraphSelector handles all "+" based selectors:
//
//	+model, model+, +model+, N+model, model+N
//
// The model portion can be an exact name or a glob pattern (e.g. stg_*, *_orders).
func matchGraphSelector(pattern string, dag *DAG, project *BuildProject) ([]string, error) {
	hasPrefix := strings.HasPrefix(pattern, "+")
	hasSuffix := strings.HasSuffix(pattern, "+")

	// Pure prefix/suffix forms: +model, model+, +model+
	if hasPrefix || hasSuffix {
		modelPattern := strings.Trim(pattern, "+")

		nodes, err := resolveNodes(modelPattern, dag, project, pattern)
		if err != nil {
			return nil, err
		}

		selected := make(map[string]bool)
		for _, modelName := range nodes {
			selected[modelName] = true
			if hasPrefix {
				for _, name := range dag.GetUpstream(modelName) {
					selected[name] = true
				}
			}
			if hasSuffix {
				for _, name := range dag.GetDownstream(modelName) {
					selected[name] = true
				}
			}
		}

		return dagOrder(selected, dag), nil
	}

	// Interior "+" — could be: N+model, model+N, or modelA+modelB
	parts := strings.SplitN(pattern, "+", 2)
	left, right := parts[0], parts[1]

	// N+model (degree upstream)
	if n, err := strconv.Atoi(left); err == nil && n >= 0 {
		nodes, err := resolveNodes(right, dag, project, pattern)
		if err != nil {
			return nil, err
		}
		selected := make(map[string]bool)
		for _, modelName := range nodes {
			selected[modelName] = true
			for _, name := range dag.GetUpstreamN(modelName, n) {
				selected[name] = true
			}
		}
		return dagOrder(selected, dag), nil
	}

	// model+N (degree downstream)
	if n, err := strconv.Atoi(right); err == nil && n >= 0 {
		nodes, err := resolveNodes(left, dag, project, pattern)
		if err != nil {
			return nil, err
		}
		selected := make(map[string]bool)
		for _, modelName := range nodes {
			selected[modelName] = true
			for _, name := range dag.GetDownstreamN(modelName, n) {
				selected[name] = true
			}
		}
		return dagOrder(selected, dag), nil
	}

	// Unrecognized + pattern — left and right are not numbers, fall through to error
	return nil, g.Error("selector '%s': invalid selector pattern", pattern)
}

// matchSlice returns all nodes between modelA and modelB (inclusive).
// This is the intersection of downstream(A) and upstream(B), plus A and B.
func matchSlice(left, right string, dag *DAG) ([]string, error) {
	// Slice = downstream of A ∩ upstream of B, plus A and B themselves
	downA := dag.GetDownstream(left)
	upB := dag.GetUpstream(right)
	downSet := make(map[string]bool)
	for _, name := range downA {
		downSet[name] = true
	}
	downSet[left] = true

	selected := make(map[string]bool)
	selected[left] = true
	selected[right] = true
	for _, name := range upB {
		if downSet[name] {
			selected[name] = true
		}
	}

	return dagOrder(selected, dag), nil
}

// dagOrder returns the selected node names in DAG topological order.
func dagOrder(selected map[string]bool, dag *DAG) []string {
	var result []string
	for _, name := range dag.Order {
		if selected[name] {
			result = append(result, name)
		}
	}
	return result
}

// isGlob returns true if the string contains glob metacharacters.
func isGlob(s string) bool {
	return strings.ContainsAny(s, "*?[{")
}

// resolveNodes resolves a name-or-glob to matching DAG node names.
// For exact names, returns the single name or errors if not found.
// For glob patterns, returns all matches (empty slice if none match).
func resolveNodes(nameOrGlob string, dag *DAG, project *BuildProject, selectorForError string) ([]string, error) {
	if !isGlob(nameOrGlob) {
		if project != nil {
			ref, err := project.ResolveName(nameOrGlob)
			if err != nil {
				return nil, selectorResolveError(err)
			}
			if _, ok := dag.Nodes[ref.Name]; !ok {
				return nil, &NameNotFoundError{Query: nameOrGlob, Kind: "selector"}
			}
			return []string{ref.Name}, nil
		}
		if _, ok := dag.Nodes[nameOrGlob]; !ok {
			return nil, g.Error("selector '%s': model '%s' not found", selectorForError, nameOrGlob)
		}
		return []string{nameOrGlob}, nil
	}
	return matchByGlob(nameOrGlob, dag)
}

// matchByGlob matches node names using a glob pattern, also trying ProdFullTableName.
func matchByGlob(pattern string, dag *DAG) ([]string, error) {
	compiled, err := glob.Compile(pattern)
	if err != nil {
		return nil, err
	}

	var result []string
	seen := map[string]bool{}
	for _, name := range dag.Order {
		if compiled.Match(name) {
			result = append(result, name)
			seen[name] = true
			continue
		}
		node := dag.Nodes[name]
		var prod string
		if node.Model != nil {
			prod = node.Model.ProdFullTableName
		} else if node.Seed != nil {
			prod = node.Seed.ProdFullTableName
		}
		if prod == "" || seen[name] {
			continue
		}
		if compiled.Match(prod) || compiled.Match(stripDatabase(prod)) {
			result = append(result, name)
			seen[name] = true
		}
	}
	return result, nil
}

// matchByTag matches nodes whose model Config.Tags contain the given tag.
func matchByTag(tag string, dag *DAG) []string {
	var result []string
	for _, name := range dag.Order {
		node := dag.Nodes[name]
		if node.Model != nil && lo.Contains(node.Model.Config.Tags, tag) {
			result = append(result, name)
		}
	}
	return result
}

// matchByPath matches nodes whose relative file path matches a glob pattern.
func matchByPath(pattern string, dag *DAG) ([]string, error) {
	g, err := glob.Compile(pattern)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, name := range dag.Order {
		node := dag.Nodes[name]
		var relPath string
		if node.Model != nil {
			relPath = filepath.ToSlash(node.Model.RelPath)
		} else if node.Seed != nil {
			relPath = filepath.ToSlash(node.Seed.RelPath)
		}
		if relPath != "" && g.Match(relPath) {
			result = append(result, name)
		}
	}
	return result, nil
}
