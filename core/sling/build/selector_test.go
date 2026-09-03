package build

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDAGProject() *BuildProject {
	return &BuildProject{
		Models: map[string]*Model{
			"stg_orders": {
				Name:          "stg_orders",
				FullTableName: "staging.stg_orders",
				RelPath:       "staging/stg_orders.sql",
				Config:        ModelConfig{Tags: []string{"daily", "staging"}},
			},
			"stg_customers": {
				Name:          "stg_customers",
				FullTableName: "staging.stg_customers",
				RelPath:       "staging/stg_customers.sql",
				Config:        ModelConfig{Tags: []string{"daily", "staging"}},
			},
			"dim_customers": {
				Name:          "dim_customers",
				FullTableName: "marts.dim_customers",
				RelPath:       "marts/core/dim_customers.sql",
				DependsOn:     []string{"stg_customers"},
				Config:        ModelConfig{Tags: []string{"weekly"}},
			},
			"fct_orders": {
				Name:          "fct_orders",
				FullTableName: "marts.fct_orders",
				RelPath:       "marts/core/fct_orders.sql",
				DependsOn:     []string{"stg_orders"},
				Config:        ModelConfig{Tags: []string{"daily"}},
			},
			"revenue": {
				Name:          "revenue",
				FullTableName: "marts.revenue",
				RelPath:       "marts/finance/revenue.sql",
				DependsOn:     []string{"fct_orders"},
			},
		},
		Seeds: map[string]*Seed{
			"country_codes": {
				Name:          "country_codes",
				FullTableName: "staging.country_codes",
				RelPath:       "staging/country_codes.csv",
			},
		},
	}
}

func TestSelectorGlob(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"stg_*"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "stg_customers")
}

func TestSelectorTag(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"tag:daily"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 3) // stg_orders, stg_customers, fct_orders
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "stg_customers")
	assert.Contains(t, result, "fct_orders")
	assert.NotContains(t, result, "dim_customers") // weekly tag
	assert.NotContains(t, result, "country_codes") // seed, no tags
}

func TestSelectorUpstream(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"+fct_orders"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	// fct_orders + stg_orders (upstream)
	assert.Len(t, result, 2)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")

	// Should be in topological order (stg_orders before fct_orders)
	stgIdx, fctIdx := -1, -1
	for i, name := range result {
		if name == "stg_orders" {
			stgIdx = i
		}
		if name == "fct_orders" {
			fctIdx = i
		}
	}
	assert.Less(t, stgIdx, fctIdx)
}

func TestSelectorUpstreamDeep(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"+revenue"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	// revenue + fct_orders + stg_orders (transitive upstream)
	assert.Len(t, result, 3)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")
}

func TestSelectorPath(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"staging/*"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	// Should match staging/stg_orders.sql, staging/stg_customers.sql, staging/country_codes.csv
	assert.Len(t, result, 3)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "stg_customers")
	assert.Contains(t, result, "country_codes")
}

func TestSelectorPathNested(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"marts/core/*"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "dim_customers")
	assert.Contains(t, result, "fct_orders")
}

func TestSelectorExclude(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// Select all, then exclude stg_*
	sel := NewSelector(nil, []string{"stg_*"})
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.NotContains(t, result, "stg_orders")
	assert.NotContains(t, result, "stg_customers")
	assert.Contains(t, result, "dim_customers")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")
	assert.Contains(t, result, "country_codes")
}

func TestSelectorNoFilter(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector(nil, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	// All nodes in DAG order
	assert.Len(t, result, 6)
	assert.Equal(t, dag.Order, result)
}

func TestSelectorCombined(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// Select tag:daily, exclude stg_*
	sel := NewSelector([]string{"tag:daily"}, []string{"stg_*"})
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	// Only fct_orders should remain (stg_ models are daily but excluded)
	assert.Len(t, result, 1)
	assert.Contains(t, result, "fct_orders")
}

func TestSelectorDownstream(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"stg_orders+"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	// stg_orders + fct_orders + revenue (downstream)
	assert.Len(t, result, 3)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")

	// Should be in topological order
	stgIdx, fctIdx, revIdx := -1, -1, -1
	for i, name := range result {
		switch name {
		case "stg_orders":
			stgIdx = i
		case "fct_orders":
			fctIdx = i
		case "revenue":
			revIdx = i
		}
	}
	assert.Less(t, stgIdx, fctIdx)
	assert.Less(t, fctIdx, revIdx)
}

func TestSelectorDownstreamLeaf(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// revenue has no downstream, so result is just revenue itself
	sel := NewSelector([]string{"revenue+"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 1)
	assert.Contains(t, result, "revenue")
}

func TestSelectorUpstreamAndDownstream(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// +fct_orders+ = upstream (stg_orders) + fct_orders + downstream (revenue)
	sel := NewSelector([]string{"+fct_orders+"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 3)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")
}

func TestSelectorDownstreamDegree(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// stg_orders+1 = stg_orders + 1 step downstream (fct_orders)
	sel := NewSelector([]string{"stg_orders+1"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
	assert.NotContains(t, result, "revenue") // 2 steps away
}

func TestSelectorDownstreamDegreeTwo(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// stg_orders+2 = stg_orders + fct_orders + revenue
	sel := NewSelector([]string{"stg_orders+2"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 3)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")
}

func TestSelectorUpstreamDegree(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// 1+revenue = revenue + 1 step upstream (fct_orders)
	sel := NewSelector([]string{"1+revenue"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")
	assert.NotContains(t, result, "stg_orders") // 2 steps away
}

func TestSelectorUpstreamDegreeTwo(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// 2+revenue = revenue + fct_orders + stg_orders
	sel := NewSelector([]string{"2+revenue"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 3)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")
}

func TestSelectorDegreeZero(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// 0+revenue = just revenue itself
	sel := NewSelector([]string{"0+revenue"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 1)
	assert.Contains(t, result, "revenue")

	// revenue+0 = just revenue itself
	sel = NewSelector([]string{"revenue+0"}, nil)
	result, err = sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 1)
	assert.Contains(t, result, "revenue")
}

func TestSelectorSlice(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// stg_orders-revenue = stg_orders, fct_orders, revenue
	sel := NewSelector([]string{"stg_orders-revenue"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 3)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")

	// Should be in topological order
	stgIdx, fctIdx, revIdx := -1, -1, -1
	for i, name := range result {
		switch name {
		case "stg_orders":
			stgIdx = i
		case "fct_orders":
			fctIdx = i
		case "revenue":
			revIdx = i
		}
	}
	assert.Less(t, stgIdx, fctIdx)
	assert.Less(t, fctIdx, revIdx)
}

func TestSelectorSliceAdjacent(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// stg_orders-fct_orders = just those two
	sel := NewSelector([]string{"stg_orders-fct_orders"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
}

func TestSelectorSliceDisjoint(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// stg_customers-revenue: no path between them, just the two endpoints
	sel := NewSelector([]string{"stg_customers-revenue"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "stg_customers")
	assert.Contains(t, result, "revenue")
}

func TestSelectorDownstreamNotFound(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"nonexistent+"}, nil)
	_, err = sel.Apply(dag)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestSelectorUpstreamNotFound(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"+nonexistent"}, nil)
	_, err = sel.Apply(dag)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestSelectorGlobNoMatch(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"zzz_*"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Empty(t, result)
}

func TestSelectorMultipleIncludes(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"stg_orders", "dim_customers"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "dim_customers")
}

func TestSelectorGlobUpstream(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// +stg_* = all stg_ models + their upstream (stg_ models have no upstream)
	sel := NewSelector([]string{"+stg_*"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "stg_customers")
}

func TestSelectorGlobDownstream(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// stg_*+ = all stg_ models + all their downstream
	sel := NewSelector([]string{"stg_*+"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 5)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "stg_customers")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "dim_customers")
	assert.Contains(t, result, "revenue")
}

func TestSelectorGlobBoth(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// +fct_*+ = fct_orders + upstream (stg_orders) + downstream (revenue)
	sel := NewSelector([]string{"+fct_*+"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 3)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "revenue")
}

func TestSelectorGlobUpstreamDegree(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// 1+*_orders = stg_orders and fct_orders, plus 1-upstream of each
	// 1-upstream of stg_orders = nothing; 1-upstream of fct_orders = stg_orders
	sel := NewSelector([]string{"1+*_orders"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "fct_orders")
}

func TestSelectorGlobDownstreamDegree(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// stg_*+1 = stg_orders and stg_customers, plus 1-downstream of each
	// 1-downstream of stg_orders = fct_orders; 1-downstream of stg_customers = dim_customers
	sel := NewSelector([]string{"stg_*+1"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Len(t, result, 4)
	assert.Contains(t, result, "stg_orders")
	assert.Contains(t, result, "stg_customers")
	assert.Contains(t, result, "fct_orders")
	assert.Contains(t, result, "dim_customers")
}

func TestSelectorGlobGraphNoMatch(t *testing.T) {
	project := newTestDAGProject()
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// +zzz_* = glob matches nothing, should return empty (no error)
	sel := NewSelector([]string{"+zzz_*"}, nil)
	result, err := sel.Apply(dag)
	require.NoError(t, err)

	assert.Empty(t, result)

	// zzz_*+ = same behavior
	sel = NewSelector([]string{"zzz_*+"}, nil)
	result, err = sel.Apply(dag)
	require.NoError(t, err)

	assert.Empty(t, result)

	// 1+zzz_* = same behavior
	sel = NewSelector([]string{"1+zzz_*"}, nil)
	result, err = sel.Apply(dag)
	require.NoError(t, err)

	assert.Empty(t, result)

	// zzz_*+1 = same behavior
	sel = NewSelector([]string{"zzz_*+1"}, nil)
	result, err = sel.Apply(dag)
	require.NoError(t, err)

	assert.Empty(t, result)
}

func TestSelectorStemAndSchemaTable(t *testing.T) {
	project := loadMartsEventsProject(t, BuildOptions{Prod: true})
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"events"}, nil)
	sel.Project = project
	result, err := sel.Apply(dag)
	require.NoError(t, err)
	assert.Equal(t, []string{"events"}, result)

	sel = NewSelector([]string{"analytics.events"}, nil)
	sel.Project = project
	result, err = sel.Apply(dag)
	require.NoError(t, err)
	assert.Equal(t, []string{"events"}, result)

	sel = NewSelector([]string{"analytics/plausible/*"}, nil)
	sel.Project = project
	result, err = sel.Apply(dag)
	require.NoError(t, err)
	assert.Equal(t, []string{"events"}, result)
}

func TestSelectorOldPrefixedNameErrors(t *testing.T) {
	project := loadMartsEventsProject(t, BuildOptions{Prod: true})
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"plausible_events"}, nil)
	sel.Project = project
	_, err = sel.Apply(dag)
	require.Error(t, err)

	var notFound *NameNotFoundError
	require.ErrorAs(t, err, &notFound)
	assert.Equal(t, "selector", notFound.Kind)
	assert.Contains(t, err.Error(), "selector 'plausible_events' not found")
}

func TestSelectorSubfolderPathGlob(t *testing.T) {
	project, err := LoadProject(getTestFixturePath("sample_project"), BuildOptions{Prod: true})
	require.NoError(t, err)
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"marts/core/*"}, nil)
	sel.Project = project
	result, err := sel.Apply(dag)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"dim_customers", "fct_orders"}, result)
}

func TestSelectorGlobThreePart(t *testing.T) {
	project, err := LoadProject(getTestFixturePath("database_project"), BuildOptions{Prod: true})
	require.NoError(t, err)
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"marts.*"}, nil)
	sel.Project = project
	result, err := sel.Apply(dag)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"dim_customers", "revenue"}, result)
}

func TestSelectorTypoSuggests(t *testing.T) {
	project := loadNestedEventsProject(t)
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	sel := NewSelector([]string{"plausible_event"}, nil)
	sel.Project = project
	_, err = sel.Apply(dag)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.Contains(t, err.Error(), "Did you mean")
	assert.Contains(t, err.Error(), "plausible_events")
}

// =============================================================================
// DAG tests
// =============================================================================

func TestDAGLinearChain(t *testing.T) {
	// A -> B -> C
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a"},
			"b": {Name: "b", FullTableName: "public.b", DependsOn: []string{"a"}},
			"c": {Name: "c", FullTableName: "public.c", DependsOn: []string{"b"}},
		},
		Seeds: map[string]*Seed{},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	assert.Equal(t, []string{"a", "b", "c"}, dag.Order)

	// Check depths
	assert.Equal(t, 0, dag.Nodes["a"].Depth)
	assert.Equal(t, 1, dag.Nodes["b"].Depth)
	assert.Equal(t, 2, dag.Nodes["c"].Depth)
}

func TestDAGDiamond(t *testing.T) {
	// A -> B, A -> C, B -> D, C -> D
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a"},
			"b": {Name: "b", FullTableName: "public.b", DependsOn: []string{"a"}},
			"c": {Name: "c", FullTableName: "public.c", DependsOn: []string{"a"}},
			"d": {Name: "d", FullTableName: "public.d", DependsOn: []string{"b", "c"}},
		},
		Seeds: map[string]*Seed{},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// A must come first, D must come last
	assert.Equal(t, "a", dag.Order[0])
	assert.Equal(t, "d", dag.Order[3])

	// B and C can be in either order but both between A and D
	middle := dag.Order[1:3]
	assert.Contains(t, middle, "b")
	assert.Contains(t, middle, "c")

	// Check depths
	assert.Equal(t, 0, dag.Nodes["a"].Depth)
	assert.Equal(t, 1, dag.Nodes["b"].Depth)
	assert.Equal(t, 1, dag.Nodes["c"].Depth)
	assert.Equal(t, 2, dag.Nodes["d"].Depth)
}

func TestDAGCycleDetection(t *testing.T) {
	// A -> B -> C -> A (cycle)
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a", DependsOn: []string{"c"}},
			"b": {Name: "b", FullTableName: "public.b", DependsOn: []string{"a"}},
			"c": {Name: "c", FullTableName: "public.c", DependsOn: []string{"b"}},
		},
		Seeds: map[string]*Seed{},
	}

	_, err := BuildDAG(project)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestDAGDisconnected(t *testing.T) {
	// Two independent chains: A -> B, C -> D
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a"},
			"b": {Name: "b", FullTableName: "public.b", DependsOn: []string{"a"}},
			"c": {Name: "c", FullTableName: "public.c"},
			"d": {Name: "d", FullTableName: "public.d", DependsOn: []string{"c"}},
		},
		Seeds: map[string]*Seed{},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	assert.Len(t, dag.Order, 4)

	// A before B, C before D
	aIdx, bIdx, cIdx, dIdx := -1, -1, -1, -1
	for i, name := range dag.Order {
		switch name {
		case "a":
			aIdx = i
		case "b":
			bIdx = i
		case "c":
			cIdx = i
		case "d":
			dIdx = i
		}
	}
	assert.Less(t, aIdx, bIdx)
	assert.Less(t, cIdx, dIdx)
}

func TestDAGSeedsFirst(t *testing.T) {
	// Seeds should be at depth 0
	project := &BuildProject{
		Models: map[string]*Model{
			"model_a": {Name: "model_a", FullTableName: "public.model_a", DependsOn: []string{"seed_x"}},
		},
		Seeds: map[string]*Seed{
			"seed_x": {Name: "seed_x", FullTableName: "public.seed_x"},
		},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	assert.Equal(t, 0, dag.Nodes["seed_x"].Depth)
	assert.Equal(t, 1, dag.Nodes["model_a"].Depth)

	// Seed should come first in order
	assert.Equal(t, "seed_x", dag.Order[0])
	assert.Equal(t, "model_a", dag.Order[1])
}

func TestDAGGetUpstream(t *testing.T) {
	// seed -> A -> B -> C
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a", DependsOn: []string{"seed"}},
			"b": {Name: "b", FullTableName: "public.b", DependsOn: []string{"a"}},
			"c": {Name: "c", FullTableName: "public.c", DependsOn: []string{"b"}},
		},
		Seeds: map[string]*Seed{
			"seed": {Name: "seed", FullTableName: "public.seed"},
		},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// Upstream of C should be seed, A, B (in topological order)
	upstream := dag.GetUpstream("c")
	assert.Equal(t, []string{"seed", "a", "b"}, upstream)

	// Upstream of A should be just seed
	upstream = dag.GetUpstream("a")
	assert.Equal(t, []string{"seed"}, upstream)

	// Upstream of seed should be empty
	upstream = dag.GetUpstream("seed")
	assert.Len(t, upstream, 0)
}

func TestDAGGetDownstream(t *testing.T) {
	// seed -> A -> B -> C
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a", DependsOn: []string{"seed"}},
			"b": {Name: "b", FullTableName: "public.b", DependsOn: []string{"a"}},
			"c": {Name: "c", FullTableName: "public.c", DependsOn: []string{"b"}},
		},
		Seeds: map[string]*Seed{
			"seed": {Name: "seed", FullTableName: "public.seed"},
		},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	downstream := dag.GetDownstream("seed")
	assert.Len(t, downstream, 3)
	assert.Contains(t, downstream, "a")
	assert.Contains(t, downstream, "b")
	assert.Contains(t, downstream, "c")
}

func TestDAGExecutionLevels(t *testing.T) {
	// seed1, seed2 -> A, B -> C
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a", DependsOn: []string{"seed1"}},
			"b": {Name: "b", FullTableName: "public.b", DependsOn: []string{"seed2"}},
			"c": {Name: "c", FullTableName: "public.c", DependsOn: []string{"a", "b"}},
		},
		Seeds: map[string]*Seed{
			"seed1": {Name: "seed1", FullTableName: "public.seed1"},
			"seed2": {Name: "seed2", FullTableName: "public.seed2"},
		},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	levels := dag.GetExecutionLevels()
	require.Len(t, levels, 3)

	// Level 0: seeds
	assert.Len(t, levels[0], 2)
	assert.Contains(t, levels[0], "seed1")
	assert.Contains(t, levels[0], "seed2")

	// Level 1: models depending on seeds
	assert.Len(t, levels[1], 2)
	assert.Contains(t, levels[1], "a")
	assert.Contains(t, levels[1], "b")

	// Level 2: model depending on level 1
	assert.Len(t, levels[2], 1)
	assert.Contains(t, levels[2], "c")
}

func TestDAGEmptyProject(t *testing.T) {
	project := &BuildProject{
		Models: map[string]*Model{},
		Seeds:  map[string]*Seed{},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)
	assert.Empty(t, dag.Order)
	assert.Empty(t, dag.Nodes)

	levels := dag.GetExecutionLevels()
	assert.Nil(t, levels)
}

func TestDAGSingleNode(t *testing.T) {
	project := &BuildProject{
		Models: map[string]*Model{
			"only_model": {Name: "only_model", FullTableName: "public.only_model"},
		},
		Seeds: map[string]*Seed{},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	assert.Equal(t, []string{"only_model"}, dag.Order)
	assert.Equal(t, 0, dag.Nodes["only_model"].Depth)
}

func TestDAGExternalDependenciesIgnored(t *testing.T) {
	// Model depends on something not in the project (external source)
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a", DependsOn: []string{"external_table"}},
		},
		Seeds: map[string]*Seed{},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	assert.Equal(t, []string{"a"}, dag.Order)
	assert.Empty(t, dag.Nodes["a"].Dependencies) // external deps filtered out
}

func TestDAGDependentsPopulated(t *testing.T) {
	project := &BuildProject{
		Models: map[string]*Model{
			"a": {Name: "a", FullTableName: "public.a"},
			"b": {Name: "b", FullTableName: "public.b", DependsOn: []string{"a"}},
			"c": {Name: "c", FullTableName: "public.c", DependsOn: []string{"a"}},
		},
		Seeds: map[string]*Seed{},
	}

	dag, err := BuildDAG(project)
	require.NoError(t, err)

	// A should have B and C as dependents
	dependents := dag.Nodes["a"].Dependents
	assert.Len(t, dependents, 2)
	assert.Contains(t, dependents, "b")
	assert.Contains(t, dependents, "c")
}
