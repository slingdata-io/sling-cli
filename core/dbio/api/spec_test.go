package api

import (
	"context"
	"fmt"
	"testing"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSpecEndpointsSort(t *testing.T) {
	tests := []struct {
		name     string
		spec     string
		expected []string
	}{
		{
			name: "simple queue dependency",
			spec: `
name: "Test API"
queues:
  - customer_ids

endpoints:
  customer_balance:
    iterate:
      over: "queue.customer_ids"
      into: "state.customer_id"
    request:
      url: "https://api.example.com/balance"
    response:
      records:
        jmespath: "data[]"

  customer:
    request:
      url: "https://api.example.com/customers"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.customer_ids"

  account:
    request:
      url: "https://api.example.com/account"
    response:
      records:
        jmespath: "data[]"
`,
			expected: []string{"account", "customer", "customer_balance"},
		},
		{
			name: "multiple queue dependencies",
			spec: `
name: "Test API"
queues:
  - customer_ids
  - invoice_ids

endpoints:
  invoice_line_item:
    iterate:
      over: "queue.invoice_ids"
      into: "state.invoice_id"
    request:
      url: "https://api.example.com/lines"
    response:
      records:
        jmespath: "data[]"

  customer_balance:
    iterate:
      over: "queue.customer_ids"
      into: "state.customer_id"
    request:
      url: "https://api.example.com/balance"
    response:
      records:
        jmespath: "data[]"

  customer:
    request:
      url: "https://api.example.com/customers"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.customer_ids"

  invoice:
    request:
      url: "https://api.example.com/invoices"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.invoice_ids"

  charge:
    request:
      url: "https://api.example.com/charges"
    response:
      records:
        jmespath: "data[]"
`,
			expected: []string{"charge", "customer", "invoice", "customer_balance", "invoice_line_item"},
		},
		{
			name: "no dependencies - alphabetical order",
			spec: `
name: "Test API"

endpoints:
  zebra:
    request:
      url: "https://api.example.com/zebra"
    response:
      records:
        jmespath: "data[]"

  apple:
    request:
      url: "https://api.example.com/apple"
    response:
      records:
        jmespath: "data[]"

  monkey:
    request:
      url: "https://api.example.com/monkey"
    response:
      records:
        jmespath: "data[]"
`,
			expected: []string{"apple", "monkey", "zebra"},
		},
		{
			name: "chain dependency",
			spec: `
name: "Test API"
queues:
  - ids_a
  - ids_b

endpoints:
  endpoint_c:
    iterate:
      over: "queue.ids_b"
      into: "state.id_b"
    request:
      url: "https://api.example.com/c"
    response:
      records:
        jmespath: "data[]"

  endpoint_b:
    iterate:
      over: "queue.ids_a"
      into: "state.id_a"
    request:
      url: "https://api.example.com/b"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.ids_b"

  endpoint_a:
    request:
      url: "https://api.example.com/a"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.ids_a"
`,
			expected: []string{"endpoint_a", "endpoint_b", "endpoint_c"},
		},
		{
			name: "multiple producers for same queue",
			spec: `
name: "Test API"
queues:
  - shared_ids

endpoints:
  consumer:
    iterate:
      over: "queue.shared_ids"
      into: "state.id"
    request:
      url: "https://api.example.com/consumer"
    response:
      records:
        jmespath: "data[]"

  producer_b:
    request:
      url: "https://api.example.com/producer_b"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.shared_ids"

  producer_a:
    request:
      url: "https://api.example.com/producer_a"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.shared_ids"
`,
			expected: []string{"producer_a", "producer_b", "consumer"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := LoadSpec(tt.spec)
			require.NoError(t, err)

			// Create endpoints slice
			var endpoints Endpoints
			for name, ep := range spec.EndpointMap {
				ep.Name = name
				endpoints = append(endpoints, ep)
			}

			// Sort
			endpoints.Sort()

			// Get names
			actual := lo.Map(endpoints, func(ep Endpoint, i int) string {
				return ep.Name
			})

			assert.Equal(t, tt.expected, actual, "Expected endpoints to be sorted as %v, got %v", tt.expected, actual)
		})
	}
}

func TestSpecEndpointsDAG(t *testing.T) {
	tests := []struct {
		name     string
		spec     string
		expected [][]string
	}{
		{
			name: "simple groups",
			spec: `
name: "Test API"
queues:
  - customer_ids
  - invoice_ids

endpoints:
  customer:
    request:
      url: "https://api.example.com/customers"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.customer_ids"

  customer_balance:
    iterate:
      over: "queue.customer_ids"
      into: "state.customer_id"
    request:
      url: "https://api.example.com/balance"
    response:
      records:
        jmespath: "data[]"

  invoice:
    request:
      url: "https://api.example.com/invoices"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.invoice_ids"

  invoice_line_item:
    iterate:
      over: "queue.invoice_ids"
      into: "state.invoice_id"
    request:
      url: "https://api.example.com/lines"
    response:
      records:
        jmespath: "data[]"

  charge:
    request:
      url: "https://api.example.com/charges"
    response:
      records:
        jmespath: "data[]"
`,
			expected: [][]string{
				{"charge"},
				{"customer", "customer_balance"},
				{"invoice", "invoice_line_item"},
			},
		},
		{
			name: "all independent",
			spec: `
name: "Test API"

endpoints:
  endpoint_a:
    request:
      url: "https://api.example.com/a"
    response:
      records:
        jmespath: "data[]"

  endpoint_b:
    request:
      url: "https://api.example.com/b"
    response:
      records:
        jmespath: "data[]"

  endpoint_c:
    request:
      url: "https://api.example.com/c"
    response:
      records:
        jmespath: "data[]"
`,
			expected: [][]string{
				{"endpoint_a"},
				{"endpoint_b"},
				{"endpoint_c"},
			},
		},
		{
			name: "chain dependency group",
			spec: `
name: "Test API"
queues:
  - ids_a
  - ids_b

endpoints:
  endpoint_a:
    request:
      url: "https://api.example.com/a"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.ids_a"

  endpoint_b:
    iterate:
      over: "queue.ids_a"
      into: "state.id_a"
    request:
      url: "https://api.example.com/b"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.ids_b"

  endpoint_c:
    iterate:
      over: "queue.ids_b"
      into: "state.id_b"
    request:
      url: "https://api.example.com/c"
    response:
      records:
        jmespath: "data[]"

  independent:
    request:
      url: "https://api.example.com/independent"
    response:
      records:
        jmespath: "data[]"
`,
			expected: [][]string{
				{"endpoint_a", "endpoint_b", "endpoint_c"},
				{"independent"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := LoadSpec(tt.spec)
			require.NoError(t, err)

			// Create endpoints slice
			var endpoints Endpoints
			for name, ep := range spec.EndpointMap {
				ep.Name = name
				endpoints = append(endpoints, ep)
			}

			// Get DAG
			actual := endpoints.DAG()

			// Compare - note that within each group, order might vary but the groups themselves should match
			assert.Equal(t, len(tt.expected), len(actual), "Expected %d groups, got %d", len(tt.expected), len(actual))

			// Check each group contains the expected endpoints
			for i, expectedGroup := range tt.expected {
				if i < len(actual) {
					assert.ElementsMatch(t, expectedGroup, actual[i], "Group %d mismatch", i)
				}
			}
		})
	}
}

func TestSpecBuildDependencyMap(t *testing.T) {
	spec := `
name: "Test API"
queues:
  - customer_ids
  - invoice_ids

endpoints:
  customer:
    request:
      url: "https://api.example.com/customers"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.customer_ids"

  customer_balance:
    iterate:
      over: "queue.customer_ids"
      into: "state.customer_id"
    request:
      url: "https://api.example.com/balance"
    response:
      records:
        jmespath: "data[]"

  invoice:
    request:
      url: "https://api.example.com/invoices"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.invoice_ids"

  invoice_line_item:
    iterate:
      over: "queue.invoice_ids"
      into: "state.invoice_id"
    request:
      url: "https://api.example.com/lines"
    response:
      records:
        jmespath: "data[]"

  charge:
    request:
      url: "https://api.example.com/charges"
    response:
      records:
        jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	// Create endpoints slice
	var endpoints Endpoints
	for name, ep := range s.EndpointMap {
		ep.Name = name
		endpoints = append(endpoints, ep)
	}

	deps := endpoints.buildDependencyMap()

	// Check dependencies
	assert.Equal(t, []string{"customer"}, deps["customer_balance"])
	assert.Equal(t, []string{"invoice"}, deps["invoice_line_item"])
	assert.Equal(t, []string{}, deps["customer"])
	assert.Equal(t, []string{}, deps["invoice"])
	assert.Equal(t, []string{}, deps["charge"])
}

func TestSpecTopologicalSort(t *testing.T) {
	tests := []struct {
		name         string
		endpoints    []string
		dependencies map[string][]string
		expected     []string
	}{
		{
			name:      "simple chain",
			endpoints: []string{"a", "b", "c"},
			dependencies: map[string][]string{
				"a": {},
				"b": {"a"},
				"c": {"b"},
			},
			expected: []string{"a", "b", "c"},
		},
		{
			name:      "diamond dependency",
			endpoints: []string{"a", "b", "c", "d"},
			dependencies: map[string][]string{
				"a": {},
				"b": {"a"},
				"c": {"a"},
				"d": {"b", "c"},
			},
			expected: []string{"a", "b", "c", "d"},
		},
		{
			name:      "no dependencies",
			endpoints: []string{"z", "a", "m"},
			dependencies: map[string][]string{
				"z": {},
				"a": {},
				"m": {},
			},
			expected: []string{"a", "m", "z"}, // alphabetical when no deps
		},
		{
			name:      "multiple roots",
			endpoints: []string{"a", "b", "c", "d", "e"},
			dependencies: map[string][]string{
				"a": {},
				"b": {},
				"c": {"a"},
				"d": {"b"},
				"e": {"c", "d"},
			},
			expected: []string{"a", "b", "c", "d", "e"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create endpoints
			var endpoints Endpoints
			for _, name := range tt.endpoints {
				endpoints = append(endpoints, Endpoint{Name: name})
			}

			// Run topological sort
			result := endpoints.topologicalSort(tt.dependencies)

			// Verify result
			assert.Equal(t, tt.expected, result)

			// Verify all dependencies are satisfied
			processed := make(map[string]int)
			for i, name := range result {
				processed[name] = i
			}

			for endpoint, deps := range tt.dependencies {
				endpointIdx := processed[endpoint]
				for _, dep := range deps {
					depIdx := processed[dep]
					assert.Less(t, depIdx, endpointIdx, "%s should come before %s", dep, endpoint)
				}
			}
		})
	}
}

func TestSpecEndpointsNames(t *testing.T) {
	endpoints := Endpoints{
		{Name: "zebra"},
		{Name: "apple"},
		{Name: "monkey"},
	}

	// Names() should return names in alphabetical order
	names := endpoints.Names()
	expected := []string{"apple", "monkey", "zebra"}
	assert.Equal(t, expected, names)
}

func TestSpecEndpointDependsOn(t *testing.T) {
	tests := []struct {
		name     string
		spec     string
		endpoint string
		expected []string
	}{
		{
			name: "endpoint with single dependency",
			spec: `
name: "Test API"
queues:
  - customer_ids

endpoints:
  customer:
    request:
      url: "https://api.example.com/customers"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.customer_ids"

  customer_balance:
    iterate:
      over: "queue.customer_ids"
      into: "state.customer_id"
    request:
      url: "https://api.example.com/balance"
    response:
      records:
        jmespath: "data[]"

  account:
    request:
      url: "https://api.example.com/account"
    response:
      records:
        jmespath: "data[]"
`,
			endpoint: "customer_balance",
			expected: []string{"customer"},
		},
		{
			name: "endpoint with no dependency",
			spec: `
name: "Test API"

endpoints:
  account:
    request:
      url: "https://api.example.com/account"
    response:
      records:
        jmespath: "data[]"
`,
			endpoint: "account",
			expected: []string{},
		},
		{
			name: "endpoint with multiple producers (returns all alphabetically)",
			spec: `
name: "Test API"
queues:
  - shared_queue

endpoints:
  producer_b:
    request:
      url: "https://api.example.com/producer_b"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.shared_queue"

  producer_a:
    request:
      url: "https://api.example.com/producer_a"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.shared_queue"

  consumer:
    iterate:
      over: "queue.shared_queue"
      into: "state.id"
    request:
      url: "https://api.example.com/consumer"
    response:
      records:
        jmespath: "data[]"
`,
			endpoint: "consumer",
			expected: []string{"producer_a", "producer_b"}, // all alphabetically
		},
		{
			name: "producer endpoint (no dependency)",
			spec: `
name: "Test API"
queues:
  - customer_ids

endpoints:
  customer:
    request:
      url: "https://api.example.com/customers"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.customer_ids"
`,
			endpoint: "customer",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := LoadSpec(tt.spec)
			require.NoError(t, err)

			// Create endpoints slice
			var endpoints Endpoints
			for name, ep := range spec.EndpointMap {
				ep.Name = name
				endpoints = append(endpoints, ep)
			}

			// Test UpstreamOf (replaces DependsOn)
			actual := endpoints.HasUpstreams(tt.endpoint)
			assert.Equal(t, tt.expected, actual, "Expected %s to depend on %v, got %v", tt.endpoint, tt.expected, actual)
		})
	}
}

func TestSpecEndpointsUpstreamOf(t *testing.T) {
	tests := []struct {
		name         string
		spec         string
		endpointName string
		expected     []string
	}{
		{
			name: "endpoint with single dependency",
			spec: `
name: "Test API"
queues:
  - customer_ids

endpoints:
  customer:
    request:
      url: "https://api.example.com/customers"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.customer_ids"

  customer_balance:
    iterate:
      over: "queue.customer_ids"
      into: "state.customer_id"
    request:
      url: "https://api.example.com/balance"
    response:
      records:
        jmespath: "data[]"
`,
			endpointName: "customer_balance",
			expected:     []string{"customer"},
		},
		{
			name: "endpoint with no dependencies",
			spec: `
name: "Test API"

endpoints:
  account:
    request:
      url: "https://api.example.com/account"
    response:
      records:
        jmespath: "data[]"
`,
			endpointName: "account",
			expected:     []string{},
		},
		{
			name: "endpoint with multiple dependencies",
			spec: `
name: "Test API"
queues:
  - shared_queue

endpoints:
  producer_a:
    request:
      url: "https://api.example.com/producer_a"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.shared_queue"

  producer_b:
    request:
      url: "https://api.example.com/producer_b"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.shared_queue"

  consumer:
    iterate:
      over: "queue.shared_queue"
      into: "state.id"
    request:
      url: "https://api.example.com/consumer"
    response:
      records:
        jmespath: "data[]"
`,
			endpointName: "consumer",
			expected:     []string{"producer_a", "producer_b"},
		},
		{
			name: "non-existent endpoint",
			spec: `
name: "Test API"

endpoints:
  account:
    request:
      url: "https://api.example.com/account"
    response:
      records:
        jmespath: "data[]"
`,
			endpointName: "non_existent",
			expected:     []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := LoadSpec(tt.spec)
			require.NoError(t, err)

			// Create endpoints slice
			var endpoints Endpoints
			for name, ep := range spec.EndpointMap {
				ep.Name = name
				endpoints = append(endpoints, ep)
			}

			// Test UpstreamOf
			actual := endpoints.HasUpstreams(tt.endpointName)
			assert.Equal(t, tt.expected, actual, "Expected upstreams of %s to be %v, got %v", tt.endpointName, tt.expected, actual)
		})
	}
}

func TestSpecEndpointsUpstreamOfEdgeCases(t *testing.T) {
	t.Run("empty endpoints slice", func(t *testing.T) {
		var endpoints Endpoints
		result := endpoints.HasUpstreams("any_endpoint")
		assert.Equal(t, []string{}, result)
	})

	t.Run("empty endpoint name", func(t *testing.T) {
		endpoints := Endpoints{
			{Name: "test_endpoint"},
		}
		result := endpoints.HasUpstreams("")
		assert.Equal(t, []string{}, result)
	})
}

func TestSpecEndpointDependsOnEdgeCases(t *testing.T) {
	t.Run("empty endpoints slice", func(t *testing.T) {
		var endpoints Endpoints
		result := endpoints.HasUpstreams("test")
		assert.Equal(t, []string{}, result)
	})

	t.Run("iterate over non-queue", func(t *testing.T) {
		endpoint := Endpoint{
			Name: "test",
			Iterate: Iterate{
				Over: "some_array",
				Into: "state.item",
			},
		}

		endpoints := Endpoints{endpoint}
		result := endpoints.HasUpstreams("test")
		assert.Equal(t, []string{}, result)
	})

	t.Run("iterate over non-string", func(t *testing.T) {
		endpoint := Endpoint{
			Name: "test",
			Iterate: Iterate{
				Over: []string{"item1", "item2"},
				Into: "state.item",
			},
		}

		endpoints := Endpoints{endpoint}
		result := endpoints.HasUpstreams("test")
		assert.Equal(t, []string{}, result)
	})

	t.Run("self-referencing endpoint (should not depend on self)", func(t *testing.T) {
		endpoint := Endpoint{
			Name: "self_producer",
			Iterate: Iterate{
				Over: "queue.self_queue",
				Into: "state.id",
			},
			Response: Response{
				Processors: []Processor{
					{
						Expression: "record.id",
						Output:     "queue.self_queue",
					},
				},
			},
		}

		endpoints := Endpoints{endpoint}
		result := endpoints.HasUpstreams("self_producer")
		assert.Equal(t, []string{}, result) // Should return empty since no other producer
	})

	t.Run("endpoint with duplicate producers (should deduplicate)", func(t *testing.T) {
		producer := Endpoint{
			Name: "producer",
			Response: Response{
				Processors: []Processor{
					{
						Expression: "record.id",
						Output:     "queue.test_queue",
					},
					{
						Expression: "record.other_id",
						Output:     "queue.test_queue", // Same queue, duplicate
					},
				},
			},
		}

		consumer := Endpoint{
			Name: "consumer",
			Iterate: Iterate{
				Over: "queue.test_queue",
				Into: "state.id",
			},
		}

		endpoints := Endpoints{producer, consumer}
		result := endpoints.HasUpstreams("consumer")
		assert.Equal(t, []string{"producer"}, result) // Should not have duplicates
	})
}

func TestLoadSpecUnknownDependsOn(t *testing.T) {
	_, err := LoadSpec(`
name: "Bad Depends"
endpoints:
  child:
    request:
      url: "https://api.example.com/child"
    depends_on: [does_not_exist]
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does_not_exist")
}

func TestSpecCircularDependencyHandling(t *testing.T) {
	// While circular dependencies shouldn't occur with proper queue usage,
	// the topological sort should handle them gracefully
	endpoints := Endpoints{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	}

	// Create circular dependency
	dependencies := map[string][]string{
		"a": {"c"}, // a depends on c
		"b": {"a"}, // b depends on a
		"c": {"b"}, // c depends on b (creates cycle)
	}

	result := endpoints.topologicalSort(dependencies)

	// Should return all endpoints even with cycle
	assert.Equal(t, 3, len(result))
	assert.Contains(t, result, "a")
	assert.Contains(t, result, "b")
	assert.Contains(t, result, "c")
}

func TestSpecMultipleProducersForSameQueue(t *testing.T) {
	spec := `
name: "Test API"
queues:
  - shared_queue

endpoints:
  producer_a:
    request:
      url: "https://api.example.com/producer_a"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.shared_queue"

  producer_b:
    request:
      url: "https://api.example.com/producer_b"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.shared_queue"

  consumer:
    iterate:
      over: "queue.shared_queue"
      into: "state.id"
    request:
      url: "https://api.example.com/consumer"
    response:
      records:
        jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	// Create endpoints slice
	var endpoints Endpoints
	for name, ep := range s.EndpointMap {
		ep.Name = name
		endpoints = append(endpoints, ep)
	}

	deps := endpoints.buildDependencyMap()

	// Consumer should depend on both producers
	assert.ElementsMatch(t, []string{"producer_a", "producer_b"}, deps["consumer"])

	// After sorting, both producers should come before consumer
	endpoints.Sort()

	consumerIdx := -1
	producerAIdx := -1
	producerBIdx := -1

	for i, endpoint := range endpoints {
		switch endpoint.Name {
		case "consumer":
			consumerIdx = i
		case "producer_a":
			producerAIdx = i
		case "producer_b":
			producerBIdx = i
		}
	}

	assert.Less(t, producerAIdx, consumerIdx)
	assert.Less(t, producerBIdx, consumerIdx)
}

func TestSpecDisabledEndpoints(t *testing.T) {
	spec := `
name: "Test API"
queues:
  - test_queue

endpoints:
  producer:
    request:
      url: "https://api.example.com/producer"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.test_queue"

  consumer:
    disabled: true
    iterate:
      over: "queue.test_queue"
      into: "state.id"
    request:
      url: "https://api.example.com/consumer"
    response:
      records:
        jmespath: "data[]"

  other:
    request:
      url: "https://api.example.com/other"
    response:
      records:
        jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	// Create endpoints slice - including disabled
	var endpoints Endpoints
	for name, ep := range s.EndpointMap {
		ep.Name = name
		endpoints = append(endpoints, ep)
	}

	// Sort should handle disabled endpoints
	endpoints.Sort()

	// DAG should include all endpoints
	dag := endpoints.DAG()
	assert.NotNil(t, dag)
}

func TestSpecEmptyEndpoints(t *testing.T) {
	var endpoints Endpoints

	// Sort should handle empty slice
	endpoints.Sort()
	assert.Equal(t, 0, len(endpoints))

	// DAG should return empty
	dag := endpoints.DAG()
	assert.Equal(t, 0, len(dag))

	// Names should return empty
	names := endpoints.Names()
	assert.Equal(t, 0, len(names))
}

func TestSpecSortStability(t *testing.T) {
	// Test that sorting is stable and consistent
	spec := `
name: "Test API"

endpoints:
  endpoint_a:
    request:
      url: "https://api.example.com/a"
    response:
      records:
        jmespath: "data[]"

  endpoint_b:
    request:
      url: "https://api.example.com/b"
    response:
      records:
        jmespath: "data[]"

  endpoint_c:
    request:
      url: "https://api.example.com/c"
    response:
      records:
        jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	// Sort multiple times and ensure consistency
	for i := 0; i < 10; i++ {
		var endpoints Endpoints
		for name, ep := range s.EndpointMap {
			ep.Name = name
			endpoints = append(endpoints, ep)
		}

		endpoints.Sort()
		names := endpoints.Names()

		expected := []string{"endpoint_a", "endpoint_b", "endpoint_c"}
		assert.Equal(t, expected, names, "Sort should be consistent on iteration %d", i)
	}
}

// Benchmark tests
func BenchmarkSort(b *testing.B) {
	// Create a spec with many endpoints
	spec := `
name: "Test API"
queues:
  - queue1
  - queue2
  - queue3

endpoints:
`

	// Add many endpoints
	for i := 0; i < 100; i++ {
		if i%10 == 0 {
			spec += fmt.Sprintf(`
  producer_%d:
    request:
      url: "https://api.example.com/producer_%d"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.queue%d"
`, i, i, (i/10)%3+1)
		} else if i%10 == 5 {
			spec += fmt.Sprintf(`
  consumer_%d:
    iterate:
      over: "queue.queue%d"
      into: "state.id"
    request:
      url: "https://api.example.com/consumer_%d"
    response:
      records:
        jmespath: "data[]"
`, i, (i/10)%3+1, i)
		} else {
			spec += fmt.Sprintf(`
  endpoint_%d:
    request:
      url: "https://api.example.com/endpoint_%d"
    response:
      records:
        jmespath: "data[]"
`, i, i)
		}
	}

	s, err := LoadSpec(spec)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var endpoints Endpoints
		for name, ep := range s.EndpointMap {
			ep.Name = name
			endpoints = append(endpoints, ep)
		}
		endpoints.Sort()
	}
}

func BenchmarkDAG(b *testing.B) {
	// Similar to BenchmarkSort but for DAG
	spec := `
name: "Test API"
queues:
  - queue1
  - queue2

endpoints:
`

	for i := 0; i < 50; i++ {
		if i%5 == 0 {
			spec += fmt.Sprintf(`
  producer_%d:
    request:
      url: "https://api.example.com/producer_%d"
    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "queue.queue%d"
`, i, i, (i/5)%2+1)
		} else if i%5 == 2 {
			spec += fmt.Sprintf(`
  consumer_%d:
    iterate:
      over: "queue.queue%d"
      into: "state.id"
    request:
      url: "https://api.example.com/consumer_%d"
    response:
      records:
        jmespath: "data[]"
`, i, (i/5)%2+1, i)
		} else {
			spec += fmt.Sprintf(`
  endpoint_%d:
    request:
      url: "https://api.example.com/endpoint_%d"
    response:
      records:
        jmespath: "data[]"
`, i, i)
		}
	}

	s, err := LoadSpec(spec)
	require.NoError(b, err)

	var endpoints Endpoints
	for name, ep := range s.EndpointMap {
		ep.Name = name
		endpoints = append(endpoints, ep)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = endpoints.DAG()
	}
}

// Dynamic Endpoints Tests

func TestDynamicEndpointsBasic(t *testing.T) {
	spec := `
name: "Test Dynamic API"
description: "API with dynamic endpoints"

defaults:
  state:
    base_url: "https://api.example.com"

dynamic_endpoints:
  - iterate: '["users", "orders", "products"]'
    into: "state.resource_type"

    endpoint:
      name: "{state.resource_type}"
      description: "Endpoint for {state.resource_type}"
      request:
        url: "{state.base_url}/{state.resource_type}"
      response:
        records:
          jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	assert.Equal(t, 1, len(s.DynamicEndpoints))

	dynEndpoint := s.DynamicEndpoints[0]
	assert.Equal(t, `["users", "orders", "products"]`, dynEndpoint.Iterate)
	assert.Equal(t, "state.resource_type", dynEndpoint.Into)
	assert.Equal(t, "{state.resource_type}", dynEndpoint.Endpoint.Name)
}

func TestDynamicEndpointsWithSetup(t *testing.T) {
	spec := `
name: "Test Dynamic API with Setup"
description: "API with setup sequence"

defaults:
  state:
    base_url: "https://api.example.com"

dynamic_endpoints:
  - setup:
      - request:
          url: "{state.base_url}/metadata/tables"
        response:
          processors:
            - expression: "response.json.tables[].name"
              output: "state.available_tables"
              aggregation: flatten

    iterate: "state.available_tables"
    into: "state.table_name"

    endpoint:
      name: "table_{state.table_name}"
      description: "Data from {state.table_name} table"
      request:
        url: "{state.base_url}/tables/{state.table_name}"
        parameters:
          limit: 1000
      response:
        records:
          jmespath: "rows[]"
          primary_key: ["id"]
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	assert.Equal(t, 1, len(s.DynamicEndpoints))

	dynEndpoint := s.DynamicEndpoints[0]
	assert.Equal(t, 1, len(dynEndpoint.Setup))
	assert.Equal(t, "state.available_tables", dynEndpoint.Iterate)
	assert.Equal(t, "state.table_name", dynEndpoint.Into)
	assert.Equal(t, "table_{state.table_name}", dynEndpoint.Endpoint.Name)
}

func TestDynamicEndpointsMultipleDefinitions(t *testing.T) {
	spec := `
name: "Test Multiple Dynamic Endpoints"

defaults:
  state:
    base_url: "https://api.example.com"

dynamic_endpoints:
  - iterate: '["us-east", "us-west", "eu-west"]'
    into: "state.region"
    endpoint:
      name: "sales_{state.region}"
      request:
        url: "{state.base_url}/regions/{state.region}/sales"
      response:
        records:
          jmespath: "sales[]"

  - iterate: '["daily", "weekly", "monthly"]'
    into: "state.period"
    endpoint:
      name: "report_{state.period}"
      request:
        url: "{state.base_url}/reports/{state.period}"
      response:
        records:
          jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	assert.Equal(t, 2, len(s.DynamicEndpoints))

	assert.Equal(t, "state.region", s.DynamicEndpoints[0].Into)
	assert.Equal(t, "state.period", s.DynamicEndpoints[1].Into)
}

func TestDynamicEndpointsWithStaticEndpoints(t *testing.T) {
	spec := `
name: "Hybrid API"

defaults:
  state:
    base_url: "https://api.example.com"

endpoints:
  health_check:
    request:
      url: "{state.base_url}/health"
    response:
      records:
        jmespath: "status"

  metadata:
    request:
      url: "{state.base_url}/metadata"
    response:
      records:
        jmespath: "data[]"

dynamic_endpoints:
  - iterate: '["users", "posts", "comments"]'
    into: "state.entity"
    endpoint:
      name: "{state.entity}"
      request:
        url: "{state.base_url}/{state.entity}"
      response:
        records:
          jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	assert.Equal(t, 2, len(s.EndpointMap)) // Static endpoints
	assert.Equal(t, 1, len(s.DynamicEndpoints))

	// Static endpoints should exist
	_, hasHealthCheck := s.EndpointMap["health_check"]
	_, hasMetadata := s.EndpointMap["metadata"]
	assert.True(t, hasHealthCheck)
	assert.True(t, hasMetadata)
}

func TestDynamicEndpointsWithComplexTemplates(t *testing.T) {
	spec := `
name: "Complex Template API"

defaults:
  state:
    base_url: "https://api.example.com"
    api_version: "v2"

dynamic_endpoints:
  - iterate: |
      [
        {"id": "123", "name": "acme"},
        {"id": "456", "name": "globex"}
      ]
    into: "state.current_org"

    endpoint:
      name: "org_{state.current_org.id}_events"
      description: "Events for organization: {state.current_org.name}"
      request:
        url: "{state.base_url}/{state.api_version}/organizations/{state.current_org.id}/events"
        parameters:
          from_date: "{state.start_date}"
          to_date: "{state.end_date}"
      response:
        records:
          jmespath: "events[]"
          primary_key: ["event_id"]
        processors:
          - expression: "state.current_org.id"
            output: "record.organization_id"
          - expression: "state.current_org.name"
            output: "record.organization_name"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	dynEndpoint := s.DynamicEndpoints[0]

	assert.Equal(t, "state.current_org", dynEndpoint.Into)
	assert.Equal(t, "org_{state.current_org.id}_events", dynEndpoint.Endpoint.Name)
	assert.Equal(t, "{state.base_url}/{state.api_version}/organizations/{state.current_org.id}/events", dynEndpoint.Endpoint.Request.URL)
	assert.Equal(t, 2, len(dynEndpoint.Endpoint.Response.Processors))
}

func TestDynamicEndpointsEmptySpec(t *testing.T) {
	spec := `
name: "No Dynamic API"

endpoints:
  test:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.False(t, s.IsDynamic())
	assert.Equal(t, 0, len(s.DynamicEndpoints))
}

func TestDynamicEndpointsWithStateVariables(t *testing.T) {
	spec := `
name: "State Variables API"

defaults:
  state:
    base_url: "https://api.example.com"
    regions:
      - code: "us-east"
        name: "US East Coast"
      - code: "us-west"
        name: "US West Coast"
      - code: "eu-west"
        name: "Europe West"

dynamic_endpoints:
  - iterate: "state.regions"
    into: "state.region"

    endpoint:
      name: "sales_{state.region.code}"
      description: "Sales data for {state.region.name}"
      request:
        url: "{state.base_url}/regions/{state.region.code}/sales"
      response:
        records:
          jmespath: "sales[]"
      processors:
        - expression: "state.region.code"
          output: "record.region_code"
        - expression: "state.region.name"
          output: "record.region_name"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	dynEndpoint := s.DynamicEndpoints[0]

	assert.Equal(t, "state.regions", dynEndpoint.Iterate)
	assert.Equal(t, "state.region", dynEndpoint.Into)
	assert.Equal(t, "sales_{state.region.code}", dynEndpoint.Endpoint.Name)
}

func TestDynamicEndpointsJMESPathIterate(t *testing.T) {
	spec := `
name: "JMESPath Iterate API"

defaults:
  state:
    base_url: "https://api.example.com"
    config:
      resources:
        - name: "users"
          path: "/v1/users"
        - name: "orders"
          path: "/v1/orders"

dynamic_endpoints:
  - iterate: "state.config.resources[].name"
    into: "state.resource_name"

    endpoint:
      name: "{state.resource_name}"
      request:
        url: "{state.base_url}{state.resource_name}"
      response:
        records:
          jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	dynEndpoint := s.DynamicEndpoints[0]

	assert.Equal(t, "state.config.resources[].name", dynEndpoint.Iterate)
	assert.Equal(t, "state.resource_name", dynEndpoint.Into)
}

func TestDynamicEndpointsWithPagination(t *testing.T) {
	spec := `
name: "Dynamic Pagination API"

defaults:
  state:
    base_url: "https://api.example.com"

dynamic_endpoints:
  - iterate: '["customers", "invoices"]'
    into: "state.entity_type"

    endpoint:
      name: "{state.entity_type}"
      request:
        url: "{state.base_url}/{state.entity_type}"
        parameters:
          page: 1
      pagination:
        next_state:
          page: "{state.page + 1}"
        stop_condition: "response.json.has_more == false"
      response:
        records:
          jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	dynEndpoint := s.DynamicEndpoints[0]

	assert.NotNil(t, dynEndpoint.Endpoint.Pagination.NextState)
	assert.Equal(t, "response.json.has_more == false", dynEndpoint.Endpoint.Pagination.StopCondition)
}

func TestDynamicEndpointsWithSync(t *testing.T) {
	spec := `
name: "Dynamic Sync API"

dynamic_endpoints:
  - iterate: '["table_a", "table_b"]'
    into: "state.table_name"

    endpoint:
      name: "{state.table_name}"
      sync:
        - last_updated_at
      request:
        url: "https://api.example.com/tables/{state.table_name}"
      response:
        records:
          jmespath: "rows[]"
        processors:
          - expression: "max(records[].updated_at)"
            output: "state.last_updated_at"
            aggregation: last
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	assert.True(t, s.IsDynamic())
	dynEndpoint := s.DynamicEndpoints[0]

	assert.Equal(t, 1, len(dynEndpoint.Endpoint.Sync))
	assert.Equal(t, "last_updated_at", dynEndpoint.Endpoint.Sync[0])
}

func TestIsDynamicMethod(t *testing.T) {
	tests := []struct {
		name     string
		spec     string
		expected bool
	}{
		{
			name: "has dynamic endpoints",
			spec: `
name: "Dynamic API"
dynamic_endpoints:
  - iterate: '["a", "b"]'
    into: "state.item"
    endpoint:
      name: "{state.item}"
      request:
        url: "https://api.example.com/{state.item}"
      response:
        records:
          jmespath: "data[]"
`,
			expected: true,
		},
		{
			name: "no dynamic endpoints",
			spec: `
name: "Static API"
endpoints:
  test:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
`,
			expected: false,
		},
		{
			name: "empty dynamic endpoints array",
			spec: `
name: "Empty Dynamic API"
dynamic_endpoints: []
endpoints:
  test:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
`,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := LoadSpec(tt.spec)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, s.IsDynamic())
		})
	}
}

func TestResponseUnmarshalYAML(t *testing.T) {
	// Test the Response UnmarshalYAML directly
	yamlStr := `
format: json
records:
  jmespath: "data[]"
+rules:
  - action: break
    condition: "response.status == 403"
rules:
  - action: retry
    condition: "response.status == 429"
rules+:
  - action: continue
    condition: "response.status == 404"
+processors:
  - expression: "log(record)"
processors:
  - expression: "record.id"
    output: "state.last_id"
processors+:
  - expression: "record.name"
    output: "state.last_name"
`
	var r Response
	err := yaml.Unmarshal([]byte(yamlStr), &r)
	require.NoError(t, err)

	t.Logf("PrependRules: %+v", r.PrependRules)
	t.Logf("Rules: %+v", r.Rules)
	t.Logf("AppendRules: %+v", r.AppendRules)
	t.Logf("PrependProcessors: %+v", r.PrependProcessors)
	t.Logf("Processors: %+v", r.Processors)
	t.Logf("AppendProcessors: %+v", r.AppendProcessors)

	assert.Len(t, r.PrependRules, 1, "PrependRules should have 1 rule")
	assert.Len(t, r.Rules, 1, "Rules should have 1 rule")
	assert.Len(t, r.AppendRules, 1, "AppendRules should have 1 rule")
	assert.Len(t, r.PrependProcessors, 1, "PrependProcessors should have 1 processor")
	assert.Len(t, r.Processors, 1, "Processors should have 1 processor")
	assert.Len(t, r.AppendProcessors, 1, "AppendProcessors should have 1 processor")

	assert.Equal(t, RuleTypeBreak, r.PrependRules[0].Action)
	assert.Equal(t, RuleTypeRetry, r.Rules[0].Action)
	assert.Equal(t, RuleTypeContinue, r.AppendRules[0].Action)
}

func TestEndpointResponseUnmarshal(t *testing.T) {
	// Test that Response modifier fields are preserved when unmarshalling an Endpoint
	yamlStr := `
name: test_endpoint
request:
  url: "https://api.example.com/test"
response:
  records:
    jmespath: "data[]"
  +rules:
    - action: break
      condition: "response.status == 403"
  rules:
    - action: retry
      condition: "response.status == 429"
`
	var ep Endpoint
	err := yaml.Unmarshal([]byte(yamlStr), &ep)
	require.NoError(t, err)

	t.Logf("Endpoint.Response.PrependRules: %+v", ep.Response.PrependRules)
	t.Logf("Endpoint.Response.Rules: %+v", ep.Response.Rules)

	assert.Len(t, ep.Response.PrependRules, 1, "PrependRules should have 1 rule")
	assert.Equal(t, RuleTypeBreak, ep.Response.PrependRules[0].Action)
}

func TestSpecResponseModifierSyntax(t *testing.T) {
	t.Run("prepend rules with +rules", func(t *testing.T) {
		specYaml := `
name: "Test API"

defaults:
  response:
    rules:
      - action: retry
        condition: "response.status == 429"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
      +rules:
        - action: break
          condition: "response.status == 403"
`

		s, err := LoadSpec(specYaml)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// The prepend rule should come first (before default rules)
		// Order: +rules -> defaults -> hardcoded
		assert.GreaterOrEqual(t, len(ep.Response.Rules), 3) // at least: prepend + default + hardcoded
		assert.Equal(t, RuleTypeBreak, ep.Response.Rules[0].Action)
		assert.Equal(t, "response.status == 403", ep.Response.Rules[0].Condition)
	})

	t.Run("append rules with rules+", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  response:
    rules:
      - action: retry
        condition: "response.status == 429"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
      rules+:
        - action: continue
          condition: "response.status == 404"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: defaults -> rules+ -> hardcoded
		// The first rule should be the default retry rule
		assert.Equal(t, RuleTypeRetry, ep.Response.Rules[0].Action)
		assert.Equal(t, "response.status == 429", ep.Response.Rules[0].Condition)

		// The second rule should be the appended continue rule
		assert.Equal(t, RuleTypeContinue, ep.Response.Rules[1].Action)
		assert.Equal(t, "response.status == 404", ep.Response.Rules[1].Condition)
	})

	t.Run("both +rules and rules+ together", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  response:
    rules:
      - action: retry
        condition: "response.status == 429"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
      +rules:
        - action: break
          condition: "response.status == 403"
      rules+:
        - action: continue
          condition: "response.status == 404"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: +rules -> defaults -> rules+ -> hardcoded
		assert.Equal(t, RuleTypeBreak, ep.Response.Rules[0].Action)                        // prepend
		assert.Equal(t, RuleTypeRetry, ep.Response.Rules[1].Action)                        // default
		assert.Equal(t, RuleTypeContinue, ep.Response.Rules[2].Action)                     // append
		assert.Equal(t, RuleTypeRetry, ep.Response.Rules[len(ep.Response.Rules)-3].Action) // hardcoded retry
		assert.Equal(t, RuleTypeFail, ep.Response.Rules[len(ep.Response.Rules)-2].Action)  // hardcoded JSON-RPC fail
		assert.Equal(t, RuleTypeFail, ep.Response.Rules[len(ep.Response.Rules)-1].Action)  // hardcoded status fail
	})

	t.Run("combine explicit rules with +rules and rules+", func(t *testing.T) {
		spec := `
name: "Test API"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
      +rules:
        - action: break
          condition: "response.status == 403"
      rules:
        - action: stop
          condition: "response.status == 410"
      rules+:
        - action: continue
          condition: "response.status == 404"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: +rules -> rules -> rules+ -> hardcoded
		assert.Equal(t, RuleTypeBreak, ep.Response.Rules[0].Action)    // prepend
		assert.Equal(t, RuleTypeStop, ep.Response.Rules[1].Action)     // explicit
		assert.Equal(t, RuleTypeContinue, ep.Response.Rules[2].Action) // append
	})

	t.Run("prepend processors with +processors", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  response:
    processors:
      - expression: "record.id"
        output: "state.last_id"
        aggregation: "last"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
      +processors:
        - expression: "log(record)"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// The prepend processor should come first
		assert.GreaterOrEqual(t, len(ep.Response.Processors), 2)
		assert.Equal(t, "log(record)", ep.Response.Processors[0].Expression)
		assert.Equal(t, "record.id", ep.Response.Processors[1].Expression)
	})

	t.Run("append processors with processors+", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  response:
    processors:
      - expression: "record.id"
        output: "state.last_id"
        aggregation: "last"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
      processors+:
        - expression: "log(record)"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// The first processor should be the default
		assert.Equal(t, "record.id", ep.Response.Processors[0].Expression)
		// The second processor should be the appended one
		assert.Equal(t, "log(record)", ep.Response.Processors[1].Expression)
	})

	t.Run("no modifier syntax - defaults work normally", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  response:
    rules:
      - action: retry
        condition: "response.status == 429"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Default retry rule should be first (before hardcoded rules)
		assert.Equal(t, RuleTypeRetry, ep.Response.Rules[0].Action)
		assert.Equal(t, "response.status == 429", ep.Response.Rules[0].Condition)
	})

	t.Run("explicit rules override defaults", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  response:
    rules:
      - action: retry
        condition: "response.status == 429"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
      rules:
        - action: stop
          condition: "response.status == 410"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Explicit rule should be first (default is overridden)
		assert.Equal(t, RuleTypeStop, ep.Response.Rules[0].Action)
		assert.Equal(t, "response.status == 410", ep.Response.Rules[0].Condition)
	})
}

func TestSpecEndpointSetupTeardownModifierSyntax(t *testing.T) {
	t.Run("prepend setup with +setup", func(t *testing.T) {
		specYaml := `
name: "Test API"

defaults:
  setup:
    - request:
        url: "https://api.example.com/auth/refresh"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
    +setup:
      - request:
          url: "https://api.example.com/pre-check"
`

		s, err := LoadSpec(specYaml)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// The prepend setup should come first (before default setup)
		// Order: +setup -> defaults
		assert.Len(t, ep.Setup, 2)
		assert.Equal(t, "https://api.example.com/pre-check", ep.Setup[0].Request.URL)
		assert.Equal(t, "https://api.example.com/auth/refresh", ep.Setup[1].Request.URL)
	})

	t.Run("append setup with setup+", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  setup:
    - request:
        url: "https://api.example.com/auth/refresh"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
    setup+:
      - request:
          url: "https://api.example.com/post-init"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: defaults -> setup+
		assert.Len(t, ep.Setup, 2)
		assert.Equal(t, "https://api.example.com/auth/refresh", ep.Setup[0].Request.URL)
		assert.Equal(t, "https://api.example.com/post-init", ep.Setup[1].Request.URL)
	})

	t.Run("both +setup and setup+ together", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  setup:
    - request:
        url: "https://api.example.com/auth/refresh"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
    +setup:
      - request:
          url: "https://api.example.com/pre-check"
    setup+:
      - request:
          url: "https://api.example.com/post-init"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: +setup -> defaults -> setup+
		assert.Len(t, ep.Setup, 3)
		assert.Equal(t, "https://api.example.com/pre-check", ep.Setup[0].Request.URL)
		assert.Equal(t, "https://api.example.com/auth/refresh", ep.Setup[1].Request.URL)
		assert.Equal(t, "https://api.example.com/post-init", ep.Setup[2].Request.URL)
	})

	t.Run("combine explicit setup with +setup and setup+", func(t *testing.T) {
		spec := `
name: "Test API"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
    +setup:
      - request:
          url: "https://api.example.com/pre-check"
    setup:
      - request:
          url: "https://api.example.com/explicit-setup"
    setup+:
      - request:
          url: "https://api.example.com/post-init"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: +setup -> setup -> setup+
		assert.Len(t, ep.Setup, 3)
		assert.Equal(t, "https://api.example.com/pre-check", ep.Setup[0].Request.URL)
		assert.Equal(t, "https://api.example.com/explicit-setup", ep.Setup[1].Request.URL)
		assert.Equal(t, "https://api.example.com/post-init", ep.Setup[2].Request.URL)
	})

	t.Run("prepend teardown with +teardown", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  teardown:
    - request:
        url: "https://api.example.com/cleanup"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
    +teardown:
      - request:
          url: "https://api.example.com/pre-cleanup"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: +teardown -> defaults
		assert.Len(t, ep.Teardown, 2)
		assert.Equal(t, "https://api.example.com/pre-cleanup", ep.Teardown[0].Request.URL)
		assert.Equal(t, "https://api.example.com/cleanup", ep.Teardown[1].Request.URL)
	})

	t.Run("append teardown with teardown+", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  teardown:
    - request:
        url: "https://api.example.com/cleanup"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
    teardown+:
      - request:
          url: "https://api.example.com/post-cleanup"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: defaults -> teardown+
		assert.Len(t, ep.Teardown, 2)
		assert.Equal(t, "https://api.example.com/cleanup", ep.Teardown[0].Request.URL)
		assert.Equal(t, "https://api.example.com/post-cleanup", ep.Teardown[1].Request.URL)
	})

	t.Run("both +teardown and teardown+ together", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  teardown:
    - request:
        url: "https://api.example.com/cleanup"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
    +teardown:
      - request:
          url: "https://api.example.com/pre-cleanup"
    teardown+:
      - request:
          url: "https://api.example.com/post-cleanup"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Order: +teardown -> defaults -> teardown+
		assert.Len(t, ep.Teardown, 3)
		assert.Equal(t, "https://api.example.com/pre-cleanup", ep.Teardown[0].Request.URL)
		assert.Equal(t, "https://api.example.com/cleanup", ep.Teardown[1].Request.URL)
		assert.Equal(t, "https://api.example.com/post-cleanup", ep.Teardown[2].Request.URL)
	})

	t.Run("no modifier syntax - defaults work normally", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  setup:
    - request:
        url: "https://api.example.com/auth/refresh"
  teardown:
    - request:
        url: "https://api.example.com/cleanup"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Defaults should be used
		assert.Len(t, ep.Setup, 1)
		assert.Equal(t, "https://api.example.com/auth/refresh", ep.Setup[0].Request.URL)
		assert.Len(t, ep.Teardown, 1)
		assert.Equal(t, "https://api.example.com/cleanup", ep.Teardown[0].Request.URL)
	})

	t.Run("explicit setup/teardown override defaults", func(t *testing.T) {
		spec := `
name: "Test API"

defaults:
  setup:
    - request:
        url: "https://api.example.com/auth/refresh"
  teardown:
    - request:
        url: "https://api.example.com/cleanup"

endpoints:
  test_endpoint:
    request:
      url: "https://api.example.com/test"
    response:
      records:
        jmespath: "data[]"
    setup:
      - request:
          url: "https://api.example.com/custom-setup"
    teardown:
      - request:
          url: "https://api.example.com/custom-teardown"
`

		s, err := LoadSpec(spec)
		require.NoError(t, err)

		ep := s.EndpointMap["test_endpoint"]

		// Explicit setup/teardown should override defaults
		assert.Len(t, ep.Setup, 1)
		assert.Equal(t, "https://api.example.com/custom-setup", ep.Setup[0].Request.URL)
		assert.Len(t, ep.Teardown, 1)
		assert.Equal(t, "https://api.example.com/custom-teardown", ep.Teardown[0].Request.URL)
	})
}

// TestSpecValidateQueueProducers_MissingProducer ensures that a spec consuming
// a queue with no producer endpoint fails validation at LoadSpec time.
func TestSpecValidateQueueProducers_MissingProducer(t *testing.T) {
	spec := `
name: "Test API"
endpoints:
  consumer:
    iterate:
      over: "queue.does_not_exist"
      into: "state.id"
    request:
      url: "https://api.example.com/item"
    response:
      records:
        jmespath: "[]"
`
	_, err := LoadSpec(spec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does_not_exist")
	assert.Contains(t, err.Error(), "no endpoint produces")
}

// TestSpecQueueOnlyEndpointSorts ensures that an endpoint with queue_only: true
// is sorted before its consumer based on the producer/consumer graph and that
// no top-level `queues:` declaration is needed.
func TestSpecQueueOnlyEndpointSorts(t *testing.T) {
	spec := `
name: "Test API"
endpoints:
  consumer:
    iterate:
      over: "queue.ids"
      into: "state.id"
    request:
      url: "https://api.example.com/item/{state.id}"
    response:
      records:
        jmespath: "[]"

  producer:
    queue_only: true
    request:
      url: "https://api.example.com/ids"
    response:
      records:
        jmespath: "[]"
      processors:
        - expression: "record.id"
          output: "queue.ids"
`
	s, err := LoadSpec(spec)
	require.NoError(t, err)

	// queue_only field round-trips through compilation
	prod := s.EndpointMap["producer"]
	assert.True(t, prod.QueueOnly, "producer endpoint should have queue_only=true")

	// Build sortable endpoint list
	eps := Endpoints{}
	for _, name := range s.endpointsOrdered {
		eps = append(eps, s.EndpointMap[name])
	}
	eps.Sort()

	names := make([]string, len(eps))
	for i, ep := range eps {
		names[i] = ep.Name
	}
	assert.Equal(t, []string{"producer", "consumer"}, names, "producer should sort before consumer")

	// HasUpstreams should find the producer as the consumer's upstream
	upstreams := eps.HasUpstreams("consumer")
	assert.Equal(t, []string{"producer"}, upstreams)
}

// TestSpecDeprecatedQueuesFieldStillLoads ensures that specs declaring the
// legacy top-level `queues:` field still load cleanly (the field is now a
// no-op accompanied by a deprecation warning, not an error).
func TestSpecDeprecatedQueuesFieldStillLoads(t *testing.T) {
	spec := `
name: "Test API"
queues:
  - ids

endpoints:
  consumer:
    iterate:
      over: "queue.ids"
      into: "state.id"
    request:
      url: "https://api.example.com/item/{state.id}"
    response:
      records:
        jmespath: "[]"

  producer:
    request:
      url: "https://api.example.com/ids"
    response:
      records:
        jmespath: "[]"
      processors:
        - expression: "record.id"
          output: "queue.ids"
`
	s, err := LoadSpec(spec)
	require.NoError(t, err)
	assert.Equal(t, []string{"ids"}, s.Queues, "Queues field should still be populated for backwards compat")
}

// TestDynamicEndpointsInlineYAMLArray ensures that DynamicEndpoint.Iterate
// accepts a raw YAML array (incl. nested objects), not only a string.
func TestDynamicEndpointsInlineYAMLArray(t *testing.T) {
	spec := `
name: "Inline YAML Iterate API"
defaults:
  state:
    base_url: "https://api.example.com"

dynamic_endpoints:
  - iterate:
      - model: 'res.partner'
        fields: [id, name, email]
      - model: 'product.product'
        fields: [id, name, default_code]
    into: 'state.endpoint'
    endpoint:
      name: '{snake(state.endpoint.model)}'
      request:
        url: "{state.base_url}/{state.endpoint.model}"
      response:
        records:
          jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	require.True(t, s.IsDynamic())
	require.Equal(t, 1, len(s.DynamicEndpoints))

	dynEndpoint := s.DynamicEndpoints[0]
	assert.Equal(t, "state.endpoint", dynEndpoint.Into)

	// YAML decoders typically produce []any with map[interface{}]interface{} or
	// map[string]any depending on version; normalise via JSON round-trip to
	// match how RenderDynamicEndpoints consumes the field.
	var iterList []map[string]any
	require.NoError(t, g.JSONConvert(dynEndpoint.Iterate, &iterList))
	require.Len(t, iterList, 2)

	assert.Equal(t, "res.partner", iterList[0]["model"])
	assert.ElementsMatch(t, []any{"id", "name", "email"}, iterList[0]["fields"])
	assert.Equal(t, "product.product", iterList[1]["model"])
}

// TestDynamicEndpointsInlineSingleObject ensures that a single inline
// object (not wrapped in a list) is treated as a one-element iteration.
func TestDynamicEndpointsInlineSingleObject(t *testing.T) {
	spec := `
name: "Inline Single Object Iterate API"
defaults:
  state:
    base_url: "https://api.example.com"

dynamic_endpoints:
  - iterate:
      model: 'res.partner'
      fields: [id, name]
    into: 'state.endpoint'
    endpoint:
      name: '{snake(state.endpoint.model)}'
      request:
        url: "{state.base_url}/{state.endpoint.model}"
      response:
        records:
          jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	dynEndpoint := s.DynamicEndpoints[0]
	var asMap map[string]any
	require.NoError(t, g.JSONConvert(dynEndpoint.Iterate, &asMap))
	assert.Equal(t, "res.partner", asMap["model"])
}

// TestDynamicEndpointsStringIterateBackwardsCompat ensures that the existing
// string forms (JSON literal and JMESPath expression) still parse into a string,
// keeping older specs working unchanged after Iterate became `any`.
func TestDynamicEndpointsStringIterateBackwardsCompat(t *testing.T) {
	for _, tc := range []struct{ name, iterate string }{
		{"json_literal", `["users", "orders"]`},
		{"jmespath_expr", `state.config.resources[].name`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := `
name: "Backwards Compat Iterate API"
defaults:
  state:
    base_url: "https://api.example.com"

dynamic_endpoints:
  - iterate: '` + tc.iterate + `'
    into: 'state.resource'
    endpoint:
      name: '{state.resource}'
      request:
        url: "{state.base_url}/{state.resource}"
      response:
        records:
          jmespath: "data[]"
`
			s, err := LoadSpec(spec)
			require.NoError(t, err)
			assert.Equal(t, tc.iterate, s.DynamicEndpoints[0].Iterate, "string iterate should round-trip unchanged")
		})
	}
}

// Default rules (retry on 429/5xx, fail on >= 400) historically applied only to
// endpoint, setup, and teardown responses. Auth sequence call responses were
// skipped, so an OAuth token endpoint returning 401 {"error":"invalid_client"}
// fell through to processors and died with cryptic "object has no member
// 'access_token'" instead of failing loudly on the 401.
func TestSpecAuthSequenceGetsDefaultRules(t *testing.T) {
	spec := `
name: "Auth Sequence Default Rules"

defaults:
  state:
    base_url: "https://api.example.com"
  request:
    headers:
      Authorization: "Bearer {state.token}"

authentication:
  type: "sequence"
  expires: 270
  sequence:
    - request:
        url: "https://auth.example.com/token"
        method: POST
        headers:
          Content-Type: "application/x-www-form-urlencoded"
        payload: "grant_type=client_credentials&client_id=x&client_secret=y"
      response:
        processors:
          - expression: "response.json.access_token"
            output: "state.token"
            aggregation: last

endpoints:
  things:
    request:
      url: "{state.base_url}/things"
    response:
      records:
        jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	require.NotNil(t, s.Authentication)
	require.Equal(t, AuthTypeSequence, s.Authentication.Type())

	rawSeq, ok := s.Authentication["sequence"]
	require.True(t, ok, "spec authentication.sequence must be present after compile")
	authSeq := parseSequenceFromInterface(rawSeq)
	require.Len(t, authSeq, 1)

	rules := authSeq[0].Response.Rules
	require.GreaterOrEqual(t, len(rules), 3, "auth sequence response should have at least 3 default rules appended")
	assert.Equal(t, "Default Retry Rule", rules[len(rules)-3].Message)
	assert.Equal(t, "JSON-RPC error response (HTTP 200 with error envelope)", rules[len(rules)-2].Message)
	assert.Equal(t, "Default Fail Rule", rules[len(rules)-1].Message)
	assert.Equal(t, "response.status >= 400", rules[len(rules)-1].Condition)
}

// Compiling the same spec twice (or two endpoints sharing spec.Defaults) must
// not stack the default rules — checkResponse should be idempotent.
func TestSpecDefaultRulesIdempotent(t *testing.T) {
	spec := `
name: "Idempotent Default Rules"

defaults:
  state:
    base_url: "https://api.example.com"

authentication:
  type: "sequence"
  sequence:
    - request:
        url: "https://auth.example.com/token"
        method: POST
      response:
        processors:
          - expression: "response.json.access_token"
            output: "state.token"
            aggregation: last

endpoints:
  a:
    request:
      url: "{state.base_url}/a"
    response:
      records:
        jmespath: "data[]"
  b:
    request:
      url: "{state.base_url}/b"
    response:
      records:
        jmespath: "data[]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)

	// Recompile both endpoints — simulates being run more than once (e.g. via
	// ReadDataflow -> CompileSpecEndpoints / compileSpecEndpoint paths).
	for _, name := range []string{"a", "b"} {
		ep := s.EndpointMap[name]
		require.NoError(t, compileSpecEndpoint(&ep, s))
		s.EndpointMap[name] = ep
	}

	for _, name := range []string{"a", "b"} {
		ep := s.EndpointMap[name]
		// Endpoint response should have exactly 3 default rules (retry, jsonrpc, fail).
		assert.Equal(t, 3, len(ep.Response.Rules), "endpoint %s should have exactly 3 default rules, got %d", name, len(ep.Response.Rules))
	}

	// Spec-level auth sequence should also have exactly 3 default rules
	// despite being processed multiple times via compileSpecEndpoint above.
	require.NotNil(t, s.Authentication)
	authSeq := parseSequenceFromInterface(s.Authentication["sequence"])
	require.Len(t, authSeq, 1)
	assert.Equal(t, 3, len(authSeq[0].Response.Rules), "spec auth sequence should have exactly 3 default rules across multiple compiles, got %d", len(authSeq[0].Response.Rules))
}

// TestDynamicEndpointInnerIterateSurvivesRender reproduces the report that a
// dynamic endpoint with an inner `iterate.over: chunk(...)` loop loses that
// expression: `state.batch_ids` is never populated and `{state.batch_ids}` is
// passed through literally in the request payload.
//
// renderEndpointTemplate deep-copies the endpoint template via a JSON round-trip
// (g.Marshal -> g.Unmarshal) and builds an `originalMap` from that same JSON.
// CompileSpecEndpoint then uses `originalMap` to decide which fields were
// explicitly set: a field absent from originalMap is overwritten by the spec
// default. The root cause was a wrong JSON tag on the Iterate.Over field
// (`json:"iterate"` instead of `json:"over"`): the JSON had key `iterate`
// instead of `over` inside the `iterate` object, so the `iterate.over` presence
// check failed and CompileSpecEndpoint wiped Over with the (empty) default.
// This test asserts the inner iterate.over expression survives both render and
// compile of a dynamic endpoint.
func TestDynamicEndpointInnerIterateSurvivesRender(t *testing.T) {
	spec := `
name: "Dynamic Inner Iterate API"
defaults:
  state:
    base_url: "https://api.example.com"

dynamic_endpoints:
  - iterate:
      - model: res.partner
    into: 'state.endpoint'
    endpoint:
      name: '{snake(state.endpoint.model)}'
      iterate:
        over: 'chunk(state.item_ids, 7)'
        into: 'state.batch_ids'
        concurrency: 1
      request:
        method: POST
        url: "{state.base_url}/{state.endpoint.model}"
        payload:
          ids: '{state.batch_ids}'
      response:
        records:
          jmespath: "result[*]"
`

	s, err := LoadSpec(spec)
	require.NoError(t, err)
	require.True(t, s.IsDynamic())
	require.Equal(t, 1, len(s.DynamicEndpoints))

	dynEndpoint := s.DynamicEndpoints[0]

	// Sanity-check the inner iterate.over survives loading the spec itself.
	require.NotNil(t, dynEndpoint.Endpoint.Iterate.Over)
	assert.Equal(t, "chunk(state.item_ids, 7)", cast.ToString(dynEndpoint.Endpoint.Iterate.Over),
		"inner iterate.over should be preserved when loading the spec")

	// renderEndpointTemplate is what RenderDynamicEndpoints calls per iteration;
	// it deep-copies the endpoint template via a JSON round-trip.
	ac, err := NewAPIConnection(context.Background(), s, map[string]any{})
	require.NoError(t, err)

	rendered, err := ac.renderEndpointTemplate(dynEndpoint, map[string]any{"model": "res.partner"}, nil)
	require.NoError(t, err)

	require.NotNil(t, rendered.Iterate.Over,
		"inner iterate.over must not be lost in renderEndpointTemplate's JSON round-trip")
	assert.Equal(t, "chunk(state.item_ids, 7)", cast.ToString(rendered.Iterate.Over),
		"rendered dynamic endpoint must keep the inner chunk() iterate expression")

	// CompileSpecEndpoint runs next in the real pipeline; it uses originalMap
	// (built from the same JSON) to detect explicitly-set fields. With the wrong
	// JSON tag this is where Over got reset to the empty default.
	require.NoError(t, CompileSpecEndpoint(rendered, s))

	require.NotNil(t, rendered.Iterate.Over,
		"inner iterate.over must survive CompileSpecEndpoint (not be overwritten by default)")
	assert.Equal(t, "chunk(state.item_ids, 7)", cast.ToString(rendered.Iterate.Over),
		"compiled dynamic endpoint must keep the inner chunk() iterate expression")
	assert.Equal(t, "state.batch_ids", rendered.Iterate.Into,
		"compiled dynamic endpoint must keep the inner iterate.into target")
}
