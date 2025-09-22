package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			actual := endpoints.Names()

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
			name: "endpoint depends on non-existent queue",
			spec: `
name: "Test API"
queues:
  - missing_queue

endpoints:
  consumer:
    iterate:
      over: "queue.missing_queue"
      into: "state.id"
    request:
      url: "https://api.example.com/consumer"
    response:
      records:
        jmespath: "data[]"
`,
			endpoint: "consumer",
			expected: []string{}, // no producers found
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
	names := endpoints.Names()

	consumerIdx := -1
	producerAIdx := -1
	producerBIdx := -1

	for i, name := range names {
		switch name {
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
