package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestAPIConnectionRender(t *testing.T) {
	// Test cases for the render function
	tests := []struct {
		name        string
		input       any
		expected    any
		setup       func(*APIConnection)
		epState     map[string]any
		expectError bool
	}{
		{
			name:     "simple_state_variable",
			input:    "Hello, {state.name}!",
			expected: "Hello, World!",
			setup: func(ac *APIConnection) {
				ac.State.State["name"] = "World"
			},
		},
		{
			name:     "environment_variable",
			input:    "Environment: {env.TEST_VAR}",
			expected: "Environment: test_value",
			setup: func(ac *APIConnection) {
				ac.State.Env["TEST_VAR"] = "test_value"
			},
		},
		{
			name:     "secret_variable",
			input:    "API Key: {secrets.API_KEY}",
			expected: "API Key: secret123",
			setup: func(ac *APIConnection) {
				ac.State.Secrets["API_KEY"] = "secret123"
			},
		},
		{
			name:     "auth_variable",
			input:    "Token: {auth.token}",
			expected: "Token: my_token",
			setup: func(ac *APIConnection) {
				ac.State.Auth.Token = "my_token"
			},
		},
		{
			name:     "multiple_variables",
			input:    "From {env.ORIGIN} to {state.destination}",
			expected: "From Earth to Mars",
			setup: func(ac *APIConnection) {
				ac.State.Env["ORIGIN"] = "Earth"
				ac.State.State["destination"] = "Mars"
			},
		},
		{
			name:     "endpoint_state_override",
			input:    "Value: {state.counter}",
			expected: "Value: 10",
			setup: func(ac *APIConnection) {
				ac.State.State["counter"] = 5
			},
			epState: map[string]any{"counter": 10},
		},
		{
			name:     "endpoint_state_integer",
			input:    "{ state.counter }",
			expected: 5.0,
			setup: func(ac *APIConnection) {
				ac.State.State["counter"] = 5
			},
		},
		{
			name:     "endpoint_state_map",
			input:    g.M("counter", "{ state.counter }"),
			expected: g.M("counter", 5.0),
			setup: func(ac *APIConnection) {
			},
			epState: g.M("counter", 5),
		},
		{
			name:     "endpoint_state_map_nested",
			input:    g.M("pagination", g.M("limit", "{ state.limit }")),
			expected: g.M("pagination", g.M("limit", 5.0)),
			setup: func(ac *APIConnection) {
				ac.State.State["limit"] = 5
			},
		},
		{
			name:     "object_serialization",
			input:    "User: {state.user}",
			expected: "User: {\"age\":30,\"name\":\"Alice\"}",
			setup: func(ac *APIConnection) {
				ac.State.State["user"] = map[string]any{"name": "Alice", "age": 30}
			},
		},
		// {
		// 	name:     "json_response",
		// 	input:    "{response.json.pagination.id}",
		// 	expected: "abc123",
		// 	setup: func(ac *APIConnection) {
		// 	},
		// 	epState: g.M("response", g.M(
		// 		"json", g.M(
		// 			"pagination", g.M("id", "abc123"),
		// 		),
		// 	)),
		// },
		{
			name:     "array_serialization",
			input:    "Items: {state.items}",
			expected: "Items: [\"one\",\"two\",\"three\"]",
			setup: func(ac *APIConnection) {
				ac.State.State["items"] = []string{"one", "two", "three"}
			},
		},
		{
			name:     "if_function",
			input:    "Result: {if(true, 3, 0)}",
			expected: "Result: 3",
		},
		{
			name:     "now_function",
			input:    "Current time: {now()}",
			expected: "Current time: ",
		},
		{
			name:     "non_existent_var",
			input:    `Missing: { log("state.missing = " + state.missing) }`,
			expected: "Missing: state.missing = nil",
		},
		{
			name:     "jmespath_lookup",
			input:    "Name: { state.nested.name }",
			expected: "Name: John",
			setup: func(ac *APIConnection) {
				ac.State.State["nested"] = map[string]any{"name": "John", "age": 42}
			},
		},
		// {
		// 	name:        "invalid_function",
		// 	input:       "Invalid: {invalid_function()}",
		// 	expected:    "",
		// 	expectError: true,
		// },
		{
			name:        "coalesce",
			input:       `{ coalesce(env.START_DATE, state.start_time, "2025-01-01") }`,
			expected:    "2025-01-01",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test API connection with our custom values
			ac := &APIConnection{
				Context: g.NewContext(context.Background()),
				State: &APIState{
					Env:     make(map[string]string),
					State:   make(map[string]any),
					Secrets: make(map[string]any),
					Auth:    APIStateAuth{},
				},
				Spec:      Spec{},
				evaluator: iop.NewEvaluator(g.ArrStr("env", "state", "secrets", "auth", "response", "request", "sync")),
			}

			// Configure the test state
			if tt.setup != nil {
				tt.setup(ac)
			}

			// Process using our mock implementation
			var result any
			var err error

			// Use the mock implementation that correctly handles state
			if tt.epState != nil {
				result, err = ac.renderAny(tt.input, g.M("state", tt.epState))
			} else {
				result, err = ac.renderAny(tt.input)
			}

			// Check for expected errors
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// For time-based functions, just check that something was rendered
			if tt.name == "now_function" {
				assert.NotEqual(t, tt.input, result)
				assert.Contains(t, result, "Current time: ")
				return
			}

			// Check the result
			if !assert.Equal(t, tt.expected, result) {
				sm := ac.getStateMap(nil)
				g.Warn(g.F("%s => %#v", tt.name, sm))
				g.Warn(g.F("%s => %s", tt.name, g.Marshal(sm)))
			}
		})
	}
}

func TestOAuth2Authentication(t *testing.T) {
	tests := []struct {
		name        string
		auth        Authentication
		expectError bool
		errorMsg    string
	}{
		{
			name: "client_credentials_missing_client_id",
			auth: Authentication{
				Type:              AuthTypeOAuth2,
				Flow:              "client_credentials",
				ClientSecret:      "secret123",
				AuthenticationURL: "https://api.example.com/oauth/token",
			},
			expectError: true,
			errorMsg:    "client_id is required",
		},
		{
			name: "client_credentials_missing_client_secret",
			auth: Authentication{
				Type:              AuthTypeOAuth2,
				Flow:              "client_credentials",
				ClientID:          "client123",
				AuthenticationURL: "https://api.example.com/oauth/token",
			},
			expectError: true,
			errorMsg:    "client_secret is required",
		},
		{
			name: "client_credentials_missing_auth_url",
			auth: Authentication{
				Type:         AuthTypeOAuth2,
				Flow:         "client_credentials",
				ClientID:     "client123",
				ClientSecret: "secret123",
			},
			expectError: true,
			errorMsg:    "authentication_url is required",
		},
		{
			name: "refresh_token_missing_token",
			auth: Authentication{
				Type:              AuthTypeOAuth2,
				Flow:              "refresh_token",
				AuthenticationURL: "https://api.example.com/oauth/token",
			},
			expectError: true,
			errorMsg:    "refresh_token is required",
		},
		{
			name: "password_missing_username",
			auth: Authentication{
				Type:              AuthTypeOAuth2,
				Flow:              "password",
				Password:          "pass123",
				AuthenticationURL: "https://api.example.com/oauth/token",
			},
			expectError: true,
			errorMsg:    "username is required",
		},
		{
			name: "password_missing_password",
			auth: Authentication{
				Type:              AuthTypeOAuth2,
				Flow:              "password",
				Username:          "user123",
				AuthenticationURL: "https://api.example.com/oauth/token",
			},
			expectError: true,
			errorMsg:    "password is required",
		},
		{
			name: "authorization_code_missing_code",
			auth: Authentication{
				Type:              AuthTypeOAuth2,
				Flow:              "authorization_code",
				ClientID:          "client123",
				ClientSecret:      "secret123",
				RedirectURI:       "https://app.example.com/callback",
				AuthenticationURL: "https://api.example.com/oauth/token",
			},
			expectError: true,
			errorMsg:    "authorization code is required",
		},
		{
			name: "unsupported_flow",
			auth: Authentication{
				Type: AuthTypeOAuth2,
				Flow: "unsupported_flow",
			},
			expectError: true,
			errorMsg:    "unsupported OAuth2 flow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ac := &APIConnection{
				Context: g.NewContext(context.Background()),
				State: &APIState{
					Env:     make(map[string]string),
					State:   make(map[string]any),
					Secrets: make(map[string]any),
					Auth:    APIStateAuth{},
				},
				Spec:      Spec{Authentication: tt.auth},
				evaluator: iop.NewEvaluator(g.ArrStr("env", "state", "secrets", "auth", "response", "request", "sync")),
			}

			_, err := ac.performOAuth2Flow(tt.auth)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestOAuth2FlowValidation(t *testing.T) {
	ac := &APIConnection{
		Context: g.NewContext(context.Background()),
		State: &APIState{
			Env:     make(map[string]string),
			State:   make(map[string]any),
			Secrets: make(map[string]any),
			Auth:    APIStateAuth{},
		},
		evaluator: iop.NewEvaluator(g.ArrStr("env", "state", "secrets", "auth", "response", "request", "sync")),
	}

	// Test template rendering in OAuth2 fields
	ac.State.Secrets["CLIENT_ID"] = "secret_client_123"
	ac.State.Secrets["CLIENT_SECRET"] = "secret_value_456"

	auth := Authentication{
		Type:              AuthTypeOAuth2,
		Flow:              "client_credentials",
		ClientID:          "{secrets.CLIENT_ID}",
		ClientSecret:      "{secrets.CLIENT_SECRET}",
		AuthenticationURL: "https://api.example.com/oauth/token",
	}

	// This should not error during validation (only during actual HTTP request)
	_, err := ac.performOAuth2Flow(auth)

	// We expect this to fail at HTTP request stage, not validation
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute OAuth2 request")
}

// TestHTTPCallAndResponseExtraction demonstrates HTTP call simulation and response extraction
// using the API framework with mock HTTP server. It tests:
// 1. Basic pagination field extraction using response processors
// 2. JMESPath filtering of response records
// 3. Complex response processing similar to OneStock API structure
func TestHTTPCallAndResponseExtraction(t *testing.T) {
	tests := []struct {
		name              string
		mockResponse      map[string]any
		expectedRecords   []map[string]any
		expectedState     map[string]any
		expectedNextState map[string]any
		endpointSpec      Endpoint
	}{
		{
			name: "extract_pagination_id_from_response",
			mockResponse: g.M(
				"pagination", g.M(
					"id", "abc123",
					"hasMore", true,
					"total", 100,
				),
				"data", []any{
					g.M("id", 1, "name", "Item 1"),
					g.M("id", 2, "name", "Item 2"),
				},
			),
			endpointSpec: Endpoint{
				Response: Response{
					Processors: []Processor{
						{
							Expression:  "response.json.pagination.id",
							Output:      "state.next_page_id",
							Aggregation: AggregationTypeLast,
						},
					},
					Records: Records{
						JmesPath: "data",
					},
				},
			},
			expectedRecords: []map[string]any{
				{"id": "1", "name": "Item 1"},
				{"id": "2", "name": "Item 2"},
			},
			expectedState: map[string]any{
				"next_page_id": "abc123",
			},
			expectedNextState: map[string]any{},
		},
		{
			name: "extract_records_with_jmespath_filter",
			mockResponse: g.M(
				"data", g.M(
					"items", []any{
						g.M("id", 1, "name", "Active Item", "active", true),
						g.M("id", 2, "name", "Inactive Item", "active", false),
						g.M("id", 3, "name", "Another Active", "active", true),
					},
				),
			),
			endpointSpec: Endpoint{
				Response: Response{
					Records: Records{
						JmesPath: "data.items[?active==`true`]",
					},
				},
			},
			expectedRecords: []map[string]any{
				{"id": "1", "name": "Active Item", "active": "true"},
				{"id": "3", "name": "Another Active", "active": "true"},
			},
			expectedState:     map[string]any{},
			expectedNextState: map[string]any{},
		},
		{
			name: "onestock_style_response",
			mockResponse: g.M(
				"pagination", g.M(
					"id", "cursor_xyz789",
					"limit", 50,
					"hasMore", true,
				),
				"items", []any{
					g.M("id", 1, "status", "shipped", "createdAt", "2025-01-10"),
					g.M("id", 2, "status", "pending", "createdAt", "2025-01-11"),
				},
			),
			endpointSpec: Endpoint{
				Response: Response{
					Processors: []Processor{
						{
							Expression:  "response.json.pagination.id",
							Output:      "state.cursor",
							Aggregation: AggregationTypeLast,
						},
						{
							Expression:  "response.json.pagination.limit",
							Output:      "state.limit",
							Aggregation: AggregationTypeLast,
						},
					},
					Records: Records{
						JmesPath: "items",
					},
				},
				Pagination: Pagination{
					StopCondition: "length(response.json.items) == 0",
					NextState: map[string]any{
						"page_cursor": "{state.cursor}",
						"id":          "{response.json.pagination.id}",
					},
				},
			},
			expectedRecords: []map[string]any{
				{"id": 1, "status": "shipped", "createdAt": "2025-01-10"},
				{"id": 2, "status": "pending", "createdAt": "2025-01-11"},
			},
			expectedState: map[string]any{
				"cursor": "cursor_xyz789",
				"limit":  float64(50),
			},
			expectedNextState: map[string]any{
				"page_cursor": "cursor_xyz789",
				"id":          "cursor_xyz789", // Since NextState uses {response.json.pagination.id}
			},
		},
		{
			name: "test_nextstate_rendering_with_mixed_sources",
			mockResponse: g.M(
				"meta", g.M(
					"next_cursor", "next_page_123",
					"page_size", 25,
					"total", 150,
				),
				"results", []any{
					g.M("id", "rec1", "value", 100),
					g.M("id", "rec2", "value", 200),
				},
			),
			endpointSpec: Endpoint{
				State: map[string]any{
					"base_url": "https://api.example.com",
					"version":  "v2",
				},
				Response: Response{
					Processors: []Processor{
						{
							Expression:  "response.json.meta.next_cursor",
							Output:      "state.next_cursor",
							Aggregation: AggregationTypeLast,
						},
						{
							Expression:  "response.json.meta.page_size",
							Output:      "state.page_size",
							Aggregation: AggregationTypeLast,
						},
					},
					Records: Records{
						JmesPath: "results",
					},
				},
				Pagination: Pagination{
					StopCondition: "response.json.meta.next_cursor == null",
					NextState: map[string]any{
						"cursor":   "{state.next_cursor}",
						"limit":    "{state.page_size}",
						"endpoint": "{state.base_url}/{state.version}/data",
						"total":    "{response.json.meta.total}",
					},
				},
			},
			expectedRecords: []map[string]any{
				{"id": "rec1", "value": "100"},
				{"id": "rec2", "value": "200"},
			},
			expectedState: map[string]any{
				"base_url":    "https://api.example.com",
				"version":     "v2",
				"next_cursor": "next_page_123",
				"page_size":   float64(25),
			},
			expectedNextState: map[string]any{
				"cursor":   "next_page_123",
				"limit":    float64(25),
				"endpoint": "https://api.example.com/v2/data",
				"total":    float64(150),
			},
		},
		{
			name: "test_nextstate_with_functions",
			mockResponse: g.M(
				"pagination", g.M(
					"next_token", "token_456",
					"has_more", true,
					"per_page", nil, // null value to test coalesce
				),
				"data", []any{
					g.M("id", 1, "created", "2025-01-10T10:00:00Z"),
					g.M("id", 2, "created", "2025-01-10T11:00:00Z"),
					g.M("id", 3, "created", "2025-01-10T12:00:00Z"),
				},
			),
			endpointSpec: Endpoint{
				State: map[string]any{
					"default_limit": 50,
					"max_limit":     100,
					"base_time":     "2025-01-10T00:00:00Z",
				},
				Response: Response{
					Processors: []Processor{
						{
							Expression:  "response.json.pagination.next_token",
							Output:      "state.token",
							Aggregation: AggregationTypeLast,
						},
						{
							Expression:  "response.json.pagination.has_more",
							Output:      "state.has_more",
							Aggregation: AggregationTypeLast,
						},
						{
							Expression:  "greatest(response.json.data[0].created, response.json.data[1].created, response.json.data[2].created)",
							Output:      "state.max_created",
							Aggregation: AggregationTypeLast,
						},
					},
					Records: Records{
						JmesPath: "data",
					},
				},
				Pagination: Pagination{
					StopCondition: "!state.has_more || state.token == null",
					NextState: map[string]any{
						// Use coalesce to handle null per_page
						"limit": "{ coalesce(response.json.pagination.per_page, state.default_limit) }",
						// Use if function for conditional logic
						"token": "{ if(state.has_more, state.token, null) }",
						// Use date_add to calculate next time window
						"from_time": "{ date_add(state.max_created, 1, \"second\") }",
						// Use least to ensure we don't exceed max_limit
						"safe_limit": "{ least(coalesce(response.json.pagination.per_page, state.default_limit), state.max_limit) }",
						// Use string functions
						"token_prefix": "{ substring(state.token, 0, 5) }",
						// Use encode_base64
						"encoded_token": "{ encode_base64(state.token) }",
						// Complex expression with multiple functions
						"query_string": "{ join([\"limit=\", cast(least(state.default_limit, 100), \"string\"), \"&token=\", state.token], \"\") }",
					},
				},
			},
			expectedRecords: []map[string]any{
				{"id": "1", "created": "2025-01-10T10:00:00Z"},
				{"id": "2", "created": "2025-01-10T11:00:00Z"}, 
				{"id": "3", "created": "2025-01-10T12:00:00Z"},
			},
			expectedState: map[string]any{
				"default_limit": 50,
				"max_limit":     100,
				"base_time":     "2025-01-10T00:00:00Z",
				"token":         "token_456",
				"has_more":      true,
				"max_created":   "2025-01-10T12:00:00Z",
			},
			expectedNextState: map[string]any{
				"limit":         float64(50), // coalesce returns default_limit since per_page is nil
				"token":         "token_456",  // if returns token since has_more is true
				"from_time":     "2025-01-10T12:00:01Z", // date_add adds 1 second
				"safe_limit":    float64(50), // least(50, 100) = 50
				"token_prefix":  "token",     // substring of "token_456"
				"encoded_token": "dG9rZW5fNDU2", // base64 encoding of "token_456"
				"query_string":  "limit=50&token=token_456",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(tt.mockResponse)
			}))
			defer server.Close()

			// Set up endpoint with test server URL
			endpoint := tt.endpointSpec
			endpoint.Name = "test_endpoint"
			// Initialize state map with predefined values if any
			if endpoint.State == nil {
				endpoint.State = make(StateMap)
			}
			endpoint.Request = Request{
				URL:    server.URL + "/test",
				Method: MethodGet,
			}

			// Create API spec with the endpoint
			spec := Spec{
				Name: "test_api",
				Authentication: Authentication{
					Type: AuthTypeNone,
				},
				EndpointMap: EndpointMap{
					"test_endpoint": endpoint,
				},
			}
			spec.endpointsOrdered = []string{"test_endpoint"}

			// Create API connection
			ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
				"state":   map[string]any{},
				"secrets": map[string]any{},
			})
			assert.NoError(t, err)

			// Authenticate (set to true for testing)
			ac.State.Auth.Authenticated = true

			// Execute the API call and get dataflow
			df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
				Limit: 10,
			})
			assert.NoError(t, err)

			// Collect data from dataflow
			data, err := df.Collect()
			assert.NoError(t, err)

			// Convert rows to maps for easier comparison
			records := []map[string]any{}
			for _, row := range data.Rows {
				record := make(map[string]any)
				for i, col := range data.Columns {
					if i < len(row) {
						record[col.Name] = row[i]
					}
				}
				records = append(records, record)
			}

			// Check records match expected (handling type conversions)
			// For some tests, we may get more records due to limit, so check at least expected records exist
			if strings.Contains(tt.name, "onestock") || strings.Contains(tt.name, "nextstate") {
				assert.GreaterOrEqual(t, len(records), len(tt.expectedRecords))
			} else {
				assert.Equal(t, len(tt.expectedRecords), len(records))
			}

			// Check each expected record
			recordsToCheck := len(tt.expectedRecords)
			if recordsToCheck > len(records) {
				recordsToCheck = len(records)
			}

			for i := 0; i < recordsToCheck; i++ {
				expectedRecord := tt.expectedRecords[i]
				actualRecord := records[i]
				for key, expectedValue := range expectedRecord {
					actualValue, exists := actualRecord[key]
					assert.True(t, exists, "Expected key %s to exist in record %d", key, i)

					// Handle different types of comparisons
					switch expected := expectedValue.(type) {
					case string:
						// Check if actual is a date that was parsed
						if actualTime, ok := actualValue.(time.Time); ok {
							// Compare date strings
							if strings.Contains(expected, "T") {
								// Full datetime comparison
								assert.Equal(t, expected, actualTime.Format(time.RFC3339))
							} else {
								// Date only comparison
								assert.Contains(t, actualTime.Format("2006-01-02"), expected)
							}
						} else {
							// Compare as strings
							assert.Equal(t, expected, cast.ToString(actualValue))
						}
					case int:
						// Handle int comparisons (might be string or int64 in actual)
						assert.Equal(t, cast.ToInt64(expected), cast.ToInt64(actualValue))
					default:
						assert.Equal(t, expected, actualValue)
					}
				}
			}

			// Check state was updated correctly
			if len(tt.expectedState) > 0 {
				for key, expectedValue := range tt.expectedState {
					actualValue, exists := spec.EndpointMap["test_endpoint"].State[key]
					assert.True(t, exists, "Expected state key %s to exist", key)
					assert.Equal(t, expectedValue, actualValue)
				}
				
				// Test NextState rendering
				if len(tt.expectedNextState) > 0 {
					// Get the endpoint from spec
					endpointCopy := spec.EndpointMap["test_endpoint"]
					endpoint := &endpointCopy
					endpoint.conn = ac
					endpoint.context = g.NewContext(context.Background())
					
					// Create a mock single request with our response data
					iter := &Iteration{
						id:       1,
						state:    endpoint.State,
						endpoint: endpoint,
						context:  g.NewContext(context.Background()),
					}
					
					req := NewSingleRequest(iter)
					req.Response = &ResponseState{
						Status:  200,
						Headers: g.M("content-type", "application/json"),
						JSON:    tt.mockResponse,
					}
					
					// Render the NextState values
					renderedNextState := make(map[string]any)
					for key, val := range endpoint.Pagination.NextState {
						rendered, err := iter.renderAny(val, req)
						assert.NoError(t, err, "Failed to render NextState key %s", key)
						renderedNextState[key] = rendered
					}
					
					// Verify rendered NextState matches expected
					for key, expectedValue := range tt.expectedNextState {
						actualValue, exists := renderedNextState[key]
						assert.True(t, exists, "Expected NextState key %s to exist", key)
						
						// Handle time values specially
						if expectedStr, ok := expectedValue.(string); ok {
							if actualTime, ok := actualValue.(time.Time); ok && strings.Contains(expectedStr, "T") {
								// Compare as formatted time string
								assert.Equal(t, expectedStr, actualTime.Format(time.RFC3339))
							} else {
								assert.Equal(t, expectedValue, actualValue)
							}
						} else {
							assert.Equal(t, expectedValue, actualValue)
						}
					}
				}
			}
		})
	}
}
