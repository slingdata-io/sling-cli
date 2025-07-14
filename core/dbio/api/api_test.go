package api

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
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

func TestAuthenticationExpiry(t *testing.T) {
	tests := []struct {
		name           string
		expiresSeconds int
		waitSeconds    int
		shouldExpire   bool
	}{
		{
			name:           "no_expiry_configured",
			expiresSeconds: 0,
			waitSeconds:    5,
			shouldExpire:   false,
		},
		{
			name:           "not_yet_expired",
			expiresSeconds: 10,
			waitSeconds:    0,
			shouldExpire:   false,
		},
		{
			name:           "just_expired",
			expiresSeconds: 2,
			waitSeconds:    3,
			shouldExpire:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create API connection
			spec := Spec{
				Authentication: Authentication{
					Type:    AuthTypeBasic,
					Expires: tt.expiresSeconds,
					Username: "test_user",
					Password: "test_pass",
				},
				EndpointMap: make(EndpointMap),
			}

			ac, err := NewAPIConnection(context.Background(), spec, nil)
			assert.NoError(t, err)

			// Authenticate
			err = ac.Authenticate()
			assert.NoError(t, err)
			assert.True(t, ac.State.Auth.Authenticated)

			// Check initial expiry
			if tt.expiresSeconds > 0 {
				assert.Greater(t, ac.State.Auth.ExpiresAt, int64(0))
				assert.False(t, ac.IsAuthExpired())
			} else {
				assert.Equal(t, int64(0), ac.State.Auth.ExpiresAt)
				assert.False(t, ac.IsAuthExpired())
			}

			// Simulate time passing
			if tt.waitSeconds > 0 && tt.expiresSeconds > 0 {
				// Manually set the expiry time to simulate time passing
				ac.State.Auth.ExpiresAt = time.Now().Add(-time.Duration(tt.waitSeconds) * time.Second).Unix()
			}

			// Check if expired
			assert.Equal(t, tt.shouldExpire, ac.IsAuthExpired())
		})
	}
}

func TestEnsureAuthenticated(t *testing.T) {
	// Test the expiry mechanism with a simple auth type
	spec := Spec{
		Authentication: Authentication{
			Type:     AuthTypeBasic,
			Expires:  2, // Expires after 2 seconds
			Username: "test_user",
			Password: "test_pass",
		},
		EndpointMap: make(EndpointMap),
	}

	ac, err := NewAPIConnection(context.Background(), spec, nil)
	assert.NoError(t, err)

	// Initially not authenticated
	assert.False(t, ac.State.Auth.Authenticated)

	// First ensure authenticated
	err = ac.EnsureAuthenticated()
	assert.NoError(t, err)
	assert.True(t, ac.State.Auth.Authenticated)
	assert.Greater(t, ac.State.Auth.ExpiresAt, int64(0))

	initialExpiresAt := ac.State.Auth.ExpiresAt

	// Call again immediately - should not change expiry
	err = ac.EnsureAuthenticated()
	assert.NoError(t, err)
	assert.Equal(t, initialExpiresAt, ac.State.Auth.ExpiresAt)

	// Manually expire the auth
	ac.State.Auth.ExpiresAt = time.Now().Add(-1 * time.Second).Unix()

	// Small delay to ensure time difference
	time.Sleep(1 * time.Second)

	// Call again - should re-authenticate and get new expiry
	err = ac.EnsureAuthenticated()
	assert.NoError(t, err)
	assert.True(t, ac.State.Auth.Authenticated)
	assert.Greater(t, ac.State.Auth.ExpiresAt, initialExpiresAt)
}

func TestEnsureAuthenticatedConcurrency(t *testing.T) {
	// Test that concurrent calls to EnsureAuthenticated don't cause multiple authentications
	spec := Spec{
		Authentication: Authentication{
			Type:     AuthTypeBasic,
			Expires:  10,
			Username: "test_user",
			Password: "test_pass",
		},
		EndpointMap: make(EndpointMap),
	}

	ac, err := NewAPIConnection(context.Background(), spec, nil)
	assert.NoError(t, err)

	// Run multiple concurrent EnsureAuthenticated calls
	var wg sync.WaitGroup
	errors := make([]error, 10)
	
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errors[idx] = ac.EnsureAuthenticated()
		}(i)
	}

	wg.Wait()

	// Check all succeeded
	for i, err := range errors {
		assert.NoError(t, err, "goroutine %d failed", i)
	}

	// Should have authenticated successfully
	assert.True(t, ac.State.Auth.Authenticated)
}

//go:embed api_suite.yaml
var templatesSuite embed.FS

func TestHTTPCallAndResponseExtraction(t *testing.T) {
	type testCase struct {
		ID   int               `yaml:"id"`
		Name string            `yaml:"name"`
		Spec Spec              `yaml:"spec"`
		Env  map[string]string `yaml:"env"`
		Err  bool              `yaml:"err"`

		MockResponse      map[string]any   `yaml:"mock_response"`
		ExpectedRecords   []map[string]any `yaml:"expected_records"`
		ExpectedState     map[string]any   `yaml:"expected_state"`
		ExpectedNextState map[string]any   `yaml:"expected_next_state"`
	}

	// Read the YAML file
	yamlData, err := templatesSuite.ReadFile("api_suite.yaml")
	assert.NoError(t, err)

	var testCases []testCase
	err = yaml.Unmarshal(yamlData, &testCases)
	assert.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// Create a test HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)

				// Handle different endpoints based on path
				switch r.URL.Path {
				case "/login":
					// Return token for authentication sequence test
					loginResponse := map[string]any{
						"token": "mock_token_12345",
					}
					json.NewEncoder(w).Encode(loginResponse)
				case "/config":
					// Return config for setup test
					if config, ok := tc.MockResponse["config"]; ok {
						configResponse := map[string]any{
							"config": config,
						}
						json.NewEncoder(w).Encode(configResponse)
					} else {
						json.NewEncoder(w).Encode(tc.MockResponse)
					}
				case "/cleanup":
					// Return cleanup response for teardown test
					if cleanup, ok := tc.MockResponse["cleanup"]; ok {
						cleanupResponse := map[string]any{
							"cleanup": cleanup,
						}
						json.NewEncoder(w).Encode(cleanupResponse)
					} else {
						json.NewEncoder(w).Encode(tc.MockResponse)
					}
				default:
					// Return the main mock response for other endpoints
					json.NewEncoder(w).Encode(tc.MockResponse)
				}
			}))
			defer server.Close()

			// Initialize the spec's EndpointMap if it's nil
			if tc.Spec.EndpointMap == nil {
				tc.Spec.EndpointMap = make(EndpointMap)
			}

			// Set up endpoint with test server URL
			endpoint := tc.Spec.EndpointMap["test_endpoint"]
			endpoint.Name = "test_endpoint"
			// Initialize state map with predefined values if any
			if endpoint.State == nil {
				endpoint.State = make(StateMap)
			}
			// Apply any state from YAML spec
			for k, v := range tc.Spec.EndpointMap["test_endpoint"].State {
				endpoint.State[k] = v
			}
			endpoint.Request = Request{
				URL:    server.URL + "/test",
				Method: MethodGet,
			}

			// Update authentication sequence URLs if present
			if tc.Spec.Authentication.Type == AuthTypeSequence {
				for i, call := range tc.Spec.Authentication.Sequence {
					if call.Request.URL != "" {
						tc.Spec.Authentication.Sequence[i].Request.URL = server.URL + call.Request.URL
					}
				}
			}

			// Update setup and teardown sequence URLs if present
			for epName, ep := range tc.Spec.EndpointMap {
				// Update setup URLs
				for i, call := range ep.Setup {
					if call.Request.URL != "" {
						ep.Setup[i].Request.URL = server.URL + call.Request.URL
					}
				}
				// Update teardown URLs
				for i, call := range ep.Teardown {
					if call.Request.URL != "" {
						ep.Teardown[i].Request.URL = server.URL + call.Request.URL
					}
				}
				tc.Spec.EndpointMap[epName] = ep
			}

			// Update the spec with the modified endpoint
			tc.Spec.EndpointMap["test_endpoint"] = endpoint
			tc.Spec.endpointsOrdered = []string{"test_endpoint"}

			// Create API connection
			ac, err := NewAPIConnection(context.Background(), tc.Spec, map[string]any{
				"state": map[string]any{},
				"secrets": map[string]any{
					"site_id":  "store_123",
					"user_id":  "test_user",
					"password": "test_password",
				},
			})
			assert.NoError(t, err)

			// Handle authentication based on type
			if tc.Spec.Authentication.Type == AuthTypeSequence {
				// For sequence authentication, actually run the authentication
				err = ac.Authenticate()
				assert.NoError(t, err)
			} else {
				// For other auth types, bypass authentication for testing
				ac.State.Auth.Authenticated = true
			}

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
			if strings.Contains(tc.Name, "onestock") || strings.Contains(tc.Name, "nextstate") {
				assert.GreaterOrEqual(t, len(records), len(tc.ExpectedRecords))
			} else {
				assert.Equal(t, len(tc.ExpectedRecords), len(records))
			}

			// Check each expected record
			recordsToCheck := len(tc.ExpectedRecords)
			if recordsToCheck > len(records) {
				recordsToCheck = len(records)
			}

			for i := 0; i < recordsToCheck; i++ {
				expectedRecord := tc.ExpectedRecords[i]
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
			if len(tc.ExpectedState) > 0 {
				// Get the endpoint from the connection's spec (which was modified during execution)
				actualEndpoint := ac.Spec.EndpointMap["test_endpoint"]

				for key, expectedValue := range tc.ExpectedState {
					actualValue, exists := actualEndpoint.State[key]

					// For authentication sequence tests, also check API connection state and auth state
					if !exists && tc.Spec.Authentication.Type == AuthTypeSequence {
						actualValue, exists = ac.State.State[key]
						if !exists && key == "token" {
							// Check auth token
							actualValue, exists = ac.State.Auth.Token, ac.State.Auth.Token != ""
						}
					}

					assert.True(t, exists, "Expected state key %s to exist", key)

					// Handle numeric comparisons
					switch expected := expectedValue.(type) {
					case int:
						assert.Equal(t, float64(expected), cast.ToFloat64(actualValue))
					case float64:
						assert.Equal(t, expected, cast.ToFloat64(actualValue))
					default:
						assert.Equal(t, expectedValue, actualValue)
					}
				}

				// Test NextState rendering
				if len(tc.ExpectedNextState) > 0 {
					// Get the endpoint from connection's spec
					endpointCopy := ac.Spec.EndpointMap["test_endpoint"]
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
						JSON:    tc.MockResponse,
					}

					// Render the NextState values
					renderedNextState := make(map[string]any)
					for key, val := range endpoint.Pagination.NextState {
						rendered, err := iter.renderAny(val, req)
						assert.NoError(t, err, "Failed to render NextState key %s", key)
						renderedNextState[key] = rendered
					}

					// Verify rendered NextState matches expected
					for key, expectedValue := range tc.ExpectedNextState {
						actualValue, exists := renderedNextState[key]
						assert.True(t, exists, "Expected NextState key %s to exist", key)

						// Handle different types of comparisons
						switch expected := expectedValue.(type) {
						case string:
							if actualTime, ok := actualValue.(time.Time); ok && strings.Contains(expected, "T") {
								// Compare as formatted time string
								assert.Equal(t, expected, actualTime.Format(time.RFC3339))
							} else {
								assert.Equal(t, expected, cast.ToString(actualValue))
							}
						case int:
							// Handle numeric comparisons
							assert.Equal(t, float64(expected), cast.ToFloat64(actualValue))
						case float64:
							assert.Equal(t, expected, cast.ToFloat64(actualValue))
						default:
							assert.Equal(t, expectedValue, actualValue)
						}
					}
				}
			}
		})
	}
}
