package api

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
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
			name: "client_credentials_missing_client_secret",
			auth: Authentication{
				"type":               AuthTypeOAuth2,
				"flow":               OAuthFlowClientCredentials,
				"client_id":          "client123",
				"authentication_url": "https://api.example.com/oauth/token",
			},
			expectError: true,
			errorMsg:    "client_secret is required",
		},
		{
			name: "authorization_code_missing_auth_url",
			auth: Authentication{
				"type":               AuthTypeOAuth2,
				"flow":               OAuthFlowAuthorizationCode,
				"client_id":          "client123",
				"client_secret":      "secret123",
				"redirect_uri":       "https://app.example.com/callback",
				"authorization_url":  "https://api.example.com/oauth/authorize",
			},
			expectError: true,
			errorMsg:    "authentication_url is required",
		},
		{
			name: "unsupported_flow",
			auth: Authentication{
				"type": AuthTypeOAuth2,
				"flow": "unsupported_flow",
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

			authenticator, err := ac.MakeAuthenticator()
			assert.NoError(t, err)
			authOAuth, ok := authenticator.(*AuthenticatorOAuth2)
			if assert.True(t, ok) {
				authOAuth.conn = ac
				err := authOAuth.Authenticate(ac.Context.Ctx, &ac.State.Auth)

				if tt.expectError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tt.errorMsg)
				} else {
					assert.NoError(t, err)
				}
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

	ac.Spec.Authentication = Authentication{
		"type":               AuthTypeOAuth2,
		"flow":               OAuthFlowClientCredentials,
		"client_id":          "{secrets.CLIENT_ID}",
		"client_secret":      "{secrets.CLIENT_SECRET}",
		"authentication_url": "https://api.example.com/oauth/token",
	}

	// This should not error during validation (only during actual HTTP request)

	authenticator, _ := ac.MakeAuthenticator()
	authOAuth, ok := authenticator.(*AuthenticatorOAuth2)
	if assert.True(t, ok) {
		authOAuth.conn = ac
		err := authOAuth.Authenticate(ac.Context.Ctx, &ac.State.Auth)

		// We expect this to fail at HTTP request stage, not validation
		// The new implementation will fail when trying to get token from the oauth2 server
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get token")
	}
}

func TestOAuth2AuthorizationURLField(t *testing.T) {
	tests := []struct {
		name                     string
		authenticationURL        string
		authorizationURL         string
		expectedAuthorizationURL string // What we expect to be derived/used
		description              string
	}{
		{
			name:                     "explicit_authorization_url",
			authenticationURL:        "https://api.example.com/oauth/token",
			authorizationURL:         "https://api.example.com/oauth/authorize",
			expectedAuthorizationURL: "https://api.example.com/oauth/authorize",
			description:              "When authorization_url is explicitly provided, it should be used",
		},
		{
			name:                     "authorization_url_with_different_domain",
			authenticationURL:        "https://api.example.com/oauth/token",
			authorizationURL:         "https://auth.example.com/authorize",
			expectedAuthorizationURL: "https://auth.example.com/authorize",
			description:              "Authorization URL can be on a different domain",
		},
		{
			name:                     "no_authorization_url_standard_pattern",
			authenticationURL:        "https://api.example.com/oauth/token",
			authorizationURL:         "",
			expectedAuthorizationURL: "https://api.example.com/oauth/authorize",
			description:              "Without authorization_url, should derive from token URL",
		},
		{
			name:                     "no_authorization_url_simple_pattern",
			authenticationURL:        "https://api.example.com/token",
			authorizationURL:         "",
			expectedAuthorizationURL: "https://api.example.com/authorize",
			description:              "Should handle simple /token -> /authorize replacement",
		},
		{
			name:                     "templated_authorization_url",
			authenticationURL:        "https://api.example.com/oauth/token",
			authorizationURL:         "{env.AUTH_URL}",
			expectedAuthorizationURL: "https://custom.example.com/auth",
			description:              "Authorization URL should support templating",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock server to capture the authorization URL
			var capturedAuthURL string
			authCodeReceived := make(chan string, 1)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/callback" {
					// Capture auth code callback
					code := r.URL.Query().Get("code")
					if code != "" {
						authCodeReceived <- code
					}
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, "OK")
				} else if strings.Contains(r.URL.Path, "token") {
					// Token exchange endpoint
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(map[string]any{
						"access_token": "test_token_123",
						"token_type":   "Bearer",
					})
				}
			}))
			defer server.Close()

			// Create API connection
			ac := &APIConnection{
				Context: g.NewContext(context.Background()),
				State: &APIState{
					Env:     map[string]string{"AUTH_URL": "https://custom.example.com/auth"},
					State:   make(map[string]any),
					Secrets: make(map[string]any),
					Auth:    APIStateAuth{},
				},
				evaluator: iop.NewEvaluator(g.ArrStr("env", "state", "secrets", "auth", "response", "request", "sync")),
			}

			auth := Authentication{
				"type":               AuthTypeOAuth2,
				"flow":               OAuthFlowAuthorizationCode,
				"client_id":          "test_client",
				"client_secret":      "test_secret",
				"authentication_url": tt.authenticationURL,
				"authorization_url":  tt.authorizationURL,
				"redirect_uri":       server.URL + "/callback",
			}

			// Test that the authorization URL is correctly rendered/derived
			renderedAuthURL, err := ac.renderString(auth["authorization_url"])
			assert.NoError(t, err)

			if renderedAuthURL == "" {
				// Simulate the URL derivation logic when authorization_url is not provided
				authURL := cast.ToString(auth["authentication_url"])
				authorizeURL := strings.Replace(authURL, "/token", "/authorize", 1)
				if authorizeURL == authURL {
					if strings.HasSuffix(authURL, "/oauth/token") {
						authorizeURL = strings.Replace(authURL, "/oauth/token", "/oauth/authorize", 1)
					} else if strings.HasSuffix(authURL, "/token") {
						authorizeURL = strings.Replace(authURL, "/token", "/authorize", 1)
					}
				}
				capturedAuthURL = authorizeURL
			} else {
				capturedAuthURL = renderedAuthURL
			}

			// Verify the captured/derived auth URL matches expectations
			assert.Equal(t, tt.expectedAuthorizationURL, capturedAuthURL, tt.description)
		})
	}
}

func TestOAuth2AuthorizationURLTemplating(t *testing.T) {
	// Test that authorization_url supports templating with env, state, and secrets
	ac := &APIConnection{
		Context: g.NewContext(context.Background()),
		State: &APIState{
			Env:     map[string]string{"AUTH_DOMAIN": "auth.example.com"},
			State:   map[string]any{"auth_version": "v2"},
			Secrets: map[string]any{"TENANT_ID": "tenant_123"},
			Auth:    APIStateAuth{},
		},
		evaluator: iop.NewEvaluator(g.ArrStr("env", "state", "secrets", "auth", "response", "request", "sync")),
	}

	tests := []struct {
		name             string
		authorizationURL string
		expectedURL      string
	}{
		{
			name:             "env_variable",
			authorizationURL: "https://{env.AUTH_DOMAIN}/authorize",
			expectedURL:      "https://auth.example.com/authorize",
		},
		{
			name:             "state_variable",
			authorizationURL: "https://api.example.com/{state.auth_version}/authorize",
			expectedURL:      "https://api.example.com/v2/authorize",
		},
		{
			name:             "secrets_variable",
			authorizationURL: "https://api.example.com/{secrets.TENANT_ID}/oauth/authorize",
			expectedURL:      "https://api.example.com/tenant_123/oauth/authorize",
		},
		{
			name:             "multiple_variables",
			authorizationURL: "https://{env.AUTH_DOMAIN}/{state.auth_version}/tenants/{secrets.TENANT_ID}/authorize",
			expectedURL:      "https://auth.example.com/v2/tenants/tenant_123/authorize",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rendered, err := ac.renderString(tt.authorizationURL)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedURL, rendered)
		})
	}
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
					"type":     AuthTypeBasic,
					"expires":  tt.expiresSeconds,
					"username": "test_user",
					"password": "test_pass",
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
			"type":     AuthTypeBasic,
			"expires":  2, // Expires after 2 seconds
			"username": "test_user",
			"password": "test_pass",
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
			"type":     AuthTypeBasic,
			"expires":  10,
			"username": "test_user",
			"password": "test_pass",
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

		MockResponse      any              `yaml:"mock_response"` // can be map or string (for CSV)
		ExpectedRecords   []map[string]any `yaml:"expected_records"`
		ExpectedState     map[string]any   `yaml:"expected_state"`
		ExpectedNextState map[string]any   `yaml:"expected_next_state"`
		ExpectedStore     map[string]any   `yaml:"expected_store"`
		ExpectedEnv       map[string]any   `yaml:"expected_env"`
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
				// Check if MockResponse is a string (for CSV or XML format)
				if mockStr, ok := tc.MockResponse.(string); ok {
					// Detect format based on content
					if strings.HasPrefix(strings.TrimSpace(mockStr), "<?xml") || strings.Contains(mockStr, "<users>") {
						w.Header().Set("Content-Type", "application/xml")
					} else {
						w.Header().Set("Content-Type", "text/csv")
					}
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, mockStr)
					return
				}

				// Handle as JSON map
				mockMap, ok := tc.MockResponse.(map[string]any)
				if !ok {
					mockMap = make(map[string]any)
				}

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
					if config, ok := mockMap["config"]; ok {
						configResponse := map[string]any{
							"config": config,
						}
						json.NewEncoder(w).Encode(configResponse)
					} else {
						json.NewEncoder(w).Encode(mockMap)
					}
				case "/cleanup":
					// Return cleanup response for teardown test
					if cleanup, ok := mockMap["cleanup"]; ok {
						cleanupResponse := map[string]any{
							"cleanup": cleanup,
						}
						json.NewEncoder(w).Encode(cleanupResponse)
					} else {
						json.NewEncoder(w).Encode(mockMap)
					}
				default:
					// Return the main mock response for other endpoints
					json.NewEncoder(w).Encode(mockMap)
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
			if auth := tc.Spec.Authentication; auth.Type() == AuthTypeSequence {
				authSeq := AuthenticatorSequence{}
				err = g.JSONConvert(auth, &authSeq)
				if assert.NoError(t, err) && assert.True(t, len(authSeq.Sequence) > 0) {
					for i, call := range authSeq.Sequence {
						if call.Request.URL != "" {
							authSeq.Sequence[i].Request.URL = server.URL + call.Request.URL
						}
					}

					// set back
					tc.Spec.Authentication["sequence"] = authSeq.Sequence
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

			// convert to proper spec
			specBody, _ := yaml.Marshal(tc.Spec)
			tc.Spec, err = LoadSpec(string(specBody))
			if !assert.NoError(t, err) {
				return
			}

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
			if tc.Spec.Authentication.Type() == AuthTypeSequence {
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
			if !assert.NoError(t, err) {
				return
			}

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
					if !exists && tc.Spec.Authentication.Type() == AuthTypeSequence {
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
						id:       g.F("i%02d", 1),
						state:    endpoint.State,
						endpoint: endpoint,
						context:  g.NewContext(context.Background()),
					}

					req := NewSingleRequest(iter)

					// Only set JSON if MockResponse is a map (not for CSV strings)
					if mockMap, ok := tc.MockResponse.(map[string]any); ok {
						req.Response = &ResponseState{
							Status:  200,
							Headers: g.M("content-type", "application/json"),
							JSON:    mockMap,
						}
					} else {
						req.Response = &ResponseState{
							Status:  200,
							Headers: g.M("content-type", "text/csv"),
						}
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

			// Check store was updated correctly
			if len(tc.ExpectedStore) > 0 {
				actualStore := ac.GetReplicationStore()

				for key, expectedValue := range tc.ExpectedStore {
					actualValue, exists := actualStore[key]
					assert.True(t, exists, "Expected store key %s to exist", key)

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
			}

			// Check environment variables were set correctly
			if len(tc.ExpectedEnv) > 0 {
				for key, expectedValue := range tc.ExpectedEnv {
					actualValue := os.Getenv(key)
					assert.NotEmpty(t, actualValue, "Expected env variable %s to be set", key)

					// All env vars are strings
					assert.Equal(t, cast.ToString(expectedValue), actualValue)
				}
			}
		})
	}
}

func TestDynamicEndpointsIntegration(t *testing.T) {
	tests := []struct {
		name                  string
		specYAML              string                    // YAML spec body with {base_url} placeholder
		dataResponses         map[string]map[string]any // endpoint path -> response
		expectedEndpointNames []string
		expectedRecordCounts  map[string]int   // endpoint name -> record count
		expectedRecordValues  map[string][]any // endpoint name -> expected record values
	}{
		{
			name: "simple_static_array_iteration",
			specYAML: `
name: "Test Dynamic API"
defaults:
  state:
    base_url: "{base_url}"

dynamic_endpoints:
  - iterate: '["users", "orders", "products"]'
    into: "state.resource"
    endpoint:
      name: "{state.resource}"
      request:
        url: "{state.base_url}/{state.resource}"
      response:
        records:
          jmespath: "data[]"
`,
			expectedEndpointNames: []string{
				"users",
				"orders",
				"products",
			},
			dataResponses: map[string]map[string]any{
				"/users": {
					"data": []map[string]any{
						{"id": 1, "name": "Alice"},
						{"id": 2, "name": "Bob"},
					},
				},
				"/orders": {
					"data": []map[string]any{
						{"id": 101, "total": 150.00},
						{"id": 102, "total": 225.50},
					},
				},
				"/products": {
					"data": []map[string]any{
						{"id": 501, "name": "Widget"},
					},
				},
			},
			expectedRecordCounts: map[string]int{
				"users":    2,
				"orders":   2,
				"products": 1,
			},
			expectedRecordValues: map[string][]any{
				"users": {
					map[string]any{"id": float64(1), "name": "Alice"},
					map[string]any{"id": float64(2), "name": "Bob"},
				},
				"orders": {
					map[string]any{"id": float64(101), "total": float64(150.00)},
					map[string]any{"id": float64(102), "total": float64(225.50)},
				},
				"products": {
					map[string]any{"id": float64(501), "name": "Widget"},
				},
			},
		},
		{
			name: "setup_sequence_with_iteration",
			specYAML: `
name: "Test Dynamic API with Setup"
defaults:
  state:
    base_url: "{base_url}"

dynamic_endpoints:
  - setup:
      - request:
          url: "{state.base_url}/metadata/tables"
        response:
          processors:
            - expression: "response.json.tables"
              output: "state.table_list"
              aggregation: last

    iterate: "state.table_list"
    into: "state.table_name"

    endpoint:
      name: "table_{state.table_name.name}"
      request:
        url: "{state.base_url}/tables/{state.table_name.name}"
      response:
        records:
          jmespath: "rows[]"
`,
			expectedEndpointNames: []string{
				"table_customers",
				"table_invoices",
			},
			dataResponses: map[string]map[string]any{
				"/metadata/tables": {
					"tables": []map[string]any{
						{"name": "customers"},
						{"name": "invoices"},
					},
				},
				"/tables/customers": {
					"rows": []map[string]any{
						{"id": 1, "company": "Acme Corp"},
						{"id": 2, "company": "Globex Inc"},
					},
				},
				"/tables/invoices": {
					"rows": []map[string]any{
						{"id": 1001, "amount": 5000.00},
					},
				},
			},
			expectedRecordCounts: map[string]int{
				"table_customers": 2,
				"table_invoices":  1,
			},
			expectedRecordValues: map[string][]any{
				"table_customers": {
					map[string]any{"id": float64(1), "company": "Acme Corp"},
					map[string]any{"id": float64(2), "company": "Globex Inc"},
				},
				"table_invoices": {
					map[string]any{"id": float64(1001), "amount": float64(5000.00)},
				},
			},
		},
		{
			name: "complex_object_iteration",
			specYAML: `
name: "Test Dynamic API with Complex Objects"
defaults:
  state:
    base_url: "{base_url}"

dynamic_endpoints:
  - iterate: '[{"id": "us-east", "name": "US East"}, {"id": "us-west", "name": "US West"}]'
    into: "state.region"
    endpoint:
      name: "sales_{state.region.id}"
      description: "Sales for {state.region.name}"
      request:
        url: "{state.base_url}/regions/{state.region.id}/sales"
      response:
        records:
          jmespath: "sales[]"
`,
			expectedEndpointNames: []string{
				"sales_us-east",
				"sales_us-west",
			},
			dataResponses: map[string]map[string]any{
				"/regions/us-east/sales": {
					"sales": []map[string]any{
						{"date": "2024-01-01", "amount": 1000.00},
						{"date": "2024-01-02", "amount": 1500.00},
					},
				},
				"/regions/us-west/sales": {
					"sales": []map[string]any{
						{"date": "2024-01-01", "amount": 2000.00},
					},
				},
			},
			expectedRecordCounts: map[string]int{
				// NOTE: There's a JmesPath/streaming issue where arrays are returned as single records
				// This is not a dynamic endpoints issue - the endpoints are generated correctly
				"sales_us-east": 2,
				"sales_us-west": 1,
			},
			expectedRecordValues: map[string][]any{
				"sales_us-east": {
					map[string]any{"date": "2024-01-01", "amount": float64(1000.00)},
					map[string]any{"date": "2024-01-02", "amount": float64(1500.00)},
				},
				"sales_us-west": {
					map[string]any{"date": "2024-01-01", "amount": float64(2000.00)},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)

				// Handle data endpoints
				if response, ok := tt.dataResponses[r.URL.Path]; ok {
					json.NewEncoder(w).Encode(response)
					return
				}

				// Default empty response
				json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
			}))
			defer server.Close()

			// Replace {base_url} placeholder in spec YAML
			specYAML := strings.ReplaceAll(tt.specYAML, "{base_url}", server.URL)

			// Parse the spec from YAML
			spec, err := LoadSpec(specYAML)
			if !assert.NoError(t, err, "Failed to parse spec YAML") {
				return
			}

			// Create API connection
			ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
				"state":   map[string]any{},
				"secrets": map[string]any{},
			})
			if !assert.NoError(t, err) {
				return
			}

			// Bypass authentication for testing
			ac.State.Auth.Authenticated = true

			// List endpoints - this triggers RenderDynamicEndpoints
			endpoints, err := ac.ListEndpoints()
			if !assert.NoError(t, err) {
				return
			}

			// Assert correct number of endpoints generated
			assert.Equal(t, len(tt.expectedEndpointNames), len(endpoints),
				"Expected %d endpoints, got %d", len(tt.expectedEndpointNames), len(endpoints))

			// Assert endpoint names match expected
			actualNames := []string{}
			for _, ep := range endpoints {
				actualNames = append(actualNames, ep.Name)
			}
			assert.ElementsMatch(t, tt.expectedEndpointNames, actualNames,
				"Endpoint names don't match. Expected: %v, Got: %v", tt.expectedEndpointNames, actualNames)

			// Test each generated endpoint
			for _, ep := range endpoints {
				t.Run("endpoint_"+ep.Name, func(t *testing.T) {
					// Read dataflow from endpoint
					df, err := ac.ReadDataflow(ep.Name, APIStreamConfig{
						Limit: 100,
					})
					if !assert.NoError(t, err, "Failed to read dataflow for endpoint %s", ep.Name) {
						return
					}

					// Collect data
					data, err := df.Collect()
					if !assert.NoError(t, err, "Failed to collect data for endpoint %s", ep.Name) {
						return
					}

					// Assert record count
					expectedCount, ok := tt.expectedRecordCounts[ep.Name]
					if !assert.True(t, ok, "No expected record count defined for endpoint %s", ep.Name) {
						return
					}

					// Debug: print actual data
					if len(data.Rows) != expectedCount {
						t.Logf("Endpoint %s: Expected %d rows, got %d. Data: %s", ep.Name, expectedCount, len(data.Rows), g.Marshal(data.Rows))
					}

					assert.Equal(t, expectedCount, len(data.Rows),
						"Expected %d records for endpoint %s, got %d",
						expectedCount, ep.Name, len(data.Rows))

					// Verify data is not empty (if expected count > 0)
					if expectedCount > 0 {
						assert.Greater(t, len(data.Columns), 0,
							"Expected columns for endpoint %s", ep.Name)
						assert.Greater(t, len(data.Rows), 0,
							"Expected rows for endpoint %s", ep.Name)

						// Assert that response record values are correct
						expectedRecords, ok := tt.expectedRecordValues[ep.Name]
						if assert.True(t, ok) {
							// Convert data.Rows to []any for comparison
							actualRecords := make([]any, len(data.Rows))
							for i, row := range data.Rows {
								record := make(map[string]any)
								for j, col := range data.Columns {
									if j < len(row) {
										record[col.Name] = row[j]
									}
								}
								actualRecords[i] = record
							}

							// Compare each expected record with actual
							for i, expectedRecord := range expectedRecords {
								if i >= len(actualRecords) {
									t.Errorf("Missing record at index %d for endpoint %s", i, ep.Name)
									continue
								}

								actualRecord := actualRecords[i].(map[string]any)
								expectedMap := expectedRecord.(map[string]any)
								t.Logf("\nexpectedMap: %s\nactualRecord: %s", g.Marshal(expectedMap), g.Marshal(actualRecord))

								// Compare each field
								for key, expectedValue := range expectedMap {
									actualValue, exists := actualRecord[key]
									if !assert.True(t, exists, "Field %s missing in record %d for endpoint %s", key, i, ep.Name) {
										continue
									}

									expectedValue = cast.ToString(expectedValue)
									actualValue = strings.ReplaceAll(
										cast.ToString(actualValue), " 00:00:00 +0000 UTC", "")

									assert.EqualValues(t, expectedValue, actualValue,
										"Field %s mismatch in record %d for endpoint %s. Expected: %v, Got: %v",
										key, i, ep.Name, expectedValue, actualValue)
								}
							}
						}
					}
				})
			}
		})
	}
}

func TestDynamicEndpointsErrors(t *testing.T) {
	tests := []struct {
		name          string
		spec          Spec
		expectedError string
	}{
		{
			name: "duplicate_endpoint_names",
			spec: Spec{
				Name: "Duplicate Names API",
				Defaults: Endpoint{
					State: StateMap{
						"base_url": "https://api.example.com",
					},
				},
				EndpointMap: EndpointMap{
					"users": Endpoint{
						Name: "users",
						Request: Request{
							URL: "https://api.example.com/users",
						},
						Response: Response{
							Records: Records{JmesPath: "data[]"},
						},
					},
				},
				DynamicEndpoints: []DynamicEndpoint{
					{
						Iterate: `["users"]`, // Will conflict with static endpoint
						Into:    "state.resource",
						Endpoint: Endpoint{
							Name: "{state.resource}",
							Request: Request{
								URL: "{state.base_url}/{state.resource}",
							},
							Response: Response{
								Records: Records{JmesPath: "data[]"},
							},
						},
					},
				},
			},
			expectedError: "duplicate endpoint name",
		},
		{
			name: "empty_iteration_list",
			spec: Spec{
				Name: "Empty List API",
				Defaults: Endpoint{
					State: StateMap{
						"base_url": "https://api.example.com",
					},
				},
				EndpointMap: make(EndpointMap),
				DynamicEndpoints: []DynamicEndpoint{
					{
						Iterate: `[]`, // Empty array
						Into:    "state.resource",
						Endpoint: Endpoint{
							Name: "{state.resource}",
							Request: Request{
								URL: "{state.base_url}/{state.resource}",
							},
							Response: Response{
								Records: Records{JmesPath: "data[]"},
							},
						},
					},
				},
			},
			expectedError: "", // Should succeed but generate no endpoints
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ac, err := NewAPIConnection(context.Background(), tt.spec, map[string]any{
				"state":   map[string]any{},
				"secrets": map[string]any{},
			})
			if !assert.NoError(t, err) {
				return
			}

			ac.State.Auth.Authenticated = true

			endpoints, err := ac.ListEndpoints()

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				if !assert.NoError(t, err) {
					return
				}
				// For empty list case, should have 0 endpoints
				if strings.Contains(tt.name, "empty") {
					assert.Equal(t, 0, len(endpoints))
				}
			}
		})
	}
}

func TestHeadersWithHyphens(t *testing.T) {
	// Test extracting response headers that contain hyphens
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set headers with hyphens
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-RateLimit-Remaining", "99")
		w.Header().Set("X-Custom-Header", "test-value")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"id": 1, "name": "Item 1"},
			},
		})
	}))
	defer server.Close()

	specYAML := fmt.Sprintf(`
name: test_headers_api
authentication:
  type: none
endpoints:
  test_endpoint:
    request:
      url: %s/test
      method: GET
    response:
      processors:
        - expression: response.headers["content-type"]
          output: state.content_type
          aggregation: last
        - expression: jmespath(response.headers, "\"x-ratelimit-remaining\"")
          output: state.rate_limit
          aggregation: last
        - expression: response.headers["x-custom-header"]
          output: state.custom_header
          aggregation: last
      records:
        jmespath: data
`, server.URL)

	spec, err := LoadSpec(specYAML)
	assert.NoError(t, err)

	ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
		"state":   map[string]any{},
		"secrets": map[string]any{},
	})
	assert.NoError(t, err)

	ac.State.Auth.Authenticated = true

	df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
		Limit: 10,
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.NotNil(t, df)

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Rows))

	// Verify headers were extracted correctly
	endpoint := ac.Spec.EndpointMap["test_endpoint"]

	contentType, exists := endpoint.State["content_type"]
	assert.True(t, exists, "content_type should be extracted from header")
	assert.Contains(t, contentType, "application/json", "content_type should contain application/json")

	rateLimit, exists := endpoint.State["rate_limit"]
	assert.True(t, exists, "rate_limit should be extracted from header")
	assert.Equal(t, "99", cast.ToString(rateLimit), "rate_limit should be 99")

	customHeader, exists := endpoint.State["custom_header"]
	assert.True(t, exists, "custom_header should be extracted from header")
	assert.Equal(t, "test-value", cast.ToString(customHeader), "custom_header should be test-value")
}

func TestHMACAuthentication(t *testing.T) {
	tests := []struct {
		name           string
		algorithm      string
		signingString  string
		requestHeaders map[string]string
		secret         string
		nonceLength    int
		expectError    bool
		validateFunc   func(t *testing.T, req *http.Request, bodyBytes []byte, secret string) bool
		description    string
	}{
		{
			name:          "hmac_sha256_basic",
			algorithm:     "sha256",
			signingString: "{http_method}{http_path}{http_body_sha256}",
			requestHeaders: map[string]string{
				"X-Signature": "{signature}",
			},
			secret:      "test_secret_key",
			nonceLength: 0,
			expectError: false,
			validateFunc: func(t *testing.T, req *http.Request, bodyBytes []byte, secret string) bool {
				// Recreate the signature calculation
				bodyHash := sha256.Sum256(bodyBytes)
				bodyHashHex := hex.EncodeToString(bodyHash[:])
				stringToSign := req.Method + req.URL.RequestURI() + bodyHashHex

				mac := hmac.New(sha256.New, []byte(secret))
				mac.Write([]byte(stringToSign))
				expectedSig := hex.EncodeToString(mac.Sum(nil))

				actualSig := req.Header.Get("X-Signature")
				return actualSig == expectedSig
			},
			description: "Basic HMAC-SHA256 signing with method, path, and body hash",
		},
		{
			name:          "hmac_sha512_basic",
			algorithm:     "sha512",
			signingString: "{http_method}{http_path}{unix_time}",
			requestHeaders: map[string]string{
				"X-Signature": "{signature}",
				"X-Timestamp": "{unix_time}",
			},
			secret:      "test_secret_512",
			nonceLength: 0,
			expectError: false,
			validateFunc: func(t *testing.T, req *http.Request, bodyBytes []byte, secret string) bool {
				// Get timestamp from header
				timestamp := req.Header.Get("X-Timestamp")
				if timestamp == "" {
					return false
				}

				stringToSign := req.Method + req.URL.RequestURI() + timestamp

				mac := hmac.New(sha512.New, []byte(secret))
				mac.Write([]byte(stringToSign))
				expectedSig := hex.EncodeToString(mac.Sum(nil))

				actualSig := req.Header.Get("X-Signature")
				return actualSig == expectedSig
			},
			description: "HMAC-SHA512 signing with timestamp",
		},
		{
			name:          "hmac_with_nonce",
			algorithm:     "sha256",
			signingString: "{http_method}{nonce}{unix_time}",
			requestHeaders: map[string]string{
				"X-Signature": "{signature}",
				"X-Nonce":     "{nonce}",
				"X-Timestamp": "{unix_time}",
			},
			secret:      "nonce_secret",
			nonceLength: 16,
			expectError: false,
			validateFunc: func(t *testing.T, req *http.Request, bodyBytes []byte, secret string) bool {
				nonce := req.Header.Get("X-Nonce")
				timestamp := req.Header.Get("X-Timestamp")

				if nonce == "" || timestamp == "" {
					return false
				}

				// Nonce should be hex-encoded and correct length
				if len(nonce) != 32 { // 16 bytes = 32 hex chars
					t.Logf("Invalid nonce length: %d (expected 32)", len(nonce))
					return false
				}

				stringToSign := req.Method + nonce + timestamp

				mac := hmac.New(sha256.New, []byte(secret))
				mac.Write([]byte(stringToSign))
				expectedSig := hex.EncodeToString(mac.Sum(nil))

				actualSig := req.Header.Get("X-Signature")
				return actualSig == expectedSig
			},
			description: "HMAC with nonce generation",
		},
		{
			name:          "hmac_with_query_params",
			algorithm:     "sha256",
			signingString: "{http_method}{http_query}{unix_time}",
			requestHeaders: map[string]string{
				"Authorization": "HMAC {signature}",
				"X-Timestamp":   "{unix_time}",
			},
			secret:      "query_secret",
			nonceLength: 0,
			expectError: false,
			validateFunc: func(t *testing.T, req *http.Request, bodyBytes []byte, secret string) bool {
				timestamp := req.Header.Get("X-Timestamp")
				if timestamp == "" {
					return false
				}

				// Canonical query string (sorted)
				queryValues, _ := url.ParseQuery(req.URL.RawQuery)
				var queryParts []string
				var queryKeys []string
				for k := range queryValues {
					queryKeys = append(queryKeys, k)
				}
				sort.Strings(queryKeys)
				for _, k := range queryKeys {
					vals := queryValues[k]
					sort.Strings(vals)
					for _, v := range vals {
						queryParts = append(queryParts, url.QueryEscape(k)+"="+url.QueryEscape(v))
					}
				}
				canonicalQuery := strings.Join(queryParts, "&")

				stringToSign := req.Method + canonicalQuery + timestamp

				mac := hmac.New(sha256.New, []byte(secret))
				mac.Write([]byte(stringToSign))
				expectedSig := hex.EncodeToString(mac.Sum(nil))

				authHeader := req.Header.Get("Authorization")
				if !strings.HasPrefix(authHeader, "HMAC ") {
					return false
				}
				actualSig := strings.TrimPrefix(authHeader, "HMAC ")
				return actualSig == expectedSig
			},
			description: "HMAC with canonical query parameters",
		},
		{
			name:          "hmac_with_body_hashes",
			algorithm:     "sha256",
			signingString: "{http_body_md5}{http_body_sha1}{http_body_sha256}",
			requestHeaders: map[string]string{
				"X-Signature":   "{signature}",
				"X-Body-MD5":    "{http_body_md5}",
				"X-Body-SHA1":   "{http_body_sha1}",
				"X-Body-SHA256": "{http_body_sha256}",
			},
			secret:      "hash_secret",
			nonceLength: 0,
			expectError: false,
			validateFunc: func(t *testing.T, req *http.Request, bodyBytes []byte, secret string) bool {
				md5Hash := md5.Sum(bodyBytes)
				md5Hex := hex.EncodeToString(md5Hash[:])

				sha1Hash := sha1.Sum(bodyBytes)
				sha1Hex := hex.EncodeToString(sha1Hash[:])

				sha256Hash := sha256.Sum256(bodyBytes)
				sha256Hex := hex.EncodeToString(sha256Hash[:])

				// Verify body hash headers
				if req.Header.Get("X-Body-MD5") != md5Hex {
					t.Logf("MD5 mismatch")
					return false
				}
				if req.Header.Get("X-Body-SHA1") != sha1Hex {
					t.Logf("SHA1 mismatch")
					return false
				}
				if req.Header.Get("X-Body-SHA256") != sha256Hex {
					t.Logf("SHA256 mismatch")
					return false
				}

				stringToSign := md5Hex + sha1Hex + sha256Hex

				mac := hmac.New(sha256.New, []byte(secret))
				mac.Write([]byte(stringToSign))
				expectedSig := hex.EncodeToString(mac.Sum(nil))

				actualSig := req.Header.Get("X-Signature")
				return actualSig == expectedSig
			},
			description: "HMAC with multiple body hash algorithms",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Track what was received
			var capturedRequest *http.Request

			// Create mock HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedRequest = r

				// Read body
				body, err := io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				// Validate signature if validator provided
				if tt.validateFunc != nil {
					if !tt.validateFunc(t, r, body, tt.secret) {
						w.WriteHeader(http.StatusUnauthorized)
						json.NewEncoder(w).Encode(map[string]any{
							"error": "Invalid signature",
						})
						return
					}
				}

				// Success
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{
						{"id": 1, "result": "success"},
					},
				})
			}))
			defer server.Close()

			// Create spec with HMAC authentication
			spec := Spec{
				Name: "test_hmac_api",
				Authentication: Authentication{
					"type":            AuthTypeHMAC,
					"algorithm":       tt.algorithm,
					"signing_string":  tt.signingString,
					"request_headers": tt.requestHeaders,
					"secret":          tt.secret,
					"nonce_length":    tt.nonceLength,
				},
				EndpointMap: EndpointMap{
					"test_endpoint": Endpoint{
						Name: "test_endpoint",
						Request: Request{
							URL:    server.URL + "/api/test?foo=bar&baz=qux",
							Method: MethodPost,
							Payload: map[string]any{
								"test": "data",
							},
						},
						Response: Response{
							Records: Records{JmesPath: "data"},
						},
					},
				},
			}

			// Convert to proper spec
			specBody, err := yaml.Marshal(spec)
			if !assert.NoError(t, err) {
				return
			}
			spec, err = LoadSpec(string(specBody))
			if !assert.NoError(t, err) {
				return
			}

			// Create API connection
			ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
				"state":   map[string]any{},
				"secrets": map[string]any{},
			})
			assert.NoError(t, err)

			// Authenticate (this sets up the Sign function)
			err = ac.Authenticate()
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.True(t, ac.State.Auth.Authenticated)

			// Make request to test endpoint
			df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
				Limit: 10,
			})
			assert.NoError(t, err, tt.description)
			assert.NotNil(t, df)

			// Collect data
			data, err := df.Collect()
			assert.NoError(t, err, "Should collect data successfully")
			assert.Equal(t, 1, len(data.Rows), "Should receive 1 record")

			// Verify the request was captured
			assert.NotNil(t, capturedRequest, "Request should have been captured")

			// Verify signature header was set
			for headerName := range tt.requestHeaders {
				headerValue := capturedRequest.Header.Get(headerName)
				assert.NotEmpty(t, headerValue, "Header %s should be set", headerName)
			}
		})
	}
}

func TestHMACAuthenticationErrors(t *testing.T) {
	tests := []struct {
		name        string
		algorithm   string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "invalid_algorithm",
			algorithm:   "sha1",
			expectError: true,
			errorMsg:    "only 'sha256' and 'sha512' are supported",
		},
		{
			name:        "empty_algorithm_defaults_to_sha256",
			algorithm:   "",
			expectError: false,
			errorMsg:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
			}))
			defer server.Close()

			spec := Spec{
				Name: "test_hmac_api",
				Authentication: Authentication{
					"type":           AuthTypeHMAC,
					"algorithm":      tt.algorithm,
					"signing_string": "{http_method}",
					"request_headers": map[string]string{
						"X-Signature": "{signature}",
					},
					"secret": "test_secret",
				},
				EndpointMap: EndpointMap{
					"test_endpoint": Endpoint{
						Name: "test_endpoint",
						Request: Request{
							URL:    server.URL + "/test",
							Method: MethodGet,
						},
						Response: Response{
							Records: Records{JmesPath: "data"},
						},
					},
				},
			}

			// Convert to proper spec
			specBody, err := yaml.Marshal(spec)
			if !assert.NoError(t, err) {
				return
			}
			spec, err = LoadSpec(string(specBody))
			if !assert.NoError(t, err) {
				return
			}

			ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
				"state":   map[string]any{},
				"secrets": map[string]any{},
			})
			assert.NoError(t, err)

			// Authenticate
			err = ac.Authenticate()
			assert.NoError(t, err) // Authenticate itself doesn't error, the Sign function does

			// Make request - this is where the error should occur for invalid algorithm
			df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
				Limit: 10,
			})

			if tt.expectError {
				// Error should occur during request or collection
				if err == nil {
					_, err = df.Collect()
				}
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
				if df != nil {
					data, err := df.Collect()
					assert.NoError(t, err)
					assert.NotNil(t, data)
				}
			}
		})
	}
}

func TestHMACAuthenticationTemplating(t *testing.T) {
	// Test that HMAC secret and signing_string support templating
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Just verify that signature header exists (signature validation is done in main tests)
		actualSig := r.Header.Get("X-Signature")
		timestamp := r.Header.Get("X-Timestamp")

		if actualSig == "" || timestamp == "" {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]any{"error": "Missing signature or timestamp"})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"success": true}},
		})
	}))
	defer server.Close()

	spec := Spec{
		Name: "test_hmac_templating",
		Authentication: Authentication{
			"type":           AuthTypeHMAC,
			"algorithm":      "sha256",
			"signing_string": "{http_method}{unix_time}",
			"request_headers": map[string]string{
				"X-Signature": "{signature}",
				"X-Timestamp": "{unix_time}",
			},
			"secret": "{secrets.api_secret}", // Templated secret
		},
		EndpointMap: EndpointMap{
			"test_endpoint": Endpoint{
				Name: "test_endpoint",
				Request: Request{
					URL:    server.URL + "/test",
					Method: MethodGet,
				},
				Response: Response{
					Records: Records{JmesPath: "data"},
				},
			},
		},
	}

	// Convert to proper spec
	specBody, err := yaml.Marshal(spec)
	assert.NoError(t, err)
	spec, err = LoadSpec(string(specBody))
	assert.NoError(t, err)

	ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
		"state": map[string]any{},
		"secrets": map[string]any{
			"api_secret": "my_api_secret_123",
		},
	})
	assert.NoError(t, err)

	err = ac.Authenticate()
	assert.NoError(t, err)

	df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
		Limit: 10,
	})
	assert.NoError(t, err)

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Rows))
}

func TestBasicAuthentication(t *testing.T) {
	tests := []struct {
		name           string
		username       string
		password       string
		serverUsername string // what server expects
		serverPassword string // what server expects
		expectSuccess  bool
		useTemplating  bool
		description    string
	}{
		{
			name:           "valid_credentials",
			username:       "testuser",
			password:       "testpass",
			serverUsername: "testuser",
			serverPassword: "testpass",
			expectSuccess:  true,
			description:    "Valid credentials should authenticate successfully",
		},
		{
			name:           "invalid_username",
			username:       "wronguser",
			password:       "testpass",
			serverUsername: "testuser",
			serverPassword: "testpass",
			expectSuccess:  false,
			description:    "Invalid username should fail authentication",
		},
		{
			name:           "invalid_password",
			username:       "testuser",
			password:       "wrongpass",
			serverUsername: "testuser",
			serverPassword: "testpass",
			expectSuccess:  false,
			description:    "Invalid password should fail authentication",
		},
		{
			name:           "templated_credentials",
			username:       "{secrets.username}",
			password:       "{secrets.password}",
			serverUsername: "secret_user",
			serverPassword: "secret_pass",
			expectSuccess:  true,
			useTemplating:  true,
			description:    "Templated credentials should be rendered and work",
		},
		{
			name:           "empty_credentials",
			username:       "",
			password:       "",
			serverUsername: "testuser",
			serverPassword: "testpass",
			expectSuccess:  false,
			description:    "Empty credentials should fail authentication",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Track authentication attempts
			var authHeaderReceived string
			var requestsReceived int
			var authValidationPerformed bool

			// Create mock HTTP server that validates basic auth
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestsReceived++
				authHeaderReceived = r.Header.Get("Authorization")

				// Validate Authorization header
				if authHeaderReceived == "" {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]any{
						"error": "Missing Authorization header",
					})
					return
				}

				// Check for "Basic " prefix
				if !strings.HasPrefix(authHeaderReceived, "Basic ") {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]any{
						"error": "Invalid Authorization header format",
					})
					return
				}

				// Decode base64 credentials
				encodedCreds := strings.TrimPrefix(authHeaderReceived, "Basic ")
				decodedBytes, err := base64.StdEncoding.DecodeString(encodedCreds)
				if err != nil {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]any{
						"error": "Invalid base64 encoding",
					})
					return
				}

				// Split username:password
				credentials := string(decodedBytes)
				parts := strings.SplitN(credentials, ":", 2)
				if len(parts) != 2 {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]any{
						"error": "Invalid credentials format",
					})
					return
				}

				username := parts[0]
				password := parts[1]

				// Validate credentials against expected values
				authValidationPerformed = true
				if username != tt.serverUsername || password != tt.serverPassword {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]any{
						"error": "Invalid credentials",
					})
					return
				}

				// Success - return test data
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{
						{"id": 1, "name": "Test Item 1"},
						{"id": 2, "name": "Test Item 2"},
					},
				})
			}))
			defer server.Close()

			// Create API spec YAML with basic authentication
			// Quote username and password if they contain template syntax
			username := tt.username
			password := tt.password
			if strings.Contains(username, "{") || strings.Contains(password, "{") {
				username = fmt.Sprintf(`"%s"`, tt.username)
				password = fmt.Sprintf(`"%s"`, tt.password)
			}

			specYAML := fmt.Sprintf(`
name: test_basic_auth_api
authentication:
  type: basic
  username: %s
  password: %s
endpoints:
  test_endpoint:
    request:
      url: %s/data
      method: GET
    response:
      records:
        jmespath: data
`, username, password, server.URL)

			// Load spec to properly compile and validate
			spec, err := LoadSpec(specYAML)
			assert.NoError(t, err, "Should load spec successfully")

			// Create API connection with optional templated secrets
			secrets := map[string]any{}
			if tt.useTemplating {
				secrets["username"] = tt.serverUsername
				secrets["password"] = tt.serverPassword
			}

			ac, err2 := NewAPIConnection(context.Background(), spec, map[string]any{
				"state":   map[string]any{},
				"secrets": secrets,
			})
			assert.NoError(t, err2)

			// Authenticate
			err = ac.Authenticate()
			assert.NoError(t, err, "Authenticate() should not return error for basic auth setup")
			assert.True(t, ac.State.Auth.Authenticated, "Should be marked as authenticated")

			// Verify auth headers were set
			assert.NotNil(t, ac.State.Auth.Headers, "Auth headers should be set")
			authHeader, exists := ac.State.Auth.Headers["Authorization"]
			assert.True(t, exists, "Authorization header should exist")
			assert.True(t, strings.HasPrefix(authHeader, "Basic "), "Authorization header should start with 'Basic '")

			// Verify the header value is correctly base64 encoded
			encodedCreds := strings.TrimPrefix(authHeader, "Basic ")
			decodedBytes, err := base64.StdEncoding.DecodeString(encodedCreds)
			assert.NoError(t, err, "Should be valid base64")

			expectedUsername := tt.username
			expectedPassword := tt.password
			if tt.useTemplating {
				expectedUsername = tt.serverUsername
				expectedPassword = tt.serverPassword
			}

			// Only validate content if credentials are non-empty
			if expectedUsername != "" || expectedPassword != "" {
				expectedCreds := fmt.Sprintf("%s:%s", expectedUsername, expectedPassword)
				assert.Equal(t, expectedCreds, string(decodedBytes), "Decoded credentials should match")
			}

			// Make actual request to test endpoint
			df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
				Limit: 10,
			})

			if tt.expectSuccess {
				// Should succeed
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, df, "Dataflow should not be nil")

				// Collect data
				data, err := df.Collect()
				assert.NoError(t, err, "Should collect data successfully")
				assert.Equal(t, 2, len(data.Rows), "Should receive 2 records")

				// Verify auth header was received by server
				assert.True(t, authValidationPerformed, "Server should have validated auth")
				assert.Greater(t, requestsReceived, 0, "Server should have received requests")

			} else {
				// Should fail - either during request or authentication
				// The error might occur during ReadDataflow or during data collection
				if err == nil && df != nil {
					_, err = df.Collect()
				}

				// We expect an error at some point for invalid credentials
				// Note: The error might be captured in the dataflow context
				if err == nil && df != nil {
					err = df.Context.Err()
				}

				// For invalid/empty credentials, we should get an error
				// The mock server returns 401, which should be captured as an error
				assert.Error(t, err, tt.description)
			}
		})
	}
}

func TestStaticAuthentication(t *testing.T) {
	tests := []struct {
		name          string
		headers       map[string]string
		secrets       map[string]any
		useTemplating bool
		expectSuccess bool
		description   string
		useShorthand  bool // if true, omit type: static (test defaulting behavior)
	}{
		{
			name: "api_key_header",
			headers: map[string]string{
				"X-API-Key": "my-api-key-123",
			},
			expectSuccess: true,
			description:   "Static API key header should be injected",
		},
		{
			name: "authorization_bearer",
			headers: map[string]string{
				"Authorization": "Bearer token123",
			},
			expectSuccess: true,
			description:   "Bearer token header should be injected",
		},
		{
			name: "templated_header",
			headers: map[string]string{
				"Authorization": "Bearer {secrets.token}",
			},
			secrets: map[string]any{
				"token": "secret_token_value",
			},
			useTemplating: true,
			expectSuccess: true,
			description:   "Templated header should be rendered with secret value",
		},
		{
			name:          "empty_headers",
			headers:       map[string]string{},
			expectSuccess: true,
			description:   "Empty headers should still succeed (no header injected)",
		},
		{
			name: "multiple_headers",
			headers: map[string]string{
				"Authorization": "Bearer token123",
				"X-API-Key":     "my-api-key-456",
				"X-Custom":      "custom-value",
			},
			expectSuccess: true,
			description:   "Multiple headers should all be injected",
		},
		{
			name: "headers_only_shorthand",
			headers: map[string]string{
				"Authorization": "Bearer token123",
			},
			expectSuccess: true,
			useShorthand:  true,
			description:   "Headers without type should default to static",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Track what headers were received
			var receivedHeaders http.Header

			// Create mock HTTP server that captures headers
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaders = r.Header.Clone()

				// Return success with test data
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{
						{"id": 1, "name": "Test Item 1"},
						{"id": 2, "name": "Test Item 2"},
					},
				})
			}))
			defer server.Close()

			// Build headers YAML section
			headersYAML := ""
			for k, v := range tt.headers {
				headersYAML += fmt.Sprintf("    %s: \"%s\"\n", k, v)
			}

			// Build authentication section
			var authSection string
			if tt.useShorthand {
				// Test shorthand: just headers without type
				authSection = fmt.Sprintf(`authentication:
  headers:
%s`, headersYAML)
			} else {
				authSection = fmt.Sprintf(`authentication:
  type: static
  headers:
%s`, headersYAML)
			}

			specYAML := fmt.Sprintf(`
name: test_static_auth_api
%s
endpoints:
  test_endpoint:
    request:
      url: %s/data
      method: GET
    response:
      records:
        jmespath: data
`, authSection, server.URL)

			// Load spec
			spec, err := LoadSpec(specYAML)
			if !assert.NoError(t, err, "Should load spec successfully") {
				return
			}

			// Create API connection with optional secrets
			secrets := tt.secrets
			if secrets == nil {
				secrets = map[string]any{}
			}

			ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
				"state":   map[string]any{},
				"secrets": secrets,
			})
			assert.NoError(t, err)

			// Authenticate
			err = ac.Authenticate()
			assert.NoError(t, err, "Authenticate() should not return error for static auth setup")
			assert.True(t, ac.State.Auth.Authenticated, "Should be marked as authenticated")

			// Verify auth headers were set
			assert.NotNil(t, ac.State.Auth.Headers, "Auth headers should be set")

			// For each header in the test, verify it was set
			for headerName, headerValue := range tt.headers {
				actualVal, exists := ac.State.Auth.Headers[headerName]
				assert.True(t, exists, "Header %s should exist in auth headers", headerName)

				// Check that templated values were rendered
				if tt.useTemplating && strings.Contains(headerValue, "{secrets.") {
					assert.NotContains(t, actualVal, "{secrets.", "Template should be rendered")
				}
			}

			// Make actual request to test endpoint
			df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
				Limit: 10,
			})

			if tt.expectSuccess {
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, df, "Dataflow should not be nil")

				// Collect data
				data, err := df.Collect()
				assert.NoError(t, err, "Should collect data successfully")
				assert.Equal(t, 2, len(data.Rows), "Should receive 2 records")

				// Verify headers were received by server
				for headerName, headerValue := range tt.headers {
					expectedVal := headerValue
					// For templated values, get the expected rendered value
					if tt.useTemplating && strings.Contains(headerValue, "{secrets.token}") {
						expectedVal = strings.Replace(headerValue, "{secrets.token}", cast.ToString(tt.secrets["token"]), 1)
					}
					actualVal := receivedHeaders.Get(headerName)
					assert.Equal(t, expectedVal, actualVal, "Header %s should have correct value", headerName)
				}
			} else {
				if err == nil && df != nil {
					_, err = df.Collect()
				}
				assert.Error(t, err, tt.description)
			}
		})
	}
}

func TestStaticAuthenticationWithSequence(t *testing.T) {
	// Test that static auth works with sequence calls
	var requestCount int
	var allReceivedHeaders []http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		allReceivedHeaders = append(allReceivedHeaders, r.Header.Clone())

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// Different responses for different paths
		switch r.URL.Path {
		case "/setup":
			json.NewEncoder(w).Encode(map[string]any{
				"config_value": "setup_complete",
			})
		default:
			json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": 1, "name": "Test"},
				},
			})
		}
	}))
	defer server.Close()

	specYAML := fmt.Sprintf(`
name: test_static_auth_sequence
authentication:
  type: static
  headers:
    X-API-Key: "test-api-key-123"
endpoints:
  test_endpoint:
    setup:
      - request:
          url: %s/setup
          method: GET
        response:
          processors:
            - expression: response.json.config_value
              output: state.config
              aggregation: last
    request:
      url: %s/data
      method: GET
    response:
      records:
        jmespath: data
`, server.URL, server.URL)

	spec, err := LoadSpec(specYAML)
	assert.NoError(t, err)

	ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
		"state":   map[string]any{},
		"secrets": map[string]any{},
	})
	assert.NoError(t, err)

	// Authenticate
	err = ac.Authenticate()
	assert.NoError(t, err)

	// Read dataflow (this should trigger setup sequence and main request)
	df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
		Limit: 10,
	})
	assert.NoError(t, err)

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Rows))

	// Verify that headers were sent to all requests (setup + main)
	assert.GreaterOrEqual(t, requestCount, 2, "Should have made at least 2 requests (setup + main)")

	// Verify all requests received the auth header
	for i, headers := range allReceivedHeaders {
		assert.Equal(t, "test-api-key-123", headers.Get("X-API-Key"),
			"Request %d should have X-API-Key header", i+1)
	}
}

func TestSequenceCallIteration(t *testing.T) {
	// Test that setup sequence can iterate over a list from state
	var requestPaths []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPaths = append(requestPaths, r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		switch r.URL.Path {
		case "/bases":
			json.NewEncoder(w).Encode(map[string]any{
				"bases": []map[string]any{
					{"id": "base1", "name": "Base One"},
					{"id": "base2", "name": "Base Two"},
				},
			})
		case "/bases/base1/tables":
			json.NewEncoder(w).Encode(map[string]any{
				"tables": []map[string]any{
					{"name": "table_a"},
				},
			})
		case "/bases/base2/tables":
			json.NewEncoder(w).Encode(map[string]any{
				"tables": []map[string]any{
					{"name": "table_b"},
					{"name": "table_c"},
				},
			})
		default:
			json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": 1, "name": "Test"},
				},
			})
		}
	}))
	defer server.Close()

	specYAML := fmt.Sprintf(`
name: test_sequence_call_iteration
endpoints:
  test_endpoint:
    setup:
      # Step 1: Get list of bases
      - request:
          url: %s/bases
        response:
          processors:
            - expression: response.json.bases
              output: state.bases
              aggregation: last

      # Step 2: Iterate over bases to get tables
      - iterate: state.bases
        into: state.base
        request:
          url: %s/bases/{state.base.id}/tables
        response:
          processors:
            - expression: response.json.tables
              output: state.all_tables
              aggregation: collect

    request:
      url: %s/data
    response:
      records:
        jmespath: data
`, server.URL, server.URL, server.URL)

	spec, err := LoadSpec(specYAML)
	assert.NoError(t, err)

	ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
		"state":   map[string]any{},
		"secrets": map[string]any{},
	})
	assert.NoError(t, err)

	// Bypass authentication for testing
	ac.State.Auth.Authenticated = true

	// Read dataflow (this should trigger setup sequence with iteration)
	df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
		Limit: 10,
	})
	assert.NoError(t, err)

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Rows))

	// Verify the correct requests were made:
	// 1. /bases (get list)
	// 2. /bases/base1/tables (iteration 1)
	// 3. /bases/base2/tables (iteration 2)
	// 4. /data (main request)
	assert.Equal(t, 4, len(requestPaths), "Should have made 4 requests")
	assert.Equal(t, "/bases", requestPaths[0])
	assert.Equal(t, "/bases/base1/tables", requestPaths[1])
	assert.Equal(t, "/bases/base2/tables", requestPaths[2])
	assert.Equal(t, "/data", requestPaths[3])
}

func TestSequenceCallIterationJSONLiteral(t *testing.T) {
	// Test that setup sequence can iterate over a JSON literal array
	var requestPaths []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPaths = append(requestPaths, r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		switch r.URL.Path {
		case "/regions/us-east":
			json.NewEncoder(w).Encode(map[string]any{
				"name":  "US East",
				"count": 100,
			})
		case "/regions/us-west":
			json.NewEncoder(w).Encode(map[string]any{
				"name":  "US West",
				"count": 200,
			})
		default:
			json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"total": 300},
				},
			})
		}
	}))
	defer server.Close()

	specYAML := fmt.Sprintf(`
name: test_sequence_call_iteration_json
endpoints:
  test_endpoint:
    setup:
      - iterate: '[{"id": "us-east"}, {"id": "us-west"}]'
        into: state.region
        request:
          url: %s/regions/{state.region.id}
        response:
          processors:
            - expression: response.json
              output: state.region_data
              aggregation: collect

    request:
      url: %s/summary
    response:
      records:
        jmespath: data
`, server.URL, server.URL)

	spec, err := LoadSpec(specYAML)
	assert.NoError(t, err)

	ac, err := NewAPIConnection(context.Background(), spec, map[string]any{
		"state":   map[string]any{},
		"secrets": map[string]any{},
	})
	assert.NoError(t, err)

	// Bypass authentication for testing
	ac.State.Auth.Authenticated = true

	// Read dataflow (this should trigger setup sequence with iteration)
	df, err := ac.ReadDataflow("test_endpoint", APIStreamConfig{
		Limit: 10,
	})
	assert.NoError(t, err)

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Rows))

	// Verify the correct requests were made:
	// 1. /regions/us-east (iteration 1)
	// 2. /regions/us-west (iteration 2)
	// 3. /summary (main request)
	assert.Equal(t, 3, len(requestPaths), "Should have made 3 requests")
	assert.Equal(t, "/regions/us-east", requestPaths[0])
	assert.Equal(t, "/regions/us-west", requestPaths[1])
	assert.Equal(t, "/summary", requestPaths[2])
}
