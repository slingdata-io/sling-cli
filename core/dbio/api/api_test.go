package api

import (
	"context"
	"testing"

	"github.com/flarco/g"
	"github.com/maja42/goval"
	"github.com/stretchr/testify/assert"
)

func TestExtractVars(t *testing.T) {
	type testCase struct {
		name     string
		expr     string
		expected []string
	}

	tests := []testCase{
		{
			name:     "empty_string",
			expr:     ``,
			expected: []string{},
		},
		{
			name:     "no_references",
			expr:     `value(123, 456, "2025-01-01")`,
			expected: []string{},
		},
		{
			name:     "simple_env_reference",
			expr:     `value(env.START_DATE, "2025-01-01")`,
			expected: []string{"env.START_DATE"},
		},
		{
			name:     "simple_state_reference",
			expr:     `value(state.max_start_time, "2025-01-01")`,
			expected: []string{"state.max_start_time"},
		},
		{
			name:     "simple_secrets_reference",
			expr:     `value(secrets.API_KEY, "default-key")`,
			expected: []string{"secrets.API_KEY"},
		},
		{
			name:     "simple_auth_reference",
			expr:     `value(auth.token, "default-token")`,
			expected: []string{"auth.token"},
		},
		{
			name:     "multiple_references",
			expr:     `value(env.START_DATE, state.max_start_time, "2025-01-01")`,
			expected: []string{"env.START_DATE", "state.max_start_time"},
		},
		{
			name:     "references_with_quotes",
			expr:     `log("auth.token: " + auth.token)`,
			expected: []string{"auth.token"},
		},
		{
			name:     "references_in_quotes",
			expr:     `log("env.DEBUG should not be extracted but " + env.DEBUG + " should")`,
			expected: []string{"env.DEBUG"},
		},
		{
			name:     "reference_in_the_middle",
			expr:     `concat("prefix_", state.user_id, "_suffix")`,
			expected: []string{"state.user_id"},
		},
		{
			name:     "nested_functions",
			expr:     `value(env.END_DATE, date_format(now(), "%Y-%m-%dT%H:%M:%S.%fZ"))`,
			expected: []string{"env.END_DATE"},
		},
		{
			name:     "complex_expression",
			expr:     `if(is_null(state.last_run_date), now(), date_add(state.last_run_date, "1d"))`,
			expected: []string{"state.last_run_date", "state.last_run_date"},
		},
		{
			name:     "reference_with_underscore",
			expr:     `value(state.last_sync_time, state.default_time)`,
			expected: []string{"state.last_sync_time", "state.default_time"},
		},
		{
			name:     "reference_with_numbers",
			expr:     `value(env.API_KEY2, secrets.BACKUP_KEY1)`,
			expected: []string{"env.API_KEY2", "secrets.BACKUP_KEY1"},
		},
		{
			name:     "parameter_inside_quotes",
			expr:     `format("The value of state.count is {}", state.count1)`,
			expected: []string{"state.count1"},
		},
		{
			name:     "escaped_quotes",
			expr:     `value(state.query, "SELECT * FROM \"table\" WHERE id = 5")`,
			expected: []string{"state.query"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractVars(tt.expr)
			assert.ElementsMatch(t, tt.expected, result, "References should match expected values")
		})
	}
}

func TestAPIConnectionRender(t *testing.T) {
	// Test cases for the render function
	tests := []struct {
		name        string
		input       string
		expected    string
		setup       func(*APIConnection)
		epState     map[string]any
		expectError bool
	}{
		{
			name:     "simple_state_variable",
			input:    "Hello, ${state.name}!",
			expected: "Hello, World!",
			setup: func(ac *APIConnection) {
				ac.State.State["name"] = "World"
			},
		},
		{
			name:     "environment_variable",
			input:    "Environment: ${env.TEST_VAR}",
			expected: "Environment: test_value",
			setup: func(ac *APIConnection) {
				ac.State.Env["TEST_VAR"] = "test_value"
			},
		},
		{
			name:     "secret_variable",
			input:    "API Key: ${secrets.API_KEY}",
			expected: "API Key: secret123",
			setup: func(ac *APIConnection) {
				ac.State.Secrets["API_KEY"] = "secret123"
			},
		},
		{
			name:     "auth_variable",
			input:    "Token: ${auth.token}",
			expected: "Token: my_token",
			setup: func(ac *APIConnection) {
				ac.State.Auth.Token = "my_token"
			},
		},
		{
			name:     "multiple_variables",
			input:    "From ${env.ORIGIN} to ${state.destination}",
			expected: "From Earth to Mars",
			setup: func(ac *APIConnection) {
				ac.State.Env["ORIGIN"] = "Earth"
				ac.State.State["destination"] = "Mars"
			},
		},
		{
			name:     "endpoint_state_override",
			input:    "Value: ${state.counter}",
			expected: "Value: 10",
			setup: func(ac *APIConnection) {
				ac.State.State["counter"] = 5
			},
			epState: map[string]any{"counter": 10},
		},
		{
			name:     "object_serialization",
			input:    "User: ${state.user}",
			expected: "User: {\"age\":30,\"name\":\"Alice\"}",
			setup: func(ac *APIConnection) {
				ac.State.State["user"] = map[string]any{"name": "Alice", "age": 30}
			},
		},
		{
			name:     "array_serialization",
			input:    "Items: ${state.items}",
			expected: "Items: [\"one\",\"two\",\"three\"]",
			setup: func(ac *APIConnection) {
				ac.State.State["items"] = []string{"one", "two", "three"}
			},
		},
		{
			name:     "if_function",
			input:    "Result: ${if(true, 3, 0)}",
			expected: "Result: 3",
		},
		{
			name:     "now_function",
			input:    "Current time: ${now()}",
			expected: "Current time: ",
		},
		{
			name:     "non_existent_var",
			input:    `Missing: ${ log("state.missing = " + state.missing) }`,
			expected: "Missing: state.missing = nil",
		},
		{
			name:     "jmespath_lookup",
			input:    "Name: ${ state.nested.name }",
			expected: "Name: John",
			setup: func(ac *APIConnection) {
				ac.State.State["nested"] = map[string]any{"name": "John", "age": 42}
			},
		},
		{
			name:        "invalid_function",
			input:       "Invalid: ${invalid_function()}",
			expected:    "",
			expectError: true,
		},
		{
			name:        "coalesce",
			input:       `${ coalesce(env.START_DATE, state.start_time, "2025-01-01") }`,
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
				Spec: Spec{},
				eval: goval.NewEvaluator(),
			}

			// Configure the test state
			if tt.setup != nil {
				tt.setup(ac)
			}

			// Process using our mock implementation
			var result string
			var err error

			// Use the mock implementation that correctly handles state
			if tt.epState != nil {
				result, err = ac.renderString(tt.input, g.M("state", tt.epState))
			} else {
				result, err = ac.renderString(tt.input)
			}

			// Check for expected errors
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// For time-based functions, just check that something was rendered
			if tt.name == "now_function" {
				assert.NotEqual(t, tt.input, result)
				assert.Contains(t, result, "Current time: ")
				return
			}

			// Check the result
			if !assert.Equal(t, tt.expected, result) {
				sm := ac.getStateMap(nil, nil)
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
				Spec: Spec{Authentication: tt.auth},
				eval: goval.NewEvaluator(),
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
		eval: goval.NewEvaluator(),
	}

	// Test template rendering in OAuth2 fields
	ac.State.Secrets["CLIENT_ID"] = "secret_client_123"
	ac.State.Secrets["CLIENT_SECRET"] = "secret_value_456"

	auth := Authentication{
		Type:              AuthTypeOAuth2,
		Flow:              "client_credentials",
		ClientID:          "${secrets.CLIENT_ID}",
		ClientSecret:      "${secrets.CLIENT_SECRET}",
		AuthenticationURL: "https://api.example.com/oauth/token",
	}

	// This should not error during validation (only during actual HTTP request)
	_, err := ac.performOAuth2Flow(auth)

	// We expect this to fail at HTTP request stage, not validation
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute OAuth2 request")
}
