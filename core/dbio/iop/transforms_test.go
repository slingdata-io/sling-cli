package iop

import (
	"os"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestNonPrintable(t *testing.T) {
	chars := []string{"\x00", "\u00A0", " ", "\t", "\n", "\x01"}
	for _, char := range chars {
		g.Info("%#v => %d => %#v => %#v", char, char[0], char[0], Transforms.ReplaceNonPrintable(char))
	}
	uints := []uint8{0, 1, 2, 3, 49, 127, 160}
	for _, uintVal := range uints {
		g.Warn("%#v => %d => %#v", string(uintVal), uintVal, Transforms.ReplaceNonPrintable(string(uintVal)))
	}
}

func TestFIX(t *testing.T) {
	messages := []string{
		"8=FIX.4.2|9=332|35=8|49=XXX|56=SYS1|34=190|52=20181106-08:00:23|128=802c88|1=802c88_ISA|6=1.2557|11=7314956|14=12|15=GBP|17=EAVVA18KA1117184|20=0|22=4|30=XLON|31=1.2557|32=12|37=OAVVA18KA8302522|38=12|39=2|40=1|48=JE00B6173J15|54=2|55=GCP|59=1|60=20181106-08:00:21|63=6|64=20181108|76=CSTEGB21|110=0|119=15.0684|120=GBP|150=2|151=0|167=CS|207=XLON|10=105|",
		"8=FIX.4.2|9=393|35=8|49=XXX|56=SYS1|34=191|52=20181106-08:00:33|128=802c11|1=569_C11_TPAB|6=0.2366|11=16669868|14=6061|15=GBP|17=EBSTI18KA1117185|20=0|21=2|22=4|30=XOFF|31=0.2366|32=6061|37=OBSTI18KA8302657|38=6061|39=2|40=2|44=0.2366|48=GB00B0DG3H29|54=1|55=SXX|59=6|60=20181106-08:00:31|63=3|64=20181108|76=WNTSGB2LBIC|110=0|119=1434.03|120=GBP|126=20181106-23:00:00|150=2|151=0|152=1434.03|167=CS|207=XLON|10=178|",
		"8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=062|",
		"8=FIX.4.2 | 9=178 | 35=8 | 49=PHLX | 56=PERS | 52=20071123-05:30:00.000 | 11=ATOMNOCCC9990900 | 20=3 | 150=E | 39=E | 55=MSFT | 167=CS | 54=1 | 38=15 | 40=2 | 44=15 | 58=PHLX EQUITY TESTING | 59=0 | 47=C | 32=0 | 31=0 | 151=15 | 14=0 | 6=0 | 10=128 |",
		"8=FIX.4.09=12835=D34=249=TW52=20060102-15:04:0556=ISLD115=116=CS128=MG129=CB11=ID21=338=10040=w54=155=INTC60=20060102-15:04:0510=123",
	}
	for i, message := range messages {
		fixMap, err := Transforms.ParseFIXMap(message)
		g.LogFatal(err)

		switch i {
		case 0:
			assert.Contains(t, fixMap, "account")
			assert.Contains(t, fixMap, "avg_px")
		case 1:
			assert.Contains(t, fixMap, "account")
			assert.Contains(t, fixMap, "settl_curr_amt")
		case 3:
			assert.Contains(t, fixMap, "begin_string")
			assert.Contains(t, fixMap, "sending_time")
		case 4:
			assert.Contains(t, fixMap, "cl_ord_id")
			assert.Contains(t, fixMap, "deliver_to_sub_id")
		}
		// g.Info("%s", g.Marshal(fixMap))
	}
}

func TestDecode(t *testing.T) {
	filePath := "test/my_file.utf16.csv"
	bytes, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	for i, r := range bytes {
		if i > 6 {
			break
		}
		g.Info("%#v, %#v, %d", string(r), r, r)
	}
}

func TestTransformMsUUID(t *testing.T) {
	uuidBytes := []byte{0x78, 0x56, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc}
	sp := NewStreamProcessor()
	val, _ := Transforms.ParseMsUUID(sp, cast.ToString(uuidBytes))
	assert.Equal(t, "12345678-1234-1234-1234-123456789abc", val)
}

func TestBinaryToDecimal(t *testing.T) {
	sp := NewStreamProcessor()

	// Test cases for various BIT sizes
	testCases := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "BIT(1) - 0",
			input:    []byte{0x00},
			expected: "0",
		},
		{
			name:     "BIT(1) - 1",
			input:    []byte{0x01},
			expected: "1",
		},
		{
			name:     "BIT(8) - 255",
			input:    []byte{0xFF},
			expected: "255",
		},
		{
			name:     "BIT(16) - 65535",
			input:    []byte{0xFF, 0xFF},
			expected: "65535",
		},
		{
			name:     "BIT(24) - 16777215",
			input:    []byte{0xFF, 0xFF, 0xFF},
			expected: "16777215",
		},
		{
			name:     "BIT(32) - 4294967295",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF},
			expected: "4294967295",
		},
		{
			name:     "BIT(64) - max value",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			expected: "18446744073709551615",
		},
		{
			name:     "BIT(8) - binary 10101010",
			input:    []byte{0xAA}, // binary 10101010
			expected: "170",
		},
		{
			name:     "BIT(16) - binary pattern",
			input:    []byte{0x12, 0x34}, // 0x1234 = 4660
			expected: "4660",
		},
		{
			name:     "Regular text should not be converted",
			input:    []byte("hello"),
			expected: "hello",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := Transforms.BinaryToDecimal(sp, string(tc.input))
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, val, "Failed for test case: %s", tc.name)
		})
	}
}

func TestBinaryToHex(t *testing.T) {
	// Test cases for ToHex transform
	testCases := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "Empty input",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "Single byte - 0x00",
			input:    []byte{0x00},
			expected: "00",
		},
		{
			name:     "Single byte - 0x01",
			input:    []byte{0x01},
			expected: "01",
		},
		{
			name:     "Single byte - 0xFF",
			input:    []byte{0xFF},
			expected: "FF",
		},
		{
			name:     "Two bytes - 0x1234",
			input:    []byte{0x12, 0x34},
			expected: "1234",
		},
		{
			name:     "Four bytes - 0xDEADBEEF",
			input:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
			expected: "DEADBEEF",
		},
		{
			name:     "Text - Hello",
			input:    []byte("Hello"),
			expected: "48656C6C6F",
		},
		{
			name:     "Eight bytes - all 0xFF",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			expected: "FFFFFFFFFFFFFFFF",
		},
		{
			name:     "Alternating pattern",
			input:    []byte{0xAA, 0x55, 0xAA, 0x55},
			expected: "AA55AA55",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Transforms.BinaryToHex(string(tc.input))
			assert.Equal(t, tc.expected, result, "Failed for test case: %s", tc.name)
		})
	}
}

func TestEvaluator(t *testing.T) {

	// Test cases for the Evaluator
	tests := []struct {
		name        string
		input       any
		expected    any
		state       map[string]any
		extraState  map[string]any
		expectError bool
	}{
		// Basic variable substitution tests
		{
			name:     "simple_state_variable",
			input:    "Hello, {state.name}!",
			expected: "Hello, World!",
			state: map[string]any{
				"state": map[string]any{"name": "World"},
			},
		},
		{
			name:     "environment_variable",
			input:    "Environment: {env.TEST_VAR}",
			expected: "Environment: test_value",
			state: map[string]any{
				"env": map[string]any{"TEST_VAR": "test_value"},
			},
		},
		{
			name:     "store_variable",
			input:    "Stored value: {store.data}",
			expected: "Stored value: stored_data",
			state: map[string]any{
				"store": map[string]any{"data": "stored_data"},
			},
		},
		{
			name:     "env_and_store_together",
			input:    "Config: {env.MODE} with cache: {store.cache_enabled}",
			expected: "Config: production with cache: true",
			state: map[string]any{
				"env":   map[string]any{"MODE": "production"},
				"store": map[string]any{"cache_enabled": true},
			},
		},
		{
			name:     "multiple_variables",
			input:    "From {env.ORIGIN} to {state.destination}",
			expected: "From Earth to Mars",
			state: map[string]any{
				"env":   map[string]any{"ORIGIN": "Earth"},
				"state": map[string]any{"destination": "Mars"},
			},
		},
		{
			name:     "state_override_with_extras",
			input:    "Value: {state.counter}",
			expected: "Value: 10",
			state: map[string]any{
				"state": map[string]any{"counter": 5},
			},
			extraState: map[string]any{
				"state": map[string]any{"counter": 10},
			},
		},

		// Direct value rendering tests
		{
			name:     "direct_integer_value",
			input:    "{ state.counter }",
			expected: 5,
			state: map[string]any{
				"state": map[string]any{"counter": 5},
			},
		},
		{
			name:     "direct_boolean_value",
			input:    "{ state.active }",
			expected: true,
			state: map[string]any{
				"state": map[string]any{"active": true},
			},
		},
		{
			name:     "direct_float_value",
			input:    "{ state.price }",
			expected: 19.99,
			state: map[string]any{
				"state": map[string]any{"price": 19.99},
			},
		},

		// Map and nested structures
		{
			name:     "render_map_value",
			input:    g.M("counter", "{ state.counter }"),
			expected: g.M("counter", 5),
			state: map[string]any{
				"state": map[string]any{"counter": 5},
			},
		},
		{
			name:     "render_nested_map",
			input:    g.M("pagination", g.M("limit", "{ state.limit }", "offset", "{ state.offset }", "limit_str", `{ cast(state.limit, "string") }`, "name", "{ env.name }")),
			expected: g.M("pagination", g.M("limit", 10, "offset", 0, "limit_str", "10", "name", "oops")),
			state: map[string]any{
				"state": map[string]any{"limit": 10, "offset": 0},
				"env":   map[string]any{"name": "oops"},
			},
		},
		{
			name:     "render_array",
			input:    []string{"{ state.first }", "{ state.second }"},
			expected: []any{"one", "two"},
			state: map[string]any{
				"state": map[string]any{"first": "one", "second": "two"},
			},
		},

		// JSON serialization tests
		{
			name:     "object_serialization",
			input:    "User: {state.user}",
			expected: "User: {\"age\":30,\"name\":\"Alice\"}",
			state: map[string]any{
				"state": map[string]any{"user": map[string]any{"name": "Alice", "age": 30}},
			},
		},
		{
			name:     "array_serialization",
			input:    "Items: {state.items}",
			expected: "Items: [\"one\",\"two\",\"three\"]",
			state: map[string]any{
				"state": map[string]any{"items": []string{"one", "two", "three"}},
			},
		},

		// JMESPath lookups
		{
			name:     "jmespath_nested_lookup",
			input:    "Name: { state.nested.name }",
			expected: "Name: John",
			state: map[string]any{
				"state": map[string]any{"nested": map[string]any{"name": "John", "age": 42}},
			},
		},
		{
			name:     "jmespath_array_index",
			input:    "First item: { state.items[0] }",
			expected: "First item: apple",
			state: map[string]any{
				"state": map[string]any{"items": []string{"apple", "banana", "cherry"}},
			},
		},
		{
			name:     "jmespath_array_slice",
			input:    "{ state.numbers[1:3] }",
			expected: []any{2, 3},
			state: map[string]any{
				"state": map[string]any{"numbers": []int{1, 2, 3, 4, 5}},
			},
		},

		// Expression evaluation tests
		{
			name:     "arithmetic_expression",
			input:    "Total: { state.price * state.quantity }",
			expected: "Total: 59.97",
			state: map[string]any{
				"state": map[string]any{"price": 19.99, "quantity": 3},
			},
		},
		{
			name:     "comparison_expression",
			input:    "Is valid: { state.age >= 18 }",
			expected: "Is valid: true",
			state: map[string]any{
				"state": map[string]any{"age": 21},
			},
		},
		{
			name:     "logical_expression",
			input:    "Access: { state.authenticated && state.authorized }",
			expected: "Access: true",
			state: map[string]any{
				"state": map[string]any{"authenticated": true, "authorized": true},
			},
		},
		{
			name:     "ternary_expression",
			input:    "Status: { if(state.active, \"ON\", \"OFF\") }",
			expected: "Status: ON",
			state: map[string]any{
				"state": map[string]any{"active": true},
			},
		},

		// Function calls
		{
			name:     "if_function",
			input:    "Result: { if(true, 3, 0) }",
			expected: "Result: 3",
			state:    map[string]any{},
		},
		{
			name:     "coalesce_function",
			input:    "{ coalesce(\"2025-01-01\") }",
			expected: "2025-01-01",
			state:    map[string]any{},
		},
		{
			name:     "value_function_with_state",
			input:    "{ coalesce(state.value, \"default\") }",
			expected: "provided",
			state:    map[string]any{"state": map[string]any{"value": "provided"}},
		},
		{
			name:     "string_concatenation",
			input:    "{ \"Hello, \" + state.name + \"!\" }",
			expected: "Hello, Alice!",
			state: map[string]any{
				"state": map[string]any{"name": "Alice"},
			},
		},

		// Edge cases
		{
			name:     "empty_string_input",
			input:    "",
			expected: "",
			state:    map[string]any{},
		},
		{
			name:     "no_brackets",
			input:    "No variables here",
			expected: "No variables here",
			state:    map[string]any{},
		},
		{
			name:     "non_existent_variable_1",
			input:    "{ state.missing }",
			expected: nil,
			state:    map[string]any{"state": map[string]any{}},
		},
		{
			name:     "non_existent_variable_2",
			input:    "Value: { state.missing }",
			expected: "Value: ",
			state:    map[string]any{"state": map[string]any{}},
		},
		{
			name:     "non_existent_variable_3",
			input:    "Foo: { state.foo } | Bar: { env.bar } ",
			expected: "Foo:  | Bar:  ",
			state:    map[string]any{},
		},
		{
			name:     "nested_brackets",
			input:    "Nested: { state.prop1 }",
			expected: "Nested: value1",
			state: map[string]any{
				"state": map[string]any{"key": "prop1", "prop1": "value1"},
			},
		},

		// Error cases
		{
			name:        "invalid_function",
			input:       "Invalid: {invalid_function()}",
			expected:    "Invalid: {invalid_function()}",
			state:       map[string]any{},
			expectError: false, // Unknown functions are not replaced
		},
		{
			name:        "invalid_expression_syntax",
			input:       "Bad: { state.value ++ }",
			expected:    "",
			state:       map[string]any{"state": map[string]any{"value": 5}},
			expectError: true,
		},

		// NoCompute flag test - currently the flag doesn't prevent computation for non-function expressions
		{
			name:     "no_compute_flag",
			input:    "Expression: { state.value * 2 }",
			expected: "Expression: 10",
			state: map[string]any{
				"state": map[string]any{"value": 5},
			},
			extraState: map[string]any{
				"__sling_no_compute__": true,
			},
		},

		// Additional test cases for enhanced coverage
		{
			name:     "multiple_brackets_in_string",
			input:    "User {state.user} logged in at {env.TIME} from {state.location}",
			expected: "User alice logged in at 14:30 from NYC",
			state: map[string]any{
				"state": map[string]any{"user": "alice", "location": "NYC"},
				"env":   map[string]any{"TIME": "14:30"},
			},
		},
		{
			name:     "nested_state_access",
			input:    "Config: {state.config.database.host}:{state.config.database.port}",
			expected: "Config: localhost:5432",
			state: map[string]any{
				"state": map[string]any{
					"config": map[string]any{
						"database": map[string]any{
							"host": "localhost",
							"port": 5432,
						},
					},
				},
			},
		},
		{
			name:     "array_access_with_jmespath",
			input:    "First user: {state.users[0].name}, Last user: {state.users[-1].name}",
			expected: "First user: Alice, Last user: Charlie",
			state: map[string]any{
				"state": map[string]any{
					"users": []map[string]any{
						{"name": "Alice", "id": 1},
						{"name": "Bob", "id": 2},
						{"name": "Charlie", "id": 3},
					},
				},
			},
		},
		{
			name:     "jmespath_filtering",
			input:    "Active users: { state.users[?active == `true`].name }",
			expected: "Active users: [\"Alice\",\"Charlie\"]",
			state: map[string]any{
				"state": map[string]any{
					"users": []map[string]any{
						{"name": "Alice", "active": true},
						{"name": "Bob", "active": false},
						{"name": "Charlie", "active": true},
					},
				},
			},
		},
		{
			name:     "function_with_multiple_args",
			input:    "{ coalesce(state.val1, state.val2, state.val3, \"default\") }",
			expected: "value2",
			state: map[string]any{
				"state": map[string]any{
					"val1": nil,
					"val2": "value2",
					"val3": "value3",
				},
			},
		},
		{
			name:     "nested_function_calls",
			input:    "{ if(length(state.items) > 2, \"many items\", \"few items\") }",
			expected: "many items",
			state: map[string]any{
				"state": map[string]any{
					"items": []string{"a", "b", "c", "d"},
				},
			},
		},
		{
			name:     "mixed_types_in_expression",
			input:    "Total: { state.price * state.quantity + state.tax }",
			expected: "Total: 55.5",
			state: map[string]any{
				"state": map[string]any{
					"price":    10.5,
					"quantity": 5,
					"tax":      3.0,
				},
			},
		},
		{
			name:     "string_interpolation_with_nil",
			input:    "User: {state.user}, Email: {state.email}",
			expected: "User: john, Email: ",
			state: map[string]any{
				"state": map[string]any{
					"user":  "john",
					"email": nil,
				},
			},
		},
		{
			name:     "complex_conditional",
			input:    "Status: { if(state.count > 100, \"high\", if(state.count > 50, \"medium\", \"low\")) }",
			expected: "Status: medium",
			state: map[string]any{
				"state": map[string]any{"count": 75},
			},
		},
		{
			name:     "escaped_brackets",
			input:    "Template: {{not_replaced}} but {state.value} is",
			expected: "Template: {{not_replaced}} but 42 is",
			state: map[string]any{
				"state": map[string]any{"value": 42},
			},
		},
		{
			name:     "run_prefix_variable",
			input:    "Run ID: {run.id}",
			expected: "Run ID: 12345",
			state: map[string]any{
				"run": map[string]any{"id": "12345"},
			},
		},
		{
			name:     "target_source_prefixes",
			input:    "From {source.table} to {target.table}",
			expected: "From users to users_copy",
			state: map[string]any{
				"source": map[string]any{"table": "users"},
				"target": map[string]any{"table": "users_copy"},
			},
		},
		{
			name:     "stream_object_prefixes",
			input:    "Processing {stream.name} into {object.path}",
			expected: "Processing data_stream into /tmp/output.csv",
			state: map[string]any{
				"stream": map[string]any{"name": "data_stream"},
				"object": map[string]any{"path": "/tmp/output.csv"},
			},
		},
		{
			name:     "timestamp_execution_prefixes",
			input:    "Started at {timestamp.start}, execution {execution.id}",
			expected: "Started at 2024-01-01T10:00:00Z, execution exec_123",
			state: map[string]any{
				"timestamp": map[string]any{"start": "2024-01-01T10:00:00Z"},
				"execution": map[string]any{"id": "exec_123"},
			},
		},
		{
			name:     "loop_prefix",
			input:    "Loop iteration {loop.index} of {loop.total}",
			expected: "Loop iteration 5 of 10",
			state: map[string]any{
				"loop": map[string]any{"index": 5, "total": 10},
			},
		},
		{
			name:     "boolean_operations",
			input:    "Can proceed: { state.hasPermission && (state.isActive || state.isAdmin) }",
			expected: "Can proceed: true",
			state: map[string]any{
				"state": map[string]any{
					"hasPermission": true,
					"isActive":      false,
					"isAdmin":       true,
				},
			},
		},
		{
			name:     "modulo_operation",
			input:    "Remainder: { state.value % 3 }",
			expected: "Remainder: 2",
			state: map[string]any{
				"state": map[string]any{"value": 8},
			},
		},
		{
			name:     "bitwise_operations",
			input:    "Result: { state.a & state.b }",
			expected: "Result: 8",
			state: map[string]any{
				"state": map[string]any{"a": 12, "b": 10}, // 12 & 10 = 8
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create evaluator with initial state
			eval := NewEvaluator(g.ArrStr("state", "store", "env", "run", "target", "source", "stream", "object", "timestamp", "execution", "loop"), tt.state)

			// Process the input
			var result any
			var err error

			// Test different methods based on input type
			if tt.extraState != nil {
				result, err = eval.RenderAny(tt.input, tt.extraState)
			} else {
				result, err = eval.RenderAny(tt.input)
			}

			// Check for expected errors
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// For functions that return dynamic values like timestamps
			if strings.Contains(tt.name, "now_function") {
				assert.NotEqual(t, tt.input, result)
				return
			}

			// Check the result
			if !assert.Equal(t, tt.expected, result) {
				sm := tt.state
				g.Warn(g.F("%s => %#v", tt.name, sm))
				g.Warn(g.F("%s => %s", tt.name, g.Marshal(sm)))
			}
		})
	}
}

func TestEvaluatorRenderPayload(t *testing.T) {

	tests := []struct {
		name        string
		input       any
		expected    any
		state       map[string]any
		expectError bool
	}{
		// Test map[string]any rendering
		{
			name: "render_string_map",
			input: map[string]any{
				"name":    "{ state.user.name }",
				"age":     "{ state.user.age }",
				"active":  "{ state.active }",
				"missing": "{ state.missing }",
				"literal": "plain text",
			},
			expected: map[string]any{
				"name":    "John Doe",
				"age":     25,
				"active":  true,
				"missing": nil,
				"literal": "plain text",
			},
			state: map[string]any{
				"state": map[string]any{
					"user":   map[string]any{"name": "John Doe", "age": 25},
					"active": true,
				},
			},
		},
		// Test map[any]any rendering
		{
			name: "render_any_map",
			input: map[any]any{
				"key1": "{ state.VAR1 }",
				"key2": "{ store.data }",
				123:    "{ state.numeric }",
				true:   "literal",
			},
			expected: map[string]any{
				"key1": "value1",
				"key2": "stored",
				"123":  456,
				"true": "literal",
			},
			state: map[string]any{
				"state": map[string]any{"VAR1": "value1", "numeric": 456},
				"store": map[string]any{"data": "stored"},
			},
		},
		// Test nested map rendering
		{
			name: "render_nested_maps",
			input: map[string]any{
				"config": map[string]any{
					"host": "{ state.HOST }",
					"port": "{ state.PORT }",
					"settings": map[string]any{
						"timeout": "{ state.timeout_ms }",
						"retries": "{ state.max_retries }",
					},
				},
			},
			expected: map[string]any{
				"config": map[string]any{
					"host": "localhost",
					"port": "8080",
					"settings": map[string]any{
						"timeout": 5000,
						"retries": 3,
					},
				},
			},
			state: map[string]any{
				"state": map[string]any{"HOST": "localhost", "PORT": "8080", "timeout_ms": 5000, "max_retries": 3},
			},
		},
		// Test array rendering
		{
			name: "render_array",
			input: []any{
				"{ state.item1 }",
				"{ state.item2 }",
				"literal value",
				map[string]any{"nested": "{ state.item3 }"},
			},
			expected: []any{
				"first",
				"second",
				"literal value",
				map[string]any{"nested": "third"},
			},
			state: map[string]any{
				"state": map[string]any{"item1": "first", "item2": "second", "item3": "third"},
			},
		},
		// Test string array rendering
		{
			name: "render_string_array",
			input: []string{
				"{ state.PATH1 }",
				"{ state.PATH2 }",
				"/static/path",
			},
			expected: []any{
				"/usr/bin",
				"/usr/local/bin",
				"/static/path",
			},
			state: map[string]any{
				"state": map[string]any{"PATH1": "/usr/bin", "PATH2": "/usr/local/bin"},
			},
		},
		// Test mixed complex structure
		{
			name: "render_complex_structure",
			input: map[string]any{
				"users": []any{
					map[string]any{
						"id":   "{ state.user1.id }",
						"name": "{ state.user1.name }",
					},
					map[string]any{
						"id":   "{ state.user2.id }",
						"name": "{ state.user2.name }",
					},
				},
				"total": "{ length(state.users) }",
			},
			expected: map[string]any{
				"users": []any{
					map[string]any{
						"id":   1,
						"name": "Alice",
					},
					map[string]any{
						"id":   2,
						"name": "Bob",
					},
				},
				"total": 2,
			},
			state: map[string]any{
				"state": map[string]any{
					"user1": map[string]any{"id": 1, "name": "Alice"},
					"user2": map[string]any{"id": 2, "name": "Bob"},
					"users": []any{
						map[string]any{"id": 1, "name": "Alice"},
						map[string]any{"id": 2, "name": "Bob"},
					},
				},
			},
		},
		// Test non-renderable types
		{
			name: "non_renderable_types",
			input: map[string]any{
				"number": 42,
				"bool":   true,
				"nil":    nil,
				"float":  3.14,
			},
			expected: map[string]any{
				"number": 42,
				"bool":   true,
				"nil":    nil,
				"float":  3.14,
			},
			state: map[string]any{},
		},
		// Test with env and store prefixes
		{
			name: "render_with_env_store_prefixes",
			input: map[string]any{
				"env_mode":   "{ env.MODE }",
				"store_data": "{ store.cache_key }",
				"combined":   "Mode: { env.MODE }, Cache: { store.cache_key }",
			},
			expected: map[string]any{
				"env_mode":   "production",
				"store_data": "user_123",
				"combined":   "Mode: production, Cache: user_123",
			},
			state: map[string]any{
				"env":   map[string]any{"MODE": "production"},
				"store": map[string]any{"cache_key": "user_123"},
			},
		},
		// Test deeply nested rendering
		{
			name: "render_deeply_nested",
			input: map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"level3": []any{
							map[string]any{
								"value": "{ state.deep.value }",
								"items": []string{
									"{ state.item1 }",
									"{ state.item2 }",
								},
							},
						},
					},
				},
			},
			expected: map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"level3": []any{
							map[string]any{
								"value": "nested_value",
								"items": []any{
									"first",
									"second",
								},
							},
						},
					},
				},
			},
			state: map[string]any{
				"state": map[string]any{
					"deep":  map[string]any{"value": "nested_value"},
					"item1": "first",
					"item2": "second",
				},
			},
		},
		// Test with expressions in payload
		{
			name: "render_with_expressions",
			input: map[string]any{
				"calculation": "{ state.a + state.b }",
				"condition":   "{ if(state.enabled, \"active\", \"inactive\") }",
				"array_op":    "{ length(state.items) }",
			},
			expected: map[string]any{
				"calculation": 15,
				"condition":   "active",
				"array_op":    3,
			},
			state: map[string]any{
				"state": map[string]any{
					"a":       10,
					"b":       5,
					"enabled": true,
					"items":   []string{"a", "b", "c"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create evaluator with state
			eval := NewEvaluator(g.ArrStr("state", "store", "env", "run", "target", "source", "stream", "object", "timestamp", "execution", "loop"), tt.state)

			// Render the payload
			result, err := eval.RenderPayload(tt.input)

			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluatorExtractVars(t *testing.T) {
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
		{
			name:     "context_vars",
			expr:     `context.store.user_id`,
			expected: []string{"context.store.user_id"},
		},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			// Create evaluator with initial state
			eval := NewEvaluator(g.ArrStr("env", "state", "secrets", "auth", "response", "request", "sync", "context"))
			result := eval.ExtractVars(tt.expr)
			assert.ElementsMatch(t, tt.expected, result, "References should match expected values")
		})
	}
}

func TestEvaluatorFillMissingKeys(t *testing.T) {
	tests := []struct {
		name         string
		initialState map[string]any
		varsToCheck  []string
		expected     map[string]any
	}{
		{
			name:         "2-level key - simple",
			initialState: map[string]any{},
			varsToCheck:  []string{"state.value"},
			expected: map[string]any{
				"state": map[string]any{"value": nil},
			},
		},
		{
			name:         "3-level key",
			initialState: map[string]any{},
			varsToCheck:  []string{"context.store.user_id"},
			expected: map[string]any{
				"context": map[string]any{
					"store": map[string]any{
						"user_id": nil,
					},
				},
			},
		},
		{
			name:         "4-level key",
			initialState: map[string]any{},
			varsToCheck:  []string{"context.store.user_id.part4"},
			expected: map[string]any{
				"context": map[string]any{
					"store": map[string]any{
						"user_id": map[string]any{
							"part4": nil,
						},
					},
				},
			},
		},
		{
			name: "existing intermediate levels",
			initialState: map[string]any{
				"state": map[string]any{
					"nested": map[string]any{
						"existing": "value",
					},
				},
			},
			varsToCheck: []string{"state.nested.new_key"},
			expected: map[string]any{
				"state": map[string]any{
					"nested": map[string]any{
						"existing": "value",
						"new_key":  nil,
					},
				},
			},
		},
		{
			name:         "mixed levels",
			initialState: map[string]any{},
			varsToCheck: []string{
				"state.simple",
				"env.nested.key",
				"store.deep.nested.value",
			},
			expected: map[string]any{
				"state": map[string]any{"simple": nil},
				"env": map[string]any{
					"nested": map[string]any{"key": nil},
				},
				"store": map[string]any{
					"deep": map[string]any{
						"nested": map[string]any{
							"value": nil,
						},
					},
				},
			},
		},
		{
			name: "key already exists with value",
			initialState: map[string]any{
				"state": map[string]any{
					"nested": map[string]any{
						"key": "existing_value",
					},
				},
			},
			varsToCheck: []string{"state.nested.key"},
			expected: map[string]any{
				"state": map[string]any{
					"nested": map[string]any{
						"key": "existing_value",
					},
				},
			},
		},
		{
			name:         "invalid prefix - should skip",
			initialState: map[string]any{},
			varsToCheck:  []string{"invalid.key.path"},
			expected:     map[string]any{},
		},
		{
			name:         "single part - should skip",
			initialState: map[string]any{},
			varsToCheck:  []string{"state"},
			expected:     map[string]any{},
		},
		{
			name:         "5-level deep nesting",
			initialState: map[string]any{},
			varsToCheck:  []string{"context.a.b.c.d.e"},
			expected: map[string]any{
				"context": map[string]any{
					"a": map[string]any{
						"b": map[string]any{
							"c": map[string]any{
								"d": map[string]any{
									"e": nil,
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "multiple vars with shared prefixes",
			initialState: map[string]any{},
			varsToCheck: []string{
				"state.user.id",
				"state.user.name",
				"state.user.profile.email",
			},
			expected: map[string]any{
				"state": map[string]any{
					"user": map[string]any{
						"id":   nil,
						"name": nil,
						"profile": map[string]any{
							"email": nil,
						},
					},
				},
			},
		},
		{
			name: "partially existing nested structure",
			initialState: map[string]any{
				"context": map[string]any{
					"store": map[string]any{
						"existing_key": "value",
					},
				},
			},
			varsToCheck: []string{"context.store.user_id.nested"},
			expected: map[string]any{
				"context": map[string]any{
					"store": map[string]any{
						"existing_key": "value",
						"user_id": map[string]any{
							"nested": nil,
						},
					},
				},
			},
		},
		{
			name: "non-map value in path - should skip",
			initialState: map[string]any{
				"state": map[string]any{
					"user": "john_doe", // This is a string, not a map
				},
			},
			varsToCheck: []string{"state.user.profile.name"},
			expected: map[string]any{
				"state": map[string]any{
					"user": "john_doe", // Should remain unchanged - not replaced with a map
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval := NewEvaluator(g.ArrStr("state", "store", "env", "context"))
			result := eval.FillMissingKeys(tt.initialState, tt.varsToCheck)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluatorCheckExpression(t *testing.T) {
	tests := []struct {
		name        string
		expression  string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty_expression",
			expression:  "",
			expectError: false,
		},
		{
			name:        "simple_expression_no_quotes",
			expression:  "state.counter > 10",
			expectError: false,
		},
		{
			name:        "valid_double_quotes",
			expression:  `state.name == "John"`,
			expectError: false,
		},
		{
			name:        "multiple_valid_double_quotes",
			expression:  `state.firstName == "John" && state.lastName == "Doe"`,
			expectError: false,
		},
		{
			name:        "nested_double_quotes_with_escape",
			expression:  `state.message == "He said \"Hello\""`,
			expectError: false,
		},
		{
			name:        "single_quote_error",
			expression:  `state.name == 'John'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "single_quote_in_middle",
			expression:  `state.name == "John" && state.title == 'Mr'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "single_quote_at_beginning",
			expression:  `'test' == state.value`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "apostrophe_outside_double_quotes",
			expression:  `state.name == "John's car"`,
			expectError: false,
		},
		{
			name:        "apostrophe_and_single_quote_mix",
			expression:  `state.name == "John's car" && state.other == 'test'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "escaped_double_quote",
			expression:  `state.quote == "She said \"hi\""`,
			expectError: false,
		},
		{
			name:        "multiple_escaped_double_quotes",
			expression:  `state.json == "{\"name\": \"John\", \"age\": 30}"`,
			expectError: false,
		},
		{
			name:        "single_quote_after_escaped_double_quote",
			expression:  `state.text == "He said \"hello\"" && state.bad == 'world'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "complex_valid_expression",
			expression:  `response.status == 200 && response.data.message == "Success" && len(response.items) > 0`,
			expectError: false,
		},
		{
			name:        "backslash_before_single_quote_still_error",
			expression:  `state.test == "valid" && state.invalid == \'bad\'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "double_backslash_before_double_quote",
			expression:  `state.path == "C:\\Program Files\\"`,
			expectError: false,
		},
		{
			name:        "single_quote_inside_double_quotes_is_valid",
			expression:  `state.message == "Don't do that"`,
			expectError: false,
		},
		{
			name:        "unclosed_double_quote_with_single_quote",
			expression:  `state.name == "John && state.other == 'test'`,
			expectError: true,
			errorMsg:    "unclosed double quote",
		},
		{
			name:        "only_single_quotes",
			expression:  `'hello world'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "only_double_quotes",
			expression:  `"hello world"`,
			expectError: false,
		},
		{
			name:        "mixed_quotes_complex",
			expression:  `state.a == "test" && state.b == 'invalid' && state.c == "valid"`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "unicode_characters_with_double_quotes",
			expression:  `state.emoji == "Hello 👋 world"`,
			expectError: false,
		},
		{
			name:        "unicode_characters_with_single_quotes",
			expression:  `state.emoji == 'Hello 👋 world'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "json_like_string",
			expression:  `state.config == "{\"timeout\": 30, \"retries\": 3}"`,
			expectError: false,
		},
		{
			name:        "regex_like_pattern_double_quotes",
			expression:  `state.pattern == "^[a-zA-Z0-9]+$"`,
			expectError: false,
		},
		{
			name:        "regex_like_pattern_single_quotes",
			expression:  `state.pattern == '^[a-zA-Z0-9]+$'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "empty_string_double_quotes",
			expression:  `state.value == ""`,
			expectError: false,
		},
		{
			name:        "empty_string_single_quotes",
			expression:  `state.value == ''`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "multiple_consecutive_escapes",
			expression:  `state.path == "C:\\\\server\\\\path\\\\"`,
			expectError: false,
		},
		{
			name:        "quote_at_end_of_string",
			expression:  `state.sql == "SELECT * FROM table WHERE name = \"John\""`,
			expectError: false,
		},
		{
			name:        "single_quote_at_very_end",
			expression:  `someexpression'`,
			expectError: true,
			errorMsg:    "cannot use single quotes",
		},
		{
			name:        "double_quote_at_very_end",
			expression:  `someexpression"`,
			expectError: true,
			errorMsg:    "unclosed double quote",
		},
		// Parentheses tests
		{
			name:        "valid_parentheses_simple",
			expression:  `func(arg1, arg2)`,
			expectError: false,
		},
		{
			name:        "valid_parentheses_nested",
			expression:  `outer(inner(value), other)`,
			expectError: false,
		},
		{
			name:        "valid_parentheses_multiple",
			expression:  `func1() && func2(arg) || func3(a, b)`,
			expectError: false,
		},
		{
			name:        "unclosed_parenthesis_simple",
			expression:  `func(arg1, arg2`,
			expectError: true,
			errorMsg:    "unclosed parenthesis",
		},
		{
			name:        "unclosed_parenthesis_nested",
			expression:  `outer(inner(value), other`,
			expectError: true,
			errorMsg:    "unclosed parenthesis",
		},
		{
			name:        "extra_closing_parenthesis",
			expression:  `func(arg1, arg2))`,
			expectError: true,
			errorMsg:    "unmatched closing parenthesis",
		},
		{
			name:        "parentheses_inside_double_quotes_valid",
			expression:  `state.value == "text with (parentheses)"`,
			expectError: false,
		},
		{
			name:        "unclosed_quote_simple",
			expression:  `state.name == "John`,
			expectError: true,
			errorMsg:    "unclosed double quote",
		},
		{
			name:        "unclosed_quote_at_beginning",
			expression:  `"unclosed string and other code`,
			expectError: true,
			errorMsg:    "unclosed double quote",
		},
		{
			name:        "mixed_unclosed_quote_and_paren",
			expression:  `func("unclosed string and missing paren`,
			expectError: true,
			errorMsg:    "unclosed double quote", // Quote error should be caught first
		},
		{
			name:        "mixed_unclosed_paren_after_quote",
			expression:  `func("closed string" and missing paren`,
			expectError: true,
			errorMsg:    "unclosed parenthesis",
		},
		{
			name:        "complex_valid_expression_with_quotes_and_parens",
			expression:  `len(state.items) > 0 && state.name == "John" && func(state.age)`,
			expectError: false,
		},
		{
			name:        "parentheses_with_escaped_quotes",
			expression:  `func(state.message == "He said \"Hello\"")`,
			expectError: false,
		},
		{
			name:        "multiple_unclosed_parentheses",
			expression:  `outer(inner(deep(value)`,
			expectError: true,
			errorMsg:    "unclosed parenthesis",
		},
		{
			name:        "parentheses_only_closing",
			expression:  `)`,
			expectError: true,
			errorMsg:    "unmatched closing parenthesis",
		},
		{
			name:        "parentheses_only_opening",
			expression:  `(`,
			expectError: true,
			errorMsg:    "unclosed parenthesis",
		},
		{
			name:        "empty_parentheses",
			expression:  `func()`,
			expectError: false,
		},
		// Additional edge cases for escape handling
		{
			name:        "odd_escapes_before_quote",
			expression:  `state.text == "He said \\\"Hello\\\""`,
			expectError: false,
		},
		{
			name:        "escaped_backslash_before_quote",
			expression:  `state.path == "C:\\\\"`,
			expectError: false,
		},
		{
			name:        "parentheses_and_quotes_complex",
			expression:  `func(state.name == "value") && other("test")`,
			expectError: false,
		},
		{
			name:        "nested_parens_with_unclosed_quote",
			expression:  `func(inner(state.name == "unclosed))`,
			expectError: true,
			errorMsg:    "unclosed double quote",
		},
	}

	eval := NewEvaluator(nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := eval.Check(tt.expression)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" && err != nil {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
func TestEvaluatorLiterals(t *testing.T) {
	// t.Skip()

	variables := g.M(
		"object", g.M(
			"a", 1,
			"b", 1,
			"echo", func(a any) (any, error) {
				return a, nil
			},
		),
		"array", []any{g.M("id", 1), g.M("id", 2)},
		"array_empty", []any{},
	)

	tests := []struct {
		name        string
		expression  string
		expectValue any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "one_level_existent",
			expression:  `object.a`,
			expectError: false,
			expectValue: 1,
		},
		{
			name:        "array_jmespath",
			expression:  `jmespath(array, "[-1].id")`,
			expectError: false,
			expectValue: 2,
		},
		{
			name:        "array_empty_jmespath",
			expression:  `jmespath(array_empty, "[-1].id")`,
			expectError: false,
			expectValue: nil,
		},
		{
			name:        "array_empty_error",
			expression:  `array_empty[-1].id`,
			expectError: true,
			expectValue: nil,
		},
	}

	eval := NewEvaluator(nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := eval.Eval.Evaluate(tt.expression, variables, GlobalFunctionMap)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" && err != nil {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectValue, value)
			}
		})
	}
}

// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkPtEvaluatorRenderPayload'
func BenchmarkPtEvaluatorRenderPayload(b *testing.B) {

	eState := g.M("value1", 1, "value2", g.M("nested1", "a"))
	eval := NewEvaluator(g.ArrStr("state", "store", "env", "run", "target", "source", "stream", "object", "timestamp", "execution", "loop"), eState)

	// Render the payload
	var err error
	expr := "value1={state.value1} | value2={state.value2.nested1}"
	for n := 0; n < b.N; n++ {
		newState := g.M("value1", 11)
		_, err = eval.RenderPayload(expr, newState)
		if err != nil {
			return
		}
	}
}

// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkPtEvaluatorRenderPayload'
func BenchmarkPtEvaluatorEval(b *testing.B) {

	eState := g.M("value1", 1, "value2", g.M("nested1", "a"))
	eval := NewEvaluator(g.ArrStr("state", "store", "env", "run", "target", "source", "stream", "object", "timestamp", "execution", "loop"), eState)

	// Render the payload
	var err error
	expr := "value1={state.value1} | value2={state.value2.nested1}"
	for n := 0; n < b.N; n++ {
		newState := g.M("value1", 1, "value2", g.M("nested1", "a"))
		_, err = eval.Eval.Evaluate(expr, newState, GlobalFunctionMap)
		if err != nil {
			return
		}
	}
}

func TestEvaluatorKeepMissingExpr(t *testing.T) {
	// Test cases for KeepMissingExpr functionality
	tests := []struct {
		name            string
		input           any
		expected        any
		state           map[string]any
		keepMissingExpr bool
		expectError     bool
	}{
		// KeepMissingExpr=true - missing expressions should remain unchanged
		{
			name:            "missing_variable_kept_intact",
			input:           "Value: {state.missing_var}",
			expected:        "Value: {state.missing_var}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "missing_nested_variable_kept_intact",
			input:           "Config: {state.config.database.host}",
			expected:        "Config: {state.config.database.host}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "multiple_missing_variables_kept_intact",
			input:           "User: {state.user}, Email: { upper(state.email) }",
			expected:        "User: {state.user}, Email: { upper(state.email) }",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "mixed_existing_and_missing_variables",
			input:           "Name: {state.name}, Missing: {state.missing}",
			expected:        "Name: John, Missing: {state.missing}",
			state:           map[string]any{"state": map[string]any{"name": "John"}},
			keepMissingExpr: true,
		},
		{
			name:            "missing_env_variable_kept_intact",
			input:           "API: {env.API_URL}",
			expected:        "API: {env.API_URL}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "missing_store_variable_kept_intact",
			input:           "Cache: {store.cache_key}",
			expected:        "Cache: {store.cache_key}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "missing_deeply_nested_variable_kept_intact",
			input:           "Path: {state.level1.level2.level3.value}",
			expected:        "Path: {state.level1.level2.level3.value}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "partially_missing_nested_path",
			input:           "Value: {state.config.missing.field}",
			expected:        "Value: {state.config.missing.field}",
			state:           map[string]any{"state": map[string]any{"config": map[string]any{"existing": "value"}}},
			keepMissingExpr: true,
		},
		{
			name:            "direct_missing_expression",
			input:           "{state.missing}",
			expected:        "{state.missing}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},

		// KeepMissingExpr=false - missing expressions should be evaluated to nil/empty
		{
			name:            "missing_variable_evaluated_to_empty",
			input:           "Value: {state.missing_var}",
			expected:        "Value: ",
			state:           map[string]any{},
			keepMissingExpr: false,
		},
		{
			name:            "missing_nested_variable_evaluated_to_empty",
			input:           "Config: {state.config.database.host}",
			expected:        "Config: ",
			state:           map[string]any{},
			keepMissingExpr: false,
		},
		{
			name:            "direct_missing_expression_evaluated_to_nil",
			input:           "{state.missing}",
			expected:        nil,
			state:           map[string]any{},
			keepMissingExpr: false,
		},
		{
			name:            "mixed_existing_and_missing_default_behavior",
			input:           "Name: {state.name}, Missing: {state.missing}",
			expected:        "Name: John, Missing: ",
			state:           map[string]any{"state": map[string]any{"name": "John"}},
			keepMissingExpr: false,
		},

		// Edge cases with expressions and functions
		{
			name:            "missing_variable_in_expression_kept",
			input:           "Result: {state.missing * 2}",
			expected:        "Result: {state.missing * 2}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "missing_variable_in_comparison_kept",
			input:           "Valid: {state.missing > 10}",
			expected:        "Valid: {state.missing > 10}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "multiple_expressions_some_missing",
			input:           "A: {state.existing}, B: {state.missing}, C: {state.another}",
			expected:        "A: found, B: {state.missing}, C: {state.another}",
			state:           map[string]any{"state": map[string]any{"existing": "found"}},
			keepMissingExpr: true,
		},

		// Complex scenarios
		{
			name:            "template_with_missing_placeholders",
			input:           "Hello {state.username}, your balance is {state.account.balance}",
			expected:        "Hello {state.username}, your balance is {state.account.balance}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "api_url_template_with_missing_vars",
			input:           "https://api.example.com/{env.VERSION}/users/{state.user_id}",
			expected:        "https://api.example.com/{env.VERSION}/users/{state.user_id}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "sql_query_template_with_missing_vars",
			input:           "SELECT * FROM users WHERE id = {state.user_id} AND status = '{state.status}'",
			expected:        "SELECT * FROM users WHERE id = {state.user_id} AND status = '{state.status}'",
			state:           map[string]any{},
			keepMissingExpr: true,
		},

		// Partial evaluation scenarios
		{
			name:            "some_vars_exist_some_dont",
			input:           "Config: host={env.HOST}, port={env.PORT}, db={env.DATABASE}",
			expected:        "Config: host=localhost, port={env.PORT}, db={env.DATABASE}",
			state:           map[string]any{"env": map[string]any{"HOST": "localhost"}},
			keepMissingExpr: true,
		},
		{
			name:            "nested_map_with_missing_field",
			input:           "User: {state.user.name}, Role: {state.user.role}",
			expected:        "User: Alice, Role: {state.user.role}",
			state:           map[string]any{"state": map[string]any{"user": map[string]any{"name": "Alice"}}},
			keepMissingExpr: true,
		},

		// Empty and null states
		{
			name:            "empty_state_all_missing",
			input:           "{state.a} {state.b} {state.c}",
			expected:        "{state.a} {state.b} {state.c}",
			state:           map[string]any{},
			keepMissingExpr: true,
		},
		{
			name:            "nil_state_handled_gracefully",
			input:           "Value: {state.value}",
			expected:        "Value: {state.value}",
			state:           nil,
			keepMissingExpr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create evaluator with initial state
			eval := NewEvaluator(g.ArrStr("state", "store", "env", "secrets"), tt.state)
			eval.KeepMissingExpr = tt.keepMissingExpr

			// Process the input
			result, err := eval.RenderAny(tt.input)

			// Check for expected errors
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// Check the result
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluatorAllowNoPrefix(t *testing.T) {
	// Test cases for AllowNoPrefix functionality
	tests := []struct {
		name          string
		input         any
		expected      any
		state         map[string]any
		allowNoPrefix bool
		expectError   bool
	}{
		// Basic unprefixed variable tests with AllowNoPrefix=true
		{
			name:          "simple_unprefixed_variable",
			input:         "Hello, {MY_VAR}!",
			expected:      "Hello, World!",
			state:         map[string]any{"MY_VAR": "World"},
			allowNoPrefix: true,
		},
		{
			name:          "multiple_unprefixed_variables",
			input:         "User: {USERNAME}, ID: {USER_ID}",
			expected:      "User: alice, ID: 123",
			state:         map[string]any{"USERNAME": "alice", "USER_ID": 123},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_variable_direct_value",
			input:         "{COUNTER}",
			expected:      42,
			state:         map[string]any{"COUNTER": 42},
			allowNoPrefix: true,
		},

		// Mixed prefixed and unprefixed variables
		{
			name:     "mixed_prefixed_and_unprefixed",
			input:    "Env: {env.MODE}, Var: {MY_VAR}",
			expected: "Env: production, Var: test",
			state: map[string]any{
				"env":    map[string]any{"MODE": "production"},
				"MY_VAR": "test",
			},
			allowNoPrefix: true,
		},
		{
			name:     "mixed_state_and_unprefixed",
			input:    "State value: {state.value}, Custom: {CUSTOM}",
			expected: "State value: 10, Custom: 20",
			state: map[string]any{
				"state":  map[string]any{"value": 10},
				"CUSTOM": 20,
			},
			allowNoPrefix: true,
		},

		// AllowNoPrefix=false - unprefixed variables should NOT be rendered
		{
			name:          "unprefixed_without_allow_no_prefix",
			input:         "Value: {MY_VAR}",
			expected:      "Value: {MY_VAR}",
			state:         map[string]any{"MY_VAR": "test"},
			allowNoPrefix: false,
		},
		{
			name:     "prefixed_works_without_allow_no_prefix",
			input:    "Value: {state.value}",
			expected: "Value: 100",
			state: map[string]any{
				"state": map[string]any{"value": 100},
			},
			allowNoPrefix: false,
		},

		// Edge cases
		{
			name:          "unprefixed_with_underscore",
			input:         "{SOME_LONG_VAR_NAME}",
			expected:      "value",
			state:         map[string]any{"SOME_LONG_VAR_NAME": "value"},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_lowercase",
			input:         "{my_var}",
			expected:      "lowercase",
			state:         map[string]any{"my_var": "lowercase"},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_with_numbers",
			input:         "{VAR123}",
			expected:      "numbered",
			state:         map[string]any{"VAR123": "numbered"},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_missing_variable",
			input:         "{MISSING}",
			expected:      nil,
			state:         map[string]any{},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_in_string_context",
			input:         "Prefix: {PREFIX}, Value: {VALUE}, Suffix: {SUFFIX}",
			expected:      "Prefix: pre, Value: val, Suffix: post",
			state:         map[string]any{"PREFIX": "pre", "VALUE": "val", "SUFFIX": "post"},
			allowNoPrefix: true,
		},

		// Unprefixed variables in expressions
		{
			name:          "unprefixed_in_arithmetic",
			input:         "Total: {COUNT * 2}",
			expected:      "Total: 20",
			state:         map[string]any{"COUNT": 10},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_in_comparison",
			input:         "Valid: {AGE >= 18}",
			expected:      "Valid: true",
			state:         map[string]any{"AGE": 21},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_in_function",
			input:         "Result: {if(ENABLED, \"yes\", \"no\")}",
			expected:      "Result: yes",
			state:         map[string]any{"ENABLED": true},
			allowNoPrefix: true,
		},

		// More function tests with unprefixed variables
		{
			name:          "coalesce_with_unprefixed",
			input:         "{coalesce(MISSING_VAR, DEFAULT_VALUE)}",
			expected:      "default",
			state:         map[string]any{"DEFAULT_VALUE": "default"},
			allowNoPrefix: true,
		},
		{
			name:          "nested_if_with_unprefixed",
			input:         "{if(PREMIUM, if(ADMIN, \"premium-admin\", \"premium-user\"), \"free\")}",
			expected:      "premium-admin",
			state:         map[string]any{"PREMIUM": true, "ADMIN": true},
			allowNoPrefix: true,
		},
		{
			name:          "string_concat_with_unprefixed",
			input:         "{FIRST_NAME + \" \" + LAST_NAME}",
			expected:      "John Doe",
			state:         map[string]any{"FIRST_NAME": "John", "LAST_NAME": "Doe"},
			allowNoPrefix: true,
		},
		{
			name:          "cast_function_with_unprefixed",
			input:         "{cast(NUMBER_VAR, \"string\")}",
			expected:      "42",
			state:         map[string]any{"NUMBER_VAR": 42},
			allowNoPrefix: true,
		},
		{
			name:          "mixed_prefixed_unprefixed_in_function",
			input:         "{coalesce(env.API_KEY, BACKUP_KEY, \"default-key\")}",
			expected:      "backup123",
			state:         map[string]any{"env": map[string]any{}, "BACKUP_KEY": "backup123"},
			allowNoPrefix: true,
		},
		{
			name:          "logical_and_with_unprefixed",
			input:         "{AUTHENTICATED && AUTHORIZED}",
			expected:      true,
			state:         map[string]any{"AUTHENTICATED": true, "AUTHORIZED": true},
			allowNoPrefix: true,
		},
		{
			name:          "logical_or_with_unprefixed",
			input:         "{IS_ADMIN || IS_MODERATOR}",
			expected:      true,
			state:         map[string]any{"IS_ADMIN": false, "IS_MODERATOR": true},
			allowNoPrefix: true,
		},
		{
			name:          "ternary_with_unprefixed",
			input:         "{STATUS == \"active\" ? ACTIVE_COUNT : INACTIVE_COUNT}",
			expected:      100,
			state:         map[string]any{"STATUS": "active", "ACTIVE_COUNT": 100, "INACTIVE_COUNT": 5},
			allowNoPrefix: true,
		},
		{
			name:          "multiple_unprefixed_in_complex_expression",
			input:         "{(PRICE * QUANTITY) + TAX - DISCOUNT}",
			expected:      115.0,
			state:         map[string]any{"PRICE": 10.0, "QUANTITY": 10, "TAX": 20.0, "DISCOUNT": 5.0},
			allowNoPrefix: true,
		},

		// Unprefixed in maps and arrays
		{
			name:          "unprefixed_in_map",
			input:         map[string]any{"key": "{MY_VALUE}"},
			expected:      map[string]any{"key": "mapped"},
			state:         map[string]any{"MY_VALUE": "mapped"},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_in_array",
			input:         []string{"{ITEM1}", "{ITEM2}"},
			expected:      []any{"first", "second"},
			state:         map[string]any{"ITEM1": "first", "ITEM2": "second"},
			allowNoPrefix: true,
		},

		// Complex scenarios
		{
			name:     "complex_mixed_scenario",
			input:    "API: {API_URL}, Token: {secrets.token}, User: {USERNAME}",
			expected: "API: https://api.example.com, Token: abc123, User: john",
			state: map[string]any{
				"API_URL":  "https://api.example.com",
				"secrets":  map[string]any{"token": "abc123"},
				"USERNAME": "john",
			},
			allowNoPrefix: true,
		},
		{
			name:     "unprefixed_with_nested_map",
			input:    map[string]any{"config": map[string]any{"host": "{HOST}", "port": "{PORT}"}},
			expected: map[string]any{"config": map[string]any{"host": "localhost", "port": 8080}},
			state: map[string]any{
				"HOST": "localhost",
				"PORT": 8080,
			},
			allowNoPrefix: true,
		},

		// When a prefix name exists as a key in state, it gets rendered as JSON
		{
			name:     "prefix_name_rendered_as_json",
			input:    "State: {state.value}, Prefix: {state}",
			expected: "State: 5, Prefix: {\"value\":5}",
			state: map[string]any{
				"state": map[string]any{"value": 5},
			},
			allowNoPrefix: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create evaluator with initial state
			eval := NewEvaluator(g.ArrStr("state", "store", "env", "secrets"), tt.state)
			eval.AllowNoPrefix = tt.allowNoPrefix

			// Process the input
			result, err := eval.RenderAny(tt.input)

			// Check for expected errors
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// Check the result
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluatorExtractVarsWithAllowNoPrefix(t *testing.T) {
	tests := []struct {
		name          string
		expr          string
		expected      []string
		allowNoPrefix bool
	}{
		// With AllowNoPrefix=true
		{
			name:          "extract_unprefixed_variable",
			expr:          "MY_VAR",
			expected:      []string{"MY_VAR"},
			allowNoPrefix: true,
		},
		{
			name:          "extract_multiple_unprefixed",
			expr:          "VAR1 + VAR2",
			expected:      []string{"VAR1", "VAR2"},
			allowNoPrefix: true,
		},
		{
			name:          "extract_mixed_prefixed_unprefixed",
			expr:          "env.MODE + MY_VAR",
			expected:      []string{"env.MODE", "MY_VAR"},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_with_function",
			expr:          "if(ENABLED, state.value, 0)",
			expected:      []string{"ENABLED", "state.value"},
			allowNoPrefix: true,
		},
		{
			name:          "unprefixed_in_string_concat",
			expr:          `"Prefix: " + MY_VAR + " Suffix"`,
			expected:      []string{"MY_VAR"},
			allowNoPrefix: true,
		},
		{
			name:          "skip_prefixes_themselves",
			expr:          "state.value + env",
			expected:      []string{"state.value"},
			allowNoPrefix: true,
		},

		// With AllowNoPrefix=false
		{
			name:          "no_extraction_without_allow_no_prefix",
			expr:          "MY_VAR",
			expected:      []string{},
			allowNoPrefix: false,
		},
		{
			name:          "only_prefixed_extracted_without_allow",
			expr:          "env.MODE + MY_VAR",
			expected:      []string{"env.MODE"},
			allowNoPrefix: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval := NewEvaluator(g.ArrStr("env", "state", "secrets"))
			eval.AllowNoPrefix = tt.allowNoPrefix
			result := eval.ExtractVars(tt.expr)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestEvaluatorFillMissingKeysWithAllowNoPrefix(t *testing.T) {
	tests := []struct {
		name          string
		initialState  map[string]any
		varsToCheck   []string
		expected      map[string]any
		allowNoPrefix bool
	}{
		{
			name:          "fill_unprefixed_variable",
			initialState:  map[string]any{},
			varsToCheck:   []string{"MY_VAR"},
			expected:      map[string]any{"MY_VAR": nil},
			allowNoPrefix: true,
		},
		{
			name:         "fill_multiple_unprefixed",
			initialState: map[string]any{},
			varsToCheck:  []string{"VAR1", "VAR2", "VAR3"},
			expected: map[string]any{
				"VAR1": nil,
				"VAR2": nil,
				"VAR3": nil,
			},
			allowNoPrefix: true,
		},
		{
			name:         "fill_mixed_prefixed_and_unprefixed",
			initialState: map[string]any{},
			varsToCheck:  []string{"state.value", "MY_VAR"},
			expected: map[string]any{
				"state":  map[string]any{"value": nil},
				"MY_VAR": nil,
			},
			allowNoPrefix: true,
		},
		{
			name:          "dont_fill_unprefixed_without_allow",
			initialState:  map[string]any{},
			varsToCheck:   []string{"MY_VAR"},
			expected:      map[string]any{},
			allowNoPrefix: false,
		},
		{
			name: "preserve_existing_unprefixed_value",
			initialState: map[string]any{
				"MY_VAR": "existing",
			},
			varsToCheck: []string{"MY_VAR"},
			expected: map[string]any{
				"MY_VAR": "existing",
			},
			allowNoPrefix: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval := NewEvaluator(g.ArrStr("state", "env"))
			eval.AllowNoPrefix = tt.allowNoPrefix
			result := eval.FillMissingKeys(tt.initialState, tt.varsToCheck)
			assert.Equal(t, tt.expected, result)
		})
	}
}
