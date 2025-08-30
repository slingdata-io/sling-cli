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
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			// Create evaluator with initial state
			eval := NewEvaluator(g.ArrStr("env", "state", "secrets", "auth", "response", "request", "sync"))
			result := eval.ExtractVars(tt.expr)
			assert.ElementsMatch(t, tt.expected, result, "References should match expected values")
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval := NewEvaluator(nil)
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
