package api

import (
	"regexp"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/maja42/goval"
	"github.com/stretchr/testify/assert"
)

func TestFunctions(t *testing.T) {
	type test struct {
		name     string
		expr     string
		expected any
		err      bool
	}

	variables := map[string]any{
		"t1":        time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
		"t2":        time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		"t3":        time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		"someVar":   "actual",
		"someNil":   nil,
		"someEmpty": "",
		"null":      nil,
		// Add a sample date with various parts for testing date_extract
		"sampleDate": time.Date(2022, 3, 15, 14, 30, 45, 0, time.UTC),
	}

	testSuites := map[string][]test{}

	testSuites["coalesce"] = []test{
		{"no_args", `coalesce()`, nil, true},
		{"single_arg", `coalesce("test")`, "test", false},
		{"first_arg_not_null", `coalesce("test", "default")`, "test", false},
		{"first_arg_null", `coalesce(nil, "default")`, "default", false},
		{"first_arg_empty_string", `coalesce("", "default")`, "", false}, // Empty string is NOT null
		{"multiple_args_first_valid", `coalesce("test", "default1", "default2")`, "test", false},
		{"multiple_args_all_null", `coalesce(nil, nil, nil)`, nil, false},
		{"multiple_args_some_null", `coalesce(nil, "default", nil)`, "default", false},
		{"multiple_args_last_valid", `coalesce(nil, nil, "last")`, "last", false},
		{"with_variables", `coalesce(someNil, someVar)`, "actual", false},
		{"with_variables_reversed", `coalesce(someVar, someNil)`, "actual", false},
		{"empty_string_is_valid", `coalesce(someEmpty, "default")`, "", false},
	}

	testSuites["value"] = []test{
		{"no_args", `value()`, nil, true},
		{"single_arg", `value("test")`, "test", false},
		{"first_arg_not_empty", `value("test", "default")`, "test", false},
		{"first_arg_empty", `value("", "default")`, "default", false},
		{"first_arg_nil", `value(nil, "default")`, "default", false},
		{"multiple_args_first_valid", `value("test", "default1", "default2")`, "test", false},
		{"multiple_args_first_empty", `value("", "default1", "default2")`, "default1", false},
		{"multiple_args_first_nil_second_empty", `value(nil, "", "default2")`, "default2", false},
	}

	testSuites["int_parse"] = []test{
		{"no_args", `int_parse()`, nil, true},
		{"int_value", `int_parse(42)`, 42, false},
		{"float_value", `int_parse(42.0)`, 42, false},
		{"string_numeric", `int_parse("42")`, 42, false},
		{"invalid_string", `int_parse("abc")`, nil, true},
	}

	testSuites["split"] = []test{
		{"no_args", `split()`, nil, true},
		{"one_arg", `split("a,b,c")`, nil, true},
		{"too_many_args", `split("a,b,c", ",", "extra")`, nil, true},
		{"comma_separator", `split("a,b,c", ",")`, []string{"a", "b", "c"}, false},
		{"empty_separator", `split("abc", "")`, []string{"a", "b", "c"}, false},
		{"space_separator", `split("a b c", " ")`, []string{"a", "b", "c"}, false},
		{"non-string_first_arg", `split(123, ",")`, []string{"123"}, false},
		{"non-string_second_arg", `split("a,b,c", 123)`, []string{"a,b,c"}, false},
	}

	testSuites["range"] = []test{
		{"sample_int_range", `range(1, 5)`, []any{1, 2, 3, 4, 5}, false},
		{"sample_date_range", `range(date_parse("2022-01-01 00:00:00", "auto"), date_parse("2022-01-01 00:04:00", "auto"), "1m")`,
			[]any{
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 1, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 2, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 3, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 4, 0, 0, time.UTC),
			}, false},
	}

	testSuites["int_range"] = []test{
		{"no_args", `int_range()`, nil, true},
		{"one_arg", `int_range(5)`, nil, true},
		{"simple_range", `int_range(1, 5)`, []int{1, 2, 3, 4, 5}, false},
		{"single_value_range", `int_range(3, 3)`, []int{3}, false},
		{"range_with_step", `int_range(1, 10, 2)`, []int{1, 3, 5, 7, 9}, false},
		{"negative_range", `int_range(5, 1, -1)`, []int{5, 4, 3, 2, 1}, false},
		{"zero_step", `int_range(1, 5, 0)`, nil, true},
		{"string_start", `int_range("1", 5)`, []int{1, 2, 3, 4, 5}, false},
		{"string_end", `int_range(1, "5")`, []int{1, 2, 3, 4, 5}, false},
		{"string_step", `int_range(1, 10, "2")`, []int{1, 3, 5, 7, 9}, false},
		{"non-numeric_start", `int_range("abc", 5)`, nil, true},
		{"non-numeric_end", `int_range(1, "abc")`, nil, true},
		{"non-numeric_step", `int_range(1, 5, "abc")`, nil, true},
	}

	testSuites["greatest"] = []test{
		{"no_args", `greatest()`, nil, true},
		{"single_numeric", `greatest(42)`, 42, false},
		{"multiple_numeric", `greatest(1, 5, 3, 2)`, 5, false},
		{"mixed_types", `greatest(1, 5.5, 3)`, 5.5, false},
		{"array_input", `greatest([1, 5, 3, 2])`, 5, false},
		{"string_comparison", `greatest("a", "b", "c")`, "c", false},
		{"string_array", `greatest(["a", "b", "c"])`, "c", false},
		{"empty_array", `greatest([])`, nil, true},
		{"incomparable_types", `greatest(1, "abc")`, nil, true},
		{"timestamps", `greatest(t1, t2, t3)`, variables["t2"], false},
	}

	testSuites["least"] = []test{
		{"no_args", `least()`, nil, true},
		{"single_numeric", `least(42)`, 42, false},
		{"multiple_numeric", `least(5, 1, 3, 2)`, 1, false},
		{"mixed_types", `least(5, 1.5, 3)`, 1.5, false},
		{"array_input", `least([5, 1, 3, 2])`, 1, false},
		{"string_comparison", `least("a", "b", "c")`, "a", false},
		{"string_array", `least(["a", "b", "c"])`, "a", false},
		{"empty_array", `least([])`, nil, true},
		{"incomparable_types", `least(1, "abc")`, nil, true},
		{"timestamps", `least(t1, t2, t3)`, variables["t3"], false},
	}

	testSuites["combined"] = []test{
		{"range_with_max", `greatest(int_range(1, 10))`, 10, false},
		{"range_with_min", `least(int_range(1, 10))`, 1, false},
		{"split_with_max", `greatest(split("1,5,3,7", ","))`, "7", false},
		{"int_with_split", `int_parse(element(split("1,5,3", ","), 1))`, 5, false},
		{"value_with_variables", `value(someVar, "default")`, "actual", false},
		{"value_with_nil_variables", `value(someNil, "default")`, "default", false},
		{"value_with_empty_variables", `value(someEmpty, "default")`, "default", false},
		{"value_with_missing_variables", `value(nonExistentVar, "default")`, "default", true},
	}

	testSuites["date_format"] = []test{
		{"no_args", `date_format()`, nil, true},
		{"one_arg", `date_format(t1)`, nil, true},
		{"too_many_args", `date_format(t1, "%Y-%m-%d", "extra")`, nil, true},
		{"basic_format", `date_format(t1, "%Y-%m-%d")`, "2022-01-01", false},
		{"datedate_format", `date_format(t1, "%Y-%m-%d %H:%M:%S")`, "2022-01-01 00:00:00", false},
		{"year_only", `date_format(t1, "%Y")`, "2022", false},
		{"month_only", `date_format(t1, "%m")`, "01", false},
		{"month_name", `date_format(t1, "%B")`, "January", false},
		{"abbreviated_month", `date_format(t1, "%b")`, "Jan", false},
		{"day_of_month", `date_format(t1, "%d")`, "01", false},
		{"hour_24", `date_format(t1, "%H")`, "00", false},
		{"minute", `date_format(t1, "%M")`, "00", false},
		{"second", `date_format(t1, "%S")`, "00", false},
		{"weekday_name", `date_format(t1, "%A")`, "Saturday", false},
		{"abbreviated_weekday", `date_format(t1, "%a")`, "Sat", false},
		{"complex_format", `date_format(t1, "%a, %d %b %Y %T %z")`, "Sat, 01 Jan 2022 00:00:00 +0000", false},
		{"invalid_time_value", `date_format("not a time", "%Y-%m-%d")`, nil, true},
	}

	testSuites["date_parse"] = []test{
		{"no_args", `date_parse()`, nil, true},
		{"one_arg", `date_parse("2022-01-01")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"too_many_args", `date_parse("2022-01-01", "%Y-%m-%d", "extra")`, nil, true},
		{"basic_format", `date_parse("2022-01-01", "%Y-%m-%d")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"datedate_format", `date_parse("2022-01-01 15:30:45", "%Y-%m-%d %H:%M:%S")`, time.Date(2022, 1, 1, 15, 30, 45, 0, time.UTC), false},
		{"year_month_only", `date_parse("2022-01", "%Y-%m")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"year_only", `date_parse("2022", "%Y")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"month_name", `date_parse("January 2022", "%B %Y")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"abbreviated_month", `date_parse("Jan 2022", "%b %Y")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"weekday_format", `date_parse("Saturday, 01-Jan-2022", "%A, %d-%b-%Y")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"rfc_format", `date_parse("Sat, 01 Jan 2022 15:30:45 +0000", "%a, %d %b %Y %H:%M:%S %z")`, time.Date(2022, 1, 1, 15, 30, 45, 0, time.UTC), false},
		{"invalid_date_string", `date_parse("not a date", "%Y-%m-%d")`, nil, true},
		{"invalid_format_string", `date_parse("2022-01-01", "invalid")`, nil, true},
		{"mismatched_format", `date_parse("2022-01-01", "%H:%M:%S")`, nil, true},

		// Unix timestamp tests
		{"auto_unix_seconds", `date_parse("1640995200", "auto")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"auto_unix_seconds_9digits", `date_parse("946684800", "auto")`, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"auto_unix_milliseconds", `date_parse("1640995200000", "auto")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"auto_unix_microseconds", `date_parse("1640995200000000", "auto")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"auto_unix_nanoseconds", `date_parse("1640995200000000000", "auto")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},

		// Standard auto format tests
		{"auto_iso_date", `date_parse("2022-01-01", "auto")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"auto_iso_datetime", `date_parse("2022-01-01 15:30:45", "auto")`, time.Date(2022, 1, 1, 15, 30, 45, 0, time.UTC), false},
		{"auto_iso_datetime_t", `date_parse("2022-01-01T15:30:45", "auto")`, time.Date(2022, 1, 1, 15, 30, 45, 0, time.UTC), false},
		{"auto_iso_datetime_zone", `date_parse("2022-01-01T15:30:45Z", "auto")`, time.Date(2022, 1, 1, 15, 30, 45, 0, time.UTC), false},

		// Precision tests for Unix timestamps
		{"auto_unix_seconds_with_time", `date_parse("1641031845", "auto")`, time.Date(2022, 1, 1, 10, 10, 45, 0, time.UTC), false}, // 2022-01-01 10:30:45
		{"auto_unix_milli_with_time", `date_parse("1641031845123", "auto")`, time.Date(2022, 1, 1, 10, 10, 45, 123000000, time.UTC), false},
		{"auto_unix_micro_with_time", `date_parse("1641031845123456", "auto")`, time.Date(2022, 1, 1, 10, 10, 45, 123456000, time.UTC), false},
		{"auto_unix_nano_with_time", `date_parse("1641031845123456789", "auto")`, time.Date(2022, 1, 1, 10, 10, 45, 123456789, time.UTC), false},
	}

	testSuites["combined_time"] = []test{
		{"parse_then_format", `date_format(date_parse("2022-01-01", "%Y-%m-%d"), "%B %d, %Y")`, "January 01, 2022", false},
		{"format_variable", `date_format(t2, "%Y")`, "2023", false},
		{"compare_parsed_times", `is_greater(date_parse("2023-01-01", "%Y-%m-%d"), date_parse("2022-01-01", "%Y-%m-%d"))`, true, false},
		{"max_with_parsed_time", `date_format(greatest(date_parse("2021-01-01", "%Y-%m-%d"), t1, t2), "%Y")`, "2023", false},
		{"min_with_parsed_time", `date_format(least(date_parse("2021-01-01", "%Y-%m-%d"), t1, t2), "%Y")`, "2021", false},
	}

	testSuites["contains"] = []test{
		{"no_args", `contains()`, nil, true},
		{"one_arg", `contains("hello world")`, nil, true},
		{"too_many_args", `contains("hello world", "world", "extra")`, nil, true},
		{"basic_contains_true", `contains("hello world", "world")`, true, false},
		{"basic_contains_false", `contains("hello world", "universe")`, false, false},
		{"empty_string_search", `contains("hello world", "")`, true, false}, // Empty string is always contained
		{"empty_target_string", `contains("", "world")`, false, false},
		{"case_sensitive", `contains("Hello World", "hello")`, false, false},
		{"number_as_string", `contains("value123", "123")`, true, false},
		{"substring_at_start", `contains("hello world", "hello")`, true, false},
		{"substring_at_end", `contains("hello world", "world")`, true, false},
		{"substring_in_middle", `contains("hello wonderful world", "wonderful")`, true, false},
		{"non_string_first_arg", `contains(123, "2")`, true, false},                       // 123 stringified to "123"
		{"non_string_second_arg", `contains("value123", 123)`, true, false},               // 123 stringified to "123"
		{"variable_input", `contains(someVar, "tual")`, true, false},                      // someVar = "actual"
		{"variable_substring", `contains("I am the actual value", someVar)`, true, false}, // someVar = "actual"
	}

	testSuites["try_cast"] = []test{
		{"no_args", `try_cast()`, nil, false},
		{"one_arg", `try_cast("42")`, nil, false},
		{"too_many_args", `try_cast("42", "int", "extra")`, nil, false},

		// String casting
		{"to_string_from_int", `try_cast(42, "string")`, "42", false},
	}

	testSuites["cast"] = []test{
		{"no_args", `cast()`, nil, true},
		{"one_arg", `cast("42")`, nil, true},
		{"too_many_args", `cast("42", "int", "extra")`, nil, true},

		// String casting
		{"to_string_from_int", `cast(42, "string")`, "42", false},
		{"to_string_from_float", `cast(42.5, "string")`, "42.5", false},
		{"to_string_from_bool", `cast(true, "string")`, "true", false},
		{"to_string_from_nil", `cast(nil, "string")`, nil, false},

		// Int casting
		{"int_parse_from_string", `cast("42", "int")`, 42, false},
		{"int_parse_from_invalid_string", `cast("abc", "int")`, nil, true},
		{"int_parse_from_float", `cast(42.7, "int")`, 42, false},
		{"int_parse_from_bool_true", `cast(true, "int")`, 1, false},
		{"int_parse_from_bool_false", `cast(false, "int")`, 0, false},
		{"int_parse_from_nil", `cast(nil, "int")`, nil, false},

		// Float casting
		{"float_parse_from_string", `cast("42.5", "float")`, 42.5, false},
		{"float_parse_from_invalid_string", `cast("abc", "float")`, nil, true},
		{"float_parse_from_int", `cast(42, "float")`, 42.0, false},
		{"float_parse_from_bool_true", `cast(true, "float")`, 1.0, false},
		{"float_parse_from_bool_false", `cast(false, "float")`, 0.0, false},
		{"float_parse_from_nil", `cast(nil, "float")`, nil, false},
		{"to_decimal_from_string", `cast("42.5", "decimal")`, 42.5, false},

		// Boolean casting
		{"to_bool_from_string_true", `cast("true", "bool")`, true, false},
		{"to_bool_from_string_yes", `cast("yes", "bool")`, true, false},
		{"to_bool_from_string_1", `cast("1", "bool")`, true, false},
		{"to_bool_from_string_false", `cast("false", "bool")`, false, false},
		{"to_bool_from_string_no", `cast("no", "bool")`, false, false},
		{"to_bool_from_string_0", `cast("0", "bool")`, false, false},
		{"to_bool_from_invalid_string", `cast("maybe", "bool")`, nil, true},
		{"to_bool_from_int_1", `cast(1, "bool")`, true, false},
		{"to_bool_from_int_0", `cast(0, "bool")`, false, false},
		{"to_bool_from_float_1", `cast(1.0, "bool")`, true, false},
		{"to_bool_from_float_0", `cast(0.0, "bool")`, false, false},
		{"to_bool_from_nil", `cast(nil, "bool")`, nil, false},
		{"to_boolean_from_string_true", `cast("true", "boolean")`, true, false},

		// Timestamp casting
		{"to_timestamp_from_iso_string", `cast("2022-01-01T12:30:45Z", "timestamp")`, time.Date(2022, 1, 1, 12, 30, 45, 0, time.UTC), false},
		{"to_timestamp_from_date_string", `cast("2022-01-01", "timestamp")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"to_timestamp_from_invalid_string", `cast("not a date", "timestamp")`, nil, true},
		{"to_timestamp_from_unix_seconds", `cast(1640995200, "timestamp")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"to_timestamp_from_nil", `cast(nil, "timestamp")`, nil, false},
		{"to_date_from_string", `cast("2022-01-01", "date")`, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), false},

		// Invalid type
		{"invalid_cast_type", `cast("value", "invalid_type")`, nil, true},
	}

	testSuites["int_format"] = []test{
		{"no_args", `int_format()`, nil, true},
		{"one_arg", `int_format(42)`, nil, true},
		{"basic_format", `int_format(42, "")`, "42", false},
		{"negative_number", `int_format(-42, "")`, "-42", false},
		{"zero_padding", `int_format(42, "05")`, "00042", false},
		{"hex_lowercase", `int_format(255, "hex")`, "ff", false},
		{"hex_lowercase_short", `int_format(255, "x")`, "ff", false},
		{"hex_uppercase", `int_format(255, "HEX")`, "FF", false},
		{"hex_uppercase_short", `int_format(255, "X")`, "FF", false},
		{"binary", `int_format(10, "bin")`, "1010", false},
		{"binary_short", `int_format(10, "b")`, "1010", false},
		{"octal", `int_format(64, "oct")`, "100", false},
		{"octal_short", `int_format(64, "o")`, "100", false},
		{"string_input", `int_format("42", "05")`, "00042", false},
		{"float_input", `int_format(42.7, "")`, "42", false},
		{"invalid_input", `int_format("abc", "")`, nil, true},
		{"invalid_format", `int_format(42, 123)`, nil, true},
	}

	testSuites["float_format"] = []test{
		{"no_args", `float_format()`, nil, true},
		{"one_arg", `float_format(42.5)`, nil, true},
		{"basic_format", `float_format(42.5, "")`, "42.5", false},
		{"decimal_places", `float_format(42.5678, ".2")`, "42.57", false},
		{"width_and_precision", `float_format(42.5678, "10.2")`, "     42.57", false},
		{"scientific_lowercase", `float_format(12345.6789, "e")`, "1.234568e+04", false},
		{"scientific_uppercase", `float_format(12345.6789, "E")`, "1.234568E+04", false},
		{"scientific_word", `float_format(12345.6789, "sci")`, "1.234568e+04", false},
		{"scientific_uppercase_word", `float_format(12345.6789, "SCI")`, "1.234568E+04", false},
		{"negative_number", `float_format(-42.5678, ".2")`, "-42.57", false},
		{"zero_value", `float_format(0, ".2")`, "0.00", false},
		{"large_number", `float_format(1000000.5678, ".2")`, "1000000.57", false},
		{"tiny_number", `float_format(0.0000123, ".10")`, "0.0000123000", false},
		{"string_input", `float_format("42.5678", ".2")`, "42.57", false},
		{"int_input", `float_format(42, ".2")`, "42.00", false},
		{"invalid_input", `float_format("abc", ".2")`, nil, true},
		{"invalid_format", `float_format(42.5, 123)`, nil, true},
	}

	// Tests for string manipulation functions
	testSuites["substring"] = []test{
		{"no_args", `substring()`, nil, true},
		{"one_arg", `substring("hello")`, nil, true},
		{"basic_substring", `substring("hello world", 0, 5)`, "hello", false},
		{"substring_to_end", `substring("hello world", 6)`, "world", false},
		{"negative_start", `substring("hello world", -5)`, "world", false},
		{"negative_end", `substring("hello world", 0, -6)`, "hello", false},
		{"out_of_bounds_start", `substring("hello", 10)`, "", false},
		{"out_of_bounds_end", `substring("hello", 0, 10)`, "hello", false},
		{"start_greater_than_end", `substring("hello", 3, 2)`, "", false},
		{"non_string_input", `substring(12345, 0, 3)`, "123", false},
	}

	testSuites["uppercase_lowercase"] = []test{
		{"uppercase_no_args", `upper()`, nil, true},
		{"uppercase_basic", `upper("hello")`, "HELLO", false},
		{"uppercase_mixed", `upper("Hello World")`, "HELLO WORLD", false},
		{"uppercase_already", `upper("HELLO")`, "HELLO", false},
		{"uppercase_numbers", `upper("hello123")`, "HELLO123", false},
		{"uppercase_symbols", `upper("hello!@#")`, "HELLO!@#", false},
		{"uppercase_non_string", `upper(123)`, "123", false},

		{"lowercase_no_args", `lower()`, nil, true},
		{"lowercase_basic", `lower("HELLO")`, "hello", false},
		{"lowercase_mixed", `lower("Hello World")`, "hello world", false},
		{"lowercase_already", `lower("hello")`, "hello", false},
		{"lowercase_numbers", `lower("HELLO123")`, "hello123", false},
		{"lowercase_symbols", `lower("HELLO!@#")`, "hello!@#", false},
		{"lowercase_non_string", `lower(123)`, "123", false},
	}

	testSuites["regex"] = []test{
		{"match_no_args", `regex_match()`, nil, true},
		{"match_one_arg", `regex_match("hello")`, nil, true},
		{"match_basic", `regex_match("hello world", "world")`, true, false},
		{"match_pattern", `regex_match("hello world", "^hello.*")`, true, false},
		{"match_negative", `regex_match("hello world", "^world")`, false, false},
		{"match_case_sensitive", `regex_match("Hello World", "hello")`, false, false},
		{"match_case_insensitive", `regex_match("Hello World", "(?i)hello")`, true, false},
		{"match_digits", `regex_match("abc123", "\\d+")`, true, false},
		{"match_invalid_pattern", `regex_match("hello", "[")`, nil, true},

		{"extract_no_args", `regex_extract()`, nil, true},
		{"extract_one_arg", `regex_extract("hello")`, nil, true},
		{"extract_basic", `regex_extract("hello world", "hello")`, []string{"hello"}, false},
		{"extract_with_groups", `regex_extract("hello world", "(hello) (world)")`, []string{"hello world", "hello", "world"}, false},
		{"extract_no_match", `regex_extract("hello world", "xyz")`, nil, false},
		{"extract_specific_group", `regex_extract("hello world", "(hello) (world)", 2)`, "world", false},
		{"extract_specific_group_out_of_range", `regex_extract("hello world", "(hello) (world)", 3)`, nil, true},
		{"extract_invalid_pattern", `regex_extract("hello", "[")`, nil, true},
	}

	// Tests for comparison operations
	testSuites["equals"] = []test{
		{"no_args", `equals()`, nil, true},
		{"one_arg", `equals("test")`, nil, true},
		{"strings_equal", `equals("test", "test")`, true, false},
		{"strings_not_equal", `equals("test", "other")`, false, false},
		{"numbers_equal", `equals(42, 42)`, true, false},
		{"numbers_not_equal", `equals(42, 43)`, false, false},
		{"mixed_types_number_string", `equals(42, "42")`, false, false},
		{"mixed_types_bool_number", `equals(true, 1)`, false, false},
		{"nil_values_equal", `equals(null, null)`, true, false},
		{"nil_and_value_not_equal", `equals(null, "test")`, false, false},
		{"booleans_equal", `equals(true, true)`, true, false},
		{"booleans_not_equal", `equals(true, false)`, false, false},
		{"arrays_equal", `equals([1, 2, 3], [1, 2, 3])`, true, false},
		{"arrays_different_order", `equals([1, 2, 3], [3, 2, 1])`, false, false},
		{"arrays_different_length", `equals([1, 2, 3], [1, 2])`, false, false},
		{"nested_arrays_equal", `equals([1, [2, 3]], [1, [2, 3]])`, true, false},
		{"float_comparison", `equals(1.0, 1.0)`, true, false},
		{"float_precision", `equals(1.0000001, 1.0000002)`, true, false}, // Within epsilon
		{"float_different", `equals(1.1, 1.2)`, false, false},
		{"times_equal", `equals(t1, t1)`, true, false},
		{"times_not_equal", `equals(t1, t2)`, false, false},
	}

	testSuites["is_null_is_empty"] = []test{
		{"is_null_no_args", `is_null()`, nil, true},
		{"is_null_true", `is_null(null)`, true, false},
		{"is_null_false_string", `is_null("test")`, false, false},
		{"is_null_false_empty_string", `is_null("")`, false, false},
		{"is_null_false_number", `is_null(0)`, false, false},
		{"is_null_false_bool", `is_null(false)`, false, false},
		{"is_null_variable_true", `is_null(someNil)`, true, false},
		{"is_null_variable_false", `is_null(someVar)`, false, false},

		{"is_empty_no_args", `is_empty()`, nil, true},
		{"is_empty_null", `is_empty(null)`, true, false},
		{"is_empty_string_true", `is_empty("")`, true, false},
		{"is_empty_string_false", `is_empty("test")`, false, false},
		{"is_empty_array_true", `is_empty([])`, true, false},
		{"is_empty_array_false", `is_empty([1, 2, 3])`, false, false},
		{"is_empty_number_true", `is_empty(0)`, true, false},
		{"is_empty_number_false", `is_empty(42)`, false, false},
		{"is_empty_bool_true", `is_empty(false)`, true, false},
		{"is_empty_bool_false", `is_empty(true)`, false, false},
		{"is_empty_variable_true", `is_empty(someEmpty)`, true, false},
		{"is_empty_variable_false", `is_empty(someVar)`, false, false},
	}

	// Tests for object operations
	testSuites["keys_values"] = []test{
		{"keys_no_args", `keys()`, nil, true},
		{"keys_null", `keys(null)`, []any{}, false},
		{"keys_map", `join(sort(keys({"a": 1, "b": 2})), ",")`, "a,b", false},
		{"keys_empty_map", `keys({})`, []any{}, false},
		{"keys_not_map_or_struct", `keys("string")`, nil, true},

		{"values_no_args", `values()`, nil, true},
		{"values_null", `values(null)`, []any{}, false},
		{"values_map_contains", `length(values({"a": 1, "b": 2}))`, 2, false},
		{"values_empty_map", `values({})`, []any{}, false},
		{"values_not_map_or_struct", `values("string")`, nil, true},
	}

	// Tests for date_diff operation
	testSuites["date_diff"] = []test{
		{"no_args", `date_diff()`, nil, true},
		{"one_arg", `date_diff(t1)`, nil, true},
		{"basic_diff_seconds", `date_diff(t2, t1, "second")`, float64(365 * 24 * 60 * 60), false}, // t2 is 1 year after t1
		{"diff_with_unit_hour", `date_diff(t2, t1, "hour")`, float64(365 * 24), false},
		{"diff_with_unit_day_implicit", `date_diff(t2, t1)`, float64(365), false},
		{"diff_with_unit_day", `date_diff(t2, t1, "day")`, float64(365), false},
		{"diff_with_unit_month", `date_diff(t2, t1, "month")`, float64(12), false}, // Exactly 12 months difference
		{"diff_with_unit_year", `date_diff(t2, t1, "year")`, float64(1), false},
		{"negative_diff", `date_diff(t1, t2, "second")`, float64(-365 * 24 * 60 * 60), false},
		{"diff_same_time", `date_diff(t1, t1)`, float64(0), false},
		{"invalid_first_arg", `date_diff("not a time", t1)`, nil, true},
		{"invalid_second_arg", `date_diff(t1, "not a time")`, nil, true},
		{"invalid_unit", `date_diff(t2, t1, "invalid")`, nil, true},
	}

	// Tests for array operations
	testSuites["length"] = []test{
		{"no_args", `length()`, nil, true},
		{"string_length", `length("hello")`, 5, false},
		{"array_length", `length([1, 2, 3, 4, 5])`, 5, false},
		{"empty_array", `length([])`, 0, false},
		{"empty_string", `length("")`, 0, false},
		{"map_length", `length({"a": 1, "b": 2})`, 2, false},
		{"nil_value", `length(null)`, 0, false},
		{"unsupported_type", `length(42)`, nil, true},
	}

	testSuites["join"] = []test{
		{"no_args", `join()`, nil, true},
		{"one_arg", `join([1, 2, 3])`, nil, true},
		{"basic_join", `join(["a", "b", "c"], ", ")`, "a, b, c", false},
		{"join_numbers", `join([1, 2, 3], "-")`, "1-2-3", false},
		{"join_mixed", `join([1, "b", true], "/")`, "1/b/true", false},
		{"join_empty_array", `join([], ", ")`, "", false},
		{"join_single_element", `join(["only"], ", ")`, "only", false},
		{"join_empty_separator", `join(["a", "b", "c"], "")`, "abc", false},
		{"not_an_array", `join("abc", ", ")`, nil, true},
		{"non_string_separator", `join(["a", "b", "c"], 123)`, "a123b123c", false},
	}

	// Tests for URL encoding/decoding
	testSuites["url_encoding"] = []test{
		{"encode_no_args", `encode_url()`, nil, true},
		{"encode_basic", `encode_url("hello world")`, "hello+world", false},
		{"encode_special_chars", `encode_url("test?&=#+%")`, "test%3F%26%3D%23%2B%25", false},
		{"encode_unicode", `encode_url("你好")`, "%E4%BD%A0%E5%A5%BD", false},
		{"encode_empty", `encode_url("")`, "", false},
		{"encode_non_string", `encode_url(123)`, "123", false},

		{"decode_no_args", `decode_url()`, nil, true},
		{"decode_basic", `decode_url("hello+world")`, "hello world", false},
		{"decode_special_chars", `decode_url("test%3F%26%3D%23%2B%25")`, "test?&=#+%", false},
		{"decode_unicode", `decode_url("%E4%BD%A0%E5%A5%BD")`, "你好", false},
		{"decode_empty", `decode_url("")`, "", false},
		{"decode_non_string", `decode_url(123)`, "123", false},
		{"decode_invalid", `decode_url("%invalid")`, nil, true},
	}

	// Tests for base64 encoding/decoding
	testSuites["base64"] = []test{
		{"encode_no_args", `encode_base64()`, nil, true},
		{"encode_basic", `encode_base64("hello world")`, "aGVsbG8gd29ybGQ=", false},
		{"encode_special_chars", `encode_base64("test?&=#+%")`, "dGVzdD8mPSMrJQ==", false},
		{"encode_unicode", `encode_base64("你好")`, "5L2g5aW9", false},
		{"encode_empty", `encode_base64("")`, "", false},
		{"encode_non_string", `encode_base64(123)`, "MTIz", false},

		{"decode_no_args", `decode_base64()`, nil, true},
		{"decode_basic", `decode_base64("aGVsbG8gd29ybGQ=")`, "hello world", false},
		{"decode_special_chars", `decode_base64("dGVzdD8mPSMrJQ==")`, "test?&=#+%", false},
		{"decode_unicode", `decode_base64("5L2g5aW9")`, "你好", false},
		{"decode_empty", `decode_base64("")`, "", false},
		{"decode_non_string", `decode_base64(123)`, nil, true},
		{"decode_invalid", `decode_base64("invalid===")`, nil, true},
	}

	// Tests for hash function
	testSuites["hash"] = []test{
		{"no_args", `hash()`, nil, true},
		{"md5", `hash("hello world", "md5")`, "5eb63bbbe01eeed093cb22bb8f5acdc3", false},
		{"sha1", `hash("hello world", "sha1")`, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed", false},
		{"sha256_explicit", `hash("hello world", "sha256")`, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", false},
		{"sha256_default", `hash("hello world")`, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", false},
		{"sha512", `hash("hello world", "sha512")`, "309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f", false},
		{"empty_string", `hash("")`, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", false}, // SHA-256 of empty string
		{"unsupported_algorithm", `hash("hello", "unknown")`, nil, true},
		{"non_string_input", `hash(123)`, "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3", false}, // SHA-256 of "123"
	}

	// Special test for UUID and date_diff with months (which gives approximate results)
	t.Run("special_tests", func(t *testing.T) {
		// Test date_diff with month unit
		eval := goval.NewEvaluator()

		// Test month calculation
		result, err := eval.Evaluate(`date_diff(t2, t1, "month")`, variables, Functions.Generate())
		assert.NoError(t, err)
		monthDiff, ok := result.(float64)
		if assert.True(t, ok, "Month diff should be a float64") {
			// The month calculation is approximate
			assert.InDelta(t, 12.0, monthDiff, 0.5, "Month diff should be approximately 12")
		}

		// UUID validation
		result, err = eval.Evaluate(`uuid()`, variables, Functions.Generate())

		// Check that no error occurred
		if assert.NoError(t, err) {
			// Check that the result is a string
			uuidStr, ok := result.(string)
			if assert.True(t, ok, "UUID result should be a string") {
				// Check that the UUID has the correct format using a regex
				matched, err := regexp.MatchString(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, uuidStr)
				assert.NoError(t, err, "UUID format regex should be valid")
				assert.True(t, matched, "UUID should have the correct format: %s", uuidStr)

				// Verify it's a valid v4 UUID (version bits)
				assert.Equal(t, "4", string(uuidStr[14]), "UUID version should be 4")

				// Check the variant bits (should be 8, 9, a, or b)
				variantChar := string(uuidStr[19])
				assert.Contains(t, []string{"8", "9", "a", "b"}, variantChar, "UUID variant should be correct")
			}
		}
	})

	// Tests for UUID function
	testSuites["uuid"] = []test{
		// UUID is random, so we can't test for a specific value
		// We can only verify that it produces a string with the correct format
		{"uuid_format", `length(uuid())`, 36, false}, // UUID format: 8-4-4-4-12 chars + 4 hyphens = 36 chars
	}

	// Special test for `uuid()` since it generates random values - now moved to special_tests

	// Tests for date_trunc operation
	testSuites["date_trunc"] = []test{
		{"no_args", `date_trunc()`, nil, true},
		{"one_arg", `date_trunc(t1)`, nil, true},
		{"invalid_time", `date_trunc("not a time", "day")`, nil, true},
		{"invalid_unit", `date_trunc(t1, "invalid")`, nil, true},

		// Test with a specific time: 2022-01-15 14:30:45.123456789
		{"trunc_microsecond", `date_format(date_trunc(date_parse("2022-01-15 14:30:45.123456789", "auto"), "microsecond"), "%Y-%m-%d %H:%M:%S.%f")`, "2022-01-15 14:30:45.123456", false},
		{"trunc_millisecond", `date_format(date_trunc(date_parse("2022-01-15 14:30:45.123456789", "auto"), "millisecond"), "%Y-%m-%d %H:%M:%S.%f")`, "2022-01-15 14:30:45.123000", false},
		{"trunc_second", `date_format(date_trunc(date_parse("2022-01-15 14:30:45.123456789", "auto"), "second"), "%Y-%m-%d %H:%M:%S")`, "2022-01-15 14:30:45", false},
		{"trunc_minute", `date_format(date_trunc(date_parse("2022-01-15 14:30:45.123456789", "auto"), "minute"), "%Y-%m-%d %H:%M:%S")`, "2022-01-15 14:30:00", false},
		{"trunc_hour", `date_format(date_trunc(date_parse("2022-01-15 14:30:45.123456789", "auto"), "hour"), "%Y-%m-%d %H:%M:%S")`, "2022-01-15 14:00:00", false},
		{"trunc_day", `date_format(date_trunc(date_parse("2022-01-15 14:30:45.123456789", "auto"), "day"), "%Y-%m-%d %H:%M:%S")`, "2022-01-15 00:00:00", false},

		// Test week truncation with a known date: 2022-01-15 is a Saturday, so week truncation should go to 2022-01-09 (Sunday)
		{"trunc_week", `date_format(date_trunc(date_parse("2022-01-15", "auto"), "week"), "%Y-%m-%d")`, "2022-01-09", false},

		{"trunc_month", `date_format(date_trunc(date_parse("2022-01-15", "auto"), "month"), "%Y-%m-%d")`, "2022-01-01", false},

		// Test quarter truncation
		{"trunc_quarter_q1", `date_format(date_trunc(date_parse("2022-02-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-01-01", false},
		{"trunc_quarter_q2", `date_format(date_trunc(date_parse("2022-05-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-04-01", false},
		{"trunc_quarter_q3", `date_format(date_trunc(date_parse("2022-08-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-07-01", false},
		{"trunc_quarter_q4", `date_format(date_trunc(date_parse("2022-11-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-10-01", false},

		{"trunc_year", `date_format(date_trunc(date_parse("2022-06-15", "auto"), "year"), "%Y-%m-%d")`, "2022-01-01", false},
	}

	// Tests for date_last operation
	testSuites["date_last"] = []test{
		{"no_args", `date_last()`, nil, true},
		{"invalid_time", `date_last("not a time")`, nil, true},
		{"invalid_period", `date_last(t1, "invalid")`, nil, true},

		// Test last day of month
		{"last_day_jan", `date_format(date_last(date_parse("2022-01-15", "auto")), "%Y-%m-%d")`, "2022-01-31", false},
		{"last_day_feb_regular", `date_format(date_last(date_parse("2022-02-15", "auto")), "%Y-%m-%d")`, "2022-02-28", false},
		{"last_day_feb_leap", `date_format(date_last(date_parse("2020-02-15", "auto")), "%Y-%m-%d")`, "2020-02-29", false},
		{"last_day_apr", `date_format(date_last(date_parse("2022-04-15", "auto"), "month"), "%Y-%m-%d")`, "2022-04-30", false},

		// Test last day of quarter
		{"last_day_q1", `date_format(date_last(date_parse("2022-02-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-03-31", false},
		{"last_day_q2", `date_format(date_last(date_parse("2022-05-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-06-30", false},
		{"last_day_q3", `date_format(date_last(date_parse("2022-08-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-09-30", false},
		{"last_day_q4", `date_format(date_last(date_parse("2022-11-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-12-31", false},

		// Test last day of year
		{"last_day_year", `date_format(date_last(date_parse("2022-06-15", "auto"), "year"), "%Y-%m-%d")`, "2022-12-31", false},

		// Test last day of week - for 2022-01-12 (Wednesday), last day should be 2022-01-15 (Saturday)
		{"last_day_week", `date_format(date_last(date_parse("2022-01-12", "auto"), "week"), "%Y-%m-%d")`, "2022-01-15", false},
	}

	// Tests for date_first operation
	testSuites["date_first"] = []test{
		{"no_args", `date_first()`, nil, true},
		{"invalid_time", `date_first("not a time")`, nil, true},
		{"invalid_period", `date_first(t1, "invalid")`, nil, true},

		// Test first day of month
		{"first_day_jan", `date_format(date_first(date_parse("2022-01-15", "auto")), "%Y-%m-%d")`, "2022-01-01", false},
		{"first_day_feb", `date_format(date_first(date_parse("2022-02-15", "auto")), "%Y-%m-%d")`, "2022-02-01", false},
		{"first_day_month_explicit", `date_format(date_first(date_parse("2022-04-15", "auto"), "month"), "%Y-%m-%d")`, "2022-04-01", false},

		// Test first day of quarter
		{"first_day_q1", `date_format(date_first(date_parse("2022-02-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-01-01", false},
		{"first_day_q2", `date_format(date_first(date_parse("2022-05-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-04-01", false},
		{"first_day_q3", `date_format(date_first(date_parse("2022-08-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-07-01", false},
		{"first_day_q4", `date_format(date_first(date_parse("2022-11-15", "auto"), "quarter"), "%Y-%m-%d")`, "2022-10-01", false},

		// Test first day of year
		{"first_day_year", `date_format(date_first(date_parse("2022-06-15", "auto"), "year"), "%Y-%m-%d")`, "2022-01-01", false},

		// Test first day of week - for 2022-01-12 (Wednesday), first day should be 2022-01-09 (Sunday)
		{"first_day_week", `date_format(date_first(date_parse("2022-01-12", "auto"), "week"), "%Y-%m-%d")`, "2022-01-09", false},
	}

	// Tests for date_add operation
	testSuites["date_add"] = []test{
		{"no_args", `date_add()`, nil, true},
		{"one_arg", `date_add(t1)`, nil, true},
		{"invalid_time", `date_add("not a time", "1h")`, nil, true},
		{"invalid_duration", `date_add(t1, "invalid")`, nil, true},

		// Test with string duration format
		{"add_hours_string", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), "2h"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 14:00:00", false},
		{"add_minutes_string", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), "30m"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 12:30:00", false},
		{"add_complex_duration", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), "1h30m45s"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 13:30:45", false},
		{"add_negative_duration", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), "-1h"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 11:00:00", false},

		// Test with numeric duration (seconds)
		{"add_seconds_numeric", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 120), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 12:02:00", false},
		{"add_negative_seconds", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), -120), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 11:58:00", false},

		// Test with amount and unit
		{"add_seconds_with_unit", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 30, "second"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 12:00:30", false},
		{"add_minutes_with_unit", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 15, "minute"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 12:15:00", false},
		{"add_hours_with_unit", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 3, "hour"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 15:00:00", false},
		{"add_days_with_unit", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 2, "day"), "%Y-%m-%d %H:%M:%S")`, "2022-01-03 12:00:00", false},
		{"add_weeks_with_unit", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 1, "week"), "%Y-%m-%d %H:%M:%S")`, "2022-01-08 12:00:00", false},
		{"add_months_with_unit", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 1, "month"), "%Y-%m-%d %H:%M:%S")`, "2022-01-31 12:00:00", false},
		{"add_years_with_unit", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 1, "year"), "%Y-%m-%d %H:%M:%S")`, "2023-01-01 12:00:00", false},

		// Test with abbreviated unit names
		{"add_with_abbrev_s", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 30, "s"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 12:00:30", false},
		{"add_with_abbrev_m", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 15, "m"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 12:15:00", false},
		{"add_with_abbrev_h", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 3, "h"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 15:00:00", false},
		{"add_with_abbrev_d", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 2, "d"), "%Y-%m-%d %H:%M:%S")`, "2022-01-03 12:00:00", false},
		{"add_with_abbrev_w", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 1, "w"), "%Y-%m-%d %H:%M:%S")`, "2022-01-08 12:00:00", false},
		{"add_with_abbrev_y", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 1, "y"), "%Y-%m-%d %H:%M:%S")`, "2023-01-01 12:00:00", false},

		// Test with plural unit names
		{"add_with_plural_seconds", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 30, "seconds"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 12:00:30", false},
		{"add_with_plural_minutes", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 15, "minutes"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 12:15:00", false},
		{"add_with_plural_hours", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 3, "hours"), "%Y-%m-%d %H:%M:%S")`, "2022-01-01 15:00:00", false},
		{"add_with_plural_days", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 2, "days"), "%Y-%m-%d %H:%M:%S")`, "2022-01-03 12:00:00", false},
		{"add_with_plural_weeks", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 1, "weeks"), "%Y-%m-%d %H:%M:%S")`, "2022-01-08 12:00:00", false},
		{"add_with_plural_months", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 1, "months"), "%Y-%m-%d %H:%M:%S")`, "2022-01-31 12:00:00", false},
		{"add_with_plural_years", `date_format(date_add(date_parse("2022-01-01 12:00:00", "auto"), 1, "years"), "%Y-%m-%d %H:%M:%S")`, "2022-12-31 12:00:00", false},

		// Test with negative values
		{"add_negative_days", `date_format(date_add(date_parse("2022-01-15 12:00:00", "auto"), -5, "day"), "%Y-%m-%d %H:%M:%S")`, "2022-01-10 12:00:00", false},
		{"add_negative_months", `date_format(date_add(date_parse("2022-03-15 12:00:00", "auto"), -1, "month"), "%Y-%m-%d %H:%M:%S")`, "2022-02-13 12:00:00", false},

		// Edge cases
		{"cross_month_boundary", `date_format(date_add(date_parse("2022-01-31 12:00:00", "auto"), 1, "day"), "%Y-%m-%d %H:%M:%S")`, "2022-02-01 12:00:00", false},
		{"cross_year_boundary", `date_format(date_add(date_parse("2022-12-31 12:00:00", "auto"), 1, "day"), "%Y-%m-%d %H:%M:%S")`, "2023-01-01 12:00:00", false},
		{"leap_year", `date_format(date_add(date_parse("2020-02-28 12:00:00", "auto"), 1, "day"), "%Y-%m-%d %H:%M:%S")`, "2020-02-29 12:00:00", false},
		{"daylight_saving", `date_format(date_add(date_parse("2022-03-12 12:00:00", "auto"), 24, "hour"), "%Y-%m-%d %H:%M:%S")`, "2022-03-13 12:00:00", false},
	}

	testSuites["jmespath"] = []test{
		{"no_args", `jmespath()`, nil, true},
		{"one_arg", `jmespath({"foo": "bar"})`, nil, true},
		{"basic_query", `jmespath({"foo": "bar"}, "foo")`, "bar", false},
		{"nested_query", `jmespath({"foo": {"bar": "baz"}}, "foo.bar")`, "baz", false},
		{"array_query", `jmespath({"foo": ["bar", "baz"]}, "foo[0]")`, "bar", false},
		{"array_slice", `jmespath({"foo": ["bar", "baz", "qux"]}, "foo[0:2]")`, []any{"bar", "baz"}, false},
		// {"filter_expression", `jmespath({"people": [{"name": "bob", "age": 25}, {"name": "alice", "age": 30}]}, "people[?age==30].name")`, []any{"alice"}, false},
		{"multiselect_hash", `jmespath({"foo": {"bar": "baz", "qux": "quux"}}, "foo.{b: bar, q: qux}")`, map[string]any{"b": "baz", "q": "quux"}, false},
		{"multiselect_list", `jmespath({"foo": {"bar": "baz", "qux": "quux"}}, "foo.[bar, qux]")`, []any{"baz", "quux"}, false},
		{"function_expression", `jmespath({"foo": ["bar", "baz"]}, "length(foo)")`, float64(2), false},
		{"projection", `jmespath({"people": [{"name": "bob"}, {"name": "alice"}]}, "people[].name")`, []any{"bob", "alice"}, false},
		{"not_found", `jmespath({"foo": "bar"}, "baz")`, nil, false},
		{"invalid_expression", `jmespath({"foo": "bar"}, "[invalid")`, nil, true},
		{"non_object_input", `jmespath("not an object", "foo")`, nil, false},
		{"null_input", `jmespath(null, "foo")`, nil, false},
	}

	testSuites["sort"] = []test{
		{"no_args", `sort()`, nil, true},
		{"empty_array", `sort([])`, []any{}, false},
		{"single_element", `sort([1])`, []any{1}, false},
		{"numeric_array", `sort([3, 1, 4, 2])`, []any{1, 2, 3, 4}, false},
		{"string_array", `sort(["c", "a", "b"])`, []any{"a", "b", "c"}, false},
		{"mixed_array", `sort([3, "1", 4, "2"])`, []any{"1", "2", 3, 4}, false},
		{"array_with_nulls", `sort([3, nil, 1, nil, 2])`, []any{1, 2, 3, nil, nil}, false},

		// Test descending order with string parameter
		{"numeric_desc_string", `sort([3, 1, 4, 2], "desc")`, []any{4, 3, 2, 1}, false},
		{"string_desc_string", `sort(["c", "a", "b"], "DESC")`, []any{"c", "b", "a"}, false},

		// Test descending order with boolean parameter
		{"numeric_desc_bool", `sort([3, 1, 4, 2], true)`, []any{4, 3, 2, 1}, false},
		{"string_desc_bool", `sort(["c", "a", "b"], true)`, []any{"c", "b", "a"}, false},

		// Test with non-array input
		{"single_value", `sort(42)`, []any{42}, false},

		// Test integration with other functions
		{"sort_split", `sort(split("c,a,b", ","))`, []any{"a", "b", "c"}, false},
		{"sort_map_keys", `sort(keys({"c": 1, "a": 2, "b": 3}))`, []any{"a", "b", "c"}, false},
		// Fix the expected type to match the Range function's output (int64 instead of int)
		{"sort_range", `equals(sort(int_range(5, 1, -1)), [1, 2, 3, 4, 5])`, true, false},
	}

	// Tests for date_extract operation
	testSuites["date_extract"] = []test{
		{"no_args", `date_extract()`, nil, true},
		{"one_arg", `date_extract(sampleDate)`, nil, true},
		{"invalid_time", `date_extract("not a time", "day")`, nil, true},
		{"invalid_unit", `date_extract(sampleDate, "invalid")`, nil, true},
		{"non_string_unit", `date_extract(sampleDate, 123)`, nil, true},

		// Test extracting various parts
		{"extract_second", `date_extract(sampleDate, "second")`, 45, false},
		{"extract_minute", `date_extract(sampleDate, "minute")`, 30, false},
		{"extract_hour", `date_extract(sampleDate, "hour")`, 14, false},
		{"extract_day", `date_extract(sampleDate, "day")`, 15, false},
		{"extract_month", `date_extract(sampleDate, "month")`, 3, false},
		{"extract_quarter", `date_extract(sampleDate, "quarter")`, 1, false},
		{"extract_year", `date_extract(sampleDate, "year")`, 2022, false},

		// Test week extraction (March 15, 2022 is in week 11)
		{"extract_week", `date_extract(sampleDate, "week")`, 11, false},

		// Test day of week (March 15, 2022 is a Tuesday = 2)
		{"extract_dayofweek", `date_extract(sampleDate, "dow")`, 2, false},

		// Test day of year (March 15 is day 74 of the year)
		{"extract_dayofyear", `date_extract(sampleDate, "doy")`, 74, false},

		// Test abbreviations and plurals
		{"extract_seconds_plural", `date_extract(sampleDate, "seconds")`, 45, false},
		{"extract_minute_abbrev", `date_extract(sampleDate, "m")`, 30, false},
		{"extract_hour_plural", `date_extract(sampleDate, "hours")`, 14, false},
		{"extract_day_abbrev", `date_extract(sampleDate, "d")`, 15, false},
		{"extract_month_plural", `date_extract(sampleDate, "months")`, 3, false},
		{"extract_quarter_abbrev", `date_extract(sampleDate, "q")`, 1, false},
		{"extract_year_abbrev", `date_extract(sampleDate, "y")`, 2022, false},

		// Combination tests with other functions
		{"extract_with_parsed_date", `date_extract(date_parse("2022-05-20 10:15:30", "auto"), "hour")`, 10, false},
		{"extract_with_date_add", `date_extract(date_add(sampleDate, 1, "day"), "day")`, 16, false},
		{"int_format_with_extract", `int_format(date_extract(sampleDate, "year"), "")`, "2022", false},
		{"comparison_with_extract", `is_greater(date_extract(t2, "year"), date_extract(t1, "year"))`, true, false},
	}

	testSuites["date_range"] = []test{
		{"no_args", `date_range()`, nil, true},
		{"one_arg", `date_range(t1)`, nil, true},
		{"invalid_start", `date_range("not a date", t2)`, nil, true},
		{"invalid_end", `date_range(t1, "not a date")`, nil, true},
		{"zero_step", `date_range(t1, t2, 0, "day")`, nil, true},
		{"negative_step", `date_range(t1, t2, -1, "day")`, nil, true},
		{"invalid_unit", `date_range(t1, t2, 1, "invalid")`, nil, true},

		// Test with duration string format - test full array values
		{"minute_duration_string", `date_range(date_parse("2022-01-01 00:00:00", "auto"), date_parse("2022-01-01 00:04:00", "auto"), "1m")`,
			[]time.Time{
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 1, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 2, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 3, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 4, 0, 0, time.UTC),
			}, false},
		{"day_duration_string", `date_range(date_parse("2022-01-01", "auto"), date_parse("2022-01-05", "auto"), "1d")`,
			[]time.Time{
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 3, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 4, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 5, 0, 0, 0, 0, time.UTC),
			}, false},
		{"month_duration_string", `date_range(date_parse("2022-01-01", "auto"), date_parse("2022-03-01", "auto"), "1m")`,
			[]time.Time{
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 3, 1, 0, 0, 0, 0, time.UTC),
			}, false},

		// Test with step and unit format
		{"minute_step_unit", `date_range(date_parse("2022-01-01 00:00:00", "auto"), date_parse("2022-01-01 00:04:00", "auto"), 1, "minute")`,
			[]time.Time{
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 1, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 2, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 3, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 4, 0, 0, time.UTC),
			}, false},

		// Test with larger steps
		{"two_day_step", `date_range(date_parse("2022-01-01", "auto"), date_parse("2022-01-07", "auto"), 2, "days")`,
			[]time.Time{
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 3, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 7, 0, 0, 0, 0, time.UTC),
			}, false},
		{"two_month_step", `date_range(date_parse("2022-01-01", "auto"), date_parse("2022-07-01", "auto"), 2, "months")`,
			[]time.Time{
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 5, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 7, 1, 0, 0, 0, 0, time.UTC),
			}, false},

		// Test edge cases like month boundaries
		{"month_with_day_preservation", `date_range(date_parse("2022-01-31", "auto"), date_parse("2022-04-30", "auto"), "1m")`,
			[]time.Time{
				time.Date(2022, 1, 31, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 2, 28, 0, 0, 0, 0, time.UTC), // February has only 28 days in 2022
				time.Date(2022, 3, 31, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 4, 30, 0, 0, 0, 0, time.UTC),
			}, false},
		{"leap_year_handling", `date_range(date_parse("2020-01-31", "auto"), date_parse("2020-03-31", "auto"), "1m")`,
			[]time.Time{
				time.Date(2020, 1, 31, 0, 0, 0, 0, time.UTC),
				time.Date(2020, 2, 29, 0, 0, 0, 0, time.UTC), // February has 29 days in 2020 (leap year)
				time.Date(2020, 3, 31, 0, 0, 0, 0, time.UTC),
			}, false},

		// Test with hour steps
		{"hour_step", `date_range(date_parse("2022-01-01 00:00:00", "auto"), date_parse("2022-01-01 04:00:00", "auto"), "2h")`,
			[]time.Time{
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 2, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 4, 0, 0, 0, time.UTC),
			}, false},

		// Keep one existing test to ensure backward compatibility
		{"verify_days", `date_format(element(date_range(date_parse("2022-01-01", "auto"), date_parse("2022-01-03", "auto"), "1d"), 1), "%Y-%m-%d")`, "2022-01-02", false},

		// Test crossing year boundary
		{"year_boundary", `date_range(date_parse("2022-12-30", "auto"), date_parse("2023-01-02", "auto"), "1d")`,
			[]time.Time{
				time.Date(2022, 12, 30, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC),
			}, false},

		// Test year increment
		{"year_increment", `date_range(date_parse("2020-01-01", "auto"), date_parse("2023-01-01", "auto"), "1y")`,
			[]time.Time{
				time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			}, false},
	}

	eval := goval.NewEvaluator()
	for suite, tests := range testSuites {
		for _, tt := range tests {
			testName := g.F("%s/%s", suite, tt.name)
			t.Run(testName, func(t *testing.T) {
				result, err := eval.Evaluate(tt.expr, variables, Functions.Generate())
				if tt.err {
					assert.Error(t, err)
				} else {
					if assert.NoError(t, err) {
						assert.Equal(t, tt.expected, result)
					}
				}
			})
		}
	}
}
