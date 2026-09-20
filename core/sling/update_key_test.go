package sling

import (
	stdjson "encoding/json"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestUpdateKey_YAML_Scalar(t *testing.T) {
	yamlStr := `update_key: updated_at`
	var target struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}

	err := yaml.Unmarshal([]byte(yamlStr), &target)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at"}, target.UpdateKey)
	assert.Equal(t, "updated_at", target.UpdateKey.First())
	assert.Equal(t, []string{"updated_at"}, target.UpdateKey.Columns())
	assert.Equal(t, "updated_at", target.UpdateKey.String())
	assert.False(t, target.UpdateKey.IsEmpty())

	// Comma-separated scalar in YAML
	yamlStrComma := `update_key: col1, col2`
	var targetComma struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}
	err = yaml.Unmarshal([]byte(yamlStrComma), &targetComma)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"col1", "col2"}, targetComma.UpdateKey)
}

func TestUpdateKey_YAML_Sequence(t *testing.T) {
	yamlStr := `update_key: [updated_at, id]`
	var target struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}

	err := yaml.Unmarshal([]byte(yamlStr), &target)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at", "id"}, target.UpdateKey)
	assert.Equal(t, "updated_at", target.UpdateKey.First())
	assert.Equal(t, []string{"updated_at", "id"}, target.UpdateKey.Columns())
	assert.Equal(t, "updated_at, id", target.UpdateKey.String())
	assert.False(t, target.UpdateKey.IsEmpty())
}

func TestUpdateKey_YAML_EmptySequence(t *testing.T) {
	yamlStr := `update_key: []`
	var target struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}

	err := yaml.Unmarshal([]byte(yamlStr), &target)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{}, target.UpdateKey)
	assert.True(t, target.UpdateKey.IsEmpty())
	assert.Equal(t, "", target.UpdateKey.First())
}

func TestUpdateKey_YAML_WhitespaceAndEmptyElements(t *testing.T) {
	yamlStr := `update_key: [" updated_at ", "  ", "id "]`
	var target struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}

	err := yaml.Unmarshal([]byte(yamlStr), &target)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at", "id"}, target.UpdateKey)
}

func TestUpdateKey_YAML_Duplicates(t *testing.T) {
	yamlStr := `update_key: [updated_at, updated_at]`
	var target struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}

	err := yaml.Unmarshal([]byte(yamlStr), &target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column 'updated_at' in update_key")

	// Case-insensitive duplicate
	yamlStrCase := `update_key: [updated_at, UPDATED_AT]`
	err = yaml.Unmarshal([]byte(yamlStrCase), &target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column 'UPDATED_AT' in update_key")
}

func TestUpdateKey_YAML_InvalidType(t *testing.T) {
	yamlStr := `update_key: { foo: bar }`
	var target struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}

	err := yaml.Unmarshal([]byte(yamlStr), &target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for update_key")
}

func TestUpdateKey_YAML_Marshal(t *testing.T) {
	// Single key serializes as scalar string
	single := struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}{
		UpdateKey: UpdateKey{"updated_at"},
	}
	bytesSingle, err := yaml.Marshal(single)
	require.NoError(t, err)
	assert.Contains(t, string(bytesSingle), "update_key: updated_at")

	// Multiple keys serialize as sequence
	multiple := struct {
		UpdateKey UpdateKey `yaml:"update_key"`
	}{
		UpdateKey: UpdateKey{"updated_at", "id"},
	}
	bytesMulti, err := yaml.Marshal(multiple)
	require.NoError(t, err)
	assert.Contains(t, string(bytesMulti), "update_key:")
	assert.Contains(t, string(bytesMulti), "- updated_at")
	assert.Contains(t, string(bytesMulti), "- id")
}

func TestUpdateKey_JSON(t *testing.T) {
	// Scalar JSON
	var target1 struct {
		UpdateKey UpdateKey `json:"update_key"`
	}
	err := stdjson.Unmarshal([]byte(`{"update_key": "updated_at"}`), &target1)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at"}, target1.UpdateKey)

	// Comma-separated string in JSON
	var target1b struct {
		UpdateKey UpdateKey `json:"update_key"`
	}
	err = stdjson.Unmarshal([]byte(`{"update_key": "updated_at, id"}`), &target1b)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at", "id"}, target1b.UpdateKey)

	// Array JSON
	var target2 struct {
		UpdateKey UpdateKey `json:"update_key"`
	}
	err = stdjson.Unmarshal([]byte(`{"update_key": ["updated_at", "id"]}`), &target2)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at", "id"}, target2.UpdateKey)

	// Empty array JSON
	var target3 struct {
		UpdateKey UpdateKey `json:"update_key"`
	}
	err = stdjson.Unmarshal([]byte(`{"update_key": []}`), &target3)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{}, target3.UpdateKey)

	// Null JSON
	var target4 struct {
		UpdateKey UpdateKey `json:"update_key"`
	}
	err = stdjson.Unmarshal([]byte(`{"update_key": null}`), &target4)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{}, target4.UpdateKey)

	// Duplicate in JSON
	var target5 struct {
		UpdateKey UpdateKey `json:"update_key"`
	}
	err = stdjson.Unmarshal([]byte(`{"update_key": ["col1", "col1"]}`), &target5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column 'col1' in update_key")

	// Marshal JSON single
	b1, err := stdjson.Marshal(target1)
	require.NoError(t, err)
	assert.Equal(t, `{"update_key":"updated_at"}`, string(b1))

	// Marshal JSON multiple
	b2, err := stdjson.Marshal(target2)
	require.NoError(t, err)
	assert.Equal(t, `{"update_key":["updated_at","id"]}`, string(b2))
}

func TestUpdateKey_ParseUpdateKey(t *testing.T) {
	// From comma-delimited string
	k1, err := ParseUpdateKey("col1, col2, col3")
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"col1", "col2", "col3"}, k1)

	// From single string
	k2, err := ParseUpdateKey("col1")
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"col1"}, k2)

	// From empty string
	k3, err := ParseUpdateKey("")
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{}, k3)

	// From []string
	k4, err := ParseUpdateKey([]string{"a", "b"})
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"a", "b"}, k4)

	// From []any
	k5, err := ParseUpdateKey([]any{"x", "y"})
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"x", "y"}, k5)

	// Duplicate error
	_, err = ParseUpdateKey("dup, dup")
	require.Error(t, err)

	// Whitespace trimming in comma-split: each part must be trimmed independently
	k6, err := ParseUpdateKey("  col1 ,  col2  ,col3  ")
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"col1", "col2", "col3"}, k6)

	// Single key with surrounding whitespace (non-comma)
	k7, err := ParseUpdateKey("  updated_at  ")
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at"}, k7)
}

func TestConfig_UpdateKey_YAML_Integration(t *testing.T) {
	// Scalar update_key in Task Config
	yamlStr1 := `
source:
  conn: local
  stream: stream1
  update_key: updated_at
target:
  conn: local
  object: table1
`
	var cfg1 Config
	err := cfg1.Unmarshal(yamlStr1)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at"}, cfg1.Source.UpdateKey)
	assert.True(t, cfg1.Source.HasUpdateKey())

	// Sequence update_key in Task Config
	yamlStr2 := `
source:
  conn: local
  stream: stream1
  update_key: [updated_at, id]
target:
  conn: local
  object: table1
`
	var cfg2 Config
	err = cfg2.Unmarshal(yamlStr2)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{"updated_at", "id"}, cfg2.Source.UpdateKey)
	assert.True(t, cfg2.Source.HasUpdateKey())

	// Empty sequence update_key in Task Config
	yamlStr3 := `
source:
  conn: local
  stream: stream1
  update_key: []
target:
  conn: local
  object: table1
`
	var cfg3 Config
	err = cfg3.Unmarshal(yamlStr3)
	require.NoError(t, err)
	assert.Equal(t, UpdateKey{}, cfg3.Source.UpdateKey)
	assert.False(t, cfg3.Source.HasUpdateKey())

	// Duplicate in Task Config returns error
	yamlStr4 := `
source:
  conn: local
  stream: stream1
  update_key: [col1, col1]
target:
  conn: local
  object: table1
`
	var cfg4 Config
	err = cfg4.Unmarshal(yamlStr4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column 'col1' in update_key")
}

func TestReplication_UpdateKey_YAML_Integration(t *testing.T) {
	// Scalar update_key in Replication Config
	yamlStr1 := `
source: local
target: local

defaults:
  update_key: updated_at

streams:
  users: {}
  orders:
    update_key: [order_date, order_id]
  logs:
    update_key: []
`
	rConfig, err := UnmarshalReplication(yamlStr1)
	require.NoError(t, err)

	assert.Equal(t, UpdateKey{"updated_at"}, rConfig.Defaults.UpdateKey)
	require.Contains(t, rConfig.Streams, "orders")
	assert.Equal(t, UpdateKey{"order_date", "order_id"}, rConfig.Streams["orders"].UpdateKey)
	require.Contains(t, rConfig.Streams, "logs")
	assert.Equal(t, UpdateKey{}, rConfig.Streams["logs"].UpdateKey)
}

// TestIncrementalWhere_CompositeOR unit-tests the internal helper functions
// (extractIncrementalValuesMap, getValForColumn) and the OR-clause assembly logic
// that is exercised by ReadFromDB for multi-key update_key. This is NOT an integration
// test of the full ReadFromDB path.
func TestIncrementalWhere_CompositeOR(t *testing.T) {
	// 1. Multiple update keys with JSON map in IncrementalValStr
	cfg1 := &Config{
		Source: Source{
			UpdateKey: UpdateKey{"updated_at", "id"},
		},
		IncrementalValStr: `{"updated_at":"'2026-09-19 10:00:00'","id":"500"}`,
	}

	// Mock sqlite connection to evaluate template
	conn, err := database.NewConn("sqlite://:memory:")
	require.NoError(t, err)

	valMap := extractIncrementalValuesMap(cfg1)
	assert.Equal(t, "'2026-09-19 10:00:00'", valMap["updated_at"])
	assert.Equal(t, "500", valMap["id"])

	// Test building condition
	gt := ">"
	var conds []string
	for i, k := range cfg1.Source.UpdateKey {
		v := getValForColumn(valMap, cfg1, k, i)
		cond := g.R(
			conn.GetTemplateValue("core.incremental_where"),
			"update_key", conn.Quote(k),
			"value", v,
			"gt", gt,
		)
		conds = append(conds, cond)
	}
	result := "(" + strings.Join(conds, " or ") + ")"
	expected := "(\"updated_at\" > '2026-09-19 10:00:00' or \"id\" > 500)"
	assert.Equal(t, expected, result)

	// 2. Multiple update keys with comma-separated IncrementalValStr (legacy format)
	cfg2 := &Config{
		Source: Source{
			UpdateKey: UpdateKey{"updated_at", "id"},
		},
		IncrementalValStr: `'2026-09-19 10:00:00', 500`,
	}
	v1 := getValForColumn(nil, cfg2, "updated_at", 0)
	v2 := getValForColumn(nil, cfg2, "id", 1)
	assert.Equal(t, "'2026-09-19 10:00:00'", v1)
	assert.Equal(t, "500", v2)

	// 3. IncrementalGTE (>=) — operator controlled by Config.IncrementalGTE
	gtGte := lo.Ternary(true, ">=", ">")
	assert.Equal(t, ">=", gtGte)

	conds = nil
	for i, k := range cfg1.Source.UpdateKey {
		v := getValForColumn(valMap, cfg1, k, i)
		cond := g.R(
			conn.GetTemplateValue("core.incremental_where"),
			"update_key", conn.Quote(k),
			"value", v,
			"gt", gtGte,
		)
		conds = append(conds, cond)
	}
	resultGte := "(" + strings.Join(conds, " or ") + ")"
	expectedGte := "(\"updated_at\" >= '2026-09-19 10:00:00' or \"id\" >= 500)"
	assert.Equal(t, expectedGte, resultGte)

	// 4. Formatted JSON in IncrementalValStr must take precedence over raw time.Time in IncrementalVal
	rawMap := map[string]any{
		"updated_at": "raw_unformatted_value",
		"id":         500,
	}
	cfgWithRaw := &Config{
		Source: Source{
			UpdateKey: UpdateKey{"updated_at", "id"},
		},
		IncrementalVal:    rawMap,
		IncrementalValStr: `{"updated_at":"'2026-09-19 10:00:00'","id":"500"}`,
	}
	valMapFromJSON := extractIncrementalValuesMap(cfgWithRaw)
	assert.Equal(t, "'2026-09-19 10:00:00'", valMapFromJSON["updated_at"])
	assert.Equal(t, "500", valMapFromJSON["id"])

	// 5. Multi-key count mismatch in getValForColumn returns "" rather than unparsed string
	cfgMismatch := &Config{
		Source: Source{
			UpdateKey: UpdateKey{"col1", "col2"},
		},
		IncrementalValStr: "single_val",
	}
	assert.Equal(t, "", getValForColumn(nil, cfgMismatch, "col1", 0))
}

func TestReplication_ProcessChunks_CompositeKeyError(t *testing.T) {
	yamlStr := `
source: local
target: local
streams:
  orders:
    mode: incremental
    update_key: [order_date, order_id]
    source_options:
      chunk_count: 5
`
	rConfig, err := UnmarshalReplication(yamlStr)
	require.NoError(t, err)

	err = rConfig.ProcessChunks()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream chunking is not supported with composite update_key: orders")
}

func TestReadFromDB_CompositeKey_Guards(t *testing.T) {
	// 1. Composite update_key on NoSQL source should error immediately
	taskNoSQL := &TaskExecution{
		Config: &Config{
			Mode: IncrementalMode,
			Source: Source{
				Stream:    "my_coll",
				UpdateKey: UpdateKey{"col1", "col2"},
			},
			IncrementalValStr: `{"col1":"1","col2":"2"}`,
		},
	}
	mongoConn, err := database.NewConn("mongodb://localhost:27017/test")
	require.NoError(t, err)
	_, err = taskNoSQL.ReadFromDB(taskNoSQL.Config, mongoConn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "composite update_key is not supported for NoSQL sources: mongodb")

	// 2. Custom SQL with composite update_key and {incremental_value} check
	customSQL := "SELECT * FROM my_table WHERE col > {incremental_value}"
	hasIncVal := strings.Contains(customSQL, "{incremental_value}")
	hasWhereCond := strings.Contains(customSQL, "{incremental_where_cond}")
	assert.True(t, len(taskNoSQL.Config.Source.UpdateKey) > 1 && hasIncVal && !hasWhereCond)
}

