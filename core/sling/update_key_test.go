package sling

import (
	stdjson "encoding/json"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
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

func TestIncrementalWhere_CompositeOR(t *testing.T) {
	// 1. Multiple update keys with JSON map in IncrementalValStr
	cfg1 := &Config{
		Source: Source{
			UpdateKey: UpdateKey{"updated_at", "id"},
		},
		IncrementalValStr: `{"updated_at":"'2026-09-19 10:00:00'","id":"500"}`,
	}
	sTable := database.Table{
		Columns: []iop.Column{
			{Name: "updated_at", Type: iop.TimestampType},
			{Name: "id", Type: iop.IntegerType},
		},
	}
	task := &TaskExecution{Config: cfg1}

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

	// 2. Multiple update keys with comma-separated IncrementalValStr
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

	// 3. IncrementalGTE (>=)
	task.Config.IncrementalGTE = true
	gtGte := lo.Ternary(task.Config.IncrementalGTE, ">=", ">")
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

	// Verify columns exist in table
	assert.NotNil(t, sTable.Columns.GetColumn("updated_at"))
	assert.NotNil(t, sTable.Columns.GetColumn("id"))
}


