package env

import (
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestFlatten(t *testing.T) {
	m := g.Map{
		"parent": g.Map{
			"child_1": "val_1",
			"child_2": g.Map{
				"grandchild_1": "val_2",
			},
		},
	}

	env := map[string]string{}
	for key, val := range m {
		for k, v := range flatten(key, val) {
			env[k] = v
		}
	}

	assert.Contains(t, env, "parent_child_1")
	assert.Contains(t, env, "parent_child_2_grandchild_1")
}
