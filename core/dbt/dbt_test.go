package dbt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDbtCompile(t *testing.T) {
	// https://docs.getdbt.com/reference/node-selection/syntax/
	dbtConfig := `{
		"version": "0.18",
		"repo_url": "https://github.com/fishtown-analytics/dbt-starter-project",
		"schema": "public",
		"models": "+my_second_dbt_model",
		"debug": false
	}`

	dbtObj, err := NewDbt(dbtConfig, "POSTGRES_URL")
	if !assert.NoError(t, err) {
		return
	}

	dbtObj.Session.Print = true

	err = dbtObj.Init()
	if !assert.NoError(t, err) {
		return
	}

	err = dbtObj.Compile()
	assert.NoError(t, err)

	assert.Greater(t, len(dbtObj.Manifest.Nodes), 0)
	assert.Greater(t, len(dbtObj.RunResult.Results), 0)

	// os.RemoveAll(dbtObj.ProjectPath)
}

func TestDbtRun(t *testing.T) {
	dbtConfig := `{
		"version": "0.18",
		"repo_url": "https://github.com/fishtown-analytics/dbt-starter-project",
		"schema": "public",
		"models": "+my_second_dbt_model",
		"debug": false
	}`

	dbtObj, err := NewDbt(dbtConfig, "POSTGRES_URL")
	if !assert.NoError(t, err) {
		return
	}

	dbtObj.Session.Print = true

	err = dbtObj.Init()
	if !assert.NoError(t, err) {
		return
	}

	err = dbtObj.Run()
	assert.NoError(t, err)
	assert.Greater(t, len(dbtObj.Manifest.Nodes), 0)
	assert.Greater(t, len(dbtObj.RunResult.Results), 0)

	// os.RemoveAll(dbtObj.ProjectPath)
}
