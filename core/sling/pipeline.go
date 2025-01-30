package sling

import (
	"io"
	"os"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"gopkg.in/yaml.v2"
)

type Pipeline struct {
	Steps Hooks          `json:"steps" yaml:"steps"`
	Env   map[string]any `json:"env,omitempty" yaml:"env,omitempty"`

	state *PipelineState
}

type PipelineSteps []PipelineStep

type PipelineStep struct {
}

func LoadPipelineConfigFromFile(cfgPath string) (pipeline *Pipeline, err error) {
	cfgFile, err := os.Open(cfgPath)
	if err != nil {
		err = g.Error(err, "Unable to open pipeline path: "+cfgPath)
		return
	}

	cfgBytes, err := io.ReadAll(cfgFile)
	if err != nil {
		err = g.Error(err, "could not read from pipeline path: "+cfgPath)
		return
	}

	return LoadPipelineConfig(string(cfgBytes))
}

func LoadPipelineConfig(content string) (pipeline *Pipeline, err error) {
	pipeline = &Pipeline{
		Env: map[string]any{},
	}

	m := g.M()
	err = yaml.Unmarshal([]byte(content), &m)
	if err != nil {
		err = g.Error(err, "Error parsing yaml content")
		return
	}

	// parse env & expand variables
	var Env map[string]any
	g.Unmarshal(g.Marshal(m["env"]), &Env)
	for k, v := range Env {
		if s, ok := v.(string); ok {
			Env[k] = os.ExpandEnv(s)
		}
	}

	// replace variables across the yaml file
	Env = lo.Ternary(Env == nil, map[string]any{}, Env)
	content = g.Rm(content, Env)

	// parse again
	m = g.M()
	err = yaml.Unmarshal([]byte(content), &m)
	if err != nil {
		err = g.Error(err, "Error parsing yaml content")
		return
	}

	values, ok := m["steps"]
	if !ok {
		err = g.Error("did not find 'steps' key")
		return
	}

	// parse pipeline
	var stepsRaw []any
	err = g.Unmarshal(g.Marshal(values), &stepsRaw)
	if err != nil {
		err = g.Error(err, "could not parse 'steps'")
		return
	}

	state, err := pipeline.RuntimeState()
	if err != nil {
		return nil, g.Error(err, "could not render runtime state")
	}

	for i, stepRaw := range stepsRaw {
		opts := ParseOptions{
			index: i,
			state: state,
			stage: HookStage(g.F("step-%02d", i+1)),
			kind:  HookKindStep,
		}
		step, err := ParseHook(stepRaw, opts)
		if err != nil {
			return pipeline, g.Error(err, "error parsing pipeline step-%d", i+1)
		} else if step != nil {
			pipeline.Steps = append(pipeline.Steps, step)
		}
	}

	return
}

func (pl *Pipeline) Execute() (err error) {
	for _, step := range pl.Steps {
		if !g.In(step.Type(), "log") {
			g.Debug(`executing step "%s" (type: %s)`, step.ID(), step.Type())
		}

		stepErr := step.Execute()
		err = step.ExecuteOnDone(stepErr)

		if err != nil {
			return g.Error(err, "error executing step")
		}
	}

	return
}

// RuntimeState returns the state for use
func (pl *Pipeline) RuntimeState() (_ *PipelineState, err error) {
	if pl.state == nil {
		pl.state = &PipelineState{
			State: map[string]map[string]any{},
			Env:   pl.Env,
			Runs:  map[string]*RunState{},
		}
	}

	pl.state.Timestamp.Update()

	return pl.state, nil
}

type PipelineState struct {
	State     map[string]map[string]any `json:"state,omitempty"`
	Env       map[string]any            `json:"env,omitempty"`
	Timestamp DateTimeState             `json:"timestamp,omitempty"`
	Runs      map[string]*RunState      `json:"runs,omitempty"`
	Run       *RunState                 `json:"run,omitempty"`
}

func (ps *PipelineState) SetStateData(id string, data map[string]any) {
	ps.State[id] = data
}

func (ps *PipelineState) SetStateKeyValue(id, key string, value any) {
	ps.State[id][key] = value
}

func (ps *PipelineState) Marshall() string {
	return g.Marshal(ps)
}

func (ps *PipelineState) TaskExecution() *TaskExecution {
	return nil
}
