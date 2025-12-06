package sling

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

type Pipeline struct {
	Steps []any          `json:"steps,omitempty" yaml:"steps,omitempty"`
	Env   map[string]any `json:"env,omitempty" yaml:"env,omitempty"`

	Context     *g.Context             `json:"-"`
	Output      strings.Builder        `json:"-"`
	OutputLines chan *g.LogLine        `json:"-"`
	CurrentStep *PipelineStepExecution `json:"-"`
	MD5         string                 `json:"-"`
	FileName    string                 `json:"-"`
	Body        string                 `json:"body,omitempty" yaml:"-"` // raw body of pipeline

	state  *PipelineState
	steps  Hooks
	execID string
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

	pipeline, err = LoadPipelineConfig(string(cfgBytes))
	pipeline.FileName = cfgPath
	if fileName := os.Getenv("SLING_FILE_NAME"); fileName != "" {
		pipeline.FileName = fileName
	}

	return
}

func LoadPipelineConfig(content string) (pipeline *Pipeline, err error) {
	pipeline = &Pipeline{
		Env:         map[string]any{},
		OutputLines: make(chan *g.LogLine, 5000),
		MD5:         g.MD5(content),
		Body:        content,
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

	// set env
	pipeline.Env = Env

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
	err = g.Unmarshal(g.Marshal(values), &pipeline.Steps)
	if err != nil {
		err = g.Error(err, "could not parse 'steps'")
		return
	}

	state, err := pipeline.RuntimeState()
	if err != nil {
		return nil, g.Error(err, "could not render runtime state")
	}

	for i, stepRaw := range pipeline.Steps {
		opts := ParseOptions{
			index:   i,
			state:   state,
			kind:    HookKindStep,
			md5:     g.MD5(g.Marshal(stepRaw)),
			context: pipeline.Context,
		}
		step, err := ParseHook(stepRaw, opts)
		if err != nil {
			return pipeline, g.Error(err, "error parsing pipeline step-%d", i+1)
		} else if step != nil {
			pipeline.steps = append(pipeline.steps, step)
		}
	}

	pipeline.execID = os.Getenv("SLING_EXEC_ID")
	if pipeline.execID == "" {
		pipeline.execID = NewExecID()
	}

	return
}

func (pl *Pipeline) GetSteps() Hooks {
	return pl.steps
}

// Execute executes the pipeline steps using PipelineStepExecution
func (pl *Pipeline) Execute() (err error) {
	// Build step ID map for goto functionality
	idStepMap := map[string]int{}
	for i, step := range pl.steps {
		idStepMap[step.ID()] = i
	}

	// set envs
	for k, v := range pl.Env {
		os.Setenv(k, g.CastToString(v))
	}

	// Execute each step
	var lastErr error
	for i := 0; i < len(pl.steps); i++ {
		step := pl.steps[i]
		step.SetContext(pl.Context) // update with latest context

		// Create execution context for this step
		pse := &PipelineStepExecution{
			Pipeline:    pl,
			Step:        step,
			ExecID:      pl.execID,
			OutputLines: pl.OutputLines,
			Map:         g.M(),
		}

		// Set log sink for capturing output
		env.LogSink = func(ll *g.LogLine) {
			ll.Group = g.F("%s,%s", pse.ExecID, pse.Step.ID())

			// Push line to channel if not full
			select {
			case pse.OutputLines <- ll:
			default:
			}

			// Add to output buffer
			pse.Output.WriteString(ll.Line() + "\n")
			pse.Pipeline.Output.WriteString(ll.Line() + "\n")
		}

		// Execute the step
		pl.CurrentStep = pse
		err = pse.Execute(lastErr != nil) // skip if errored
		if err != nil {
			lastErr = err // this allows to mark the rest of the steps as skipped
		}

		// continue to mark rest of steps if already errored
		if lastErr != nil {
			continue
		}

		// Check for break
		if br, _ := step.Context().Map.Get("break"); br == true {
			break
		}

		// Handle goto
		if gotoID := pse.Map["goto"]; gotoID != nil {
			if gotoIndex, ok := idStepMap[cast.ToString(gotoID)]; ok {
				i = gotoIndex - 1 // -1 because i++ will increment it
			} else {
				g.Warn("did not find step ID (%s) for goto", gotoID)
			}
		}
	}

	return lastErr
}

// PipelineStepExecution represents a single step execution context
type PipelineStepExecution struct {
	ExecID      string          `json:"exec_id"`
	Status      ExecStatus      `json:"status"`
	Err         error           `json:"error"`
	StartTime   *time.Time      `json:"start_time"`
	EndTime     *time.Time      `json:"end_time"`
	Progress    string          `json:"progress"`
	Output      strings.Builder `json:"-"`
	OutputLines chan *g.LogLine `json:"-"`
	Pipeline    *Pipeline       `json:"-"`
	Map         map[string]any  `json:"-"`
	Step        Hook            `json:"-"` // The specific step to execute
}

func (pse *PipelineStepExecution) Context() *g.Context {
	if pse.Pipeline == nil {
		return nil
	}
	return pse.Pipeline.Context
}

func (pse *PipelineStepExecution) setLogDetails() {
	var duration int
	if pse.StartTime != nil {
		duration = int(time.Since(*pse.StartTime).Seconds())
	}
	os.Setenv("SLING_LOG_DETAILS", g.Marshal(g.M(
		"run_file", pse.Pipeline.FileName,
		"run_type", "pipeline",
		"step_id", pse.Step.ID(),
		"status", pse.Status,
		"duration", duration,
	)))
}

// Execute executes a single pipeline step
func (pse *PipelineStepExecution) Execute(skip bool) (err error) {
	if pse.Pipeline == nil {
		return g.Error("pipeline is nil")
	}
	if pse.Step == nil {
		return g.Error("step is nil")
	}

	// Set start time and status
	pse.StartTime = g.Ptr(time.Now())
	pse.Status = ExecStatusRunning

	// Start ticker to update state every 5 seconds
	ticker5s := time.NewTicker(5 * time.Second)
	go func() {
		defer ticker5s.Stop()
		for range ticker5s.C {
			if pse.Status != ExecStatusRunning {
				return // is done
			}
			if pse.Context() != nil {
				select {
				case <-pse.Context().Ctx.Done():
					return
				case <-ticker5s.C:
					pse.StateSet()
				}
			} else {
				pse.StateSet()
			}
		}
	}()

	pse.Context().Lock() // for map access
	pse.Map = pse.Step.PayloadMap()
	pse.Context().Unlock() // for map access

	defer pse.StateSet()
	if skip {
		pse.Status = ExecStatusSkipped
		return // mark as skipped
	}

	pse.StateSet()

	// Update current step in pipeline
	if !g.In(pse.Step.Type(), "log") {
		g.Debug(`executing step "%s" (type: %s)`, pse.Step.ID(), pse.Step.Type())
	}

	// Execute the step
	stepErr := pse.Step.Execute()
	_, err = pse.Step.ExecuteOnDone(stepErr)

	pse.Context().Lock() // for map access
	pse.Map = pse.Step.PayloadMap()
	pse.Context().Unlock() // for map access

	// Set completion status and end time
	pse.EndTime = g.Ptr(time.Now())

	// Handle errors
	if err != nil {
		pse.Err = err
		pse.Status = ExecStatusError
		return g.Error(err, "error executing step: %s", pse.Step.ID())
	}
	pse.Status = ExecStatusSuccess

	return nil
}

func (pse *PipelineStepExecution) StateSet() {
	StoreSet(pse)

	if pse != nil && pse.Pipeline != nil {
		pse.Context().Lock()
		defer pse.Context().Unlock()

		state, err := pse.Pipeline.RuntimeState()
		if err != nil {
			return
		}

		if state.Run == nil {
			state.Run = &RunState{
				Step: pse,
			}
		} else if state.Run.Step == nil {
			state.Run.Step = pse
		}

		state.Run.ID = pse.Step.ID()
		state.Run.StartTime = pse.StartTime
		state.Run.EndTime = pse.EndTime
		state.Run.Status = pse.Status
	}
}

// RuntimeState returns the state for use
func (pl *Pipeline) RuntimeState() (_ *PipelineState, err error) {
	if pl.state == nil {
		pl.state = &PipelineState{
			State: map[string]map[string]any{},
			Store: map[string]any{},
			Env:   pl.Env,
			Runs:  map[string]*RunState{},
		}
	}

	if pl.CurrentStep != nil {
		pl.state.Run.Step = pl.CurrentStep
		pl.CurrentStep.setLogDetails()
	}

	if pl.state.Run == nil {
		pl.state.Run = &RunState{
			Step: pl.CurrentStep,
		}
	}

	pl.state.Timestamp.Update()

	return pl.state, nil
}

type PipelineState struct {
	State     map[string]map[string]any `json:"state,omitempty"`
	Store     map[string]any            `json:"store,omitempty"`
	Env       map[string]any            `json:"env,omitempty"`
	Timestamp DateTimeState             `json:"timestamp,omitempty"`
	Runs      map[string]*RunState      `json:"runs,omitempty"`
	Run       *RunState                 `json:"run,omitempty"`
}

func (ps *PipelineState) GetStore() map[string]any {
	return ps.Store
}

func (ps *PipelineState) SetStoreData(key string, value any, del bool) {
	if del {
		delete(ps.Store, key)
	} else {
		ps.Store[key] = value
	}
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

func (ps *PipelineState) StepExecution() *PipelineStepExecution {
	if ps.Run != nil && ps.Run.Step != nil {
		return ps.Run.Step
	}
	return nil
}

func SetPipelineStoreEnv(store map[string]any) {
	payload, err := g.JSONMarshal(store)
	if err == nil {
		err = os.Setenv("SLING_PIPELINE_STORE", string(payload))
		if err != nil {
			g.Warn("could not set pipeline store payload into env: %s", err.Error())
		}
	} else {
		g.Warn("could not marshal pipeline store payload before replication: %s", err.Error())
	}
}

func GetPipelineStoreEnv() (store map[string]any) {

	var err error
	if storePayload := os.Getenv("SLING_PIPELINE_STORE"); storePayload != "" {
		store, err = g.UnmarshalMap(storePayload)
		if err != nil {
			g.Warn("could not unmarshal pipeline store payload: %s", err.Error())
		}
	}

	if store == nil {
		store = g.M()
	}

	return
}
