package sling

import (
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
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

	stepYamlNodes []*yaml.Node // to maintain order
	state         *PipelineState
	steps         Hooks
	execID        string
	outputMux     sync.Mutex
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

	// to maintain order using yaml.Node
	var rootNode yaml.Node
	err = yaml.Unmarshal([]byte(content), &rootNode)
	if err != nil {
		err = g.Error(err, "Error parsing yaml content")
		return
	}

	// rootNode is a DocumentNode, its Content[0] is the actual mapping
	if rootNode.Kind == yaml.DocumentNode && len(rootNode.Content) > 0 {
		docNode := rootNode.Content[0]
		if docNode.Kind == yaml.MappingNode {
			// Iterate key-value pairs (alternating in Content)
			for i := 0; i < len(docNode.Content); i += 2 {
				keyNode := docNode.Content[i]
				valueNode := docNode.Content[i+1]

				if keyNode.Value == "steps" {
					if valueNode.Kind == yaml.SequenceNode {
						pipeline.stepYamlNodes = make([]*yaml.Node, len(valueNode.Content))
						for j, stepNode := range valueNode.Content {
							pipeline.stepYamlNodes[j] = stepNode
						}
					} else {
						return nil, g.Error("steps value is not a sequence")
					}
				}
			}
		}
	}

	pipeline.execID = env.ExecID

	state, err := pipeline.RuntimeState()
	if err != nil {
		return nil, g.Error(err, "could not render runtime state")
	}

	for i, stepRaw := range pipeline.Steps {
		var yamlNode *yaml.Node
		if i < len(pipeline.stepYamlNodes) {
			yamlNode = pipeline.stepYamlNodes[i]
		}
		opts := ParseOptions{
			index:    i,
			state:    state,
			kind:     HookKindStep,
			md5:      g.MD5(g.Marshal(stepRaw)),
			context:  pipeline.Context,
			yamlNode: yamlNode,
		}
		step, err := ParseHook(stepRaw, opts)
		if err != nil {
			return pipeline, g.Error(err, "error parsing pipeline step-%d", i+1)
		} else if step != nil {
			pipeline.steps = append(pipeline.steps, step)
		}
	}

	// check for duplicate step IDs
	seenIDs := map[string]int{}
	for i, step := range pipeline.steps {
		id := step.ID()
		if id == "" {
			continue
		}
		if prevIdx, exists := seenIDs[id]; exists {
			return pipeline, g.Error("duplicate step id %q found at index %d and %d. Each step must have a unique id", id, prevIdx+1, i+1)
		}
		seenIDs[id] = i
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
		// honor context cancellation (e.g., SLING_TIMEOUT deadline) between steps
		if pl.Context != nil {
			select {
			case <-pl.Context.Ctx.Done():
				if lastErr == nil {
					lastErr = g.Error("pipeline cancelled: %s", pl.Context.Ctx.Err())
				}
			default:
			}
		}

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

			// Add to output buffer. Use a dedicated mutex (not Context.Mux)
			pse.Pipeline.outputMux.Lock()
			pse.Output.WriteString(ll.Line() + "\n")
			pse.Pipeline.Output.WriteString(ll.Line() + "\n")
			pse.Pipeline.outputMux.Unlock()
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
					if isTimeoutDeadlinePassed(pse.Context()) {
						pse.Status = ExecStatusTimedOut
					}
					pse.StateSet()
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
retry:
	stepErr := pse.Step.Execute()
	onFail, err := pse.Step.ExecuteOnDone(stepErr)

	if err != nil {
		retried, _ := pse.Step.Context().Map.Get("retried")
		if onFail == "retry" && !cast.ToBool(retried) {
			pse.Step.Context().Map.Set("retried", true) // only retry once
			g.Debug(`retrying step "%s" (type: %s)`, pse.Step.ID(), pse.Step.Type())
			time.Sleep(5 * time.Second)
			goto retry
		}
	}

	pse.Context().Lock() // for map access
	pse.Map = pse.Step.PayloadMap()
	pse.Context().Unlock() // for map access

	// Set completion status and end time
	pse.EndTime = g.Ptr(time.Now())

	// Handle errors
	if err != nil {
		pse.Err = err
		// classify as timed-out if the step ended after the configured SLING_TIMEOUT deadline
		if isTimeoutDeadlinePassed(pse.Context()) {
			pse.Status = ExecStatusTimedOut
			return g.Error(err, "step timed-out: %s", pse.Step.ID())
		}
		pse.Status = ExecStatusError
		return g.Error(err, "error executing step: %s", pse.Step.ID())
	}

	// mark the step as warning so the status bubbles up to the overall pipeline
	if onFail == OnFailWarn || pse.Step.Status().IsWarning() {
		pse.Status = ExecStatusWarning
		return nil
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
			Env:   env.MergeDeclaredEnv(pl.Env),
			Runs:  map[string]*RunState{},
			mu:    &sync.RWMutex{},
		}

		// populate execution-level context (mirrors ReplicationState.Execution).
		// FilePath/FileName are set in updateExecutionState since pl.FileName is
		// assigned by the caller after the pipeline is parsed.
		pl.state.Execution.ID = pl.execID
		pl.state.Execution.StartTime = g.Ptr(time.Now())

		// populate cli args from env (set in processRun via SLING_CLI_ARGS_MAP)
		pl.state.Execution.CLIArgs = map[string]any{
			`streams`: nil, `select`: nil, `limit`: nil, `range`: nil, `where`: nil}
		if args := os.Getenv("SLING_CLI_ARGS_MAP"); args != "" {
			g.Unmarshal(args, &pl.state.Execution.CLIArgs)
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
	pl.updateExecutionState()

	return pl.state, nil
}

// updateExecutionState aggregates step statuses and errors into the
// execution-level state (mirrors the aggregation in TaskExecution.StateSet).
func (pl *Pipeline) updateExecutionState() {
	es := &pl.state.Execution

	// FileName is assigned by the caller after the pipeline is parsed, so
	// refresh the file path/name here once it becomes available.
	if es.FilePath == "" && pl.FileName != "" {
		es.FilePath = pl.FileName
		es.FileName = path.Base(pl.FileName)
	}

	es.Status = StatusMap{}
	errGroup := g.ErrorGroup{}

	for _, step := range pl.steps {
		es.Status.Count++
		switch step.Status() {
		case ExecStatusSuccess:
			es.Status.Success++
		case ExecStatusError:
			es.Status.Error++
		case ExecStatusWarning:
			es.Status.Warning++
		case ExecStatusSkipped:
			es.Status.Skipped++
		case ExecStatusCancelled:
			es.Status.Cancelled++
		case ExecStatusRunning:
			es.Status.Running++
		}

		if stepData, ok := pl.state.State[step.ID()]; ok {
			if errMsg := cast.ToString(stepData["error"]); errMsg != "" {
				errGroup.Add(g.Error(errMsg))
			}
		}
	}

	if err := errGroup.Err(); err != nil {
		es.Error = g.Ptr(err.Error())
	}

	// determine if ended (nothing running and every step accounted for)
	finished := es.Status.Count == (es.Status.Success + es.Status.Error + es.Status.Warning + es.Status.Skipped + es.Status.Cancelled)
	if finished && es.Status.Running == 0 && es.StartTime != nil {
		es.EndTime = g.Ptr(time.Now())
		es.Duration = es.EndTime.Unix() - es.StartTime.Unix()
	} else if es.StartTime != nil {
		es.Duration = time.Now().Unix() - es.StartTime.Unix()
	}
}

type PipelineState struct {
	State     map[string]map[string]any `json:"state,omitempty"`
	Store     map[string]any            `json:"store,omitempty"`
	Env       map[string]any            `json:"env,omitempty"`
	Timestamp DateTimeState             `json:"timestamp,omitempty"`
	Execution ExecutionState            `json:"execution,omitempty"`
	Runs      map[string]*RunState      `json:"runs,omitempty"`
	Run       *RunState                 `json:"run,omitempty"`

	mu *sync.RWMutex `json:"-" yaml:"-"`
}

func (ps *PipelineState) lock() {
	if ps.mu == nil {
		ps.mu = &sync.RWMutex{}
	}
	ps.mu.Lock()
}

func (ps *PipelineState) unlock() { ps.mu.Unlock() }

func (ps *PipelineState) rlock() {
	if ps.mu == nil {
		ps.mu = &sync.RWMutex{}
	}
	ps.mu.RLock()
}

func (ps *PipelineState) runlock() { ps.mu.RUnlock() }

func (ps *PipelineState) GetStore() map[string]any {
	return ps.Store
}

func (ps *PipelineState) SetStoreData(key string, value any, del bool) {
	ps.lock()
	defer ps.unlock()
	if del {
		delete(ps.Store, key)
	} else {
		ps.Store[key] = value
	}
}

func (ps *PipelineState) SetStateData(id string, data map[string]any) {
	ps.lock()
	defer ps.unlock()
	ps.State[id] = data
}

func (ps *PipelineState) SetStateKeyValue(id, key string, value any) {
	ps.lock()
	defer ps.unlock()
	if ps.State[id] == nil {
		ps.State[id] = map[string]any{}
	}
	ps.State[id][key] = value
}

func (ps *PipelineState) Marshall() string {
	ps.rlock()
	defer ps.runlock()
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

// SetPipelineStoreEnv syncs pipeline store to replication step run
func SetPipelineStoreEnv(store map[string]any) {
	payload, err := g.JSONMarshal(store)
	if err != nil {
		g.Warn("could not marshal pipeline store payload: %s", err.Error())
		return
	}

	filePath := env.RuntimeFilePath("_pipeline_store_")
	err = os.WriteFile(filePath, payload, 0644)
	if err != nil {
		g.Warn("could not write pipeline store to file: %s", err.Error())
	}
}

// GetPipelineStoreEnv syncs pipeline store from replication step run
func GetPipelineStoreEnv() (store map[string]any) {
	filePath := env.RuntimeFilePath("_pipeline_store_")
	payload, err := os.ReadFile(filePath)
	if err == nil && len(payload) > 0 {
		store, err = g.UnmarshalMap(string(payload))
		if err != nil {
			g.Warn("could not unmarshal pipeline store payload: %s", err.Error())
		}
	}

	if store == nil {
		store = g.M()
	}

	return
}
