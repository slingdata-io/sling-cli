package api

import (
	"context"
	"encoding/base64"
	"os"
	"regexp"
	"strings"

	"github.com/flarco/g"
	"github.com/jmespath/go-jmespath"
	"github.com/maja42/goval"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

type APIConnection struct {
	Spec    Spec
	State   *APIState
	Context *g.Context
	eval    *goval.Evaluator
}

// NewAPIConnection creates an
func NewAPIConnection(ctx context.Context, spec Spec, data map[string]any) (ac *APIConnection, err error) {

	ac = &APIConnection{
		Context: g.NewContext(ctx),
		State: &APIState{
			Env:    g.KVArrToMap(os.Environ()...),
			State:  g.M(),
			Queues: make(map[string]*Queue),
		},
		Spec: spec,
		eval: goval.NewEvaluator(),
	}

	// load state / secrets
	if state, ok := data["state"]; ok {
		if err = g.JSONConvert(state, &ac.State.State); err != nil {
			return ac, g.Error(err)
		}
	}
	if secrets, ok := data["secrets"]; ok {
		if err = g.JSONConvert(secrets, &ac.State.Secrets); err != nil {
			return ac, g.Error(err)
		}
	}

	// set endpoint contexts
	for key, endpoint := range ac.Spec.EndpointMap {
		endpoint.context = g.NewContext(ac.Context.Ctx)
		ac.Spec.EndpointMap[key] = endpoint
	}

	// register queues
	for _, queueName := range ac.Spec.Queues {
		if val := os.Getenv("SLING_THREADS"); cast.ToInt(val) > 1 {
			g.Warn("API queues may not work correctly for multiple threads.")
		}
		_, err = ac.RegisterQueue(queueName)
		if err != nil {
			return ac, g.Error(err)
		}
	}

	return ac, nil
}

// Authenticate performs the auth workflow if needed. Like a Connect step.
// Header based auths (such as Basic, or Bearer) don't need this step
// save payload in APIState.Auth
func (ac *APIConnection) Authenticate() (err error) {
	auth := ac.Spec.Authentication

	// set auth data
	setAuthenticated := func() {
		ac.State.Auth.Authenticated = true
		for key, endpoint := range ac.Spec.EndpointMap {
			for k, v := range ac.State.Auth.Headers {
				endpoint.Request.Headers[k] = v
			}
			ac.Spec.EndpointMap[key] = endpoint
		}
	}

	switch auth.Type {
	case AuthTypeNone:
		ac.State.Auth.Authenticated = true
		return nil

	case AuthTypeBearer:
		token, err := ac.renderString(auth.Token)
		if err != nil {
			return g.Error(err, "could not render token")
		} else if token == "" {
			return g.Error(err, "no token was provided for bearer authentication")
		}
		ac.State.Auth.Headers = map[string]string{"Authorization": g.F("Bearer %s", token)}
		setAuthenticated()
		return nil

	case AuthTypeBasic:
		userPass, err := ac.renderString(g.F("%s:%s", auth.Username, auth.Password))
		if err != nil {
			return g.Error(err, "could not render user-password")
		}
		credentialsB64 := base64.StdEncoding.EncodeToString([]byte(userPass))
		ac.State.Auth.Headers = map[string]string{"Authorization": g.F("Basic %s", credentialsB64)}
		setAuthenticated()
		return nil

	case AuthTypeOAuth2:
		// TODO: implement various OAuth2 flows

	default:
		setAuthenticated()
		return nil
	}

	return
}

// Close performs cleanup of all resources
func (ac *APIConnection) Close() error {
	ac.CloseAllQueues()
	return nil
}

func (ac *APIConnection) ListEndpoints(patterns ...string) (endpoints Endpoints, err error) {

	for _, endpointName := range ac.Spec.endpointsOrdered {
		endpoint := ac.Spec.EndpointMap[endpointName]
		if !endpoint.Disabled {
			endpoints = append(endpoints, endpoint)
		}
	}

	if ac.Spec.IsDynamic() {
		// TODO: generate/obtain list of dynamic endpoints
	}

	if len(patterns) > 0 && patterns[0] != "" {
		pattern := patterns[0]
		filterEndpoints := Endpoints{}
		for _, endpoint := range endpoints {
			if g.IsMatched([]string{pattern}, endpoint.Name) {
				filterEndpoints = append(filterEndpoints, endpoint)
			}
		}
		endpoints = filterEndpoints
	}

	return endpoints, nil
}

type APIStreamConfig struct {
	Flatten     int // levels of flattening. 0 is infinite
	JmesPath    string
	Select      []string // select specific columns
	Limit       int
	Metadata    iop.Metadata
	Mode        string
	DsConfigMap map[string]any // stream processor options
}

func (ac *APIConnection) ReadDataflow(endpointName string, sCfg APIStreamConfig) (df *iop.Dataflow, err error) {
	if !ac.State.Auth.Authenticated {
		return nil, g.Error("not authenticated")
	}

	// get endpoint, match to name
	var endpoint *Endpoint
	{
		for _, ep := range ac.Spec.EndpointMap {
			if strings.EqualFold(ep.Name, endpointName) {
				endpoint = &ep
				break
			}
		}
		if endpoint == nil {
			return nil, g.Error("endpoint not found: %s", endpointName)
		}
		// set endpoint conn
		endpoint.conn = ac

		if err = validateAndSetDefaults(endpoint, ac.Spec); err != nil {
			return nil, g.Error(err, "endpoint validation failed")
		}

		// TODO: perform pre-endpoint calls
	}

	if endpoint.Disabled {
		return nil, g.Error(err, "endpoint is disabled in spec")
	}

	// start request process
	ds, err := streamRequests(endpoint, sCfg)
	if err != nil {
		return nil, g.Error(err, "could not stream requests")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return nil, g.Error(err, "could not make dataflow")
	}

	// now that columns are detected, set the metadata for PK
	if len(df.Buffer) > 0 {
		err = df.Columns.SetKeys(iop.PrimaryKey, endpoint.Response.Records.PrimaryKey...)
		if err != nil {
			return nil, g.Error(err, "could not set primary key column")
		}
	}

	return
}

type APIState struct {
	Env     map[string]string `json:"env,omitempty"`
	State   map[string]any    `json:"state,omitempty"`
	Secrets map[string]any    `json:"secrets,omitempty"`
	Queues  map[string]*Queue `json:"queues,omitempty"` // appends to file
	Auth    APIStateAuth      `json:"auth,omitempty"`
}

type APIStateAuth struct {
	Authenticated bool              `json:"authenticated,omitempty"`
	Token         string            `json:"token,omitempty"` // refresh token?
	Headers       map[string]string `json:"-"`               // to inject
}

var bracketRegex = regexp.MustCompile(`\$\{([^\{\}]+)\}`)

func (ac *APIConnection) getStateMap(extraMaps map[string]any, varsToCheck []string) map[string]any {

	sections := []string{"env", "state", "secrets", "auth", "sync"}

	ac.Context.Lock()
	statePayload := g.Marshal(ac.State)
	stateMap, _ := g.UnmarshalMap(statePayload)
	stateMap["null"] = nil

	// Add queues to the state map
	if ac.State.Queues != nil {
		queueMap := g.M()
		for name, queue := range ac.State.Queues {
			queueMap[name] = queue // Store the queue name as a reference
		}
		stateMap["queue"] = queueMap
	}

	ac.Context.Unlock()

	for mapKey, newVal := range extraMaps {
		// non-map values
		if g.In(mapKey, "records") {
			stateMap[mapKey] = newVal
			continue
		}

		// get old/existing map
		var oldMap map[string]any
		if oldMapV, ok := stateMap[mapKey]; ok {
			oldMap, _ = g.UnmarshalMap(g.Marshal(oldMapV))
		} else {
			oldMap = g.M()
		}

		// get new map
		newMap, _ := g.UnmarshalMap(g.Marshal(newVal))
		for k, v := range newMap {
			oldMap[k] = v // overwrite
		}

		// rewrite after merging
		stateMap[mapKey] = oldMap
	}

	// remake map for jmespath to work
	for _, section := range sections {
		subMap, _ := g.UnmarshalMap(g.Marshal(stateMap[section]))
		stateMap[section] = subMap // set back
	}

	// ensure vars exist. if it doesn't, set at nil
	for _, varToCheck := range varsToCheck {
		varToCheck = strings.TrimSpace(varToCheck)
		parts := strings.Split(varToCheck, ".")
		if len(parts) != 2 {
			// continue
		}
		section := parts[0]
		key := parts[1]
		if g.In(section, sections...) {
			nested, _ := g.UnmarshalMap(g.Marshal(stateMap[section]))
			if nested == nil {
				nested = g.M()
			}
			if _, ok := nested[key]; !ok {
				nested[key] = nil
			}
			stateMap[section] = nested // set back
		}
	}

	return stateMap
}

func (ac *APIConnection) renderString(val any, extraMaps ...map[string]any) (newVal string, err error) {
	output, err := ac.renderAny(val, extraMaps...)
	if err != nil {
		return
	}

	switch output.(type) {
	case map[string]string, map[string]any, map[any]any, []any, []string:
		newVal = g.Marshal(output)
	default:
		newVal = cast.ToString(output)
	}

	return
}

func (ac *APIConnection) renderAny(input any, extraMaps ...map[string]any) (output any, err error) {

	output = input

	matches := bracketRegex.FindAllStringSubmatch(cast.ToString(output), -1)

	expressions := []string{}
	varsToCheck := []string{} // to ensure existence in state maps
	for _, match := range matches {
		expression := match[1]
		expressions = append(expressions, expression)
		varsToCheck = append(varsToCheck, extractVars(expression)...)
	}
	em := g.M()
	if len(extraMaps) > 0 {
		em = extraMaps[0]
	}

	stateMap := ac.getStateMap(em, varsToCheck)

	// we'll use those keys as jmespath expr
	for _, expr := range expressions {
		var value any
		callsFunc := strings.Contains(expr, "(") && strings.Contains(expr, ")")

		// attempt to resolve with jmespath first if no function is called
		if !callsFunc {
			value, err = jmespath.Search(expr, stateMap)
		}

		if callsFunc || err != nil {
			value, err = ac.eval.Evaluate(expr, stateMap, GlobalFunctionMap)
			if err != nil {
				return "", g.Error(err, "could not render expression")
			}
		}

		key := "${" + expr + "}"

		if strings.TrimSpace(cast.ToString(input)) == key {
			output = value
		} else {
			// treat as string if whole input isn't the expression
			outputStr := cast.ToString(output)
			switch value.(type) {
			case map[string]string, map[string]any, map[any]any, []any, []string:
				output = strings.ReplaceAll(outputStr, key, g.Marshal(value))
			default:
				output = strings.ReplaceAll(outputStr, key, cast.ToString(value))
			}
		}
	}

	return output, nil
}

// GetSyncedState cycles through each endpoint, and collects the values
// for each of the Endpoint.Sync values. Output is a
// map[Endpoint.Name]map[Sync.value] = Endpoint.syncMap[Sync.value]
func (ac *APIConnection) GetSyncedState(endpointName string) (data map[string]map[string]any, err error) {
	data = make(map[string]map[string]any)

	// Iterate through all endpoints
	for _, endpoint := range ac.Spec.EndpointMap {
		// Skip if no sync values defined
		if len(endpoint.Sync) == 0 || !strings.EqualFold(endpoint.Name, endpointName) {
			continue
		}

		// Initialize map for this endpoint
		data[endpoint.Name] = make(map[string]any)

		// Collect each sync value from endpoint's state
		for _, syncKey := range endpoint.Sync {
			if val, ok := endpoint.State[syncKey]; ok {
				data[endpoint.Name][syncKey] = val
			}
		}
	}

	return data, nil
}

// PutSyncedState restores the state from previous run in each endpoint
// using the Endpoint.Sync values.
// Inputs is map[Endpoint.Name]map[Sync.value] = Endpoint.syncMap[Sync.value]
func (ac *APIConnection) PutSyncedState(endpointName string, data map[string]map[string]any) (err error) {
	// Iterate through all endpoints
	for key, endpoint := range ac.Spec.EndpointMap {

		// Skip if no sync values defined or no data for this endpoint
		if len(endpoint.Sync) == 0 || !strings.EqualFold(endpoint.Name, endpointName) || len(data) == 0 {
			continue
		}

		endpointData, exists := data[endpoint.Name]
		if !exists {
			continue
		}

		// Initialize state map if nil
		if endpoint.syncMap == nil {
			endpoint.syncMap = make(StateMap)
		}

		// Restore each sync value to endpoint's state
		for key, val := range endpointData {
			// Only restore if it's in the sync list
			for _, syncKey := range endpoint.Sync {
				if syncKey == key {
					endpoint.syncMap[key] = val
					break
				}
			}
		}
		ac.Spec.EndpointMap[key] = endpoint
	}

	return nil
}

func (ac *APIConnection) MakeDynamicEndpointIterator(iter *Iterate) (err error) {
	return
}

// extractVars identifies variable references in a string expression,
// ignoring those inside double quotes. It recognizes patterns like env.VAR,
// state.VAR, secrets.VAR, and auth.VAR.
func extractVars(expr string) []string {
	// To track found variable references
	var references []string

	// Regular expression for finding variable references
	// Matches env., state., secrets., auth. followed by a variable name
	refRegex := regexp.MustCompile(`(env|state|secrets|auth|response|request|sync)\.\w+`)

	// First, we need to identify string literals to exclude them
	// Track positions of string literals
	inString := false
	stringRanges := make([][]int, 0)
	var start int

	for i, char := range expr {
		if char == '"' {
			// Check if the quote is escaped
			if i > 0 && expr[i-1] == '\\' {
				continue
			}

			if !inString {
				// Start of a string
				inString = true
				start = i
			} else {
				// End of a string
				inString = false
				stringRanges = append(stringRanges, []int{start, i})
			}
		}
	}

	// Find all potential references
	matches := refRegex.FindAllStringIndex(expr, -1)

	// Filter out references that are inside string literals
	for _, match := range matches {
		isInString := false
		for _, strRange := range stringRanges {
			if match[0] >= strRange[0] && match[1] <= strRange[1] {
				isInString = true
				break
			}
		}

		if !isInString {
			// Extract the actual reference
			reference := expr[match[0]:match[1]]
			references = append(references, reference)
		}
	}

	return references
}

func hasBrackets(expr string) bool {
	matches := bracketRegex.FindAllStringSubmatch(expr, -1)
	return len(matches) > 0
}

var (
	streamRequests = func(ep *Endpoint, sCfg APIStreamConfig) (ds *iop.Datastream, err error) {
		return nil, g.Error("please use the official sling-cli release for reading APIs")
	}
	validateAndSetDefaults = func(ep *Endpoint, spec Spec) (err error) {
		return g.Error("please use the official sling-cli release for reading APIs")
	}
)

// RegisterQueue creates a new queue with the given name
// If a queue with the same name already exists, it is returned
func (ac *APIConnection) RegisterQueue(name string) (*Queue, error) {
	ac.Context.Lock()
	defer ac.Context.Unlock()

	// Return existing queue if it exists
	if q, exists := ac.State.Queues[name]; exists {
		return q, nil
	}

	// Create new queue
	q, err := NewQueue(name)
	if err != nil {
		return nil, err
	}

	// Register the queue
	ac.State.Queues[name] = q
	return q, nil
}

// GetQueue retrieves a queue by name
func (ac *APIConnection) GetQueue(name string) (*Queue, bool) {
	ac.Context.Lock()
	defer ac.Context.Unlock()

	q, exists := ac.State.Queues[name]
	return q, exists
}

// RemoveQueue closes and removes a queue
func (ac *APIConnection) RemoveQueue(name string) error {
	ac.Context.Lock()
	defer ac.Context.Unlock()

	q, exists := ac.State.Queues[name]
	if !exists {
		return nil
	}

	err := q.Close()
	delete(ac.State.Queues, name)
	return err
}

// CloseAllQueues closes all queues associated with this connection
func (ac *APIConnection) CloseAllQueues() {
	ac.Context.Lock()
	defer ac.Context.Unlock()

	for name, queue := range ac.State.Queues {
		if err := queue.Close(); err != nil {
			g.Warn("failed to close queue %s: %v", name, err)
		}
		delete(ac.State.Queues, name)
	}
}
