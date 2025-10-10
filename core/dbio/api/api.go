package api

import (
	"context"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/jmespath/go-jmespath"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

type APIConnection struct {
	Spec      Spec
	State     *APIState
	Context   *g.Context
	evaluator *iop.Evaluator `json:"-" yaml:"-"`
}

// NewAPIConnection creates an
func NewAPIConnection(ctx context.Context, spec Spec, data map[string]any) (ac *APIConnection, err error) {

	ac = &APIConnection{
		Context: g.NewContext(ctx),
		State: &APIState{
			Env:    g.KVArrToMap(os.Environ()...),
			State:  g.M(),
			Queues: make(map[string]*iop.Queue),
			Auth:   APIStateAuth{Mutex: &sync.Mutex{}},
		},
		Spec:      spec,
		evaluator: iop.NewEvaluator(g.ArrStr("env", "state", "secrets", "auth", "response", "request", "sync")),
	}

	// Merge spec defaults state into ac.State.State first
	if len(spec.Defaults.State) > 0 {
		for k, v := range spec.Defaults.State {
			ac.State.State[k] = v
		}
	}

	// load state / secrets from data (these override defaults)
	if state, ok := data["state"]; ok {
		var stateMap map[string]any
		if err = g.JSONConvert(state, &stateMap); err != nil {
			return ac, g.Error(err)
		}
		// Merge into existing state (overriding defaults)
		for k, v := range stateMap {
			ac.State.State[k] = v
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

	return ac, nil
}

func (ac *APIConnection) GetReplicationStore() (store map[string]any) {
	err := g.JSONConvert(ac.State.State["replication_store"], &store)
	if err != nil {
		g.Warn("could not unmarshal from API state replication_store: " + err.Error())
	}

	if store == nil {
		store = g.M()
	}
	return store
}

func (ac *APIConnection) SetReplicationStore(store map[string]any) {
	if store == nil {
		store = g.M()
	}
	ac.State.State["replication_store"] = store
}

// Close performs cleanup of all resources
func (ac *APIConnection) Close() error {
	ac.CloseAllQueues()
	return nil
}

func (ac *APIConnection) ListEndpoints(patterns ...string) (endpoints Endpoints, err error) {

	// Render dynamic endpoints if needed
	if ac.Spec.IsDynamic() {
		// Ensure authentication before rendering dynamic endpoints
		if err := ac.EnsureAuthenticated(); err != nil {
			return nil, g.Error(err, "authentication required for dynamic endpoints")
		}

		// Render dynamic endpoints (this will populate ac.Spec.EndpointMap)
		if err := ac.RenderDynamicEndpoints(); err != nil {
			return nil, g.Error(err, "failed to render dynamic endpoints")
		}
	}

	// Collect all endpoints (static + dynamically generated)
	for _, endpointName := range ac.Spec.endpointsOrdered {
		endpoint := ac.Spec.EndpointMap[endpointName]
		if !endpoint.Disabled {
			endpoints = append(endpoints, endpoint)
		}
	}

	// fill DependsOn
	for i, ep := range endpoints {
		if len(ep.DependsOn) == 0 {
			ep.DependsOn = endpoints.HasUpstreams(ep.Name)
		}
		endpoints[i] = ep
	}

	// Filter by pattern if provided
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

	endpoints.Sort()

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

		if err = compileSpecEndpoint(endpoint, ac.Spec); err != nil {
			return nil, g.Error(err, "endpoint validation failed")
		}

		g.Trace(`Compiled Spec for Endpoint "%s": %s`, endpoint.Name, g.Marshal(endpoint))
	}

	if endpoint.Disabled {
		return nil, g.Error(err, "endpoint is disabled in spec")
	}

	// setup if specified
	if err = endpoint.setup(); err != nil {
		return nil, g.Error(err, "could not setup for main request")
	}

	// register queues being used by endpoint
	{
		for _, processor := range endpoint.Response.Processors {
			if strings.HasPrefix(processor.Output, "queue.") {
				queueName := strings.TrimPrefix(processor.Output, "queue.")

				if !g.In(queueName, ac.Spec.Queues...) {
					return nil, g.Error("did not declare queue %s in queues list", queueName)
				}

				_, err = ac.RegisterQueue(queueName)
				if err != nil {
					return nil, g.Error(err, "could not register processor output queue: %s", queueName)
				}
			}
		}

		if overStr := cast.ToString(endpoint.Iterate.Over); strings.HasPrefix(overStr, "queue.") {
			queueName := strings.TrimPrefix(overStr, "queue.")

			if !g.In(queueName, ac.Spec.Queues...) {
				return nil, g.Error("did not declare queue %s in queues list", queueName)
			}

			_, err = ac.RegisterQueue(queueName)
			if err != nil {
				return nil, g.Error(err, "could not register iterate over queue: %s", queueName)
			}
		}
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

	df.Defer(func() {
		// teardown if specified
		if err = endpoint.teardown(); err != nil {
			df.Context.CaptureErr(g.Error(err, "could not teardown for main request"))
		}
	})

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
	Env     map[string]string     `json:"env,omitempty"`
	State   map[string]any        `json:"state,omitempty"`
	Secrets map[string]any        `json:"secrets,omitempty"`
	Queues  map[string]*iop.Queue `json:"queues,omitempty"` // appends to file
	Auth    APIStateAuth          `json:"auth,omitempty"`
}

type APIStateAuth struct {
	Authenticated bool              `json:"authenticated,omitempty"`
	Token         string            `json:"token,omitempty"`      // refresh token?
	Headers       map[string]string `json:"-"`                    // to inject
	ExpiresAt     int64             `json:"expires_at,omitempty"` // Unix timestamp when auth expires

	Sign  func(context.Context, *http.Request, []byte) error `json:"-"`          // for AWS Sigv4
	Mutex *sync.Mutex                                        `json:"-" yaml:"-"` // Mutex for auth operations
}

var bracketRegex = regexp.MustCompile(`\{([^\{\}]+)\}`)

func (ac *APIConnection) getStateMap(extraMaps map[string]any) map[string]any {

	sections := []string{"env", "state", "secrets", "auth", "sync"}

	ac.Context.Lock()
	defer ac.Context.Unlock()

	// Create deep copies of the state maps to avoid concurrent access issues
	stateMapCopy := make(map[string]any)
	if ac.State.State != nil {
		for k, v := range ac.State.State {
			stateMapCopy[k] = v
		}
	}

	envCopy := make(map[string]string)
	if ac.State.Env != nil {
		for k, v := range ac.State.Env {
			envCopy[k] = v
		}
	}

	secretsCopy := make(map[string]any)
	if ac.State.Secrets != nil {
		for k, v := range ac.State.Secrets {
			secretsCopy[k] = v
		}
	}

	stateMap := g.M(
		"env", envCopy,
		"state", stateMapCopy,
		"secrets", secretsCopy,
		"auth", ac.State.Auth,
		"null", nil,
	)

	// Add queues to the state map
	if ac.State.Queues != nil {
		queueMap := g.M()
		for name, queue := range ac.State.Queues {
			queueMap[name] = queue // Store the queue name as a reference
		}
		stateMap["queue"] = queueMap
	}

	// Process extraMaps without holding the main lock
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

	return stateMap
}

func (ac *APIConnection) renderString(val any, extraMaps ...map[string]any) (newVal string, err error) {
	em := g.M()
	if len(extraMaps) > 0 {
		em = extraMaps[0]
	}

	return ac.evaluator.RenderString(val, ac.getStateMap(em))
}

func (ac *APIConnection) renderAny(input any, extraMaps ...map[string]any) (output any, err error) {
	em := g.M()
	if len(extraMaps) > 0 {
		em = extraMaps[0]
	}

	return ac.evaluator.RenderAny(input, ac.getStateMap(em))
}

// GetSyncedState cycles through each endpoint, and collects the values
// for each of the Endpoint.Sync values. Output is a
// map[Sync.value] = Endpoint.syncMap[Sync.value]
func (ac *APIConnection) GetSyncedState(endpointName string) (data map[string]any, err error) {
	data = make(map[string]any)

	// Iterate through all endpoints
	for _, endpoint := range ac.Spec.EndpointMap {
		// Skip if no sync values defined
		if len(endpoint.Sync) == 0 || !strings.EqualFold(endpoint.Name, endpointName) {
			continue
		}

		// Collect each sync value from endpoint's state with proper locking
		endpoint.context.Lock()
		for _, syncKey := range endpoint.Sync {
			if val, ok := endpoint.State[syncKey]; ok {
				data[syncKey] = val
			}
		}
		endpoint.context.Unlock()
	}

	return data, nil
}

// PutSyncedState restores the state from previous run in each endpoint
// using the Endpoint.Sync values.
// Inputs is map[Sync.value] = Endpoint.syncMap[Sync.value]
func (ac *APIConnection) PutSyncedState(endpointName string, data map[string]any) (err error) {
	// Iterate through all endpoints
	for key, endpoint := range ac.Spec.EndpointMap {

		// Skip if no sync values defined or no data for this endpoint
		if len(endpoint.Sync) == 0 || !strings.EqualFold(endpoint.Name, endpointName) || len(data) == 0 {
			continue
		}

		// Initialize state map if nil and sync state with proper locking
		endpoint.context.Lock()
		if endpoint.syncMap == nil {
			endpoint.syncMap = make(StateMap)
		}

		// Restore each sync value to endpoint's state
		for key, val := range data {
			// Only restore if it's in the sync list
			for _, syncKey := range endpoint.Sync {
				if syncKey == key {
					endpoint.syncMap[key] = val
					break
				}
			}
		}
		endpoint.context.Unlock()

		ac.Spec.EndpointMap[key] = endpoint
	}

	return nil
}

func hasBrackets(expr string) bool {
	matches := bracketRegex.FindAllStringSubmatch(expr, -1)
	return len(matches) > 0
}

var (
	streamRequests = func(ep *Endpoint, sCfg APIStreamConfig) (ds *iop.Datastream, err error) {
		return nil, g.Error("please use the official sling-cli release for reading APIs")
	}
	compileSpecEndpoint = func(ep *Endpoint, spec Spec) (err error) {
		return g.Error("please use the official sling-cli release for reading APIs")
	}
	runSequence = func(s Sequence, ep *Endpoint) (err error) {
		return g.Error("please use the official sling-cli release for running API sequences")
	}
	FetchSpec = func(_ string) (string, error) {
		return "", g.Error("please use the official sling-cli release for fetching API specs")
	}
)

// RegisterQueue creates a new queue with the given name
// If a queue with the same name already exists, it is returned
func (ac *APIConnection) RegisterQueue(name string) (*iop.Queue, error) {
	ac.Context.Lock()
	defer ac.Context.Unlock()

	// Return existing queue if it exists
	if q, exists := ac.State.Queues[name]; exists {
		return q, nil
	}

	// Create new queue
	q, err := iop.NewQueue(name)
	if err != nil {
		return nil, err
	}

	// Register the queue
	ac.State.Queues[name] = q
	return q, nil
}

// GetQueue retrieves a queue by name
func (ac *APIConnection) GetQueue(name string) (*iop.Queue, bool) {
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

// renderEndpointTemplate renders an endpoint template with the given state variables
func (ac *APIConnection) renderEndpointTemplate(dynEndpoint DynamicEndpoint, iterValue any, extraState map[string]any) (*Endpoint, error) {
	// Deep copy the endpoint template
	endpointJSON := g.Marshal(dynEndpoint.Endpoint)
	if endpointJSON == "" {
		return nil, g.Error("could not marshal endpoint template")
	}

	var renderedEndpoint Endpoint
	if err := g.Unmarshal(endpointJSON, &renderedEndpoint); err != nil {
		return nil, g.Error(err, "could not unmarshal endpoint template")
	}

	// Set originalMap so validateAndSetDefaults knows which fields were explicitly set
	// This prevents defaults from overwriting our rendered values
	var originalMap map[string]any
	if err := g.Unmarshal(endpointJSON, &originalMap); err != nil {
		return nil, g.Error(err, "could not unmarshal endpoint template to map")
	}
	renderedEndpoint.originalMap = originalMap

	// Build state map for rendering with the iteration value
	stateMap := g.M()

	// Copy any extra state values
	for k, v := range extraState {
		stateMap[k] = v
	}

	// Set the iteration value in state (into variable)
	if dynEndpoint.Into != "" {
		// Parse the into path (e.g., "state.table_name")
		parts := strings.Split(dynEndpoint.Into, ".")
		if len(parts) == 2 && parts[0] == "state" {
			stateMap[parts[1]] = iterValue
		} else {
			return nil, g.Error("invalid 'into' variable: %s (must be in format 'state.variable_name')", dynEndpoint.Into)
		}
	}

	// Create extra maps for rendering - this will be merged with ac.State.State
	extraMaps := g.M("state", stateMap)

	// Render the endpoint name
	if renderedEndpoint.Name != "" {
		renderedName, err := ac.renderString(renderedEndpoint.Name, extraMaps)
		if err != nil {
			return nil, g.Error(err, "could not render endpoint name")
		}
		renderedEndpoint.Name = renderedName
	}

	// Render description if present
	if renderedEndpoint.Description != "" {
		renderedDesc, err := ac.renderString(renderedEndpoint.Description, extraMaps)
		if err != nil {
			return nil, g.Error(err, "could not render endpoint description")
		}
		renderedEndpoint.Description = renderedDesc
	}

	// Render docs URL if present
	if renderedEndpoint.Docs != "" {
		renderedDocs, err := ac.renderString(renderedEndpoint.Docs, extraMaps)
		if err != nil {
			return nil, g.Error(err, "could not render endpoint docs")
		}
		renderedEndpoint.Docs = renderedDocs
	}

	// Render the request URL
	if renderedEndpoint.Request.URL != "" {
		renderedURL, err := ac.renderString(renderedEndpoint.Request.URL, extraMaps)
		if err != nil {
			return nil, g.Error(err, "could not render request URL")
		}
		renderedEndpoint.Request.URL = renderedURL
	}

	// Render request headers
	if len(renderedEndpoint.Request.Headers) > 0 {
		renderedHeaders, err := ac.renderAny(renderedEndpoint.Request.Headers, extraMaps)
		if err != nil {
			return nil, g.Error(err, "could not render request headers")
		}
		if headers, ok := renderedHeaders.(map[string]any); ok {
			renderedEndpoint.Request.Headers = headers
		}
	}

	// Render request parameters
	if len(renderedEndpoint.Request.Parameters) > 0 {
		renderedParams, err := ac.renderAny(renderedEndpoint.Request.Parameters, extraMaps)
		if err != nil {
			return nil, g.Error(err, "could not render request parameters")
		}
		if params, ok := renderedParams.(map[string]any); ok {
			renderedEndpoint.Request.Parameters = params
		}
	}

	// Render request payload if present
	if renderedEndpoint.Request.Payload != nil {
		renderedPayload, err := ac.renderAny(renderedEndpoint.Request.Payload, extraMaps)
		if err != nil {
			return nil, g.Error(err, "could not render request payload")
		}
		renderedEndpoint.Request.Payload = renderedPayload
	}

	// Render state values
	if len(renderedEndpoint.State) > 0 {
		renderedState, err := ac.renderAny(renderedEndpoint.State, extraMaps)
		if err != nil {
			return nil, g.Error(err, "could not render endpoint state")
		}
		if state, ok := renderedState.(map[string]any); ok {
			renderedEndpoint.State = state
		}
	}

	// Initialize context for the endpoint
	renderedEndpoint.context = g.NewContext(ac.Context.Ctx)

	return &renderedEndpoint, nil
}

// RenderDynamicEndpoints will render the dynamic objects
// basically mutating the spec endpoints.
// Needs to authenticate first
func (ac *APIConnection) RenderDynamicEndpoints() (err error) {
	if !ac.Spec.IsDynamic() {
		return nil // No dynamic endpoints to render
	}

	// Initialize EndpointMap if nil
	if ac.Spec.EndpointMap == nil {
		ac.Spec.EndpointMap = make(EndpointMap)
	}

	g.Debug("rendering %d dynamic endpoint definition(s)", len(ac.Spec.DynamicEndpoints))

	// Process each dynamic endpoint definition
	for dynIdx, dynEndpoint := range ac.Spec.DynamicEndpoints {
		g.Debug("processing dynamic endpoint definition %d", dynIdx+1)

		// Create ephemeral state for setup sequence
		setupState := g.M()

		// Copy current state
		ac.Context.Lock()
		for k, v := range ac.State.State {
			setupState[k] = v
		}
		ac.Context.Unlock()

		// Execute setup sequence if defined
		if len(dynEndpoint.Setup) > 0 {
			g.Debug("running setup sequence (%d calls)", len(dynEndpoint.Setup))

			baseEndpoint := &Endpoint{
				context: g.NewContext(ac.Context.Ctx),
				conn:    ac,
				State:   setupState,
			}

			if err := runSequence(dynEndpoint.Setup, baseEndpoint); err != nil {
				return g.Error(err, "dynamic endpoint setup failed for definition %d", dynIdx+1)
			}

			// Sync state back
			setupState = baseEndpoint.State
		}

		// Evaluate the iterate expression to get the list of items
		var iterList []any

		// Check if it's a JSON literal (starts with [ or {) before rendering
		// JSON literals should not be rendered as templates
		trimmed := strings.TrimSpace(dynEndpoint.Iterate)
		var iterExpr string

		if strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "{") {
			// Don't render JSON literals - they might contain { } that aren't templates
			iterExpr = dynEndpoint.Iterate
		} else {
			// Render the iterate expression (for state variables like "state.table_list")
			var err error
			iterExpr, err = ac.renderString(dynEndpoint.Iterate, g.M("state", setupState))
			if err != nil {
				return g.Error(err, "could not render iterate expression: %s", dynEndpoint.Iterate)
			}
		}

		// Try to parse as JSON literal first (for arrays like '["a", "b", "c"]' or objects)
		trimmed = strings.TrimSpace(iterExpr)
		if strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "{") {
			if err := g.Unmarshal(iterExpr, &iterList); err == nil {
				// Successfully parsed as JSON literal
				g.Debug("parsed iterate expression as JSON literal")
			} else {
				return g.Error(err, "could not parse iterate expression as JSON: %s", iterExpr)
			}
		} else {
			// Evaluate as JMESPath expression
			stateMap := ac.getStateMap(g.M("state", setupState))
			result, err := jmespath.Search(iterExpr, stateMap)
			if err != nil {
				return g.Error(err, "could not evaluate iterate expression: %s", iterExpr)
			}

			// Convert result to array
			switch v := result.(type) {
			case []any:
				iterList = v
			default:
				// Try to convert to array
				if err := g.JSONConvert(result, &iterList); err != nil {
					return g.Error(err, "iterate expression did not return an array: %s (got type %T)", iterExpr, result)
				}
			}
		}

		if len(iterList) == 0 {
			g.Warn("dynamic endpoint definition %d: iterate expression returned empty list", dynIdx+1)
			continue
		}

		g.Debug("iterating over %d items to generate endpoints", len(iterList))

		// Generate endpoints for each item in the iteration list
		generatedCount := 0
		for itemIdx, iterValue := range iterList {
			// Render the endpoint template with the current iteration value
			renderedEndpoint, err := ac.renderEndpointTemplate(dynEndpoint, iterValue, setupState)
			if err != nil {
				return g.Error(err, "failed to render endpoint template for item %d", itemIdx+1)
			}

			// Check for duplicate endpoint names
			if _, exists := ac.Spec.EndpointMap[renderedEndpoint.Name]; exists {
				return g.Error("duplicate endpoint name generated: %s (check your dynamic endpoint template)", renderedEndpoint.Name)
			}

			// Validate and set defaults for the rendered endpoint
			if err := compileSpecEndpoint(renderedEndpoint, ac.Spec); err != nil {
				return g.Error(err, "validation failed for generated endpoint: %s", renderedEndpoint.Name)
			}

			// Add to endpoint map
			ac.Spec.EndpointMap[renderedEndpoint.Name] = *renderedEndpoint
			ac.Spec.endpointsOrdered = append(ac.Spec.endpointsOrdered, renderedEndpoint.Name)

			generatedCount++
		}

		g.Debug("generated %d endpoints from dynamic endpoint definition %d", generatedCount, dynIdx+1)
	}

	g.Debug("dynamic endpoint rendering complete, total endpoints: %d", len(ac.Spec.EndpointMap))
	return nil
}

// IsAuthExpired checks if the authentication has expired
func (ac *APIConnection) IsAuthExpired() bool {
	if ac.State.Auth.ExpiresAt == 0 {
		return false // No expiry set
	}
	return time.Now().Unix() >= ac.State.Auth.ExpiresAt
}

// EnsureAuthenticated checks if authentication is valid and re-authenticates if needed
// This method ensures thread-safe authentication checks and re-authentication
func (ac *APIConnection) EnsureAuthenticated() error {
	ac.State.Auth.Mutex.Lock()
	defer ac.State.Auth.Mutex.Unlock()

	// Check if authentication has expired or not authenticated
	if !ac.State.Auth.Authenticated || ac.IsAuthExpired() {
		g.Debug("Authentication expired or not authenticated, re-authenticating...")
		if err := ac.Authenticate(); err != nil {
			return g.Error(err, "failed to authenticate")
		}
	}
	return nil
}
