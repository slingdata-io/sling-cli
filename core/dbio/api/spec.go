package api

import (
	"context"
	"maps"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/flarco/g"
	"github.com/jmespath/go-jmespath"
	"github.com/maja42/goval"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

func LoadSpec(specBody string) (spec Spec, err error) {

	// set maps
	err = yaml.Unmarshal([]byte(specBody), &spec.originalMap)
	if err != nil {
		err = g.Error(err, "Error parsing yaml content")
		return
	}

	// so that JMESPath works
	if err = g.Unmarshal(g.Marshal(spec.originalMap), &spec.originalMap); err != nil {
		return spec, g.Error(err, "error loading API spec into map")
	}

	// load spec from body
	if err = yaml.Unmarshal([]byte(specBody), &spec); err != nil {
		return spec, g.Error(err, "error loading API spec")
	}

	rootMap := yaml.MapSlice{}
	err = yaml.Unmarshal([]byte(specBody), &rootMap)
	if err != nil {
		err = g.Error(err, "Error parsing yaml content")
		return
	}

	// get endpoint order
	for _, rootNode := range rootMap {
		if cast.ToString(rootNode.Key) == "endpoints" {
			endpointNodes, ok := rootNode.Value.(yaml.MapSlice)
			if !ok {
				continue
			}
			for _, endpointNode := range endpointNodes {
				key := cast.ToString(endpointNode.Key)
				spec.endpointsOrdered = append(spec.endpointsOrdered, key)
			}
		}
	}

	// set endpoint index
	for _, endpointName := range spec.endpointsOrdered {
		endpoint := spec.EndpointMap[endpointName]

		// get original map for endpoint
		originalMap, err := jmespath.Search("endpoints."+endpointName, spec.originalMap)
		if err != nil {
			return spec, g.Error(err, "error getting endpoint spec map value: %s", endpointName)
		} else if err = g.Unmarshal(g.Marshal(originalMap), &endpoint.originalMap); err != nil {
			return spec, g.Error(err, "error loading endpoint spec into map: %s", endpointName)
		}

		endpoint.Name = endpointName
		endpoint.originalMap["name"] = endpointName
		endpoint.context = g.NewContext(context.Background())
		endpoint.auth = APIStateAuth{Mutex: &sync.Mutex{}}

		if endpoint.State == nil {
			endpoint.State = g.M() // set default state
		}
		spec.EndpointMap[endpointName] = endpoint
	}

	// compile and validate endpoints
	compiledEndpointMap := EndpointMap{}
	for name, endpoint := range spec.EndpointMap {
		if err = compileSpecEndpoint(&endpoint, spec); err != nil {
			return spec, g.Error(err, "endpoint validation failed")
		}
		compiledEndpointMap[name] = endpoint
	}

	// validate that all queues used by endpoints are declared
	if err = spec.validateQueues(compiledEndpointMap); err != nil {
		return spec, g.Error(err, "queue validation failed")
	}

	// validate that all sync keys have corresponding processors
	if err = spec.validateSync(compiledEndpointMap); err != nil {
		return spec, g.Error(err, "sync validation failed")
	}

	return
}

// Spec defines the complete API specification with endpoints and authentication
type Spec struct {
	Name             string           `yaml:"name" json:"name"`
	Description      string           `yaml:"description" json:"description"`
	Queues           []string         `yaml:"queues" json:"queues"`
	Defaults         Endpoint         `yaml:"defaults" json:"defaults"`
	Authentication   Authentication   `yaml:"authentication" json:"authentication"`
	EndpointMap      EndpointMap      `yaml:"endpoints" json:"endpoints"`
	DynamicEndpoints DynamicEndpoints `yaml:"dynamic_endpoints" json:"dynamic_endpoints"`

	originalMap      map[string]any
	endpointsOrdered []string
	rendered         bool
}

func (s *Spec) IsDynamic() bool {
	return len(s.DynamicEndpoints) > 0
}

// validateQueues checks that all queues used by endpoints are declared at the Spec level
func (s *Spec) validateQueues(endpointMap EndpointMap) error {
	// Create a set of declared queues for fast lookup
	declaredQueues := make(map[string]bool)
	for _, queue := range s.Queues {
		declaredQueues[queue] = true
	}

	usedQueues := make(map[string][]string) // map[queueName][]endpointNames

	// Check all endpoints for queue usage
	for endpointName, endpoint := range endpointMap {
		// Check if endpoint iterates over a queue
		if iterateOver, ok := endpoint.Iterate.Over.(string); ok {
			if strings.HasPrefix(iterateOver, "queue.") {
				queueName := strings.TrimPrefix(iterateOver, "queue.")
				usedQueues[queueName] = append(usedQueues[queueName], endpointName)
			}
		}

		// Check if endpoint produces to a queue via processors
		for _, processor := range endpoint.Response.Processors {
			if strings.HasPrefix(processor.Output, "queue.") {
				queueName := strings.TrimPrefix(processor.Output, "queue.")
				usedQueues[queueName] = append(usedQueues[queueName], endpointName)
			}
		}
	}

	// Validate that all used queues are declared
	var undeclaredQueues []string
	for queueName, endpointNames := range usedQueues {
		if !declaredQueues[queueName] {
			undeclaredQueues = append(undeclaredQueues, g.F("queue '%s' used by endpoint(s): %s", queueName, strings.Join(endpointNames, ", ")))
		}
	}

	if len(undeclaredQueues) > 0 {
		sort.Strings(undeclaredQueues)
		return g.Error("undeclared queue(s) found:\n  - %s\n\nPlease declare them in the 'queues' section at the spec level", strings.Join(undeclaredQueues, "\n  - "))
	}

	return nil
}

// validateSync checks that all sync keys have corresponding processors that write to state
func (s *Spec) validateSync(endpointMap EndpointMap) error {
	var validationErrors []string

	// Check all endpoints for sync usage
	for endpointName, endpoint := range endpointMap {
		if len(endpoint.Sync) == 0 {
			continue
		}

		// Build a set of state keys written by processors
		stateKeysWritten := make(map[string]bool)
		for _, processor := range endpoint.Response.Processors {
			if strings.HasPrefix(processor.Output, "state.") {
				stateKey := strings.TrimPrefix(processor.Output, "state.")
				stateKeysWritten[stateKey] = true
			}
		}

		// Check that each sync key has a corresponding processor
		var missingSyncKeys []string
		for _, syncKey := range endpoint.Sync {
			if !stateKeysWritten[syncKey] {
				missingSyncKeys = append(missingSyncKeys, syncKey)
			}
		}

		if len(missingSyncKeys) > 0 {
			sort.Strings(missingSyncKeys)
			validationErrors = append(validationErrors,
				g.F("endpoint '%s' declares sync key(s) [%s] but has no processor writing to state.%s",
					endpointName,
					strings.Join(missingSyncKeys, ", "),
					strings.Join(missingSyncKeys, ", state.")))
		}
	}

	if len(validationErrors) > 0 {
		sort.Strings(validationErrors)
		return g.Error("sync validation failed:\n  - %s", strings.Join(validationErrors, "\n  - "))
	}

	return nil
}

type DynamicEndpoints []DynamicEndpoint

type DynamicEndpoint struct {
	Setup    Sequence `yaml:"setup" json:"setup"`
	Iterate  string   `yaml:"iterate" json:"iterate"`
	Into     string   `yaml:"into" json:"into"`
	Endpoint Endpoint `yaml:"endpoint" json:"endpoint"`
}

type Authentication map[string]any

func (a Authentication) Type() AuthType {
	if a == nil {
		return AuthTypeNone
	}
	authType := AuthType(cast.ToString(a["type"]))

	// If no type specified but headers are provided, default to static
	if authType == AuthTypeNone {
		if _, hasHeaders := a["headers"]; hasHeaders {
			return AuthTypeStatic
		}
	}

	return authType
}

func (a Authentication) Expires() int {
	if a == nil {
		return 0
	}
	return cast.ToInt(a["expires"])
}

// Authentication defines how to authenticate with the API
type Authentication0 struct {
	Type AuthType `yaml:"type" json:"type"`

	// when set, re-auth after number of seconds
	Expires int `yaml:"expires" json:"expires,omitempty"`

	// custom authentication workflow
	Sequence Sequence `yaml:"sequence" json:"sequence,omitempty"`

	// Basic Auth
	Username string `yaml:"username,omitempty" json:"username,omitempty"`
	Password string `yaml:"password,omitempty" json:"password,omitempty"`

	// OAuth
	Flow              OAuthFlow `yaml:"flow,omitempty" json:"flow,omitempty"`
	AuthenticationURL string    `yaml:"authentication_url,omitempty" json:"authentication_url,omitempty"` // Token endpoint
	AuthorizationURL  string    `yaml:"authorization_url,omitempty" json:"authorization_url,omitempty"`   // Authorization endpoint (for auth code flow)
	ClientID          string    `yaml:"client_id,omitempty" json:"client_id,omitempty"`
	ClientSecret      string    `yaml:"client_secret,omitempty" json:"client_secret,omitempty"`
	Token             string    `yaml:"token,omitempty" json:"token,omitempty"`
	Scopes            []string  `yaml:"scopes,omitempty" json:"scopes,omitempty"`
	RedirectURI       string    `yaml:"redirect_uri,omitempty" json:"redirect_uri,omitempty"`
	RefreshToken      string    `yaml:"refresh_token,omitempty" json:"refresh_token,omitempty"`
	RefreshOnExpire   bool      `yaml:"refresh_on_expire,omitempty" json:"refresh_on_expire,omitempty"`

	// AWS
	AwsService         string `yaml:"aws_service,omitempty" json:"aws_service,omitempty"`
	AwsAccessKeyID     string `yaml:"aws_access_key_id,omitempty" json:"aws_access_key_id,omitempty"`
	AwsSecretAccessKey string `yaml:"aws_secret_access_key,omitempty" json:"aws_secret_access_key,omitempty"`
	AwsSessionToken    string `yaml:"aws_session_token,omitempty" json:"aws_session_token,omitempty"`
	AwsRegion          string `yaml:"aws_region,omitempty" json:"aws_region,omitempty"`
	AwsProfile         string `yaml:"aws_profile,omitempty" json:"aws_profile,omitempty"`
}

type AuthType string

const (
	AuthTypeNone     AuthType = ""
	AuthTypeStatic   AuthType = "static"
	AuthTypeSequence AuthType = "sequence"
	AuthTypeBasic    AuthType = "basic"
	AuthTypeOAuth2   AuthType = "oauth2"
	AuthTypeAWSSigV4 AuthType = "aws-sigv4"
	AuthTypeHMAC     AuthType = "hmac"
	AuthTypeMTls     AuthType = "mtls"
)

func (at AuthType) String() string {
	return string(at)
}

type OAuthFlow string

const (
	OAuthFlowClientCredentials OAuthFlow = "client_credentials"
	OAuthFlowAuthorizationCode OAuthFlow = "authorization_code"
	OAuthFlowDeviceCode        OAuthFlow = "device_code"
)

// Sequence is many calls (perfect for async jobs, custom auth)
type Sequence []Call

type Call struct {
	If             string          `yaml:"if" json:"if"`
	Request        Request         `yaml:"request" json:"request"`
	Pagination     Pagination      `yaml:"pagination" json:"pagination"`
	Response       Response        `yaml:"response" json:"response"`
	Authentication *Authentication `yaml:"authentication,omitempty" json:"authentication,omitempty"`
	Iterate        string          `yaml:"iterate" json:"iterate"`
	Into           string          `yaml:"into" json:"into"`
}

// Endpoints is a collection of API endpoints
type EndpointMap map[string]Endpoint
type Endpoints []Endpoint

// Endpoint is the top-level configuration structure
type Endpoint struct {
	Name        string     `yaml:"name" json:"name"`
	Description string     `yaml:"description" json:"description,omitempty"`
	Docs        string     `yaml:"docs" json:"docs,omitempty"`
	Disabled    bool       `yaml:"disabled" json:"disabled"`
	Dynamic     bool       `yaml:"dynamic,omitempty" json:"dynamic,omitempty"` // is generated
	State       StateMap   `yaml:"state" json:"state"`
	Sync        []string   `yaml:"sync" json:"sync,omitempty"`
	Request     Request    `yaml:"request" json:"request"`
	Pagination  Pagination `yaml:"pagination" json:"pagination"`
	Response    Response   `yaml:"response" json:"response"`
	Iterate     Iterate    `yaml:"iterate" json:"iterate,omitempty"` // state expression to use to loop
	Setup       Sequence   `yaml:"setup" json:"setup,omitempty"`
	Teardown    Sequence   `yaml:"teardown" json:"teardown,omitempty"`
	DependsOn   []string   `yaml:"depends_on" json:"depends_on"` // upstream endpoints
	Overrides   any        `yaml:"overrides" json:"overrides"`   // stream overrides

	Authentication *Authentication `yaml:"authentication,omitempty" json:"authentication,omitempty"`

	// whether we should stop the endpoint process.
	// inflight iterations will continue, no new iterations created
	stopIters    bool
	conn         *APIConnection
	client       http.Client
	eval         *goval.Evaluator
	backoffUntil time.Time // backoff expiration time - all goroutines can check this
	totalRecords int
	totalReqs    int
	contextMap   StateMap // extra contextual properties
	syncMap      StateMap // values to sync
	context      *g.Context
	uniqueKeys   map[string]struct{} // for PrimaryKey deduplication
	bloomFilter  *bloom.BloomFilter
	aggregate    AggregateState
	originalMap  map[string]any
	auth         APIStateAuth
}

func (ep *Endpoint) SetStateVal(key string, val any) {
	ep.context.Lock()
	defer ep.context.Unlock()
	if ep.State == nil {
		ep.State = make(StateMap)
	}
	ep.State[key] = val
}

func (ep *Endpoint) setContextMap(sCfg APIStreamConfig) {
	contextMap := g.M(
		"mode", sCfg.Mode,
		"store", ep.conn.State.Store,
		"limit", sCfg.Limit,
	)

	// set backfill params
	if rangeParts := strings.Split(sCfg.Range, ","); sCfg.Range != "" {
		contextMap["range_start"] = rangeParts[0]
		if len(rangeParts) > 1 {
			contextMap["range_end"] = lo.Ternary[any](rangeParts[1] == "", nil, rangeParts[1])
		}
	}

	ep.contextMap = contextMap
}

func (ep *Endpoint) renderString(val any, extraMaps ...map[string]any) (newVal string, err error) {

	extra := g.M()
	if len(extraMaps) > 0 {
		extra = extraMaps[0]
	}

	// Copy endpoint sync and context maps (lock ep.context separately to avoid nested locks)
	syncCopy := make(map[string]any)
	contextCopy := make(map[string]any)

	ep.context.Lock()
	if ep.syncMap != nil {
		for k, v := range ep.syncMap {
			syncCopy[k] = v
		}
	}
	if ep.contextMap != nil {
		for k, v := range ep.contextMap {
			contextCopy[k] = v
		}
	}
	ep.context.Unlock()

	extra["sync"] = syncCopy
	extra["context"] = contextCopy

	return ep.conn.renderString(val, extra)
}

func (ep *Endpoint) renderAny(val any, extraMaps ...map[string]any) (newVal any, err error) {
	extra := g.M()
	if len(extraMaps) > 0 {
		extra = extraMaps[0]
	}

	// Copy endpoint sync and context maps (lock ep.context separately to avoid nested locks)
	syncCopy := make(map[string]any)
	contextCopy := make(map[string]any)

	ep.context.Lock()
	if ep.syncMap != nil {
		for k, v := range ep.syncMap {
			syncCopy[k] = v
		}
	}
	if ep.contextMap != nil {
		for k, v := range ep.contextMap {
			contextCopy[k] = v
		}
	}
	ep.context.Unlock()

	extra["sync"] = syncCopy
	extra["context"] = contextCopy

	return ep.conn.renderAny(val, extra)
}

func (ep *Endpoint) Evaluate(expr string, state map[string]interface{}) (result any, err error) {
	if state == nil {
		state = g.M()
	}

	// fill in state for missing vars
	state = ep.conn.evaluator.FillMissingKeys(state, ep.conn.evaluator.ExtractVars(expr))

	return ep.eval.Evaluate(expr, state, iop.GlobalFunctionMap)
}

// Names returns names in alphabetical order
func (eps Endpoints) Names() (names []string) {
	for _, e := range eps {
		names = append(names, e.Name)
	}

	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})

	return names
}

// HasUpstreams returns all upstream endpoint names that the specified endpoint depends on.
// Returns empty slice if the endpoint doesn't exist or has no dependencies.
func (eps Endpoints) HasUpstreams(endpointName string) (upstreams []string) {
	upstreams = make([]string, 0)

	// Find the target endpoint
	var targetEndpoint *Endpoint
	for _, ep := range eps {
		if ep.Name == endpointName {
			targetEndpoint = &ep
			break
		}
	}

	if targetEndpoint == nil {
		return upstreams // Return empty slice if endpoint not found
	}

	// Check if endpoint iterates over a queue
	if iterateOver, ok := targetEndpoint.Iterate.Over.(string); ok {
		if strings.HasPrefix(iterateOver, "queue.") {
			queueName := strings.TrimPrefix(iterateOver, "queue.")

			// Find endpoints that produce to this queue
			var producers []string
			for _, otherEp := range eps {
				for _, processor := range otherEp.Response.Processors {
					if strings.HasPrefix(processor.Output, "queue.") {
						producerQueueName := strings.TrimPrefix(processor.Output, "queue.")
						if producerQueueName == queueName && otherEp.Name != targetEndpoint.Name {
							producers = append(producers, otherEp.Name)
						}
					}
				}
			}

			if len(producers) > 0 {
				// Remove duplicates and sort
				sort.Strings(producers)
				// Remove duplicates after sorting
				uniqueProducers := make([]string, 0, len(producers))
				for i, producer := range producers {
					if i == 0 || producer != producers[i-1] {
						uniqueProducers = append(uniqueProducers, producer)
					}
				}
				upstreams = append(upstreams, uniqueProducers...)
			}
		}
	}

	return upstreams
}

func (eps Endpoints) Sort() {
	// First sort alphabetically
	sort.Slice(eps, func(i, j int) bool {
		return eps[i].Name < eps[j].Name
	})

	// Then reorder based on dependencies
	deps := eps.buildDependencyMap()
	sorted := eps.topologicalSort(deps)

	// Reorder the slice based on topological sort
	sortedMap := make(map[string]int)
	for i, name := range sorted {
		sortedMap[name] = i
	}

	sort.Slice(eps, func(i, j int) bool {
		return sortedMap[eps[i].Name] < sortedMap[eps[j].Name]
	})
}

// buildDependencyMap creates a map of endpoint dependencies based on queue usage
func (eps Endpoints) buildDependencyMap() map[string][]string {
	dependencies := make(map[string][]string)

	// For each endpoint, get its upstream dependencies
	for _, ep := range eps {
		dependencies[ep.Name] = eps.HasUpstreams(ep.Name)
	}

	return dependencies
}

// topologicalSort performs a topological sort on endpoints based on dependencies
func (eps Endpoints) topologicalSort(dependencies map[string][]string) []string {
	// Build reverse dependency graph (what depends on me)
	reverseDeps := make(map[string][]string)
	inDegree := make(map[string]int)

	// Initialize all endpoints
	for _, ep := range eps {
		inDegree[ep.Name] = 0
		reverseDeps[ep.Name] = []string{}
	}

	// Build reverse dependencies and count in-degrees
	for endpoint, deps := range dependencies {
		inDegree[endpoint] = len(deps)
		for _, dep := range deps {
			reverseDeps[dep] = append(reverseDeps[dep], endpoint)
		}
	}

	// Queue for endpoints with no dependencies
	var queue []string
	for name, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, name)
		}
	}

	// Sort the initial queue alphabetically for consistent ordering
	sort.Strings(queue)

	var result []string

	for len(queue) > 0 {
		// Process the first endpoint in queue
		current := queue[0]
		queue = queue[1:]

		result = append(result, current)

		// Reduce in-degree for endpoints that depend on current
		var newReady []string
		for _, dependent := range reverseDeps[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				newReady = append(newReady, dependent)
			}
		}

		// Sort new ready endpoints alphabetically before adding to queue
		sort.Strings(newReady)
		queue = append(queue, newReady...)
	}

	// Check for cycles (if we haven't processed all endpoints)
	if len(result) != len(eps) {
		// Add any remaining endpoints (shouldn't happen with valid DAG)
		processed := make(map[string]bool)
		for _, name := range result {
			processed[name] = true
		}
		for _, ep := range eps {
			if !processed[ep.Name] {
				result = append(result, ep.Name)
			}
		}
	}

	return result
}

// DAG returns groups of interdependent endpoints
func (eps Endpoints) DAG() [][]string {
	dependencies := eps.buildDependencyMap()

	// Build reverse dependencies (who depends on me)
	reverseDeps := make(map[string][]string)
	for endpoint, deps := range dependencies {
		for _, dep := range deps {
			reverseDeps[dep] = append(reverseDeps[dep], endpoint)
		}
	}

	// Find connected components
	visited := make(map[string]bool)
	var groups [][]string

	var dfs func(endpoint string, group *[]string)
	dfs = func(endpoint string, group *[]string) {
		if visited[endpoint] {
			return
		}
		visited[endpoint] = true
		*group = append(*group, endpoint)

		// Visit upstream dependencies
		for _, dep := range dependencies[endpoint] {
			dfs(dep, group)
		}

		// Visit downstream dependencies
		for _, dep := range reverseDeps[endpoint] {
			dfs(dep, group)
		}
	}

	// Process each endpoint
	for _, ep := range eps {
		if !visited[ep.Name] {
			var group []string
			dfs(ep.Name, &group)

			// Sort group for consistent output
			sort.Strings(group)
			groups = append(groups, group)
		}
	}

	// Sort groups by their first element for consistent ordering
	sort.Slice(groups, func(i, j int) bool {
		if len(groups[i]) == 0 || len(groups[j]) == 0 {
			return len(groups[i]) > len(groups[j])
		}
		return groups[i][0] < groups[j][0]
	})

	return groups
}

// setup executes the setup sequence for an endpoint
func (ep *Endpoint) setup() (err error) {
	if len(ep.Setup) == 0 {
		return nil
	}

	g.Debug("running endpoint setup sequence (%d calls)", len(ep.Setup))

	baseEndpoint := &Endpoint{
		Name:       g.F("%s.setup", ep.Name),
		context:    g.NewContext(ep.context.Ctx),
		conn:       ep.conn,
		State:      g.M(),
		contextMap: ep.contextMap,
		aggregate:  ep.aggregate,
		auth:       APIStateAuth{Mutex: &sync.Mutex{}},
	}

	// only copy over headers if not exists
	if baseEndpoint.Request.Headers == nil {
		baseEndpoint.Request.Headers = map[string]any{}
	}
	for k, v := range ep.Request.Headers {
		if _, exists := baseEndpoint.Request.Headers[k]; !exists {
			baseEndpoint.Request.Headers[k] = v
		}
	}

	// set in context original sequence array
	array, _ := jmespath.Search("setup", ep.originalMap)
	baseEndpoint.context.Map.Set("sequence_array", array)

	// copy over state from endpoint with proper locking
	ep.context.Lock()
	if ep.State != nil {
		maps.Copy(baseEndpoint.State, ep.State)
	}
	ep.context.Unlock()

	if err := runSequence(ep.Setup, baseEndpoint); err != nil {
		return g.Error(err, "endpoint setup failed")
	}

	// sync state back with proper locking
	ep.context.Lock()
	maps.Copy(ep.State, baseEndpoint.State)
	ep.stopIters = baseEndpoint.stopIters
	ep.aggregate = baseEndpoint.aggregate
	ep.context.Unlock()

	g.Debug("endpoint setup completed successfully")
	return nil
}

// teardown executes the teardown sequence for an endpoint
func (ep *Endpoint) teardown() (err error) {
	if len(ep.Teardown) == 0 {
		return nil
	}

	g.Debug("running endpoint teardown sequence (%d calls)", len(ep.Teardown))

	baseEndpoint := &Endpoint{
		Name:      g.F("%s.teardown", ep.Name),
		context:   g.NewContext(ep.context.Ctx),
		conn:      ep.conn,
		State:     g.M(),
		aggregate: ep.aggregate,
		auth:      APIStateAuth{Mutex: &sync.Mutex{}},
	}

	// set in context original sequence array
	array, _ := jmespath.Search("teardown", ep.originalMap)
	baseEndpoint.context.Map.Set("sequence_array", array)

	// only copy over headers if not exists
	if baseEndpoint.Request.Headers == nil {
		baseEndpoint.Request.Headers = map[string]any{}
	}
	for k, v := range ep.Request.Headers {
		if _, exists := baseEndpoint.Request.Headers[k]; !exists {
			baseEndpoint.Request.Headers[k] = v
		}
	}

	// copy over state from endpoint with proper locking
	ep.context.Lock()
	if ep.State != nil {
		maps.Copy(baseEndpoint.State, ep.State)
	}
	ep.context.Unlock()

	if err := runSequence(ep.Teardown, baseEndpoint); err != nil {
		return g.Error(err, "endpoint teardown failed")
	}

	// sync state back with proper locking
	ep.context.Lock()
	maps.Copy(ep.State, baseEndpoint.State)
	ep.aggregate = baseEndpoint.aggregate
	ep.context.Unlock()

	g.Debug("endpoint teardown completed successfully")
	return nil
}

func (iter *Iteration) DetermineStateRenderOrder() (order []string, err error) {
	iter.context.Lock()
	remaining := lo.Keys(iter.state)
	iter.context.Unlock()

	processing := map[string]bool{} // track variables being processed in current chain

	addAndRemove := func(key string) {
		if !g.In(key, order...) {
			order = append(order, key)
			remaining = lo.Filter(remaining, func(k string, i int) bool {
				return k != key // remove from remaining
			})
		}
	}

	var processVar func(key string) error
	processVar = func(key string) error {
		// Check for circular dependency
		if processing[key] {
			return g.Error("circular dependency detected for state variable: %s", key)
		}

		// Skip if already processed
		if g.In(key, order...) {
			return nil
		}

		// Mark as being processed
		processing[key] = true
		defer func() { processing[key] = false }()

		iter.context.Lock()
		expr := cast.ToString(iter.state[key])
		iter.context.Unlock()

		// Use FindMatches instead of bracketRegex to properly handle nested brackets and quoted strings
		matches, matchErr := iter.endpoint.conn.evaluator.FindMatches(expr)
		if matchErr != nil {
			g.Trace("could not find matches for state variable %s: %v", key, matchErr)
		}
		if len(matches) > 0 {
			for _, match := range matches {
				varsReferenced := iter.endpoint.conn.evaluator.ExtractVars(match)
				for _, varReferenced := range varsReferenced {
					if strings.HasPrefix(varReferenced, "state.") {
						refKey := strings.TrimPrefix(varReferenced, "state.")
						// Process dependency first
						if err := processVar(refKey); err != nil {
							return g.Error(err, "while processing dependency chain for %s", key)
						}
					}
				}
			}
		}
		addAndRemove(key)
		return nil
	}

	// Process all remaining variables
	for len(remaining) > 0 {
		key := remaining[0] // take first remaining key
		if err := processVar(key); err != nil {
			return nil, g.Error(err, "error determining render order")
		}
	}

	return
}

// Iterate is for configuring looping values for requests
type Iterate struct {
	Over        any    `yaml:"over" json:"iterate,omitempty"` // expression
	Into        string `yaml:"into" json:"into,omitempty"`    // state variable
	Concurrency int    `yaml:"concurrency" json:"concurrency,omitempty"`
	iterations  chan *Iteration
}

type Iteration struct {
	state     StateMap // each iteration has its own state
	id        string   // iteration ID
	sequence  int      // paginated request number
	field     string   // state field
	value     any      // state value
	stop      bool     // whether to stop the iteration
	context   *g.Context
	endpoint  *Endpoint
	requestWg sync.WaitGroup // request wait group
}

func (iter *Iteration) Debug(text string, args ...any) {
	if iter.id != "" {
		text = env.DarkGrayString(iter.id) + " " + text
	}
	g.Debug(text, args...)
}

func (iter *Iteration) renderString(val any, req ...*SingleRequest) (newVal string, err error) {

	extra := g.M()
	if len(req) > 0 {
		extra = req[0].Map()
	}

	// Copy iteration state (lock iter.context separately)
	iter.context.Lock()
	stateCopy := make(map[string]any)
	for k, v := range iter.state {
		stateCopy[k] = v
	}
	iter.context.Unlock()

	extra["state"] = stateCopy

	return iter.endpoint.renderString(val, extra)
}

func (iter *Iteration) renderAny(val any, req ...*SingleRequest) (newVal any, err error) {
	extra := g.M()
	if len(req) > 0 {
		extra = req[0].Map()
	}

	// Copy iteration state (lock iter.context separately)
	iter.context.Lock()
	stateCopy := make(map[string]any)
	for k, v := range iter.state {
		stateCopy[k] = v
	}
	iter.context.Unlock()

	extra["state"] = stateCopy

	return iter.endpoint.renderAny(val, extra)
}

// StateMap stores the current state of an endpoint's execution
type StateMap map[string]any

type HTTPMethod string

const (
	MethodGet     HTTPMethod = "GET"
	MethodHead    HTTPMethod = "HEAD"
	MethodPost    HTTPMethod = "POST"
	MethodPut     HTTPMethod = "PUT"
	MethodPatch   HTTPMethod = "PATCH"
	MethodDelete  HTTPMethod = "DELETE"
	MethodConnect HTTPMethod = "CONNECT"
	MethodOptions HTTPMethod = "OPTIONS"
	MethodTrace   HTTPMethod = "TRACE"
)

// Request defines how to construct an HTTP request to the API
type Request struct {
	URL         string         `yaml:"url" json:"url,omitempty"`
	Timeout     int            `yaml:"timeout" json:"timeout,omitempty"`
	Method      HTTPMethod     `yaml:"method" json:"method,omitempty"`
	Headers     map[string]any `yaml:"headers" json:"headers,omitempty"`
	Parameters  map[string]any `yaml:"parameters" json:"parameters,omitempty"`
	Payload     any            `yaml:"payload" json:"payload,omitempty"`
	Rate        float64        `yaml:"rate" json:"rate,omitempty"`               // maximum request per second
	Concurrency int            `yaml:"concurrency" json:"concurrency,omitempty"` // maximum concurrent requests
}

// Pagination configures how to navigate through multiple pages of API results
type Pagination struct {
	NextState     map[string]any `yaml:"next_state" json:"next_state,omitempty"`
	StopCondition string         `yaml:"stop_condition" json:"stop_condition,omitempty"`
}

// Response defines how to process the API response and extract records
type Response struct {
	Format     dbio.FileType `yaml:"format" json:"format,omitempty"` // force response format
	Records    Records       `yaml:"records" json:"records"`
	Processors []Processor   `yaml:"processors" json:"processors,omitempty"`
	Rules      []Rule        `yaml:"rules" json:"rules,omitempty"`
}

// Records configures how to extract and process data records from a response
type Records struct {
	JmesPath   string   `yaml:"jmespath" json:"jmespath,omitempty"` // for json or xml
	PrimaryKey []string `yaml:"primary_key" json:"primary_key,omitempty"`
	UpdateKey  string   `yaml:"update_key" json:"update_key,omitempty"`
	Limit      int      `yaml:"limit" json:"limit,omitempty"`   // to limit the records, useful for testing
	Casing     string   `yaml:"casing" json:"casing,omitempty"` // "snake" or "camel"
	Select     []string `yaml:"select" json:"select,omitempty"` // include/exclude/rename

	DuplicateTolerance string `yaml:"duplicate_tolerance" json:"duplicate_tolerance,omitempty"`
}

type AggregationType string

const (
	AggregationTypeNone    AggregationType = ""        // No aggregation, apply transformation at record level
	AggregationTypeMaximum AggregationType = "maximum" // Keep the maximum value across records
	AggregationTypeMinimum AggregationType = "minimum" // Keep the minimum value across records
	AggregationTypeCollect AggregationType = "collect" // Collect all values into an array
	AggregationTypeFirst   AggregationType = "first"   // Keep only the first encountered value
	AggregationTypeLast    AggregationType = "last"    // Keep only the last encountered value
)

var AggregationTypes = []AggregationType{
	AggregationTypeNone, AggregationTypeMaximum,
	AggregationTypeMinimum, AggregationTypeCollect,
	AggregationTypeFirst, AggregationTypeLast,
}

// Processor represents a way to process data
// without aggregation, represents a transformation applied at record level
// with aggregation to reduce/aggregate record data, and save into the state
type Processor struct {
	Aggregation AggregationType `yaml:"aggregation" json:"aggregation"`
	Expression  string          `yaml:"expression" json:"expression"`
	Output      string          `yaml:"output" json:"output"`
	If          string          `yaml:"if" json:"if,omitempty"` // if we should evaluate processor
}

type RuleType string

const (
	RuleTypeRetry    RuleType = "retry"    // Retry the request up to MaxAttempts times
	RuleTypeContinue RuleType = "continue" // Continue processing responses and rules
	RuleTypeStop     RuleType = "stop"     // Stop processing requests for this endpoint
	RuleTypeBreak    RuleType = "break"    // Stop processing requests for this iteration
	RuleTypeFail     RuleType = "fail"     // Stop processing and return an error
	RuleTypeSkip     RuleType = "skip"     // Skip records from response and continue
)

type BackoffType string

const (
	BackoffTypeNone        BackoffType = ""            // No delay between retries
	BackoffTypeConstant    BackoffType = "constant"    // Fixed delay between retries
	BackoffTypeLinear      BackoffType = "linear"      // Delay increases linearly with each attempt
	BackoffTypeExponential BackoffType = "exponential" // Delay increases exponentially (common pattern)
	BackoffTypeJitter      BackoffType = "jitter"      // Exponential backoff with randomization to avoid thundering herd
)

// Rule represents a response rule
type Rule struct {
	Action      RuleType    `yaml:"action" json:"action"`
	Condition   string      `yaml:"condition" json:"condition"` // an expression
	MaxAttempts int         `yaml:"max_attempts" json:"max_attempts"`
	Backoff     BackoffType `yaml:"backoff" json:"backoff"`
	BackoffBase int         `yaml:"backoff_base" json:"backoff_base"` // base duration, number of seconds. default is 1
	Message     string      `yaml:"message" json:"message"`
}

// SingleRequest represents a single HTTP request/response cycle
type SingleRequest struct {
	Request    *RequestState  `yaml:"request" json:"request"`
	Response   *ResponseState `yaml:"response" json:"response"`
	httpReq    *http.Request  `yaml:"-" json:"-"`
	httpRespWg sync.WaitGroup `yaml:"-" json:"-"`
	id         string         `yaml:"-" json:"-"`
	timestamp  int64          `yaml:"-" json:"-"`
	iter       *Iteration     `yaml:"-" json:"-"` // the iteration that the req belongs to
	state      StateMap       `yaml:"-" json:"-"` // copy of iteration state for request (prevents mutation)
	endpoint   *Endpoint      `yaml:"-" json:"-"`
}

func NewSingleRequest(iter *Iteration) *SingleRequest {
	iter.sequence++
	iter.endpoint.totalReqs++

	id := g.F("r.%04d.%s", iter.endpoint.totalReqs, g.RandString(g.AlphaRunesLower, 3))
	if iter.field != "" {
		id = g.F("%s.r%03d.%s", iter.id, iter.sequence, g.RandString(g.AlphaRunesLower, 3))
	}

	// set state to prevent mutation by next_state
	state := StateMap{}
	maps.Copy(state, iter.state)

	return &SingleRequest{
		id:        id,
		timestamp: time.Now().UnixMilli(),
		endpoint:  iter.endpoint,
		iter:      iter,
		state:     state,
	}
}

func (lrs *SingleRequest) Records() []map[string]any {
	if lrs.Response == nil || len(lrs.Response.Records) == 0 {
		return make([]map[string]any, 0)
	}
	return lrs.Response.Records
}

func (lrs *SingleRequest) Debug(text string, args ...any) {
	text = env.DarkGrayString(lrs.id) + " " + text
	g.Debug(text, args...)
}

func (lrs *SingleRequest) Trace(text string, args ...any) {
	text = env.DarkGrayString(lrs.id) + " " + text
	g.Trace(text, args...)
}

func (lrs *SingleRequest) Map() map[string]any {
	vars := g.M()
	if err := g.JSONConvert(lrs, &vars); err != nil {
		g.LogError(g.Error(err, "error converting single-request-state"))
	}
	return vars
}

// RequestState captures the state of the HTTP request for reference and debugging
type RequestState struct {
	Method   string         `yaml:"method" json:"method"`
	URL      string         `yaml:"url" json:"url"`
	Headers  map[string]any `yaml:"headers" json:"headers"`
	Payload  any            `yaml:"payload" json:"payload"`
	Attempts int            `yaml:"attempts" json:"attempts"`
}

// ResponseState captures the state of the HTTP response for reference and debugging
type ResponseState struct {
	Status  int              `yaml:"status" json:"status"`
	Headers map[string]any   `yaml:"headers" json:"headers"`
	Text    string           `yaml:"text" json:"text"`
	JSON    any              `yaml:"json" json:"json"`
	Records []map[string]any `yaml:"records" json:"records"`
}

// AggregateState stores aggregated values during response processing
type AggregateState struct {
	value map[string]any   // map of key to single value
	array map[string][]any // map of key to array
}

func NewAggregateState() AggregateState {
	return AggregateState{value: g.M(), array: map[string][]any{}}
}
