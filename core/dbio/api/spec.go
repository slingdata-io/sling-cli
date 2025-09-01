package api

import (
	"maps"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/flarco/g"
	"github.com/maja42/goval"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
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
		endpoint.Name = endpointName
		if endpoint.State == nil {
			endpoint.State = g.M() // set default state
		}
		spec.EndpointMap[endpointName] = endpoint
	}

	// so that JMESPath works
	g.Unmarshal(g.Marshal(spec.originalMap), &spec.originalMap)

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
}

func (s *Spec) IsDynamic() bool {
	return len(s.DynamicEndpoints) > 0
}

type DynamicEndpoints []DynamicEndpoint

type DynamicEndpoint struct {
	Setup    Sequence `yaml:"setup" json:"setup"`
	Iterate  string   `yaml:"iterate" json:"iterate"`
	Into     string   `yaml:"into" json:"into"`
	Endpoint Endpoint `yaml:"endpoint" json:"endpoint"`
}

// Authentication defines how to authenticate with the API
type Authentication struct {
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
	AuthenticationURL string    `yaml:"authentication_url,omitempty" json:"authentication_url,omitempty"`
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
	AuthTypeSequence AuthType = "sequence"
	AuthTypeBasic    AuthType = "basic"
	AuthTypeOAuth2   AuthType = "oauth2"
	AuthTypeAWSSigV4 AuthType = "aws-sigv4"
)

type OAuthFlow string

const (
	OAuthFlowClientCredentials OAuthFlow = "client_credentials"
	OAuthFlowAuthorizationCode OAuthFlow = "authorization_code"
	OAuthFlowPassword          OAuthFlow = "password"
	OAuthFlowRefreshToken      OAuthFlow = "refresh_token"
)

// Sequence is many calls (perfect for async jobs, custom auth)
type Sequence []Call

type Call struct {
	If         string     `yaml:"if" json:"if"`
	Request    Request    `yaml:"request" json:"request"`
	Pagination Pagination `yaml:"pagination" json:"pagination"`
	Response   Response   `yaml:"response" json:"response"`
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
	State       StateMap   `yaml:"state" json:"state"`
	Sync        []string   `yaml:"sync" json:"sync,omitempty"`
	Request     Request    `yaml:"request" json:"request"`
	Pagination  Pagination `yaml:"pagination" json:"pagination"`
	Response    Response   `yaml:"response" json:"response"`
	Iterate     Iterate    `yaml:"iterate" json:"iterate,omitempty"` // state expression to use to loop
	Setup       Sequence   `yaml:"setup" json:"setup,omitempty"`
	Teardown    Sequence   `yaml:"teardown" json:"teardown,omitempty"`

	stop         bool // whether we should stop the endpoint process
	conn         *APIConnection
	client       http.Client
	eval         *goval.Evaluator
	backoffTimer *time.Timer
	totalRecords int
	totalReqs    int
	syncMap      StateMap // values to sync
	context      *g.Context
	uniqueKeys   map[string]struct{} // for PrimaryKey deduplication
	bloomFilter  *bloom.BloomFilter
}

func (ep *Endpoint) SetStateVal(key string, val any) {
	ep.context.Lock()
	defer ep.context.Unlock()
	if ep.State == nil {
		ep.State = make(StateMap)
	}
	ep.State[key] = val
}

func (eps Endpoints) Sort() {
	sort.Slice(eps, func(i, j int) bool {
		return eps[i].Name < eps[j].Name
	})
}

// setup executes the setup sequence for an endpoint
func (ep *Endpoint) setup() (err error) {
	if len(ep.Setup) == 0 {
		return nil
	}

	g.Debug("running endpoint setup sequence (%d calls)", len(ep.Setup))

	baseEndpoint := &Endpoint{
		context: g.NewContext(ep.context.Ctx),
		conn:    ep.conn,
		State:   g.M(),
	}

	// only copy over headers
	baseEndpoint.Request.Headers = ep.Request.Headers

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
		context: g.NewContext(ep.context.Ctx),
		conn:    ep.conn,
		State:   g.M(),
	}

	// only copy over headers
	baseEndpoint.Request.Headers = ep.Request.Headers

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

		matches := bracketRegex.FindAllStringSubmatch(expr, -1)
		if len(matches) > 0 {
			for _, match := range matches {
				varsReferenced := iter.endpoint.conn.evaluator.ExtractVars(match[1])
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
	If          string `yaml:"id" json:"id,omitempty"`        // if we should iterate
	Concurrency int    `yaml:"concurrency" json:"concurrency,omitempty"`
	iterations  chan *Iteration
}

type Iteration struct {
	state    StateMap // each iteration has its own state
	id       int      // iteration ID
	sequence int      // paginated request number
	field    string   // state field
	value    any      // state value
	stop     bool     // whether to stop the iteration
	context  *g.Context
	endpoint *Endpoint
}

func (iter *Iteration) Debug(text string, args ...any) {
	if iter.field != "" {
		id := g.F("i%02d", iter.id)
		text = env.DarkGrayString(id) + " " + text
	}
	g.Debug(text, args...)
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
	Limit      int      `yaml:"limit" json:"limit,omitempty"` // to limit the records, useful for testing

	DuplicateTolerance string `yaml:"duplicate_tolerance" json:"duplicate_tolerance,omitempty"`
}

type AggregationType string

const (
	AggregationTypeNone    AggregationType = ""        // No aggregation, apply transformation at record level
	AggregationTypeMaximum AggregationType = "maximum" // Keep the maximum value across records
	AggregationTypeMinimum AggregationType = "minimum" // Keep the minimum value across records
	AggregationTypeFlatten AggregationType = "flatten" // Collect all values into an array
	AggregationTypeFirst   AggregationType = "first"   // Keep only the first encountered value
	AggregationTypeLast    AggregationType = "last"    // Keep only the last encountered value
)

var AggregationTypes = []AggregationType{
	AggregationTypeNone, AggregationTypeMaximum,
	AggregationTypeMinimum, AggregationTypeFlatten,
	AggregationTypeFirst, AggregationTypeLast,
}

// Processor represents a way to process data
// without aggregation, represents a transformation applied at record level
// with aggregation to reduce/aggregate record data, and save into the state
type Processor struct {
	Aggregation AggregationType `yaml:"aggregation" json:"aggregation"`
	Expression  string          `yaml:"expression" json:"expression"`
	Output      string          `yaml:"output" json:"output"`
}

type RuleType string

const (
	RuleTypeRetry    RuleType = "retry"    // Retry the request up to MaxAttempts times
	RuleTypeContinue RuleType = "continue" // Continue processing responses and rules
	RuleTypeStop     RuleType = "stop"     // Stop processing requests for this endpoint
	RuleTypeFail     RuleType = "fail"     // Stop processing and return an error
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
	Aggregate  AggregateState `yaml:"-" json:"-"`
	httpReq    *http.Request  `yaml:"-" json:"-"`
	httpRespWg sync.WaitGroup `yaml:"-" json:"-"`
	id         string         `yaml:"-" json:"-"`
	timestamp  int64          `yaml:"-" json:"-"`
	iter       *Iteration     `yaml:"-" json:"-"` // the iteration that the req belongs to
	endpoint   *Endpoint      `yaml:"-" json:"-"`
}

func NewSingleRequest(iter *Iteration) *SingleRequest {
	iter.sequence++
	iter.endpoint.totalReqs++

	id := g.F("r.%04d.%s", iter.endpoint.totalReqs, g.RandString(g.AlphaRunesLower, 3))
	if iter.field != "" {
		id = g.F("i%02d.r%03d.%s", iter.id, iter.sequence, g.RandString(g.AlphaRunesLower, 3))
	}

	return &SingleRequest{
		id:        id,
		timestamp: time.Now().UnixMilli(),
		endpoint:  iter.endpoint,
		iter:      iter,
	}
}

func (lrs *SingleRequest) Records() []any {
	if lrs.Response == nil || len(lrs.Response.Records) == 0 {
		return make([]any, 0)
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
	Status  int            `yaml:"status" json:"status"`
	Headers map[string]any `yaml:"headers" json:"headers"`
	Text    string         `yaml:"text" json:"text"`
	JSON    any            `yaml:"json" json:"json"`
	Records []any          `yaml:"records" json:"records"`
}

// AggregateState stores aggregated values during response processing
type AggregateState struct {
	value any
	array []any
}

func escapeErrVal(val string) string {
	return strings.ReplaceAll(val, `%`, `%`+`%`)
}
