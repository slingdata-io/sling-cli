package api

import (
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/maja42/goval"
	"github.com/slingdata-io/sling-cli/core/env"
	"gopkg.in/yaml.v2"
)

func LoadSpec(specBody string) (spec Spec, err error) {

	// load spec from body
	if err = yaml.Unmarshal([]byte(specBody), &spec); err != nil {
		return spec, g.Error(err, "error loading API spec")
	}

	// set endpoint index
	for i := range spec.Endpoints {
		spec.Endpoints[i].index = i
	}

	// set maps
	err = yaml.Unmarshal([]byte(specBody), &spec.OriginalMap)
	if err != nil {
		err = g.Error(err, "Error parsing yaml content")
		return
	}

	// so that JMESPath works
	g.Unmarshal(g.Marshal(spec.OriginalMap), &spec.OriginalMap)

	return
}

// Spec defines the complete API specification with endpoints and authentication
type Spec struct {
	Name           string         `yaml:"string" json:"string"`
	Description    string         `yaml:"description" json:"description"`
	Calls          Calls          `yaml:"calls" json:"calls"`
	Queues         []string       `yaml:"queues" json:"queues"`
	Defaults       Endpoint       `yaml:"defaults" json:"defaults"`
	Authentication Authentication `yaml:"authentication" json:"authentication"`
	Endpoints      Endpoints      `yaml:"endpoints" json:"endpoints"`

	OriginalMap map[string]any
}

func (s *Spec) IsDynamic() bool {
	for _, e := range s.Endpoints {
		if e.Dynamic.Iterate != "" {
			return true
		}
	}
	return false
}

// Authentication defines how to authenticate with the API
type Authentication struct {
	Type              AuthType           `yaml:"type" json:"type"`
	Token             string             `yaml:"token" json:"token"`
	Username          string             `yaml:"username" json:"username"`
	Password          string             `yaml:"password" json:"password"`
	Flow              AuthenticationFlow `yaml:"flow" json:"flow"`
	AuthenticationURL string             `yaml:"authentication_url" json:"authentication_url"`
	ClientID          string             `yaml:"client_id" json:"client_id"`
	ClientSecret      string             `yaml:"client_secret" json:"client_secret"`
	Scopes            []string           `yaml:"scopes" json:"scopes"`
	RedirectURI       string             `yaml:"redirect_uri" json:"redirect_uri"`
	RefreshToken      string             `yaml:"refresh_token" json:"refresh_token"`
	RefreshOnExpire   bool               `yaml:"refresh_on_expire" json:"refresh_on_expire"`
}

type AuthType string

const (
	AuthTypeNone   AuthType = ""
	AuthTypeBearer AuthType = "bearer"
	AuthTypeBasic  AuthType = "basic"
	AuthTypeOAuth2 AuthType = "oauth2"
)

type AuthenticationFlow string

// Endpoints is a collection of API endpoints
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
	Dynamic     Loop       `yaml:"dynamic" json:"dynamic"`

	index        int  // for jmespath lookup
	stop         bool // whether we should stop the endpoint process
	conn         *APIConnection
	client       http.Client
	eval         *goval.Evaluator
	totalRecords int
	totalReqs    int
	syncMap      StateMap // values to sync
	context      *g.Context
}

func (ep *Endpoint) SetStateVal(key string, val any) {
	ep.context.Lock()
	ep.State[key] = val
	ep.context.Unlock()
}

func (eps Endpoints) Sort() {
	sort.Slice(eps, func(i, j int) bool {
		return eps[i].Name < eps[j].Name
	})
}

// Loop is for configuring looping values for requests
type Loop struct {
	Iterate    any    `yaml:"iterate" json:"iterate,omitempty"` // expression
	Into       string `yaml:"into" json:"into,omitempty"`       // expression
	iterations chan *Iteration
}

type Iteration struct {
	counter int
	field   string // state field
	value   any    // state value
	stop    bool   // whether to stop the iteration
}

// Calls are steps that are executed at different stages of the API request lifecycle
type Calls []Call

// Call defines an executable action to be performed at a specific stage
type Call interface {
	ID() string
	Type() CallType
	Stage() CallStage
	Execute() error
}

type CallType string

const (
	CallTypeRequest CallType = "request"
	CallTypeAuth    CallType = "auth"
)

type CallStage string

const (
	CallStageStart CallStage = "start" // called once when run begins
	CallStageEnd   CallStage = "end"   // called once when run finishes
	CallStagePre   CallStage = "pre"   // called right before each stream begins
	CallStagePost  CallStage = "post"  // called right after each stream finishes
)

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
	Loop        Loop           `yaml:"loop" json:"loop,omitempty"`               // state expression to use to loop
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
	Records    Records     `yaml:"records" json:"records"`
	Processors []Processor `yaml:"processors" json:"processors,omitempty"`
	Rules      []Rule      `yaml:"rules" json:"rules,omitempty"`
}

// Records configures how to extract and process data records from a response
type Records struct {
	JmesPath   string   `yaml:"jmespath" json:"jmespath,omitempty"` // for json or xml
	PrimaryKey []string `yaml:"primary_key" json:"primary_key,omitempty"`
	UpdateKey  string   `yaml:"update_key" json:"update_key,omitempty"`
	Limit      int      `yaml:"limit" json:"limit,omitempty"` // to limit the records, useful for testing
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

func NewSingleRequest(ep *Endpoint, iter *Iteration) *SingleRequest {
	ep.totalReqs++
	return &SingleRequest{
		id:        g.F("r.%04d.%s", ep.totalReqs, g.RandString(g.AlphaRunesLower, 3)),
		timestamp: time.Now().UnixMilli(),
		endpoint:  ep,
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
