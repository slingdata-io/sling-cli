package api

import (
	"net/http"
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

type Spec struct {
	Name           string         `yaml:"string" json:"string"`
	Description    string         `yaml:"description" json:"description"`
	Calls          Calls          `yaml:"calls" json:"calls"`
	Defaults       Endpoint       `yaml:"defaults" json:"defaults"`
	Authentication Authentication `yaml:"authentication" json:"authentication"`
	Endpoints      Endpoints      `yaml:"endpoints" json:"endpoints"`

	OriginalMap map[string]any
}

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

type Endpoints []Endpoint

// Endpoint is the top-level configuration structure
type Endpoint struct {
	Name        string          `yaml:"name" json:"name"`
	Description string          `yaml:"description" json:"description,omitempty"`
	State       StateMap        `yaml:"state" json:"state"`
	Request     Request         `yaml:"request" json:"request"`
	Pagination  Pagination      `yaml:"pagination" json:"pagination"`
	Response    Response        `yaml:"response" json:"response"`
	Dynamic     DynamicEndpoint `yaml:"dynamic" json:"dynamic"`

	index     int  // for jmespath lookup
	stop      bool // whether we should stop
	conn      *APIConnection
	client    http.Client
	eval      *goval.Evaluator
	totalReqs int
}

// DynamicEndpoint is for configuring dynamic streams
type DynamicEndpoint struct {
	Iterate string `yaml:"iterate" json:"iterate,omitempty"`
	Into    string `yaml:"into" json:"into,omitempty"`
}

// Calls are steps that are executed at different stages
type Calls []Call

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

// StateMap is a map of the state
type StateMap map[string]any

// StateValue represents the value of a state
type StateValue struct {
	Initial any  `yaml:"initial"`
	Sync    bool `yaml:"sync"`
}

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

// Request represents the request configuration
type Request struct {
	URL         string         `yaml:"url" json:"url,omitempty"`
	Timeout     int            `yaml:"timeout" json:"timeout,omitempty"`
	Method      HTTPMethod     `yaml:"method" json:"method,omitempty"`
	Headers     map[string]any `yaml:"headers" json:"headers,omitempty"`
	Parameters  map[string]any `yaml:"parameters" json:"parameters,omitempty"`
	Payload     any            `yaml:"payload" json:"payload,omitempty"`
	Loop        string         `yaml:"loop" json:"loop,omitempty"`               // state expression to use to loop
	Rate        float64        `yaml:"rate" json:"rate,omitempty"`               // maximum request per second
	Concurrency int            `yaml:"concurrency" json:"concurrency,omitempty"` // maximum concurrent requests
}

// Pagination represents the pagination configuration
type Pagination struct {
	NextState     map[string]any `yaml:"next_state" json:"next_state,omitempty"`
	StopCondition string         `yaml:"stop_condition" json:"stop_condition,omitempty"`
}

type PaginationType string

const (
	PaginationTypeNone   PaginationType = ""
	PaginationTypeStep   PaginationType = "step"
	PaginationTypeCursor PaginationType = "cursor"
	PaginationTypeLink   PaginationType = "link"
)

// Response represents the response configuration
type Response struct {
	Records    Records     `yaml:"records" json:"records"`
	Processors []Processor `yaml:"processors" json:"processors,omitempty"`
	Rules      []Rule      `yaml:"rules" json:"rules,omitempty"`
}

// Records represents the records configuration
type Records struct {
	JmesPath   string   `yaml:"jmespath" json:"jmespath,omitempty"` // for json or xml
	PrimaryKey []string `yaml:"primary_key" json:"primary_key,omitempty"`
	UpdateKey  string   `yaml:"update_key" json:"update_key,omitempty"`
	Limit      int      `yaml:"limit" json:"limit,omitempty"` // to limit the records, useful for testing
}

type AggregationType string

const (
	AggregationTypeNone    AggregationType = ""
	AggregationTypeMaximum AggregationType = "maximum"
	AggregationTypeMinimum AggregationType = "minimum"
	AggregationTypeFlatten AggregationType = "flatten"
	AggregationTypeFirst   AggregationType = "first"
	AggregationTypeLast    AggregationType = "last"
)

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
	RuleTypeRetry RuleType = "retry"
	RuleTypeStop  RuleType = "stop"
	RuleTypeFail  RuleType = "fail"
)

// Rule represents a response rule
type Rule struct {
	Action      RuleType `yaml:"action" json:"action"`
	Condition   string   `yaml:"condition" json:"condition"` // an expression
	MaxAttempts int      `yaml:"max_attempts" json:"max_attempts"`
	Backoff     string   `yaml:"backoff" json:"backoff"`
	Message     string   `yaml:"message" json:"message"`
}

type SingleRequest struct {
	Records    []any          `yaml:"records" json:"records"`
	Request    *RequestState  `yaml:"request" json:"request"`
	Response   *ResponseState `yaml:"response" json:"response"`
	Aggregate  AggregateState `yaml:"-" json:"-"`
	httpReq    *http.Request  `yaml:"-" json:"-"`
	httpRespWg sync.WaitGroup `yaml:"-" json:"-"`
	id         string         `yaml:"-" json:"-"`
	timestamp  int64          `yaml:"-" json:"-"`
	endpoint   *Endpoint      `yaml:"-" json:"-"`
}

func NewSingleRequest(ep *Endpoint) *SingleRequest {
	ep.totalReqs++
	return &SingleRequest{
		id:        g.F("r.%04d.%s", ep.totalReqs, g.RandString(g.AlphaRunesLower, 3)),
		timestamp: time.Now().UnixMilli(),
		endpoint:  ep,
	}
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

type RequestState struct {
	Method   string         `yaml:"method" json:"method"`
	URL      string         `yaml:"url" json:"url"`
	Headers  map[string]any `yaml:"headers" json:"headers"`
	Payload  any            `yaml:"payload" json:"payload"`
	Attempts int            `yaml:"attempts" json:"attempts"`
}

type ResponseState struct {
	Status  int            `yaml:"status" json:"status"`
	Headers map[string]any `yaml:"headers" json:"headers"`
	Text    string         `yaml:"text" json:"text"`
	JSON    any            `yaml:"json" json:"json"`
}

type AggregateState struct {
	value any
	array []any
}
