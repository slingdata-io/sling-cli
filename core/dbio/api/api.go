package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/jmespath/go-jmespath"
	"github.com/maja42/goval"
	"github.com/samber/lo"
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
			Env:   g.KVArrToMap(os.Environ()...),
			State: g.M(),
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

	return ac, nil
}

// Authenticate performs the auth workflow if needed. Like a Connect step.
// Header based auths (such as Basic, or Bearer) don't need this step
// save payload in APIState.Auth
func (ac *APIConnection) Authenticate() (err error) {
	auth := ac.Spec.Authentication
	switch auth.Type {
	case AuthTypeNone:
		ac.State.Auth.Authenticated = true
		return nil
	case AuthTypeBearer:
		token, err := ac.renderString(auth.Token)
		if err != nil {
			return g.Error(err, "could not render token")
		}
		ac.State.Auth.Authenticated = true
		ac.State.Auth.Headers = map[string]string{
			"Authorization": g.F("Bearer %s", token),
		}
		return nil
	case AuthTypeBasic:
		userPass, err := ac.renderString(g.F("%s:%s", auth.Username, auth.Password))
		if err != nil {
			return g.Error(err, "could not render user-password")
		}
		credentialsB64 := base64.StdEncoding.EncodeToString([]byte(userPass))
		ac.State.Auth.Headers = map[string]string{
			"Authorization": g.F("Basic %s", credentialsB64),
		}
		ac.State.Auth.Authenticated = true
		return nil
	case AuthTypeOAuth2:
		// TODO: implement various OAuth2 flows
	default:
		ac.State.Auth.Authenticated = true
		return nil
	}

	return
}

func (ac *APIConnection) ListEndpoints(patterns ...string) (endpoints Endpoints, err error) {

	endpoints = ac.Spec.Endpoints

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

func (ac *APIConnection) ReadDataflow(endpointName string) (df *iop.Dataflow, err error) {
	if !ac.State.Auth.Authenticated {
		return nil, g.Error("not authenticated")
	}

	// get endpoint, match to name
	var endpoint *Endpoint
	{
		for _, ep := range ac.Spec.Endpoints {
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

		if err = ValidateAndSetDefaults(endpoint, ac.Spec); err != nil {
			return nil, g.Error(err, "endpoint validation failed")
		}

		// TODO: perform pre-endpoint calls
	}

	// start request process
	ds, err := streamRequests(endpoint)
	if err != nil {
		return nil, g.Error(err, "could not stream requests")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return nil, g.Error(err, "could not make dataflow")
	}

	return
}

type APIState struct {
	Env     map[string]string `json:"env,omitempty"`
	State   map[string]any    `json:"state,omitempty"`
	Secrets map[string]any    `json:"secrets,omitempty"`
	Auth    APIStateAuth      `json:"auth,omitempty"`
}

type APIStateAuth struct {
	Authenticated bool              `json:"authenticated,omitempty"`
	Token         string            `json:"token,omitempty"` // refresh token?
	Headers       map[string]string `json:"-"`               // to inject
}

var bracketRegex = regexp.MustCompile(`\$\{([^\{\}]+)\}`)

func (ac *APIConnection) getStateMap(extraMaps map[string]any, varsToCheck []string) map[string]any {

	sections := []string{"env", "state", "secrets", "auth"}

	statePayload := g.Marshal(ac.State)
	stateMap, _ := g.UnmarshalMap(statePayload)

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

// extractVars identifies variable references in a string expression,
// ignoring those inside double quotes. It recognizes patterns like env.VAR,
// state.VAR, secrets.VAR, and auth.VAR.
func extractVars(expr string) []string {
	// To track found variable references
	var references []string

	// Regular expression for finding variable references
	// Matches env., state., secrets., auth. followed by a variable name
	refRegex := regexp.MustCompile(`(env|state|secrets|auth|response|request)\.\w+`)

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
	streamRequests = func(ep *Endpoint) (ds *iop.Datastream, err error) {
		return nil, g.Error("please use the official sling-cli release for reading APIs")
	}
)

// ValidateAndSetDefaults checks and sets defaults
func ValidateAndSetDefaults(ep *Endpoint, spec Spec) (err error) {
	var eG g.ErrorGroup

	{
		if ep.Name == "" {
			ep.Name = spec.Defaults.Name
		}
		if ep.Description == "" {
			ep.Description = spec.Defaults.Description
		}
		if ep.Request.URL == "" {
			ep.Request.URL = spec.Defaults.Request.URL
		}
		if ep.Request.Timeout == 0 {
			ep.Request.Timeout = spec.Defaults.Request.Timeout
		}
		if ep.Request.Method == "" {
			ep.Request.Method = spec.Defaults.Request.Method
		}
		if ep.Request.Headers == nil {
			ep.Request.Headers = spec.Defaults.Request.Headers
		}
		if ep.Request.Parameters == nil {
			ep.Request.Parameters = spec.Defaults.Request.Parameters
		}
		if ep.Request.Payload == nil {
			ep.Request.Payload = spec.Defaults.Request.Payload
		}
		if ep.Request.Loop == "" {
			ep.Request.Loop = spec.Defaults.Request.Loop
		}
		if ep.Request.Rate == 0 {
			ep.Request.Rate = spec.Defaults.Request.Rate
		}
		if ep.Request.Concurrency == 0 {
			ep.Request.Concurrency = spec.Defaults.Request.Concurrency
		}
		if ep.Pagination.StopCondition == "" {
			ep.Pagination.StopCondition = spec.Defaults.Pagination.StopCondition
		}
		if ep.Pagination.NextState == nil {
			ep.Pagination.NextState = spec.Defaults.Pagination.NextState
		}
		if ep.Response.Records.JmesPath == "" {
			ep.Response.Records.JmesPath = spec.Defaults.Response.Records.JmesPath
		}
		if ep.Response.Records.UpdateKey == "" {
			ep.Response.Records.UpdateKey = spec.Defaults.Response.Records.UpdateKey
		}
		if ep.Response.Records.PrimaryKey == nil {
			ep.Response.Records.PrimaryKey = spec.Defaults.Response.Records.PrimaryKey
		}
		if ep.Response.Records.Limit == 0 {
			ep.Response.Records.Limit = spec.Defaults.Response.Records.Limit
		}
		if ep.Response.Processors == nil {
			ep.Response.Processors = spec.Defaults.Response.Processors
		}
		if ep.Response.Rules == nil {
			ep.Response.Rules = spec.Defaults.Response.Rules
		}
	}

	// for state, merge the default into endpoint's
	if len(ep.State) == 0 {
		ep.State = spec.Defaults.State
	} else {
		for k, v := range spec.Defaults.State {
			if _, ok := ep.State[k]; !ok {
				ep.State[k] = v
			}
		}
	}

	// check request
	if ep.Request.URL == "" {
		eG.Add(g.Error("empty request url"))
	}
	if ep.Request.Method == "" {
		ep.Request.Method = MethodGet
	}
	ep.Request.Method = HTTPMethod(strings.ToUpper(string(ep.Request.Method)))

	if ep.Request.Headers == nil {
		ep.Request.Headers = g.M()
	}
	if ep.Request.Parameters == nil {
		ep.Request.Parameters = g.M()
	}

	// check response
	for _, processor := range ep.Response.Processors {
		if strings.TrimSpace(processor.Expression) == "" {
			eG.Add(g.Error("empty response processor expression"))
		}
		if strings.TrimSpace(processor.Output) == "" &&
			!strings.Contains(processor.Expression, "log(") {
			eG.Add(g.Error("empty response processor output"))
		}
	}

	// set defaults
	ep.eval = goval.NewEvaluator()
	ep.client = http.Client{}
	if ep.Request.Timeout > 0 {
		to := time.Duration(ep.Request.Timeout) * time.Second
		ep.client = http.Client{Timeout: to}
	}

	if ep.Request.Rate == 0 {
		ep.Request.Rate = 10 // default max new requests per second
	}

	if ep.Request.Concurrency == 0 {
		ep.Request.Concurrency = 10 // default max concurrent requests
	}

	// append default rules: retry on 429, fail if 400 or above
	// we can override with "Action: RuleTypeContinue", rules are evaluated in order
	rule429 := Rule{
		Action:      RuleTypeRetry,
		Condition:   "response.status == 429",
		MaxAttempts: 3,
		BackoffBase: 2,
		Backoff:     BackoffTypeLinear,
	}
	ep.Response.Rules = append(ep.Response.Rules, rule429)

	rule400 := Rule{Action: RuleTypeFail, Condition: "response.status >= 400"}
	ep.Response.Rules = append(ep.Response.Rules, rule400)

	// determine order render
	order := []string{}
	{
		remaining := lo.Keys(ep.State)
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

			expr := cast.ToString(ep.State[key])
			matches := bracketRegex.FindAllStringSubmatch(expr, -1)
			if len(matches) > 0 {
				for _, match := range matches {
					varsReferenced := extractVars(match[1])
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
				return g.Error(err, "error determining render order")
			}
		}
	}

	// render initial state
	for _, k := range order {
		if expr := cast.ToString(ep.State[k]); hasBrackets(expr) {
			val, err := ep.renderAny(ep.State[k])
			if err != nil {
				if strings.Contains(err.Error(), `"require" - input required`) {
					return g.Error("state variable input was required but not provided: %s = %s", k, expr)
				}
				return g.Error(err, "could not render state var (%s) : %s", k, expr)
			}
			ep.State[k] = val
		}
	}

	g.Trace("endpoint: %s", g.Marshal(ep))

	return eG.Err()
}
