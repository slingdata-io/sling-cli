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

		if err = validateAndSetDefaults(endpoint, ac.Spec); err != nil {
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
	validateAndSetDefaults = func(ep *Endpoint, spec Spec) (err error) {
		return g.Error("please use the official sling-cli release for reading APIs")
	}
	streamRequests = func(ep *Endpoint) (ds *iop.Datastream, err error) {
		return nil, g.Error("please use the official sling-cli release for reading APIs")
	}
)
