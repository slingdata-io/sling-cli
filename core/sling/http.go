package sling

import (
	"bytes"
	"net/url"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/spf13/cast"
)

// RouteName is the name of a route
type RouteName string

const (
	slingAppRoot        string    = "https://slingdata.io"
	RouteStatus         RouteName = "/status"
	RouteNotice         RouteName = "/notice"
	RouteError          RouteName = "/error"
	RouteSignUpUser     RouteName = "/sign-up"
	RouteUser           RouteName = "/user"
	RouteForgotPassword RouteName = "/forgot-password"
	RouteResetPassword  RouteName = "/reset-password"
	RouteLogin          RouteName = "/login"
	RouteLogout         RouteName = "/logout"
	RouteProxy          RouteName = "/p"

	RouteAppIndex  RouteName = "/app"
	RouteAppLogin  RouteName = "/app/login"
	RouteAppLogout RouteName = "/app/logout"
	RouteAppAPIKey RouteName = "/app/apikey"

	RouteAPI               RouteName = "/api/v1"
	RouteMasterStatus      RouteName = "/api/v1/master-status"
	RouteMasterDBReset     RouteName = "/api/v1/master-db-reset"
	RouteUploads           RouteName = "/api/v1/uploads"
	RouteAPIAccounts       RouteName = "/api/v1/accounts"
	RouteAPIProjects       RouteName = "/api/v1/projects"
	RouteAPIKey            RouteName = "/api/v1/apikey"
	RouteAPIUsers          RouteName = "/api/v1/users"
	RouteAPIJobs           RouteName = "/api/v1/jobs"
	RouteAPILogs           RouteName = "/api/v1/logs"
	RouteAPIExecutions     RouteName = "/api/v1/executions"
	RouteAPIConnections    RouteName = "/api/v1/connections"
	RouteAPIConnectionTest RouteName = "/api/v1/connection-test"
	RouteAPIResetPassword  RouteName = "/api/v1/reset-password"
	RouteAPIDataRequest    RouteName = "/api/v1/data-request"
	RouteAPIWorkers        RouteName = "/api/v1/workers"
	RouteAPISettings       RouteName = "/api/v1/settings"
	RouteAlertLog          RouteName = "/alert/log"

	RouteWs       RouteName = "/ws"
	RouteWsClient RouteName = "/ws/client"
	RouteWsWorker RouteName = "/ws/worker"
)

// ClientPost sends a POST request
func ClientPost(serverURL string, route RouteName, m map[string]interface{}, headers map[string]string) (respStr string, err error) {
	URL := serverURL + string(route)
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		err = g.Error(err)
		return
	}
	_, respBytes, err := net.ClientDo(
		"POST",
		URL,
		bytes.NewBuffer(jsonBytes),
		headers,
	)
	respStr = string(respBytes)

	if err != nil {
		err = g.Error(err, "error sending POST to url")
		return
	}

	return
}

// ClientPatch sends a PATCH request
func ClientPatch(serverURL string, route RouteName, m map[string]interface{}, headers map[string]string) (respStr string, err error) {
	URL := serverURL + string(route)
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		err = g.Error(err)
		return
	}
	_, respBytes, err := net.ClientDo(
		"PATCH",
		URL,
		bytes.NewBuffer(jsonBytes),
		headers,
	)
	respStr = string(respBytes)

	if err != nil {
		err = g.Error(err, "error sending PATCH to url")
		return
	}

	return
}

// ClientPut sends a PUT request
func ClientPut(serverURL string, route RouteName, m map[string]interface{}, headers map[string]string) (respStr string, err error) {
	URL := serverURL + string(route)
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		err = g.Error(err)
		return
	}
	_, respBytes, err := net.ClientDo(
		"PUT",
		URL,
		bytes.NewBuffer(jsonBytes),
		headers,
	)
	respStr = string(respBytes)

	if err != nil {
		err = g.Error(err, "error sending PUT to url")
		return
	}

	return
}

// ClientOptions sends a HEAD request
func ClientOptions(serverURL string, route RouteName, m map[string]interface{}, headers map[string]string) (respStr string, err error) {
	URL := serverURL + string(route)
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		err = g.Error(err)
		return
	}
	_, respBytes, err := net.ClientDo(
		"OPTIONS",
		URL,
		bytes.NewBuffer(jsonBytes),
		headers,
	)
	respStr = string(respBytes)

	if err != nil {
		err = g.Error(err, "error sending OPTIONS to url")
		return
	}

	return
}

// ClientGet sends a GET request
func ClientGet(serverURL string, route RouteName, m map[string]interface{}, headers map[string]string) (respStr string, err error) {
	vals := url.Values{}
	for k, v := range m {
		switch v.(type) {
		case map[string]interface{}:
			v = string(g.MarshalMap(v.(map[string]interface{})))
		}
		val := cast.ToString(v)
		if val == "" {
			continue
		}
		vals.Set(k, val)
	}
	URL := serverURL + string(route) + "?" + vals.Encode()

	_, respBytes, err := net.ClientDo(
		"GET",
		URL,
		nil,
		headers,
	)
	respStr = string(respBytes)
	if err != nil {
		err = g.Error(err, "error sending GET to url")
		return
	}

	return respStr, nil
}

// ClientDelete sends a DELETE request
func ClientDelete(serverURL string, route RouteName, m map[string]interface{}, headers map[string]string) (respStr string, err error) {
	URL := serverURL + string(route)
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		err = g.Error(err)
		return
	}
	_, respBytes, err := net.ClientDo(
		"DELETE",
		URL,
		bytes.NewBuffer(jsonBytes),
		headers,
	)
	if err != nil {
		err = g.Error(err, "error sending DELETE to url")
		return
	}
	respStr = string(respBytes)

	return
}

// GetJWTFromKey logs in and returns the JWT based on the provided key
func GetJWTFromKey(masterServerURL, key string) (string, error) {
	respStr, err := ClientPost(
		masterServerURL, RouteAppLogin,
		g.M("password", key), map[string]string{"Content-Type": "application/json"},
	)
	if err != nil {
		return "", err
	}

	m := g.M()
	err = json.Unmarshal([]byte(respStr), &m)
	if err != nil {
		return "", g.Error(err, "could not parse token response")
	}

	jwt := cast.ToString(cast.ToStringMap(m["user"])["token"])
	if jwt == "" {
		return "", g.Error(err, "blank token")
	}

	return jwt, nil
}
