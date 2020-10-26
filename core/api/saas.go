package api

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/slingdata-io/sling/core/env"

	h "github.com/flarco/gutil"
	"github.com/jmespath/go-jmespath"
	jsoniter "github.com/json-iterator/go"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/spf13/cast"
	yaml "gopkg.in/yaml.v2"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type APIProvider string

const (
	// DigitalOcean is APIProvider
	DigitalOcean APIProvider = "digitalocean"
	Git          APIProvider = "git"
	Github       APIProvider = "github"
	SurveyMonkey APIProvider = "surveymonkey"
)

// String returns the db type string
func (ap APIProvider) String() string {
	return string(ap)
}

// API is the Base interface for apis
type API interface {
	Self() API
	Base() *BaseAPI
	Init() error
	Close() error
	GetProp(key string) string
	SetProp(key string, val string)
	GetBaseURL() string
	Call(name string, params map[string]interface{}, body []byte) (respBytes []byte, err error)
	Stream(name string, params map[string]interface{}, body []byte) (ds *iop.Datastream, err error)
	LoadEndpoints() (err error)
}

// BaseAPI is the base api struct
type BaseAPI struct {
	API
	Provider      APIProvider
	Endpoints     map[string]*APIEndpoint
	BaseURL       string
	User          string
	Key           string
	Context       h.Context
	FlattenNested bool
	DefHeaders    map[string]string
	DefParams     map[string]string
	properties    map[string]string
	getNextURL    func(resp *http.Response) (url string)
}

// APIEndpoint represents an api endpoint
type APIEndpoint struct {
	API       API                    `yaml:"-"`
	Name      string                 `yaml:"-"`
	ColumnMap map[string]*iop.Column `yaml:"-"`
	URL       string                 `yaml:"-"`
	Endpoint  string                 `yaml:"endpoint,omitempty"`
	Method    string                 `yaml:"method,omitempty"`
	RecordsJP string                 `yaml:"records_jp,omitempty"`
	NextJP    string                 `yaml:"next_jp,omitempty"`
	RespType  string                 `yaml:"resp_type,omitempty"`
	Headers   map[string]string      `yaml:"headers"`
	Ds        *iop.Datastream        `yaml:"-"`
	rows      [][]interface{}
	client    *http.Client
	buffer    chan []interface{}
}

// NewAPI creates an API
func NewAPI(ap APIProvider, props ...string) (api API, err error) {
	return NewAPIClientContext(context.Background(), ap, props...)
}

// NewAPI creates an API Client with provided context
func NewAPIClientContext(ctx context.Context, ap APIProvider, props ...string) (api API, err error) {

	switch ap {
	case DigitalOcean:
		api = &DigitalOceanAPI{}
	case Github:
		api = &GithubAPI{}
	case SurveyMonkey:
		api = &SurveyMonkeyAPI{}
	default:
		err = h.Error("unhandled provider: %#v", ap)
		return
	}

	for k, v := range env.EnvVars() {
		api.SetProp(k, v)
	}

	for k, v := range h.KVArrToMap(props...) {
		api.SetProp(k, v) // overwrite if provided
	}

	err = api.Init()
	if err != nil {
		err = h.Error(err, "could not initialize api")
	}

	return

}

// NewAPIClientFromDataConn provides an API client with the given Dataconn URL
func NewAPIClientFromDataConn(dc *iop.DataConn) (api API, err error) {
	return NewAPIClientFromDataConnContext(context.Background(), dc)
}

// NewAPIClientFromDataConnContext provides an API client with the given Dataconn URL
func NewAPIClientFromDataConnContext(ctx context.Context, dc *iop.DataConn) (api API, err error) {
	if dc.URL != "" {
		dc.Vars["URL"] = dc.URL
	}
	switch dc.GetType() {
	case iop.ConnTypeAPIGit:
		return NewAPIClientContext(ctx, Git, h.MapToKVArr(dc.VarsS())...)
	case iop.ConnTypeAPIGithub:
		return NewAPIClientContext(ctx, Github, h.MapToKVArr(dc.VarsS())...)
	default:
		err = h.Error("invalid API connection request")
	}
	return
}

// Init initializes the base API
func (api *BaseAPI) Init() (err error) {

	api.Endpoints = map[string]*APIEndpoint{}
	err = api.LoadEndpoints()
	if err != nil {
		err = h.Error(err, "could not load endpoints")
		return
	}

	api.Context = h.NewContext(context.Background())
	return
}

// Base return the base API instance
func (api *BaseAPI) Base() *BaseAPI {
	return api
}

// GetBaseURL return the base URL
func (api *BaseAPI) GetBaseURL() string {
	return api.BaseURL
}

// GetProp returns the value of a property
func (api *BaseAPI) GetProp(key string) string {
	return api.properties[key]
}

// SetProp sets the value of a property
func (api *BaseAPI) SetProp(key string, val string) {
	if api.properties == nil {
		api.properties = map[string]string{}
	}
	api.properties[key] = val
}

// Call calls an endpoint by name with the provided params and body
func (api *BaseAPI) Call(name string, params map[string]interface{}, body []byte) (respBytes []byte, err error) {
	aos, ok := api.Endpoints[name]
	if !ok {
		err = h.Error("api endpoint '%s' does not exists", name)
		return
	}

	for k, v := range api.DefParams {
		if _, ok := params[k]; !ok {
			params[k] = v
		}
	}

	url := h.Rm(aos.URL, params)
	_, respBytes, err = aos.Request(url, body)
	if err != nil {
		err = h.Error(err, "could not perform request")
		return
	}

	return
}

// Stream returns a stream of the endpoint
func (api *BaseAPI) Stream(name string, params map[string]interface{}, body []byte) (ds *iop.Datastream, err error) {
	aos, ok := api.Endpoints[name]
	if !ok {
		err = h.Error("api endpoint '%s' does not exists", name)
		return
	}

	for k, v := range api.DefParams {
		if _, ok := params[k]; !ok {
			params[k] = v
		}
	}

	url := h.Rm(aos.URL, params)
	aos.buffer = make(chan []interface{}, 100000)

	nextFunc := func(it *iop.Iterator) bool {
		var data interface{}
		var recordsInterf []map[interface{}]interface{}
		if it.Closed {
			return false
		}

		select {
		case row := <-aos.buffer:
			it.Row = row
			return true
		default:
			if url == "" {
				// for row := range aos.buffer {
				// 	it.Row = row
				// 	return true
				// }
				return false
			}
			// proceed with request
		}

		url = h.Rm(url, params)
		resp, respBytes, err := aos.Request(url, body)
		if err != nil {
			// TODO: need to implement retry logic
			it.Context.CaptureErr(h.Error(err, "could not perform request"))
			return false
		}

		// h.P(string(respBytes))
		err = aos.Unmarshal(respBytes, &data)
		if err != nil {
			it.Context.CaptureErr(h.Error(err, "could not parse response"))
			return false
		}

		url = aos.NextURL(data, resp)

		records, err := jmespath.Search(aos.RecordsJP, data)
		if err != nil {
			it.Context.CaptureErr(h.Error(err, "could not find records"))
			return false
		}

		switch t := records.(type) {
		case map[interface{}]interface{}:
			// is one record
			interf := records.(map[interface{}]interface{})
			recordsInterf = []map[interface{}]interface{}{interf}
		case map[string]interface{}:
			// is one record
			interf := map[interface{}]interface{}{}
			for k, v := range records.(map[string]interface{}) {
				interf[k] = v
			}
			recordsInterf = []map[interface{}]interface{}{interf}
		case []interface{}:
			recordsInterf = []map[interface{}]interface{}{}
			recList := records.([]interface{})
			if len(recList) == 0 {
				return false
			}

			switch recList[0].(type) {
			case map[string]interface{}:
				for _, rec := range recList {
					newRec := map[interface{}]interface{}{}
					for k, v := range rec.(map[string]interface{}) {
						newRec[k] = v
					}
					recordsInterf = append(recordsInterf, newRec)
				}
			case map[interface{}]interface{}:
				for _, val := range recList {
					recordsInterf = append(recordsInterf, val.(map[interface{}]interface{}))
				}
			default:
				// is array of single values
				for _, val := range recList {
					recordsInterf = append(recordsInterf, map[interface{}]interface{}{"data": val})
				}
			}

		case []map[string]interface{}:
			recordsInterf = []map[interface{}]interface{}{}
			for _, rec := range records.([]map[string]interface{}) {
				newRec := map[interface{}]interface{}{}
				for k, v := range rec {
					newRec[k] = v
				}
				recordsInterf = append(recordsInterf, newRec)
			}
		case []map[interface{}]interface{}:
			recordsInterf = records.([]map[interface{}]interface{})
		default:
			err = h.Error("unhandled interface type: %#v", t)
			it.Context.CaptureErr(err)
			return false
		}

		// parse records as go routine
		api.Context.Wg.Read.Add()
		go func() {
			aos.parseRecords(recordsInterf, api.Context.Wg.Read.Done)
		}()
		api.Context.Wg.Read.Wait()

		if err = api.Context.Err(); err != nil {
			err = h.Error(err, "error parsing records")
			it.Context.CaptureErr(err)
			return false
		}

		select {
		// wait for row
		case row := <-aos.buffer:
			it.Row = row
			return true
		}
	}

	ds = iop.NewDatastreamIt(context.Background(), []iop.Column{}, nextFunc)
	aos.Ds = ds

	err = ds.Start()
	if err != nil {
		return ds, h.Error(err, "could start datastream")
	}

	return
}

func (aos *APIEndpoint) extractRow(rec map[interface{}]interface{}, parentFields ...string) []interface{} {
	flatten := aos.API.Base().FlattenNested
	row := make([]interface{}, len(aos.Ds.Columns))
	for k, v := range rec {
		pFields := append(parentFields, cast.ToString(k))
		switch vt := v.(type) {
		case map[interface{}]interface{}:
			if flatten {
				v2 := v.(map[interface{}]interface{})
				row2 := aos.extractRow(v2, pFields...)
				if len(row2) > len(row) {
					row = append(row, row2[len(row):len(row2)]...)
				}
				for i := range row2 {
					if row2[i] != nil && row[i] == nil {
						row[i] = row2[i]
					}
				}
			}
		case []interface{}:
			// is array. skip for now, this would create multiple rows
		case map[string]interface{}:
			if flatten {
				v2 := map[interface{}]interface{}{}
				for k, v := range v.(map[string]interface{}) {
					v2[k] = v
				}
				row2 := aos.extractRow(v2, pFields...)
				if len(row2) > len(row) {
					row = append(row, row2[len(row):len(row2)]...)
				}
				for i := range row2 {
					if row2[i] != nil && row[i] == nil {
						row[i] = row2[i]
					}
				}

			}
		default:
			colName := strings.Join(pFields, "__")
			_, ok := aos.ColumnMap[strings.ToLower(colName)]
			if !ok {
				col := iop.Column{
					Name:     colName,
					Type:     aos.Ds.Sp.GetType(v),
					Position: len(aos.Ds.Columns) + 1,
				}
				aos.Ds.Columns = append(aos.Ds.Columns, col)
				aos.ColumnMap[strings.ToLower(colName)] = &col
				row = append(row, nil)
			}
			i := aos.ColumnMap[strings.ToLower(colName)].Position - 1
			row[i] = v
			_ = vt
		}
	}
	return row
}

func (aos *APIEndpoint) parseRecords(records []map[interface{}]interface{}, rowReady func()) {
	if aos.ColumnMap == nil {
		aos.ColumnMap = map[string]*iop.Column{}
	}

	rowReadied := false
	for _, rec := range records {
		aos.buffer <- aos.extractRow(rec)
		if !rowReadied {
			rowReady()
			rowReadied = true
		}
	}
	h.Debug("API Endpoint %s -> Parsed %d records", aos.Name, len(records))
}

// LoadEndpoints loads the endpoints from a yaml file
func (api *BaseAPI) LoadEndpoints() (err error) {
	fName := h.F("specs/%s.yaml", api.Provider.String())
	SpecFile, err := h.PkgerFile(fName)
	if err != nil {
		return h.Error(err, `cannot read `+fName)
	}

	SpecBytes, err := ioutil.ReadAll(SpecFile)
	if err != nil {
		return h.Error(err, "ioutil.ReadAll(baseTemplateFile)")
	}

	if err = yaml.Unmarshal(SpecBytes, &api.Endpoints); err != nil {
		err = h.Error(err, "could not parse "+fName)
		return
	}

	client := http.Client{Timeout: 30 * time.Second}
	for name, endpoint := range api.Endpoints {
		endpoint.Name = name
		endpoint.API = api
		endpoint.URL = api.BaseURL + endpoint.Endpoint
		endpoint.client = &client
		if endpoint.Headers == nil {
			endpoint.Headers = map[string]string{}
		}
		for k, v := range api.DefHeaders {
			if _, ok := endpoint.Headers[k]; !ok {
				endpoint.Headers[k] = v
			}
		}
	}

	h.Trace("loaded %d endpoints", len(api.Endpoints))
	return
}

// Unmarshal parses the response based on the response type
func (aos *APIEndpoint) Unmarshal(body []byte, data *interface{}) (err error) {
	switch aos.RespType {
	case "json":
		return json.Unmarshal(body, &data)
	case "xml":
		return xml.Unmarshal(body, &data)
	default:
		err = fmt.Errorf("unhandled response type '%s'", aos.RespType)
		return h.Error(err)
	}
}

// Request performs a request
func (aos *APIEndpoint) Request(url string, body []byte) (resp *http.Response, respBytes []byte, err error) {
	payload := bytes.NewReader(body)
	h.Debug("API Endpoint %s -> %s %s", aos.Name, aos.Method, url)

	req, err := http.NewRequest(aos.Method, url, payload)
	if err != nil {
		err = h.Error(err, "could not create request")
		return
	}

	for k, v := range aos.Headers {
		req.Header.Set(k, v)
	}

	resp, err = aos.client.Do(req)
	if err != nil {
		err = h.Error(err, "could not perform request")
		return
	}

	// h.P(resp.Header)

	h.Trace("API Endpoint %s -> %d: %s", aos.Name, resp.StatusCode, resp.Status)
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		err = h.Error("Bad API Response %d: %s", resp.StatusCode, resp.Status)
		return
	}

	respBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = h.Error(err, "could not read from request body")
	}

	h.Trace("API Endpoint %s -> Got %d bytes", aos.Name, len(respBytes))
	return
}

// NextURL determines and sets the next URL in a stream call
func (aos *APIEndpoint) NextURL(data interface{}, resp *http.Response) string {
	var val interface{}
	if aos.API.Base().getNextURL != nil {
		val = aos.API.Base().getNextURL(resp)
	} else if aos.NextJP != "" {
		var err error
		val, err = jmespath.Search(aos.NextJP, data)
		if err != nil {
			err = h.Error(err, "could not parse response")
			return ""
		}
	} else {
		return ""
	}

	switch val.(type) {
	case string:
		sVal := val.(string)
		if sVal == "" {
			return ""
		} else if strings.HasPrefix(sVal, "/") {
			sVal = h.F("%s%s", aos.API.GetBaseURL(), sVal)
		}
		return sVal
	}

	return ""
}
