package api

import (
	"log"
	"testing"

	"github.com/flarco/g"
	"github.com/jmespath/go-jmespath"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

type account struct {
	DropletLimit    int    `json:"droplet_limit"`
	FloatingIPLimit int    `json:"floating_ip_limit"`
	Email           string `json:"email"`
	UUID            string `json:"uuid"`
	EmailVerified   bool   `json:"email_verified"`
	Status          string `json:"status"`
	StatusMessage   string `json:"status_message"`
}

type accountResp struct {
	Account account `json:"account"`
}

type billingHistory struct {
	Description string `json:"description"`
	Amount      string `json:"amount"`
	InvoiceID   string `json:"invoice_id"`
	InvoiceUUID string `json:"invoice_uuid"`
	Date        string `json:"date"`
	Type        string `json:"type"`
}

type linkPage struct {
	Next string `json:"next"`
	Last string `json:"last"`
}

type link struct {
	pages map[string]linkPage
}

type billingHistoryResp struct {
	BillingHistory []billingHistory    `json:"billing_history"`
	Links          map[string]linkPage `json:"links"`
	Meta           map[string]int      `json:"meta"`
}

var json2 = jsoniter.ConfigCompatibleWithStandardLibrary
var jsonAccountExample = []byte(`{
  "account": {
    "droplet_limit": 25,
    "floating_ip_limit": 5,
    "email": "sammy@digitalocean.com",
    "uuid": "b6fr89dbf6d9156cace5f3c78dc9851d957381ef",
    "email_verified": true,
    "status": "active",
    "status_message": ""
  }
}`)
var jsonBillingHistoryExample = []byte(`{
	"billing_history": [
		{
			"description": "Invoice for May 2018",
			"amount": "12.34",
			"invoice_id": "123",
			"invoice_uuid": "example-uuid",
			"date": "2018-06-01T08:44:38Z",
			"type": "Invoice"
		},
		{
			"description": "Payment (MC 2018)",
			"amount": "-12.34",
			"date": "2018-06-02T08:44:38Z",
			"type": "Payment"
		}
	],
	"links": {
		"pages": {
			"next": "https://api.digitalocean.com/v2/customers/my/billing_history?page=2&per_page=2",
			"last": "https://api.digitalocean.com/v2/customers/my/billing_history?page=3&per_page=2"
		}
	},
	"meta": {
		"total": 5
	}
}`)

func TestParse(t *testing.T) {
	var bi, ai interface{}
	var bs billingHistoryResp
	var as accountResp
	json2.Unmarshal(jsonBillingHistoryExample, &bi)
	g.P(bi)
	json2.Unmarshal(jsonBillingHistoryExample, &bs)
	g.P(bs)

	json2.Unmarshal(jsonAccountExample, &ai)
	g.P(ai)
	json2.Unmarshal(jsonAccountExample, &as)
	g.P(as)
}

// go test -benchmem -run='^$ github.com/slingdata-io/sling/core/api' -bench '^BenchmarkParseJSON'
func BenchmarkParseJSON1iBH(b *testing.B) {
	var data interface{}
	for n := 0; n < b.N; n++ {
		json.Unmarshal(jsonBillingHistoryExample, &data)
	}
}
func BenchmarkParseJSON1sBH(b *testing.B) {
	var data billingHistoryResp
	for n := 0; n < b.N; n++ {
		json.Unmarshal(jsonBillingHistoryExample, &data)
	}
}
func BenchmarkParseJSON2iBH(b *testing.B) {
	var data interface{}
	for n := 0; n < b.N; n++ {
		json2.Unmarshal(jsonBillingHistoryExample, &data)
	}
}
func BenchmarkParseJSON2sBH(b *testing.B) {
	var data billingHistoryResp
	for n := 0; n < b.N; n++ {
		json2.Unmarshal(jsonBillingHistoryExample, &data)
	}
}
func BenchmarkParseJSON1iA(b *testing.B) {
	var data interface{}
	for n := 0; n < b.N; n++ {
		json.Unmarshal(jsonAccountExample, &data)
	}
}
func BenchmarkParseJSON1sA(b *testing.B) {
	var data accountResp
	for n := 0; n < b.N; n++ {
		json.Unmarshal(jsonAccountExample, &data)
	}
}
func BenchmarkParseJSON2iA(b *testing.B) {
	var data interface{}
	for n := 0; n < b.N; n++ {
		json2.Unmarshal(jsonAccountExample, &data)
	}
}
func BenchmarkParseJSON2sA(b *testing.B) {
	var data accountResp
	for n := 0; n < b.N; n++ {
		json2.Unmarshal(jsonAccountExample, &data)
	}
}

func TestJSONPath(t *testing.T) {

	var jsonBlob = []byte(`{"name":"ben", "items": [{"age":38}]}`)
	var d interface{}

	err := json.Unmarshal(jsonBlob, &d)
	if err != nil {
		log.Fatal(err)
	}

	v, err := jmespath.Search("items[0].age", d)
	if err != nil {
		log.Fatal(err)
	}
	g.P(v)
}

func TestDigitalOcean(t *testing.T) {

	api, err := NewAPI(DigitalOcean)
	if !assert.NoError(t, err) {
		return
	}
	// return

	// ds, err := api.Stream("droplets", nil, nil)
	ds, err := api.Stream("billing_history", nil, nil)
	if !assert.NoError(t, err) {
		return
	}

	data, err := ds.Collect(0)
	assert.NoError(t, err)
	g.P(len(data.Rows))
	g.P(len(data.Columns))
	if len(data.Rows) > 0 {
		g.P(data.GetFields())
		g.P(data.Rows[0])
	}
}
func TestGithub(t *testing.T) {

	api, err := NewAPI(Github)
	if !assert.NoError(t, err) {
		return
	}
	// return

	// params := map[string]interface{}{
	// 	"owner": "slingdata",
	// 	"repo":  "sling",
	// }

	// params := map[string]interface{}{
	// 	"owner": "mattm",
	// 	"repo":  "sql-style-guide",
	// }
	params := map[string]interface{}{
		"owner": "lib",
		"repo":  "pq",
	}
	// ds, err := api.Stream("issue", params, nil)
	ds, err := api.Stream("pull_request", params, nil)
	// ds, err := api.Stream("stargazer", params, nil)
	if !assert.NoError(t, err) {
		return
	}

	data, err := ds.Collect(0)
	assert.NoError(t, err)
	g.P(len(data.Rows))
	g.P(len(data.Columns))
	if len(data.Rows) > 0 {
		g.P(data.GetFields())
		fields := data.GetFields()
		// g.P(data.Records()[0])
		for i, val := range data.Rows[0] {
			println(g.F("%d - %s - %s", i+1, fields[i], cast.ToString(val)))
		}
	}
}

func TestSurveyMonkey(t *testing.T) {

	api, err := NewAPI(SurveyMonkey)
	if !assert.NoError(t, err) {
		return
	}
	// return

	// params := map[string]interface{}{
	// 	"owner": "slingdata",
	// 	"repo":  "sling",
	// }
	// ds, err := api.Stream("commits", params, nil)
	ds, err := api.Stream("surveys", nil, nil)
	if !assert.NoError(t, err) {
		return
	}

	data, err := ds.Collect(0)
	assert.NoError(t, err)
	g.P(len(data.Rows))
	g.P(len(data.Columns))
	if len(data.Rows) > 0 {
		g.P(data.GetFields())
		fields := data.GetFields()
		// g.P(data.Records()[0])
		for i, val := range data.Rows[0] {
			println(g.F("%d - %s - %s", i+1, fields[i], cast.ToString(val)))
		}
	}
}
