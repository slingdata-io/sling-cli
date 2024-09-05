package database

import (
	"context"
	"crypto/tls"
	"database/sql"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/trinodb/trino-go-client/trino"
	_ "github.com/trinodb/trino-go-client/trino"
)

// TrinoConn is a Trino connection
type TrinoConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *TrinoConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbTrino

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	// register client to skip tls
	skipTLSClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	trino.RegisterCustomClient("skip_tls", skipTLSClient)

	return conn.BaseConn.Init()
}

func (conn *TrinoConn) ConnString() string {

	configMap := g.M()

	propMapping := map[string]string{
		"source":               "Source",
		"catalog":              "Catalog",
		"schema":               "Schema",
		"session_properties":   "SessionProperties",
		"extra_credentials":    "ExtraCredentials",
		"custom_client_name":   "CustomClientName",
		"kerberos_enabled":     "KerberosEnabled",
		"kerberos_keytab_path": "KerberosKeytabPath",
		"kerberos_principal":   "KerberosPrincipal",
		"kerberos_realm":       "KerberosRealm",
		"kerberos_config_path": "KerberosConfigPath",
		"ssl_cert_path":        "SSLCertPath",
		"ssl_cert":             "SSLCert",
	}

	for key, new_key := range propMapping {
		if val := conn.GetProp(key); val != "" {
			configMap[new_key] = val
		}
	}

	URI := g.F(
		"http://%s:%s@%s:%s",
		conn.GetProp("username"),
		conn.GetProp("password"),
		conn.GetProp("host"),
		conn.GetProp("port"),
	)
	config := trino.Config{ServerURI: URI}
	g.Unmarshal(g.Marshal(configMap), &config)

	if url := conn.GetProp("http_url"); url != "" {
		config.ServerURI = url
	}

	dsn, err := config.FormatDSN()
	if err != nil {
		g.Warn("invalid dsn: %s", err.Error())
	}

	return dsn
}

// NewTransaction creates a new transaction
func (conn *TrinoConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// does not support transactions
	return nil, nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *TrinoConn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	return conn.BaseConn.ExecContext(ctx, strings.TrimRight(strings.TrimSpace(q), ";"), args...)
}

func processTrinoInsertRow(columns iop.Columns, row []any) []any {
	for i := range row {
		if row[i] == nil {
			continue
		}

		if columns[i].Type == iop.DecimalType {
			row[i] = trino.Numeric(cast.ToString(row[i]))
		} else if columns[i].Type == iop.BoolType {
			row[i] = cast.ToBool(row[i])
		} else if columns[i].Type == iop.FloatType {
			row[i] = trino.Numeric(cast.ToString(row[i]))
		}
	}
	return row
}
