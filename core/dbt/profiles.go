package dbt

import (
	"io/ioutil"
	"os"

	"github.com/spf13/cast"

	"github.com/flarco/dbio"
	"github.com/flarco/g/net"

	"github.com/flarco/dbio/connection"

	"github.com/flarco/dbio/local"
	g "github.com/flarco/g"
	"gopkg.in/yaml.v2"
)

type ProfileConn struct {
	Target  string           `yaml:"target"`
	Outputs map[string]g.Map `yaml:"outputs"`
}

type profileConfig struct {
	SendAnonymousUsageStats bool `yaml:"send_anonymous_usage_stats"`
	UseColors               bool `yaml:"use_colors"`
}

// generateProfile creates the connection profile YAML file
func (d *Dbt) generateProfile(conns []connection.Connection) (err error) {
	filepath := g.F("%s/profiles.yml", d.HomePath)
	defTarget := "main"
	prof := g.M()
	prof["config"] = profileConfig{false, false}
	if cast.ToFloat64(d.Version) >= 17.0 {
		prof["config-version"] = 1
		if cast.ToFloat64(d.Version) >= 19.0 {
			prof["config-version"] = 2
		}
	}

	home, err := local.GetHome()
	if err != nil {
		err = g.Error(err, "could not obtain sling home folder")
		return
	}

	connsLocal, err := home.Profile.ListConnections(true)
	g.LogError(err, "could not obtain local connections")
	conns = append(conns, connsLocal...)

	for _, conn := range conns {
		if !conn.Info().Type.IsDb() {
			continue
		}
		pe, err := d.getProfileEntry(conn)
		if err != nil {
			err = g.Error(err, "could not obtain profile entry for "+conn.Info().Name)
			return err
		}
		prof[conn.Info().Name] = ProfileConn{
			Target: defTarget,
			Outputs: map[string]g.Map{
				defTarget: pe,
			},
		}
	}

	yamlStr, err := yaml.Marshal(prof)
	if err != nil {
		err = g.Error(err, "could not encode dbt profile")
		return
	}

	err = ioutil.WriteFile(filepath, yamlStr, 0600)
	if err != nil {
		err = g.Error(err, "could not write dbt profile")
	}
	return
}

func (d *Dbt) getProfileEntry(conn connection.Connection) (pe map[string]interface{}, err error) {

	pe = conn.Info().Data
	pe["type"] = conn.Info().Type.String()

	u, err := net.NewURL(conn.URL())
	if err != nil {
		return pe, g.Error("could not parse url")
	}

	pe["user"] = pe["username"]
	pe["pass"] = pe["password"]
	pe["dbname"] = pe["database"]
	pe["threads"] = 3

	delete(pe, "username")
	delete(pe, "password")
	delete(pe, "database")
	delete(pe, "url")

	if d.Schema != "" {
		pe["schema"] = d.Schema
	}
	switch conn.Info().Type {
	case dbio.TypeDbPostgres:
		pe["sslmode"] = u.GetParam("sslmode")
	case dbio.TypeDbRedshift:
		pe["sslmode"] = u.GetParam("sslmode")
	case dbio.TypeDbOracle:
	case dbio.TypeDbSQLServer:
		pe["server"] = pe["host"]
		pe["driver"] = "ODBC Driver 17 for SQL Server" // need to ensure the ODBC driver is installable on image
		delete(pe, "host")

	case dbio.TypeDbBigQuery:
		pe["project"] = u.GetParam("GC_CRED_FILE")
		pe["project"] = u.GetParam("PROJECT_ID")
		pe["location"] = u.GetParam("location")
		pe["dataset"] = pe["schema"]
		pe["method"] = "service-account"

		// write the service json key to dbt folder
		jsonBody := conn.DataS()["GC_CRED_JSON_BODY"]
		if jsonBody == "" {
			jsonBody = os.Getenv("GC_CRED_JSON_BODY")
		}
		if jsonBody == "" {
			bytes, _ := ioutil.ReadFile(u.GetParam("GC_CRED_FILE"))
			jsonBody = string(bytes)
		}

		if jsonBody != "" {
			filePath := g.F("%s/bigquery.%s.json", d.HomePath, pe["project"])
			err = ioutil.WriteFile(filePath, []byte(jsonBody), 0600)
			if err != nil {
				err = g.Error(err, "could not write bigquery json: "+filePath)
				return
			}
			pe["keyfile"] = filePath
		}

		delete(pe, "port")
		delete(pe, "schema")
		delete(pe, "dbname")
		delete(pe, "pass")
		delete(pe, "user")

	case dbio.TypeDbSnowflake:
		pe["account"] = pe["host"]
		pe["password"] = pe["pass"]
		pe["database"] = pe["dbname"]
		pe["warehouse"] = u.GetParam("warehouse")
		delete(pe, "port")
		delete(pe, "pass")
		delete(pe, "dbname")
		delete(pe, "host")
	}

	return
}
