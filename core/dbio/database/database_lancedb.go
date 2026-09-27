package database

import (
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// LanceDB exposes Lance datasets as a SQL database by way of DuckDB's `lance`
// extension: the namespace root is attached as a DuckDB catalog, so every
// `<table>.lance` dataset under the root is a table in its `main` schema.
// The DuckDB dialect, type mapping and merge strategies therefore apply.
// See https://duckdb.org/docs/current/core_extensions/lance.html

// lanceDBNamespace is the catalog name the namespace root is attached as.
const lanceDBNamespace = "lancedb"

// LanceDBConn is a LanceDB connection
type LanceDBConn struct {
	DuckDbConn

	Path string // namespace root: local directory or object store URI
}

// Init initiates the object
func (conn *LanceDBConn) Init() error {
	conn.Path = conn.GetProp("path")
	if conn.Path == "" {
		conn.Path = conn.GetProp("instance")
	}
	if strings.TrimSpace(conn.Path) == "" {
		return g.Error("did not provide 'path' for LanceDB connection (the namespace root)")
	}
	conn.SetProp("path", conn.Path)

	// the DuckDB process runs in memory and the namespace is attached to it,
	// so `instance` must not become the process's database file
	conn.SetProp("instance", "")

	if cast.ToBool(conn.GetProp("read_only")) {
		// the DuckDB CLI cannot launch its in-memory database read-only, and
		// the lance extension rejects a READ_ONLY attach
		return g.Error("the `read_only` property is not supported for LanceDB connections")
	}

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbLanceDB

	// initialize DuckDB instance (inheriting from DuckDbConn)
	conn.duck = iop.NewDuckDb(conn.Context().Ctx, g.MapToKVArr(conn.properties)...)

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// Connect establishes the LanceDB connection
func (conn *LanceDBConn) Connect(timeOut ...int) (err error) {
	// the namespace is attached to an in-memory DuckDB instance
	conn.DuckDbConn.URL = "duckdb::memory:"

	if err = conn.DuckDbConn.Connect(timeOut...); err != nil {
		return g.Error(err, "could not connect to DuckDB for LanceDB")
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	// the lance extension provides Lance dataset read/write
	conn.duck.AddExtension("lance")

	if conn.isObjectStore() {
		// the lance extension requires httpfs to reach object stores
		conn.duck.AddExtension("httpfs")
		if secret, ok := conn.makeSecret(); ok {
			conn.duck.AddSecret(secret)
		}
	}
	// a local namespace root is created by the extension on first write

	if _, err = conn.Exec(conn.buildAttachSQL() + noDebugKey); err != nil {
		return g.Error(err, "could not attach LanceDB namespace: %s", conn.Path)
	}

	// make the namespace the default catalog, so unqualified tables resolve to it
	if _, err = conn.Exec("USE " + lanceDBNamespace + ";" + noDebugKey); err != nil {
		return g.Error(err, "could not use LanceDB namespace")
	}

	return nil
}

// buildAttachSQL creates the ATTACH statement for the namespace root
func (conn *LanceDBConn) buildAttachSQL() string {
	path := strings.ReplaceAll(conn.Path, "'", "''")
	return g.F("ATTACH IF NOT EXISTS '%s' AS %s (TYPE lance)", path, lanceDBNamespace)
}

// GetURL returns the processed URL
func (conn *LanceDBConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	return connURL
}

// isObjectStore returns whether the namespace root lives in an object store
func (conn *LanceDBConn) isObjectStore() bool {
	switch conn.objectStoreScheme() {
	case "", "file":
		return false
	}
	return true
}

// objectStoreScheme returns the object store family of the namespace root. Only
// the schemes the lance extension knows are mapped: it accepts `s3` (plus the
// `s3a` / `s3n` aliases), `gs` and `az` (plus the `abfss` alias). Any other
// scheme is returned as-is, so that an unsupported one (e.g. `r2://`) surfaces
// the extension's own error instead of being silently rewritten.
func (conn *LanceDBConn) objectStoreScheme() string {
	scheme, _, ok := strings.Cut(strings.ToLower(conn.Path), "://")
	if !ok {
		return ""
	}
	switch scheme {
	case "s3", "s3a", "s3n":
		return "s3"
	case "gs":
		return "gs"
	case "az", "abfss":
		return "az"
	default:
		return scheme
	}
}

// lanceScope returns the secret scope of the namespace root: the URI up to the
// end of its bucket / container (e.g. `s3://my-bucket/`). Secrets are matched
// by URI prefix, and datasets live below the namespace root.
func (conn *LanceDBConn) lanceScope() string {
	path := strings.ReplaceAll(conn.Path, "\\", "/")
	scheme, rest, ok := strings.Cut(path, "://")
	if !ok {
		return path
	}
	bucket, _, _ := strings.Cut(rest, "/")
	return g.F("%s://%s/", scheme, bucket)
}

// makeSecret builds the scoped Lance secret used to reach an object store.
// Credentials are taken from the connection properties when provided, and fall
// back to the upstream SDK credential chain otherwise. It returns false for
// object stores whose credentials the extension resolves on its own (e.g. OSS,
// Hugging Face Hub), where a secret would only get in the way.
func (conn *LanceDBConn) makeSecret() (iop.DuckDbSecret, bool) {
	props := map[string]string{"scope": conn.lanceScope()}

	switch conn.objectStoreScheme() {
	case "s3":
		accessKey := conn.GetProp("s3_access_key_id")
		secretKey := conn.GetProp("s3_secret_access_key")
		if accessKey == "" || secretKey == "" {
			props["provider"] = "credential_chain"
		} else {
			props["provider"] = "config"
			props["access_key_id"] = accessKey
			props["secret_access_key"] = secretKey
			if val := conn.GetProp("s3_session_token"); val != "" {
				props["session_token"] = val
			}
		}
		if val := conn.GetProp("s3_region"); val != "" {
			props["region"] = val
		}
		if val := conn.GetProp("s3_endpoint"); val != "" {
			props["endpoint"] = val
			props["allow_http"] = cast.ToString(strings.HasPrefix(val, "http://"))
		}
	case "gs":
		props["provider"] = "credential_chain"
		if val := conn.GetProp("gcs_key_file"); val != "" {
			props["provider"] = "config"
			props["google_application_credentials"] = val
		}
	case "az":
		props["provider"] = "config"
		if val := conn.GetProp("azure_account_name"); val != "" {
			props["account_name"] = val
		}
		if val := conn.GetProp("azure_account_key"); val != "" {
			props["account_key"] = val
		} else if val := conn.GetProp("azure_sas_token"); val != "" {
			props["sas_token"] = strings.TrimPrefix(val, "?")
		} else {
			props["provider"] = "credential_chain"
		}
	default:
		return iop.DuckDbSecret{}, false
	}

	return iop.NewDuckDbSecret("lance_secret", iop.DuckDbSecretType("lance"), props), true
}
