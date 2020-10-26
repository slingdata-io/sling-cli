package local

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/flarco/gutil"
	h "github.com/flarco/gutil"
	"github.com/flarco/gutil/process"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/spf13/cast"
	yaml "gopkg.in/yaml.v2"
)

// Home is the local home directory object of sling
type Home struct {
	Path        string
	Db          *sqlx.DB
	InstanceURL string
	Profile     Profile
	authPath    string
}

// Profile is the yaml file for profile
type Profile struct {
	Connections map[string]map[string]interface{} `yaml:"connections"`
}

var home *Home

// GetHome initializes the home folder
func GetHome() (homeObj *Home, err error) {
	if home != nil {
		return home, nil
	}

	homePath := os.Getenv("SLING_HOME")
	if homePath == "" {
		homePath = h.F("%s/.sling", h.UserHomeDir())
	}
	err = os.MkdirAll(homePath, os.ModeExclusive)
	if err != nil {
		err = h.Error(err, "could not initialize sling home dir: "+homePath)
		return
	}

	home = &Home{
		Path:     homePath,
		authPath: h.F("%s/auth.json", homePath),
	}

	err = home.parseProfile()
	if err != nil {
		err = h.Error(err)
		return
	}

	err = home.initDB()
	if err != nil {
		err = h.Error(err)
		return
	}

	return home, nil
}

func (home *Home) parseProfile() (err error) {
	profilePath := h.F("%s/profile.yaml", home.Path)
	if h.PathExists(profilePath) {

		cfgFile, err := os.Open(profilePath)
		if err != nil {
			return h.Error(err, "Unable to open profile: "+profilePath)
		}

		cfgBytes, err := ioutil.ReadAll(cfgFile)
		if err != nil {
			return h.Error(err, "could not read from home profile")
		}

		err = yaml.Unmarshal(cfgBytes, &home.Profile)
		if err != nil {
			return h.Error(err, "Error parsing profile")
		}
	} else {
		home.Profile = Profile{
			Connections: map[string]map[string]interface{}{},
		}
	}

	return
}

func (home *Home) initDB() (err error) {
	sqlitePath := h.F("%s/db.db", home.Path)
	home.Db, err = sqlx.Open("sqlite3", "file:"+sqlitePath)
	if err != nil {
		err = h.Error(err, "could not initialize sling home db: "+sqlitePath)
	}
	return
}

// Authenticate authenticates to a sling instance
func (home *Home) Authenticate(url string) (err error) {
	key := ""
	if url == "" {
		url = "https://api.slingdata.io"
	}
	// prompt for key
	authURL := h.F("%s/account/api", url)
	fmt.Printf("Go to the following link in your browser -> %s\nCopy your API key and paste it below.\n\nEnter API key: ", authURL)
	if _, err = fmt.Scanln(&key); err != nil {
		err = h.Error(err, "Unable to read api key")
		return
	}

	m := map[string]string{"key": key, "url": url}

	fileBytes, err := json.Marshal(m)
	if err != nil {
		err = h.Error(err, "Could not encode auth.json")
		return
	}

	err = ioutil.WriteFile(home.authPath, fileBytes, 0600)
	if err != nil {
		err = h.Error(err, "could not create sling auth.json")
	}

	home.InstanceURL = url
	return
}

// APIKey returns the API key
func (home *Home) APIKey() (key string) {
	key = os.Getenv("sling_API_KEY")
	if key == "" {
		fileBytes, err := ioutil.ReadFile(home.authPath)
		if err != nil {
			return
		}
		m := map[string]string{}
		json.Unmarshal(fileBytes, &m)
		if err != nil {
			h.LogError(fmt.Errorf("could not parse auth.json"))
			return
		}
		key = m["key"]
	}
	return
}

// CloneRepo clones a Git repository from. Returns the repo local path
func (home *Home) CloneRepo(URL string) (path string, err error) {

	if URL == "" {
		err = h.Error("did not provide repository URL")
		return
	}

	// get owner for local folder path
	u, err := url.Parse(URL)
	if err != nil {
		err = h.Error(err, "could not parse Git URL provided")
		return
	}

	path = h.F("%s/repos%s", home.Path, u.Path)
	path = strings.TrimSuffix(path, ".git")

	doClone := true
	if h.PathExists(path) {
		// path exists, pull instead of clone
		proc, err := process.NewProc("git", "pull")
		if err != nil {
			err = h.Error(err, "could not init 'git pull'")
			return "", err
		}
		proc.Workdir = path
		// proc.Print = true
		err = proc.Run()
		if err != nil {
			// try clone after deleting path
			os.RemoveAll(path)
		} else {
			doClone = false
		}
	}

	if doClone {
		proc, err := process.NewProc("git", "clone", URL, path)
		if err != nil {
			err = h.Error(err, "could not init 'git clone'")
			return "", err
		}
		// proc.Print = true
		err = proc.Run()
		if err != nil {
			err = h.Error(err, "could not run 'git clone'")
		}
	}

	return
}

// ListConnections returns an array of connections
func (p *Profile) ListConnections(includeEnv bool) (dcs []iop.DataConn, err error) {

	getConn := func(name string, connObj map[string]interface{}) (dc *iop.DataConn, err error) {
		URL, ok := connObj["url"]
		if !ok {
			err = fmt.Errorf("no url provided for profile connection: " + name)
			err = h.Error(err)
			return
		}
		delete(connObj, "url")
		vars := h.M()
		for k, v := range connObj {
			vars[strings.ToUpper(k)] = v
		}

		dc = iop.NewDataConnFromMap(h.M(
			"id", strings.ToUpper(name),
			"url", cast.ToString(URL),
			"vars", vars,
		))
		dc.SetFromEnv()

		// BigQuery: adjust path of service account json file
		if dc.GetType() == iop.ConnTypeDbBigQuery {
			if val, ok := vars["GC_CRED_FILE"]; ok {
				vars["GC_CRED_FILE"] = h.F("%s/%s", home.Path, val)
			}
		}

		return
	}

	dcs = []iop.DataConn{}
	for name, connObj := range p.Connections {
		dc, err := getConn(name, connObj)
		if err != nil {
			err = h.Error(err, "error parsing profile connection:"+name)
			return dcs, err
		}
		dcs = append(dcs, *dc)
	}

	// from Environment
	if includeEnv {
		for id, val := range gutil.KVArrToMap(os.Environ()...) {
			conn := iop.DataConn{ID: strings.ToUpper(id), URL: val}
			if conn.GetTypeKey() == "" || conn.GetType() == iop.ConnTypeFileLocal {
				continue
			}
			if conn.GetType() == iop.ConnTypeFileHTTP {
				continue
			}
			dcs = append(dcs, conn)
		}
	}

	return
}
