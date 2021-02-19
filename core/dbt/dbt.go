package dbt

import (
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/flarco/dbio/connection"

	"github.com/flarco/dbio/local"
	g "github.com/flarco/g"
	"github.com/flarco/g/process"
	"gopkg.in/yaml.v2"
)

// Dbt represents a dbt instance
type Dbt struct {
	Version string `json:"version" yaml:"version"`
	RepoURL string `json:"repo_url" yaml:"repo_url"`
	Models  string `json:"models" yaml:"models"`

	ProjectRoot  string `json:"project_root,omitempty" yaml:"project_root,omitempty"`
	Debug        bool   `json:"debug,omitempty" yaml:"debug,omitempty"`
	Schema       string `json:"schema,omitempty" yaml:"schema,omitempty"`
	ProjectPath  string `json:"project_path,omitempty" yaml:"project_path,omitempty"`
	RepoUser     string `json:"repo_user,omitempty" yaml:"repo_user,omitempty"`
	RepoPassword string `json:"repo_password,omitempty" yaml:"repo_password,omitempty"`

	Profile   string           `json:"-" yaml:"-"` // the name of the connection
	HomePath  string           `json:"-" yaml:"-"`
	Session   *process.Session `json:"-" yaml:"-"`
	Manifest  Manifest         `json:"-" yaml:"-"`
	RunResult RunResult        `json:"-" yaml:"-"`
}

// NewDbt creates a new DBT instance
func NewDbt(dbtConfig, profile string) (d *Dbt, err error) {
	d = new(Dbt)
	err = yaml.Unmarshal([]byte(dbtConfig), &d)
	if err != nil {
		err = g.Error(err, "could not parse dbt config")
		return
	}

	// in case this is referring to a env var.
	d.Profile = strings.TrimPrefix(profile, "$")

	// create process seesion
	d.Session = process.NewSession()

	return
}

// Init initializes the DBT Project and generates profiles
func (d *Dbt) Init(conns ...connection.Connection) (err error) {
	// clone git repo
	if d.ProjectPath == "" {
		// will clone into ~/.sling/repos/<owner>/<repo>
		repoURL := d.RepoURL
		if d.RepoUser != "" {
			arrURL := strings.Split(repoURL, "://")
			if len(arrURL) != 2 {
				return g.Error("Invalid DBT Repo URL")
			}
			repoURL = g.F("%s://%s:%s@%s", arrURL[0], d.RepoUser, d.RepoPassword, arrURL[1])
		}
		err = d.CloneRepo(repoURL)
		if err != nil {
			err = g.Error(err, "could not sync from git repo")
			return
		}
	} else if !g.PathExists(d.ProjectPath) {
		err = g.Error("proj path '%s' doesn't exist", d.ProjectPath)
		return
	}

	venvPath := g.F("%s/.venv", d.ProjectPath)
	d.Session.AddAlias("dbt", g.F("%s/bin/dbt", venvPath))
	d.Session.AddAlias("$DBT_PROJ", d.ProjectPath)

	err = d.Install(venvPath)
	if err != nil {
		err = g.Error(err, "unable to install via python's pip")
		return
	}

	d.Session.Workdir = d.ProjectPath
	d.HomePath = g.F("%s/.dbt", d.ProjectPath) // .dbt in each project path

	err = os.MkdirAll(d.HomePath, os.ModeExclusive)
	if err != nil {
		err = g.Error(err, "could not create dbt home folder")
	}

	err = d.generateProfile(conns)
	if err != nil {
		err = g.Error(err, "could not generate profiles.yml")
	}
	return
}

// CollectTarget collects manifest.json & run_result.json
func (d *Dbt) CollectTarget(targetPath string) (err error) {
	bytes, err := ioutil.ReadFile(g.F("%s/manifest.json", targetPath))
	if err != nil {
		return g.Error(err)
	}
	g.Unmarshal(string(bytes), &d.Manifest)

	bytes, err = ioutil.ReadFile(g.F("%s/run_results.json", targetPath))
	g.Unmarshal(string(bytes), &d.RunResult)
	if err != nil {
		return g.Error(err)
	}
	return
}

// Compile executes the dbt compile command
func (d *Dbt) Compile() (err error) {
	args := []string{
		"compile",
		"--profiles-dir", d.HomePath,
		"--profile", d.Profile,
		"-m", d.Models,
	}
	if d.Debug {
		args = append([]string{"-d"}, args...)
	}

	err = d.Session.Run("dbt", args...)
	if err != nil {
		err = g.Error(err, "could not run 'dbt compile'")
	} else {
		d.CollectTarget(g.F("%s/target", d.ProjectPath))
	}
	return
}

// Start executes the dbt command
func (d *Dbt) Start() (err error) {
	args := []string{
		"run",
		"--fail-fast",
		"--profiles-dir", d.HomePath,
		"--profile", d.Profile,
		"-m", d.Models,
	}
	if d.Debug {
		args = append([]string{"-d"}, args...)
	}

	err = d.Session.Run("dbt", args...)
	if err != nil {
		err = g.Error(err, "could not start dbt task")
	}
	return
}

// Run executes the dbt command and waits
func (d *Dbt) Run() (err error) {
	args := []string{
		"run",
		"--fail-fast",
		"--profiles-dir", d.HomePath,
		"--profile", d.Profile,
		"-m", d.Models,
	}
	if d.Debug {
		args = append([]string{"-d"}, args...)
	}
	err = d.Session.Run("dbt", args...)
	if err != nil {
		err = g.Error(err, "could not run dbt task")
	} else {
		d.CollectTarget(g.F("%s/target", d.ProjectPath))
	}
	return
}

// Install attempts to install dbt in a virtual env
func (d *Dbt) Install(venvPath string) (err error) {

	// Create virtual env
	err = d.Session.Run("python", "-m", "venv", venvPath)
	if err != nil {
		if strings.Contains(err.Error(), "No module named venv") {
			return g.Error(err, "Please use a recent version of Python 3")
		}
		return g.Error(err, "could not initiate dbt virtual environment")
	}

	dbtPkg := "dbt"
	if d.Version != "" {
		dbtPkg = g.F("dbt==%s", d.Version)
	}

	pip := g.F("%s/bin/pip", venvPath)
	g.Debug("ensuring pip package is '%s'", dbtPkg)
	err = d.Session.Run(pip, "install", "-q", "-U", "--disable-pip-version-check", dbtPkg)
	if err != nil {
		err = g.Error(err, "could not install dbt via python pip")
	}

	// install cx_Oracle for oracle compatibility ?
	// is unstable when tested in Oct 2020. removing from prod code.
	// err = d.Session.Run(pip, "install", "dbt-oracle")
	// if err != nil {
	// 	err = g.Error(err, "could not install dbt via python pip")
	// }

	return
}

// CloneRepo clones a Git repository from. Returns the repo local path
func (d *Dbt) CloneRepo(URL string) (err error) {

	home, err := local.GetHome()
	if err != nil {
		err = g.Error(err, "could not obtain sling home folder")
		return
	}

	if URL == "" {
		err = g.Error("did not provide repository URL")
		return
	}

	// get owner for local folder path
	u, err := url.Parse(URL)
	if err != nil {
		err = g.Error(err, "could not parse Git URL provided")
		return
	}

	path := g.F("%s/repos%s", home.Path, u.Path)
	path = strings.TrimSuffix(path, ".git")
	d.Session.AddAlias("$DBT_PROJ", path)

	doClone := true
	if g.PathExists(path) {
		// path exists, pull instead of clone
		d.Session.Workdir = path
		err := d.Session.Run("git", "pull")
		if err != nil {
			// try clone after deleting path
			os.RemoveAll(path)
		} else {
			doClone = false
		}
	}

	if doClone {
		err = d.Session.Run("git", "clone", URL, path)
		if err != nil {
			os.RemoveAll(path) // delete path since it is created anyways
			err = g.Error(err, "could not run 'git clone'")
			return
		}
	}

	d.ProjectPath = path
	d.ProjectRoot = strings.TrimPrefix(d.ProjectRoot, "/")
	if d.ProjectRoot != "" {
		d.ProjectPath = g.F("%s/%s", d.ProjectPath, d.ProjectRoot)
	}

	return
}
