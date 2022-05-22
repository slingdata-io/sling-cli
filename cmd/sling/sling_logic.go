package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/integrii/flaggy"
	"gopkg.in/yaml.v2"

	"github.com/flarco/g/net"
	core2 "github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"

	"github.com/flarco/g"
	"github.com/jedib0t/go-pretty/table"
	"github.com/kardianos/osext"
	"github.com/spf13/cast"
)

var (
	masterServerURL = os.Getenv("SLING_MASTER_URL")
	masterSecret    = os.Getenv("SLING_MASTER_SECRET")
	headers         = map[string]string{
		"Content-Type": "application/json",
	}
	apiTokenFile = path.Join(env.HomeDir, ".api_token")
)

func init() {
	if masterServerURL == "" {
		masterServerURL = "https://api.slingdata.io"
	}
}

func setJWT() {
	if a := headers["Authorization"]; a != "" {
		return
	}

	// login and get JWT
	apiTokenBytes, _ := ioutil.ReadFile(apiTokenFile)
	apiToken := string(apiTokenBytes)
	if apiToken == "" {
		g.LogFatal(g.Error("no token cached. Are you logged in?"))
	}

	respStr, err := sling.ClientPost(
		masterServerURL, "/app/login",
		g.M("password", apiToken), headers)
	g.LogFatal(err)

	m := g.M()
	err = json.Unmarshal([]byte(respStr), &m)
	g.LogFatal(err, "could not parse token response")

	jwt := cast.ToString(cast.ToStringMap(m["user"])["token"])
	if jwt == "" {
		g.LogFatal(g.Error("blank token"))
	}

	if headers["Authorization"] == "" {
		headers["Authorization"] = "Bearer " + jwt
	}
}

func processRun(c *g.CliSC) (ok bool, err error) {
	ok = true
	cfg := &sling.Config{}
	cfg.SetDefault()
	cfgStr := ""
	showExamples := false
	// saveAsJob := false

	// determine if stdin data is piped
	// https://stackoverflow.com/a/26567513
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		cfg.Options.StdIn = true
	}

	for k, v := range c.Vals {
		switch k {
		case "config":
			cfgStr = cast.ToString(v)
		case "src-conn":
			cfg.Source.Conn = cast.ToString(v)
		case "src-stream", "src-table", "src-sql", "src-file":
			cfg.Source.Stream = cast.ToString(v)
			if strings.Contains(cfg.Source.Stream, "://") {
				if _, ok := c.Vals["src-conn"]; !ok { // src-conn not specified
					cfg.Source.Conn = cfg.Source.Stream
				}
			}
		case "src-options":
			err = yaml.Unmarshal([]byte(cast.ToString(v)), &cfg.Source.Options)
			if err != nil {
				return ok, g.Error(err, "invalid source options -> %s", cast.ToString(v))
			}

		case "tgt-conn":
			cfg.Target.Conn = cast.ToString(v)

		case "primary-key":
			cfg.Source.PrimaryKey = strings.Split(cast.ToString(v), ",")

		case "update-key":
			cfg.Source.UpdateKey = cast.ToString(v)

		case "tgt-object", "tgt-table":
			cfg.Target.Object = cast.ToString(v)
			if strings.Contains(cfg.Target.Object, "://") {
				if _, ok := c.Vals["tgt-conn"]; !ok { // tgt-conn not specified
					cfg.Target.Conn = cfg.Target.Object
				}
			}
		case "tgt-options":
			stdout := cfg.Options.StdOut
			err = yaml.Unmarshal([]byte(cast.ToString(v)), &cfg.Target.Options)
			if err != nil {
				return ok, g.Error(err, "invalid target options -> %s", cast.ToString(v))
			}
			if stdout {
				cfg.Options.StdOut = stdout
			}
		case "stdout":
			cfg.Options.StdOut = cast.ToBool(v)
		case "mode":
			cfg.Mode = sling.Mode(cast.ToString(v))
		case "debug":
			cfg.Options.Debug = cast.ToBool(v)
		case "examples":
			showExamples = cast.ToBool(v)
		}
	}

	if showExamples {
		println(examples)
		return ok, nil
	}

	if cfgStr != "" {
		err = cfg.Unmarshal(cfgStr)
	} else {
		err = cfg.Prepare()
	}
	if err != nil {
		return ok, g.Error(err, "Unable to parse config string")
	}

	task := sling.NewTask(0, cfg)
	if task.Err != nil {
		return ok, g.Error(task.Err)
	}

	// set context
	task.Context = &ctx

	// track usage
	defer func() {
		inBytes, outBytes := task.GetBytes()
		props := g.M(
			"cmd", "exec",
			"error", g.ErrMsgSimple(task.Err),
			"job_type", task.Type,
			"job_mode", task.Config.Mode,
			"job_status", task.Status,
			"job_src_type", task.Config.SrcConn.Type,
			"job_tgt_type", task.Config.TgtConn.Type,
			"job_start_time", task.StartTime,
			"job_end_time", task.EndTime,
			"job_rows_count", task.GetCount(),
			"job_rows_in_bytes", inBytes,
			"job_rows_out_bytes", outBytes,
		)
		Track("task.Execute", props)
	}()

	// run task
	err = task.Execute()
	if err != nil {
		return ok, g.Error(err)
	}

	// g.PP(task.ProcStatsStart)
	// g.PP(g.GetProcStats(os.Getpid()))

	return ok, nil
}

func processConns(c *g.CliSC) (bool, error) {
	ok := true
	switch c.UsedSC() {
	case "add", "new":
		if len(c.Vals) == 0 {
			flaggy.ShowHelp("")
			return ok, nil
		}
	case "list", "show":
		conns := env.GetLocalConns()
		T := table.NewWriter()
		T.AppendHeader(table.Row{"Conn Name", "Conn Type", "Source"})
		for _, conn := range conns {
			T.AppendRow(table.Row{conn.Name, conn.Description, conn.Source})
		}
		println(T.Render())
	case "test":
		if len(c.Vals) == 0 {
			flaggy.ShowHelp("")
			return ok, nil
		}
		name := cast.ToString(c.Vals["name"])
		conns := map[string]env.Conn{}
		for _, conn := range env.GetLocalConns() {
			conns[strings.ToLower(conn.Name)] = conn
		}

		conn, ok1 := conns[strings.ToLower(name)]
		if !ok1 || name == "" {
			g.Warn("Invalid Connection name: %s", name)
			return ok, nil
		}

		switch {
		case conn.Connection.Type.IsDb():
			dbConn, err := conn.Connection.AsDatabase()
			if err != nil {
				return ok, g.Error(err, "could not initiate %s", name)
			}
			err = dbConn.Connect()
			if err != nil {
				return ok, g.Error(err, "could not connect to %s", name)
			}
			g.Info("success!")
		case conn.Connection.Type.IsFile():
			fileClient, err := conn.Connection.AsFile()
			if err != nil {
				return ok, g.Error(err, "could not initiate %s", name)
			}
			err = fileClient.Init(context.Background())
			if err != nil {
				return ok, g.Error(err, "could not connect to %s", name)
			}
			g.Info("success!")
		case conn.Connection.Type.IsAirbyte():
			client, err := conn.Connection.AsAirbyte()
			if err != nil {
				return ok, g.Error(err, "could not initiate %s", name)
			}
			err = client.Init()
			if err != nil {
				return ok, g.Error(err, "could not connect to %s", name)
			}
			g.Info("success!")
		default:
			g.Warn("Unhandled connection type: %s", conn.Connection.Type)
		}

	case "":
		flaggy.ShowHelp("")
	}
	return ok, nil
}

func slingUiServer(c *g.CliSC) (ok bool, err error) {
	Track("slingUiServer")
	ok = true
	return
}

func checkLatestVersion() (string, error) {
	url := ""
	if runtime.GOOS == "linux" {
		url = "https://files.ocral.org/slingdata.io/dist/version-linux"
	} else if runtime.GOOS == "darwin" {
		url = "https://files.ocral.org/slingdata.io/dist/version-mac"
	} else if runtime.GOOS == "windows" {
		url = "https://files.ocral.org/slingdata.io/dist/version-win"
	} else {
		return "", g.Error("OS Unsupported: %s", runtime.GOOS)
	}

	_, respBytes, err := net.ClientDo("GET", url, nil, map[string]string{}, 5)
	newVersion := strings.TrimSpace(string(respBytes))
	if err != nil {
		return "", g.Error(err, "Unable to check for latest version")
	}
	return newVersion, nil
}

func updateCLI(c *g.CliSC) (ok bool, err error) {
	// Print Progress: https://gist.github.com/albulescu/e61979cc852e4ee8f49c
	Track("updateCLI")
	ok = true

	// get latest version number
	newVersion, err := checkLatestVersion()
	if err != nil {
		return ok, g.Error(err)
	} else if newVersion == core2.Version {
		g.Info("Already up-to-date!")
		return
	}

	url := ""
	if runtime.GOOS == "linux" {
		url = "https://files.ocral.org/slingdata.io/dist/sling-linux"
	} else if runtime.GOOS == "darwin" {
		url = "https://files.ocral.org/slingdata.io/dist/sling-mac"
	} else if runtime.GOOS == "windows" {
		url = "https://files.ocral.org/slingdata.io/dist/sling-win.exe"
	} else {
		return ok, g.Error("OS Unsupported: %s", runtime.GOOS)
	}

	execFileName, err := osext.Executable()
	if err != nil {
		return ok, g.Error(err, "Unable to determine executable path")
	}

	fileStat, _ := os.Stat(execFileName)
	fileMode := fileStat.Mode()

	filePath := execFileName + ".new"

	g.Info("Downloading latest version (%s)", newVersion)
	err = net.DownloadFile(url, filePath)
	if err != nil {
		println("Unable to download update!")
		return ok, g.Error(strings.ReplaceAll(err.Error(), url, ""))
	}
	err = os.Chmod(filePath, fileMode)
	if err != nil {
		println("Unable to make new binary executable.")
		return ok, err
	}

	err = os.Rename(execFileName, execFileName+".old")
	if err != nil {
		println("Unable to rename current binary executable. Try with sudo or admin?")
		return ok, err
	}

	err = os.Rename(filePath, execFileName)
	if err != nil {
		println("Unable to rename current binary executable. Try with sudo or admin?")
		return ok, err
	}

	os.Remove(execFileName + ".old")

	g.Info("Updated to " + strings.TrimSpace(string(newVersion)))

	return ok, nil
}
