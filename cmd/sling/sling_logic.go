package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/integrii/flaggy"
	"github.com/manifoldco/promptui"
	"gopkg.in/yaml.v2"

	"github.com/flarco/g/net"
	core2 "github.com/flarco/sling/core"
	"github.com/flarco/sling/core/env"
	"github.com/flarco/sling/core/sling"

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
)

func init() {
	if masterServerURL == "" {
		masterServerURL = "https://api.slingdata.io"
	}
}

func getJWT() {
	// login and get JWT
	respStr, err := ClientPost(
		masterServerURL, "/app/login",
		g.M("password", key), map[string]string{"Content-Type": "application/json"},
	)
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
	cfg := sling.Config{}
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
			cfg.Target.PrimaryKey = strings.Split(cast.ToString(v), ",")

		case "update-key":
			cfg.Target.UpdateKey = cast.ToString(v)

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
			cfg.Target.Mode = sling.Mode(cast.ToString(v))
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
	task.Ctx = ctx

	// track usage
	defer func() {
		props := g.M(
			"cmd", "exec",
			"error", g.ErrMsgSimple(task.Err),
			"job_type", task.Type,
			"job_mode", task.Config.Target.Mode,
			"job_status", task.Status,
			"job_src_type", task.Config.SrcConn.Type,
			"job_tgt_type", task.Config.TgtConn.Type,
			"job_start_time", task.StartTime,
			"job_end_time", task.EndTime,
			"job_rows_count", task.GetCount(),
			"job_rows_bytes", task.GetBytes(),
		)
		Track("task.Execute", props)
	}()

	// run task
	err = task.Execute()
	if err != nil {
		return ok, g.Error(err)
	}

	return ok, nil
}

func processAuthLogin(c *g.CliSC) (ok bool, err error)  { return }
func processAuthLogout(c *g.CliSC) (ok bool, err error) { return }
func processAuthSignUp(c *g.CliSC) (ok bool, err error) { return }
func processAuthToken(c *g.CliSC) (ok bool, err error)  { return }

func processProjectConns(c *g.CliSC) (ok bool, err error) {
	switch c.Name {
	case "set":
	case "unset":
	case "list":
	case "test":
	}
	return
}
func processProjectVars(c *g.CliSC) (ok bool, err error) { return }

func processProjectTasksList(c *g.CliSC) (ok bool, err error)   { return }
func processProjectTasksShow(c *g.CliSC) (ok bool, err error)   { return }
func processProjectTasksToggle(c *g.CliSC) (ok bool, err error) { return }
func processProjectTasksTrigger(c *g.CliSC) (ok bool, err error) {
	ok = true
	g.P(c.Vals)
	return
}
func processProjectTasksTerminate(c *g.CliSC) (ok bool, err error) { return }
func processProjectTasksGenerate(c *g.CliSC) (ok bool, err error) {
	/*
		- Choose source connection
		- Choose target connection
		- Choose target schema or folder
		- Choose source streams (schema, tables), checkbox?
				- include expression (comma separated): `mkg_*,finance_*`  or `*`
				- exclude expression (comma separated): `eng*`
		- Default schedule
	*/

	items := []string{"Vim", "Emacs", "Sublime", "VSCode", "Atom"}
	index := -1
	var result string

	for index < 0 {
		prompt := promptui.SelectWithAdd{
			Label:    "What's your text editor",
			Items:    items,
			AddLabel: "Other",
		}

		index, result, err = prompt.Run()

		if index == -1 {
			items = append(items, result)
		}
	}

	if err != nil {
		fmt.Printf("Prompt failed %v\n", err)
		return
	}

	fmt.Printf("You choose %s\n", result)
	return
}

func processProjectWorkersList(c *g.CliSC) (ok bool, err error)   { return }
func processProjectWorkersAttach(c *g.CliSC) (ok bool, err error) { return }
func processProjectWorkersDetach(c *g.CliSC) (ok bool, err error) { return }

func processProjectValidate(c *g.CliSC) (ok bool, err error) { return }
func processProjectDeploy(c *g.CliSC) (ok bool, err error)   { return }
func processProjectHistory(c *g.CliSC) (ok bool, err error)  { return }
func processProjectLogs(c *g.CliSC) (ok bool, err error)     { return }

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

		conn, ok := conns[strings.ToLower(name)]
		if !ok || name == "" {
			g.Info("Invalid Connection name: %s", name)
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

func updateCLI(c *g.CliSC) (ok bool, err error) {
	// Print Progress: https://gist.github.com/albulescu/e61979cc852e4ee8f49c
	Track("updateCLI")
	ok = true

	url := ""
	if runtime.GOOS == "linux" {
		url = "https://f.slingdata.io/linux/sling"
	} else if runtime.GOOS == "darwin" {
		url = "https://f.slingdata.io/mac/sling"
	} else if runtime.GOOS == "windows" {
		url = "https://f.slingdata.io/windows/sling.exe"
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

	g.Info("Checking update...")
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

	versionOut, err := exec.Command(filePath, "--version").Output()
	if err != nil {
		println("Unable to get version of current binary executable.")
		return ok, err
	}

	if strings.HasSuffix(strings.TrimSpace(string(versionOut)), core2.Version) {
		os.Remove(filePath)
		g.Info("Already using latest version: " + core2.Version)
		return ok, nil
	}

	os.Rename(execFileName, execFileName+".old")
	os.Rename(filePath, execFileName)

	os.Remove(execFileName + ".old")

	g.Info("Updated to " + strings.TrimSpace(string(versionOut)))

	return ok, nil
}
