package main

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	"github.com/flarco/dbio/connection"
	"github.com/integrii/flaggy"

	"github.com/flarco/dbio"
	"github.com/flarco/g/net"
	core2 "github.com/slingdata-io/sling/core"
	"github.com/slingdata-io/sling/core/elt"

	"github.com/flarco/g"
	"github.com/jedib0t/go-pretty/table"
	"github.com/kardianos/osext"
	"github.com/spf13/cast"
)

type Conn struct {
	Name        string
	Description string
	Connection  connection.Connection
}

func getLocalConns() []Conn {
	conns := []Conn{}
	for key, val := range g.KVArrToMap(os.Environ()...) {
		if !strings.Contains(val, ":/") || strings.Contains(val, "{") {
			continue
		}
		conn, err := connection.NewConnectionFromURL(key, val)
		if err != nil {
			e := g.F("could not parse %s: %s", key, g.ErrMsgSimple(err))
			g.Warn(e)
			continue
		}

		if connection.GetTypeNameLong(conn) == "" || conn.Info().Type == dbio.TypeUnknown || conn.Info().Type == dbio.TypeFileHTTP {
			continue
		}
		conns = append(conns, Conn{conn.Info().Name, connection.GetTypeNameLong(conn), conn})
	}

	dbtConns, err := connection.ReadDbtConnections()
	if !g.LogError(err) {
		for _, conn := range dbtConns {
			conns = append(conns, Conn{conn.Info().Name, connection.GetTypeNameLong(conn) + " [dbt]", conn})
		}
	}

	sort.Slice(conns, func(i, j int) bool {
		return cast.ToString(conns[i].Name) < cast.ToString(conns[j].Name)
	})
	return conns
}

func processELT(c *g.CliSC) (err error) {

	cfg := elt.Config{}
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
		case "src-file":
			url := cast.ToString(v)
			if !strings.Contains(url, "://") {
				url = g.F("file://%s", url) // is local path
			}
			cfg.Source.Conn = url
			cfg.Source.Stream = url
		case "src-table", "src-sql", "query":
			cfg.Source.Stream = cast.ToString(v)
		case "tgt-file":
			url := cast.ToString(v)
			if !strings.Contains(url, "://") {
				url = g.F("file://%s", url) // is local path
			}
			cfg.Target.Conn = url
			cfg.Target.Object = url
		case "src-conn", "conn":
			cfg.Source.Conn = cast.ToString(v)
		case "tgt-conn":
			cfg.Target.Conn = cast.ToString(v)
		case "tgt-table":
			cfg.Target.Object = cast.ToString(v)
		case "pre-sql":
			cfg.Target.Options.PreSQL = cast.ToString(v)
		case "post-sql":
			cfg.Target.Options.PostSQL = cast.ToString(v)
		case "stdout":
			cfg.Options.StdOut = cast.ToBool(v)
		case "mode":
			cfg.Target.Mode = elt.Mode(cast.ToString(v))
		case "examples":
			showExamples = cast.ToBool(v)
			// case "save":
			// 	saveAsJob = cast.ToBool(v)
		}
	}

	if showExamples {
		println(examples)
		return nil
	}

	if cfgStr != "" {
		err = cfg.Unmarshal(cfgStr)
	} else {
		err = cfg.Prepare()
	}
	if err != nil {
		return g.Error(err, "Unable to parse config string")
	}

	task := elt.NewTask(0, cfg)
	if task.Err != nil {
		return g.Error(task.Err)
	}

	// set context
	task.Ctx = ctx

	// track usage
	defer func() {
		props := g.M(
			"cmd", "exec",
			"error", g.ErrMsgSimple(task.Err),
			"job_type", task.Type,
			"job_mode", task.Cfg.Target.Mode,
			"job_status", task.Status,
			"job_src_type", task.Cfg.SrcConn.Type,
			"job_tgt_type", task.Cfg.TgtConn.Type,
			"job_start_time", task.StartTime,
			"job_end_time", task.EndTime,
			"job_rows_count", task.GetCount(),
		)
		Track("task.Execute", props)
	}()

	// run task
	err = task.Execute()
	if err != nil {
		return g.Error(err)
	}

	return nil
}

func processConns(c *g.CliSC) error {
	switch c.UsedSC() {
	case "add", "new":
		if len(c.Vals) == 0 {
			flaggy.ShowHelp("")
			return nil
		}
	case "list", "show":
		conns := getLocalConns()
		T := table.NewWriter()
		T.AppendHeader(table.Row{"Conn Name", "Conn Type"})
		for _, conn := range conns {
			T.AppendRow(table.Row{conn.Name, conn.Description})
		}
		println(T.Render())
	case "test":
		if len(c.Vals) == 0 {
			flaggy.ShowHelp("")
			return nil
		}
		name := cast.ToString(c.Vals["name"])
		conns := map[string]Conn{}
		for _, conn := range getLocalConns() {
			conns[strings.ToLower(conn.Name)] = conn
		}

		conn, ok := conns[strings.ToLower(name)]
		if !ok || name == "" {
			g.Info("Invalid Connection name: %s", name)
			return nil
		}

		switch {
		case conn.Connection.Type.IsDb():
			dbConn, err := conn.Connection.AsDatabase()
			if err != nil {
				return g.Error(err, "could not initiate %s", name)
			}
			err = dbConn.Connect()
			if err != nil {
				return g.Error(err, "could not connect to %s", name)
			}
			g.Info("success!")
		case conn.Connection.Type.IsFile():
			fileClient, err := conn.Connection.AsFile()
			if err != nil {
				return g.Error(err, "could not initiate %s", name)
			}
			err = fileClient.Init(context.Background())
			if err != nil {
				return g.Error(err, "could not connect to %s", name)
			}
			g.Info("success!")
		default:
			g.Warn("Unhandled connection type: %s", conn.Connection.Type)
		}

	case "":
		flaggy.ShowHelp("")
	}
	return nil
}

func updateCLI(c *g.CliSC) (err error) {
	// Print Progress: https://gist.github.com/albulescu/e61979cc852e4ee8f49c
	Track("updateCLI")

	url := ""
	if runtime.GOOS == "linux" {
		url = "https://f.slingdata.io/linux/sling"
	} else if runtime.GOOS == "darwin" {
		url = "https://f.slingdata.io/mac/sling"
	} else if runtime.GOOS == "windows" {
		url = "https://f.slingdata.io/windows/sling.exe"
	} else {
		return g.Error("OS Unsupported: %s", runtime.GOOS)
	}

	execFileName, err := osext.Executable()
	if err != nil {
		return g.Error(err, "Unable to determine executable path")
	}

	fileStat, _ := os.Stat(execFileName)
	fileMode := fileStat.Mode()

	filePath := execFileName + ".new"

	g.Info("Checking update...")
	err = net.DownloadFile(url, filePath)
	if err != nil {
		println("Unable to download update!")
		return g.Error(strings.ReplaceAll(err.Error(), url, ""))
	}
	err = os.Chmod(filePath, fileMode)
	if err != nil {
		println("Unable to make new binary executable.")
		return err
	}

	versionOut, err := exec.Command(filePath, "--version").Output()
	if err != nil {
		println("Unable to get version of current binary executable.")
		return err
	}

	if strings.HasSuffix(strings.TrimSpace(string(versionOut)), core2.Version) {
		os.Remove(filePath)
		g.Info("Already using latest version: " + core2.Version)
		return nil
	}

	os.Rename(execFileName, execFileName+".old")
	os.Rename(filePath, execFileName)

	os.Remove(execFileName + ".old")

	g.Info("Updated to " + strings.TrimSpace(string(versionOut)))

	return nil
}
