package main

import (
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	"github.com/flarco/dbio/connection"

	"github.com/flarco/dbio"
	"github.com/flarco/g/net"
	core2 "github.com/slingdata-io/sling/core"
	"github.com/slingdata-io/sling/core/elt"
	"github.com/slingdata-io/sling/core/env"

	"github.com/flarco/g"
	"github.com/jedib0t/go-pretty/table"
	"github.com/kardianos/osext"
	"github.com/spf13/cast"
)

func listLocalConns() {
	rows := [][]interface{}{}
	for key, val := range g.KVArrToMap(os.Environ()...) {
		if !strings.Contains(val, ":/") {
			continue
		}
		conn, err := connection.NewConnectionFromURL(key, val)
		if g.LogError(err) {
			continue
		}

		if conn.Info().Type == dbio.TypeUnknown || conn.Info().Type == dbio.TypeFileHTTP {
			continue
		}
		rows = append(rows, []interface{}{conn.Info().Name, connection.GetTypeNameLong(conn)})
	}

	sort.Slice(rows, func(i, j int) bool {
		return cast.ToString(rows[i][0]) < cast.ToString(rows[j][0])
	})

	T := table.NewWriter()
	T.AppendHeader(table.Row{"Conn Name", "Conn Type"})
	for _, row := range rows {
		T.AppendRow(row)
	}

	println(T.Render())
	os.Exit(0)
}

func processELT(c *g.CliSC) (err error) {

	doLog := func() {
		logUsage(g.M(
			"cmd", "elt",
			"error", g.ErrorText(err),
		))
	}
	defer doLog()

	local, ok := c.Vals["local-conns"]
	if ok && cast.ToBool(local) {
		listLocalConns()
	}

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

	doLog = func() {
		logUsage(g.M(
			"cmd", "elt",
			"error", g.ErrorText(task.Err),
			"job_type", task.Type,
			"job_mode", task.Cfg.Target.Mode,
			"job_status", task.Status,
			"job_src_type", connection.GetTypeNameLong(task.Cfg.SrcConn),
			"job_tgt_type", connection.GetTypeNameLong(task.Cfg.TgtConn),
			"job_start_time", task.StartTime,
			"job_end_time", task.EndTime,
			"job_rows_count", task.GetCount(),
		))
	}

	// set context
	task.Ctx = ctx

	// run task
	err = task.Execute()
	if err != nil {
		return g.Error(err)
	}

	return nil
}

func updateCLI(c *g.CliSC) (err error) {
	// Print Progress: https://gist.github.com/albulescu/e61979cc852e4ee8f49c
	go logUsage(g.M(
		"cmd", "update",
	))

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

func logUsage(m map[string]interface{}) {
	m["application_name"] = "sling"
	m["version"] = core2.Version
	m["os"] = runtime.GOOS
	return
	env.LogEvent(m)
}
