package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"

	"github.com/integrii/flaggy"
	"github.com/manifoldco/promptui"
	"github.com/robfig/cron/v3"
	"github.com/samber/lo"
	"gopkg.in/yaml.v2"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/database"
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
		inBytes, outBytes := task.GetBytes()
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
			"job_rows_in_bytes", inBytes,
			"job_rows_ou_tbytes", outBytes,
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

func processAuth(c *g.CliSC) (ok bool, err error) {
	ok = true

	switch c.Name {
	case "login":
		// obtain api key for user, prompt for password. TODO: use web login redirect

		// get email
		validate := func(input string) error {
			if len(strings.TrimSpace(input)) < 3 {
				return errors.New("Username must have more than 3 characters")
			}
			return nil
		}
		prompt := promptui.Prompt{
			Label:    "email",
			Validate: validate,
		}
		email, err := prompt.Run()
		if err != nil {
			err = g.Error(err, "could not get email")
			return ok, err
		}

		// get password
		validate = func(input string) error {
			if len(strings.TrimSpace(input)) < 6 {
				return errors.New("Password must have more than 6 characters")
			}
			return nil
		}
		prompt = promptui.Prompt{
			Label:    "password",
			Validate: validate,
			Mask:     '*',
		}
		password, err := prompt.Run()
		if err != nil {
			err = g.Error(err, "could not get password")
			return ok, err
		}

		m := g.M("email", email, "password", password)
		respStr, err := sling.ClientPost(masterServerURL, sling.RouteAppLogin, m, headers)
		if err != nil {
			err = g.Error(err, "failed to login")
			return ok, err
		}

		rm, err := g.UnmarshalMap(respStr)
		if err != nil {
			err = g.Error(err, "failed to parse response")
			return ok, err
		}

		apiKey := []byte(cast.ToString(rm["api_key"]))

		apiTokenFile := path.Join(env.HomeDir, ".api_token")
		err = ioutil.WriteFile(apiTokenFile, apiKey, 600)
		if err != nil {
			err = g.Error(err, "failed to cache token")
			return ok, err
		}

	case "logout":
		os.RemoveAll(apiTokenFile)
		g.Info("logged out!")
	case "signup":
		// open in browser signup page
	case "token":
		apiTokenBytes, _ := ioutil.ReadFile(apiTokenFile)
		apiToken := string(apiTokenBytes)
		if apiToken == "" {
			return ok, g.Error("no token cached. Are you logged in?")
		}
		println(apiToken)
	}
	return
}

func processProjectConns(c *g.CliSC) (ok bool, err error) {
	setJWT()
	ok = true
	switch c.Name {
	case "set":
	case "unset":
	case "list":
	case "test":
	}
	return
}

func processProjectVars(c *g.CliSC) (ok bool, err error) {
	setJWT()
	ok = true
	switch c.Name {
	case "set":
	case "unset":
	case "list":
	}
	return
}

func processProjectTasks(c *g.CliSC) (ok bool, err error) {
	ok = true
	switch c.Name {
	case "list":
	case "show":
	case "toggle-active":
	case "trigger":
	case "terminate":
	case "generate":
		return processProjectTasksGenerate(c)
	}
	return
}

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
	ok = true
	conns := lo.Filter(
		env.GetLocalConns(),
		func(c env.Conn, i int) bool {
			return c.Connection.Type.IsDb() || c.Connection.Type.IsFile()
		},
	)
	dbConns := lo.Filter(
		env.GetLocalConns(),
		func(c env.Conn, i int) bool {
			return c.Connection.Type.IsDb()
		},
	)

	if len(dbConns) == 0 {
		err = g.Error("task generator only works for database source connections. No DB connections found. Please create one and try again.")
		return
	}

	dbConnNames := lo.Map(
		conns,
		func(c env.Conn, i int) string { return c.Name },
	)
	connNames := lo.Map(
		conns,
		func(c env.Conn, i int) string { return c.Name },
	)

	prompt := promptui.Select{Label: "Source Connection", Items: dbConnNames}
	sourceIndex, _, err := prompt.Run()
	if err != nil {
		err = g.Error(err, "could not get source connection")
		return ok, err
	}

	prompt = promptui.Select{Label: "Target Connection", Items: connNames}
	targetIndex, _, err := prompt.Run()
	if err != nil {
		err = g.Error(err, "could not get target connection")
		return ok, err
	}

	sourceConn := dbConns[sourceIndex]
	targetConn := conns[targetIndex]

	var sourceSchema, targetSchema, targetFolder string
	var includeExpr, excludeExpr string
	var sourceTables []database.Table
	quoteFunc := func(field string, normalize ...bool) string { return field }

	if targetConn.Connection.Type.IsFile() {
		// if target is file, get the target folder url
		validate := func(input string) error {
			U, err := net.NewURL(input)
			if err != nil {
				return err
			}

			if t, _ := dbio.ValidateType(U.U.Scheme); t != targetConn.Connection.Type {
				return g.Error("url does not match: %s", targetConn.Connection.Type.NameLong())
			}
			return nil
		}
		prompt := promptui.Prompt{
			Label:    g.F("Target storage folder path (%s)", targetConn.Connection.Type.NameLong()),
			Validate: validate,
		}
		targetFolder, err = prompt.Run()
		if err != nil {
			err = g.Error(err, "could not get target folder path")
			return ok, err
		}

	} else if targetConn.Connection.Type.IsDb() {
		validate := func(input string) error {
			if len(strings.TrimSpace(input)) < 3 {
				return errors.New("Schema name must have more than 3 characters")
			}
			return nil
		}
		prompt := promptui.Prompt{
			Label:    g.F("Target schema (%s)", targetConn.Connection.Type.NameLong()),
			Validate: validate,
		}
		targetSchema, err = prompt.Run()
		if err != nil {
			err = g.Error(err, "could not get target schema")
			return ok, err
		}
	}

	if sourceConn.Connection.Type.IsDb() {
		conn, err := sourceConn.Connection.AsDatabase()
		if err != nil {
			err = g.Error(err, "could not connect to source database %s", sourceConn.Name)
			return ok, err
		}
		schemasData, err := conn.GetSchemas()
		if err != nil {
			err = g.Error(err, "could not get schemas from source database %s", sourceConn.Name)
			return ok, err
		}

		schemas := schemasData.ColValuesStr(0)
		prompt := promptui.Select{Label: "Source Schema", Items: schemas}
		index, _, err := prompt.Run()
		if err != nil {
			err = g.Error(err, "could not get source connection")
			return ok, err
		}
		sourceSchema = schemas[index]

		// include expression
		validate := func(input string) error {
			if len(strings.TrimSpace(input)) < 1 {
				return errors.New(`Include expression must have characters. Try "*" to include all tables.`)
			}
			return nil
		}
		promptFilter := promptui.Prompt{
			Label:    g.F(`Source database tables include expression. See https://docs.slingdata.io for help`),
			Validate: validate,
			Default:  " * ",
		}
		includeExpr, err = promptFilter.Run()
		if err != nil {
			err = g.Error(err, "could not get include expression")
			return ok, err
		}

		// exclude expression
		validate = func(input string) error {
			return nil
		}
		promptFilter = promptui.Prompt{
			Label:    g.F(`Source database tables exclude expression. See https://docs.slingdata.io for help`),
			Validate: validate,
		}
		excludeExpr, err = promptFilter.Run()
		if err != nil {
			err = g.Error(err, "could not get exclude expression")
			return ok, err
		}

		// get all objects
		tableColumnsData, err := conn.GetSchemata(sourceSchema, "")
		if err != nil {
			err = g.Error(err, "could not get objects in source schema %s", sourceSchema)
			return ok, err
		}
		quoteFunc = conn.Quote

		sourceTables = lo.Filter(
			lo.Values(tableColumnsData.Tables()),
			func(table database.Table, i int) bool {
				fullName := strings.ToLower(g.F("%s.%s", table.Schema, table.Name))
				if g.WildCardMatch(fullName, strings.Split(excludeExpr, ",")) {
					return false
				}
				return g.WildCardMatch(fullName, strings.Split(includeExpr, ","))
			},
		)
	}

	// default schedule
	validate := func(cronExpr string) error {
		if strings.TrimSpace(cronExpr) == "" {
			return nil
		}
		specParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		specParserD := cron.NewParser(cron.Descriptor)
		_, err := specParser.Parse(cronExpr)
		if err != nil {
			_, err = specParserD.Parse(cronExpr)
			if err != nil {
				return g.Error(err, "invalid job cron expression")
			}
		}
		return nil
	}
	promptSchedule := promptui.Prompt{
		Label:    `Default schedule. e.g. "every @1h", "0 0 1 1 *" `,
		Validate: validate,
	}
	scheduleExpr, err := promptSchedule.Run()
	if err != nil {
		err = g.Error(err, "could not get default schedule")
		return ok, err
	}
	// Create folder structure
	/*
		|- sling-tasks/
			|- SOURCE_CONN→TARGET_CONN/
			|- SOURCE_CONN/
				|- TARGET_CONN/
					|- source_schema/
						|- _sling_defaults.yaml
						|- table_1.yaml
						|- table_2.yaml
					|- _sling_defaults.yaml
		|- sling_project.yaml
	*/

	tasksFolderPath := path.Join(
		".",
		"sling-tasks",
		g.F("%s→%s", sourceConn.Name, targetConn.Name),
		sourceSchema,
	)

	err = os.MkdirAll(tasksFolderPath, 0755)
	if err != nil {
		err = g.Error(err, "could not create tasks folder")
		return
	}

	target := sling.Target{
		Conn:   targetConn.Name,
		Object: g.F("%s.{source_schema}_{source_table}", targetSchema),
		Mode:   sling.DropMode,
	}
	if targetConn.Connection.Type.IsFile() {
		target = sling.Target{
			Conn:   targetConn.Name,
			Object: g.F("%s/{source_schema}/{source_table}.csv", targetFolder),
			Mode:   sling.DropMode,
		}
	}

	folderDefaults := yaml.MapSlice{
		{Key: "source", Value: sling.Source{Conn: sourceConn.Name}},
		{Key: "target", Value: target},
		{Key: "config", Value: g.Map{"schedule": scheduleExpr}},
	}

	folderDefaultsBytes, err := yaml.Marshal(folderDefaults)
	if err != nil {
		err = g.Error(err, "could not marshal _sling_defaults")
		return
	}

	folderDefaultsPath := path.Join(tasksFolderPath, "_sling_defaults.yaml")
	err = ioutil.WriteFile(folderDefaultsPath, folderDefaultsBytes, 0755)
	if err != nil {
		err = g.Error(err, "could not write _sling_defaults.yaml")
		return
	}

	for _, table := range sourceTables {
		fullName := g.F(
			"%s.%s.%s", quoteFunc(table.Database),
			quoteFunc(table.Schema), quoteFunc(table.Name),
		)
		cols := lo.Map(table.Columns, func(c database.Column, i int) string {
			return quoteFunc(c.Name)
		})
		colsSQL := "  " + strings.Join(cols, ",\n  ")
		task := g.Map{
			"source": g.Map{
				"stream": g.F("select\n%s\nfrom %s", colsSQL, fullName),
			},
		}

		taskBytes, _ := yaml.Marshal(task)
		taskString := "# using _sling_defaults.yaml\n" + string(taskBytes)
		taskPath := path.Join(tasksFolderPath, table.Name+".yaml")
		err = ioutil.WriteFile(taskPath, []byte(taskString), 0755)
		if err != nil {
			err = g.Error(err, "could not write %s.yaml", table.Name)
			return
		}
	}

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
