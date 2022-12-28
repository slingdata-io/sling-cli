package main

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/connection"
	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/manifoldco/promptui"
	"github.com/robfig/cron/v3"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

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
		g.Info("logged out!")
	case "signup":
		// open in browser signup page
	case "token":
	}
	return
}

func processProjectConns(c *g.CliSC) (ok bool, err error) {
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
		connection.GetLocalConns(),
		func(c connection.ConnEntry, i int) bool {
			return c.Connection.Type.IsDb() || c.Connection.Type.IsFile()
		},
	)
	dbConns := lo.Filter(
		connection.GetLocalConns(),
		func(c connection.ConnEntry, i int) bool {
			return c.Connection.Type.IsDb()
		},
	)

	if len(dbConns) == 0 {
		err = g.Error("task generator only works for database source connections. No DB connections found. Please create one and try again.")
		return
	}

	dbConnNames := lo.Map(
		conns,
		func(c connection.ConnEntry, i int) string { return c.Name },
	)
	connNames := lo.Map(
		conns,
		func(c connection.ConnEntry, i int) string { return c.Name },
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
	quoteFunc := func(field string) string { return field }

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
	}
	if targetConn.Connection.Type.IsFile() {
		target = sling.Target{
			Conn:   targetConn.Name,
			Object: g.F("%s/{source_schema}/{source_table}.csv", targetFolder),
		}
	}

	folderDefaults := yaml.MapSlice{
		{Key: "source", Value: sling.Source{Conn: sourceConn.Name}},
		{Key: "target", Value: target},
		{Key: "mode", Value: sling.FullRefreshMode},
		{Key: "config", Value: g.M("schedule", scheduleExpr)},
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
		cols := lo.Map(table.Columns, func(c iop.Column, i int) string {
			return quoteFunc(c.Name)
		})
		colsSQL := "  " + strings.Join(cols, ",\n  ")
		task := map[string]interface{}{
			"source": map[string]interface{}{
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
