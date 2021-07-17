package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/rudderlabs/analytics-go"
	"github.com/slingdata-io/sling/core"
	"github.com/slingdata-io/sling/core/env"

	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/spf13/cast"
)

var examples = `

###### Database to Database ######
# drop target table
export PG_DB_URL=postgres://xxxxxxxxxxxxxxxx
export MYSQL_DB_URL=mysql://xxxxxxxxxxxxxxxx
sling elt -c '
src_conn: PG_DB_URL
src_table: bank.transactions
tgt_conn: MYSQL_DB_URL
tgt_table: mysql.bank_transactions
mode: drop
'
# OR
sling elt --src-conn PG_DB_URL --src-table bank.transactions --tgt-conn MYSQL_DB_URL  --tgt-table mysql.bank_transactions --mode drop

# custom sql in-line
sling elt -c '
src_conn: PG_DB_URL
src_sql: select id, created_at, account_id, amount from bank.transactions where type = 'A'
tgt_conn: MYSQL_DB_URL
tgt_table: mysql.bank_transactions
mode: append
'

# custom sql file
sling elt -c '
src_conn: PG_DB_URL
src_sql: /path/to/query.sql
tgt_conn: MYSQL_DB_URL
tgt_table: mysql.bank_transactions
mode: append
'

# upsert
sling elt -c '
src_conn: PG_DB_URL
src_table: bank.transactions
tgt_conn: MYSQL_DB_URL
tgt_table: mysql.bank_transactions
mode: upsert
update_key: modified_at
primary_key: id
'


###### Database to File ######
# CSV export full table
sling elt -c '
src_conn: PG_DB_URL
src_table: bank.transactions
tgt_file: /tmp/bank.transactions.csv
'
# OR
sling elt --src-conn PG_DB_URL --src-table bank.transactions --tgt-file /tmp/bank.transactions.csv

# CSV dump, custom SQL
sling elt -c '
src_conn: PG_DB_URL
src_sql: select id, created_at, account_id, amount from bank.transactions where type = 'A'
tgt_file: /tmp/bank.transactions.csv
'

# CSV export full table to S3, gzip
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxx
sling elt -c '
src_conn: PG_DB_URL
src_table: bank.transactions
tgt_file: s3://my-bucket/bank.transactions.csv.gz
options: gzip
'

###### File to Database ######
# CSV import into table
sling elt -c '
src_file: /tmp/bank.transactions.csv.gz
tgt_conn: PG_DB_URL
tgt_table: bank.transactions
mode: append
'
# OR
cat /tmp/bank.transactions.csv.gz | sling elt --tgt-conn PG_DB_URL --tgt-table bank.transactions


# CSV folder import into table, upsert
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxx
sling elt -c '
src_file: s3://my-bucket/bank.transactions/
tgt_conn: PG_DB_URL
tgt_table: bank.transactions
mode: upsert
update_key: modified_at
primary_key: id
'
`
var ctx, cancel = context.WithCancel(context.Background())

var cliRun = &g.CliSC{
	Name:        "run",
	Description: "execute an ad-hoc task",
	Flags: []g.Flag{
		{
			Name:        "config",
			ShortName:   "c",
			Type:        "string",
			Description: "The config string or file to use (JSON or YAML).\n",
		},
		{
			Name:        "src-conn",
			ShortName:   "",
			Type:        "string",
			Description: "The source database / API connection (name, conn string or URL).",
		},
		{
			Name:        "src-table",
			ShortName:   "",
			Type:        "string",
			Description: "The source table (schema.table) or API supported object name.",
		},
		{
			Name:        "src-sql",
			ShortName:   "",
			Type:        "string",
			Description: "The path of sql file or in-line text to use as query\n",
		},
		{
			Name:        "tgt-conn",
			ShortName:   "",
			Type:        "string",
			Description: "The target database connection (name, conn string or URL).",
		},
		{
			Name:        "tgt-table",
			ShortName:   "",
			Type:        "string",
			Description: "The target table (schema.table).",
		},
		{
			Name:        "pre-sql",
			ShortName:   "",
			Type:        "string",
			Description: "The path of sql file or in-line text to run on tgtConn prior to the data load",
		},
		{
			Name:        "post-sql",
			ShortName:   "",
			Type:        "string",
			Description: "The path of sql file or in-line text to run on tgtConn after the data is loaded.",
		},
		{
			Name:        "stdout",
			ShortName:   "",
			Type:        "bool",
			Description: "Output the stream to standard output (STDOUT).\n",
		},
		{
			Name:        "options",
			ShortName:   "o",
			Type:        "string",
			Description: "in-line options to further configure ELT task.",
		},
		{
			Name:        "mode",
			ShortName:   "m",
			Type:        "string",
			Description: "The target load mode to use: append, upsert, truncate, drop.\n                     Default is append. For upsert, must provide `update_key` and `primary_key` in a configuration file.\n                     All modes load into a new temp table on tgtConn prior to final load.",
		},
		{
			Name:        "examples",
			ShortName:   "e",
			Type:        "bool",
			Description: "Shows some examples.",
		},
	},
	ExecProcess: processRun,
}

var cliInteractive = &g.CliSC{
	Name:        "it",
	Description: "launch interactive mode",
	ExecProcess: slingPrompt,
}

var cliUpdate = &g.CliSC{
	Name:        "update",
	Description: "update to the latest version",
	ExecProcess: updateCLI,
}

var cliConns = &g.CliSC{
	Name:        "conns",
	Singular:    "local connection",
	Description: "manage local connections",
	SubComs: []*g.CliSC{
		// {
		// 	Name:        "add",
		// 	Description: "add new connection",
		// },
		{
			Name:        "show",
			Description: "list local connections detected",
		},
		{
			Name:        "test",
			Description: "test a local connection",
			Flags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to test",
				},
			},
		},
	},
	ExecProcess: processConns,
}

func init() {
	// we need a webserver to get the pprof webserver
	if os.Getenv("SLING_PPROF") == "TRUE" {
		go func() {
			g.Trace("Starting pprof webserver @ localhost:6060")
			g.LogError(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	cliInteractive.Make().Add()
	cliConns.Make().Add()
	cliRun.Make().Add()
	cliUpdate.Make().Add()

	sentry.Init(sentry.ClientOptions{
		// Either set your DSN here or set the SENTRY_DSN environment variable.
		Dsn: "https://abb36e36341a4a2fa7796b6f9a0b3766@o881232.ingest.sentry.io/5835484",
		// Either set environment and release here or set the SENTRY_ENVIRONMENT
		// and SENTRY_RELEASE environment variables.
		// Environment: "",
		Release: "sling@" + core.Version,
		// Enable printing of SDK debug messages.
		// Useful when getting started or trying to figure something out.
		Debug: false,
	})
}

func getRsClient() analytics.Client {
	return analytics.New("1uXKxEPgIB9HXkTduvfwXmFak2l", "https://liveflarccszw.dataplane.rudderstack.com")
}

func Track(event string, props ...g.Map) {
	if val := os.Getenv("SLING_SEND_ANON_USAGE"); val != "" {
		if !cast.ToBool(val) {
			return
		}
	}

	rsClient := getRsClient()
	properties := analytics.NewProperties().
		Set("application", "sling-cli").
		Set("version", core.Version).
		Set("os", runtime.GOOS)

	if len(props) > 0 {
		for k, v := range props[0] {
			properties.Set(k, v)
		}
	}

	rsClient.Enqueue(analytics.Track{
		UserId:     "sling",
		Event:      event,
		Properties: properties,
	})
	rsClient.Close()
}

func main() {

	exitCode := 11
	done := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	kill := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	signal.Notify(kill, os.Kill)

	iop.ShowProgress = os.Getenv("SLING_SHOW_PROGRESS") != "false"
	database.UseBulkExportFlowCSV = cast.ToBool(os.Getenv("SLING_BULK_EXPORT_FLOW_CSV"))

	go func() {
		defer close(done)
		exitCode = cliInit()
	}()

	select {
	case <-done:
		os.Exit(exitCode)
	case <-kill:
		println("\nkilling process...")
		os.Exit(111)
	case <-interrupt:
		if cliRun.Sc.Used {
			println("\ninterrupting...")
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
		os.Exit(exitCode)
		return
	}
}

func cliInit() int {
	env.InitLogger()

	// Set your program's name and description.  These appear in help output.
	flaggy.SetName("sling")
	flaggy.SetDescription("An Extract-Load tool.")
	flaggy.DefaultParser.ShowHelpOnUnexpected = true
	flaggy.DefaultParser.AdditionalHelpPrepend = "Slings data from a data source to a data target.\nVersion " + core.Version

	flaggy.SetVersion(core.Version)
	for _, cli := range g.CliArr {
		flaggy.AttachSubcommand(cli.Sc, 1)
	}

	flaggy.Parse()

	ok, err := g.CliProcess()
	if ok {
		if err != nil {
			sentry.CaptureException(err)
		}
		g.LogFatal(err)
	} else {
		flaggy.ShowHelp("")
		Track("ShowHelp")
	}
	return 0
}
