package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

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

var cliELT = &g.CliSC{
	Name:        "elt",
	Description: "execute an ad-hoc ELT task",
	Flags: []g.Flag{
		g.Flag{
			Name:        "local-conns",
			Type:        "bool",
			Description: "show the locally defined connections",
		},
		g.Flag{
			Type:        "bool",
			ShortName:   "R",
			Name:        "remote",
			Description: "execute the task remotely from your SlingELT account / instance",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "c",
			Name:        "config",
			Description: "The config string or file to use (JSON or YAML).\n",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "src-file",
			Description: "The path/url of the source file (local, s3, gc, azure, http, sftp).",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "src-conn",
			Description: "The source database / API connection (name, conn string or URL).",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "src-table",
			Description: "The source table (schema.table) or API supported object name.",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "src-sql",
			Description: "The path of sql file or in-line text to use as query\n",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "tgt-file",
			Description: "The path/url of the target file (local, s3, gc, azure).",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "tgt-conn",
			Description: "The target database connection (name, conn string or URL).",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "tgt-table",
			Description: "The target table (schema.table).",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "pre-sql",
			Description: "The path of sql file or in-line text to run on tgtConn prior to the data load",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "",
			Name:        "post-sql",
			Description: "The path of sql file or in-line text to run on tgtConn after the data is loaded.",
		},
		g.Flag{
			Type:        "bool",
			ShortName:   "",
			Name:        "stdout",
			Description: "Output the stream to standard output (STDOUT).\n",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "o",
			Name:        "options",
			Description: "in-line options to further configure ELT task.",
		},
		g.Flag{
			Type:        "string",
			ShortName:   "m",
			Name:        "mode",
			Description: "The target load mode to use: append, upsert, truncate, drop.\n                     Default is append. For upsert, must provide `update_key` and `primary_key` in a configuration file.\n                     All modes load into a new temp table on tgtConn prior to final load.",
		},
		// cmd.Flag{
		// 	Type:        "string",
		// 	ShortName:   "",
		// 	Name:        "upsertCol",
		// 	Description: "The column to use to as an anchor for new values. Must be provided with `upsert` load mode.",
		// },
		// cmd.Flag{
		// 	Type:        "string",
		// 	ShortName:   "",
		// 	Name:        "upsertPK",
		// 	Description: "The column(s) to use to as the primark key for updating. Comma delimited values allowed for a composite key. Must be provided with `upsert` load mode.\n",
		// },
		g.Flag{
			Type:        "bool",
			ShortName:   "e",
			Name:        "examples",
			Description: "Shows some examples.",
		},
	},
	ExecProcess: processELT,
}

var cliUpdate = &g.CliSC{
	Name:        "update",
	Description: "update the cli application to the latest version",
	ExecProcess: updateCLI,
}

func init() {
	// we need a webserver to get the pprof webserver
	if os.Getenv("SLING_PPROF") == "TRUE" {
		go func() {
			g.Trace("Starting pprof webserver @ localhost:6060")
			g.LogError(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	cliELT.Make().Add()
	cliUpdate.Make().Add()
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
		if cliELT.Sc.Used {
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
	flaggy.SetDescription("An ELT tool.")
	flaggy.DefaultParser.ShowHelpOnUnexpected = true
	flaggy.DefaultParser.AdditionalHelpPrepend = "Slings data from a data source to a data target.\nVersion " + core.Version

	flaggy.SetVersion(core.Version)
	for _, cli := range g.CliArr {
		flaggy.AttachSubcommand(cli.Sc, 1)
	}

	flaggy.Parse()

	ok, err := g.CliProcess()
	if ok {
		g.LogFatal(err)
	} else {
		flaggy.ShowHelp("")
	}
	return 0
}
