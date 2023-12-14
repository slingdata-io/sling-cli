
<p align="center"><img src="logo-with-text.png" alt="drawing" width="250"/></p>

<p align="center" style="margin-bottom: 0px">Slings from a data source to a data target.</p>
<p align="center">See <a href="https://docs.slingdata.io/">docs.slingdata.io</a> for more details.</p>


Sling is a passion project turned into a free CLI Product which offers an easy solution to create and maintain high volume data pipelines using the Extract & Load (EL) approach. It focuses on data movement between:

* Database to Database
* File System to Database
* Database to File System

Ever wanted to quickly pipe in a CSV or JSON file into your database? Use sling to do so:

```bash
cat my_file.csv | sling run --tgt-conn MYDB --tgt-object my_schema.my_table
```
  
Or want to copy data between two databases? Do it with sling:
```bash
sling run --src-conn PG_DB --src-stream public.transactions --tgt-conn MYSQL_DB --tgt-object mysql.bank_transactions --mode full-refresh
```

Sling can also easily manage our local connections with the `sling conns` command:

```bash
$ sling conns set MY_PG url='postgresql://postgres:myPassword@pghost:5432/postgres'

$ sling conns list
+--------------------------+-----------------+-------------------+
| CONN NAME                | CONN TYPE       | SOURCE            |
+--------------------------+-----------------+-------------------+
| AWS_S3                   | FileSys - S3    | sling env yaml    |
| FINANCE_BQ               | DB - BigQuery   | sling env yaml    |
| DO_SPACES                | FileSys - S3    | sling env yaml    |
| LOCALHOST_DEV            | DB - PostgreSQL | dbt profiles yaml |
| MSSQL                    | DB - SQLServer  | sling env yaml    |
| MYSQL                    | DB - MySQL      | sling env yaml    |
| ORACLE_DB                | DB - Oracle     | env variable      |
| MY_PG                    | DB - PostgreSQL | sling env yaml    |
+--------------------------+-----------------+-------------------+

$ sling conns discover LOCALHOST_DEV
9:05AM INF Found 344 streams:
 - "public"."accounts"
 - "public"."bills"
 - "public"."connections"
 ...
```

## Installation

#### Brew on Mac

```shell
brew install slingdata-io/sling/sling

# You're good to go!
sling -h
```

#### Scoop on Windows

```powershell
scoop bucket add sling https://github.com/slingdata-io/scoop-sling.git
scoop install sling

# You're good to go!
sling -h
```

#### Binary on Linux

```powershell
curl -LO 'https://github.com/slingdata-io/sling-cli/releases/latest/download/sling_linux_amd64.tar.gz' \
  && tar xf sling_linux_amd64.tar.gz \
  && rm -f sling_linux_amd64.tar.gz \
  && chmod +x sling

# You're good to go!
sling -h
```

### Compiling From Source

#### Linux or Mac
```bash
git clone https://github.com/slingdata-io/sling-cli.git
cd sling-cli
bash scripts/build.sh

./sling --help
```

#### Windows (PowerShell)
```bash
git clone https://github.com/slingdata-io/sling-cli.git
cd sling-cli

.\scripts\build.ps1

.\sling --help
```

### Installing via Python Wrapper

`pip install sling`

Then you should be able to run `sling --help` from command line.

## Running a Extract-Load Task

### CLI

```shell
sling run --src-conn POSTGRES_URL --src-stream myschema.mytable \
  --tgt-conn SNOWFLAKE_URL --tgt-object yourschema.yourtable \
  --mode full-refresh
```

Or passing a yaml/json string or file

```shell
sling run -c '
source:
  conn: $POSTGRES_URL
  stream: myschema.mytable

target:
  conn: $SNOWFLAKE_URL
  object: yourschema.yourtable

mode: full-refresh
'
# OR
sling run -c /path/to/config.json
```

### From Lib

```go
package main

import (
	"log"

	"github.com/slingdata-io/sling-cli/core/sling"
)

func main() {
  // cfgStr can be JSON or YAML
	cfgStr := `
    source:
        conn: $POSTGRES_URL
        stream: myschema.mytable
    
    target:
        conn: $SNOWFLAKE_URL
        object: yourschema.yourtable
    
    mode: full-refresh
  `
	cfg, err := sling.NewConfig(cfgStr)
	if err != nil {
		log.Fatal(err)
	}

	err = sling.Sling(cfg)
	if err != nil {
		log.Fatal(err)
	}
}

```

## Config Schema

An example. Put this in https://jsonschema.net/home

`--src-conn`/`source.conn` and `--tgt-conn`/`target.conn`  can be a name or URL of a folder:
- `MY_PG` (connection ref in db, profile or env)
- `$MY_PG` (connection ref in env)
- `postgresql://user:password!@host.loc:5432/database`
- `s3://my_bucket/my_folder/file.csv`
- `gs://my_google_bucket/my_folder/file.json`
- `file:///tmp/my_folder/file.csv` (local storage)

`--src-stream`/`source.stream` can be an object name to stream from:
- `TABLE1`
- `SCHEMA1.TABLE2`
- `OBJECT_NAME`
- `select * from SCHEMA1.TABLE3`
- `/path/to/file.sql` (if source conn is DB)

`--tgt-object`/`target.object` can be an object name to write to:
- `TABLE1`
- `SCHEMA1.TABLE2`

### Example as JSON

```json
{
  "source": {
    "conn": "MY_PG_URL",
    "stream": "select * from my_table",
    "options": {}
  },
  "target": {
    "conn": "s3://my_bucket/my_folder/new_file.csv",
    "options": {
      "header": false
    }
  }
}
```
