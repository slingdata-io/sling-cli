
<p align="center"><img src="logo-with-text.png" alt="drawing" width="250"/></p>

<p align="center" style="margin-bottom: 0px">Slings from a data source to a data target.</p>
<p align="center">See <a href="https://docs.slingdata.io/">docs.slingdata.io</a> for more details.</p>


<p align="center">
  <a href="https://github.com/slingdata-io/sling-cli/blob/master/LICENSE">
    <img alt="GitHub" src="https://img.shields.io/github/license/slingdata-io/sling-cli"/>
  </a>
  <a href="https://goreportcard.com/report/github.com/slingdata-io/sling-cli">
    <img src="https://goreportcard.com/badge/github.com/slingdata-io/sling-cli" />
  </a>
  <a href="https://pkg.go.dev/github.com/slingdata-io/sling-cli">
    <img src="https://pkg.go.dev/badge/github.com/slingdata-io/sling-cli.svg" alt="Go Reference"/>
  </a>
  <a href="https://discord.gg/q5xtaSNDvp">
    <img alt="Discord" src="https://img.shields.io/discord/1210406123744796702?style=flat&logo=discord&label=discord"/>
  </a>
  <a href="https://github.com/slingdata-io/sling-cli/tags" rel="nofollow">
    <img alt="GitHub tag (latest SemVer pre-release)" src="https://img.shields.io/github/v/tag/slingdata-io/sling-cli?include_prereleases&label=version"/>
  </a>
  <a href="https://pypi.org/project/sling/" rel="nofollow">
    <img alt="Pip Downloads" src="https://img.shields.io/pepy/dt/sling"/>
  </a>
  <a href="https://pypi.org/project/sling/" rel="nofollow">
    <img alt="Pip Downloads" src="https://img.shields.io/docker/pulls/slingdata/sling"/>
  </a>
</p>

Sling is a passion project turned into a free CLI Product which offers an easy solution to create and maintain small to medium volume data pipelines using the Extract & Load (EL) approach. It focuses on data movement between:

* Database to Database
* File System to Database
* Database to File System


https://github.com/slingdata-io/sling-cli/assets/7671010/e10ee716-1de8-4d53-8eb2-95c6d9d7f9f0

Some key features:
- Single Binary deployment (built with Go). See [installation](https://docs.slingdata.io/sling-cli/getting-started) page.
- Use Custom SQL as a stream: `--src-stream='select * from my_table where col1 > 10'`
- Manage / View / Test / Discover your connections with the [`sling conns`](https://docs.slingdata.io/sling-cli/environment#managing-connections) sub-command
- Use Environment Variable as connections if you prefer (`export MY_PG='postgres//...`)'
- Provide YAML or JSON configurations (perfect for git version control).
- Powerful [Replication](https://docs.slingdata.io/sling-cli/run/configuration/replication) logic, to replication many tables with a wildcard (`my_schema.*`).
- Reads your existing [DBT connections](https://docs.slingdata.io/sling-cli/environment#dbt-profiles-dbt-profiles.yml)
- Use your environment variable in your YAML / JSON config (`select * from my_table where date = '{date}'`)
- Convenient [Transformations](https://docs.slingdata.io/sling-cli/run/configuration/transformations), such as the `flatten` option, which auto-creates columns from your nested fields.
- Run Pre & Post SQL commands.
- many more!

---

Example [Replication](https://docs.slingdata.io/sling-cli/run/configuration/replication):

<p align="center"><img src="https://github.com/slingdata-io/sling-cli/assets/7671010/fcbd2c90-7d6c-4a03-9934-ba1fbeaad838" alt="replication.yaml" width="700"></p>

---

Available Connectors:
- **Databases**: [`bigquery`](https://docs.slingdata.io/connections/database-connections/bigquery) [`bigtable`](https://docs.slingdata.io/connections/database-connections/bigtable) [`clickhouse`](https://docs.slingdata.io/connections/database-connections/clickhouse) [`duckdb`](https://docs.slingdata.io/connections/database-connections/duckdb) [`mariadb`](https://docs.slingdata.io/connections/database-connections/mariadb) [`motherduck`](https://docs.slingdata.io/connections/database-connections/motherduck) [`mysql`](https://docs.slingdata.io/connections/database-connections/mysql) [`oracle`](https://docs.slingdata.io/connections/database-connections/oracle) [`postgres`](https://docs.slingdata.io/connections/database-connections/postgres) [`redshift`](https://docs.slingdata.io/connections/database-connections/redshift) [`snowflake`](https://docs.slingdata.io/connections/database-connections/snowflake) [`sqlite`](https://docs.slingdata.io/connections/database-connections/sqlite) [`sqlserver`](https://docs.slingdata.io/connections/database-connections/sqlserver) [`starrocks`](https://docs.slingdata.io/connections/database-connections/starrocks) [`prometheus`](https://docs.slingdata.io/connections/database-connections/prometheus) [`proton`](https://docs.slingdata.io/connections/database-connections/proton)
- **File Systems**: [`azure`](https://docs.slingdata.io/connections/file-connections/azure) [`b2`](https://docs.slingdata.io/connections/file-connections/b2) [`dospaces`](https://docs.slingdata.io/connections/file-connections/dospaces) [`gs`](https://docs.slingdata.io/connections/file-connections/gs) [`local`](https://docs.slingdata.io/connections/file-connections/local) [`minio`](https://docs.slingdata.io/connections/file-connections/minio) [`r2`](https://docs.slingdata.io/connections/file-connections/r2) [`s3`](https://docs.slingdata.io/connections/file-connections/s3) [`sftp`](https://docs.slingdata.io/connections/file-connections/sftp) [`wasabi`](https://docs.slingdata.io/connections/file-connections/wasabi)
- **File Formats**: `csv`, `parquet`, `xlsx`, `json`, `avro`, `xml`, `sas7bday`

Here are some additional links:
- https://slingdata.io
- https://docs.slingdata.io
- https://blog.slingdata.io

---

Ever wanted to quickly pipe in a CSV or JSON file into your database? Use sling to do so:

```bash
cat my_file.csv | sling run --tgt-conn MYDB --tgt-object my_schema.my_table
```

Or want to copy data between two databases? Do it with sling:
```bash
sling run --src-conn PG_DB --src-stream public.transactions \
  --tgt-conn MYSQL_DB --tgt-object mysql.bank_transactions \
  --mode full-refresh
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

Requirements:
- Install Go 1.22+ (https://go.dev/doc/install)
- Install a C compiler ([gcc](https://www.google.com/search?q=install+gcc&oq=install+gcc), [tdm-gcc](https://jmeubank.github.io/tdm-gcc/), [mingw](https://www.google.com/search?q=install+mingw), etc)

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

Repository link: https://github.com/slingdata-io/sling-python

### Automated Dev Builds

Here are the links of the official development builds, which are the latest builds of the upcoming release.

- **Linux (x64)**: https://f.slingdata.io/dev/latest/sling_linux_amd64.tar.gz
- **Mac (arm64)**: https://f.slingdata.io/dev/latest/sling_darwin_arm64.tar.gz
- **Windows (x64)**: https://f.slingdata.io/dev/latest/sling_windows_amd64.tar.gz

If you're using the python wrapper, decompress the downloaded binary and put the path in the environment variable `SLING_BINARY` before running your python program.

## Contributing

We welcome contributions to improve Sling! Here are some guidelines to help you get started.

### Branch Naming Convention

When creating a new branch for your contribution, please use the following naming convention:

- `feature/your-feature-name` for new features
- `bugfix/issue-description` for bug fixes
- `docs/update-description` for documentation updates

### Testing Guidelines

Sling has three main test suites: Database, File and CLI. When contributing, please ensure that your changes pass the relevant tests.

#### Running Tests

To run the full test suite, run below. However you'd need to define all the needed connections as shown [here](https://github.com/slingdata-io/sling-cli/blob/main/cmd/sling/sling_test.go#L52-L83), so it's recommended to target specific tests instead.

```sh
./scripts/build.sh
./scripts/test.sh
```

#### Targeting Specific Tests

You can target specific tests or suites using environment variables:

1. Database Suite:
   ```sh
   cd cmd/sling
   go test -v -run TestSuiteDatabasePostgres             # run all Postgres tests
   TESTS="1-3" go test -v -run TestSuiteDatabasePostgres # run Postgres tests 1, 2, 3
   ```

2. File Suite:
   ```sh
   cd cmd/sling
   go test -v -run TestSuiteFileS3               # run all S3 tests
   TESTS="1,2,3" go test -v -run TestSuiteFileS3 # run S3 tests 1, 2, 3
   ```

3. CLI Suite:
   ```sh
   cd cmd/sling
   export SLING_BIN=./sling
   go test -v -run TestCLI             # run all CLI tests
   TESTS="31+" go test -v -run TestCLI # run CLI tests 31 and all subsequent tests
   ```

You can specify individual test numbers, ranges, or use the '+' suffix to run all tests from a certain number:

- `TESTS="1,2,3"`: Run tests 1, 2, and 3
- `TESTS="1-5"`: Run tests 1 through 5
- `TESTS="3+"`: Run test 3 and all subsequent tests

#### Test Suites Overview

1. **Database Suite**: Tests database-related functionality.
   - Located in: `cmd/sling/sling_test.go`
   - Configuration: `cmd/sling/tests/suite.db.template.tsv`

2. **File Suite**: Tests file system operations.
   - Located in: `cmd/sling/sling_test.go`
   - Configuration: `cmd/sling/tests/suite.file.template.tsv`

3. **CLI Suite**: Tests command-line interface functionality.
   - Located in: `cmd/sling/sling_cli_test.go`
   - Configuration: `cmd/sling/tests/suite.cli.tsv`

### Adding New Tests

When introducing new features or addressing bugs, it's essential to incorporate relevant tests, focusing mainly on the CLI suite file located at `cmd/sling/suite.cli.tsv`. The database and file suites serve as templates applicable across all connectors, making them more sensitive to modifications. Therefore, any changes to these suites will be managed internally.

 When adding new test entries in the CLI suite file, feel free to create a new replication file in folder `cmd/sling/tests/replications`, or a corresponding source file in the `cmd/sling/tests/files` directory. Also include the expected output or the number of expected rows/streams in the new test entry.

### Pull Request Process

1. Ensure your code adheres to the existing style and passes all tests.
2. Update the README.md with details of changes to the interface, if applicable.
3. Create a Pull Request with a clear title and description.

Thank you for contributing to Sling!