---
name: sling-replications
description: >
  Configure data replications between sources and targets in Sling. Use when creating replication YAML files, setting up data sync, copying tables, moving data between databases and files, transforming/cleaning columns, adding hooks around streams, setting up change capture (CDC) from transaction logs (binlog, WAL), capturing deletes, or tuning merge strategies.
---

# Replications

Replications are YAML configurations that define data movement from source to target systems.

Driving replications from Python? Load the `sling-python` skill — the `Replication` class kwargs mirror these YAML keys exactly.

## Gather first

Resolve each row before you write YAML, in this order: the user's request, existing files, discovery (MCP / CLI), then the default in the table. Ask the user only for rows that stay unresolved, one at a time, with a proposed default. If every row resolves, do not ask — build, and state the assumptions you took. Confirm first only for destructive resolutions (for example a mode that drops or rewrites existing tables).

| Decision | Resolve by | Default |
|----------|------------|---------|
| source conn | request, existing YAML, `connection.list` | first non-built-in connection |
| target conn | request, existing YAML, `connection.list` | warehouse / second connection |
| streams | request, `connection.discover`, existing YAML | tables named in the request |
| mode | request, existing YAML | `full-refresh` |
| update / primary key | request, columns, existing YAML | required for incremental/backfill |
| object naming | request, existing YAML, project convention | `raw_<source>.{stream_table}` in a project |
| schedule | request, existing pipeline | none (`sling run` by hand) |

When you update a replication, read the file first. Preserve stream-level overrides unless asked to drop them.

When the ask names a table but not its connection, check each DB connection with `connection.discover` / `sling conns discover` before you conclude the table does not exist. Pick the connection that has the table. Treat the table as absent only when no configured connection has it; then write the file with the requested names.

## Project layout

In a Sling project, replications live in `replications/` and target `raw_*` schemas only. Staging, intermediate, and marts are build models — not replication targets. See [PROJECT.md](../sling/PROJECT.md) in the `sling` skill.

```yaml
source: MY_STRIPE
target: MY_WAREHOUSE
defaults:
  object: 'raw_stripe.{stream_table}'
```

## Basic Structure

```yaml
source: SOURCE_CONNECTION
target: TARGET_CONNECTION

defaults:
  mode: full-refresh
  object: '{target_schema}.{stream_table}'

streams:
  schema.table1:
  schema.table2:
    mode: incremental
    primary_key: [id]

env:
  SLING_THREADS: 5
```

## Validate, then run

### Validate (MCP)
```json
{"action": "validate", "input": {"file_path": "/path/to/replication.yaml"}}
```

Finish with a compiled check: MCP `replication validate` (compile is the default). `sling validate --parse-only` is syntax-only. It is not evidence that the file is run-ready.

### Run (CLI only)

There is no MCP `run` action. Execute with the CLI:

```bash
sling run -r /path/to/replication.yaml
sling run -r /path/to/replication.yaml --streams table1 -m incremental
sling run -r /path/to/replication.yaml --debug
```

## Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `full-refresh` | Drop and recreate table | Complete replacement |
| `incremental` | Upsert by primary key | Ongoing updates |
| `truncate` | Clear then load | Preserve DDL |
| `snapshot` | Append with timestamp | Historical tracking |
| `backfill` | Load date/ID ranges | Historical recovery |
| `change-capture` | Stream changes from the transaction log (CDC) | Real-time sync incl. deletes — see [CDC.md](CDC.md) |

### Incremental Strategies

| Primary Key | Update Key | Strategy |
|-------------|------------|----------|
| Yes | Yes | Load only new records, upsert |
| Yes | No | Load all, upsert by PK |
| No | Yes | Append only new records |

```yaml
streams:
  orders:
    mode: incremental
    primary_key: [order_id]
    update_key: updated_at
```

To also capture deletes in incremental mode, set the `delete_missing` target option (`soft` marks rows, `hard` removes them). For true log-based change capture, use `mode: change-capture` — see [CDC.md](CDC.md).

### Backfill Mode

```yaml
streams:
  large_table:
    mode: backfill
    update_key: created_date
    source_options:
      range: '2023-01-01,2023-12-31'
      chunk_size: 7d
```

## Stream Configuration

Every stream must resolve an `object` (per stream or `defaults.object`). A database target without it fails compile.

```yaml
streams:
  my_table:
    object: schema.target_table     # Target table
    mode: incremental               # Loading mode
    primary_key: [id]               # PK columns
    update_key: updated_at          # Incremental key
    columns: {id: bigint, amount: decimal(10,2)}
    select: [id, name, email]       # or [-password] to exclude
    sql: |                          # Custom SQL
      SELECT * FROM my_table
      WHERE status = 'active'
    disabled: false
```

## Wildcards and Patterns

```yaml
streams:
  public.*:                    # All tables in schema
  sales.customer_*:            # Tables with prefix
  public.sensitive_data:
    disabled: true             # Exclude specific
  'data/*.csv':                # All CSV files
  'logs/**/*.json':            # Recursive matching
```

## Variables

| Variable | Description |
|----------|-------------|
| `{stream_name}` | Full stream name |
| `{stream_schema}` | Source schema |
| `{stream_table}` | Source table |
| `{target_schema}` | Target default schema |
| `{stream_file_name}` | File name without extension |
| `{YYYY}`, `{MM}`, `{DD}` | Date parts |

```yaml
defaults:
  object: '{target_schema}.{stream_schema}_{stream_table}'
```

## Source Options

### Database Sources

| Option | Description |
|--------|-------------|
| `limit` | Max rows to read |
| `empty_as_null` | Treat empty strings as NULL |
| `chunk_size` | Time interval per chunk (e.g., `6h`) |

### File Sources

| Option | Description |
|--------|-------------|
| `format` | csv, json, parquet, xlsx |
| `header` | First row is header |
| `delimiter` | Column delimiter |
| `encoding` | Character encoding |
| `jmespath` | Extract JSON data |

```yaml
defaults:
  source_options:
    format: csv
    header: true
    encoding: utf8
```

## Target Options

### Database Targets

| Option | Description |
|--------|-------------|
| `column_casing` | snake, upper, lower, source |
| `add_new_columns` | Auto-add new columns |
| `use_bulk` | Use bulk loading |
| `delete_missing` | `soft` or `hard` delete of PK rows absent from source |
| `merge_strategy` | `update_insert`, `delete_insert`, `insert`, `update` |

### File Targets

| Option | Description |
|--------|-------------|
| `format` | csv, parquet, jsonlines |
| `compression` | gzip, snappy, zstd |
| `file_max_rows` | Split files by row count |

Partition a file target with part tokens in `object` (year, month, year_month, day, week, hour). These require `update_key` — a datetime column, or `_sling_loaded_at` (Unix epoch seconds).

## Transforms

Clean, normalize, or compute columns with 50+ built-in functions — see [TRANSFORMS.md](TRANSFORMS.md) for the full function reference and syntax.

```yaml
streams:
  public.users:
    transforms:
      - name: 'trim_space(upper(value))'
        email: 'lower(value)'
      - full_name: 'record.first_name + " " + record.last_name'
```

## Hooks

Run actions at lifecycle points: `start`/`end` (replication-level), `pre`/`post` (per stream), and `pre_merge`/`post_merge` (in the merge transaction). Hook types (`query`, `check`, `http`, `log`, …) are shared with pipeline steps — see [../sling-pipelines/STEPS.md](../sling-pipelines/STEPS.md) for the full reference of all types, variables, and error handling.

```yaml
hooks:
  end:
    - type: check
      check: execution.status.error == 0
      on_failure: break
    - type: http
      url: '{env.SLACK_WEBHOOK}'
      payload: '{"text": "Replicated {execution.total_rows} rows"}'

streams:
  public.users:
    hooks:
      post:
        - type: query
          connection: '{target.name}'
          query: "ANALYZE {object.full_name}"
```

## Environment Variables

```yaml
env:
  SLING_THREADS: 10           # Parallel streams
  SLING_RETRIES: 3            # Retry failed streams
  SLING_LOADED_AT_COLUMN: true
```

## Examples

### Database to Database

```yaml
source: POSTGRES
target: SNOWFLAKE

defaults:
  mode: incremental
  object: 'warehouse.{stream_schema}_{stream_table}'
  primary_key: [id]
  update_key: updated_at

streams:
  public.customers:
  public.orders:
  public.products:
```

### Files to Database

```yaml
source: S3
target: POSTGRES

defaults:
  mode: full-refresh
  object: 'staging.{stream_file_name}'
  source_options:
    format: csv
    header: true

streams:
  'data/customers/*.csv':
  'data/orders/*.csv':
```

### Database to Files

```yaml
source: POSTGRES
target: S3

defaults:
  mode: full-refresh
  object: 'exports/{stream_table}/{YYYY}/{MM}/'
  target_options:
    format: parquet
    compression: snappy

streams:
  public.transactions:
  public.events:
```

## Topics Reference

| Topic | Where |
|-------|-------|
| Change capture (CDC) | [CDC.md](CDC.md) |
| Transform functions | [TRANSFORMS.md](TRANSFORMS.md) |
| Hook/step types | [../sling-pipelines/STEPS.md](../sling-pipelines/STEPS.md) |

## More Topics (docs index)

Fetch the doc URL when a topic below comes up — this skill does not duplicate them.

| Topic | Doc |
|-------|-----|
| Full source options reference | https://docs.slingdata.io/concepts/replication/source-options.md |
| Full target options reference | https://docs.slingdata.io/concepts/replication/target-options.md |
| Merge strategies (upsert control) | https://docs.slingdata.io/concepts/replication/merge-strategy.md |
| Columns: typing, casing, casting | https://docs.slingdata.io/concepts/replication/columns.md |
| Data quality constraints (`col: type \| value > 0`) | https://docs.slingdata.io/concepts/data-quality/constraints.md |
| Runtime variables (full list) | https://docs.slingdata.io/concepts/replication/runtime-variables.md |
| Tags & wildcards for stream selection | https://docs.slingdata.io/concepts/replication/tags-wildcards.md |
| DB template overrides (custom DDL/SQL) | https://docs.slingdata.io/concepts/replication/templates.md |
| Parallel chunked loads | https://docs.slingdata.io/examples/database-to-database/chunking.md |
| Capture deletes (`delete_missing`) | https://docs.slingdata.io/examples/database-to-database/capture_deletes.md |
| Env vars (`SLING_STATE`, `SLING_THREADS`, ...) | https://docs.slingdata.io/sling-cli/environment.md |

## Full Documentation

See https://docs.slingdata.io/concepts/replication.md for complete reference.
