---
name: sling-pipelines
description: >
  Create multi-step data workflows with Sling pipelines, and use hooks/steps in replications. Use when orchestrating multiple operations, adding hooks before/after replications or streams, executing SQL queries, sending HTTP webhooks, validating data with checks, running shell commands, or building workflows with conditionals and loops.
---

# Pipelines

Pipelines are multi-step workflows that execute a sequence of tasks with control flow.

Pipeline steps and replication hooks are the same engine — see [STEPS.md](STEPS.md) for the full reference of every step/hook type, all variable scopes, and error handling. This file covers pipeline authoring.

Driving pipelines from Python? Load the `sling-python` skill — the `Pipeline` class accepts typed `Step*` objects or the same dicts as the YAML.

## Gather first

Resolve each row before you write YAML, in this order: the user's request, existing files, discovery (MCP / CLI), then the default in the table. Ask the user only for rows that stay unresolved, one at a time, with a proposed default. If every row resolves, do not ask — build, and state the assumptions you took. Confirm first only for destructive resolutions (for example a mode that drops or rewrites existing tables).

| Decision | Resolve by | Default |
|----------|------------|---------|
| goal | request | none — propose the daily spine |
| step kinds | request, existing YAML | replicate → build → check → log |
| connections | request, files, `connection.list` | connections already in the project |
| failure policy | request, existing YAML | `on_failure: abort` for checks |

When you update a pipeline, read the file first. Preserve `id:` values where possible.

## When to Use Pipelines

- Run tasks before/after replications
- Run queries, commands and checks
- Chain multiple replications
- Implement data validation
- Send notifications
- Manage file operations

## Basic Structure

```yaml
env:
  MY_VAR: "value"

steps:
  - type: log
    message: "Starting pipeline"

  - type: replication
    path: /path/to/replication.yaml
    id: main_repl

  - type: query
    connection: MY_DB
    query: "SELECT COUNT(*) FROM table"
    if: state.main_repl.status == "success"
```

## Validate, then run

### Validate (MCP)
```json
{"action": "validate", "input": {"file_path": "/path/to/pipeline.yaml"}}
```

### Run (CLI only)

There is no MCP `run` action. Execute with the CLI:

```bash
sling run -p /path/to/pipeline.yaml
sling run -p /path/to/pipeline.yaml --debug
```

SQL models use `sling build run` (there is no MCP `build` action).

## Step Types

See [STEPS.md](STEPS.md) for full syntax and options of each type.

| Type | Description |
|------|-------------|
| `log` | Output messages |
| `replication` | Run a replication (file or inline) |
| `build` | Run a SQL model project (`sling build run`) |
| `query` | Execute SQL |
| `http` | HTTP requests |
| `command` | Shell commands |
| `check` | Validate conditions |
| `copy` | Copy files |
| `delete` | Delete files |
| `write` | Write to files |
| `read` | Read file contents |
| `list` | List files |
| `inspect` | Get file/object metadata |
| `set` | Set values in `store.*` (legacy alias: `store`) |
| `group` | Group steps, enable looping/concurrency |
| `routine` | Run a named, reusable group of steps |

## Pipeline spine

In a Sling project, the daily pipeline is **replicate → build → check → log**. Use real step types only. Do not use `notify` — that type is not active. See [PROJECT.md](../sling/PROJECT.md) in the `sling` skill.

```yaml
steps:
  - type: log
    message: "Start daily run"

  - type: replication
    path: replications/stripe.yaml
    id: load_raw

  - type: build
    build: models
    prod: true
    id: transform

  - type: check
    check: state.transform.failed == 0
    failure_message: "Build reported failures"
    on_failure: abort

  - type: log
    message: "Done. build ok={state.transform.ok}"
```

Paths are root-relative. Run from the project root.

## Common Steps

### log
```yaml
- type: log
  level: info  # debug, info, warn, error
  message: "Processing {env.MY_VAR}"
```

### replication
```yaml
- type: replication
  path: /path/to/replication.yaml
  streams: ["users", "orders"]
  mode: "incremental"
  id: my_repl
```

### query
```yaml
- type: query
  connection: MY_POSTGRES
  query: |
    SELECT COUNT(*) as cnt FROM users
    WHERE created_at > NOW() - INTERVAL '1 day'
  into: result
  id: count_query
  # rows land at state.<id>.result AND store.<into>
```

### http
```yaml
- type: http
  url: "https://api.example.com/webhook"
  method: POST
  headers:
    # {env.X} reads the process env; declare env: only to pin/override
    Authorization: "Bearer {env.API_TOKEN}"
  payload: |
    {"status": "{state.my_repl.status}"}
  into: response
```

### command
```yaml
- type: command
  command: "python validate.py {store.file_path}"
  working_dir: "/scripts"
  into: output
```

### check
```yaml
- type: check
  check: "state.count_query.result[0].cnt > 0"
  failure_message: "No records found"
  on_failure: abort
```

### copy
```yaml
- type: copy
  from: "local/data/today/file.csv"
  to: "aws_s3/archive/{timestamp.YYYY}/{timestamp.MM}/file.csv"
```

`CONN/path`: `local/rel` is cwd-relative, `local//abs` is absolute. After `list`, copy `{loop.value.location}` (already `CONN/path`); `{loop.value.path}` is also accepted as a local relative path.

### set
```yaml
- type: set
  key: my_value
  value: "something"
# Later: {store.my_value}
```

### group (with loop)
```yaml
- type: group
  loop: ["users", "products", "orders"]
  steps:
    - type: log
      message: "Processing: {loop.value}"
    - type: query
      connection: SNOWFLAKE
      query: "ANALYZE {loop.value}"
```

## Control Flow

### Conditional Execution
```yaml
- type: replication
  path: main.yaml
  id: main_job

- type: http
  url: "https://slack.com/webhook"
  payload: '{"text": "Success!"}'
  if: state.main_job.status == "success"

- type: http
  url: "https://slack.com/webhook"
  payload: '{"text": "Failed!"}'
  if: state.main_job.status == "error"
```

### Looping
```yaml
- type: list
  location: "local/data/*.csv"
  id: files
  only: files

- type: group
  loop: state.files.result
  steps:
    - type: log
      message: "Processing: {loop.value.name}"
    - type: copy
      from: "{loop.value.location}"   # CONN/path — also accepts {loop.value.path}
      to: "aws_s3/eval/{loop.value.name}"
      single_file: true
```

## Error Handling

`on_failure` options: `abort` (default), `error`, `warn`, `quiet`, `skip`, `break`, `retry`, `defer` — see [STEPS.md](STEPS.md) for behavior details.

```yaml
- type: delete
  location: "local/tmp/old/"
  on_failure: warn  # Don't fail if files don't exist
```

## Variables

| Variable | Description |
|----------|-------------|
| `{env.VAR}` | Environment variables |
| `{store.key}` | Stored values |
| `{state.id.*}` | Step results by ID |
| `{timestamp.date}` | Current date |
| `{loop.value}` | Current loop item |
| `{loop.index}` | Loop iteration index |

## Expressions

Expressions use function calls, not methods. `x.split('.')` and `x.upper()` are invalid and fail at run time. Use the built-in functions: `split`, `split_part`, `upper`, and the rest of the function library (see the `FUNCTIONS.md` topic in the `sling-api-specs` skill).

A common loop pattern derives a table name from a file name. Use `split_part` for the stem. Quote the whole YAML value in single quotes, so the separator needs no escaping:

```yaml
object: 'main.{split_part(loop.value.name, ".", 0)}'
```

## Complete Example

```yaml
env:
  SLACK_WEBHOOK: "${SLACK_WEBHOOK_URL}"

steps:
  - type: log
    message: "Starting data pipeline"

  - type: replication
    path: replications/main.yaml
    id: main_sync

  - type: check
    check: state.main_sync.status == "success"
    on_failure: break

  - type: query
    connection: TARGET_DB
    query: "CALL refresh_materialized_views()"

  - type: http
    url: "{env.SLACK_WEBHOOK}"
    method: POST
    payload: |
      {"text": "Pipeline completed: {state.main_sync.total_rows} rows"}

  - type: log
    message: "Pipeline finished"
```

## Full Documentation

See https://docs.slingdata.io/concepts/pipeline.md for complete reference.
