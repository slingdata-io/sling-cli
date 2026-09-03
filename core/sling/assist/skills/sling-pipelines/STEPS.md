# Steps & Hooks Reference

Steps (in pipelines) and hooks (in replications) are the same engine — every type below works in both. A pipeline is a list of steps; a replication attaches the same actions as hooks at specific lifecycle points.

## Hook Locations (Replications)

| Location | When | Use Case |
|----------|------|----------|
| `start` | Before any stream runs | Setup, notifications |
| `end` | After all streams complete | Cleanup, notifications |
| `pre` | Before each stream | Validation, setup |
| `post` | After each stream | Logging, notifications |
| `pre_merge` | Before merge (in transaction) | Session settings |
| `post_merge` | After merge (in transaction) | Additional SQL |

### Replication-Level Hooks
```yaml
hooks:
  start:
    - type: log
      message: "Starting replication"
  end:
    - type: check
      check: execution.status.error == 0

defaults:
  ...
streams:
  ...
```

### Stream-Level Hooks
```yaml
streams:
  my_table:
    hooks:
      pre:
        - type: query
          connection: '{source.name}'
          query: "SELECT 1"
      post:
        - type: http
          url: "https://webhook.example.com"
```

## Common Properties

| Property | Description |
|----------|-------------|
| `type` | Step/hook type (can be inferred from keys, e.g. `message` → log) |
| `id` | Identifier for referencing output via `{state.<id>}` |
| `if` | Conditional execution expression |
| `on_failure` | abort (default), error, warn, quiet, skip, break, retry, defer |

## Step / Hook Types

### log
```yaml
- type: log
  level: info  # debug, info, warn, error
  message: "Processed {run.total_rows} rows"
```

### query
```yaml
- type: query
  connection: MY_DB
  query: |
    UPDATE status SET last_run = NOW()
    WHERE table_name = '{stream.table}'
  into: result       # optional: store result rows
  transient: false   # optional: use a transient connection
  id: update_status
```

### http
```yaml
- type: http
  url: "https://api.example.com/webhook"
  method: POST
  timeout: 30
  headers:
    Authorization: "Bearer {env.API_TOKEN}"
    Content-Type: "application/json"
  payload: |
    {
      "table": "{stream.table}",
      "rows": {run.total_rows},
      "status": "{run.status}"
    }
```

### check
```yaml
- type: check
  check: "run.total_rows > 0"
  failure_message: "No rows processed"
  on_failure: abort
  vars:            # optional local scope
    threshold: 100
```

### command
```yaml
- type: command
  command: "python validate.py {object.table}"
  working_dir: "/scripts"
  print: true      # print output to console
  capture: true    # capture output into state
  env:
    MY_VAR: "value"
```

### copy
```yaml
- type: copy
  from: "local/data/source.csv"
  to: "aws_s3/archive/{timestamp.YYYY}/{timestamp.MM}/source.csv"
```

Location form is `CONN/path`. `local/rel/file.csv` is relative to the working directory of whichever host runs the step; `local//abs/path` (two slashes) is absolute. Bare relative paths (`data/a.csv`, `{loop.value.path}`) are treated as local relative paths. Prefer a named file connection over `local` when the step may run on a remote agent. After a `list` step prefer `{loop.value.location}` — it is already `CONN/path`.

### delete
```yaml
- type: delete
  location: "aws_s3/temp/"
  recursive: true
```

### write
```yaml
- type: write
  to: "local/logs/run_{timestamp.file_name}.txt"
  content: |
    Run completed at {timestamp.datetime}
    Rows: {run.total_rows}
```

### read
```yaml
- type: read
  from: "local/config/settings.json"
  into: settings
```

### list
```yaml
- type: list
  location: "aws_s3/data/"
  recursive: false
  only: files  # files or folders
  id: file_list
```

### inspect
```yaml
- type: inspect
  location: "aws_s3/data/file.csv"   # or connection + object
  id: file_info
```

### set
Sets values in the `store.*` scope. (`store` is the legacy alias for this type.)
```yaml
- type: set
  key: my_value
  value: "{run.total_rows}"

# or multiple at once
- type: set
  map:
    batch_id: "{timestamp.unix}"
    row_count: "{run.total_rows}"

# or delete a key
- type: set
  key: my_value
  delete: true
```

### replication
```yaml
- type: replication
  path: /path/to/other_replication.yaml
  streams: ["table1"]      # optional subset
  mode: incremental        # optional override
  range: "2024-01-01,2024-06-30"  # optional backfill range
  env:
    MY_VAR: "value"
```
An inline config can be given via `replication:` instead of `path`.

### build
Runs a Sling Build project (SQL models). Completes load → transform in one pipeline. `command` matches the CLI verbs: `run` (default), `test`, `compile`, `list`.

```yaml
- type: build
  build: models          # project directory (also the working dir)
  command: run           # run | test | compile | list
  prod: true             # write folder-based schemas
  id: transform
  # optional:
  # target: MY_WAREHOUSE
  # select: [stg_orders, fct_orders]
  # full_refresh: true    # run only
  # fail_fast: true
  # threads: 2
  # env:
  #   SLING_DEV_USER: alice
```

Shortcut form: `build: models` (type is inferred). `test: true` is an alias for `command: test`. Results land in `state.<id>.ok`, `state.<id>.failed`, `state.<id>.total`, `state.<id>.command`.

### group
```yaml
- type: group
  loop: ["users", "orders", "products"]
  concurrency: 2   # optional parallel execution
  env:
    MY_VAR: "value"
  steps:
    - type: log
      message: "Processing {loop.value}"
    - type: query
      connection: DB
      query: "ANALYZE {loop.value}"
```

### routine
Runs a named, reusable group of steps defined in a YAML file that declares a top-level `routines:` map. The CLI reads these from `~/.sling/routines/` (or `SLING_ROUTINES_DIR`); on the platform they are project files.
```yaml
- type: routine
  routine: notify_slack
  params:
    channel: "#data-alerts"
  env:
    MY_VAR: "value"
```

## Variables

Variables use `{variable.path}` syntax within hook properties.

### Debug: Print All Variables
```yaml
- type: log
  message: '{runtime_state}'  # Prints all available variables as JSON
```

### Variable Scopes

| Scope | Description | Example |
|-------|-------------|---------|
| `env.*` | Environment variables | `{env.API_TOKEN}` |
| `store.*` | Values from `set` hooks | `{store.my_key}` |
| `state.*` | Hook outputs by ID | `{state.my_query.result[0].id}` |
| `timestamp.*` | Date/time parts | `{timestamp.YYYY}-{timestamp.MM}` |
| `execution.*` | Replication-level info | `{execution.total_rows}` |
| `source.*` | Source connection | `{source.name}`, `{source.type}` |
| `target.*` | Target connection | `{target.database}` |
| `stream.*` | Current stream | `{stream.table}`, `{stream.file_name}` |
| `object.*` | Target object | `{object.full_name}` |
| `run.*` | Current run | `{run.total_rows}`, `{run.status}` |
| `runs.*` | All runs by ID | `{runs.my_stream.status}` |
| `loop.*` | Current group loop item | `{loop.value}`, `{loop.index}` |

### Commonly Used Variables

```yaml
# Timestamps
timestamp.date              # "2025-01-19"
timestamp.datetime          # "2025-01-19 08:27:31"
timestamp.YYYY / MM / DD    # Year, month, day parts
timestamp.file_name         # "2025_01_19_082731" (safe for filenames)
timestamp.unix              # 1737286051

# Run metrics
run.total_rows              # Rows processed
run.total_bytes             # Bytes processed
run.status                  # "success" or "error"
run.duration                # Seconds

# Execution totals
execution.total_rows        # All rows across streams
execution.status.error      # Count of failed streams
execution.status.success    # Count of successful streams

# Stream/Object names
stream.table / stream.schema      # Source table info
object.table / object.full_name   # Target table info
```

## Error Handling

### on_failure Options

| Option | Behavior |
|--------|----------|
| `abort` | Cancel execution immediately (default) |
| `error` | Fail the run, but don't cancel in-flight work |
| `warn` | Log warning, mark warning status, continue |
| `quiet` | Silent, continue |
| `skip` | Skip the current stream, continue |
| `break` | Exit current group / hook sequence |
| `retry` | Retry the hook |
| `defer` | (in groups) defer the failure until the group ends |

### Check Pattern
```yaml
hooks:
  end:
    # Stop if any errors
    - type: check
      check: execution.status.error == 0
      on_failure: break

    # Only runs if the check passed
    - type: http
      url: "https://webhook.example.com"
      payload: '{"status": "success"}'
```

## Complete Example

```yaml
hooks:
  start:
    - type: log
      message: "Starting replication at {timestamp.datetime}"

    - type: query
      connection: '{source.name}'
      query: "SELECT MAX(updated_at) as max_date FROM audit"
      into: audit_check
      id: pre_check

  end:
    - type: check
      check: execution.status.error == 0
      on_failure: break

    - type: http
      url: "{env.SLACK_WEBHOOK}"
      method: POST
      payload: |
        {
          "text": "Replicated {execution.total_rows} rows"
        }

defaults:
  hooks:
    pre:
      - type: log
        message: "Starting {stream.table}"

    post:
      - type: log
        message: "{stream.table}: {run.total_rows} rows in {run.duration}s"

streams:
  public.users:
    hooks:
      post:
        - type: query
          connection: '{target.name}'
          query: "ANALYZE {object.full_name}"
```

## Full Documentation

See https://docs.slingdata.io/concepts/hooks.md for complete reference.
