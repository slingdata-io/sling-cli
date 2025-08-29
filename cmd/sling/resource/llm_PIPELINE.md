# Sling Pipeline Guide for LLMs

## Table of Contents

1. [Introduction and Overview](#1-introduction-and-overview)
2. [Quick Start Guide](#2-quick-start-guide)
3. [Pipeline Structure](#3-pipeline-structure)
4. [Task Types (Hooks)](#4-task-types-hooks)
5. [Variables and Dynamic Configuration](#5-variables-and-dynamic-configuration)
6. [Control Flow](#6-control-flow)
7. [Error Handling](#7-error-handling)
8. [Examples](#8-examples)
9. [MCP Tool Integration](#9-mcp-tool-integration)
10. [Best Practices](#10-best-practices)

---

## 1. Introduction and Overview

### What are Sling Pipelines?

Sling pipelines are powerful YAML-based workflow definitions that allow you to orchestrate a sequence of tasks. Unlike replications, which are focused on moving data between a single source and target, pipelines are designed to execute a series of arbitrary steps, enabling complex workflows, automation, and integration between various systems.

### Key Benefits

- **Orchestration**: Chain multiple operations in a specific order.
- **Modularity**: Create reusable tasks and group them logically.
- **Flexibility**: Combine data replication, shell commands, HTTP requests, SQL queries, and more.
- **Automation**: Automate complex processes like data validation, cleanup, and notifications.
- **Control Flow**: Implement conditional logic and looping.

### When to Use Pipelines

- When you need to run tasks before or after a data replication.
- To automate multi-step data processing jobs.
- For data quality checks and validation.
- To integrate Sling with other tools and APIs.
- For cleanup, archiving, or file management tasks.

### Core Concepts

- **Pipeline**: A YAML file containing a list of steps to be executed.
- **Step**: A single task to be performed within the pipeline.
- **Task Type (Hook)**: The type of operation for a step (e.g., `command`, `query`, `http`).
- **Variables**: Dynamic placeholders for values that are resolved at runtime.

### MCP Tool Integration

The Sling MCP tool provides these pipeline commands:
- `pipeline/docs` - Get documentation.
- `pipeline/parse` - Parse and validate the pipeline configuration.
- `pipeline/run` - Execute the pipeline.

---

## 2. Quick Start Guide

### Essential MCP Commands

```json
// Get pipeline documentation
{
  "action": "docs",
  "input": {}
}

// Parse a pipeline file
{
  "action": "parse",
  "input": {
    "file_path": "/path/to/pipeline.yaml"
  }
}

// Run a pipeline
{
  "action": "run",
  "input": {
    "file_path": "/path/to/pipeline.yaml"
  }
}
```

### Basic Pipeline Structure

```yaml
env:
  MY_VARIABLE: "some_value"

steps:
  - type: log
    message: "Starting the pipeline..."

  - type: replication
    path: /path/to/my_replication.yaml
    id: my_replication

  - type: query
    connection: MY_DB
    query: "SELECT COUNT(*) FROM my_table;"
    id: count_query
```

### Simple Example: Load data and then run a query

```yaml
steps:
  - type: run
    source: "file://data/users.csv"
    target: "MY_POSTGRES.public.users"
    mode: "full-refresh"
    id: load_data

  - type: query
    connection: "MY_POSTGRES"
    query: "SELECT * FROM public.users LIMIT 5;"
    if: state.load_data.status == "success"
```

---

## 3. Pipeline Structure

### Root Level Keys

```yaml
# Optional keys
env: <environment_variables>
steps: <list_of_steps>
```

### Step Configuration

Each step in the `steps` list is an object with the following keys:

```yaml
steps:
  - name: "A descriptive name for the step"  # Optional
    type: <task_type>                      # Required (e.g., command, query, http)
    id: <step_id>                         # Optional: identifier for referencing output
    if: <condition>                       # Optional: condition to run the step
    on_failure: abort                     # Optional: abort/warn/quiet/skip
    ...                                   # Other parameters depend on the type
```

### Example with multiple steps

```yaml
env:
  TARGET_SCHEMA: "analytics"

steps:
  - type: log
    message: "Pipeline started at {timestamp.datetime}"

  - type: replication
    path: replications/main_replication.yaml
    env:
      SLING_THREADS: 8
    id: main_replication

  - type: command
    command: "grep -q 'ERROR' /var/log/app.log"
    if: "false" # This step will be skipped

  - type: http
    url: "https://hooks.slack.com/services/..."
    method: "POST"
    payload: |
      {
        "text": "Pipeline completed successfully for schema: {env.TARGET_SCHEMA}"
      }
    if: state.main_replication.status == "success"
```

---

## 4. Task Types (Hooks)

Pipelines are composed of steps, and each step has a `type`, which is also known as a "hook". 

**Important Note:** The hook configurations shown here are for use in **pipelines**. These are different from replication hooks which use `hooks:` at the root level with `pre:`, `post:`, `end:` sections. Pipeline hooks use `steps:` at the root level with `type:` directly in each step.

Here are the available task types:

### `log`

Prints a message to the console.

```yaml
- type: log
  level: info # debug, info, warn, error (default: info)
  message: "This is a log message. Current user: {env.USER}"
```

[Log Hook Documentation](https://docs.slingdata.io/concepts/hooks/log)

### `run`

Executes a simple data transfer, like a one-off `sling` CLI command.

```yaml
- type: run
  source: "MY_POSTGRES.public.users"
  target: "file://users.csv"
  mode: "full-refresh"
  source_options:
    limit: 100
```

Note: The `run` type is used for simple data transfers. For complex replications with multiple streams and configurations, use the `replication` type instead.

### `replication`

Runs a full replication job from a YAML file.

```yaml
- type: replication
  path: /path/to/my_replication.yaml
  select_streams: ["users", "orders"] # Optional
  mode: "incremental" # Optional
  env: # Optional
    SLING_THREADS: 4
```

[Replication Hook Documentation](https://docs.slingdata.io/concepts/hooks/replication)

### `query`

Executes a SQL query against a connection.

```yaml
- type: query
  connection: MY_SNOWFLAKE
  query: "GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO ROLE reporting;"
  into: query_result
```

[Query Hook Documentation](https://docs.slingdata.io/concepts/hooks/query)

### `http`

Makes an HTTP request.

```yaml
- type: http
  url: "https://api.example.com/v1/status"
  method: "GET" # GET, POST, PUT, DELETE, etc.
  headers:
    Authorization: "Bearer {env.API_TOKEN}"
  payload: '{"key": "value"}' # For POST/PUT requests
  into: api_response
```

[HTTP Hook Documentation](https://docs.slingdata.io/concepts/hooks/http)

### `command`

Executes a shell command.

```yaml
- type: command
  command: "ls -la /data"
  working_dir: "/app" # Optional
  print: true # Optional: Print command output (default: true)
  into: command_output
```

[Command Hook Documentation](https://docs.slingdata.io/concepts/hooks/command)

### `check`

Evaluates a condition and fails the pipeline if the condition is not met.

```yaml
- name: "Check if row count is greater than 0"
  type: check
  check: "run.total_rows > threshold"  # Required: The condition to evaluate
  failure_message: '{run.total_rows} is below threshold'  # Optional: the message to use as an error
  vars:                       # Optional: Local variables for the check
    threshold: 1000
  on_failure: abort          # Optional: abort/warn/quiet/skip
```

[Check Hook Documentation](https://docs.slingdata.io/concepts/hooks/check)

### `copy`

Copies files or directories.

```yaml
- type: copy
  from: "file://data/today/"
  to: "s3://my-bucket/archive/{YYYY}/{MM}/{DD}/"
  recursive: true
```

[Copy Hook Documentation](https://docs.slingdata.io/concepts/hooks/copy)

### `delete`

Deletes files or directories.

```yaml
- type: delete
  location: "file:///tmp/temp_files/"
  recursive: true
```

[Delete Hook Documentation](https://docs.slingdata.io/concepts/hooks/delete)

### `write`

Writes content to a file.

```yaml
- type: write
  to: "file://report.txt"
  content: "Pipeline run on {YYYY}-{MM}-{DD}. Status: OK"
```

[Write Hook Documentation](https://docs.slingdata.io/concepts/hooks/write)

### `inspect`

Inspects files, directories, or database objects to retrieve metadata.

```yaml
- type: inspect
  location: "connection_name/path/to/file_or_table"
  recursive: true  # Optional: For files, get nested file stats
  id: file_info
```

Example for database table:
```yaml
- type: inspect
  location: "postgres/public.users"
  id: table_info
```

[Inspect Hook Documentation](https://docs.slingdata.io/concepts/hooks/inspect)

### `list`

Lists files or directories from a storage connection.

```yaml
- type: list
  location: "aws_s3/path/to/directory"
  recursive: false  # Optional: List files recursively
  only: files  # Optional: List only files or only folders
  id: file_list
```

[List Hook Documentation](https://docs.slingdata.io/concepts/hooks/list)

### `store`

Stores values in memory for later access within the pipeline.

```yaml
- type: store
  key: my_variable
  value: "some value to store"
```

Later access the stored value:
```yaml
- type: log
  message: "Stored value: {store.my_variable}"
```

[Store Hook Documentation](https://docs.slingdata.io/concepts/hooks/store)

### `read`

Reads the contents of files from storage connections.

```yaml
- type: read
  from: "connection/path/to/file.txt"
  into: file_content  # Optional: Store content in variable
```

[Read Hook Documentation](https://docs.slingdata.io/concepts/hooks/read)

### `group`

Groups a set of steps to be executed together. Useful for looping.

```yaml
- type: group
  loop: ["users", "products", "orders"]
  steps:
    - type: log
      message: "Processing table: {loop.value}"
    - type: run
      source: "POSTGRES.public.{loop.value}"
      target: "SNOWFLAKE.analytics.{loop.value}"
```

[Group Hook Documentation](https://docs.slingdata.io/concepts/hooks/group)

---

## 5. Variables and Dynamic Configuration

### Environment Variables

Define variables at the root `env` block or pass them at runtime. Access them using `{env.VAR_NAME}`.

```yaml
env:
  SCHEMA: "production"

steps:
  - type: log
    message: "Using schema: {env.SCHEMA}"
```

### Runtime Variables

Sling provides built-in variables that are resolved at execution time.

- `{timestamp.YYYY}`, `{timestamp.MM}`, `{timestamp.DD}`, `{timestamp.HH}`: Date and time parts.
- `{timestamp.date}`: Date in YYYY-MM-DD format.
- `{timestamp.datetime}`: Full datetime.
- `{timestamp.unix}`: Unix timestamp.

### Accessing Step Results

Save the output of a step using the `id` field and access it in subsequent steps using `{state.step_id}`.

```yaml
steps:
  - type: query
    connection: "MY_DB"
    query: "SELECT COUNT(*) as user_count FROM users;"
    id: "user_count_query"

  - type: log
    message: "Number of users: {state.user_count_query.result[0].user_count}"
```

For `command` steps, access output using `{state.step_id.output.stdout}`.

---

## 6. Control Flow

### Conditional Execution (`if`)

Use the `if` key to conditionally execute a step based on an expression.

```yaml
steps:
  - type: command
    command: "date +%u" # Returns 1-7 (Mon-Sun)
    id: "day_of_week"

  - type: replication
    path: "weekend_job.yaml"
    if: "cast(state.day_of_week.output.stdout, 'int') > 5" # Only run on Sat or Sun
```

### Looping (`loop` in `group`)

Use a `group` step with `loop` to iterate over a list of items. The current item is available as `{loop.value}` and the index as `{loop.index}`.

```yaml
- type: group
  loop:
    - "customers.csv"
    - "products.csv"
    - "orders.csv"
  steps:
    - type: log
      message: "Processing file {loop.index + 1}: {loop.value}"
    - type: run
      source: "file://data/{loop.value}"
      target: "STAGING.{loop.value.split('.')[0]}"
      mode: "truncate"
```

You can also loop over the results of a previous step:

```yaml
steps:
  - type: list
    location: "s3://my-bucket/data/"
    id: "files_list"

  - type: group
    loop: state.files_list.result
    steps:
      - type: log
        message: "Processing: {loop.value.name}"
```

---

## 7. Error Handling

### `on_failure`

Control what happens when a step fails using the `on_failure` parameter.

```yaml
steps:
  - type: delete
    location: "file:///tmp/old_logs"
    on_failure: warn  # Options: abort (default), warn, quiet, skip
```

---

## 8. Examples

### Example 1: Daily Data Warehouse Load

This pipeline runs a replication, then executes a dbt model, and sends a Slack notification.

```yaml
env:
  SLACK_WEBHOOK_URL: "..."

steps:
  - type: log
    message: "Starting daily data warehouse load"

  - type: replication
    path: "replications/pg_to_sflake.yaml"
    mode: "incremental"
    id: dw_load

  - type: command
    command: "dbt run --select my_models"
    working_dir: "/path/to/dbt/project"
    if: state.dw_load.status == "success"

  - type: http
    url: "{env.SLACK_WEBHOOK_URL}"
    method: "POST"
    payload: '{"text": "Daily DW load completed successfully!"}'
    if: state.dw_load.status == "success"

  - type: http
    url: "{env.SLACK_WEBHOOK_URL}"
    method: "POST"
    payload: '{"text": "ERROR: Daily DW load failed!"}'
    if: state.dw_load.status == "error"
```

### Example 2: File Processing Loop

This pipeline downloads a file, processes each line, and then archives the file.

```yaml
steps:
  - type: copy
    from: "sftp://remote/data/transactions.csv"
    to: "file://temp/transactions.csv"
    id: download_file

  - type: read
    from: "file://temp/transactions.csv"
    id: file_content

  - type: group
    loop: state.file_content.content.split('\n')
    steps:
      - type: http
        url: "https://api.my-app.com/process"
        method: "POST"
        payload: "{loop.value}"
        if: "loop.value != ''" # Avoid processing empty lines

  - type: copy
    from: "file://temp/transactions.csv"
    to: "s3://my-archive/transactions/{timestamp.YYYY}_{timestamp.MM}_{timestamp.DD}.csv"

  - type: delete
    location: "file://temp/transactions.csv"
```

---

## 9. MCP Tool Integration

### Using Pipeline Commands

#### Get Documentation
```json
{
  "action": "docs",
  "input": {}
}
```

#### Parse Configuration
```json
{
  "action": "parse",
  "input": {
    "file_path": "/path/to/pipeline.yaml"
  }
}
```

#### Execute Pipeline
```json
{
  "action": "run",
  "input": {
    "file_path": "/path/to/pipeline.yaml",
    "env": {
      "CUSTOM_VAR": "value"
    }
  }
}
```

### Development Workflow

1.  **Write** your pipeline YAML file.
2.  **Parse** the configuration to validate syntax: `{"action": "parse", "input": {"file_path": "my_pipeline.yaml"}}`.
3.  **Test** individual steps if possible (e.g., run `query` or `command` steps manually).
4.  **Run** the full pipeline: `{"action": "run", "input": {"file_path": "my_pipeline.yaml"}}`.
5.  **Debug** by checking the logs and the output of each step.

---

## 10. Best Practices

### Configuration Organization

- Use descriptive names for your steps.
- Group related steps logically.
- Use the `env` block for global variables to avoid hardcoding values.

### Performance

- Be mindful of long-running `command` or `http` steps that might block the pipeline.
- Use `replication` for large data movements as it's optimized for performance.

### Security

- Store sensitive values like API keys and passwords in environment variables, not directly in the pipeline file.
- Be cautious with the `command` hook, as it can execute any shell command.

### Readability

- Add comments to your YAML file to explain complex steps.
- Break down large pipelines into smaller, more manageable sub-pipelines if possible (though Sling doesn't directly support nested pipelines, you can call one from another using a `command` step).
