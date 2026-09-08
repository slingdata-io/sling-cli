---
name: sling-python
description: >
  Drive Sling from Python with the `sling` pip package — Sling, Replication, Pipeline and Connection classes, streaming records or DataFrames in/out, and orchestrator integration (Airflow, Dagster, scripts). Use when writing Python code that runs Sling, streams data into or out of Python, or builds replications/pipelines/API specs programmatically.
---

# Sling Python Package

The `sling` pip package wraps the Sling CLI binary (auto-downloaded on first use). It adds Python-only capabilities: streaming records into/out of Python, DataFrame input, and programmatic config building.

**Key principle**: constructor kwargs mirror the YAML keys exactly. For what the keys *mean* (modes, options, hooks, selectors), load the concept skill — this skill only covers the Python invocation surface.

| Concept | Skill to load |
|---------|---------------|
| Replication YAML semantics | `sling-replications` |
| Pipeline steps / hooks | `sling-pipelines` |
| API spec structure | `sling-api-specs` |
| Connection setup | [CONNECTIONS.md](../sling/CONNECTIONS.md) |

## Installation

Prefer [uv](https://docs.astral.sh/uv/) — it's much faster and manages the venv for you:

```bash
uv add sling               # add to a uv project (pyproject.toml)
uv add 'sling[arrow]'      # + PyArrow: faster streaming, exact type preservation
uv pip install sling       # or into an existing venv
uv run --with sling script.py   # one-off run, no install step
```

Plain pip also works: `pip install sling` / `pip install 'sling[arrow]'`. The sling binary auto-downloads on first use either way.

| Env Variable | Description |
|--------------|-------------|
| `SLING_BINARY` | Path to a specific sling binary (e.g. a local dev build) |
| `SLING_PYTHON_USE_SHELL` | Run the binary with `shell=True` (default `false`) |

## Class Overview

| Class | Purpose |
|-------|---------|
| `Sling` | Single task, mirrors CLI flags; supports `input=` and `.stream()` |
| `Replication` / `ReplicationStream` | Multi-stream replication (file or built in code) |
| `Pipeline` | Multi-step workflow (file or built in code) |
| `Connection` | Test a connection, run SQL (`list`/`dataframe`/`arrow` results) |
| `Mode`, `Format`, `Compression`, `MergeStrategy` | Enums for config values |
| `sling.hooks.Step*` / `Hook*` | Typed pipeline steps and replication hooks |

## `Sling` — single tasks and streaming

Kwargs mirror CLI flags: `src_conn`, `src_stream`, `src_options`, `tgt_conn`, `tgt_object`, `tgt_options`, `mode`, `primary_key`, `update_key`, `select`, `where`, `limit`, `offset`, `range`, `streams`, `columns`, `transforms`, `cdc_options`, `stdout`, `env`, `debug`, `trace`, `replication`, `pipeline`, `directory`, `job`, `config`, `home_dir`. Python-only: `input=` for pushing data in.

```python
from sling import Sling, Mode

# Database to database
Sling(
    src_conn="POSTGRES", src_stream="public.users",
    tgt_conn="SNOWFLAKE", tgt_object="public.users_copy",
    mode=Mode.FULL_REFRESH,
).run()

# SQL query to file
Sling(
    src_conn="POSTGRES",
    src_stream="select * from users where active = true",
    tgt_object="file:///tmp/active_users.csv",
).run()
```

### Input: Python data → target (`input=`)

Accepts list of dicts, a generator (memory-efficient), pandas or polars DataFrames (types preserved with `sling[arrow]`):

```python
data = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
Sling(input=data, tgt_conn="POSTGRES", tgt_object="public.users").run()

import pandas as pd
df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
Sling(input=df, tgt_conn="POSTGRES", tgt_object="public.employees").run()
```

### Output: source → Python (`.stream()` / `.stream_arrow()`)

```python
# Row dicts, memory-efficient iteration
for record in Sling(src_conn="POSTGRES", src_stream="public.users").stream():
    print(record["name"])

# Arrow: fastest, exact types (requires sling[arrow]); no tgt_object allowed
reader = Sling(src_conn="POSTGRES", src_stream="select * from big_table").stream_arrow()
for batch in reader:            # RecordBatchStreamReader — batch-wise for large data
    df = batch.to_pandas()
# or: table = reader.read_all()
```

**Performance caveat**: every `Sling` call spawns the binary and re-opens connections. For many tables in one run, use `Replication` (one process, one connection, parallel streams) instead of a `Sling`-per-table loop.

## `Replication` — multi-stream, built dynamically

```python
from sling import Replication, ReplicationStream, Mode

# From a YAML file
Replication(file_path="path/to/replication.yaml").run()

# Built in code — kwargs mirror the replication YAML keys
streams = {
    folder: ReplicationStream(mode=Mode.FULL_REFRESH, object=table, primary_key="_hash_id")
    for folder, table in folders
}
Replication(
    source="AWS_S3", target="SNOWFLAKE",
    defaults=ReplicationStream(mode=Mode.INCREMENTAL),
    streams=streams,
    env={"SLING_LOADED_AT_COLUMN": "true"},
).run()
```

Helpers: `add_streams({...})`, `enable_streams([...])`, `disable_streams([...])`, `set_default_mode(mode)`.

## `Pipeline` — multi-step workflows

```python
from sling import Pipeline
from sling.hooks import StepLog, StepCopy, StepReplication, StepHTTP, StepCommand

Pipeline(file_path="path/to/pipeline.yaml").run()

# Or typed steps (dicts with {"type": ...} also work)
Pipeline(
    steps=[
        StepCopy(from_="sftp/path/file.csv", to="aws_s3/path/file.csv"),
        StepReplication(path="path/to/replication.yaml"),
        StepHTTP(url="https://hooks.example.com/notify"),
        StepCommand(command=["ls", "-l"], print_output=True),
        StepLog(message="done"),
    ],
    env={"MY_VAR": "value"},
).run()
```

Step classes (aliases of `Hook*`): `StepQuery`, `StepHTTP`, `StepCheck`, `StepRead`, `StepWrite`, `StepCopy`, `StepDelete`, `StepLog`, `StepInspect`, `StepList`, `StepReplication`, `StepCommand`, `StepGroup`, `StepSet` (legacy `StepStore`), `StepRoutine`, `StepBuild`. See the `sling-pipelines` skill for each step's parameters.

## `Connection` — test and query

```python
from sling import Connection

conn = Connection("POSTGRES")        # named conn or env-var URL both work
conn.test()                          # -> TestResult(success, error)
rows = conn.exec("select 1 as a")    # -> [{'a': 1}]
df = conn.exec("select * from users", return_type="dataframe", limit=0)
```

- `return_type`: `list` | `dataframe` | `dataset` | `arrow` (arrow streams, memory-bounded; others materialize fully).
- `limit=None` applies the CLI default cap of 100 rows; pass `limit=0` for no limit. For large results prefer `Sling(...).stream()`.

## `ApiSpec` — build API specs programmatically

`sling.api_spec` provides typed builders (`ApiSpec`, `Endpoint`, `Request`, `Response`, `Pagination`, `Records`, `Processor`, `Rule`, ...) that validate and serialize to spec YAML:

```python
from sling.api_spec import ApiSpec

spec = ApiSpec.parse_file("path/to/spec.yaml")   # parse, modify, re-export
assert spec.validate() == []
spec.to_yaml_file("updated_spec.yaml")
```

For spec structure (auth, pagination, processors), load the `sling-api-specs` skill.

## Orchestrators (Airflow, Dagster, etc.)

Call `Replication(...).run()` or `Pipeline(...).run()` inside a task. Errors raise `SlingError` with the CLI output, so normal task retry/alerting applies. Pass per-run values via `env=` rather than editing YAML files.

```python
@task
def load_orders():
    from sling import Replication
    Replication(file_path="replications/orders.yaml", env={"DATE": "{{ ds }}"}).run()
```

## Full Documentation

- Package: https://github.com/slingdata-io/sling-python
- Concepts: https://docs.slingdata.io
