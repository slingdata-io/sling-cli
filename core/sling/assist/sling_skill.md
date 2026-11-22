---
name: sling
description: >
  Sling data movement (CLI, MCP tools, Python package and Platform). Use for any Sling task: connections and credentials (env.yaml, databases, files, S3/GCS/Azure, APIs), querying databases through Sling MCP, sling run, replication YAML (full-refresh, incremental, backfill, merge, CDC change-capture from binlog/WAL, deletes, column transforms), pipelines, hooks and steps (SQL, HTTP webhooks, checks, shell, loops), REST API specs (OAuth/API key/HMAC auth, pagination, rate limits, queues, incremental sync), SQL models with sling build (sling_build.yml, materialization, dbt compatibility), Python code with the sling pip package (Replication, Pipeline, DataFrames, Airflow, Dagster), and Sling Platform jobs, schedules, executions, agents and monitors. Also for Sling errors: failed runs, connection or auth failures, type conversion, memory and performance.
---

# Sling

The `sling` CLI holds the full skill text for its own version. This file is only a pointer.

Before you create, change, run or debug anything in Sling, run the command for the task and follow its output. Do not work from memory.

| Task | Command |
|---|---|
| Overview, MCP tools, connections, project layout, troubleshooting | `sling skills sling` |
| Replications: modes, incremental, CDC, deletes, transforms | `sling skills sling-replications` |
| Pipelines, hooks and steps | `sling skills sling-pipelines` |
| REST API specs | `sling skills sling-api-specs` |
| SQL models with `sling build` | `sling skills sling-build` |
| Python code with the `sling` package | `sling skills sling-python` |
| Sling Platform: jobs, executions, agents, monitors | `sling skills sling-platform` |

- Run `sling skills` to list all topics with their sub-files.
- Run `sling skills <topic>/<FILE>.md` for a sub-file. A link such as `[CDC.md](CDC.md)` in the output means `sling skills <topic>/CDC.md`.
- You can give more than one topic in one command.
- If the `sling` command is not found, ask the user to install it: https://docs.slingdata.io/sling-cli/getting-started
