---
name: sling
description: >
  Sling data integration platform — overview, MCP tools, CLI quick reference, and troubleshooting. Use when asked about Sling in general, what it does, how to use the MCP tools, or when debugging errors, connection failures, authentication issues, type conversion problems, memory/performance issues, or API rate limits.
---

# Sling - Data Integration Platform

Move data between 40+ databases, file systems, and APIs with a single CLI.

## When to Use Sling

| Use Case | Solution |
|----------|----------|
| Move data between databases | Replication (DB-to-DB) |
| Load files into databases | Replication (file-to-DB) |
| Export data to files | Replication (DB-to-file) |
| Extract API data | Replication (API-to-DB/file) |
| Log-based change capture (CDC) | Replication (`mode: change-capture`) — see `sling-replications` skill |
| Multi-step workflows | Pipelines |
| Build SQL models | `sling build` |
| Drive Sling from Python | `pip install sling` — see the `sling-python` skill |
| Hosted jobs / scheduling | Sling Platform |

## MCP Tools Reference

| Tool | Actions | Use Case |
|------|---------|----------|
| `connection` | list, test, discover, set | Manage connections |
| `database` | query, get_schemata | Inspect/query databases |
| `file_system` | list, copy, inspect | Browse/copy files |
| `replication` | docs, validate | Validate replication configs |
| `pipeline` | docs, validate | Validate pipeline configs |
| `api_spec` | docs, validate, test | Build API integrations |

There is no MCP `run`, `build`, or `report` action. Execute with the CLI: `sling run` (replications/pipelines), `sling build` (SQL models), and `sling assist report` (issue reports).

After you classify a failure as a sling defect (not user config), offer: "Report this? GitHub issue (public, needs account) or email to support." Never offer this for user-config errors. Send only after the user agrees. Show the user the draft first (`sling assist report --id <id>`), then send with `--github` or `--email`. Add `--submit` to skip the interactive confirm (for agents and non-TTY use). `--email` opens the contact page with the report prefilled; the user fills in their name and email there — do not ask for an email address yourself.

Improve the report before sending: pass `--title "<plain-English summary>"` (the default title is a terse error label, e.g. `did_not_find_any_columns_for_ident_ident`; write what a maintainer would search for, e.g. `"Incremental replication fails when source table has no columns"`) and `--description "<context>"` (what you tried, what you expected, what happened; it appears above the raw error). Example:

```bash
sling assist report --id <id> --github --submit \
  --title "Incremental replication fails when source table has no columns" \
  --description "Replication from Postgres to ClickHouse fails on the first stream. The source table exists and the user can select from it."
```

### Quick Examples

```json
// List connections
{"action": "list", "input": {}}

// Test connection
{"action": "test", "input": {"connection": "MY_CONN", "debug": true}}

// Validate a replication (compile is the default)
{"action": "validate", "input": {"file_path": "/path/to/replication.yaml"}}
```

## CLI Quick Reference

```bash
sling conns list                    # List connections
sling conns test MY_CONN --debug    # Test connection
sling conns discover MY_PG          # Discover tables
sling run -r replication.yaml       # Run replication
sling run -p pipeline.yaml          # Run pipeline
sling run -r replication.yaml --debug   # Run with debug logging
sling build                         # Run SQL models
sling build --compile               # Compile models without executing
sling assist --id <id>              # Investigate a failure (id from the error footer)
sling assist error <sig>            # Look up an error signature (from prompt context)
sling assist report --id <id>       # Review a redacted issue report
sling assist                        # Open assist (offers to investigate failures)
```

## Topics Reference

| Topic | Description |
|-------|-------------|
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Common errors: connections, auth, type conversion, memory, API rate limits, performance |

## Documentation Map

Append `.md` paths to https://docs.slingdata.io to fetch raw markdown. Full index: https://docs.slingdata.io/llms.txt

| Topic | Doc |
|-------|-----|
| Replications | https://docs.slingdata.io/concepts/replication.md |
| Build (SQL models) | https://docs.slingdata.io/concepts/build.md |
| Change capture (CDC) | https://docs.slingdata.io/concepts/change-capture.md |
| API specs | https://docs.slingdata.io/concepts/api-specs.md |
| Hooks / steps | https://docs.slingdata.io/concepts/hooks.md |
| Pipelines | https://docs.slingdata.io/concepts/pipeline.md |
| Data quality constraints | https://docs.slingdata.io/concepts/data-quality.md |
| Functions | https://docs.slingdata.io/concepts/functions.md |
| Env vars (`SLING_STATE`, `SLING_THREADS`, ...) | https://docs.slingdata.io/sling-cli/environment.md |
| CLI usage (`sling run`, flags) | https://docs.slingdata.io/sling-cli/run.md |
| CLI Pro | https://docs.slingdata.io/sling-cli/cli-pro.md |
| Platform | https://docs.slingdata.io/sling-platform/platform.md |
| Connections (per-connector guides) | https://docs.slingdata.io/connections/ — see `sling-connections` skill |

## Documentation

- **Official Docs**: https://docs.slingdata.io
- **GitHub**: https://github.com/slingdata-io/sling-cli
- **Discord**: https://discord.gg/q5xtaSNDvp
