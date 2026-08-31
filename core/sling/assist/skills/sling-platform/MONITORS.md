# Monitors

A monitor observes objects on one connection and records metrics over time. It moves no data. Each run collects what you enable, compares it against history, and emits events — schema changes, stale data, anomalies, validation failures.

Monitors are a Sling Platform feature on the **Advanced plan**. A monitor file lives in `monitors/` and is scheduled as a job of type `monitor`.

## Gather first

Resolve each row before you write YAML, in this order: the user's request, existing files, discovery (MCP / CLI), then the default in the table. Ask the user only for rows that stay unresolved, one at a time, with a proposed default. If every row resolves, do not ask — build, and state the assumptions you took.

| Decision | Resolve by | Default |
|----------|------------|---------|
| connection | request, existing monitors, `connection.list` | the connection the user names |
| objects | request, `connection.discover` | wildcard per schema in scope, e.g. `public.*` |
| object metrics | request, what the alert should catch | `metadata: true`, `row_count: true` in `defaults` |
| freshness column | request, discover columns | a timestamp column updated on write (`updated_at`, `created_at`) |
| freshness threshold | request, the table's load cadence | roughly 2× the expected load interval |
| column metrics | request | none — add only where a question needs them |
| schema drift | request | `schemata.enabled: true` when drift is the reason for the monitor |
| anomaly tuning | request | leave unset; the defaults (z 3.0, 7 points, 30d) suit most tables |

Enable only the metrics that answer a question. Every metric is a query per object per run.

## File structure

Four top-level keys. `connection` and `objects` are required.

```yaml
connection: MY_POSTGRES

defaults:            # applied to every object; each object may override
  metadata: true
  row_count: true

objects:
  public.*:          # name or wildcard pattern
    freshness_column: updated_at
    freshness_threshold: "24h"

  public.orders:
    freshness_column: created_at
    freshness_threshold: "6h"
    columns:
      total:
        min_max_mean: true

schemata:            # connection-wide schema drift detection
  enabled: true
  exclude:
    - "*.tmp_*"
```

## Object keys

| Key | Type | Default | Purpose |
|-----|------|---------|---------|
| `disabled` | bool | `false` | Skip this object |
| `metadata` | bool | `false` | Columns, types, table type. **Required for schema change detection** |
| `row_count` | bool | `false` | `COUNT(*)`. Also the baseline for row-count anomalies |
| `body_md5` | bool | `false` | Hash of a view or procedure definition — catches code changes |
| `freshness_column` | string | — | Column queried with `MAX()` to find data age |
| `freshness_threshold` | string | — | Max age before a `data_stale` event, e.g. `"24h"`, `"7d"`, `"1d12h"` |
| `anomaly_detection` | object | — | Override detection tuning for this object |
| `alert_on_change` | string[] | — | Which changes alert: `name`, `type`, `timestamp`, `size`, `body`, `count` |
| `columns` | map | — | Per-column config, keyed by column name or `"*"` |

Duration units: `d`, `h`, `m`, `s`. Combine them (`"1h30m"`).

Freshness without `freshness_column` falls back to the object's metadata timestamp — imprecise for tables that take inserts without DDL. Set the column whenever one exists.

## Column keys

| Key | Purpose |
|-----|---------|
| `count` | Non-null and null counts |
| `null_count` | Null count |
| `count_distinct` / `unique_count` | Cardinality |
| `size` | Total bytes |
| `min_max_mean` | Min, max, mean — numeric columns |
| `min_max_len` | Min and max string length — text columns |
| `percentile` | p50/p90/p95/p99 and standard deviation |
| `regex_match` / `regex_not_match` | Patterns values must / must not match (string[]) |
| `accepted_values` / `rejected_values` | Value allowlist / denylist (string[]) |
| `alert_on_change` | Same change types as object level |

Validation results carry match count, violation count, and a `valid` flag; violations surface as `anomaly_validation_failure` events.

## Patterns and precedence

`*` matches any sequence, `?` matches one character. Objects are processed in **definition order**, and a later entry naming the same object replaces the earlier one entirely.

```yaml
objects:
  public.*:                 # everything in public...
    metadata: true
    row_count: true

  public.sensitive_table:   # ...except this one
    disabled: true
```

The inverse works too: `public.*: {disabled: true}` first, then list the tables you want.

Use `"*"` as a column name under `defaults` to apply column metrics everywhere:

```yaml
defaults:
  columns:
    "*":
      null_count: true
```

Object settings merge over `defaults` key by key; a `columns` block adds to the inherited wildcard rather than replacing it.

## Schema drift

```yaml
schemata:
  enabled: true
  exclude:
    - "temp_schema.*"
    - "*.staging_*"
```

Captures the full column state of the connection each run and diffs it against the last. Requires `metadata: true` on the monitored objects. `exclude` defaults to `["*.*_tmp"]` when unset; use `exclude: []` to exclude nothing.

Events: `schema_added`, `schema_dropped`, `table_added`, `table_dropped`, `table_recreated`, `column_added`, `column_dropped`, `column_type_altered`.

## Anomalies

Detection is automatic on every metric you enable — no configuration needed. Sling computes a mean and standard deviation from recent history and fires when the z-score of a new value exceeds the threshold.

| Key | Default | Purpose |
|-----|---------|---------|
| `z_score_threshold` | `3.0` | Lower is more sensitive |
| `min_history_points` | `7` | Measurements needed before detection starts |
| `min_history_days` | `7` | Days of history needed |
| `history_days` | `30` | Baseline lookback window |

Events: `anomaly_spike`, `anomaly_drop`, `anomaly_pattern_change`, `anomaly_validation_failure`. Severity follows the z-score: low ≥ 2.0, medium ≥ 3.0, high ≥ 4.0, critical ≥ 5.0.

Nothing fires until the history minimums are met — about a week for a daily monitor. Tell the user this when they set one up. For seasonal data, widen `history_days` (e.g. `90`) instead of loosening the threshold.

## Notifications

Configured on the monitor **job**, not in the YAML. Four triggers: on schema change, on freshness, on anomaly, on failure. Delivered by email, Slack, Discord, or Microsoft Teams.

## Support by database

Freshness works everywhere. Table size and percentiles do not: SQLite and D1 support neither, MySQL/MariaDB has no percentiles. Postgres, Snowflake, BigQuery (approximate), ClickHouse, DuckDB, SQL Server, Oracle, Redshift, Databricks, Trino and Athena support all three.

## Worked example

```yaml
connection: MY_POSTGRES

defaults:
  metadata: true
  row_count: true
  columns:
    "*":
      null_count: true

objects:
  public.users:
    freshness_column: updated_at
    freshness_threshold: "24h"
    columns:
      email:
        count_distinct: true
        regex_match:
          - "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
      status:
        accepted_values: [active, inactive, pending]

  public.orders:
    freshness_column: created_at
    freshness_threshold: "6h"
    alert_on_change: [count]
    columns:
      total:
        min_max_mean: true
        percentile: true

  public.daily_report_view:
    body_md5: true
    alert_on_change: [body]

  public.audit_logs:
    disabled: true

schemata:
  enabled: true
```

## Running

Runs process objects in parallel (2 threads by default) and retry transient errors up to 3 times. Save the file under `monitors/`, create a job of type `monitor` with a cron schedule, and read the results back from the monitor history — object metrics, column metrics, and events.
