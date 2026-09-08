# Incremental Sync

Persist state between runs for efficient incremental data loading.

## Basic Pattern

```yaml
endpoints:
  users:
    state:
      # Read last sync value, default to 30 days ago
      updated_since: >
        { coalesce(
            sync.last_updated,
            date_format(date_add(now(), -30, "day"), "%Y-%m-%dT%H:%M:%SZ")
          )
        }

    # Persist these state variables for next run
    sync: [last_updated]

    request:
      url: "{state.base_url}/users"
      parameters:
        updated_since: "{state.updated_since}"

    response:
      records:
        jmespath: "data[]"
      processors:
        # Track maximum timestamp
        - expression: "record.updated_at"
          output: "state.last_updated"
          aggregation: "maximum"
```

## Workflow

1. **Run Start**: Sling loads persisted `sync.*` values from previous run
2. **State Init**: `state` reads from `sync.*` with `coalesce()` for defaults
3. **Requests**: Use initialized state values in request parameters
4. **Processing**: Processors update `state.*` with aggregation
5. **Run End**: Variables in `sync` array are persisted for next run

## Sync Key Validation

Each `sync` key must have a processor writing to `state.<key>`:

```yaml
# ✅ Valid
endpoints:
  valid:
    sync: ["last_id"]
    response:
      processors:
        - expression: "record.id"
          output: "state.last_id"  # Matches sync key
          aggregation: "last"

# ❌ Invalid - wrong scope
endpoints:
  invalid:
    sync: ["last_id"]
    response:
      processors:
        - expression: "record.id"
          output: "record.last_id"  # Should be state.last_id
```

## Aggregation Types

Choose aggregation based on your incremental strategy:

| Type | Use Case | Example |
|------|----------|---------|
| `maximum` | Latest timestamp | `updated_at`, `modified_at` |
| `last` | Last cursor value | `next_cursor`, `page_token` |
| `minimum` | Earliest timestamp | `oldest_record_date` |
| `first` | First value encountered | `first_record_id` |

## Cursor-Based Incremental

```yaml
endpoints:
  items:
    state:
      cursor: '{coalesce(sync.last_cursor, null)}'
    sync: [last_cursor]

    request:
      parameters:
        cursor: '{state.cursor}'

    response:
      processors:
        - expression: "response.json.next_cursor"
          output: "state.last_cursor"
          aggregation: "last"
```

## Timestamp-Based Incremental

```yaml
endpoints:
  events:
    state:
      since: >
        { coalesce(
            env.START_DATE,
            sync.last_event_time,
            date_format(date_add(now(), -7, "day"), "%Y-%m-%dT%H:%M:%SZ")
          )
        }
    sync: [last_event_time]

    request:
      parameters:
        since: "{state.since}"

    response:
      processors:
        - expression: "record.event_time"
          output: "state.last_event_time"
          aggregation: "maximum"
```

## Context Variables

Read-only runtime values from replication config:

| Variable | Description | Source |
|----------|-------------|--------|
| `context.mode` | Replication mode | Config `mode` field |
| `context.limit` | Max records | `source_options.limit` |
| `context.range_start` | Backfill start | `source_options.range` (first) |
| `context.range_end` | Backfill end | `source_options.range` (second) |
| `context.store` | Store values | Replication `store` variable |

## Backfill with Incremental Fallback

Support both backfill (explicit range) and incremental (from last sync):

```yaml
endpoints:
  events:
    sync: [last_date]

    iterate:
      over: >
        range(
          coalesce(context.range_start, sync.last_date, "2024-01-01"),
          coalesce(context.range_end, date_format(now(), "%Y-%m-%d")),
          "1d"
        )
      into: "state.current_date"

    request:
      parameters:
        date: "{state.current_date}"

    response:
      processors:
        - expression: "state.current_date"
          output: "state.last_date"
          aggregation: "maximum"
```

**Replication config for backfill:**

```yaml
source: MY_API
target: MY_DB
streams:
  events:
    source_options:
      range: '2024-01-01,2024-01-31'  # Sets context.range_start/end
```

## Environment Variable Override

Allow override via environment variable:

```yaml
state:
  start_date: >
    { coalesce(
        env.OVERRIDE_START_DATE,
        sync.last_date,
        "2024-01-01"
      )
    }
```

Set `OVERRIDE_START_DATE` as a run variable (CLI: an env var on `sling run`; platform: a job `config.variables` entry).

## Multiple Sync Variables

Track multiple values:

```yaml
endpoints:
  orders:
    state:
      last_order_id: '{coalesce(sync.last_order_id, 0)}'
      last_modified: '{coalesce(sync.last_modified, "1970-01-01")}'

    sync: [last_order_id, last_modified]

    response:
      processors:
        - expression: "record.id"
          output: "state.last_order_id"
          aggregation: "maximum"
        - expression: "record.modified_at"
          output: "state.last_modified"
          aggregation: "maximum"
```

## Tips

- Always use `coalesce()` to handle first run (when sync values don't exist)
- Use `maximum` aggregation for timestamps
- Use `last` aggregation for cursors
- Context variables are read-only
- Inspect the run log to see sync state (CLI: `--debug`; platform: `executions.log`)

## Related Topics

- [PROCESSORS.md](PROCESSORS.md) - Aggregation in processors
- [VARIABLES.md](VARIABLES.md) - Sync and context scopes
- [PAGINATION.md](PAGINATION.md) - Cursor-based pagination
