# Response Processors

Processors transform and aggregate data from each record.

## Basic Structure

```yaml
response:
  processors:
    - expression: "upper(record.name)"
      output: "record.name_upper"
      if: "!is_null(record.name)"
      aggregation: "maximum"
```

## Processor Fields

| Field | Required | Description |
|-------|----------|-------------|
| `expression` | Yes | Expression to evaluate |
| `output` | Yes | Where to store result |
| `if` | No | Condition to evaluate first |
| `aggregation` | No | How to aggregate across records |

## Output Targets

### Record Fields

Add or modify fields in each record:

```yaml
processors:
  # Add computed field
  - expression: "upper(record.name)"
    output: "record.name_upper"

  # Parse date
  - expression: 'date_parse(record.created_at, "auto")'
    output: "record.created_dt"

  # Combine fields
  - expression: 'record.first_name + " " + record.last_name'
    output: "record.full_name"
```

### State Variables

Update state (requires aggregation for multiple records):

```yaml
processors:
  - expression: "record.updated_at"
    output: "state.last_updated"
    aggregation: "maximum"
```

### Queues

Send values to a queue for other endpoints:

```yaml
processors:
  - expression: "record.id"
    output: "queue.user_ids"
```

### Environment Variables

Store in environment (requires aggregation):

```yaml
processors:
  - expression: "record.updated_at"
    output: "env.LAST_UPDATE"
    aggregation: "maximum"
```

Available in hooks as `{env.LAST_UPDATE}`.

### Context Store

Store for use in replication hooks (requires aggregation):

```yaml
processors:
  - expression: "record.id"
    output: "context.store.first_id"
    aggregation: "first"
```

Available in hooks as `{store.first_id}`.

## Aggregation Types

| Type | Description | Use Case |
|------|-------------|----------|
| `maximum` | Keep highest value | Latest timestamp |
| `minimum` | Keep lowest value | Earliest timestamp |
| `last` | Keep last value | Last cursor |
| `first` | Keep first value | First record ID |
| `collect` | Collect into array | All unique values |

Example:

```yaml
processors:
  # Track max timestamp
  - expression: "record.updated_at"
    output: "state.last_updated"
    aggregation: "maximum"

  # Collect all categories
  - expression: "record.category"
    output: "state.categories"
    aggregation: "collect"
```

## IF Conditions

Control processor execution:

```yaml
processors:
  # Only process non-null emails
  - expression: "record.email"
    if: "!is_null(record.email)"
    output: "record.email_normalized"

  # Multiple conditions
  - expression: "record.id"
    if: 'record.country == "US" && record.status == "active"'
    output: "queue.us_customer_ids"

  # Date filtering
  - expression: "record.id"
    if: 'date_parse(record.created_at, "auto") > date_add(now(), -7, "day")'
    output: "queue.recent_ids"
```

**Note**: `if` is evaluated BEFORE the main expression. If `false`, the entire processor is skipped.

## Overwriting Records

Replace entire record with `output: "record"`:

```yaml
processors:
  # Select specific fields
  - expression: 'object("id", record.id, "name", record.name)'
    output: "record"

  # Rename fields
  - expression: 'object("user_id", record.id, "full_name", record.name)'
    output: "record"

  # Flatten nested structure (jmespath)
  - expression: 'jmespath(record, "{id: id, name: user.name, country: address.country}")'
    output: "record"

  # Flatten nested structure (jq) — [0] needed since jq() returns array
  # - expression: 'jq(record, "{id, name: .user.name, country: .address.country}")[0]'
  #   output: "record"
```

## Extending Processors

Use modifier syntax to prepend or append:

```yaml
defaults:
  response:
    processors:
      - expression: "record.updated_at"
        output: "state.last_updated"
        aggregation: "maximum"

endpoints:
  users:
    response:
      # Add BEFORE defaults
      +processors:
        - expression: "upper(record.name)"
          output: "record.name_upper"

      # Add AFTER defaults
      processors+:
        - expression: "record.id"
          output: "queue.user_ids"
```

**Order:**
1. `+processors` (prepend)
2. `processors` (default or explicit)
3. `processors+` (append)

## Common Patterns

### Track Incremental Timestamp

```yaml
processors:
  - expression: "record.updated_at"
    output: "state.last_updated"
    aggregation: "maximum"
```

### Filter to Queue

```yaml
processors:
  - expression: "record.id"
    if: 'record.status == "active"'
    output: "queue.active_ids"
```

### Transform and Keep

```yaml
processors:
  - expression: "lower(record.email)"
    output: "record.email"
  - expression: "upper(record.country_code)"
    output: "record.country"
```

### Extract Nested Value

```yaml
processors:
  - expression: "record.metadata.subscription.plan"
    output: "record.plan"
```

## Related Topics

- [VARIABLES.md](VARIABLES.md) - Available scopes in expressions
- [FUNCTIONS.md](FUNCTIONS.md) - Expression functions
- [QUEUES.md](QUEUES.md) - Using queues for endpoint chaining
- [INCREMENTAL.md](INCREMENTAL.md) - Sync state for incremental loads
