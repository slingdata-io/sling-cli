# Response Processing

Configure how Sling extracts and handles data from API responses.

## Basic Structure

```yaml
response:
  # Force format (auto-detected if omitted)
  format: "json"  # json, csv, xml

  # Record extraction
  records:
    jmespath: "data.items[]"
    # or: jq: ".data.items[]"
    primary_key: ["id"]
    update_key: "updated_at"
    limit: 1000
    duplicate_tolerance: "1000000,0.001"
```

## Record Extraction

### JMESPath / jq

Inspect the raw response before you choose `records.jmespath`. Most APIs wrap records (`{"data": [...]}` → `jmespath: data[]`). An endpoint test that returns 0 records means the records path is wrong — fix the path, do not ship.

Extract records array from response using JMESPath or jq (mutually exclusive — use one or the other):

```yaml
response:
  records:
    # Simple array (JMESPath)
    jmespath: "data[]"
    # Simple array (jq)
    # jq: ".data[]"

    # Nested path (JMESPath)
    jmespath: "response.results.items[]"
    # Nested path (jq)
    # jq: ".response.results.items[]"

    # Single object as array
    jmespath: "[user]"
    # jq: "[.user]"
```

### Primary Key

Field(s) for deduplication:

```yaml
records:
  jmespath: "data[]"
  primary_key: ["id"]  # Single key

  # Composite key
  primary_key: ["id", "location_id"]
```

### Update Key

Informational field for incremental logic:

```yaml
records:
  jmespath: "data[]"
  primary_key: ["id"]
  update_key: "updated_at"
```

### Record Limit

Max records per endpoint run (for testing):

```yaml
records:
  jmespath: "data[]"
  limit: 5000
```

## Deduplication

### Default (In-Memory)

100% accurate, ~100 bytes per record:

```yaml
records:
  primary_key: ["id"]
  # Uses in-memory map by default
```

### Bloom Filter

For large datasets (>1M records). Less memory, allows false positives:

```yaml
records:
  primary_key: ["id"]
  # Format: "capacity,error_rate"
  # Example: 10M items, 0.1% false positives, ~15MB memory
  duplicate_tolerance: "10000000,0.001"
```

## Format Override

Force response format (overrides Content-Type header):

```yaml
response:
  format: "json"  # json, csv, xml
  records:
    jmespath: "data[]"
```

## Complete Example

```yaml
endpoints:
  orders:
    request:
      url: "{state.base_url}/orders"

    response:
      format: "json"

      records:
        jmespath: "data.orders[]"
        primary_key: ["order_id", "line_item_id"]
        update_key: "modified_at"
        limit: 100000
        duplicate_tolerance: "5000000,0.001"

      processors:
        # Transform records
        - expression: "upper(record.status)"
          output: "record.status_upper"

        # Track max timestamp for incremental
        - expression: "record.modified_at"
          output: "state.last_modified"
          aggregation: "maximum"

      rules:
        # Handle rate limits
        - action: "retry"
          condition: "response.status == 429"
          max_attempts: 5
          backoff: "exponential"
```

## Accessing Response Data

In expressions, access response via `response.*`:

| Variable | Description |
|----------|-------------|
| `response.status` | HTTP status code |
| `response.headers` | Response headers (map) |
| `response.text` | Raw response body |
| `response.json` | Parsed JSON body |
| `response.records` | Extracted records array |

Examples:

```yaml
# Check status
stop_condition: 'response.status != 200'

# Access header
next_state:
  cursor: "{response.headers.x-next-cursor}"

# Access JSON field
stop_condition: 'response.json.has_more == false'

# Access last record
next_state:
  starting_after: "{response.records[-1].id}"
```

## Related Topics

- [PROCESSORS.md](PROCESSORS.md) - Response transformations
- [RULES.md](RULES.md) - Response rules and error handling
- [VARIABLES.md](VARIABLES.md) - Variable scopes
