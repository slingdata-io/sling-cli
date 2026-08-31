# Variable Scopes and Expressions

Sling uses `{...}` syntax for embedding expressions and accessing variables.

## Variable Scopes

| Scope | Description | Read/Write | Example |
|-------|-------------|------------|---------|
| `env` | Environment variables | Read | `{env.USER}` |
| `secrets` | Credentials from connection | Read | `{secrets.api_key}` |
| `state` | Endpoint state variables | Read/Write | `{state.page}` |
| `sync` | Persisted values from previous run | Read | `{sync.last_id}` |
| `context` | Runtime values from replication | Read | `{context.mode}` |
| `auth` | Authentication data after auth | Read | `{auth.token}` |
| `request` | Current HTTP request | Read | `{request.url}` |
| `response` | HTTP response received | Read | `{response.status}` |
| `record` | Current record in processor | Read/Write | `{record.id}` |
| `queue` | Queue for endpoint chaining | Read/Write | `{queue.user_ids}` |
| `null` | Null value | Read | `{null}` |

## State Scope

Local to each endpoint. Updated by `next_state` or processors:

```yaml
endpoints:
  users:
    state:
      page: 1
      limit: 100
      base_url: "https://api.example.com"

    request:
      url: "{state.base_url}/users"
      parameters:
        page: "{state.page}"
        limit: "{state.limit}"
```

## Sync Scope

Read persisted values from previous run:

```yaml
endpoints:
  users:
    state:
      # Read last run's value, default to 30 days ago
      updated_since: >
        { coalesce(
            sync.last_updated,
            date_format(date_add(now(), -30, "day"), "%Y-%m-%dT%H:%M:%SZ")
          )
        }

    # Persist state.last_updated as sync.last_updated
    sync: [last_updated]
```

## Context Scope

Read-only runtime values from replication config:

| Variable | Type | Description |
|----------|------|-------------|
| `context.mode` | string | Replication mode (full-refresh, incremental) |
| `context.store` | map | Store values from replication |
| `context.limit` | integer | Max records from source_options |
| `context.range_start` | string | Backfill range start |
| `context.range_end` | string | Backfill range end |

```yaml
state:
  start_date: '{coalesce(context.range_start, sync.last_date, "2024-01-01")}'
```

## Auth Scope

Available after authentication:

```yaml
request:
  headers:
    Authorization: "Bearer {auth.token}"
```

## Request Scope

Information about current request (in rules/pagination):

| Variable | Description |
|----------|-------------|
| `request.url` | Request URL |
| `request.method` | HTTP method |
| `request.headers` | Request headers |
| `request.payload` | Request body |
| `request.attempts` | Retry attempt count |

## Response Scope

Available after response received:

| Variable | Description |
|----------|-------------|
| `response.status` | HTTP status code |
| `response.headers` | Response headers |
| `response.text` | Raw response body |
| `response.json` | Parsed JSON body |
| `response.records` | Extracted records |

## Record Scope

Available in processors:

```yaml
processors:
  - expression: "upper(record.name)"
    output: "record.name_upper"
```

## State Variable Rendering Order

State variables resolve dependencies automatically via topological sort:

```yaml
state:
  base_url: "https://api.com"              # No dependencies
  users_url: "{state.base_url}/users"      # Depends on base_url
  full_url: "{state.users_url}?limit=100"  # Depends on users_url
# Renders: base_url → users_url → full_url
```

### Circular Dependency Detection

Circular dependencies cause errors at validation time:

```yaml
# ❌ ERROR: Circular dependency
state:
  var_a: "{state.var_b}"  # A → B
  var_b: "{state.var_a}"  # B → A (circular!)
```

## Expression Syntax

Use `{...}` for embedding expressions:

```yaml
# Simple variable access
url: "{state.base_url}/users"

# Arithmetic
page: "{state.page + 1}"

# Function call
timestamp: "{date_format(now(), \"%Y-%m-%dT%H:%M:%SZ\")}"

# Conditional
cursor: '{if(is_null(sync.cursor), "", sync.cursor)}'

# Comparison
stop_condition: 'response.json.has_more == false'

# Multiline expression
updated_since: >
  { coalesce(
      env.LAST_UPDATED,
      sync.last_updated,
      date_format(date_add(now(), -30, "day"), "%Y-%m-%dT%H:%M:%SZ")
    )
  }
```

## String Literals

Double quotes (`"`) are the usual form. Single-quoted SQL-style literals (`'%Y-%m-%d'`) are also accepted.

```yaml
# ✅ Either quote style
expression: 'record.status == "active"'
expression: "date_format(now(), '%Y-%m-%d')"
```

## Best Practices

1. **Keep chains short**: Avoid deep state variable dependencies
2. **Use descriptive names**: `state.last_updated` not `state.lu`
3. **Reference external scopes directly**: Use `{secrets.key}` instead of `{state.key: "{secrets.key}"}`
4. **Use coalesce for defaults**: `{coalesce(sync.val, state.val, "default")}`
5. **Check nulls before accessing nested fields**: `{if(!is_null(record.meta), record.meta.field, null)}`

## Related Topics

- [FUNCTIONS.md](FUNCTIONS.md) - Expression functions
- [INCREMENTAL.md](INCREMENTAL.md) - Sync scope for incremental loading
- [PROCESSORS.md](PROCESSORS.md) - Using expressions in processors
