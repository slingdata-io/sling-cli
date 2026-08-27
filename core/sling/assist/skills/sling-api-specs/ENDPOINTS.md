# Endpoints

Endpoints define specific API operations. They inherit settings from `defaults` and can override them.

## Basic Structure

```yaml
endpoints:
  # The key 'users' is the endpoint name
  users:
    description: "Retrieve users from the API"
    docs: "https://api.example.com/docs/users"  # Optional
    disabled: false  # Optional
    depends_on: []  # Optional, auto-inferred from queue usage

    state:
      # Endpoint-specific state variables
      limit: 100

    request:
      url: "{state.base_url}/users"
      # ... request config

    response:
      records:
        jmespath: "data[]"
      # ... response config
```

## Endpoint Options

| Field | Type | Description |
|-------|------|-------------|
| `description` | string | Human-readable description |
| `docs` | string | Documentation URL |
| `disabled` | boolean | Skip this endpoint when `true` |
| `depends_on` | array | Endpoints that must run first |
| `overrides` | object | Stream processor config (mode, hooks, object, primary_key) |
| `authentication` | object | Override connection-level auth (or `null` to disable) |
| `state` | object | Endpoint-specific state variables |
| `sync` | array | Variables to persist between runs |

## Setup Sequences

Run once before main requests. Use for job creation, metadata fetching, or polling:

```yaml
endpoints:
  export_data:
    setup:
      # Call 1: Create async export job
      - request:
          url: "{state.base_url}/export/create"
          method: "POST"
          payload:
            format: "json"
            filters:
              date_from: "{state.start_date}"
        response:
          processors:
            - expression: response.json.job_id
              output: state.job_id

      # Call 2: Poll until job completes
      - request:
          url: "{state.base_url}/export/status/{state.job_id}"
          method: "GET"
        response:
          rules:
            # Retry while processing
            - action: retry
              condition: response.json.status != "COMPLETED"
              max_attempts: 20
              backoff: constant
              backoff_base: 5  # Check every 5 seconds
            # Fail if errored
            - action: fail
              condition: response.json.status == "FAILED"
              message: "Export job failed: {response.json.error}"
          processors:
            - expression: response.json.download_url
              output: state.data_url

    # Main request uses URL from setup
    request:
      url: "{state.data_url}"
      method: "GET"
```

## Teardown Sequences

Run once after all requests complete. Use for cleanup, logging, or notifications:

```yaml
endpoints:
  export_data:
    # ... setup and request config ...

    teardown:
      - request:
          url: "{state.base_url}/export/cleanup/{state.job_id}"
          method: "DELETE"
```

## Extending Setup/Teardown

Use modifier syntax to prepend or append without overriding defaults:

```yaml
defaults:
  setup:
    - request:
        url: "{base_url}/auth/refresh"

endpoints:
  my_endpoint:
    # Runs BEFORE default setup
    +setup:
      - request:
          url: "{base_url}/pre-check"

    # Runs AFTER default teardown
    teardown+:
      - request:
          url: "{base_url}/cleanup"

    request:
      url: "{base_url}/data"
```

**Execution order:**
1. `+setup` (prepend)
2. `setup` (default or explicit)
3. `setup+` (append)

Same pattern for teardown.

## Dependencies

Dependencies are auto-inferred from queue usage, but can be explicit:

```yaml
endpoints:
  list_users:
    response:
      processors:
        - expression: "record.id"
          output: "queue.user_ids"

  get_user_details:
    # Auto-inferred: depends on list_users (consumes queue.user_ids)
    iterate:
      over: "queue.user_ids"
      into: "state.user_id"
    request:
      url: "{state.base_url}/users/{state.user_id}"
```

Explicit dependency:

```yaml
endpoints:
  second_endpoint:
    depends_on: ["first_endpoint"]
```

## Stream Overrides

Override replication settings per endpoint:

```yaml
endpoints:
  users:
    overrides:
      mode: "incremental"
      object: "schema.users_table"
      primary_key: ["id"]
      update_key: "updated_at"
      hooks:
        post:
          - type: query
            query: "REFRESH MATERIALIZED VIEW users_summary"
```

## Related Topics

- [REQUEST.md](REQUEST.md) - Request configuration
- [RESPONSE.md](RESPONSE.md) - Response handling
- [QUEUES.md](QUEUES.md) - Queue-based dependencies
