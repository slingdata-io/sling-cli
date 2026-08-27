# Request Configuration

HTTP request configuration for API endpoints.

## Full Structure

```yaml
request:
  # Full URL with state variables
  url: "{state.base_url}/users/{state.user_id}?active=true"

  # HTTP method (default: GET)
  method: "POST"

  # Headers merged with defaults.request.headers
  headers:
    Content-Type: "application/json"
    Authorization: "Bearer {auth.token}"
    X-Request-ID: "{uuid()}"

  # URL query parameters
  parameters:
    page: "{state.page}"
    limit: 100
    status: "active"

  # Request body for POST/PUT/PATCH
  payload:
    user:
      name: "New User"
      email: "{state.user_email}"

  # Request timeout in seconds (default: 30)
  timeout: 60

  # Max requests per second (default: 2)
  rate: 5

  # Max concurrent requests (default: 5)
  concurrency: 3
```

## URL Configuration

URLs can include state variables and expressions:

```yaml
request:
  url: "{state.base_url}/users/{state.user_id}"
```

## Parameters

Query parameters for GET/DELETE, or form-encoded body for POST/PUT/PATCH:

```yaml
request:
  url: "{state.base_url}/search"
  parameters:
    q: "{state.search_term}"
    page: "{state.page}"
    per_page: 100
    sort: "created_at"
    order: "desc"
```

Becomes: `/search?q=...&page=...&per_page=100&sort=created_at&order=desc`

## Payload

Request body for POST/PUT/PATCH. Auto-JSON-encoded if Content-Type is `application/json`:

```yaml
request:
  method: "POST"
  headers:
    Content-Type: "application/json"
  payload:
    filters:
      status: "active"
      created_after: "{state.start_date}"
    options:
      include_metadata: true
```

## Rate Limiting

Control request frequency:

```yaml
defaults:
  request:
    rate: 10        # Max 10 requests per second
    concurrency: 5  # Max 5 concurrent requests
    timeout: 30     # 30 second timeout
```

Endpoint-level override:

```yaml
endpoints:
  heavy_endpoint:
    request:
      rate: 2          # Slower rate for this endpoint
      concurrency: 1   # Sequential only
      timeout: 120     # Longer timeout
```

## Dynamic URLs

Using state variables for dynamic paths:

```yaml
endpoints:
  user_orders:
    iterate:
      over: "queue.user_ids"
      into: "state.user_id"

    request:
      url: "{state.base_url}/users/{state.user_id}/orders"
```

## Conditional Headers

Using expressions in headers:

```yaml
request:
  headers:
    Authorization: "Bearer {auth.token}"
    X-Request-ID: "{uuid()}"
    X-Timestamp: "{date_format(now(), \"%Y-%m-%dT%H:%M:%SZ\")}"
    X-Idempotency-Key: '{if(state.retry_count > 0, state.idempotency_key, uuid())}'
```

## Defaults

Set defaults for all endpoints:

```yaml
defaults:
  request:
    method: "GET"
    headers:
      Accept: "application/json"
      User-Agent: "Sling/1.0"
    timeout: 30
    rate: 10
    concurrency: 5
```

Endpoint-level config merges with and can override defaults.

## Related Topics

- [VARIABLES.md](VARIABLES.md) - Using state variables in requests
- [PAGINATION.md](PAGINATION.md) - How requests are repeated for pagination
- [FUNCTIONS.md](FUNCTIONS.md) - Expression functions for dynamic values
