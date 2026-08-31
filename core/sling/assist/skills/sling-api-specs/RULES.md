# Response Rules

Rules define how to handle different response conditions. Rules are evaluated in order; execution stops at the first match.

## Basic Structure

```yaml
response:
  rules:
    - action: "retry"
      condition: "response.status == 429"
      max_attempts: 5
      backoff: "exponential"
      backoff_base: 2
      message: "Rate limited, retrying..."
```

## Rule Actions

| Action | Description |
|--------|-------------|
| `retry` | Retry the request |
| `continue` | Process rules but continue normally |
| `skip` | Skip extracting records, continue pagination |
| `break` | Stop current iteration, continue other iterations |
| `stop` | Stop entire endpoint execution |
| `fail` | Fail with error |

## Action Details

### retry

Retry the request with backoff:

```yaml
- action: "retry"
  condition: "response.status == 429 || response.status >= 500"
  max_attempts: 5
  backoff: "exponential"
  backoff_base: 2
  message: "Server error, retrying..."
```

### continue

Acknowledge condition but continue normally:

```yaml
- action: "continue"
  condition: "response.status == 404"
  message: "Resource not found, continuing..."
```

### skip

Skip extracting records from this response but continue pagination:

```yaml
- action: "skip"
  condition: "response.status == 204"
  message: "No content, skipping records"
```

### break

Stop processing for current iteration only (in iterate loops):

```yaml
- action: "break"
  condition: "state.items_processed >= state.max_items"
  message: "Reached max items for this iteration"
```

### stop

Completely stop endpoint execution (all iterations):

```yaml
- action: "stop"
  condition: "response.json.is_complete == true"
  message: "Data extraction complete"
```

### fail

Fail with error message:

```yaml
- action: "fail"
  condition: "response.status == 401 || response.status == 403"
  message: "Authentication failed"
```

## Backoff Strategies

| Strategy | Description |
|----------|-------------|
| `none` | No delay between retries |
| `constant` | Fixed delay (`backoff_base` seconds) |
| `linear` | Increasing delay (attempt × `backoff_base`) |
| `exponential` | Exponential delay (2^attempt × `backoff_base`) |
| `jitter` | Exponential + random jitter |

```yaml
# Constant: 2s, 2s, 2s...
backoff: "constant"
backoff_base: 2

# Linear: 2s, 4s, 6s...
backoff: "linear"
backoff_base: 2

# Exponential: 2s, 4s, 8s, 16s...
backoff: "exponential"
backoff_base: 2

# Jitter: exponential + random
backoff: "jitter"
backoff_base: 2
```

## Rate Limit Headers

For `response.status == 429`, Sling auto-detects rate limit headers:

- `RateLimit-Reset`
- `Retry-After`
- `RateLimit-Policy`

If found, uses header value instead of configured backoff:

```yaml
rules:
  - action: "retry"
    condition: "response.status == 429"
    max_attempts: 3
    backoff: "exponential"  # Fallback if no headers
    backoff_base: 2
```

## Default Rules

These rules are implicitly added (processed last):

```yaml
# Auto-retry on rate limit
- action: retry
  condition: response.status == 429
  max_attempts: 3
  backoff: linear
  backoff_base: 2

# Fail on client/server errors
- action: fail
  condition: response.status >= 400
```

## Extending Rules

Use modifier syntax to prepend or append:

```yaml
defaults:
  response:
    rules:
      - action: retry
        condition: response.status >= 500
        max_attempts: 3

endpoints:
  special_endpoint:
    response:
      # Add BEFORE defaults
      +rules:
        - action: break
          condition: response.status == 403
          message: "Access denied"

      # Add AFTER defaults (before hardcoded rules)
      rules+:
        - action: continue
          condition: response.status == 404
```

**Final order:**
1. `+rules` (prepend)
2. `rules` (default or explicit)
3. `rules+` (append)
4. Hardcoded retry/fail rules (always last)

## Common Patterns

### Handle Rate Limits

```yaml
rules:
  - action: retry
    condition: response.status == 429
    max_attempts: 10
    backoff: exponential
    backoff_base: 1
```

### Ignore Not Found

```yaml
rules:
  - action: continue
    condition: response.status == 404
    message: "Resource not found, skipping"
```

### Stop on Empty

```yaml
rules:
  - action: stop
    condition: length(response.records) == 0
    message: "No more records"
```

### Fail on Auth Error

```yaml
rules:
  - action: fail
    condition: response.status == 401
    message: "Invalid credentials"
```

### Retry on Specific Error

```yaml
rules:
  - action: retry
    condition: 'contains(response.text, "temporarily unavailable")'
    max_attempts: 5
    backoff: constant
    backoff_base: 30
```

### Break on Permission Denied

```yaml
rules:
  - action: break
    condition: response.status == 403
    message: "Permission denied for this resource"
```

## Condition Expressions

Access response data in conditions:

```yaml
# Status code
condition: response.status == 429

# Header value
condition: 'response.headers.x-rate-limit-remaining == "0"'

# JSON body
condition: response.json.error_code == "RATE_LIMITED"

# Text content
condition: 'contains(response.text, "error")'

# Combined
condition: response.status >= 500 || response.status == 429

# State-based
condition: request.attempts >= 3
```

## Related Topics

- [RESPONSE.md](RESPONSE.md) - Response processing
- [VARIABLES.md](VARIABLES.md) - Accessing response variables
- [FUNCTIONS.md](FUNCTIONS.md) - Functions for conditions
