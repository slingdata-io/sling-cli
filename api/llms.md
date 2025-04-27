# Sling API Specification Guide for LLMs

This guide explains how to create API specifications for Sling. Follow this document along with the target API\'s official documentation to build a properly formatted API spec.

## Overview

Sling API specs are YAML files that define how to interact with REST APIs. They specify authentication, endpoints, request formation, iteration, pagination, response processing, and state management.

Principal Instructions for LLMs:
- Use the documentation details below to construct a Sling API spec.
- The Sling API specs are intended to extract data from API sources. It is not intended for pushing or updating data into an API endpoint (typically via methods `POST`/`PATCH`/`PUT`/`DELETE`). Ignore or omit endpoints which are intended for creating/updating/deleting data into an API resource.
- When reading API documentation, if an endpoint is marked as `DEPRECATED`, you should ignore this endpoint unless explicitly instructed by the user.
- Don't be verbose with extra comments indicating optional additional inputs/parameters, or whether a pagination is required or not, etc. Only put comments in a section of the API spec where it might be confusing for a human.
- Make sure to use the browser MCP (if available) to understand how the subject API resource works. This includes: Authentication, Pagination and how the response is structured for each endpoint, so that Sling can parse the data correctly.
- If something in the API documentation is unclear, pause and ask the human operator for instructions. It is better to get confirmation than to build an unstable API spec.


## Basic Structure

```yaml
name: "API Name"
description: "Description of the API"

defaults:
  state:
    base_url: https://api.base.url/v1
  
  # Default settings for all endpoints
  request:
    headers:
      Accept: "application/json"

endpoints:
  # Name must match the key in the endpoints map
  endpoint_name:
    description: "Endpoint description"
    request:
      url: "${state.base_url}/resource"
      method: "GET"
    
    response:
      records:
        jmespath: "data[]" # JMESPath to extract records
```

## Full Structure

```yaml
# Name of the API
name: "Example API"

# Description of what the API does
description: "This API provides access to example data"

# Declares queues for passing data between endpoints
queues:
  - user_ids

# Authentication configuration for accessing the API
authentication:
  # Type of authentication: "bearer", "basic", "oauth2", or empty for none
  type: "bearer"

  # Bearer token for authentication
  token: "${secrets.api_token}"

  # Basic authentication credentials
  username: "${secrets.username}"
  password: "${secrets.password}"


# Default settings applied to all endpoints
defaults:
  # Default request configuration
  request:
    method: "GET"
    headers:
      Accept: "application/json"
    timeout: 30 # seconds
    rate: 10    # max requests per second (default: 2)
    concurrency: 5 # max concurrent requests (default: 5)

  # Default state variables
  state:
    limit: 100
    base_url: "https://api.example.com/v1"

  # Default pagination settings
  pagination:
    stop_condition: "!response.json.has_more" # Common for cursor/offset

  # Default response settings
  response:
    rules:
      # Default rules are implicitly added:
      # - action: retry, condition: response.status == 429, max_attempts: 3, backoff: linear, backoff_base: 2
      # - action: fail, condition: response.status >= 400
      # You can override these or add more specific rules.
      - action: "continue" # Example: ignore 404 errors
        condition: "response.status == 404"

# Map of API endpoints to interact with
endpoints:
  users:
    # Description of what this endpoint does
    description: "Retrieve a list of users with incremental sync"

    # Initial state variables for this endpoint (merged with defaults.state)
    state:
      # Get last sync timestamp from persistent state, default to 30 days ago
      updated_since: >
        ${ coalesce(
             sync.last_updated,
             date_format(date_add(now(), -30, 'day'), '%Y-%m-%dT%H:%M:%SZ')
           )
        }
      page: 1 # Example for page-based pagination

    # Variables to persist between API runs for incremental loading
    # Values are read from 'sync.' scope and written to 'state.' scope for the next run.
    sync: ["last_updated"]

    # HTTP request configuration
    request:
      # Path relative to defaults.request.url
      url: ${state.base_url}/users
      # Method overrides default if needed
      method: "GET"
      # Headers merged with defaults.request.headers
      headers:
        X-Custom-Header: "value"
      # URL query parameters
      parameters:
        page: "${state.page}"
        limit: "${state.limit}"
        updated_since: "${state.updated_since}" # Use the state variable

    # Pagination configuration
    pagination:
      # How to update state for the next page request
      next_state:
        page: "${state.page + 1}"
      # Overrides default stop_condition
      stop_condition: "response.json.page >= response.json.total_pages"

    # Response processing configuration
    response:
      # How to extract records from response
      records:
        # JMESPath expression to extract records array from response
        jmespath: "data.users[]"
        # Field(s) that uniquely identify each record (used for deduplication)
        primary_key: ["id"]
        # Optional: Field used for incremental updates (informational)
        update_key: "updated_at"
        # Optional: Max records to process (useful for testing)
        limit: 1000
        # Optional: Specify Bloom filter parameters for efficient deduplication of large datasets
        # Format: "<estimated_items>,<false_positive_probability>" (e.g., "1000000,0.001")
        duplicate_tolerance: "1000000,0.001"

      # Post-processing transformations and aggregations
      processors:
        # Example 1: Add a processed field to each record
        - expression: "upper(record.name)"
          output: "record.upper_name" # Target: record.<new_field>

        # Example 2: Aggregate the maximum 'updated_at' timestamp into state for the next sync
        - expression: "record.updated_at"
          output: "state.last_updated" # Target: state.<variable_name>
          aggregation: "maximum"

        # Example 3: Send user IDs to a queue for another endpoint to process
        - expression: "record.id"
          output: "queue.user_ids" # Target: queue.<queue_name>

      # Endpoint-specific rules (merged with defaults.response.rules)
      rules:
        - action: "fail"
          condition: "response.status == 403"
          message: "Authentication failed for list_users"

  # Example of an endpoint that iterates over values from a queue
  get_user_details:
    description: "Retrieve details for each user ID from the queue"

    # Iteration configuration
    iterate:
      # Iterate over items received from the 'user_ids' queue
      over: "queue.user_ids"
      # Store the current user ID in state.current_user_id for each iteration
      into: "state.current_user_id"
      # Number of concurrent iterations/requests to run
      concurrency: 10

    request:
      # Use the iterated user ID in the URL
      url: ${state.base_url}/users/${state.current_user_id}
      method: "GET"

    response:
      records:
        jmespath: "user" # Assuming the response structure is {"user": {...}}
        primary_key: ["id"]
```

## Queues

You can declare queues at the top level of your spec using the `queues` key. Queues allow passing data (like IDs or other values) generated by one endpoint to another endpoint for further processing using the `iterate` feature.

```yaml
# Declare queues at the top level
queues:
  - product_ids
  - order_ids

endpoints:
  products:
    # ... request config ...
    response:
      processors:
        # Send product IDs to the 'product_ids' queue
        - expression: "record.id"
          output: "queue.product_ids"

  product_inventory:
    description: "Get inventory for each product ID from the queue"
    iterate:
      # Iterate over values from the 'product_ids' queue
      over: "queue.product_ids"
      into: "state.current_product_id"
      concurrency: 10 # Process 10 products concurrently
    
    request:
      url: ${state.base_url}/products/${state.current_product_id}/inventory
      # ... other request config ...
    response:
      # ... response config ...
```

Queues are backed by temporary files and operate in a write-then-read manner within a single Sling run. An endpoint producing data writes to the queue (`output: queue.<name>`), and another endpoint consumes it (`iterate.over: queue.<name>`).

## Authentication

Supported authentication types:

```yaml
authentication:
  # No Authentication
  type: "" # or omit the authentication block

  # Basic Auth
  type: "basic"
  username: "${secrets.username}"
  password: "${secrets.password}"

  # Bearer Token
  type: "bearer"
  token: "${secrets.api_token}" # Can use secrets, env, or state vars
```

## Variable Scopes and Expressions

Sling uses `${...}` syntax for embedding expressions and accessing variables within YAML strings. Expressions are evaluated using the `goval` library with custom functions.

Available variable scopes:
- `env`: Environment variables (e.g., `${env.USER}`).
- `secrets`: Sensitive credentials passed to Sling (e.g., `${secrets.api_key}`).
- `state`: Variables defined in `defaults.state` or `endpoints.<name>.state`. These are local to each endpoint iteration and can be updated by pagination (`next_state`) or processors (`output: state.<var>`).
- `sync`: Persistent state variables read at the start of an endpoint run (values from the previous run's `state` matching the `sync` list). Use `${coalesce(sync.var, state.var, default_value)}`.
- `auth`: Authentication data after successful authentication (e.g., `${auth.token}` for OAuth2).
- `request`: Information about the current HTTP request being made (available in rule/pagination evaluation). Includes `request.url`, `request.method`, `request.headers`, `request.payload`, `request.attempts`.
- `response`: Information about the HTTP response received (available in rule/pagination/processor evaluation). Includes `response.status`, `response.headers`, `response.text`, `response.json` (parsed body), `response.records` (extracted records).
- `record`: The current data record being processed by a processor (available only within `response.processors`).
- `queue`: Access queues declared at the top level (e.g., `iterate.over: queue.my_queue`).
- `null`: Represents a null value (e.g., `coalesce(state.value, null)`).

State variables (`state.`) within an endpoint have a defined render order. If `state.b` depends on `state.a` (`state.b: "${state.a + 1}"`), `state.a` will be evaluated first. Circular dependencies are detected and cause an error.

## Endpoints

Each key under the `endpoints` map defines an endpoint. The key itself **must** be used as the `name` of the endpoint internally, though you can add a `description` field for clarity.

```yaml
endpoints:
  # The key 'users' is the effective name
  users:
    description: "Retrieve users from the API"
    # ... other endpoint config ...

  # The key 'get_details' is the effective name
  get_details:
    description: "Get item details"
    # ... other endpoint config ...
```

Endpoints define specific API operations. They inherit settings from `defaults` and can override them.

## Request Configuration

```yaml
request:
  # Request full URL, can includ state variables
  url: "${state.base_url}/users/${state.user_id}?active=true"
  # Method: GET, POST, PUT, PATCH, DELETE, etc.
  method: "POST"
  # Headers: Merged with defaults.request.headers
  headers:
    Content-Type: "application/json"
    Authorization: "Bearer ${auth.token}"
    X-Request-ID: "${uuid()}"
  # Parameters: Added as URL query parameters for GET/DELETE,
  # or form-encoded body for POST/PUT/PATCH if Content-Type is application/x-www-form-urlencoded
  parameters:
    page: ${state.page}
    limit: 100
    status: "active"
  # Payload: Used as request body for POST/PUT/PATCH.
  # Can be a string, map, or list. Will be JSON-encoded if Content-Type is application/json.
  payload:
    user:
      name: "New User"
      email: "${state.user_email}"
  # Timeout: Request timeout in seconds (default: 30)
  timeout: 60
  # Rate: Max requests per second for this endpoint (default: 2)
  rate: 5
  # Concurrency: Max concurrent requests for this endpoint (default: 5)
  concurrency: 3
```

## Iteration (Looping Requests)

The `iterate` section allows an endpoint to make multiple requests based on a list of items (e.g., IDs from a queue, date ranges).

```yaml
iterate:
  # Expression evaluated to get items to loop over.
  # Can be an array, a queue reference ('queue.name'), or a function like range() or chunk().
  over: "queue.product_ids"
  # Name of the state variable to store the current item in each iteration. Must start with 'state.'.
  into: "state.current_product_id"
  # Optional: Number of iterations to run concurrently (default: 10).
  concurrency: 5
  # Optional: Condition to evaluate before starting iteration.
  # if: "state.process_details == true"
```

Each iteration runs independently with its own copy of the initial state, modified by the `into` variable and subsequent pagination/processors within that iteration.

**Example: Iterating over a date range:**

```yaml
iterate:
  over: >
    range(
      date_trunc(date_add(now(), -7, "day"), "day"), # Start date: 7 days ago
      date_trunc(now(), "day"),                      # End date: today
      "1d"                                           # Step: 1 day
    )
  into: state.current_day
  concurrency: 10
request:
  parameters:
    date: ${date_format(state.current_day, "%Y-%m-%d")}
    # ... other params ...
```

**Example: Iterating over chunks of IDs from a queue:**

```yaml
iterate:
  # Use the chunk() function to process IDs in batches of 50
  over: "chunk(queue.variant_ids, 50)"
  into: "state.variant_id_batch" # state.variant_id_batch will be an array/list
  concurrency: 5
request:
  parameters:
    # Join the batch of IDs into a comma-separated string for the API parameter
    ids: ${join(state.variant_id_batch, ",")}
```

## Pagination

Controls how Sling navigates through multiple pages of results for *each iteration* (if `iterate` is used) or for the single endpoint execution (if `iterate` is not used).

```yaml
pagination:
  # Map of state variables to update *before* the next request in the current iteration.
  # Expressions are evaluated based on the *current* state and response (if needed).
  next_state:
    # Example 1: Cursor-based (using last record ID)
    starting_after: "${response.records[-1].id}"
    # Example 2: Page number based
    # page: "${state.page + 1}"
    # Example 3: Offset based
    # offset: "${state.offset + state.limit}"
    # Example 4: Using URL from response header (e.g., Link header)
    # url: >
    #  ${
    #      if(contains(response.headers.link, "rel=\"next\""),
    #         trim(split_part(split(response.headers.link, ",")[0], ";", 0), "<>"),
    #         null # Or keep current state.url? Needs careful handling.
    #      )
    #   }

  # Expression evaluated *after* a response is received to check if pagination should stop for the current iteration.
  # If true, no more requests are made for this iteration/endpoint.
  stop_condition: "!response.json.has_more || length(response.records) == 0"
  # Example: Page-based stop
  # stop_condition: "response.json.page >= response.json.total_pages"
  # Example: Stop if next page URL is missing in header
  # stop_condition: "!contains(jmespath(response.headers, \"link\"), \"rel=\\\"next\\\"\")"
```

**Note:** If `next_state` expressions depend on `response.*` variables, Sling waits for the current request to finish before evaluating them. Otherwise, `next_state` is evaluated immediately to prepare the next request potentially in parallel.

## Response Processing

Extracts and transforms data from responses.

```yaml
response:
  records:
    # JMESPath expression to extract an array of records from the response.json
    jmespath: "data.items[]"
    # Primary Key: Field(s) used to uniquely identify records for deduplication.
    primary_key: ["id", "location_id"] # Can be a single string or list
    # Update Key: Informational field often used in incremental logic.
    update_key: "updated_at"
    # Limit: Max number of records to process per endpoint run (for testing).
    limit: 5000
    # Duplicate Tolerance: Enables Bloom filter for efficient deduplication on large datasets.
    # Format: "<estimated_items>,<false_positive_probability>"
    duplicate_tolerance: "1000000,0.001"

  processors:
    # Applied to each record extracted by jmespath.
    - # Expression to evaluate. Can access 'record', 'state', 'response', etc.
      expression: "date_parse(record.created_ts, 'auto')"
      # Target output location:
      # - 'record.<new_field>': Adds/updates a field in the current record.
      # - 'queue.<queue_name>': Appends the result to the named queue.
      output: "record.created_at_dt"

    - # Example using aggregation
      expression: "record.amount"
      # Target output for aggregation must be 'state.<variable>'
      output: "state.total_amount"
      # Aggregation type: maximum, minimum, flatten, first, last (default: none)
      aggregation: "sum" # Note: 'sum' is not explicitly listed but implied by common need. Check functions.go

  rules:
    # Define actions based on response conditions (status code, headers, body).
    # Rules are evaluated in order. Execution stops at the first matching rule with action stop/fail/retry.
    - # Action: retry, continue, stop, fail
      action: "retry"
      # Condition: Expression evaluating to true triggers the action.
      condition: "response.status == 429 || response.status >= 500"
      # Max attempts for retry action.
      max_attempts: 5
      # Backoff strategy for retry: none, constant, linear, exponential, jitter
      backoff: "exponential"
      # Base delay in seconds for backoff calculation (default: 1 for retry)
      backoff_base: 2
      # Optional message for logging when rule is met.
      message: "Server error or rate limit hit, retrying..."

    - action: "fail"
      condition: "response.status == 401 || response.status == 403"
      message: "Authentication/Authorization failed"

    # Default rules (implicitly added if not defined, processed last):
    # - action: retry, condition: response.status == 429, ...
    # - action: fail, condition: response.status >= 400
    # You can override the defaults by providing your own rules for these conditions.
    # Example: To ignore 404 errors and continue:
    # - action: "continue"
    #   condition: "response.status == 404" # Ignore 404s
```

**Deduplication:** If `primary_key` is defined, Sling automatically tracks seen keys within the current run and skips duplicate records. For very large datasets where storing all keys in memory is infeasible, specifying `duplicate_tolerance` activates a Bloom filter for probabilistic deduplication.

**Rules & Retries:** Sling automatically handles standard `RateLimit-Reset` or `Retry-After` headers when `response.status == 429` and the action is `retry`, using the header value for the backoff duration if available, otherwise falling back to the configured `backoff` strategy.

## Sync State for Incremental Loads

The `sync` key in an endpoint definition allows persisting specific `state` variables between runs, enabling incremental data loading.

```yaml
endpoints:
  incremental_data:
    state:
      # Read the last sync timestamp from persistent state ('sync.last_sync_ts').
      # If it's the first run (sync.last_sync_ts is null), use a default start date.
      start_timestamp: >
        ${ coalesce(
             sync.last_sync_ts,
             date_format(date_add(now(), -7, 'day'), '%Y-%m-%dT%H:%M:%SZ')
           )
        }

    # List of state variables to persist.
    # At the end of the run, the final values of state.last_sync_ts (and any others listed)
    # will be saved and made available as sync.last_sync_ts in the next run.
    sync: [last_sync_ts]

    request:
      parameters:
        updated_since: "${state.start_timestamp}"

    response:
      processors:
        # Find the maximum timestamp from the current batch of records.
        - expression: "record.updated_at"
          # Store the maximum value in state.last_sync_ts.
          output: "state.last_sync_ts"
          # Use the 'maximum' aggregation across all records processed by this endpoint run.
          aggregation: "maximum"
```

**Workflow:**
1.  **Run Start:** Sling loads persisted sync values (e.g., `last_sync_ts` from the previous run) into the `sync.` scope.
2.  **State Initialization:** The endpoint's `state` is initialized. Expressions like `${coalesce(sync.last_sync_ts, ...)}` read the persisted value or use a default.
3.  **Requests:** Requests are made using the initialized state (e.g., `updated_since: "${state.start_timestamp}"`).
4.  **Processing:** Processors update state variables (e.g., `output: state.last_sync_ts`, `aggregation: maximum`).
5.  **Run End:** The final values of the state variables listed in the `sync` array (e.g., `state.last_sync_ts`) are persisted for the next run.

## Template for New API Specs

```yaml
name: "API Name"
description: "API Description"

# Optional: Declare queues if needed
# queues:
#   - item_ids

authentication:
  type: "bearer" # bearer|basic|oauth2|""
  token: "${secrets.api_token}" # Or username/password, or OAuth2 config

defaults:
  request:
    method: "GET"
    headers:
      Accept: "application/json"
      # Common headers like User-Agent can go here
    timeout: 30
    rate: 10 # Adjust based on API limits
    concurrency: 5
  state:
    base_url: "https://api.base.url/v1" # Base URL for API requests
    limit: 100 # Default page size or limit

endpoints:
  # Key is the endpoint name
  items:
    description: "Fetch a list of items"
    state:
      # Example state for pagination or filtering
      starting_after: ${sync.last_item_id} # For cursor pagination + incremental
      created_since: ${coalesce(sync.last_sync_time, date_format(date_add(now(), -1, 'day'), '%Y-%m-%d'))}

    # Persist last item ID and timestamp for next run
    sync: [last_item_id, last_sync_time]

    request:
      url: "${state.base_url}/items" # Relative to defaults.request.url
      parameters:
        limit: ${state.limit}
        starting_after: ${state.starting_after}
        created_since: ${state.created_since}

    pagination:
      # Update 'starting_after' state with the ID of the last record from the response
      next_state:
        starting_after: "${response.records[-1].id}"
      # Stop when the API indicates no more pages or returns an empty list
      stop_condition: "!response.json.has_more || length(response.records) == 0"

    response:
      records:
        jmespath: "data[]" # JMESPath to extract records array
        primary_key: ["id"] # Field(s) for deduplication
        # Optional: for large datasets
        # duplicate_tolerance: "1000000,0.001"

      processors:
        # Store the ID of the last record processed in this run for the next pagination cursor
        - expression: "record.id"
          output: "state.last_item_id"
          aggregation: "last"

        # Store the latest updated_at timestamp for the next incremental run
        - expression: "record.updated_at"
          output: "state.last_sync_time"
          aggregation: "maximum"

        # Optional: Send IDs to a queue for another endpoint
        # - expression: "record.id"
        #   output: "queue.item_ids"

      # Optional: Custom rules for this endpoint
      # rules:
      #   - action: "continue"
      #     condition: "response.status == 404" # Ignore 404s

  # Optional: Another endpoint using iteration
  # get_item_details:
  #   description: "Get details for each item ID"
  #   iterate:
  #     over: "queue.item_ids"
  #     into: "state.current_item_id"
  #     concurrency: 10
  #   request:
  #     url: "${state.base_url}/items/${state.current_item_id}"
  #   response:
  #     records:
  #       jmespath: "data" # Assuming single item response
  #       primary_key: ["id"]

```

---

## Expression Functions

You can use the following functions within `${...}` expressions in your API spec. Functions provide capabilities for data manipulation, type casting, date operations, control flow, and more.

See [functions.go](https://github.com/slingdata-io/sling-cli/blob/main/core/dbio/api/functions.go) for the source code and authoritative list.
See [functions_test.go](https://github.com/slingdata-io/sling-cli/blob/main/core/dbio/api/functions_test.go) for usage examples.

*(Function tables need careful review against functions.go - the following is based on the previous version and source analysis)*

### String Functions

| Function                            | Description                          | Parameters                                                       | Returns          | Example                                            |
| ----------------------------------- | ------------------------------------ | ---------------------------------------------------------------- | ---------------- | -------------------------------------------------- |
| `upper(string)`                     | Converts string to uppercase         | `string`: Input string                                           | Uppercase string | `upper("hello")` → "HELLO"                       |
| `lower(string)`                     | Converts string to lowercase         | `string`: Input string                                           | Lowercase string | `lower("HELLO")` → "hello"                       |
| `substring(string, start[, end])`   | Extracts part of a string            | `string`: Input string<br>`start`: Start index<br>`end`: Optional end index | Substring        | `substring("hello world", 0, 5)` → "hello"       |
| `replace(string, pattern, replace)` | Replaces occurrences of pattern      | `string`, `pattern`, `replacement`                               | Modified string  | `replace("hello", "l", "x")` → "hexxo"           |
| `trim(string)`                      | Removes whitespace from start/end    | `string`: Input string                                           | Trimmed string   | `trim(" hello ")` → "hello"                      |
| `contains(string, substring)`       | Checks if string contains substring  | `string`, `substring`                                            | Boolean          | `contains("hello world", "world")` → true        |
| `split(string, separator)`          | Splits string into array             | `string`, `separator`                                            | Array of strings | `split("a,b,c", ",")` → `["a", "b", "c"]`      |
| `split_part(string, sep, index)`    | Gets part of split string by index   | `string`, `separator`, `index` (1-based)                         | String part      | `split_part("a,b,c", ",", 2)` → "b"              |
| `join(array, separator)`            | Joins array elements into string     | `array`, `separator`                                             | String           | `join(["a", "b", "c"], ", ")` → "a, b, c"        |
| `length(string\|array\|map)`        | Gets length of string/array/map      | `value`: String, array or map                                    | Number           | `length("hello")` → 5, `length([1,2])` → 2       |

### Numeric Functions

| Function                         | Description                      | Parameters                               | Returns           | Example                                    |
| -------------------------------- | -------------------------------- | ---------------------------------------- | ----------------- | ------------------------------------------ |
| `int_parse(value)`               | Converts value to integer        | `value`: Value to convert                | Integer or error  | `int_parse("42")` → 42                     |
| `float_parse(value)`             | Converts value to float          | `value`: Value to convert                | Float or error    | `float_parse("42.5")` → 42.5               |
| `int_format(value, format)`      | Formats integer (Go format)      | `value`: Number<br>`format`: Format string | Formatted string  | `int_format(42, "%05d")` → "00042"         |
| `float_format(value, format)`    | Formats float (Go format)        | `value`: Number<br>`format`: Format string | Formatted string  | `float_format(42.5678, "%.2f")` → "42.57"  |
| `greatest(array\|val1, val2...)` | Finds maximum value              | `array` or multiple values               | Maximum value     | `greatest(1, 5, 3)` → 5                    |
| `least(array\|val1, val2...)`    | Finds minimum value              | `array` or multiple values               | Minimum value     | `least(1, 5, 3)` → 1                       |
| `is_greater(val1, val2)`         | Checks if `val1 > val2`          | `val1`, `val2`: Values to compare        | Boolean           | `is_greater(5, 3)` → true                  |
| `is_less(val1, val2)`            | Checks if `val1 < val2`          | `val1`, `val2`: Values to compare        | Boolean           | `is_less(3, 5)` → true                     |

### Date Functions

Uses Go's `time` package and `strftime` conventions via [timefmt-go](https://github.com/itchyny/timefmt-go).

| Function                           | Description                    | Parameters                                              | Returns              | Example                                                                    |
| ---------------------------------- | ------------------------------ | ------------------------------------------------------- | -------------------- | -------------------------------------------------------------------------- |
| `now()`                            | Gets current date and time     | None                                                    | Time object          | `now()`                                                                    |
| `date_parse(string[, format])`     | Parses string to time object   | `string`: Date string<br>`format`: "auto" or `strftime` | Time object or error | `date_parse("2022-01-01T10:00:00Z", "auto")`                              |
| `date_format(date, format)`        | Formats time object to string  | `date`: Time object<br>`format`: `strftime` format        | Formatted string     | `date_format(now(), "%Y-%m-%d")` → "2023-10-27"                            |
| `date_add(date, duration[, unit])` | Adds duration to time object   | `date`, `duration` (int), `unit` (string, default "s")  | Time object          | `date_add(now(), -7, "day")`                                               |
| `date_diff(date1, date2[, unit])`  | Time between dates             | `date1`, `date2`, `unit` (string, default "s")          | Number (float)       | `date_diff(date_add(now(), 1, "day"), now(), "hour")` → 24.0            |
| `date_trunc(date, unit)`           | Truncates date to unit start   | `date`, `unit` ("year", "month", "day", "hour", etc.)   | Time object          | `date_trunc(now(), "month")` → First day of current month at 00:00:00      |
| `date_extract(date, part)`         | Extracts part from date        | `date`, `part` ("year", "month", "day", "hour", etc.)   | Number               | `date_extract(now(), "year")` → 2023                                       |
| `date_last(date[, period])`        | Gets last day of period        | `date`, `period` ("month", "year", default "month")     | Time object          | `date_last(now())` → Last day of current month                             |
| `date_first(date[, period])`       | Gets first day of period       | `date`, `period` ("month", "year", default "month")     | Time object          | `date_first(now())` → First day of current month                           |
| `range(start, end[, step])`        | Creates array of time objects  | `start`, `end` (time obj), `step` (string duration)     | Array of Time objects | `range(date_add(now(),-2,'day'), now(), '1d')` → `[t-2d, t-1d, t]`         |

*Date function `unit`/`part`/`period` parameters often accept: "year", "month", "week", "day", "hour", "minute", "second".*
*`range` function with dates requires time objects as start/end.*

#### Date Parsing and Formatting Examples

```yaml
# Auto-detect format
- expression: date_parse("05/15/2022", "auto")
  output: state.parsed_auto

# Specify strftime format
- expression: date_parse("15-May-2022 10:30", "%d-%b-%Y %H:%M")
  output: state.parsed_specific

# Format a date object
- expression: date_format(state.parsed_auto, "%Y%m%d")
  output: state.formatted_compact

# Format for API parameter (ISO 8601 with timezone)
request:
  parameters:
    updated_since: "${date_format(date_add(now(), -1, 'hour'), '%Y-%m-%dT%H:%M:%SZ')}"
```

### Value Handling Functions

| Function                     | Description                        | Parameters                                       | Returns                | Example                                                   |
| ---------------------------- | ---------------------------------- | ------------------------------------------------ | ---------------------- | --------------------------------------------------------- |
| `coalesce(val1, val2, ...)`  | Returns first non-null value       | Multiple values                                  | First non-null value   | `coalesce(null, sync.val, state.val, "default")`        |
| `value(val1, val2, ...)`     | Returns first non-empty value      | Multiple values                                  | First non-empty value  | `value("", state.val, "default")` → "default"             |
| `require(val[, error_msg])`  | Ensures value is not null or error | `val`, `error_msg` (optional)                    | Value or error         | `require(secrets.api_key, "API Key is required")`         |
| `cast(value, type)`          | Converts value to type             | `value`, `type` ("int", "float", "string", "bool") | Converted value or error | `cast("42", "int")` → 42                                |
| `try_cast(value, type)`      | Tries conversion, returns null     | `value`, `type`                                  | Converted value or null | `try_cast("abc", "int")` → null                         |
| `element(array, index)`      | Gets element at 0-based index      | `array`, `index` (integer)                       | Element or error       | `element(["a", "b"], 1)` → "b"                            |
| `is_null(value)`             | Checks if value is null            | `value`                                          | Boolean                | `is_null(state.optional_param)` → true or false         |
| `is_empty(value)`            | Checks if value is empty           | `value` (string, array, map)                     | Boolean                | `is_empty("")` → true, `is_empty([])` → true            |
| `if(condition, then, else)`  | Conditional expression             | `condition` (bool), `then_val`, `else_val`       | Selected value         | `if(state.count > 0, "has items", "empty")`             |
| `equals(val1, val2)`         | Checks deep equality             | `val1`, `val2`                                   | Boolean                | `equals(response.status, 200)` → true                   |

### Collection Functions

| Function                       | Description                     | Parameters                                | Returns           | Example                                                        |
| ------------------------------ | ------------------------------- | ----------------------------------------- | ----------------- | -------------------------------------------------------------- |
| `keys(map)`                    | Gets keys from map              | `map` object                              | Array of keys     | `keys({"a": 1, "b": 2})` → `["a", "b"]`                       |
| `values(map)`                  | Gets values from map            | `map` object                              | Array of values   | `values({"a": 1, "b": 2})` → `[1, 2]`                         |
| `jmespath(object, expression)` | Evaluates JMESPath expression   | `object`, `expression` (string)           | Query result      | `jmespath(response.json, "data.items[?age > 30]")`            |
| `get_path(object, path)`       | Gets value using dot notation   | `object`, `path` (string, e.g., "a.b[0]") | Value at path     | `get_path(response.json, "user.profile.email")`              |
| `filter(array, expression)`    | Filters array (goval expression)| `array`, `expression` (string uses `value`) | Filtered array    | `filter([1, 2, 3], "value > 1")` → `[2, 3]`                  |
| `map(array, expression)`       | Maps array (goval expression)   | `array`, `expression` (string uses `value`) | Transformed array | `map([1, 2, 3], "value * 2")` → `[2, 4, 6]`                    |
| `sort(array[, descending])`    | Sorts array elements            | `array`, `descending` (optional bool)     | Sorted array      | `sort([3, 1, 2])` → `[1, 2, 3]`                               |
| `chunk(array\|queue, size)`    | Splits array/queue into chunks  | `array` or `queue.name`, `size` (int)     | Channel of arrays | `iterate: over: chunk(queue.ids, 50)`                           |

### Encoding/Decoding Functions

| Function                    | Description             | Parameters                        | Returns         | Example                                          |
| --------------------------- | ----------------------- | --------------------------------- | --------------- | ------------------------------------------------ |
| `encode_url(string)`        | URL encodes a string    | `string`                          | Encoded string  | `encode_url("a b")` → "a%20b"                     |
| `decode_url(string)`        | URL decodes a string    | `string`                          | Decoded string  | `decode_url("a%20b")` → "a b"                    |
| `encode_base64(string)`     | Base64 encodes string   | `string`                          | Encoded string  | `encode_base64("user:pass")` → "dXNlcjpwYXNz"    |
| `decode_base64(string)`     | Base64 decodes string   | `string`                          | Decoded string  | `decode_base64("dXNlcjpwYXNz")` → "user:pass"   |
| `hash(string[, algorithm])` | Creates hash of string  | `string`, `algorithm` ("md5", "sha1", "sha256") | Hash string     | `hash("hello", "md5")` → "5d4..."              |

### Utility Functions

| Function                               | Description                       | Parameters                            | Returns                | Example                                                       |
| -------------------------------------- | --------------------------------- | ------------------------------------- | ---------------------- | ------------------------------------------------------------- |
| `uuid()`                               | Generates random UUID             | None                                  | UUID string            | `uuid()` → "..."                                              |
| `log(message)`                         | Logs a message during evaluation  | `message`                             | The message (passthru) | `log("Processing record: " + record.id)`                      |
| `regex_match(string, pattern)`         | Checks if string matches pattern  | `string`, `pattern` (Go regex)        | Boolean                | `regex_match("img_123.jpg", "^img_.*\\.jpg$")` → true        |
| `regex_extract(string, pattern[, idx])`| Extracts matches using regex      | `string`, `pattern`, `idx` (optional) | Matches/group or null  | `regex_extract("id=123", "id=(\\d+)", 1)` → "123"           |
