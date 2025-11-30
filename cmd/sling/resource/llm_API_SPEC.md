# Sling API Specification Guide for LLMs

This guide explains how to create API specifications for Sling. Follow this document along with the target API\'s official documentation to build a properly formatted API spec.

## Overview

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/structure)

Sling API specs are YAML files that define how to interact with REST APIs. They specify authentication, endpoints, request formation, iteration, pagination, response processing, and state management.

## Principal Instructions for LLMs

**Core Guidelines:**
- Use this document to construct Sling API specs for **data extraction only** (GET endpoints). Ignore POST/PATCH/PUT/DELETE endpoints intended for creating/updating/deleting data.
- Skip endpoints marked as `DEPRECATED` unless explicitly instructed by the user.
- Keep specs clean: avoid verbose comments about optional parameters or pagination details. Only add comments where the configuration might confuse a human.

**Research Process:**
1. **Use browser MCP** (or equivalent) to explore the target API's documentation. Understand:
   - Authentication method (Bearer token, OAuth2, Basic Auth, API key in header/query)
   - Pagination style (see Common Pagination Patterns below)
   - Response structure (where records are located, e.g., `data[]`, `items[]`, `results[]`)
2. **Identify incremental sync fields** (e.g., `updated_at`, `modified_since`, `cursor`) for efficient data loading.
3. **Ask for clarification** if documentation is unclear. It's better to confirm than build an unstable spec.

**Common Pagination Patterns:**
- **Cursor-based** (Stripe, Shopify): Use `starting_after`, `cursor`, or `page_token`. Stop when `has_more: false` or empty response.
- **Page number** (older APIs): Increment `page` parameter. Stop when `page >= total_pages` or empty response.
- **Offset-based**: Increment `offset` by `limit`. Stop when `offset >= total_count` or empty response.
- **Link header** (GitHub): Parse `Link` header for `rel="next"` URL. Stop when no next link.

**Incremental Sync Best Practices:**
- Always look for timestamp/cursor fields to enable incremental loading.
- Use `sync: [field_name]` to persist state between runs.
- Initialize with `coalesce(sync.field, default_value)` to handle first run.
- Use `aggregation: "maximum"` for timestamps, `aggregation: "last"` for cursors.


## Basic Structure

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/structure)

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
      url: "{state.base_url}/resource"
      method: "GET"
    
    response:
      records:
        jmespath: "data[]" # JMESPath to extract records
```

## Full Structure

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/structure)

```yaml
# Name of the API
name: "Example API"

# Description of what the API does
description: "This API provides access to example data"

# Queues pass data between endpoints (write-then-read, temporary storage)
queues:
  - user_ids

# Authentication configuration for accessing the API
authentication:
  # Type of authentication: "basic", "oauth2", "aws-sigv4", "sequence", or empty for none
  type: "basic"

  # Basic authentication credentials
  username: "{secrets.username}"
  password: "{secrets.password}"

  # OAuth2 Client Credentials Flow (most common for API integrations)
  type: "oauth2"
  flow: "client_credentials"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  authentication_url: "https://api.example.com/oauth/token"
  scopes: ["read:data", "write:data"]

  expires: 3600  # Re-auth interval in seconds; automatic before each request if expired
  refresh_on_expire: true  # Auto-refresh OAuth2 tokens (requires refresh_token)


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
    stop_condition: "response.json.has_more == false" # Common for cursor/offset

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

    # Documentation URL for this endpoint (optional)
    docs: "https://api.example.com/docs/users"

    # Disable this endpoint (optional)
    disabled: false

    # Declare dependencies on other endpoints (optional)
    # This is automatically inferred from queue usage, but can be explicitly set
    # Ensures execution order: dependencies run first
    depends_on: []

    overrides: {}  # Stream processor config: mode, hooks, object, primary_key, update_key, etc.

    # Initial state variables for this endpoint (merged with defaults.state)
    state:
      # Get last sync timestamp from persistent state, default to 30 days ago
      # Accepts env var override
      updated_since: >
        { coalesce(
             env.LAST_UPDATED,
             sync.last_updated,
             date_format(date_add(now(), -30, 'day'), '%Y-%m-%dT%H:%M:%SZ')
           )
        }
      page: 1 # Example for page-based pagination

    # Variables to persist between API runs for incremental loading
    # Values are read from 'sync.' scope and written to 'state.' scope for the next run.
    sync: ["last_updated"]
    
    # Setup: runs once before main requests (job creation, metadata fetching, polling)
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
            # Extract job ID from response
            - expression: response.json.job_id
              output: state.job_id

      # Call 2: Poll until job completes (with retry logic)
      - request:
          url: "{state.base_url}/export/status/{state.job_id}"
          method: "GET"
        response:
          rules:
            # Retry while job is still processing
            - action: retry
              condition: response.json.status != "COMPLETED"
              max_attempts: 20
              backoff: constant
              backoff_base: 5  # Check every 5 seconds
            # Fail if job errored
            - action: fail
              condition: response.json.status == "FAILED"
              message: "Export job failed: {response.json.error}"
          processors:
            # Extract download URL once job completes
            - expression: response.json.download_url
              output: state.data_url

    # Main request now uses the download URL from setup
    request:
      url: "{state.data_url}"  # URL obtained from setup
      method: "GET"

    # Teardown: runs once after all requests complete (cleanup, logging, notifications)
    teardown:
      - request:
          url: "{state.base_url}/export/cleanup/{state.job_id}"
          method: "DELETE"

    # HTTP request configuration
    request:
      # Path relative to defaults.request.url
      url: '{state.base_url}/users'
      # Method overrides default if needed
      method: "GET"
      # Headers merged with defaults.request.headers
      headers:
        X-Custom-Header: "value"
      # URL query parameters
      parameters:
        page: "{state.page}"
        limit: "{state.limit}"
        updated_since: "{state.updated_since}" # Use the state variable

    # Pagination configuration
    pagination:
      # How to update state for the next page request
      next_state:
        page: "{state.page + 1}"
      # Overrides default stop_condition
      stop_condition: 'response.json.page >= response.json.total_pages'

    # Response processing configuration
    response:
      format: "json"  # Force format: json/csv/xml (overrides Content-Type header, auto-detected if omitted)
      
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
        
        duplicate_tolerance: "1000000,0.001"  # Bloom filter for large datasets: "capacity,error_rate"

      # Post-processing transformations and aggregations
      processors:
        # Example 1: Add a processed field to each record
        - expression: "upper(record.name)"
          output: "record.upper_name" # Target: record.<new_field>

        # Example 2: Aggregate the maximum 'updated_at' timestamp into state for the next sync
        - expression: "record.updated_at"
          output: "state.last_updated" # Target: state.<variable_name>
          aggregation: "maximum"

        # Example 3: Send user IDs to a queue for another endpoint to process, only when IF condition is met
        - expression: "record.id"
          if: record.country == "United States"  # optional if condition
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
      url: '{state.base_url}/users/{state.current_user_id}'
      method: "GET"

    response:
      records:
        jmespath: "user" # Assuming the response structure is {"user": {...}}
        primary_key: ["id"]
```

## Queues

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/queues)

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
      url: '{state.base_url}/products/{state.current_product_id}/inventory'
      # ... other request config ...
    response:
      # ... response config ...
```

### Queue Details

**Lifecycle**: Declare → Write (via processors) → Read (via iterate) → Auto-cleanup. Queues are temporary JSONL files. Producers run before consumers (auto-ordered). Cannot write after reading starts.

**Usage Tips**: Use descriptive names, keep items small (IDs/strings), let Sling determine execution order.

### Direct Queue-to-Records

Set `iterate.into: "response.records"` to process queue items **without HTTP requests**. Useful for transforming, filtering, or validating queue data.

```yaml
queues:
  - user_ids

endpoints:
  fetch_users:  # Producer
    request:
      url: "{state.base_url}/users"
    response:
      processors:
        - expression: "record.id"
          output: "queue.user_ids"

  transform_user_ids:  # Transform without API call
    iterate:
      over: "queue.user_ids"
      into: "response.records"  # No HTTP request
    response:
      processors:
        - expression: "upper(record)"
          output: "record.user_id_upper"
        - expression: "record"
          if: '!is_null(record) && record != ""'  # Filter
          output: "queue.valid_user_ids"

  fetch_user_details:  # Consumer with API call
    iterate:
      over: "queue.valid_user_ids"
      into: "state.user_id"
    request:
      url: "{state.base_url}/users/{state.user_id}"
```

## Authentication

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/authentication)

Supported authentication types:

```yaml
authentication:
  # No Authentication
  type: "" # or omit the authentication block

  # Static Header Authentication (API Key / Bearer Token)
  type: "static"
  headers:
    Authorization: "Bearer {secrets.api_token}"  # or X-API-Key, etc.

  # Basic Auth
  type: "basic"
  username: "{secrets.username}"
  password: "{secrets.password}"

  # OAuth2 - Client Credentials Flow (recommended for server-to-server)
  type: "oauth2"
  flow: "client_credentials"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  authentication_url: "https://api.example.com/oauth/token"
  scopes:
    - "read:data"
    - "write:data"

  # OAuth2 - Authorization Code (automatic browser or manual with pre-obtained code)
  type: "oauth2"
  flow: "authorization_code"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  authentication_url: "https://api.example.com/oauth/token"
  authorization_url: "https://api.example.com/oauth/authorize"  # Auto-derived if omitted
  # redirect_uri: "" # Empty/localhost = auto browser flow, custom URI = manual mode
  token: "{secrets.authorization_code}"  # For manual mode with pre-obtained code
  scopes: ["read:data"]

  # OAuth2 - Resource Owner Password Credentials (deprecated, avoid if possible)
  type: "oauth2"
  flow: "password"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  username: "{secrets.username}"
  password: "{secrets.password}"
  authentication_url: "https://api.example.com/oauth/token"
  scopes:
    - "read:user"

  # OAuth2 - Refresh Token Flow
  type: "oauth2"
  flow: "refresh_token"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  refresh_token: "{secrets.refresh_token}"
  authentication_url: "https://api.example.com/oauth/token"

  # OAuth2 - Device Code Flow (for CLI/headless environments - Google, Microsoft, GitHub)
  type: "oauth2"
  flow: "device_code"
  client_id: "{secrets.oauth_client_id}"
  # client_secret is optional for public clients (e.g., native apps)
  authentication_url: "https://oauth2.googleapis.com/token"
  device_auth_url: "https://oauth2.googleapis.com/device/code"  # Auto-derived if omitted
  scopes:
    - "https://www.googleapis.com/auth/spreadsheets.readonly"
  # User will be prompted to visit a URL and enter a code; token is stored automatically

  # AWS Signature v4 Authentication
  type: "aws-sigv4"
  aws_service: "execute-api"  # Service name (e.g., execute-api, s3, lambda)
  aws_region: "{secrets.aws_region}"
  aws_access_key_id: "{secrets.aws_access_key_id}"
  aws_secret_access_key: "{secrets.aws_secret_access_key}"
  aws_session_token: "{secrets.aws_session_token}"  # Optional for temporary credentials
  aws_profile: "{secrets.aws_profile}"  # Optional, use AWS profile instead of keys

  # HMAC Authentication (e.g., Kraken, Binance, custom APIs)
  type: "hmac"
  algorithm: "sha256"  # sha256 or sha512
  secret: "{secrets.api_secret}"
  signing_string: "{http_method}{http_path}{unix_time}{http_body_sha256}"
  request_headers:
    X-Signature: "{signature}"
    X-Timestamp: "{unix_time}"
    X-API-Key: "{secrets.api_key}"
  nonce_length: 16  # Optional: generates random nonce (in bytes, hex-encoded)

  # Custom Authentication Sequence
  type: "sequence"
  sequence:
    - request:
        url: "https://api.example.com/auth/login"
        method: "POST"
        payload:
          username: "{secrets.username}"
          password: "{secrets.password}"
      response:
        processors:
          - expression: 'response.json.token'
            output: "state.access_token"
    - request:
        url: "https://api.example.com/auth/validate"
        headers:
          Authorization: "Bearer {state.access_token}"
```

### Endpoint-Level Authentication Override

Endpoints can override or disable the connection-level authentication. This is useful when some endpoints don't require auth or use different credentials.

```yaml
authentication:
  type: "basic"
  username: "{secrets.main_user}"
  password: "{secrets.main_pass}"

endpoints:
  # Uses connection-level auth (basic)
  protected_data:
    request:
      url: "{state.base_url}/protected"

  # No auth for this endpoint
  public_data:
    authentication: null  # Explicitly disables auth
    request:
      url: "{state.base_url}/public"

  # Different auth for this endpoint
  admin_data:
    authentication:
      type: "basic"
      username: "{secrets.admin_user}"
      password: "{secrets.admin_pass}"
    request:
      url: "{state.base_url}/admin"
```

### HMAC Authentication

HMAC signs requests using a secret key. Common for crypto exchanges and custom APIs.

```yaml
authentication:
  type: "hmac"
  algorithm: "sha256"  # sha256 or sha512
  secret: "{secrets.api_secret}"
  # Template vars: http_method, http_path, http_query, http_body_*, unix_time*, date_*, nonce
  signing_string: "{http_method}{http_path}{unix_time}{http_body_sha256}"
  request_headers:
    X-Signature: "{signature}"  # Computed HMAC (only available in headers)
    X-Timestamp: "{unix_time}"
    X-API-Key: "{secrets.api_key}"
  nonce_length: 16  # Optional: random hex nonce (bytes)

# Common patterns:
# Kraken: "{http_method}{http_path}{nonce}{http_body_sha256}"
# Simple: "{http_method}{unix_time}{http_body_raw}"
# AWS: "{http_method}\n{http_path}\n{http_query}\n{http_headers}"
```

## Variable Scopes and Expressions

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/advanced)

Sling uses `{...}` syntax for embedding expressions and accessing variables within YAML strings. Expressions are evaluated using the `goval` library with custom functions.

Available variable scopes:
- `env`: Environment variables (e.g., `{env.USER}`).
- `secrets`: Sensitive credentials passed to Sling (e.g., `{secrets.api_key}`).
- `state`: Variables defined in `defaults.state` or `endpoints.<name>.state`. These are local to each endpoint iteration and can be updated by pagination (`next_state`) or processors (`output: state.<var>`).
- `sync`: Persistent state variables read at the start of an endpoint run (values from the previous run's `state` matching the `sync` list). Use `{coalesce(sync.var, state.var, default_value)}`.
- `context`: **Runtime values** passed from replication configuration (read-only). Includes `context.mode` (replication mode), `context.store` (state storage location), `context.limit` (max records), `context.range_start`, `context.range_end` (for backfill ranges from `source_options.range`). See the Context Variables section below for details.
- `auth`: Authentication data after successful authentication (e.g., `{auth.token}` for OAuth2 access tokens, refresh tokens stored here).
- `request`: Information about the current HTTP request being made (available in rule/pagination evaluation). Includes `request.url`, `request.method`, `request.headers`, `request.payload`, `request.attempts` (number of retry attempts).
- `response`: Information about the HTTP response received (available in rule/pagination/processor evaluation). Includes `response.status`, `response.headers`, `response.text`, `response.json` (parsed body), `response.records` (extracted records).
- `record`: The current data record being processed by a processor (available only within `response.processors`).
- `queue`: Access queues declared at the top level (e.g., `iterate.over: queue.my_queue`). Queues must be registered in the top-level `queues` list.
- `null`: Represents a null value (e.g., `coalesce(state.value, null)`).

State variables (`state.`) within an endpoint have a defined render order determined by a topological sort based on dependencies. If `state.b` depends on `state.a` (`state.b: "{state.a + 1}"`), `state.a` will be evaluated first. Circular dependencies are detected and cause an error at validation time. This ensures that all variable references are properly resolved before making any requests.

### State Variable Rendering Order

State variables resolve dependencies automatically via topological sort. Dependencies are evaluated first. Circular dependencies cause errors.

```yaml
state:
  base_url: "https://api.com"              # No dependencies
  users_url: "{state.base_url}/users"      # Depends on base_url
  full_url: "{state.users_url}?limit=100"  # Depends on users_url
# Renders: base_url → users_url → full_url

# ❌ Circular dependency error:
state:
  var_a: "{state.var_b}"  # A → B
  var_b: "{state.var_a}"  # B → A (circular!)
```

**Best Practices**: Keep chains short, use descriptive names, reference external scopes directly (e.g., `{secrets.key}` instead of `{state.key: "{secrets.key}"}`)

## Endpoints

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/structure)

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

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/request)

```yaml
request:
  # Request full URL, can include state variables
  url: "{state.base_url}/users/{state.user_id}?active=true"
  # Method: GET, POST, PUT, PATCH, DELETE, etc.
  method: "POST"
  # Headers: Merged with defaults.request.headers
  headers:
    Content-Type: "application/json"
    Authorization: "Bearer {auth.token}"
    X-Request-ID: "{uuid()}"
  # Parameters: Added as URL query parameters for GET/DELETE,
  # or form-encoded body for POST/PUT/PATCH if Content-Type is application/x-www-form-urlencoded
  parameters:
    page: {state.page}
    limit: 100
    status: "active"
  # Payload: Used as request body for POST/PUT/PATCH.
  # Can be a string, map, or list. Will be JSON-encoded if Content-Type is application/json.
  payload:
    user:
      name: "New User"
      email: "{state.user_email}"
  # Timeout: Request timeout in seconds (default: 30)
  timeout: 60
  # Rate: Max requests per second for this endpoint (default: 2)
  rate: 5
  # Concurrency: Max concurrent requests for this endpoint (default: 5)
  concurrency: 3
```

## Iteration (Looping Requests)

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/advanced)

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
  if: "state.process_details == true"
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
    date: '{date_format(state.current_day, "%Y-%m-%d")}'
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
    ids: '{join(state.variant_id_batch, ",")}'
```

## Pagination

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/advanced)

Controls how Sling navigates through multiple pages of results for *each iteration* (if `iterate` is used) or for the single endpoint execution (if `iterate` is not used).

```yaml
pagination:
  # Map of state variables to update *before* the next request in the current iteration.
  # Expressions are evaluated based on the *current* state and response (if needed).
  next_state:
    # Example 1: Cursor-based (using last record ID)
    starting_after: "{response.records[-1].id}"
    # Example 2: Page number based
    # page: "{state.page + 1}"
    # Example 3: Offset based
    # offset: "{state.offset + state.limit}"
    # Example 4: Using URL from response header (e.g., Link header)
    # url: >
    #  {
    #      if(contains(response.headers.link, "rel=\"next\""),
    #         trim(split_part(split(response.headers.link, ",")[0], ";", 0), "<>"),
    #         null # Or keep current state.url? Needs careful handling.
    #      )
    #   }

  # Expression evaluated *after* a response is received to check if pagination should stop for the current iteration.
  # If true, no more requests are made for this iteration/endpoint.
  stop_condition: jmespath(response.json, "has_more") == false || length(response.records) == 0
  # Example: Page-based stop
  # stop_condition: 'response.json.page >= response.json.total_pages'
  # Example: Stop if next page URL is missing in header
  # stop_condition: "!contains(jmespath(response.headers, \"link\"), \"rel=\\\"next\\\"\")"
```

**Note:** If `next_state` expressions depend on `response.*` variables, Sling waits for the current request to finish before evaluating them. Otherwise, `next_state` is evaluated immediately to prepare the next request potentially in parallel.

## Response Processing

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/response)

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
      expression: date_parse(record.created_ts, "auto")
      # Target output location:
      # - 'record.<new_field>': Adds/updates a field in the current record.
      # - 'record': Replaces the entire record (useful for field selection/renaming)
      # - 'queue.<queue_name>': Appends the result to the named queue.
      # - 'state.<variable>': Updates state variable (requires aggregation if processing multiple records)
      # - 'env.<variable>': Sets an environment variable (requires aggregation, stored as string)
      # - 'context.store.<key>': Stores value in replication store for use in hooks (requires aggregation)
      output: "record.created_at_dt"

    - # Example using aggregation with IF condition
      expression: "record.amount"
      # Optional: Control whether processor is evaluated with IF condition
      # - Evaluated BEFORE the main expression
      # - Must return a boolean value (true/false)
      # - If false, this processor is COMPLETELY SKIPPED for the current record
      # - Has access to full state map: record, state, response, env, secrets
      # - Common use: Filter records, skip nulls, conditional logic
      if: '!is_null(record.amount) && record.amount > 0'
      # Target output for aggregation must be 'state.<variable>'
      output: "state.total_amount"
      # Aggregation type: maximum, minimum, collect, first, last (default: none)
      aggregation: "maximum"

**Overwriting Records**: Set `output: "record"` to replace entire record (field selection, renaming, flattening).

```yaml
processors:
  # Select fields
  - expression: 'object("id", record.id, "name", record.name)'
    output: "record"

  # Rename fields
  - expression: 'object("user_id", record.id, "full_name", record.name)'
    output: "record"

  # Flatten nested
  - expression: 'jmespath(record, "{id: id, name: user.name, country: address.country}")'
    output: "record"
```

**Processor IF Conditions**: Control processor execution with boolean conditions.

```yaml
processors:
  - expression: "record.email"
    if: "!is_null(record.email)"  # Only non-null
    output: "record.email_normalized"

  - expression: "record.id"
    if: 'record.country == "US" && record.status == "active"'  # Multiple checks
    output: "queue.us_customer_ids"

  - expression: "record.id"
    if: 'date_parse(record.created_at, "auto") > date_add(now(), -7, "day")'  # Date filtering
    output: "queue.recent_ids"
```

**Tip**: Use `is_null()`, `is_empty()`, `contains()` for cleaner conditions. Check nulls before accessing nested fields.

**Environment Variables and Store Output**: Use `env.*` and `context.store.*` outputs to pass values to replication hooks.

```yaml
processors:
  # Store max timestamp in environment variable (accessible as {env.LAST_UPDATE} in hooks)
  - expression: "record.updated_at"
    output: "env.LAST_UPDATE"
    aggregation: "maximum"

  # Store first record ID in replication store (accessible as {store.first_id} in hooks)
  - expression: "record.id"
    output: "context.store.first_id"
    aggregation: "first"

  # Store last processed date in store for validation in hooks
  - expression: "record.process_date"
    output: "context.store.last_process_date"
    aggregation: "last"
```

**Note**: Both `env.*` and `context.store.*` require an aggregation type since they operate across all records. Values stored in `context.store.*` are available in replication hooks as `{store.key_name}`, while environment variables are accessible as `{env.VAR_NAME}`.

```yaml
  rules:
    # Define actions based on response conditions (status code, headers, body).
    # Rules are evaluated in order. Execution stops at the first matching rule.
    # Actions: retry, continue, stop, fail, skip, break
    - action: "retry"
      condition: "response.status == 429 || response.status >= 500"
      max_attempts: 5
      backoff: "exponential"  # none, constant, linear, exponential, jitter
      backoff_base: 2         # Base delay in seconds
      message: "Server error or rate limit hit, retrying..."

    - action: "fail"
      condition: "response.status == 401 || response.status == 403"
      message: "Authentication/Authorization failed"

    - action: "continue"
      condition: "response.status == 404"
      message: "Resource not found, continuing..."

    - action: "skip"
      condition: "response.status == 204"
      message: "No content, skipping records from this response"
      # skip: Processes rules but skips extracting records from response

    - action: "break"
      condition: "state.items_processed >= state.max_items"
      message: "Reached max items limit"
      # break: Stops processing for current iteration only (not entire endpoint)

    - action: "stop"
      condition: "response.json.is_complete == true"
      message: "Data extraction complete"
      # stop: Completely stops endpoint execution (all iterations)

    # Default rules (implicitly added, processed last):
    # - action: retry, condition: response.status == 429, max_attempts: 3, backoff: linear
    # - action: fail, condition: response.status >= 400
```

**Deduplication**: When `primary_key` is set, Sling tracks seen keys and skips duplicates.

- **Default**: In-memory map (100% accurate, ~100 bytes/record)
- **Bloom Filter**: Set `duplicate_tolerance` for large datasets (>1M records). Format: `"capacity,error_rate"` (e.g., `"10000000,0.001"` = 10M items, 0.1% false positives, ~15MB memory)

**Rules & Retries**: For `response.status == 429`, Sling auto-detects rate limit headers (`RateLimit-Reset`, `Retry-After`, `RateLimit-Policy`). If found, uses header value; otherwise falls back to configured `backoff` strategy.

```yaml
rules:
  - action: "retry"
    condition: "response.status == 429"
    max_attempts: 3
    backoff: "exponential"  # none, constant, linear, exponential, jitter
    backoff_base: 2  # Fallback if no rate limit headers
```

**Extending Default Rules/Processors**: Instead of completely overriding defaults, use modifier syntax to prepend or append:

- `+rules` - Prepend rules (evaluated BEFORE defaults)
- `rules+` - Append rules (evaluated AFTER defaults, before hardcoded rules)
- `+processors` / `processors+` - Same pattern for processors

```yaml
# Example: Prepend a rule to break on 403 for a specific endpoint
# without losing the default retry/rate-limit handling
endpoints:
  issue_timeline:
    response:
      +rules:
        - action: break
          condition: response.status == 403
          message: "Access denied for this timeline"
      # Default rules from 'defaults.response.rules' are still applied after +rules
```

When combined, the final rule order is:
1. `+rules` (prepend)
2. `rules` (explicit or inherited from defaults)
3. `rules+` (append)
4. Hardcoded retry/fail rules (always last)

**Extending Default Setup/Teardown**: The same modifier syntax works for endpoint `setup` and `teardown` sequences:

- `+setup` - Prepend setup calls (executed BEFORE defaults)
- `setup+` - Append setup calls (executed AFTER defaults)
- `+teardown` / `teardown+` - Same pattern for teardown

```yaml
# Example: Add pre-check before default setup and cleanup after default teardown
defaults:
  setup:
    - request:
        url: "{base_url}/auth/refresh"

endpoints:
  my_endpoint:
    +setup:  # Runs BEFORE default setup
      - request:
          url: "{base_url}/pre-check"
    teardown+:  # Runs AFTER default teardown
      - request:
          url: "{base_url}/cleanup"
    request:
      url: "{base_url}/data"
```

When combined, the final execution order is:
1. `+setup` (prepend)
2. `setup` (explicit or inherited from defaults)
3. `setup+` (append)

Same pattern applies to teardown.

## Sync State for Incremental Loads

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/advanced)

The `sync` key in an endpoint definition allows persisting specific `state` variables between runs, enabling incremental data loading.

```yaml
endpoints:
  incremental_data:
    state:
      # Read the last sync timestamp from persistent state ('sync.last_sync_ts').
      # If it's the first run (sync.last_sync_ts is null), use a default start date.
      start_timestamp: >
        { coalesce(
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
        updated_since: "{state.start_timestamp}"

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
2.  **State Initialization:** The endpoint's `state` is initialized. Expressions like `{coalesce(sync.last_sync_ts, ...)}` read the persisted value or use a default.
3.  **Requests:** Requests are made using the initialized state (e.g., `updated_since: "{state.start_timestamp}"`).
4.  **Processing:** Processors update state variables (e.g., `output: state.last_sync_ts`, `aggregation: maximum`).
5.  **Run End:** The final values of the state variables listed in the `sync` array (e.g., `state.last_sync_ts`) are persisted for the next run.

### Sync Validation

Each `sync` key must have a processor writing to `state.<key>`. Validated at spec load time.

```yaml
endpoints:
  valid:
    sync: ["last_id"]
    response:
      processors:
        - expression: "record.id"
          output: "state.last_id"  # ✅ Matches sync key
          aggregation: "last"

  invalid:
    sync: ["last_id"]
    response:
      processors:
        - expression: "record.id"
          output: "record.last_id"  # ❌ Wrong scope (should be state.last_id)
```

## Context Variables

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/advanced)

Read-only runtime values from replication config. Support backfill and incremental modes.

| Variable | Type | Description | Set From |
|----------|------|-------------|----------|
| `context.mode` | string | Replication mode | Config `mode` field |
| `context.store` | map | Store values from replication | Replication `store` variable |
| `context.limit` | integer | Max records | Config `source_options.limit` |
| `context.range_start` | string | Backfill range start | Config `source_options.range` (first) |
| `context.range_end` | string | Backfill range end | Config `source_options.range` (second) |

**Pattern: Backfill with Incremental Fallback**

```yaml
endpoints:
  events:
    sync: [last_date]
    iterate:
      over: >
        range(
          coalesce(context.range_start, sync.last_date, "2024-01-01"),  # Priority order
          coalesce(context.range_end, date_format(now(), "%Y-%m-%d")),
          "1d"
        )
      into: "state.current_date"
    response:
      processors:
        - expression: "state.current_date"
          output: "state.last_date"
          aggregation: "maximum"

# Replication config for backfill:
# source_options:
#   range: '2024-01-01,2024-01-31'  # Sets context.range_start/end
```

**Tip**: Always use `coalesce()` to provide fallbacks. Context variables are read-only.

## Dynamic Endpoints

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/dynamic-endpoints)

Generate endpoints at runtime from discovery/catalog APIs. Runs after auth, before static endpoints.

```yaml
dynamic_endpoints:
  - setup:  # Optional: fetch list of items
      - request:
          url: "{state.base_url}/catalog"
        response:
          processors:
            - expression: 'jmespath(response.json, "tables")'
              output: "state.table_list"

    iterate: "state.table_list"  # Or JSON literal: '["a","b","c"]'
    into: "state.current_table"  # Must be state.*

    endpoint:  # Template with {state.current_table} access
      name: "{state.current_table.name}"
      description: "Table: {state.current_table.description}"
      state:
        table_id: "{state.current_table.id}"
      request:
        url: "{state.base_url}/data/{state.table_id}"
      response:
        records:
          jmespath: "rows[]"
          primary_key: ["id"]
```

**Notes**: Generated names must be unique. All endpoint fields support templates with access to `state.*`, `env.*`, `secrets.*`.

## Template for New API Specs

📖 [View documentation](https://docs.slingdata.io/concepts/api-specs/structure)

```yaml
name: "API Name"
description: "API Description"

authentication:
  type: "basic"
  username: "{secrets.username}"
  password: "{secrets.password}"

defaults:
  state:
    base_url: "https://api.base.url/v1"
    limit: 100
  request:
    headers:
      Accept: "application/json"
    rate: 10
    concurrency: 5

endpoints:
  items:
    description: "Fetch items"
    state:
      starting_after: '{coalesce(sync.last_item_id, null)}'
    sync: [last_item_id]

    request:
      url: "{state.base_url}/items"
      parameters:
        limit: '{state.limit}'
        starting_after: '{state.starting_after}'

    pagination:
      next_state:
        starting_after: "{response.records[-1].id}"
      stop_condition: 'length(response.records) == 0'

    response:
      records:
        jmespath: "data[]"
        primary_key: ["id"]
      processors:
        - expression: "record.id"
          output: "state.last_item_id"
          aggregation: "last"
```

---

## Expression Functions

📖 [View documentation](https://docs.slingdata.io/concepts/functions)

You can use the following functions within `{...}` expressions in your API spec. Functions provide capabilities for data manipulation, type casting, date operations, control flow, and more.

**IMPORTANT:** Always use double quotes (`"`) for string literals in expressions, never single quotes (`'`). This is required by the [goval](https://github.com/maja42/goval) expression library that Sling uses.

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
    updated_since: "{date_format(date_add(now(), -1, 'hour'), '%Y-%m-%dT%H:%M:%SZ')}"
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
| `array(val1, val2, ...)`       | Creates array from values       | Multiple values                           | Array             | `array(1, 2, 3)` → `[1, 2, 3]`                                 |
| `object(k1, v1, k2, v2, ...)`  | Creates object from key/value pairs | Even number of arguments (key, value pairs) | Map/object        | `object("name", "John", "age", 30)` → `{"name": "John", "age": 30}` |
| `keys(map)`                    | Gets keys from map              | `map` object                              | Array of keys     | `keys({"a": 1, "b": 2})` → `["a", "b"]`                       |
| `values(map)`                  | Gets values from map            | `map` object                              | Array of values   | `values({"a": 1, "b": 2})` → `[1, 2]`                         |
| `exists(collection, item)`     | Checks if key exists in map or value exists in array | `collection`: Map or array, `item`: Key or value to find | Boolean           | `exists({"a": 1}, "a")` → true, `exists([1, 2], 2)` → true   |
| `jmespath(object, expression)` | Evaluates JMESPath expression   | `object`, `expression` (string)           | Query result      | `jmespath(response.json, "data.items[?age > 30]")`            |
| `get_path(object, path)`       | Gets value using dot notation   | `object`, `path` (string, e.g., "a.b[0]") | Value at path     | `get_path(response.json, "user.profile.email")`              |
| `object_rename(map, old, new, ...)` | Renames keys in map       | `map`: Object, followed by pairs of `old_key`, `new_key` | Modified map      | `object_rename({"a": 1}, "a", "x")` → `{"x": 1}` |
| `object_delete(map, key1, ...)`  | Deletes keys from map        | `map`: Object, followed by keys to delete | Modified map      | `object_delete({"a": 1, "b": 2}, "a")` → `{"b": 2}` |
| `object_casing(map, casing)`  | Transforms keys to specified casing | `map`: Object, `casing`: "snake", "camel", "upper", "lower" | Modified map      | `object_casing({"firstName": "John"}, "snake")` → `{"first_name": "John"}` |
| `object_merge(map1, map2, ...)` | Merges multiple maps together | Two or more maps (later maps override earlier) | Merged map      | `object_merge({"a": 1}, {"b": 2})` → `{"a": 1, "b": 2}` |
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
