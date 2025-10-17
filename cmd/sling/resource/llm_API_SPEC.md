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
      url: "{state.base_url}/resource"
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
# Queues are file-backed temporary storage that operate in write-then-read mode
# Once a queue transitions to reading mode, no more writes are allowed
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

  # Expiration time in seconds (for automatic re-authentication)
  # When set, Sling automatically re-authenticates before each request if auth has expired
  # Example: expires: 3600 means authentication is valid for 1 hour
  #
  # How it works:
  # 1. After successful authentication, ExpiresAt timestamp is calculated (now + expires seconds)
  # 2. Before EVERY request, Sling checks if current time >= ExpiresAt
  # 3. If expired (or not authenticated), automatic re-authentication is triggered
  # 4. Re-authentication is thread-safe via mutex - concurrent requests wait for a single re-auth
  # 5. All endpoints share the same authentication state
  #
  # Use cases:
  # - OAuth2 access tokens that expire after a set time
  # - Session-based APIs with time limits
  # - Any auth mechanism with known expiration
  expires: 3600


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

    # Stream-level overrides for replication configuration (optional)
    # Used in special circumstances to further configure the corresponding 
    # replication stream. Can set keys such as `mode`, `hooks`, etc.
    overrides: {}

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
    
    # Setup sequence - calls executed BEFORE the main endpoint requests (optional)
    # Perfect for:
    # - Async job creation patterns (create job, poll for completion, get download URL)
    # - Authentication tokens that need special handling
    # - Fetching metadata needed for main requests (e.g., workspace IDs, API versions)
    # - Rate limit pre-checking
    #
    # Execution:
    # - Runs ONCE before any pagination or iteration
    # - State changes are preserved for main request
    # - Shares same endpoint context and state
    # - Can use same Call structure as sequence authentication
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

    # Teardown sequence - calls executed AFTER the main endpoint completes (optional)
    # Perfect for:
    # - Cleanup of temporary resources (delete jobs, close sessions)
    # - Sending completion notifications
    # - Logging or analytics
    # - Releasing locks or quotas
    #
    # Execution:
    # - Runs ONCE after all pagination and iteration complete
    # - Runs even if main request fails (for cleanup)
    # - Has access to final endpoint state
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
      # Optional: Force response format interpretation
      # Overrides the Content-Type header from the API response
      # Supported formats: "json", "csv", "xml"
      #
      # Use cases:
      # - API returns incorrect Content-Type header
      # - API returns data in a format different from what it advertises
      # - You want to parse text/plain as JSON or CSV
      #
      # Format behaviors:
      # - "json": Parses response as JSON, applies JMESPath, extracts records
      # - "csv": Parses as CSV, converts to JSON records (first row = headers)
      # - "xml": Parses as XML, converts to JSON structure
      #
      # If not specified, Sling auto-detects from Content-Type header:
      # - application/json → JSON
      # - text/csv → CSV
      # - application/xml → XML
      format: "json"
      
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

### Queue Implementation Details

Queues are backed by **temporary files** on disk and operate in a **write-then-read** manner within a single Sling run:

**Lifecycle Phases**:

1. **Declaration Phase**: Queue must be declared in top-level `queues` array before use
   ```yaml
   queues:
     - product_ids
     - order_ids
   ```

2. **Writing Phase**: Endpoints produce data to queues via processors
   - Each `output: queue.<name>` writes one item to the queue file
   - Items are JSON-encoded and written line-by-line
   - Multiple endpoints can write to the same queue concurrently (thread-safe)
   - Queue remains in writing mode until a consumer reads it

3. **Transition Phase**: Triggered when an endpoint starts iterating over a queue
   - `Reset()` is called to transition from writing to reading mode
   - File pointer moves to the beginning
   - **No more writes allowed after this point**
   - Attempting to write after transition results in error

4. **Reading Phase**: Consumer endpoint reads items from queue
   - `Next()` reads one line at a time from the file
   - Each line is JSON-decoded back to original value
   - Multiple consumers can read from the same queue (but rare)
   - Continues until end of file

5. **Cleanup Phase**: When connection closes
   - All queue temporary files are closed
   - Files are deleted from disk
   - Memory is freed

**File Location**:
- Temporary directory: System temp dir (e.g., `/tmp` on Linux, `%TEMP%` on Windows)
- File name format: `sling_queue_<queue_name>_<random>.jsonl`
- Automatically created when first write occurs
- Automatically removed on cleanup

**Execution Order**:
- Sling uses **topological sorting** to determine execution order
- Producer endpoints (write to queue) run before consumer endpoints (read from queue)
- Dependencies are automatically inferred from queue usage
- Can be explicitly set with `depends_on` if needed

**Thread Safety**:
- Queue writes are protected by mutex
- Multiple endpoints can write concurrently
- Reads are sequential (one item at a time)
- Transition is protected to ensure no concurrent writes during switch

**Error Handling**:
```yaml
# This will ERROR: Cannot write to queue after reading starts
endpoints:
  producer:
    # Runs first, writes to queue
    response:
      processors:
        - output: "queue.user_ids"

  consumer:
    # Runs second, reads from queue
    iterate:
      over: "queue.user_ids"

  late_producer:
    # Would run third, tries to write - ERROR!
    depends_on: ["consumer"]  # This creates the problematic ordering
    response:
      processors:
        - output: "queue.user_ids"  # ERROR: Queue already in reading mode
```

**Best Practices**:
- Declare all queues upfront in `queues` array
- Use descriptive queue names (e.g., `product_ids`, not `q1`)
- Keep queue items simple (IDs, strings, small objects)
- Don't use queues for large objects (use direct API calls instead)
- Let Sling determine execution order (don't override unnecessarily)

**Performance Considerations**:
- Queue files use JSON Lines format (efficient line-by-line read/write)
- No memory limit - queues can hold millions of items
- Disk I/O is the bottleneck (but typically fast enough)
- Good for passing IDs/small values between endpoints
- Not ideal for large payloads (consider direct pass-through instead)

### Special Queue Syntax: Direct Queue-to-Records

You can pipe a queue directly into an endpoint's records **without making any HTTP requests**. This is useful for transforming or processing queue data without calling an API.

**Key Behavior**:
- When `iterate.into` is set to `"response.records"`, **NO HTTP request is made**
- Queue items are read and become response records directly
- Processors can still transform the records
- Useful for data transformation, filtering, or aggregation

**Example: Transform Queue Data**

```yaml
queues:
  - user_ids

endpoints:
  # Endpoint 1: Produces user IDs to queue
  fetch_users:
    request:
      url: "{state.base_url}/users"
    response:
      records:
        jmespath: "users[]"
      processors:
        - expression: "record.id"
          output: "queue.user_ids"

  # Endpoint 2: Transforms queue data WITHOUT API calls
  transform_user_ids:
    description: "Transform queue data without making API calls"
    iterate:
      over: "queue.user_ids"
      into: "response.records"  # Special syntax: NO HTTP request!

    # NO request block needed - it would be ignored anyway

    response:
      processors:
        # record is the queue item itself
        - expression: "upper(record)"
          output: "record.user_id_upper"
        - expression: "{record}_transformed"
          output: "record.transformed_id"

  # Endpoint 3: Uses transformed queue in API call
  fetch_user_details:
    iterate:
      over: "queue.user_ids"  # Original queue, not transformed
      into: "state.user_id"

    request:
      url: "{state.base_url}/users/{state.user_id}"  # Makes HTTP request
```

**When to Use**:
- **Data transformation**: Convert queue items before using in API calls
- **Filtering**: Remove unwanted items from queue
- **Aggregation**: Combine multiple queue items into one
- **Validation**: Check queue items before processing
- **Debugging**: Inspect queue contents

**Example: Filter Queue Items**

```yaml
filter_valid_ids:
  description: "Only keep valid user IDs (non-null, non-empty)"
  iterate:
    over: "queue.raw_user_ids"
    into: "response.records"

  response:
    processors:
      # Only write valid IDs to new queue
      - expression: "record"
        if: "!is_null(record) && record != ''"
        output: "queue.valid_user_ids"
```

**Important Notes**:
- The `request` block is **ignored** when using `into: "response.records"`
- `iterate.over` must reference a queue (e.g., `queue.my_queue`)
- Each queue item becomes one record
- Processors run on each record as usual
- Can write to other queues in processors

## Authentication

Supported authentication types:

```yaml
authentication:
  # No Authentication
  type: "" # or omit the authentication block

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

  # OAuth2 - Authorization Code Flow (automatic browser mode - recommended for CLI)
  type: "oauth2"
  flow: "authorization_code"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  authentication_url: "https://api.example.com/oauth/token"        # Token exchange endpoint
  authorization_url: "https://api.example.com/oauth/authorize"     # Optional: Authorization endpoint (auto-derived if not provided)
  # redirect_uri: "" # Leave empty or use localhost for automatic browser flow
  scopes:
    - "read:data"
  # Automatic browser flow:
  # 1. Starts local HTTP server on random available port
  # 2. Automatically opens user's default browser to authorization URL
  # 3. User authorizes in browser
  # 4. Browser redirects to localhost callback with authorization code
  # 5. Local server receives code and exchanges it for access token
  # 6. Displays success/error page to user (auto-closes after 3 seconds)
  # 7. Timeout: 5 minutes for user to complete authorization
  # 8. Refresh token (if provided by API) is stored for future use

  # OAuth2 - Authorization Code Flow (manual mode for web apps or pre-obtained code)
  type: "oauth2"
  flow: "authorization_code"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  authentication_url: "https://api.example.com/oauth/token"
  redirect_uri: "https://myapp.example.com/callback"
  token: "{secrets.authorization_code}" # Pre-obtained authorization code
  # Manual mode: Assumes you have already obtained an authorization code through
  # your own OAuth flow. Sling will exchange this code for an access token.

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
  client_id: "{secrets.oauth_client_id}" # Optional for some providers
  client_secret: "{secrets.oauth_client_secret}" # Optional for some providers
  refresh_token: "{secrets.refresh_token}"
  authentication_url: "https://api.example.com/oauth/token"
  
  # AWS Signature v4 Authentication
  type: "aws-sigv4"
  aws_service: "execute-api"  # Service name (e.g., execute-api, s3, lambda)
  aws_region: "{secrets.aws_region}"
  aws_access_key_id: "{secrets.aws_access_key_id}"
  aws_secret_access_key: "{secrets.aws_secret_access_key}"
  aws_session_token: "{secrets.aws_session_token}"  # Optional for temporary credentials
  aws_profile: "{secrets.aws_profile}"  # Optional, use AWS profile instead of keys
  
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

### OAuth2 Flow Details

**Client Credentials Flow**: Best for server-to-server authentication where the application authenticates itself.

**Authorization Code Flow**: 
- **Interactive Mode**: When `redirect_uri` is empty or points to localhost, Sling automatically starts a local server and opens your browser for authorization.
- **Manual Mode**: When you have a specific redirect URI, provide the authorization code in the `token` field.

**Password Flow**: For trusted applications with user credentials (deprecated by OAuth2 spec).

**Refresh Token Flow**: For refreshing expired access tokens using a previously obtained refresh token.

## Variable Scopes and Expressions

Sling uses `{...}` syntax for embedding expressions and accessing variables within YAML strings. Expressions are evaluated using the `goval` library with custom functions.

Available variable scopes:
- `env`: Environment variables (e.g., `{env.USER}`).
- `secrets`: Sensitive credentials passed to Sling (e.g., `{secrets.api_key}`).
- `state`: Variables defined in `defaults.state` or `endpoints.<name>.state`. These are local to each endpoint iteration and can be updated by pagination (`next_state`) or processors (`output: state.<var>`).
- `sync`: Persistent state variables read at the start of an endpoint run (values from the previous run's `state` matching the `sync` list). Use `{coalesce(sync.var, state.var, default_value)}`.
- `auth`: Authentication data after successful authentication (e.g., `{auth.token}` for OAuth2 access tokens, refresh tokens stored here).
- `request`: Information about the current HTTP request being made (available in rule/pagination evaluation). Includes `request.url`, `request.method`, `request.headers`, `request.payload`, `request.attempts` (number of retry attempts).
- `response`: Information about the HTTP response received (available in rule/pagination/processor evaluation). Includes `response.status`, `response.headers`, `response.text`, `response.json` (parsed body), `response.records` (extracted records).
- `record`: The current data record being processed by a processor (available only within `response.processors`).
- `queue`: Access queues declared at the top level (e.g., `iterate.over: queue.my_queue`). Queues must be registered in the top-level `queues` list.
- `null`: Represents a null value (e.g., `coalesce(state.value, null)`).

State variables (`state.`) within an endpoint have a defined render order determined by a topological sort based on dependencies. If `state.b` depends on `state.a` (`state.b: "{state.a + 1}"`), `state.a` will be evaluated first. Circular dependencies are detected and cause an error at validation time. This ensures that all variable references are properly resolved before making any requests.

### State Variable Rendering Order

Sling uses **topological sorting** to determine the correct order to evaluate state variables based on their dependencies. This ensures that variables are always rendered with their dependencies already resolved.

**How It Works**:

1. **Dependency Detection**: Sling scans all state variable expressions for references to other state variables
2. **Topological Sort**: Variables are ordered so dependencies come before dependents
3. **Evaluation**: Variables are rendered in the determined order
4. **Circular Detection**: Circular dependencies are caught and reported as errors

**Example: Simple Dependencies**

```yaml
state:
  # No dependencies - rendered first
  base_url: "https://api.example.com"

  # Depends on base_url - rendered second
  users_url: "{state.base_url}/users"

  # Depends on users_url - rendered third
  full_url: "{state.users_url}?limit=100"

  # No dependencies - rendered first (order with base_url doesn't matter)
  api_key: "{secrets.api_key}"
```

**Rendering Order**: `base_url`, `api_key` → `users_url` → `full_url`

**Example: Complex Dependencies**

```yaml
state:
  # Level 0: No dependencies
  tenant_id: "{env.TENANT_ID}"
  region: "us-west-2"

  # Level 1: Depends on Level 0
  base_url: "https://{state.tenant_id}.api.{state.region}.example.com"
  workspace_id: "{sync.workspace_id}"

  # Level 2: Depends on Level 1
  api_endpoint: "{state.base_url}/workspaces/{state.workspace_id}"

  # Level 3: Depends on Level 2
  full_request_url: "{state.api_endpoint}/data?format=json"
```

**Rendering Order**:
1. `tenant_id`, `region` (no dependencies)
2. `base_url`, `workspace_id` (depend on Level 0)
3. `api_endpoint` (depends on Level 1)
4. `full_request_url` (depends on Level 2)

**Circular Dependency Detection**:

```yaml
# ❌ This will ERROR: Circular dependency detected
state:
  var_a: "{state.var_b + 1}"  # Depends on var_b
  var_b: "{state.var_a + 1}"  # Depends on var_a (circular!)

# Error message:
# "circular dependency detected for state variable: var_a"
```

**Complex Circular Example**:

```yaml
# ❌ This will also ERROR: Indirect circular dependency
state:
  var_a: "{state.var_b}"     # A depends on B
  var_b: "{state.var_c}"     # B depends on C
  var_c: "{state.var_a}"     # C depends on A (circular!)

# Error: Chain A → B → C → A creates a cycle
```

**Best Practices**:

1. **Keep Dependencies Simple**: Avoid long dependency chains
   ```yaml
   # ✅ GOOD: Short chain
   state:
     base: "https://api.com"
     url: "{state.base}/users"

   # ⚠️ AVOID: Long chain (harder to debug)
   state:
     a: "value"
     b: "{state.a}"
     c: "{state.b}"
     d: "{state.c}"
     e: "{state.d}"
   ```

2. **Use Descriptive Names**: Makes dependency chains easier to understand
   ```yaml
   # ✅ GOOD: Clear dependency flow
   state:
     tenant_id: "{env.TENANT}"
     tenant_url: "https://{state.tenant_id}.api.com"
     workspace_endpoint: "{state.tenant_url}/workspace"

   # ❌ BAD: Unclear dependencies
   state:
     var1: "{env.TENANT}"
     var2: "https://{state.var1}.api.com"
     var3: "{state.var2}/workspace"
   ```

3. **Avoid Unnecessary Dependencies**: Reference external scopes directly when possible
   ```yaml
   # ✅ GOOD: Direct reference
   state:
     api_key_header: "Bearer {secrets.api_key}"

   # ⚠️ UNNECESSARY: Extra indirection
   state:
     api_key: "{secrets.api_key}"
     api_key_header: "Bearer {state.api_key}"
   ```

4. **Test Dependency Order**: If unsure, add debug logging to see evaluation order
   ```yaml
   state:
     var_a: "{log('Rendering var_a') || 'value_a'}"
     var_b: "{log('Rendering var_b using var_a=' + state.var_a) || state.var_a}"
   ```

**Edge Cases**:

```yaml
# ✅ VALID: Variable references itself in transformation
state:
  counter: "{ coalesce(state.counter, 0) + 1 }"
  # This is allowed because it's not a circular dependency in the evaluation phase

# ✅ VALID: Optional dependency (won't cause circular error)
state:
  workspace: "{ coalesce(state.workspace, env.DEFAULT_WORKSPACE) }"

# ✅ VALID: Conditional dependency
state:
  url: "{ if(env.USE_CUSTOM, state.custom_url, state.default_url) }"
  default_url: "https://api.com"
  custom_url: "https://custom.api.com"
```

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
      # Aggregation type: maximum, minimum, flatten, first, last (default: none)
      aggregation: "maximum"

**Overwriting Records with `output: "record"`**:

You can completely replace a record by setting `output: "record"`. This is useful for:
- Selecting a subset of fields
- Renaming fields
- Reshaping the record structure
- Flattening nested data

```yaml
# Example 1: Select specific fields using object()
processors:
  - expression: 'object("id", record.id, "name", record.name, "email", record.email)'
    output: "record"
# Before: {"id": 123, "name": "John", "email": "john@example.com", "internal_id": "abc", "metadata": {...}}
# After:  {"id": 123, "name": "John", "email": "john@example.com"}

# Example 2: Rename fields
processors:
  - expression: 'object("user_id", record.id, "full_name", record.name, "contact_email", record.email)'
    output: "record"
# Before: {"id": 123, "name": "John Doe", "email": "john@example.com"}
# After:  {"user_id": 123, "full_name": "John Doe", "contact_email": "john@example.com"}

# Example 3: Flatten nested data using jmespath()
processors:
  - expression: 'jmespath(record, "{id: id, name: user.profile.name, country: user.address.country}")'
    output: "record"
# Before: {"id": 123, "user": {"profile": {"name": "John"}, "address": {"country": "USA"}}}
# After:  {"id": 123, "name": "John", "country": "USA"}

# Example 4: Create array fields
processors:
  - expression: 'object("id", record.id, "tags", array(record.tag1, record.tag2, record.tag3))'
    output: "record"
# Before: {"id": 123, "tag1": "urgent", "tag2": "customer", "tag3": "priority"}
# After:  {"id": 123, "tags": ["urgent", "customer", "priority"]}
```

**Important Notes**:
- When using `output: "record"`, the ENTIRE record is replaced
- All previous fields are discarded unless explicitly included in the expression
- Use `object()` to build records field-by-field
- Use `jmespath()` for complex transformations with projection syntax
- Use `array()` to create array fields from multiple values

**Processor IF Condition Examples**:

```yaml
response:
  processors:
    # Example 1: Only process non-null values
    - expression: "record.email"
      if: "!is_null(record.email)"
      output: "record.email_normalized"

    # Example 2: Only process US customers
    - expression: "record.id"
      if: "record.country == 'US'"
      output: "queue.us_customer_ids"

    # Example 3: Only process if amount is above threshold
    - expression: "record.amount * 1.1"  # Add 10% markup
      if: "record.amount > 100"
      output: "record.amount_with_markup"

    # Example 4: Complex condition with multiple checks
    - expression: "record.user_id"
      if: "record.status == 'active' && record.verified == true && record.balance > 0"
      output: "queue.eligible_user_ids"

    # Example 5: Check if field exists and is not empty
    - expression: "upper(record.name)"
      if: "!is_null(record.name) && record.name != ''"
      output: "record.name_upper"

    # Example 6: Date-based filtering
    - expression: "record.id"
      if: "date_parse(record.created_at, 'auto') > date_add(now(), -7, 'day')"
      output: "queue.recent_ids"

    # Example 7: Type checking
    - expression: "cast(record.value, 'int')"
      if: "is_null(try_cast(record.value, 'int')) == false"
      output: "record.value_int"
```

**IF Condition Best Practices**:
- Always return a boolean (use comparison operators: `==`, `!=`, `>`, `<`, `>=`, `<=`)
- Use `&&` for AND, `||` for OR
- Check for null before accessing nested fields: `!is_null(record.user) && !is_null(record.user.email)`
- Use functions like `is_null()`, `is_empty()`, `contains()` for cleaner conditions
- Keep conditions simple and readable
- Test with actual data to ensure conditions work as expected

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

**Deduplication:** If `primary_key` is defined, Sling automatically tracks seen keys within the current run and skips duplicate records:

**Default Behavior (In-Memory Map)**:
- Uses a Go map to store all unique primary key values
- Guarantees 100% deduplication accuracy (no false positives)
- Memory usage: ~100 bytes per unique key on average
- Best for: Small to medium datasets (< 1 million records)
- Example memory: 1M records ≈ 100MB RAM

**Bloom Filter Mode (Low Memory)**:
- Enabled when `duplicate_tolerance` is specified
- Format: `"estimated_items,false_positive_probability"`
- Uses probabilistic data structure for space efficiency
- Memory usage: ~1.5 bytes per item for 0.1% false positive rate
- Trade-off: Small chance of false positives (marking unique records as duplicates)

**When to Use Bloom Filter**:
- Expecting millions of records (> 1 million)
- Memory is constrained
- Can tolerate occasional false positives
- Deduplication is more important than 100% accuracy

**Examples**:

```yaml
# Default: Accurate but memory-intensive (no duplicate_tolerance)
response:
  records:
    jmespath: "data[]"
    primary_key: ["id"]
    # Uses in-memory map, 100% accurate

# Bloom filter: Memory-efficient for large datasets
response:
  records:
    jmespath: "data[]"
    primary_key: ["id"]
    duplicate_tolerance: "10000000,0.001"  # 10M items, 0.1% false positive rate
    # Memory: ~15MB instead of ~1GB
```

**Bloom Filter Parameters**:
- `"1000000,0.001"`: 1M items, 0.1% false positives, ~1.5MB memory
- `"10000000,0.001"`: 10M items, 0.1% false positives, ~15MB memory
- `"100000000,0.001"`: 100M items, 0.1% false positives, ~150MB memory
- Lower false positive rate = more memory (0.01% ≈ 2x memory vs 0.1%)

**How It Works**:
1. Primary key fields are extracted from each record
2. Key values are concatenated with separator `||~~||`
3. For in-memory map: Direct lookup in hash map
4. For Bloom filter: Hash the key and check/set bits
5. Duplicate records are silently skipped (not streamed to destination)

**Rules & Retries:** Sling automatically handles rate limiting with intelligent header parsing:

When `response.status == 429` and the action is `retry`, Sling checks for standard rate limit headers in this priority order:

1. **`RateLimit-Reset`**: Seconds to wait before retrying (most direct)
   - Format: `RateLimit-Reset: 45` means wait 45 seconds
   - This is the preferred header as it's most explicit

2. **`Retry-After`**: Standard HTTP header for retry delays
   - Format: `Retry-After: 30` (seconds) or `Retry-After: Wed, 21 Oct 2025 07:28:00 GMT` (HTTP date)
   - Widely supported across APIs

3. **`RateLimit-Policy`** with **`RateLimit-Remaining`**: IETF draft format for complex rate limiting
   - Policy format: `"hour";q=1000;w=3600, "day";q=5000;w=86400`
     - `q=1000`: Quota of 1000 requests
     - `w=3600`: Window of 3600 seconds (1 hour)
   - Remaining format: `RateLimit-Remaining: 50` (50 requests left)
   - **Calculation logic**:
     - If `remaining == 0`: Wait for full window (`w` seconds)
     - If `remaining > 0`: Calculate proportional wait time:
       - `wait_time = window * (1 - (remaining / quota))`
       - Example: `q=100, w=60, remaining=10` → `wait_time = 60 * (1 - 10/100) = 54 seconds`
   - This provides intelligent backoff based on actual quota consumption

4. **Alternate header names** (case-insensitive):
   - `Rate-Limit-Reset`, `ratelimit-reset` (variations of RateLimit-Reset)
   - Checked if standard headers not found

**Header Priority and Fallback**:
- If **any** rate limit header is found and valid, it **overrides** the configured `backoff` strategy
- If **no** rate limit headers are found or they're invalid:
  - Falls back to configured `backoff` strategy (none, constant, linear, exponential, jitter)
  - Uses `backoff_base` and attempt number to calculate delay
- This ensures optimal retry timing whether the API provides hints or not

**Examples**:

```yaml
rules:
  # Will use RateLimit-Reset header if provided by API (status 429)
  # Otherwise uses exponential backoff: 2s, 4s, 8s
  - action: "retry"
    condition: "response.status == 429"
    max_attempts: 3
    backoff: "exponential"
    backoff_base: 2

  # Custom retry for specific error with linear backoff
  - action: "retry"
    condition: "response.status == 503"
    max_attempts: 5
    backoff: "linear"     # 3s, 6s, 9s, 12s, 15s
    backoff_base: 3
```

## Sync State for Incremental Loads

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

Sling performs **automatic validation** at spec load time to ensure sync configuration is correct. This catches common errors early before any API calls are made.

**What is Validated**:
- Every key in the `sync` array must have a corresponding processor that writes to `state.<key>`
- Prevents silent failures where sync values are never updated
- Ensures incremental loading works as intended

**Validation Rules**:

```yaml
endpoints:
  valid_example:
    sync: ["last_id", "max_timestamp"]
    response:
      processors:
        # ✅ VALID: Writes to state.last_id
        - expression: "record.id"
          output: "state.last_id"
          aggregation: "last"
        # ✅ VALID: Writes to state.max_timestamp
        - expression: "record.updated_at"
          output: "state.max_timestamp"
          aggregation: "maximum"

  invalid_example:
    sync: ["last_id", "max_timestamp"]
    response:
      processors:
        # ✅ Has processor for last_id
        - expression: "record.id"
          output: "state.last_id"
          aggregation: "last"
        # ❌ MISSING processor for max_timestamp!
        # This will fail validation with error:
        # "endpoint 'invalid_example' declares sync key(s) [max_timestamp]
        #  but has no processor writing to state.max_timestamp"
```

**Error Messages**:

When validation fails, you'll see a clear error like:
```
sync validation failed:
  - endpoint 'users' declares sync key(s) [last_user_id, last_sync_time] but has no processor writing to state.last_user_id, state.last_sync_time
  - endpoint 'orders' declares sync key(s) [max_order_date] but has no processor writing to state.max_order_date
```

**Common Mistakes to Avoid**:

```yaml
# ❌ WRONG: Writing to sync.* instead of state.*
sync: ["last_id"]
response:
  processors:
    - expression: "record.id"
      output: "sync.last_id"  # ❌ Should be state.last_id
      aggregation: "last"

# ❌ WRONG: Writing to record.* instead of state.*
sync: ["last_timestamp"]
response:
  processors:
    - expression: "record.created_at"
      output: "record.last_timestamp"  # ❌ Should be state.last_timestamp
      aggregation: "maximum"

# ❌ WRONG: Typo in sync key name
sync: ["last_updated_at"]
response:
  processors:
    - expression: "record.updated_at"
      output: "state.last_update_at"  # ❌ Mismatch: last_updated_at vs last_update_at
      aggregation: "maximum"

# ✅ CORRECT: Match sync key with state.* output
sync: ["last_updated_at"]
response:
  processors:
    - expression: "record.updated_at"
      output: "state.last_updated_at"  # ✅ Matches sync key
      aggregation: "maximum"
```

**Best Practices**:
- Always use aggregation with sync values (`maximum`, `minimum`, `last`, etc.)
- Use descriptive sync key names (`last_sync_timestamp`, not `ts`)
- Test incremental loading by running endpoint multiple times
- Check that sync values are actually being persisted between runs
- Use `coalesce(sync.key, default_value)` to handle first run

## Dynamic Endpoints

Dynamic endpoints allow you to generate API endpoints at runtime based on discovery or catalog endpoints. This is a fully implemented feature useful when:
- The API provides a catalog/discovery endpoint that lists available data sources
- Endpoints are tenant-specific or user-specific
- The list of available endpoints changes frequently

**How It Works**:
1. Authentication occurs first (if specified)
2. Setup sequence runs to discover/fetch the list of items
3. The `iterate` expression is evaluated to get an array of items
4. For each item, a new endpoint is generated from the template
5. All generated endpoints are validated, added to the spec, and executed

**Structure**:

```yaml
name: "Dynamic API"
description: "API with dynamically generated endpoints"

defaults:
  request:
    headers:
      Authorization: Bearer {secrets.api_key}
  state:
    base_url: "https://api.example.com/v1"

# Define dynamic endpoint definitions
# Each definition can generate multiple endpoints
dynamic_endpoints:
  - # Optional: Setup sequence to fetch/discover available items
    setup:
      - request:
          url: "{state.base_url}/api/catalog"
          method: "GET"
        response:
          processors:
            # Extract list of tables/endpoints from response
            - expression: 'jmespath(response.json, "tables")'
              output: "state.table_list"

    # Expression to iterate over (can be state var, JSON literal, or JMESPath)
    # Examples:
    # - "state.table_list" - state variable from setup
    # - '["users", "orders", "products"]' - JSON literal array
    # - '["table1", "table2"]' - inline JSON array
    iterate: "state.table_list"

    # Variable to store current iteration value (must be state.*)
    into: "state.current_table"

    # Template for generated endpoints
    # All fields support template rendering with {state.current_table}
    endpoint:
      name: "{state.current_table.name}"
      description: "Table: {state.current_table.description}"

      state:
        table_id: "{state.current_table.id}"
        table_path: "{state.current_table.path}"

      request:
        url: "{state.base_url}/data/{state.table_path}"
        method: "GET"
        parameters:
          limit: 1000

      response:
        records:
          jmespath: "data.rows[]"
          primary_key: ["id"]
```

**Key Features**:

1. **Multiple Dynamic Definitions**: You can have multiple `dynamic_endpoints` entries, each generating its own set of endpoints
2. **Template Rendering**: All endpoint fields are rendered as templates with access to:
   - `state.current_table` (or whatever variable name you specify in `into`)
   - All other state variables from setup
   - Environment variables via `env.*`
   - Secrets via `secrets.*`
3. **Iterate Options**:
   - **State variable**: `"state.my_list"` - from setup processors
   - **JSON literal**: `'["a", "b", "c"]'` - inline array
   - **JMESPath**: `"state.data.items[*].name"` - extract from state
4. **Automatic Ordering**: Generated endpoints are automatically ordered based on dependencies
5. **Validation**: Each generated endpoint is validated and merged with defaults

**Example: Iterate Over JSON Literal**

```yaml
dynamic_endpoints:
  - # No setup needed - iterate directly over inline list
    iterate: '["customers", "orders", "products", "invoices"]'
    into: "state.table_name"

    endpoint:
      name: "{state.table_name}"
      description: "Sync {state.table_name} table"

      request:
        url: "{state.base_url}/tables/{state.table_name}/export"
        method: "GET"

      response:
        records:
          jmespath: "data[]"
          primary_key: ["id"]
```

**Example: Complex Discovery**

```yaml
dynamic_endpoints:
  - setup:
      # First call: Get list of workspaces
      - request:
          url: "{state.base_url}/workspaces"
        response:
          processors:
            - expression: 'jmespath(response.json, "workspaces")'
              output: "state.workspaces"

      # Second call: For each workspace, get tables
      - request:
          url: "{state.base_url}/workspace/{state.workspaces[0].id}/tables"
        response:
          processors:
            - expression: 'jmespath(response.json, "tables")'
              output: "state.all_tables"

    iterate: "state.all_tables"
    into: "state.table"

    endpoint:
      name: "{state.table.workspace_name}_{state.table.name}"
      description: "{state.table.workspace_name} / {state.table.name}"

      state:
        workspace_id: "{state.table.workspace_id}"
        table_id: "{state.table.id}"

      request:
        url: "{state.base_url}/workspace/{state.workspace_id}/table/{state.table_id}/rows"

      response:
        records:
          jmespath: "rows[]"
```

**Important Notes**:
- Dynamic endpoints are rendered **after authentication** and **before** static endpoint execution
- Generated endpoint names must be unique (duplicates will cause an error)
- The `into` variable must start with `state.`
- All fields in the endpoint template support template rendering
- Generated endpoints share the same `defaults` as static endpoints

## Template for New API Specs

```yaml
name: "API Name"
description: "API Description"

# Optional: Declare queues if needed
# queues:
#   - item_ids

authentication:
  type: "basic" # basic|oauth2|aws-sigv4|sequence|""
  username: "{secrets.username}"
  password: "{secrets.password}"
  
  # OAuth2 Client Credentials example (most common for APIs):
  # type: "oauth2"
  # flow: "client_credentials"
  # client_id: "{secrets.oauth_client_id}"
  # client_secret: "{secrets.oauth_client_secret}"
  # authentication_url: "https://api.example.com/oauth/token"
  # scopes: ["read:data"]

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
      starting_after: '{sync.last_item_id}' # For cursor pagination + incremental
      created_since: '{coalesce(sync.last_sync_time, date_format(date_add(now(), -1, 'day'), '%Y-%m-%d'))}'

    # Persist last item ID and timestamp for next run
    sync: [last_item_id, last_sync_time]

    request:
      url: "{state.base_url}/items" # Relative to defaults.request.url
      parameters:
        limit: '{state.limit}'
        starting_after: '{state.starting_after}'
        created_since: '{state.created_since}'

    pagination:
      # Update 'starting_after' state with the ID of the last record from the response
      next_state:
        starting_after: "{response.records[-1].id}"
      # Stop when the API indicates no more pages or returns an empty list
      stop_condition: 'jmespath(response.json, "has_more") || length(response.records) == 0'

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
  #     url: "{state.base_url}/items/{state.current_item_id}"
  #   response:
  #     records:
  #       jmespath: "data" # Assuming single item response
  #       primary_key: ["id"]

```

---

## Expression Functions

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
