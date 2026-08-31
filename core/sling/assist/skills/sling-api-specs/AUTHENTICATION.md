# Authentication

Sling API specs support 8 authentication types. Authentication is configured at the spec level and can be overridden per-endpoint.

The `{secrets.X}` names below are examples. Reference only keys that exist on the connection. Learn the real key names from the connection's existing spec or its property keys — names only, never values. An unknown secret renders empty and produces 401s.

## No Authentication

Omit the `authentication` block or set `type: ""`:

```yaml
authentication:
  type: ""  # or omit entirely
```

## Static Headers (API Key / Bearer Token)

Most common for simple API keys or bearer tokens:

```yaml
authentication:
  type: "static"
  headers:
    Authorization: "Bearer {secrets.api_token}"
    # or
    X-API-Key: "{secrets.api_key}"
```

## Basic Auth

HTTP Basic Authentication with username/password:

```yaml
authentication:
  type: "basic"
  username: "{secrets.username}"
  password: "{secrets.password}"
```

## OAuth2

### Client Credentials Flow (Server-to-Server)

Most common for API integrations. Gets access token using client credentials:

```yaml
authentication:
  type: "oauth2"
  flow: "client_credentials"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  authentication_url: "https://api.example.com/oauth/token"
  scopes:
    - "read:data"
    - "write:data"
```

### Authorization Code Flow

For user-authorized access. Can use automatic browser flow or manual code:

```yaml
authentication:
  type: "oauth2"
  flow: "authorization_code"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  authentication_url: "https://api.example.com/oauth/token"
  authorization_url: "https://api.example.com/oauth/authorize"
  # Empty redirect_uri = auto browser flow
  # Custom URI = manual mode with pre-obtained code
  token: "{secrets.authorization_code}"  # For manual mode
  scopes: ["read:data"]
```

### Resource Owner Password (Deprecated)

Avoid if possible. Uses username/password directly:

```yaml
authentication:
  type: "oauth2"
  flow: "password"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  username: "{secrets.username}"
  password: "{secrets.password}"
  authentication_url: "https://api.example.com/oauth/token"
  scopes: ["read:user"]
```

### Refresh Token Flow

Use existing refresh token to get new access token:

```yaml
authentication:
  type: "oauth2"
  flow: "refresh_token"
  client_id: "{secrets.oauth_client_id}"
  client_secret: "{secrets.oauth_client_secret}"
  refresh_token: "{secrets.refresh_token}"
  authentication_url: "https://api.example.com/oauth/token"
```

### Device Code Flow (CLI/Headless)

For environments without browser (Google, Microsoft, GitHub):

```yaml
authentication:
  type: "oauth2"
  flow: "device_code"
  client_id: "{secrets.oauth_client_id}"
  # client_secret optional for public clients
  authentication_url: "https://oauth2.googleapis.com/token"
  device_auth_url: "https://oauth2.googleapis.com/device/code"
  scopes:
    - "https://www.googleapis.com/auth/spreadsheets.readonly"
```

User is prompted to visit a URL and enter a code. Token is stored automatically.

### OAuth2 Options

```yaml
authentication:
  type: "oauth2"
  # ...
  expires: 3600  # Re-auth interval in seconds
  refresh_on_expire: true  # Auto-refresh tokens
```

## AWS Signature v4

For AWS services (API Gateway, S3, Lambda):

```yaml
authentication:
  type: "aws-sigv4"
  aws_service: "execute-api"  # Service name
  aws_region: "{secrets.aws_region}"
  aws_access_key_id: "{secrets.aws_access_key_id}"
  aws_secret_access_key: "{secrets.aws_secret_access_key}"
  aws_session_token: "{secrets.aws_session_token}"  # Optional for temp creds
  aws_profile: "{secrets.aws_profile}"  # Optional, use profile instead
```

## HMAC Authentication

For crypto exchanges (Kraken, Binance) and custom APIs:

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
```

### Common HMAC Patterns

```yaml
# Kraken style
signing_string: "{http_method}{http_path}{nonce}{http_body_sha256}"

# Simple timestamp
signing_string: "{http_method}{unix_time}{http_body_raw}"

# AWS style
signing_string: "{http_method}\n{http_path}\n{http_query}\n{http_headers}"
```

## Sequence Authentication

Custom multi-step authentication for complex APIs:

```yaml
authentication:
  type: "sequence"
  sequence:
    # Step 1: Login
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

    # Step 2: Validate
    - request:
        url: "https://api.example.com/auth/validate"
        headers:
          Authorization: "Bearer {state.access_token}"
```

## Endpoint-Level Override

Override or disable authentication per endpoint:

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

## Related Topics

- [VARIABLES.md](VARIABLES.md) - Using `secrets.*` and `auth.*` scopes
- [ENDPOINTS.md](ENDPOINTS.md) - Endpoint-level configuration
