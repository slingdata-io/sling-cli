# OAuth2 Authentication Examples

This document provides examples of how to configure OAuth2 authentication in Sling API specifications.

## Supported OAuth2 Flows

The following OAuth2 flows are supported:

1. **Client Credentials Flow** - For server-to-server authentication
2. **Authorization Code Flow** - For web applications (requires pre-obtained authorization code)
3. **Resource Owner Password Credentials Flow** - For trusted applications (deprecated but still supported)
4. **Refresh Token Flow** - For refreshing expired access tokens

## Configuration Examples

### 1. Client Credentials Flow

Used for server-to-server authentication where the application itself needs to authenticate.

```yaml
authentication:
  type: oauth2
  flow: client_credentials
  client_id: "${secrets.OAUTH_CLIENT_ID}"
  client_secret: "${secrets.OAUTH_CLIENT_SECRET}"
  authentication_url: "https://api.example.com/oauth/token"
  scopes:
    - "read:data"
    - "write:data"
```

### 2. Authorization Code Flow

Used for web and CLI applications. This flow supports two modes:

#### Interactive Mode (CLI Applications)
When `redirect_uri` is empty or points to localhost, Sling will automatically start a local server and open your browser:

```yaml
authentication:
  type: oauth2
  flow: authorization_code
  client_id: "${secrets.OAUTH_CLIENT_ID}"
  client_secret: "${secrets.OAUTH_CLIENT_SECRET}"
  authentication_url: "https://api.example.com/oauth/token"
  # redirect_uri: ""  # Leave empty for automatic localhost server
  scopes:
    - "read:data"
```

#### Manual Mode (Web Applications)
When you have a specific redirect URI, provide the authorization code manually:

```yaml
authentication:
  type: oauth2
  flow: authorization_code
  client_id: "${secrets.OAUTH_CLIENT_ID}"
  client_secret: "${secrets.OAUTH_CLIENT_SECRET}"
  authentication_url: "https://api.example.com/oauth/token"
  redirect_uri: "https://myapp.example.com/callback"
  token: "${secrets.AUTHORIZATION_CODE}"  # The authorization code
```

### 3. Resource Owner Password Credentials Flow

Used for trusted applications where you have the user's credentials. **Note: This flow is deprecated and should be avoided when possible.**

```yaml
authentication:
  type: oauth2
  flow: password
  client_id: "${secrets.OAUTH_CLIENT_ID}"
  client_secret: "${secrets.OAUTH_CLIENT_SECRET}"
  username: "${secrets.USERNAME}"
  password: "${secrets.PASSWORD}"
  authentication_url: "https://api.example.com/oauth/token"
  scopes:
    - "read:user"
    - "write:user"
```

### 4. Refresh Token Flow

Used to refresh an expired access token using a refresh token.

```yaml
authentication:
  type: oauth2
  flow: refresh_token
  client_id: "${secrets.OAUTH_CLIENT_ID}"  # Optional for some providers
  client_secret: "${secrets.OAUTH_CLIENT_SECRET}"  # Optional for some providers
  refresh_token: "${secrets.REFRESH_TOKEN}"
  authentication_url: "https://api.example.com/oauth/token"
```

## Template Variables

All OAuth2 configuration fields support template variables from:

- `${env.VARIABLE_NAME}` - Environment variables
- `${secrets.VARIABLE_NAME}` - Secrets provided at runtime
- `${state.VARIABLE_NAME}` - State variables

## Error Handling

The OAuth2 implementation includes comprehensive error handling:

- **Validation Errors**: Missing required fields (client_id, client_secret, etc.)
- **HTTP Errors**: Network issues, server errors (4xx/5xx responses)
- **OAuth2 Errors**: Standard OAuth2 error responses (invalid_grant, invalid_client, etc.)

## Usage in Sling

When using OAuth2 authentication, the system will:

1. **Validate Configuration**: Check that all required fields are provided for the selected flow
2. **Render Templates**: Process any template variables in the configuration
3. **Handle Authorization**: 
   - For **Client Credentials**: Direct token request
   - For **Authorization Code (Interactive)**: Start local server, open browser, wait for callback
   - For **Authorization Code (Manual)**: Use provided authorization code
   - For **Password**: Direct token request with credentials
   - For **Refresh Token**: Exchange refresh token for new access token
4. **Make Token Request**: Send the appropriate OAuth2 request to get an access token
5. **Store Token**: Save the access token and any refresh token for future use
6. **Set Headers**: Automatically add the `Authorization: Bearer <token>` header to all API requests

## Interactive Authorization Flow Details

For the Authorization Code flow in interactive mode, Sling will:

1. **Start Local Server**: Automatically find an available port and start an HTTP server
2. **Build Authorization URL**: Construct the proper OAuth2 authorization URL with all parameters
3. **Open Browser**: Automatically open your default browser to the authorization URL
4. **Wait for Callback**: Listen for the OAuth2 provider to redirect back with the authorization code
5. **Exchange Code**: Automatically exchange the authorization code for an access token
6. **Clean Up**: Shut down the local server and continue with the API requests

The local server will display a user-friendly success/error page and automatically close after 3 seconds.

## Best Practices

1. **Store Sensitive Data Securely**: Always use `${secrets.*}` variables for sensitive information like client secrets and passwords
2. **Use Client Credentials for Server-to-Server**: This is the most secure flow for automated systems
3. **Implement Token Refresh**: For long-running processes, implement refresh token handling
4. **Handle Errors Gracefully**: Always check for authentication failures and implement retry logic
5. **Follow OAuth2 Security Guidelines**: Use HTTPS, validate redirect URIs, and follow the OAuth2 security best practices

## Complete Example

Here's a complete API specification example using OAuth2 client credentials flow:

```yaml
name: "Example API with OAuth2"
description: "API specification demonstrating OAuth2 client credentials flow"

authentication:
  type: oauth2
  flow: client_credentials
  client_id: "${secrets.API_CLIENT_ID}"
  client_secret: "${secrets.API_CLIENT_SECRET}"
  authentication_url: "https://api.example.com/oauth/token"
  scopes:
    - "read:data"

endpoints:
  users:
    request:
      url: "https://api.example.com/users"
      method: GET
    response:
      records:
        jmespath: "data[*]"
        primary_key: ["id"]
```

This specification will:
1. Authenticate using OAuth2 client credentials flow
2. Obtain an access token
3. Make requests to the `/users` endpoint with the token
4. Extract user records from the response 