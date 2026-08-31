---
name: sling-api-specs
description: >
  Build REST API specifications for Sling data extraction. Use when creating API specs, configuring authentication (OAuth, API key, Bearer token, HMAC), setting up pagination (cursor, offset, page), processing responses, handling rate limits, chaining endpoints with queues, or implementing incremental sync.
---

# API Specifications

API specs are YAML definitions for extracting data from REST APIs. They handle authentication, pagination, response processing, and incremental sync automatically.

Building specs from Python? Load the `sling-python` skill — `sling.api_spec` provides typed builders (`ApiSpec`, `Endpoint`, ...) that validate and serialize to this YAML.

## Gather first

Resolve each row before you write YAML, in this order: the user's request, existing files, discovery (MCP / CLI), then the default in the table. Ask the user only for rows that stay unresolved, one at a time, with a proposed default. If every row resolves, do not ask — build, and state the assumptions you took. Confirm first only for destructive resolutions (for example a mode that drops or rewrites existing tables).

| Decision | Resolve by | Default |
|----------|------------|---------|
| API name | request, docs title | title from the live docs |
| auth scheme | request, live docs, AUTHENTICATION.md | `static` Bearer |
| endpoints | request, live docs (agent-browser) | do not invent paths |
| pagination | request, live docs, PAGINATION.md | none (single page). If paginated, `stop_condition` is required |
| incremental key | request, docs | none |

Never invent endpoint paths. Never paste cookies, tokens, or password fields into the spec. Use agent-browser (`snapshot`, `read`, `open`) for live docs. If the page needs a login or CAPTCHA, ask the user to allow Chrome remote debugging, then use `--auto-connect` or `--cdp 9222`.

## When to Use

- Extract data from REST APIs (GET endpoints only)
- Build incremental sync workflows
- Handle complex pagination patterns
- Process nested JSON responses
- Chain multiple API calls with queues

## Basic Structure

```yaml
name: "My API"
description: "Data extraction from My API"

authentication:
  type: "static"
  headers:
    Authorization: "Bearer {secrets.api_token}"

defaults:
  state:
    base_url: "https://api.example.com/v1"
  request:
    headers:
      Accept: "application/json"

endpoints:
  users:
    description: "Fetch users"
    request:
      url: "{state.base_url}/users"
    response:
      records:
        jmespath: "data[]"
        primary_key: ["id"]
```

## Live API docs

`sling assist setup` installs the **agent-browser** MCP server (Vercel Labs). Use it to read JS-rendered docs:

- `open` / `snapshot` / `read` for page text
- `--auto-connect` or `--cdp 9222` when the page needs a login or CAPTCHA
- Do not paste cookies, tokens, or password fields into the spec

If agent-browser is missing, run `sling assist setup`. Then run `agent-browser install` once if Chrome is not on the machine.

## MCP Operations

### Parse a Spec
```json
{
  "action": "validate",
  "input": {"file_path": "/path/to/spec.yaml"}
}
```

### Test Endpoints
```json
{
  "action": "test",
  "input": {
    "connection": "MY_API",
    "endpoints": ["users"],
    "debug": true,
    "limit": 10
  }
}
```

MCP `api_spec test` tests the connection's CONFIGURED spec, not your local file. A SUCCESS there does not validate your draft. Before you test, point the connection at your draft. If you did not re-point the spec, you have not tested your work.

```bash
sling conns set CONN "spec=file://$PWD/spec.yaml"
sling conns test CONN --endpoints <name> --debug
```

Pass `spec_file_path` in the MCP `test` input to test your draft file directly, without re-pointing the connection:

```json
{
  "action": "test",
  "input": {"connection": "MY_API", "spec_file_path": "/path/to/spec.yaml", "endpoints": ["users"], "debug": true}
}
```

Reference only `{secrets.X}` keys that exist on the connection. Learn the key names from the connection's existing spec (its `authentication` block is ground truth) or from the connection's property keys — names only, never values. An unknown secret renders empty and produces 401s. Do not invent names like `api_token` when the key is `token`.

Definition of done for a spec task: the endpoint test against YOUR spec file returns more than 0 records with no 4xx. A zero-record result means `records.jmespath` is wrong — fix the path (see [RESPONSE.md](RESPONSE.md)). Do not ship.

Parent-child extraction uses `depends_on` plus a queue. See [QUEUES.md](QUEUES.md).

## Topics Reference

This skill includes detailed documentation for each aspect of API specification building:

| Topic | Description |
|-------|-------------|
| [AUTHENTICATION.md](AUTHENTICATION.md) | All 8 authentication types (static, basic, OAuth2, AWS, HMAC, sequence) |
| [ENDPOINTS.md](ENDPOINTS.md) | Endpoint configuration, setup/teardown sequences |
| [REQUEST.md](REQUEST.md) | HTTP request configuration, rate limiting |
| [PAGINATION.md](PAGINATION.md) | All pagination patterns (cursor, offset, page, link header) |
| [RESPONSE.md](RESPONSE.md) | Record extraction, deduplication |
| [PROCESSORS.md](PROCESSORS.md) | Data transformations, aggregations |
| [VARIABLES.md](VARIABLES.md) | Variable scopes, expressions, rendering order |
| [QUEUES.md](QUEUES.md) | Endpoint chaining, iteration |
| [INCREMENTAL.md](INCREMENTAL.md) | Sync state, context variables |
| [DYNAMIC.md](DYNAMIC.md) | Runtime endpoint generation |
| [FUNCTIONS.md](FUNCTIONS.md) | Expression functions reference |
| [RULES.md](RULES.md) | Response rules, retries, error handling |

## Quick Reference

### Authentication Types

| Type | Use Case |
|------|----------|
| `static` | API key, Bearer token |
| `basic` | Username/password |
| `oauth2` | OAuth 2.0 flows (client_credentials, authorization_code, device_code) |
| `aws-sigv4` | AWS services |
| `hmac` | Crypto exchanges, custom signing |
| `sequence` | Multi-step custom auth |

### Pagination Patterns

| Pattern | Example |
|---------|---------|
| Cursor | `starting_after`, `page_token` |
| Offset | `offset` + `limit` |
| Page | `page` number |
| Link header | GitHub-style `rel="next"` |

### Variable Scopes

| Scope | Description |
|-------|-------------|
| `secrets.*` | Credentials from connection |
| `state.*` | Endpoint state variables |
| `sync.*` | Persisted from previous run |
| `response.*` | HTTP response data |
| `record.*` | Current record in processor |
| `queue.*` | Endpoint chaining |

## Full Documentation

See https://docs.slingdata.io/concepts/api-specs.md for complete reference.
