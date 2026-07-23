# Authentication

Cube API Server supports an external callback or a built-in static API key for authentication. When either mode is configured, sandbox, template, volume, and Web Terminal requests are authenticated. Web Terminal is always protected because it grants interactive command execution; without either authentication mode, ordinary APIs remain available but terminal access is disabled.

## Enabling Authentication

For callback authentication, pass `--auth-callback-url` at startup or set the equivalent environment variable:

```bash
# CLI flag
./cube-api --auth-callback-url https://your-auth-service/verify

# Or via environment variable
export AUTH_CALLBACK_URL=https://your-auth-service/verify
./cube-api
```

For a local static key instead, leave `AUTH_CALLBACK_URL` unset and configure:

```bash
export CUBE_API_KEY=your-actual-api-key
./cube-api
```

Callback authentication takes priority when both values are set. When neither is set, ordinary APIs keep their existing behavior but Web Terminal access is disabled. CubeAPI does not fall back to anonymous terminal access.

## How It Works

When a protected request arrives in callback mode, Cube API Server:

1. Extracts the credential from the request header (`Authorization: Bearer` takes priority over `X-API-Key`).
2. Forwards a `POST` request to the callback URL with the credential header, the original request path, **and the HTTP method**.
3. If the callback returns **HTTP 200**, the request is allowed through.
4. Any other status code causes the request to be rejected with **HTTP 401 Unauthorized**.

```
Client ──→ Cube API Server
                │
                ├─ extract credential (Bearer / API Key)
                ├─ capture method (GET / POST / DELETE / PATCH …)
                │
                └─ POST → your auth service
                                │
                       200 ─────┤──→ allow request
                    non-200 ────┘──→ 401 Unauthorized
```

## Sending Credentials from the SDK

The E2B SDK passes the value of `E2B_API_KEY` as `Authorization: Bearer <key>` on every request.

```bash
export E2B_API_KEY=your-actual-api-key
```

You can also send `X-API-Key` directly if your integration does not use the E2B SDK:

```
X-API-Key: your-actual-api-key
```

Both formats are accepted. `Authorization: Bearer` takes priority if both are present.

Browser WebSocket APIs cannot add these headers to an upgrade request. The WebUI therefore base64url-encodes its existing login token or API key into a terminal-only query parameter. CubeAPI decodes it and applies the same Bearer or API-key authentication before upgrading the connection.

## Callback Request Format

Cube API Server sends a `POST` to your callback URL with the following headers:

| Header | Value |
|--------|-------|
| `Authorization` | `Bearer <token>` — present when the client used Bearer auth |
| `X-API-Key` | `<key>` — present when the client used API Key auth |
| `X-Request-Path` | The original request path (e.g. `/templates/my-tmpl`) |
| `X-Request-Method` | The HTTP method of the original request (e.g. `GET`, `DELETE`) |

The two credential headers are mutually exclusive. Your callback receives whichever one the client sent.

::: warning Validate both path **and** method
Multiple HTTP methods are mounted on the same path — for example, `/templates/:id` handles `GET` (read), `POST` (rebuild), `DELETE` (delete), and `PATCH` (update). A callback that only whitelists by path cannot distinguish a read from a destructive operation: a caller with read-only access could escalate to delete or overwrite a template.

Always check **both** `X-Request-Path` and `X-Request-Method` in your callback.
:::

### Example callback (Python/FastAPI)

```python
from fastapi import FastAPI, Request
from fastapi.responses import Response

app = FastAPI()

VALID_KEYS = {"secret-key-1", "secret-key-2"}

# Define which methods each key is allowed to use per path prefix.
# Always check BOTH path and method — the same path (e.g. /templates/:id)
# serves GET (read), DELETE, POST (rebuild), and PATCH (update).
READ_METHODS = {"GET", "HEAD"}
WRITE_METHODS = {"POST", "DELETE", "PATCH", "PUT"}

READONLY_KEYS = {"readonly-key-1"}
FULL_ACCESS_KEYS = {"secret-key-1", "secret-key-2"}

@app.post("/verify")
async def verify(request: Request):
    path = request.headers.get("X-Request-Path", "")
    method = request.headers.get("X-Request-Method", "").upper()

    # Extract credential (Bearer takes priority)
    key = None
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        key = auth.removeprefix("Bearer ").strip()
    else:
        key = request.headers.get("X-API-Key", "")

    if not key:
        return Response(status_code=401)

    if key in FULL_ACCESS_KEYS:
        return {}                           # 200 → allow all

    if key in READONLY_KEYS:
        # A terminal upgrade is GET but grants command execution, so it is never read-only.
        if path.rstrip("/").endswith("/terminal"):
            return Response(status_code=403)
        if method in READ_METHODS:
            return {}                       # 200 → allow reads
        return Response(status_code=403)   # deny writes/deletes

    return Response(status_code=401)
```

## Local Mock

For local development, you can run a built-in mock callback instead of writing your own
service:

```bash
export MOCK_AUTH_KEYS="secret-key-1,secret-key-2"
export MOCK_READONLY_KEYS="readonly-key-1"
python3 scripts/mock-auth-service.py --host 127.0.0.1 --port 8081
```

Then point CubeAPI at:

```bash
export AUTH_CALLBACK_URL="http://127.0.0.1:8081/verify"
```

For local WebUI development, start the mock callback and CubeAPI together:

```bash
make cubeapi-dev-auth
```

When `MOCK_AUTH_KEYS` is unset, the mock accepts any non-empty WebUI login token or
API key. The WebUI reuses its current login token when opening a terminal.

## Error Responses

| Scenario | HTTP Status |
|----------|-------------|
| No credential provided | `401 Unauthorized` |
| Callback returned non-200 | `401 Unauthorized` |
| Callback unreachable | `500 Internal Server Error` |
