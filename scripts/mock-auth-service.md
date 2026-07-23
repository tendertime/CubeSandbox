# Mock Auth Service

This directory contains a minimal auth callback server for local CubeAPI/WebUI
development.

## Behavior

- `POST /verify`
- Returns `200` for allowed credentials
- Returns `401` for missing or unknown credentials
- Returns `403` for read-only credentials on non-read methods
- `GET /health` returns `200`

## Run

```bash
export MOCK_AUTH_KEYS="secret-key-1,secret-key-2"
export MOCK_READONLY_KEYS="readonly-key-1"
python3 scripts/mock-auth-service.py --host 127.0.0.1 --port 8081
```

Then point CubeAPI at it:

```bash
export AUTH_CALLBACK_URL="http://127.0.0.1:8081/verify"
```

Or start both processes from the repository root with one command:

```bash
make cubeapi-dev-auth
```

This command leaves `MOCK_AUTH_KEYS` unset by default, so any non-empty WebUI login
token or API key is accepted. Set `MOCK_AUTH_KEYS` before running the make target
when a fixed allowlist is needed.

## Credential Rules

- `Authorization: Bearer <token>` takes priority
- `X-API-Key: <key>` is accepted if no bearer token is present
- When `MOCK_AUTH_KEYS` is set, only listed keys are accepted
- When `MOCK_AUTH_KEYS` is empty, any non-empty credential is accepted
- Terminal WebSocket paths require a full-access key. A terminal upgrade uses
  HTTP `GET`, but it is not treated as a read-only operation.

## Example Requests

```bash
curl -i -X POST http://127.0.0.1:8081/verify \
  -H 'Authorization: Bearer secret-key-1' \
  -H 'X-Request-Path: /sandboxes/demo/terminal' \
  -H 'X-Request-Method: GET'
```

```bash
curl -i -X POST http://127.0.0.1:8081/verify \
  -H 'X-API-Key: readonly-key-1' \
  -H 'X-Request-Path: /templates/demo' \
  -H 'X-Request-Method: DELETE'
```
