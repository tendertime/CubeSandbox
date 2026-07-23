# Web Terminal

CubeSandbox WebUI can open an interactive terminal for a running sandbox through CubeAPI. The browser connects to CubeAPI with WebSocket, and CubeAPI bridges the session to the sandbox runtime PTY. The browser never connects to envd directly.

## Endpoint

The WebUI uses:

```text
GET /sandboxes/{sandboxID}/terminal
```

The endpoint is protected by the same CubeAPI authentication middleware as other sandbox APIs. When auth is enabled, make sure the WebSocket upgrade request carries the same credentials or session context as normal WebUI API calls.

## Backend Environment

CubeAPI needs to reach the sandbox proxy path used for envd:

```bash
export AGENTHUB_SANDBOX_PROXY_URL="http://127.0.0.1"
```

If the envd proxy path requires an Authorization header, configure it explicitly:

```bash
export CUBESANDBOX_TERMINAL_ENVD_AUTH="Basic <redacted>"
```

Do not rely on hardcoded root credentials. Prefer a scoped runtime credential or proxy-mediated auth when available.

Optional session limits:

```bash
export CUBESANDBOX_TERMINAL_MAX_SESSIONS=64
export CUBESANDBOX_TERMINAL_MAX_SESSIONS_PER_SANDBOX=8
```

## Reverse Proxy

The reverse proxy in front of WebUI/CubeAPI must allow WebSocket upgrades for `/sandboxes/`.

Nginx-style example:

```nginx
location /sandboxes/ {
    proxy_pass http://127.0.0.1:3000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
}
```

Use WSS when WebUI is served over HTTPS.

## Usage

1. Open WebUI.
2. Go to the sandbox list or sandbox detail page.
3. Use **Open Terminal** on a running sandbox.
4. Validate basic terminal behavior:
   - `ls`
   - `top`
   - `ping 127.0.0.1`
   - terminal resize
   - copy/paste
   - disconnect and reconnect

The Open Terminal action is only shown for running sandboxes.

## Session Lifecycle

CubeAPI tracks active terminal sessions in memory. Each session has:

- session id;
- sandbox id;
- operator id when supplied by trusted headers;
- start time.

CubeAPI applies:

- idle timeout;
- maximum session lifetime;
- WebSocket keepalive;
- PTY cleanup on disconnect;
- periodic sandbox hold refresh while a terminal session is active.

Audit events are emitted for open, close, timeout, validation failure, session rejection, hold failure, and PTY connection failure.

## Troubleshooting

### `sandbox_not_found`

The sandbox id does not exist or CubeMaster no longer reports it. Refresh the list and open a terminal only from a current running sandbox.

### `sandbox_not_running`

The sandbox is paused, pausing, stopped, or otherwise not in running state. Resume the sandbox first.

### `session_limit`

The CubeAPI terminal session limit has been reached. Increase `CUBESANDBOX_TERMINAL_MAX_SESSIONS` or `CUBESANDBOX_TERMINAL_MAX_SESSIONS_PER_SANDBOX`, or close existing terminals.

### `sandbox_hold_failed`

CubeAPI could not refresh/hold the sandbox while opening the terminal. Check CubeMaster connectivity and sandbox lifecycle state.

### `pty_connect_failed`

CubeAPI could not start the backend PTY through envd. Check:

- `AGENTHUB_SANDBOX_PROXY_URL`;
- envd proxy routing to port `49983`;
- `CUBESANDBOX_TERMINAL_ENVD_AUTH` if your deployment requires it;
- CubeAPI logs under `/data/log/CubeAPI/`.
