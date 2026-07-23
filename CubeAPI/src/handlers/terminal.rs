// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{
    extract::{Extension, Path, Query, State, WebSocketUpgrade},
    http::HeaderMap,
    response::IntoResponse,
};
use tracing::{info, warn};

use crate::{
    error::AppError,
    logging::{LogEvent, LogLevel},
    middleware::auth::RequestIdentity,
    state::AppState,
};

pub async fn terminal_ws(
    State(state): State<AppState>,
    Path(sandbox_id): Path<String>,
    Query(query): Query<TerminalQuery>,
    identity: Option<Extension<RequestIdentity>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    state
        .logger
        .log(
            LogEvent::new(LogLevel::Debug, "api.request")
                .field("handler", "terminal_ws")
                .field("sandbox_id", &sandbox_id),
        )
        .await;

    validate_terminal_origin(&headers, &state.config.terminal_allowed_origins)?;

    let state_clone = state.clone();
    let operator_id = identity
        .map(|Extension(identity)| identity.operator_id)
        .unwrap_or_else(|| "unknown".to_string());

    if !state.config.auth_enabled() {
        warn!(
            sandbox_id = %sandbox_id,
            "terminal authentication unavailable: no authentication mode is configured"
        );
        state
            .logger
            .log(
                LogEvent::new(LogLevel::Warn, "terminal.auth_unavailable")
                    .field("sandbox_id", &sandbox_id)
                    .field("reason", "authentication_not_configured"),
            )
            .await;
        return Err(AppError::Unauthorized(
            "terminal requires authenticated access".to_string(),
        ));
    }

    Ok(ws.on_upgrade(move |socket| async move {
        info!(sandbox_id = %sandbox_id, "terminal websocket upgraded");

        if let Err(e) = state_clone
            .services
            .terminal
            .handle_terminal(
                sandbox_id.clone(),
                query.container.clone(),
                socket,
                state_clone.logger.clone(),
                operator_id,
            )
            .await
        {
            info!(sandbox_id = %sandbox_id, error = %e, "terminal error");
        }
    }))
}

#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct TerminalQuery {
    container: Option<String>,
}

fn validate_terminal_origin(
    headers: &HeaderMap,
    allowed_origins: &[String],
) -> Result<(), AppError> {
    let origin = headers
        .get("origin")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::Unauthorized("missing WebSocket Origin".to_string()))?;

    if !allowed_origins.is_empty() {
        if allowed_origins
            .iter()
            .any(|allowed| origins_are_equivalent(origin, allowed))
        {
            return Ok(());
        }
        return Err(AppError::Unauthorized(format!(
            "WebSocket Origin {} is not allowed",
            origin
        )));
    }

    let host = headers
        .get("host")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::Unauthorized("missing Host header".to_string()))?;

    if origin_matches_host(origin, host) {
        return Ok(());
    }

    Err(AppError::Unauthorized(format!(
        "WebSocket Origin {} does not match Host {}",
        origin, host
    )))
}

fn origins_are_equivalent(left: &str, right: &str) -> bool {
    matches!(
        (strict_origin(left), strict_origin(right)),
        (Some(left), Some(right)) if left == right
    )
}

fn strict_origin(origin: &str) -> Option<(String, String, u16)> {
    let parsed = reqwest::Url::parse(origin).ok()?;
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return None;
    }
    let scheme = parsed.scheme().to_ascii_lowercase();
    if !matches!(scheme.as_str(), "http" | "https" | "ws" | "wss") {
        return None;
    }
    Some((
        scheme,
        parsed.host_str()?.to_ascii_lowercase(),
        parsed.port_or_known_default()?,
    ))
}

fn origin_matches_host(origin: &str, host: &str) -> bool {
    let (origin_scheme, origin_host, origin_port) = match parse_origin(origin) {
        Some(value) => value,
        None => return false,
    };
    let (host_name, host_port) = match parse_host_like(host) {
        Some(value) => value,
        None => return false,
    };

    if origin_host != host_name {
        return false;
    }

    match (origin_port, host_port) {
        (Some(a), Some(b)) => a == b,
        (Some(port), None) => matches!(port, 80 | 443),
        (None, Some(port)) => matches!(
            (origin_scheme.as_str(), port),
            ("http" | "ws", 80) | ("https" | "wss", 443)
        ),
        (None, None) => true,
    }
}

fn parse_origin(origin: &str) -> Option<(String, String, Option<u16>)> {
    let (scheme, rest) = origin.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if !matches!(scheme.as_str(), "http" | "https" | "ws" | "wss") {
        return None;
    }
    let host_part = rest.split_once('/').map(|(host, _)| host).unwrap_or(rest);
    let (host, port) = parse_host_like(host_part)?;
    Some((scheme, host, port))
}

fn parse_host_like(value: &str) -> Option<(String, Option<u16>)> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }

    if let Some(rest) = value.strip_prefix('[') {
        let (host, remainder) = rest.split_once(']')?;
        let port = remainder
            .strip_prefix(':')
            .and_then(|value| value.parse::<u16>().ok());
        return Some((host.to_ascii_lowercase(), port));
    }

    let colon_count = value.chars().filter(|ch| *ch == ':').count();
    if colon_count == 1 {
        let (host, port) = value.rsplit_once(':')?;
        return Some((host.to_ascii_lowercase(), port.parse::<u16>().ok()));
    }

    Some((value.to_ascii_lowercase(), None))
}

#[cfg(test)]
mod tests {
    use super::{origin_matches_host, validate_terminal_origin};
    use crate::{
        config::ServerConfig,
        logging::{arc, noop::NoopLogger},
        routes::build_router,
        state::AppState,
    };
    use axum::{
        body::Body,
        extract::{Json, Path, State},
        http::{header, HeaderMap, HeaderValue, StatusCode},
        response::IntoResponse,
        routing::{any, post},
        Router,
    };
    use base64::Engine;
    use futures::{SinkExt, StreamExt};
    use serde_json::Value;
    use std::sync::{Arc, OnceLock};
    use tokio::{
        net::TcpListener,
        sync::Mutex,
        time::{timeout, Duration},
    };
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Message as WsMessage},
    };

    #[derive(Clone, Default)]
    struct ProxyCapture {
        start_payloads: Arc<Mutex<Vec<Value>>>,
        input_bodies: Arc<Mutex<Vec<Value>>>,
        update_bodies: Arc<Mutex<Vec<Value>>>,
        signal_bodies: Arc<Mutex<Vec<Value>>>,
    }

    static AGENTHUB_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    async fn spawn_server(app: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{}", addr)
    }

    fn connect_stream_bytes() -> Vec<u8> {
        let raw = serde_json::to_vec(&serde_json::json!({
            "event": {
                "start": { "pid": 4242 },
                "data": {
                    "pty": base64::engine::general_purpose::STANDARD.encode("hello from pty\n")
                }
            }
        }))
        .unwrap();
        let mut body = Vec::with_capacity(5 + raw.len());
        body.push(0x00);
        body.extend_from_slice(&(raw.len() as u32).to_be_bytes());
        body.extend_from_slice(&raw);
        body
    }

    fn decode_connect_payload(raw: &[u8]) -> Value {
        assert!(raw.len() >= 5);
        assert_eq!(raw[0], 0);
        let size = u32::from_be_bytes(raw[1..5].try_into().unwrap()) as usize;
        assert_eq!(raw.len(), 5 + size);
        serde_json::from_slice(&raw[5..]).unwrap()
    }

    fn sandbox_detail_response() -> Value {
        serde_json::json!({
            "requestID": "req-1",
            "data": [{
                "sandbox_id": "sb-123",
                "status": 1,
                "host_id": "host-1",
                "template_id": "tpl-1",
                "annotations": {},
                "labels": {},
                "containers": [{
                    "name": "sandbox",
                    "container_id": "sb-123",
                    "status": 1,
                    "image": "cube/test:latest",
                    "create_at": 1_720_000_000_000_000_000i64,
                    "cpu": "100m",
                    "mem": "128Mi",
                    "type": "sandbox"
                }, {
                    "name": "worker",
                    "container_id": "worker-1",
                    "status": 1,
                    "image": "cube/worker:1.0.0",
                    "create_at": 1_720_000_000_000_000_000i64,
                    "cpu": "100m",
                    "mem": "128Mi",
                    "type": "workload"
                }],
                "namespace": "default"
            }],
            "ret": { "ret_code": 0, "ret_msg": "ok" }
        })
    }

    fn sandbox_list_response() -> Value {
        serde_json::json!({
            "requestID": "req-2",
            "sandboxes": [{
                "sandbox_id": "sb-123",
                "host_id": "host-1",
                "status": "running",
                "started_at": "2026-07-09T00:00:00Z",
                "create_at": 1_720_000_000_000_000_000i64,
                "end_at": null,
                "cpuCount": 1,
                "memoryMB": 128,
                "template_id": "tpl-1",
                "annotations": {},
                "labels": {}
            }],
            "ret": { "ret_code": 0, "ret_msg": "ok" }
        })
    }

    async fn spawn_fake_cubemaster() -> String {
        async fn get_sandbox_handler() -> impl IntoResponse {
            Json(sandbox_detail_response())
        }

        async fn list_sandbox_handler() -> impl IntoResponse {
            Json(sandbox_list_response())
        }

        async fn ok_handler() -> impl IntoResponse {
            Json(serde_json::json!({
                "requestID": "req-ok",
                "sandboxID": "sb-123",
                "end_at": "2026-07-13T00:00:00Z",
                "ret": { "ret_code": 0, "ret_msg": "ok" }
            }))
        }

        let app = Router::new()
            .route("/cube/sandbox/info", any(get_sandbox_handler))
            .route("/cube/sandbox/list", post(list_sandbox_handler));
        let app = app
            .route("/cube/sandbox/timeout", post(ok_handler))
            .route("/cube/sandbox/refresh", post(ok_handler));
        spawn_server(app).await
    }

    async fn spawn_fake_proxy(capture: ProxyCapture) -> String {
        async fn start_handler(
            State(capture): State<ProxyCapture>,
            Path(sandbox_id): Path<String>,
            headers: HeaderMap,
            body: axum::body::Bytes,
        ) -> impl IntoResponse {
            assert_eq!(sandbox_id, "sb-123");
            assert!(headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .contains("application/connect+json"));
            capture
                .start_payloads
                .lock()
                .await
                .push(decode_connect_payload(&body));
            (StatusCode::OK, Body::from(connect_stream_bytes()))
        }

        async fn capture_json(
            State(capture): State<ProxyCapture>,
            Path((sandbox_id, action)): Path<(String, String)>,
            Json(body): Json<Value>,
        ) -> impl IntoResponse {
            assert_eq!(sandbox_id, "sb-123");
            match action.as_str() {
                "SendInput" => capture.input_bodies.lock().await.push(body),
                "Update" => capture.update_bodies.lock().await.push(body),
                "SendSignal" => capture.signal_bodies.lock().await.push(body),
                other => panic!("unexpected action {other}"),
            }
            StatusCode::OK
        }

        let app = Router::new()
            .route(
                "/sandbox/:sandbox_id/49983/process.Process/Start",
                post(start_handler),
            )
            .route(
                "/sandbox/:sandbox_id/49983/process.Process/:action",
                post(capture_json),
            )
            .with_state(capture);
        spawn_server(app).await
    }

    async fn spawn_fake_auth() -> String {
        async fn auth_handler() -> impl IntoResponse {
            StatusCode::OK
        }

        let app = Router::new().route("/", post(auth_handler));
        spawn_server(app).await
    }

    struct EnvVarGuard {
        key: &'static str,
        old: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let old = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, old }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = self.old.take() {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    async fn connect_terminal(
        ws_url: &str,
        origin: &str,
        auth_token: &str,
        operator_id: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let mut request = ws_url.to_string().into_client_request().unwrap();
        request
            .headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
        request.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", auth_token)).unwrap(),
        );
        request
            .headers_mut()
            .insert("x-operator-id", HeaderValue::from_str(operator_id).unwrap());
        let (ws_stream, _) = timeout(Duration::from_secs(5), connect_async(request))
            .await
            .expect("websocket connect timed out")
            .expect("websocket connect failed");
        ws_stream
    }

    async fn wait_for_len<T>(items: &Arc<Mutex<Vec<T>>>, expected: usize) {
        timeout(Duration::from_secs(5), async {
            loop {
                let len = items.lock().await.len();
                if len >= expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("timed out waiting for captured request");
    }

    #[test]
    fn accepts_same_origin_without_explicit_port() {
        assert!(origin_matches_host("https://example.com", "example.com"));
    }

    #[test]
    fn accepts_same_origin_with_explicit_port() {
        assert!(origin_matches_host(
            "https://example.com:8443",
            "example.com:8443"
        ));
    }

    #[test]
    fn rejects_cross_origin_host() {
        assert!(!origin_matches_host(
            "https://example.com",
            "malicious.example"
        ));
    }

    #[test]
    fn rejects_non_web_origin_scheme() {
        assert!(!origin_matches_host("ftp://example.com", "example.com"));
    }

    #[test]
    fn configured_origin_allowlist_takes_priority_over_host() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://console.example.com"),
        );
        headers.insert(
            header::HOST,
            HeaderValue::from_static("untrusted-proxy-host.example"),
        );

        assert!(
            validate_terminal_origin(&headers, &["https://console.example.com".to_string()])
                .is_ok()
        );
    }

    #[test]
    fn configured_origin_allowlist_rejects_unlisted_origin_even_when_host_matches() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://other.example.com"),
        );
        headers.insert(header::HOST, HeaderValue::from_static("other.example.com"));

        assert!(
            validate_terminal_origin(&headers, &["https://console.example.com".to_string()])
                .is_err()
        );
    }

    #[tokio::test]
    async fn terminal_websocket_happy_path_round_trip() {
        let _env_lock = AGENTHUB_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .await;
        let capture = ProxyCapture::default();
        let auth_url = spawn_fake_auth().await;
        let cubemaster_url = spawn_fake_cubemaster().await;
        let proxy_url = spawn_fake_proxy(capture.clone()).await;
        let _env_guard = EnvVarGuard::set("AGENTHUB_SANDBOX_PROXY_URL", &proxy_url);

        let config = ServerConfig {
            auth_callback_url: Some(auth_url),
            cubemaster_url,
            instance_type: "cubebox".to_string(),
            sandbox_domain: "cube.app".to_string(),
            ..ServerConfig::default()
        };
        let state = AppState::new(config, arc(NoopLogger)).await;
        let app = build_router(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let ws_url = format!("ws://{}/sandboxes/sb-123/terminal?container=worker-1", addr);
        let origin = format!("http://{}", addr);
        let mut ws = connect_terminal(&ws_url, &origin, "test-token", "tester-1").await;
        ws.send(WsMessage::Text(
            r#"{"type":"resize","rows":41,"cols":133}"#.to_string(),
        ))
        .await
        .unwrap();

        wait_for_len(&capture.start_payloads, 1).await;
        let start_payload = capture.start_payloads.lock().await[0].clone();
        assert_eq!(
            start_payload["process"]["container_id"],
            serde_json::json!("worker-1")
        );

        let mut saw_ready = None;
        let mut saw_output = None;
        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("timed out waiting for terminal message")
                .expect("websocket stream ended unexpectedly")
                .expect("websocket message error");
            match msg {
                WsMessage::Text(text) => {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    match value.get("type").and_then(|v| v.as_str()) {
                        Some("ready") => {
                            saw_ready = value
                                .get("session_id")
                                .and_then(|v| v.as_str())
                                .map(str::to_string);
                        }
                        Some("output") => {
                            saw_output = value
                                .get("data")
                                .and_then(|v| v.as_str())
                                .map(str::to_string);
                        }
                        _ => {}
                    }
                }
                WsMessage::Ping(data) => {
                    ws.send(WsMessage::Pong(data)).await.unwrap();
                }
                WsMessage::Close(_) => break,
                _ => {}
            }
            if saw_ready.is_some() && saw_output.is_some() {
                break;
            }
        }

        let session_id = saw_ready.expect("terminal ready event missing");
        assert!(!session_id.is_empty());
        assert_eq!(saw_output.as_deref(), Some("hello from pty\n"));

        ws.send(WsMessage::Text("echo hello from e2e\n".to_string()))
            .await
            .unwrap();

        wait_for_len(&capture.update_bodies, 1).await;
        wait_for_len(&capture.input_bodies, 1).await;

        let update = capture.update_bodies.lock().await[0].clone();
        assert_eq!(update["pty"]["size"]["rows"], serde_json::json!(41));
        assert_eq!(update["pty"]["size"]["cols"], serde_json::json!(133));

        let input = capture.input_bodies.lock().await[0].clone();
        let encoded_input = input["input"]["pty"].as_str().expect("input encoding");
        let decoded_input = base64::engine::general_purpose::STANDARD
            .decode(encoded_input)
            .expect("input should decode");
        assert_eq!(
            String::from_utf8(decoded_input).unwrap(),
            "echo hello from e2e\n"
        );

        ws.close(None).await.unwrap();

        wait_for_len(&capture.signal_bodies, 1).await;
        let signal = capture.signal_bodies.lock().await[0].clone();
        assert_eq!(signal["signal"], serde_json::json!("SIGNAL_SIGKILL"));
    }
}
