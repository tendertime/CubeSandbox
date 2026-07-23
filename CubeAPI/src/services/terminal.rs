// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::extract::ws::{self, Message};
use base64::Engine;
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use reqwest::{Client, Response};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU32, Ordering},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    error::AppError,
    logging::{ArcLogger, LogEvent, LogLevel},
    models::{SandboxContainer, SandboxState},
    services::sandboxes::SandboxService,
};

const CONNECT_CONTENT_TYPE: &str = "application/connect+json";
const CONNECT_PROTOCOL_VERSION: &str = "1";
const ENVD_PORT: u16 = 49983;
const TERMINAL_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const TERMINAL_MAX_LIFETIME: Duration = Duration::from_secs(2 * 60 * 60);
const TERMINAL_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const TERMINAL_KEEPALIVE_TIMEOUT_SECONDS: i32 = 3600;
const TERMINAL_PTY_STARTUP_WAIT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct TerminalService {
    sandbox_service: SandboxService,
    http_client: Client,
    sessions: Arc<Mutex<HashMap<String, TerminalSession>>>,
}

#[derive(Clone)]
struct TerminalSession {
    sandbox_id: String,
    container_id: Option<String>,
    operator_id: String,
    started_at: chrono::DateTime<chrono::Utc>,
}

struct ResolvedTerminalTarget {
    access_token: Option<String>,
    container_id: Option<String>,
    container_name: Option<String>,
    end_at: Option<DateTime<Utc>>,
}

impl TerminalService {
    pub fn new(sandbox_service: SandboxService) -> Self {
        Self {
            sandbox_service,
            http_client: Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .build()
                .expect("failed to build HTTP client"),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn max_total_sessions() -> usize {
        std::env::var("CUBESANDBOX_TERMINAL_MAX_SESSIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(64)
    }

    fn max_sessions_per_sandbox() -> usize {
        std::env::var("CUBESANDBOX_TERMINAL_MAX_SESSIONS_PER_SANDBOX")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8)
    }

    async fn register_session(
        &self,
        session_id: String,
        sandbox_id: String,
        container_id: Option<String>,
        operator_id: String,
    ) -> Result<(), AppError> {
        let mut sessions = self.sessions.lock().await;
        let max_total = Self::max_total_sessions();
        if sessions.len() >= max_total {
            return Err(AppError::TooManyRequests(format!(
                "terminal session limit reached: {}",
                max_total
            )));
        }

        let per_sandbox = sessions
            .values()
            .filter(|session| session.sandbox_id == sandbox_id)
            .count();
        let max_per_sandbox = Self::max_sessions_per_sandbox();
        if per_sandbox >= max_per_sandbox {
            return Err(AppError::TooManyRequests(format!(
                "terminal session limit reached for sandbox {}: {}",
                sandbox_id, max_per_sandbox
            )));
        }

        sessions.insert(
            session_id.clone(),
            TerminalSession {
                sandbox_id,
                container_id,
                operator_id,
                started_at: chrono::Utc::now(),
            },
        );
        Ok(())
    }

    async fn unregister_session(
        sessions: Arc<Mutex<HashMap<String, TerminalSession>>>,
        session_id: &str,
    ) -> Option<TerminalSession> {
        sessions.lock().await.remove(session_id)
    }

    async fn audit(
        logger: &ArcLogger,
        level: LogLevel,
        action: &str,
        sandbox_id: &str,
        session_id: Option<&str>,
        operator_id: &str,
        container_id: Option<&str>,
        reason: Option<&str>,
    ) {
        let mut event = LogEvent::new(level, "terminal.session")
            .field("action", action)
            .field("sandbox_id", sandbox_id)
            .field("operator_id", operator_id);
        if let Some(container_id) = container_id {
            event = event.field("container_id", container_id);
        }
        if let Some(session_id) = session_id {
            event = event.field("session_id", session_id);
        }
        if let Some(reason) = reason {
            event = event.field("reason", reason);
        }
        logger.log(event).await;
    }

    async fn validate_terminal_target(
        &self,
        sandbox_id: &str,
        container_selector: Option<&str>,
    ) -> Result<ResolvedTerminalTarget, AppError> {
        let sandbox = self.sandbox_service.get_sandbox(sandbox_id).await?;
        if sandbox.state != SandboxState::Running {
            return Err(AppError::Conflict(format!(
                "sandbox {} is not running",
                sandbox_id
            )));
        }
        let access_token = sandbox.envd_access_token.filter(|token| !token.is_empty());

        let containers = sandbox.containers.unwrap_or_default();
        let (container_id, container_name) =
            Self::resolve_terminal_container(&containers, container_selector, sandbox_id)?;

        Ok(ResolvedTerminalTarget {
            access_token,
            container_id: Some(container_id),
            container_name: Some(container_name),
            end_at: sandbox.end_at,
        })
    }

    fn resolve_terminal_container(
        containers: &[SandboxContainer],
        selector: Option<&str>,
        sandbox_id: &str,
    ) -> Result<(String, String), AppError> {
        let selector = selector.map(str::trim).filter(|value| !value.is_empty());
        let selected = if let Some(selector) = selector {
            containers
                .iter()
                .find(|container| container.container_id == selector || container.name == selector)
                .ok_or_else(|| {
                    AppError::NotFound(format!(
                        "container {} not found in sandbox {}",
                        selector, sandbox_id
                    ))
                })?
        } else if containers.len() == 1 {
            &containers[0]
        } else if containers.is_empty() {
            return Err(AppError::NotFound(format!(
                "sandbox {} does not expose any containers",
                sandbox_id
            )));
        } else {
            return Err(AppError::Conflict(format!(
                "sandbox {} has multiple containers; specify one via container query parameter",
                sandbox_id
            )));
        };

        if !selected.status.eq_ignore_ascii_case("running") {
            return Err(AppError::Conflict(format!(
                "container {} in sandbox {} is not running",
                selected.container_id, sandbox_id
            )));
        }

        Ok((selected.container_id.clone(), selected.name.clone()))
    }

    async fn send_terminal_event<T: Serialize>(
        socket: &mut ws::WebSocket,
        event: &T,
    ) -> Result<(), axum::Error> {
        let text = serde_json::to_string(event).unwrap_or_else(|_| {
            r#"{"type":"error","code":"encode_error","message":"failed to encode terminal event"}"#
                .to_string()
        });
        socket.send(Message::Text(text)).await
    }

    async fn close_with_error(socket: &mut ws::WebSocket, code: &str, message: String) {
        let _ = Self::send_terminal_event(
            socket,
            &TerminalServerEvent::Error {
                code,
                message: &message,
            },
        )
        .await;
        let _ = socket.send(Message::Close(None)).await;
    }

    fn terminal_auth_header() -> Option<String> {
        std::env::var("CUBESANDBOX_TERMINAL_ENVD_AUTH")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    fn parse_resize_message(text: &str) -> Option<(u32, u32)> {
        let value = serde_json::from_str::<serde_json::Value>(text).ok()?;
        if value.get("type")?.as_str()? != "resize" {
            return None;
        }
        let rows = value.get("rows").and_then(|v| v.as_u64()).unwrap_or(24);
        let cols = value.get("cols").and_then(|v| v.as_u64()).unwrap_or(80);
        Some((rows.clamp(1, 512) as u32, cols.clamp(1, 512) as u32))
    }

    fn parse_ping_message(text: &str) -> bool {
        serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .and_then(|value| {
                value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            })
            .is_some_and(|kind| kind == "ping")
    }

    fn terminal_error_code(error: &AppError) -> &'static str {
        match error {
            AppError::NotFound(_) => "sandbox_not_found",
            AppError::Unauthorized(_) => "unauthorized",
            AppError::Conflict(message) => {
                let message = message.as_str();
                if message.contains("has multiple containers") {
                    "container_selection_required"
                } else if message.contains("does not expose any containers") {
                    "sandbox_has_no_containers"
                } else if message.contains("not found in sandbox") {
                    "container_not_found"
                } else if message.contains("is not running") && message.contains("container ") {
                    "container_not_running"
                } else if message.contains("is not running") && message.contains("sandbox ") {
                    "sandbox_not_running"
                } else {
                    "sandbox_not_running"
                }
            }
            AppError::TooManyRequests(_) => "session_limit",
            AppError::BadRequest(_) => "bad_request",
            _ => "terminal_error",
        }
    }

    async fn hold_sandbox_for_terminal(
        &self,
        sandbox_id: &str,
        has_deadline: bool,
    ) -> Result<(), AppError> {
        if !has_deadline {
            return Ok(());
        }

        self.sandbox_service
            .refresh(sandbox_id, TERMINAL_KEEPALIVE_TIMEOUT_SECONDS)
            .await
    }

    async fn refresh_terminal_hold(
        sandbox_service: &SandboxService,
        sandbox_id: &str,
        session_id: &str,
    ) {
        if let Err(e) = sandbox_service
            .refresh(sandbox_id, TERMINAL_KEEPALIVE_INTERVAL.as_secs() as i32)
            .await
        {
            warn!(
                sandbox_id = %sandbox_id,
                session_id = %session_id,
                error = %e,
                "failed to refresh terminal sandbox hold"
            );
        }
    }

    fn is_control_message(text: &str) -> bool {
        if !text.starts_with('{') {
            return false;
        }
        serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .and_then(|value| {
                value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            })
            .is_some_and(|kind| matches!(kind.as_str(), "resize" | "ping"))
    }

    async fn handle_client_text(
        pty_handle: &PtyHandle,
        sandbox_id: &str,
        session_id: &str,
        text: &str,
    ) -> Result<(), String> {
        if let Some((rows, cols)) = Self::parse_resize_message(text) {
            info!(sandbox_id = %sandbox_id, session_id = %session_id, rows = rows, cols = cols, "terminal resize");
            if let Err(e) = pty_handle.resize(rows, cols).await {
                if e.contains("PTY process did not start in time")
                    || e.contains("PTY process has not started yet")
                {
                    warn!(sandbox_id = %sandbox_id, session_id = %session_id, error = %e, "terminal resize deferred until PTY is ready");
                } else {
                    warn!(sandbox_id = %sandbox_id, session_id = %session_id, error = %e, "terminal resize failed; keeping session alive");
                }
            } else {
                info!(sandbox_id = %sandbox_id, session_id = %session_id, rows = rows, cols = cols, "terminal resize applied");
            }
            return Ok(());
        }
        if Self::parse_ping_message(text) {
            return Ok(());
        }
        if Self::is_control_message(text) {
            return Ok(());
        }
        info!(sandbox_id = %sandbox_id, session_id = %session_id, input_len = text.len(), "terminal input");
        match pty_handle.send_stdin(text).await {
            Ok(()) => Ok(()),
            Err(e)
                if e.contains("PTY process did not start in time")
                    || e.contains("PTY process has not started yet") =>
            {
                warn!(sandbox_id = %sandbox_id, session_id = %session_id, error = %e, "terminal input deferred until PTY is ready");
                tokio::time::sleep(Duration::from_millis(200)).await;
                pty_handle.send_stdin(text).await
            }
            Err(e) => Err(e),
        }
    }

    async fn handle_client_binary(
        pty_handle: &PtyHandle,
        sandbox_id: &str,
        session_id: &str,
        data: &[u8],
    ) -> Result<(), String> {
        info!(sandbox_id = %sandbox_id, session_id = %session_id, input_len = data.len(), "terminal binary input");
        match pty_handle.send_stdin(&String::from_utf8_lossy(data)).await {
            Ok(()) => Ok(()),
            Err(e)
                if e.contains("PTY process did not start in time")
                    || e.contains("PTY process has not started yet") =>
            {
                warn!(sandbox_id = %sandbox_id, session_id = %session_id, error = %e, "terminal binary input deferred until PTY is ready");
                tokio::time::sleep(Duration::from_millis(200)).await;
                pty_handle.send_stdin(&String::from_utf8_lossy(data)).await
            }
            Err(e) => Err(e),
        }
    }

    fn server_event_message<T: Serialize>(event: &T) -> Message {
        Message::Text(serde_json::to_string(event).unwrap_or_else(|_| {
            r#"{"type":"error","code":"encode_error","message":"failed to encode terminal event"}"#
                .to_string()
        }))
    }

    async fn run_pty_session(
        mut socket: ws::WebSocket,
        mut send_rx: futures::channel::mpsc::Receiver<Message>,
        pty_handle: PtyHandle,
        sandbox_service: SandboxService,
        sessions: Arc<Mutex<HashMap<String, TerminalSession>>>,
        logger: ArcLogger,
        sandbox_id: String,
        session_id: String,
        operator_id: String,
        container_id: Option<String>,
        hold_enabled: bool,
    ) {
        let start = Instant::now();
        let mut last_activity = Instant::now();
        let mut hold_interval = tokio::time::interval(TERMINAL_KEEPALIVE_INTERVAL);

        let _ = socket
            .send(Self::server_event_message(&TerminalServerEvent::Ready {
                session_id: &session_id,
            }))
            .await;

        loop {
            let idle_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(
                last_activity + TERMINAL_IDLE_TIMEOUT,
            ));
            tokio::pin!(idle_sleep);

            tokio::select! {
                _ = &mut idle_sleep => {
                    Self::audit(&logger, LogLevel::Info, "timeout", &sandbox_id, Some(&session_id), &operator_id, container_id.as_deref(), Some("idle_timeout")).await;
                    let _ = socket
                        .send(Self::server_event_message(&TerminalServerEvent::Closed {
                            reason: "idle_timeout",
                            message: "Terminal session closed after idle timeout",
                        }))
                        .await;
                    break;
                }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(start + TERMINAL_MAX_LIFETIME)) => {
                    Self::audit(&logger, LogLevel::Info, "timeout", &sandbox_id, Some(&session_id), &operator_id, container_id.as_deref(), Some("max_lifetime")).await;
                    let _ = socket
                        .send(Self::server_event_message(&TerminalServerEvent::Closed {
                            reason: "max_lifetime",
                            message: "Terminal session reached maximum lifetime",
                        }))
                        .await;
                    break;
                }
                _ = hold_interval.tick() => {
                    if hold_enabled {
                        Self::refresh_terminal_hold(&sandbox_service, &sandbox_id, &session_id).await;
                    }
                    if socket.send(Message::Ping(Vec::new())).await.is_err() {
                        break;
                    }
                }
                msg = socket.recv() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            last_activity = Instant::now();
                            if let Err(e) = Self::handle_client_text(&pty_handle, &sandbox_id, &session_id, &text).await {
                                warn!(sandbox_id = %sandbox_id, session_id = %session_id, error = %e, "terminal input handling error");
                                Self::audit(&logger, LogLevel::Warn, "close", &sandbox_id, Some(&session_id), &operator_id, container_id.as_deref(), Some("pty_input_error")).await;
                                let _ = socket
                                    .send(Self::server_event_message(&TerminalServerEvent::Closed {
                                        reason: "pty_input_error",
                                        message: "Terminal input failed",
                                    }))
                                    .await;
                                break;
                            }
                        }
                        Some(Ok(Message::Binary(data))) => {
                            last_activity = Instant::now();
                            if let Err(e) = Self::handle_client_binary(&pty_handle, &sandbox_id, &session_id, &data).await {
                                warn!(sandbox_id = %sandbox_id, session_id = %session_id, error = %e, "terminal binary input handling error");
                                Self::audit(&logger, LogLevel::Warn, "close", &sandbox_id, Some(&session_id), &operator_id, container_id.as_deref(), Some("pty_input_error")).await;
                                let _ = socket
                                    .send(Self::server_event_message(&TerminalServerEvent::Closed {
                                        reason: "pty_input_error",
                                        message: "Terminal input failed",
                                    }))
                                    .await;
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = socket.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) => break,
                Some(Err(e)) => {
                    warn!(sandbox_id = %sandbox_id, session_id = %session_id, error = %e, "terminal websocket input error");
                    break;
                }
                        None => break,
                    }
                }
                msg = send_rx.next() => {
                    if let Some(msg) = msg {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        let _ = pty_handle.kill().await;
        let _ = socket.send(Message::Close(None)).await;
        if let Some(session) = Self::unregister_session(sessions, &session_id).await {
            info!(
                sandbox_id = %session.sandbox_id,
                session_id = %session_id,
                container_id = ?session.container_id,
                operator_id = %session.operator_id,
                started_at = %session.started_at,
                "terminal session unregistered"
            );
        }
        Self::audit(
            &logger,
            LogLevel::Info,
            "close",
            &sandbox_id,
            Some(&session_id),
            &operator_id,
            container_id.as_deref(),
            None,
        )
        .await;
        info!(sandbox_id = %sandbox_id, session_id = %session_id, "terminal connection closed");
    }

    pub async fn handle_terminal(
        &self,
        sandbox_id: String,
        container_selector: Option<String>,
        mut socket: ws::WebSocket,
        logger: ArcLogger,
        operator_id: String,
    ) -> Result<(), AppError> {
        info!(sandbox_id = %sandbox_id, "terminal connection started");

        let resolved = match self
            .validate_terminal_target(&sandbox_id, container_selector.as_deref())
            .await
        {
            Ok(resolved) => resolved,
            Err(e) => {
                let message = e.to_string();
                let code = Self::terminal_error_code(&e);
                warn!(sandbox_id = %sandbox_id, error = %message, "terminal target validation failed");
                Self::audit(
                    &logger,
                    LogLevel::Warn,
                    "target_validation_failed",
                    &sandbox_id,
                    None,
                    &operator_id,
                    None,
                    Some(code),
                )
                .await;
                Self::close_with_error(&mut socket, code, message).await;
                return Ok(());
            }
        };
        let access_token = resolved.access_token.unwrap_or_default();
        let selected_container_id = resolved.container_id.clone();
        let selected_container_name = resolved.container_name.clone();
        let hold_enabled = resolved.end_at.is_some();

        let session_id = Uuid::new_v4().to_string();
        if let Err(e) = self
            .register_session(
                session_id.clone(),
                sandbox_id.clone(),
                selected_container_id.clone(),
                operator_id.clone(),
            )
            .await
        {
            let message = e.to_string();
            let code = Self::terminal_error_code(&e);
            warn!(sandbox_id = %sandbox_id, session_id = %session_id, error = %message, "terminal session registration failed");
            Self::audit(
                &logger,
                LogLevel::Warn,
                "session_rejected",
                &sandbox_id,
                Some(&session_id),
                &operator_id,
                selected_container_id.as_deref(),
                Some(code),
            )
            .await;
            Self::close_with_error(&mut socket, code, message).await;
            return Ok(());
        }

        if let Err(e) = self
            .hold_sandbox_for_terminal(&sandbox_id, hold_enabled)
            .await
        {
            let message = e.to_string();
            warn!(sandbox_id = %sandbox_id, error = %message, "failed to hold sandbox for terminal");
            let _ = Self::unregister_session(self.sessions.clone(), &session_id).await;
            Self::audit(
                &logger,
                LogLevel::Warn,
                "hold_failed",
                &sandbox_id,
                Some(&session_id),
                &operator_id,
                selected_container_id.as_deref(),
                Some("sandbox_hold_failed"),
            )
            .await;
            Self::close_with_error(&mut socket, "sandbox_hold_failed", message).await;
            return Ok(());
        }

        if hold_enabled {
            info!(
                sandbox_id = %sandbox_id,
                session_id = %session_id,
                container_id = ?selected_container_id,
                container_name = ?selected_container_name,
                "sandbox terminal hold established"
            );
        } else {
            info!(
                sandbox_id = %sandbox_id,
                session_id = %session_id,
                container_id = ?selected_container_id,
                container_name = ?selected_container_name,
                "sandbox has no deadline; terminal hold skipped"
            );
        }
        Self::audit(
            &logger,
            LogLevel::Info,
            "open",
            &sandbox_id,
            Some(&session_id),
            &operator_id,
            selected_container_id.as_deref(),
            None,
        )
        .await;

        let proxy_base = std::env::var("AGENTHUB_SANDBOX_PROXY_URL")
            .unwrap_or_else(|_| "http://127.0.0.1".to_string());
        let url = format!(
            "{}/sandbox/{}/{}/process.Process/Start",
            proxy_base.trim_end_matches('/'),
            sandbox_id,
            ENVD_PORT
        );
        info!(sandbox_id = %sandbox_id, url = %url, "connecting to envd PTY via proxy");

        let sandbox_id_clone = sandbox_id.clone();
        let http_client = self.http_client.clone();
        let access_token = access_token.clone();
        let envd_auth = Self::terminal_auth_header();
        let sandbox_service = self.sandbox_service.clone();
        let sessions = self.sessions.clone();

        let (send_tx, send_rx) = futures::channel::mpsc::channel(100);
        let send_tx = Arc::new(Mutex::new(send_tx));

        tokio::spawn(async move {
            match Self::connect_envd_pty(
                &http_client,
                &url,
                &access_token,
                envd_auth.as_deref(),
                selected_container_id.as_deref(),
                send_tx.clone(),
                sandbox_id_clone.clone(),
            )
            .await
            {
                Ok(pty_handle) => {
                    Self::run_pty_session(
                        socket,
                        send_rx,
                        pty_handle,
                        sandbox_service,
                        sessions.clone(),
                        logger.clone(),
                        sandbox_id_clone.clone(),
                        session_id,
                        operator_id,
                        selected_container_id.clone(),
                        hold_enabled,
                    )
                    .await;
                }
                Err(e) => {
                    warn!(sandbox_id = %sandbox_id_clone, error = %e, "envd PTY connection failed");
                    let _ = Self::unregister_session(sessions.clone(), &session_id).await;
                    Self::audit(
                        &logger,
                        LogLevel::Warn,
                        "pty_connect_failed",
                        &sandbox_id_clone,
                        Some(&session_id),
                        &operator_id,
                        selected_container_id.as_deref(),
                        Some("pty_connect_failed"),
                    )
                    .await;
                    let _ = socket
                        .send(Self::server_event_message(&TerminalServerEvent::Error {
                            code: "pty_connect_failed",
                            message: &e,
                        }))
                        .await;
                    let _ = socket.send(Message::Close(None)).await;
                }
            }
        });

        Ok(())
    }

    async fn connect_envd_pty(
        http_client: &Client,
        url: &str,
        access_token: &str,
        envd_auth: Option<&str>,
        container_id: Option<&str>,
        sender: Arc<Mutex<futures::channel::mpsc::Sender<Message>>>,
        sandbox_id: String,
    ) -> Result<PtyHandle, String> {
        let payload = serde_json::json!({
            "process": {
                "cmd": "/bin/bash",
                "args": ["-i", "-l"],
                "envs": {
                    "TERM": "xterm-256color",
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                }
            },
            "pty": {"size": {"rows": 24, "cols": 80}}
        });
        let mut payload = payload;
        if let Some(container_id) = container_id {
            payload["process"]["container_id"] = serde_json::json!(container_id);
        }

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", CONNECT_CONTENT_TYPE.parse().unwrap());
        headers.insert(
            "Connect-Protocol-Version",
            CONNECT_PROTOCOL_VERSION.parse().unwrap(),
        );
        headers.insert("Connect-Content-Encoding", "identity".parse().unwrap());
        if !access_token.is_empty() {
            headers.insert("X-Access-Token", access_token.parse().unwrap());
        }
        if let Some(envd_auth) = envd_auth {
            headers.insert("Authorization", envd_auth.parse().unwrap());
        }

        let raw_body = serde_json::to_vec(&payload)
            .map_err(|e| format!("failed to encode Start request: {}", e))?;
        let body = Self::encode_connect_envelope(&raw_body);

        let resp = http_client
            .post(url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("HTTP error: {}", resp.status()));
        }

        let pid = Arc::new(AtomicU32::new(0));
        let pty_handle = PtyHandle::new(
            http_client.clone(),
            url,
            access_token.to_string(),
            envd_auth.map(ToOwned::to_owned),
            pid.clone(),
        );
        tokio::spawn(async move {
            Self::handle_envd_stream(resp, sender, pid, &sandbox_id).await;
        });

        Ok(pty_handle)
    }

    async fn handle_envd_stream(
        mut resp: Response,
        sender: Arc<Mutex<futures::channel::mpsc::Sender<Message>>>,
        pid: Arc<AtomicU32>,
        sandbox_id: &str,
    ) {
        let mut buffer = Vec::new();
        loop {
            match resp.chunk().await {
                Ok(Some(data)) => {
                    buffer.extend(data);
                    loop {
                        if buffer.len() < 5 {
                            break;
                        }
                        let flags = buffer[0];
                        let size = u32::from_be_bytes(buffer[1..5].try_into().unwrap()) as usize;
                        if buffer.len() < 5 + size {
                            break;
                        }
                        let raw = buffer[5..5 + size].to_vec();
                        buffer = buffer[5 + size..].to_vec();

                        if (flags & 0x01) != 0 {
                            warn!(sandbox_id = %sandbox_id, "compressed Connect stream messages are not supported");
                            continue;
                        }

                        if (flags & 0x02) != 0 {
                            if let Ok(end) = serde_json::from_slice::<serde_json::Value>(&raw) {
                                if let Some(error) = end.get("error") {
                                    warn!(sandbox_id = %sandbox_id, error = %error, "Connect stream ended with error");
                                } else {
                                    info!(sandbox_id = %sandbox_id, "Connect stream ended");
                                }
                            } else {
                                info!(sandbox_id = %sandbox_id, "Connect stream ended");
                            }
                            return;
                        }

                        if let Ok(message) = serde_json::from_slice::<serde_json::Value>(&raw) {
                            if let Some(event) = message.get("event") {
                                if let Some(start) = event.get("start") {
                                    if let Some(start_pid) =
                                        start.get("pid").and_then(|v| v.as_u64())
                                    {
                                        pid.store(start_pid as u32, Ordering::SeqCst);
                                        info!(sandbox_id = %sandbox_id, pid = start_pid, "terminal PTY started");
                                    }
                                }
                                if let Some(data_val) = event.get("data") {
                                    if let Some(pty) = data_val.get("pty") {
                                        if let Some(pty_data) = pty.as_str() {
                                            if let Ok(decoded) =
                                                base64::engine::general_purpose::STANDARD
                                                    .decode(pty_data)
                                            {
                                                if let Ok(text) = String::from_utf8(decoded) {
                                                    let _ = sender
                                                        .lock()
                                                        .await
                                                        .send(Self::server_event_message(
                                                            &TerminalServerEvent::Output {
                                                                data: &text,
                                                            },
                                                        ))
                                                        .await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(sandbox_id = %sandbox_id, error = %e, "envd stream error");
                    break;
                }
            }
        }
    }

    fn encode_connect_envelope(data: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(5 + data.len());
        result.push(0x00);
        result.extend_from_slice(&(data.len() as u32).to_be_bytes());
        result.extend_from_slice(data);
        result
    }
}

struct PtyHandle {
    http_client: Client,
    send_input_url: String,
    update_url: String,
    send_signal_url: String,
    access_token: String,
    envd_auth: Option<String>,
    pid: Arc<AtomicU32>,
}

impl PtyHandle {
    fn new(
        http_client: Client,
        url: &str,
        access_token: String,
        envd_auth: Option<String>,
        pid: Arc<AtomicU32>,
    ) -> Self {
        let send_input_url = url.replace("/process.Process/Start", "/process.Process/SendInput");
        let update_url = url.replace("/process.Process/Start", "/process.Process/Update");
        let send_signal_url = url.replace("/process.Process/Start", "/process.Process/SendSignal");
        Self {
            http_client,
            send_input_url,
            update_url,
            send_signal_url,
            access_token,
            envd_auth,
            pid,
        }
    }

    async fn send_stdin(&self, data: &str) -> Result<(), String> {
        let pid = self.wait_for_pid(TERMINAL_PTY_STARTUP_WAIT).await?;
        let payload = serde_json::json!({
            "process": {"pid": pid},
            "input": {"pty": base64::engine::general_purpose::STANDARD.encode(data.as_bytes())}
        });

        let headers = self.unary_headers();

        let resp = self
            .http_client
            .post(&self.send_input_url)
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("SendInput failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("SendInput error: {}", resp.status()));
        }

        Ok(())
    }

    async fn resize(&self, rows: u32, cols: u32) -> Result<(), String> {
        let pid = self.wait_for_pid(TERMINAL_PTY_STARTUP_WAIT).await?;
        tracing::info!(
            pid = pid,
            rows = rows,
            cols = cols,
            "sending terminal resize to envd"
        );
        let payload = serde_json::json!({
            "process": {"pid": pid},
            "pty": {"size": {"rows": rows, "cols": cols}}
        });

        let headers = self.unary_headers();

        let resp = self
            .http_client
            .post(&self.update_url)
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("Resize failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Resize error: {}", resp.status()));
        }

        tracing::info!(
            pid = pid,
            rows = rows,
            cols = cols,
            "terminal resize acknowledged by envd"
        );
        Ok(())
    }

    async fn wait_for_pid(&self, timeout: Duration) -> Result<u32, String> {
        let start = Instant::now();
        loop {
            let pid = self.pid.load(Ordering::SeqCst);
            if pid != 0 {
                return Ok(pid);
            }
            if start.elapsed() >= timeout {
                return Err("PTY process did not start in time".to_string());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn kill(&self) -> Result<(), String> {
        let Ok(pid) = self.pid() else {
            return Ok(());
        };
        let payload = serde_json::json!({
            "process": {"pid": pid},
            "signal": "SIGNAL_SIGKILL"
        });

        let headers = self.unary_headers();

        let resp = self
            .http_client
            .post(&self.send_signal_url)
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("SendSignal failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("SendSignal error: {}", resp.status()));
        }

        Ok(())
    }

    fn pid(&self) -> Result<u32, String> {
        match self.pid.load(Ordering::SeqCst) {
            0 => Err("PTY process has not started yet".to_string()),
            pid => Ok(pid),
        }
    }

    fn unary_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert(
            "Connect-Protocol-Version",
            CONNECT_PROTOCOL_VERSION.parse().unwrap(),
        );
        if let Some(envd_auth) = self.envd_auth.as_deref() {
            headers.insert("Authorization", envd_auth.parse().unwrap());
        }
        if !self.access_token.is_empty() {
            headers.insert("X-Access-Token", self.access_token.parse().unwrap());
        }
        headers
    }
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum TerminalServerEvent<'a> {
    #[serde(rename = "ready")]
    Ready { session_id: &'a str },
    #[serde(rename = "output")]
    Output { data: &'a str },
    #[serde(rename = "error")]
    Error { code: &'a str, message: &'a str },
    #[serde(rename = "closed")]
    Closed { reason: &'a str, message: &'a str },
}

#[cfg(test)]
mod tests {
    use super::TerminalService;
    use crate::{error::AppError, models::SandboxContainer};

    #[test]
    fn parses_resize_control_message() {
        assert_eq!(
            TerminalService::parse_resize_message(r#"{"type":"resize","rows":40,"cols":120}"#),
            Some((40, 120))
        );
    }

    #[test]
    fn clamps_resize_dimensions() {
        assert_eq!(
            TerminalService::parse_resize_message(r#"{"type":"resize","rows":0,"cols":9999}"#),
            Some((1, 512))
        );
    }

    #[test]
    fn ignores_non_resize_json_as_resize() {
        assert_eq!(
            TerminalService::parse_resize_message(r#"{"type":"input","data":"ls"}"#),
            None
        );
    }

    #[test]
    fn parses_ping_control_message() {
        assert!(TerminalService::parse_ping_message(r#"{"type":"ping"}"#));
        assert!(!TerminalService::parse_ping_message(r#"{"type":"resize"}"#));
    }

    #[test]
    fn resolve_terminal_container_prefers_single_container() {
        let containers = vec![SandboxContainer {
            name: "sandbox".to_string(),
            container_id: "sb-1".to_string(),
            status: "running".to_string(),
            image: "img".to_string(),
            kind: Some("sandbox".to_string()),
            primary: true,
        }];

        let (id, name) = TerminalService::resolve_terminal_container(&containers, None, "sb-1")
            .expect("single container should resolve");
        assert_eq!(id, "sb-1");
        assert_eq!(name, "sandbox");
    }

    #[test]
    fn resolve_terminal_container_requires_selector_for_multiple_containers() {
        let containers = vec![
            SandboxContainer {
                name: "sandbox".to_string(),
                container_id: "sb-1".to_string(),
                status: "running".to_string(),
                image: "img".to_string(),
                kind: Some("sandbox".to_string()),
                primary: true,
            },
            SandboxContainer {
                name: "workload".to_string(),
                container_id: "work-1".to_string(),
                status: "running".to_string(),
                image: "img2".to_string(),
                kind: Some("workload".to_string()),
                primary: false,
            },
        ];

        let err = TerminalService::resolve_terminal_container(&containers, None, "sb-1")
            .expect_err("multiple containers should require a selector");
        assert!(err.to_string().contains("multiple containers"));
    }

    #[test]
    fn terminal_error_code_distinguishes_container_conflicts() {
        assert_eq!(
            TerminalService::terminal_error_code(&AppError::Conflict(
                "sandbox sb-1 has multiple containers; specify one via container query parameter"
                    .to_string(),
            )),
            "container_selection_required"
        );
        assert_eq!(
            TerminalService::terminal_error_code(&AppError::Conflict(
                "sandbox sb-1 does not expose any containers".to_string(),
            )),
            "sandbox_has_no_containers"
        );
        assert_eq!(
            TerminalService::terminal_error_code(&AppError::Conflict(
                "container worker in sandbox sb-1 is not running".to_string(),
            )),
            "container_not_running"
        );
        assert_eq!(
            TerminalService::terminal_error_code(&AppError::Conflict(
                "sandbox sb-1 is not running".to_string(),
            )),
            "sandbox_not_running"
        );
        assert_eq!(
            TerminalService::terminal_error_code(&AppError::Conflict(
                "container worker not found in sandbox sb-1".to_string(),
            )),
            "container_not_found"
        );
    }

    #[test]
    fn resolve_terminal_container_accepts_selector_by_name_or_id() {
        let containers = vec![
            SandboxContainer {
                name: "sandbox".to_string(),
                container_id: "sb-1".to_string(),
                status: "running".to_string(),
                image: "img".to_string(),
                kind: Some("sandbox".to_string()),
                primary: true,
            },
            SandboxContainer {
                name: "workload".to_string(),
                container_id: "work-1".to_string(),
                status: "running".to_string(),
                image: "img2".to_string(),
                kind: Some("workload".to_string()),
                primary: false,
            },
        ];

        let (id, name) =
            TerminalService::resolve_terminal_container(&containers, Some("workload"), "sb-1")
                .expect("selector by name should resolve");
        assert_eq!(id, "work-1");
        assert_eq!(name, "workload");

        let (id, name) =
            TerminalService::resolve_terminal_container(&containers, Some("work-1"), "sb-1")
                .expect("selector by id should resolve");
        assert_eq!(id, "work-1");
        assert_eq!(name, "workload");
    }
}
