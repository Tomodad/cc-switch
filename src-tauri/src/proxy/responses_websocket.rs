//! Protocol-aware WebSocket transport for the OpenAI Responses API.
//!
//! The downstream Codex socket is terminated locally so CC-Switch can retain
//! provider selection, auth replacement, model mapping, ToolSearch shims, and
//! namespace restoration instead of bypassing them with a transparent tunnel.

use super::{
    forwarder::{
        apply_local_proxy_body_overrides, apply_local_proxy_header_overrides,
        prepare_upstream_request_body, ActiveConnectionGuard,
    },
    handler_context::RequestContext,
    providers::{
        codex_provider_supports_responses_websocket, should_inject_codex_tool_search_shim,
        should_restore_codex_native_tool_search, transform_codex_chat,
        transform_codex_responses_namespace, CodexAdapter, ProviderAdapter,
    },
    response_processor::log_codex_websocket_usage,
    server::ProxyState,
    ProxyError,
};
use crate::{app_config::AppType, provider::Provider, proxy::types::RectifierConfig};
use axum::{
    extract::{
        ws::{CloseFrame as DownstreamCloseFrame, Message as DownstreamMessage, WebSocket},
        State, WebSocketUpgrade,
    },
    http::{HeaderMap, HeaderName},
    response::Response,
};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{borrow::Cow, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::Instant;
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{
        client::IntoClientRequest,
        protocol::{frame::coding::CloseCode, CloseFrame as UpstreamCloseFrame, WebSocketConfig},
        Error as WebSocketError, Message as UpstreamMessage,
    },
    Connector, MaybeTlsStream, WebSocketStream,
};
use url::Url;

const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_WEBSOCKET_MESSAGE_SIZE: usize = 200 * 1024 * 1024;
const RESPONSES_ENDPOINT: &str = "/responses";

trait AsyncIo: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncIo for T {}
type BoxedIo = Box<dyn AsyncIo + Send + Unpin>;
type UpstreamSocket = WebSocketStream<MaybeTlsStream<BoxedIo>>;

#[derive(Default)]
struct TurnTransformState {
    namespace_restore_map:
        std::collections::HashMap<String, transform_codex_responses_namespace::NamespacedName>,
    restore_tool_search: bool,
    request_model: String,
    outbound_model: String,
    session_id: String,
}

struct WebSocketTurnAccounting {
    state: ProxyState,
    provider_id: String,
    provider_name: String,
    current_provider_id_at_start: String,
    used_half_open_permit: bool,
    _active_guard: Option<ActiveConnectionGuard>,
    finalized: bool,
}

impl WebSocketTurnAccounting {
    async fn begin(state: &ProxyState, ctx: &RequestContext) -> Result<Self, ProxyError> {
        let active_guard = ActiveConnectionGuard::acquire(state.status.clone()).await;
        {
            let mut status = state.status.write().await;
            status.total_requests = status.total_requests.saturating_add(1);
            status.last_request_at = Some(chrono::Utc::now().to_rfc3339());
            status.current_provider = Some(ctx.provider.name.clone());
            status.current_provider_id = Some(ctx.provider.id.clone());
        }

        let used_half_open_permit = if ctx.app_config.auto_failover_enabled {
            let permit = state
                .provider_router
                .allow_provider_request(&ctx.provider.id, ctx.app_type_str)
                .await;
            if !permit.allowed {
                record_websocket_status_failure(
                    state,
                    format!(
                        "provider {} rejected by the circuit breaker",
                        ctx.provider.name
                    ),
                )
                .await;
                drop(active_guard);
                return Err(ProxyError::NoAvailableProvider);
            }
            permit.used_half_open_permit
        } else {
            false
        };

        Ok(Self {
            state: state.clone(),
            provider_id: ctx.provider.id.clone(),
            provider_name: ctx.provider.name.clone(),
            current_provider_id_at_start: ctx.current_provider_id.clone(),
            used_half_open_permit,
            _active_guard: Some(active_guard),
            finalized: false,
        })
    }

    async fn finish_success(mut self) {
        self.finalized = true;
        if let Err(error) = self
            .state
            .provider_router
            .record_result(
                &self.provider_id,
                "codex",
                self.used_half_open_permit,
                true,
                None,
            )
            .await
        {
            log::warn!(
                "[CodexWS] Failed to record provider success (provider={}): {}",
                self.provider_id,
                error
            );
        }
        {
            let mut current_providers = self.state.current_providers.write().await;
            current_providers.insert(
                "codex".to_string(),
                (self.provider_id.clone(), self.provider_name.clone()),
            );
        }

        let should_switch = self.current_provider_id_at_start != self.provider_id;
        {
            let mut status = self.state.status.write().await;
            status.success_requests = status.success_requests.saturating_add(1);
            status.last_error = None;
            if should_switch {
                status.failover_count = status.failover_count.saturating_add(1);
            }
            update_proxy_success_rate(&mut status);
        }

        if should_switch {
            let failover_manager = self.state.failover_manager.clone();
            let app_handle = self.state.app_handle.clone();
            let provider_id = self.provider_id.clone();
            let provider_name = self.provider_name.clone();
            tokio::spawn(async move {
                if let Err(error) = failover_manager
                    .try_switch(app_handle.as_ref(), "codex", &provider_id, &provider_name)
                    .await
                {
                    log::warn!(
                        "[CodexWS] Failed to synchronize successful failover (provider={}): {}",
                        provider_id,
                        error
                    );
                }
            });
        }
    }

    async fn finish_provider_failure(mut self, message: String) {
        self.finalized = true;
        if let Err(error) = self
            .state
            .provider_router
            .record_result(
                &self.provider_id,
                "codex",
                self.used_half_open_permit,
                false,
                Some(message.clone()),
            )
            .await
        {
            log::warn!(
                "[CodexWS] Failed to record provider failure (provider={}): {}",
                self.provider_id,
                error
            );
        }
        record_websocket_status_failure(&self.state, message).await;
    }

    async fn finish_neutral_failure(mut self, message: String) {
        self.finalized = true;
        self.state
            .provider_router
            .release_permit_neutral(&self.provider_id, "codex", self.used_half_open_permit)
            .await;
        record_websocket_status_failure(&self.state, message).await;
    }
}

impl Drop for WebSocketTurnAccounting {
    fn drop(&mut self) {
        if self.finalized {
            return;
        }
        let state = self.state.clone();
        let provider_id = self.provider_id.clone();
        let used_half_open_permit = self.used_half_open_permit;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let message = "WebSocket turn ended before a terminal response".to_string();
                let _ = state
                    .provider_router
                    .record_result(
                        &provider_id,
                        "codex",
                        used_half_open_permit,
                        false,
                        Some(message.clone()),
                    )
                    .await;
                record_websocket_status_failure(&state, message).await;
            });
        }
    }
}

fn update_proxy_success_rate(status: &mut crate::proxy::types::ProxyStatus) {
    if status.total_requests > 0 {
        status.success_rate =
            (status.success_requests as f32 / status.total_requests as f32) * 100.0;
    }
}

async fn record_websocket_status_failure(state: &ProxyState, message: String) {
    let mut status = state.status.write().await;
    status.failed_requests = status.failed_requests.saturating_add(1);
    status.last_error = Some(message);
    update_proxy_success_rate(&mut status);
}

pub async fn handle_responses_websocket(
    State(state): State<ProxyState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade
        .max_message_size(MAX_WEBSOCKET_MESSAGE_SIZE)
        .max_frame_size(MAX_WEBSOCKET_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_connection(socket, state, headers))
}

async fn handle_connection(mut downstream: WebSocket, state: ProxyState, headers: HeaderMap) {
    let _connection_guard = state.track_websocket_connection();
    let result = handle_connection_inner(&mut downstream, &state, &headers).await;
    if let Err(error) = result {
        log::warn!("[CodexWS] Closing local Responses WebSocket: {error}");
        send_proxy_error_and_close(&mut downstream, &error).await;
    }
}

async fn handle_connection_inner(
    downstream: &mut WebSocket,
    state: &ProxyState,
    headers: &HeaderMap,
) -> Result<(), ProxyError> {
    let mut shutdown_rx = state.websocket_shutdown_tx.subscribe();
    if *shutdown_rx.borrow() {
        send_proxy_shutdown_close(downstream).await;
        return Ok(());
    }
    let Some(first_text) = receive_first_text_or_shutdown(downstream, &mut shutdown_rx).await?
    else {
        send_proxy_shutdown_close(downstream).await;
        return Ok(());
    };
    let first_event: Value = serde_json::from_str(&first_text)
        .map_err(|error| ProxyError::InvalidRequest(format!("invalid WebSocket JSON: {error}")))?;
    if first_event.get("type").and_then(Value::as_str) != Some("response.create") {
        return Err(ProxyError::InvalidRequest(
            "the first WebSocket event must be response.create".to_string(),
        ));
    }
    let response_body = response_create_body(&first_event)?;

    let ctx = RequestContext::new(
        state,
        &response_body,
        headers,
        AppType::Codex,
        "CodexWS",
        "codex",
    )
    .await?;
    let provider = ctx.provider.clone();
    if !codex_provider_supports_responses_websocket(&provider) {
        return Err(ProxyError::ConfigError(
            "selected Codex provider does not support native Responses WebSocket".to_string(),
        ));
    }

    let (first_text, turn_state) =
        transform_client_text(&first_text, &provider, &ctx.rectifier_config, true)?
            .expect("validated response.create");
    let mut turn_state = turn_state.expect("response.create transform state");
    turn_state.session_id = ctx.session_id.clone();
    let mut turn_accounting = Some(WebSocketTurnAccounting::begin(state, &ctx).await?);
    let mut response_in_flight = true;
    let mut turn_started = Instant::now();
    let mut first_token_ms = None;
    let mut received_response_event = false;
    let mut last_response_event_at = turn_started;
    let mut timeout_config = ctx.streaming_timeout_config();
    let request = build_upstream_request(&provider, headers)?;
    let mut upstream = tokio::time::timeout(
        UPSTREAM_CONNECT_TIMEOUT,
        connect_upstream_websocket(request),
    )
    .await
    .map_err(|_| ProxyError::Timeout("upstream WebSocket handshake timed out".to_string()))?
    .map_err(|error| {
        ProxyError::ForwardFailed(format!("upstream WebSocket handshake failed: {error}"))
    })?;

    upstream
        .send(UpstreamMessage::Text(first_text))
        .await
        .map_err(|error| {
            ProxyError::ForwardFailed(format!("failed to send initial WebSocket event: {error}"))
        })?;

    log::info!(
        "[CodexWS] Connected protocol-aware Responses WebSocket (provider={})",
        provider.id
    );

    loop {
        let timeout_deadline = if response_in_flight {
            let timeout_secs = if received_response_event {
                timeout_config.idle_timeout
            } else {
                timeout_config.first_byte_timeout
            };
            (timeout_secs > 0).then(|| {
                let anchor = if received_response_event {
                    last_response_event_at
                } else {
                    turn_started
                };
                anchor + Duration::from_secs(timeout_secs)
            })
        } else {
            None
        };

        tokio::select! {
            _ = async {
                if let Some(deadline) = timeout_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                let kind = if received_response_event { "idle" } else { "first response event" };
                return Err(ProxyError::Timeout(format!(
                    "upstream WebSocket {kind} timed out"
                )));
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    if let Some(accounting) = turn_accounting.take() {
                        accounting
                            .finish_neutral_failure("CC-Switch proxy stopping".to_string())
                            .await;
                    }
                    let _ = upstream.close(None).await;
                    send_proxy_shutdown_close(downstream).await;
                    break;
                }
            }
            downstream_message = downstream.recv() => {
                let Some(downstream_message) = downstream_message else {
                    if let Some(accounting) = turn_accounting.take() {
                        accounting
                            .finish_neutral_failure(
                                "downstream WebSocket ended before terminal response".to_string(),
                            )
                            .await;
                    }
                    let _ = upstream.close(None).await;
                    break;
                };
                let downstream_message = match downstream_message {
                    Ok(message) => message,
                    Err(error) => {
                        if let Some(accounting) = turn_accounting.take() {
                            accounting
                                .finish_neutral_failure(format!(
                                    "downstream WebSocket read failed: {error}"
                                ))
                                .await;
                        }
                        return Err(ProxyError::ForwardFailed(format!(
                            "downstream WebSocket read failed: {error}"
                        )));
                    }
                };

                match downstream_message {
                    DownstreamMessage::Text(text) => {
                        if response_in_flight && websocket_event_is(&text, "response.create") {
                            if let Some(accounting) = turn_accounting.take() {
                                accounting
                                    .finish_neutral_failure(
                                        "multiple response.create events were sent concurrently"
                                            .to_string(),
                                    )
                                    .await;
                            }
                            return Err(ProxyError::InvalidRequest(
                                "only one response.create may be in flight per WebSocket".to_string(),
                            ));
                        }
                        let (text, next_state) = if websocket_event_is(&text, "response.create") {
                            let body = response_create_body(
                                &serde_json::from_str::<Value>(&text).map_err(|error| {
                                    ProxyError::InvalidRequest(format!(
                                        "invalid WebSocket JSON: {error}"
                                    ))
                                })?,
                            )?;
                            let next_ctx = RequestContext::new(
                                state,
                                &body,
                                headers,
                                AppType::Codex,
                                "CodexWS",
                                "codex",
                            )
                            .await?;
                            let next_provider = next_ctx.provider.clone();
                            if provider_snapshot_changed(&provider, &next_provider) {
                                return Err(ProxyError::ConfigError(
                                    "selected Codex provider changed; reconnect WebSocket".to_string(),
                                ));
                            }
                            let (text, mut next_state) = transform_client_text(
                                &text,
                                &provider,
                                &next_ctx.rectifier_config,
                                false,
                            )?
                            .unwrap_or((text, None));
                            if let Some(state) = next_state.as_mut() {
                                state.session_id = next_ctx.session_id.clone();
                            }
                            timeout_config = next_ctx.streaming_timeout_config();
                            turn_accounting = Some(
                                WebSocketTurnAccounting::begin(state, &next_ctx).await?,
                            );
                            (text, next_state)
                        } else {
                            (text, None)
                        };
                        if let Some(next_state) = next_state {
                            turn_state = next_state;
                            response_in_flight = true;
                            turn_started = Instant::now();
                            first_token_ms = None;
                            received_response_event = false;
                            last_response_event_at = turn_started;
                        }
                        upstream.send(UpstreamMessage::Text(text)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("upstream WebSocket write failed: {error}"))
                        })?;
                    }
                    DownstreamMessage::Binary(data) => {
                        upstream.send(UpstreamMessage::Binary(data)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("upstream WebSocket write failed: {error}"))
                        })?;
                    }
                    DownstreamMessage::Ping(data) => {
                        upstream.send(UpstreamMessage::Ping(data)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("upstream WebSocket ping failed: {error}"))
                        })?;
                    }
                    DownstreamMessage::Pong(data) => {
                        upstream.send(UpstreamMessage::Pong(data)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("upstream WebSocket pong failed: {error}"))
                        })?;
                    }
                    DownstreamMessage::Close(frame) => {
                        if let Some(accounting) = turn_accounting.take() {
                            accounting
                                .finish_neutral_failure(
                                    "downstream WebSocket closed before terminal response".to_string(),
                                )
                                .await;
                        }
                        let frame = frame.map(|frame| UpstreamCloseFrame {
                            code: CloseCode::from(frame.code),
                            reason: Cow::Owned(frame.reason.into_owned()),
                        });
                        let _ = upstream.send(UpstreamMessage::Close(frame)).await;
                        break;
                    }
                }
            }
            upstream_message = upstream.next() => {
                let Some(upstream_message) = upstream_message else {
                    let _ = downstream.send(DownstreamMessage::Close(None)).await;
                    break;
                };
                let upstream_message = match upstream_message {
                    Ok(message) => message,
                    Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed) => {
                        let _ = downstream.send(DownstreamMessage::Close(None)).await;
                        break;
                    }
                    Err(error) => {
                        return Err(ProxyError::ForwardFailed(format!(
                            "upstream WebSocket read failed: {error}"
                        )));
                    }
                };

                match upstream_message {
                    UpstreamMessage::Text(text) => {
                        received_response_event = true;
                        last_response_event_at = Instant::now();
                        let terminal = websocket_event_is_terminal(&text);
                        if first_token_ms.is_none()
                            && (websocket_event_has_generated_output(&text) || terminal)
                        {
                            first_token_ms = Some(turn_started.elapsed().as_millis() as u64);
                        }
                        if terminal {
                            if let Ok(event) = serde_json::from_str::<Value>(&text) {
                                log_codex_websocket_usage(
                                    state,
                                    &provider.id,
                                    &turn_state.request_model,
                                    &turn_state.outbound_model,
                                    &event,
                                    turn_started.elapsed().as_millis() as u64,
                                    first_token_ms,
                                    &turn_state.session_id,
                                )
                                .await;
                            }
                            if let Some(accounting) = turn_accounting.take() {
                                if websocket_event_is_successful_terminal(&text) {
                                    accounting.finish_success().await;
                                } else {
                                    accounting
                                        .finish_provider_failure(format!(
                                            "upstream WebSocket terminal event: {}",
                                            websocket_event_type(&text)
                                                .as_deref()
                                                .unwrap_or("unknown")
                                        ))
                                        .await;
                                }
                            }
                            response_in_flight = false;
                        }
                        let text = restore_upstream_text(text, &turn_state);
                        downstream.send(DownstreamMessage::Text(text)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("downstream WebSocket write failed: {error}"))
                        })?;
                    }
                    UpstreamMessage::Binary(data) => {
                        received_response_event = true;
                        last_response_event_at = Instant::now();
                        if first_token_ms.is_none() {
                            first_token_ms = Some(turn_started.elapsed().as_millis() as u64);
                        }
                        downstream.send(DownstreamMessage::Binary(data)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("downstream WebSocket write failed: {error}"))
                        })?;
                    }
                    UpstreamMessage::Ping(data) => {
                        downstream.send(DownstreamMessage::Ping(data)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("downstream WebSocket ping failed: {error}"))
                        })?;
                    }
                    UpstreamMessage::Pong(data) => {
                        downstream.send(DownstreamMessage::Pong(data)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("downstream WebSocket pong failed: {error}"))
                        })?;
                    }
                    UpstreamMessage::Close(frame) => {
                        let frame = frame.map(|frame| DownstreamCloseFrame {
                            code: u16::from(frame.code),
                            reason: Cow::Owned(frame.reason.into_owned()),
                        });
                        let _ = downstream.send(DownstreamMessage::Close(frame)).await;
                        break;
                    }
                    UpstreamMessage::Frame(_) => {}
                }
            }
        }
    }

    Ok(())
}

async fn receive_first_text_or_shutdown(
    downstream: &mut WebSocket,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<String>, ProxyError> {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(None);
                }
            }
            message = receive_first_text(downstream) => return message.map(Some),
        }
    }
}

async fn send_proxy_shutdown_close(downstream: &mut WebSocket) {
    let _ = downstream
        .send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
            code: 1001,
            reason: Cow::Borrowed("CC-Switch proxy stopping"),
        })))
        .await;
}

async fn receive_first_text(downstream: &mut WebSocket) -> Result<String, ProxyError> {
    loop {
        let message = downstream
            .recv()
            .await
            .ok_or_else(|| {
                ProxyError::InvalidRequest(
                    "WebSocket closed before the first response.create event".to_string(),
                )
            })?
            .map_err(|error| {
                ProxyError::ForwardFailed(format!("downstream WebSocket read failed: {error}"))
            })?;
        match message {
            DownstreamMessage::Text(text) => return Ok(text),
            DownstreamMessage::Ping(data) => {
                downstream
                    .send(DownstreamMessage::Pong(data))
                    .await
                    .map_err(|error| {
                        ProxyError::ForwardFailed(format!(
                            "failed to answer downstream WebSocket ping: {error}"
                        ))
                    })?;
            }
            DownstreamMessage::Pong(_) => {}
            DownstreamMessage::Close(_) => {
                return Err(ProxyError::InvalidRequest(
                    "WebSocket closed before the first response.create event".to_string(),
                ));
            }
            DownstreamMessage::Binary(_) => {
                return Err(ProxyError::InvalidRequest(
                    "the first WebSocket event must be JSON text".to_string(),
                ));
            }
        }
    }
}

fn response_create_body(event: &Value) -> Result<Value, ProxyError> {
    let event_object = event.as_object().ok_or_else(|| {
        ProxyError::InvalidRequest("response.create payload must be an object".to_string())
    })?;

    if let Some(response) = event_object.get("response") {
        if response.is_object() {
            return Ok(response.clone());
        }
        return Err(ProxyError::InvalidRequest(
            "response.create.response must be an object".to_string(),
        ));
    }

    let mut body = event_object.clone();
    body.remove("type");
    Ok(Value::Object(body))
}
fn transform_client_text(
    text: &str,
    provider: &Provider,
    rectifier_config: &RectifierConfig,
    require_response_create: bool,
) -> Result<Option<(String, Option<TurnTransformState>)>, ProxyError> {
    let mut event: Value = match serde_json::from_str(text) {
        Ok(event) => event,
        Err(error) if require_response_create => {
            return Err(ProxyError::InvalidRequest(format!(
                "invalid WebSocket JSON: {error}"
            )));
        }
        Err(_) => return Ok(None),
    };

    if event.get("type").and_then(Value::as_str) != Some("response.create") {
        if require_response_create {
            return Err(ProxyError::InvalidRequest(
                "the first WebSocket event must be response.create".to_string(),
            ));
        }
        return Ok(None);
    }

    let original_body = response_create_body(&event)?;
    let request_model = original_body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();

    let namespace_restore_map =
        transform_codex_responses_namespace::namespace_restore_map(&original_body);
    let request_uses_tool_search_shim =
        transform_codex_chat::request_uses_responses_tool_search_shim(&original_body);
    let (body, _, _) = super::model_mapper::apply_model_mapping(original_body, provider);
    let mut body = super::model_mapper::strip_one_m_suffix_for_upstream_from_body(body);

    if rectifier_config.enabled && rectifier_config.request_media_fallback {
        let replaced = super::media_sanitizer::replace_images_for_text_only_model(
            &mut body,
            provider,
            rectifier_config.request_media_heuristic,
        );
        if replaced > 0 {
            log::info!(
                "[CodexWS] Replaced {replaced} image block(s) for text-only provider={}",
                provider.id
            );
        }
    }

    if should_inject_codex_tool_search_shim(provider, RESPONSES_ENDPOINT) {
        transform_codex_chat::ensure_responses_tool_search_shim(&mut body, true);
        transform_codex_responses_namespace::flatten_request_namespaces(&mut body)?;
    }

    let mut body = prepare_upstream_request_body(body);
    if let Some(overrides) = provider
        .meta
        .as_ref()
        .and_then(|meta| meta.local_proxy_request_overrides.as_ref())
    {
        if apply_local_proxy_body_overrides(&mut body, overrides) {
            body = prepare_upstream_request_body(body);
        }
    }
    let outbound_model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(&request_model)
        .to_string();
    let body_object = body.as_object_mut().ok_or_else(|| {
        ProxyError::InvalidRequest("response.create payload must be an object".to_string())
    })?;
    body_object.insert(
        "type".to_string(),
        Value::String("response.create".to_string()),
    );
    event = body;

    let restore_tool_search = request_uses_tool_search_shim
        && should_restore_codex_native_tool_search(provider, RESPONSES_ENDPOINT);
    let state = TurnTransformState {
        namespace_restore_map,
        restore_tool_search,
        request_model,
        outbound_model,
        session_id: String::new(),
    };
    let encoded = serde_json::to_string(&event).map_err(|error| {
        ProxyError::Internal(format!("failed to serialize WebSocket event: {error}"))
    })?;
    Ok(Some((encoded, Some(state))))
}

fn websocket_event_type(text: &str) -> Option<String> {
    serde_json::from_str::<Value>(text).ok().and_then(|event| {
        event
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_string)
    })
}

fn websocket_event_is(text: &str, expected: &str) -> bool {
    websocket_event_type(text).as_deref() == Some(expected)
}

fn websocket_event_has_generated_output(text: &str) -> bool {
    let Ok(event) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    let Some(event_type) = event.get("type").and_then(Value::as_str) else {
        return false;
    };
    if !event_type.starts_with("response.") || !event_type.ends_with(".delta") {
        return false;
    }
    match event.get("delta") {
        Some(Value::String(delta)) => !delta.is_empty(),
        Some(Value::Array(delta)) => !delta.is_empty(),
        Some(Value::Object(delta)) => !delta.is_empty(),
        Some(Value::Null) | None => false,
        Some(_) => true,
    }
}

fn websocket_event_is_terminal(text: &str) -> bool {
    websocket_event_type(text).is_some_and(|event_type| {
        matches!(
            event_type.as_str(),
            "response.completed" | "response.failed" | "response.incomplete" | "error"
        )
    })
}

fn websocket_event_is_successful_terminal(text: &str) -> bool {
    websocket_event_type(text).is_some_and(|event_type| {
        matches!(
            event_type.as_str(),
            "response.completed" | "response.incomplete"
        )
    })
}

fn provider_snapshot_changed(current: &Provider, next: &Provider) -> bool {
    if current.id != next.id || current.settings_config != next.settings_config {
        return true;
    }

    match (
        serde_json::to_value(&current.meta),
        serde_json::to_value(&next.meta),
    ) {
        (Ok(current_meta), Ok(next_meta)) => current_meta != next_meta,
        _ => true,
    }
}

fn restore_upstream_text(text: String, state: &TurnTransformState) -> String {
    if state.namespace_restore_map.is_empty() && !state.restore_tool_search {
        return text;
    }
    let Ok(mut event) = serde_json::from_str::<Value>(&text) else {
        return text;
    };
    if !transform_codex_responses_namespace::restore_response_tool_calls(
        &mut event,
        &state.namespace_restore_map,
        state.restore_tool_search,
    ) {
        return text;
    }
    serde_json::to_string(&event).unwrap_or(text)
}

fn build_upstream_request(
    provider: &Provider,
    downstream_headers: &HeaderMap,
) -> Result<http::Request<()>, ProxyError> {
    if !codex_provider_supports_responses_websocket(provider) {
        return Err(ProxyError::ConfigError(
            "provider does not support native Responses WebSocket".to_string(),
        ));
    }

    let adapter = CodexAdapter::new();
    let base_url = adapter.extract_base_url(provider)?;
    let is_full_url = provider
        .meta
        .as_ref()
        .and_then(|meta| meta.is_full_url)
        .unwrap_or(false);
    let http_url = if is_full_url || url_is_responses_endpoint(&base_url) {
        base_url
    } else {
        adapter.build_url(&base_url, RESPONSES_ENDPOINT)
    };
    let ws_url = websocket_url(&http_url)?;
    let mut request = ws_url.as_str().into_client_request().map_err(|error| {
        ProxyError::ConfigError(format!("invalid upstream WebSocket URL: {error}"))
    })?;

    for name in [
        "user-agent",
        "openai-beta",
        "openai-organization",
        "openai-project",
        "x-client-request-id",
        "x-codex-window-id",
        "session-id",
        "session_id",
        "thread-id",
        "originator",
        "x-codex-turn-metadata",
        "x-codex-beta-features",
        "accept-language",
    ] {
        let name = HeaderName::from_static(name);
        for value in downstream_headers.get_all(&name) {
            request.headers_mut().append(name.clone(), value.clone());
        }
    }

    if let Some(user_agent) = provider
        .meta
        .as_ref()
        .and_then(|meta| meta.custom_user_agent_header().ok().flatten())
    {
        request.headers_mut().insert("user-agent", user_agent);
    }

    if let Some(auth) = adapter.extract_auth(provider) {
        for (name, value) in adapter.get_auth_headers(&auth)? {
            request.headers_mut().insert(name, value);
        }
    }

    apply_local_proxy_header_overrides(
        request.headers_mut(),
        provider
            .meta
            .as_ref()
            .and_then(|meta| meta.local_proxy_request_overrides.as_ref()),
        false,
    );

    Ok(request)
}

async fn connect_upstream_websocket(
    request: http::Request<()>,
) -> Result<UpstreamSocket, ProxyError> {
    let target_url = Url::parse(&request.uri().to_string()).map_err(|error| {
        ProxyError::ConfigError(format!("invalid upstream WebSocket URL: {error}"))
    })?;
    let target_host = target_url
        .host_str()
        .ok_or_else(|| ProxyError::ConfigError("upstream WebSocket URL has no host".to_string()))?;
    let target_port = target_url
        .port_or_known_default()
        .ok_or_else(|| ProxyError::ConfigError("upstream WebSocket URL has no port".to_string()))?;

    let stream: BoxedIo = match super::http_client::get_current_proxy_url() {
        Some(proxy_url) => {
            let parsed = Url::parse(&proxy_url).map_err(|error| {
                ProxyError::ConfigError(format!("invalid configured proxy URL: {error}"))
            })?;
            match parsed.scheme() {
                "http" | "https" => Box::new(
                    super::hyper_client::connect_via_proxy(&proxy_url, target_host, target_port)
                        .await?,
                ),
                "socks5" | "socks5h" => {
                    Box::new(connect_via_socks5(&parsed, target_host, target_port).await?)
                }
                scheme => {
                    return Err(ProxyError::ConfigError(format!(
                        "unsupported configured proxy scheme for WebSocket: {scheme}"
                    )))
                }
            }
        }
        None => Box::new(
            tokio::net::TcpStream::connect((target_host, target_port))
                .await
                .map_err(|error| {
                    ProxyError::ForwardFailed(format!(
                        "upstream WebSocket TCP connect failed: {error}"
                    ))
                })?,
        ),
    };

    let websocket_config = WebSocketConfig {
        max_message_size: Some(MAX_WEBSOCKET_MESSAGE_SIZE),
        max_frame_size: Some(MAX_WEBSOCKET_MESSAGE_SIZE),
        ..Default::default()
    };
    let connector = (target_url.scheme() == "wss")
        .then(|| Connector::Rustls(super::hyper_client::build_tls_client_config()));
    let (socket, _) =
        client_async_tls_with_config(request, stream, Some(websocket_config), connector)
            .await
            .map_err(|error| {
                ProxyError::ForwardFailed(format!("WebSocket protocol handshake failed: {error}"))
            })?;
    Ok(socket)
}

async fn connect_via_socks5(
    proxy_url: &Url,
    target_host: &str,
    target_port: u16,
) -> Result<tokio::net::TcpStream, ProxyError> {
    let proxy_host = proxy_url
        .host_str()
        .ok_or_else(|| ProxyError::ConfigError("SOCKS proxy URL has no host".to_string()))?;
    let proxy_port = proxy_url.port().unwrap_or(1080);
    let mut stream = tokio::net::TcpStream::connect((proxy_host, proxy_port))
        .await
        .map_err(|error| {
            ProxyError::ForwardFailed(format!("SOCKS proxy TCP connect failed: {error}"))
        })?;

    let has_auth = !proxy_url.username().is_empty();
    let methods: &[u8] = if has_auth { &[0x00, 0x02] } else { &[0x00] };
    let mut greeting = vec![0x05, methods.len() as u8];
    greeting.extend_from_slice(methods);
    stream
        .write_all(&greeting)
        .await
        .map_err(|error| ProxyError::ForwardFailed(format!("SOCKS greeting failed: {error}")))?;
    let mut selection = [0u8; 2];
    stream
        .read_exact(&mut selection)
        .await
        .map_err(|error| ProxyError::ForwardFailed(format!("SOCKS method read failed: {error}")))?;
    if selection[0] != 0x05 || selection[1] == 0xff {
        return Err(ProxyError::AuthError(
            "SOCKS proxy rejected authentication methods".to_string(),
        ));
    }
    if selection[1] == 0x02 {
        let username = proxy_url.username().as_bytes();
        let password = proxy_url.password().unwrap_or("").as_bytes();
        if username.len() > u8::MAX as usize || password.len() > u8::MAX as usize {
            return Err(ProxyError::ConfigError(
                "SOCKS proxy credentials are too long".to_string(),
            ));
        }
        let mut auth = vec![0x01, username.len() as u8];
        auth.extend_from_slice(username);
        auth.push(password.len() as u8);
        auth.extend_from_slice(password);
        stream.write_all(&auth).await.map_err(|error| {
            ProxyError::ForwardFailed(format!("SOCKS authentication write failed: {error}"))
        })?;
        let mut auth_result = [0u8; 2];
        stream.read_exact(&mut auth_result).await.map_err(|error| {
            ProxyError::ForwardFailed(format!("SOCKS authentication read failed: {error}"))
        })?;
        if auth_result != [0x01, 0x00] {
            return Err(ProxyError::AuthError(
                "SOCKS proxy authentication failed".to_string(),
            ));
        }
    } else if selection[1] != 0x00 {
        return Err(ProxyError::AuthError(format!(
            "SOCKS proxy selected unsupported authentication method {}",
            selection[1]
        )));
    }

    let mut request = vec![0x05, 0x01, 0x00];
    if proxy_url.scheme() == "socks5h" {
        let host = target_host.as_bytes();
        if host.len() > u8::MAX as usize {
            return Err(ProxyError::ConfigError(
                "WebSocket target hostname is too long for SOCKS5".to_string(),
            ));
        }
        request.push(0x03);
        request.push(host.len() as u8);
        request.extend_from_slice(host);
    } else {
        let address = if let Ok(ip) = target_host.parse::<std::net::IpAddr>() {
            ip
        } else {
            tokio::net::lookup_host((target_host, target_port))
                .await
                .map_err(|error| {
                    ProxyError::ForwardFailed(format!("SOCKS target DNS lookup failed: {error}"))
                })?
                .next()
                .ok_or_else(|| {
                    ProxyError::ForwardFailed(
                        "SOCKS target DNS lookup returned no address".to_string(),
                    )
                })?
                .ip()
        };
        match address {
            std::net::IpAddr::V4(ip) => {
                request.push(0x01);
                request.extend_from_slice(&ip.octets());
            }
            std::net::IpAddr::V6(ip) => {
                request.push(0x04);
                request.extend_from_slice(&ip.octets());
            }
        }
    }
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await.map_err(|error| {
        ProxyError::ForwardFailed(format!("SOCKS CONNECT write failed: {error}"))
    })?;

    let mut reply = [0u8; 4];
    stream.read_exact(&mut reply).await.map_err(|error| {
        ProxyError::ForwardFailed(format!("SOCKS CONNECT read failed: {error}"))
    })?;
    if reply[0] != 0x05 || reply[1] != 0x00 {
        return Err(ProxyError::ForwardFailed(format!(
            "SOCKS CONNECT rejected with code {}",
            reply[1]
        )));
    }
    let address_len = match reply[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await.map_err(|error| {
                ProxyError::ForwardFailed(format!("SOCKS bound domain read failed: {error}"))
            })?;
            len[0] as usize
        }
        atyp => {
            return Err(ProxyError::ForwardFailed(format!(
                "SOCKS CONNECT returned unsupported address type {atyp}"
            )))
        }
    };
    let mut bound = vec![0u8; address_len + 2];
    stream.read_exact(&mut bound).await.map_err(|error| {
        ProxyError::ForwardFailed(format!("SOCKS bound address read failed: {error}"))
    })?;
    Ok(stream)
}

fn url_is_responses_endpoint(url: &str) -> bool {
    Url::parse(url)
        .ok()
        .is_some_and(|url| url.path().trim_end_matches('/').ends_with("/responses"))
}

fn websocket_url(http_url: &str) -> Result<Url, ProxyError> {
    let mut url = Url::parse(http_url)
        .map_err(|error| ProxyError::ConfigError(format!("invalid upstream URL: {error}")))?;
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" | "wss" => return Ok(url),
        scheme => {
            return Err(ProxyError::ConfigError(format!(
                "unsupported upstream WebSocket scheme: {scheme}"
            )));
        }
    };
    url.set_scheme(scheme).map_err(|_| {
        ProxyError::ConfigError("failed to convert upstream URL to WebSocket".to_string())
    })?;
    Ok(url)
}

async fn send_proxy_error_and_close(downstream: &mut WebSocket, error: &ProxyError) {
    let event = json!({
        "type": "error",
        "error": {
            "type": "proxy_error",
            "message": error.to_string(),
        }
    });
    let _ = downstream
        .send(DownstreamMessage::Text(event.to_string()))
        .await;
    let _ = downstream
        .send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
            code: 1011,
            reason: Cow::Borrowed("CC-Switch upstream WebSocket failure"),
        })))
        .await;
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        database::Database,
        proxy::{server::ProxyServer, types::ProxyConfig},
    };
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use rustls::{
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
        ServerConfig,
    };
    use serial_test::serial;
    use std::{env, ffi::OsString, fs, sync::Arc};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::oneshot,
    };
    use tokio_rustls::TlsAcceptor;
    use tokio_tungstenite::{
        accept_async_with_config, accept_hdr_async, connect_async, connect_async_with_config,
    };

    const TEST_CA_DER_BASE64: &str = "MIIDETCCAfmgAwIBAgIJAJ6Bgah1Zn3AMA0GCSqGSIb3DQEBCwUAMCYxJDAiBgNVBAMTG0NDLVN3aXRjaCBXZWJTb2NrZXQgVGVzdCBDQTAeFw0yMDAxMDEwMDAwMDBaFw00NTAxMDEwMDAwMDBaMCYxJDAiBgNVBAMTG0NDLVN3aXRjaCBXZWJTb2NrZXQgVGVzdCBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKp6fTz+YiwlBeJhhAycVaJ7FIgACUMW/pHAst3ktdxaSC3uC5axDxLlTBDrVXLcVMUmvSrLbDNxLRY9gfPyTmb10SJufklHXZfC7w3qTL8C+ah7QXEbpMq08KozwQxSQFm21Zm4jHeUgGwVsYwQaAUVfT6ntAUzuOPWPZiLifDwwDKBwwUOC/E1Oq1h0en9RwNq1UK/z+LKIPs7p5SgNQl+/9RwaBvtLqXiMhpXaIsntUsKqVbzZxqJqfDKdKJE5Qbl3+ZWbjFLRVJ+SgNUiViLcyS8uJpEr4ziU7c8dJQqv0pWy9lpWf4z+zMj/2NB/ixoeGq6KN2od8n4Z/zZWOECAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMCAQYwHQYDVR0OBBYEFAN2yJ1qTsYtvXbVRms3JkF5V5d0MA0GCSqGSIb3DQEBCwUAA4IBAQCTtGuy8Yfj2HVHVeLy6GxkqOCPQh0G/JZudJsGApOp275w1iXur6ObzDPfpmaalqCkBoGwKJbWOFyW1bRoVhSlUPgCeEgjTzpOEoJgYOurv1iJEVtKSI2r77fTiDA4WNiULeRF9tn+pL+owp9clJ8/+3Pvv4BWEDnOqyS+St/SkzW8oOfKwDUwMOy2GaYxh5+98vaPyzE7gZYr7z1VxKhz59WMh0PpRWx6Hua2KObzeASvhNpC2J1IedSXoYpD1tDv9F/ERSSeofuIM8vM3hkH/Ul9KXczW6iSG5Nm5TAWO4wPCvECK1fIqMUgiLCZOPnRl2YE+GNYkS3fFqOdcK3v";
    const TEST_SERVER_CERT_DER_BASE64: &str = "MIIDGDCCAgCgAwIBAgIQPo6+OoGWp9S9hXSlWuTm8jANBgkqhkiG9w0BAQsFADAmMSQwIgYDVQQDExtDQy1Td2l0Y2ggV2ViU29ja2V0IFRlc3QgQ0EwHhcNMjAwMTAxMDAwMDAwWhcNNDUwMTAxMDAwMDAwWjAUMRIwEAYDVQQDEwlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCtC+fZvyVVjsyfMurnGtdwVuzgVJApe8nd/w0E3u5E1YaZLAhMZz7FzPmqhK5E6R04yAbQFWFAMkYvJl99KAMKyRkUTw6/ojWCaB76z0MDfi62CBkRiK/Zq8+5oncen79jN6J5TAzB9fhqCPD/R6UHcvT7xZCRvWLXrAhXRu0QRg8bMcpf8Q3IiPnNvtcIdcrIE8WVrGSaRkfiOxxaKS9BRm7+ehx9nweRe5xrcOI9vSDLI/L/sGafT+Tf/pWzGwz9uiC2fqQKw860faox0Oq6Qhj5NEr1TEunaA0GEjDWJwxKWYuUFrGeruo21JP8J2G6DjsH8CErYazu/3v4tQeJAgMBAAGjVDBSMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMBYGA1UdJQEB/wQMMAoGCCsGAQUFBwMBMBoGA1UdEQQTMBGCCWxvY2FsaG9zdIcEfwAAATANBgkqhkiG9w0BAQsFAAOCAQEAYhw4NjXQu+422MT7kH88ezNKWBUwFSWlwTHcN+nO/qWbLu8VqIQmpB/HJqjMOiSJ1dDZfVbRxvJvrr/j4iCbjtP9kdmsDvj4ISyYVUPjDlZT5vBgB774BOlaI8YHtz+xxB2lslYFhwqbqF35tYUfeVWx/c5+OQEzSOPeDP1zHNw71LK8py/af3w5qEKk95Jz6SqgSX8KFdZ4V42iwLgB4A++IOkMPxjpd4WpwWZThcPbGIyzxm1FI4pOTGLxE9SrG9gglOmHZ9QW+PbdZQQEfW3hMx+GkI3W76HrjgCualJhWP4IibxaAmAJhrto2gVE0rrwa6HevPfArwsGX3AfyQ==";
    const TEST_SERVER_KEY_DER_BASE64: &str = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCtC+fZvyVVjsyfMurnGtdwVuzgVJApe8nd/w0E3u5E1YaZLAhMZz7FzPmqhK5E6R04yAbQFWFAMkYvJl99KAMKyRkUTw6/ojWCaB76z0MDfi62CBkRiK/Zq8+5oncen79jN6J5TAzB9fhqCPD/R6UHcvT7xZCRvWLXrAhXRu0QRg8bMcpf8Q3IiPnNvtcIdcrIE8WVrGSaRkfiOxxaKS9BRm7+ehx9nweRe5xrcOI9vSDLI/L/sGafT+Tf/pWzGwz9uiC2fqQKw860faox0Oq6Qhj5NEr1TEunaA0GEjDWJwxKWYuUFrGeruo21JP8J2G6DjsH8CErYazu/3v4tQeJAgMBAAECggEATSCTU+/oGfwto380R5ElGMMFjO7j2jl8Pd/h05vxIujwtvBzOmqCBfNYC/JbIgesqJQuxSviTpSZx4YY5VWiFXqQHQcnka4gn2D8/djHC5WACE4Prkr35dK4IQsSgKm+yeAQIHQO85xH/irCD2XFXk6UdmsWBn8cwPfCN/Q60RdMtBW02NHtIIsnZfAFvWQhM2RQLhp5nZtj4OmXY8h1kfWl8qupS9iTKGn/rRaU8KrRmrQrBT87W6BMIzJ7PX1v7WvuW4tbHi4xqq76Bav71CnM2QN8AU8zpy4v23WJZeCXEFRELLrv08MSQNg8SGOtus87abBGfEvfcTx8pxi26QKBgQDhJgHR4/JnjHIwZmnprlGHzh8LJm5E382ASTB8nBkFGNuSzeHfjXokgCy7nACXyd0bUhydMBCBSQYLqOOyfieBB4deOuTvLfxi9D5+w0nzSIpF3hV8AEvCvUy6t7VTfrpDqt0IyOf1qbZMhsTGlbJ6CZnlXzEo3PE4S0UhwHvHewKBgQDEwjYvXFT59ojTdI2ZSrNN74xnZkwqoVhSZr12Rq36JPQkYsgOLVGcOjD94VCv6aZDcHWXG97IKZyr6L+UPgrmQnuA+/9P2ekPFscuwaVmZY6E7D81j3fJ3uzT18ocU1vF5ec36/xV6zwv7TanD3tauQ1oJLVc6dvsb3WXEFW7ywKBgQCCACES4SxpL8YLPkcvX7DB2nlARetrp1IQHbJ6cONddxHpfSlLnHQHOV8a4KPTAQLDMLFG7abKD7EG8Hiw6njC3ucBuL3RgNr3BBJFvVsotxzn5KjBFaapBgaU1VhEoqrIQZMo7GBLD7gsDbD2/R61qm+K6mEHODOsDoIXT/3omwKBgGfwICeMoucYsNbjLxnXODjnXkgQ5hNu//UniNY+KBGIC+Bcvkme7wmUQ+UZbUJALzBY7AVTF7CtKrI1VV6+F4vjetJ8TDamalMqOTYd3X3mEA9vrURh8WmWdYzC5WVpM4WrGSWVZ8sLZNP8f25o40TdlJN7MMNQVnjjuD6AxolZAoGAcy2REEDgXq39ykhMAZWWJVh1Q8gWHfkZFP+sz7U0F2nvMGEuTcLNqzQC4VlUK4QLo6O88VPPP/dKBq3Ry1C80LCqXSqBvd//jToiHMGwehCFEHV8St7+ka8rR35a8UQvxoR868Kuz0ODRwZ3gklDJY33JUaI3zCKb/S1wFnV/s0=";

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = env::var_os(key);
            env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => env::set_var(self.key, value),
                None => env::remove_var(self.key),
            }
        }
    }

    fn websocket_provider(base_url: String) -> Provider {
        websocket_provider_with_id("ws-provider", base_url)
    }

    fn websocket_provider_with_id(id: &str, base_url: String) -> Provider {
        let mut provider = Provider::with_id(
            id.to_string(),
            format!("WebSocket Provider {id}"),
            json!({
                "base_url": base_url,
                "supports_websockets": true,
                "env": {
                    "OPENAI_API_KEY": "provider-secret",
                    "ANTHROPIC_MODEL": "upstream-model"
                }
            }),
            None,
        );
        provider.category = Some("custom".to_string());
        provider
    }

    fn response_create(model: &str, previous_response_id: Option<&str>) -> Value {
        let mut response = json!({
            "type": "response.create",
            "model": model,
            "_private": "do-not-forward",
            "input": [{"role": "user", "content": "hello"}],
            "tools": [
                {"type": "tool_search"},
                {
                    "type": "namespace",
                    "name": "demo",
                    "tools": [{
                        "type": "function",
                        "name": "run",
                        "description": "run a demo",
                        "parameters": {"type": "object", "properties": {}}
                    }]
                }
            ]
        });
        if let Some(previous_response_id) = previous_response_id {
            response["previous_response_id"] = json!(previous_response_id);
        }
        response
    }

    async fn next_text<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> String
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(30), socket.next())
                .await
                .expect("timed out waiting for websocket message")
                .expect("websocket closed")
                .expect("websocket message error");
            match message {
                UpstreamMessage::Text(text) => return text,
                UpstreamMessage::Ping(_) | UpstreamMessage::Pong(_) => continue,
                other => panic!("expected text websocket message, got {other:?}"),
            }
        }
    }

    #[test]
    fn transforms_flat_codex_response_create_for_upstream() {
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        let input = json!({
            "type": "response.create",
            "model": "local-model",
            "_private": "do-not-forward",
            "input": [{"role": "user", "content": "hello"}]
        })
        .to_string();

        let (encoded, state) =
            transform_client_text(&input, &provider, &RectifierConfig::default(), true)
                .expect("transform")
                .expect("response.create");
        assert!(state.is_some());

        let event: Value = serde_json::from_str(&encoded).expect("transformed event JSON");
        assert_eq!(event["type"], "response.create");
        assert_eq!(event["model"], "upstream-model");
        assert!(event.get("_private").is_none());
        assert!(event.get("response").is_none());
    }

    #[test]
    fn strips_local_one_m_marker_after_websocket_model_mapping() {
        let mut provider = websocket_provider("http://127.0.0.1:1".to_string());
        provider.settings_config["env"]["ANTHROPIC_MODEL"] =
            Value::String("upstream-model [1M]".to_string());
        let input = json!({
            "type": "response.create",
            "model": "local-model",
            "input": [{"role": "user", "content": "hello"}]
        })
        .to_string();

        let (encoded, _) =
            transform_client_text(&input, &provider, &RectifierConfig::default(), true)
                .expect("transform")
                .expect("response.create");

        let event: Value = serde_json::from_str(&encoded).expect("transformed event JSON");
        assert_eq!(event["model"], "upstream-model");
    }

    #[test]
    fn transforms_response_create_and_restores_native_events() {
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        let input = response_create("local-model", None).to_string();
        let (encoded, state) =
            transform_client_text(&input, &provider, &RectifierConfig::default(), true)
                .expect("transform")
                .expect("response.create");
        let state = state.expect("turn state");
        let event: Value = serde_json::from_str(&encoded).unwrap();
        let response = &event;

        assert_eq!(response["model"], "upstream-model");
        assert!(response.get("_private").is_none());
        let tools = response["tools"].as_array().unwrap();
        assert!(tools.iter().any(|tool| {
            tool.get("type").and_then(Value::as_str) == Some("function")
                && tool.get("name").and_then(Value::as_str) == Some("tool_search")
        }));
        assert!(tools.iter().any(|tool| {
            tool.get("type").and_then(Value::as_str) == Some("function")
                && tool.get("name").and_then(Value::as_str) == Some("demo__run")
        }));
        assert!(!tools
            .iter()
            .any(|tool| tool.get("type").and_then(Value::as_str) == Some("namespace")));

        let restored = restore_upstream_text(
            json!({
                "type": "response.output_item.added",
                "item": {
                    "type": "function_call",
                    "name": "demo__run",
                    "call_id": "call-1",
                    "arguments": "{}"
                }
            })
            .to_string(),
            &state,
        );
        let restored: Value = serde_json::from_str(&restored).unwrap();
        assert_eq!(restored["item"]["name"], "run");
        assert_eq!(restored["item"]["namespace"], "demo");

        let full_endpoint =
            websocket_provider("http://127.0.0.1:1/custom/v1/responses".to_string());
        let request = build_upstream_request(&full_endpoint, &HeaderMap::new()).unwrap();
        assert_eq!(request.uri().path(), "/custom/v1/responses");
    }

    #[test]
    fn forwards_current_codex_websocket_session_and_feature_headers() {
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("session-id", "session-123"),
            ("thread-id", "thread-456"),
            ("originator", "codex_desktop_rs"),
            ("x-codex-turn-metadata", "turn-metadata"),
            ("x-codex-beta-features", "responses_websockets"),
        ] {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }

        let request = build_upstream_request(&provider, &headers).unwrap();

        for (name, expected) in [
            ("session-id", "session-123"),
            ("thread-id", "thread-456"),
            ("originator", "codex_desktop_rs"),
            ("x-codex-turn-metadata", "turn-metadata"),
            ("x-codex-beta-features", "responses_websockets"),
        ] {
            assert_eq!(
                request
                    .headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok()),
                Some(expected),
                "missing native Codex header {name}"
            );
        }
    }

    #[tokio::test]
    #[serial]
    #[allow(clippy::result_large_err)]
    async fn proxies_native_responses_websocket_with_auth_transforms_and_close() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (headers_tx, headers_rx) = oneshot::channel();
        let (events_tx, events_rx) = oneshot::channel();

        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut headers_tx = Some(headers_tx);
            let mut websocket =
                accept_hdr_async(stream, move |request: &http::Request<()>, response| {
                    let capture = (
                        request.uri().path().to_string(),
                        request
                            .headers()
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string),
                        request
                            .headers()
                            .get("openai-beta")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string),
                    );
                    if let Some(tx) = headers_tx.take() {
                        let _ = tx.send(capture);
                    }
                    Ok(response)
                })
                .await
                .expect("accept websocket handshake");

            let first = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-1",
                            "output": [{
                                "type": "function_call",
                                "name": "demo__run",
                                "call_id": "call-1",
                                "arguments": "{}"
                            }],
                            "model": "upstream-model",
                            "usage": {"input_tokens": 12, "output_tokens": 3}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send first upstream event");

            let second = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-2",
                            "output": [{
                                "type": "function_call",
                                "name": "tool_search",
                                "call_id": "search-1",
                                "status": "completed",
                                "arguments": "{\"query\":\"websocket\"}"
                            }],
                            "model": "upstream-model",
                            "usage": {"input_tokens": 8, "output_tokens": 2}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send second upstream event");
            let _ = events_tx.send((first, second));
            websocket
                .send(UpstreamMessage::Close(Some(UpstreamCloseFrame {
                    code: CloseCode::Library(4001),
                    reason: Cow::Borrowed("mock complete"),
                })))
                .await
                .expect("send upstream close");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let db_for_assert = db.clone();
        let provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");

        let config = ProxyConfig {
            listen_address: "127.0.0.1".to_string(),
            listen_port: 0,
            ..Default::default()
        };
        let server = ProxyServer::new(config, db, None);
        let info = server.start().await.expect("start proxy");

        let mut request = format!("ws://127.0.0.1:{}/v1/responses", info.port)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("authorization", "Bearer downstream-secret".parse().unwrap());
        request.headers_mut().insert(
            "openai-beta",
            "responses_websockets=2026-02-06".parse().unwrap(),
        );
        let (mut client, _) = connect_async(request).await.expect("connect local proxy");

        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send first response.create");
        let first_response: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("first response JSON");
        assert_eq!(first_response["response"]["output"][0]["name"], "run");
        assert_eq!(first_response["response"]["output"][0]["namespace"], "demo");

        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-1")).to_string(),
            ))
            .await
            .expect("send second response.create");
        let second_response: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("second response JSON");
        assert_eq!(
            second_response["response"]["output"][0]["type"],
            "tool_search_call"
        );
        assert_eq!(
            second_response["response"]["output"][0]["arguments"]["query"],
            "websocket"
        );

        let close = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("timed out waiting for close")
            .expect("client stream closed before close frame")
            .expect("client close error");
        match close {
            UpstreamMessage::Close(Some(frame)) => {
                assert_eq!(u16::from(frame.code), 4001);
                assert_eq!(frame.reason, "mock complete");
            }
            other => panic!("expected close frame, got {other:?}"),
        }

        let (path, authorization, beta) = headers_rx.await.expect("upstream headers");
        assert_eq!(path, "/v1/responses");
        assert_eq!(authorization.as_deref(), Some("Bearer provider-secret"));
        assert_eq!(beta.as_deref(), Some("responses_websockets=2026-02-06"));

        let (first, second) = events_rx.await.expect("upstream events");
        let first: Value = serde_json::from_str(&first).unwrap();
        let second: Value = serde_json::from_str(&second).unwrap();
        assert_eq!(first["type"], "response.create");
        assert_eq!(first["model"], "upstream-model");
        assert!(first.get("_private").is_none());
        assert_eq!(second["previous_response_id"], "resp-1");

        let usage_rows = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let count: i64 = db_for_assert
                    .conn
                    .lock()
                    .expect("lock database")
                    .query_row(
                        "SELECT COUNT(*) FROM proxy_request_logs WHERE provider_id = 'ws-provider' AND app_type = 'codex'",
                        [],
                        |row| row.get(0),
                    )
                    .expect("count websocket usage rows");
                if count == 2 {
                    break count;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("timed out waiting for websocket usage rows");
        assert_eq!(usage_rows, 2);

        upstream_task.await.expect("mock upstream task");
        server.stop().await.expect("stop proxy");
    }
    #[tokio::test]
    #[serial]
    async fn upstream_connect_failure_emits_error_and_close_for_sse_fallback() {
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve unused upstream port");
        let unavailable_addr = unused_listener.local_addr().unwrap();
        drop(unused_listener);

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider(format!("http://{unavailable_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");

        let config = ProxyConfig {
            listen_address: "127.0.0.1".to_string(),
            listen_port: 0,
            ..Default::default()
        };
        let server = ProxyServer::new(config, db, None);
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");

        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let error: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("proxy error JSON");
        assert_eq!(error["type"], "error");
        assert_eq!(error["error"]["type"], "proxy_error");
        assert!(error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("WebSocket handshake failed")));

        let close = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("timed out waiting for fallback close")
            .expect("client stream closed before fallback close")
            .expect("fallback close error");
        match close {
            UpstreamMessage::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1011),
            other => panic!("expected fallback close frame, got {other:?}"),
        }

        server.stop().await.expect("stop proxy");
    }
    #[tokio::test]
    #[serial]
    async fn times_out_when_upstream_never_emits_first_response_event() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let mut provider = websocket_provider(format!("http://{upstream_addr}"));
        provider.in_failover_queue = true;
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let mut app_config = db
            .get_proxy_config_for_app("codex")
            .await
            .expect("load app proxy config");
        app_config.enabled = true;
        app_config.auto_failover_enabled = true;
        app_config.streaming_first_byte_timeout = 1;
        app_config.streaming_idle_timeout = 1;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("set websocket timeouts");

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");

        let error: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("timeout error event JSON");
        assert_eq!(error["type"], "error");
        assert!(
            error["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("first response event timed out")),
            "unexpected timeout error: {error}"
        );

        upstream_task.abort();
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn applies_media_prevention_before_sending_websocket_turn() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind media upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (event_tx, event_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let event = next_text(&mut websocket).await;
            let _ = event_tx.send(event);
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-media","model":"upstream-model","output":[]}}).to_string(),
                ))
                .await
                .expect("send completion");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let mut provider = websocket_provider(format!("http://{upstream_addr}"));
        provider.settings_config["models"] = json!([{"id":"upstream-model","input":["text"]}]);
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                json!({
                    "type":"response.create",
                    "model":"local-model",
                    "input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]
                })
                .to_string(),
            ))
            .await
            .expect("send image turn");
        let _ = next_text(&mut client).await;

        let event: Value = serde_json::from_str(&event_rx.await.expect("upstream event"))
            .expect("upstream event JSON");
        assert_eq!(event["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(
            event["input"][0]["content"][0]["text"],
            crate::proxy::media_sanitizer::UNSUPPORTED_IMAGE_MARKER
        );

        upstream_task.await.expect("media upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn closes_at_turn_boundary_when_current_provider_changes() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind first upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (old_received_tx, old_received_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-one","output":[]}})
                        .to_string(),
                ))
                .await
                .expect("send first completion");
            let received_old = matches!(
                tokio::time::timeout(Duration::from_secs(2), websocket.next()).await,
                Ok(Some(Ok(UpstreamMessage::Text(_))))
            );
            let _ = old_received_tx.send(received_old);
            if received_old {
                let _ = websocket
                    .send(UpstreamMessage::Text(
                        json!({"type":"response.completed","response":{"id":"resp-old","output":[]}}).to_string(),
                    ))
                    .await;
            }
        });

        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve second upstream");
        let second_addr = unused_listener.local_addr().unwrap();
        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let first =
            websocket_provider_with_id("ws-provider-one", format!("http://{upstream_addr}"));
        let second = websocket_provider_with_id("ws-provider-two", format!("http://{second_addr}"));
        db.save_provider("codex", &first)
            .expect("save first provider");
        db.save_provider("codex", &second)
            .expect("save second provider");
        db.set_current_provider("codex", &first.id)
            .expect("select first provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send first turn");
        let _ = next_text(&mut client).await;

        db.set_current_provider("codex", &second.id)
            .expect("switch provider");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-one")).to_string(),
            ))
            .await
            .expect("send second turn");
        let error: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("provider-change error JSON");
        assert_eq!(error["type"], "error");
        assert!(error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("provider changed")));
        assert!(!old_received_rx.await.expect("old provider observation"));

        drop(unused_listener);
        upstream_task.await.expect("first upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn closes_at_turn_boundary_when_provider_metadata_changes() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind metadata upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (old_received_tx, old_received_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {"id": "resp-one", "output": []}
                    })
                    .to_string(),
                ))
                .await
                .expect("send first completion");
            let received_old = matches!(
                tokio::time::timeout(Duration::from_secs(2), websocket.next()).await,
                Ok(Some(Ok(UpstreamMessage::Text(_))))
            );
            let _ = old_received_tx.send(received_old);
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let mut provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send first turn");
        let _ = next_text(&mut client).await;

        provider.meta = Some(crate::provider::ProviderMeta {
            custom_user_agent: Some("cc-switch-hot-edit".to_string()),
            ..Default::default()
        });
        db.save_provider("codex", &provider)
            .expect("save provider metadata edit");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-one")).to_string(),
            ))
            .await
            .expect("send second turn");
        let error: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("provider-metadata error JSON");
        assert_eq!(error["type"], "error");
        assert!(error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("provider changed")));
        assert!(!old_received_rx.await.expect("old provider observation"));

        upstream_task.await.expect("metadata upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn stopping_proxy_closes_active_responses_websocket() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind shutdown upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (active_tx, active_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            let _ = active_tx.send(());
            let _ = websocket.next().await;
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        active_rx.await.expect("websocket became active");

        server.stop().await.expect("stop proxy");
        let close = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .expect("active websocket survived proxy stop")
            .expect("client stream ended without close")
            .expect("client close error");
        assert!(matches!(close, UpstreamMessage::Close(_)));
        upstream_task.await.expect("shutdown upstream task");
    }
    #[tokio::test]
    #[serial]
    async fn stopping_proxy_closes_responses_websocket_before_first_turn() {
        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");

        tokio::time::timeout(Duration::from_secs(2), server.stop())
            .await
            .expect("proxy stop waited on a pre-turn websocket")
            .expect("stop proxy");
        let close = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .expect("pre-turn websocket survived proxy stop")
            .expect("client stream ended without close")
            .expect("client close error");
        assert!(matches!(close, UpstreamMessage::Close(_)));
    }

    #[tokio::test]
    #[serial]
    async fn records_websocket_turn_status_ttft_and_per_turn_sessions() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind accounting upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            for turn in 1..=2 {
                let terminal_type = if turn == 1 {
                    "response.completed"
                } else {
                    "response.incomplete"
                };
                let _ = next_text(&mut websocket).await;
                websocket
                    .send(UpstreamMessage::Text(
                        json!({"type": "response.created", "response": {"id": format!("resp-{turn}")}})
                            .to_string(),
                    ))
                    .await
                    .expect("send response.created");
                tokio::time::sleep(Duration::from_millis(50)).await;
                websocket
                    .send(UpstreamMessage::Text(
                        json!({"type": "response.output_text.delta", "delta": "hello"}).to_string(),
                    ))
                    .await
                    .expect("send output delta");
                websocket
                    .send(UpstreamMessage::Text(
                        json!({
                            "type": terminal_type,
                            "response": {
                                "id": format!("resp-{turn}"),
                                "model": "upstream-model",
                                "usage": {"input_tokens": 10, "output_tokens": 2}
                            }
                        })
                        .to_string(),
                    ))
                    .await
                    .expect("send response.completed");
            }
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let db_for_assert = db.clone();
        let provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");

        for (turn, session_id) in [
            (1, "019fb240-1234-7000-8000-000000000011"),
            (2, "019fb240-1234-7000-8000-000000000012"),
        ] {
            let mut event = response_create("local-model", (turn == 2).then_some("resp-1"));
            event["client_metadata"] = json!({"session_id": session_id});
            client
                .send(UpstreamMessage::Text(event.to_string()))
                .await
                .expect("send response.create");
            for _ in 0..3 {
                let _ = next_text(&mut client).await;
            }
        }

        let rows = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let rows = {
                    let conn = db_for_assert.conn.lock().expect("lock database");
                    let mut statement = conn
                        .prepare(
                            "SELECT session_id, first_token_ms FROM proxy_request_logs WHERE provider_id = 'ws-provider' AND app_type = 'codex' ORDER BY session_id",
                        )
                        .expect("prepare usage query");
                    statement
                        .query_map([], |row| {
                            Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Option<i64>>(1)?))
                        })
                        .expect("query usage rows")
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .expect("collect usage rows")
                };
                if rows.len() == 2 {
                    break rows;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("timed out waiting for usage rows");
        let status = server.get_status().await;
        assert_eq!(status.total_requests, 2);
        assert_eq!(status.success_requests, 2);
        assert_eq!(status.failed_requests, 0);
        assert_eq!(status.active_connections, 0);

        assert!(rows.iter().all(|row| row.1.is_some_and(|ttft| ttft >= 40)));
        assert_eq!(
            rows[0].0.as_deref(),
            Some("codex_019fb240-1234-7000-8000-000000000011")
        );
        assert_eq!(
            rows[1].0.as_deref(),
            Some("codex_019fb240-1234-7000-8000-000000000012")
        );

        drop(client);
        upstream_task.await.expect("accounting upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn websocket_failure_updates_circuit_breaker_before_next_connection() {
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve failing upstream port");
        let failing_addr = unused_listener.local_addr().unwrap();
        drop(unused_listener);

        let healthy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind healthy upstream");
        let healthy_addr = healthy_listener.local_addr().unwrap();
        let healthy_task = tokio::spawn(async move {
            let (stream, _) = healthy_listener
                .accept()
                .await
                .expect("accept healthy upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept healthy websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-good",
                            "model": "upstream-model",
                            "usage": {"input_tokens": 1, "output_tokens": 1}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send healthy completion");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let bad = websocket_provider_with_id("ws-bad", format!("http://{failing_addr}"));
        let good = websocket_provider_with_id("ws-good", format!("http://{healthy_addr}"));
        db.save_provider("codex", &bad).expect("save bad provider");
        db.save_provider("codex", &good)
            .expect("save good provider");
        db.add_to_failover_queue("codex", &bad.id)
            .expect("queue bad provider");
        db.add_to_failover_queue("codex", &good.id)
            .expect("queue good provider");
        let mut app_config = db
            .get_proxy_config_for_app("codex")
            .await
            .expect("load codex proxy config");
        app_config.auto_failover_enabled = true;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("enable failover");

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.expect("start proxy");

        let (mut first, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect first local websocket");
        first
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send first response.create");
        let error: Value =
            serde_json::from_str(&next_text(&mut first).await).expect("first failure event JSON");
        assert_eq!(error["type"], "error");
        drop(first);

        let health = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let health = db
                    .get_provider_health(&bad.id, "codex")
                    .await
                    .expect("load bad provider health");
                if health.consecutive_failures >= 1 {
                    break health;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("WebSocket failure did not update provider health");
        assert!(!health.is_healthy);

        let (mut second, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect second local websocket");
        second
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send second response.create");
        let completed: Value =
            serde_json::from_str(&next_text(&mut second).await).expect("healthy completion JSON");
        assert_eq!(completed["type"], "response.completed");

        let status = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = server.get_status().await;
                if status.failover_count == 1
                    && status
                        .active_targets
                        .iter()
                        .any(|target| target.app_type == "codex" && target.provider_id == good.id)
                {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("successful WebSocket failover did not update proxy routing state");
        assert_eq!(status.failover_count, 1);

        drop(second);
        healthy_task.await.expect("healthy upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn relays_websocket_messages_above_default_tungstenite_limit() {
        const LARGE_MESSAGE_BYTES: usize = 65 * 1024 * 1024;
        const EXPECTED_LIMIT: usize = 200 * 1024 * 1024;
        let large_config = WebSocketConfig {
            max_message_size: Some(EXPECTED_LIMIT),
            max_frame_size: Some(EXPECTED_LIMIT),
            ..Default::default()
        };
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind large-message upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = accept_async_with_config(stream, Some(large_config))
                .await
                .expect("accept large-message websocket");
            let request = next_text(&mut websocket).await;
            let request: Value = serde_json::from_str(&request).expect("large request JSON");
            assert_eq!(
                request["input"].as_str().map(str::len),
                Some(LARGE_MESSAGE_BYTES)
            );
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.output_text.delta",
                        "delta": "y".repeat(LARGE_MESSAGE_BYTES)
                    })
                    .to_string(),
                ))
                .await
                .expect("send large upstream event");
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-large",
                            "model": "upstream-model",
                            "usage": {"input_tokens": 1, "output_tokens": 1}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send terminal event");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async_with_config(
            format!("ws://127.0.0.1:{}/v1/responses", info.port),
            Some(large_config),
            false,
        )
        .await
        .expect("connect large-message client");
        let event = json!({
            "type": "response.create",
            "model": "local-model",
            "input": "x".repeat(LARGE_MESSAGE_BYTES)
        });
        client
            .send(UpstreamMessage::Text(event.to_string()))
            .await
            .expect("send large response.create");
        let delta = next_text(&mut client).await;
        let delta: Value = serde_json::from_str(&delta).expect("large delta JSON");
        assert_eq!(
            delta["delta"].as_str().map(str::len),
            Some(LARGE_MESSAGE_BYTES)
        );
        let terminal: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("terminal event JSON");
        assert_eq!(terminal["type"], "response.completed");

        drop(client);
        upstream_task.await.expect("large-message upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn trusts_ssl_cert_file_for_upstream_websocket_tls() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let temp = tempfile::tempdir().expect("create certificate directory");
        let ca_path = temp.path().join("ws-test-ca.pem");
        let ca_body = TEST_CA_DER_BASE64
            .as_bytes()
            .chunks(64)
            .map(|chunk| std::str::from_utf8(chunk).expect("base64 UTF-8"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            &ca_path,
            format!("-----BEGIN CERTIFICATE-----\n{ca_body}\n-----END CERTIFICATE-----\n"),
        )
        .expect("write test CA");
        let _ssl_cert_file = EnvVarGuard::set("SSL_CERT_FILE", &ca_path);

        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind TLS upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let certificate = CertificateDer::from(
            STANDARD
                .decode(TEST_SERVER_CERT_DER_BASE64)
                .expect("decode server certificate"),
        );
        let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            STANDARD
                .decode(TEST_SERVER_KEY_DER_BASE64)
                .expect("decode server key"),
        ));
        let tls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate], private_key)
            .expect("build TLS server config");
        let acceptor = TlsAcceptor::from(Arc::new(tls_config));
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener
                .accept()
                .await
                .expect("accept upstream TCP");
            let stream = acceptor.accept(stream).await.expect("accept upstream TLS");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept upstream websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {"id": "resp-tls", "output": []}
                    })
                    .to_string(),
                ))
                .await
                .expect("send TLS completion");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider(format!("https://{upstream_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let response: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("TLS response JSON");
        assert_eq!(response["type"], "response.completed");

        drop(client);
        upstream_task.await.expect("TLS upstream task");
        server.stop().await.expect("stop proxy");
    }

    struct GlobalProxyReset;

    impl Drop for GlobalProxyReset {
        fn drop(&mut self) {
            let _ = crate::proxy::http_client::apply_proxy(None);
        }
    }

    async fn run_proxy_routing_case(proxy_scheme: &str) {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind proxied upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-proxy","output":[]}})
                        .to_string(),
                ))
                .await
                .expect("send completion");
        });

        let is_http_proxy = proxy_scheme == "http";
        let proxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind configured proxy");
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let (observed_tx, observed_rx) = oneshot::channel();
        let proxy_task = tokio::spawn(async move {
            let (mut client, _) = proxy_listener.accept().await.expect("accept proxy client");
            if is_http_proxy {
                let mut request = Vec::new();
                let mut byte = [0u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    client.read_exact(&mut byte).await.expect("read CONNECT");
                    request.push(byte[0]);
                }
                let request = String::from_utf8(request).expect("CONNECT request UTF-8");
                let _ = observed_tx.send(request.starts_with("CONNECT unreachable.invalid:443 "));
                client
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .expect("write CONNECT response");
            } else {
                let mut greeting = [0u8; 2];
                client
                    .read_exact(&mut greeting)
                    .await
                    .expect("read SOCKS greeting");
                let mut methods = vec![0u8; greeting[1] as usize];
                client
                    .read_exact(&mut methods)
                    .await
                    .expect("read SOCKS methods");
                client.write_all(&[5, 0]).await.expect("select no-auth");
                let mut head = [0u8; 4];
                client
                    .read_exact(&mut head)
                    .await
                    .expect("read SOCKS request");
                let host = match head[3] {
                    3 => {
                        let mut len = [0u8; 1];
                        client
                            .read_exact(&mut len)
                            .await
                            .expect("read domain length");
                        let mut host = vec![0u8; len[0] as usize];
                        client.read_exact(&mut host).await.expect("read domain");
                        String::from_utf8(host).expect("SOCKS domain UTF-8")
                    }
                    other => panic!("expected SOCKS domain request, got atyp={other}"),
                };
                let mut port = [0u8; 2];
                client.read_exact(&mut port).await.expect("read SOCKS port");
                let _ = observed_tx
                    .send(host == "unreachable.invalid" && u16::from_be_bytes(port) == 443);
                client
                    .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                    .await
                    .expect("write SOCKS success");
            }

            let mut upstream = tokio::net::TcpStream::connect(upstream_addr)
                .await
                .expect("connect proxied upstream");
            tokio::io::copy_bidirectional(&mut client, &mut upstream)
                .await
                .expect("relay configured proxy");
        });

        let proxy_url = format!("{proxy_scheme}://{proxy_addr}");
        crate::proxy::http_client::init(Some(&proxy_url)).expect("configure global proxy");
        let _reset = GlobalProxyReset;

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider("http://unreachable.invalid:443/v1".to_string());
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let response: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("proxied response JSON");
        assert_eq!(response["type"], "response.completed");
        assert!(observed_rx.await.expect("configured proxy observation"));

        server.stop().await.expect("stop proxy");
        proxy_task.await.expect("configured proxy task");
        upstream_task.await.expect("proxied upstream task");
    }

    #[tokio::test]
    #[serial]
    async fn routes_websocket_through_configured_http_proxy() {
        run_proxy_routing_case("http").await;
    }

    #[tokio::test]
    #[serial]
    async fn routes_websocket_through_configured_socks5h_proxy() {
        run_proxy_routing_case("socks5h").await;
    }
}
