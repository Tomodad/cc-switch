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
    handler_context::{RequestContext, StreamingTimeoutConfig},
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
    response::{IntoResponse, Response},
};
use futures::{FutureExt, SinkExt, StreamExt};
use percent_encoding::percent_decode_str;
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
const DOWNSTREAM_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_WEBSOCKET_MESSAGE_SIZE: usize = 200 * 1024 * 1024;
const RESPONSES_ENDPOINT: &str = "/responses";

trait AsyncIo: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncIo for T {}
type BoxedIo = Box<dyn AsyncIo + Send + Unpin>;
type UpstreamSocket = WebSocketStream<MaybeTlsStream<BoxedIo>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WebSocketTerminalOutcome {
    Success,
    NeutralFailure,
    ProviderFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WebSocketSendOutcome {
    Sent,
    Shutdown,
}

struct PreRelayFallbackConnection {
    provider: Provider,
    proxy_url: Option<String>,
    upstream: UpstreamSocket,
    turn_state: TurnTransformState,
    accounting: WebSocketTurnAccounting,
    started: Instant,
}

struct PreRelayFallbackRequest<'a> {
    state: &'a ProxyState,
    headers: &'a HeaderMap,
    turn_context: &'a RequestContext,
    turn_providers: &'a [Provider],
    provider_attempt_limit: usize,
    original_response_create: &'a str,
    timeout_config: StreamingTimeoutConfig,
}

enum PreRelayFallbackOutcome {
    Connected(Box<PreRelayFallbackConnection>),
    Shutdown(Box<WebSocketTurnAccounting>),
    Exhausted,
}

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
    async fn begin_for_provider(
        state: &ProxyState,
        ctx: &RequestContext,
        provider: &Provider,
        count_request: bool,
    ) -> Result<Self, ProxyError> {
        let active_guard = ActiveConnectionGuard::acquire(state.status.clone()).await;
        let used_half_open_permit = if ctx.app_config.auto_failover_enabled {
            let permit = state
                .provider_router
                .allow_provider_request(&provider.id, ctx.app_type_str)
                .await;
            if !permit.allowed {
                drop(active_guard);
                return Err(ProxyError::NoAvailableProvider);
            }
            permit.used_half_open_permit
        } else {
            state
                .provider_router
                .prepare_provider_result(&provider.id, ctx.app_type_str)
                .await;
            false
        };
        {
            let mut status = state.status.write().await;
            if count_request {
                status.total_requests = status.total_requests.saturating_add(1);
                status.last_request_at = Some(chrono::Utc::now().to_rfc3339());
            }
            status.current_provider = Some(provider.name.clone());
            status.current_provider_id = Some(provider.id.clone());
        }

        Ok(Self {
            state: state.clone(),
            provider_id: provider.id.clone(),
            provider_name: provider.name.clone(),
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
            .record_result_detached(
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
        self.used_half_open_permit = false;
        drop(self._active_guard.take());
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
    async fn finish_provider_attempt_failure(mut self, message: String) {
        self.finalized = true;
        if let Err(error) = self
            .state
            .provider_router
            .record_result_detached(
                &self.provider_id,
                "codex",
                self.used_half_open_permit,
                false,
                Some(message),
            )
            .await
        {
            log::warn!(
                "[CodexWS] Failed to record provider attempt failure (provider={}): {}",
                self.provider_id,
                error
            );
        }
    }
    #[cfg(test)]
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

    async fn finish_provider_failure_detached(mut self, message: String) {
        self.finalized = true;
        if let Err(error) = self
            .state
            .provider_router
            .record_result_detached(
                &self.provider_id,
                "codex",
                self.used_half_open_permit,
                false,
                Some(message.clone()),
            )
            .await
        {
            log::warn!(
                "[CodexWS] Failed to record detached provider failure (provider={}): {}",
                self.provider_id,
                error
            );
        }
        self.used_half_open_permit = false;
        drop(self._active_guard.take());
        record_websocket_status_failure(&self.state, message).await;
    }

    async fn finish_neutral_attempt(mut self) {
        self.finalized = true;
        self.state
            .provider_router
            .release_permit_neutral(&self.provider_id, "codex", self.used_half_open_permit)
            .await;
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

#[cfg(test)]
static DELAY_DROP_ACCOUNTING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static REQUEST_CONTEXT_LOADS_STARTED: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static PAUSE_REQUEST_CONTEXT_LOAD: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static TRANSFORM_CLIENT_TEXT_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

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
                #[cfg(test)]
                if DELAY_DROP_ACCOUNTING.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

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

async fn record_rejected_later_turn_failure(state: &ProxyState, message: String) {
    let mut status = state.status.write().await;
    status.total_requests = status.total_requests.saturating_add(1);
    status.failed_requests = status.failed_requests.saturating_add(1);
    status.last_request_at = Some(chrono::Utc::now().to_rfc3339());
    status.last_error = Some(message);
    update_proxy_success_rate(&mut status);
}

async fn finalize_later_turn_provider_exhaustion(
    state: &ProxyState,
    error: ProxyError,
    provider_attempts: usize,
) -> ProxyError {
    if provider_attempts > 0 {
        record_websocket_status_failure(state, error.to_string()).await;
    } else {
        record_rejected_later_turn_failure(state, error.to_string()).await;
    }
    error
}

async fn connect_pre_relay_fallback(
    request: PreRelayFallbackRequest<'_>,
    provider_index: &mut usize,
    provider_attempts: &mut usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<PreRelayFallbackOutcome, ProxyError> {
    while *provider_index + 1 < request.turn_providers.len()
        && *provider_attempts < request.provider_attempt_limit
    {
        *provider_index += 1;
        let candidate = request.turn_providers[*provider_index].clone();
        if !codex_provider_supports_responses_websocket(&candidate) {
            continue;
        }
        let retry_accounting = match WebSocketTurnAccounting::begin_for_provider(
            request.state,
            request.turn_context,
            &candidate,
            false,
        )
        .await
        {
            Ok(accounting) => accounting,
            Err(ProxyError::NoAvailableProvider) => continue,
            Err(error) => return Err(error),
        };
        *provider_attempts += 1;
        let (retry_text, retry_state) = match transform_client_text(
            request.original_response_create,
            &candidate,
            &request.turn_context.rectifier_config,
            true,
        ) {
            Ok(Some((text, Some(mut retry_state)))) => {
                retry_state.session_id = request.turn_context.session_id.clone();
                (text, retry_state)
            }
            Ok(_) => {
                retry_accounting
                    .finish_provider_attempt_failure(
                        "fallback response.create transform produced no turn state".to_string(),
                    )
                    .await;
                continue;
            }
            Err(error) => {
                finish_fallback_transform_error(retry_accounting, &error).await;
                continue;
            }
        };
        let upstream_request = match build_upstream_request(&candidate, request.headers) {
            Ok(request) => request,
            Err(error) => {
                retry_accounting
                    .finish_provider_attempt_failure(error.to_string())
                    .await;
                continue;
            }
        };
        let retry_started = Instant::now();
        let retry_deadline = websocket_turn_timeout_deadline(
            true,
            false,
            retry_started,
            retry_started,
            request.timeout_config,
        );
        let retry_proxy_url = super::http_client::get_current_proxy_url();
        let mut retry_upstream = match connect_upstream_with_shutdown(
            upstream_request,
            retry_proxy_url.as_deref(),
            retry_deadline,
            shutdown_rx,
        )
        .await
        {
            Ok(Some(upstream)) => upstream,
            Ok(None) => {
                return Ok(PreRelayFallbackOutcome::Shutdown(Box::new(
                    retry_accounting,
                )))
            }
            Err(error) => {
                retry_accounting
                    .finish_provider_attempt_failure(error.to_string())
                    .await;
                continue;
            }
        };
        match send_upstream_message(
            &mut retry_upstream,
            UpstreamMessage::Text(retry_text),
            retry_deadline,
            shutdown_rx,
            "fallback initial event write",
        )
        .await
        {
            Ok(WebSocketSendOutcome::Sent) => {
                return Ok(PreRelayFallbackOutcome::Connected(Box::new(
                    PreRelayFallbackConnection {
                        provider: candidate,
                        proxy_url: retry_proxy_url,
                        upstream: retry_upstream,
                        turn_state: retry_state,
                        accounting: retry_accounting,
                        started: retry_started,
                    },
                )));
            }
            Ok(WebSocketSendOutcome::Shutdown) => {
                return Ok(PreRelayFallbackOutcome::Shutdown(Box::new(
                    retry_accounting,
                )));
            }
            Err(error) => {
                retry_accounting
                    .finish_provider_attempt_failure(error.to_string())
                    .await;
            }
        }
    }
    Ok(PreRelayFallbackOutcome::Exhausted)
}

fn websocket_origin_is_trusted(headers: &HeaderMap) -> bool {
    let origins = headers.get_all(http::header::ORIGIN);
    if origins.iter().next().is_none() {
        return true;
    }

    origins.iter().all(|origin| {
        let Ok(origin) = origin.to_str() else {
            return false;
        };
        let Ok(origin) = Url::parse(origin) else {
            return false;
        };
        if !matches!(origin.scheme(), "http" | "https") {
            return false;
        }
        match origin.host() {
            Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        }
    })
}

pub async fn handle_responses_websocket(
    State(state): State<ProxyState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if !websocket_origin_is_trusted(&headers) {
        return http::StatusCode::FORBIDDEN.into_response();
    }

    let connection_guard = state.track_websocket_connection();
    upgrade
        .max_message_size(MAX_WEBSOCKET_MESSAGE_SIZE)
        .max_frame_size(MAX_WEBSOCKET_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_connection(socket, state, headers, connection_guard))
}

async fn handle_connection(
    mut downstream: WebSocket,
    state: ProxyState,
    headers: HeaderMap,
    _connection_guard: super::server::WebSocketConnectionGuard,
) {
    let result = handle_connection_inner(&mut downstream, &state, &headers).await;
    if let Err(error) = result {
        log::warn!("[CodexWS] Closing local Responses WebSocket: {error}");
        send_proxy_error_and_close(&mut downstream, &error).await;
    }
}

async fn request_context_or_shutdown(
    state: &ProxyState,
    body: &Value,
    headers: &HeaderMap,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<RequestContext>, ProxyError> {
    let state = state.clone();
    let body = body.clone();
    let headers = headers.clone();
    let runtime = tokio::runtime::Handle::current();
    let mut context_task = tokio::task::spawn_blocking(move || {
        runtime.block_on(RequestContext::new(
            &state,
            &body,
            &headers,
            AppType::Codex,
            "CodexWS",
            "codex",
        ))
    });

    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    context_task.abort();
                    return Ok(None);
                }
            }
            result = &mut context_task => {
                return result
                    .map_err(|error| ProxyError::ForwardFailed(format!(
                        "WebSocket request context task failed: {error}"
                    )))?
                    .map(Some);
            }
        }
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

    #[cfg(test)]
    {
        REQUEST_CONTEXT_LOADS_STARTED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        while PAUSE_REQUEST_CONTEXT_LOAD.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    }
    let Some(mut turn_context) =
        request_context_or_shutdown(state, &response_body, headers, &mut shutdown_rx).await?
    else {
        send_proxy_shutdown_close(downstream).await;
        return Ok(());
    };
    let mut original_response_create = first_text;
    let initial_allows_provider_fallback =
        !response_create_has_provider_cursor(&original_response_create);
    let mut turn_providers = turn_context.get_providers();
    let mut provider_index = turn_providers
        .iter()
        .position(|candidate| candidate.id == turn_context.provider.id)
        .unwrap_or(0);
    let selected_provider_id = turn_context.provider.id.clone();
    if !initial_allows_provider_fallback
        && turn_providers
            .get(provider_index)
            .is_some_and(|provider| !codex_provider_supports_responses_websocket(provider))
    {
        let message =
            "selected Codex provider does not support native Responses WebSocket; reconnect without provider cursor"
                .to_string();
        record_rejected_later_turn_failure(state, message.clone()).await;
        return Err(ProxyError::ConfigError(message));
    }
    while provider_index < turn_providers.len()
        && !codex_provider_supports_responses_websocket(&turn_providers[provider_index])
    {
        provider_index += 1;
    }
    let Some(mut provider) = turn_providers.get(provider_index).cloned() else {
        let message =
            "selected Codex provider chain does not include a native Responses WebSocket provider"
                .to_string();
        record_rejected_later_turn_failure(state, message.clone()).await;
        return Err(ProxyError::ConfigError(message));
    };
    let mut provider_attempt_limit = websocket_provider_attempt_limit(&turn_context);
    let mut provider_attempts = 0_usize;

    let mut count_request = true;
    let mut upstream_proxy_url = super::http_client::get_current_proxy_url();
    let (mut upstream, mut turn_state, initial_accounting, mut turn_started, mut timeout_config) = loop {
        let initial_accounting = loop {
            match WebSocketTurnAccounting::begin_for_provider(
                state,
                &turn_context,
                &provider,
                count_request,
            )
            .await
            {
                Ok(accounting) => {
                    provider_attempts += 1;
                    count_request = false;
                    break accounting;
                }
                Err(ProxyError::NoAvailableProvider)
                    if turn_context.app_config.auto_failover_enabled =>
                {
                    if initial_allows_provider_fallback {
                        if let Some(next) = next_websocket_provider(
                            &turn_providers,
                            &mut provider_index,
                            provider_attempts,
                            provider_attempt_limit,
                        ) {
                            provider = next;
                            continue;
                        }
                    }
                    let error = ProxyError::NoAvailableProvider;
                    if count_request {
                        record_rejected_later_turn_failure(state, error.to_string()).await;
                    } else {
                        record_websocket_status_failure(state, error.to_string()).await;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        };

        let transformed = match transform_client_text(
            &original_response_create,
            &provider,
            &turn_context.rectifier_config,
            true,
        ) {
            Ok(transformed) => transformed,
            Err(error) => {
                finish_fallback_transform_error(initial_accounting, &error).await;
                if turn_context.app_config.auto_failover_enabled && initial_allows_provider_fallback
                {
                    if let Some(next) = next_websocket_provider(
                        &turn_providers,
                        &mut provider_index,
                        provider_attempts,
                        provider_attempt_limit,
                    ) {
                        provider = next;
                        continue;
                    }
                }
                record_websocket_status_failure(state, error.to_string()).await;
                return Err(error);
            }
        };
        let (first_text, turn_state) = transformed.expect("validated response.create");
        let mut turn_state = turn_state.expect("response.create transform state");
        turn_state.session_id = turn_context.session_id.clone();
        let turn_started = Instant::now();
        let timeout_config = turn_context.streaming_timeout_config();
        let initial_deadline = websocket_turn_timeout_deadline(
            true,
            false,
            turn_started,
            turn_started,
            timeout_config,
        );
        let request = match build_upstream_request(&provider, headers) {
            Ok(request) => request,
            Err(error) => {
                initial_accounting
                    .finish_provider_attempt_failure(error.to_string())
                    .await;
                if initial_allows_provider_fallback {
                    if let Some(next) = next_websocket_provider(
                        &turn_providers,
                        &mut provider_index,
                        provider_attempts,
                        provider_attempt_limit,
                    ) {
                        provider = next;
                        continue;
                    }
                }
                record_websocket_status_failure(state, error.to_string()).await;
                return Err(error);
            }
        };
        let mut candidate_upstream = match connect_upstream_with_shutdown(
            request,
            upstream_proxy_url.as_deref(),
            initial_deadline,
            &mut shutdown_rx,
        )
        .await
        {
            Ok(Some(upstream)) => upstream,
            Ok(None) => {
                let mut accounting = Some(initial_accounting);
                finish_websocket_shutdown(downstream, &mut accounting).await;
                return Ok(());
            }
            Err(error) => {
                initial_accounting
                    .finish_provider_attempt_failure(error.to_string())
                    .await;
                if initial_allows_provider_fallback {
                    if let Some(next) = next_websocket_provider(
                        &turn_providers,
                        &mut provider_index,
                        provider_attempts,
                        provider_attempt_limit,
                    ) {
                        provider = next;
                        upstream_proxy_url = super::http_client::get_current_proxy_url();
                        continue;
                    }
                }
                record_websocket_status_failure(state, error.to_string()).await;
                return Err(error);
            }
        };
        match send_upstream_message(
            &mut candidate_upstream,
            UpstreamMessage::Text(first_text),
            initial_deadline,
            &mut shutdown_rx,
            "initial event write",
        )
        .await
        {
            Ok(WebSocketSendOutcome::Sent) => {
                break (
                    candidate_upstream,
                    turn_state,
                    initial_accounting,
                    turn_started,
                    timeout_config,
                );
            }
            Ok(WebSocketSendOutcome::Shutdown) => {
                let mut accounting = Some(initial_accounting);
                finish_websocket_shutdown(downstream, &mut accounting).await;
                return Ok(());
            }
            Err(error) => {
                initial_accounting
                    .finish_provider_attempt_failure(error.to_string())
                    .await;
                if initial_allows_provider_fallback {
                    if let Some(next) = next_websocket_provider(
                        &turn_providers,
                        &mut provider_index,
                        provider_attempts,
                        provider_attempt_limit,
                    ) {
                        provider = next;
                        upstream_proxy_url = super::http_client::get_current_proxy_url();
                        continue;
                    }
                }
                record_websocket_status_failure(state, error.to_string()).await;
                return Err(error);
            }
        }
    };
    let mut retain_fallback_provider = provider.id != selected_provider_id;
    let mut turn_accounting = Some(initial_accounting);
    let mut response_in_flight = true;
    let mut media_rectifier_retried = false;
    let mut first_token_ms = None;
    let mut received_response_event = false;
    let mut relayed_response_event = false;
    let mut last_response_event_at = turn_started;
    log::info!(
        "[CodexWS] Connected protocol-aware Responses WebSocket (provider={})",
        provider.id
    );

    loop {
        let timeout_deadline = websocket_turn_timeout_deadline(
            response_in_flight,
            received_response_event,
            turn_started,
            last_response_event_at,
            timeout_config,
        );

        tokio::select! {
            _ = async {
                if let Some(deadline) = timeout_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                let kind = if received_response_event { "idle" } else { "first response event" };
                let message = format!("upstream WebSocket {kind} timed out");
                if received_response_event {
                    if let Some(accounting) = turn_accounting.take() {
                        accounting
                            .finish_provider_failure_detached(message.clone())
                            .await;
                    }
                    return Err(ProxyError::Timeout(message));
                }
                if let Some(accounting) = turn_accounting.take() {
                    accounting
                        .finish_provider_attempt_failure(message.clone())
                        .await;
                }
                if !response_create_has_provider_cursor(&original_response_create) {
                    match connect_pre_relay_fallback(
                        PreRelayFallbackRequest {
                            state,
                            headers,
                            turn_context: &turn_context,
                            turn_providers: &turn_providers,
                            provider_attempt_limit,
                            original_response_create: &original_response_create,
                            timeout_config,
                        },
                        &mut provider_index,
                        &mut provider_attempts,
                        &mut shutdown_rx,
                    )
                    .await?
                    {
                        PreRelayFallbackOutcome::Connected(connection) => {
                            let PreRelayFallbackConnection {
                                provider: retry_provider,
                                proxy_url,
                                upstream: retry_upstream,
                                turn_state: retry_state,
                                accounting: retry_accounting,
                                started: retry_started,
                            } = *connection;
                            provider = retry_provider;
                            retain_fallback_provider = provider.id != turn_context.provider.id;
                            upstream_proxy_url = proxy_url;
                            upstream = retry_upstream;
                            turn_state = retry_state;
                            turn_accounting = Some(retry_accounting);
                            media_rectifier_retried = false;
                            response_in_flight = true;
                            turn_started = retry_started;
                            first_token_ms = None;
                            received_response_event = false;
                            relayed_response_event = false;
                            last_response_event_at = retry_started;
                            continue;
                        }
                        PreRelayFallbackOutcome::Shutdown(accounting) => {
                            turn_accounting = Some(*accounting);
                            finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                            break;
                        }
                        PreRelayFallbackOutcome::Exhausted => {}
                    }
                }
                record_websocket_status_failure(state, message.clone()).await;
                return Err(ProxyError::Timeout(message));
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    finish_websocket_shutdown(downstream, &mut turn_accounting).await;
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
                    let close_deadline = websocket_turn_timeout_deadline(
                        response_in_flight,
                        received_response_event,
                        turn_started,
                        last_response_event_at,
                        timeout_config,
                    );
                    let _ = send_upstream_message(
                        &mut upstream,
                        UpstreamMessage::Close(None),
                        close_deadline,
                        &mut shutdown_rx,
                        "close frame write",
                    )
                    .await;
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
                        let (text, next_state, already_sent, next_turn_started) =
                            if websocket_event_is(&text, "response.create") {
                            let next_original_response_create = text.clone();
                            let body = response_create_body(
                                &serde_json::from_str::<Value>(&text).map_err(|error| {
                                    ProxyError::InvalidRequest(format!(
                                        "invalid WebSocket JSON: {error}"
                                    ))
                                })?,
                            )?;
                            #[cfg(test)]
                            {
                                REQUEST_CONTEXT_LOADS_STARTED
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                while PAUSE_REQUEST_CONTEXT_LOAD
                                    .load(std::sync::atomic::Ordering::SeqCst)
                                {
                                    tokio::task::yield_now().await;
                                }
                            }
                            let Some(next_ctx) = request_context_or_shutdown(
                                state,
                                &body,
                                headers,
                                &mut shutdown_rx,
                            )
                            .await?
                            else {
                                finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                                break;
                            };
                            let current_proxy_url = super::http_client::get_current_proxy_url();
                            if current_proxy_url.as_deref() != upstream_proxy_url.as_deref() {
                                let message =
                                    "global proxy changed; reconnect WebSocket".to_string();
                                record_rejected_later_turn_failure(state, message.clone()).await;
                                return Err(ProxyError::ConfigError(message));
                            }
                            let mut next_turn_providers = next_ctx.get_providers();
                            let has_provider_cursor = response_create_has_provider_cursor(&text);
                            if next_ctx.provider.id == provider.id {
                                retain_fallback_provider = false;
                            }
                            let retained_index = retain_fallback_provider
                                .then(|| {
                                    next_turn_providers.iter().position(|candidate| {
                                        candidate.id == provider.id
                                            && !provider_snapshot_changed(&provider, candidate)
                                    })
                                })
                                .flatten();
                            if retain_fallback_provider
                                && retained_index.is_none()
                                && has_provider_cursor
                            {
                                let message =
                                    "selected Codex provider changed; reconnect WebSocket".to_string();
                                record_rejected_later_turn_failure(state, message.clone()).await;
                                return Err(ProxyError::ConfigError(message));
                            }
                            if let Some(retained_index) = retained_index {
                                let retained = next_turn_providers.remove(retained_index);
                                next_turn_providers.insert(0, retained);
                            }
                            let mut next_index = if retained_index.is_some() {
                                0
                            } else {
                                next_turn_providers
                                    .iter()
                                    .position(|candidate| candidate.id == next_ctx.provider.id)
                                    .unwrap_or(0)
                            };
                            if (!retain_fallback_provider
                                || next_turn_providers[next_index].id == provider.id)
                                && provider_snapshot_changed(
                                    &provider,
                                    &next_turn_providers[next_index],
                                )
                            {
                                let message =
                                    "selected Codex provider changed; reconnect WebSocket".to_string();
                                record_rejected_later_turn_failure(state, message.clone()).await;
                                return Err(ProxyError::ConfigError(message));
                            }

                            let next_timeout_config = next_ctx.streaming_timeout_config();
                            let next_provider_attempt_limit =
                                websocket_provider_attempt_limit(&next_ctx);
                            let mut next_provider_attempts = 0_usize;
                            let (
                                next_provider,
                                text,
                                next_state,
                                next_accounting,
                                next_upstream,
                                already_sent,
                                next_turn_started,
                            ) = loop {
                                let candidate = next_turn_providers[next_index].clone();
                                let mut attempt_error = None;
                                if codex_provider_supports_responses_websocket(&candidate) {
                                    match WebSocketTurnAccounting::begin_for_provider(
                                        state,
                                        &next_ctx,
                                        &candidate,
                                        next_provider_attempts == 0,
                                    )
                                    .await
                                    {
                                        Ok(accounting) => {
                                            next_provider_attempts += 1;
                                            let transformed = match transform_client_text(
                                                &text,
                                                &candidate,
                                                &next_ctx.rectifier_config,
                                                false,
                                            ) {
                                                Ok(transformed) => transformed,
                                                Err(error) => {
                                                    accounting
                                                        .finish_neutral_failure(error.to_string())
                                                        .await;
                                                    attempt_error = Some(error);
                                                    if has_provider_cursor {
                                                        let error = attempt_error
                                                            .take()
                                                            .expect("provider attempt failed");
                                                        return Err(
                                                            finalize_later_turn_provider_exhaustion(
                                                                state,
                                                                error,
                                                                next_provider_attempts,
                                                            )
                                                            .await,
                                                        );
                                                    }
                                                    if next_provider_attempts
                                                        >= next_provider_attempt_limit
                                                    {
                                                        let error = attempt_error
                                                            .expect("provider attempt failed");
                                                        return Err(
                                                            finalize_later_turn_provider_exhaustion(
                                                                state,
                                                                error,
                                                                next_provider_attempts,
                                                            )
                                                            .await,
                                                        );
                                                    }
                                                    next_index += 1;
                                                    if next_index >= next_turn_providers.len() {
                                                        let error = attempt_error
                                                            .expect("provider attempt failed");
                                                        return Err(
                                                            finalize_later_turn_provider_exhaustion(
                                                                state,
                                                                error,
                                                                next_provider_attempts,
                                                            )
                                                            .await,
                                                        );
                                                    }
                                                    continue;
                                                }
                                            };
                                            let (text, mut next_state) =
                                                transformed.unwrap_or((text.clone(), None));
                                            if let Some(state) = next_state.as_mut() {
                                                state.session_id = next_ctx.session_id.clone();
                                            }
                                            let attempt_started = Instant::now();
                                            if candidate.id == provider.id {
                                                let write_deadline = websocket_turn_timeout_deadline(
                                                    true,
                                                    false,
                                                    attempt_started,
                                                    attempt_started,
                                                    next_timeout_config,
                                                );
                                                match send_upstream_message(
                                                    &mut upstream,
                                                    UpstreamMessage::Text(text.clone()),
                                                    write_deadline,
                                                    &mut shutdown_rx,
                                                    "text frame write",
                                                )
                                                .await
                                                {
                                                    Ok(WebSocketSendOutcome::Sent) => {
                                                        break (
                                                            candidate,
                                                            text,
                                                            next_state,
                                                            accounting,
                                                            None,
                                                            true,
                                                            attempt_started,
                                                        );
                                                    }
                                                    Ok(WebSocketSendOutcome::Shutdown) => {
                                                        turn_accounting = Some(accounting);
                                                        finish_websocket_shutdown(
                                                            downstream,
                                                            &mut turn_accounting,
                                                        )
                                                        .await;
                                                        return Ok(());
                                                    }
                                                    Err(error) => {
                                                        accounting
                                                            .finish_provider_attempt_failure(
                                                                error.to_string(),
                                                            )
                                                            .await;
                                                        if response_create_has_provider_cursor(&text)
                                                        {
                                                            return Err(
                                                                finalize_later_turn_provider_exhaustion(
                                                                    state,
                                                                    error,
                                                                    next_provider_attempts,
                                                                )
                                                                .await,
                                                            );
                                                        }
                                                        attempt_error = Some(error);
                                                    }
                                                }
                                            } else {
                                                let request = match build_upstream_request(
                                                    &candidate,
                                                    headers,
                                                ) {
                                                    Ok(request) => request,
                                                    Err(error) => {
                                                        accounting
                                                            .finish_provider_attempt_failure(
                                                                error.to_string(),
                                                            )
                                                            .await;
                                                        attempt_error = Some(error);
                                                        if next_provider_attempts
                                                            >= next_provider_attempt_limit
                                                        {
                                                            let error = attempt_error
                                                                .expect("provider attempt failed");
                                                            return Err(
                                                                finalize_later_turn_provider_exhaustion(
                                                                    state,
                                                                    error,
                                                                    next_provider_attempts,
                                                                )
                                                                .await,
                                                            );
                                                        }
                                                        next_index += 1;
                                                        if next_index >= next_turn_providers.len() {
                                                            let error = attempt_error
                                                                .expect("provider attempt failed");
                                                            return Err(
                                                                finalize_later_turn_provider_exhaustion(
                                                                    state,
                                                                    error,
                                                                    next_provider_attempts,
                                                                )
                                                                .await,
                                                            );
                                                        }
                                                        continue;
                                                    }
                                                };
                                                let reconnect_started = attempt_started;
                                                let reconnect_deadline = websocket_turn_timeout_deadline(
                                                    true,
                                                    false,
                                                    reconnect_started,
                                                    reconnect_started,
                                                    next_timeout_config,
                                                );
                                                let mut candidate_upstream =
                                                    match connect_upstream_with_shutdown(
                                                        request,
                                                        current_proxy_url.as_deref(),
                                                        reconnect_deadline,
                                                        &mut shutdown_rx,
                                                    )
                                                    .await
                                                    {
                                                        Ok(Some(upstream)) => upstream,
                                                        Ok(None) => {
                                                            turn_accounting = Some(accounting);
                                                            finish_websocket_shutdown(
                                                                downstream,
                                                                &mut turn_accounting,
                                                            )
                                                            .await;
                                                            return Ok(());
                                                        }
                                                        Err(error) => {
                                                            accounting
                                                                .finish_provider_attempt_failure(
                                                                    error.to_string(),
                                                                )
                                                                .await;
                                                            attempt_error = Some(error);
                                                            if next_provider_attempts
                                                                >= next_provider_attempt_limit
                                                            {
                                                                let error = attempt_error
                                                                    .expect("provider attempt failed");
                                                                return Err(
                                                                    finalize_later_turn_provider_exhaustion(
                                                                        state,
                                                                        error,
                                                                        next_provider_attempts,
                                                                    )
                                                                    .await,
                                                                );
                                                            }
                                                            next_index += 1;
                                                            if next_index >= next_turn_providers.len() {
                                                                let error = attempt_error
                                                                    .expect("provider attempt failed");
                                                                return Err(
                                                                    finalize_later_turn_provider_exhaustion(
                                                                        state,
                                                                        error,
                                                                        next_provider_attempts,
                                                                    )
                                                                    .await,
                                                                );
                                                            }
                                                            continue;
                                                        }
                                                    };
                                                match send_upstream_message(
                                                    &mut candidate_upstream,
                                                    UpstreamMessage::Text(text.clone()),
                                                    reconnect_deadline,
                                                    &mut shutdown_rx,
                                                    "later-turn initial event write",
                                                )
                                                .await
                                                {
                                                    Ok(WebSocketSendOutcome::Sent) => {
                                                        break (
                                                            candidate,
                                                            text,
                                                            next_state,
                                                            accounting,
                                                            Some(candidate_upstream),
                                                            true,
                                                            reconnect_started,
                                                        );
                                                    }
                                                    Ok(WebSocketSendOutcome::Shutdown) => {
                                                        turn_accounting = Some(accounting);
                                                        finish_websocket_shutdown(
                                                            downstream,
                                                            &mut turn_accounting,
                                                        )
                                                        .await;
                                                        return Ok(());
                                                    }
                                                    Err(error) => {
                                                        accounting
                                                            .finish_provider_attempt_failure(
                                                                error.to_string(),
                                                            )
                                                            .await;
                                                        attempt_error = Some(error);
                                                    }
                                                }
                                            }
                                        }
                                        Err(ProxyError::NoAvailableProvider)
                                            if next_ctx.app_config.auto_failover_enabled =>
                                        {
                                            if response_create_has_provider_cursor(&text) {
                                                let error = ProxyError::NoAvailableProvider;
                                                record_rejected_later_turn_failure(
                                                    state,
                                                    error.to_string(),
                                                )
                                                .await;
                                                return Err(error);
                                            }
                                        }
                                        Err(error) => return Err(error),
                                    }
                                }
                                next_index += 1;
                                if next_index >= next_turn_providers.len()
                                    || next_provider_attempts >= next_provider_attempt_limit
                                {
                                    let error =
                                        attempt_error.unwrap_or(ProxyError::NoAvailableProvider);
                                    return Err(finalize_later_turn_provider_exhaustion(
                                        state,
                                        error,
                                        next_provider_attempts,
                                    )
                                    .await);
                                }
                            };
                            if next_upstream.is_none() {
                                if let Err(error) =
                                    reject_queued_upstream_data_before_next_turn(&mut upstream)
                                {
                                    next_accounting
                                        .finish_neutral_failure(error.to_string())
                                        .await;
                                    return Err(error);
                                }
                            }
                            if let Some(next_upstream) = next_upstream {
                                provider = next_provider.clone();
                                upstream_proxy_url = current_proxy_url;
                                upstream = next_upstream;
                            }
                            retain_fallback_provider = provider.id != next_ctx.provider.id;
                            timeout_config = next_timeout_config;
                            turn_accounting = Some(next_accounting);
                            turn_context = next_ctx;
                            turn_providers = next_turn_providers;
                            provider_index = next_index;
                            provider_attempt_limit =
                                websocket_provider_attempt_limit(&turn_context);
                            provider_attempts = next_provider_attempts;
                            original_response_create = next_original_response_create;
                            (text, next_state, already_sent, Some(next_turn_started))
                        } else {
                            (text, None, false, None)
                        };
                        if let Some(next_state) = next_state {
                            turn_state = next_state;
                            response_in_flight = true;
                            turn_started = next_turn_started.expect("response.create start time");
                            first_token_ms = None;
                            received_response_event = false;
                            media_rectifier_retried = false;
                            relayed_response_event = false;
                            last_response_event_at = turn_started;
                        }
                        let write_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        if !already_sent && send_upstream_turn_message(
                            &mut upstream,
                            UpstreamMessage::Text(text),
                            write_deadline,
                            &mut shutdown_rx,
                            "text frame write",
                            &mut turn_accounting,
                        )
                        .await?
                            == WebSocketSendOutcome::Shutdown
                        {
                            finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                            break;
                        }
                    }
                    DownstreamMessage::Binary(data) => {
                        let write_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        if send_upstream_turn_message(
                            &mut upstream,
                            UpstreamMessage::Binary(data),
                            write_deadline,
                            &mut shutdown_rx,
                            "binary frame write",
                            &mut turn_accounting,
                        )
                        .await?
                            == WebSocketSendOutcome::Shutdown
                        {
                            finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                            break;
                        }
                    }
                    DownstreamMessage::Ping(data) => {
                        let write_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        if send_upstream_turn_message(
                            &mut upstream,
                            UpstreamMessage::Ping(data),
                            write_deadline,
                            &mut shutdown_rx,
                            "ping frame write",
                            &mut turn_accounting,
                        )
                        .await?
                            == WebSocketSendOutcome::Shutdown
                        {
                            finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                            break;
                        }
                    }
                    DownstreamMessage::Pong(data) => {
                        let write_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        if send_upstream_turn_message(
                            &mut upstream,
                            UpstreamMessage::Pong(data),
                            write_deadline,
                            &mut shutdown_rx,
                            "pong frame write",
                            &mut turn_accounting,
                        )
                        .await?
                            == WebSocketSendOutcome::Shutdown
                        {
                            finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                            break;
                        }
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
                        let close_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        let _ = send_upstream_message(
                            &mut upstream,
                            UpstreamMessage::Close(frame),
                            close_deadline,
                            &mut shutdown_rx,
                            "close frame write",
                        )
                        .await;
                        break;
                    }
                }
            }
            upstream_message = upstream.next() => {
                let upstream_message = match upstream_message {
                    Some(message) => message,
                    None if response_in_flight && !relayed_response_event => Ok(
                        websocket_transport_failure_event(
                            "upstream WebSocket ended before a terminal response".to_string(),
                        ),
                    ),
                    None if response_in_flight => {
                        return Err(premature_upstream_close_error(
                            &mut turn_accounting,
                            "upstream WebSocket ended before a terminal response",
                        )
                        .await);
                    }
                    None => {
                        let close_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        let _ = send_downstream_message(
                            downstream,
                            DownstreamMessage::Close(None),
                            close_deadline,
                            &mut shutdown_rx,
                            "close frame write",
                        )
                        .await;
                        break;
                    }
                };
                let upstream_message = match upstream_message {
                    Ok(UpstreamMessage::Close(_))
                        if response_in_flight && !relayed_response_event =>
                    {
                        websocket_transport_failure_event(
                            "upstream WebSocket closed before a terminal response".to_string(),
                        )
                    }
                    Ok(message) => message,
                    Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed)
                        if response_in_flight && !relayed_response_event =>
                    {
                        websocket_transport_failure_event(
                            "upstream WebSocket closed before a terminal response".to_string(),
                        )
                    }
                    Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed) => {
                        if response_in_flight {
                            return Err(premature_upstream_close_error(
                                &mut turn_accounting,
                                "upstream WebSocket closed before a terminal response",
                            )
                            .await);
                        }
                        let close_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        let _ = send_downstream_message(
                            downstream,
                            DownstreamMessage::Close(None),
                            close_deadline,
                            &mut shutdown_rx,
                            "close frame write",
                        )
                        .await;
                        break;
                    }
                    Err(error) => {
                        let message = format!("upstream WebSocket read failed: {error}");
                        if response_in_flight && !relayed_response_event {
                            websocket_transport_failure_event(message)
                        } else {
                            if response_in_flight {
                                if let Some(accounting) = turn_accounting.take() {
                                    accounting.finish_provider_failure_detached(message.clone()).await;
                                }
                            }
                            return Err(ProxyError::ForwardFailed(message));
                        }
                    }
                };
                let upstream_message = match upstream_message {
                    UpstreamMessage::Text(text) if !websocket_text_is_responses_event(&text) => {
                        websocket_transport_failure_event(
                            "upstream WebSocket sent an invalid Responses event".to_string(),
                        )
                    }
                    UpstreamMessage::Binary(_)
                        if response_in_flight && !relayed_response_event =>
                    {
                        websocket_transport_failure_event(
                            "upstream WebSocket sent a binary Responses event".to_string(),
                        )
                    }
                    message => message,
                };

                match upstream_message {
                    UpstreamMessage::Text(text) => {
                        if !response_in_flight {
                            let event_type = websocket_event_type(&text)
                                .unwrap_or_else(|| "Responses event".to_string());
                            return Err(ProxyError::ForwardFailed(format!(
                                "upstream WebSocket sent {event_type} while no response was in flight"
                            )));
                        }
                        received_response_event = true;
                        last_response_event_at = Instant::now();
                        let terminal = websocket_event_is_terminal(&text);
                        if first_token_ms.is_none()
                            && (websocket_event_has_generated_output(&text) || terminal)
                        {
                            first_token_ms = Some(turn_started.elapsed().as_millis() as u64);
                        }
                        let terminal_data = terminal.then(|| {
                            let outcome = websocket_terminal_outcome(&text);
                            let status_code = websocket_terminal_status_code(&text, outcome);
                            let event = serde_json::from_str::<Value>(&text).ok();
                            (outcome, status_code, event)
                        });
                        let mut media_retry_write_failure = None;
                        let mut terminal_usage_already_logged = false;
                        if terminal
                            && !relayed_response_event
                            && !media_rectifier_retried
                            && turn_context.rectifier_config.enabled
                            && turn_context.rectifier_config.request_media_fallback
                        {
                            let status_code = terminal_data
                                .as_ref()
                                .map(|(_, status, _)| *status)
                                .unwrap_or(500);
                            let media_error = ProxyError::UpstreamError {
                                status: status_code,
                                body: Some(text.clone()),
                            };
                            let mut retry_event: Value = serde_json::from_str(
                                &original_response_create,
                            )
                            .map_err(|error| {
                                ProxyError::InvalidRequest(format!(
                                    "invalid WebSocket JSON: {error}"
                                ))
                            })?;
                            let mut retry_body = response_create_body(&retry_event)?;
                            if super::media_sanitizer::contains_image_blocks(&retry_body)
                                && super::media_sanitizer::is_unsupported_image_error(&media_error)
                                && super::media_sanitizer::replace_image_blocks_with_marker(
                                    &mut retry_body,
                                ) > 0
                            {
                                if let Some((_, status_code, Some(event))) = terminal_data.as_ref() {
                                    spawn_codex_websocket_usage(
                                        state,
                                        &provider,
                                        &turn_state,
                                        event.clone(),
                                        *status_code,
                                        turn_started.elapsed().as_millis() as u64,
                                        first_token_ms,
                                    );
                                    terminal_usage_already_logged = true;
                                }
                                if retry_event.get("response").is_some() {
                                    retry_event["response"] = retry_body;
                                } else {
                                    retry_body["type"] = Value::String("response.create".to_string());
                                    retry_event = retry_body;
                                }
                                original_response_create = retry_event.to_string();
                                let (retry_text, retry_state) = transform_client_text(
                                    &original_response_create,
                                    &provider,
                                    &turn_context.rectifier_config,
                                    true,
                                )?
                                .expect("rectified response.create");
                                let mut retry_state = retry_state.expect("rectified turn state");
                                retry_state.session_id = turn_context.session_id.clone();
                                let retry_started = Instant::now();
                                let retry_deadline = websocket_turn_timeout_deadline(
                                    true, false, retry_started, retry_started, timeout_config,
                                );
                                match send_upstream_message(
                                    &mut upstream,
                                    UpstreamMessage::Text(retry_text),
                                    retry_deadline,
                                    &mut shutdown_rx,
                                    "media fallback event write",
                                )
                                .await
                                {
                                    Ok(WebSocketSendOutcome::Sent) => {
                                        turn_state = retry_state;
                                        media_rectifier_retried = true;
                                        received_response_event = false;
                                        relayed_response_event = false;
                                        turn_started = retry_started;
                                        first_token_ms = None;
                                        last_response_event_at = retry_started;
                                        continue;
                                    }
                                    Ok(WebSocketSendOutcome::Shutdown) => {
                                        finish_websocket_shutdown(
                                            downstream,
                                            &mut turn_accounting,
                                        )
                                        .await;
                                        return Ok(());
                                    }
                                    Err(error) => {
                                        let failure_message = error.to_string();
                                        if let Some(accounting) = turn_accounting.take() {
                                            accounting
                                                .finish_provider_attempt_failure(
                                                    failure_message.clone(),
                                                )
                                                .await;
                                        }
                                        media_retry_write_failure = Some(failure_message);
                                    }
                                }
                            }
                        }
                        if (media_retry_write_failure.is_some()
                            || terminal_data.as_ref().is_some_and(|(outcome, _, _)| {
                                *outcome == WebSocketTerminalOutcome::ProviderFailure
                            }))
                            && !relayed_response_event
                        {
                            let failure_message = media_retry_write_failure.take().unwrap_or_else(|| {
                                format!(
                                    "upstream WebSocket terminal event: {}",
                                    websocket_event_type(&text)
                                        .as_deref()
                                        .unwrap_or("provider failure")
                                )
                            });
                            let mut failed_usage = if terminal_usage_already_logged {
                                None
                            } else {
                                terminal_data.as_ref().and_then(|(_, status_code, event)| {
                                    event.clone().map(|event| (*status_code, event))
                                })
                            };
                            if let Some(accounting) = turn_accounting.take() {
                                accounting
                                    .finish_provider_attempt_failure(failure_message.clone())
                                    .await;
                            }

                            if let Some((status_code, event)) = failed_usage.take() {
                                spawn_codex_websocket_usage(
                                    state,
                                    &provider,
                                    &turn_state,
                                    event,
                                    status_code,
                                    turn_started.elapsed().as_millis() as u64,
                                    first_token_ms,
                                );
                            }
                            let allow_provider_fallback =
                                !response_create_has_provider_cursor(&original_response_create);
                            let mut retried = false;
                            if allow_provider_fallback {
                                match connect_pre_relay_fallback(
                                    PreRelayFallbackRequest {
                                        state,
                                        headers,
                                        turn_context: &turn_context,
                                        turn_providers: &turn_providers,
                                        provider_attempt_limit,
                                        original_response_create: &original_response_create,
                                        timeout_config,
                                    },
                                    &mut provider_index,
                                    &mut provider_attempts,
                                    &mut shutdown_rx,
                                )
                                .await?
                                {
                                    PreRelayFallbackOutcome::Connected(connection) => {
                                        let PreRelayFallbackConnection {
                                            provider: retry_provider,
                                            proxy_url,
                                            upstream: retry_upstream,
                                            turn_state: retry_state,
                                            accounting: retry_accounting,
                                            started: retry_started,
                                        } = *connection;
                                        provider = retry_provider;
                                        retain_fallback_provider =
                                            provider.id != turn_context.provider.id;
                                        upstream_proxy_url = proxy_url;
                                        upstream = retry_upstream;
                                        turn_state = retry_state;
                                        turn_accounting = Some(retry_accounting);
                                        media_rectifier_retried = false;
                                        response_in_flight = true;
                                        turn_started = retry_started;
                                        first_token_ms = None;
                                        received_response_event = false;
                                        relayed_response_event = false;
                                        last_response_event_at = retry_started;
                                        retried = true;
                                    }
                                    PreRelayFallbackOutcome::Shutdown(accounting) => {
                                        turn_accounting = Some(*accounting);
                                        finish_websocket_shutdown(
                                            downstream,
                                            &mut turn_accounting,
                                        )
                                        .await;
                                        return Ok(());
                                    }
                                    PreRelayFallbackOutcome::Exhausted => {}
                                }
                            }
                            if retried {
                                continue;
                            }
                            if let Some((status_code, event)) = failed_usage {
                                spawn_codex_websocket_usage(
                                    state,
                                    &provider,
                                    &turn_state,
                                    event,
                                    status_code,
                                    turn_started.elapsed().as_millis() as u64,
                                    first_token_ms,
                                );
                            }
                            record_websocket_status_failure(state, failure_message.clone()).await;
                            return Err(ProxyError::ForwardFailed(format!(
                                "{failure_message}; no eligible Responses WebSocket fallback remains"
                            )));
                        }
                        if let Some((_, status_code, Some(event))) = terminal_data.as_ref() {
                            spawn_codex_websocket_usage(
                                state,
                                &provider,
                                &turn_state,
                                event.clone(),
                                *status_code,
                                turn_started.elapsed().as_millis() as u64,
                                first_token_ms,
                            );
                        }
                        let text = restore_upstream_text(text, &turn_state);
                        let write_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        if send_downstream_message_with_accounting(
                            downstream,
                            &mut turn_accounting,
                            DownstreamMessage::Text(text),
                            write_deadline,
                            &mut shutdown_rx,
                            "text frame write",
                        )
                        .await?
                            == WebSocketSendOutcome::Shutdown
                        {
                            finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                            break;
                        }
                        relayed_response_event = true;
                        if let Some((outcome, _, _)) = terminal_data {
                            if let Some(accounting) = turn_accounting.take() {
                                let message = format!(
                                    "upstream WebSocket terminal event: {}",
                                    websocket_event_type_from_outcome(outcome)
                                );
                                match outcome {
                                    WebSocketTerminalOutcome::Success => {
                                        accounting.finish_success().await;
                                    }
                                    WebSocketTerminalOutcome::NeutralFailure => {
                                        accounting.finish_neutral_failure(message).await;
                                    }
                                    WebSocketTerminalOutcome::ProviderFailure => {
                                        accounting.finish_provider_failure_detached(message).await;
                                    }
                                }
                            }
                            response_in_flight = false;
                        }
                    }
                    UpstreamMessage::Binary(_) => {
                        if !response_in_flight {
                            return Err(ProxyError::ForwardFailed(
                                "upstream WebSocket sent binary data while no response was in flight"
                                    .to_string(),
                            ));
                        }
                        let message =
                            "upstream WebSocket sent binary data during an active response"
                                .to_string();
                        if let Some(accounting) = turn_accounting.take() {
                            accounting
                                .finish_provider_failure_detached(message.clone())
                                .await;
                        }
                        return Err(ProxyError::ForwardFailed(message));
                    }
                    UpstreamMessage::Ping(data) => {
                        let write_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        if send_downstream_message_with_accounting(
                            downstream,
                            &mut turn_accounting,
                            DownstreamMessage::Ping(data),
                            write_deadline,
                            &mut shutdown_rx,
                            "ping frame write",
                        )
                        .await?
                            == WebSocketSendOutcome::Shutdown
                        {
                            finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                            break;
                        }
                    }
                    UpstreamMessage::Pong(data) => {
                        let write_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        if send_downstream_message_with_accounting(
                            downstream,
                            &mut turn_accounting,
                            DownstreamMessage::Pong(data),
                            write_deadline,
                            &mut shutdown_rx,
                            "pong frame write",
                        )
                        .await?
                            == WebSocketSendOutcome::Shutdown
                        {
                            finish_websocket_shutdown(downstream, &mut turn_accounting).await;
                            break;
                        }
                    }
                    UpstreamMessage::Close(frame) => {
                        if response_in_flight {
                            return Err(premature_upstream_close_error(
                                &mut turn_accounting,
                                "upstream WebSocket sent a close frame before a terminal response",
                            )
                            .await);
                        }
                        let frame = frame.map(|frame| DownstreamCloseFrame {
                            code: u16::from(frame.code),
                            reason: Cow::Owned(frame.reason.into_owned()),
                        });
                        let close_deadline = websocket_turn_timeout_deadline(
                            response_in_flight,
                            received_response_event,
                            turn_started,
                            last_response_event_at,
                            timeout_config,
                        );
                        let _ = send_downstream_message(
                            downstream,
                            DownstreamMessage::Close(frame),
                            close_deadline,
                            &mut shutdown_rx,
                            "close frame write",
                        )
                        .await;
                        break;
                    }
                    UpstreamMessage::Frame(_) => {}
                }
            }
        }
    }

    Ok(())
}

fn websocket_transport_failure_event(message: String) -> UpstreamMessage {
    UpstreamMessage::Text(
        json!({
            "type": "response.failed",
            "response": {
                "error": {
                    "type": "websocket_transport_error",
                    "message": message
                }
            }
        })
        .to_string(),
    )
}

async fn premature_upstream_close_error(
    turn_accounting: &mut Option<WebSocketTurnAccounting>,
    message: &str,
) -> ProxyError {
    if let Some(accounting) = turn_accounting.take() {
        accounting
            .finish_provider_failure_detached(message.to_string())
            .await;
    }
    ProxyError::ForwardFailed(format!("{message}; falling back to HTTP/SSE"))
}
fn spawn_codex_websocket_usage(
    state: &ProxyState,
    provider: &Provider,
    turn_state: &TurnTransformState,
    event: Value,
    status_code: u16,
    latency_ms: u64,
    first_token_ms: Option<u64>,
) {
    let logging_state = state.clone();
    let provider_id = provider.id.clone();
    let request_model = turn_state.request_model.clone();
    let outbound_model = turn_state.outbound_model.clone();
    let session_id = turn_state.session_id.clone();
    tokio::spawn(async move {
        log_codex_websocket_usage(
            &logging_state,
            &provider_id,
            &request_model,
            &outbound_model,
            &event,
            status_code,
            latency_ms,
            first_token_ms,
            &session_id,
        )
        .await;
    });
}
async fn connect_upstream_with_shutdown(
    request: http::Request<()>,
    proxy_url: Option<&str>,
    configured_deadline: Option<Instant>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<UpstreamSocket>, ProxyError> {
    if *shutdown_rx.borrow() {
        return Ok(None);
    }
    let hard_deadline = Instant::now() + UPSTREAM_CONNECT_TIMEOUT;
    let handshake_deadline = configured_deadline
        .map(|deadline| deadline.min(hard_deadline))
        .unwrap_or(hard_deadline);
    let connect = connect_upstream_websocket(request, proxy_url);
    tokio::pin!(connect);
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(None);
                }
            }
            _ = tokio::time::sleep_until(handshake_deadline) => {
                return Err(ProxyError::Timeout(
                    "upstream WebSocket handshake timed out".to_string(),
                ));
            }
            result = &mut connect => {
                return result
                    .map(Some)
                    .map_err(|error| {
                        ProxyError::ForwardFailed(format!(
                            "upstream WebSocket handshake failed: {error}"
                        ))
                    });
            }
        }
    }
}

async fn send_downstream_message(
    downstream: &mut WebSocket,
    message: DownstreamMessage,
    deadline: Option<Instant>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    description: &str,
) -> Result<WebSocketSendOutcome, ProxyError> {
    #[cfg(test)]
    if description == "text frame write"
        && FAIL_NEXT_DOWNSTREAM_TERMINAL_WRITE.swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(ProxyError::ForwardFailed(
            "downstream WebSocket text frame write failed: injected terminal relay failure"
                .to_string(),
        ));
    }
    let send = downstream.send(message);
    tokio::pin!(send);
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(WebSocketSendOutcome::Shutdown);
                }
            }
            _ = async {
                if let Some(deadline) = deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                return Err(ProxyError::Timeout(format!(
                    "downstream WebSocket {description} timed out"
                )));
            }
            result = &mut send => {
                return result
                    .map(|_| WebSocketSendOutcome::Sent)
                    .map_err(|error| {
                        ProxyError::ForwardFailed(format!(
                            "downstream WebSocket {description} failed: {error}"
                        ))
                    });
            }
        }
    }
}
async fn finish_fallback_transform_error(accounting: WebSocketTurnAccounting, _error: &ProxyError) {
    accounting.finish_neutral_attempt().await;
}

async fn finish_downstream_write_failure(
    turn_accounting: &mut Option<WebSocketTurnAccounting>,
    error: &ProxyError,
) {
    if let Some(accounting) = turn_accounting.take() {
        accounting.finish_neutral_failure(error.to_string()).await;
    }
}
async fn send_downstream_message_with_accounting(
    downstream: &mut WebSocket,
    turn_accounting: &mut Option<WebSocketTurnAccounting>,
    message: DownstreamMessage,
    deadline: Option<Instant>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    description: &str,
) -> Result<WebSocketSendOutcome, ProxyError> {
    match send_downstream_message(downstream, message, deadline, shutdown_rx, description).await {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            finish_downstream_write_failure(turn_accounting, &error).await;
            Err(error)
        }
    }
}
fn websocket_turn_timeout_deadline(
    response_in_flight: bool,
    received_response_event: bool,
    turn_started: Instant,
    last_response_event_at: Instant,
    timeout_config: StreamingTimeoutConfig,
) -> Option<Instant> {
    if !response_in_flight {
        return None;
    }
    let (timeout_secs, anchor) = if received_response_event {
        (timeout_config.idle_timeout, last_response_event_at)
    } else {
        (timeout_config.first_byte_timeout, turn_started)
    };
    (timeout_secs > 0).then(|| anchor + Duration::from_secs(timeout_secs))
}

#[cfg(test)]
static FAIL_NEXT_REUSED_TURN_WRITE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static FAIL_NEXT_RELAY_WRITE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static FAIL_NEXT_DOWNSTREAM_TERMINAL_WRITE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static FAIL_NEXT_MEDIA_RETRY_WRITE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
async fn send_upstream_turn_message(
    upstream: &mut UpstreamSocket,
    message: UpstreamMessage,
    deadline: Option<Instant>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    description: &str,
    turn_accounting: &mut Option<WebSocketTurnAccounting>,
) -> Result<WebSocketSendOutcome, ProxyError> {
    let result = send_upstream_message(upstream, message, deadline, shutdown_rx, description).await;
    if let Err(error) = &result {
        if let Some(accounting) = turn_accounting.take() {
            accounting
                .finish_provider_failure_detached(error.to_string())
                .await;
        }
    }
    result
}

async fn send_upstream_message(
    upstream: &mut UpstreamSocket,
    message: UpstreamMessage,
    deadline: Option<Instant>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    description: &str,
) -> Result<WebSocketSendOutcome, ProxyError> {
    #[cfg(test)]
    if description == "text frame write"
        && FAIL_NEXT_REUSED_TURN_WRITE.swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(ProxyError::ForwardFailed(
            "upstream WebSocket text frame write failed: injected stale socket".to_string(),
        ));
    }
    #[cfg(test)]
    if description == "ping frame write"
        && FAIL_NEXT_RELAY_WRITE.swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(ProxyError::ForwardFailed(
            "upstream WebSocket ping frame write failed: injected relay failure".to_string(),
        ));
    }
    #[cfg(test)]
    if description == "media fallback event write"
        && FAIL_NEXT_MEDIA_RETRY_WRITE.swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(ProxyError::ForwardFailed(
            "upstream WebSocket media fallback event write failed: injected stale socket"
                .to_string(),
        ));
    }

    let send = upstream.send(message);
    tokio::pin!(send);
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(WebSocketSendOutcome::Shutdown);
                }
            }
            _ = async {
                if let Some(deadline) = deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                return Err(ProxyError::Timeout(format!(
                    "upstream WebSocket {description} timed out"
                )));
            }
            result = &mut send => {
                return result
                    .map(|_| WebSocketSendOutcome::Sent)
                    .map_err(|error| {
                        ProxyError::ForwardFailed(format!(
                            "upstream WebSocket {description} failed: {error}"
                        ))
                    });
            }
        }
    }
}

async fn finish_websocket_shutdown(
    downstream: &mut WebSocket,
    turn_accounting: &mut Option<WebSocketTurnAccounting>,
) {
    if let Some(accounting) = turn_accounting.take() {
        accounting
            .finish_neutral_failure("CC-Switch proxy stopping".to_string())
            .await;
    }
    send_proxy_shutdown_close(downstream).await;
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
    let _ = tokio::time::timeout(
        DOWNSTREAM_CLOSE_TIMEOUT,
        downstream.send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
            code: 1001,
            reason: Cow::Borrowed("CC-Switch proxy stopping"),
        }))),
    )
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
fn response_create_has_provider_cursor(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|event| response_create_body(&event).ok())
        .and_then(|body| body.get("previous_response_id").cloned())
        .is_some_and(|value| !value.is_null())
}
fn reject_queued_upstream_data_before_next_turn(
    upstream: &mut UpstreamSocket,
) -> Result<(), ProxyError> {
    loop {
        let Some(message) = upstream.next().now_or_never() else {
            return Ok(());
        };
        match message {
            Some(Ok(UpstreamMessage::Ping(_) | UpstreamMessage::Pong(_))) => continue,
            Some(Ok(UpstreamMessage::Text(text))) => {
                let event_type =
                    websocket_event_type(&text).unwrap_or_else(|| "Responses event".to_string());
                return Err(ProxyError::ForwardFailed(format!(
                    "upstream WebSocket queued stale {event_type} from the previous turn"
                )));
            }
            Some(Ok(UpstreamMessage::Binary(_))) => {
                return Err(ProxyError::ForwardFailed(
                    "upstream WebSocket queued stale binary data from the previous turn"
                        .to_string(),
                ));
            }
            Some(Ok(UpstreamMessage::Close(_))) | None => {
                return Err(ProxyError::ForwardFailed(
                    "upstream WebSocket closed between Responses turns".to_string(),
                ));
            }
            Some(Err(error)) => {
                return Err(ProxyError::ForwardFailed(format!(
                    "upstream WebSocket failed between Responses turns: {error}"
                )));
            }
            Some(Ok(_)) => continue,
        }
    }
}

fn transform_client_text(
    text: &str,
    provider: &Provider,
    rectifier_config: &RectifierConfig,
    require_response_create: bool,
) -> Result<Option<(String, Option<TurnTransformState>)>, ProxyError> {
    #[cfg(test)]
    TRANSFORM_CLIENT_TEXT_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

fn websocket_text_is_responses_event(text: &str) -> bool {
    let Ok(Value::Object(event)) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    event
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|event_type| {
            event_type == "error"
                || (event_type != "response.create" && event_type.starts_with("response."))
        })
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

fn websocket_event_type_from_outcome(outcome: WebSocketTerminalOutcome) -> &'static str {
    match outcome {
        WebSocketTerminalOutcome::Success => "success",
        WebSocketTerminalOutcome::NeutralFailure => "client failure",
        WebSocketTerminalOutcome::ProviderFailure => "provider failure",
    }
}
fn websocket_terminal_outcome(text: &str) -> WebSocketTerminalOutcome {
    match websocket_event_type(text).as_deref() {
        Some("response.completed" | "response.incomplete") => WebSocketTerminalOutcome::Success,
        Some("response.failed" | "error") if websocket_event_is_client_error(text) => {
            WebSocketTerminalOutcome::NeutralFailure
        }
        _ => WebSocketTerminalOutcome::ProviderFailure,
    }
}

fn websocket_event_is_client_error(text: &str) -> bool {
    const NON_RETRYABLE_STATUSES: &[u16] = &[400, 405, 406, 413, 414, 415, 422];
    const CLIENT_ERROR_MARKERS: &[&str] = &[
        "invalid_request_error",
        "invalid_request",
        "bad_request",
        "unprocessable_entity",
        "invalid_prompt",
        "context_length_exceeded",
        "invalid_parameter",
        "invalid_parameters",
        "invalid_tool_schema",
        "unsupported_value",
        "missing_required_parameter",
        "unknown_parameter",
    ];

    let Ok(event) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    if websocket_error_status(&event).is_some_and(|status| NON_RETRYABLE_STATUSES.contains(&status))
    {
        return true;
    }

    let client_error = websocket_error_values(&event).any(|value| {
        value
            .as_str()
            .map(str::to_ascii_lowercase)
            .is_some_and(|value| CLIENT_ERROR_MARKERS.contains(&value.as_str()))
    });
    client_error
}

fn websocket_terminal_status_code(text: &str, outcome: WebSocketTerminalOutcome) -> u16 {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|event| websocket_error_status(&event))
        .unwrap_or(match outcome {
            WebSocketTerminalOutcome::Success => 200,
            WebSocketTerminalOutcome::NeutralFailure => 400,
            WebSocketTerminalOutcome::ProviderFailure => 500,
        })
}

fn websocket_error_status(event: &Value) -> Option<u16> {
    let error = event
        .get("error")
        .or_else(|| event.pointer("/response/error"));
    [error, Some(event)]
        .into_iter()
        .flatten()
        .flat_map(|value| [value.get("status"), value.get("status_code")])
        .flatten()
        .find_map(|value| {
            value
                .as_u64()
                .and_then(|status| u16::try_from(status).ok())
                .or_else(|| value.as_str().and_then(|status| status.parse().ok()))
                .filter(|status| (100..=599).contains(status))
        })
}

fn websocket_error_values(event: &Value) -> impl Iterator<Item = &Value> {
    let error = event
        .get("error")
        .or_else(|| event.pointer("/response/error"));
    [error, Some(event)]
        .into_iter()
        .flatten()
        .flat_map(|value| [value.get("type"), value.get("code")])
        .flatten()
}

fn next_websocket_provider(
    providers: &[Provider],
    provider_index: &mut usize,
    provider_attempts: usize,
    provider_attempt_limit: usize,
) -> Option<Provider> {
    while *provider_index + 1 < providers.len() && provider_attempts < provider_attempt_limit {
        *provider_index += 1;
        let candidate = providers[*provider_index].clone();
        if codex_provider_supports_responses_websocket(&candidate) {
            return Some(candidate);
        }
    }
    None
}
fn websocket_provider_attempt_limit(ctx: &RequestContext) -> usize {
    if ctx.app_config.auto_failover_enabled {
        ctx.app_config.max_retries as usize + 1
    } else {
        1
    }
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
    proxy_url: Option<&str>,
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

    let system_proxy_url = proxy_url
        .is_none()
        .then(|| super::http_client::get_system_proxy_url(&target_url))
        .flatten();
    let proxy_url = proxy_url.or(system_proxy_url.as_deref());

    let stream: BoxedIo = match proxy_url {
        Some(proxy_url) => {
            let parsed = Url::parse(proxy_url).map_err(|error| {
                ProxyError::ConfigError(format!("invalid configured proxy URL: {error}"))
            })?;
            match parsed.scheme() {
                "http" | "https" => Box::new(
                    super::hyper_client::connect_via_proxy(proxy_url, target_host, target_port)
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
        .then(|| Connector::Rustls(super::hyper_client::global_tls_client_config()));
    let (socket, _) =
        client_async_tls_with_config(request, stream, Some(websocket_config), connector)
            .await
            .map_err(|error| {
                ProxyError::ForwardFailed(format!("WebSocket protocol handshake failed: {error}"))
            })?;
    Ok(socket)
}

enum Socks5Target {
    Ip(std::net::IpAddr),
    Domain(String),
}

async fn connect_via_socks5(
    proxy_url: &Url,
    target_host: &str,
    target_port: u16,
) -> Result<tokio::net::TcpStream, ProxyError> {
    let targets = if let Ok(address) = target_host.parse::<std::net::IpAddr>() {
        vec![Socks5Target::Ip(address)]
    } else if proxy_url.scheme() == "socks5h" {
        vec![Socks5Target::Domain(target_host.to_string())]
    } else {
        let addresses = tokio::net::lookup_host((target_host, target_port))
            .await
            .map_err(|error| {
                ProxyError::ForwardFailed(format!("SOCKS target DNS lookup failed: {error}"))
            })?
            .map(|address| address.ip())
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(ProxyError::ForwardFailed(
                "SOCKS target DNS lookup returned no address".to_string(),
            ));
        }
        addresses.into_iter().map(Socks5Target::Ip).collect()
    };

    let mut last_error = None;
    for target in targets {
        match connect_via_socks5_target(proxy_url, &target, target_port).await {
            Ok(stream) => return Ok(stream),
            Err(error @ (ProxyError::AuthError(_) | ProxyError::ConfigError(_))) => {
                return Err(error);
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error
        .unwrap_or_else(|| ProxyError::ForwardFailed("SOCKS target connection failed".to_string())))
}

async fn connect_via_socks5_target(
    proxy_url: &Url,
    target: &Socks5Target,
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
        let username = percent_decode_str(proxy_url.username()).collect::<Vec<_>>();
        let password = percent_decode_str(proxy_url.password().unwrap_or("")).collect::<Vec<_>>();
        if username.len() > u8::MAX as usize || password.len() > u8::MAX as usize {
            return Err(ProxyError::ConfigError(
                "SOCKS proxy credentials are too long".to_string(),
            ));
        }
        let mut auth = vec![0x01, username.len() as u8];
        auth.extend_from_slice(&username);
        auth.push(password.len() as u8);
        auth.extend_from_slice(&password);
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
    match target {
        Socks5Target::Ip(std::net::IpAddr::V4(ip)) => {
            request.push(0x01);
            request.extend_from_slice(&ip.octets());
        }
        Socks5Target::Ip(std::net::IpAddr::V6(ip)) => {
            request.push(0x04);
            request.extend_from_slice(&ip.octets());
        }
        Socks5Target::Domain(host) => {
            let host = host.as_bytes();
            if host.len() > u8::MAX as usize {
                return Err(ProxyError::ConfigError(
                    "WebSocket target hostname is too long for SOCKS5".to_string(),
                ));
            }
            request.push(0x03);
            request.push(host.len() as u8);
            request.extend_from_slice(host);
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
    let _ = tokio::time::timeout(
        DOWNSTREAM_CLOSE_TIMEOUT,
        downstream.send(DownstreamMessage::Text(event.to_string())),
    )
    .await;
    let _ = tokio::time::timeout(
        DOWNSTREAM_CLOSE_TIMEOUT,
        downstream.send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
            code: 1011,
            reason: Cow::Borrowed("CC-Switch upstream WebSocket failure"),
        }))),
    )
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
        net::{TcpListener, TcpSocket},
        sync::oneshot,
    };
    use tokio_rustls::TlsAcceptor;
    use tokio_tungstenite::{
        accept_async_with_config, accept_hdr_async, client_async_with_config, connect_async,
        connect_async_with_config,
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

    fn non_websocket_provider_with_id(id: &str, base_url: String) -> Provider {
        let mut provider = Provider::with_id(
            id.to_string(),
            format!("Non-WebSocket Provider {id}"),
            json!({
                "base_url": base_url,
                "supports_websockets": false,
                "env": {"OPENAI_API_KEY": "provider-secret"}
            }),
            None,
        );
        provider.category = Some("custom".to_string());
        provider
    }
    fn accounting_for_tests(server: &ProxyServer, provider: &Provider) -> WebSocketTurnAccounting {
        WebSocketTurnAccounting {
            state: server.state_for_tests(),
            provider_id: provider.id.clone(),
            provider_name: provider.name.clone(),
            current_provider_id_at_start: provider.id.clone(),
            used_half_open_permit: false,
            _active_guard: None,
            finalized: false,
        }
    }

    #[tokio::test]
    #[serial]
    async fn fallback_transform_error_helper_is_neutral() {
        let db = Arc::new(Database::memory().unwrap());
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        db.save_provider("codex", &provider).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(ProxyConfig::default(), db, None);
        let router = server.provider_router_for_tests();

        server.state_for_tests().status.write().await.total_requests = 1;

        for message in ["namespace collision", "tool collision"] {
            finish_fallback_transform_error(
                accounting_for_tests(&server, &provider),
                &ProxyError::InvalidRequest(message.to_string()),
            )
            .await;
        }

        let stats = router
            .get_circuit_breaker_stats(&provider.id, "codex")
            .await;
        assert!(
            stats.is_none()
                || stats.is_some_and(
                    |stats| stats.state == crate::proxy::circuit_breaker::CircuitState::Closed
                )
        );

        let state = server.state_for_tests();
        assert_eq!(
            state.status.read().await.failed_requests,
            0,
            "fallback transform attempts must not update request-level failure counters"
        );
        record_websocket_status_failure(&state, "fallbacks exhausted".to_string()).await;
        let status = state.status.read().await;
        assert_eq!(status.total_requests, 1);
        assert_eq!(status.failed_requests, 1);
    }

    #[tokio::test]
    #[serial]
    async fn downstream_write_error_helper_is_neutral() {
        let db = Arc::new(Database::memory().unwrap());
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        db.save_provider("codex", &provider).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(ProxyConfig::default(), db, None);
        let router = server.provider_router_for_tests();
        let mut accounting = Some(accounting_for_tests(&server, &provider));

        finish_downstream_write_failure(
            &mut accounting,
            &ProxyError::ForwardFailed("client disconnected".to_string()),
        )
        .await;

        let stats = router
            .get_circuit_breaker_stats(&provider.id, "codex")
            .await;
        assert!(
            stats.is_none()
                || stats.is_some_and(
                    |stats| stats.state == crate::proxy::circuit_breaker::CircuitState::Closed
                )
        );
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
    fn upstream_response_create_is_not_a_server_event() {
        assert!(!websocket_text_is_responses_event(
            &json!({"type": "response.create", "model": "echoed-client-request"}).to_string()
        ));
        assert!(websocket_text_is_responses_event(
            &json!({"type": "error"}).to_string()
        ));
        assert!(websocket_text_is_responses_event(
            &json!({"type": "response.completed"}).to_string()
        ));
    }

    #[test]
    fn status_501_terminal_event_is_a_provider_failure() {
        let event = json!({
            "type": "response.failed",
            "response": {
                "status": "failed",
                "error": {"status_code": 501, "message": "not implemented"}
            }
        })
        .to_string();
        assert_eq!(
            websocket_terminal_outcome(&event),
            WebSocketTerminalOutcome::ProviderFailure
        );
    }

    #[tokio::test]
    async fn zero_attempt_later_turn_exhaustion_counts_rejected_request() {
        let db = Arc::new(Database::memory().unwrap());
        let server = ProxyServer::new(ProxyConfig::default(), db, None);
        let state = server.state_for_tests();

        let error =
            finalize_later_turn_provider_exhaustion(&state, ProxyError::NoAvailableProvider, 0)
                .await;
        let status = state.status.read().await;

        assert!(matches!(error, ProxyError::NoAvailableProvider));
        assert_eq!(status.total_requests, 1);
        assert_eq!(status.failed_requests, 1);
        assert!(status.last_request_at.is_some());
        assert!(status.last_error.is_some());
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
    async fn rejects_non_loopback_browser_origin_before_websocket_upgrade() {
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
        let mut request = format!("ws://127.0.0.1:{}/v1/responses", info.port)
            .into_client_request()
            .expect("build local websocket request");
        request.headers_mut().insert(
            http::header::ORIGIN,
            http::HeaderValue::from_static("https://attacker.example"),
        );

        let status = match connect_async(request).await {
            Err(WebSocketError::Http(response)) => response.status(),
            Err(error) => panic!("expected HTTP origin rejection, got {error}"),
            Ok((socket, _)) => {
                drop(socket);
                panic!("untrusted browser origin completed the websocket upgrade")
            }
        };

        assert_eq!(status, http::StatusCode::FORBIDDEN);
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
    async fn configured_first_byte_timeout_bounds_upstream_websocket_handshake() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled-handshake upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener
                .accept()
                .await
                .expect("accept upstream TCP");
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
            drop(stream);
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
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("set websocket first-byte timeout");

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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        accepted_rx.await.expect("upstream TCP accept signal");

        let result = tokio::time::timeout(Duration::from_secs(3), next_text(&mut client)).await;
        drop(client);
        upstream_task.abort();
        let _ = upstream_task.await;
        server.stop().await.expect("stop proxy");

        let error: Value = serde_json::from_str(
            &result.expect("configured first-byte timeout did not bound websocket handshake"),
        )
        .expect("timeout error event JSON");
        assert_eq!(error["type"], "error");
        assert!(error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("handshake timed out")));
    }
    #[tokio::test]
    #[serial]
    async fn first_response_event_timeout_retries_websocket_fallback() {
        let stalled_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled_addr = stalled_listener.local_addr().unwrap();
        let stalled_task = tokio::spawn(async move {
            let (stream, _) = stalled_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (fallback_tx, fallback_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let used = match tokio::time::timeout(
                Duration::from_secs(3),
                fallback_listener.accept(),
            )
            .await
            {
                Ok(Ok((stream, _))) => {
                    let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    let _ = next_text(&mut websocket).await;
                    websocket
                        .send(UpstreamMessage::Text(
                            json!({"type":"response.completed","response":{"id":"resp-timeout-fallback","output":[]}}).to_string(),
                        ))
                        .await
                        .unwrap();
                    true
                }
                _ => false,
            };
            let _ = fallback_tx.send(used);
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut stalled =
            websocket_provider_with_id("ws-first-event-stalled", format!("http://{stalled_addr}"));
        stalled.sort_index = Some(0);
        let mut fallback = websocket_provider_with_id(
            "ws-first-event-fallback",
            format!("http://{fallback_addr}"),
        );
        fallback.sort_index = Some(1);
        for provider in [&stalled, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &stalled.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.streaming_first_byte_timeout = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        let fallback_used = fallback_rx.await.unwrap();

        assert!(
            fallback_used,
            "first-event timeout did not try the fallback provider"
        );
        assert_eq!(terminal["type"], "response.completed", "{terminal}");
        assert_eq!(terminal["response"]["id"], "resp-timeout-fallback");

        drop(client);
        stalled_task.abort();
        let _ = stalled_task.await;
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn timeout_failure_does_not_block_shutdown_on_health_persistence() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (request_tx, request_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            let _ = request_tx.send(());
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let db_for_lock = db.clone();
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
        app_config.circuit_failure_threshold = 1;
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
        let router = server.provider_router_for_tests();
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

        request_rx.await.expect("upstream received request");
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let lock_task = std::thread::spawn(move || {
            let _guard = db_for_lock.conn.lock().expect("lock database");
            locked_tx.send(()).expect("signal database lock");
            release_rx.recv().expect("release database lock");
        });
        locked_rx.recv().expect("wait for database lock");

        let read_task = tokio::spawn(async move { next_text(&mut client).await });
        std::thread::sleep(Duration::from_millis(1500));
        let error_result = tokio::time::timeout(Duration::from_secs(1), read_task).await;
        let stop_result = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
        release_tx.send(()).expect("release database lock");
        lock_task.join().expect("database lock task");

        let error_text = error_result
            .expect("timeout persistence blocked downstream timeout delivery")
            .expect("timeout reader task");
        let error: Value = serde_json::from_str(&error_text).expect("timeout error event JSON");
        assert_eq!(error["type"], "error");
        assert!(
            error["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("first response event timed out")),
            "unexpected timeout error: {error}"
        );
        let stats = router
            .get_circuit_breaker_stats(&provider.id, "codex")
            .await
            .expect("provider breaker finalized before timeout delivery");
        assert_eq!(
            stats.state,
            crate::proxy::circuit_breaker::CircuitState::Open
        );
        let status = server.get_status().await;
        assert_eq!(status.failed_requests, 1);

        upstream_task.abort();
        let _ = upstream_task.await;
        stop_result
            .expect("timeout persistence blocked proxy shutdown")
            .expect("stop proxy");
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
    async fn retries_websocket_turn_after_reactive_unsupported_image_error() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind reactive media upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (events_tx, events_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let first = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type":"response.failed",
                        "response":{
                            "id":"resp-media-failed",
                            "model":"upstream-model",
                            "error":{
                                "status":400,
                                "type":"invalid_request_error",
                                "message":"image inputs are not supported"
                            },
                            "usage":{"input_tokens":5,"output_tokens":1}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send unsupported-image failure");
            let retry = tokio::time::timeout(Duration::from_millis(500), next_text(&mut websocket))
                .await
                .ok();
            if retry.is_some() {
                websocket
                    .send(UpstreamMessage::Text(
                        json!({"type":"response.completed","response":{"id":"resp-media-retry","model":"upstream-model","output":[],"usage":{"input_tokens":7,"output_tokens":3}}})
                            .to_string(),
                    ))
                    .await
                    .expect("send retry completion");
            }
            let _ = events_tx.send((first, retry));
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

        let terminal: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("retry completion JSON");
        let (first, retry) = events_rx.await.expect("upstream events");
        let first: Value = serde_json::from_str(&first).expect("first event JSON");
        let retry = retry.expect("rectified retry reached upstream");
        let retry: Value = serde_json::from_str(&retry).expect("retry event JSON");
        assert_eq!(first["input"][0]["content"][0]["type"], "input_image");
        assert_eq!(retry["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(
            retry["input"][0]["content"][0]["text"],
            crate::proxy::media_sanitizer::UNSUPPORTED_IMAGE_MARKER
        );
        assert_eq!(terminal["type"], "response.completed");

        let rows = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let rows = {
                    let conn = db_for_assert.conn.lock().expect("lock database");
                    let mut statement = conn
                        .prepare(
                            "SELECT status_code, input_tokens, output_tokens FROM proxy_request_logs WHERE provider_id = 'ws-provider' AND app_type = 'codex'",
                        )
                        .expect("prepare usage query");
                    statement
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
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
        .expect("timed out waiting for both media-attempt usage rows");
        assert!(rows.contains(&(400, 5, 1)));
        assert!(rows.contains(&(200, 7, 3)));

        upstream_task.await.expect("reactive media upstream task");
        server.stop().await.expect("stop proxy");
    }
    #[tokio::test]
    #[serial]
    async fn media_retry_write_failure_uses_next_provider_after_sync_accounting() {
        let primary_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind primary media upstream");
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.expect("accept primary");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept primary websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type":"response.failed",
                        "response":{
                            "id":"resp-media-write-failed",
                            "model":"primary-model",
                            "error":{
                                "status":400,
                                "type":"invalid_request_error",
                                "message":"image inputs are not supported"
                            },
                            "usage":{"input_tokens":5,"output_tokens":1}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send unsupported-image failure");
        });

        let fallback_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fallback media upstream");
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (fallback_received_tx, fallback_received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.expect("accept fallback");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept fallback websocket");
            let retry = next_text(&mut websocket).await;
            let _ = fallback_received_tx.send(retry);
            let _ = release_rx.await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-media-fallback","model":"fallback-model","output":[],"usage":{"input_tokens":7,"output_tokens":3}}})
                        .to_string(),
                ))
                .await
                .expect("send fallback completion");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let db_for_assert = db.clone();
        let mut primary =
            websocket_provider_with_id("ws-media-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-media-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).expect("save provider");
            db.add_to_failover_queue("codex", &provider.id)
                .expect("queue provider");
        }
        db.set_current_provider("codex", &primary.id)
            .expect("select primary provider");
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
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        FAIL_NEXT_MEDIA_RETRY_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);
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

        let retry = tokio::time::timeout(Duration::from_secs(2), fallback_received_rx)
            .await
            .expect("media retry did not reach fallback")
            .expect("fallback retry sender dropped");
        let retry: Value = serde_json::from_str(&retry).expect("fallback retry JSON");
        assert_eq!(retry["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(
            retry["input"][0]["content"][0]["text"],
            crate::proxy::media_sanitizer::UNSUPPORTED_IMAGE_MARKER
        );
        let stats = router
            .get_circuit_breaker_stats(&primary.id, "codex")
            .await
            .expect("primary breaker finalized before fallback replay");
        assert_eq!(
            stats.state,
            crate::proxy::circuit_breaker::CircuitState::Open
        );
        let _ = release_tx.send(());
        let terminal: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("fallback completion JSON");
        assert_eq!(terminal["response"]["id"], "resp-media-fallback");

        tokio::time::sleep(Duration::from_millis(100)).await;
        let rows = {
            let conn = db_for_assert.conn.lock().expect("lock database");
            let mut statement = conn
                .prepare("SELECT status_code FROM proxy_request_logs WHERE provider_id IN ('ws-media-primary', 'ws-media-fallback') AND app_type = 'codex'")
                .expect("prepare usage query");
            statement
                .query_map([], |row| row.get::<_, i64>(0))
                .expect("query usage rows")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect usage rows")
        };
        assert_eq!(rows.len(), 2, "media failure usage must not be duplicated");
        assert!(rows.contains(&400));
        assert!(rows.contains(&200));

        drop(client);
        primary_task.await.expect("primary media upstream task");
        fallback_task.await.expect("fallback media upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn proxy_stop_interrupts_stalled_reactive_media_retry_write() {
        const LARGE_FIELD_BYTES: usize = 16 * 1024 * 1024;
        const EXPECTED_LIMIT: usize = 200 * 1024 * 1024;
        let large_config = WebSocketConfig {
            max_message_size: Some(EXPECTED_LIMIT),
            max_frame_size: Some(EXPECTED_LIMIT),
            ..Default::default()
        };
        let upstream_socket = TcpSocket::new_v4().expect("create media-retry socket");
        upstream_socket
            .set_recv_buffer_size(1024)
            .expect("shrink media-retry receive buffer");
        upstream_socket
            .bind("127.0.0.1:0".parse().expect("parse media-retry address"))
            .expect("bind media-retry upstream");
        let upstream_listener = upstream_socket
            .listen(1)
            .expect("listen for media-retry upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (failure_tx, failure_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = accept_async_with_config(stream, Some(large_config))
                .await
                .expect("accept media-retry websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type":"response.failed",
                        "response":{"error":{"status":400,"type":"invalid_request_error","message":"image inputs are not supported"}}
                    })
                    .to_string(),
                ))
                .await
                .expect("send unsupported-image failure");
            let _ = failure_tx.send(());
            let _ = release_rx.await;
            drop(websocket);
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
        client
            .send(UpstreamMessage::Text(
                json!({
                    "type":"response.create",
                    "model":"local-model",
                    "instructions":"x".repeat(LARGE_FIELD_BYTES),
                    "input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]
                })
                .to_string(),
            ))
            .await
            .expect("send large media turn");
        tokio::time::timeout(Duration::from_secs(5), failure_rx)
            .await
            .expect("unsupported-image failure timed out")
            .expect("failure sender dropped");
        let stop_result = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
        let _ = release_tx.send(());
        upstream_task.await.expect("media-retry upstream task");
        stop_result
            .expect("proxy stop waited on a stalled media-retry write")
            .expect("stop proxy");
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
        assert!(
            error["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("provider changed")),
            "unexpected provider-change error: {error}"
        );
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
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = server.get_status().await;
                if status.total_requests == 1 && status.success_requests == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first turn status was not recorded");

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
        assert!(
            error["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("provider changed")),
            "unexpected provider-change error: {error}"
        );
        assert!(!old_received_rx.await.expect("old provider observation"));
        let status = server.get_status().await;
        assert_eq!(status.total_requests, 2);
        assert_eq!(status.success_requests, 1);
        assert_eq!(status.failed_requests, 1);
        assert!(status
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("provider changed")));

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn stopping_proxy_interrupts_initial_request_context_load() {
        REQUEST_CONTEXT_LOADS_STARTED.store(0, std::sync::atomic::Ordering::SeqCst);
        PAUSE_REQUEST_CONTEXT_LOAD.store(true, std::sync::atomic::Ordering::SeqCst);
        let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_addr = unavailable_listener.local_addr().unwrap();
        drop(unavailable_listener);

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider(format!("http://{unavailable_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = Arc::new(ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        ));
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
        tokio::time::timeout(Duration::from_secs(2), async {
            while REQUEST_CONTEXT_LOADS_STARTED.load(std::sync::atomic::Ordering::SeqCst) < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("initial request context load did not start");

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let locked_db = db.clone();
        let lock_thread = std::thread::spawn(move || {
            let _guard = locked_db.conn.lock().expect("lock database");
            locked_tx.send(()).expect("signal database lock");
            release_rx.recv().expect("release database lock");
        });
        locked_rx.recv().expect("wait for database lock");
        PAUSE_REQUEST_CONTEXT_LOAD.store(false, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stop_server = server.clone();
        let stop_thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build stop runtime");
            runtime.block_on(stop_server.stop())
        });
        std::thread::sleep(Duration::from_millis(1500));
        let stopped_while_locked = stop_thread.is_finished();
        drop(client);
        release_tx.send(()).expect("release database lock");
        lock_thread.join().expect("database lock task");
        let stop_result = stop_thread.join().expect("proxy stop thread");
        stop_result.expect("stop proxy");
        assert!(
            stopped_while_locked,
            "initial request context load blocked proxy shutdown"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn stopping_proxy_interrupts_later_request_context_load() {
        REQUEST_CONTEXT_LOADS_STARTED.store(0, std::sync::atomic::Ordering::SeqCst);
        PAUSE_REQUEST_CONTEXT_LOAD.store(false, std::sync::atomic::Ordering::SeqCst);
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-context-first","output":[]}}).to_string(),
                ))
                .await
                .unwrap();
            while websocket.next().await.is_some() {}
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let server = Arc::new(ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        ));
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send first response.create");
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-context-first");

        PAUSE_REQUEST_CONTEXT_LOAD.store(true, std::sync::atomic::Ordering::SeqCst);
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-context-first")).to_string(),
            ))
            .await
            .expect("send later response.create");
        tokio::time::timeout(Duration::from_secs(2), async {
            while REQUEST_CONTEXT_LOADS_STARTED.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("later request context load did not start");

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let locked_db = db.clone();
        let lock_thread = std::thread::spawn(move || {
            let _guard = locked_db.conn.lock().expect("lock database");
            locked_tx.send(()).expect("signal database lock");
            release_rx.recv().expect("release database lock");
        });
        locked_rx.recv().expect("wait for database lock");
        PAUSE_REQUEST_CONTEXT_LOAD.store(false, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stop_server = server.clone();
        let stop_thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build stop runtime");
            runtime.block_on(stop_server.stop())
        });
        std::thread::sleep(Duration::from_millis(1500));
        let stopped_while_locked = stop_thread.is_finished();
        drop(client);
        release_tx.send(()).expect("release database lock");
        lock_thread.join().expect("database lock task");
        let stop_result = stop_thread.join().expect("proxy stop thread");
        let _ = tokio::time::timeout(Duration::from_secs(2), upstream_task).await;
        stop_result.expect("stop proxy");
        assert!(
            stopped_while_locked,
            "later request context load blocked proxy shutdown"
        );
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
                            "SELECT session_id, first_token_ms, input_tokens, output_tokens, status_code FROM proxy_request_logs WHERE provider_id = 'ws-provider' AND app_type = 'codex' ORDER BY session_id",
                        )
                        .expect("prepare usage query");
                    statement
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, Option<String>>(0)?,
                                row.get::<_, Option<i64>>(1)?,
                                row.get::<_, i64>(2)?,
                                row.get::<_, i64>(3)?,
                                row.get::<_, i64>(4)?,
                            ))
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
        assert!(rows.iter().all(|row| (row.2, row.3, row.4) == (10, 2, 200)));
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
    async fn malformed_response_create_does_not_trip_provider_breaker() {
        let db = Arc::new(Database::memory().expect("create in-memory database"));
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            ..Default::default()
        })
        .await
        .expect("configure circuit breaker");
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        db.save_provider("codex", &provider).expect("save provider");
        db.add_to_failover_queue("codex", &provider.id)
            .expect("queue provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        let mut app_config = db
            .get_proxy_config_for_app("codex")
            .await
            .expect("load codex proxy config");
        app_config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("enable circuit breaker routing");

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                json!({
                    "type": "response.create",
                    "model": "local-model",
                    "input": [{"role": "user", "content": "hello"}],
                    "tools": [
                        {"type": "function", "name": "mcp__files____read", "parameters": {}},
                        {
                            "type": "namespace",
                            "name": "mcp__files__",
                            "tools": [{"type": "function", "name": "read", "parameters": {}}]
                        }
                    ]
                })
                .to_string(),
            ))
            .await
            .expect("send malformed response.create");
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("transform error event JSON");
        assert_eq!(terminal["type"], "error");

        let stats = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(stats) = router
                    .get_circuit_breaker_stats(&provider.id, "codex")
                    .await
                {
                    break stats;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("timed out waiting for provider breaker stats");
        assert_eq!(
            stats.state,
            crate::proxy::circuit_breaker::CircuitState::Closed
        );
        assert_eq!(stats.consecutive_failures, 0);
        assert_eq!(stats.failed_requests, 0);
        let status = server.get_status().await;
        assert_eq!(status.total_requests, 1);
        assert_eq!(status.failed_requests, 1);

        drop(client);
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn client_error_terminal_is_neutral_and_logged_as_failed() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind client-error upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.failed",
                        "response": {
                            "id": "resp-client-error",
                            "model": "upstream-model",
                            "status": "failed",
                            "error": {
                                "type": "invalid_request_error",
                                "code": "invalid_parameter",
                                "message": "invalid tool schema"
                            },
                            "usage": {
                                "input_tokens": 7,
                                "output_tokens": 1
                            }
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send client error");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).expect("save provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select provider");
        db.add_to_failover_queue("codex", &provider.id)
            .expect("queue provider");
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
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let failed: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("failed response JSON");
        assert_eq!(failed["type"], "response.failed");

        let health = db
            .get_provider_health(&provider.id, "codex")
            .await
            .expect("load provider health");
        assert_eq!(health.consecutive_failures, 0);
        assert!(health.is_healthy);

        let row = {
            let conn = db.conn.lock().expect("lock database");
            conn.query_row(
                "SELECT status_code, input_tokens, output_tokens FROM proxy_request_logs WHERE provider_id = ?1 AND app_type = 'codex'",
                [&provider.id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
            )
            .expect("load failed usage row")
        };
        assert_eq!(row, (400, 7, 1));
        let status = server.get_status().await;
        assert_eq!(status.failed_requests, 1);

        drop(client);
        upstream_task.await.expect("client-error upstream task");
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
        app_config.max_retries = 0;
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
    async fn proxy_stop_interrupts_stalled_upstream_websocket_handshake() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled-handshake upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener
                .accept()
                .await
                .expect("accept upstream TCP");
            let _ = accepted_tx.send(());
            let _ = release_rx.await;
            drop(stream);
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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        accepted_rx.await.expect("upstream TCP accept signal");

        let stop_result = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
        let _ = release_tx.send(());
        upstream_task
            .await
            .expect("stalled-handshake upstream task");
        stop_result
            .expect("proxy stop waited on a stalled upstream WebSocket handshake")
            .expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn proxy_stop_interrupts_stalled_downstream_websocket_write() {
        const BINARY_FRAME_BYTES: usize = 16 * 1024 * 1024;
        const EXPECTED_LIMIT: usize = 200 * 1024 * 1024;
        let large_config = WebSocketConfig {
            max_message_size: Some(EXPECTED_LIMIT),
            max_frame_size: Some(EXPECTED_LIMIT),
            ..Default::default()
        };
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind downstream-stall upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = accept_async_with_config(stream, Some(large_config))
                .await
                .expect("accept upstream websocket");
            let _ = next_text(&mut websocket).await;
            let _ = ready_tx.send(());
            let payload = vec![0_u8; BINARY_FRAME_BYTES];
            loop {
                if websocket
                    .send(UpstreamMessage::Binary(payload.clone()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
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
        let client_socket = TcpSocket::new_v4().expect("create downstream client socket");
        client_socket
            .set_recv_buffer_size(1024)
            .expect("shrink downstream receive buffer");
        let stream = client_socket
            .connect(format!("127.0.0.1:{}", info.port).parse().unwrap())
            .await
            .expect("connect downstream TCP");
        let (mut client, _) = client_async_with_config(
            format!("ws://127.0.0.1:{}/v1/responses", info.port),
            stream,
            Some(large_config),
        )
        .await
        .expect("connect downstream websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        ready_rx.await.expect("upstream ready signal");
        tokio::time::sleep(Duration::from_millis(500)).await;

        tokio::time::timeout(Duration::from_secs(2), server.stop())
            .await
            .expect("proxy stop waited on a stalled downstream WebSocket write")
            .expect("stop proxy");
        drop(client);
        upstream_task.await.expect("downstream-stall upstream task");
    }

    #[tokio::test]
    #[serial]
    async fn premature_upstream_close_forces_sse_fallback_close() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind premature-close upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Close(None))
                .await
                .expect("send premature close");
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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");

        let error = client
            .next()
            .await
            .expect("client closed before fallback error")
            .expect("fallback error frame");
        let error = match error {
            UpstreamMessage::Text(text) => {
                serde_json::from_str::<Value>(&text).expect("fallback error JSON")
            }
            other => panic!("expected fallback error event, got {other:?}"),
        };
        assert_eq!(error["type"], "error");
        let close = client
            .next()
            .await
            .expect("client closed before fallback close")
            .expect("fallback close frame");
        match close {
            UpstreamMessage::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1011),
            other => panic!("expected 1011 fallback close, got {other:?}"),
        }

        upstream_task.await.expect("premature-close upstream task");
        server.stop().await.expect("stop proxy");
    }
    #[tokio::test]
    #[serial]
    async fn provider_terminal_failure_retries_same_turn_on_fallback() {
        let bad_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind bad upstream");
        let bad_addr = bad_listener.local_addr().unwrap();
        let bad_task = tokio::spawn(async move {
            let (stream, _) = bad_listener.accept().await.expect("accept bad upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept bad websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.failed",
                        "response": {
                            "id": "resp-bad",
                            "status": "failed",
                            "error": {"type": "server_error", "code": "internal_error"}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send provider failure");
        });
        let good_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind good upstream");
        let good_addr = good_listener.local_addr().unwrap();
        let good_task = tokio::spawn(async move {
            let (stream, _) = good_listener.accept().await.expect("accept good upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept good websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-good",
                            "model": "upstream-model",
                            "usage": {"input_tokens": 3, "output_tokens": 2}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send fallback completion");
            let _ = tokio::time::timeout(Duration::from_secs(2), next_text(&mut websocket))
                .await
                .expect("second turn did not stay on fallback socket");
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-good-2","model":"upstream-model","output":[]}})
                        .to_string(),
                ))
                .await
                .expect("send second fallback completion");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let bad = websocket_provider_with_id("ws-1-bad", format!("http://{bad_addr}"));
        let chat = non_websocket_provider_with_id("ws-2-chat", "http://127.0.0.1:1".to_string());
        let good = websocket_provider_with_id("ws-3-good", format!("http://{good_addr}"));
        db.save_provider("codex", &bad).expect("save bad provider");
        db.save_provider("codex", &chat)
            .expect("save chat-only provider");
        db.save_provider("codex", &good)
            .expect("save good provider");
        db.add_to_failover_queue("codex", &bad.id)
            .expect("queue bad provider");
        db.add_to_failover_queue("codex", &chat.id)
            .expect("queue chat-only provider");
        db.add_to_failover_queue("codex", &good.id)
            .expect("queue good provider");
        let mut app_config = db
            .get_proxy_config_for_app("codex")
            .await
            .expect("load codex proxy config");
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 4;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("enable failover");

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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let terminal: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("fallback terminal JSON");
        assert_eq!(terminal["type"], "response.completed");
        let status = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = server.get_status().await;
                if status.success_requests == 1 && status.failover_count == 1 {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("same-turn failover accounting did not finish");
        assert_eq!(status.total_requests, 1);
        assert_eq!(status.success_requests, 1);
        assert_eq!(status.failed_requests, 0);
        assert_eq!(status.failover_count, 1);

        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-good")).to_string(),
            ))
            .await
            .expect("send immediate second turn");
        let second: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("second fallback terminal JSON");
        assert_eq!(second["type"], "response.completed");
        assert_eq!(second["response"]["id"], "resp-good-2");

        drop(client);
        bad_task.await.expect("bad upstream task");
        good_task.await.expect("good upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn provider_failure_after_relayed_events_is_not_retried() {
        let bad_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind partial-response upstream");
        let bad_addr = bad_listener.local_addr().unwrap();
        let bad_task = tokio::spawn(async move {
            let (stream, _) = bad_listener.accept().await.expect("accept bad upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept bad websocket");
            let _ = next_text(&mut websocket).await;
            for event in [
                json!({"type":"response.created","response":{"id":"resp-partial"}}),
                json!({"type":"response.output_text.delta","delta":"partial"}),
                json!({
                    "type":"response.failed",
                    "response": {
                        "id":"resp-partial",
                        "status":"failed",
                        "error":{"type":"server_error","code":"internal_error"}
                    }
                }),
            ] {
                websocket
                    .send(UpstreamMessage::Text(event.to_string()))
                    .await
                    .expect("send partial response event");
            }
        });

        let good_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fallback upstream");
        let good_addr = good_listener.local_addr().unwrap();
        let (fallback_tx, fallback_rx) = oneshot::channel();
        let good_task = tokio::spawn(async move {
            let accepted = match tokio::time::timeout(
                Duration::from_secs(2),
                good_listener.accept(),
            )
            .await
            {
                Ok(Ok((stream, _))) => {
                    let mut websocket = tokio_tungstenite::accept_async(stream)
                        .await
                        .expect("accept fallback websocket");
                    let _ = next_text(&mut websocket).await;
                    websocket
                        .send(UpstreamMessage::Text(
                            json!({"type":"response.completed","response":{"id":"resp-duplicate","output":[]}})
                                .to_string(),
                        ))
                        .await
                        .expect("send duplicate completion");
                    true
                }
                _ => false,
            };
            let _ = fallback_tx.send(accepted);
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let mut bad = websocket_provider_with_id("ws-partial", format!("http://{bad_addr}"));
        bad.sort_index = Some(0);
        let mut good = websocket_provider_with_id("ws-fallback", format!("http://{good_addr}"));
        good.sort_index = Some(1);
        for provider in [&bad, &good] {
            db.save_provider("codex", provider).expect("save provider");
            db.add_to_failover_queue("codex", &provider.id)
                .expect("queue provider");
        }
        db.set_current_provider("codex", &bad.id)
            .expect("select bad provider");
        let mut app_config = db
            .get_proxy_config_for_app("codex")
            .await
            .expect("load codex proxy config");
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("enable failover");

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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");

        let created: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        let delta: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        let fallback_used = fallback_rx.await.expect("fallback observation");

        assert_eq!(created["type"], "response.created");
        assert_eq!(delta["type"], "response.output_text.delta");
        assert_eq!(terminal["type"], "response.failed");
        assert!(!fallback_used);

        drop(client);
        bad_task.await.expect("partial-response upstream task");
        good_task.await.expect("fallback upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn initial_cursor_does_not_skip_selected_non_websocket_provider() {
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (fallback_tx, fallback_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let used = match tokio::time::timeout(
                Duration::from_secs(2),
                fallback_listener.accept(),
            )
            .await
            {
                Ok(Ok((stream, _))) => {
                    let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    let _ = next_text(&mut websocket).await;
                    websocket
                        .send(UpstreamMessage::Text(
                            json!({"type":"response.completed","response":{"id":"resp-crossed-capability-boundary","output":[]}}).to_string(),
                        ))
                        .await
                        .unwrap();
                    true
                }
                _ => false,
            };
            let _ = fallback_tx.send(used);
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut selected = non_websocket_provider_with_id(
            "chat-selected-cursor",
            "http://127.0.0.1:1".to_string(),
        );
        selected.sort_index = Some(0);
        let mut fallback = websocket_provider_with_id(
            "ws-capable-cursor-fallback",
            format!("http://{fallback_addr}"),
        );
        fallback.sort_index = Some(1);
        for provider in [&selected, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &selected.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-selected-provider")).to_string(),
            ))
            .await
            .unwrap();
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        let fallback_used = fallback_rx.await.unwrap();
        let status = server.get_status().await;

        assert_eq!(terminal["type"], "error", "{terminal}");
        assert!(
            !fallback_used,
            "cursor request skipped the selected provider"
        );
        assert_eq!(status.total_requests, 1);
        assert_eq!(status.failed_requests, 1);

        drop(client);
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn initial_chain_skips_non_websocket_provider_after_open_breaker() {
        let fallback_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind WebSocket fallback");
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.expect("accept fallback");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept fallback websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-capable-fallback","output":[]}})
                        .to_string(),
                ))
                .await
                .expect("send fallback completion");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-capable-fallback-second","output":[]}})
                        .to_string(),
                ))
                .await
                .expect("send second fallback completion");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let mut original =
            websocket_provider_with_id("ws-open-original", "http://127.0.0.1:1".to_string());
        original.sort_index = Some(0);
        let mut chat =
            non_websocket_provider_with_id("chat-first-fallback", "http://127.0.0.1:1".to_string());
        chat.sort_index = Some(1);
        let mut fallback =
            websocket_provider_with_id("ws-capable-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(2);
        for provider in [&original, &chat, &fallback] {
            db.save_provider("codex", provider).expect("save provider");
            db.add_to_failover_queue("codex", &provider.id)
                .expect("queue provider");
        }
        db.set_current_provider("codex", &original.id)
            .expect("select original provider");
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
            db,
            None,
        );
        server
            .provider_router_for_tests()
            .record_result(
                &original.id,
                "codex",
                false,
                false,
                Some("open original provider".to_string()),
            )
            .await
            .expect("open original breaker");
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let terminal: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("fallback completion JSON");
        assert_eq!(terminal["type"], "response.completed");
        assert_eq!(terminal["response"]["id"], "resp-capable-fallback");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send second response.create");
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("second fallback completion JSON");
        assert_eq!(terminal["type"], "response.completed");
        assert_eq!(terminal["response"]["id"], "resp-capable-fallback-second");

        drop(client);
        fallback_task.await.expect("fallback upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn initial_half_open_permit_denial_uses_healthy_fallback() {
        let fallback_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fallback upstream");
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.expect("accept fallback");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept fallback websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-fallback","output":[]}})
                        .to_string(),
                ))
                .await
                .expect("send fallback completion");
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .expect("configure half-open breaker");
        let mut primary =
            websocket_provider_with_id("ws-primary", "http://127.0.0.1:1".to_string());
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).expect("save provider");
            db.add_to_failover_queue("codex", &provider.id)
                .expect("queue provider");
        }
        db.set_current_provider("codex", &primary.id)
            .expect("select primary provider");
        let mut app_config = db
            .get_proxy_config_for_app("codex")
            .await
            .expect("load codex proxy config");
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("enable failover");

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        router
            .record_result(
                &primary.id,
                "codex",
                false,
                false,
                Some("prime half-open state".to_string()),
            )
            .await
            .expect("open primary breaker");
        let held_permit = router.allow_provider_request(&primary.id, "codex").await;
        assert!(held_permit.allowed && held_permit.used_half_open_permit);

        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let terminal: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("fallback completion JSON");

        router
            .release_permit_neutral(&primary.id, "codex", held_permit.used_half_open_permit)
            .await;
        assert_eq!(terminal["type"], "response.completed");

        drop(client);
        fallback_task.await.expect("fallback upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn all_initial_half_open_permit_denials_count_one_failed_request() {
        let db = Arc::new(Database::memory().expect("create in-memory database"));
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .expect("configure half-open breaker");
        let mut primary =
            websocket_provider_with_id("ws-primary-denied", "http://127.0.0.1:1".to_string());
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-fallback-denied", "http://127.0.0.1:1".to_string());
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).expect("save provider");
            db.add_to_failover_queue("codex", &provider.id)
                .expect("queue provider");
        }
        db.set_current_provider("codex", &primary.id)
            .expect("select primary provider");
        let mut app_config = db
            .get_proxy_config_for_app("codex")
            .await
            .expect("load codex proxy config");
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("enable failover");

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        for provider in [&primary, &fallback] {
            router
                .record_result(
                    &provider.id,
                    "codex",
                    false,
                    false,
                    Some("prime half-open state".to_string()),
                )
                .await
                .expect("open provider breaker");
        }
        let primary_permit = router.allow_provider_request(&primary.id, "codex").await;
        let fallback_permit = router.allow_provider_request(&fallback.id, "codex").await;
        assert!(primary_permit.allowed && primary_permit.used_half_open_permit);
        assert!(fallback_permit.allowed && fallback_permit.used_half_open_permit);

        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let terminal: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("permit-denial error JSON");

        let status = server.get_status().await;
        assert_eq!(terminal["type"], "error");
        assert_eq!(status.total_requests, 1);
        assert_eq!(status.success_requests, 0);
        assert_eq!(status.failed_requests, 1);
        assert!(status.last_request_at.is_some());
        assert!(status.last_error.is_some());

        router
            .release_permit_neutral(&primary.id, "codex", primary_permit.used_half_open_permit)
            .await;
        router
            .release_permit_neutral(&fallback.id, "codex", fallback_permit.used_half_open_permit)
            .await;
        drop(client);
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn chain_without_websocket_provider_counts_rejected_turn_once() {
        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let provider =
            non_websocket_provider_with_id("chat-only", "http://127.0.0.1:1".to_string());
        db.save_provider("codex", &provider)
            .expect("save chat-only provider");
        db.set_current_provider("codex", &provider.id)
            .expect("select chat-only provider");

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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("unsupported-provider error JSON");

        let status = server.get_status().await;
        assert_eq!(terminal["type"], "error");
        assert_eq!(status.total_requests, 1);
        assert_eq!(status.success_requests, 0);
        assert_eq!(status.failed_requests, 1);
        assert!(status.last_request_at.is_some());
        assert!(status.last_error.as_deref().is_some_and(|message| {
            message.contains("does not include a native Responses WebSocket provider")
        }));

        drop(client);
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn later_turn_cursor_permit_denial_does_not_reconnect_to_fallback() {
        let primary_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind primary upstream");
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.expect("accept primary");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept primary websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-primary","output":[]}})
                        .to_string(),
                ))
                .await
                .expect("send primary completion");
            let _ = websocket.next().await;
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fallback upstream");
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (fallback_check_tx, fallback_check_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let _ = fallback_check_rx.await;
            let accepted =
                tokio::time::timeout(Duration::from_millis(500), fallback_listener.accept()).await;
            let Ok(Ok((stream, _))) = accepted else {
                return false;
            };
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept fallback websocket");
            let _ = next_text(&mut websocket).await;
            true
        });

        let db = Arc::new(Database::memory().expect("create in-memory database"));
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .expect("configure half-open breaker");
        let mut primary =
            websocket_provider_with_id("ws-later-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-later-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).expect("save provider");
            db.add_to_failover_queue("codex", &provider.id)
                .expect("queue provider");
        }
        db.set_current_provider("codex", &primary.id)
            .expect("select primary provider");
        let mut app_config = db
            .get_proxy_config_for_app("codex")
            .await
            .expect("load codex proxy config");
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("enable failover");

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send first turn");
        let first: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("first completion JSON");
        assert_eq!(first["type"], "response.completed");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if server.get_status().await.success_requests == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("first turn accounting did not finish");

        router
            .record_result(
                &primary.id,
                "codex",
                false,
                false,
                Some("prime later-turn half-open state".to_string()),
            )
            .await
            .expect("open primary breaker");
        let held_permit = router.allow_provider_request(&primary.id, "codex").await;
        assert!(held_permit.allowed && held_permit.used_half_open_permit);

        let _ = fallback_check_tx.send(());
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-primary")).to_string(),
            ))
            .await
            .expect("send second turn");
        let second: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("later-turn affinity error JSON");
        let fallback_connected = fallback_task.await.expect("fallback task");
        let status = server.get_status().await;
        assert_eq!(second["type"], "error", "{second}");
        assert!(
            !fallback_connected,
            "provider cursor was replayed on fallback"
        );
        assert_eq!(status.total_requests, 2);
        assert_eq!(status.failed_requests, 1);

        router
            .release_permit_neutral(&primary.id, "codex", held_permit.used_half_open_permit)
            .await;
        drop(client);
        primary_task.await.expect("primary upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn initial_handshake_failure_retries_healthy_websocket_fallback() {
        let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_addr = unavailable_listener.local_addr().unwrap();
        drop(unavailable_listener);
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket.send(UpstreamMessage::Text(
                json!({"type":"response.completed","response":{"id":"resp-handshake-fallback","output":[]}}).to_string()
            )).await.unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut primary = websocket_provider_with_id(
            "ws-handshake-primary",
            format!("http://{unavailable_addr}"),
        );
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-handshake-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(terminal["type"], "response.completed");
        assert_eq!(terminal["response"]["id"], "resp-handshake-fallback");

        drop(client);
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn initial_cursor_handshake_failure_does_not_cross_provider_boundary() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (mut stream, _) = primary_listener.accept().await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (fallback_tx, fallback_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let used = match tokio::time::timeout(
                Duration::from_secs(2),
                fallback_listener.accept(),
            )
            .await
            {
                Ok(Ok((stream, _))) => {
                    let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    let _ = next_text(&mut websocket).await;
                    websocket
                        .send(UpstreamMessage::Text(
                            json!({"type":"response.completed","response":{"id":"resp-cursor-replayed","output":[]}}).to_string(),
                        ))
                        .await
                        .unwrap();
                    true
                }
                _ => false,
            };
            let _ = fallback_tx.send(used);
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut primary = websocket_provider_with_id(
            "ws-cursor-handshake-primary",
            format!("http://{primary_addr}"),
        );
        primary.sort_index = Some(0);
        let mut fallback = websocket_provider_with_id(
            "ws-cursor-handshake-fallback",
            format!("http://{fallback_addr}"),
        );
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-provider-cursor")).to_string(),
            ))
            .await
            .unwrap();
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        let fallback_used = fallback_rx.await.unwrap();
        let status = server.get_status().await;

        assert_eq!(terminal["type"], "error", "{terminal}");
        assert!(
            !fallback_used,
            "cursor request was replayed on the fallback provider"
        );
        assert_eq!(status.total_requests, 1);
        assert_eq!(status.failed_requests, 1);

        drop(client);
        primary_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn initial_handshake_failure_does_not_block_fallback_on_health_persistence() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let (handshake_started_tx, handshake_started_rx) = oneshot::channel();
        let (fail_handshake_tx, fail_handshake_rx) = oneshot::channel();
        let primary_task = tokio::spawn(async move {
            let (mut stream, _) = primary_listener.accept().await.unwrap();
            let _ = handshake_started_tx.send(());
            let _ = fail_handshake_rx.await;
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let mut fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-detached-handshake-fallback","output":[]}}).to_string(),
                ))
                .await
                .unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut primary = websocket_provider_with_id(
            "ws-detached-handshake-primary",
            format!("http://{primary_addr}"),
        );
        primary.sort_index = Some(0);
        let mut fallback = websocket_provider_with_id(
            "ws-detached-handshake-fallback",
            format!("http://{fallback_addr}"),
        );
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        handshake_started_rx.await.unwrap();

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let locked_db = db.clone();
        let lock_thread = std::thread::spawn(move || {
            let _guard = locked_db.conn.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();
        let _ = fail_handshake_tx.send(());

        let accepted_result = tokio::time::timeout(Duration::from_millis(750), accepted_rx).await;
        let _ = release_tx.send(());
        lock_thread.join().unwrap();

        if accepted_result.is_err() {
            drop(client);
            if tokio::time::timeout(Duration::from_secs(2), &mut fallback_task)
                .await
                .is_err()
            {
                fallback_task.abort();
                let _ = fallback_task.await;
            }
            primary_task.await.unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
            panic!("provider health persistence blocked WebSocket fallback");
        }

        let terminal: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(terminal["type"], "response.completed");
        assert_eq!(
            terminal["response"]["id"],
            "resp-detached-handshake-fallback"
        );

        drop(client);
        primary_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn provider_cursor_turn_does_not_replay_on_fallback() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket.send(UpstreamMessage::Text(
                json!({"type":"response.failed","response":{"id":"resp-cursor-failed","error":{"type":"server_error"}}}).to_string()
            )).await.unwrap();
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (fallback_tx, fallback_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let accepted =
                tokio::time::timeout(Duration::from_secs(2), fallback_listener.accept()).await;
            let used = if let Ok(Ok((stream, _))) = accepted {
                let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let _ = next_text(&mut websocket).await;
                websocket.send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-cursor-fallback","output":[]}}).to_string()
                )).await.unwrap();
                true
            } else {
                false
            };
            let _ = fallback_tx.send(used);
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut primary =
            websocket_provider_with_id("ws-cursor-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-cursor-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-primary")).to_string(),
            ))
            .await
            .unwrap();
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(terminal["type"], "error");
        assert!(!fallback_rx.await.unwrap());

        drop(client);
        primary_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }
    #[tokio::test]
    #[serial]
    async fn failed_usage_is_logged_before_fallback_handshake_shutdown() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket.send(UpstreamMessage::Text(
                json!({"type":"response.failed","response":{"id":"resp-billed-failure","model":"upstream-model","error":{"type":"server_error"},"usage":{"input_tokens":7,"output_tokens":3}}}).to_string()
            )).await.unwrap();
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let _ = release_rx.await;
            drop(stream);
        });

        let db = Arc::new(Database::memory().unwrap());
        let db_for_assert = db.clone();
        let mut primary =
            websocket_provider_with_id("ws-billed-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-billed-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        accepted_rx.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), server.stop())
            .await
            .unwrap()
            .unwrap();
        let _ = release_tx.send(());
        fallback_task.await.unwrap();
        primary_task.await.unwrap();
        drop(client);

        let count = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let count: i64 = db_for_assert.conn.lock().unwrap().query_row(
                    "SELECT COUNT(*) FROM proxy_request_logs WHERE provider_id = 'ws-billed-primary' AND app_type = 'codex'",
                    [],
                    |row| row.get(0),
                ).unwrap();
                if count == 1 { break count; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("failed-attempt usage was not persisted before shutdown");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    #[serial]
    async fn upstream_heartbeats_do_not_refresh_active_turn_idle_deadline() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind heartbeat upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept heartbeat websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.created","response":{"id":"resp-heartbeat"}})
                        .to_string(),
                ))
                .await
                .expect("send response.created");
            for heartbeat in 0..4_u8 {
                tokio::time::sleep(Duration::from_millis(350)).await;
                if websocket
                    .send(UpstreamMessage::Ping(vec![heartbeat]))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-heartbeat","output":[]}})
                        .to_string(),
                ))
                .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
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
            .expect("load codex proxy config");
        app_config.auto_failover_enabled = true;
        app_config.streaming_first_byte_timeout = 1;
        app_config.streaming_idle_timeout = 1;
        db.update_proxy_config_for_app(app_config)
            .await
            .expect("set websocket idle timeout");

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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        let created: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        let terminal: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();

        assert_eq!(created["type"], "response.created");
        assert_eq!(terminal["type"], "error", "{terminal}");
        assert!(
            terminal["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("idle timed out")),
            "unexpected timeout error: {terminal}"
        );

        drop(client);
        upstream_task.await.expect("heartbeat upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn terminal_event_is_relayed_before_usage_database_write() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind terminal upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (request_tx, request_rx) = oneshot::channel();
        let (send_tx, send_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            let _ = request_tx.send(());
            let _ = send_rx.await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-logging",
                            "model": "upstream-model",
                            "usage": {"input_tokens": 4, "output_tokens": 2}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send completion");
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
            db.clone(),
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        request_rx.await.expect("upstream request signal");

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let locked_db = db.clone();
        let lock_thread = std::thread::spawn(move || {
            let _guard = locked_db.conn.lock().expect("lock usage database");
            locked_tx.send(()).expect("signal database lock");
            release_rx.recv().expect("release database lock");
        });
        locked_rx.recv().expect("wait for database lock");
        let _ = send_tx.send(());

        let terminal =
            tokio::time::timeout(Duration::from_millis(500), next_text(&mut client)).await;
        let terminal = terminal.expect("terminal relay waited for usage database write");
        let terminal: Value = serde_json::from_str(&terminal).expect("terminal JSON");
        assert_eq!(terminal["type"], "response.completed");

        drop(client);

        let stop_result = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
        let _ = release_tx.send(());
        lock_thread.join().expect("database lock thread");
        stop_result
            .expect("terminal accounting blocked WebSocket shutdown")
            .expect("stop proxy");
        upstream_task.await.expect("terminal upstream task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn terminal_usage_is_logged_when_downstream_relay_fails() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind terminal upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (request_tx, request_rx) = oneshot::channel();
        let (send_tx, send_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            let _ = request_tx.send(());
            let _ = send_rx.await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-relay-failed-usage",
                            "model": "upstream-model",
                            "usage": {"input_tokens": 4, "output_tokens": 2}
                        }
                    })
                    .to_string(),
                ))
                .await
                .expect("send completion");
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
            db.clone(),
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");
        request_rx.await.expect("upstream request signal");

        FAIL_NEXT_DOWNSTREAM_TERMINAL_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = send_tx.send(());

        let usage = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let usage = db
                    .conn
                    .lock()
                    .expect("lock usage database")
                    .query_row(
                        "SELECT status_code, input_tokens, output_tokens FROM proxy_request_logs WHERE provider_id = 'ws-provider' AND app_type = 'codex'",
                        [],
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
                    )
                    .ok();
                if let Some(usage) = usage {
                    break usage;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;

        drop(client);
        server.stop().await.expect("stop proxy");
        upstream_task.await.expect("terminal upstream task");

        assert_eq!(
            usage.expect("timed out waiting for usage after downstream relay failure"),
            (200, 4, 2)
        );
    }

    #[tokio::test]
    #[serial]
    async fn proxy_stop_interrupts_stalled_upstream_websocket_write() {
        const BINARY_FRAME_BYTES: usize = 16 * 1024 * 1024;
        const EXPECTED_LIMIT: usize = 200 * 1024 * 1024;
        let large_config = WebSocketConfig {
            max_message_size: Some(EXPECTED_LIMIT),
            max_frame_size: Some(EXPECTED_LIMIT),
            ..Default::default()
        };
        let upstream_socket = TcpSocket::new_v4().expect("create stalled-write socket");
        upstream_socket
            .set_recv_buffer_size(1024)
            .expect("shrink stalled-write receive buffer");
        upstream_socket
            .bind("127.0.0.1:0".parse().expect("parse stalled-write address"))
            .expect("bind stalled-write upstream");
        let upstream_listener = upstream_socket
            .listen(1)
            .expect("listen for stalled-write upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let websocket = accept_async_with_config(stream, Some(large_config))
                .await
                .expect("accept stalled-write websocket");
            let _ = ready_tx.send(());
            let _ = release_rx.await;
            drop(websocket);
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
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send initial response.create");
        tokio::time::timeout(Duration::from_secs(5), ready_rx)
            .await
            .expect("upstream websocket handshake timed out")
            .expect("upstream readiness sender dropped");

        let flood_task = tokio::spawn(async move {
            let payload = vec![0_u8; BINARY_FRAME_BYTES];
            loop {
                client
                    .send(UpstreamMessage::Binary(payload.clone()))
                    .await
                    .expect("send binary flood frame");
            }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !flood_task.is_finished(),
            "binary flood ended before backpressure"
        );

        let stop_result = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
        let _ = release_tx.send(());
        flood_task.abort();
        let _ = flood_task.await;
        upstream_task.await.expect("stalled-write upstream task");

        stop_result
            .expect("proxy stop waited on a stalled upstream WebSocket write")
            .expect("stop proxy");
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

    #[tokio::test]
    #[serial]
    async fn rejects_duplicate_terminal_event_between_turns() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type":"response.completed",
                        "response":{"id":"resp-terminal","output":[]}
                    })
                    .to_string(),
                ))
                .await
                .expect("send terminal response");
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type":"response.completed",
                        "response":{"id":"resp-terminal-duplicate","output":[]}
                    })
                    .to_string(),
                ))
                .await
                .expect("send duplicate terminal response");
            let _ = tokio::time::timeout(Duration::from_secs(1), websocket.next()).await;
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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");

        let first: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("first completion JSON");
        assert_eq!(first["type"], "response.completed");
        let second = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .expect("proxy did not reject the duplicate terminal event")
            .expect("proxy closed without an error event")
            .expect("websocket read failed");
        let UpstreamMessage::Text(second) = second else {
            panic!("expected proxy error event, got {second:?}");
        };
        let second: Value = serde_json::from_str(&second).expect("proxy error JSON");
        assert_eq!(second["type"], "error");
        assert!(second["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("no response was in flight")));

        drop(client);
        upstream_task.await.expect("upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn rejects_binary_data_between_turns() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut websocket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket");
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-terminal","output":[]}})
                        .to_string(),
                ))
                .await
                .expect("send terminal response");
            websocket
                .send(UpstreamMessage::Binary(vec![1, 2, 3]))
                .await
                .expect("send unsolicited binary data");
            let _ = tokio::time::timeout(Duration::from_secs(1), websocket.next()).await;
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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send response.create");

        let first: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("completion JSON");
        assert_eq!(first["type"], "response.completed");
        let second = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .expect("proxy did not reject unsolicited binary data")
            .expect("proxy closed without an error event")
            .expect("websocket read failed");
        let UpstreamMessage::Text(second) = second else {
            panic!("expected proxy error event, got {second:?}");
        };
        let second: Value = serde_json::from_str(&second).expect("proxy error JSON");
        assert_eq!(second["type"], "error");
        assert!(second["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("no response was in flight")));

        drop(client);
        upstream_task.await.expect("upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn initial_transform_collision_continues_to_later_provider() {
        TRANSFORM_CLIENT_TEXT_CALLS.store(0, std::sync::atomic::Ordering::SeqCst);
        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let mut flattening =
            websocket_provider_with_id("flattening-provider", "http://127.0.0.1:9".to_string());
        flattening.sort_index = Some(1);
        let mut later =
            websocket_provider_with_id("later-provider", "http://127.0.0.1:10".to_string());
        later.sort_index = Some(2);
        db.save_provider("codex", &flattening)
            .expect("save flattening provider");
        db.save_provider("codex", &later)
            .expect("save later provider");
        db.set_current_provider("codex", &flattening.id)
            .expect("select flattening provider");
        db.add_to_failover_queue("codex", &flattening.id)
            .expect("queue flattening provider");
        db.add_to_failover_queue("codex", &later.id)
            .expect("queue later provider");
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

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
            .expect("connect local websocket");
        let mut request = response_create("local-model", None);
        request["tools"] = json!([
            {"type":"function","name":"mcp__files____read","parameters":{}},
            {"type":"namespace","name":"mcp__files__","tools":[
                {"type":"function","name":"read","parameters":{}}
            ]}
        ]);
        client
            .send(UpstreamMessage::Text(request.to_string()))
            .await
            .expect("send colliding response.create");

        let response: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("proxy error JSON");
        assert_eq!(response["type"], "error");
        assert_eq!(
            TRANSFORM_CLIENT_TEXT_CALLS.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "provider-specific transform failure must continue to the next eligible provider"
        );

        drop(client);
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn later_turn_transform_collision_continues_to_later_provider() {
        TRANSFORM_CLIENT_TEXT_CALLS.store(0, std::sync::atomic::Ordering::SeqCst);
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-first-turn","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(2), websocket.next()).await;
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut current = websocket_provider_with_id(
            "later-transform-current",
            format!("http://{upstream_addr}"),
        );
        current.sort_index = Some(0);
        let mut later =
            websocket_provider_with_id("later-transform-next", "http://127.0.0.1:9".to_string());
        later.sort_index = Some(1);
        for provider in [&current, &later] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &current.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-first-turn");

        let mut later_request = response_create("local-model", None);
        later_request["tools"] = json!([
            {"type":"function","name":"mcp__files____read","parameters":{}},
            {"type":"namespace","name":"mcp__files__","tools":[
                {"type":"function","name":"read","parameters":{}}
            ]}
        ]);
        client
            .send(UpstreamMessage::Text(later_request.to_string()))
            .await
            .unwrap();
        let response: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("proxy error JSON");
        assert_eq!(response["type"], "error");
        assert_eq!(
            TRANSFORM_CLIENT_TEXT_CALLS.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "later-turn transform failure must continue to the next eligible provider"
        );

        drop(client);
        upstream_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn later_turn_cursor_transform_collision_does_not_advance_provider() {
        TRANSFORM_CLIENT_TEXT_CALLS.store(0, std::sync::atomic::Ordering::SeqCst);
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-first-turn","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(2), websocket.next()).await;
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut current = websocket_provider_with_id(
            "later-transform-current",
            format!("http://{upstream_addr}"),
        );
        current.sort_index = Some(0);
        let mut later =
            websocket_provider_with_id("later-transform-next", "http://127.0.0.1:9".to_string());
        later.sort_index = Some(1);
        for provider in [&current, &later] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &current.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-first-turn");

        let mut later_request = response_create("local-model", Some("resp-first-turn"));
        later_request["tools"] = json!([
            {"type":"function","name":"mcp__files____read","parameters":{}},
            {"type":"namespace","name":"mcp__files__","tools":[
                {"type":"function","name":"read","parameters":{}}
            ]}
        ]);
        client
            .send(UpstreamMessage::Text(later_request.to_string()))
            .await
            .unwrap();
        let response: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("proxy error JSON");
        assert_eq!(response["type"], "error");
        assert_eq!(
            TRANSFORM_CLIENT_TEXT_CALLS.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a provider-scoped cursor must not be transformed for a fallback provider"
        );

        drop(client);
        upstream_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn queued_prior_turn_event_is_rejected_before_next_turn() {
        REQUEST_CONTEXT_LOADS_STARTED.store(0, std::sync::atomic::Ordering::SeqCst);
        PAUSE_REQUEST_CONTEXT_LOAD.store(false, std::sync::atomic::Ordering::SeqCst);
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (send_late_tx, send_late_rx) = oneshot::channel();
        let (late_sent_tx, late_sent_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-queued-first","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
            send_late_rx.await.expect("release late prior-turn event");
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-queued-late","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
            late_sent_tx.send(()).expect("signal late event sent");
            let _ = tokio::time::timeout(Duration::from_secs(2), websocket.next()).await;
        });

        let db = Arc::new(Database::memory().unwrap());
        let provider = websocket_provider_with_id(
            "queued-prior-turn-provider",
            format!("http://{upstream_addr}"),
        );
        db.save_provider("codex", &provider).unwrap();
        db.set_current_provider("codex", &provider.id).unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-queued-first");

        PAUSE_REQUEST_CONTEXT_LOAD.store(true, std::sync::atomic::Ordering::SeqCst);
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-queued-first")).to_string(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while REQUEST_CONTEXT_LOADS_STARTED.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("later request context load did not pause");
        send_late_tx.send(()).expect("send late prior-turn event");
        late_sent_rx
            .await
            .expect("late prior-turn event was not sent");
        tokio::time::sleep(Duration::from_millis(50)).await;
        PAUSE_REQUEST_CONTEXT_LOAD.store(false, std::sync::atomic::Ordering::SeqCst);

        let response: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("proxy should reject queued prior-turn data");
        assert_eq!(
            response["type"], "error",
            "a queued prior-turn terminal event must not be accepted as the next turn"
        );

        drop(client);
        upstream_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn active_turn_binary_frame_is_rejected_and_marks_provider_failed() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.output_text.delta","delta":"hello","item_id":"item-1","output_index":0,"content_index":0}).to_string(),
                ))
                .await
                .unwrap();
            websocket
                .send(UpstreamMessage::Binary(vec![1, 2, 3]))
                .await
                .unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(1), websocket.next()).await;
        });

        let db = Arc::new(Database::memory().unwrap());
        let provider =
            websocket_provider_with_id("active-binary-provider", format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).unwrap();
        db.set_current_provider("codex", &provider.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["type"], "response.output_text.delta");
        let second = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .expect("proxy did not reject active binary data")
            .expect("proxy closed without an error event")
            .expect("websocket read failed");
        let UpstreamMessage::Text(second) = second else {
            panic!("expected proxy error event, got {second:?}");
        };
        let second: Value = serde_json::from_str(&second).expect("proxy error JSON");
        assert_eq!(second["type"], "error");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let health = db.get_provider_health(&provider.id, "codex").await.unwrap();
                if !health.is_healthy {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("active binary protocol violation did not mark provider unhealthy");

        drop(client);
        upstream_task.await.unwrap();
        server.stop().await.unwrap();
    }

    struct GlobalProxyReset;

    impl Drop for GlobalProxyReset {
        fn drop(&mut self) {
            let _ = crate::proxy::http_client::apply_proxy(None);
        }
    }

    #[tokio::test]
    #[serial]
    async fn reconnects_when_global_proxy_changes_between_websocket_turns() {
        crate::proxy::http_client::apply_proxy(None).expect("start with direct routing");
        let _reset = GlobalProxyReset;

        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind direct upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (second_turn_tx, second_turn_rx) = oneshot::channel();
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

            let second_turn = match tokio::time::timeout(Duration::from_secs(2), websocket.next())
                .await
            {
                Ok(Some(Ok(UpstreamMessage::Text(_)))) => {
                    websocket
                        .send(UpstreamMessage::Text(
                            json!({"type":"response.completed","response":{"id":"resp-two","output":[]}})
                                .to_string(),
                        ))
                        .await
                        .expect("send second completion");
                    true
                }
                _ => false,
            };
            let _ = second_turn_tx.send(second_turn);
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
            .expect("connect local websocket");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .expect("send first response.create");
        let first: Value =
            serde_json::from_str(&next_text(&mut client).await).expect("first completion JSON");
        assert_eq!(first["type"], "response.completed");

        crate::proxy::http_client::apply_proxy(Some("http://127.0.0.1:9"))
            .expect("change global proxy");
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-one")).to_string(),
            ))
            .await
            .expect("send second response.create");
        let second: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("proxy reconnect response JSON");
        let second_reached_upstream = second_turn_rx.await.expect("second turn observation");
        let status = server.get_status().await;

        drop(client);
        upstream_task.await.expect("direct upstream task");
        server.stop().await.expect("stop proxy");

        assert_eq!(second["type"], "error");
        assert!(second["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("global proxy changed")));
        assert!(!second_reached_upstream);
        assert_eq!(status.total_requests, 2);
        assert_eq!(status.success_requests, 1);
        assert_eq!(status.failed_requests, 1);
        assert!(status
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("global proxy changed")));
    }

    #[tokio::test]
    #[serial]
    async fn decodes_percent_encoded_socks5_credentials_before_authentication() {
        let proxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind authenticated SOCKS proxy");
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let (credentials_tx, credentials_rx) = oneshot::channel();
        let proxy_task = tokio::spawn(async move {
            let (mut client, _) = proxy_listener.accept().await.expect("accept SOCKS client");
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
            assert!(methods.contains(&2));
            client
                .write_all(&[5, 2])
                .await
                .expect("select username/password auth");

            let mut auth_head = [0u8; 2];
            client
                .read_exact(&mut auth_head)
                .await
                .expect("read SOCKS auth head");
            assert_eq!(auth_head[0], 1);
            let mut username = vec![0u8; auth_head[1] as usize];
            client
                .read_exact(&mut username)
                .await
                .expect("read SOCKS username");
            let mut password_len = [0u8; 1];
            client
                .read_exact(&mut password_len)
                .await
                .expect("read SOCKS password length");
            let mut password = vec![0u8; password_len[0] as usize];
            client
                .read_exact(&mut password)
                .await
                .expect("read SOCKS password");
            let _ = credentials_tx.send((username, password));
            client
                .write_all(&[1, 0])
                .await
                .expect("accept SOCKS credentials");

            let mut request_head = [0u8; 4];
            client
                .read_exact(&mut request_head)
                .await
                .expect("read SOCKS connect head");
            assert_eq!(request_head[3], 3);
            let mut host_len = [0u8; 1];
            client
                .read_exact(&mut host_len)
                .await
                .expect("read SOCKS target length");
            let mut target = vec![0u8; host_len[0] as usize + 2];
            client
                .read_exact(&mut target)
                .await
                .expect("read SOCKS target and port");
            client
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                .await
                .expect("accept SOCKS connect");
        });

        let proxy_url = Url::parse(&format!("socks5h://user%40name:p%C3%A4ss@{proxy_addr}"))
            .expect("parse authenticated SOCKS URL");
        let stream = connect_via_socks5(&proxy_url, "example.com", 443)
            .await
            .expect("connect through authenticated SOCKS proxy");
        drop(stream);
        let (username, password) = credentials_rx.await.expect("SOCKS credentials");
        proxy_task.await.expect("authenticated SOCKS proxy task");

        assert_eq!(username, b"user@name");
        assert_eq!(password, "päss".as_bytes());
    }

    #[tokio::test]
    #[serial]
    async fn socks5h_encodes_ipv6_literal_with_ipv6_address_type() {
        let proxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind SOCKS proxy");
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let (request_tx, request_rx) = oneshot::channel();
        let proxy_task = tokio::spawn(async move {
            let (mut client, _) = proxy_listener.accept().await.expect("accept SOCKS client");
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

            let mut request_head = [0u8; 4];
            client
                .read_exact(&mut request_head)
                .await
                .expect("read SOCKS connect head");
            let mut address = match request_head[3] {
                1 => vec![0u8; 4],
                3 => {
                    let mut length = [0u8; 1];
                    client
                        .read_exact(&mut length)
                        .await
                        .expect("read SOCKS domain length");
                    vec![0u8; length[0] as usize]
                }
                4 => vec![0u8; 16],
                other => panic!("unexpected SOCKS address type {other}"),
            };
            client
                .read_exact(&mut address)
                .await
                .expect("read SOCKS target address");
            let mut port = [0u8; 2];
            client.read_exact(&mut port).await.expect("read SOCKS port");
            let _ = request_tx.send((request_head, address, u16::from_be_bytes(port)));
            client
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                .await
                .expect("accept SOCKS connect");
        });

        let proxy_url =
            Url::parse(&format!("socks5h://{proxy_addr}")).expect("parse SOCKS proxy URL");
        let stream = connect_via_socks5(&proxy_url, "::1", 443)
            .await
            .expect("connect IPv6 literal through SOCKS proxy");
        drop(stream);
        let (request_head, address, port) = request_rx.await.expect("SOCKS request");
        proxy_task.await.expect("SOCKS proxy task");

        assert_eq!(request_head, [5, 1, 0, 4]);
        assert_eq!(address, std::net::Ipv6Addr::LOCALHOST.octets().to_vec());
        assert_eq!(port, 443);
    }

    #[tokio::test]
    #[serial]
    async fn socks5_local_dns_retries_all_resolved_addresses() {
        let resolved = tokio::net::lookup_host(("localhost", 443))
            .await
            .expect("resolve localhost")
            .collect::<Vec<_>>();
        assert!(
            resolved.len() >= 2,
            "test requires localhost to resolve to multiple addresses: {resolved:?}"
        );

        let proxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind SOCKS proxy");
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let (attempts_tx, attempts_rx) = oneshot::channel();
        let proxy_task = tokio::spawn(async move {
            let mut attempts = Vec::new();
            for attempt in 0..2 {
                let (mut client, _) = proxy_listener.accept().await.expect("accept SOCKS client");
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

                let mut request_head = [0u8; 4];
                client
                    .read_exact(&mut request_head)
                    .await
                    .expect("read SOCKS connect head");
                let mut address = match request_head[3] {
                    1 => vec![0u8; 4],
                    4 => vec![0u8; 16],
                    other => panic!("expected locally resolved IP address, got atyp={other}"),
                };
                client
                    .read_exact(&mut address)
                    .await
                    .expect("read SOCKS target address");
                let mut port = [0u8; 2];
                client.read_exact(&mut port).await.expect("read SOCKS port");
                attempts.push((request_head[3], address));

                let reply_code = if attempt == 0 { 4 } else { 0 };
                client
                    .write_all(&[5, reply_code, 0, 1, 127, 0, 0, 1, 0, 0])
                    .await
                    .expect("write SOCKS reply");
            }
            let _ = attempts_tx.send(attempts);
        });

        let proxy_url =
            Url::parse(&format!("socks5://{proxy_addr}")).expect("parse SOCKS proxy URL");
        let stream = tokio::time::timeout(
            Duration::from_secs(2),
            connect_via_socks5(&proxy_url, "localhost", 443),
        )
        .await
        .expect("SOCKS multi-address retry timed out")
        .expect("later locally resolved address should succeed");
        drop(stream);
        let attempts = attempts_rx.await.expect("SOCKS attempts");
        proxy_task.await.expect("SOCKS proxy task");

        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0], attempts[1]);
    }

    async fn run_proxy_routing_case(proxy_scheme: &str, use_system_proxy: bool) {
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
        let _system_proxy = use_system_proxy.then(|| EnvVarGuard::set("HTTP_PROXY", &proxy_url));
        if use_system_proxy {
            crate::proxy::http_client::apply_proxy(None).expect("follow system proxy");
        } else {
            crate::proxy::http_client::init(Some(&proxy_url)).expect("configure global proxy");
        }
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
        run_proxy_routing_case("http", false).await;
    }

    #[tokio::test]
    #[serial]
    async fn routes_websocket_through_configured_socks5h_proxy() {
        run_proxy_routing_case("socks5h", false).await;
    }

    #[tokio::test]
    #[serial]
    async fn routes_websocket_through_system_http_proxy() {
        run_proxy_routing_case("http", true).await;
    }
    #[tokio::test]
    #[serial]
    async fn later_turn_without_cursor_retries_remaining_websocket_provider() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-primary","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
            let _ = websocket.next().await;
        });

        let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_addr = unavailable_listener.local_addr().unwrap();
        drop(unavailable_listener);
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-later-fallback","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut primary =
            websocket_provider_with_id("ws-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut unavailable =
            websocket_provider_with_id("ws-unavailable", format!("http://{unavailable_addr}"));
        unavailable.sort_index = Some(1);
        let mut fallback =
            websocket_provider_with_id("ws-later-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(2);
        for provider in [&primary, &unavailable, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 2;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-primary");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if server.get_status().await.success_requests == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("first turn accounting did not finish");

        router
            .record_result(
                &primary.id,
                "codex",
                false,
                false,
                Some("open primary before later turn".to_string()),
            )
            .await
            .unwrap();
        let held_permit = router.allow_provider_request(&primary.id, "codex").await;
        assert!(held_permit.allowed && held_permit.used_half_open_permit);
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let second: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(second["response"]["id"], "resp-later-fallback", "{second}");

        router
            .release_permit_neutral(&primary.id, "codex", held_permit.used_half_open_permit)
            .await;
        drop(client);
        primary_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn clean_upstream_close_before_relay_retries_fallback() {
        let closing_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closing_addr = closing_listener.local_addr().unwrap();
        let closing_task = tokio::spawn(async move {
            let (stream, _) = closing_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket.close(None).await.unwrap();
        });

        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-clean-close-fallback","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut closing =
            websocket_provider_with_id("ws-clean-close", format!("http://{closing_addr}"));
        closing.sort_index = Some(0);
        let mut fallback = websocket_provider_with_id(
            "ws-clean-close-fallback",
            format!("http://{fallback_addr}"),
        );
        fallback.sort_index = Some(1);
        for provider in [&closing, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &closing.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let response: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(response["response"]["id"], "resp-clean-close-fallback");

        drop(client);
        closing_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn upstream_binary_before_relay_retries_fallback() {
        let binary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let binary_addr = binary_listener.local_addr().unwrap();
        let binary_task = tokio::spawn(async move {
            let (stream, _) = binary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Binary(vec![0x01, 0x02, 0x03]))
                .await
                .unwrap();
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-binary-fallback","output":[]}}).to_string(),
                ))
                .await
                .unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut binary = websocket_provider_with_id("ws-binary", format!("http://{binary_addr}"));
        binary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-binary-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&binary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &binary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let response: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("upstream binary frame must not be relayed");
        assert_eq!(response["response"]["id"], "resp-binary-fallback");

        drop(client);
        binary_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn malformed_upstream_text_before_relay_retries_fallback() {
        let malformed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let malformed_addr = malformed_listener.local_addr().unwrap();
        let malformed_task = tokio::spawn(async move {
            let (stream, _) = malformed_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text("not-json".to_string()))
                .await
                .unwrap();
        });

        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-valid-fallback","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut malformed =
            websocket_provider_with_id("ws-malformed", format!("http://{malformed_addr}"));
        malformed.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-malformed-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&malformed, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &malformed.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let response: Value = serde_json::from_str(&next_text(&mut client).await)
            .expect("malformed upstream text must not be relayed");
        assert_eq!(response["response"]["id"], "resp-valid-fallback");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let health = db
                    .get_provider_health(&malformed.id, "codex")
                    .await
                    .unwrap();
                if !health.is_healthy {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("protocol violation did not mark provider unhealthy");

        drop(client);
        malformed_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn changed_retained_fallback_snapshot_closes_before_reuse() {
        let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_addr = unavailable_listener.local_addr().unwrap();
        drop(unavailable_listener);

        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (old_received_tx, old_received_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-retained-one","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
            let reused = matches!(
                tokio::time::timeout(Duration::from_secs(2), websocket.next()).await,
                Ok(Some(Ok(UpstreamMessage::Text(_))))
            );
            if reused {
                websocket
                    .send(UpstreamMessage::Text(
                        json!({"type":"response.completed","response":{"id":"resp-stale-reuse","output":[]}})
                            .to_string(),
                    ))
                    .await
                    .unwrap();
            }
            let _ = old_received_tx.send(reused);
        });

        let db = Arc::new(Database::memory().unwrap());
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 60,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut primary =
            websocket_provider_with_id("ws-retained-primary", format!("http://{unavailable_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-retained-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-retained-one");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if server.get_status().await.success_requests == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        fallback.meta = Some(crate::provider::ProviderMeta {
            custom_user_agent: Some("edited-retained-provider".to_string()),
            ..Default::default()
        });
        db.save_provider("codex", &fallback).unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-retained-one")).to_string(),
            ))
            .await
            .unwrap();
        let second: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(
            second["type"], "error",
            "unexpected second response: {second}"
        );
        assert!(
            second["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("provider changed")),
            "unexpected retained-provider error: {second}"
        );
        assert!(!old_received_rx.await.unwrap());

        drop(client);
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn missing_retained_fallback_rejects_provider_cursor_turn() {
        let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_addr = unavailable_listener.local_addr().unwrap();
        drop(unavailable_listener);

        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-retained-removed","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
            let _ = websocket.next().await;
        });

        let replacement_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let replacement_addr = replacement_listener.local_addr().unwrap();
        let (replacement_received_tx, replacement_received_rx) = oneshot::channel();
        let (replacement_cancel_tx, mut replacement_cancel_rx) = oneshot::channel();
        let replacement_task = tokio::spawn(async move {
            let received = tokio::select! {
                accepted = replacement_listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    let text = next_text(&mut websocket).await;
                    websocket
                        .send(UpstreamMessage::Text(
                            json!({"type":"response.completed","response":{"id":"resp-wrong-provider","output":[]}})
                                .to_string(),
                        ))
                        .await
                        .unwrap();
                    response_create_has_provider_cursor(&text)
                }
                _ = &mut replacement_cancel_rx => false,
            };
            let _ = replacement_received_tx.send(received);
        });

        let db = Arc::new(Database::memory().unwrap());
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 60,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut primary =
            websocket_provider_with_id("ws-retained-primary", format!("http://{unavailable_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-retained-removed", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        let mut replacement = websocket_provider_with_id(
            "ws-retained-replacement",
            format!("http://{replacement_addr}"),
        );
        replacement.sort_index = Some(2);
        for provider in [&primary, &fallback, &replacement] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 2;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-retained-removed");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if server.get_status().await.success_requests == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let before_rejection = server.get_status().await;

        db.delete_provider("codex", &primary.id)
            .expect("delete failed primary");
        db.delete_provider("codex", &fallback.id)
            .expect("delete retained fallback");
        let remaining = db
            .get_failover_queue("codex")
            .expect("remaining failover queue");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].provider_id, replacement.id);
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-retained-removed")).to_string(),
            ))
            .await
            .unwrap();
        let second: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(
            second["type"], "error",
            "unexpected second response: {second}"
        );
        assert!(
            second["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("provider changed")),
            "unexpected retained-provider rejection: {second}"
        );
        let _ = replacement_cancel_tx.send(());
        assert!(!replacement_received_rx.await.unwrap());
        let after_rejection = server.get_status().await;
        assert_eq!(
            after_rejection.total_requests,
            before_rejection.total_requests + 1
        );
        assert_eq!(
            after_rejection.failed_requests,
            before_rejection.failed_requests + 1
        );
        assert!(after_rejection
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("provider changed")));

        drop(client);
        fallback_task.await.unwrap();
        replacement_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn generic_upstream_read_error_before_relay_retries_fallback() {
        let bad_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bad_addr = bad_listener.local_addr().unwrap();
        let bad_task = tokio::spawn(async move {
            let (stream, _) = bad_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket.get_mut().write_all(&[0x83, 0x00]).await.unwrap();
            websocket.get_mut().shutdown().await.unwrap();
        });

        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-read-fallback","output":[]}})
                        .to_string(),
                ))
                .await
                .unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut bad = websocket_provider_with_id("ws-bad-read", format!("http://{bad_addr}"));
        bad.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-read-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&bad, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &bad.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();

        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let response: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(response["response"]["id"], "resp-read-fallback");

        drop(client);
        bad_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn websocket_provider_health_persistence_preserves_result_order() {
        let db = Arc::new(Database::memory().unwrap());
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        db.save_provider("codex", &provider).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(ProxyConfig::default(), db.clone(), None);

        accounting_for_tests(&server, &provider)
            .finish_success()
            .await;
        accounting_for_tests(&server, &provider)
            .finish_provider_failure("later provider failure".to_string())
            .await;
        tokio::task::yield_now().await;

        let health = db
            .get_provider_health(&provider.id, "codex")
            .await
            .expect("provider health row");
        assert!(!health.is_healthy);
        assert_eq!(health.consecutive_failures, 1);
        assert_eq!(health.last_error.as_deref(), Some("later provider failure"));
    }
    #[tokio::test]
    #[serial]
    async fn retained_fallback_write_failure_wraps_to_primary_provider() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (failed_stream, _) = primary_listener.accept().await.unwrap();
            drop(failed_stream);

            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-wrapped-primary","output":[]}}).to_string(),
                ))
                .await
                .unwrap();
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-retained-fallback","output":[]}}).to_string(),
                ))
                .await
                .unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(3), websocket.next()).await;
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut primary =
            websocket_provider_with_id("ws-wrap-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-wrap-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 2;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-retained-fallback");

        FAIL_NEXT_REUSED_TURN_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let second: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(second["response"]["id"], "resp-wrapped-primary", "{second}");

        drop(client);
        primary_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn reused_upstream_write_failure_retries_remaining_provider() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-reused-primary","output":[]}}).to_string(),
                ))
                .await
                .unwrap();
            let _ = websocket.next().await;
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let fallback_task = tokio::spawn(async move {
            let (stream, _) = fallback_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-reused-fallback","output":[]}}).to_string(),
                ))
                .await
                .unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut primary =
            websocket_provider_with_id("ws-reused-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-reused-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-reused-primary");
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.get_status().await.success_requests != 1 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        FAIL_NEXT_REUSED_TURN_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let second: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(second["response"]["id"], "resp-reused-fallback", "{second}");

        drop(client);
        primary_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }
    #[tokio::test]
    #[serial]
    async fn reused_upstream_write_failure_preserves_provider_cursor_affinity() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.completed","response":{"id":"resp-cursor-primary","output":[]}}).to_string(),
                ))
                .await
                .unwrap();
            let _ = websocket.next().await;
        });
        let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_addr = fallback_listener.local_addr().unwrap();
        let (fallback_tx, fallback_rx) = oneshot::channel();
        let fallback_task = tokio::spawn(async move {
            let accepted =
                tokio::time::timeout(Duration::from_secs(2), fallback_listener.accept()).await;
            let used = if let Ok(Ok((stream, _))) = accepted {
                let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let _ = next_text(&mut websocket).await;
                websocket
                    .send(UpstreamMessage::Text(
                        json!({"type":"response.completed","response":{"id":"resp-cursor-replayed","output":[]}}).to_string(),
                    ))
                    .await
                    .unwrap();
                true
            } else {
                false
            };
            let _ = fallback_tx.send(used);
        });

        let db = Arc::new(Database::memory().unwrap());
        let mut primary =
            websocket_provider_with_id("ws-cursor-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut fallback =
            websocket_provider_with_id("ws-cursor-fallback", format!("http://{fallback_addr}"));
        fallback.sort_index = Some(1);
        for provider in [&primary, &fallback] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(first["response"]["id"], "resp-cursor-primary");
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.get_status().await.success_requests != 1 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        FAIL_NEXT_REUSED_TURN_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-cursor-primary")).to_string(),
            ))
            .await
            .unwrap();
        let second: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(second["type"], "error", "{second}");
        assert!(!fallback_rx.await.unwrap(), "provider cursor was replayed");

        drop(client);
        primary_task.await.unwrap();
        fallback_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn exhausted_later_turn_reconnects_record_proxy_failure_once() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket.send(UpstreamMessage::Text(json!({"type":"response.completed","response":{"id":"resp-status-primary","output":[]}}).to_string())).await.unwrap();
            let _ = websocket.next().await;
        });
        let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_addr = unavailable_listener.local_addr().unwrap();
        drop(unavailable_listener);

        let db = Arc::new(Database::memory().unwrap());
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut primary =
            websocket_provider_with_id("ws-status-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut unavailable = websocket_provider_with_id(
            "ws-status-unavailable",
            format!("http://{unavailable_addr}"),
        );
        unavailable.sort_index = Some(1);
        for provider in [&primary, &unavailable] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let _ = next_text(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.get_status().await.success_requests != 1 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        router
            .record_result(
                &primary.id,
                "codex",
                false,
                false,
                Some("open primary".into()),
            )
            .await
            .unwrap();
        let held = router.allow_provider_request(&primary.id, "codex").await;
        assert!(held.allowed && held.used_half_open_permit);

        client
            .send(UpstreamMessage::Text(
                response_create("local-model", Some("resp-status-primary")).to_string(),
            ))
            .await
            .unwrap();
        let error: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(error["type"], "error");
        let status = server.get_status().await;
        assert_eq!(status.total_requests, 2);
        assert_eq!(status.failed_requests, 1);
        assert!(status.last_error.is_some());

        router
            .release_permit_neutral(&primary.id, "codex", held.used_half_open_permit)
            .await;
        drop(client);
        primary_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn partial_read_error_updates_breaker_before_downstream_close() {
        let bad_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bad_addr = bad_listener.local_addr().unwrap();
        let bad_task = tokio::spawn(async move {
            let (stream, _) = bad_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.created","response":{"id":"resp-partial"}}).to_string(),
                ))
                .await
                .unwrap();
            websocket.get_mut().write_all(&[0x83, 0x00]).await.unwrap();
            websocket.get_mut().shutdown().await.unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let bad = websocket_provider_with_id("ws-partial-read", format!("http://{bad_addr}"));
        db.save_provider("codex", &bad).unwrap();
        db.add_to_failover_queue("codex", &bad.id).unwrap();
        db.set_current_provider("codex", &bad.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        DELAY_DROP_ACCOUNTING.store(true, std::sync::atomic::Ordering::SeqCst);
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let created: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        let error: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(created["type"], "response.created");
        assert_eq!(error["type"], "error");
        let permit = router.allow_provider_request(&bad.id, "codex").await;
        assert!(
            !permit.allowed,
            "failed provider remained selectable before close"
        );

        tokio::time::sleep(Duration::from_millis(600)).await;
        drop(client);
        bad_task.await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn upstream_relay_write_failure_updates_breaker_before_downstream_close() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.created","response":{"id":"resp-relay-write"}})
                        .to_string(),
                ))
                .await
                .unwrap();
            let _ = websocket.next().await;
        });

        let db = Arc::new(Database::memory().unwrap());
        let provider =
            websocket_provider_with_id("ws-relay-write", format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).unwrap();
        db.add_to_failover_queue("codex", &provider.id).unwrap();
        db.set_current_provider("codex", &provider.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let created: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(created["type"], "response.created");

        DELAY_DROP_ACCOUNTING.store(true, std::sync::atomic::Ordering::SeqCst);
        FAIL_NEXT_RELAY_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);
        client
            .send(UpstreamMessage::Ping(vec![1, 2, 3]))
            .await
            .unwrap();
        let error: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(error["type"], "error");
        let permit = router.allow_provider_request(&provider.id, "codex").await;
        let provider_remained_selectable = permit.allowed;
        if permit.allowed {
            router
                .release_permit_neutral(&provider.id, "codex", permit.used_half_open_permit)
                .await;
        }

        tokio::time::sleep(Duration::from_millis(600)).await;
        drop(client);
        upstream_task.await.unwrap();
        server.stop().await.unwrap();
        assert!(
            !provider_remained_selectable,
            "relay write failure did not update the breaker before downstream close"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn relayed_provider_failure_does_not_block_shutdown_on_health_persistence() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (created_tx, created_rx) = oneshot::channel();
        let (failed_tx, failed_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.created","response":{"id":"resp-relayed-failure"}})
                        .to_string(),
                ))
                .await
                .unwrap();
            let _ = created_tx.send(());
            let _ = failed_rx.await;
            websocket.send(UpstreamMessage::Text(json!({"type":"response.failed","response":{"id":"resp-relayed-failure","error":{"type":"server_error","message":"failed"}}}).to_string())).await.unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).unwrap();
        db.set_current_provider("codex", &provider.id).unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let created: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(created["type"], "response.created");
        created_rx.await.unwrap();

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let locked_db = db.clone();
        let lock_thread = std::thread::spawn(move || {
            let _guard = locked_db.conn.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();
        let _ = failed_tx.send(());
        let failed: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(failed["type"], "response.failed");
        drop(client);

        let stop_result = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
        let _ = release_tx.send(());
        lock_thread.join().unwrap();
        stop_result
            .expect("relayed provider failure blocked shutdown")
            .unwrap();
        upstream_task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn partial_response_clean_close_does_not_block_shutdown_on_health_persistence() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (created_tx, created_rx) = oneshot::channel();
        let (close_tx, close_rx) = oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket
                .send(UpstreamMessage::Text(
                    json!({"type":"response.created","response":{"id":"resp-partial-clean-close"}})
                        .to_string(),
                ))
                .await
                .unwrap();
            let _ = created_tx.send(());
            let _ = close_rx.await;
            websocket.close(None).await.unwrap();
        });

        let db = Arc::new(Database::memory().unwrap());
        let provider = websocket_provider(format!("http://{upstream_addr}"));
        db.save_provider("codex", &provider).unwrap();
        db.set_current_provider("codex", &provider.id).unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db.clone(),
            None,
        );
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let created: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(created["type"], "response.created");
        created_rx.await.unwrap();

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let locked_db = db.clone();
        let lock_thread = std::thread::spawn(move || {
            let _guard = locked_db.conn.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();
        let _ = close_tx.send(());
        upstream_task.await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(client);

        let stop_result = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
        let _ = release_tx.send(());
        lock_thread.join().unwrap();
        stop_result
            .expect("partial-response clean close blocked shutdown")
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn later_turn_deadline_includes_reconnect_handshake_time() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_task = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            websocket.send(UpstreamMessage::Text(json!({"type":"response.completed","response":{"id":"resp-deadline-primary","output":[]}}).to_string())).await.unwrap();
            let _ = websocket.next().await;
        });
        let delayed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let delayed_addr = delayed_listener.local_addr().unwrap();
        let delayed_task = tokio::spawn(async move {
            let (stream, _) = delayed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(700)).await;
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = next_text(&mut websocket).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = websocket.send(UpstreamMessage::Text(json!({"type":"response.completed","response":{"id":"resp-deadline-late","output":[]}}).to_string())).await;
        });

        let db = Arc::new(Database::memory().unwrap());
        db.update_circuit_breaker_config(&crate::proxy::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut primary =
            websocket_provider_with_id("ws-deadline-primary", format!("http://{primary_addr}"));
        primary.sort_index = Some(0);
        let mut delayed =
            websocket_provider_with_id("ws-deadline-delayed", format!("http://{delayed_addr}"));
        delayed.sort_index = Some(1);
        for provider in [&primary, &delayed] {
            db.save_provider("codex", provider).unwrap();
            db.add_to_failover_queue("codex", &provider.id).unwrap();
        }
        db.set_current_provider("codex", &primary.id).unwrap();
        let mut app_config = db.get_proxy_config_for_app("codex").await.unwrap();
        app_config.auto_failover_enabled = true;
        app_config.max_retries = 1;
        app_config.circuit_failure_threshold = 1;
        app_config.streaming_first_byte_timeout = 1;
        db.update_proxy_config_for_app(app_config).await.unwrap();
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".into(),
                listen_port: 0,
                ..Default::default()
            },
            db,
            None,
        );
        let router = server.provider_router_for_tests();
        let info = server.start().await.unwrap();
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .unwrap();
        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let _ = next_text(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.get_status().await.success_requests != 1 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        router
            .record_result(
                &primary.id,
                "codex",
                false,
                false,
                Some("open primary".into()),
            )
            .await
            .unwrap();
        let held = router.allow_provider_request(&primary.id, "codex").await;
        assert!(held.allowed && held.used_half_open_permit);

        client
            .send(UpstreamMessage::Text(
                response_create("local-model", None).to_string(),
            ))
            .await
            .unwrap();
        let result: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(
            result["type"], "error",
            "reconnect time was excluded: {result}"
        );

        router
            .release_permit_neutral(&primary.id, "codex", held.used_half_open_permit)
            .await;
        drop(client);
        primary_task.await.unwrap();
        delayed_task.await.unwrap();
        server.stop().await.unwrap();
    }
}
