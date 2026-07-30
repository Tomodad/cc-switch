//! Protocol-aware WebSocket transport for the OpenAI Responses API.
//!
//! The downstream Codex socket is terminated locally so CC-Switch can retain
//! provider selection, auth replacement, model mapping, ToolSearch shims, and
//! namespace restoration instead of bypassing them with a transparent tunnel.

use super::{
    forwarder::{
        apply_local_proxy_body_overrides, apply_local_proxy_header_overrides,
        prepare_upstream_request_body,
    },
    handler_context::RequestContext,
    providers::{
        codex_provider_supports_responses_websocket, should_inject_codex_tool_search_shim,
        should_restore_codex_native_tool_search, transform_codex_chat,
        transform_codex_responses_namespace, CodexAdapter, ProviderAdapter,
    },
    server::ProxyState,
    ProxyError,
};
use crate::{app_config::AppType, provider::Provider};
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
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    protocol::{frame::coding::CloseCode, CloseFrame as UpstreamCloseFrame},
    Error as WebSocketError, Message as UpstreamMessage,
};
use url::Url;

const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const RESPONSES_ENDPOINT: &str = "/responses";

#[derive(Default)]
struct TurnTransformState {
    namespace_restore_map:
        std::collections::HashMap<String, transform_codex_responses_namespace::NamespacedName>,
    restore_tool_search: bool,
}

pub async fn handle_responses_websocket(
    State(state): State<ProxyState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade.on_upgrade(move |socket| handle_connection(socket, state, headers))
}

async fn handle_connection(mut downstream: WebSocket, state: ProxyState, headers: HeaderMap) {
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
    let first_text = receive_first_text(downstream).await?;
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
    let provider = ctx
        .get_providers()
        .into_iter()
        .find(codex_provider_supports_responses_websocket)
        .ok_or_else(|| {
            ProxyError::ConfigError(
                "selected Codex provider does not support native Responses WebSocket".to_string(),
            )
        })?;

    let (first_text, turn_state) =
        transform_client_text(&first_text, &provider, true)?.expect("validated response.create");
    let mut turn_state = turn_state.expect("response.create transform state");
    let mut response_in_flight = true;
    let request = build_upstream_request(&provider, headers)?;
    let connect = tokio_tungstenite::connect_async(request);
    let (mut upstream, _) = tokio::time::timeout(UPSTREAM_CONNECT_TIMEOUT, connect)
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
        tokio::select! {
            downstream_message = downstream.recv() => {
                let Some(downstream_message) = downstream_message else {
                    let _ = upstream.close(None).await;
                    break;
                };
                let downstream_message = downstream_message.map_err(|error| {
                    ProxyError::ForwardFailed(format!("downstream WebSocket read failed: {error}"))
                })?;

                match downstream_message {
                    DownstreamMessage::Text(text) => {
                        if response_in_flight && websocket_event_is(&text, "response.create") {
                            return Err(ProxyError::InvalidRequest(
                                "only one response.create may be in flight per WebSocket".to_string(),
                            ));
                        }
                        let (text, next_state) = transform_client_text(&text, &provider, false)?
                            .unwrap_or((text, None));
                        if let Some(next_state) = next_state {
                            turn_state = next_state;
                            response_in_flight = true;
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
                        let terminal = websocket_event_is_terminal(&text);
                        let text = restore_upstream_text(text, &turn_state);
                        downstream.send(DownstreamMessage::Text(text)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("downstream WebSocket write failed: {error}"))
                        })?;
                        if terminal {
                            response_in_flight = false;
                        }
                    }
                    UpstreamMessage::Binary(data) => {
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

    let namespace_restore_map =
        transform_codex_responses_namespace::namespace_restore_map(&original_body);
    let request_uses_tool_search_shim =
        transform_codex_chat::request_uses_responses_tool_search_shim(&original_body);
    let (mut body, _, _) = super::model_mapper::apply_model_mapping(original_body, provider);

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
    };
    let encoded = serde_json::to_string(&event).map_err(|error| {
        ProxyError::Internal(format!("failed to serialize WebSocket event: {error}"))
    })?;
    Ok(Some((encoded, Some(state))))
}

fn websocket_event_is(text: &str, expected: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|event| {
            event
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .is_some_and(|event_type| event_type == expected)
}

fn websocket_event_is_terminal(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|event| {
            event
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .is_some_and(|event_type| {
            matches!(
                event_type.as_str(),
                "response.completed" | "response.failed" | "response.incomplete" | "error"
            )
        })
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
        "session_id",
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
    use serial_test::serial;
    use std::sync::Arc;
    use tokio::{net::TcpListener, sync::oneshot};
    use tokio_tungstenite::{accept_hdr_async, connect_async};

    fn websocket_provider(base_url: String) -> Provider {
        let mut provider = Provider::with_id(
            "ws-provider".to_string(),
            "WebSocket Provider".to_string(),
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
            let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
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

        let (encoded, state) = transform_client_text(&input, &provider, true)
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
    fn transforms_response_create_and_restores_native_events() {
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        let input = response_create("local-model", None).to_string();
        let (encoded, state) = transform_client_text(&input, &provider, true)
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
                            }]
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
                            }]
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
}
