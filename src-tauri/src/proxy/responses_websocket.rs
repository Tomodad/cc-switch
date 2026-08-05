//! Minimal protocol-aware proxy for Codex Responses WebSocket requests.

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
use std::{borrow::Cow, collections::HashMap, time::Duration};
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    protocol::{frame::coding::CloseCode, CloseFrame as UpstreamCloseFrame},
    Error as WebSocketError, Message as UpstreamMessage,
};
use url::Url;

const RESPONSES_ENDPOINT: &str = "/responses";
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const UPSTREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Default)]
struct RestoreState {
    namespace_restore_map: HashMap<String, transform_codex_responses_namespace::NamespacedName>,
    restore_tool_search: bool,
    tool_search_item_ids: HashMap<String, String>,
}

pub async fn handle_responses_websocket(
    State(state): State<ProxyState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade.on_upgrade(move |socket| handle_connection(socket, state, headers))
}

async fn handle_connection(mut downstream: WebSocket, state: ProxyState, headers: HeaderMap) {
    if let Err(error) = handle_connection_inner(&mut downstream, &state, &headers).await {
        log::warn!("[CodexWS] Closing Responses WebSocket: {error}");
        send_proxy_error_and_close(&mut downstream, &error).await;
    }
}

async fn handle_connection_inner(
    downstream: &mut WebSocket,
    state: &ProxyState,
    headers: &HeaderMap,
) -> Result<(), ProxyError> {
    let first_event_timeout = {
        let config = state.config.read().await;
        Duration::from_secs(config.streaming_first_byte_timeout.max(1))
    };
    let first_text = tokio::time::timeout(first_event_timeout, receive_first_text(downstream))
        .await
        .map_err(|_| ProxyError::Timeout("downstream response.create timed out".to_string()))??;
    let first_event: Value = serde_json::from_str(&first_text)
        .map_err(|error| ProxyError::InvalidRequest(format!("invalid WebSocket JSON: {error}")))?;
    let response_body = response_create_body(&first_event)?;

    let context = RequestContext::new(
        state,
        &response_body,
        headers,
        AppType::Codex,
        "CodexWS",
        "codex",
    )
    .await?;
    let provider = context
        .get_providers()
        .into_iter()
        .next()
        .ok_or(ProxyError::NoAvailableProvider)?;
    if !codex_provider_supports_responses_websocket(&provider) {
        return Err(ProxyError::ConfigError(
            "selected Codex provider does not support native Responses WebSocket".to_string(),
        ));
    }

    let (first_text, mut restore_state) = transform_response_create(&first_text, &provider)?;
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
            ProxyError::ForwardFailed(format!("failed to send response.create: {error}"))
        })?;

    loop {
        let idle = tokio::time::sleep(UPSTREAM_IDLE_TIMEOUT);
        tokio::pin!(idle);
        tokio::select! {
            _ = &mut idle => {
                return Err(ProxyError::Timeout(
                    "upstream Responses WebSocket became idle".to_string(),
                ));
            }
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
                        if websocket_event_is(&text, "response.create") {
                            return Err(ProxyError::InvalidRequest(
                                "one response.create is supported per WebSocket connection".to_string(),
                            ));
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
                        let text = restore_upstream_text(text, &mut restore_state);
                        downstream.send(DownstreamMessage::Text(text)).await.map_err(|error| {
                            ProxyError::ForwardFailed(format!("downstream WebSocket write failed: {error}"))
                        })?;
                        if terminal {
                            let _ = upstream.close(None).await;
                            let _ = downstream
                                .send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
                                    code: 1000,
                                    reason: Cow::Borrowed("response complete"),
                                })))
                                .await;
                            break;
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
                ProxyError::InvalidRequest("WebSocket closed before response.create".to_string())
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
                    "WebSocket closed before response.create".to_string(),
                ));
            }
            DownstreamMessage::Binary(_) => {
                return Err(ProxyError::InvalidRequest(
                    "response.create must be JSON text".to_string(),
                ));
            }
        }
    }
}

fn response_create_body(event: &Value) -> Result<Value, ProxyError> {
    let event = event.as_object().ok_or_else(|| {
        ProxyError::InvalidRequest("response.create payload must be an object".to_string())
    })?;
    if event.get("type").and_then(Value::as_str) != Some("response.create") {
        return Err(ProxyError::InvalidRequest(
            "the first WebSocket event must be response.create".to_string(),
        ));
    }
    if let Some(response) = event.get("response") {
        return response
            .as_object()
            .map(|_| response.clone())
            .ok_or_else(|| {
                ProxyError::InvalidRequest("response.create.response must be an object".to_string())
            });
    }

    let mut body = event.clone();
    body.remove("type");
    Ok(Value::Object(body))
}

fn transform_response_create(
    text: &str,
    provider: &Provider,
) -> Result<(String, RestoreState), ProxyError> {
    let event: Value = serde_json::from_str(text)
        .map_err(|error| ProxyError::InvalidRequest(format!("invalid WebSocket JSON: {error}")))?;
    let original_body = response_create_body(&event)?;
    let namespace_restore_map =
        transform_codex_responses_namespace::namespace_restore_map(&original_body);
    let request_uses_tool_search_shim =
        transform_codex_chat::request_uses_responses_tool_search_shim(&original_body);
    let (body, _, _) = super::model_mapper::apply_model_mapping(original_body, provider);
    let body = super::thinking_rectifier::normalize_thinking_type(body);
    let mut body = super::model_mapper::strip_one_m_suffix_for_upstream_from_body(body);

    let inject_tool_search = should_inject_codex_tool_search_shim(provider, RESPONSES_ENDPOINT);
    if inject_tool_search {
        transform_codex_chat::ensure_responses_tool_search_shim(&mut body, true);
    }
    if inject_tool_search
        && transform_codex_responses_namespace::flatten_request_namespaces(&mut body)?
    {
        log::debug!(
            "[CodexWS] Flattened namespace tools for native upstream (provider={})",
            provider.id
        );
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
    body.as_object_mut()
        .ok_or_else(|| {
            ProxyError::InvalidRequest("response.create payload must be an object".to_string())
        })?
        .insert(
            "type".to_string(),
            Value::String("response.create".to_string()),
        );

    let encoded = serde_json::to_string(&body).map_err(|error| {
        ProxyError::Internal(format!("failed to serialize response.create: {error}"))
    })?;
    Ok((
        encoded,
        RestoreState {
            namespace_restore_map,
            restore_tool_search: request_uses_tool_search_shim
                && should_restore_codex_native_tool_search(provider, RESPONSES_ENDPOINT),
            tool_search_item_ids: HashMap::new(),
        },
    ))
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

fn restore_upstream_text(text: String, state: &mut RestoreState) -> String {
    if state.namespace_restore_map.is_empty() && !state.restore_tool_search {
        return text;
    }
    let Ok(mut event) = serde_json::from_str::<Value>(&text) else {
        return text;
    };
    if !transform_codex_responses_namespace::restore_sse_event_tool_calls(
        &mut event,
        &state.namespace_restore_map,
        state.restore_tool_search,
        &mut state.tool_search_item_ids,
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

    fn response_create() -> Value {
        json!({
            "type": "response.create",
            "model": "local-model",
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
        })
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
    fn response_create_matches_native_http_transform_semantics() {
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        let (encoded, mut state) =
            transform_response_create(&response_create().to_string(), &provider)
                .expect("transform");
        let event: Value = serde_json::from_str(&encoded).expect("transformed event JSON");

        assert_eq!(event["type"], "response.create");
        assert_eq!(event["model"], "upstream-model");
        assert!(event.get("_private").is_none());
        let tools = event["tools"].as_array().expect("tools");
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
                "type": "response.completed",
                "response": {
                    "output": [{
                        "type": "function_call",
                        "name": "demo__run",
                        "call_id": "call-1",
                        "arguments": "{}"
                    }]
                }
            })
            .to_string(),
            &mut state,
        );
        let restored: Value = serde_json::from_str(&restored).expect("restored event");
        assert_eq!(restored["response"]["output"][0]["name"], "run");
        assert_eq!(restored["response"]["output"][0]["namespace"], "demo");
    }

    #[test]
    fn websocket_stream_restores_tool_search_ids_consistently() {
        let provider = websocket_provider("http://127.0.0.1:1".to_string());
        let (_, mut state) = transform_response_create(&response_create().to_string(), &provider)
            .expect("transform");

        let restore = |state: &mut RestoreState, event: Value| {
            serde_json::from_str::<Value>(&restore_upstream_text(event.to_string(), state))
                .expect("restored event")
        };
        let added = restore(
            &mut state,
            json!({
                "type": "response.output_item.added",
                "item": {
                    "id": "fc_search-1",
                    "type": "function_call",
                    "name": "tool_search",
                    "call_id": "search-1",
                    "arguments": ""
                }
            }),
        );
        let delta = restore(
            &mut state,
            json!({
                "type": "response.function_call_arguments.delta",
                "item_id": "fc_search-1",
                "delta": "{\"query\":\"thread tools\"}"
            }),
        );
        let done = restore(
            &mut state,
            json!({
                "type": "response.output_item.done",
                "item": {
                    "id": "fc_search-1",
                    "type": "function_call",
                    "name": "tool_search",
                    "call_id": "search-1",
                    "arguments": "{\"query\":\"thread tools\"}"
                }
            }),
        );
        let completed = restore(
            &mut state,
            json!({
                "type": "response.completed",
                "response": {
                    "output": [{
                        "id": "fc_search-1",
                        "type": "function_call",
                        "name": "tool_search",
                        "call_id": "search-1",
                        "arguments": "{\"query\":\"thread tools\"}"
                    }]
                }
            }),
        );

        for item in [
            &added["item"],
            &done["item"],
            &completed["response"]["output"][0],
        ] {
            assert_eq!(item["type"], "tool_search_call");
            assert_eq!(item["id"], "tsc_search-1");
            assert_eq!(item["call_id"], "search-1");
        }
        assert_eq!(delta["item_id"], "tsc_search-1");
    }

    #[test]
    fn upstream_request_targets_selected_provider_and_replaces_downstream_auth() {
        let provider = websocket_provider("https://provider.example/v1".to_string());
        let mut downstream_headers = HeaderMap::new();
        downstream_headers.insert("authorization", "Bearer downstream-secret".parse().unwrap());
        downstream_headers.insert(
            "openai-beta",
            "responses_websockets=2026-02-06".parse().unwrap(),
        );

        let request = build_upstream_request(&provider, &downstream_headers).expect("request");
        assert_eq!(request.uri().scheme_str(), Some("wss"));
        assert_eq!(request.uri().host(), Some("provider.example"));
        assert_eq!(request.uri().path(), "/v1/responses");
        assert_eq!(
            request.headers().get("authorization").unwrap(),
            "Bearer provider-secret"
        );
        assert_eq!(
            request.headers().get("openai-beta").unwrap(),
            "responses_websockets=2026-02-06"
        );
    }

    #[tokio::test]
    #[serial]
    async fn proxies_native_responses_websocket_end_to_end() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake upstream");
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (headers_tx, headers_rx) = oneshot::channel();
        let (event_tx, event_rx) = oneshot::channel();

        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.expect("accept upstream");
            let mut headers_tx = Some(headers_tx);
            let mut websocket =
                accept_hdr_async(stream, move |request: &http::Request<()>, response| {
                    let captured = (
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
                        let _ = tx.send(captured);
                    }
                    Ok(response)
                })
                .await
                .expect("accept websocket handshake");

            let event = next_text(&mut websocket).await;
            let _ = event_tx.send(event);
            websocket
                .send(UpstreamMessage::Text(
                    json!({
                        "type": "response.created",
                        "response": {"id": "resp-1", "output": []}
                    })
                    .to_string(),
                ))
                .await
                .expect("send created event");
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
            .send(UpstreamMessage::Text(response_create().to_string()))
            .await
            .expect("send response.create");

        let created: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(created["type"], "response.created");
        let completed: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(completed["type"], "response.completed");
        assert_eq!(completed["response"]["output"][0]["name"], "run");
        assert_eq!(completed["response"]["output"][0]["namespace"], "demo");

        let close = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("timed out waiting for close")
            .expect("client stream closed before close frame")
            .expect("client close error");
        assert!(matches!(close, UpstreamMessage::Close(_)));

        let (path, authorization, beta) = headers_rx.await.expect("upstream headers");
        assert_eq!(path, "/v1/responses");
        assert_eq!(authorization.as_deref(), Some("Bearer provider-secret"));
        assert_eq!(beta.as_deref(), Some("responses_websockets=2026-02-06"));
        let upstream_event: Value =
            serde_json::from_str(&event_rx.await.expect("upstream event")).unwrap();
        assert_eq!(upstream_event["type"], "response.create");
        assert_eq!(upstream_event["model"], "upstream-model");
        assert!(upstream_event.get("_private").is_none());

        upstream_task.await.expect("fake upstream task");
        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn idle_downstream_before_response_create_times_out_and_closes() {
        let db = Arc::new(Database::memory().expect("create in-memory database"));
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                streaming_first_byte_timeout: 1,
                ..Default::default()
            },
            db,
            None,
        );
        let info = server.start().await.expect("start proxy");
        let (mut client, _) = connect_async(format!("ws://127.0.0.1:{}/v1/responses", info.port))
            .await
            .expect("connect local proxy");

        let error_text = tokio::time::timeout(Duration::from_secs(2), next_text(&mut client))
            .await
            .expect("proxy did not bound the wait for response.create");
        let error: Value = serde_json::from_str(&error_text).expect("proxy error event JSON");
        assert_eq!(error["type"], "error");
        assert!(error["error"]["message"]
            .as_str()
            .is_some_and(
                |message| message.contains("response.create") && message.contains("timed out")
            ));

        let close = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .expect("timed out waiting for close")
            .expect("client stream closed before close frame")
            .expect("client close error");
        match close {
            UpstreamMessage::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1011),
            other => panic!("expected close frame, got {other:?}"),
        }

        server.stop().await.expect("stop proxy");
    }

    #[tokio::test]
    #[serial]
    async fn upstream_connect_failure_emits_error_and_close() {
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
            .send(UpstreamMessage::Text(response_create().to_string()))
            .await
            .expect("send response.create");

        let error: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
        assert_eq!(error["type"], "error");
        assert_eq!(error["error"]["type"], "proxy_error");
        assert!(error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("WebSocket handshake failed")));
        let close = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("timed out waiting for close")
            .expect("client stream closed before close frame")
            .expect("client close error");
        match close {
            UpstreamMessage::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1011),
            other => panic!("expected close frame, got {other:?}"),
        }

        server.stop().await.expect("stop proxy");
    }
}
