//! HTTP代理服务器
//!
//! 基于Axum的HTTP服务器，处理代理请求
//!
//! Uses a manual hyper HTTP/1.1 accept loop with `preserve_header_case(true)` so
//! that the original header-name casing from the CLI client is captured in a
//! `HeaderCaseMap` extension.  This map is later forwarded to the upstream via
//! the hyper-based HTTP client, producing wire-level header casing identical to
//! a direct (non-proxied) CLI request.

use super::{
    failover_switch::FailoverSwitchManager,
    handlers,
    log_codes::srv as log_srv,
    provider_router::ProviderRouter,
    providers::{codex_chat_history::CodexChatHistoryStore, gemini_shadow::GeminiShadowStore},
    types::*,
    ProxyError,
};
use crate::database::Database;
use axum::{
    extract::DefaultBodyLimit,
    routing::{any, get, post},
    Router,
};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{oneshot, watch, RwLock};
use tokio::task::{JoinHandle, JoinSet};

#[cfg(test)]
static PENDING_CONNECTION_PEEKS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
struct PendingConnectionPeekGuard;

#[cfg(test)]
impl PendingConnectionPeekGuard {
    fn new() -> Self {
        PENDING_CONNECTION_PEEKS.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

#[cfg(test)]
impl Drop for PendingConnectionPeekGuard {
    fn drop(&mut self) {
        PENDING_CONNECTION_PEEKS.fetch_sub(1, Ordering::AcqRel);
    }
}

/// 代理服务器状态（共享）
#[derive(Clone)]
pub struct ProxyState {
    pub db: Arc<Database>,
    pub config: Arc<RwLock<ProxyConfig>>,
    pub status: Arc<RwLock<ProxyStatus>>,
    pub start_time: Arc<RwLock<Option<std::time::Instant>>>,
    /// 每个应用类型当前使用的 provider (app_type -> (provider_id, provider_name))
    pub current_providers: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
    /// 共享的 ProviderRouter（持有熔断器状态，跨请求保持）
    pub provider_router: Arc<ProviderRouter>,
    /// Gemini Native shadow state，用于 thoughtSignature / tool call 回放
    pub gemini_shadow: Arc<GeminiShadowStore>,
    /// Codex Chat bridge history，用于恢复 previous_response_id 指向的 tool call
    pub codex_chat_history: Arc<CodexChatHistoryStore>,
    /// AppHandle，用于发射事件和更新托盘菜单
    pub app_handle: Option<tauri::AppHandle>,
    /// 故障转移切换管理器
    pub failover_manager: Arc<FailoverSwitchManager>,
    /// Notifies upgraded WebSocket connections that proxy shutdown has begun.
    pub websocket_shutdown_tx: watch::Sender<bool>,
    pub(crate) websocket_active: Arc<AtomicUsize>,
}

pub(crate) struct WebSocketConnectionGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for WebSocketConnectionGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ProxyState {
    pub(crate) fn track_websocket_connection(&self) -> WebSocketConnectionGuard {
        self.websocket_active.fetch_add(1, Ordering::AcqRel);
        WebSocketConnectionGuard {
            active: self.websocket_active.clone(),
        }
    }
}

/// 代理HTTP服务器
pub struct ProxyServer {
    config: ProxyConfig,
    state: ProxyState,
    shutdown_tx: Arc<RwLock<Option<oneshot::Sender<()>>>>,
    /// 服务器任务句柄，用于等待服务器实际关闭
    server_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl ProxyServer {
    pub fn new(
        config: ProxyConfig,
        db: Arc<Database>,
        app_handle: Option<tauri::AppHandle>,
    ) -> Self {
        // 创建共享的 ProviderRouter（熔断器状态将跨所有请求保持）
        let provider_router = Arc::new(ProviderRouter::new(db.clone()));
        // 创建故障转移切换管理器
        let failover_manager = Arc::new(FailoverSwitchManager::new(db.clone()));

        let (websocket_shutdown_tx, _) = watch::channel(false);
        let state = ProxyState {
            db,
            config: Arc::new(RwLock::new(config.clone())),
            status: Arc::new(RwLock::new(ProxyStatus::default())),
            start_time: Arc::new(RwLock::new(None)),
            current_providers: Arc::new(RwLock::new(std::collections::HashMap::new())),
            provider_router,
            gemini_shadow: Arc::new(GeminiShadowStore::default()),
            codex_chat_history: Arc::new(CodexChatHistoryStore::default()),
            app_handle,
            failover_manager,
            websocket_shutdown_tx,
            websocket_active: Arc::new(AtomicUsize::new(0)),
        };

        Self {
            config,
            state,
            shutdown_tx: Arc::new(RwLock::new(None)),
            server_handle: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn start(&self) -> Result<ProxyServerInfo, ProxyError> {
        self.state.websocket_shutdown_tx.send_replace(false);
        // 检查是否已在运行
        if self.shutdown_tx.read().await.is_some() {
            return Err(ProxyError::AlreadyRunning);
        }

        let addr: SocketAddr =
            format!("{}:{}", self.config.listen_address, self.config.listen_port)
                .parse()
                .map_err(|e| ProxyError::BindFailed(format!("无效的地址: {e}")))?;

        // 创建关闭通道
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // 构建路由
        let app = self.build_router();

        // 绑定监听器
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| ProxyError::BindFailed(e.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| ProxyError::BindFailed(e.to_string()))?;
        let actual_port = local_addr.port();

        log::info!("[{}] 代理服务器启动于 {local_addr}", log_srv::STARTED);

        // 更新全局代理端口，用于系统代理检测
        crate::proxy::http_client::set_proxy_port(actual_port);

        // 保存关闭句柄
        *self.shutdown_tx.write().await = Some(shutdown_tx);

        // 更新状态
        let mut status = self.state.status.write().await;
        status.running = true;
        status.address = self.config.listen_address.clone();
        status.port = actual_port;
        drop(status);

        // 记录启动时间
        *self.state.start_time.write().await = Some(std::time::Instant::now());

        // 启动服务器 — 使用手动 hyper HTTP/1.1 accept loop
        // 开启 preserve_header_case 以捕获客户端请求头的原始大小写
        let state = self.state.clone();
        let handle = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    result = listener.accept() => {
                        let (stream, _remote_addr) = match result {
                            Ok(v) => v,
                            Err(e) => {
                                log::error!("[{SRV}] accept 失败: {e}", SRV = log_srv::ACCEPT_ERR);
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                        let app = app.clone();
                        connections.spawn(async move {
                            #[cfg(test)]
                            let _pending_peek_guard = PendingConnectionPeekGuard::new();

                            // Peek raw TCP bytes to capture original header casing
                            // before hyper parses (and lowercases) the header names.
                            let original_cases = {
                                let mut peek_buf = vec![0u8; 8192];
                                match stream.peek(&mut peek_buf).await {
                                    Ok(n) => {
                                        let cases = super::hyper_client::OriginalHeaderCases::from_raw_bytes(&peek_buf[..n]);
                                        log::debug!(
                                            "[ProxyServer] Peeked {} bytes, captured {} header casings",
                                            n, cases.cases.len()
                                        );
                                        cases
                                    }
                                    Err(e) => {
                                        log::debug!("[ProxyServer] peek failed (non-fatal): {e}");
                                        super::hyper_client::OriginalHeaderCases::default()
                                    }
                                }
                            };

                            // service_fn 将 axum Router（tower::Service）桥接到 hyper
                            let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                                let mut router = app.clone();
                                let cases = original_cases.clone();
                                async move {
                                    // 将 hyper::body::Incoming 转为 axum::body::Body，保留 extensions
                                    let (mut parts, body) = req.into_parts();

                                    // Insert our own header case map alongside hyper's internal one
                                    parts.extensions.insert(cases);

                                    let body = axum::body::Body::new(body);
                                    let axum_req = http::Request::from_parts(parts, body);
                                    <Router as tower::Service<http::Request<axum::body::Body>>>::call(&mut router, axum_req).await
                                }
                            });

                            if let Err(e) = hyper::server::conn::http1::Builder::new()
                                .preserve_header_case(true)
                                .serve_connection(TokioIo::new(stream), service)
                                .with_upgrades()
                                .await
                            {
                                // Connection reset / broken pipe 等在代理场景下很常见，debug 级别
                                log::debug!("[{SRV}] connection error: {e}", SRV = log_srv::CONN_ERR);
                            }
                        });
                    }
                    completed = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(error)) = completed {
                            log::debug!(
                                "[{SRV}] connection task ended unexpectedly: {error}",
                                SRV = log_srv::CONN_ERR
                            );
                        }
                    }
                }
            }

            connections.abort_all();
            while connections.join_next().await.is_some() {}

            // 服务器停止后更新状态
            state.status.write().await.running = false;
            *state.start_time.write().await = None;
        });

        // 保存服务器任务句柄
        *self.server_handle.write().await = Some(handle);

        Ok(ProxyServerInfo {
            address: self.config.listen_address.clone(),
            port: actual_port,
            started_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    pub async fn stop(&self) -> Result<(), ProxyError> {
        // Close upgraded WebSocket connections before the listener is marked stopped.
        self.state.websocket_shutdown_tx.send_replace(true);

        // 1. 发送关闭信号
        if let Some(tx) = self.shutdown_tx.write().await.take() {
            let _ = tx.send(());
        } else {
            return Err(ProxyError::NotRunning);
        }

        // 2. 等待服务器任务结束（带 5 秒超时保护）
        if let Some(handle) = self.server_handle.write().await.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    log::warn!("[{}] 代理服务器任务异常终止: {e}", log_srv::TASK_ERROR);
                    return Err(ProxyError::StopFailed(e.to_string()));
                }
                Err(_) => {
                    log::warn!(
                        "[{}] 代理服务器停止超时（5秒），强制继续",
                        log_srv::STOP_TIMEOUT
                    );
                    return Err(ProxyError::StopTimeout);
                }
            }
        }

        // 3. Upgraded connections are detached from hyper's listener task. Wait
        // until their shutdown branch has sent close frames and dropped guards.
        let active = self.state.websocket_active.clone();
        let wait_for_websockets = async move {
            while active.load(Ordering::Acquire) > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        if tokio::time::timeout(std::time::Duration::from_secs(5), wait_for_websockets)
            .await
            .is_err()
        {
            log::warn!(
                "[{}] WebSocket connections did not drain before stop timeout",
                log_srv::STOP_TIMEOUT
            );
            return Err(ProxyError::StopTimeout);
        }

        log::info!("[{}] 代理服务器已完全停止", log_srv::STOPPED);
        Ok(())
    }

    pub async fn get_status(&self) -> ProxyStatus {
        let mut status = self.state.status.read().await.clone();

        // 计算运行时间
        if let Some(start) = *self.state.start_time.read().await {
            status.uptime_seconds = start.elapsed().as_secs();
        }

        // 从 current_providers HashMap 获取每个应用类型当前正在使用的 provider
        let current_providers = self.state.current_providers.read().await;
        status.active_targets = current_providers
            .iter()
            .map(|(app_type, (provider_id, provider_name))| ActiveTarget {
                app_type: app_type.clone(),
                provider_id: provider_id.clone(),
                provider_name: provider_name.clone(),
            })
            .collect();

        status
    }

    #[cfg(test)]
    pub(crate) fn state_for_tests(&self) -> ProxyState {
        self.state.clone()
    }
    #[cfg(test)]
    pub(crate) fn provider_router_for_tests(&self) -> Arc<ProviderRouter> {
        self.state.provider_router.clone()
    }

    /// 更新某个应用类型当前“目标供应商”（用于 UI 展示 active_targets）
    ///
    /// 注意：这不代表该供应商一定已经处理过请求，而是用于“热切换/启用故障转移立即切 P1”
    /// 等场景下，让 UI 能立刻反映最新目标。
    pub async fn set_active_target(&self, app_type: &str, provider_id: &str, provider_name: &str) {
        let mut current_providers = self.state.current_providers.write().await;
        current_providers.insert(
            app_type.to_string(),
            (provider_id.to_string(), provider_name.to_string()),
        );
    }

    fn build_router(&self) -> Router {
        Router::new()
            // 健康检查
            .route("/health", get(handlers::health_check))
            .route("/status", get(handlers::get_status))
            // Claude API (支持带前缀和不带前缀两种格式)
            .route("/v1/messages", post(handlers::handle_messages))
            .route("/claude/v1/messages", post(handlers::handle_messages))
            // Claude Desktop 3P 本地 gateway（独立 provider namespace）
            .route(
                "/claude-desktop/v1/models",
                get(handlers::handle_claude_desktop_models),
            )
            .route(
                "/claude-desktop/v1/messages",
                post(handlers::handle_claude_desktop_messages),
            )
            // OpenAI Chat Completions API (Codex CLI，支持带前缀和不带前缀)
            .route("/chat/completions", post(handlers::handle_chat_completions))
            .route(
                "/v1/chat/completions",
                post(handlers::handle_chat_completions),
            )
            .route(
                "/v1/v1/chat/completions",
                post(handlers::handle_chat_completions),
            )
            .route(
                "/codex/v1/chat/completions",
                post(handlers::handle_chat_completions),
            )
            // OpenAI Models API (Codex CLI reachability check)
            .route("/models", get(handlers::handle_models))
            .route("/v1/models", get(handlers::handle_models))
            // OpenAI Responses API (Codex CLI，支持带前缀和不带前缀)
            .route(
                "/responses",
                get(super::responses_websocket::handle_responses_websocket)
                    .post(handlers::handle_responses),
            )
            .route(
                "/v1/responses",
                get(super::responses_websocket::handle_responses_websocket)
                    .post(handlers::handle_responses),
            )
            .route(
                "/v1/v1/responses",
                get(super::responses_websocket::handle_responses_websocket)
                    .post(handlers::handle_responses),
            )
            .route(
                "/codex/v1/responses",
                get(super::responses_websocket::handle_responses_websocket)
                    .post(handlers::handle_responses),
            )
            // Grok Build uses the Responses protocol but has an independent
            // provider namespace and failover queue.
            .route(
                "/grokbuild/v1/responses",
                post(handlers::handle_grokbuild_responses),
            )
            // OpenAI Responses Compact API (Codex CLI 远程压缩，透传)
            .route(
                "/responses/compact",
                post(handlers::handle_responses_compact),
            )
            .route(
                "/v1/responses/compact",
                post(handlers::handle_responses_compact),
            )
            .route(
                "/v1/v1/responses/compact",
                post(handlers::handle_responses_compact),
            )
            .route(
                "/codex/v1/responses/compact",
                post(handlers::handle_responses_compact),
            )
            .route(
                "/grokbuild/v1/responses/compact",
                post(handlers::handle_grokbuild_responses_compact),
            )
            // Gemini API (支持带前缀和不带前缀)
            //
            // 用 `any(..)` 覆盖所有 HTTP 方法：除了 POST `:generateContent` /
            // `:streamGenerateContent` / `:countTokens` 之外，Gemini SDK / CLI 还会发
            // GET `/models`、GET `/models/<id>` 等只读端点。如果只挂 POST，这些 GET
            // 请求会在路由层 404，绕过本地代理的统计、整流和故障转移。
            .route("/v1beta/*path", any(handlers::handle_gemini))
            .route("/gemini/v1beta/*path", any(handlers::handle_gemini))
            // Gemini 的 GA 版本也叫 /v1，给原 SDK 留一条出口
            .route("/gemini/v1/*path", any(handlers::handle_gemini))
            // 提高默认请求体大小限制（避免 413 Payload Too Large）
            .layer(DefaultBodyLimit::max(200 * 1024 * 1024))
            .with_state(self.state.clone())
    }

    /// 在不重启服务的情况下更新运行时配置
    pub async fn apply_runtime_config(&self, config: &ProxyConfig) {
        *self.state.config.write().await = config.clone();
    }

    /// 热更新熔断器配置
    ///
    /// 将新配置应用到所有已创建的熔断器实例
    pub async fn update_circuit_breaker_configs(
        &self,
        config: super::circuit_breaker::CircuitBreakerConfig,
    ) {
        self.state.provider_router.update_all_configs(config).await;
    }

    pub async fn update_circuit_breaker_config_for_app(
        &self,
        app_type: &str,
        config: super::circuit_breaker::CircuitBreakerConfig,
    ) {
        self.state
            .provider_router
            .update_app_configs(app_type, config)
            .await;
    }

    /// 重置指定 Provider 的熔断器
    pub async fn reset_provider_circuit_breaker(&self, provider_id: &str, app_type: &str) {
        self.state
            .provider_router
            .reset_provider_breaker(provider_id, app_type)
            .await;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn websocket_upgrade_status(port: u16, path: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect test proxy");
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write websocket handshake");

        let mut response = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let read =
                tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut buffer))
                    .await
                    .expect("timed out reading websocket handshake")
                    .expect("read websocket handshake");
            if read == 0 {
                break;
            }
            response.extend_from_slice(&buffer[..read]);
            if response.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        String::from_utf8_lossy(&response)
            .lines()
            .next()
            .unwrap_or_default()
            .to_string()
    }

    #[tokio::test]
    #[serial]
    async fn responses_routes_accept_websocket_upgrade() {
        let config = ProxyConfig {
            listen_address: "127.0.0.1".to_string(),
            listen_port: 0,
            ..Default::default()
        };
        let server = ProxyServer::new(
            config,
            Arc::new(Database::memory().expect("create in-memory database")),
            None,
        );
        let info = server.start().await.expect("start test proxy");

        let mut statuses = Vec::new();
        for path in ["/responses", "/v1/responses", "/codex/v1/responses"] {
            statuses.push((path, websocket_upgrade_status(info.port, path).await));
        }

        server.stop().await.expect("stop test proxy");

        for (path, status) in statuses {
            assert_eq!(
                status, "HTTP/1.1 101 Switching Protocols",
                "{path} must accept a valid WebSocket Upgrade request"
            );
        }
    }

    #[tokio::test]
    #[serial]
    async fn stop_retains_websocket_shutdown_state_without_subscribers() {
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            Arc::new(Database::memory().expect("create in-memory database")),
            None,
        );
        server.start().await.expect("start test proxy");

        assert_eq!(server.state.websocket_shutdown_tx.receiver_count(), 0);
        server.stop().await.expect("stop test proxy");
        assert!(
            *server.state.websocket_shutdown_tx.borrow(),
            "shutdown state was lost when no upgraded socket had subscribed"
        );
    }

    #[tokio::test]
    #[serial]
    async fn stop_cancels_connections_waiting_for_upgrade_bytes() {
        let server = ProxyServer::new(
            ProxyConfig {
                listen_address: "127.0.0.1".to_string(),
                listen_port: 0,
                ..Default::default()
            },
            Arc::new(Database::memory().expect("create in-memory database")),
            None,
        );
        let info = server.start().await.expect("start test proxy");
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", info.port))
            .await
            .expect("connect test proxy");

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while PENDING_CONNECTION_PEEKS.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted connection did not reach header peek");

        server.stop().await.expect("stop test proxy");

        let request = format!(
            "GET /v1/responses HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
            info.port
        );
        let write_result = stream.write_all(request.as_bytes()).await;
        let mut response = [0u8; 1024];
        let status = if write_result.is_ok() {
            match tokio::time::timeout(
                std::time::Duration::from_secs(1),
                stream.read(&mut response),
            )
            .await
            {
                Ok(Ok(read)) => String::from_utf8_lossy(&response[..read])
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_string(),
                _ => String::new(),
            }
        } else {
            String::new()
        };

        assert_ne!(
            status, "HTTP/1.1 101 Switching Protocols",
            "an accepted connection completed its WebSocket upgrade after stop returned"
        );
    }
}
