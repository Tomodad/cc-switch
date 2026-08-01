//! 供应商路由器模块
//!
//! 负责选择和管理代理目标供应商，实现智能故障转移

use crate::app_config::AppType;
use crate::database::Database;
use crate::error::AppError;
use crate::provider::Provider;
use crate::proxy::circuit_breaker::{AllowResult, CircuitBreaker, CircuitBreakerConfig};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
#[cfg(test)]
static PAUSE_RESULT_BEFORE_ENQUEUE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static RESULT_PAUSED_BEFORE_ENQUEUE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static RELEASE_RESULT_ENQUEUE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static RESULTS_ENQUEUED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static PAUSE_RESULT_PERSISTENCE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static RESULT_PERSISTENCE_PAUSED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static RELEASE_RESULT_PERSISTENCE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static PAUSE_PROVIDER_RESET_BEFORE_BARRIER: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static PROVIDER_RESET_PAUSED_BEFORE_BARRIER: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static RELEASE_PROVIDER_RESET_BARRIER: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(not(test))]
const PROVIDER_RESULT_PERSISTENCE_CAPACITY: usize = 256;
#[cfg(test)]
const PROVIDER_RESULT_PERSISTENCE_CAPACITY: usize = 8;

/// 供应商路由器
#[derive(Clone)]
pub struct ProviderRouter {
    /// 数据库连接
    db: Arc<Database>,
    /// 熔断器管理器 - key 格式: "app_type:provider_id"
    circuit_breakers: Arc<RwLock<HashMap<String, Arc<CircuitBreaker>>>>,
    /// Per-provider lock spanning breaker mutation and persistence enqueue.
    result_ordering_locks: Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>,
    /// Gate that orders app-wide clears against in-flight persistence enqueues.
    result_enqueue_gate: Arc<RwLock<()>>,
    /// Ordered persistence queue for synchronous result/control jobs.
    result_persistence_tx: mpsc::Sender<ProviderPersistenceJob>,
    /// Bounded-by-provider coalescing store for detached lifecycle results.
    detached_result_pending: Arc<StdMutex<HashMap<String, CoalescedProviderResult>>>,
    /// Serializes pending-map flushes against synchronous result/reset/clear barriers.
    detached_result_flush_lock: Arc<Mutex<()>>,
    /// Capacity-one wakeup channel; duplicate wakeups coalesce with pending results.
    detached_result_notify_tx: mpsc::Sender<()>,
    /// Reset/clear operations that must win over racing detached lifecycle results.
    result_controls_in_progress: Arc<StdMutex<HashMap<String, usize>>>,
}

struct ResultControlMarker {
    controls: Arc<StdMutex<HashMap<String, usize>>>,
    key: String,
}

impl ResultControlMarker {
    fn new(controls: Arc<StdMutex<HashMap<String, usize>>>, key: String) -> Result<Self, AppError> {
        let mut guard = controls.lock().map_err(|_| {
            AppError::Message("provider result control marker lock poisoned".to_string())
        })?;
        *guard.entry(key.clone()).or_default() += 1;
        drop(guard);
        Ok(Self { controls, key })
    }
}

impl Drop for ResultControlMarker {
    fn drop(&mut self) {
        if let Ok(mut controls) = self.controls.lock() {
            if let Some(count) = controls.get_mut(&self.key) {
                *count -= 1;
                if *count == 0 {
                    controls.remove(&self.key);
                }
            }
        }
    }
}

#[derive(Debug)]
struct CoalescedProviderResult {
    provider_id: String,
    app_type: String,
    saw_success: bool,
    trailing_failures: u32,
    last_error: Option<String>,
    failure_threshold: u32,
}

impl CoalescedProviderResult {
    fn new(
        provider_id: &str,
        app_type: &str,
        success: bool,
        error_msg: Option<String>,
        failure_threshold: u32,
    ) -> Self {
        Self {
            provider_id: provider_id.to_string(),
            app_type: app_type.to_string(),
            saw_success: success,
            trailing_failures: u32::from(!success),
            last_error: if success { None } else { error_msg },
            failure_threshold,
        }
    }

    fn merge(&mut self, success: bool, error_msg: Option<String>, failure_threshold: u32) {
        self.failure_threshold = failure_threshold;
        if success {
            self.saw_success = true;
            self.trailing_failures = 0;
            self.last_error = None;
        } else {
            self.trailing_failures = self.trailing_failures.saturating_add(1);
            self.last_error = error_msg;
        }
    }

    fn into_job(self) -> ProviderPersistenceJob {
        ProviderPersistenceJob::CoalescedResult {
            provider_id: self.provider_id,
            app_type: self.app_type,
            saw_success: self.saw_success,
            trailing_failures: self.trailing_failures,
            last_error: self.last_error,
            failure_threshold: self.failure_threshold,
        }
    }
}

enum ProviderPersistenceJob {
    Result {
        provider_id: String,
        app_type: String,
        success: bool,
        error_msg: Option<String>,
        failure_threshold: u32,
        completion: Option<oneshot::Sender<Result<(), AppError>>>,
    },
    CoalescedResult {
        provider_id: String,
        app_type: String,
        saw_success: bool,
        trailing_failures: u32,
        last_error: Option<String>,
        failure_threshold: u32,
    },
    Reset {
        provider_id: String,
        app_type: String,
        completion: oneshot::Sender<Result<(), AppError>>,
    },
    ClearApp {
        app_type: String,
        completion: oneshot::Sender<Result<(), AppError>>,
    },
    ClearAll {
        completion: oneshot::Sender<Result<(), AppError>>,
    },
}

impl ProviderRouter {
    /// 创建新的供应商路由器
    pub fn new(db: Arc<Database>) -> Self {
        let (result_persistence_tx, mut result_persistence_rx) =
            mpsc::channel::<ProviderPersistenceJob>(PROVIDER_RESULT_PERSISTENCE_CAPACITY);
        let persistence_db = db.clone();
        let persistence_worker = async move {
            while let Some(result) = result_persistence_rx.recv().await {
                #[cfg(test)]
                {
                    if PAUSE_RESULT_PERSISTENCE.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        RESULT_PERSISTENCE_PAUSED.store(true, std::sync::atomic::Ordering::SeqCst);
                        while !RELEASE_RESULT_PERSISTENCE.load(std::sync::atomic::Ordering::SeqCst)
                        {
                            tokio::task::yield_now().await;
                        }
                    }
                }
                match result {
                    ProviderPersistenceJob::Result {
                        provider_id,
                        app_type,
                        success,
                        error_msg,
                        failure_threshold,
                        completion,
                    } => {
                        let job_db = persistence_db.clone();
                        let job_provider_id = provider_id.clone();
                        let job_app_type = app_type.clone();
                        let persisted = tokio::task::spawn_blocking(move || {
                            futures::executor::block_on(async move {
                                job_db
                                    .update_provider_health_with_threshold(
                                        &job_provider_id,
                                        &job_app_type,
                                        success,
                                        error_msg,
                                        failure_threshold,
                                    )
                                    .await
                            })
                        })
                        .await
                        .map_err(|error| {
                            AppError::Message(format!("provider result worker failed: {error}"))
                        })
                        .and_then(|result| result);
                        if let Some(completion) = completion {
                            let _ = completion.send(persisted);
                        } else if let Err(error) = persisted {
                            log::warn!(
                                "[{}] Failed to persist detached provider result (provider={}): {}",
                                app_type,
                                provider_id,
                                error
                            );
                        }
                    }
                    ProviderPersistenceJob::CoalescedResult {
                        provider_id,
                        app_type,
                        saw_success,
                        trailing_failures,
                        last_error,
                        failure_threshold,
                    } => {
                        let job_db = persistence_db.clone();
                        let job_provider_id = provider_id.clone();
                        let job_app_type = app_type.clone();
                        let persisted = tokio::task::spawn_blocking(move || {
                            futures::executor::block_on(async move {
                                job_db
                                    .update_provider_health_coalesced_with_threshold(
                                        &job_provider_id,
                                        &job_app_type,
                                        saw_success,
                                        trailing_failures,
                                        last_error,
                                        failure_threshold,
                                    )
                                    .await
                            })
                        })
                        .await
                        .map_err(|error| {
                            AppError::Message(format!("provider result worker failed: {error}"))
                        })
                        .and_then(|result| result);
                        if let Err(error) = persisted {
                            log::warn!(
                                "[{}] Failed to persist coalesced provider result (provider={}): {}",
                                app_type,
                                provider_id,
                                error
                            );
                        }
                    }
                    ProviderPersistenceJob::Reset {
                        provider_id,
                        app_type,
                        completion,
                    } => {
                        let job_db = persistence_db.clone();
                        let persisted = tokio::task::spawn_blocking(move || {
                            futures::executor::block_on(async move {
                                job_db.reset_provider_health(&provider_id, &app_type).await
                            })
                        })
                        .await
                        .map_err(|error| {
                            AppError::Message(format!("provider reset worker failed: {error}"))
                        })
                        .and_then(|result| result);
                        let _ = completion.send(persisted);
                    }
                    ProviderPersistenceJob::ClearApp {
                        app_type,
                        completion,
                    } => {
                        let job_db = persistence_db.clone();
                        let persisted = tokio::task::spawn_blocking(move || {
                            futures::executor::block_on(async move {
                                job_db.clear_provider_health_for_app(&app_type).await
                            })
                        })
                        .await
                        .map_err(|error| {
                            AppError::Message(format!("provider app clear worker failed: {error}"))
                        })
                        .and_then(|result| result);
                        let _ = completion.send(persisted);
                    }
                    ProviderPersistenceJob::ClearAll { completion } => {
                        let job_db = persistence_db.clone();
                        let persisted = tokio::task::spawn_blocking(move || {
                            futures::executor::block_on(async move {
                                job_db.clear_all_provider_health().await
                            })
                        })
                        .await
                        .map_err(|error| {
                            AppError::Message(format!("provider clear worker failed: {error}"))
                        })
                        .and_then(|result| result);
                        let _ = completion.send(persisted);
                    }
                }
            }
        };
        let detached_result_pending = Arc::new(StdMutex::new(HashMap::<
            String,
            CoalescedProviderResult,
        >::new()));
        let detached_result_flush_lock = Arc::new(Mutex::new(()));
        let (detached_result_notify_tx, mut detached_result_notify_rx) = mpsc::channel::<()>(1);
        let flusher_pending = detached_result_pending.clone();
        let flusher_lock = detached_result_flush_lock.clone();
        let flusher_tx = result_persistence_tx.downgrade();
        let detached_flusher = async move {
            while detached_result_notify_rx.recv().await.is_some() {
                loop {
                    let _flush_guard = flusher_lock.lock().await;
                    let key = flusher_pending
                        .lock()
                        .ok()
                        .and_then(|pending| pending.keys().next().cloned());
                    let Some(key) = key else {
                        break;
                    };
                    let Some(tx) = flusher_tx.upgrade() else {
                        return;
                    };
                    let permit = match tx.reserve().await {
                        Ok(permit) => permit,
                        Err(_) => return,
                    };
                    let result = flusher_pending
                        .lock()
                        .ok()
                        .and_then(|mut pending| pending.remove(&key));
                    if let Some(result) = result {
                        permit.send(result.into_job());
                    }
                }
            }
        };
        let persistence_tasks = async move {
            let _ = tokio::join!(persistence_worker, detached_flusher);
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(persistence_tasks);
        } else {
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build provider result persistence runtime")
                    .block_on(persistence_tasks);
            });
        }

        Self {
            db,
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            result_ordering_locks: Arc::new(RwLock::new(HashMap::new())),
            result_enqueue_gate: Arc::new(RwLock::new(())),
            result_persistence_tx,
            detached_result_pending,
            detached_result_flush_lock,
            detached_result_notify_tx,
            result_controls_in_progress: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// 选择可用的供应商（支持故障转移）
    ///
    /// 返回按优先级排序的可用供应商列表：
    /// - 故障转移关闭时：仅返回当前供应商
    /// - 故障转移开启时：仅使用故障转移队列，按队列顺序依次尝试（P1 → P2 → ...）
    pub async fn select_providers(&self, app_type: &str) -> Result<Vec<Provider>, AppError> {
        let mut result = Vec::new();
        let mut total_providers = 0usize;
        let mut circuit_open_count = 0usize;

        // 检查该应用的自动故障转移开关是否开启（从 proxy_config 表读取）
        let auto_failover_enabled = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(config) => config.auto_failover_enabled,
            Err(e) => {
                log::error!("[{app_type}] 读取 proxy_config 失败: {e}，默认禁用故障转移");
                false
            }
        };

        if auto_failover_enabled {
            // 故障转移开启：仅按队列顺序依次尝试（P1 → P2 → ...）
            let all_providers = self.db.get_all_providers(app_type)?;

            // 使用 DAO 返回的排序结果，确保和前端展示一致
            let ordered_ids: Vec<String> = self
                .db
                .get_failover_queue(app_type)?
                .into_iter()
                .map(|item| item.provider_id)
                .collect();

            total_providers = ordered_ids.len();

            for provider_id in ordered_ids {
                let Some(provider) = all_providers.get(&provider_id).cloned() else {
                    continue;
                };

                let circuit_key = format!("{app_type}:{}", provider.id);
                let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;

                if breaker.is_available().await {
                    result.push(provider);
                } else {
                    circuit_open_count += 1;
                }
            }
        } else {
            // 故障转移关闭：仅使用当前供应商，跳过熔断器检查
            let current_id = AppType::from_str(app_type)
                .ok()
                .and_then(|app_enum| {
                    crate::settings::get_effective_current_provider(&self.db, &app_enum)
                        .ok()
                        .flatten()
                })
                .or_else(|| self.db.get_current_provider(app_type).ok().flatten());

            if let Some(current_id) = current_id {
                if let Some(current) = self.db.get_provider_by_id(&current_id, app_type)? {
                    total_providers = 1;
                    result.push(current);
                }
            }
        }

        if result.is_empty() {
            if total_providers > 0 && circuit_open_count == total_providers {
                log::warn!("[{app_type}] [FO-004] 所有供应商均已熔断");
                return Err(AppError::AllProvidersCircuitOpen);
            } else {
                log::warn!("[{app_type}] [FO-005] 未配置供应商");
                return Err(AppError::NoProvidersConfigured);
            }
        }

        Ok(result)
    }

    /// Prepare the in-memory breaker before a request so terminal accounting never reads SQLite.
    pub async fn prepare_provider_result(&self, provider_id: &str, app_type: &str) {
        let circuit_key = format!("{app_type}:{provider_id}");
        self.get_or_create_circuit_breaker(&circuit_key).await;
    }

    /// 请求执行前获取熔断器“放行许可”
    ///
    /// - Closed：直接放行
    /// - Open：超时到达后切到 HalfOpen 并放行一次探测
    /// - HalfOpen：按限流规则放行探测
    ///
    /// 注意：调用方必须在请求结束后通过 `record_result()` 释放 HalfOpen 名额，
    /// 否则会导致该 Provider 长时间无法进入探测状态。
    pub async fn allow_provider_request(&self, provider_id: &str, app_type: &str) -> AllowResult {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.allow_request().await
    }
    async fn result_ordering_lock(&self, provider_id: &str, app_type: &str) -> Arc<Mutex<()>> {
        let key = format!("{app_type}:{provider_id}");
        if let Some(lock) = self.result_ordering_locks.read().await.get(&key).cloned() {
            return lock;
        }
        self.result_ordering_locks
            .write()
            .await
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn detached_result_key(provider_id: &str, app_type: &str) -> String {
        format!("{app_type}:{provider_id}")
    }

    fn detached_result_control_in_progress(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Result<bool, AppError> {
        let provider_reset_key = format!("provider:{app_type}:{provider_id}");
        let app_clear_key = format!("app:{app_type}");
        let controls = self.result_controls_in_progress.lock().map_err(|_| {
            AppError::Message("provider result control marker lock poisoned".to_string())
        })?;
        Ok(controls.contains_key("all")
            || controls.contains_key(&app_clear_key)
            || controls.contains_key(&provider_reset_key))
    }

    async fn finish_detached_result_ordered(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
        success: bool,
        error_msg: Option<String>,
        control_started_before_ordering: bool,
    ) -> Result<(), AppError> {
        if control_started_before_ordering
            || self.detached_result_control_in_progress(provider_id, app_type)?
        {
            self.release_permit_neutral(provider_id, app_type, used_half_open_permit)
                .await;
            return Ok(());
        }
        let failure_threshold = self
            .record_circuit_result(provider_id, app_type, used_half_open_permit, success)
            .await;
        self.queue_detached_result(provider_id, app_type, success, error_msg, failure_threshold)
    }

    fn queue_detached_result(
        &self,
        provider_id: &str,
        app_type: &str,
        success: bool,
        error_msg: Option<String>,
        failure_threshold: u32,
    ) -> Result<(), AppError> {
        let key = Self::detached_result_key(provider_id, app_type);
        let mut pending = self.detached_result_pending.lock().map_err(|_| {
            AppError::Message("detached provider result coalescer lock poisoned".to_string())
        })?;
        if let Some(existing) = pending.get_mut(&key) {
            existing.merge(success, error_msg, failure_threshold);
        } else {
            pending.insert(
                key,
                CoalescedProviderResult::new(
                    provider_id,
                    app_type,
                    success,
                    error_msg,
                    failure_threshold,
                ),
            );
        }
        drop(pending);
        match self.detached_result_notify_tx.try_send(()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(AppError::Message(
                "provider result persistence worker stopped".to_string(),
            )),
        }
    }

    async fn flush_detached_results_locked(
        &self,
        provider_id: Option<&str>,
        app_type: Option<&str>,
    ) -> Result<(), AppError> {
        loop {
            let key = {
                let pending = self.detached_result_pending.lock().map_err(|_| {
                    AppError::Message(
                        "detached provider result coalescer lock poisoned".to_string(),
                    )
                })?;
                pending
                    .iter()
                    .find(|(_, result)| {
                        provider_id.is_none_or(|id| result.provider_id == id)
                            && app_type.is_none_or(|app| result.app_type == app)
                    })
                    .map(|(key, _)| key.clone())
            };
            let Some(key) = key else {
                return Ok(());
            };
            let permit = self.result_persistence_tx.reserve().await.map_err(|_| {
                AppError::Message("provider result persistence worker stopped".to_string())
            })?;
            let result = self
                .detached_result_pending
                .lock()
                .map_err(|_| {
                    AppError::Message(
                        "detached provider result coalescer lock poisoned".to_string(),
                    )
                })?
                .remove(&key);
            if let Some(result) = result {
                permit.send(result.into_job());
            }
        }
    }

    /// 记录供应商请求结果
    pub async fn record_result(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
        success: bool,
        error_msg: Option<String>,
    ) -> Result<(), AppError> {
        let enqueue_guard = self.result_enqueue_gate.read().await;
        let ordering_lock = self.result_ordering_lock(provider_id, app_type).await;
        let ordering_guard = ordering_lock.lock().await;
        let flush_guard = self.detached_result_flush_lock.lock().await;
        self.flush_detached_results_locked(Some(provider_id), Some(app_type))
            .await?;
        let failure_threshold = self
            .record_circuit_result(provider_id, app_type, used_half_open_permit, success)
            .await;
        #[cfg(test)]
        {
            if PAUSE_RESULT_BEFORE_ENQUEUE.swap(false, std::sync::atomic::Ordering::SeqCst) {
                RESULT_PAUSED_BEFORE_ENQUEUE.store(true, std::sync::atomic::Ordering::SeqCst);
                while !RELEASE_RESULT_ENQUEUE.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            }
        }
        let (completion_tx, completion_rx) = oneshot::channel();
        self.enqueue_result_persistence(
            provider_id,
            app_type,
            success,
            error_msg,
            failure_threshold,
            Some(completion_tx),
        )
        .await?;
        #[cfg(test)]
        RESULTS_ENQUEUED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        drop(flush_guard);
        drop(ordering_guard);
        drop(enqueue_guard);
        completion_rx.await.map_err(|_| {
            AppError::Message("provider result persistence worker stopped".to_string())
        })?
    }

    /// Record the in-memory breaker result and queue ordered persistence without awaiting SQLite.
    pub async fn record_result_detached(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
        success: bool,
        error_msg: Option<String>,
    ) -> Result<(), AppError> {
        let Some(_enqueue_guard) = self.result_enqueue_gate.try_read().ok() else {
            self.release_permit_neutral(provider_id, app_type, used_half_open_permit)
                .await;
            return Ok(());
        };
        let control_started_before_ordering =
            self.detached_result_control_in_progress(provider_id, app_type)?;
        let ordering_lock = self.result_ordering_lock(provider_id, app_type).await;
        match ordering_lock.clone().try_lock_owned() {
            Ok(_ordering_guard) => {
                self.finish_detached_result_ordered(
                    provider_id,
                    app_type,
                    used_half_open_permit,
                    success,
                    error_msg,
                    control_started_before_ordering,
                )
                .await
            }
            Err(_) => {
                let router = self.clone();
                let provider_id = provider_id.to_string();
                let app_type = app_type.to_string();
                tokio::spawn(async move {
                    let _ordering_guard = ordering_lock.lock_owned().await;
                    if let Err(error) = router
                        .finish_detached_result_ordered(
                            &provider_id,
                            &app_type,
                            used_half_open_permit,
                            success,
                            error_msg,
                            control_started_before_ordering,
                        )
                        .await
                    {
                        log::warn!(
                            "Failed to record deferred provider result (provider={provider_id}, app={app_type}): {error}"
                        );
                    }
                });
                Ok(())
            }
        }
    }

    async fn record_circuit_result(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
        success: bool,
    ) -> u32 {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        if success {
            breaker.record_success(used_half_open_permit).await
        } else {
            breaker.record_failure(used_half_open_permit).await
        }
    }

    async fn enqueue_result_persistence(
        &self,
        provider_id: &str,
        app_type: &str,
        success: bool,
        error_msg: Option<String>,
        failure_threshold: u32,
        completion: Option<oneshot::Sender<Result<(), AppError>>>,
    ) -> Result<(), AppError> {
        self.result_persistence_tx
            .send(ProviderPersistenceJob::Result {
                provider_id: provider_id.to_string(),
                app_type: app_type.to_string(),
                success,
                error_msg,
                failure_threshold,
                completion,
            })
            .await
            .map_err(|_| {
                AppError::Message("provider result persistence worker stopped".to_string())
            })
    }

    /// 重置熔断器（手动恢复）
    pub async fn reset_circuit_breaker(&self, circuit_key: &str) {
        let breakers = self.circuit_breakers.read().await;
        if let Some(breaker) = breakers.get(circuit_key) {
            breaker.reset().await;
        }
    }

    /// 重置指定供应商的熔断器和持久化健康状态。
    pub async fn reset_provider_breaker(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Result<(), AppError> {
        let reset_key = format!("provider:{app_type}:{provider_id}");
        let _reset_marker =
            ResultControlMarker::new(self.result_controls_in_progress.clone(), reset_key)?;
        let enqueue_guard = self.result_enqueue_gate.read().await;
        let ordering_lock = self.result_ordering_lock(provider_id, app_type).await;
        let ordering_guard = ordering_lock.lock().await;
        let flush_guard = self.detached_result_flush_lock.lock().await;
        self.flush_detached_results_locked(Some(provider_id), Some(app_type))
            .await?;
        let circuit_key = format!("{app_type}:{provider_id}");
        self.reset_circuit_breaker(&circuit_key).await;
        #[cfg(test)]
        {
            if PAUSE_PROVIDER_RESET_BEFORE_BARRIER.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                PROVIDER_RESET_PAUSED_BEFORE_BARRIER
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                while !RELEASE_PROVIDER_RESET_BARRIER.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            }
        }
        let (completion_tx, completion_rx) = oneshot::channel();
        self.result_persistence_tx
            .send(ProviderPersistenceJob::Reset {
                provider_id: provider_id.to_string(),
                app_type: app_type.to_string(),
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                AppError::Message("provider result persistence worker stopped".to_string())
            })?;
        drop(flush_guard);
        drop(ordering_guard);
        drop(enqueue_guard);
        completion_rx.await.map_err(|_| {
            AppError::Message("provider result persistence worker stopped".to_string())
        })?
    }

    /// Clear one app's persisted provider health after all older queued results.
    pub async fn clear_provider_health_for_app(&self, app_type: &str) -> Result<(), AppError> {
        let _clear_marker = ResultControlMarker::new(
            self.result_controls_in_progress.clone(),
            format!("app:{app_type}"),
        )?;
        let enqueue_guard = self.result_enqueue_gate.write().await;
        let flush_guard = self.detached_result_flush_lock.lock().await;
        self.flush_detached_results_locked(None, Some(app_type))
            .await?;
        let circuit_prefix = format!("{app_type}:");
        self.circuit_breakers
            .write()
            .await
            .retain(|key, _| !key.starts_with(&circuit_prefix));
        let (completion_tx, completion_rx) = oneshot::channel();
        self.result_persistence_tx
            .send(ProviderPersistenceJob::ClearApp {
                app_type: app_type.to_string(),
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                AppError::Message("provider result persistence worker stopped".to_string())
            })?;
        drop(flush_guard);
        drop(enqueue_guard);
        completion_rx.await.map_err(|_| {
            AppError::Message("provider result persistence worker stopped".to_string())
        })?
    }

    /// Clear all persisted provider health after all older queued results.
    pub async fn clear_all_provider_health(&self) -> Result<(), AppError> {
        let _clear_marker =
            ResultControlMarker::new(self.result_controls_in_progress.clone(), "all".to_string())?;
        let enqueue_guard = self.result_enqueue_gate.write().await;
        let flush_guard = self.detached_result_flush_lock.lock().await;
        self.flush_detached_results_locked(None, None).await?;
        self.circuit_breakers.write().await.clear();
        let (completion_tx, completion_rx) = oneshot::channel();
        self.result_persistence_tx
            .send(ProviderPersistenceJob::ClearAll {
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                AppError::Message("provider result persistence worker stopped".to_string())
            })?;
        drop(flush_guard);
        drop(enqueue_guard);
        completion_rx.await.map_err(|_| {
            AppError::Message("provider result persistence worker stopped".to_string())
        })?
    }
    /// 仅释放 HalfOpen permit，不影响健康统计（neutral 接口）
    ///
    /// 用于整流器等场景：请求结果不应计入 Provider 健康度，
    /// 但仍需释放占用的探测名额，避免 HalfOpen 状态卡死
    pub async fn release_permit_neutral(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
    ) {
        if !used_half_open_permit {
            return;
        }
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.release_half_open_permit();
    }

    /// 更新所有熔断器的配置（热更新）
    pub async fn update_all_configs(&self, config: CircuitBreakerConfig) {
        let breakers = self.circuit_breakers.read().await;
        for breaker in breakers.values() {
            breaker.update_config(config.clone()).await;
        }
    }

    /// 更新指定应用已创建熔断器的配置（热更新）
    pub async fn update_app_configs(&self, app_type: &str, config: CircuitBreakerConfig) {
        let prefix = format!("{app_type}:");
        let breakers = self.circuit_breakers.read().await;
        for (key, breaker) in breakers.iter() {
            if key.starts_with(&prefix) {
                breaker.update_config(config.clone()).await;
            }
        }
    }

    /// 获取熔断器状态
    #[allow(dead_code)]
    pub async fn get_circuit_breaker_stats(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Option<crate::proxy::circuit_breaker::CircuitBreakerStats> {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breakers = self.circuit_breakers.read().await;

        if let Some(breaker) = breakers.get(&circuit_key) {
            Some(breaker.get_stats().await)
        } else {
            None
        }
    }

    /// 获取或创建熔断器
    async fn get_or_create_circuit_breaker(&self, key: &str) -> Arc<CircuitBreaker> {
        // 先尝试读锁获取
        {
            let breakers = self.circuit_breakers.read().await;
            if let Some(breaker) = breakers.get(key) {
                return breaker.clone();
            }
        }

        // 如果不存在，获取写锁创建
        let mut breakers = self.circuit_breakers.write().await;

        // 双重检查，防止竞争条件
        if let Some(breaker) = breakers.get(key) {
            return breaker.clone();
        }

        // 从 key 中提取 app_type (格式: "app_type:provider_id")
        let app_type = key.split(':').next().unwrap_or("claude");

        // 按应用独立读取熔断器配置
        let config = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(app_config) => crate::proxy::circuit_breaker::CircuitBreakerConfig {
                failure_threshold: app_config.circuit_failure_threshold,
                success_threshold: app_config.circuit_success_threshold,
                timeout_seconds: app_config.circuit_timeout_seconds as u64,
                error_rate_threshold: app_config.circuit_error_rate_threshold,
                min_requests: app_config.circuit_min_requests,
            },
            Err(_) => crate::proxy::circuit_breaker::CircuitBreakerConfig::default(),
        };

        let breaker = Arc::new(CircuitBreaker::new(config));
        breakers.insert(key.to_string(), breaker.clone());

        breaker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use serde_json::json;
    use serial_test::serial;
    use std::env;
    use std::time::Duration;
    use tempfile::TempDir;

    struct TempHome {
        #[allow(dead_code)]
        dir: TempDir,
        original_home: Option<String>,
        original_userprofile: Option<String>,
        original_test_home: Option<String>,
    }

    impl TempHome {
        fn new() -> Self {
            let dir = TempDir::new().expect("failed to create temp home");
            let original_home = env::var("HOME").ok();
            let original_userprofile = env::var("USERPROFILE").ok();
            let original_test_home = env::var("CC_SWITCH_TEST_HOME").ok();

            env::set_var("HOME", dir.path());
            env::set_var("USERPROFILE", dir.path());
            env::set_var("CC_SWITCH_TEST_HOME", dir.path());
            crate::settings::reload_settings().expect("reload settings");

            Self {
                dir,
                original_home,
                original_userprofile,
                original_test_home,
            }
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            match &self.original_home {
                Some(value) => env::set_var("HOME", value),
                None => env::remove_var("HOME"),
            }

            match &self.original_userprofile {
                Some(value) => env::set_var("USERPROFILE", value),
                None => env::remove_var("USERPROFILE"),
            }

            match &self.original_test_home {
                Some(value) => env::set_var("CC_SWITCH_TEST_HOME", value),
                None => env::remove_var("CC_SWITCH_TEST_HOME"),
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_provider_router_creation() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let router = ProviderRouter::new(db);

        let breaker = router.get_or_create_circuit_breaker("claude:test").await;
        assert!(breaker.allow_request().await.allowed);
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_disabled_uses_current_provider() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "a");
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_enabled_uses_queue_order_ignoring_current() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 设置 sort_index 来控制顺序：b=1, a=2
        let mut provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        provider_a.sort_index = Some(2);
        let mut provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        provider_b.sort_index = Some(1);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        db.add_to_failover_queue("claude", "b").unwrap();
        db.add_to_failover_queue("claude", "a").unwrap();

        // 启用自动故障转移（使用新的 proxy_config API）
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 2);
        // 故障转移开启时：仅按队列顺序选择（忽略当前供应商）
        assert_eq!(providers[0].id, "b");
        assert_eq!(providers[1].id, "a");
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_enabled_uses_queue_only_even_if_current_not_in_queue() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let mut provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        provider_b.sort_index = Some(1);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        // 只把 b 加入故障转移队列（模拟“当前供应商不在队列里”的常见配置）
        db.add_to_failover_queue("claude", "b").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "b");
    }

    #[tokio::test]
    #[serial]
    async fn test_select_providers_does_not_consume_half_open_permit() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();

        db.add_to_failover_queue("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        // 启用自动故障转移（使用新的 proxy_config API）
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        router
            .record_result("b", "claude", false, false, Some("fail".to_string()))
            .await
            .unwrap();

        let providers = router.select_providers("claude").await.unwrap();
        assert_eq!(providers.len(), 2);

        assert!(router.allow_provider_request("b", "claude").await.allowed);
    }

    #[tokio::test]
    #[serial]
    async fn test_release_permit_neutral_frees_half_open_slot() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 配置熔断器：1 次失败即熔断，0 秒超时立即进入 HalfOpen
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        db.save_provider("claude", &provider_a).unwrap();
        db.add_to_failover_queue("claude", "a").unwrap();

        // 启用自动故障转移
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        // 触发熔断：1 次失败
        router
            .record_result("a", "claude", false, false, Some("fail".to_string()))
            .await
            .unwrap();

        // 第一次请求：获取 HalfOpen 探测名额
        let first = router.allow_provider_request("a", "claude").await;
        assert!(first.allowed);
        assert!(first.used_half_open_permit);

        // 第二次请求应被拒绝（名额已被占用）
        let second = router.allow_provider_request("a", "claude").await;
        assert!(!second.allowed);

        // 使用 release_permit_neutral 释放名额（不影响健康统计）
        router
            .release_permit_neutral("a", "claude", first.used_half_open_permit)
            .await;

        // 第三次请求应被允许（名额已释放）
        let third = router.allow_provider_request("a", "claude").await;
        assert!(third.allowed);
        assert!(third.used_half_open_permit);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn concurrent_result_updates_preserve_breaker_and_persistence_order() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let provider = Provider::with_id(
            "ordered-provider".to_string(),
            "Ordered Provider".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &provider).unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(config).await.unwrap();
        let router = Arc::new(ProviderRouter::new(db.clone()));

        RESULTS_ENQUEUED.store(0, std::sync::atomic::Ordering::SeqCst);
        RESULT_PAUSED_BEFORE_ENQUEUE.store(false, std::sync::atomic::Ordering::SeqCst);
        RELEASE_RESULT_ENQUEUE.store(false, std::sync::atomic::Ordering::SeqCst);
        PAUSE_RESULT_BEFORE_ENQUEUE.store(true, std::sync::atomic::Ordering::SeqCst);

        let success_router = router.clone();
        let success = tokio::spawn(async move {
            success_router
                .record_result("ordered-provider", "codex", false, true, None)
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !RESULT_PAUSED_BEFORE_ENQUEUE.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("success result did not pause before enqueue");

        let failure_router = router.clone();
        let failure = tokio::spawn(async move {
            failure_router
                .record_result(
                    "ordered-provider",
                    "codex",
                    false,
                    false,
                    Some("later failure".to_string()),
                )
                .await
                .unwrap();
        });
        let _ = tokio::time::timeout(Duration::from_millis(200), async {
            while RESULTS_ENQUEUED.load(std::sync::atomic::Ordering::SeqCst) < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        RELEASE_RESULT_ENQUEUE.store(true, std::sync::atomic::Ordering::SeqCst);
        success.await.unwrap();
        failure.await.unwrap();

        let stats = router
            .get_circuit_breaker_stats("ordered-provider", "codex")
            .await
            .expect("breaker stats");
        assert_eq!(
            stats.state,
            crate::proxy::circuit_breaker::CircuitState::Open
        );
        assert_eq!(stats.failed_requests, 1);
        let health = db
            .get_provider_health("ordered-provider", "codex")
            .await
            .unwrap();
        assert!(
            !health.is_healthy,
            "SQLite must match the live failed breaker"
        );
        assert_eq!(health.consecutive_failures, 1);
        assert_eq!(health.last_error.as_deref(), Some("later failure"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn queued_result_uses_the_breaker_threshold_from_result_time() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let provider = Provider::with_id(
            "threshold-provider".to_string(),
            "Threshold Provider".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &provider).unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(config.clone())
            .await
            .unwrap();
        let router = Arc::new(ProviderRouter::new(db.clone()));

        RESULT_PAUSED_BEFORE_ENQUEUE.store(false, std::sync::atomic::Ordering::SeqCst);
        RELEASE_RESULT_ENQUEUE.store(false, std::sync::atomic::Ordering::SeqCst);
        PAUSE_RESULT_BEFORE_ENQUEUE.store(true, std::sync::atomic::Ordering::SeqCst);

        let result_router = router.clone();
        let result = tokio::spawn(async move {
            result_router
                .record_result(
                    "threshold-provider",
                    "codex",
                    false,
                    false,
                    Some("threshold-one failure".to_string()),
                )
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !RESULT_PAUSED_BEFORE_ENQUEUE.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("result did not pause before persistence enqueue");

        let stats = router
            .get_circuit_breaker_stats("threshold-provider", "codex")
            .await
            .expect("breaker stats");
        assert_eq!(
            stats.state,
            crate::proxy::circuit_breaker::CircuitState::Open
        );

        config.circuit_failure_threshold = 5;
        db.update_proxy_config_for_app(config).await.unwrap();
        RELEASE_RESULT_ENQUEUE.store(true, std::sync::atomic::Ordering::SeqCst);
        result.await.unwrap();

        let health = db
            .get_provider_health("threshold-provider", "codex")
            .await
            .unwrap();
        assert!(
            !health.is_healthy,
            "persistence re-evaluated the result with the later threshold"
        );
        assert_eq!(health.consecutive_failures, 1);
        assert_eq!(health.last_error.as_deref(), Some("threshold-one failure"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn manual_reset_is_ordered_after_detached_result_persistence() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let provider = Provider::with_id(
            "reset-provider".to_string(),
            "Reset Provider".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &provider).unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(config).await.unwrap();
        let router = Arc::new(ProviderRouter::new(db.clone()));

        RESULT_PERSISTENCE_PAUSED.store(false, std::sync::atomic::Ordering::SeqCst);
        RELEASE_RESULT_PERSISTENCE.store(false, std::sync::atomic::Ordering::SeqCst);
        PAUSE_RESULT_PERSISTENCE.store(true, std::sync::atomic::Ordering::SeqCst);

        router
            .record_result_detached(
                &provider.id,
                "codex",
                false,
                false,
                Some("stale failure".to_string()),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !RESULT_PERSISTENCE_PAUSED.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached result persistence did not pause");

        let reset_router = router.clone();
        let reset_provider_id = provider.id.clone();
        let reset = tokio::spawn(async move {
            reset_router
                .reset_provider_breaker(&reset_provider_id, "codex")
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        assert!(
            !reset.is_finished(),
            "reset must wait behind older persistence"
        );
        RELEASE_RESULT_PERSISTENCE.store(true, std::sync::atomic::Ordering::SeqCst);
        reset.await.unwrap();
        let health = db.get_provider_health(&provider.id, "codex").await.unwrap();
        assert!(
            health.is_healthy,
            "manual reset must win over older results"
        );
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.last_error, None);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn detached_result_persistence_does_not_block_when_queue_is_full() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let id = "overflow-provider";
        db.save_provider(
            "codex",
            &Provider::with_id(id.to_string(), id.to_string(), json!({}), None),
        )
        .unwrap();
        let router = Arc::new(ProviderRouter::new(db.clone()));

        RESULT_PERSISTENCE_PAUSED.store(false, std::sync::atomic::Ordering::SeqCst);
        RELEASE_RESULT_PERSISTENCE.store(false, std::sync::atomic::Ordering::SeqCst);
        PAUSE_RESULT_PERSISTENCE.store(true, std::sync::atomic::Ordering::SeqCst);

        router
            .record_result_detached("seed-provider", "codex", false, true, None)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !RESULT_PERSISTENCE_PAUSED.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("persistence worker did not pause");

        for index in 0..PROVIDER_RESULT_PERSISTENCE_CAPACITY {
            tokio::time::timeout(
                Duration::from_secs(1),
                router.record_result_detached(
                    &format!("queued-provider-{index}"),
                    "codex",
                    false,
                    true,
                    None,
                ),
            )
            .await
            .expect("queue did not accept its bounded capacity")
            .unwrap();
        }

        tokio::time::timeout(
            Duration::from_millis(250),
            router.record_result_detached(
                "overflow-provider",
                "codex",
                false,
                false,
                Some("queue saturated".to_string()),
            ),
        )
        .await
        .expect("detached accounting blocked on a saturated persistence queue")
        .unwrap();

        RELEASE_RESULT_PERSISTENCE.store(true, std::sync::atomic::Ordering::SeqCst);
        let overflow_health = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let health = db
                    .get_provider_health("overflow-provider", "codex")
                    .await
                    .unwrap();
                if health.consecutive_failures == 1 {
                    break health;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("coalesced detached result was not eventually persisted");
        assert_eq!(overflow_health.consecutive_failures, 1);
        assert_eq!(
            overflow_health.last_error.as_deref(),
            Some("queue saturated")
        );
        router
            .clear_all_provider_health()
            .await
            .expect("drain queued persistence before test exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn detached_result_returns_while_same_provider_ordering_lock_is_busy() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        db.save_provider(
            "codex",
            &Provider::with_id(
                "shared-provider".to_string(),
                "Shared Provider".to_string(),
                json!({}),
                None,
            ),
        )
        .unwrap();
        let router = Arc::new(ProviderRouter::new(db));

        RESULT_PAUSED_BEFORE_ENQUEUE.store(false, std::sync::atomic::Ordering::SeqCst);
        RELEASE_RESULT_ENQUEUE.store(false, std::sync::atomic::Ordering::SeqCst);
        PAUSE_RESULT_BEFORE_ENQUEUE.store(true, std::sync::atomic::Ordering::SeqCst);

        let blocking_router = router.clone();
        let blocking = tokio::spawn(async move {
            blocking_router
                .record_result("shared-provider", "codex", false, true, None)
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !RESULT_PAUSED_BEFORE_ENQUEUE.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordered result did not pause while holding the provider lock");

        tokio::time::timeout(
            Duration::from_millis(250),
            router.record_result_detached(
                "shared-provider",
                "codex",
                false,
                false,
                Some("concurrent failure".to_string()),
            ),
        )
        .await
        .expect("detached accounting blocked on the per-provider ordering lock")
        .unwrap();

        RELEASE_RESULT_ENQUEUE.store(true, std::sync::atomic::Ordering::SeqCst);
        blocking.await.unwrap();
        let health = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let health = router
                    .db
                    .get_provider_health("shared-provider", "codex")
                    .await
                    .unwrap();
                if health.consecutive_failures == 1 {
                    break health;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached result was not persisted after the ordering lock released");
        assert_eq!(health.last_error.as_deref(), Some("concurrent failure"));
        router.clear_all_provider_health().await.unwrap();
    }

    #[test]
    fn overlapping_result_control_markers_remain_active_until_last_drop() {
        let controls = Arc::new(StdMutex::new(HashMap::new()));
        let key = "provider:codex:shared-provider".to_string();
        let first = ResultControlMarker::new(controls.clone(), key.clone()).unwrap();
        let second = ResultControlMarker::new(controls.clone(), key.clone()).unwrap();

        drop(first);
        assert!(
            controls.lock().unwrap().contains_key(&key),
            "dropping one overlapping control operation must not clear the shared marker"
        );

        drop(second);
        assert!(
            !controls.lock().unwrap().contains_key(&key),
            "the marker should be removed after the final overlapping operation completes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn detached_result_waits_for_provider_reset_and_reset_wins() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let provider = Provider::with_id(
            "reset-race-provider".to_string(),
            "Reset Race Provider".to_string(),
            json!({}),
            None,
        );
        let barrier_provider = Provider::with_id(
            "reset-race-barrier".to_string(),
            "Reset Race Barrier".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &provider).unwrap();
        db.save_provider("codex", &barrier_provider).unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(config).await.unwrap();
        let router = Arc::new(ProviderRouter::new(db.clone()));

        PROVIDER_RESET_PAUSED_BEFORE_BARRIER.store(false, std::sync::atomic::Ordering::SeqCst);
        RELEASE_PROVIDER_RESET_BARRIER.store(false, std::sync::atomic::Ordering::SeqCst);
        PAUSE_PROVIDER_RESET_BEFORE_BARRIER.store(true, std::sync::atomic::Ordering::SeqCst);

        let reset_router = router.clone();
        let reset_provider_id = provider.id.clone();
        let reset = tokio::spawn(async move {
            reset_router
                .reset_provider_breaker(&reset_provider_id, "codex")
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !PROVIDER_RESET_PAUSED_BEFORE_BARRIER.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("provider reset did not pause before its persistence barrier");

        let detached_router = router.clone();
        let detached_provider_id = provider.id.clone();
        let detached = tokio::spawn(async move {
            detached_router
                .record_result_detached(
                    &detached_provider_id,
                    "codex",
                    false,
                    false,
                    Some("failure racing with reset".to_string()),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !detached.is_finished(),
            "detached result bypassed the provider ordering lock"
        );

        RELEASE_PROVIDER_RESET_BARRIER.store(true, std::sync::atomic::Ordering::SeqCst);
        reset.await.unwrap();
        detached.await.unwrap().unwrap();
        let flush_guard = router.detached_result_flush_lock.lock().await;
        router
            .flush_detached_results_locked(Some(&provider.id), Some("codex"))
            .await
            .unwrap();
        drop(flush_guard);
        router
            .record_result(&barrier_provider.id, "codex", false, true, None)
            .await
            .unwrap();

        let health = db.get_provider_health(&provider.id, "codex").await.unwrap();
        assert!(health.is_healthy, "manual reset must win in SQLite");
        assert_eq!(health.consecutive_failures, 0);
        if let Some(stats) = router
            .get_circuit_breaker_stats(&provider.id, "codex")
            .await
        {
            assert_eq!(
                stats.consecutive_failures, 0,
                "manual reset must win in memory"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn app_health_clear_waits_behind_detached_result_persistence() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let provider = Provider::with_id(
            "clear-provider".to_string(),
            "Clear Provider".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &provider).unwrap();
        let barrier_provider = Provider::with_id(
            "clear-barrier-provider".to_string(),
            "Clear Barrier Provider".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &barrier_provider).unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(config).await.unwrap();
        let router = Arc::new(ProviderRouter::new(db.clone()));

        RESULT_PERSISTENCE_PAUSED.store(false, std::sync::atomic::Ordering::SeqCst);
        RELEASE_RESULT_PERSISTENCE.store(false, std::sync::atomic::Ordering::SeqCst);
        PAUSE_RESULT_PERSISTENCE.store(true, std::sync::atomic::Ordering::SeqCst);

        router
            .record_result_detached(
                &provider.id,
                "codex",
                false,
                false,
                Some("stale failure".to_string()),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !RESULT_PERSISTENCE_PAUSED.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached result persistence did not pause");

        let clear_router = router.clone();
        let clear = tokio::spawn(async move {
            clear_router
                .clear_provider_health_for_app("codex")
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let clear_finished_before_older_result = clear.is_finished();
        tokio::time::timeout(
            Duration::from_millis(250),
            router.record_result_detached(
                &provider.id,
                "codex",
                false,
                false,
                Some("failure racing with app clear".to_string()),
            ),
        )
        .await
        .expect("detached result blocked behind app-wide clear")
        .unwrap();

        RELEASE_RESULT_PERSISTENCE.store(true, std::sync::atomic::Ordering::SeqCst);
        clear.await.unwrap();
        router
            .record_result(&barrier_provider.id, "codex", false, true, None)
            .await
            .unwrap();

        assert!(
            !clear_finished_before_older_result,
            "app-wide health clear bypassed older queued persistence"
        );
        let health = db.get_provider_health(&provider.id, "codex").await.unwrap();
        assert!(
            health.is_healthy,
            "older queued result recreated health after app-wide clear"
        );
        let permit = router.allow_provider_request(&provider.id, "codex").await;
        assert!(
            permit.allowed,
            "app-wide health clear left the in-memory circuit breaker open"
        );
    }
}
