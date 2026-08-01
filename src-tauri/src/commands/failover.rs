//! 故障转移队列命令
//!
//! 管理代理模式下的故障转移队列（基于 providers 表的 in_failover_queue 字段）

use crate::database::FailoverQueueItem;
use crate::provider::Provider;
use crate::store::AppState;
use std::str::FromStr;
use tauri::Emitter;

#[cfg(test)]
pub(crate) static FAIL_NEXT_AUTO_FAILOVER_CONFIG_WRITE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// 获取故障转移队列
#[tauri::command]
pub async fn get_failover_queue(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<Vec<FailoverQueueItem>, String> {
    state
        .db
        .get_failover_queue(&app_type)
        .map_err(|e| e.to_string())
}

/// 获取可添加到故障转移队列的供应商（不在队列中的）
#[tauri::command]
pub async fn get_available_providers_for_failover(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<Vec<Provider>, String> {
    state
        .db
        .get_available_providers_for_failover(&app_type)
        .map_err(|e| e.to_string())
}

/// 添加供应商到故障转移队列
#[tauri::command]
pub async fn add_to_failover_queue(
    state: tauri::State<'_, AppState>,
    app_type: String,
    provider_id: String,
) -> Result<(), String> {
    add_to_failover_queue_inner(&state, &app_type, &provider_id).await
}

async fn add_to_failover_queue_inner(
    state: &AppState,
    app_type: &str,
    provider_id: &str,
) -> Result<(), String> {
    let previous_health = state
        .db
        .get_provider_health_record(provider_id, app_type)
        .await
        .map_err(|e| e.to_string())?;
    state
        .db
        .add_to_failover_queue(app_type, provider_id)
        .map_err(|e| e.to_string())?;
    if let Err(error) = state
        .proxy_service
        .refresh_failover_projection_if_active(app_type)
        .await
    {
        let rollback = async {
            state
                .db
                .remove_from_failover_queue(app_type, provider_id)
                .map_err(|e| e.to_string())?;
            if let Some(health) = previous_health.as_ref() {
                state
                    .db
                    .restore_provider_health(health)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            state
                .proxy_service
                .refresh_failover_projection_if_active(app_type)
                .await
        }
        .await;
        return match rollback {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(format!(
                "{error}; failed to restore failover queue state: {rollback_error}"
            )),
        };
    }
    Ok(())
}

/// 从故障转移队列移除供应商
#[tauri::command]
pub async fn remove_from_failover_queue(
    state: tauri::State<'_, AppState>,
    app_type: String,
    provider_id: String,
) -> Result<(), String> {
    remove_from_failover_queue_inner(&state, &app_type, &provider_id).await
}

async fn remove_from_failover_queue_inner(
    state: &AppState,
    app_type: &str,
    provider_id: &str,
) -> Result<(), String> {
    let previous_health = state
        .db
        .get_provider_health_record(provider_id, app_type)
        .await
        .map_err(|e| e.to_string())?;
    state
        .db
        .remove_from_failover_queue(app_type, provider_id)
        .map_err(|e| e.to_string())?;
    if let Err(error) = state
        .proxy_service
        .refresh_failover_projection_if_active(app_type)
        .await
    {
        let rollback = async {
            state
                .db
                .add_to_failover_queue(app_type, provider_id)
                .map_err(|e| e.to_string())?;
            if let Some(health) = previous_health.as_ref() {
                state
                    .db
                    .restore_provider_health(health)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            state
                .proxy_service
                .refresh_failover_projection_if_active(app_type)
                .await
        }
        .await;
        return match rollback {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(format!(
                "{error}; failed to restore failover queue state: {rollback_error}"
            )),
        };
    }
    Ok(())
}

/// 获取指定应用的自动故障转移开关状态（从 proxy_config 表读取）
#[tauri::command]
pub async fn get_auto_failover_enabled(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<bool, String> {
    state
        .db
        .get_proxy_config_for_app(&app_type)
        .await
        .map(|config| config.auto_failover_enabled)
        .map_err(|e| e.to_string())
}

/// 设置指定应用的自动故障转移开关状态（写入 proxy_config 表）
///
/// 注意：关闭故障转移时不会清除队列，队列内容会保留供下次开启时使用
#[tauri::command]
pub async fn set_auto_failover_enabled(
    app: tauri::AppHandle,
    state: tauri::State<'_, AppState>,
    app_type: String,
    enabled: bool,
) -> Result<(), String> {
    let switched_provider =
        set_auto_failover_enabled_inner(&state, app_type.clone(), enabled).await?;
    if let Some(provider_id) = switched_provider {
        let event_data = serde_json::json!({
            "appType": app_type,
            "providerId": provider_id,
            "source": "failoverEnabled"
        });
        let _ = app.emit("provider-switched", event_data);
    }
    if let Ok(new_menu) = crate::tray::create_tray_menu(&app, &state) {
        if let Some(tray) = app.tray_by_id(crate::tray::TRAY_ID) {
            let _ = tray.set_menu(Some(new_menu));
        }
    }
    Ok(())
}

async fn set_auto_failover_enabled_inner(
    state: &AppState,
    app_type: String,
    enabled: bool,
) -> Result<Option<String>, String> {
    log::info!(
        "[Failover] Setting auto_failover_enabled: app_type='{app_type}', enabled={enabled}"
    );

    // 读取当前配置
    let mut config = state
        .db
        .get_proxy_config_for_app(&app_type)
        .await
        .map_err(|e| e.to_string())?;

    if enabled && !config.enabled {
        return Err("需要先启用该应用的代理接管，再开启故障转移".to_string());
    }
    let previous_provider_id = if enabled {
        let app_enum = crate::app_config::AppType::from_str(&app_type)
            .map_err(|_| format!("无效的应用类型: {app_type}"))?;
        crate::settings::get_effective_current_provider(&state.db, &app_enum)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "未设置当前供应商，无法安全开启故障转移".to_string())?
    } else {
        String::new()
    };

    // 队列为空时把当前供应商自动加入作为 P1，避免用户陷入"必须先加队列才能开启"的死锁
    let mut auto_added_provider_id: Option<String> = None;
    let mut auto_added_previous_health = None;
    let p1_provider_id = if enabled {
        let mut queue = state
            .db
            .get_failover_queue(&app_type)
            .map_err(|e| e.to_string())?;

        if queue.is_empty() {
            let app_enum = crate::app_config::AppType::from_str(&app_type)
                .map_err(|_| format!("无效的应用类型: {app_type}"))?;

            let current_id = crate::settings::get_effective_current_provider(&state.db, &app_enum)
                .map_err(|e| e.to_string())?;

            let Some(current_id) = current_id else {
                return Err("故障转移队列为空，且未设置当前供应商，无法开启故障转移".to_string());
            };

            auto_added_previous_health = state
                .db
                .get_provider_health_record(&current_id, &app_type)
                .await
                .map_err(|e| e.to_string())?;
            state
                .db
                .add_to_failover_queue(&app_type, &current_id)
                .map_err(|e| e.to_string())?;
            auto_added_provider_id = Some(current_id);

            queue = state
                .db
                .get_failover_queue(&app_type)
                .map_err(|e| e.to_string())?;
        }

        queue
            .first()
            .map(|item| item.provider_id.clone())
            .ok_or_else(|| "故障转移队列为空，无法开启故障转移".to_string())?
    } else {
        String::new()
    };

    // 开启前先切到 P1。只有切换成功后才写入 auto_failover_enabled=true，
    // 避免 P1 不可切换（例如 official provider）时留下“开关已开但目标未切”的脏状态。
    if enabled {
        if let Err(e) = state
            .proxy_service
            .switch_proxy_target(&app_type, &p1_provider_id)
            .await
        {
            if let Some(provider_id) = auto_added_provider_id.as_ref() {
                let _ = state.db.remove_from_failover_queue(&app_type, provider_id);
                if let Some(health) = auto_added_previous_health.as_ref() {
                    let _ = state.db.restore_provider_health(health).await;
                }
            }
            return Err(e);
        }
    }

    // 更新 auto_failover_enabled 字段
    let previous_config = config.clone();
    config.auto_failover_enabled = enabled;

    // 写回数据库。失败时也必须撤销已经完成的 P1 热切换和自动入队。
    #[cfg(test)]
    let config_write_result =
        if FAIL_NEXT_AUTO_FAILOVER_CONFIG_WRITE.swap(false, std::sync::atomic::Ordering::SeqCst) {
            Err("injected auto-failover config write failure".to_string())
        } else {
            state
                .db
                .update_proxy_config_for_app(config)
                .await
                .map_err(|e| e.to_string())
        };
    #[cfg(not(test))]
    let config_write_result = state
        .db
        .update_proxy_config_for_app(config)
        .await
        .map_err(|e| e.to_string());
    if let Err(error) = config_write_result {
        let mut rollback_errors = Vec::new();
        if enabled && previous_provider_id != p1_provider_id {
            if let Err(rollback_error) = state
                .proxy_service
                .switch_proxy_target(&app_type, &previous_provider_id)
                .await
            {
                rollback_errors.push(format!("restore provider target: {rollback_error}"));
            }
        }
        if let Some(provider_id) = auto_added_provider_id.as_ref() {
            if let Err(rollback_error) = state.db.remove_from_failover_queue(&app_type, provider_id)
            {
                rollback_errors.push(format!("restore auto-populated queue: {rollback_error}"));
            }
            if let Some(health) = auto_added_previous_health.as_ref() {
                if let Err(rollback_error) = state.db.restore_provider_health(health).await {
                    rollback_errors.push(format!(
                        "restore auto-populated provider health: {rollback_error}"
                    ));
                }
            }
        }
        if let Err(rollback_error) = state
            .proxy_service
            .refresh_failover_projection_if_active(&app_type)
            .await
        {
            rollback_errors.push(format!("restore live projection: {rollback_error}"));
        }
        if rollback_errors.is_empty() {
            return Err(error);
        }
        return Err(format!(
            "{error}; failover enable rollback failed: {}",
            rollback_errors.join("; ")
        ));
    }
    if let Err(error) = state
        .proxy_service
        .refresh_failover_projection_if_active(&app_type)
        .await
    {
        let mut rollback_errors = Vec::new();
        if let Err(rollback_error) = state.db.update_proxy_config_for_app(previous_config).await {
            rollback_errors.push(format!("restore proxy config: {rollback_error}"));
        }
        if enabled && previous_provider_id != p1_provider_id {
            if let Err(rollback_error) = state
                .proxy_service
                .switch_proxy_target(&app_type, &previous_provider_id)
                .await
            {
                rollback_errors.push(format!("restore provider target: {rollback_error}"));
            }
        }
        if let Some(provider_id) = auto_added_provider_id.as_ref() {
            if let Err(rollback_error) = state.db.remove_from_failover_queue(&app_type, provider_id)
            {
                rollback_errors.push(format!("restore auto-populated queue: {rollback_error}"));
            }
            if let Some(health) = auto_added_previous_health.as_ref() {
                if let Err(rollback_error) = state.db.restore_provider_health(health).await {
                    rollback_errors.push(format!(
                        "restore auto-populated provider health: {rollback_error}"
                    ));
                }
            }
        }
        if let Err(rollback_error) = state
            .proxy_service
            .refresh_failover_projection_if_active(&app_type)
            .await
        {
            rollback_errors.push(format!("restore live projection: {rollback_error}"));
        }
        if rollback_errors.is_empty() {
            return Err(error);
        }
        return Err(format!(
            "{error}; failover enable rollback failed: {}",
            rollback_errors.join("; ")
        ));
    }

    Ok(enabled.then_some(p1_provider_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use serial_test::serial;
    use std::{env, sync::Arc};
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
            let dir = TempDir::new().expect("create temp home");
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
            crate::settings::reload_settings().expect("restore settings");
        }
    }

    fn codex_provider(id: &str, supports_websockets: bool) -> Provider {
        let config = format!(
            r#"model_provider = "{id}"

[model_providers.{id}]
name = "{id}"
base_url = "https://{id}.example/v1"
wire_api = "responses"
supports_websockets = {supports_websockets}
"#
        );
        Provider::with_id(
            id.to_string(),
            id.to_string(),
            serde_json::json!({
                "auth": {},
                "config": config,
                "base_url": format!("https://{id}.example/v1"),
                "supports_websockets": supports_websockets,
            }),
            None,
        )
    }

    fn live_supports_websockets(provider_id: &str) -> bool {
        let live = std::fs::read_to_string(crate::codex_config::get_codex_config_path())
            .expect("read Codex live config");
        let parsed: toml::Value = toml::from_str(&live).expect("parse Codex live config");
        parsed["model_providers"][provider_id]["supports_websockets"]
            .as_bool()
            .expect("projected supports_websockets")
    }

    #[tokio::test]
    #[serial]
    async fn queue_mutations_refresh_active_codex_websocket_projection() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().expect("create database"));
        let current = codex_provider("queue-current", false);
        let fallback = codex_provider("queue-fallback", true);
        db.save_provider("codex", &current).unwrap();
        db.save_provider("codex", &fallback).unwrap();
        db.set_current_provider("codex", &current.id).unwrap();
        db.add_to_failover_queue("codex", &current.id).unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.enabled = true;
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let state = AppState::new(db.clone());
        state
            .proxy_service
            .sync_codex_live_from_provider_while_proxy_active(&current)
            .await
            .expect("seed active Codex projection");
        assert!(!live_supports_websockets(&current.id));

        add_to_failover_queue_inner(&state, "codex", &fallback.id)
            .await
            .expect("add fallback");
        assert!(
            live_supports_websockets(&current.id),
            "adding a WebSocket-capable fallback must refresh the active takeover projection"
        );

        remove_from_failover_queue_inner(&state, "codex", &fallback.id)
            .await
            .expect("remove fallback");
        assert!(
            !live_supports_websockets(&current.id),
            "removing the final WebSocket-capable fallback must refresh the active takeover projection"
        );
    }

    #[tokio::test]
    #[serial]
    async fn remove_queue_refresh_failure_restores_provider_health() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().expect("create database"));
        let current = codex_provider("rollback-current", false);
        let fallback = codex_provider("rollback-fallback", true);
        db.save_provider("codex", &current).unwrap();
        db.save_provider("codex", &fallback).unwrap();
        db.set_current_provider("codex", &current.id).unwrap();
        db.add_to_failover_queue("codex", &current.id).unwrap();
        db.add_to_failover_queue("codex", &fallback.id).unwrap();
        db.update_provider_health_with_threshold(
            &fallback.id,
            "codex",
            false,
            Some("preserve this failure".to_string()),
            1,
        )
        .await
        .unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.enabled = true;
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let state = AppState::new(db.clone());
        state
            .proxy_service
            .sync_codex_live_from_provider_while_proxy_active(&current)
            .await
            .unwrap();
        crate::services::proxy::FAIL_NEXT_FAILOVER_PROJECTION_REFRESH
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let error = remove_from_failover_queue_inner(&state, "codex", &fallback.id)
            .await
            .expect_err("projection refresh should fail");
        assert!(error.contains("injected"));
        assert!(db.is_in_failover_queue("codex", &fallback.id).unwrap());
        let health = db.get_provider_health(&fallback.id, "codex").await.unwrap();
        assert_eq!(health.consecutive_failures, 1);
        assert_eq!(health.last_error.as_deref(), Some("preserve this failure"));
    }

    #[tokio::test]
    #[serial]
    async fn add_queue_refresh_failure_restores_provider_health() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().expect("create database"));
        let current = codex_provider("add-rollback-current", false);
        let fallback = codex_provider("add-rollback-fallback", true);
        db.save_provider("codex", &current).unwrap();
        db.save_provider("codex", &fallback).unwrap();
        db.set_current_provider("codex", &current.id).unwrap();
        db.add_to_failover_queue("codex", &current.id).unwrap();
        db.update_provider_health_with_threshold(
            &fallback.id,
            "codex",
            false,
            Some("preserve add failure".to_string()),
            1,
        )
        .await
        .unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.enabled = true;
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();
        let state = AppState::new(db.clone());
        state
            .proxy_service
            .sync_codex_live_from_provider_while_proxy_active(&current)
            .await
            .unwrap();
        crate::services::proxy::FAIL_NEXT_FAILOVER_PROJECTION_REFRESH
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let error = add_to_failover_queue_inner(&state, "codex", &fallback.id)
            .await
            .expect_err("projection refresh should fail");
        assert!(error.contains("injected"));
        assert!(!db.is_in_failover_queue("codex", &fallback.id).unwrap());
        let health = db.get_provider_health(&fallback.id, "codex").await.unwrap();
        assert_eq!(health.consecutive_failures, 1);
        assert_eq!(health.last_error.as_deref(), Some("preserve add failure"));
    }

    #[tokio::test]
    #[serial]
    async fn enabling_failover_refresh_failure_restores_previous_target_and_queue() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().expect("create database"));
        let current = codex_provider("z-enable-current", false);
        let mut p1 = codex_provider("a-enable-p1", true);
        p1.sort_index = Some(0);
        db.save_provider("codex", &current).unwrap();
        db.save_provider("codex", &p1).unwrap();
        db.set_current_provider("codex", &current.id).unwrap();
        crate::settings::set_current_provider(
            &crate::app_config::AppType::Codex,
            Some(&current.id),
        )
        .unwrap();
        db.add_to_failover_queue("codex", &p1.id).unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.enabled = true;
        config.auto_failover_enabled = false;
        db.update_proxy_config_for_app(config).await.unwrap();
        let state = AppState::new(db.clone());
        state
            .proxy_service
            .sync_codex_live_from_provider_while_proxy_active(&current)
            .await
            .unwrap();
        crate::services::proxy::FAIL_NEXT_FAILOVER_PROJECTION_REFRESH
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let error = set_auto_failover_enabled_inner(&state, "codex".to_string(), true)
            .await
            .expect_err("projection refresh should fail");
        assert!(error.contains("injected"));
        let config = db.get_proxy_config_for_app("codex").await.unwrap();
        assert!(!config.auto_failover_enabled);
        assert_eq!(
            crate::settings::get_effective_current_provider(
                &db,
                &crate::app_config::AppType::Codex
            )
            .unwrap()
            .as_deref(),
            Some(current.id.as_str()),
            "failed enable must restore the pre-switch logical target"
        );
        assert!(db.is_in_failover_queue("codex", &p1.id).unwrap());
        assert_eq!(db.get_failover_queue("codex").unwrap().len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn enabling_failover_config_write_failure_restores_previous_target() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().expect("create database"));
        let current = codex_provider("config-write-current", false);
        let mut p1 = codex_provider("config-write-p1", true);
        p1.sort_index = Some(0);
        db.save_provider("codex", &current).unwrap();
        db.save_provider("codex", &p1).unwrap();
        db.set_current_provider("codex", &current.id).unwrap();
        crate::settings::set_current_provider(
            &crate::app_config::AppType::Codex,
            Some(&current.id),
        )
        .unwrap();
        db.add_to_failover_queue("codex", &p1.id).unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.enabled = true;
        config.auto_failover_enabled = false;
        db.update_proxy_config_for_app(config).await.unwrap();
        let state = AppState::new(db.clone());
        state
            .proxy_service
            .sync_codex_live_from_provider_while_proxy_active(&current)
            .await
            .unwrap();
        FAIL_NEXT_AUTO_FAILOVER_CONFIG_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);

        let error = set_auto_failover_enabled_inner(&state, "codex".to_string(), true)
            .await
            .expect_err("config write should fail");
        assert!(error.contains("injected"));
        assert_eq!(
            crate::settings::get_effective_current_provider(
                &db,
                &crate::app_config::AppType::Codex,
            )
            .unwrap()
            .as_deref(),
            Some(current.id.as_str())
        );
        assert!(db.is_in_failover_queue("codex", &p1.id).unwrap());
        assert!(
            !db.get_proxy_config_for_app("codex")
                .await
                .unwrap()
                .auto_failover_enabled
        );
    }

    #[tokio::test]
    #[serial]
    async fn auto_populated_enable_rollback_restores_provider_health() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().expect("create database"));
        let current = codex_provider("auto-health-current", true);
        db.save_provider("codex", &current).unwrap();
        db.set_current_provider("codex", &current.id).unwrap();
        crate::settings::set_current_provider(
            &crate::app_config::AppType::Codex,
            Some(&current.id),
        )
        .unwrap();
        db.update_provider_health_with_threshold(
            &current.id,
            "codex",
            false,
            Some("preserve auto-added health".to_string()),
            1,
        )
        .await
        .unwrap();
        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.enabled = true;
        config.auto_failover_enabled = false;
        db.update_proxy_config_for_app(config).await.unwrap();
        let state = AppState::new(db.clone());
        state
            .proxy_service
            .sync_codex_live_from_provider_while_proxy_active(&current)
            .await
            .unwrap();
        crate::services::proxy::FAIL_NEXT_FAILOVER_PROJECTION_REFRESH
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let error = set_auto_failover_enabled_inner(&state, "codex".to_string(), true)
            .await
            .expect_err("projection refresh should fail");
        assert!(error.contains("injected"));
        assert!(db.get_failover_queue("codex").unwrap().is_empty());
        let health = db.get_provider_health(&current.id, "codex").await.unwrap();
        assert_eq!(health.consecutive_failures, 1);
        assert_eq!(
            health.last_error.as_deref(),
            Some("preserve auto-added health")
        );
    }
}
