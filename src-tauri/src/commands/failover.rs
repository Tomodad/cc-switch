//! 故障转移队列命令
//!
//! 管理代理模式下的故障转移队列（基于 providers 表的 in_failover_queue 字段）

use crate::database::FailoverQueueItem;
use crate::provider::Provider;
use crate::store::AppState;
use std::str::FromStr;
use tauri::Emitter;

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
    state
        .db
        .add_to_failover_queue(app_type, provider_id)
        .map_err(|e| e.to_string())?;
    if let Err(error) = state
        .proxy_service
        .refresh_failover_projection_if_active(app_type)
        .await
    {
        let _ = state.db.remove_from_failover_queue(app_type, provider_id);
        let _ = state
            .proxy_service
            .refresh_failover_projection_if_active(app_type)
            .await;
        return Err(error);
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
    state
        .db
        .remove_from_failover_queue(app_type, provider_id)
        .map_err(|e| e.to_string())?;
    if let Err(error) = state
        .proxy_service
        .refresh_failover_projection_if_active(app_type)
        .await
    {
        let _ = state.db.add_to_failover_queue(app_type, provider_id);
        let _ = state
            .proxy_service
            .refresh_failover_projection_if_active(app_type)
            .await;
        return Err(error);
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

    // 队列为空时把当前供应商自动加入作为 P1，避免用户陷入"必须先加队列才能开启"的死锁
    let mut auto_added_provider_id: Option<String> = None;
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
            if let Some(provider_id) = auto_added_provider_id {
                let _ = state.db.remove_from_failover_queue(&app_type, &provider_id);
            }
            return Err(e);
        }
    }

    // 更新 auto_failover_enabled 字段
    let previous_config = config.clone();
    config.auto_failover_enabled = enabled;

    // 写回数据库
    state
        .db
        .update_proxy_config_for_app(config)
        .await
        .map_err(|e| e.to_string())?;
    if let Err(error) = state
        .proxy_service
        .refresh_failover_projection_if_active(&app_type)
        .await
    {
        let _ = state.db.update_proxy_config_for_app(previous_config).await;
        let _ = state
            .proxy_service
            .refresh_failover_projection_if_active(&app_type)
            .await;
        return Err(error);
    }

    if enabled {
        // 发射 provider-switched 事件（让前端刷新当前供应商）
        let event_data = serde_json::json!({
            "appType": app_type,
            "providerId": p1_provider_id,
            "source": "failoverEnabled"
        });
        let _ = app.emit("provider-switched", event_data);
    }

    // 刷新托盘菜单，确保状态同步
    if let Ok(new_menu) = crate::tray::create_tray_menu(&app, &state) {
        if let Some(tray) = app.tray_by_id(crate::tray::TRAY_ID) {
            let _ = tray.set_menu(Some(new_menu));
        }
    }

    Ok(())
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
}
