// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    if std::env::args().any(|arg| arg == "--verify-embedded-ui") {
        std::process::exit(if verify_embedded_ui() { 0 } else { 1 });
    }
    // 在 Linux 上设置 WebKit 环境变量以解决 DMA-BUF 渲染问题
    // 某些 Linux 系统（如 Debian 13.2、Nvidia GPU）上 WebKitGTK 的 DMA-BUF 渲染器可能导致白屏/黑屏
    // 参考: https://github.com/tauri-apps/tauri/issues/9394
    #[cfg(target_os = "linux")]
    {
        if std::env::var("WEBKIT_DISABLE_DMABUF_RENDERER").is_err() {
            std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");
        }
        // 禁用 WebKitGTK 合成模式，规避 resize 时 webview 崩溃以及部分 Wayland
        // 合成器下的 surface 协商问题（整窗 UI 点击无响应、必须最大化-还原才能恢复）。
        // 参考: https://github.com/tauri-apps/tauri/issues/9394
        if std::env::var("WEBKIT_DISABLE_COMPOSITING_MODE").is_err() {
            std::env::set_var("WEBKIT_DISABLE_COMPOSITING_MODE", "1");
        }

        // AppImage 的 GTK 启动钩子 (linuxdeploy-plugin-gtk.sh) 会无条件
        // `export GDK_BACKEND=x11` 强制走 XWayland，以规避历史上的 Wayland 崩溃
        // (tauri-apps/tauri#8541)。但在较新的 Wayland + NVIDIA 环境下，强制 XWayland
        // 反而使 WebKitGTK 的 webview 收不到指针事件（标题栏可点、网页内容点不动），
        // resize 后黑屏；改回原生 Wayland 即可解决，且该崩溃在 WebKitGTK 2.52 上已不复现。
        // 由于该钩子会覆盖用户预设的 GDK_BACKEND，这里提供一个钩子不会触碰的逃生开关：
        // 设置 CC_SWITCH_GDK_BACKEND=wayland 即可强制覆盖，默认行为保持不变（零回归）。
        if let Ok(backend) = std::env::var("CC_SWITCH_GDK_BACKEND") {
            if !backend.is_empty() {
                std::env::set_var("GDK_BACKEND", backend);
            }
        }
    }

    cc_switch_lib::run();
}

fn verify_embedded_ui() -> bool {
    use tauri::utils::assets::AssetKey;
    let context = cc_switch_lib::application_context();
    let index = context.assets().get(&AssetKey::from("index.html"));
    let html = index
        .as_deref()
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .unwrap_or("");
    let mut references = Vec::new();
    for tag in html.split('<') {
        let attribute = if tag.starts_with("script ") {
            "src=\""
        } else if tag.starts_with("link ") && tag.contains("rel=\"stylesheet\"") {
            "href=\""
        } else {
            continue;
        };
        let Some((_, value)) = tag.split_once(attribute) else {
            continue;
        };
        let Some((path, _)) = value.split_once('"') else {
            continue;
        };
        let path = path.trim_start_matches("./");
        let bytes = context
            .assets()
            .get(&AssetKey::from(path))
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        references.push(serde_json::json!({"path":path,"bytes":bytes}));
    }
    let valid = !tauri::is_dev()
        && !html.is_empty()
        && !html.contains("localhost:3000")
        && references
            .iter()
            .any(|asset| asset["path"].as_str().is_some_and(|p| p.ends_with(".js")))
        && references
            .iter()
            .any(|asset| asset["path"].as_str().is_some_and(|p| p.ends_with(".css")))
        && references
            .iter()
            .all(|asset| asset["bytes"].as_u64().unwrap_or(0) > 0);
    println!(
        "{}",
        serde_json::json!({"production_protocol":!tauri::is_dev(),
        "identifier":context.config().identifier,"index_bytes":html.len(),"assets":references,"passed":valid})
    );
    valid
}
