//! Pulex Desktop Tauri v2 Core Engine
//!
//! Features:
//! - Portable directory resolution (`pulexAppData_[5chars]`) & `config.ini` management
//! - AppState concurrency, Derived View registry, & panic guard
//! - Native OS plugins (single instance, window state, dialog, opener, clipboard, fs)
//! - Native Desktop Menu Bar (`File`, `Edit`, `View`, `Help`) with keyboard accelerators
//! - Command dispatcher & aliases router

pub mod commands;
pub mod engine;
pub mod ingest;
pub mod ledger;
pub mod logger;
pub mod portable;
pub mod state;

#[cfg_attr(test, allow(unused_imports))]
use tauri::{Emitter, Manager};

#[cfg(not(test))]
fn setup_native_menu(app: &tauri::App) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::{Arc, Mutex};
    use tauri::menu::{MenuBuilder, MenuItemBuilder, PredefinedMenuItem, SubmenuBuilder};
    use tauri_plugin_dialog::{DialogExt, MessageDialogKind};
    use tauri_plugin_opener::OpenerExt;

    // File menu
    let new_window = MenuItemBuilder::with_id("menu_new_window", "New Pulex Window")
        .accelerator("CmdOrCtrl+N")
        .build(app)?;
    let open_file = MenuItemBuilder::with_id("menu_open_file", "Add Files to Active Project...")
        .accelerator("CmdOrCtrl+O")
        .build(app)?;
    let open_folder =
        MenuItemBuilder::with_id("menu_open_folder", "Add Folder to Active Project...")
            .accelerator("CmdOrCtrl+Shift+O")
            .build(app)?;
    let reopen_view =
        MenuItemBuilder::with_id("menu_reopen_view", "Reopen / Reload Current File")
            .accelerator("CmdOrCtrl+R")
            .build(app)?;
    let reopen_view_f5 = MenuItemBuilder::with_id("menu_reopen_view_f5", "Reload (F5)")
        .accelerator("F5")
        .build(app)?;
    let save_ws = MenuItemBuilder::with_id("menu_save_workspace", "Save As...")
        .accelerator("Shift+CmdOrCtrl+S")
        .build(app)?;
    let export_file = MenuItemBuilder::with_id("menu_export_file", "Export...").build(app)?;
    let quit_app = MenuItemBuilder::with_id("menu_quit", "Quit")
        .accelerator("Alt+F4")
        .build(app)?;

    let file_menu = SubmenuBuilder::new(app, "File")
        .item(&new_window)
        .item(&open_file)
        .item(&open_folder)
        .item(&reopen_view)
        .item(&reopen_view_f5)
        .separator()
        .item(&save_ws)
        .item(&export_file)
        .separator()
        .item(&quit_app)
        .build()?;

    // Edit menu
    let cut_item = PredefinedMenuItem::cut(app, None)?;
    let copy_item = PredefinedMenuItem::copy(app, None)?;
    let paste_item = PredefinedMenuItem::paste(app, None)?;
    let edit_menu = SubmenuBuilder::new(app, "Edit")
        .item(&cut_item)
        .item(&copy_item)
        .item(&paste_item)
        .build()?;

    // View menu
    let zoom_reset = MenuItemBuilder::with_id("menu_zoom_reset", "Zoom Reset")
        .accelerator("CmdOrCtrl+0")
        .build(app)?;
    let zoom_in = MenuItemBuilder::with_id("menu_zoom_in", "Zoom In")
        .accelerator("CmdOrCtrl+=")
        .build(app)?;
    let zoom_out = MenuItemBuilder::with_id("menu_zoom_out", "Zoom Out")
        .accelerator("CmdOrCtrl+-")
        .build(app)?;
    let toggle_sidebar =
        MenuItemBuilder::with_id("menu_toggle_sidebar", "Toggle Workspace Sidebar")
            .accelerator("CmdOrCtrl+B")
            .build(app)?;
    let show_hidden = MenuItemBuilder::with_id("menu_show_hidden_cols", "Show Hidden Columns")
        .accelerator("CmdOrCtrl+Shift+H")
        .build(app)?;
    let toggle_theme = MenuItemBuilder::with_id("menu_toggle_theme", "Toggle Dark / Light Theme")
        .build(app)?;

    let view_menu = SubmenuBuilder::new(app, "View")
        .item(&zoom_reset)
        .item(&zoom_in)
        .item(&zoom_out)
        .separator()
        .item(&toggle_sidebar)
        .item(&show_hidden)
        .item(&toggle_theme)
        .build()?;

    // Help menu
    let quick_start =
        MenuItemBuilder::with_id("menu_quick_start", "Quick Start Guide").build(app)?;
    let open_example =
        MenuItemBuilder::with_id("menu_open_example", "Open Sample Dataset").build(app)?;
    let feedback = MenuItemBuilder::with_id(
        "menu_feedback",
        "Send Feedback / Bug Reports (GitHub)",
    )
    .build(app)?;
    let about = MenuItemBuilder::with_id("menu_about", "About Pulex").build(app)?;

    let help_menu = SubmenuBuilder::new(app, "Help")
        .item(&quick_start)
        .item(&open_example)
        .item(&feedback)
        .separator()
        .item(&about)
        .build()?;

    let menu = MenuBuilder::new(app)
        .items(&[&file_menu, &edit_menu, &view_menu, &help_menu])
        .build()?;

    app.set_menu(menu)?;

    let zoom_factor = Arc::new(Mutex::new(1.0_f64));
    app.on_menu_event(move |app_handle, event| {
        let id = event.id().as_ref();
        match id {
            "menu_quit" => {
                app_handle.exit(0);
            }
            "menu_new_window" => {
                let handle = app_handle.clone();
                tauri::async_runtime::spawn(async move {
                    if let Err(e) = commands::system::handle_new_window(handle, None).await {
                        tracing::warn!("Failed to spawn new Pulex window from menu: {}", e);
                    }
                });
            }
            "menu_zoom_reset" | "menu_zoom_in" | "menu_zoom_out" => {
                let next_zoom = {
                    let mut guard = zoom_factor.lock().unwrap_or_else(|e| e.into_inner());
                    let updated = match id {
                        "menu_zoom_reset" => 1.0,
                        "menu_zoom_in" => (*guard + 0.1).clamp(0.5, 2.5),
                        "menu_zoom_out" => (*guard - 0.1).clamp(0.5, 2.5),
                        _ => *guard,
                    };
                    *guard = (updated * 10.0).round() / 10.0;
                    *guard
                };
                if let Some(win) = app_handle
                    .webview_windows()
                    .into_values()
                    .find(|w| w.is_focused().unwrap_or(false))
                    .or_else(|| app_handle.get_webview_window("main"))
                {
                    let _ = win.set_zoom(next_zoom);
                }
            }
            "menu_feedback" => {
                let _ = app_handle.opener().open_url(
                    "https://github.com/HelloKittyXamKinLung/pulex/issues",
                    None::<&str>,
                );
            }
            "menu_about" => {
                let about_text = concat!(
                    "Pulex Data Analyzer\n",
                    "Version 3.0.0\n\n",
                    "Engine: Tauri v2 + Rust + DuckDB 1.2.2 + Apache Arrow IPC\n\n",
                    "Copyright (c) 2026 HelloKittyXamKinLung"
                );
                app_handle
                    .dialog()
                    .message(about_text)
                    .title("About Pulex")
                    .kind(MessageDialogKind::Info)
                    .show(|_| {});
            }
            _ => {
                let _ = app_handle.emit("toad:menu-action", serde_json::json!({ "action": id }));
            }
        }
    });

    Ok(())
}

#[cfg(not(test))]
pub fn build_app() -> Result<tauri::Builder<tauri::Wry>, Box<dyn std::error::Error>> {
    let (paths, mut config) = portable::ensure_portable_environment(None)?;
    // Pin WebView2 user data inside the canonical portable pulexAppData_<5chars> folder
    if std::env::var_os("WEBVIEW2_USER_DATA_FOLDER").is_none() {
        let wv2_dir = paths.app_data_dir.join("WebView2");
        let _ = std::fs::create_dir_all(&wv2_dir);
        std::env::set_var("WEBVIEW2_USER_DATA_FOLDER", &wv2_dir);
    }
    if let Ok(env_lvl) = std::env::var("PULEX_LOG_LEVEL")
        .or_else(|_| std::env::var("TOAD_LOG_LEVEL"))
        .or_else(|_| std::env::var("RUST_LOG"))
    {
        if !env_lvl.trim().is_empty() {
            config.system.log_level = env_lvl.trim().to_string();
        }
    }
    let _ = logger::init_logger(&paths.logs_dir, &config.system.log_level);
    let app_state = state::AppState::new(paths.app_data_dir, config);

    let builder = tauri::Builder::default()
        // MANDATORY: Single instance MUST be the first plugin registered!
        .plugin(tauri_plugin_single_instance::init(|app, args, cwd| {
            tracing::info!(
                args = ?args,
                cwd = ?cwd,
                "Single instance triggered; focusing existing window and forwarding args"
            );
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.unminimize();
                let _ = window.set_focus();
                let _ = window.emit(
                    "toad:single-instance-open",
                    serde_json::json!({
                        "args": args,
                        "cwd": cwd,
                    }),
                );
            }
        }))
        .plugin(tauri_plugin_window_state::Builder::default().build())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_fs::init())
        .manage(app_state)
        .setup(|app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let st = handle.state::<state::AppState>();
                st.attach_app_handle(handle.clone()).await;
            });
            if let Err(e) = setup_native_menu(app) {
                tracing::warn!("Non-fatal error configuring native menu bar: {}", e);
            }
            Ok(())
        })
        .invoke_handler(commands::dispatch_invoke());

    Ok(builder)
}

/// Main application entry point invoked from main.rs
#[cfg(not(test))]
pub fn run() {
    tracing::info!("Starting Pulex Desktop v3.0.0 Core Runtime...");

    match build_app() {
        Ok(builder) => {
            builder
                .run(tauri::generate_context!())
                .expect("Failed to run Pulex Tauri v2 application");
        }
        Err(e) => {
            eprintln!("Fatal error during Pulex initialization: {}", e);
            tracing::error!("Fatal error during Pulex initialization: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_portable_paths_and_config() {
        let temp_dir = std::env::temp_dir().join("pulex_test_lib_init");
        let (paths, config) = portable::ensure_portable_environment(Some(&temp_dir)).unwrap();

        assert!(paths.app_data_dir.exists());
        assert!(paths.temp_dir.exists());
        assert!(paths.logs_dir.exists());
        assert!(paths.config_path.exists());
        assert_eq!(config.general.viewer, "pulex");

        let _ = std::fs::remove_dir_all(&paths.app_data_dir);
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
