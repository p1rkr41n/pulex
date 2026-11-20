//! system.rs
//!
//! System & Environment Command Handlers for Pulex Desktop (Tauri v2)
//! - init_main: Reports environment readiness and setup metadata.
//! - get_config / save_config: Provides and updates AppConfigDTO matching config.ini.
//! - get_startup_args: Returns CLI launch arguments & working directory.
//! - open_file_dialog / browse_export_path: Native OS file & save pickers via `tauri-plugin-dialog`.
//! - export_file: Vectorized DuckDB `COPY (...) TO` exporter (CSV, Parquet, JSONL).
//! - save_workspace / load_workspace: `.pulex` / `.tad` JSON workspace snapshot persistence.
//! - write_clipboard: Writes text to system clipboard via `tauri-plugin-clipboard-manager`.
//! - open_url: Opens safe `http://` / `https://` URLs via `tauri-plugin-opener` (blocks arbitrary protocol RCE).
//! - new_window: Spawns an isolated secondary WebviewWindow via `WebviewWindowBuilder`.

use crate::engine::query_compiler::QueryExpCompiler;
use crate::ledger::parquet_ledger::generate_project_id;
use crate::portable::ToadConfig;
use crate::state::AppState;
use serde_json::{json, Value};
use std::path::Path;
use tauri::{AppHandle, Manager, Runtime, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_clipboard_manager::ClipboardExt;
use tauri_plugin_dialog::DialogExt;
use tauri_plugin_opener::OpenerExt;

/// Validate external URL scheme to strictly allow `http://` and `https://`
pub fn assert_safe_external_url(url: &str) -> Result<(), String> {
    let trimmed = url.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("https://") || lower.starts_with("http://") {
        if trimmed.chars().any(|c| c.is_control()) {
            return Err("Rejected external URL containing control characters".to_string());
        }
        Ok(())
    } else {
        Err(format!(
            "Security policy blocked non-HTTP(S) external URL scheme: {}",
            trimmed
        ))
    }
}

/// Handle toad:init_main
pub async fn handle_init_main<R: Runtime>(
    _app: AppHandle<R>,
    state: &AppState,
) -> Result<Value, String> {
    let cfg = state.get_config().await;
    tracing::info!("toad:init_main invoked - Pulex Core v3.0.3 ready");

    Ok(json!({
        "status": "ready",
        "product": "Pulex",
        "version": "3.0.3",
        "portableDir": state.portable_dir.to_string_lossy(),
        "config": cfg,
    }))
}

/// Handle toad:get_config
pub async fn handle_get_config(state: &AppState) -> Result<ToadConfig, String> {
    Ok(state.get_config().await)
}

/// Handle toad:get_startup_args
pub fn handle_get_startup_args() -> Value {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cwd = std::env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();
    json!({
        "args": args,
        "cwd": cwd
    })
}

/// Handle toad:open_file_dialog / dialog:showOpenDialog
pub async fn handle_open_file_dialog<R: Runtime>(
    app: AppHandle<R>,
    state: &AppState,
    payload: Option<Value>,
) -> Result<Value, String> {
    let mut builder = app.dialog().file();

    if let Some(win) = app.get_webview_window("main") {
        builder = builder.set_parent(&win);
    }

    let cfg = state.get_config().await;
    let mut multiple = false;
    let mut directory = false;
    let mut has_explicit_dir = false;
    let mut has_explicit_filters = false;

    if let Some(p) = &payload {
        if let Some(title) = p.get("title").and_then(|v| v.as_str()) {
            builder = builder.set_title(title);
        }
        if let Some(dir) = p
            .get("defaultPath")
            .or_else(|| p.get("default_path"))
            .and_then(|v| v.as_str())
        {
            if !dir.trim().is_empty() && Path::new(dir).exists() {
                builder = builder.set_directory(dir);
                has_explicit_dir = true;
            }
        }
        if let Some(filters) = p.get("filters").and_then(|v| v.as_array()) {
            for f in filters {
                let name = f.get("name").and_then(|v| v.as_str()).unwrap_or("Files");
                if let Some(exts) = f.get("extensions").and_then(|v| v.as_array()) {
                    let ext_strs: Vec<&str> = exts.iter().filter_map(|e| e.as_str()).collect();
                    if !ext_strs.is_empty() {
                        builder = builder.add_filter(name, &ext_strs);
                        has_explicit_filters = true;
                    }
                }
            }
        }
        multiple = p
            .get("multiple")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        directory = p
            .get("directory")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
    }

    if !directory && !has_explicit_filters {
        builder = builder
            .add_filter(
                "All Supported Data Files",
                &[
                    "csv", "tsv", "tab", "txt", "log", "parquet", "json", "jsonl", "ndjson",
                    "evtx", "gz", "sqlite", "sqlite3", "db", "duckdb", "pulex", "toad", "tad",
                ],
            )
            .add_filter("All Files", &["*"]);
    }

    if !has_explicit_dir
        && cfg.general.auto_restore_last_directory
        && !cfg.general.last_directory.is_empty()
        && Path::new(&cfg.general.last_directory).exists()
    {
        builder = builder.set_directory(&cfg.general.last_directory);
    }

    let (tx, rx) = tokio::sync::oneshot::channel();

    if directory {
        if multiple {
            builder.pick_folders(move |res| {
                let _ = tx.send(res.map(|list| {
                    list.into_iter()
                        .filter_map(|fp| fp.into_path().ok().map(|p| p.to_string_lossy().to_string()))
                        .collect::<Vec<_>>()
                }));
            });
        } else {
            builder.pick_folder(move |res| {
                let _ = tx.send(res.map(|fp| {
                    fp.into_path()
                        .ok()
                        .map(|p| vec![p.to_string_lossy().to_string()])
                        .unwrap_or_default()
                }));
            });
        }
    } else if multiple {
        builder.pick_files(move |res| {
            let _ = tx.send(res.map(|list| {
                list.into_iter()
                    .filter_map(|fp| fp.into_path().ok().map(|p| p.to_string_lossy().to_string()))
                    .collect::<Vec<_>>()
            }));
        });
    } else {
        builder.pick_file(move |res| {
            let _ = tx.send(res.map(|fp| {
                fp.into_path()
                    .ok()
                    .map(|p| vec![p.to_string_lossy().to_string()])
                    .unwrap_or_default()
            }));
        });
    }

    match rx.await {
        Ok(Some(paths)) => {
            if let Some(first) = paths.first() {
                let p = Path::new(first);
                let dir_to_save = if p.is_dir() {
                    Some(p.to_string_lossy().to_string())
                } else {
                    p.parent().map(|par| par.to_string_lossy().to_string())
                };
                if let Some(last_dir) = dir_to_save {
                    let _ = state
                        .update_config(move |c| {
                            c.general.last_directory = last_dir;
                        })
                        .await;
                }
            }
            Ok(json!(paths))
        }
        Ok(None) => Ok(json!([])),
        Err(e) => Err(format!("Dialog execution channel error: {}", e)),
    }
}

/// Handle toad:browse_export_path / dialog:showSaveDialog
pub async fn handle_browse_export_path<R: Runtime>(
    app: AppHandle<R>,
    state: &AppState,
    payload: Option<Value>,
) -> Result<Value, String> {
    let mut builder = app.dialog().file();
    if let Some(win) = app.get_webview_window("main") {
        builder = builder.set_parent(&win);
    }

    let cfg = state.get_config().await;
    if !cfg.general.last_directory.is_empty() && Path::new(&cfg.general.last_directory).exists() {
        builder = builder.set_directory(&cfg.general.last_directory);
    }

    let mut has_explicit_filters = false;
    let mut preferred_format: Option<String> = None;

    if let Some(p) = &payload {
        if let Some(title) = p.get("title").and_then(|v| v.as_str()) {
            builder = builder.set_title(title);
        }
        if let Some(default_name) = p
            .get("defaultName")
            .or_else(|| p.get("defaultFileName"))
            .or_else(|| p.get("defaultPath"))
            .and_then(|v| v.as_str())
        {
            let fname = Path::new(default_name)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(default_name);
            builder = builder.set_file_name(fname);
        }
        preferred_format = p
            .get("exportFormat")
            .or_else(|| p.get("format"))
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_ascii_lowercase());
        if let Some(filters) = p.get("filters").and_then(|v| v.as_array()) {
            for f in filters {
                let name = f.get("name").and_then(|v| v.as_str()).unwrap_or("Export");
                if let Some(exts) = f.get("extensions").and_then(|v| v.as_array()) {
                    let ext_strs: Vec<&str> = exts.iter().filter_map(|e| e.as_str()).collect();
                    if !ext_strs.is_empty() {
                        builder = builder.add_filter(name, &ext_strs);
                        has_explicit_filters = true;
                    }
                }
            }
        }
    }

    if !has_explicit_filters {
        match preferred_format.as_deref() {
            Some("parquet") => {
                builder = builder
                    .add_filter("Parquet", &["parquet"])
                    .add_filter("CSV", &["csv"])
                    .add_filter("JSON Lines", &["jsonl"]);
            }
            Some("jsonl") | Some("json") | Some("ndjson") => {
                builder = builder
                    .add_filter("JSON Lines", &["jsonl"])
                    .add_filter("CSV", &["csv"])
                    .add_filter("Parquet", &["parquet"]);
            }
            _ => {
                builder = builder
                    .add_filter("CSV", &["csv"])
                    .add_filter("Parquet", &["parquet"])
                    .add_filter("JSON Lines", &["jsonl"]);
            }
        }
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    builder.save_file(move |res| {
        let _ = tx.send(
            res.and_then(|fp| fp.into_path().ok())
                .map(|p| p.to_string_lossy().to_string()),
        );
    });

    match rx.await {
        Ok(Some(path_str)) => Ok(json!({ "filePath": path_str, "canceled": false })),
        Ok(None) => Ok(json!({ "filePath": Value::Null, "canceled": true })),
        Err(e) => Err(format!("Save dialog channel error: {}", e)),
    }
}

/// Handle toad:export_file (exports current table or QueryExp to CSV, Parquet, or JSONL via DuckDB COPY)
pub async fn handle_export_file(state: &AppState, payload: Value) -> Result<Value, String> {
    let target_path = payload
        .get("exportPath")
        .or_else(|| payload.get("filePath"))
        .or_else(|| payload.get("targetPath"))
        .or_else(|| payload.get("path"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Missing filePath/exportPath in toad:export_file".to_string())?
        .to_string();

    let format_str = payload
        .get("exportFormat")
        .or_else(|| payload.get("format"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| {
            Path::new(&target_path)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("csv")
                .to_lowercase()
        });

    let source_sql = if let Some(qval) = payload.get("queryExp").or_else(|| payload.get("query")) {
        let qrep = QueryExpCompiler::parse_query_exp(qval)?;
        QueryExpCompiler::compile(&qrep, None, None)?
    } else if let Some(sql) = payload.get("sql").and_then(|v| v.as_str()) {
        sql.trim().trim_end_matches(';').to_string()
    } else if let Some(tbl) = payload.get("tableName").and_then(|v| v.as_str()) {
        format!("SELECT * FROM \"{}\"", tbl.replace('"', "\"\""))
    } else {
        "SELECT * FROM \"main_table\"".to_string()
    };

    if let Some(parent) = Path::new(&target_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let safe_dst = target_path.replace('\\', "/").replace('\'', "''");
    let parquet_compression = payload
        .get("parquetExportOptions")
        .and_then(|o| o.get("compression"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_uppercase())
        .and_then(|c| match c.as_str() {
            "SNAPPY" | "ZSTD" | "GZIP" | "UNCOMPRESSED" => Some(c),
            _ => None,
        })
        .unwrap_or_else(|| "SNAPPY".to_string());

    let copy_opts = match format_str.as_str() {
        "parquet" => format!("(FORMAT PARQUET, COMPRESSION {})", parquet_compression),
        "json" | "jsonl" | "ndjson" => "(FORMAT JSON)".to_string(),
        "tsv" => "(FORMAT CSV, HEADER true, DELIMITER '\\t')".to_string(),
        _ => "(FORMAT CSV, HEADER true)".to_string(),
    };

    let copy_sql = format!("COPY ({}) TO '{}' {};", source_sql, safe_dst, copy_opts);
    let pool = state.get_duckdb_pool().await;
    pool.eval_ui_conn(move |conn| conn.execute_batch(&copy_sql).map_err(|e| e.to_string()))
        .await?;

    let bytes = std::fs::metadata(&target_path).map(|m| m.len()).unwrap_or(0);
    Ok(json!({
        "success": true,
        "filePath": target_path,
        "bytesWritten": bytes,
        "format": format_str
    }))
}

/// Handle toad:save_workspace (writes Toad-v2-compatible `.pulex` / `.toad` / `.tad` workspace JSON to disk)
pub async fn handle_save_workspace<R: Runtime>(
    app: AppHandle<R>,
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    let explicit_path = payload
        .get("filePath")
        .or_else(|| payload.get("path"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string());

    let target_path = match explicit_path {
        Some(p) => p,
        None => {
            let mut builder = app.dialog().file();
            if let Some(win) = app
                .webview_windows()
                .into_values()
                .find(|w| w.is_focused().unwrap_or(false))
                .or_else(|| app.get_webview_window("main"))
            {
                builder = builder.set_parent(&win);
            }
            let cfg = state.get_config().await;
            if !cfg.general.last_directory.is_empty()
                && Path::new(&cfg.general.last_directory).exists()
            {
                builder = builder.set_directory(&cfg.general.last_directory);
            }
            let default_name = payload
                .get("defaultFileName")
                .or_else(|| payload.get("defaultName"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or("workspace.pulex");
            builder = builder
                .set_title("Save Pulex Workspace")
                .set_file_name(default_name)
                .add_filter("Pulex/Toad Workspace", &["pulex", "toad", "tad"])
                .add_filter("All Files", &["*"]);

            let (tx, rx) = tokio::sync::oneshot::channel();
            builder.save_file(move |res| {
                let _ = tx.send(
                    res.and_then(|fp| fp.into_path().ok())
                        .map(|p| p.to_string_lossy().to_string()),
                );
            });

            match rx.await {
                Ok(Some(chosen)) => chosen,
                Ok(None) => {
                    return Ok(json!({
                        "canceled": true,
                        "filePath": Value::Null
                    }));
                }
                Err(e) => return Err(format!("Save workspace dialog channel error: {}", e)),
            }
        }
    };

    let effective = payload.get("workspaceData").unwrap_or(&payload);
    let contents = if let Some(c) = effective.get("contents") {
        c.clone()
    } else if effective.get("dsPath").is_some() || effective.get("viewParams").is_some() {
        json!({
            "dsPath": effective.get("dsPath").cloned().unwrap_or(Value::Null),
            "viewParams": effective.get("viewParams").cloned().unwrap_or(Value::Null),
        })
    } else {
        effective
            .get("workspace")
            .or_else(|| effective.get("viewState"))
            .cloned()
            .unwrap_or_else(|| effective.clone())
    };

    if let Some(parent) = Path::new(&target_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let doc = if let Some(s) = contents.as_str() {
        s.to_string()
    } else {
        let (active_pid, projects) = {
            let ledger = state.parquet_ledger.read().await;
            ledger.get_workspace().await
        };
        let enriched = json!({
            "tadFileFormatVersion": 2,
            "application": "Pulex",
            "version": "3.0.3",
            "savedAtMs": crate::ledger::parquet_ledger::current_time_ms(),
            "contents": contents,
            "activeProjectId": active_pid,
            "projects": projects,
        });
        serde_json::to_string_pretty(&enriched).map_err(|e| e.to_string())?
    };

    std::fs::write(&target_path, doc.as_bytes())
        .map_err(|e| format!("Save workspace error: {}", e))?;

    if let Some(parent_str) = Path::new(&target_path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
    {
        let _ = state
            .update_config(move |c| {
                c.general.last_directory = parent_str;
            })
            .await;
    }

    Ok(json!({
        "canceled": false,
        "success": true,
        "filePath": target_path
    }))
}

/// Handle toad:open_example (generates a built-in sample forensic dataset in temp_dir and returns its path)
pub async fn handle_open_example(state: &AppState) -> Result<Value, String> {
    let sample_dir = state.portable_dir.join("examples");
    std::fs::create_dir_all(&sample_dir).map_err(|e| e.to_string())?;
    let sample_file = sample_dir.join("pulex_forensics_sample.csv");
    if !sample_file.exists() {
        let sample_csv = concat!(
            "TimeCreated,EventID,Level,Provider,Computer,UserId,ProcessName,CommandLine\n",
            "2026-11-16T08:00:01.120Z,4624,Information,Microsoft-Windows-Security-Auditing,DC01.secops.local,S-1-5-18,lsass.exe,C:\\Windows\\System32\\lsass.exe\n",
            "2026-11-16T08:01:14.450Z,4688,Information,Microsoft-Windows-Security-Auditing,WS-ANALYST01,S-1-5-21-1001,powershell.exe,powershell.exe -NoProfile -ExecutionPolicy Bypass\n",
            "2026-11-16T08:02:39.890Z,7045,Warning,Service Control Manager,DC01.secops.local,S-1-5-18,services.exe,C:\\ProgramData\\updater\\svc_helper.exe --service\n",
            "2026-11-16T08:04:02.005Z,4625,Error,Microsoft-Windows-Security-Auditing,VPN-GW01,S-1-0-0,svchost.exe,Failed logon from 198.51.100.42\n",
            "2026-11-16T08:05:55.310Z,1102,Critical,Microsoft-Windows-Eventlog,WS-ANALYST01,S-1-5-21-1001,wevtutil.exe,wevtutil cl Security\n"
        );
        std::fs::write(&sample_file, sample_csv).map_err(|e| e.to_string())?;
    }
    let path_str = sample_file.to_string_lossy().to_string();
    Ok(json!({
        "success": true,
        "filePath": path_str,
        "path": path_str
    }))
}

/// Handle toad:write_clipboard / clipboard:writeText
pub async fn handle_write_clipboard<R: Runtime>(
    app: AppHandle<R>,
    text: String,
) -> Result<Value, String> {
    app.clipboard()
        .write_text(text)
        .map_err(|e| format!("Clipboard error: {}", e))?;
    Ok(json!({ "success": true }))
}

/// Handle toad:open_url / shell:openExternal (strictly HTTP/HTTPS allowlisted)
pub async fn handle_open_url<R: Runtime>(
    app: AppHandle<R>,
    url: String,
) -> Result<Value, String> {
    assert_safe_external_url(&url)?;
    app.opener()
        .open_url(&url, None::<&str>)
        .map_err(|e| format!("Opener error: {}", e))?;
    Ok(json!({ "success": true }))
}

/// Extract a file/resource path string from either a plain string payload,
/// `{ dsPath: "..." }`, `{ filePath: "..." }`, `{ path: "..." }`, or an object-shaped
/// Toad `DataSourcePath` (`{ dsPath: { sourceId: { resourceId: "..." }, path: [...] } }`
/// or `{ sourceId: { resourceId: "..." } }`).
pub fn extract_ds_path_str(payload: Option<&Value>) -> Option<String> {
    let p = payload?;
    if let Some(s) = p.as_str() {
        let trimmed = s.trim();
        return (!trimmed.is_empty()).then(|| trimmed.to_string());
    }

    if let Some(ds) = p.get("dsPath") {
        if let Some(s) = ds.as_str() {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        } else if let Some(res_id) = ds
            .get("sourceId")
            .and_then(|sid| sid.get("resourceId"))
            .and_then(|v| v.as_str())
        {
            let trimmed = res_id.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }

    if let Some(res_id) = p
        .get("sourceId")
        .and_then(|sid| sid.get("resourceId"))
        .and_then(|v| v.as_str())
    {
        let trimmed = res_id.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    p.get("filePath")
        .or_else(|| p.get("path"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Handle toad:new_window — spawns a real secondary Tauri WebviewWindow
pub async fn handle_new_window<R: Runtime>(
    app: AppHandle<R>,
    payload: Option<Value>,
) -> Result<Value, String> {
    let ds_path = extract_ds_path_str(payload.as_ref());

    let win_label = format!("pulex_{}", generate_project_id());
    let url_target = if let Some(ref p) = ds_path {
        let encoded: String = p
            .bytes()
            .flat_map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    vec![b as char]
                }
                _ => format!("%{:02X}", b).chars().collect(),
            })
            .collect();
        format!("index.html?openFile={}", encoded)
    } else {
        "index.html".to_string()
    };

    let title = match &ds_path {
        Some(p) => {
            let fname = Path::new(p)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(p);
            format!("{} - Pulex", fname)
        }
        None => "Pulex".to_string(),
    };

    let existing_count = app.webview_windows().len().max(1);
    let cascade_offset = ((existing_count % 10) as f64) * 30.0;

    let mut win_builder =
        WebviewWindowBuilder::new(&app, &win_label, WebviewUrl::App(url_target.into()))
            .title(&title)
            .inner_size(1280.0, 980.0)
            .min_inner_size(960.0, 600.0);

    if let Some(base_win) = app.get_webview_window("main") {
        if let (Ok(pos), Ok(scale)) = (base_win.outer_position(), base_win.scale_factor()) {
            let logical_x = (pos.x as f64 / scale) + cascade_offset;
            let logical_y = (pos.y as f64 / scale) + cascade_offset;
            win_builder = win_builder.position(logical_x, logical_y);
        }
    } else {
        win_builder = win_builder.position(100.0 + cascade_offset, 80.0 + cascade_offset);
    }

    win_builder
        .build()
        .map_err(|e| format!("Failed to create new Pulex window: {}", e))?;

    Ok(json!({
        "success": true,
        "windowLabel": win_label,
        "dsPath": ds_path
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_handler() {
        let temp_dir = std::env::temp_dir().join("pulex_test_sys");
        let _ = std::fs::create_dir_all(&temp_dir);
        let cfg = ToadConfig::default();
        let state = AppState::new(temp_dir.clone(), cfg);

        let res = handle_get_config(&state).await.unwrap();
        assert_eq!(res.general.viewer, "pulex");
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_url_scheme_guard_blocks_dangerous_schemes() {
        assert!(assert_safe_external_url("https://github.com/morluto/rea").is_ok());
        assert!(assert_safe_external_url("http://localhost:8080/docs").is_ok());
        assert!(assert_safe_external_url("file:///C:/Windows/System32/calc.exe").is_err());
        assert!(assert_safe_external_url("ms-msdt:/id PCWDiagnostic").is_err());
        assert!(assert_safe_external_url("javascript:alert(1)").is_err());
        assert!(assert_safe_external_url("\\\\attacker.local\\share\\payload.exe").is_err());
    }

    #[tokio::test]
    async fn test_export_file_accepts_export_path_and_format() {
        let temp_dir = std::env::temp_dir().join(format!(
            "pulex_test_export_{}",
            generate_project_id()
        ));
        let _ = std::fs::create_dir_all(&temp_dir);
        let state = AppState::new(temp_dir.clone(), ToadConfig::default());

        let csv_path = temp_dir.join("out.csv");
        let csv_res = handle_export_file(
            &state,
            json!({
                "exportPath": csv_path.to_string_lossy(),
                "exportFormat": "csv",
                "sql": "SELECT 42 AS id, 'alice' AS analyst"
            }),
        )
        .await
        .expect("CSV export via exportPath/exportFormat should succeed");
        assert_eq!(csv_res["success"], true);
        assert_eq!(csv_res["format"], "csv");
        let csv_text = std::fs::read_to_string(&csv_path).unwrap();
        assert!(csv_text.contains("alice"));

        let pq_path = temp_dir.join("out.parquet");
        let pq_res = handle_export_file(
            &state,
            json!({
                "exportPath": pq_path.to_string_lossy(),
                "exportFormat": "parquet",
                "sql": "SELECT 42 AS id, 'alice' AS analyst",
                "parquetExportOptions": { "compression": "ZSTD" }
            }),
        )
        .await
        .expect("Parquet export with ZSTD compression should succeed");
        assert_eq!(pq_res["success"], true);
        assert_eq!(pq_res["format"], "parquet");
        assert!(pq_res["bytesWritten"].as_u64().unwrap_or(0) > 0);
        assert!(pq_path.exists());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_open_example_returns_both_filepath_and_path() {
        let temp_dir = std::env::temp_dir().join(format!(
            "pulex_test_example_{}",
            generate_project_id()
        ));
        let _ = std::fs::create_dir_all(&temp_dir);
        let state = AppState::new(temp_dir.clone(), ToadConfig::default());

        let res = handle_open_example(&state).await.unwrap();
        assert_eq!(res["success"], true);
        let fp = res["filePath"].as_str().expect("filePath key must exist");
        let p = res["path"].as_str().expect("path key must exist");
        assert_eq!(fp, p);
        assert!(Path::new(fp).exists());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_extract_ds_path_str_handles_strings_and_object_dspath() {
        assert_eq!(extract_ds_path_str(None), None);

        let plain = json!("C:\\data\\logs.csv");
        assert_eq!(
            extract_ds_path_str(Some(&plain)).as_deref(),
            Some("C:\\data\\logs.csv")
        );

        let wrapped_str = json!({ "dsPath": "D:\\events.parquet" });
        assert_eq!(
            extract_ds_path_str(Some(&wrapped_str)).as_deref(),
            Some("D:\\events.parquet")
        );

        let object_ds = json!({
            "dsPath": {
                "sourceId": {
                    "providerName": "localfs",
                    "resourceId": "D:\\cases\\case01\\security.evtx"
                },
                "path": ["security.evtx"]
            }
        });
        assert_eq!(
            extract_ds_path_str(Some(&object_ds)).as_deref(),
            Some("D:\\cases\\case01\\security.evtx")
        );

        let direct_object = json!({
            "sourceId": {
                "providerName": "localfs",
                "resourceId": "E:\\forensics\\timeline.ndjson"
            },
            "path": ["timeline.ndjson"]
        });
        assert_eq!(
            extract_ds_path_str(Some(&direct_object)).as_deref(),
            Some("E:\\forensics\\timeline.ndjson")
        );

        let legacy = json!({ "filePath": "F:\\export.jsonl" });
        assert_eq!(
            extract_ds_path_str(Some(&legacy)).as_deref(),
            Some("F:\\export.jsonl")
        );
    }
}
