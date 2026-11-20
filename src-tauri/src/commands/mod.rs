//! mod.rs
//!
//! Tauri Command Router & Aliases Dispatcher for Toad Desktop
//! - Classifies and normalizes namespaced commands (`toad:*`, `project:*`, `engine:*`) and legacy aliases (`evalQuery`, etc.).
//! - Passes all native Tauri plugin commands (`plugin:*`) through to Tauri's registered plugins (`return false`).
//! - Routes system requests to `commands::system` handlers with genuine plugin extension traits.
//! - Provides clean typed DTO stubs for Query (M3), Ledger (M4), and Forensics (M5).

pub mod ingest;
pub mod query;
pub mod system;
pub mod workspace;

use crate::state::AppState;
use serde_json::{json, Value};
use tauri::ipc::{Invoke, InvokeBody};
use tauri::Manager;

/// Discrete actions supported by the Toad command dispatcher
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandAction {
    // System Commands
    InitMain,
    GetConfig,
    GetStartupArgs,
    OpenFileDialog,
    BrowseExportPath,
    ExportFile,
    SaveWorkspace,
    LoadWorkspaceFile,
    StatPath,
    WriteClipboard,
    OpenUrl,
    NewWindow,
    GetSetupInfo,
    OpenExample,
    Log,

    // Query, Engine & Derived Views (Milestone M3)
    EvalQuery,
    RowCount,
    Schema,
    RunSqlArrow,
    GetTableName,
    GetRootNode,
    GetChildren,
    GetColumnStatsMap,
    RunSql,
    GetDataSources,
    RegisterDerivedView,
    GetDerivedView,

    // Project Workspace CRUD Stubs (Milestone M4)
    ProjectGetWorkspace,
    ProjectGetFiles,
    ProjectAddFiles,
    ProjectAddFolder,
    ProjectDelete,
    ProjectRename,
    ProjectCreate,
    ProjectSetActive,
    ProjectReconcile,
    ProjectDeleteAll,
    ProjectGetCacheSize,
    ProjectPurgeCache,
    ProjectRemoveFile,
    ProjectMoveOrphanFiles,

    // Forensics & Ingestion Commands (Milestone M5)
    IngestFile,
    IngestCancel,
    IngestVip,
    IngestGetQueue,

    // Engine Control Stubs
    EngineGetStatus,
    EngineDoRestart,

    /// Native Tauri plugin commands (or unhandled commands) bypassed to the plugin pipeline
    PluginPassthrough,
}

/// Classify an incoming IPC command string into a `CommandAction`.
/// Canonical Tauri plugin command strings (`plugin:*`) MUST NOT be matched here,
/// ensuring they resolve to `CommandAction::PluginPassthrough`.
pub fn route_command(cmd: &str) -> CommandAction {
    match cmd {
        // ================================================================
        // Core Toad System Commands
        // ================================================================
        "toad:init_main" | "initMain" | "init_main" => CommandAction::InitMain,
        "toad:get_config" | "getToadConfig" | "get_config" => CommandAction::GetConfig,
        "toad:get_startup_args" | "getStartupArgs" | "get_startup_args" => {
            CommandAction::GetStartupArgs
        }
        "toad:open_file_dialog" | "dialog:showOpenDialog" => CommandAction::OpenFileDialog,
        "toad:browse_export_path" | "dialog:showSaveDialog" | "browseExportPath" => {
            CommandAction::BrowseExportPath
        }
        "toad:export_file" | "exportFile" | "export_file" => CommandAction::ExportFile,
        "toad:save_workspace" | "saveWorkspace" | "save_workspace" => CommandAction::SaveWorkspace,
        "toad:load_workspace_file" | "loadWorkspaceFile" | "load_workspace_file" => {
            CommandAction::LoadWorkspaceFile
        }
        "toad:stat_path" | "statPath" | "stat_path" => CommandAction::StatPath,
        "toad:write_clipboard" | "clipboard:writeText" => CommandAction::WriteClipboard,
        "toad:open_url" | "shell:openExternal" => CommandAction::OpenUrl,
        "toad:new_window" | "newWindowFromDSPath" | "new_window" => CommandAction::NewWindow,
        "toad:get_setup_info" | "getSetupInfo" | "get_setup_info" => CommandAction::GetSetupInfo,
        "toad:open_example" | "openExample" | "open_example" => CommandAction::OpenExample,
        "toad:log" | "log" => CommandAction::Log,

        // ================================================================
        // Query & Engine Stubs (Milestone M3)
        // ================================================================
        "toad:eval_query"
        | "DataSourceConnection.evalQuery"
        | "evalQuery"
        | "toad:query"
        | "eval_query" => CommandAction::EvalQuery,

        "toad:row_count"
        | "DataSourceConnection.rowCount"
        | "rowCount"
        | "row_count" => CommandAction::RowCount,

        "toad:schema"
        | "toad:get_table_schema"
        | "DataSourceConnection.getTableSchema"
        | "schema"
        | "getTableSchema"
        | "get_table_schema" => CommandAction::Schema,

        "toad:run_sql_arrow"
        | "DataSourceConnection.runSqlQueryArrow"
        | "run_sql_arrow" => CommandAction::RunSqlArrow,

        "toad:get_table_name"
        | "DataSourceConnection.getTableName"
        | "getTableName"
        | "get_table_name" => CommandAction::GetTableName,

        "toad:get_root_node"
        | "DataSourceConnection.getRootNode"
        | "getRootNode"
        | "get_root_node" => CommandAction::GetRootNode,

        "toad:get_children"
        | "DataSourceConnection.getChildren"
        | "getChildren"
        | "get_children" => CommandAction::GetChildren,

        "toad:get_column_stats_map"
        | "DataSourceConnection.getColumnStatsMap"
        | "getColumnStatsMap"
        | "get_column_stats_map" => CommandAction::GetColumnStatsMap,

        "toad:run_sql"
        | "DataSourceConnection.runSqlQuery"
        | "runSqlQuery"
        | "run_sql" => CommandAction::RunSql,

        "toad:get_data_sources"
        | "getDataSources"
        | "get_data_sources" => CommandAction::GetDataSources,

        "view:register_derived"
        | "view:registerDerived"
        | "registerDerivedView"
        | "register_derived_view" => CommandAction::RegisterDerivedView,
        "view:get_derived"
        | "view:getDerived"
        | "getDerivedView"
        | "get_derived_view" => CommandAction::GetDerivedView,

        // ================================================================
        // Project Workspace CRUD Stubs (Milestone M4)
        // ================================================================
        "project:get_workspace" | "project:getWorkspace" | "get_workspace" => {
            CommandAction::ProjectGetWorkspace
        }
        "project:get_files" | "project:getFiles" | "get_files" => {
            CommandAction::ProjectGetFiles
        }
        "project:add_files" | "project:addFiles" | "add_files" => {
            CommandAction::ProjectAddFiles
        }
        "project:add_folder" | "project:addFolder" | "add_folder" => {
            CommandAction::ProjectAddFolder
        }
        "project:delete" | "delete_project" => CommandAction::ProjectDelete,
        "project:rename" | "rename_project" => CommandAction::ProjectRename,
        "project:create" | "create_project" => CommandAction::ProjectCreate,
        "project:set_active" | "project:setActive" | "set_active" => {
            CommandAction::ProjectSetActive
        }
        "project:reconcile" | "reconcile_project" => CommandAction::ProjectReconcile,
        "project:delete_all" | "project:deleteAll" | "delete_all" => {
            CommandAction::ProjectDeleteAll
        }
        "project:get_cache_size" | "project:getCacheSize" | "get_cache_size" => {
            CommandAction::ProjectGetCacheSize
        }
        "project:purge_cache" | "project:purgeCache" | "purge_cache" => {
            CommandAction::ProjectPurgeCache
        }
        "project:remove_file" | "project:removeFile" | "remove_file" => {
            CommandAction::ProjectRemoveFile
        }
        "project:move_orphan_files" | "project:moveOrphanFiles" | "move_orphan_files" => {
            CommandAction::ProjectMoveOrphanFiles
        }

        // ================================================================
        // Forensics & Ingestion Commands (Milestone M5)
        // ================================================================
        "toad:ingest_file" | "ingestFile" | "ingest_file" => CommandAction::IngestFile,
        "toad:ingest_cancel" | "cancelIngest" | "ingest_cancel" => CommandAction::IngestCancel,
        "toad:ingest_vip" | "prioritizeIngest" | "ingest_vip" => CommandAction::IngestVip,
        "toad:get_ingest_queue" | "getIngestQueue" | "get_ingest_queue" => {
            CommandAction::IngestGetQueue
        }

        // ================================================================
        // Engine Control Stubs
        // ================================================================
        "engine:get_status" | "engine:status" | "engine_status" => {
            CommandAction::EngineGetStatus
        }
        "engine:do_restart" | "engine:restart" | "engine_restart" => {
            CommandAction::EngineDoRestart
        }

        // Any other command (including canonical `plugin:*`) falls through
        _ => CommandAction::PluginPassthrough,
    }
}

/// Build the custom invoke handler closure for Tauri Builder
pub fn dispatch_invoke() -> impl Fn(Invoke<tauri::Wry>) -> bool + Send + Sync + 'static {
    move |invoke: Invoke<tauri::Wry>| {
        let cmd = invoke.message.command().to_string();
        let app = invoke.message.webview().app_handle().clone();

        let payload: Value = match invoke.message.payload() {
            InvokeBody::Json(val) => val.clone(),
            InvokeBody::Raw(_) => Value::Null,
        };

        tracing::trace!(cmd = %cmd, payload = ?payload, "Tauri IPC Command Received");

        match route_command(cmd.as_str()) {
            CommandAction::PluginPassthrough => {
                tracing::debug!("Bypassing unintercepted/plugin command to Tauri plugin pipeline: {}", cmd);
                false
            }

            CommandAction::InitMain => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match system::handle_init_main(app.clone(), &state).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetConfig => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match system::handle_get_config(&state).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetStartupArgs => {
                invoke.resolver.resolve(system::handle_get_startup_args());
                true
            }

            CommandAction::OpenFileDialog => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match system::handle_open_file_dialog(app.clone(), &state, Some(payload)).await
                    {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::BrowseExportPath => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match system::handle_browse_export_path(app.clone(), &state, Some(payload))
                        .await
                    {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ExportFile => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match system::handle_export_file(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::SaveWorkspace => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match system::handle_save_workspace(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::LoadWorkspaceFile => {
                let resolver = invoke.resolver;
                let file_path = payload
                    .get("filePath")
                    .or_else(|| payload.get("workspacePath"))
                    .or_else(|| payload.get("path"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                match std::fs::read_to_string(&file_path) {
                    Ok(contents) => resolver.resolve(json!({
                        "success": true,
                        "filePath": file_path,
                        "contents": contents
                    })),
                    Err(e) => resolver.reject(format!("Failed to read workspace file '{}': {}", file_path, e)),
                }
                true
            }

            CommandAction::StatPath => {
                let resolver = invoke.resolver;
                if let Some(paths_arr) = payload.get("paths").and_then(|v| v.as_array()) {
                    let mut results = Vec::with_capacity(paths_arr.len());
                    for item in paths_arr {
                        if let Some(p_str) = item.as_str() {
                            let p = std::path::Path::new(p_str);
                            let meta = std::fs::metadata(p).ok();
                            results.push(json!({
                                "path": p_str,
                                "exists": meta.is_some(),
                                "isDirectory": meta.as_ref().map(|m| m.is_dir()).unwrap_or(false),
                                "isFile": meta.as_ref().map(|m| m.is_file()).unwrap_or(false),
                                "sizeBytes": meta.as_ref().map(|m| m.len()).unwrap_or(0)
                            }));
                        }
                    }
                    resolver.resolve(json!(results));
                } else {
                    let p_str = payload
                        .get("path")
                        .or_else(|| payload.get("filePath"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let p = std::path::Path::new(p_str);
                    let meta = std::fs::metadata(p).ok();
                    resolver.resolve(json!({
                        "path": p_str,
                        "exists": meta.is_some(),
                        "isDirectory": meta.as_ref().map(|m| m.is_dir()).unwrap_or(false),
                        "isFile": meta.as_ref().map(|m| m.is_file()).unwrap_or(false),
                        "sizeBytes": meta.as_ref().map(|m| m.len()).unwrap_or(0)
                    }));
                }
                true
            }

            CommandAction::Log => {
                let resolver = invoke.resolver;
                if let Some(entries) = payload.get("entries").and_then(|v| v.as_array()) {
                    for entry in entries {
                        let level = entry
                            .get("level")
                            .and_then(|v| v.as_str())
                            .unwrap_or("info");
                        let message = entry
                            .get("message")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let details = entry.get("details");
                        match level.to_lowercase().as_str() {
                            "trace" => tracing::trace!(details = ?details, "[FRONTEND] {}", message),
                            "debug" => tracing::debug!(details = ?details, "[FRONTEND] {}", message),
                            "warn" => tracing::warn!(details = ?details, "[FRONTEND] {}", message),
                            "error" => tracing::error!(details = ?details, "[FRONTEND] {}", message),
                            _ => tracing::info!(details = ?details, "[FRONTEND] {}", message),
                        }
                    }
                } else {
                    let level = payload
                        .get("level")
                        .and_then(|v| v.as_str())
                        .unwrap_or("info");
                    let message = payload
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let details = payload.get("details");
                    match level.to_lowercase().as_str() {
                        "trace" => tracing::trace!(details = ?details, "[FRONTEND] {}", message),
                        "debug" => tracing::debug!(details = ?details, "[FRONTEND] {}", message),
                        "warn" => tracing::warn!(details = ?details, "[FRONTEND] {}", message),
                        "error" => tracing::error!(details = ?details, "[FRONTEND] {}", message),
                        _ => tracing::info!(details = ?details, "[FRONTEND] {}", message),
                    }
                }
                resolver.resolve(serde_json::json!({ "success": true }));
                true
            }

            CommandAction::WriteClipboard => {
                let text = payload
                    .get("text")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    match system::handle_write_clipboard(app.clone(), text).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::OpenUrl => {
                let url = payload
                    .get("url")
                    .and_then(|u| u.as_str())
                    .unwrap_or("")
                    .to_string();
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    match system::handle_open_url(app.clone(), url).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::NewWindow => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    match system::handle_new_window(app.clone(), Some(payload)).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetSetupInfo => {
                invoke.resolver.resolve(json!({
                    "appName": "Pulex",
                    "version": "3.0.3",
                    "engine": "tauri",
                    "platform": "win32"
                }));
                true
            }

            CommandAction::OpenExample => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match system::handle_open_example(&state).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::EvalQuery => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_eval_query(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::RowCount => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_row_count(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::Schema => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_schema(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::RunSqlArrow => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_run_sql_arrow(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetTableName => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_get_table_name(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetRootNode => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_get_root_node(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetChildren => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_get_children(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetColumnStatsMap => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_get_column_stats_map(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::RunSql => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_run_sql(app.clone(), &state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetDataSources => {
                let resolver = invoke.resolver;
                resolver.resolve(serde_json::json!([
                    {
                        "providerName": "localfs",
                        "resourceId": "",
                        "displayName": "Local Filesystem"
                    },
                    {
                        "providerName": "duckdb",
                        "resourceId": "",
                        "displayName": "DuckDB"
                    }
                ]));
                true
            }

            CommandAction::RegisterDerivedView => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_register_derived_view(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::GetDerivedView => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match query::handle_get_derived_view(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectGetWorkspace => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_get_workspace(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectGetFiles => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_get_files(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectAddFiles => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_add_files(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectAddFolder => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_add_folder(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectDelete => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_delete_project(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectRename => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_rename_project(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectCreate => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_create_project(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectSetActive => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_set_active_project(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectReconcile => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_reconcile(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectDeleteAll => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_delete_all_projects(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectGetCacheSize => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_get_cache_size(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectPurgeCache => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_purge_cache(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectRemoveFile => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_remove_file(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::ProjectMoveOrphanFiles => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match workspace::handle_move_orphan_files(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::IngestFile => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match ingest::handle_ingest_file(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::IngestCancel => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match ingest::handle_ingest_cancel(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::IngestVip => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match ingest::handle_ingest_vip(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::IngestGetQueue => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    match ingest::handle_get_ingest_queue(&state, payload).await {
                        Ok(res) => resolver.resolve(res),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }

            CommandAction::EngineGetStatus => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    let pool = state.get_duckdb_pool().await;
                    resolver.resolve(json!({
                        "engine": "duckdb-1.2.2",
                        "status": "READY",
                        "activeConnections": pool.active_connections()
                    }));
                });
                true
            }

            CommandAction::EngineDoRestart => {
                let resolver = invoke.resolver;
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    let pool = state.get_duckdb_pool().await;
                    match pool.restart().await {
                        Ok(()) => resolver.resolve(json!({ "restarted": true, "engine": "duckdb-1.2.2" })),
                        Err(e) => resolver.reject(e),
                    }
                });
                true
            }
        }
    }
}
