//! query.rs
//!
//! Query and Engine Command Handlers for Pulex Desktop (Tauri v2)
//! Bridges frontend Reltab and Viewer IPC commands to the native DuckDB engine,
//! Binary Guard, Streaming Transcoder, Derived View Registry, and Arrow IPC streamer.

use crate::engine::query_compiler::{QueryExpCompiler, QueryRep};
use crate::engine::stats_cache::{ColumnStatsDto, ColumnStatsMap};
use crate::ingest::binary_guard::{inspect_file, FileFormatCategory};
use crate::ingest::csv_streamer::{
    effective_extension, ingest_csv_file_with_options, CsvIngestOptions,
};
use crate::ingest::dirty_log::ingest_dirty_log_duckdb;
use crate::ingest::evtx_parser::ingest_evtx_file;
use crate::ingest::jsonl_streamer::ingest_jsonl_file_with_options;
use crate::ingest::scheduler::CancellationToken;
use crate::ledger::parquet_ledger::{build_cache_parquet_name, compute_cache_hash};
use crate::state::{AppState, DerivedViewSpec};
use duckdb::types::ValueRef;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tauri::ipc::Response;
use tauri::AppHandle;

/// Evaluates a QueryExp AST and streams the results as an Apache Arrow IPC Stream (0xFFFFFFFF marker)
pub async fn handle_eval_query(
    _app: AppHandle,
    state: &AppState,
    payload: Value,
) -> Result<Response, String> {
    let (query_rep, offset, limit) = extract_query_and_pagination(&payload)?;
    ensure_referenced_view_exists(state, &query_rep).await;

    let sql = QueryExpCompiler::compile(&query_rep, offset, limit)
        .map_err(|e| format!("QueryExp AST compilation error: {}", e))?;

    tracing::debug!("Compiled DuckDB SQL: {}", sql);

    let pool = state.get_duckdb_pool().await;
    pool.eval_query_arrow_cancellable(sql, 30_000).await
}

/// Computes the exact row count for a QueryExp or table
pub async fn handle_row_count(
    _app: AppHandle,
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    row_count_with_state(state, payload).await
}

/// Locate a live cached Parquet file in `portable_dir/temp/**` matching `tbl_<8hex>` suffix.
fn find_live_parquet_for_view(state: &AppState, table_name: &str) -> Option<PathBuf> {
    let hash_suffix = table_name.strip_prefix("tbl_")?;
    if hash_suffix.len() != 8 || !hash_suffix.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let expected_ending = format!("_cached_{}.parquet", hash_suffix.to_ascii_lowercase());
    let temp_dir = state.portable_dir.join("temp");
    if !temp_dir.exists() {
        return None;
    }
    if let Ok(entries) = std::fs::read_dir(&temp_dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir()
                && !p
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .starts_with(".trash")
            {
                if let Ok(sub_entries) = std::fs::read_dir(&p) {
                    for sub in sub_entries.flatten() {
                        let sp = sub.path();
                        if sp.is_file() {
                            let fname = sp
                                .file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .to_ascii_lowercase();
                            if fname.ends_with(&expected_ending)
                                && sp.metadata().map(|m| m.len() > 0).unwrap_or(false)
                            {
                                return Some(sp);
                            }
                        }
                    }
                }
            } else if p.is_file() {
                let fname = p
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_ascii_lowercase();
                if fname.ends_with(&expected_ending)
                    && p.metadata().map(|m| m.len() > 0).unwrap_or(false)
                {
                    return Some(p);
                }
            }
        }
    }
    None
}

/// Re-bind a stale DuckDB `tbl_<hash>` view to a live Parquet cache file on disk if found.
async fn try_rebind_stale_table_view(state: &AppState, table_name: &str) -> Option<PathBuf> {
    let live_pq = find_live_parquet_for_view(state, table_name)?;
    let safe_pq = live_pq
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    let pool = state.get_duckdb_pool().await;
    let is_evtx = pool
        .eval_ui_conn({
            let sp = safe_pq.clone();
            move |conn| Ok(is_evtx_parquet_schema(conn, &sp))
        })
        .await
        .unwrap_or(false);
    let source_expr = if is_evtx {
        build_evtx_parquet_projection(&safe_pq)
    } else {
        format!("read_parquet('{}')", safe_pq)
    };
    let escaped_v = table_name.replace('"', "\"\"");
    let ddl = format!(
        "CREATE OR REPLACE VIEW \"{v}\" AS SELECT * FROM {src};",
        v = escaped_v,
        src = source_expr
    );
    if pool
        .eval_ui_conn(move |conn| conn.execute_batch(&ddl).map_err(|e| e.to_string()))
        .await
        .is_ok()
    {
        state
            .register_view_parquet_path(table_name.to_string(), live_pq.clone())
            .await;
        Some(live_pq)
    } else {
        None
    }
}

/// Core row count implementation testable without `AppHandle`.
pub async fn row_count_with_state(state: &AppState, payload: Value) -> Result<Value, String> {
    let (query_rep, _, _) = extract_query_and_pagination(&payload)?;
    ensure_referenced_view_exists(state, &query_rep).await;

    let source_hint = match &query_rep {
        QueryRep::Table { table_name } => {
            if let Some(pq_path) = state.get_view_parquet_path(table_name).await {
                if pq_path.exists() {
                    Some(pq_path.to_string_lossy().to_string())
                } else {
                    state.remove_view_parquet_path(table_name).await;
                    try_rebind_stale_table_view(state, table_name)
                        .await
                        .map(|p| p.to_string_lossy().to_string())
                        .or_else(|| Some(table_name.clone()))
                }
            } else {
                Some(table_name.clone())
            }
        }
        _ => None,
    };

    let count_sql = QueryExpCompiler::compile_count(&query_rep)
        .map_err(|e| format!("QueryExp count compilation error: {}", e))?;

    let pool = state.get_duckdb_pool().await;
    match pool.eval_row_count(count_sql.clone(), source_hint).await {
        Ok(count) => Ok(json!({ "rowCount": count })),
        Err(err) => {
            // Auto-heal stale DuckDB view pointing to a purged project's temp directory
            if err.contains("No files found that match the pattern") {
                if let QueryRep::Table { ref table_name } = query_rep {
                    if let Some(live_pq) = try_rebind_stale_table_view(state, table_name).await {
                        let healed = pool
                            .eval_row_count(
                                count_sql,
                                Some(live_pq.to_string_lossy().to_string()),
                            )
                            .await?;
                        return Ok(json!({ "rowCount": healed }));
                    }
                }
            }
            Err(err)
        }
    }
}

/// Retrieves the reltab.Schema DTO for a table or query
pub async fn handle_schema(
    _app: AppHandle,
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    let target = if let Some(tbl) = payload
        .get("tableName")
        .or_else(|| payload.get("table_name"))
        .and_then(|v| v.as_str())
    {
        if let Some(derived) = state.get_derived_view(tbl).await {
            let pool = state.get_duckdb_pool().await;
            let ddl = derived.sql_ddl.clone();
            let _ = pool
                .eval_ui_conn(move |conn| conn.execute_batch(&ddl).map_err(|e| e.to_string()))
                .await;
        }
        tbl.to_string()
    } else if let Some(sql) = payload
        .get("sql")
        .or_else(|| payload.get("sqlQuery"))
        .and_then(|v| v.as_str())
    {
        sql.to_string()
    } else if let Ok((query_rep, _, _)) = extract_query_and_pagination(&payload) {
        ensure_referenced_view_exists(state, &query_rep).await;
        QueryExpCompiler::compile(&query_rep, None, Some(0))
            .unwrap_or_else(|_| "main_table".to_string())
    } else {
        "main_table".to_string()
    };

    let pool = state.get_duckdb_pool().await;
    let schema_dto = pool.eval_schema(target).await?;
    serde_json::to_value(&schema_dto).map_err(|e| format!("Schema serialization error: {}", e))
}

/// Executes arbitrary SQL and streams Arrow IPC
pub async fn handle_run_sql_arrow(
    _app: AppHandle,
    state: &AppState,
    payload: Value,
) -> Result<Response, String> {
    let sql = payload
        .get("sql")
        .or_else(|| payload.get("sqlQuery"))
        .or_else(|| payload.get("query"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required 'sql' or 'sqlQuery' field in payload".to_string())?;

    tracing::debug!("Executing arbitrary SQL Arrow query: {}", sql);
    let pool = state.get_duckdb_pool().await;
    pool.eval_query_arrow(sql.to_string()).await
}

/// Inspect a cached Parquet file for single-column corruption (`raw_event`, `column0`, unsplit CSV header)
fn is_corrupted_single_column_cache(conn: &duckdb::Connection, parquet_path: &Path, is_delimited: bool) -> bool {
    let pq_str = parquet_path
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    let sql = format!("DESCRIBE SELECT * FROM read_parquet('{}') LIMIT 0;", pq_str);
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mut rows) = stmt.query([]) {
            let mut cols = Vec::new();
            while let Ok(Some(row)) = rows.next() {
                if let Ok(name) = row.get::<_, String>(0) {
                    cols.push(name);
                }
            }
            if cols.len() == 1 && is_delimited {
                let c0 = cols[0].trim();
                if c0.eq_ignore_ascii_case("column0")
                    || c0.eq_ignore_ascii_case("column00")
                    || c0.contains(',')
                    || c0.contains('\t')
                {
                    return true;
                }
            }
        }
    }
    false
}

/// Build an EVTX projection expression that preserves both Toad DFIR PascalCase aliases
/// and Pulex snake_case columns. All identifiers are double-quoted so DuckDB reserved
/// keywords like `binary` are never misparsed as SQL data types.
fn build_evtx_parquet_projection(sanitized_parquet_path: &str) -> String {
    format!(
        "(SELECT \
            \"record_id\" AS \"RecordID\", \
            \"time_created\" AS \"TimeCreated\", \
            \"event_id\" AS \"EventID\", \
            COALESCE(\"level_name\", CAST(\"level\" AS VARCHAR)) AS \"Level\", \
            \"provider_name\" AS \"Provider\", \
            \"channel\" AS \"Channel\", \
            \"computer\" AS \"Computer\", \
            \"user_id\" AS \"UserId\", \
            \"param1\", \
            \"param2\", \
            \"param3\", \
            \"binary\" AS \"Binary\", \
            COALESCE(\"event_data\", \"payload_json\") AS \"EventData\", \
            \"record_id\", \
            \"time_created\", \
            \"event_id\", \
            \"provider_name\", \
            \"level\", \
            \"channel\", \
            \"computer\", \
            \"user_id\", \
            \"payload_json\" \
         FROM read_parquet('{}'))",
        sanitized_parquet_path
    )
}

/// Retrieves the active main table name, dynamically binding the target file to DuckDB views
pub async fn handle_get_table_name(
    _app: AppHandle,
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    get_table_name_with_state(state, payload).await
}

/// Core table-name binding implementation testable without `AppHandle`.
pub async fn get_table_name_with_state(state: &AppState, payload: Value) -> Result<Value, String> {
    let raw_target = payload
        .get("resourceId")
        .or_else(|| payload.get("filePath"))
        .or_else(|| payload.get("file_path"))
        .or_else(|| {
            payload
                .get("path")
                .and_then(|p| p.get("sourceId"))
                .and_then(|s| s.get("resourceId"))
        })
        .or_else(|| {
            payload
                .get("req")
                .and_then(|r| r.get("path"))
                .and_then(|p| p.get("sourceId"))
                .and_then(|s| s.get("resourceId"))
        })
        .or_else(|| payload.get("engine").and_then(|e| e.get("resourceId")))
        .and_then(|v| v.as_str());

    // Optional explicit table/child node inside a multi-table .duckdb or .sqlite file
    let requested_subtable = payload
        .get("tableName")
        .or_else(|| {
            payload
                .get("path")
                .and_then(|p| p.get("path"))
                .and_then(|arr| arr.as_array())
                .and_then(|arr| arr.last())
        })
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty() && *s != ".")
        .map(|s| s.to_string());

    let file_path = match raw_target {
        Some(p) if !p.trim().is_empty() => p.trim(),
        _ => return Ok(json!("main_table")),
    };

    // Check if raw_target is actually a registered derived view name
    if let Some(derived) = state.get_derived_view(file_path).await {
        let pool = state.get_duckdb_pool().await;
        let ddl = derived.sql_ddl.clone();
        let vname = derived.view_name.clone();
        pool.eval_ui_conn(move |conn| conn.execute_batch(&ddl).map_err(|e| e.to_string()))
            .await?;
        state.remove_view_parquet_path(&vname).await;
        return Ok(json!(vname));
    }

    let path_obj = Path::new(file_path);
    if !path_obj.exists() {
        // Offline cache check in ParquetLedger before giving up
        let ledger = state.parquet_ledger.read().await;
        let norm = crate::ledger::parquet_ledger::normalize_path(file_path);
        let st = ledger.state.read().await;
        if let Some(f) = st.files.get(&norm) {
            let pq = Path::new(&f.parquet_path);
            if !f.parquet_path.is_empty() && pq.exists() {
                let hash = compute_cache_hash(file_path, f.file_size.max(0) as u64, f.source_mtime);
                let view_name = format!("tbl_{:08x}", hash);
                let safe_pq = pq.to_string_lossy().replace('\\', "/").replace('\'', "''");
                let ddl = format!(
                    "CREATE OR REPLACE VIEW \"{v}\" AS SELECT * FROM read_parquet('{p}');\nCREATE OR REPLACE VIEW \"main_table\" AS SELECT * FROM \"{v}\";",
                    v = view_name,
                    p = safe_pq
                );
                let pool = state.get_duckdb_pool().await;
                pool.eval_ui_conn(move |conn| conn.execute_batch(&ddl).map_err(|e| e.to_string()))
                    .await?;
                state
                    .register_view_parquet_path(view_name.clone(), pq.to_path_buf())
                    .await;
                state
                    .register_view_parquet_path("main_table".to_string(), pq.to_path_buf())
                    .await;
                return Ok(json!(view_name));
            }
        }
        return Err(format!("Target file does not exist on disk: {}", file_path));
    }

    // 1. Binary Guard inspection when enabled in config.ini
    let cfg = state.get_config().await;
    let format_cat = if cfg.system.binary_guard {
        inspect_file(path_obj).map_err(|e| e.to_string())?
    } else {
        inspect_file(path_obj).unwrap_or(FileFormatCategory::DelimitedText)
    };

    let (size, mtime_ms) = if let Ok(meta) = std::fs::metadata(path_obj) {
        let size = meta.len();
        let mtime_ms = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        (size, mtime_ms)
    } else {
        (0, 0)
    };

    let path_hash = compute_cache_hash(file_path, size, mtime_ms);
    let view_name = format!("tbl_{:08x}", path_hash);
    let cache_name = build_cache_parquet_name(path_obj, file_path, size, mtime_ms);
    let temp_dir = state.portable_dir.join("temp");
    let _ = std::fs::create_dir_all(&temp_dir);

    let ext = effective_extension(path_obj);
    let pool = state.get_duckdb_pool().await;

    // 2. Native multi-table DuckDB / SQLite database container handling
    if format_cat == FileFormatCategory::DuckDb || ext == "duckdb" {
        let alias = format!("db_{:08x}", path_hash);
        let safe_db = file_path.replace('\\', "/").replace('\'', "''");
        let vname = view_name.clone();
        let sub = requested_subtable.clone();
        pool.eval_ui_conn(move |conn| {
            let _ = conn.execute_batch(&format!("ATTACH IF NOT EXISTS '{}' AS \"{}\" (READ_ONLY);", safe_db, alias));
            let target_tbl = if let Some(t) = sub {
                t
            } else {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT table_name FROM information_schema.tables WHERE table_catalog = '{}' AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name LIMIT 1;",
                        alias
                    ))
                    .map_err(|e| e.to_string())?;
                stmt.query_row([], |r| r.get::<_, String>(0))
                    .map_err(|_| "DuckDB database contains no tables".to_string())?
            };
            let q_tbl = target_tbl.replace('"', "\"\"");
            let ddl = format!(
                "CREATE OR REPLACE VIEW \"{v}\" AS SELECT * FROM \"{a}\".\"{t}\";\nCREATE OR REPLACE VIEW \"main_table\" AS SELECT * FROM \"{v}\";",
                v = vname,
                a = alias,
                t = q_tbl
            );
            conn.execute_batch(&ddl).map_err(|e| e.to_string())
        })
        .await?;
        state.remove_view_parquet_path(&view_name).await;
        state.remove_view_parquet_path("main_table").await;
        return Ok(json!(view_name));
    }

    if format_cat == FileFormatCategory::Sqlite || matches!(ext.as_str(), "sqlite" | "sqlite3" | "db") {
        let alias = format!("sq_{:08x}", path_hash);
        let safe_db = file_path.replace('\\', "/").replace('\'', "''");
        let vname = view_name.clone();
        let sub = requested_subtable.clone();
        pool.eval_ui_conn(move |conn| {
            let _ = conn.execute_batch("INSTALL sqlite; LOAD sqlite;");
            conn.execute_batch(&format!(
                "ATTACH IF NOT EXISTS '{}' AS \"{}\" (TYPE SQLITE, READ_ONLY);",
                safe_db, alias
            ))
            .map_err(|e| format!("SQLite attach error (ensure DuckDB sqlite extension is available): {}", e))?;
            let target_tbl = if let Some(t) = sub {
                t
            } else {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT table_name FROM information_schema.tables WHERE table_catalog = '{}' ORDER BY table_name LIMIT 1;",
                        alias
                    ))
                    .map_err(|e| e.to_string())?;
                stmt.query_row([], |r| r.get::<_, String>(0))
                    .map_err(|_| "SQLite database contains no tables".to_string())?
            };
            let q_tbl = target_tbl.replace('"', "\"\"");
            let ddl = format!(
                "CREATE OR REPLACE VIEW \"{v}\" AS SELECT * FROM \"{a}\".\"{t}\";\nCREATE OR REPLACE VIEW \"main_table\" AS SELECT * FROM \"{v}\";",
                v = vname,
                a = alias,
                t = q_tbl
            );
            conn.execute_batch(&ddl).map_err(|e| e.to_string())
        })
        .await?;
        state.remove_view_parquet_path(&view_name).await;
        state.remove_view_parquet_path("main_table").await;
        return Ok(json!(view_name));
    }

    // 3. Locate existing Parquet cache across temp_dir and project subdirectories
    let force_reopen = payload
        .get("forceReopen")
        .or_else(|| payload.get("force_reopen"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let mut cached_parquet_path: Option<PathBuf> = None;
    if temp_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(&temp_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() && !p.file_name().unwrap_or_default().to_string_lossy().starts_with(".trash") {
                    let candidate = p.join(&cache_name);
                    if candidate.exists() && candidate.metadata().map(|m| m.len() > 0).unwrap_or(false) {
                        if force_reopen {
                            let _ = std::fs::remove_file(&candidate);
                        } else {
                            cached_parquet_path = Some(candidate);
                        }
                        break;
                    }
                } else if p.is_file() && p.file_name().map(|n| n == cache_name.as_str()).unwrap_or(false) {
                    if p.metadata().map(|m| m.len() > 0).unwrap_or(false) {
                        if force_reopen {
                            let _ = std::fs::remove_file(&p);
                        } else {
                            cached_parquet_path = Some(p);
                        }
                        break;
                    }
                }
            }
        }
    }

    let is_delimited = matches!(ext.as_str(), "csv" | "tsv" | "tab" | "psv");
    if let Some(ref pq_path) = cached_parquet_path {
        let pq_clone = pq_path.clone();
        let corrupt = pool
            .eval_ui_conn(move |conn| Ok(is_corrupted_single_column_cache(conn, &pq_clone, is_delimited)))
            .await
            .unwrap_or(false);
        if corrupt {
            tracing::warn!("Evicting corrupted single-column Parquet cache {:?}", pq_path);
            let _ = std::fs::remove_file(pq_path);
            cached_parquet_path = None;
        }
    }

    // 4. Foreground conversion to Parquet cache when needed (EVTX, Gzip, UTF-16, Delimited, JSONL, Dirty Log)
    if cached_parquet_path.is_none() && format_cat != FileFormatCategory::Parquet && ext != "parquet" {
        let sched = state.get_ingest_scheduler().await;
        if let Some(in_flight_pq) = sched.wait_if_in_flight(file_path).await? {
            cached_parquet_path = Some(in_flight_pq);
        }
    }

    if cached_parquet_path.is_none() && format_cat != FileFormatCategory::Parquet && ext != "parquet" {
        let sched = state.get_ingest_scheduler().await;
        let dispatcher = sched.dispatcher.clone();
        let active_pid = {
            let ledger = state.parquet_ledger.read().await;
            let (pid, _) = ledger.get_workspace().await;
            if pid.is_empty() {
                "default".to_string()
            } else {
                pid
            }
        };
        let target_dir = temp_dir.join(&active_pid);
        let _ = std::fs::create_dir_all(&target_dir);
        let target_pq = target_dir.join(&cache_name);
        let fid = format!("fg_{:08x}", path_hash);
        let ct = CancellationToken::new();
        let src_buf = path_obj.to_path_buf();
        let dst_buf = target_pq.clone();

        let csv_opts = CsvIngestOptions {
            csv_parallel: cfg.duckdb.csv_parallel.clone(),
            compression: cfg.parquet.compression.clone(),
            row_group_size: cfg.parquet.row_group_size,
        };
        let active_workers = sched
            .get_queue_snapshot()
            .await
            .active_jobs
            .len()
            .max(1);
        let evtx_threads = pool.resolve_worker_threads(active_workers);

        if format_cat == FileFormatCategory::Evtx || ext == "evtx" {
            let _permit = pool.acquire_worker_permit().await?;
            let disp_bg = dispatcher.clone();
            let res = tokio::task::spawn_blocking(move || {
                ingest_evtx_file(&fid, &src_buf, &dst_buf, evtx_threads, &ct, Some(&disp_bg))
            })
            .await
            .map_err(|e| e.to_string())?;
            res?;
            cached_parquet_path = Some(target_pq);
        } else if format_cat == FileFormatCategory::JsonText
            || matches!(ext.as_str(), "json" | "jsonl" | "ndjson")
        {
            let disp_bg = dispatcher.clone();
            let comp = csv_opts.compression.clone();
            let rg = csv_opts.row_group_size;
            pool.run_ingest_task_with_budget(active_workers, move |conn| {
                ingest_jsonl_file_with_options(
                    &fid,
                    &src_buf,
                    &dst_buf,
                    &conn,
                    &comp,
                    rg,
                    &ct,
                    Some(&disp_bg),
                )
                .map(|_| ())
            })
            .await?;
            cached_parquet_path = Some(target_pq);
        } else if ext == "log"
            && format_cat != FileFormatCategory::GzipText
            && format_cat != FileFormatCategory::Utf16Text
        {
            let disp_bg = dispatcher.clone();
            let comp = csv_opts.compression.clone();
            let rg = csv_opts.row_group_size;
            pool.run_ingest_task_with_budget(active_workers, move |conn| {
                ingest_dirty_log_duckdb(
                    &fid,
                    &src_buf,
                    &dst_buf,
                    &conn,
                    &comp,
                    rg,
                    &ct,
                    Some(&disp_bg),
                )
                .map(|_| ())
            })
            .await?;
            cached_parquet_path = Some(target_pq);
        } else {
            // CSV / TSV / TXT / GzipText / Utf16Text -> Parallel-first 3-tier fallback + native .gz
            let disp_bg = dispatcher.clone();
            let opts = csv_opts.clone();
            pool.run_ingest_task_with_budget(active_workers, move |conn| {
                ingest_csv_file_with_options(
                    &fid,
                    &src_buf,
                    &dst_buf,
                    &conn,
                    &opts,
                    &ct,
                    Some(&disp_bg),
                )
                .map(|_| ())
            })
            .await?;
            cached_parquet_path = Some(target_pq);
        }
    }

    let sanitized_target = if let Some(ref pq) = cached_parquet_path {
        pq.to_string_lossy().replace('\\', "/").replace('\'', "''")
    } else {
        file_path.replace('\\', "/").replace('\'', "''")
    };

    let source_expr = if format_cat == FileFormatCategory::Evtx || ext == "evtx" {
        build_evtx_parquet_projection(&sanitized_target)
    } else {
        format!("read_parquet('{}')", sanitized_target)
    };

    let ddl = format!(
        "CREATE OR REPLACE VIEW \"{v}\" AS SELECT * FROM {src};\nCREATE OR REPLACE VIEW \"main_table\" AS SELECT * FROM \"{v}\";",
        v = view_name,
        src = source_expr
    );

    pool.eval_ui_conn(move |conn| {
        conn.execute_batch(&ddl)
            .map_err(|e| format!("Failed to create DuckDB view for file: {}", e))
    })
    .await?;

    let backing_pq = cached_parquet_path.clone().or_else(|| {
        if format_cat == FileFormatCategory::Parquet || ext == "parquet" {
            Some(path_obj.to_path_buf())
        } else {
            None
        }
    });
    if let Some(pq) = backing_pq {
        state
            .register_view_parquet_path(view_name.clone(), pq.clone())
            .await;
        state
            .register_view_parquet_path("main_table".to_string(), pq)
            .await;
    } else {
        state.remove_view_parquet_path(&view_name).await;
        state.remove_view_parquet_path("main_table").await;
    }

    Ok(json!(view_name))
}

/// Re-materialize any persisted derived view referenced by a QueryRep before compilation/execution
async fn ensure_referenced_view_exists(state: &AppState, rep: &QueryRep) {
    if let QueryRep::Table { table_name } = rep {
        if let Some(derived) = state.get_derived_view(table_name).await {
            let pool = state.get_duckdb_pool().await;
            let ddl = derived.sql_ddl;
            let _ = pool
                .eval_ui_conn(move |conn| conn.execute_batch(&ddl).map_err(|e| e.to_string()))
                .await;
        }
    }
}

/// Register a derived SQL view (e.g. Regex Column Split `_rx1`) in memory, disk, and live DuckDB session
pub async fn handle_register_derived_view(
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    let view_name = payload
        .get("viewName")
        .or_else(|| payload.get("view_name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing viewName in view:register_derived".to_string())?
        .to_string();
    let base_table = payload
        .get("baseTable")
        .or_else(|| payload.get("base_table"))
        .or_else(|| payload.get("parentTable"))
        .or_else(|| payload.get("parent_table"))
        .and_then(|v| v.as_str())
        .unwrap_or("main_table")
        .to_string();
    let sql_ddl = payload
        .get("sqlDdl")
        .or_else(|| payload.get("sql_ddl"))
        .or_else(|| payload.get("sql"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing sqlDdl in view:register_derived".to_string())?
        .to_string();
    let source_column = payload
        .get("sourceColumn")
        .or_else(|| payload.get("source_column"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let regex_pattern = payload
        .get("regexPattern")
        .or_else(|| payload.get("regex_pattern"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let pool = state.get_duckdb_pool().await;
    let ddl_exec = sql_ddl.clone();
    pool.eval_ui_conn(move |conn| conn.execute_batch(&ddl_exec).map_err(|e| e.to_string()))
        .await?;

    let spec = DerivedViewSpec {
        view_name: view_name.clone(),
        base_table,
        sql_ddl,
        source_column,
        regex_pattern,
        created_at_ms: crate::ledger::parquet_ledger::current_time_ms(),
    };
    state.register_derived_view(spec.clone()).await?;

    Ok(json!({
        "success": true,
        "viewName": view_name,
        "spec": spec
    }))
}

/// Retrieve metadata for a registered derived view (or list all registered derived views)
pub async fn handle_get_derived_view(state: &AppState, payload: Value) -> Result<Value, String> {
    if let Some(view_name) = payload
        .get("viewName")
        .or_else(|| payload.get("view_name"))
        .and_then(|v| v.as_str())
    {
        if let Some(spec) = state.get_derived_view(view_name).await {
            return serde_json::to_value(&spec).map_err(|e| e.to_string());
        }
        return Ok(Value::Null);
    }
    let map = state.derived_views.read().await;
    serde_json::to_value(&*map).map_err(|e| e.to_string())
}

fn extract_query_and_pagination(
    payload: &Value,
) -> Result<(QueryRep, Option<i64>, Option<i64>), String> {
    let offset = payload.get("offset").and_then(|v| v.as_i64());
    let limit = payload.get("limit").and_then(|v| v.as_i64());

    let query_val = payload
        .get("queryExp")
        .or_else(|| payload.get("query"))
        .or_else(|| payload.get("queryStr"))
        .or_else(|| payload.get("req"))
        .unwrap_or(payload);

    let query_rep = QueryExpCompiler::parse_query_exp(query_val)?;
    Ok((query_rep, offset, limit))
}

/// Retrieves the root node metadata for a data source connection
pub async fn handle_get_root_node(
    _app: AppHandle,
    _state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    let resource_id = payload
        .get("resourceId")
        .or_else(|| payload.get("filePath"))
        .or_else(|| payload.get("file_path"))
        .or_else(|| {
            payload
                .get("path")
                .and_then(|p| p.get("sourceId"))
                .and_then(|s| s.get("resourceId"))
        })
        .or_else(|| {
            payload
                .get("req")
                .and_then(|r| r.get("path"))
                .and_then(|p| p.get("sourceId"))
                .and_then(|s| s.get("resourceId"))
        })
        .or_else(|| payload.get("engine").and_then(|e| e.get("resourceId")))
        .and_then(|v| v.as_str())
        .unwrap_or(".");

    let path = Path::new(resource_id);
    let display_name = path
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| resource_id.to_string());

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let is_db_container = matches!(ext.as_str(), "duckdb" | "sqlite" | "sqlite3" | "db");
    let is_dir = path.is_dir() || is_db_container;
    let kind = if path.is_dir() {
        "Directory"
    } else if is_db_container {
        "Database"
    } else {
        "File"
    };
    let size = path.metadata().ok().map(|m| m.len());

    Ok(json!({
        "id": ".",
        "kind": kind,
        "displayName": display_name,
        "isContainer": is_dir,
        "fileSizeBytes": size
    }))
}

/// Retrieves child nodes for a directory or multi-table database container (.duckdb / .sqlite)
pub async fn handle_get_children(
    _app: AppHandle,
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    let resource_id = payload
        .get("resourceId")
        .or_else(|| payload.get("filePath"))
        .or_else(|| payload.get("file_path"))
        .or_else(|| {
            payload
                .get("path")
                .and_then(|p| p.get("sourceId"))
                .and_then(|s| s.get("resourceId"))
        })
        .or_else(|| payload.get("engine").and_then(|e| e.get("resourceId")))
        .and_then(|v| v.as_str())
        .unwrap_or(".");

    let path = Path::new(resource_id);
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    if path.is_file() && matches!(ext.as_str(), "duckdb" | "sqlite" | "sqlite3" | "db") {
        let pool = state.get_duckdb_pool().await;
        let safe_db = resource_id.replace('\\', "/").replace('\'', "''");
        let is_sqlite = ext != "duckdb";
        let hash = compute_cache_hash(resource_id, 0, 0);
        let alias = format!("cat_{:08x}", hash);
        let tables = pool
            .eval_ui_conn(move |conn| {
                if is_sqlite {
                    let _ = conn.execute_batch("INSTALL sqlite; LOAD sqlite;");
                    let _ = conn.execute_batch(&format!(
                        "ATTACH IF NOT EXISTS '{}' AS \"{}\" (TYPE SQLITE, READ_ONLY);",
                        safe_db, alias
                    ));
                } else {
                    let _ = conn.execute_batch(&format!(
                        "ATTACH IF NOT EXISTS '{}' AS \"{}\" (READ_ONLY);",
                        safe_db, alias
                    ));
                }
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT table_name FROM information_schema.tables WHERE table_catalog = '{}' ORDER BY table_name;",
                        alias
                    ))
                    .map_err(|e| e.to_string())?;
                let mut out = Vec::new();
                let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
                while let Ok(Some(r)) = rows.next() {
                    if let Ok(name) = r.get::<_, String>(0) {
                        out.push(json!({
                            "id": name,
                            "kind": "Table",
                            "displayName": name,
                            "isContainer": false
                        }));
                    }
                }
                Ok(out)
            })
            .await
            .unwrap_or_default();
        return Ok(json!(tables));
    }

    if path.is_dir() {
        let mut children = Vec::new();
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                let name = p.file_name().unwrap_or_default().to_string_lossy().to_string();
                let is_entry_dir = p.is_dir();
                let kind = if is_entry_dir { "Directory" } else { "File" };
                let size = entry.metadata().ok().map(|m| m.len());
                children.push(json!({
                    "id": name,
                    "kind": kind,
                    "displayName": name,
                    "isContainer": is_entry_dir,
                    "fileSizeBytes": size
                }));
            }
        }
        Ok(json!(children))
    } else {
        Ok(json!([]))
    }
}

const MAX_SUMMARIZE_COLUMNS: usize = 100;

#[derive(Clone)]
enum WorkerViewSource {
    Parquet {
        view_name: String,
        parquet_path: PathBuf,
    },
    DerivedDdl {
        sql_ddl: String,
    },
}

fn collect_referenced_tables(rep: &QueryRep, out: &mut Vec<String>) {
    match rep {
        QueryRep::Table { table_name } => {
            if !out.contains(table_name) {
                out.push(table_name.clone());
            }
        }
        QueryRep::Sql { .. } => {}
        QueryRep::Project { from, .. }
        | QueryRep::Filter { from, .. }
        | QueryRep::GroupBy { from, .. }
        | QueryRep::Sort { from, .. }
        | QueryRep::Extend { from, .. }
        | QueryRep::MapColumns { from, .. }
        | QueryRep::MapColumnsByIndex { from, .. } => {
            collect_referenced_tables(from, out);
        }
        QueryRep::Join { lhs, rhs, .. } => {
            collect_referenced_tables(lhs, out);
            collect_referenced_tables(rhs, out);
        }
        QueryRep::Concat { from, target } => {
            collect_referenced_tables(from, out);
            collect_referenced_tables(target, out);
        }
    }
}

fn resolve_worker_view_source_rec<'a>(
    state: &'a AppState,
    name: &'a str,
    visited: &'a mut std::collections::HashSet<String>,
    visiting: &'a mut std::collections::HashSet<String>,
    ordered: &'a mut Vec<WorkerViewSource>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
    Box::pin(async move {
        if visited.contains(name) {
            return true;
        }
        if !visiting.insert(name.to_string()) {
            return false;
        }

        let ok = if let Some(derived) = state.get_derived_view(name).await {
            let base_ok = resolve_worker_view_source_rec(
                state,
                &derived.base_table,
                visited,
                visiting,
                ordered,
            )
            .await;
            if base_ok {
                ordered.push(WorkerViewSource::DerivedDdl {
                    sql_ddl: derived.sql_ddl,
                });
                true
            } else {
                false
            }
        } else if let Some(pq_path) = state.get_view_parquet_path(name).await {
            if pq_path.exists() {
                ordered.push(WorkerViewSource::Parquet {
                    view_name: name.to_string(),
                    parquet_path: pq_path,
                });
                true
            } else {
                false
            }
        } else {
            false
        };

        visiting.remove(name);
        if ok {
            visited.insert(name.to_string());
        }
        ok
    })
}

async fn resolve_worker_view_sources(
    state: &AppState,
    tables: &[String],
) -> Option<Vec<WorkerViewSource>> {
    if tables.is_empty() {
        return None;
    }

    let mut visited = std::collections::HashSet::new();
    let mut visiting = std::collections::HashSet::new();
    let mut ordered = Vec::new();

    for tbl in tables {
        if !resolve_worker_view_source_rec(state, tbl, &mut visited, &mut visiting, &mut ordered)
            .await
        {
            return None;
        }
    }

    Some(ordered)
}

fn is_evtx_parquet_schema(conn: &duckdb::Connection, safe_pq: &str) -> bool {
    let sql = format!("DESCRIBE SELECT * FROM read_parquet('{}') LIMIT 0;", safe_pq);
    let mut cols = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mut rows) = stmt.query([]) {
            while let Ok(Some(row)) = rows.next() {
                if let Ok(name) = row.get::<_, String>(0) {
                    cols.insert(name);
                }
            }
        }
    }
    cols.contains("record_id")
        && cols.contains("level_name")
        && cols.contains("event_data")
        && cols.contains("binary")
}

fn execute_summarize_on_conn(
    conn: &duckdb::Connection,
    sum_sql: &str,
) -> Result<(ColumnStatsMap, Option<i64>), String> {
    let mut stmt = conn.prepare(sum_sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut stats_map = ColumnStatsMap::new();
    let mut row_count_opt: Option<i64> = None;

    while let Ok(Some(row)) = rows.next() {
        let col_name: String = row.get(0).unwrap_or_default();
        let col_type: String =
            row.get::<usize, String>(1).unwrap_or_default().to_uppercase();
        let min_s: Option<String> = row.get(2).ok();
        let max_s: Option<String> = row.get(3).ok();
        let approx_unique: Option<i64> = row.get(4).ok();
        let count: i64 = row.get(9).unwrap_or(0);
        let null_pct_raw: Option<f64> = row
            .get::<_, f64>(10)
            .ok()
            .or_else(|| {
                row.get::<_, String>(10)
                    .ok()
                    .and_then(|s| s.trim_end_matches('%').parse::<f64>().ok())
            });

        if row_count_opt.is_none() {
            row_count_opt = Some(count);
        }
        let pct_null = null_pct_raw.map(|p| p / 100.0);
        let min_num = min_s.as_deref().and_then(|s| s.parse::<f64>().ok());
        let max_num = max_s.as_deref().and_then(|s| s.parse::<f64>().ok());

        let is_num = col_type.contains("INT")
            || col_type.contains("DOUBLE")
            || col_type.contains("FLOAT")
            || col_type.contains("DECIMAL")
            || col_type.contains("REAL");

        let dto = if is_num {
            ColumnStatsDto::Numeric {
                min: min_num,
                max: max_num,
                approx_unique,
                count,
                pct_null,
            }
        } else {
            ColumnStatsDto::Text {
                min: min_num,
                max: max_num,
                approx_unique,
                count,
                pct_null,
            }
        };
        stats_map.insert(col_name, dto);
    }
    Ok((stats_map, row_count_opt))
}

/// Computes and caches column statistics using DuckDB `SUMMARIZE` when `StatsCache` misses.
/// Isolates `SUMMARIZE` onto a worker connection for parquet-backed views and caps wide tables
/// to at most `MAX_SUMMARIZE_COLUMNS` (100) columns unless explicit `columns` are requested.
pub async fn handle_get_column_stats_map(
    _app: AppHandle,
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    get_column_stats_map_with_state(state, payload).await
}

/// Core column stats implementation testable without `AppHandle`.
pub async fn get_column_stats_map_with_state(
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    let pool = state.get_duckdb_pool().await;
    if let Ok((query_rep, _, _)) = extract_query_and_pagination(&payload) {
        ensure_referenced_view_exists(state, &query_rep).await;

        let requested_columns: Option<Vec<String>> = payload
            .get("columns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<String>>()
            })
            .filter(|cols| !cols.is_empty());

        let base_cache_key = match &query_rep {
            QueryRep::Table { table_name } => table_name.clone(),
            other => QueryExpCompiler::compile(other, None, None).unwrap_or_default(),
        };

        if !base_cache_key.is_empty() {
            let cache_key = if let Some(ref cols) = requested_columns {
                let cols_json = serde_json::to_string(cols).unwrap_or_default();
                format!("{}::cols:{}", base_cache_key, cols_json)
            } else {
                base_cache_key
            };

            if let Some(stats) = pool.stats_cache().get(&cache_key).await {
                return serde_json::to_value(&stats).map_err(|e| e.to_string());
            }

            if let Ok(base_sql) = QueryExpCompiler::compile(&query_rep, None, None) {
                let schema_target = match &query_rep {
                    QueryRep::Table { table_name } => table_name.clone(),
                    _ => base_sql.clone(),
                };
                let schema_dto = pool.eval_schema(schema_target).await.unwrap_or_else(|_| {
                    crate::engine::schema_mapper::SchemaDto {
                        dialect: "duckdb".to_string(),
                        columns: Vec::new(),
                        column_metadata: std::collections::HashMap::new(),
                        column_types: std::collections::HashMap::new(),
                    }
                });

                let target_cols: Vec<String> = if let Some(ref explicit) = requested_columns {
                    if schema_dto.columns.is_empty() {
                        explicit.clone()
                    } else {
                        let valid: std::collections::HashSet<&str> =
                            schema_dto.columns.iter().map(|s| s.as_str()).collect();
                        explicit
                            .iter()
                            .filter(|c| valid.contains(c.as_str()))
                            .cloned()
                            .collect()
                    }
                } else if schema_dto.columns.len() > MAX_SUMMARIZE_COLUMNS {
                    schema_dto.columns[..MAX_SUMMARIZE_COLUMNS].to_vec()
                } else {
                    schema_dto.columns.clone()
                };

                if requested_columns.is_some()
                    && !schema_dto.columns.is_empty()
                    && target_cols.is_empty()
                {
                    return Ok(json!({}));
                }

                let select_cols = if target_cols.is_empty() {
                    "*".to_string()
                } else {
                    target_cols
                        .iter()
                        .map(|c| QueryExpCompiler::quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", ")
                };

                let sum_sql =
                    format!("SUMMARIZE SELECT {} FROM ({}) AS _sum_src;", select_cols, base_sql);

                let mut referenced_tables = Vec::new();
                collect_referenced_tables(&query_rep, &mut referenced_tables);
                let worker_sources = resolve_worker_view_sources(state, &referenced_tables).await;

                let computed_res = if let Some(sources) = worker_sources {
                    let sum_sql_worker = sum_sql.clone();
                    pool.run_read_worker_task(move |conn| {
                        for src in sources {
                            match src {
                                WorkerViewSource::Parquet {
                                    view_name,
                                    parquet_path,
                                } => {
                                    let safe_pq = parquet_path
                                        .to_string_lossy()
                                        .replace('\\', "/")
                                        .replace('\'', "''");
                                    let src_expr = if is_evtx_parquet_schema(conn, &safe_pq) {
                                        build_evtx_parquet_projection(&safe_pq)
                                    } else {
                                        format!("read_parquet('{}')", safe_pq)
                                    };
                                    let ddl = format!(
                                        "CREATE OR REPLACE VIEW {} AS SELECT * FROM {};",
                                        QueryExpCompiler::quote_ident(&view_name),
                                        src_expr
                                    );
                                    conn.execute_batch(&ddl).map_err(|e| e.to_string())?;
                                }
                                WorkerViewSource::DerivedDdl { sql_ddl } => {
                                    conn.execute_batch(&sql_ddl).map_err(|e| e.to_string())?;
                                }
                            }
                        }
                        execute_summarize_on_conn(conn, &sum_sql_worker)
                    })
                    .await
                } else {
                    // Fallback to ui_conn only for attached .duckdb / .sqlite or raw SQL without parquet backing
                    pool.eval_ui_conn(move |conn| execute_summarize_on_conn(conn, &sum_sql))
                        .await
                };

                if let Ok((stats_map, rc)) = computed_res {
                    pool.stats_cache()
                        .insert(cache_key, stats_map.clone(), rc)
                        .await;
                    return serde_json::to_value(&stats_map).map_err(|e| e.to_string());
                }
            }
        }
    }
    Ok(json!({}))
}

fn duckdb_value_ref_to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => json!(b),
        ValueRef::TinyInt(n) => json!(n),
        ValueRef::SmallInt(n) => json!(n),
        ValueRef::Int(n) => json!(n),
        ValueRef::BigInt(n) => json!(n),
        ValueRef::HugeInt(n) => json!(n.to_string()),
        ValueRef::UTinyInt(n) => json!(n),
        ValueRef::USmallInt(n) => json!(n),
        ValueRef::UInt(n) => json!(n),
        ValueRef::UBigInt(n) => json!(n),
        ValueRef::Float(f) => json!(f),
        ValueRef::Double(f) => json!(f),
        ValueRef::Decimal(d) => json!(d.to_string()),
        ValueRef::Timestamp(_, micros) => json!(micros),
        ValueRef::Text(bytes) => json!(String::from_utf8_lossy(bytes).to_string()),
        ValueRef::Blob(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            json!(format!("0x{}", hex))
        }
        ValueRef::Date32(days) => json!(days),
        ValueRef::Time64(_, t) => json!(t),
        other => json!(format!("{:?}", other)),
    }
}

fn is_ddl_or_command_sql(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let first_word = trimmed
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        first_word.as_str(),
        "CREATE"
            | "DROP"
            | "ALTER"
            | "ATTACH"
            | "DETACH"
            | "USE"
            | "SET"
            | "RESET"
            | "LOAD"
            | "INSTALL"
            | "COPY"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "TRUNCATE"
            | "BEGIN"
            | "COMMIT"
            | "ROLLBACK"
            | "CHECKPOINT"
    )
}

/// Core synchronous executor for `toad:run_sql` on a DuckDB connection.
/// Executes DDL statements via `execute_batch` and always executes `stmt.query([])`
/// BEFORE inspecting `column_names()` (DuckDB 1.2.2 panics if `column_names()` is called before execution).
fn execute_run_sql_on_conn(conn: &duckdb::Connection, sql_str: &str) -> Result<Value, String> {
    if is_ddl_or_command_sql(sql_str) {
        conn.execute_batch(sql_str).map_err(|e| e.to_string())?;
        return Ok(Value::Array(Vec::new()));
    }

    let mut stmt = conn.prepare(sql_str).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let col_names: Vec<String> = rows
        .as_ref()
        .map(|s| {
            let count = s.column_count();
            if count > 0 {
                s.column_names().into_iter().map(|c| c.to_string()).collect()
            } else {
                Vec::new()
            }
        })
        .unwrap_or_default();

    let mut results = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let mut row_map = serde_json::Map::new();
        for (i, col_name) in col_names.iter().enumerate() {
            let val = match row.get_ref(i) {
                Ok(vref) => duckdb_value_ref_to_json(vref),
                Err(_) => Value::Null,
            };
            row_map.insert(col_name.clone(), val);
        }
        results.push(Value::Object(row_map));
    }
    Ok(Value::Array(results))
}

/// Executes arbitrary SQL query and returns rows as JSON array of objects (supporting all DuckDB types)
pub async fn handle_run_sql(
    _app: AppHandle,
    state: &AppState,
    payload: Value,
) -> Result<Value, String> {
    let sql = payload
        .get("sql")
        .or_else(|| payload.get("sqlQuery"))
        .or_else(|| payload.get("query"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required 'sql' or 'sqlQuery' field in payload".to_string())?;

    let pool = state.get_duckdb_pool().await;
    let sql_str = sql.to_string();
    pool.eval_ui_conn(move |conn| execute_run_sql_on_conn(conn, &sql_str))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evtx_parquet_projection_quotes_reserved_binary_keyword() {
        let tmp_dir = std::env::temp_dir().join(format!("pulex_evtx_proj_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp_dir);
        let pq_path = tmp_dir.join("evtx_sample.parquet");
        let pq_str = pq_path.to_string_lossy().replace('\\', "/").replace('\'', "''");

        let conn = duckdb::Connection::open_in_memory().expect("open duckdb");
        conn.execute_batch(&format!(
            "COPY (SELECT \
                1::BIGINT AS record_id, \
                TIMESTAMP '2026-11-19 10:00:00' AS time_created, \
                4624::INTEGER AS event_id, \
                'Microsoft-Windows-Security-Auditing'::VARCHAR AS provider_name, \
                4::INTEGER AS level, \
                'Security'::VARCHAR AS channel, \
                'HOST01'::VARCHAR AS computer, \
                'S-1-5-18'::VARCHAR AS user_id, \
                '{{}}'::VARCHAR AS payload_json, \
                'Information'::VARCHAR AS level_name, \
                'p1'::VARCHAR AS param1, \
                'p2'::VARCHAR AS param2, \
                'p3'::VARCHAR AS param3, \
                '00FF'::VARCHAR AS \"binary\", \
                '{{}}'::VARCHAR AS event_data, \
                NULL::VARCHAR AS raw_event\
            ) TO '{}' (FORMAT PARQUET);",
            pq_str
        ))
        .expect("write test evtx parquet");

        let proj = build_evtx_parquet_projection(&pq_str);
        assert!(proj.contains("\"binary\" AS \"Binary\""));

        let ddl = format!("CREATE OR REPLACE VIEW \"tbl_evtx_test\" AS SELECT * FROM {};", proj);
        conn.execute_batch(&ddl)
            .expect("EVTX projection view creation must succeed with quoted binary column");

        let binary_val: String = conn
            .query_row("SELECT \"Binary\" FROM \"tbl_evtx_test\" LIMIT 1", [], |r| r.get(0))
            .expect("query Binary column");
        assert_eq!(binary_val, "00FF");

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_execute_run_sql_regex_preflight_and_create_view_no_panic() {
        let conn = duckdb::Connection::open_in_memory().expect("open duckdb");
        conn.execute_batch(
            "CREATE TABLE \"tbl_cert\" (URL VARCHAR); \
             INSERT INTO \"tbl_cert\" VALUES ('https://evil.example/a.exe'), ('no-match');",
        )
        .expect("seed table");

        // 1. Preflight SELECT with regexp_extract (exact pattern sent by applyColumnExtraction)
        let preflight_sql = "SELECT 1 FROM (SELECT *, regexp_extract(\"URL\", 'https?://([^/]+)/(.*)', 1) AS \"host\" FROM \"tbl_cert\") _sub WHERE (\"host\" IS NULL OR \"host\" = '') LIMIT 1";
        let preflight_res = execute_run_sql_on_conn(&conn, preflight_sql)
            .expect("preflight SELECT must succeed without DuckDB panic");
        let arr = preflight_res.as_array().expect("JSON array");
        assert_eq!(arr.len(), 1);

        // 2. DDL CREATE OR REPLACE VIEW
        let create_view_sql = "CREATE OR REPLACE VIEW \"tbl_cert_rx1\" AS SELECT *, regexp_extract(\"URL\", 'https?://([^/]+)/(.*)', 1) AS \"host\", regexp_extract(\"URL\", 'https?://([^/]+)/(.*)', 2) AS \"path\" FROM \"tbl_cert\"";
        let ddl_res = execute_run_sql_on_conn(&conn, create_view_sql)
            .expect("CREATE OR REPLACE VIEW must succeed without panic");
        assert_eq!(ddl_res, Value::Array(Vec::new()));

        // 3. Query newly created derived view
        let query_res = execute_run_sql_on_conn(
            &conn,
            "SELECT \"host\", \"path\" FROM \"tbl_cert_rx1\" WHERE \"host\" != ''",
        )
        .expect("SELECT from derived view must succeed");
        let rows = query_res.as_array().expect("rows array");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["host"], json!("evil.example"));
        assert_eq!(rows[0]["path"], json!("a.exe"));
    }

    #[tokio::test]
    async fn test_fast_counter_via_view_parquet_paths_matches_duckdb_count() {
        let tmp_dir =
            std::env::temp_dir().join(format!("pulex_m2_fastcount_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir).expect("create temp dir");

        let pq_path = tmp_dir.join("sample_events.parquet");
        let pq_str = pq_path.to_string_lossy().replace('\\', "/").replace('\'', "''");
        {
            let conn = duckdb::Connection::open_in_memory().expect("open duckdb");
            conn.execute_batch(&format!(
                "COPY (SELECT range AS id, 'evt_' || range::VARCHAR AS msg FROM range(125)) TO '{}' (FORMAT PARQUET);",
                pq_str
            ))
            .expect("write sample parquet");
        }

        let state = AppState::new(tmp_dir.clone(), crate::portable::ToadConfig::default());
        let tbl_val = get_table_name_with_state(
            &state,
            json!({ "filePath": pq_path.to_string_lossy().to_string() }),
        )
        .await
        .expect("get_table_name_with_state should succeed");
        let view_name = tbl_val
            .as_str()
            .expect("view name string")
            .to_string();

        assert_eq!(
            state.get_view_parquet_path(&view_name).await,
            Some(pq_path.clone())
        );
        assert_eq!(
            state.get_view_parquet_path("main_table").await,
            Some(pq_path.clone())
        );

        let count_val = row_count_with_state(
            &state,
            json!({
                "query": {
                    "operator": "table",
                    "tableName": view_name
                }
            }),
        )
        .await
        .expect("row_count_with_state should succeed");
        assert_eq!(count_val["rowCount"], json!(125));

        // Prove FastCounter path is used via view_parquet_paths even without a DuckDB view
        state
            .register_view_parquet_path("tbl_fast_only".to_string(), pq_path.clone())
            .await;
        let fast_only_val = row_count_with_state(
            &state,
            json!({
                "query": {
                    "operator": "table",
                    "tableName": "tbl_fast_only"
                }
            }),
        )
        .await
        .expect("FastCounter resolution via view_parquet_paths should succeed without view");
        assert_eq!(fast_only_val["rowCount"], json!(125));

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[tokio::test]
    async fn test_summarize_caps_wide_table_at_100_columns_and_supports_explicit_columns() {
        let tmp_dir =
            std::env::temp_dir().join(format!("pulex_m2_summarize_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir).expect("create temp dir");

        let pq_path = tmp_dir.join("wide_120_cols.parquet");
        let pq_str = pq_path.to_string_lossy().replace('\\', "/").replace('\'', "''");
        let col_exprs: Vec<String> = (0..120)
            .map(|i| format!("(range + {})::BIGINT AS \"c{}\"", i, i))
            .collect();
        {
            let conn = duckdb::Connection::open_in_memory().expect("open duckdb");
            conn.execute_batch(&format!(
                "COPY (SELECT {} FROM range(10)) TO '{}' (FORMAT PARQUET);",
                col_exprs.join(", "),
                pq_str
            ))
            .expect("write 120-column parquet");
        }

        let state = AppState::new(tmp_dir.clone(), crate::portable::ToadConfig::default());
        let tbl_val = get_table_name_with_state(
            &state,
            json!({ "filePath": pq_path.to_string_lossy().to_string() }),
        )
        .await
        .expect("get_table_name_with_state should succeed");
        let view_name = tbl_val.as_str().expect("view name string").to_string();

        // 1. Default SUMMARIZE without explicit columns caps at 100 columns
        let default_stats = get_column_stats_map_with_state(
            &state,
            json!({
                "query": {
                    "operator": "table",
                    "tableName": view_name
                }
            }),
        )
        .await
        .expect("default SUMMARIZE should succeed");
        let default_obj = default_stats.as_object().expect("stats JSON object");
        assert_eq!(
            default_obj.len(),
            100,
            "Default SUMMARIZE on 120-col table must cap at 100 columns"
        );
        assert!(default_obj.contains_key("c0"));
        assert!(default_obj.contains_key("c99"));
        assert!(!default_obj.contains_key("c100"));

        // 2. Explicit columns request bypasses the first-100 slice and avoids cache collision
        let explicit_stats = get_column_stats_map_with_state(
            &state,
            json!({
                "query": {
                    "operator": "table",
                    "tableName": view_name
                },
                "columns": ["c105", "c119"]
            }),
        )
        .await
        .expect("explicit-columns SUMMARIZE should succeed");
        let explicit_obj = explicit_stats
            .as_object()
            .expect("explicit stats JSON object");
        assert_eq!(explicit_obj.len(), 2);
        assert!(explicit_obj.contains_key("c105"));
        assert!(explicit_obj.contains_key("c119"));

        // 3. Re-querying default still returns the cached 100-column entry
        let cached_default = get_column_stats_map_with_state(
            &state,
            json!({
                "query": {
                    "operator": "table",
                    "tableName": view_name
                }
            }),
        )
        .await
        .expect("cached default SUMMARIZE should succeed");
        assert_eq!(cached_default.as_object().unwrap().len(), 100);

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[tokio::test]
    async fn test_row_count_auto_heals_stale_view_after_project_cache_move() {
        let tmp_dir = std::env::temp_dir().join(format!(
            "pulex_stale_view_heal_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp_dir);
        let state = AppState::new(tmp_dir.clone(), crate::portable::ToadConfig::default());
        let old_dir = tmp_dir.join("temp").join("rp2i6");
        let new_dir = tmp_dir.join("temp").join("rptme");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        let old_pq = old_dir.join("test_all_cached_17f24271.parquet");
        let new_pq = new_dir.join("test_all_cached_17f24271.parquet");

        // Write a valid 7-row Parquet file in the NEW project directory
        let new_pq_str = new_pq.to_string_lossy().replace('\\', "/").replace('\'', "''");
        let pool = state.get_duckdb_pool().await;
        pool.eval_ui_conn({
            let dst = new_pq_str.clone();
            move |conn| {
                conn.execute_batch(&format!(
                    "COPY (SELECT range AS id FROM range(7)) TO '{}' (FORMAT PARQUET);",
                    dst
                ))
                .map_err(|e| e.to_string())
            }
        })
        .await
        .expect("write live parquet");

        // Seed old_pq so DuckDB allows creating the view, then remove old_dir to simulate project deletion
        std::fs::copy(&new_pq, &old_pq).expect("seed old parquet");
        let old_pq_str = old_pq.to_string_lossy().replace('\\', "/").replace('\'', "''");
        pool.eval_ui_conn(move |conn| {
            conn.execute_batch(&format!(
                "CREATE OR REPLACE VIEW \"tbl_17f24271\" AS SELECT * FROM read_parquet('{}');",
                old_pq_str
            ))
            .map_err(|e| e.to_string())
        })
        .await
        .expect("create stale view");
        state
            .register_view_parquet_path("tbl_17f24271".to_string(), old_pq.clone())
            .await;
        std::fs::remove_dir_all(&old_dir).expect("delete old project cache dir");

        // row_count_with_state must detect the missing old file, auto-rebind to temp/rptme/, and return 7
        let res = row_count_with_state(
            &state,
            json!({
                "query": {
                    "operator": "table",
                    "tableName": "tbl_17f24271"
                }
            }),
        )
        .await
        .expect("row_count_with_state must self-heal stale view");
        assert_eq!(res["rowCount"], json!(7));
        assert_eq!(
            state.get_view_parquet_path("tbl_17f24271").await,
            Some(new_pq)
        );

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
