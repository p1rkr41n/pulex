//! duckdb_pool.rs
//!
//! In-Process Tokio Async DuckDB Connection Pool with Dual-Engine Driver Isolation:
//! - Dedicated UI connection for sub-millisecond query evaluation.
//! - Ingestion worker pool for heavy background imports (CSV, EVTX, JSONL, Parquet) with bounded memory.
//! - Thread-safe access via tokio::sync::Mutex and tokio::task::spawn_blocking.

use duckdb::{AccessMode, Config, Connection, Result as DuckResult};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tauri::ipc::Response;
use tokio::sync::{Mutex, Semaphore};

use crate::engine::arrow_encoder::ArrowIpcEncoder;
use crate::engine::fast_count::FastCounter;
use crate::engine::schema_mapper::{SchemaDto, SchemaMapper};
use crate::engine::stats_cache::StatsCache;
use crate::ingest::io_monitor::get_total_system_ram_bytes;

fn resolve_ui_max_memory(configured: &str) -> Option<String> {
    let trimmed = configured.trim();
    if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("auto") {
        return Some(trimmed.to_string());
    }
    None
}

/// Resolve optimal DuckDB worker thread count based on `background_threads` config,
/// host logical core count, and active concurrent worker count.
pub fn compute_worker_threads(
    background_threads: &str,
    logical_cores: usize,
    active_workers: usize,
) -> usize {
    let cores = logical_cores.max(1);
    let trimmed = background_threads.trim();
    if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("auto") {
        if let Ok(explicit) = trimmed.parse::<usize>() {
            if explicit > 0 {
                return explicit.clamp(1, cores);
            }
        }
    }
    let available_pool = cores.saturating_sub(2).max(2).min(cores);
    let workers = active_workers.max(1);
    (available_pool / workers).max(2).min(cores)
}

/// Resolve optimal DuckDB worker memory limit (`PRAGMA max_memory`) based on `max_memory` config,
/// host total physical RAM, and active concurrent worker count.
/// When set to `"auto"`, allocates `clamp(2GB, 12GB, floor(25% * total_ram_gb / active_workers))`.
pub fn compute_worker_max_memory(
    configured: &str,
    total_ram_bytes: Option<u64>,
    active_workers: usize,
) -> String {
    let trimmed = configured.trim();
    if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("auto") {
        return trimmed.to_string();
    }
    let total_gb = total_ram_bytes
        .map(|b| (b as f64) / (1024.0 * 1024.0 * 1024.0))
        .unwrap_or(16.0);
    let workers = active_workers.max(1) as f64;
    let budget_gb = ((total_gb * 0.25) / workers).floor() as u64;
    let clamped_gb = budget_gb.clamp(2, 12);
    format!("{}GB", clamped_gb)
}

/// Dual-engine DuckDB connection manager
pub struct DuckDbEnginePool {
    /// Dedicated UI connection protected by an async mutex
    ui_conn: Arc<Mutex<Connection>>,
    /// Scratch/temp directory for cached parquet files
    pub temp_dir: PathBuf,
    /// Concurrency throttle for background workers
    worker_semaphore: Arc<Semaphore>,
    /// Configured threads count (UI connection)
    threads: u32,
    /// Configured background worker threads ("auto" or positive integer)
    background_threads: String,
    /// Configured max memory string ("auto" or explicit DuckDB size e.g. "8GB")
    max_memory: String,
    /// Column statistics cache
    stats_cache: Arc<StatsCache>,
    /// Live active queries / worker tasks counter
    active_queries: Arc<AtomicUsize>,
}

impl DuckDbEnginePool {
    /// Initialize in-process DuckDB dual-engine pool
    pub fn new(temp_dir: PathBuf, threads: u32, max_memory: String) -> DuckResult<Self> {
        Self::new_with_background_threads(temp_dir, threads, "auto".to_string(), max_memory)
    }

    /// Initialize in-process DuckDB dual-engine pool with explicit `background_threads` setting
    pub fn new_with_background_threads(
        temp_dir: PathBuf,
        threads: u32,
        background_threads: String,
        max_memory: String,
    ) -> DuckResult<Self> {
        let _ = std::fs::create_dir_all(&temp_dir);
        let conn = Self::open_configured_ui_connection(&temp_dir, threads, &max_memory)?;
        let max_workers = if threads > 0 { threads.max(2) } else { 4 } as usize;

        Ok(Self {
            ui_conn: Arc::new(Mutex::new(conn)),
            temp_dir,
            worker_semaphore: Arc::new(Semaphore::new(max_workers)),
            threads,
            background_threads,
            max_memory,
            stats_cache: Arc::new(StatsCache::default_cache()),
            active_queries: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn open_configured_ui_connection(
        temp_dir: &Path,
        threads: u32,
        max_memory: &str,
    ) -> DuckResult<Connection> {
        let config = Config::default().access_mode(AccessMode::Automatic)?;
        let conn = Connection::open_in_memory_with_flags(config)?;

        if threads > 0 {
            conn.execute(&format!("PRAGMA threads = {};", threads), [])?;
        }
        if let Some(mem) = resolve_ui_max_memory(max_memory) {
            let _ = conn.execute(&format!("PRAGMA max_memory = '{}';", mem.replace('\'', "''")), []);
        }
        let temp_spill = temp_dir
            .join("duckdb_spill")
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let _ = conn.execute(&format!("PRAGMA temp_directory = '{}';", temp_spill), []);
        conn.execute("PRAGMA preserve_insertion_order = true;", [])?;
        Ok(conn)
    }

    /// Access the Column Statistics Cache
    pub fn stats_cache(&self) -> &StatsCache {
        &self.stats_cache
    }

    /// Configured threads count
    pub fn threads(&self) -> u32 {
        self.threads
    }

    /// Configured max memory
    pub fn max_memory(&self) -> &str {
        &self.max_memory
    }

    /// Current active queries / tasks count
    pub fn active_connections(&self) -> usize {
        self.active_queries.load(Ordering::Relaxed).max(1)
    }

    /// Genuine engine restart: replaces the UI DuckDB connection and clears cached statistics
    pub async fn restart(&self) -> Result<(), String> {
        let temp_dir = self.temp_dir.clone();
        let threads = self.threads;
        let max_memory = self.max_memory.clone();
        let new_conn = tokio::task::spawn_blocking(move || {
            Self::open_configured_ui_connection(&temp_dir, threads, &max_memory)
                .map_err(|e| format!("Failed to re-open DuckDB connection: {}", e))
        })
        .await
        .map_err(|e| format!("Join error during DuckDB restart: {}", e))??;

        {
            let mut guard = self.ui_conn.lock().await;
            *guard = new_conn;
        }
        self.stats_cache.clear().await;
        Ok(())
    }

    /// Execute a blocking closure against the dedicated UI DuckDB connection
    pub async fn eval_ui_conn<F, T>(&self, query_fn: F) -> Result<T, String>
    where
        F: FnOnce(&Connection) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let conn_arc = self.ui_conn.clone();
        let active = self.active_queries.clone();
        active.fetch_add(1, Ordering::Relaxed);
        let res = tokio::task::spawn_blocking(move || {
            let conn = conn_arc.blocking_lock();
            query_fn(&conn)
        })
        .await
        .map_err(|e| format!("Tokio join error during DuckDB evaluation: {}", e));
        active.fetch_sub(1, Ordering::Relaxed);
        res?
    }

    /// Execute SQL query on UI connection and encode results to binary Arrow IPC Stream Response
    pub async fn eval_query_arrow(&self, sql: String) -> Result<Response, String> {
        self.eval_ui_conn(move |conn| ArrowIpcEncoder::query_to_arrow_ipc(conn, &sql))
            .await
    }

    /// Execute row count SQL statement or O(1) Parquet metadata count on UI connection
    pub async fn eval_row_count(
        &self,
        count_sql: String,
        source_hint: Option<String>,
    ) -> Result<i64, String> {
        if let Some(ref source) = source_hint {
            let path = Path::new(source);
            if path.exists()
                && path
                    .extension()
                    .map_or(false, |ext| ext.eq_ignore_ascii_case("parquet"))
            {
                if let Ok(count) = FastCounter::count_parquet_rows(path) {
                    return Ok(count);
                }
            }
        }

        self.eval_ui_conn(move |conn| {
            let mut stmt = conn
                .prepare(&count_sql)
                .map_err(|e| format!("DuckDB count query prepare error [{}]: {}", count_sql, e))?;

            let count: i64 = stmt
                .query_row([], |row| row.get(0))
                .map_err(|e| format!("DuckDB count query execution error [{}]: {}", count_sql, e))?;

            Ok(count)
        })
        .await
    }

    /// Retrieve schema DTO for a given table or query
    pub async fn eval_schema(&self, table_or_query: String) -> Result<SchemaDto, String> {
        self.eval_ui_conn(move |conn| {
            let probe_sql = if table_or_query.trim_start().to_uppercase().starts_with("SELECT")
                || table_or_query.trim_start().to_uppercase().starts_with("WITH")
            {
                format!(
                    "SELECT * FROM ({}) AS _schema_probe LIMIT 0",
                    table_or_query.trim().trim_end_matches(';')
                )
            } else {
                format!(
                    "SELECT * FROM \"{}\" LIMIT 0",
                    table_or_query.replace('"', "\"\"")
                )
            };

            if let Ok(mut stmt) = conn.prepare(&probe_sql) {
                if let Ok(arrow) = stmt.query_arrow([]) {
                    let arrow_schema = arrow.get_schema();
                    return Ok(SchemaMapper::from_arrow_schema(&arrow_schema));
                }
            }

            let desc_sql = format!("DESCRIBE \"{}\"", table_or_query.replace('"', "\"\""));
            if let Ok(mut stmt) = conn.prepare(&desc_sql) {
                if let Ok(mut rows) = stmt.query([]) {
                    let mut cols = Vec::new();
                    while let Ok(Some(row)) = rows.next() {
                        let name: String = row.get(0).unwrap_or_default();
                        let col_type: String =
                            row.get(1).unwrap_or_else(|_| "VARCHAR".to_string());
                        cols.push((name, col_type));
                    }
                    if !cols.is_empty() {
                        return Ok(SchemaMapper::from_column_types(cols));
                    }
                }
            }

            Ok(SchemaDto {
                dialect: "duckdb".to_string(),
                columns: Vec::new(),
                column_metadata: std::collections::HashMap::new(),
                column_types: std::collections::HashMap::new(),
            })
        })
        .await
    }

    /// Resolve effective worker thread count for a given active concurrency level
    pub fn resolve_worker_threads(&self, active_workers: usize) -> usize {
        let logical_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        compute_worker_threads(&self.background_threads, logical_cores, active_workers)
    }

    /// Resolve effective worker RAM limit for a given active concurrency level
    pub fn resolve_worker_max_memory(&self, active_workers: usize) -> String {
        compute_worker_max_memory(&self.max_memory, get_total_system_ram_bytes(), active_workers)
    }

    /// Create an isolated worker connection for background ingestion tasks with adaptive CPU/RAM budget
    pub fn create_worker_connection(&self) -> DuckResult<Connection> {
        self.create_worker_connection_with_budget(1)
    }

    /// Create an isolated worker connection scaled to `active_workers` concurrent jobs
    pub fn create_worker_connection_with_budget(&self, active_workers: usize) -> DuckResult<Connection> {
        let conn = Connection::open_in_memory()?;
        let worker_threads = self.resolve_worker_threads(active_workers);
        let worker_mem = self.resolve_worker_max_memory(active_workers);
        let _ = conn.execute(&format!("PRAGMA threads = {};", worker_threads), []);
        let _ = conn.execute(
            &format!("PRAGMA max_memory = '{}';", worker_mem.replace('\'', "''")),
            [],
        );
        let temp_spill = self
            .temp_dir
            .join("duckdb_worker_spill")
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let _ = conn.execute(&format!("PRAGMA temp_directory = '{}';", temp_spill), []);
        let _ = conn.execute("PRAGMA preserve_insertion_order = false;", []);
        tracing::info!(
            worker_threads = worker_threads,
            worker_mem = %worker_mem,
            active_workers = active_workers.max(1),
            "Configured DuckDB ingestion worker connection"
        );
        Ok(conn)
    }

    /// Execute background ingestion with worker semaphore throttling (exclusive 1-slot budget)
    pub async fn run_ingest_task<F, T>(&self, task: F) -> Result<T, String>
    where
        F: FnOnce(Connection) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        self.run_ingest_task_with_budget(1, task).await
    }

    /// Execute background ingestion with explicit active worker concurrency hint
    pub async fn run_ingest_task_with_budget<F, T>(
        &self,
        active_workers: usize,
        task: F,
    ) -> Result<T, String>
    where
        F: FnOnce(Connection) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let _permit = self
            .worker_semaphore
            .acquire()
            .await
            .map_err(|e| format!("Worker semaphore error: {}", e))?;

        let conn = self
            .create_worker_connection_with_budget(active_workers)
            .map_err(|e| format!("Failed to create worker connection: {}", e))?;

        let active = self.active_queries.clone();
        active.fetch_add(1, Ordering::Relaxed);
        let res = tokio::task::spawn_blocking(move || task(conn))
            .await
            .map_err(|e| format!("Worker task join error: {}", e));
        active.fetch_sub(1, Ordering::Relaxed);
        res?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_worker_threads_adaptive_and_explicit() {
        // 16-core machine, auto, exclusive big file (1 active worker) -> 14 threads (N - 2)
        assert_eq!(compute_worker_threads("auto", 16, 1), 14);
        // 16-core machine, auto, 4 concurrent workers -> 14 / 4 = 3 threads each
        assert_eq!(compute_worker_threads("auto", 16, 4), 3);
        // 4-core machine, auto, 1 worker -> 2 threads
        assert_eq!(compute_worker_threads("auto", 4, 1), 2);
        // Explicit override > 2 must be honored (no hardcoded .min(2) cap!)
        assert_eq!(compute_worker_threads("12", 16, 1), 12);
        assert_eq!(compute_worker_threads("99", 16, 1), 16);
    }

    #[test]
    fn test_compute_worker_max_memory_adaptive_and_explicit() {
        let ram_96gb = 96u64 * 1024 * 1024 * 1024;
        let ram_16gb = 16u64 * 1024 * 1024 * 1024;
        let ram_8gb = 8u64 * 1024 * 1024 * 1024;

        // 96GB RAM, 1 active worker -> 25% = 24GB clamped to 12GB
        assert_eq!(compute_worker_max_memory("auto", Some(ram_96gb), 1), "12GB");
        // 96GB RAM, 4 concurrent workers -> 24GB / 4 = 6GB each
        assert_eq!(compute_worker_max_memory("auto", Some(ram_96gb), 4), "6GB");
        // 16GB RAM, 1 active worker -> 25% = 4GB
        assert_eq!(compute_worker_max_memory("auto", Some(ram_16gb), 1), "4GB");
        // 8GB RAM, 1 active worker -> 2GB floor
        assert_eq!(compute_worker_max_memory("auto", Some(ram_8gb), 1), "2GB");
        // Explicit override
        assert_eq!(compute_worker_max_memory("8GB", Some(ram_16gb), 1), "8GB");
    }
}
