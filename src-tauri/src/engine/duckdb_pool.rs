//! duckdb_pool.rs
//!
//! In-Process Tokio Async DuckDB Connection Pool with Dual-Engine Driver Isolation:
//! - Dedicated UI connection for sub-millisecond query evaluation with query supersede & watchdog timeout.
//! - Ingestion and read-only worker pool with host-aware CPU, RAM, and disk spill budgets.
//! - Thread-safe access via tokio::sync::Mutex, tokio::sync::Semaphore, and tokio::task::spawn_blocking.

use duckdb::arrow::array::StructArray;
use duckdb::arrow::datatypes::{Schema, SchemaRef};
use duckdb::arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{ffi, Connection, Error as DuckError, Result as DuckResult};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tauri::ipc::Response;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::engine::arrow_encoder::{
    ArrowIpcEncoder, MAX_INTERACTIVE_IPC_BYTES, MAX_INTERACTIVE_IPC_CELLS,
};
use crate::engine::fast_count::FastCounter;
use crate::engine::schema_mapper::{SchemaDto, SchemaMapper};
use crate::engine::stats_cache::StatsCache;
use crate::ingest::io_monitor::{get_available_disk_bytes, get_total_system_ram_bytes};

/// Compute UI DuckDB `PRAGMA max_memory` limit.
/// Explicit non-empty, non-"auto" values are preserved; "auto" or empty allocates
/// 25% of physical RAM clamped to `512MB..=4096MB` (defaulting to 16GB host RAM if unknown).
pub fn compute_ui_max_memory(configured: &str, total_ram_bytes: Option<u64>) -> String {
    let trimmed = configured.trim();
    if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("auto") {
        return trimmed.to_string();
    }
    let total_mb = total_ram_bytes
        .map(|b| b / (1024 * 1024))
        .unwrap_or(16 * 1024);
    let budget_mb = (total_mb / 4).clamp(512, 4096);
    format!("{}MB", budget_mb)
}

/// Compute UI DuckDB `PRAGMA threads` limit.
/// When `configured_threads == 0` ("auto"), uses `(logical_cores / 2).clamp(1, 4)`.
pub fn compute_ui_threads(configured_threads: u32, logical_cores: usize) -> usize {
    if configured_threads > 0 {
        return (configured_threads as usize).max(1);
    }
    (logical_cores.max(1) / 2).clamp(1, 4)
}

/// Resolve optimal DuckDB worker thread count based on `background_threads` config,
/// host logical core count, and active concurrent worker count.
/// Floor is `1` and "auto" never oversubscribes beyond `cores.saturating_sub(2).max(1)`.
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
    let available_pool = cores.saturating_sub(2).max(1);
    let workers = active_workers.max(1);
    (available_pool / workers).clamp(1, available_pool)
}

/// Resolve optimal DuckDB worker memory limit (`PRAGMA max_memory`) based on `max_memory` config,
/// host total physical RAM, and active concurrent worker count.
/// When set to `"auto"`, allocates `((total_mb * 30 / 100) / active_workers).clamp(512, 6144)` in MB.
pub fn compute_worker_max_memory(
    configured: &str,
    total_ram_bytes: Option<u64>,
    active_workers: usize,
) -> String {
    let trimmed = configured.trim();
    if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("auto") {
        return trimmed.to_string();
    }
    let total_mb = total_ram_bytes
        .map(|b| b / (1024 * 1024))
        .unwrap_or(16 * 1024);
    let workers = active_workers.max(1) as u64;
    let budget_mb = ((total_mb * 30 / 100) / workers).clamp(512, 6144);
    format!("{}MB", budget_mb)
}

/// Compute maximum concurrent background ingestion/read workers allowed by both host RAM and cores.
pub fn compute_max_concurrent_workers(logical_cores: usize, total_ram_bytes: Option<u64>) -> usize {
    let ram_gb = total_ram_bytes
        .map(|b| b / (1024 * 1024 * 1024))
        .unwrap_or(16);
    let ram_cap = if ram_gb <= 8 {
        1
    } else if ram_gb <= 16 {
        2
    } else {
        4
    };
    let core_cap = (logical_cores.saturating_sub(2) / 2).clamp(1, 4);
    ram_cap.min(core_cap)
}

/// Compute `PRAGMA max_temp_directory_size` from available disk space (25% free, clamped `2048..=8192` MB).
pub fn compute_max_temp_directory_size_from_free(free_bytes: Option<u64>) -> String {
    let free_mb = free_bytes
        .map(|b| b / (1024 * 1024))
        .unwrap_or(16 * 1024);
    let cap_mb = (free_mb * 25 / 100).clamp(2048, 8192);
    format!("{}MB", cap_mb)
}

fn compute_max_temp_directory_size(temp_dir: &Path) -> String {
    compute_max_temp_directory_size_from_free(get_available_disk_bytes(temp_dir))
}

fn cleanup_spill_dirs(temp_dir: &Path) {
    for sub in ["duckdb_spill", "duckdb_worker_spill"] {
        let spill_path = temp_dir.join(sub);
        if spill_path.exists() {
            let _ = std::fs::remove_dir_all(&spill_path);
        }
    }
}

/// Thread-safe handle to interrupt an active FFI DuckDB connection.
#[derive(Clone, Default)]
struct InterruptHandle(Arc<std::sync::Mutex<Option<ffi::duckdb_connection>>>);

// SAFETY: `ffi::duckdb_connection` is an opaque C handle; `duckdb_interrupt` is thread-safe
// in DuckDB's C API, and access to the pointer is synchronized by `std::sync::Mutex` and
// explicitly cleared to `None` before `duckdb_disconnect` is ever called.
unsafe impl Send for InterruptHandle {}
unsafe impl Sync for InterruptHandle {}

impl InterruptHandle {
    fn set(&self, con: ffi::duckdb_connection) {
        if let Ok(mut guard) = self.0.lock() {
            *guard = Some(con);
        }
    }

    fn clear(&self) {
        if let Ok(mut guard) = self.0.lock() {
            *guard = None;
        }
    }

    fn interrupt(&self) {
        if let Ok(guard) = self.0.lock() {
            if let Some(con) = *guard {
                if !con.is_null() {
                    unsafe {
                        ffi::duckdb_interrupt(con);
                    }
                }
            }
        }
    }
}

/// Owns the underlying `duckdb_database` instance together with:
/// - `conn`: a `duckdb::Connection` (`owned = false`) for `eval_ui_conn`, DDL, schema/row count.
/// - `query_con`: a sibling `ffi::duckdb_connection` on the exact same `raw_db` for interruptible
///   Arrow queries (`eval_query_arrow_cancellable`), sharing all database views and tables.
struct UiConnectionHandle {
    raw_db: ffi::duckdb_database,
    conn: Option<Connection>,
    query_con: ffi::duckdb_connection,
    interrupt: InterruptHandle,
}

// SAFETY: `conn` is `Send`, and `raw_db` / `query_con` are exclusively mutated behind the
// `tokio::sync::Mutex` wrapping `UiConnectionHandle`, while `interrupt` clears `query_con`
// under its own mutex before `duckdb_disconnect` in `Drop`.
unsafe impl Send for UiConnectionHandle {}

impl Drop for UiConnectionHandle {
    fn drop(&mut self) {
        // Clear InterruptHandle first so no concurrent watchdog can call duckdb_interrupt
        // after query_con is disconnected.
        self.interrupt.clear();
        self.conn.take();
        unsafe {
            if !self.query_con.is_null() {
                ffi::duckdb_disconnect(&mut self.query_con);
                self.query_con = ptr::null_mut();
            }
            if !self.raw_db.is_null() {
                ffi::duckdb_close(&mut self.raw_db);
                self.raw_db = ptr::null_mut();
            }
        }
    }
}

impl UiConnectionHandle {
    fn conn(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("UiConnectionHandle conn is present until Drop")
    }

    fn open_configured(
        temp_dir: &Path,
        threads: u32,
        max_memory: &str,
        interrupt: InterruptHandle,
    ) -> DuckResult<Self> {
        let mut raw_db: ffi::duckdb_database = ptr::null_mut();
        unsafe {
            let mut config: ffi::duckdb_config = ptr::null_mut();
            if ffi::duckdb_create_config(&mut config) != ffi::DuckDBSuccess {
                return Err(DuckError::DuckDBFailure(
                    ffi::Error::new(ffi::DuckDBError),
                    Some("Failed to create DuckDB config".to_string()),
                ));
            }
            let c_access_k = CString::new("access_mode").unwrap();
            let c_access_v = CString::new("AUTOMATIC").unwrap();
            let _ = ffi::duckdb_set_config(config, c_access_k.as_ptr(), c_access_v.as_ptr());
            let c_api_k = CString::new("duckdb_api").unwrap();
            let c_api_v = CString::new("rust").unwrap();
            let _ = ffi::duckdb_set_config(config, c_api_k.as_ptr(), c_api_v.as_ptr());

            let c_mem = CString::new(":memory:").unwrap();
            let mut c_err: *mut c_char = ptr::null_mut();
            let rc = ffi::duckdb_open_ext(c_mem.as_ptr(), &mut raw_db, config, &mut c_err);
            ffi::duckdb_destroy_config(&mut config);

            if rc != ffi::DuckDBSuccess {
                let msg = if !c_err.is_null() {
                    let s = CStr::from_ptr(c_err).to_string_lossy().to_string();
                    ffi::duckdb_free(c_err as *mut c_void);
                    Some(s)
                } else {
                    Some("Failed to open DuckDB in-memory database".to_string())
                };
                return Err(DuckError::DuckDBFailure(ffi::Error::new(rc), msg));
            }
        }

        let conn = match unsafe { Connection::open_from_raw(raw_db) } {
            Ok(c) => c,
            Err(e) => {
                unsafe {
                    if !raw_db.is_null() {
                        ffi::duckdb_close(&mut raw_db);
                    }
                }
                return Err(e);
            }
        };

        let mut query_con: ffi::duckdb_connection = ptr::null_mut();
        let conn_rc = unsafe { ffi::duckdb_connect(raw_db, &mut query_con) };
        if conn_rc != ffi::DuckDBSuccess || query_con.is_null() {
            drop(conn);
            unsafe {
                if !query_con.is_null() {
                    ffi::duckdb_disconnect(&mut query_con);
                }
                if !raw_db.is_null() {
                    ffi::duckdb_close(&mut raw_db);
                }
            }
            return Err(DuckError::DuckDBFailure(
                ffi::Error::new(conn_rc),
                Some("Failed to connect cancellable UI query handle".to_string()),
            ));
        }

        let handle = Self {
            raw_db,
            conn: Some(conn),
            query_con,
            interrupt: interrupt.clone(),
        };

        let logical_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let ui_threads = compute_ui_threads(threads, logical_cores);
        let ui_mem = compute_ui_max_memory(max_memory, get_total_system_ram_bytes());
        let temp_spill = temp_dir
            .join("duckdb_spill")
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let max_temp_size = compute_max_temp_directory_size(temp_dir);

        let c_conn = handle.conn();
        c_conn.execute(&format!("PRAGMA threads = {};", ui_threads), [])?;
        let _ = c_conn.execute(&format!("PRAGMA max_memory = '{}';", ui_mem.replace('\'', "''")), []);
        let _ = c_conn.execute(&format!("PRAGMA temp_directory = '{}';", temp_spill), []);
        let _ = c_conn.execute(
            &format!(
                "PRAGMA max_temp_directory_size = '{}';",
                max_temp_size.replace('\'', "''")
            ),
            [],
        );
        c_conn.execute("PRAGMA preserve_insertion_order = true;", [])?;

        // Ensure query_con matches connection-level insertion order behavior.
        unsafe {
            let c_pragma = CString::new("PRAGMA preserve_insertion_order = true;").unwrap();
            let mut out: ffi::duckdb_arrow = ptr::null_mut();
            let _ = ffi::duckdb_query_arrow(handle.query_con, c_pragma.as_ptr(), &mut out);
            if !out.is_null() {
                ffi::duckdb_destroy_arrow(&mut out);
            }
        }

        interrupt.set(handle.query_con);
        Ok(handle)
    }
}

struct FfiPreparedStmt(ffi::duckdb_prepared_statement);

impl Drop for FfiPreparedStmt {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                ffi::duckdb_destroy_prepare(&mut self.0);
            }
        }
    }
}

struct FfiArrowResult(ffi::duckdb_arrow);

impl Drop for FfiArrowResult {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                ffi::duckdb_destroy_arrow(&mut self.0);
            }
        }
    }
}

struct FfiArrowBatchIter {
    out: ffi::duckdb_arrow,
    error: Option<String>,
}

impl FfiArrowBatchIter {
    unsafe fn capture_arrow_error(&mut self, fallback: &str) {
        if self.error.is_some() {
            return;
        }
        let msg = if !self.out.is_null() {
            let c_err = ffi::duckdb_query_arrow_error(self.out);
            if !c_err.is_null() {
                CStr::from_ptr(c_err).to_string_lossy().to_string()
            } else {
                fallback.to_string()
            }
        } else {
            fallback.to_string()
        };
        self.error = Some(msg);
    }
}

impl Iterator for FfiArrowBatchIter {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.out.is_null() {
            return None;
        }
        unsafe {
            let mut arrays = FFI_ArrowArray::empty();
            if ffi::duckdb_query_arrow_array(
                self.out,
                &mut ptr::addr_of_mut!(arrays) as *mut _ as *mut ffi::duckdb_arrow_array,
            ) != ffi::DuckDBSuccess
            {
                self.capture_arrow_error("DuckDB Arrow batch fetch failed");
                return None;
            }
            if arrays.is_empty() {
                return None;
            }
            let mut schema = FFI_ArrowSchema::empty();
            if ffi::duckdb_query_arrow_schema(
                self.out,
                &mut ptr::addr_of_mut!(schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
            ) != ffi::DuckDBSuccess
            {
                self.capture_arrow_error("DuckDB Arrow schema fetch failed during batch iteration");
                return None;
            }
            let array_data = match from_ffi(arrays, &schema) {
                Ok(data) => data,
                Err(e) => {
                    self.error = Some(format!("Arrow FFI array conversion error: {}", e));
                    return None;
                }
            };
            let struct_array = StructArray::from(array_data);
            Some(RecordBatch::from(&struct_array))
        }
    }
}

fn extract_ffi_arrow_schema(out: ffi::duckdb_arrow) -> Result<SchemaRef, String> {
    unsafe {
        let mut c_schema = FFI_ArrowSchema::empty();
        let rc = ffi::duckdb_query_arrow_schema(
            out,
            &mut ptr::addr_of_mut!(c_schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
        );
        if rc != ffi::DuckDBSuccess {
            let err_msg = if !out.is_null() {
                let c_err = ffi::duckdb_query_arrow_error(out);
                if !c_err.is_null() {
                    CStr::from_ptr(c_err).to_string_lossy().to_string()
                } else {
                    "Unknown Arrow schema error".to_string()
                }
            } else {
                "Arrow result is null".to_string()
            };
            return Err(err_msg);
        }
        let parsed = Schema::try_from(&c_schema)
            .map_err(|e| format!("Arrow FFI schema conversion error: {}", e))?;
        Ok(Arc::new(parsed))
    }
}

fn format_timeout_error(timeout_ms: u64) -> String {
    if timeout_ms % 1000 == 0 {
        format!(
            "Query timed out after {}s to protect system responsiveness. Try filtering or narrowing columns.",
            timeout_ms / 1000
        )
    } else {
        format!(
            "Query timed out after {}ms to protect system responsiveness. Try filtering or narrowing columns.",
            timeout_ms
        )
    }
}

struct ActiveQueryGuard {
    counter: Arc<AtomicUsize>,
    active_flag: Option<Arc<AtomicBool>>,
}

impl ActiveQueryGuard {
    fn new(counter: Arc<AtomicUsize>, active_flag: Option<Arc<AtomicBool>>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        if let Some(ref flag) = active_flag {
            flag.store(true, Ordering::SeqCst);
        }
        Self {
            counter,
            active_flag,
        }
    }
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        if let Some(ref flag) = self.active_flag {
            flag.store(false, Ordering::SeqCst);
        }
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Dual-engine DuckDB connection manager
pub struct DuckDbEnginePool {
    /// Dedicated UI connection handle protected by an async mutex
    ui_conn: Arc<Mutex<Option<UiConnectionHandle>>>,
    /// Interrupt handle for the active UI query connection
    ui_interrupt: InterruptHandle,
    /// Monotonic generation counter for UI query supersede detection
    ui_query_gen: Arc<AtomicU64>,
    /// Indicates whether an interruptible UI query is actively executing
    ui_query_active: Arc<AtomicBool>,
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
        cleanup_spill_dirs(&temp_dir);

        let ui_interrupt = InterruptHandle::default();
        let handle = UiConnectionHandle::open_configured(
            &temp_dir,
            threads,
            &max_memory,
            ui_interrupt.clone(),
        )?;

        let logical_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let max_workers =
            compute_max_concurrent_workers(logical_cores, get_total_system_ram_bytes());

        Ok(Self {
            ui_conn: Arc::new(Mutex::new(Some(handle))),
            ui_interrupt,
            ui_query_gen: Arc::new(AtomicU64::new(0)),
            ui_query_active: Arc::new(AtomicBool::new(false)),
            temp_dir,
            worker_semaphore: Arc::new(Semaphore::new(max_workers)),
            threads,
            background_threads,
            max_memory,
            stats_cache: Arc::new(StatsCache::default_cache()),
            active_queries: Arc::new(AtomicUsize::new(0)),
        })
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

    /// Acquire a global background worker semaphore permit.
    pub async fn acquire_worker_permit(&self) -> Result<OwnedSemaphorePermit, String> {
        self.worker_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| format!("Worker semaphore error: {}", e))
    }

    /// Genuine engine restart: interrupts active UI query, cleans spill directories,
    /// replaces the UI DuckDB connection, and clears cached statistics.
    pub async fn restart(&self) -> Result<(), String> {
        self.ui_query_gen.fetch_add(1, Ordering::SeqCst);
        if self.ui_query_active.load(Ordering::SeqCst) {
            self.ui_interrupt.interrupt();
        }

        let ui_conn_arc = self.ui_conn.clone();
        let temp_dir = self.temp_dir.clone();
        let threads = self.threads;
        let max_memory = self.max_memory.clone();
        let interrupt = self.ui_interrupt.clone();

        let guard = ui_conn_arc.lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            interrupt.clear();
            // Drop old connection handle first so Windows releases any open spill files.
            guard.take();
            cleanup_spill_dirs(&temp_dir);
            let new_handle =
                UiConnectionHandle::open_configured(&temp_dir, threads, &max_memory, interrupt)
                    .map_err(|e| format!("Failed to re-open DuckDB connection: {}", e))?;
            *guard = Some(new_handle);
            Ok::<(), String>(())
        })
        .await
        .map_err(|e| format!("Join error during DuckDB restart: {}", e))??;

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
        let _guard = ActiveQueryGuard::new(self.active_queries.clone(), None);
        tokio::task::spawn_blocking(move || {
            let guard = conn_arc.blocking_lock();
            let handle = guard
                .as_ref()
                .ok_or_else(|| "DuckDB UI connection is closed".to_string())?;
            query_fn(handle.conn())
        })
        .await
        .map_err(|e| format!("Tokio join error during DuckDB evaluation: {}", e))?
    }

    /// Execute SQL query on UI connection and encode results to binary Arrow IPC Stream Response
    pub async fn eval_query_arrow(&self, sql: String) -> Result<Response, String> {
        self.eval_ui_conn(move |conn| ArrowIpcEncoder::query_to_arrow_ipc(conn, &sql))
            .await
    }

    /// Execute SQL query on the shared UI database with supersede interruption and a watchdog timeout.
    /// Passing `timeout_ms == 0` defaults to `30_000` ms (30s).
    pub async fn eval_query_arrow_cancellable(
        &self,
        sql: String,
        timeout_ms: u64,
    ) -> Result<Response, String> {
        let effective_timeout_ms = if timeout_ms == 0 { 30_000 } else { timeout_ms };
        let my_gen = self.ui_query_gen.fetch_add(1, Ordering::SeqCst) + 1;
        if self.ui_query_active.load(Ordering::SeqCst) {
            self.ui_interrupt.interrupt();
        }

        let guard = self.ui_conn.clone().lock_owned().await;
        if self.ui_query_gen.load(Ordering::SeqCst) != my_gen {
            return Err("Query cancelled: superseded by a newer query.".to_string());
        }

        let _active_guard = ActiveQueryGuard::new(
            self.active_queries.clone(),
            Some(self.ui_query_active.clone()),
        );

        let timed_out = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));

        let wd_timed_out = timed_out.clone();
        let wd_finished = finished.clone();
        let wd_gen = self.ui_query_gen.clone();
        let wd_active = self.ui_query_active.clone();
        let wd_interrupt = self.ui_interrupt.clone();

        struct WatchdogAbortGuard(Option<tokio::task::JoinHandle<()>>);
        impl Drop for WatchdogAbortGuard {
            fn drop(&mut self) {
                if let Some(handle) = self.0.take() {
                    handle.abort();
                }
            }
        }

        let watchdog = WatchdogAbortGuard(Some(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(effective_timeout_ms)).await;
            if !wd_finished.load(Ordering::SeqCst)
                && wd_gen.load(Ordering::SeqCst) == my_gen
                && wd_active.load(Ordering::SeqCst)
            {
                wd_timed_out.store(true, Ordering::SeqCst);
                wd_interrupt.interrupt();
            }
        })));

        let task_gen = self.ui_query_gen.clone();
        let task_timed_out = timed_out.clone();

        let res = tokio::task::spawn_blocking(move || {
            if task_gen.load(Ordering::SeqCst) != my_gen {
                return Err("Query cancelled: superseded by a newer query.".to_string());
            }

            let handle = guard
                .as_ref()
                .ok_or_else(|| "DuckDB UI connection is closed".to_string())?;
            let con = handle.query_con;

            let c_sql = CString::new(sql.as_str())
                .map_err(|_| "DuckDB SQL contains an interior NUL byte".to_string())?;

            let mut c_stmt: ffi::duckdb_prepared_statement = ptr::null_mut();
            let prep_rc = unsafe { ffi::duckdb_prepare(con, c_sql.as_ptr(), &mut c_stmt) };
            let stmt_guard = FfiPreparedStmt(c_stmt);
            if prep_rc != ffi::DuckDBSuccess {
                if task_timed_out.load(Ordering::SeqCst) {
                    return Err(format_timeout_error(effective_timeout_ms));
                }
                if task_gen.load(Ordering::SeqCst) != my_gen {
                    return Err("Query cancelled: superseded by a newer query.".to_string());
                }
                let err_msg = unsafe {
                    if !stmt_guard.0.is_null() {
                        let c_err = ffi::duckdb_prepare_error(stmt_guard.0);
                        if !c_err.is_null() {
                            CStr::from_ptr(c_err).to_string_lossy().to_string()
                        } else {
                            "Unknown prepare error".to_string()
                        }
                    } else {
                        "Prepare returned null statement".to_string()
                    }
                };
                return Err(format!("DuckDB SQL Prepare error [{}]: {}", sql, err_msg));
            }

            if task_gen.load(Ordering::SeqCst) != my_gen {
                return Err("Query cancelled: superseded by a newer query.".to_string());
            }

            let mut out: ffi::duckdb_arrow = ptr::null_mut();
            let exec_rc = unsafe { ffi::duckdb_execute_prepared_arrow(stmt_guard.0, &mut out) };
            let arrow_guard = FfiArrowResult(out);
            if exec_rc != ffi::DuckDBSuccess {
                if task_timed_out.load(Ordering::SeqCst) {
                    return Err(format_timeout_error(effective_timeout_ms));
                }
                if task_gen.load(Ordering::SeqCst) != my_gen {
                    return Err("Query cancelled: superseded by a newer query.".to_string());
                }
                let err_msg = unsafe {
                    if !arrow_guard.0.is_null() {
                        let c_err = ffi::duckdb_query_arrow_error(arrow_guard.0);
                        if !c_err.is_null() {
                            CStr::from_ptr(c_err).to_string_lossy().to_string()
                        } else {
                            "Unknown execution error".to_string()
                        }
                    } else {
                        "Execution returned null Arrow result".to_string()
                    }
                };
                return Err(format!("DuckDB Query Arrow error [{}]: {}", sql, err_msg));
            }

            let schema = extract_ffi_arrow_schema(arrow_guard.0)?;
            let mut batch_iter = FfiArrowBatchIter {
                out: arrow_guard.0,
                error: None,
            };
            let bytes = ArrowIpcEncoder::encode_iter_to_vec_capped(
                &mut batch_iter,
                &schema,
                None,
                MAX_INTERACTIVE_IPC_BYTES,
                MAX_INTERACTIVE_IPC_CELLS,
            )?;
            if task_timed_out.load(Ordering::SeqCst) {
                return Err(format_timeout_error(effective_timeout_ms));
            }
            if task_gen.load(Ordering::SeqCst) != my_gen {
                return Err("Query cancelled: superseded by a newer query.".to_string());
            }
            if let Some(iter_err) = batch_iter.error {
                return Err(format!("DuckDB Query Arrow error [{}]: {}", sql, iter_err));
            }
            Ok(Response::new(bytes))
        })
        .await
        .map_err(|e| format!("Tokio join error during cancellable DuckDB query: {}", e));

        finished.store(true, Ordering::SeqCst);
        drop(watchdog);
        res?
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
        let max_temp_size = compute_max_temp_directory_size(&self.temp_dir);
        let _ = conn.execute(&format!("PRAGMA temp_directory = '{}';", temp_spill), []);
        let _ = conn.execute(
            &format!(
                "PRAGMA max_temp_directory_size = '{}';",
                max_temp_size.replace('\'', "''")
            ),
            [],
        );
        let _ = conn.execute("PRAGMA preserve_insertion_order = false;", []);
        tracing::info!(
            worker_threads = worker_threads,
            worker_mem = %worker_mem,
            max_temp_size = %max_temp_size,
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
        let _permit = self.acquire_worker_permit().await?;

        let conn = self
            .create_worker_connection_with_budget(active_workers)
            .map_err(|e| format!("Failed to create worker connection: {}", e))?;

        let _active_guard = ActiveQueryGuard::new(self.active_queries.clone(), None);
        tokio::task::spawn_blocking(move || task(conn))
            .await
            .map_err(|e| format!("Worker task join error: {}", e))?
    }

    /// Execute an isolated read/analytics task (such as `SUMMARIZE`) on a temporary worker
    /// connection gated by `worker_semaphore` and worker RAM/thread/spill budgets,
    /// without acquiring or blocking the `ui_conn` mutex.
    pub async fn run_read_worker_task<F, T>(&self, task: F) -> Result<T, String>
    where
        F: FnOnce(&Connection) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let _permit = self.acquire_worker_permit().await?;
        let active_workers = self.active_connections();
        let conn = self
            .create_worker_connection_with_budget(active_workers)
            .map_err(|e| format!("Failed to create read worker connection: {}", e))?;

        let _active_guard = ActiveQueryGuard::new(self.active_queries.clone(), None);
        tokio::task::spawn_blocking(move || task(&conn))
            .await
            .map_err(|e| format!("Read worker task join error: {}", e))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_ui_max_memory_profiles() {
        let ram_8gb = 8u64 * 1024 * 1024 * 1024;
        let ram_16gb = 16u64 * 1024 * 1024 * 1024;
        let ram_96gb = 96u64 * 1024 * 1024 * 1024;
        let ram_1gb = 1u64 * 1024 * 1024 * 1024;

        assert_eq!(compute_ui_max_memory("auto", Some(ram_8gb)), "2048MB");
        assert_eq!(compute_ui_max_memory("", Some(ram_16gb)), "4096MB");
        assert_eq!(compute_ui_max_memory("auto", Some(ram_96gb)), "4096MB");
        assert_eq!(compute_ui_max_memory("auto", Some(ram_1gb)), "512MB");
        assert_eq!(compute_ui_max_memory("auto", None), "4096MB");
        assert_eq!(compute_ui_max_memory(" 6GB ", Some(ram_16gb)), "6GB");
    }

    #[test]
    fn test_compute_ui_threads_profiles() {
        assert_eq!(compute_ui_threads(0, 4), 2);
        assert_eq!(compute_ui_threads(0, 8), 4);
        assert_eq!(compute_ui_threads(0, 16), 4);
        assert_eq!(compute_ui_threads(0, 1), 1);
        assert_eq!(compute_ui_threads(6, 16), 6);
    }

    #[test]
    fn test_compute_worker_threads_adaptive_and_explicit() {
        // 16-core machine, auto, exclusive big file (1 active worker) -> 14 threads (N - 2)
        assert_eq!(compute_worker_threads("auto", 16, 1), 14);
        // 16-core machine, auto, 4 concurrent workers -> 14 / 4 = 3 threads each
        assert_eq!(compute_worker_threads("auto", 16, 4), 3);
        // 8-core machine, auto, 2 concurrent workers -> 6 / 2 = 3 threads each
        assert_eq!(compute_worker_threads("auto", 8, 2), 3);
        // 4-core machine, auto, 1 worker -> 2 threads; 2 workers -> floor 1 (no oversubscription)
        assert_eq!(compute_worker_threads("auto", 4, 1), 2);
        assert_eq!(compute_worker_threads("auto", 4, 2), 1);
        // Explicit override > 2 must be honored
        assert_eq!(compute_worker_threads("12", 16, 1), 12);
        assert_eq!(compute_worker_threads("99", 16, 1), 16);
    }

    #[test]
    fn test_compute_worker_max_memory_adaptive_and_explicit() {
        let ram_96gb = 96u64 * 1024 * 1024 * 1024;
        let ram_16gb = 16u64 * 1024 * 1024 * 1024;
        let ram_8gb = 8u64 * 1024 * 1024 * 1024;

        // 8GB/4-core: 1 worker -> 30% of 8192MB = 2457MB
        assert_eq!(compute_worker_max_memory("auto", Some(ram_8gb), 1), "2457MB");
        // 16GB/8-core: 1 worker -> 4915MB; 2 workers -> 2457MB each
        assert_eq!(compute_worker_max_memory("auto", Some(ram_16gb), 1), "4915MB");
        assert_eq!(compute_worker_max_memory("auto", Some(ram_16gb), 2), "2457MB");
        // 96GB/16-core: 1 worker -> 6144MB cap; 4 workers -> 6144MB cap
        assert_eq!(compute_worker_max_memory("auto", Some(ram_96gb), 1), "6144MB");
        assert_eq!(compute_worker_max_memory("auto", Some(ram_96gb), 4), "6144MB");
        // Explicit override
        assert_eq!(compute_worker_max_memory("8GB", Some(ram_16gb), 1), "8GB");
    }

    #[test]
    fn test_compute_max_concurrent_workers_profiles() {
        let ram_8gb = 8u64 * 1024 * 1024 * 1024;
        let ram_16gb = 16u64 * 1024 * 1024 * 1024;
        let ram_96gb = 96u64 * 1024 * 1024 * 1024;

        // 8GB / 4-core -> 1
        assert_eq!(compute_max_concurrent_workers(4, Some(ram_8gb)), 1);
        // 16GB / 8-core -> 2
        assert_eq!(compute_max_concurrent_workers(8, Some(ram_16gb)), 2);
        // 96GB / 16-core -> 4
        assert_eq!(compute_max_concurrent_workers(16, Some(ram_96gb)), 4);
        // 96GB / 4-core -> bounded by core_cap = 1
        assert_eq!(compute_max_concurrent_workers(4, Some(ram_96gb)), 1);
    }

    #[test]
    fn test_compute_max_temp_directory_size() {
        let gb_4 = 4u64 * 1024 * 1024 * 1024;
        let gb_20 = 20u64 * 1024 * 1024 * 1024;
        let gb_100 = 100u64 * 1024 * 1024 * 1024;

        assert_eq!(compute_max_temp_directory_size_from_free(Some(gb_4)), "2048MB");
        assert_eq!(compute_max_temp_directory_size_from_free(Some(gb_20)), "5120MB");
        assert_eq!(compute_max_temp_directory_size_from_free(Some(gb_100)), "8192MB");
        assert_eq!(compute_max_temp_directory_size_from_free(None), "4096MB");
    }

    #[tokio::test]
    async fn test_eval_query_arrow_cancellable_view_sharing_and_worker_tasks() {
        let temp_dir = std::env::temp_dir().join("pulex_m1_test_views");
        let _ = std::fs::remove_dir_all(&temp_dir);
        let pool = DuckDbEnginePool::new(temp_dir.clone(), 0, "auto".to_string())
            .expect("Pool init should succeed");

        // Create a VIEW on ui_conn and verify query_con sees it cleanly
        pool.eval_ui_conn(|conn| {
            conn.execute_batch("CREATE OR REPLACE VIEW v_m1_test AS SELECT 42 AS answer, 'ok' AS status;")
                .map_err(|e| e.to_string())
        })
        .await
        .expect("Creating view on ui_conn should succeed");

        use tauri::ipc::IpcResponse;
        let resp = pool
            .eval_query_arrow_cancellable("SELECT answer, status FROM v_m1_test".to_string(), 5_000)
            .await
            .expect("Cancellable query should see views created on ui_conn");
        let resp_ref = pool
            .eval_query_arrow("SELECT answer, status FROM v_m1_test".to_string())
            .await
            .expect("Standard query should also succeed");
        match (
            resp.body().expect("Response body should unwrap"),
            resp_ref.body().expect("Ref response body should unwrap"),
        ) {
            (
                tauri::ipc::InvokeResponseBody::Raw(bytes),
                tauri::ipc::InvokeResponseBody::Raw(ref_bytes),
            ) => {
                assert!(bytes.starts_with(&[0xFF, 0xFF, 0xFF, 0xFF]));
                let r1 = duckdb::arrow::ipc::reader::StreamReader::try_new(
                    std::io::Cursor::new(&bytes),
                    None,
                )
                .expect("Decode cancellable Arrow IPC");
                let r2 = duckdb::arrow::ipc::reader::StreamReader::try_new(
                    std::io::Cursor::new(&ref_bytes),
                    None,
                )
                .expect("Decode reference Arrow IPC");
                let s1 = r1.schema();
                let s2 = r2.schema();
                assert_eq!(s1.fields(), s2.fields());
                assert_eq!(s1.metadata().get("application"), s2.metadata().get("application"));
                let ts1: serde_json::Value = serde_json::from_str(
                    s1.metadata().get("toadSchema").expect("toadSchema metadata"),
                )
                .unwrap();
                let ts2: serde_json::Value = serde_json::from_str(
                    s2.metadata().get("toadSchema").expect("toadSchema metadata"),
                )
                .unwrap();
                assert_eq!(ts1, ts2);
                let b1: Vec<RecordBatch> = r1.map(|r| r.unwrap()).collect();
                let b2: Vec<RecordBatch> = r2.map(|r| r.unwrap()).collect();
                assert_eq!(b1.len(), b2.len());
                for (a, b) in b1.iter().zip(b2.iter()) {
                    assert_eq!(a.num_rows(), b.num_rows());
                    assert_eq!(a.columns(), b.columns());
                }
            }
            _ => panic!("Expected Raw Arrow IPC response body"),
        }

        // Verify acquire_worker_permit and run_read_worker_task
        {
            let _permit = pool
                .acquire_worker_permit()
                .await
                .expect("acquire_worker_permit should succeed");
        }
        let val = pool
            .run_read_worker_task(|conn| {
                let mut stmt = conn.prepare("SELECT 7 * 6").map_err(|e| e.to_string())?;
                stmt.query_row([], |r| r.get::<_, i64>(0))
                    .map_err(|e| e.to_string())
            })
            .await
            .expect("run_read_worker_task should succeed");
        assert_eq!(val, 42);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_eval_query_arrow_cancellable_timeout_and_supersede() {
        let temp_dir = std::env::temp_dir().join("pulex_m1_test_cancel");
        let _ = std::fs::remove_dir_all(&temp_dir);
        let pool = Arc::new(
            DuckDbEnginePool::new(temp_dir.clone(), 0, "auto".to_string())
                .expect("Pool init should succeed"),
        );

        let slow_sql = "SELECT sum(a.range + b.range) AS s FROM range(10000000) a, range(1000) b".to_string();

        // 1. Timeout watchdog test (150ms)
        let timeout_res = pool
            .eval_query_arrow_cancellable(slow_sql.clone(), 150)
            .await;
        match timeout_res {
            Err(msg) => assert!(
                msg.contains("timed out"),
                "Expected timeout error message, got: {}",
                msg
            ),
            Ok(_) => panic!("Expected slow query to time out after 150ms"),
        }

        // 2. Supersede cancellation test
        let pool_bg = pool.clone();
        let slow_handle = tokio::spawn(async move {
            pool_bg.eval_query_arrow_cancellable(slow_sql, 10_000).await
        });

        // Wait until the slow query is actively executing
        for _ in 0..100 {
            if pool.ui_query_active.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        let fast_res = pool
            .eval_query_arrow_cancellable("SELECT 1 AS fast".to_string(), 5_000)
            .await;
        assert!(fast_res.is_ok(), "Superseding fast query should succeed: {:?}", fast_res.err());

        let slow_res = slow_handle.await.expect("Slow task should join cleanly");
        match slow_res {
            Err(msg) => assert!(
                msg.contains("superseded"),
                "Expected supersede cancellation error, got: {}",
                msg
            ),
            Ok(_) => panic!("Expected slow query to be cancelled when superseded"),
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
