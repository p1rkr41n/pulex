//! state.rs
//!
//! Shared Concurrency AppState for Toad Desktop (Tauri v2)
//! - Thread-safe access via Arc<RwLock<...>> on Tokio async runtime.
//! - Hosts configuration, portable environment paths, and engine stubs ready for M3-M5.

use crate::engine::DuckDbEnginePool;
use crate::portable::{save_config, ToadConfig};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// DuckDB engine connection pool state
#[derive(Clone, Default)]
pub struct DuckDbEnginePlaceholder {
    pub initialized: bool,
    pub active_queries: usize,
    pub pool: Option<Arc<DuckDbEnginePool>>,
}

use crate::ledger::ParquetLedger;

use crate::ingest::{IngestionScheduler, ProgressDispatcher};

/// Parquet Double-Valid State Ledger WAL (Milestone M4)
pub type ParquetLedgerPlaceholder = ParquetLedger;

/// Active Ingestion Scheduler & Forensics Queue (Milestone M5 / F21)
pub type IngestionQueuePlaceholder = IngestionScheduler;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedViewSpec {
    pub view_name: String,
    pub base_table: String,
    pub sql_ddl: String,
    pub source_column: Option<String>,
    pub regex_pattern: Option<String>,
    pub created_at_ms: i64,
}

/// Core thread-safe application state managed by Tauri runtime
pub struct AppState {
    pub portable_dir: PathBuf,
    pub config_path: PathBuf,
    pub config: Arc<RwLock<ToadConfig>>,
    pub duckdb_engine: Arc<RwLock<DuckDbEnginePlaceholder>>,
    pub parquet_ledger: Arc<RwLock<ParquetLedger>>,
    pub ingest_queue: Arc<RwLock<IngestionScheduler>>,
    pub derived_views: Arc<RwLock<std::collections::HashMap<String, DerivedViewSpec>>>,
}

impl AppState {
    /// Construct AppState from portable directory and initial config
    pub fn new(portable_dir: PathBuf, config: ToadConfig) -> Self {
        let config_path = portable_dir.join("config.ini");
        let temp_dir = portable_dir.join("temp");
        let _ = std::fs::create_dir_all(&temp_dir);
        let pool = DuckDbEnginePool::new_with_background_threads(
            temp_dir.clone(),
            config.duckdb.threads,
            config.duckdb.background_threads.clone(),
            config.duckdb.max_memory.clone(),
        )
        .ok()
        .map(Arc::new);

        let placeholder = DuckDbEnginePlaceholder {
            initialized: pool.is_some(),
            active_queries: 0,
            pool: pool.clone(),
        };
        let ledger = ParquetLedger::new(portable_dir.clone());
        let _ = {
            let reconciler = crate::ledger::StartupReconciler::new(
                &ledger.ledger_path,
                &ledger.backup_path,
                &ledger.temp_dir,
                &ledger.logs_dir,
                true,
            );
            if let Ok(mut st) = ledger.state.try_write() {
                let _ = reconciler.reconcile(&mut st);
            }
        };

        let config_arc = Arc::new(RwLock::new(config.clone()));
        let ledger_arc = Arc::new(RwLock::new(ledger));
        let mut scheduler = IngestionScheduler::new(
            temp_dir,
            Some(ledger_arc.clone()),
            pool,
            Arc::new(ProgressDispatcher::default()),
            config.general.max_concurrent_files as usize,
            (config.general.max_concurrent_gb * 1_073_741_824.0) as u64,
        );
        scheduler.set_config(config_arc.clone());

        let derived_path = portable_dir.join("derived_views.json");
        let initial_derived: std::collections::HashMap<String, DerivedViewSpec> =
            if derived_path.exists() {
                std::fs::read_to_string(&derived_path)
                    .ok()
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default()
            } else {
                std::collections::HashMap::new()
            };

        Self {
            portable_dir,
            config_path,
            config: config_arc,
            duckdb_engine: Arc::new(RwLock::new(placeholder)),
            parquet_ledger: ledger_arc,
            ingest_queue: Arc::new(RwLock::new(scheduler)),
            derived_views: Arc::new(RwLock::new(initial_derived)),
        }
    }

    /// Inject live Tauri AppHandle into ProgressDispatcher and ParquetLedger
    pub async fn attach_app_handle(&self, handle: tauri::AppHandle) {
        {
            let sched = self.ingest_queue.read().await;
            sched.dispatcher.set_app_handle(handle.clone());
        }
        {
            let ledger = self.parquet_ledger.read().await;
            ledger.set_app_handle(handle);
        }
    }

    /// Persist derived_views registry to `derived_views.json`
    pub async fn register_derived_view(&self, spec: DerivedViewSpec) -> Result<(), String> {
        let mut map = self.derived_views.write().await;
        map.insert(spec.view_name.clone(), spec);
        let derived_path = self.portable_dir.join("derived_views.json");
        if let Ok(json_str) = serde_json::to_string_pretty(&*map) {
            let _ = std::fs::write(&derived_path, json_str);
        }
        Ok(())
    }

    /// Look up a registered derived view by name
    pub async fn get_derived_view(&self, view_name: &str) -> Option<DerivedViewSpec> {
        let map = self.derived_views.read().await;
        map.get(view_name).cloned()
    }

    /// Retrieve or lazily initialize the DuckDB Engine Pool
    pub async fn get_duckdb_pool(&self) -> Arc<DuckDbEnginePool> {
        let mut engine = self.duckdb_engine.write().await;
        if let Some(ref pool) = engine.pool {
            return pool.clone();
        }
        let temp_dir = self.portable_dir.join("temp");
        let _ = std::fs::create_dir_all(&temp_dir);
        let cfg = self.config.read().await;
        let pool = Arc::new(
            DuckDbEnginePool::new(temp_dir, cfg.duckdb.threads, cfg.duckdb.max_memory.clone())
                .unwrap_or_else(|_| {
                    DuckDbEnginePool::new(std::env::temp_dir(), 2, "2GB".to_string())
                        .expect("Fatal: cannot create DuckDB in-memory pool")
                }),
        );
        engine.pool = Some(pool.clone());
        engine.initialized = true;
        pool
    }

    /// Read a clone of the current configuration snapshot
    pub async fn get_config(&self) -> ToadConfig {
        self.config.read().await.clone()
    }

    /// Retrieve or lazily initialize the Ingestion Scheduler
    pub async fn get_ingest_scheduler(&self) -> Arc<IngestionScheduler> {
        let sched = self.ingest_queue.read().await.clone();
        Arc::new(sched)
    }

    /// Update configuration in memory and persist to config.ini on disk with atomic rollback safety.
    ///
    /// Evaluates the update on a candidate clone and persists to disk FIRST.
    /// In-memory state is only committed if disk I/O succeeds, preventing memory/disk drift on failure.
    pub async fn update_config<F>(&self, update_fn: F) -> std::io::Result<ToadConfig>
    where
        F: FnOnce(&mut ToadConfig),
    {
        let mut cfg = self.config.write().await;
        let mut candidate = cfg.clone();
        update_fn(&mut candidate);
        save_config(&self.config_path, &candidate)?;
        *cfg = candidate.clone();
        Ok(candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_app_state_concurrency() {
        let temp_dir = std::env::temp_dir().join("toad_test_state");
        let _ = std::fs::create_dir_all(&temp_dir);

        let initial_cfg = ToadConfig::default();
        let state = AppState::new(temp_dir.clone(), initial_cfg);

        // Read snapshot
        let cfg = state.get_config().await;
        assert_eq!(cfg.general.viewer, "pulex");

        // Mutate and persist
        let updated = state
            .update_config(|c| {
                c.general.page_size = 2000;
                c.duckdb.threads = 4;
            })
            .await
            .unwrap();

        assert_eq!(updated.general.page_size, 2000);
        assert_eq!(updated.duckdb.threads, 4);

        let reread = state.get_config().await;
        assert_eq!(reread.general.page_size, 2000);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
