//! state.rs
//!
//! Shared Concurrency AppState for Toad Desktop (Tauri v2)
//! - Thread-safe access via Arc<RwLock<...>> on Tokio async runtime.
//! - Hosts configuration, portable environment paths, and engine stubs ready for M3-M5.

use crate::engine::DuckDbEnginePool;
use crate::portable::{save_config, ToadConfig};
use std::collections::HashMap;
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
    pub derived_views: Arc<RwLock<HashMap<String, DerivedViewSpec>>>,
    pub view_parquet_paths: Arc<RwLock<HashMap<String, PathBuf>>>,
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
            view_parquet_paths: Arc::new(RwLock::new(HashMap::new())),
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

    /// Register a backing Parquet file path for a DuckDB view name (e.g., `tbl_<hash>`, `main_table`)
    pub async fn register_view_parquet_path(&self, view_name: String, parquet_path: PathBuf) {
        let mut map = self.view_parquet_paths.write().await;
        map.insert(view_name, parquet_path);
    }

    /// Look up the backing Parquet file path for a DuckDB view name
    pub async fn get_view_parquet_path(&self, view_name: &str) -> Option<PathBuf> {
        let map = self.view_parquet_paths.read().await;
        map.get(view_name).cloned()
    }

    /// Remove a stale view-to-parquet mapping
    pub async fn remove_view_parquet_path(&self, view_name: &str) {
        let mut map = self.view_parquet_paths.write().await;
        map.remove(view_name);
    }

    /// Clear all view-to-parquet mappings (e.g. after Delete All Projects)
    pub async fn clear_all_view_parquet_paths(&self) {
        let mut map = self.view_parquet_paths.write().await;
        map.clear();
    }

    /// Remove view-to-parquet mappings pointing inside a specific project temp directory.
    /// Returns the list of affected view names (e.g. `tbl_<hash>`, `main_table`).
    pub async fn remove_view_parquet_paths_for_project(
        &self,
        project_temp_dir: &std::path::Path,
    ) -> Vec<String> {
        let mut map = self.view_parquet_paths.write().await;
        let stale_views: Vec<String> = map
            .iter()
            .filter(|(_, pq)| pq.starts_with(project_temp_dir))
            .map(|(k, _)| k.clone())
            .collect();
        for k in &stale_views {
            map.remove(k);
        }
        stale_views
    }

    /// Remove view-to-parquet mappings matching a specific Parquet file path.
    pub async fn remove_view_parquet_paths_for_file(
        &self,
        parquet_path: &std::path::Path,
    ) -> Vec<String> {
        let mut map = self.view_parquet_paths.write().await;
        let target_norm = parquet_path.to_string_lossy().replace('\\', "/").to_lowercase();
        let stale_views: Vec<String> = map
            .iter()
            .filter(|(_, pq)| {
                pq.to_string_lossy().replace('\\', "/").to_lowercase() == target_norm
            })
            .map(|(k, _)| k.clone())
            .collect();
        for k in &stale_views {
            map.remove(k);
        }
        stale_views
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

    #[tokio::test]
    async fn test_view_parquet_path_invalidation_per_project_and_all() {
        let temp_dir = std::env::temp_dir().join(format!("pulex_test_state_inv_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let state = AppState::new(temp_dir.clone(), ToadConfig::default());
        let p1_dir = temp_dir.join("temp").join("rp2i6");
        let p2_dir = temp_dir.join("temp").join("rptme");

        state
            .register_view_parquet_path("tbl_17f24271".to_string(), p1_dir.join("test_all_cached_17f24271.parquet"))
            .await;
        state
            .register_view_parquet_path("tbl_e5609dd9".to_string(), p2_dir.join("output_1000_cached_e5609dd9.parquet"))
            .await;

        let removed = state.remove_view_parquet_paths_for_project(&p1_dir).await;
        assert_eq!(removed, vec!["tbl_17f24271".to_string()]);
        assert!(state.get_view_parquet_path("tbl_17f24271").await.is_none());
        assert!(state.get_view_parquet_path("tbl_e5609dd9").await.is_some());

        state.clear_all_view_parquet_paths().await;
        assert!(state.get_view_parquet_path("tbl_e5609dd9").await.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
