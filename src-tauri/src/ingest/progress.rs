//! progress.rs
//!
//! Multi-Stage & Real-Time Progress Dispatcher for Ingestion Pipelines (Milestone M5 / F22)
//! - Dispatches structured events over `toad:ingest-progress` with Toad-parity telemetry
//!   (`fileName`, `percent`, `percentage`, `speedMBs`, `etaSec`, `statusText`, `isOpen`).
//! - Debounces Stage 2 (mid-parsing) updates to avoid saturating WebView IPC.
//! - Provides an RAII `ProgressTickerGuard` that polls both `.parquet.tmp` output growth and
//!   Win32 `GetProcessIoCounters` in a lightweight background thread while blocking DuckDB `COPY`
//!   operations execute on multi-gigabyte files.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::ingest::io_monitor::get_process_io_bytes;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngestProgressPayload {
    pub file_id: String,
    pub file_path: String,
    pub file_name: String,
    pub stage: String, // "sniffing" | "parsing" | "writing" | "ready" | "failed" | "cancelled"
    pub percent: u8,   // 0..=100 (Toad alias)
    pub percentage: u8, // 0..=100 (canonical)
    pub rows_ingested: u64,
    pub elapsed_ms: u64,
    #[serde(rename = "speedMBs")]
    pub speed_mbs: f64,
    pub eta_sec: u64,
    pub status_text: String,
    pub is_open: bool,
    pub status: String, // "PENDING" | "RUNNING" | "READY" | "FAILED" | "CANCELLED"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

pub type ProgressCallback = Arc<dyn Fn(&IngestProgressPayload) + Send + Sync>;

/// Extract display basename from a file path string.
pub fn basename_from_path(file_path: &str) -> String {
    Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| file_path.to_string())
}

/// Format byte sizes into human-readable B / KB / MB / GB strings matching Toad's UI.
pub fn format_bytes_human(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else if b >= MB {
        format!("{:.1} MB", b / MB)
    } else if b >= KB {
        format!("{:.0} KB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}

/// RAII guard that stops the background I/O progress ticker thread immediately when dropped.
pub struct ProgressTickerGuard {
    stop_flag: Arc<AtomicBool>,
}

impl Drop for ProgressTickerGuard {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
    }
}

/// Multi-stage progress dispatcher emitting events to Tauri windows and registered subscribers.
#[derive(Clone)]
pub struct ProgressDispatcher {
    app_handle: Arc<RwLock<Option<tauri::AppHandle>>>,
    last_emit: Arc<Mutex<Instant>>,
    listeners: Arc<Mutex<Vec<ProgressCallback>>>,
}

impl Default for ProgressDispatcher {
    fn default() -> Self {
        Self::new(None)
    }
}

impl ProgressDispatcher {
    /// Create a new progress dispatcher with optional Tauri AppHandle
    pub fn new(app_handle: Option<tauri::AppHandle>) -> Self {
        let initial_time = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        Self {
            app_handle: Arc::new(RwLock::new(app_handle)),
            last_emit: Arc::new(Mutex::new(initial_time)),
            listeners: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Inject or replace the live Tauri AppHandle after `.setup(|app| ...)` completes
    pub fn set_app_handle(&self, handle: tauri::AppHandle) {
        if let Ok(mut guard) = self.app_handle.write() {
            *guard = Some(handle);
        }
    }

    /// Attach a listener callback (useful for testing and internal telemetry)
    pub fn add_listener(&self, callback: ProgressCallback) {
        if let Ok(mut list) = self.listeners.lock() {
            list.push(callback);
        }
    }

    /// Emit an event unconditionally to all targets
    pub fn emit(&self, payload: &IngestProgressPayload) {
        if let Ok(guard) = self.app_handle.read() {
            if let Some(ref handle) = *guard {
                use tauri::Emitter;
                let _ = handle.emit("toad:ingest-progress", payload);
            }
        }

        if let Ok(listeners) = self.listeners.lock() {
            for cb in listeners.iter() {
                cb(payload);
            }
        }
    }

    /// Throttled emission for Stage 2 (row parsing / streaming) to avoid saturating WebView IPC (min 100ms).
    /// Always emits immediately if percentage >= 100 or status is not "RUNNING".
    pub fn emit_throttled(&self, payload: &IngestProgressPayload) {
        let is_terminal =
            payload.percentage >= 100 || payload.status != "RUNNING" || payload.stage != "parsing";
        let should_emit = if is_terminal {
            true
        } else if let Ok(mut last) = self.last_emit.lock() {
            if last.elapsed().as_millis() >= 100 {
                *last = Instant::now();
                true
            } else {
                false
            }
        } else {
            true
        };

        if should_emit {
            self.emit(payload);
        }
    }

    /// Helper to emit rich real-time progress (percent, speedMBs, etaSec, statusText)
    #[allow(clippy::too_many_arguments)]
    pub fn emit_live_progress(
        &self,
        file_id: &str,
        file_path: &str,
        rows: u64,
        percentage: u8,
        elapsed_ms: u64,
        speed_mbs: f64,
        eta_sec: u64,
        status_text: impl Into<String>,
    ) {
        let clamped = percentage.clamp(2, 92);
        self.emit_throttled(&IngestProgressPayload {
            file_id: file_id.to_string(),
            file_path: file_path.to_string(),
            file_name: basename_from_path(file_path),
            stage: "parsing".to_string(),
            percent: clamped,
            percentage: clamped,
            rows_ingested: rows,
            elapsed_ms,
            speed_mbs,
            eta_sec,
            status_text: status_text.into(),
            is_open: true,
            status: "RUNNING".to_string(),
            error_message: None,
        });
    }

    /// Spawn a background progress ticker thread that tracks both `.parquet.tmp` output growth
    /// and Win32 `GetProcessIoCounters` while a blocking DuckDB `COPY` (or transcode) executes.
    /// `display_source_path` is emitted as `filePath`/`fileName` so frontend matching always sees
    /// the original user file, while `measure_source_path` supplies the byte size being read.
    /// Automatically terminates when the returned `ProgressTickerGuard` is dropped.
    pub fn spawn_copy_progress_ticker(
        &self,
        file_id: &str,
        display_source_path: &Path,
        measure_source_path: &Path,
        temp_parquet_path: &Path,
        expected_compression_ratio: f64,
    ) -> ProgressTickerGuard {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_clone = stop_flag.clone();
        let disp = self.clone();
        let fid = file_id.to_string();
        let src_str = display_source_path.to_string_lossy().to_string();
        let src_bytes = std::fs::metadata(measure_source_path).map(|m| m.len()).unwrap_or(0);
        let dst_path: PathBuf = temp_parquet_path.to_path_buf();
        let ratio = expected_compression_ratio.clamp(0.05, 1.0);

        std::thread::spawn(move || {
            let t0 = Instant::now();
            let initial_io_read = get_process_io_bytes(0).map(|(r, _)| r);
            let mut last_sample_time = Instant::now();
            let mut last_metric_bytes: u64 = 0;
            let mut smoothed_speed_mbs: f64 = 0.0;
            let mut peak_percent: u8 = 5;

            while !stop_clone.load(Ordering::SeqCst) {
                // Sleep in small increments so guard Drop exits quickly without waiting 800ms
                for _ in 0..8 {
                    if stop_clone.load(Ordering::SeqCst) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                if stop_clone.load(Ordering::SeqCst) {
                    return;
                }

                let now = Instant::now();
                let dt = now.duration_since(last_sample_time).as_secs_f64();
                let elapsed_ms = t0.elapsed().as_millis() as u64;

                // Measure both output Parquet file growth and OS process read bytes
                let dst_bytes = std::fs::metadata(&dst_path).map(|m| m.len()).unwrap_or(0);
                let io_read_delta = initial_io_read
                    .and_then(|init| get_process_io_bytes(0).map(|(cur, _)| cur.saturating_sub(init)));

                // Prefer Win32 process read_bytes when actively reading input (matches Toad's primary meter);
                // fall back to `.parquet.tmp` output growth when OS I/O counter is unavailable or 0.
                let (pct, speed_mbs, eta_sec, status_text) = if let Some(delta_read) = io_read_delta {
                    if delta_read > 0 && src_bytes > 0 {
                        if dt >= 0.4 {
                            let step_bytes = delta_read.saturating_sub(last_metric_bytes);
                            let instant_mbs = (step_bytes as f64 / (1024.0 * 1024.0)) / dt;
                            smoothed_speed_mbs = if smoothed_speed_mbs <= 0.001 {
                                instant_mbs
                            } else {
                                smoothed_speed_mbs * 0.7 + instant_mbs * 0.3
                            };
                            last_metric_bytes = delta_read;
                            last_sample_time = now;
                        }
                        let capped_read = delta_read.min(src_bytes);
                        let read_ratio = (capped_read as f64 / src_bytes.max(1) as f64).clamp(0.0, 1.0);
                        let raw_pct = (5.0 + read_ratio * 85.0).round().clamp(5.0, 90.0) as u8;
                        peak_percent = peak_percent.max(raw_pct);
                        let eta = if smoothed_speed_mbs > 0.1 {
                            let rem_bytes = src_bytes.saturating_sub(capped_read);
                            ((rem_bytes as f64 / (1024.0 * 1024.0)) / smoothed_speed_mbs).round() as u64
                        } else {
                            0
                        };
                        let text = if peak_percent < 90 {
                            format!(
                                "Reading input: {} / {}",
                                format_bytes_human(capped_read),
                                format_bytes_human(src_bytes)
                            )
                        } else {
                            "Finalizing Parquet index & loading dataset...".to_string()
                        };
                        (peak_percent, (smoothed_speed_mbs * 10.0).round() / 10.0, eta, text)
                    } else if dst_bytes > 0 {
                        let expected_dst = ((src_bytes as f64 * ratio) as u64).max(1024 * 1024);
                        if dt >= 0.4 {
                            let step_bytes = dst_bytes.saturating_sub(last_metric_bytes);
                            let instant_mbs = (step_bytes as f64 / (1024.0 * 1024.0)) / dt;
                            smoothed_speed_mbs = if smoothed_speed_mbs <= 0.001 {
                                instant_mbs
                            } else {
                                smoothed_speed_mbs * 0.7 + instant_mbs * 0.3
                            };
                            last_metric_bytes = dst_bytes;
                            last_sample_time = now;
                        }
                        let dst_ratio = (dst_bytes as f64 / expected_dst as f64).clamp(0.0, 1.0);
                        let raw_pct = (5.0 + dst_ratio * 85.0).round().clamp(5.0, 90.0) as u8;
                        peak_percent = peak_percent.max(raw_pct);
                        let eta = if smoothed_speed_mbs > 0.1 {
                            let rem_bytes = expected_dst.saturating_sub(dst_bytes);
                            ((rem_bytes as f64 / (1024.0 * 1024.0)) / smoothed_speed_mbs).round() as u64
                        } else {
                            0
                        };
                        let text = format!(
                            "Streaming to Parquet: {} written",
                            format_bytes_human(dst_bytes)
                        );
                        (peak_percent, (smoothed_speed_mbs * 10.0).round() / 10.0, eta, text)
                    } else {
                        (
                            peak_percent,
                            0.0,
                            0,
                            format!("Analyzing {}...", basename_from_path(&src_str)),
                        )
                    }
                } else if dst_bytes > 0 {
                    let expected_dst = ((src_bytes as f64 * ratio) as u64).max(1024 * 1024);
                    if dt >= 0.4 {
                        let step_bytes = dst_bytes.saturating_sub(last_metric_bytes);
                        let instant_mbs = (step_bytes as f64 / (1024.0 * 1024.0)) / dt;
                        smoothed_speed_mbs = if smoothed_speed_mbs <= 0.001 {
                            instant_mbs
                        } else {
                            smoothed_speed_mbs * 0.7 + instant_mbs * 0.3
                        };
                        last_metric_bytes = dst_bytes;
                        last_sample_time = now;
                    }
                    let dst_ratio = (dst_bytes as f64 / expected_dst as f64).clamp(0.0, 1.0);
                    let raw_pct = (5.0 + dst_ratio * 85.0).round().clamp(5.0, 90.0) as u8;
                    peak_percent = peak_percent.max(raw_pct);
                    let eta = if smoothed_speed_mbs > 0.1 {
                        let rem_bytes = expected_dst.saturating_sub(dst_bytes);
                        ((rem_bytes as f64 / (1024.0 * 1024.0)) / smoothed_speed_mbs).round() as u64
                    } else {
                        0
                    };
                    let text = format!(
                        "Streaming to Parquet: {} written",
                        format_bytes_human(dst_bytes)
                    );
                    (peak_percent, (smoothed_speed_mbs * 10.0).round() / 10.0, eta, text)
                } else {
                    (
                        peak_percent,
                        0.0,
                        0,
                        format!("Processing {}...", basename_from_path(&src_str)),
                    )
                };

                disp.emit_live_progress(
                    &fid,
                    &src_str,
                    0,
                    pct,
                    elapsed_ms,
                    speed_mbs,
                    eta_sec,
                    status_text,
                );
            }
        });

        ProgressTickerGuard { stop_flag }
    }

    /// Helper to construct and emit Stage 1: Sniffing (5%)
    pub fn emit_stage_sniffing(&self, file_id: &str, file_path: &str, elapsed_ms: u64) {
        let fname = basename_from_path(file_path);
        self.emit(&IngestProgressPayload {
            file_id: file_id.to_string(),
            file_path: file_path.to_string(),
            file_name: fname.clone(),
            stage: "sniffing".to_string(),
            percent: 5,
            percentage: 5,
            rows_ingested: 0,
            elapsed_ms,
            speed_mbs: 0.0,
            eta_sec: 0,
            status_text: format!("Inspecting schema for {}...", fname),
            is_open: true,
            status: "RUNNING".to_string(),
            error_message: None,
        });
    }

    /// Helper to construct and emit Stage 2: Parsing (mid-stage, debounced)
    pub fn emit_stage_parsing(
        &self,
        file_id: &str,
        file_path: &str,
        rows: u64,
        percentage: u8,
        elapsed_ms: u64,
    ) {
        let clamped = percentage.clamp(10, 85);
        let status_text = if rows > 0 {
            format!("Parsing records ({} rows)...", rows)
        } else {
            "Parsing & streaming dataset...".to_string()
        };
        self.emit_throttled(&IngestProgressPayload {
            file_id: file_id.to_string(),
            file_path: file_path.to_string(),
            file_name: basename_from_path(file_path),
            stage: "parsing".to_string(),
            percent: clamped,
            percentage: clamped,
            rows_ingested: rows,
            elapsed_ms,
            speed_mbs: 0.0,
            eta_sec: 0,
            status_text,
            is_open: true,
            status: "RUNNING".to_string(),
            error_message: None,
        });
    }

    /// Helper to construct and emit Stage 3: Writing Parquet (90%)
    pub fn emit_stage_writing(
        &self,
        file_id: &str,
        file_path: &str,
        total_rows: u64,
        elapsed_ms: u64,
    ) {
        self.emit(&IngestProgressPayload {
            file_id: file_id.to_string(),
            file_path: file_path.to_string(),
            file_name: basename_from_path(file_path),
            stage: "writing".to_string(),
            percent: 90,
            percentage: 90,
            rows_ingested: total_rows,
            elapsed_ms,
            speed_mbs: 0.0,
            eta_sec: 0,
            status_text: "Finalizing Parquet index & loading dataset...".to_string(),
            is_open: true,
            status: "RUNNING".to_string(),
            error_message: None,
        });
    }

    /// Helper to construct and emit Stage 4: Ready (100%)
    pub fn emit_stage_ready(
        &self,
        file_id: &str,
        file_path: &str,
        total_rows: u64,
        elapsed_ms: u64,
    ) {
        self.emit(&IngestProgressPayload {
            file_id: file_id.to_string(),
            file_path: file_path.to_string(),
            file_name: basename_from_path(file_path),
            stage: "ready".to_string(),
            percent: 100,
            percentage: 100,
            rows_ingested: total_rows,
            elapsed_ms,
            speed_mbs: 0.0,
            eta_sec: 0,
            status_text: "Ready".to_string(),
            is_open: false,
            status: "READY".to_string(),
            error_message: None,
        });
    }

    /// Helper to construct and emit Failed
    pub fn emit_failed(&self, file_id: &str, file_path: &str, error_msg: &str, elapsed_ms: u64) {
        self.emit(&IngestProgressPayload {
            file_id: file_id.to_string(),
            file_path: file_path.to_string(),
            file_name: basename_from_path(file_path),
            stage: "failed".to_string(),
            percent: 0,
            percentage: 0,
            rows_ingested: 0,
            elapsed_ms,
            speed_mbs: 0.0,
            eta_sec: 0,
            status_text: format!("Error: {}", error_msg),
            is_open: false,
            status: "FAILED".to_string(),
            error_message: Some(error_msg.to_string()),
        });
    }

    /// Helper to construct and emit Cancelled
    pub fn emit_cancelled(&self, file_id: &str, file_path: &str, elapsed_ms: u64) {
        self.emit(&IngestProgressPayload {
            file_id: file_id.to_string(),
            file_path: file_path.to_string(),
            file_name: basename_from_path(file_path),
            stage: "cancelled".to_string(),
            percent: 0,
            percentage: 0,
            rows_ingested: 0,
            elapsed_ms,
            speed_mbs: 0.0,
            eta_sec: 0,
            status_text: "Cancelled".to_string(),
            is_open: false,
            status: "CANCELLED".to_string(),
            error_message: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_payload_toad_parity_serialization() {
        let disp = ProgressDispatcher::new(None);
        let captured = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let cap_clone = captured.clone();
        disp.add_listener(Arc::new(move |p| {
            cap_clone
                .lock()
                .unwrap()
                .push(serde_json::to_value(p).unwrap());
        }));

        disp.emit_live_progress(
            "f_big",
            "D:\\data\\sample_40gb.csv",
            125_000,
            37,
            4200,
            412.5,
            61,
            "Reading input: 16.40 GB / 40.00 GB",
        );

        let list = captured.lock().unwrap();
        assert_eq!(list.len(), 1);
        let v = &list[0];
        assert_eq!(v["fileName"], "sample_40gb.csv");
        assert_eq!(v["percent"], 37);
        assert_eq!(v["percentage"], 37);
        assert_eq!(v["speedMBs"], 412.5);
        assert_eq!(v["etaSec"], 61);
        assert_eq!(v["statusText"], "Reading input: 16.40 GB / 40.00 GB");
        assert_eq!(v["isOpen"], true);
    }
}
