//! logger.rs
//!
//! Structured Tracing, File Logging, and Panic Guard for Toad Desktop
//! - Configures dual-target logging (stdout + toadAppData/logs/toad.log).
//! - Custom panic hook to trap fatal crashes into structured logs.
//! - Standardized ErrorResponse DTOs for IPC error reporting.

use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::Mutex;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

/// Standard structured error response DTO returned over Tauri IPC
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ErrorResponse {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(
        code: impl Into<String>,
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: Some(details),
        }
    }

    pub fn not_implemented(feature: impl Into<String>) -> Self {
        Self::new(
            "ERR_NOT_IMPLEMENTED",
            format!("Feature not yet hooked: {}", feature.into()),
        )
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new("ERR_INVALID_ARGUMENT", message)
    }

    pub fn io_error(err: &std::io::Error) -> Self {
        Self::new("ERR_IO", err.to_string())
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new("ERR_INTERNAL", message)
    }
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}",
            self.code,
            self.message
        )
    }
}

impl std::error::Error for ErrorResponse {}

const MAX_LOG_SIZE_BYTES: u64 = 20 * 1024 * 1024; // 20 MB per file
const MAX_LOG_ROTATIONS: usize = 5; // Up to 5 rotated files (100 MB cap)

fn rotate_logs_if_needed(logs_dir: &Path, base_name: &str) {
    let active_path = logs_dir.join(base_name);
    let needs_rotate = fs::metadata(&active_path)
        .map(|m| m.len() >= MAX_LOG_SIZE_BYTES)
        .unwrap_or(false);
    if !needs_rotate {
        return;
    }

    let oldest = logs_dir.join(format!("{}.{}", base_name, MAX_LOG_ROTATIONS));
    let _ = fs::remove_file(&oldest);

    for idx in (1..MAX_LOG_ROTATIONS).rev() {
        let src = logs_dir.join(format!("{}.{}", base_name, idx));
        let dst = logs_dir.join(format!("{}.{}", base_name, idx + 1));
        if src.exists() {
            let _ = fs::rename(&src, &dst);
        }
    }

    let first_rot = logs_dir.join(format!("{}.1", base_name));
    let _ = fs::rename(&active_path, &first_rot);
}

/// Initialize structured tracing to both stdout and pulex.log (with 20MB x 5 rotation)
pub fn init_logger(logs_dir: &Path, log_level: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(logs_dir)?;
    rotate_logs_if_needed(logs_dir, "pulex.log");
    let log_file_path = logs_dir.join("pulex.log");

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)?;

    let level_filter = match log_level.to_lowercase().as_str() {
        "trace" => LevelFilter::TRACE,
        "debug" => LevelFilter::DEBUG,
        "info" => LevelFilter::INFO,
        "warn" => LevelFilter::WARN,
        "error" => LevelFilter::ERROR,
        _ => LevelFilter::INFO,
    };

    let stdout_filter = Targets::new()
        .with_default(level_filter)
        .with_target("evtx", LevelFilter::WARN);

    let file_filter = Targets::new()
        .with_default(level_filter)
        .with_target("evtx", LevelFilter::WARN);

    let stdout_layer = fmt::layer()
        .with_ansi(true)
        .with_target(true)
        .with_filter(stdout_filter);

    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_writer(Mutex::new(file))
        .with_filter(file_filter);

    let subscriber = tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer);

    // Using try_init so re-calling in tests won't panic
    let _ = subscriber.try_init();

    setup_panic_hook(logs_dir);

    tracing::info!(
        log_level = %log_level,
        log_path = %log_file_path.display(),
        "Pulex logger initialized successfully"
    );

    Ok(())
}

/// Install global panic hook to log panics into panic.log and toad.log
pub fn setup_panic_hook(logs_dir: &Path) {
    let panic_log_path = logs_dir.join("panic.log");

    std::panic::set_hook(Box::new(move |panic_info| {
        let payload = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic payload".to_string()
        };

        let location = panic_info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown location".to_string());

        let msg = format!(
            "FATAL PANIC: payload='{}' at {}\nBacktrace:\n{:?}",
            payload,
            location,
            std::backtrace::Backtrace::capture()
        );

        tracing::error!("{}", msg);
        eprintln!("{}", msg);

        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&panic_log_path)
        {
            use std::io::Write;
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let _ = writeln!(f, "[{}] {}", timestamp, msg);
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_response_serialization() {
        let err = ErrorResponse::new("ERR_TEST", "Test error message");
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("ERR_TEST"));
        assert!(json.contains("Test error message"));
        assert!(!json.contains("details"));

        let err_with_details = ErrorResponse::with_details(
            "ERR_WITH_DETAILS",
            "Detailed error",
            serde_json::json!({ "file": "data.csv", "line": 42 }),
        );
        let json2 = serde_json::to_string(&err_with_details).unwrap();
        assert!(json2.contains("data.csv"));
    }

    #[test]
    fn test_logger_initialization() {
        let temp_logs = std::env::temp_dir().join(format!("toad_test_logs_{}", std::process::id()));
        let res = init_logger(&temp_logs, "debug");
        assert!(res.is_ok());
        let _ = fs::remove_dir_all(&temp_logs);
    }
}
