//! mod.rs
//!
//! Forensics & Ingestion Subsystem for Pulex Desktop (Milestone M5 / Features F18-F22)
//!
//! Exposes:
//! - 8KB Binary Guard & Format Inspector (`binary_guard`)
//! - Streaming UTF-8 / UTF-16LE / UTF-16BE / Multi-Member Gzip Transcoder (`transcode`)
//! - RFC-4180 CSV/TSV Delimiter Sniffer & Streaming Ingest (F18)
//! - EVTX Rust SIMD Binary Decoder (F19)
//! - JSONL / NDJSON Dynamic Schema Streamer (F20)
//! - Shortest-Job-First (SJF) Ingestion Scheduler & VIP Preemption (F21)
//! - Multi-Stage Progress Dispatcher (F22)
//! - Dirty Log Fallback Engine

pub mod binary_guard;
pub mod csv_streamer;
pub mod dirty_log;
pub mod evtx_parser;
pub mod io_monitor;
pub mod jsonl_streamer;
pub mod progress;
pub mod scheduler;
pub mod transcode;

pub use binary_guard::{inspect_file, FileFormatCategory};
pub use csv_streamer::{effective_extension, ingest_csv_file, sniff_delimiter, CsvIngestResult};
pub use dirty_log::{ingest_dirty_log, DirtyLogResult};
pub use evtx_parser::{create_evtx_arrow_schema, ingest_evtx_file, EvtxIngestResult};
pub use io_monitor::get_process_io_bytes;
pub use jsonl_streamer::{ingest_jsonl_file, JsonlIngestResult};
pub use progress::{IngestProgressPayload, ProgressDispatcher, ProgressTickerGuard};
pub use scheduler::{
    CancellationToken, IngestJob, IngestPriority, IngestQueueSnapshot, IngestStatus,
    IngestionScheduler,
};
pub use transcode::{detect_text_encoding, transcode_to_utf8, DetectedEncoding, TranscodeStats};
