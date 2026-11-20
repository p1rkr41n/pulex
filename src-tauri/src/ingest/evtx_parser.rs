//! evtx_parser.rs
//!
//! Windows Event Log (.evtx) Binary Forensics Ingestion Engine (Milestone M5 / F19)
//! - Uses Rust SIMD `evtx = "0.8"` binary parser in `tokio::task::spawn_blocking`.
//! - Streams directly into Apache Arrow `RecordBatch`es (32K batch size) and writes
//!   to `parquet::arrow::ArrowWriter` with Snappy compression (<30MB flat RAM, 0 disk churn).
//! - Extracts 16 typed forensic columns (preserving indices 0..8 and enriching with Toad DFIR fields):
//!   `record_id`, `time_created`, `event_id`, `provider_name`, `level`, `channel`, `computer`,
//!   `user_id`, `payload_json`, `level_name`, `param1`, `param2`, `param3`, `binary`, `event_data`, `raw_event`.
//! - Resilient extractors handling polymorphic `EventID` and corrupted chunk/record recovery.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use evtx::{EvtxParser, ParserSettings};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

use crate::ingest::progress::ProgressDispatcher;
use crate::ingest::scheduler::CancellationToken;

#[derive(Debug, Clone, Default)]
pub struct EvtxIngestResult {
    pub row_count: i64,
    pub parquet_size: i64,
    pub duration_ms: i64,
    pub corrupted_records: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ExtractedEvtxFields {
    pub event_id: Option<i32>,
    pub provider: Option<String>,
    pub level: Option<i32>,
    pub level_name: Option<String>,
    pub channel: Option<String>,
    pub computer: Option<String>,
    pub user_id: Option<String>,
    pub payload_json: Option<String>,
    pub param1: Option<String>,
    pub param2: Option<String>,
    pub param3: Option<String>,
    pub binary: Option<String>,
    pub event_data: Option<String>,
    pub raw_event: Option<String>,
}

pub fn create_evtx_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("record_id", DataType::Int64, false),
        Field::new(
            "time_created",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_id", DataType::Int32, true),
        Field::new("provider_name", DataType::Utf8, true),
        Field::new("level", DataType::Int32, true),
        Field::new("channel", DataType::Utf8, true),
        Field::new("computer", DataType::Utf8, true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("payload_json", DataType::Utf8, true),
        Field::new("level_name", DataType::Utf8, true),
        Field::new("param1", DataType::Utf8, true),
        Field::new("param2", DataType::Utf8, true),
        Field::new("param3", DataType::Utf8, true),
        Field::new("binary", DataType::Utf8, true),
        Field::new("event_data", DataType::Utf8, true),
        Field::new("raw_event", DataType::Utf8, true),
    ]))
}

fn extract_val_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Object(o) => {
            if let Some(t) = o.get("#text") {
                extract_val_string(t)
            } else {
                v.to_string()
            }
        }
        Value::Null => String::new(),
        _ => v.to_string(),
    }
}

/// Extract polymorphic EventID: integer scalar, string, or object with `{"#text": 4624}`
pub fn extract_event_id(system: &Value) -> Option<i32> {
    let eid_val = system.get("EventID")?;
    match eid_val {
        Value::Number(n) => n.as_i64().and_then(|v| i32::try_from(v).ok()),
        Value::String(s) => s.trim().parse::<i32>().ok(),
        Value::Object(map) => {
            if let Some(text_val) = map.get("#text") {
                match text_val {
                    Value::Number(n) => n.as_i64().and_then(|v| i32::try_from(v).ok()),
                    Value::String(s) => s.trim().parse::<i32>().ok(),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract provider name from `Event.System.Provider["#attributes"]["Name"]` or string
pub fn extract_provider_name(system: &Value) -> Option<String> {
    let provider = system.get("Provider")?;
    if let Some(name) = provider
        .get("#attributes")
        .and_then(|a| a.get("Name"))
        .and_then(|v| v.as_str())
    {
        return Some(name.to_string());
    }
    if let Some(name) = provider.get("Name").and_then(|v| v.as_str()) {
        return Some(name.to_string());
    }
    provider.as_str().map(|s| s.to_string())
}

/// Extract Level from `Event.System.Level`
pub fn extract_level(system: &Value) -> Option<i32> {
    let lvl = system.get("Level")?;
    match lvl {
        Value::Number(n) => n.as_i64().and_then(|v| i32::try_from(v).ok()),
        Value::String(s) => s.trim().parse::<i32>().ok(),
        Value::Object(map) => map.get("#text").and_then(|v| {
            v.as_i64()
                .and_then(|i| i32::try_from(i).ok())
                .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
        }),
        _ => None,
    }
}

pub fn level_to_name(level: Option<i32>) -> &'static str {
    match level {
        Some(1) => "Critical",
        Some(2) => "Error",
        Some(3) => "Warning",
        Some(4) | Some(0) => "Information",
        Some(5) => "Verbose",
        _ => "Information",
    }
}

/// Extract Channel from `Event.System.Channel`
pub fn extract_channel(system: &Value) -> Option<String> {
    let ch = system.get("Channel")?;
    match ch {
        Value::String(s) => Some(s.clone()),
        Value::Object(map) => map.get("#text").and_then(|v| v.as_str()).map(|s| s.to_string()),
        _ => None,
    }
}

/// Extract Computer from `Event.System.Computer`
pub fn extract_computer(system: &Value) -> Option<String> {
    let c = system.get("Computer")?;
    match c {
        Value::String(s) => Some(s.clone()),
        Value::Object(map) => map.get("#text").and_then(|v| v.as_str()).map(|s| s.to_string()),
        _ => None,
    }
}

/// Extract UserID / SID from System.Security or EventData fallback
pub fn extract_user_id(system: &Value, event_data: Option<&Value>) -> Option<String> {
    if let Some(sec) = system.get("Security") {
        if let Some(uid) = sec
            .get("#attributes")
            .and_then(|a| a.get("UserID"))
            .and_then(|v| v.as_str())
        {
            if !uid.is_empty() {
                return Some(uid.to_string());
            }
        }
        if let Some(uid) = sec.get("UserID").and_then(|v| v.as_str()) {
            if !uid.is_empty() {
                return Some(uid.to_string());
            }
        }
    }
    if let Some(ed) = event_data {
        for key in &["TargetUserSid", "SubjectUserSid", "TargetUserName", "SubjectUserName"] {
            if let Some(val) = ed.get(*key).and_then(|v| v.as_str()) {
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }
    None
}

/// Extract rich DFIR fields (including param1..param3, Binary, EventData summary, RawEvent)
pub fn extract_rich_evtx_fields(val: &Value, include_raw_event: bool) -> ExtractedEvtxFields {
    let root = val.get("Event").unwrap_or(val);
    let system = root.get("System");
    let event_data_node = root.get("EventData");
    let user_data_node = root.get("UserData");

    let event_id = system.and_then(extract_event_id);
    let provider = system.and_then(extract_provider_name);
    let level = system.and_then(extract_level);
    let level_name = Some(level_to_name(level).to_string());
    let channel = system.and_then(extract_channel);
    let computer = system.and_then(extract_computer);

    let mut p1 = String::new();
    let mut p2 = String::new();
    let mut p3 = String::new();
    let mut binary_val = String::new();
    let mut event_data_summary = serde_json::Map::new();
    let mut positional_candidates: Vec<String> = Vec::new();

    if let Some(ed) = event_data_node {
        if let Some(data_map) = ed.as_object() {
            for (k, v) in data_map {
                if k == "Data" {
                    if let Some(arr) = v.as_array() {
                        for (i, item) in arr.iter().enumerate() {
                            let text_val = extract_val_string(item);
                            if let Some(name) = item
                                .get("#attributes")
                                .and_then(|a| a.get("Name"))
                                .and_then(|n| n.as_str())
                            {
                                event_data_summary
                                    .insert(name.to_string(), Value::String(text_val.clone()));
                                if name == "param1" {
                                    p1 = text_val;
                                } else if name == "param2" {
                                    p2 = text_val;
                                } else if name == "param3" {
                                    p3 = text_val;
                                } else {
                                    positional_candidates.push(text_val);
                                }
                            } else {
                                event_data_summary.insert(
                                    format!("param_{}", i + 1),
                                    Value::String(text_val.clone()),
                                );
                                positional_candidates.push(text_val);
                            }
                        }
                    } else if let Some(item_obj) = v.as_object() {
                        let text_val = extract_val_string(v);
                        if let Some(name) = item_obj
                            .get("#attributes")
                            .and_then(|a| a.get("Name"))
                            .and_then(|n| n.as_str())
                        {
                            event_data_summary
                                .insert(name.to_string(), Value::String(text_val.clone()));
                            if name == "param1" {
                                p1 = text_val;
                            } else if name == "param2" {
                                p2 = text_val;
                            } else if name == "param3" {
                                p3 = text_val;
                            } else {
                                positional_candidates.push(text_val);
                            }
                        } else {
                            event_data_summary
                                .insert("Data".to_string(), Value::String(text_val.clone()));
                            positional_candidates.push(text_val);
                        }
                    } else {
                        let text_val = extract_val_string(v);
                        event_data_summary
                            .insert("Data".to_string(), Value::String(text_val.clone()));
                        positional_candidates.push(text_val);
                    }
                } else if k == "Binary" {
                    binary_val = extract_val_string(v);
                    event_data_summary.insert(k.clone(), Value::String(binary_val.clone()));
                } else if k != "#attributes" {
                    let text_val = extract_val_string(v);
                    event_data_summary.insert(k.clone(), Value::String(text_val.clone()));
                    if k == "param1" {
                        p1 = text_val;
                    } else if k == "param2" {
                        p2 = text_val;
                    } else if k == "param3" {
                        p3 = text_val;
                    } else {
                        positional_candidates.push(text_val);
                    }
                }
            }
        }
    }

    if let Some(ud) = user_data_node {
        if let Some(ud_map) = ud.as_object() {
            for (k, v) in ud_map {
                if k != "#attributes" {
                    if let Some(nested) = v.as_object() {
                        for (nk, nv) in nested {
                            if nk != "#attributes" {
                                let s = extract_val_string(nv);
                                event_data_summary.insert(nk.clone(), Value::String(s.clone()));
                                positional_candidates.push(s);
                            }
                        }
                    } else {
                        let s = extract_val_string(v);
                        event_data_summary.insert(k.clone(), Value::String(s.clone()));
                        positional_candidates.push(s);
                    }
                }
            }
        }
    }

    let mut cand_iter = positional_candidates.into_iter().filter(|s| !s.is_empty());
    if p1.is_empty() {
        if let Some(c) = cand_iter.next() {
            p1 = c;
        }
    }
    if p2.is_empty() {
        if let Some(c) = cand_iter.next() {
            p2 = c;
        }
    }
    if p3.is_empty() {
        if let Some(c) = cand_iter.next() {
            p3 = c;
        }
    }

    let summary_val = Value::Object(event_data_summary);
    let user_id = extract_user_id(
        system.unwrap_or(&Value::Null),
        Some(&summary_val).or(event_data_node).or(user_data_node),
    );

    let event_data_str = serde_json::to_string(&summary_val).unwrap_or_else(|_| "{}".to_string());
    let payload_json = event_data_node
        .or(user_data_node)
        .map(|v| v.to_string())
        .or_else(|| Some(root.to_string()));
    let raw_event = if include_raw_event {
        Some(serde_json::to_string(val).unwrap_or_default())
    } else {
        None
    };

    ExtractedEvtxFields {
        event_id,
        provider,
        level,
        level_name,
        channel,
        computer,
        user_id,
        payload_json,
        param1: if p1.is_empty() { None } else { Some(p1) },
        param2: if p2.is_empty() { None } else { Some(p2) },
        param3: if p3.is_empty() { None } else { Some(p3) },
        binary: if binary_val.is_empty() {
            None
        } else {
            Some(binary_val)
        },
        event_data: Some(event_data_str),
        raw_event,
    }
}

/// Backward-compatible 7-tuple extractor used by existing unit/adversarial tests
pub fn extract_fields_from_value(
    val: &Value,
) -> (
    Option<i32>,
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let f = extract_rich_evtx_fields(val, false);
    (
        f.event_id,
        f.provider,
        f.level,
        f.channel,
        f.computer,
        f.user_id,
        f.payload_json,
    )
}

#[inline]
fn append_opt_str(builder: &mut StringBuilder, val: Option<&str>) {
    match val {
        Some(s) => builder.append_value(s),
        None => builder.append_null(),
    }
}

/// Ingest a Windows Event Log (.evtx) file into compressed Parquet cache.
/// Protected with `catch_unwind` cleanup so corrupted BinXML records never leak `.parquet.tmp`.
pub fn ingest_evtx_file(
    file_id: &str,
    evtx_path: &Path,
    target_parquet_path: &Path,
    threads: usize,
    cancellation_token: &CancellationToken,
    dispatcher: Option<&ProgressDispatcher>,
) -> Result<EvtxIngestResult, String> {
    let temp_out = target_parquet_path.with_extension("parquet.tmp");
    let unwind_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ingest_evtx_file_inner(
            file_id,
            evtx_path,
            target_parquet_path,
            &temp_out,
            threads,
            cancellation_token,
            dispatcher,
        )
    }));

    match unwind_res {
        Ok(res) => {
            if res.is_err() {
                let _ = std::fs::remove_file(&temp_out);
            }
            res
        }
        Err(_) => {
            let _ = std::fs::remove_file(&temp_out);
            Err(format!(
                "EVTX parser encountered an unrecoverable corrupted chunk in {:?}",
                evtx_path
            ))
        }
    }
}

fn ingest_evtx_file_inner(
    file_id: &str,
    evtx_path: &Path,
    target_parquet_path: &Path,
    temp_out: &Path,
    threads: usize,
    cancellation_token: &CancellationToken,
    dispatcher: Option<&ProgressDispatcher>,
) -> Result<EvtxIngestResult, String> {
    let t0 = Instant::now();
    let schema = create_evtx_arrow_schema();

    let settings = ParserSettings::default()
        .num_threads(threads.max(1))
        .validate_checksums(false);

    let mut parser = EvtxParser::from_path(evtx_path)
        .map_err(|e| format!("Failed to open EVTX file {:?}: {}", evtx_path, e))?
        .with_configuration(settings);

    if let Some(parent) = temp_out.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let file = File::create(temp_out)
        .map_err(|e| format!("Failed to create temporary output file {:?}: {}", temp_out, e))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(|e| format!("Failed to create Parquet writer: {}", e))?;

    // Keep raw_event compact so peak Working Set Delta stays strictly < 30MB on 70MB EVTX logs
    const BATCH_SIZE: usize = 16384;
    let mut b_record_id = Int64Builder::with_capacity(BATCH_SIZE);
    let mut b_time = TimestampMicrosecondBuilder::with_capacity(BATCH_SIZE).with_timezone("UTC");
    let mut b_event_id = Int32Builder::with_capacity(BATCH_SIZE);
    let mut b_provider = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 24);
    let mut b_level = Int32Builder::with_capacity(BATCH_SIZE);
    let mut b_channel = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16);
    let mut b_computer = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 20);
    let mut b_user_id = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 20);
    let mut b_payload = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 64);
    let mut b_level_name = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 12);
    let mut b_param1 = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 24);
    let mut b_param2 = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 24);
    let mut b_param3 = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16);
    let mut b_binary = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 8);
    let mut b_event_data = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 64);
    let mut b_raw_event = StringBuilder::with_capacity(BATCH_SIZE, 0);

    let mut total_rows = 0i64;
    let mut corrupted = 0u64;
    let mut batch_rows = 0usize;
    let src_bytes = std::fs::metadata(evtx_path).map(|m| m.len()).unwrap_or(0);
    // Empirical estimate: ~400 bytes per EVTX binary record on average
    let estimated_total_rows = (src_bytes / 400).max(16_384);

    if let Some(disp) = dispatcher {
        disp.emit_stage_sniffing(
            file_id,
            &evtx_path.to_string_lossy(),
            t0.elapsed().as_millis() as u64,
        );
    }

    for record_res in parser.records_json_value() {
        if cancellation_token.is_cancelled() {
            let _ = writer.close();
            let _ = std::fs::remove_file(temp_out);
            return Err("Ingestion cancelled by user".to_string());
        }

        match record_res {
            Ok(rec) => {
                let f = extract_rich_evtx_fields(&rec.data, false);

                b_record_id.append_value(rec.event_record_id as i64);
                b_time.append_value(rec.timestamp.timestamp_micros());

                if let Some(v) = f.event_id {
                    b_event_id.append_value(v);
                } else {
                    b_event_id.append_null();
                }

                append_opt_str(&mut b_provider, f.provider.as_deref());

                if let Some(v) = f.level {
                    b_level.append_value(v);
                } else {
                    b_level.append_null();
                }

                append_opt_str(&mut b_channel, f.channel.as_deref());
                append_opt_str(&mut b_computer, f.computer.as_deref());
                append_opt_str(&mut b_user_id, f.user_id.as_deref());
                append_opt_str(&mut b_payload, f.payload_json.as_deref());
                append_opt_str(&mut b_level_name, f.level_name.as_deref());
                append_opt_str(&mut b_param1, f.param1.as_deref());
                append_opt_str(&mut b_param2, f.param2.as_deref());
                append_opt_str(&mut b_param3, f.param3.as_deref());
                append_opt_str(&mut b_binary, f.binary.as_deref());
                append_opt_str(&mut b_event_data, f.event_data.as_deref());
                append_opt_str(&mut b_raw_event, f.raw_event.as_deref());

                batch_rows += 1;
                total_rows += 1;

                if batch_rows >= BATCH_SIZE {
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(b_record_id.finish()) as ArrayRef,
                            Arc::new(b_time.finish()) as ArrayRef,
                            Arc::new(b_event_id.finish()) as ArrayRef,
                            Arc::new(b_provider.finish()) as ArrayRef,
                            Arc::new(b_level.finish()) as ArrayRef,
                            Arc::new(b_channel.finish()) as ArrayRef,
                            Arc::new(b_computer.finish()) as ArrayRef,
                            Arc::new(b_user_id.finish()) as ArrayRef,
                            Arc::new(b_payload.finish()) as ArrayRef,
                            Arc::new(b_level_name.finish()) as ArrayRef,
                            Arc::new(b_param1.finish()) as ArrayRef,
                            Arc::new(b_param2.finish()) as ArrayRef,
                            Arc::new(b_param3.finish()) as ArrayRef,
                            Arc::new(b_binary.finish()) as ArrayRef,
                            Arc::new(b_event_data.finish()) as ArrayRef,
                            Arc::new(b_raw_event.finish()) as ArrayRef,
                        ],
                    )
                    .map_err(|e| format!("EVTX RecordBatch construction error: {}", e))?;

                    writer
                        .write(&batch)
                        .map_err(|e| format!("Parquet write error: {}", e))?;

                    batch_rows = 0;

                    if let Some(disp) = dispatcher {
                        let elapsed_sec = t0.elapsed().as_secs_f64().max(0.05);
                        let ratio = (total_rows as f64 / estimated_total_rows as f64).clamp(0.0, 1.0);
                        let pct = ((10.0 + ratio * 78.0).round() as u8).clamp(10, 88);
                        let est_bytes_done = (src_bytes as f64 * ratio) as u64;
                        let speed_mbs = ((est_bytes_done as f64 / (1024.0 * 1024.0)) / elapsed_sec * 10.0).round() / 10.0;
                        let eta_sec = if speed_mbs > 0.1 {
                            let rem = src_bytes.saturating_sub(est_bytes_done);
                            ((rem as f64 / (1024.0 * 1024.0)) / speed_mbs).round() as u64
                        } else {
                            0
                        };
                        disp.emit_live_progress(
                            file_id,
                            &evtx_path.to_string_lossy(),
                            total_rows as u64,
                            pct,
                            t0.elapsed().as_millis() as u64,
                            speed_mbs,
                            eta_sec,
                            format!("Extracting EVTX records: {} rows...", total_rows),
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Corrupted or unreadable EVTX record skipped: {}", e);
                corrupted += 1;
            }
        }
    }

    if batch_rows > 0 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(b_record_id.finish()) as ArrayRef,
                Arc::new(b_time.finish()) as ArrayRef,
                Arc::new(b_event_id.finish()) as ArrayRef,
                Arc::new(b_provider.finish()) as ArrayRef,
                Arc::new(b_level.finish()) as ArrayRef,
                Arc::new(b_channel.finish()) as ArrayRef,
                Arc::new(b_computer.finish()) as ArrayRef,
                Arc::new(b_user_id.finish()) as ArrayRef,
                Arc::new(b_payload.finish()) as ArrayRef,
                Arc::new(b_level_name.finish()) as ArrayRef,
                Arc::new(b_param1.finish()) as ArrayRef,
                Arc::new(b_param2.finish()) as ArrayRef,
                Arc::new(b_param3.finish()) as ArrayRef,
                Arc::new(b_binary.finish()) as ArrayRef,
                Arc::new(b_event_data.finish()) as ArrayRef,
                Arc::new(b_raw_event.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| format!("EVTX RecordBatch construction error: {}", e))?;

        writer
            .write(&batch)
            .map_err(|e| format!("Parquet write error: {}", e))?;
    }

    if let Some(disp) = dispatcher {
        disp.emit_stage_writing(
            file_id,
            &evtx_path.to_string_lossy(),
            total_rows as u64,
            t0.elapsed().as_millis() as u64,
        );
    }

    writer
        .close()
        .map_err(|e| format!("Parquet close error: {}", e))?;

    if target_parquet_path.exists() {
        let _ = std::fs::remove_file(target_parquet_path);
    }
    std::fs::rename(temp_out, target_parquet_path)
        .map_err(|e| format!("Atomic rename error: {}", e))?;

    let parquet_size = std::fs::metadata(target_parquet_path)
        .map(|m| m.len() as i64)
        .unwrap_or(0);

    if let Some(disp) = dispatcher {
        disp.emit_stage_ready(
            file_id,
            &evtx_path.to_string_lossy(),
            total_rows as u64,
            t0.elapsed().as_millis() as u64,
        );
    }

    Ok(EvtxIngestResult {
        row_count: total_rows,
        parquet_size,
        duration_ms: t0.elapsed().as_millis() as i64,
        corrupted_records: corrupted,
    })
}
