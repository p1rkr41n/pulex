//! jsonl_streamer.rs
//!
//! JSONL / NDJSON Streaming Ingestion & Dynamic Schema Unnesting Engine (Milestone M5 / F20)
//! - Transparently stream-transcodes `.gz`, UTF-16LE/BE, and UTF-8 BOM JSON/JSONL streams to UTF-8.
//! - 2-phase schema introspection via DuckDB `read_json_auto` and `DESCRIBE`.
//! - Recursively unnests nested DuckDB `STRUCT` columns into flat dot-notated columnar fields
//!   (e.g. `"Event"."System"."EventID" AS "Event.System.EventID"`).
//! - Streams directly into compressed Parquet (`_cached_<hash>.parquet`) with Snappy compression.
//! - Fallback to `dirty_log` if input contains non-JSON or severely damaged lines.

use std::path::{Path, PathBuf};
use std::time::Instant;

use duckdb::Connection;

use crate::engine::fast_count::FastCounter;
use crate::ingest::dirty_log::ingest_dirty_log;
use crate::ingest::progress::ProgressDispatcher;
use crate::ingest::scheduler::CancellationToken;
use crate::ingest::transcode::{detect_text_encoding, transcode_to_utf8};

#[derive(Debug, Clone, Default)]
pub struct JsonlIngestResult {
    pub row_count: i64,
    pub parquet_size: i64,
    pub unnested_columns_count: usize,
    pub duration_ms: i64,
}

struct TempFileCleanup(Option<PathBuf>);
impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        if let Some(ref p) = self.0 {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Recursively parses DuckDB STRUCT signature into leaf path projections.
pub fn parse_struct_fields(struct_type_str: &str) -> Vec<String> {
    let s = struct_type_str.trim();
    if !s.starts_with("STRUCT(") || !s.ends_with(')') {
        return Vec::new();
    }

    let inner = &s[7..s.len() - 1].trim();
    let mut fields = Vec::new();
    let mut depth = 0;
    let mut current_field = String::new();

    for c in inner.chars() {
        match c {
            '(' => {
                depth += 1;
                current_field.push(c);
            }
            ')' => {
                depth -= 1;
                current_field.push(c);
            }
            ',' if depth == 0 => {
                let trimmed = current_field.trim().to_string();
                if !trimmed.is_empty() {
                    fields.push(trimmed);
                }
                current_field.clear();
            }
            _ => {
                current_field.push(c);
            }
        }
    }
    let trimmed = current_field.trim().to_string();
    if !trimmed.is_empty() {
        fields.push(trimmed);
    }

    let mut result = Vec::new();
    for f in fields {
        let parts: Vec<&str> = f.splitn(2, ' ').collect();
        if parts.is_empty() {
            continue;
        }
        let raw_name = parts[0].trim().trim_matches('"');
        let field_type = if parts.len() > 1 { parts[1].trim() } else { "" };

        if field_type.starts_with("STRUCT(") {
            let sub_leaves = parse_struct_fields(field_type);
            if sub_leaves.is_empty() {
                result.push(raw_name.to_string());
            } else {
                for sub in sub_leaves {
                    result.push(format!("{}.{}", raw_name, sub));
                }
            }
        } else {
            result.push(raw_name.to_string());
        }
    }

    result
}

fn build_projection_for_leaf(root_col: &str, leaf_path: &str) -> String {
    let full_path = if leaf_path.is_empty() {
        root_col.to_string()
    } else {
        format!("{}.{}", root_col, leaf_path)
    };

    let segments: Vec<&str> = full_path.split('.').collect();
    let quoted_chain: Vec<String> = segments
        .iter()
        .map(|seg| format!("\"{}\"", seg.replace('"', "\"\"")))
        .collect();
    let expr = quoted_chain.join(".");
    format!("{} AS \"{}\"", expr, full_path.replace('"', "\"\""))
}

/// Ingest a JSON / JSONL / NDJSON (including `.gz` and UTF-16LE/BE) file into compressed Parquet.
pub fn ingest_jsonl_file(
    file_id: &str,
    source_path: &Path,
    target_parquet_path: &Path,
    conn: &Connection,
    cancellation_token: &CancellationToken,
    dispatcher: Option<&ProgressDispatcher>,
) -> Result<JsonlIngestResult, String> {
    let t0 = Instant::now();

    if let Some(disp) = dispatcher {
        disp.emit_stage_sniffing(
            file_id,
            &source_path.to_string_lossy(),
            t0.elapsed().as_millis() as u64,
        );
    }

    let temp_out = target_parquet_path.with_extension("parquet.tmp");
    if let Some(parent) = temp_out.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if temp_out.exists() {
        let _ = std::fs::remove_file(&temp_out);
    }

    let mut transcoded_guard = TempFileCleanup(None);
    let working_source: PathBuf = match detect_text_encoding(source_path) {
        Ok(enc) if enc.needs_utf8_transcode() => {
            let utf8_tmp = target_parquet_path.with_extension("utf8_tmp.jsonl");
            let _transcode_ticker = dispatcher.map(|disp| {
                disp.spawn_copy_progress_ticker(file_id, source_path, source_path, &utf8_tmp, 1.0)
            });
            transcode_to_utf8(source_path, &utf8_tmp)
                .map_err(|e| format!("Encoding transcode error for {:?}: {}", source_path, e))?;
            drop(_transcode_ticker);
            transcoded_guard.0 = Some(utf8_tmp.clone());
            utf8_tmp
        }
        _ => source_path.to_path_buf(),
    };

    let safe_src = working_source
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    let safe_dst = temp_out
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");

    if cancellation_token.is_cancelled() {
        let _ = std::fs::remove_file(&temp_out);
        return Err("Ingestion cancelled by user".to_string());
    }

    // Phase 1: Introspect schema using DESCRIBE
    let describe_sql = format!(
        "DESCRIBE SELECT * FROM read_json_auto('{src}', sample_size = 2048, ignore_errors = true, maximum_object_size = 67108864) LIMIT 0;",
        src = safe_src
    );

    let mut projections: Vec<String> = Vec::new();
    let mut has_structs = false;

    if let Ok(mut stmt) = conn.prepare(&describe_sql) {
        if let Ok(mut rows) = stmt.query([]) {
            while let Ok(Some(row)) = rows.next() {
                let col_name: String = row.get(0).unwrap_or_default();
                let col_type: String = row.get(1).unwrap_or_default();

                if col_type.starts_with("STRUCT(") {
                    has_structs = true;
                    let leaves = parse_struct_fields(&col_type);
                    if leaves.is_empty() {
                        projections.push(format!("\"{}\"", col_name.replace('"', "\"\"")));
                    } else {
                        for leaf in leaves {
                            projections.push(build_projection_for_leaf(&col_name, &leaf));
                        }
                    }
                } else {
                    projections.push(format!("\"{}\"", col_name.replace('"', "\"\"")));
                }
            }
        }
    }

    if cancellation_token.is_cancelled() {
        let _ = std::fs::remove_file(&temp_out);
        return Err("Ingestion cancelled by user".to_string());
    }

    // Spawn real-time I/O & Parquet growth progress ticker during blocking DuckDB COPY
    let _progress_ticker = dispatcher.map(|disp| {
        disp.spawn_copy_progress_ticker(file_id, source_path, &working_source, &temp_out, 0.25)
    });

    let unnested_count = projections.len();
    let copy_sql = if has_structs && !projections.is_empty() {
        format!(
            "COPY (SELECT {projections} FROM read_json_auto('{src}', sample_size = 2048, ignore_errors = true, maximum_object_size = 67108864)) TO '{dst}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880);",
            projections = projections.join(", "),
            src = safe_src,
            dst = safe_dst
        )
    } else {
        format!(
            "COPY (SELECT * FROM read_json_auto('{src}', sample_size = 2048, ignore_errors = true, maximum_object_size = 67108864)) TO '{dst}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880);",
            src = safe_src,
            dst = safe_dst
        )
    };

    let mut exec_result = conn.execute_batch(&copy_sql);

    if exec_result.is_err() && has_structs && !cancellation_token.is_cancelled() {
        tracing::warn!("Unnested JSON projection failed; falling back to flat read_json_auto");
        let _ = std::fs::remove_file(&temp_out);
        let fallback_sql = format!(
            "COPY (SELECT * FROM read_json_auto('{src}', sample_size = 2048, ignore_errors = true, maximum_object_size = 67108864)) TO '{dst}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880);",
            src = safe_src,
            dst = safe_dst
        );
        exec_result = conn.execute_batch(&fallback_sql);
    }

    drop(_progress_ticker);

    if cancellation_token.is_cancelled() {
        let _ = std::fs::remove_file(&temp_out);
        return Err("Ingestion cancelled by user".to_string());
    }

    if exec_result.is_err() {
        tracing::warn!("JSON read_json_auto failed; falling back to dirty_log");
        let _ = std::fs::remove_file(&temp_out);
        let dirty_res = ingest_dirty_log(
            file_id,
            &working_source,
            target_parquet_path,
            cancellation_token,
            dispatcher,
        )?;
        return Ok(JsonlIngestResult {
            row_count: dirty_res.row_count,
            parquet_size: dirty_res.parquet_size,
            unnested_columns_count: 3,
            duration_ms: t0.elapsed().as_millis() as i64,
        });
    }

    if let Some(disp) = dispatcher {
        disp.emit_stage_writing(
            file_id,
            &source_path.to_string_lossy(),
            0,
            t0.elapsed().as_millis() as u64,
        );
    }

    if target_parquet_path.exists() {
        let _ = std::fs::remove_file(target_parquet_path);
    }
    std::fs::rename(&temp_out, target_parquet_path)
        .map_err(|e| format!("Atomic rename error for JSONL parquet: {}", e))?;

    let row_count = FastCounter::count_parquet_rows(target_parquet_path).unwrap_or(0);
    let parquet_size = std::fs::metadata(target_parquet_path)
        .map(|m| m.len() as i64)
        .unwrap_or(0);

    if let Some(disp) = dispatcher {
        disp.emit_stage_ready(
            file_id,
            &source_path.to_string_lossy(),
            row_count as u64,
            t0.elapsed().as_millis() as u64,
        );
    }

    drop(transcoded_guard);

    Ok(JsonlIngestResult {
        row_count,
        parquet_size,
        unnested_columns_count: unnested_count,
        duration_ms: t0.elapsed().as_millis() as i64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_bom_jsonl_transcodes_cleanly() {
        let tmp_dir = std::env::temp_dir().join("pulex_utf8_bom_jsonl_test");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let src = tmp_dir.join("bom_sample.jsonl");
        let dst = tmp_dir.join("bom_sample.parquet");
        let mut bytes = vec![0xEF, 0xBB, 0xBF];
        bytes.extend_from_slice(b"{\"host\":\"SRV01\",\"level\":\"High\"}\n{\"host\":\"SRV02\",\"level\":\"Low\"}\n");
        std::fs::write(&src, &bytes).expect("write bom jsonl");

        let conn = Connection::open_in_memory().expect("open duckdb");
        let ct = CancellationToken::new();
        let res = ingest_jsonl_file("f_jbom", &src, &dst, &conn, &ct, None).expect("ingest bom jsonl");
        assert_eq!(res.row_count, 2);

        let safe_dst = dst.to_string_lossy().replace('\\', "/").replace('\'', "''");
        let first_col: String = conn
            .query_row(
                &format!("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{}')) LIMIT 1", safe_dst),
                [],
                |r| r.get(0),
            )
            .expect("describe parquet");
        assert_eq!(first_col, "host", "UTF-8 BOM should be handled cleanly by DuckDB read_json_auto");

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
