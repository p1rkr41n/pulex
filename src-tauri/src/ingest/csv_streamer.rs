//! csv_streamer.rs
//!
//! RFC-4180 CSV/TSV Delimiter Sniffer & Streaming Ingest Engine (Milestone M5 / F18)
//! - Transparently stream-transcodes UTF-16LE, UTF-16BE, UTF-8 BOM, and `.gz` compressed logs to UTF-8.
//! - Delimiter sniffer across `[',', '\t', ';', '|']` using line consistency scoring.
//! - DuckDB 3-tier fallback matrix with `all_varchar = true`, `null_padding = true`,
//!   `sample_size = 100000`, and `parallel = false` to prevent C++ vector assertion crashes.
//! - Streams into compressed Parquet (`_cached_<hash>.parquet`) in `temp/<project_id>/`.
//! - Graceful fallback to `dirty_log` if malformed or unstructured.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use duckdb::Connection;

use crate::engine::fast_count::FastCounter;
use crate::ingest::dirty_log::ingest_dirty_log;
use crate::ingest::progress::ProgressDispatcher;
use crate::ingest::scheduler::CancellationToken;
use crate::ingest::transcode::{detect_text_encoding, transcode_to_utf8, DetectedEncoding};

#[derive(Debug, Clone, Default)]
pub struct CsvIngestResult {
    pub row_count: i64,
    pub parquet_size: i64,
    pub delimiter_used: char,
    pub tier_used: u8,
    pub duration_ms: i64,
}

const CANDIDATE_DELIMITERS: [char; 4] = [',', '\t', ';', '|'];

/// Extract the effective inner extension when a file is `.gz` compressed (e.g. `foo.tsv.gz` -> `"tsv"`).
pub fn effective_extension(file_path: &Path) -> String {
    let ext = file_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    if ext == "gz" {
        if let Some(stem) = file_path.file_stem() {
            if let Some(inner) = Path::new(stem).extension().and_then(|e| e.to_str()) {
                return inner.to_lowercase();
            }
        }
    }
    ext
}

/// Sniff the most likely delimiter from the first 50 lines of a delimited UTF-8 file.
/// Applies RFC-4180 quote escaping rules and line consistency scoring.
pub fn sniff_delimiter(file_path: &Path) -> char {
    let ext = effective_extension(file_path);

    let extension_hint = match ext.as_str() {
        "tsv" | "tab" => '\t',
        "csv" => ',',
        _ => ',',
    };

    let file = match File::open(file_path) {
        Ok(f) => f,
        Err(_) => return extension_hint,
    };

    let reader = BufReader::new(file);
    let mut line_counts: [Vec<usize>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    let mut sampled_lines = 0usize;

    for line_res in reader.lines().take(50) {
        if let Ok(line) = line_res {
            if line.trim().is_empty() {
                continue;
            }
            sampled_lines += 1;
            let counts = count_delimiters_in_line(&line);
            for i in 0..4 {
                line_counts[i].push(counts[i]);
            }
        }
    }

    if sampled_lines == 0 {
        return extension_hint;
    }

    let mut best_delimiter = extension_hint;
    let mut best_score = -1.0f64;

    for (idx, &delim) in CANDIDATE_DELIMITERS.iter().enumerate() {
        let counts = &line_counts[idx];
        let total: usize = counts.iter().sum();
        if total == 0 {
            continue;
        }

        let mean = total as f64 / sampled_lines as f64;
        let variance: f64 = counts
            .iter()
            .map(|&c| {
                let diff = c as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / sampled_lines as f64;

        let consistency_bonus = if variance < 0.1 && mean >= 1.0 {
            50.0
        } else if variance < 1.0 && mean >= 1.0 {
            20.0
        } else if mean >= 1.0 {
            5.0
        } else {
            0.1
        };

        let ext_bonus = if delim == extension_hint { 10.0 } else { 0.0 };
        let score = (mean * consistency_bonus) + ext_bonus;

        if score > best_score {
            best_score = score;
            best_delimiter = delim;
        }
    }

    best_delimiter
}

fn count_delimiters_in_line(line: &str) -> [usize; 4] {
    let mut counts = [0usize; 4];
    let mut in_quotes = false;
    let chars: Vec<char> = line.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let c = chars[i];
        if c == '"' {
            if in_quotes && i + 1 < len && chars[i + 1] == '"' {
                i += 1;
            } else {
                in_quotes = !in_quotes;
            }
        } else if !in_quotes {
            for (idx, &cand) in CANDIDATE_DELIMITERS.iter().enumerate() {
                if c == cand {
                    counts[idx] += 1;
                }
            }
        }
        i += 1;
    }

    counts
}

struct TempFileCleanup(Option<PathBuf>);
impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        if let Some(ref p) = self.0 {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Ingest a CSV/TSV (including `.gz` and UTF-16LE/BE) file using DuckDB 3-tier fallback matrix into compressed Parquet.
pub fn ingest_csv_file(
    file_id: &str,
    source_path: &Path,
    target_parquet_path: &Path,
    conn: &Connection,
    cancellation_token: &CancellationToken,
    dispatcher: Option<&ProgressDispatcher>,
) -> Result<CsvIngestResult, String> {
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

    // Transparent streaming UTF-8 transcoding ONLY for .gz and UTF-16LE/BE.
    // DuckDB read_csv natively handles UTF-8 BOM (EF BB BF) directly without
    // incurring a full-file disk copy on multi-gigabyte CSV datasets.
    let mut transcoded_guard = TempFileCleanup(None);
    let working_source: PathBuf = match detect_text_encoding(source_path) {
        Ok(enc) if enc.needs_utf8_transcode() && enc != DetectedEncoding::Utf8Bom => {
            let inner_ext = effective_extension(source_path);
            let transcode_ext = if inner_ext.is_empty() { "csv" } else { inner_ext.as_str() };
            let utf8_tmp = target_parquet_path.with_extension(format!("utf8_tmp.{}", transcode_ext));
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

    let delim = sniff_delimiter(&working_source);
    let delim_str = match delim {
        '\t' => "\\t".to_string(),
        '\'' => "\\'".to_string(),
        _ => delim.to_string(),
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

    // Spawn real-time I/O & Parquet growth progress ticker during blocking DuckDB COPY
    let _progress_ticker = dispatcher.map(|disp| {
        disp.spawn_copy_progress_ticker(file_id, source_path, &working_source, &temp_out, 0.22)
    });

    // Tier 0: Standard auto_detect with all_varchar guard and parallel=false
    let tier0_sql = format!(
        "COPY (SELECT * FROM read_csv('{src}', delim = '{delim}', auto_detect = true, sample_size = 100000, ignore_errors = true, null_padding = true, all_varchar = true, parallel = false)) TO '{dst}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880);",
        src = safe_src,
        delim = delim_str,
        dst = safe_dst
    );

    let mut tier_used = 0u8;
    let mut exec_result = conn.execute_batch(&tier0_sql);

    // Tier 1: Force header=true and quote='' safe mode if Tier 0 fails
    if exec_result.is_err() && !cancellation_token.is_cancelled() {
        tracing::warn!(
            "CSV Tier 0 failed ({:?}); attempting Tier 1 safe quote/header mode",
            exec_result.as_ref().err()
        );
        let _ = std::fs::remove_file(&temp_out);
        let tier1_sql = format!(
            "COPY (SELECT * FROM read_csv('{src}', delim = '{delim}', auto_detect = true, sample_size = 100000, header = true, quote = '', ignore_errors = true, null_padding = true, all_varchar = true, parallel = false)) TO '{dst}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880);",
            src = safe_src,
            delim = delim_str,
            dst = safe_dst
        );
        tier_used = 1;
        exec_result = conn.execute_batch(&tier1_sql);
    }

    drop(_progress_ticker);

    if cancellation_token.is_cancelled() {
        let _ = std::fs::remove_file(&temp_out);
        return Err("Ingestion cancelled by user".to_string());
    }

    // Tier 2: Dirty log fallback if both Tier 0 and Tier 1 fail
    if exec_result.is_err() {
        tracing::warn!("CSV Tier 1 failed; falling back to Tier 2 (dirty_log)");
        let _ = std::fs::remove_file(&temp_out);
        tier_used = 2;
        let dirty_res = ingest_dirty_log(
            file_id,
            &working_source,
            target_parquet_path,
            cancellation_token,
            dispatcher,
        )?;
        return Ok(CsvIngestResult {
            row_count: dirty_res.row_count,
            parquet_size: dirty_res.parquet_size,
            delimiter_used: delim,
            tier_used,
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
        .map_err(|e| format!("Atomic rename error for CSV parquet: {}", e))?;

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

    Ok(CsvIngestResult {
        row_count,
        parquet_size,
        delimiter_used: delim,
        tier_used,
        duration_ms: t0.elapsed().as_millis() as i64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_bom_csv_zero_copy_native_duckdb() {
        let tmp_dir = std::env::temp_dir().join("pulex_utf8_bom_csv_test");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let src = tmp_dir.join("bom_sample.csv");
        let dst = tmp_dir.join("bom_sample.parquet");
        let mut bytes = vec![0xEF, 0xBB, 0xBF];
        bytes.extend_from_slice(b"Timestamp,Host,Level\n2026-01-01T00:00:00Z,SRV01,High\n2026-01-01T00:01:00Z,SRV02,Low\n");
        std::fs::write(&src, &bytes).expect("write bom csv");

        let conn = Connection::open_in_memory().expect("open duckdb");
        let ct = CancellationToken::new();
        let res = ingest_csv_file("f_bom", &src, &dst, &conn, &ct, None).expect("ingest bom csv");
        assert_eq!(res.row_count, 2);
        assert_eq!(res.tier_used, 0);

        // Verify DuckDB cleanly stripped the UTF-8 BOM from column 0 header
        let safe_dst = dst.to_string_lossy().replace('\\', "/").replace('\'', "''");
        let first_col: String = conn
            .query_row(
                &format!("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{}')) LIMIT 1", safe_dst),
                [],
                |r| r.get(0),
            )
            .expect("describe parquet");
        assert_eq!(first_col, "Timestamp", "UTF-8 BOM should be stripped cleanly by DuckDB read_csv");

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
