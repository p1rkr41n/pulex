//! arrow_encoder.rs
//!
//! Vectorized Apache Arrow IPC Stream Encoder for Toad Tauri v2:
//! - Streams RecordBatches directly to byte buffer starting with 0xFFFFFFFF continuation marker.
//! - Embeds `toadSchema` JSON string into Arrow Schema metadata for seamless frontend hydration.
//! - Wraps buffer into `tauri::ipc::Response`.

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use std::sync::Arc;
use tauri::ipc::Response;

use crate::engine::schema_mapper::SchemaMapper;

/// Hard laptop safety ceiling on serialized interactive Arrow IPC payloads (64 MB).
pub const MAX_INTERACTIVE_IPC_BYTES: usize = 64 * 1024 * 1024;
/// Hard laptop safety ceiling on cell count (`rows * cols`) per interactive IPC response.
pub const MAX_INTERACTIVE_IPC_CELLS: usize = 2_000_000;

pub struct ArrowIpcEncoder;

impl ArrowIpcEncoder {
    /// Enrich an Arrow Schema with `toadSchema` and `application` metadata.
    fn enrich_schema(schema: &SchemaRef, toad_schema_json: Option<&str>) -> SchemaRef {
        let mut meta = schema.metadata().clone();
        let schema_json = match toad_schema_json {
            Some(s) => s.to_string(),
            None => SchemaMapper::to_toad_schema_json(schema),
        };
        meta.insert("toadSchema".to_string(), schema_json);
        meta.insert("application".to_string(), "Pulex-Tauri-v2".to_string());
        Arc::new(schema.as_ref().clone().with_metadata(meta))
    }

    /// Encode a slice of RecordBatches to binary Arrow IPC Stream response
    pub fn encode_stream(
        batches: &[RecordBatch],
        schema: &SchemaRef,
        toad_schema_json: Option<&str>,
    ) -> Result<Response, String> {
        let bytes = Self::encode_to_vec(batches, schema, toad_schema_json)?;
        Ok(Response::new(bytes))
    }

    /// Encode RecordBatches to a raw byte vector
    pub fn encode_to_vec(
        batches: &[RecordBatch],
        schema: &SchemaRef,
        toad_schema_json: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        // 1. Enrich schema metadata with toadSchema and application branding
        let enriched_schema = Self::enrich_schema(schema, toad_schema_json);

        // 2. Pre-allocate buffer for high-throughput serialization
        let estimated_size = batches
            .iter()
            .map(|b| b.get_array_memory_size())
            .sum::<usize>()
            + 4096;
        let mut buffer = Vec::with_capacity(estimated_size);

        // 3. Serialize through StreamWriter (generates 0xFFFFFFFF continuation indicator)
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &enriched_schema)
                .map_err(|e| format!("StreamWriter init error: {}", e))?;

            for batch in batches {
                writer
                    .write(batch)
                    .map_err(|e| format!("Write batch error: {}", e))?;
            }

            writer
                .finish()
                .map_err(|e| format!("Finish stream error: {}", e))?;
        }

        // 4. Verification assertion: Must start with 0xFFFFFFFF continuation marker
        if buffer.len() >= 4 {
            debug_assert_eq!(
                &buffer[0..4],
                &[0xFF, 0xFF, 0xFF, 0xFF],
                "Arrow IPC stream must begin with 0xFFFFFFFF continuation marker"
            );
        }

        Ok(buffer)
    }

    /// Stream RecordBatches incrementally to an Arrow IPC byte buffer while enforcing
    /// `max_bytes` (in-memory batch array bytes) and `max_cells` (`rows * columns`) ceilings.
    /// Aborts immediately upon breach without pulling further batches from `batches`.
    pub fn encode_iter_to_vec_capped<I>(
        batches: I,
        schema: &SchemaRef,
        toad_schema_json: Option<&str>,
        max_bytes: usize,
        max_cells: usize,
    ) -> Result<Vec<u8>, String>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        let enriched_schema = Self::enrich_schema(schema, toad_schema_json);
        let mut buffer = Vec::with_capacity(4096);

        {
            let mut writer = StreamWriter::try_new(&mut buffer, &enriched_schema)
                .map_err(|e| format!("StreamWriter init error: {}", e))?;

            let mut accumulated_bytes: usize = 0;
            let mut accumulated_cells: usize = 0;

            for batch in batches {
                accumulated_bytes = accumulated_bytes.saturating_add(batch.get_array_memory_size());
                accumulated_cells = accumulated_cells
                    .saturating_add(batch.num_rows().saturating_mul(batch.num_columns()));

                if accumulated_bytes > max_bytes || accumulated_cells > max_cells {
                    return Err(
                        "Result payload exceeded 64 MB / 2M cells laptop safety cap. Reduce visible columns or page size."
                            .to_string(),
                    );
                }

                writer
                    .write(&batch)
                    .map_err(|e| format!("Write batch error: {}", e))?;
            }

            writer
                .finish()
                .map_err(|e| format!("Finish stream error: {}", e))?;
        }

        if buffer.len() >= 4 {
            debug_assert_eq!(
                &buffer[0..4],
                &[0xFF, 0xFF, 0xFF, 0xFF],
                "Arrow IPC stream must begin with 0xFFFFFFFF continuation marker"
            );
        }

        Ok(buffer)
    }

    /// Encode an empty table response (0 rows) preserving schema metadata
    pub fn encode_empty(
        schema: &SchemaRef,
        toad_schema_json: Option<&str>,
    ) -> Result<Response, String> {
        Self::encode_stream(&[], schema, toad_schema_json)
    }

    /// Execute SQL query directly on DuckDB connection and stream output as Arrow IPC
    pub fn query_to_arrow_ipc(conn: &Connection, sql: &str) -> Result<Response, String> {
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| format!("DuckDB SQL Prepare error [{}]: {}", sql, e))?;

        let arrow = stmt
            .query_arrow([])
            .map_err(|e| format!("DuckDB Query Arrow error [{}]: {}", sql, e))?;

        let schema = arrow.get_schema();
        let bytes = Self::encode_iter_to_vec_capped(
            arrow,
            &schema,
            None,
            MAX_INTERACTIVE_IPC_BYTES,
            MAX_INTERACTIVE_IPC_CELLS,
        )?;

        Ok(Response::new(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    fn make_batch(schema: &SchemaRef, rows: usize, payload_len: usize) -> RecordBatch {
        let ids: Vec<i32> = (0..rows as i32).collect();
        let text = "x".repeat(payload_len);
        let msgs: Vec<&str> = vec![text.as_str(); rows];
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(msgs)),
            ],
        )
        .expect("build test RecordBatch")
    }

    struct PanicOnThirdBatchIter {
        schema: SchemaRef,
        step: usize,
        rows_per_batch: usize,
        payload_len: usize,
    }

    impl Iterator for PanicOnThirdBatchIter {
        type Item = RecordBatch;

        fn next(&mut self) -> Option<Self::Item> {
            self.step += 1;
            match self.step {
                1 | 2 => Some(make_batch(&self.schema, self.rows_per_batch, self.payload_len)),
                _ => panic!("Iterator pulled batch {} after safety cap should have aborted!", self.step),
            }
        }
    }

    #[test]
    fn test_encode_iter_to_vec_capped_aborts_early_on_max_cells() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("msg", DataType::Utf8, false),
        ]));
        // Each batch has 5 rows * 2 cols = 10 cells.
        // Batch 1 = 10 cells (<= 15), Batch 2 = 20 cells (> 15) -> must abort before pulling Batch 3.
        let iter = PanicOnThirdBatchIter {
            schema: schema.clone(),
            step: 0,
            rows_per_batch: 5,
            payload_len: 4,
        };
        let res = ArrowIpcEncoder::encode_iter_to_vec_capped(
            iter,
            &schema,
            None,
            MAX_INTERACTIVE_IPC_BYTES,
            15,
        );
        let err = res.expect_err("Expected cell cap breach error");
        assert!(err.contains("64 MB / 2M cells"), "Unexpected error: {}", err);
    }

    #[test]
    fn test_encode_iter_to_vec_capped_aborts_early_on_max_bytes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("msg", DataType::Utf8, false),
        ]));
        let sample = make_batch(&schema, 10, 512);
        let single_bytes = sample.get_array_memory_size();
        // Set max_bytes between 1x and 2x batch memory size -> Batch 1 passes, Batch 2 aborts before Batch 3.
        let cap_bytes = single_bytes + (single_bytes / 2);
        let iter = PanicOnThirdBatchIter {
            schema: schema.clone(),
            step: 0,
            rows_per_batch: 10,
            payload_len: 512,
        };
        let res = ArrowIpcEncoder::encode_iter_to_vec_capped(
            iter,
            &schema,
            None,
            cap_bytes,
            MAX_INTERACTIVE_IPC_CELLS,
        );
        let err = res.expect_err("Expected byte cap breach error");
        assert!(err.contains("64 MB / 2M cells"), "Unexpected error: {}", err);
    }

    #[test]
    fn test_encode_iter_to_vec_capped_valid_multi_batch_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("msg", DataType::Utf8, false),
        ]));
        let b1 = make_batch(&schema, 4, 8);
        let b2 = make_batch(&schema, 6, 8);
        let bytes = ArrowIpcEncoder::encode_iter_to_vec_capped(
            vec![b1, b2],
            &schema,
            None,
            MAX_INTERACTIVE_IPC_BYTES,
            MAX_INTERACTIVE_IPC_CELLS,
        )
        .expect("Valid multi-batch encode should succeed");

        assert!(bytes.len() >= 8);
        assert_eq!(&bytes[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);

        let reader = StreamReader::try_new(Cursor::new(bytes), None)
            .expect("StreamReader should parse capped IPC bytes");
        let decoded_schema = reader.schema();
        assert_eq!(
            decoded_schema.metadata().get("application").map(|s| s.as_str()),
            Some("Pulex-Tauri-v2")
        );
        assert!(decoded_schema.metadata().contains_key("toadSchema"));

        let decoded_batches: Vec<RecordBatch> = reader
            .map(|r| r.expect("Decoded batch should be Ok"))
            .collect();
        assert_eq!(decoded_batches.len(), 2);
        let total_rows: usize = decoded_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
    }
}
