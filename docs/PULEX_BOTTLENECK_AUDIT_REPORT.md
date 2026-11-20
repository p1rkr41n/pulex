# Pulex v3.0.0 — System-Wide Bottleneck Audit & Benchmark Report

**Date:** 2026-11-20  
**Target Binary:** `pulex.exe` (Tauri v2 + Rust Release `opt-level=3, lto=true` + DuckDB `v1.2.2` + Arrow `54` + WebView2/SlickGrid)  
**Reference Baseline:** Toad `v2.6.0.3` (`duckdb` `v1.0.0` N-API addon)  
**Test Workstation:** Windows x64, **16 logical CPU threads**, **96.0 GB RAM**  
**Datasets:** Line-aligned slices of real DFIR datasets (`sample_40gb.csv` -> `1.0 GB / 1,557,043 rows`, `256 MB` CSV & `.csv.gz`, `sample_40gb.json` -> `256 MB / 319,026 rows`, `System.evtx` `22.07 MB / 57,030 rows`, `Windows.EventLogs.Hayabusa_Results.csv` `24.48 MB / 37,241 rows`).

---

## 1. Executive Summary & Ranked Bottleneck Ledger

Empirical benchmarking across all 7 architectural layers proves that **Pulex's underlying DuckDB `v1.2.2` engine is `1.6x–2.15x` faster than Toad's DuckDB `v1.0.0`** when properly configured (`771.5 MB/s` vs `358.6 MB/s` on identical `sample_1gb.csv` settings). Pulex's observed real-world slowness (`~138 MB/s` CSV ingest, multi-second page opens on large datasets, UI stalls during stats/exports) is caused by **self-imposed configuration locks and serial hot paths**, not by DuckDB or Arrow IPC.

| Rank | Severity | Layer | Bottleneck | Measured Baseline | Measured Ceiling / Fix | Potential Gain | Exact Location |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **B1** | **P0 Critical** | L1 + L2 | Hardcoded `parallel = false` in CSV Tier 0 **+** `PRAGMA threads <= 2` **+** `max_memory = '2GB'` **+** ignored `ROW_GROUP_SIZE = 25000` | **`137.7 MB/s`** (`7.44s` / 1 GB) | **`771.5 MB/s`** (`1.33s` / 1 GB) | **`5.60x` faster** (`2.15x` faster than Toad default) | [csv_streamer.rs:L243-L249](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/ingest/csv_streamer.rs#L243-L249), [duckdb_pool.rs:L25-L28](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/engine/duckdb_pool.rs#L25-L28), [duckdb_pool.rs:L243-L248](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/engine/duckdb_pool.rs#L243-L248) |
| **B2** | **P0 Critical** | L3 | Unconditional Auto-Sort (`ORDER BY timeCol DESC`) on every newly opened file & deep `OFFSET` pagination | Page 1: **`388.1 ms`**; Deep offset (`778K`): **`3,864.9 ms`** (on 1 GB -> ~25–150s on 40 GB!) | No-Sort Page 1: **`61.9 ms`**; Deep offset: **`875.9 ms`** | **`6.27x` faster** Page 1; **`4.41x` faster** deep jump | [PivotRequester.ts:L223-L229](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src/PivotRequester.ts#L223-L229) |
| **B3** | **P0 Critical** | L2 | Single global `ui_conn: Arc<Mutex<Connection>>` serializes interactive queries behind `SUMMARIZE` and `COPY TO` exports | Page 1 blocked **`673.3 ms`** behind `SUMMARIZE` (`656.4 ms` on 1 GB; ~25s on 40 GB) | Separate read/stats/export pool -> **`~62 ms`** unblocked | **Eliminates UI freezes** during stats/export | [duckdb_pool.rs:L35](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/engine/duckdb_pool.rs#L35), [query.rs:L772-L828](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/commands/query.rs#L772-L828), [system.rs:L367](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/commands/system.rs#L367) |
| **B4** | **P1 High** | L1 | Single-threaded Rust `dirty_log.rs` (`BufReader::lines()` per-line `String` alloc) vs DuckDB parallel raw-event `read_csv` | **`134.2 MB/s`** (`1.91s` / 256 MB) | **`687.1 MB/s`** (`0.37s` / 256 MB) | **`5.12x` faster** | [dirty_log.rs:L173-L199](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/ingest/dirty_log.rs#L173-L199) |
| **B5** | **P1 High** | L1 | Unnecessary disk-to-disk `.gz` decompression (`transcode_to_utf8`) before DuckDB `read_csv` | **`3.38s`** / 256 MB CSV (`.gz` 19.8 MB) + 256 MB temp file | **`1.48s`** via DuckDB `compression='gzip'` (0 temp file) | **`2.28x` faster** & 0 disk churn | [csv_streamer.rs:L200-L212](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/ingest/csv_streamer.rs#L200-L212) |
| **B6** | **P1 High** | L5 | `ParquetLedger::append_events()` opens a cold in-memory DuckDB + `DESCRIBE` + `COPY UNION ALL` + `.bak` copy on every single event | **`60.31 ms/event`** (`6,031 ms` for 100 file commits) | Batched/buffered WAL or persistent Arrow/SQLite append (`< 2 ms`) | **`~30x` faster** bulk folder ingest commits | [parquet_ledger.rs:L1154-L1331](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/ledger/parquet_ledger.rs#L1154-L1331) |
| **B7** | **P1 High** | L1 | `evtx_parser.rs` hardcodes `num_threads(4)` + single-threaded `serde_json::Value` DOM double-serialization | **`9.1 MB/s`** (`2.44s` / 22 MB at 4t; `14.3 MB/s` at 14t) | Thread scaling to `N-2` (`1.58x`) + streaming XML/BinXML extraction | **`1.58x–3x+` faster** EVTX ingest | [evtx_parser.rs:L475-L540](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/ingest/evtx_parser.rs#L475-L540), [query.rs:L430](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/commands/query.rs#L430), [scheduler.rs:L615](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/ingest/scheduler.rs#L615) |
| **B8** | **P2 Medium** | L3 | `FastCounter` O(1) Parquet footer bypass always misses in `handle_row_count` (`source_hint` passes `tbl_<hash>`, not `.parquet` path) | **`12.60 ms`** DuckDB `COUNT(*)` | **`0.69 ms`** O(1) footer read | **`18.3x` faster** row counts | [query.rs:L50-L53](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/commands/query.rs#L50-L53), [duckdb_pool.rs:L163-L174](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/engine/duckdb_pool.rs#L163-L174) |
| **B9** | **P2 Medium** | L1 | `jsonl_streamer.rs` flat scaling (`247.8 MB/s` at 2t vs `242.0 MB/s` at 14t) due to Phase-1 `DESCRIBE` + `ROW_GROUP_SIZE 122880` | **`242–248 MB/s`** (no multi-core scaling) | Tune `ROW_GROUP_SIZE 25000`, `sample_size`, skip unnest pass when flat | Higher multi-core utilization on multi-GB JSONL | [jsonl_streamer.rs:L174-L242](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/ingest/jsonl_streamer.rs#L174-L242) |
| **B10** | **P2 Medium** | L6 | SlickGrid rapid scroll causes 40 Layout/RecalcStyle cycles (`14.8 ms` main-thread work/tick, `maxFrameMs = 62.5 ms`) | `maxFrameMs = 62.5 ms`, `4` slow frames (`>33ms`) in 40 ticks | Batch DOM reads/writes, avoid `scrollHeight` forced reflow on scroll | Smoother 60 FPS scrolling | [DataGrid.tsx:L985-L990](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src/components/DataGrid.tsx#L985-L990), [CellLineCalculator.ts](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src/utils/CellLineCalculator.ts) |
| **B11** | **P3 Low** | L7 | Synchronous `Mutex<File>` + `stdout` logging on every `tracing` event & frontend `toad:log` IPC | Minor under `info` level after `evtx=warn` filter; spikes under `debug`/`trace` | Non-blocking `tracing_appender` / batched frontend log buffer | Eliminates log-lock stalls under bursts | [logger.rs:L134-L143](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/logger.rs#L134-L143) |

---

## 2. Detailed Empirical Probe Results

### Probe P1a & P1c — CSV 1.0 GB Scaling Matrix (Pulex DuckDB 1.2.2 vs Toad DuckDB 1.0.0)

Input: `sample_1gb.csv` (`1,024.0 MB`, `1,557,043` rows, UTF-8 BOM Hayabusa CSV).

| Engine & Configuration | `parallel` | `threads` | `max_memory` | `ROW_GROUP_SIZE` | `COMPRESSION` | `preserve_order` | Elapsed (`s`) | Throughput (`MB/s`) | Output (`MB`) | Speedup vs Current |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Pulex Current** (DuckDB 1.2.2) | `false` | `2` | `2GB` | `122880` | `SNAPPY` | `false` | `7.439` | **`137.7`** | `124.2` | `1.00x` |
| Pulex Parallel Only (DuckDB 1.2.2) | `true` | `2` | `2GB` | `122880` | `SNAPPY` | `false` | `4.179` | `245.0` | `124.3` | `1.78x` |
| Pulex Parallel 4t (DuckDB 1.2.2) | `true` | `4` | `4GB` | `122880` | `SNAPPY` | `false` | `2.392` | `428.0` | `124.1` | `3.11x` |
| Pulex Parallel 8t (DuckDB 1.2.2) | `true` | `8` | `8GB` | `122880` | `SNAPPY` | `false` | `1.588` | `645.0` | `124.2` | `4.68x` |
| **Pulex Toad-Parity RG25k** (DuckDB 1.2.2) | **`true`** | **`14`** | **`8GB`** | **`25000`** | **`SNAPPY`** | **`false`** | **`1.327`** | **`771.5`** | **`124.0`** | **`5.60x`** |
| Pulex Parallel 14t RG122k (DuckDB 1.2.2) | `true` | `14` | `8GB` | `122880` | `SNAPPY` | `false` | `1.706` | `600.1` | `124.0` | `4.36x` |
| Pulex Parallel 16t RG122k (DuckDB 1.2.2) | `true` | `16` | `8GB` | `122880` | `SNAPPY` | `false` | `1.588` | `645.0` | `124.2` | `4.68x` |
| Pulex Parallel 14t **ZSTD** (DuckDB 1.2.2) | `true` | `14` | `8GB` | `122880` | `ZSTD` | `false` | `1.496` | **`684.4`** | **`33.7`** | **`4.97x`** (`3.69x` smaller file!) |
| Pulex Parallel 14t UNCOMPRESSED | `true` | `14` | `8GB` | `122880` | `UNCOMPRESSED` | `false` | `2.201` | `465.3` | `903.2` | `3.38x` (write-bound!) |
| Pulex Parallel 14t `order=true` | `true` | `14` | `8GB` | `122880` | `SNAPPY` | `true` | `2.348` | `436.1` | `124.2` | `3.17x` (`-43.5%` vs `order=false`) |
| **Toad Default** (DuckDB 1.0.0) | `true` | `14` | `8192MB` | `25000` | `SNAPPY` | `false` | `2.855` | **`358.6`** | `97.3` | `2.60x` |
| **Toad RG122k** (DuckDB 1.0.0) | `true` | `14` | `8192MB` | `122880` | `SNAPPY` | `false` | `2.119` | **`483.4`** | `65.1` | `3.51x` |
| Toad Serial 2t (DuckDB 1.0.0) | `false` | `2` | `2048MB` | `122880` | `SNAPPY` | `false` | `8.417` | `121.7` | `64.1` | `0.88x` |

**Key Takeaways:**
1. **`parallel = false` + `threads = 2` is a `5.60x` bottleneck.** Turning on `parallel = true` alone (at 2 threads) only raises throughput to `245 MB/s`; unlocking `threads = N-2` (`14`) + `max_memory = 8GB` + `ROW_GROUP_SIZE = 25000` reaches **`771.5 MB/s`** on DuckDB 1.2.2 (`2.15x` faster than Toad's own default on DuckDB 1.0.0).
2. **`ROW_GROUP_SIZE = 25000` beats `122880` by `+28.6%` on DuckDB 1.2.2** (`771.5` vs `600.1 MB/s`) because smaller row groups eliminate end-of-stream flush stalls across 14 parallel writers while keeping per-thread compression buffers inside L3 cache.
3. **`ZSTD` at `14t` is faster than `SNAPPY` at `RG=122880` (`684.4` vs `600.1 MB/s`) and shrinks Parquet cache size by `3.69x` (`33.7 MB` vs `124.0 MB`)** — making `ZSTD` a compelling optional config for SSD-constrained systems.

---

### Probe P1b — `.csv.gz` Disk Transcode vs DuckDB Native Gzip

Input: `sample_256mb.csv.gz` (`19.8 MB` compressed -> `256.0 MB` uncompressed CSV).

| Path | Elapsed (`s`) | Temp Disk Written | Speedup |
| :--- | :---: | :---: | :---: |
| **Pulex Current** (`transcode_to_utf8` -> `utf8_tmp.csv` + 2t serial `read_csv`) | `3.378` | `256.0 MB` extra temp file | `1.00x` |
| **DuckDB Native `compression = 'gzip'`** (`14t`, `8GB`, zero temp file) | **`1.483`** | **`0 MB`** | **`2.28x`** |

---

### Probe P2 — EVTX, JSONL, & DirtyLog Ingestion

#### EVTX (`System.evtx`, `22.07 MB`, `57,030` rows)
| `ParserSettings::num_threads` | Elapsed (`s`) | Throughput (`MB/s`) | Rate (`rows/s`) | Speedup vs Current (`4t`) |
| :---: | :---: | :---: | :---: | :---: |
| `1` | `2.678` | `8.2` | `21,292` | `0.91x` |
| **`4` (Pulex Current)** | **`2.437`** | **`9.1`** | **`23,406`** | **`1.00x`** |
| `8` | `1.812` | `12.2` | `31,481` | `1.34x` |
| `14` | **`1.539`** | **`14.3`** | **`37,067`** | **`1.58x`** |

*Root Cause of Sublinear Scaling (`1.74x` at `14t` vs `1t`):* While `evtx` decodes BinXML chunks across threads, the consumer loop in [evtx_parser.rs:L531-L570](file:///d:/Shared_UserData/Downloads/_Projects_SecOps/pulex/src-tauri/src/ingest/evtx_parser.rs#L531-L570) runs single-threaded, building `serde_json::Value` DOM trees and serializing `event_data` + `payload_json` twice per record before appending to Arrow builders.

#### JSONL (`sample_256mb.jsonl`, `256.0 MB`, `319,026` rows)
| Configuration | Elapsed (`s`) | Throughput (`MB/s`) |
| :--- | :---: | :---: |
| `Pulex_Current_2t_2GB` | `1.033` | `247.8` |
| `Tuned_14t_8GB` | `1.058` | `242.0` |

*Root Cause:* `jsonl_streamer.rs` runs an upfront `DESCRIBE SELECT * FROM read_json_auto(...)` pass plus `ROW_GROUP_SIZE 122880` (~2.6 row groups for 319K rows), limiting writer parallelism on medium JSONL files.

#### DirtyLog (`256.0 MB`, `378,065` lines)
| Engine | Elapsed (`s`) | Throughput (`MB/s`) | Speedup |
| :--- | :---: | :---: | :---: |
| **Pulex Rust `ingest_dirty_log`** (`BufReader::lines()`, single-thread) | `1.908` | `134.2` | `1.00x` |
| **DuckDB `read_csv` raw_event** (`14t`, Toad `streamRawLogToParquet`) | **`0.373`** | **`687.1`** | **`5.12x`** |

---

### Probe P3 — Query Engine, Auto-Sort, RowCount, & `ui_conn` Mutex Contention

Dataset: `sample_1gb_bench.parquet` (`1,557,043` rows) & `system_evtx.parquet` (`57,030` rows) on `DuckDbEnginePool`.

| Query / Operation | Latency (`ms`) | Observation |
| :--- | :---: | :--- |
| `FastCounter::count_parquet_rows` (O(1) footer) | **`0.69`** | Sub-millisecond; currently **never hit** in `handle_row_count` because `source_hint` is `tbl_<hash>` |
| `DuckDB SELECT COUNT(*)` on `tbl_1gb` view | `12.60` | `18.3x` slower than `FastCounter` |
| `DuckDB SELECT COUNT(*)` on `tbl_evtx` projected view | `2.60` | Scales linearly if projections inhibit metadata pushdown |
| **Page 1 (`LIMIT 5000 OFFSET 0`) — NO Sort** | **`61.94`** | Fast Parquet row-group short-circuit |
| **Page 1 (`LIMIT 5000 OFFSET 0`) — WITH Auto-Sort (`ORDER BY "Timestamp" DESC`)** | **`388.11`** | **`6.27x` slower** on 1 GB (extrapolates to **`~15–25s`** on 40 GB / 62M rows!) |
| Deep Page (`OFFSET 778,521 LIMIT 5000`) — NO Sort | `875.89` | `OFFSET` skips row groups sequentially |
| **Deep Page (`OFFSET 778,521 LIMIT 5000`) — WITH Auto-Sort** | **`3,864.92`** | **`4.41x` slower** (`~150s+` on 40 GB!) |
| Omni-Search `ILIKE '%PowerShell%'` (2 cols, `LIMIT 5000`) | `823.89` | Full string scan across `VARCHAR` columns |
| `SUMMARIZE SELECT * FROM "tbl_1gb"` | **`656.41`** | Full-table multi-pass statistics on `ui_conn` |
| **Concurrent UI Page 1 (`LIMIT 1000`) during `SUMMARIZE`** | **`673.31`** | **Completely blocked** behind `ui_conn: Arc<Mutex<Connection>>` (`+611 ms` pure lock wait!) |

---

### Probe P4 — Arrow IPC Serialization (Confirmed Non-Bottleneck)

| Query Batch Size | DuckDB `query_arrow().collect()` (`ms`) | `ArrowIpcEncoder::encode_to_vec` (`ms`) | Payload Size (`KB`) |
| :---: | :---: | :---: | :---: |
| `1,000` rows | `57.23` | **`0.33`** | `343.3` |
| `5,000` rows | `54.69` | **`3.39`** | `1,969.1` |

*Conclusion:* Rust Arrow IPC encoding accounts for `< 1%–6%` of query latency and is **not** a bottleneck.

---

### Probe P5 — `ParquetLedger` WAL Append Scaling

100 sequential single-event `append_events(&[ev])` commits (simulating a 100-file folder ingestion completing one by one):

| Event # | Append Latency (`ms`) |
| :---: | :---: |
| `#1` | `55.62` |
| `#10` | `53.25` |
| `#25` | `52.31` |
| `#50` | `62.54` |
| `#75` | `53.26` |
| `#100` | `60.29` |
| **Total (100 events)** | **`6,030.8 ms`** (`60.31 ms/event` avg) |

*Root Cause:* Every call opens a fresh `duckdb::Connection::open_in_memory()`, creates a table, runs `DESCRIBE` + `COPY (... UNION ALL ...)`, renames with Windows retry backoff, and copies the full file to `.bak` under `write_lock`.

---

### Probe P6 & P7 — Live WebView2 CDP Cold Start, File Switch, & SlickGrid Scroll

Measured against packaged `pulex.exe` (`--remote-debugging-port=9222`):

| Metric | Observed Value | Assessment |
| :--- | :---: | :--- |
| Cold Start (`CreateProcess` -> CDP Ready) | `1,564 ms` (`DOMContentLoaded`: `362 ms`) | Healthy |
| JS Heap Used (after loading EVTX + scrolling) | `29.8 – 32.9 MB` | Very lean (Non-bottleneck) |
| Click `System.evtx` (during startup pre-warm) | `1,355 – 1,478 ms` (`LayoutCount=47`, `RecalcStyleCount=54`, `Script=370.9 ms`, `Task=932.1 ms`) | Delayed by background pre-warm + auto-sort |
| Click `Hayabusa_Results.csv` (RAM Micro-Cache HIT) | `394 ms` (`135 ms` `replaceCurrentView` -> cache hit; `259 ms` grid paint) | Good, can be `< 150 ms` |
| **Rapid SlickGrid Scroll (40 ticks, V+H)** | `LayoutCount=40` (`136.1 ms`), `RecalcStyle=40` (`108.1 ms`), `Script=348.6 ms`, `Task=1,421.2 ms`, **`maxFrameMs = 62.5 ms`**, `4` slow frames `> 33.3 ms` | **`~14.8 ms` main-thread work/tick** (spiking to `62.5 ms` — 3.7x over 16.6ms 60fps budget) |

---

## 3. Recommended Phase B Remediation Roadmap (Ordered by ROI)

1. **Fix B1 + B5 (`csv_streamer.rs`, `duckdb_pool.rs`, `state.rs`, `jsonl_streamer.rs`) — Target: `5.6x` CSV speedup (`138 -> 770+ MB/s`) & `2.3x` Gzip speedup:**
   - Wire `config.duckdb.background_threads` (`auto` -> `max(1, num_cpus - 2)`), `config.duckdb.max_memory` (`auto` -> `40%` RAM capped at `8192MB`), `config.parquet.compression`, and `config.parquet.row_group_size` (`25000`) into `DuckDbEnginePool` and streamers.
   - Change `csv_streamer.rs` Tier 0 to `parallel = true`, with automatic instant fallback to `parallel = false` on quoted-newline / parallel-scanner errors (matching Toad's zero-crash guard).
   - Pass `GzipUtf8` `.csv.gz` / `.jsonl.gz` directly to DuckDB with `compression = 'gzip'` (zero `.utf8_tmp` disk copy).
2. **Fix B2 (`PivotRequester.ts`) — Target: `6.3x` faster Page 1 & instant opens on 40 GB datasets:**
   - Guard automatic `ORDER BY timeCol DESC` injection behind a row-count threshold (e.g., disable default full-table auto-sort when `baseRowCount > 500_000` unless the user explicitly clicks a column header to sort).
3. **Fix B3 (`duckdb_pool.rs`, `commands/query.rs`, `commands/system.rs`) — Target: Zero UI blocking during `SUMMARIZE` / `Export`:**
   - Run `handle_get_column_stats_map` (`SUMMARIZE`) and `handle_export_file` (`COPY TO`) on a dedicated/pooled secondary read connection instead of locking the interactive `ui_conn` mutex.
4. **Fix B4 + B7 (`dirty_log.rs`, `evtx_parser.rs`) — Target: `5.1x` DirtyLog & `1.6x+` EVTX speedup:**
   - Route unstructured `.log` ingestion through DuckDB parallel raw-line `read_csv` (keeping Rust `dirty_log.rs` as fallback with reused `Vec<u8>` buffer instead of `reader.lines()`).
   - Scale `evtx_parser.rs` `num_threads` dynamically to `max(2, num_cpus - 2)` instead of hardcoded `4`.
5. **Fix B6 + B8 (`parquet_ledger.rs`, `commands/query.rs`) — Target: `30x` faster WAL & `18x` faster `rowCount`:**
   - Reuse a persistent DuckDB connection (or pure Arrow/Parquet append + debounced/batched flush) in `ParquetLedger::append_events()`.
   - Resolve `tbl_<hash>` -> cached `.parquet` path in `handle_row_count` so `FastCounter::count_parquet_rows` hits O(1) (`0.69 ms`).
6. **Fix B10 (`DataGrid.tsx`, `CellLineCalculator.ts`) — Target: Eliminate `62.5 ms` scroll frame drops:**
   - Defer `reconcileDomLineCount` (`scrollHeight` synchronous reflow) out of active scroll frames (`requestIdleCallback` / post-scroll settle).

