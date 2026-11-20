# Pulex v3.0.0 (Tauri v2 Edition) — High-Performance Forensic Data Analyzer

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Engine](https://img.shields.io/badge/Engine-DuckDB%201.2.2%20%7C%20Rust%20%7C%20Tauri%20v2-green.svg)](https://duckdb.org/)
[![Version](https://img.shields.io/badge/Version-3.0.0-brightgreen.svg)](releases/)
[![Platform](https://img.shields.io/badge/Platform-Windows%20x64%20(Portable)-orange.svg)](releases/)

**Pulex** is a lightweight, high-performance tabular data and security forensic log analyzer for Windows x64, built on **Tauri v2**, **Rust**, **DuckDB 1.2.2**, and **Apache Arrow IPC** zero-copy streaming.

---

## 1. Key Capabilities

- **Zero-Copy Streaming Architecture**:
  - Native Rust backend coupled with in-process **DuckDB 1.2.2** (separated UI exploration connection + bounded background worker pool with disk spill).
  - Binary **Apache Arrow IPC** transport (`0xFFFFFFFF` continuation framing + embedded `toadSchema` v2 metadata with `dialect: "duckdb"`).
  - Virtualized **SlickGrid** viewport with **20-Line Smart Expand** (`25px` compact, dynamic `N * 18 + 8px` up to `368px` ceiling with slim scrollbar, scroll-position persistence, and modal overlay highlight suppression).
- **Universal Forensic & Tabular Ingestion**:
  - **Windows Event Logs (`.evtx`)**: 16-column forensic schema (`EventRecordID`, `TimeCreated`, `EventID`, `Level`, `LevelName`, `Provider`, `Channel`, `Computer`, `UserID`, `Param1`..`Param3`, `Binary`, `EventData`, `RawEvent`) with `catch_unwind` chunk resilience.
  - **Delimited & Log Streams (`.csv`, `.tsv`, `.tab`, `.txt`, `.log`, `.gz`)**: 8 KB Binary Guard (rejecting PE/ELF/ZIP/binary blobs) + streaming 64 KB UTF-8 / UTF-16LE / UTF-16BE / Multi-Member GZIP transcoder + 3-tier RFC-4180 CSV fallback matrix.
  - **Structured & Relational Formats (`.parquet`, `.json`, `.jsonl`, `.ndjson`, `.duckdb`, `.sqlite`, `.sqlite3`, `.db`)**: Zero-copy Parquet metadata counting (`parquet_metadata`), dynamic 2-phase JSONL unnesting, and native `ATTACH` multi-table browsing for DuckDB and SQLite databases.
- **Interactive Analytics & Column Parsing**:
  - Hierarchical multi-column **Pivot Trees** (`sum`, `avg`, `min`, `max`, `uniq`, record counts) and histogram brush filtering.
  - Compound boolean **Filter Builder** (`AND` / `OR` nested groups, drag-and-drop rule reordering, timestamp brackets, and cross-slash Windows/UNC forensic path normalization).
  - **Pulex Parse (`v3.0.0`)**: Visual delimiter/KV/JSON/Nested-JSON flattener and Regex capture-group extractor with persistent Derived Views (`derived_views.json`) surviving engine restarts.
  - **Native Desktop Integration**: Multi-project Parquet WAL Ledger (`toad_ledger.parquet`), native menu bar (`File`, `View`, `Help`), single-instance forwarding, CLI/file-association startup loading, multi-window exploration, and Parquet/CSV/JSONL export.
- **Strict Portable Isolation & Binary Hardening**:
  - Self-contained portable execution storing `config.ini`, `toad_ledger.parquet`, `derived_views.json`, `logs/pulex.log` (20 MB x 5 rotation), `temp/`, and `WebView2/` inside adjacent `pulexAppData_<5chars>`.
  - Compiled with Windows **Control Flow Guard (`GUARD_CF`)**, High-Entropy ASLR, NX, strict CSP (`script-src 'self'`), URL scheme allowlisting (`http`/`https`), and `0` npm audit vulnerabilities.

---

## 2. Canonical Directory Layout

```text
pulex/
├── pulex.exe                       # Canonical local development/test executable
├── duckdb.dll                      # DuckDB 1.2.2 runtime library
├── WebView2Loader.dll              # Microsoft Edge WebView2 loader
├── package.json                    # Frontend dependencies (React 18, Vite 6, BlueprintJS)
├── index.html                      # CSP-compliant application entry
├── packages/
│   ├── reltab/                     # Relational algebra AST, DuckDB dialect & Arrow IPC protocol
│   ├── aggtree/                    # Hierarchical pivot tree builder
│   └── host-bridge/                # Tauri v2 / Electron / V8 host adapter layer
├── src/                            # React + SlickGrid frontend
│   ├── main.tsx                    # Bootstrap, drag-drop, CLI, menu, & export wiring
│   ├── bootstrapErrorTrap.ts       # Safe DOM error overlay
│   ├── actions.ts                  # View state & persistent derived view actions
│   ├── components/                 # DataGrid, DataSourceSidebar, RegexDialog, AppPane, etc.
│   └── utils/                      # CellLineCalculator (20-line), DuckDbJsonSqlBuilder, etc.
├── src-tauri/                      # Native Rust Tauri v2 core
│   ├── build.rs                    # Multi-size PE icon/version/manifest COFF builder & DLL staging
│   ├── .cargo/config.toml          # Control Flow Guard (CFG) linker configuration
│   ├── src/
│   │   ├── engine/                 # DuckDB pool, QueryExp SQL compiler, Arrow encoder
│   │   ├── ingest/                 # Binary guard, UTF-16/GZIP transcoder, EVTX/CSV/JSONL, SJF queue
│   │   ├── ledger/                 # Parquet WAL ledger & 6-scenario startup reconciler
│   │   └── commands/               # IPC command router, query, workspace, & system handlers
│   └── tests/                      # 14 integration & adversarial Rust test suites (+ lib/bin unit tests)
├── tests/                          # Master Node test runner (5 suites)
├── tools/
│   └── package_pulex.ps1           # Zero-duplicate production release packager
└── releases/                       # Canonical portable distribution artifacts
    ├── Pulex-3.0.0-x64-portable.exe
    ├── Pulex-3.0.0-x64-portable.zip
    ├── HelloKittyXamKinLung.Pulex.yaml
    ├── SHA256SUMS.txt
    ├── v3.0.0_NOTES.md
    └── README.md
```

---

## 3. Build, Test & Release Commands

### Frontend Build & Master Node Test Suites
```powershell
npm install
npm run build
node tests/run_all_tests.js
```

### Rust Core Unit & Integration Tests
```powershell
cd src-tauri
cargo test --locked
```

### Production Release Packaging
```powershell
powershell -ExecutionPolicy Bypass -File tools/package_pulex.ps1
```

---

## 4. Publisher & License

- **Publisher**: `HelloKittyXamKinLung` (`p1rkr41n@users.noreply.github.com`)
- **License**: MIT
