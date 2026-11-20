# Pulex v3.0.2 — Single-File Portable Release Distribution

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)
[![Engine](https://img.shields.io/badge/Engine-DuckDB%201.2.2%20%7C%20Rust%20%7C%20Tauri%20v2-green.svg)](https://duckdb.org/)
[![Version](https://img.shields.io/badge/Version-3.0.2-brightgreen.svg)](./)
[![Platform](https://img.shields.io/badge/Platform-Windows%20x64%20(Single--File%20Portable)-orange.svg)](./)

**Pulex** is packaged as a true **single-file, zero-install** Windows x64 portable desktop analyzer powered by **Tauri v2**, **DuckDB 1.2.2**, and **Apache Arrow IPC**. Copy the standalone `.exe` to any folder and run it immediately with zero external DLL dependencies.

---

## 1. Release Artifacts

| Artifact | Contents | Purpose |
| :--- | :--- | :--- |
| `Pulex-3.0.2-x64-portable.exe` | Self-contained Tauri v2 executable (delay-loaded embedded `duckdb.dll` + `WebView2Loader.dll`, Control Flow Guard enabled, multi-size transparent Pulex icons & PerMonitorV2 DPI manifest) | **Direct Standalone Binary**: Copy anywhere and double-click to run immediately |
| `Pulex-3.0.2-x64-portable.zip` | `Pulex-3.0.2-x64-portable.exe` | **Browser-Friendly Archive**: Frictionless download for Edge/Chrome & WinGet distribution |
| `HelloKittyXamKinLung.Pulex.yaml` | WinGet v1.6.0 singleton manifest (`zip` + `portable`) | Automated installation via Windows Package Manager |
| `SHA256SUMS.txt` | Lowercase SHA-256 digests | Cryptographic integrity verification |
| `v3.0.2_NOTES.md` | Release notes | Summary of single-file portable packaging and transparent branding in v3.0.2 |

---

## 2. Quick Start

1. **Copy & Launch**: Copy `Pulex-3.0.2-x64-portable.exe` (directly or extracted from `Pulex-3.0.2-x64-portable.zip`) to any folder and launch it. On first launch, Pulex automatically extracts its runtime DLLs into `pulexAppData_<id>\bin\` (or `%TEMP%\pulex_runtime_3.0.2\` if run from a read-only folder).
2. **Open Datasets**:
   - Drag and drop individual files or entire folders onto the Pulex window.
   - Use **File -> Open File... (`Ctrl+O`)** or **File -> Open Folder... (`Ctrl+Shift+O`)**.
   - Pass file, folder, or `.pulex` / `.toad` / `.tad` workspace paths directly on the command line:
     ```powershell
     Pulex-3.0.2-x64-portable.exe C:\Logs\Security.evtx
     ```
3. **Portable AppData**:
   - Pulex creates a single local `pulexAppData_<id>` folder adjacent to the executable containing `config.ini`, `toad_ledger.parquet`, `derived_views.json`, `logs/pulex.log`, `temp/`, and `WebView2/`. No user profile or registry keys are polluted.

---

## 3. Supported Formats

| Category | Extensions | Notes |
| :--- | :--- | :--- |
| **Delimited & Text Logs** | `.csv`, `.tsv`, `.tab`, `.txt`, `.log`, `.gz` | Automatic UTF-8 / UTF-16LE / UTF-16BE / GZIP transcoding + 3-tier CSV fallback |
| **Columnar Parquet** | `.parquet` | Zero-copy `parquet_scan` + O(1) `parquet_metadata` row counts |
| **Windows Event Logs** | `.evtx` | 16-column forensic BinXML/JSON extraction with `catch_unwind` safety |
| **JSON & NDJSON** | `.json`, `.jsonl`, `.ndjson` | 2-phase schema inference and nested JSON column extraction |
| **Embedded Databases** | `.duckdb`, `.sqlite`, `.sqlite3`, `.db` | Native `ATTACH` inspection and multi-table browsing |
| **Workspaces** | `.pulex`, `.toad`, `.tad` | Saves and restores active dataset path, filters, pivots, and column layouts |

---

## 4. Integrity Verification (PowerShell)

```powershell
Get-FileHash .\Pulex-3.0.2-x64-portable.zip, .\Pulex-3.0.2-x64-portable.exe -Algorithm SHA256
Get-Content .\SHA256SUMS.txt
```
