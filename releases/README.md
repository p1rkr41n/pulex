# Pulex v3.0.1 — Portable Release Distribution

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)
[![Engine](https://img.shields.io/badge/Engine-DuckDB%201.2.2%20%7C%20Rust%20%7C%20Tauri%20v2-green.svg)](https://duckdb.org/)
[![Version](https://img.shields.io/badge/Version-3.0.1-brightgreen.svg)](./)
[![Platform](https://img.shields.io/badge/Platform-Windows%20x64%20(Portable)-orange.svg)](./)

**Pulex** is packaged as a zero-install, self-contained Windows x64 portable desktop analyzer powered by **Tauri v2**, **DuckDB 1.2.2**, and **Apache Arrow IPC**.

---

## 1. Release Artifacts

| Artifact | Contents | Purpose |
| :--- | :--- | :--- |
| `Pulex-3.0.1-x64-portable.zip` | `Pulex-3.0.1-x64-portable.exe`, `duckdb.dll`, `WebView2Loader.dll` | **Recommended**: Complete standalone portable archive ready to extract and run anywhere |
| `Pulex-3.0.1-x64-portable.exe` | Native Tauri v2 executable (Control Flow Guard enabled, multi-size embedded icons & PerMonitorV2 DPI manifest) | Standalone binary for updating an existing Pulex directory containing companion DLLs |
| `HelloKittyXamKinLung.Pulex.yaml` | WinGet v1.6.0 singleton manifest (`zip` + `portable`) | Automated installation via Windows Package Manager |
| `SHA256SUMS.txt` | Lowercase SHA-256 digests | Cryptographic integrity verification |
| `v3.0.1_NOTES.md` | Release notes | Summary of features, fixes, and security hardenings in v3.0.1 |

---

## 2. Quick Start

1. **Extract & Launch**: Extract `Pulex-3.0.1-x64-portable.zip` to any folder (ensuring `Pulex-3.0.1-x64-portable.exe`, `duckdb.dll`, and `WebView2Loader.dll` remain in the same directory) and launch `Pulex-3.0.1-x64-portable.exe`.
2. **Open Datasets**:
   - Drag and drop individual files or entire folders onto the Pulex window.
   - Use **File -> Open File... (`Ctrl+O`)** or **File -> Open Folder... (`Ctrl+Shift+O`)**.
   - Pass file, folder, or `.pulex` / `.toad` / `.tad` workspace paths directly on the command line:
     ```powershell
     Pulex-3.0.1-x64-portable.exe C:\Logs\Security.evtx
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
Get-FileHash .\Pulex-3.0.1-x64-portable.zip, .\Pulex-3.0.1-x64-portable.exe -Algorithm SHA256
Get-Content .\SHA256SUMS.txt
```
