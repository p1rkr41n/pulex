import "./bootstrapErrorTrap";

// Global BigInt serialization polyfill for Arrow / DuckDB query payload serialization
try {
  if (
    typeof BigInt !== "undefined" &&
    Object.isExtensible(BigInt.prototype) &&
    !(BigInt.prototype as any).toJSON
  ) {
    (BigInt.prototype as any).toJSON = function () {
      const num = Number(this);
      return Number.isSafeInteger(num) ? num : this.toString();
    };
  }
} catch {}

// Global Console Interceptor & Lossless Queued Buffer for comprehensive Pulex diagnostics
interface QueuedLogEntry {
  level: string;
  message: string;
  details?: string | null;
  timestamp: number;
}

const rawConsoleLog = typeof console !== "undefined" ? console.log : () => {};
const rawConsoleWarn = typeof console !== "undefined" ? console.warn : () => {};
const rawConsoleError = typeof console !== "undefined" ? console.error : () => {};
const rawConsoleInfo = typeof console !== "undefined" ? console.info : () => {};
const rawConsoleDebug = typeof console !== "undefined" ? console.debug : () => {};

let isForwarding = false;
let isLoggingReentrant = false;
const logBuffer: QueuedLogEntry[] = [];
let bufferTimer: any = null;

function flushLogBuffer() {
  if (bufferTimer) {
    clearTimeout(bufferTimer);
    bufferTimer = null;
  }
  if (logBuffer.length === 0 || isForwarding) return;
  if (typeof window !== "undefined" && (window as any).__TAURI__?.core?.invoke) {
    const entriesToSend = logBuffer.splice(0, Math.min(logBuffer.length, 100));
    isForwarding = true;
    (window as any).__TAURI__.core
      .invoke("toad:log", { entries: entriesToSend })
      .catch(() => {})
      .finally(() => {
        isForwarding = false;
        if (logBuffer.length > 0) {
          flushLogBuffer();
        }
      });
  } else {
    if (logBuffer.length > 2000) {
      logBuffer.splice(0, logBuffer.length - 2000);
    }
  }
}

function forwardLogToBackend(level: string, message: string, details?: any) {
  try {
    const strDetails = details
      ? typeof details === "string"
        ? details
        : JSON.stringify(details)
      : null;

    const entry: QueuedLogEntry = {
      level,
      message,
      details: strDetails,
      timestamp: Date.now(),
    };

    logBuffer.push(entry);

    if (level === "error" || level === "warn") {
      flushLogBuffer();
    } else {
      if (logBuffer.length >= 25) {
        flushLogBuffer();
      } else if (!bufferTimer) {
        bufferTimer = setTimeout(flushLogBuffer, 50);
      }
    }
  } catch {}
}

function formatConsoleArgs(args: any[]): { message: string; details?: any } {
  if (!args || args.length === 0) return { message: "" };
  const parts: string[] = [];
  let details: any = null;
  for (const arg of args) {
    if (arg instanceof Error) {
      parts.push(arg.message || String(arg));
      details = { stack: arg.stack, ...(details || {}) };
    } else if (typeof arg === "object" && arg !== null) {
      try {
        parts.push(JSON.stringify(arg));
      } catch {
        parts.push(String(arg));
      }
    } else {
      parts.push(String(arg));
    }
  }
  return { message: parts.join(" "), details };
}

if (typeof console !== "undefined") {
  console.log = function (...args: any[]) {
    rawConsoleLog.apply(console, args);
    if (!isLoggingReentrant) {
      isLoggingReentrant = true;
      try {
        const { message, details } = formatConsoleArgs(args);
        forwardLogToBackend("info", message, details);
      } finally {
        isLoggingReentrant = false;
      }
    }
  };
  console.info = function (...args: any[]) {
    rawConsoleInfo.apply(console, args);
    if (!isLoggingReentrant) {
      isLoggingReentrant = true;
      try {
        const { message, details } = formatConsoleArgs(args);
        forwardLogToBackend("info", message, details);
      } finally {
        isLoggingReentrant = false;
      }
    }
  };
  console.warn = function (...args: any[]) {
    rawConsoleWarn.apply(console, args);
    if (!isLoggingReentrant) {
      isLoggingReentrant = true;
      try {
        const { message, details } = formatConsoleArgs(args);
        forwardLogToBackend("warn", message, details);
      } finally {
        isLoggingReentrant = false;
      }
    }
  };
  console.error = function (...args: any[]) {
    rawConsoleError.apply(console, args);
    if (!isLoggingReentrant) {
      isLoggingReentrant = true;
      try {
        const { message, details } = formatConsoleArgs(args);
        forwardLogToBackend("error", message, details);
      } finally {
        isLoggingReentrant = false;
      }
    }
  };
  console.debug = function (...args: any[]) {
    rawConsoleDebug.apply(console, args);
    if (!isLoggingReentrant) {
      isLoggingReentrant = true;
      try {
        const { message, details } = formatConsoleArgs(args);
        forwardLogToBackend("debug", message, details);
      } finally {
        isLoggingReentrant = false;
      }
    }
  };
}

if (typeof window !== "undefined") {
  window.addEventListener("error", (e) => {
    rawConsoleError("[UNCAUGHT-WINDOW-ERROR]", e.message, e.error?.stack);
    forwardLogToBackend("error", `[UNCAUGHT-WINDOW-ERROR] ${e.message}`, {
      stack: e.error?.stack,
      filename: e.filename,
      lineno: e.lineno,
      colno: e.colno,
    });
  });
  window.addEventListener("unhandledrejection", (e) => {
    const msg = e.reason?.message || String(e.reason);
    rawConsoleError("[UNCAUGHT-PROMISE-REJECTION]", msg, e.reason?.stack);
    forwardLogToBackend("error", `[UNCAUGHT-PROMISE-REJECTION] ${msg}`, {
      stack: e.reason?.stack,
    });
  });
}

import React from "react";
import ReactDOM from "react-dom/client";
import { mkRef, refContainer, mutableGet, StateRef } from "oneref";
import log from "loglevel";
import { DataSourcePath, RemoteReltabConnection } from "reltab";
import { getHostBridge, setHostBridge, TauriHostAdapter } from "host-bridge";
import { Intent } from "@blueprintjs/core";

import "./toadviewer";
import {
  AppPane,
  AppPaneBaseProps,
  ExportFormat,
  ParquetExportOptions,
  MultipleLoadingManager,
  ProjectWorkspaceManager,
  PivotRequester,
  AppState,
  ViewParams,
  actions,
  initAppState,
} from "./toadviewer";
import { getAppToaster } from "./components/AppPane";
import { toggleActiveTheme } from "./toadTheme";

const SUPPORTED_DATA_EXTS = new Set([
  ".csv",
  ".tsv",
  ".tab",
  ".txt",
  ".log",
  ".parquet",
  ".json",
  ".jsonl",
  ".ndjson",
  ".evtx",
  ".gz",
  ".sqlite",
  ".sqlite3",
  ".db",
  ".duckdb",
]);

const WORKSPACE_EXTS = new Set([".tad", ".toad", ".pulex"]);

function getLowerExtension(filePath: string): string {
  const idx = filePath.lastIndexOf(".");
  return idx >= 0 ? filePath.substring(idx).toLowerCase() : "";
}

function getBaseFileName(filePath: string): string {
  return filePath.split(/[/\\]/).pop() || filePath;
}

async function bootstrap() {
  log.setLevel(log.levels.TRACE);
  forwardLogToBackend("info", "Pulex Desktop Frontend bootstrap started");

  const bridge =
    typeof window !== "undefined" && (window as any).__TAURI__
      ? new TauriHostAdapter()
      : getHostBridge();
  setHostBridge(bridge);

  const appState = new AppState();
  const stateRef = mkRef(appState);
  const [App] = refContainer<AppState, AppPaneBaseProps>(stateRef, AppPane);

  try {
    await bridge.invoke("toad:init_main").catch(() => {});
    try {
      const cfg = await bridge.getConfig();
      if (cfg && cfg.general) {
        if (cfg.general.page_size) {
          actions.setDefaultPageSize(Number(cfg.general.page_size));
        }
        MultipleLoadingManager.getInstance().setConfig({
          maxConcurrentFiles: cfg.general.max_concurrent_files,
          maxConcurrentGB: cfg.general.max_concurrent_gb,
        });
      }
    } catch (e) {
      log.warn("Could not load Pulex config in renderer:", e);
    }

    const rtc = new RemoteReltabConnection(bridge);
    await initAppState(rtc, stateRef);
    const _pivotRequester = new PivotRequester(stateRef);

    const openURL = (url: string) => {
      bridge.openExternal(url);
    };

    const appClipboard = {
      writeText: (text: string) => bridge.copyToClipboard(text),
    };

    const newWindowFromDSPath = (
      dsPath: DataSourcePath,
      _sRef: StateRef<AppState>
    ) => bridge.openNewWindow(dsPath);

    // Event bridges for forensics & workspace
    const onIngestProgressEvent = (payload: any) => {
      MultipleLoadingManager.getInstance().handleIngestProgress(payload, stateRef);
      if (
        payload &&
        (payload.status === "READY" ||
          payload.status === "FAILED" ||
          payload.status === "CANCELLED")
      ) {
        ProjectWorkspaceManager.getInstance().notify();
      }
    };
    bridge.onEvent("toad:ingest-progress", onIngestProgressEvent);
    bridge.onEvent("toad-ingest-progress", onIngestProgressEvent);
    bridge.onEvent("project:workspaceChanged", () => {
      ProjectWorkspaceManager.getInstance().notify();
    });
    bridge.onEvent("cache:invalidate", (payload: any) => {
      MultipleLoadingManager.getInstance().clearFirstPageCache(payload?.sourcePath);
    });

    // =========================================================================
    // Unified File / Folder / Workspace Loader (Drag-Drop, CLI, Menu, Assoc)
    // =========================================================================
    const openFirstDataset = async (filePath: string, savedViewParams?: ViewParams) => {
      const ext = getLowerExtension(filePath);
      const isDatabase =
        ext === ".sqlite" || ext === ".sqlite3" || ext === ".db" || ext === ".duckdb";
      let dsPath: DataSourcePath = {
        sourceId: { providerName: "localfs", resourceId: filePath },
        path: ["."],
      };
      if (isDatabase) {
        try {
          const children: any = await bridge.invoke("toad:get_children", {
            sourceId: dsPath.sourceId,
            path: ["."],
          });
          if (Array.isArray(children) && children.length > 0 && children[0]?.id) {
            dsPath = {
              sourceId: { providerName: "localfs", resourceId: filePath },
              path: [String(children[0].id)],
            };
          }
        } catch {}
      }
      await actions.openDataSourcePath(dsPath, stateRef, savedViewParams);
    };

    const ingestAndOpenPaths = async (rawPaths: string[]) => {
      if (!rawPaths || !Array.isArray(rawPaths) || rawPaths.length === 0) return;
      const cleaned = rawPaths
        .map((p) => (typeof p === "string" ? p.trim() : ""))
        .filter((p) => p.length > 0 && !p.startsWith("-"));
      if (cleaned.length === 0) return;

      try {
        let statEntries: Array<{
          path: string;
          exists: boolean;
          isDirectory: boolean;
          isFile: boolean;
        }> = [];
        try {
          const statRes: any = await bridge.invoke("toad:stat_path", { paths: cleaned });
          if (statRes && Array.isArray(statRes.entries)) {
            statEntries = statRes.entries;
          }
        } catch {}

        const directories: string[] = [];
        const workspaceFiles: string[] = [];
        const dataFiles: string[] = [];

        if (statEntries.length > 0) {
          for (const entry of statEntries) {
            if (!entry.exists) continue;
            if (entry.isDirectory) {
              directories.push(entry.path);
            } else if (entry.isFile) {
              const ext = getLowerExtension(entry.path);
              if (WORKSPACE_EXTS.has(ext)) {
                workspaceFiles.push(entry.path);
              } else if (SUPPORTED_DATA_EXTS.has(ext) || ext.length > 0) {
                dataFiles.push(entry.path);
              }
            }
          }
        } else {
          for (const p of cleaned) {
            const ext = getLowerExtension(p);
            if (WORKSPACE_EXTS.has(ext)) {
              workspaceFiles.push(p);
            } else if (SUPPORTED_DATA_EXTS.has(ext)) {
              dataFiles.push(p);
            }
          }
        }

        // 1. Handle saved workspace files (.pulex / .toad / .tad)
        for (const wsFile of workspaceFiles) {
          try {
            const loaded: any = await bridge.invoke("toad:load_workspace_file", {
              filePath: wsFile,
              workspacePath: wsFile,
            });
            let wsPayload = loaded?.workspace || loaded;
            if (typeof wsPayload?.contents === "string") {
              try {
                wsPayload = JSON.parse(wsPayload.contents);
              } catch {}
            }
            if (wsPayload?.activeProjectId) {
              await ProjectWorkspaceManager.getInstance().setActiveProject(
                String(wsPayload.activeProjectId)
              );
            }
            let restoredVP: ViewParams | undefined;
            const rawVP = wsPayload?.viewParams || wsPayload?.contents?.viewParams;
            if (rawVP) {
              try {
                restoredVP = ViewParams.deserialize(rawVP);
              } catch {}
            }
            const restoredDsPath = wsPayload?.dsPath || wsPayload?.contents?.dsPath;
            if (restoredDsPath) {
              await actions.openDataSourcePath(restoredDsPath, stateRef, restoredVP);
              return;
            }
          } catch (wsErr) {
            console.error("[main.tsx] Failed loading workspace file:", wsErr);
          }
        }

        if (directories.length === 0 && dataFiles.length === 0) {
          return;
        }

        const pwManager = ProjectWorkspaceManager.getInstance();
        const ws = await pwManager.getWorkspace();
        let activeProjectId =
          ws.activeProjectId || (ws.projects && ws.projects[0]?.projectId);
        if (!activeProjectId) {
          activeProjectId = await pwManager.createProject("Untitled-1");
        }

        let firstCandidate: string | null = null;

        // 2. Recursively add dropped/selected folders
        for (const dirPath of directories) {
          const discovered = await pwManager.addFolderToProject(activeProjectId, dirPath);
          if (!firstCandidate && discovered.length > 0) {
            firstCandidate = discovered[0];
          }
        }

        // 3. Add individual files
        if (dataFiles.length > 0) {
          await pwManager.addFilesToProject(activeProjectId, dataFiles);
          if (!firstCandidate) {
            firstCandidate = dataFiles[0];
          }
        }

        // 4. Open first discovered/dropped file in Main DataGrid
        if (firstCandidate) {
          await openFirstDataset(firstCandidate);
        }
      } catch (err) {
        console.error("[main.tsx] Error ingesting paths:", err);
      }
    };

    // =========================================================================
    // Single Deduplicated Native Drag-and-Drop Listener
    // =========================================================================
    let lastDropSig = "";
    let lastDropTime = 0;

    const onDropPayload = (payload: any) => {
      let paths: string[] = [];
      if (Array.isArray(payload)) {
        paths = payload;
      } else if (payload && Array.isArray(payload.paths)) {
        paths = payload.paths;
      }
      if (paths.length === 0) return;

      const now = Date.now();
      const sig = paths.join("|");
      if (sig === lastDropSig && now - lastDropTime < 350) {
        return;
      }
      lastDropSig = sig;
      lastDropTime = now;
      ingestAndOpenPaths(paths);
    };

    if (typeof window !== "undefined") {
      window.addEventListener("dragover", (e) => e.preventDefault(), false);
      window.addEventListener("drop", (e) => e.preventDefault(), false);

      if ((window as any).__TAURI__) {
        import("@tauri-apps/api/webviewWindow")
          .then(({ getCurrentWebviewWindow }) => {
            const win = getCurrentWebviewWindow();
            if (win && typeof win.onDragDropEvent === "function") {
              win.onDragDropEvent((event) => {
                if (event.payload.type === "drop") {
                  onDropPayload(event.payload.paths);
                }
              });
            }
          })
          .catch(() => {
            try {
              bridge.onEvent("tauri://drag-drop", (payload: any) => {
                onDropPayload(payload);
              });
            } catch {}
          });
      } else {
        try {
          bridge.onEvent("tauri://drag-drop", (payload: any) => {
            onDropPayload(payload);
          });
        } catch {}
      }
    }

    // =========================================================================
    // Single-Instance Forwarding & Startup CLI / Secondary Window Launch
    // =========================================================================
    bridge.onEvent("toad:single-instance-open", (payload: any) => {
      const rawArgs: string[] = Array.isArray(payload?.args)
        ? payload.args.slice(1)
        : Array.isArray(payload)
        ? payload.slice(1)
        : [];
      if (rawArgs.length > 0) {
        ingestAndOpenPaths(rawArgs);
      }
    });

    (async () => {
      try {
        if (typeof window !== "undefined" && window.location?.search) {
          const params = new URLSearchParams(window.location.search);
          const openFileParam = params.get("openFile");
          if (openFileParam) {
            await openFirstDataset(openFileParam);
            return;
          }
        }
        const startupRes: any = await bridge.invoke("toad:get_startup_args", {});
        if (startupRes && Array.isArray(startupRes.files) && startupRes.files.length > 0) {
          await ingestAndOpenPaths(startupRes.files);
        }
      } catch {}
    })();

    // =========================================================================
    // Native Desktop Menu Bar Actions
    // =========================================================================
    bridge.onEvent("toad:menu-action", async (payload: any) => {
      const actionId = typeof payload === "string" ? payload : payload?.action;
      if (!actionId) return;

      try {
        switch (actionId) {
          case "menu_open_file": {
            const chosen = await bridge.showOpenDialog({
              title: "Open Data or Workspace File",
              multiple: true,
            });
            if (chosen && chosen.length > 0) {
              await ingestAndOpenPaths(chosen);
            }
            break;
          }
          case "menu_open_folder": {
            const chosen = await bridge.showOpenDialog({
              title: "Open Folder",
              directory: true,
            });
            const folderPath = Array.isArray(chosen)
              ? chosen[0]
              : typeof chosen === "string"
              ? chosen
              : null;
            if (folderPath) {
              await ingestAndOpenPaths([folderPath]);
            }
            break;
          }
          case "menu_save_workspace": {
            const st = mutableGet(stateRef);
            const vs = st.viewState;
            const ws = await ProjectWorkspaceManager.getInstance().getWorkspace();
            const serializedVP = vs?.viewParams
              ? JSON.parse(JSON.stringify(vs.viewParams.toJS()))
              : null;
            const res: any = await bridge.invoke("toad:save_workspace", {
              tadFileFormatVersion: 2,
              application: "Pulex",
              version: "3.0.0",
              savedAt: new Date().toISOString(),
              activeProjectId: ws.activeProjectId,
              contents: {
                dsPath: vs?.dsPath || null,
                viewParams: serializedVP,
              },
              dsPath: vs?.dsPath || null,
              viewParams: serializedVP,
            });
            if (res && !res.canceled && res.filePath) {
              getAppToaster()?.show({
                message: `Workspace saved to ${getBaseFileName(res.filePath)}`,
                intent: Intent.SUCCESS,
                icon: "saved",
                timeout: 3000,
              });
            }
            break;
          }
          case "menu_export_file": {
            actions.setExportBeginDialogOpen(true, stateRef);
            break;
          }
          case "menu_reopen_view":
          case "menu_reopen_view_f5": {
            await actions.reopenCurrentView(stateRef);
            break;
          }
          case "menu_toggle_sidebar": {
            const curActivity = mutableGet(stateRef).activity;
            actions.setActivity(
              curActivity === "DataSource" ? "None" : "DataSource",
              stateRef
            );
            break;
          }
          case "menu_show_hidden_cols": {
            const curShow =
              mutableGet(stateRef).viewState?.viewParams?.showHiddenCols ?? false;
            actions.setShowHiddenCols(!curShow, stateRef);
            break;
          }
          case "menu_toggle_theme": {
            toggleActiveTheme();
            break;
          }
          case "menu_open_example": {
            const ex: any = await bridge.invoke("toad:open_example", {});
            const exPath = ex?.filePath || ex?.path;
            if (exPath) {
              await ingestAndOpenPaths([exPath]);
            }
            break;
          }
          case "menu_quick_start": {
            getAppToaster()?.show({
              message:
                "Quick Start: Drag & drop data files (CSV, Parquet, JSONL, EVTX, SQLite) or press Ctrl+O to add files, Ctrl+F to filter, and Ctrl+B to toggle the sidebar.",
              intent: Intent.PRIMARY,
              icon: "info-sign",
              timeout: 8000,
            });
            if (!mutableGet(stateRef).viewState?.dsPath) {
              const ex: any = await bridge.invoke("toad:open_example", {});
              const exPath = ex?.filePath || ex?.path;
              if (exPath) {
                await ingestAndOpenPaths([exPath]);
              }
            }
            break;
          }
        }
      } catch (menuErr) {
        console.error("[main.tsx] Error handling menu action:", menuErr);
      }
    });

    // =========================================================================
    // Export Path Browse & File Export Handlers
    // =========================================================================
    const handleBrowseExportPath = async (exportFormat: ExportFormat) => {
      try {
        const st = mutableGet(stateRef);
        const rawSrc = (st.viewState?.dsPath?.sourceId as any)?.resourceId || "export";
        const stem = getBaseFileName(String(rawSrc)).replace(/\.[^.]+$/, "") || "export";
        const res: any = await bridge.invoke("toad:browse_export_path", {
          title: "Export Filtered Data",
          exportFormat,
          defaultFileName: `${stem}.${exportFormat}`,
          defaultName: `${stem}.${exportFormat}`,
        });
        if (res && !res.canceled && res.filePath) {
          actions.setExportPath(String(res.filePath), stateRef);
        }
      } catch (err) {
        console.error("[main.tsx] Error browsing export path:", err);
      }
    };

    const handleExportFile = async (
      exportFormat: ExportFormat,
      exportPath: string,
      parquetExportOptions: ParquetExportOptions
    ) => {
      if (!exportPath) return;
      const baseName = getBaseFileName(exportPath);
      try {
        const st = mutableGet(stateRef);
        const vs = st.viewState;
        const queryObj = vs?.queryView?.query || vs?.baseQuery;
        if (!queryObj) {
          throw new Error("No active dataset view available to export.");
        }
        actions.setExportProgress(0.2, stateRef);
        actions.setExportProgressDialogOpen(true, baseName, stateRef);

        await bridge.invoke("toad:export_file", {
          filePath: exportPath,
          exportPath,
          format: exportFormat,
          exportFormat,
          query: queryObj,
          parquetExportOptions,
        });

        actions.setExportProgress(1, stateRef);
        actions.setExportProgressDialogOpen(false, "", stateRef);
        getAppToaster()?.show({
          message: `Exported filtered data to ${baseName}`,
          intent: Intent.SUCCESS,
          icon: "export",
          timeout: 4000,
        });
      } catch (err: any) {
        actions.setExportProgressDialogOpen(false, "", stateRef);
        const msg = err?.message || String(err);
        console.error("[main.tsx] Export failed:", msg);
        getAppToaster()?.show({
          message: `Export failed: ${msg}`,
          intent: Intent.DANGER,
          icon: "error",
          timeout: 5000,
        });
      }
    };

    const container =
      document.getElementById("app") || document.getElementById("root");
    if (container) {
      const root = ReactDOM.createRoot(container);
      root.render(
        <React.StrictMode>
          <App
            newWindow={newWindowFromDSPath}
            clipboard={appClipboard}
            openURL={openURL}
            embedded={false}
            onBrowseExportPath={handleBrowseExportPath}
            onExportFile={handleExportFile}
          />
        </React.StrictMode>
      );
    }
  } catch (err: any) {
    console.error("[BOOTSTRAP ERROR]", err);
  }
}

bootstrap();

export default bootstrap;
