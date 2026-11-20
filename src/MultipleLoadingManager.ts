import * as reltab from "reltab";
import * as log from "loglevel";
import { StateRef, mutableGet } from "oneref";
import { AppState } from "./AppState";
import { getDefaultPageSize } from "./ViewState";
import * as actions from "./actions";
import { getHostBridge } from "host-bridge";

function getActions(): typeof actions {
  return actions;
}

export type FileIngestStatus = "idle" | "pending" | "processing" | "ready" | "error" | "modified";

export interface FileIngestItem {
  key: string;
  canonicalPath: string;
  dsPath: reltab.DataSourcePath;
  dsc: reltab.DataSourceConnection;
  fileName: string;
  fileSizeBytes?: number;
  status: FileIngestStatus;
  percent: number;
  speedMBs?: number;
  etaSec?: number;
  statusText?: string;
  error?: string;
  priority: number;
  addedTime: number;
  version: number;
}

export const MAX_FIRST_PAGE_CACHE_BYTES = 48 * 1024 * 1024;
export const MAX_SINGLE_CACHE_ENTRY_BYTES = 16 * 1024 * 1024;
export const MAX_FIRST_PAGE_CACHE_ENTRIES = 12;

export function estimateTableRepBytes(tableRep: reltab.TableRep): number {
  if (!tableRep || !tableRep.rowData || tableRep.rowData.length === 0) {
    return 0;
  }
  const rowCount = tableRep.rowData.length;
  const cols =
    tableRep.schema && tableRep.schema.columns && tableRep.schema.columns.length > 0
      ? tableRep.schema.columns
      : Object.keys(tableRep.rowData[0] || {});
  const sampleCount = Math.min(rowCount, 50);
  const step = Math.max(1, Math.floor(rowCount / sampleCount));
  let sampledRows = 0;
  let sampledBytes = 0;

  for (let i = 0; i < rowCount && sampledRows < sampleCount; i += step) {
    const row = tableRep.rowData[i];
    sampledRows++;
    sampledBytes += 64; // per-row JS object overhead
    if (!row) continue;
    for (let c = 0; c < cols.length; c++) {
      const val = (row as any)[cols[c]];
      if (typeof val === "string") {
        sampledBytes += val.length * 2 + 24;
      } else if (val instanceof Uint8Array) {
        sampledBytes += val.byteLength + 24;
      } else {
        sampledBytes += 16;
      }
    }
  }

  if (sampledRows === 0) {
    return 0;
  }
  return Math.ceil((sampledBytes / sampledRows) * rowCount);
}

export interface FirstPageCacheEntry {
  tableName: string;
  schema: reltab.Schema;
  fullSchema?: reltab.Schema;
  tableRep: reltab.TableRep;
  totalRowCount: number;
  pageSize: number;
  bufferPages: number;
  cachedAt: number;
  estimatedBytes?: number;
}

export type QueueChangeListener = () => void;

/**
 * Extracts normalized physical disk path (Canonical Path) from DataSourcePath.
 * Ensures the same underlying file is recognized whether opened from root or subfolder.
 */
export function getCanonicalPathFromDSPath(dsPath: reltab.DataSourcePath): string {
  try {
    let base = "";
    if (typeof dsPath.sourceId === "string") {
      base = dsPath.sourceId;
    } else if (
      dsPath.sourceId &&
      typeof (dsPath.sourceId as any).resourceId === "string"
    ) {
      base = (dsPath.sourceId as any).resourceId;
    }
    const cleanSub = (dsPath.path || []).filter((p) => p && p !== ".");
    let full = base;
    if (cleanSub.length > 0) {
      for (const seg of cleanSub) {
        if (!full.endsWith("\\") && !full.endsWith("/")) {
          full += "\\";
        }
        full += seg;
      }
    }
    return full.replace(/[\/\\]+/g, "\\").toLowerCase();
  } catch {
    return JSON.stringify(dsPath).toLowerCase();
  }
}

/**
 * MultipleLoadingManager:
 * Background multi-file ingestion scheduler using Shortest Job First (SJF) & Dynamic Data Size Budget:
 * - Polls the data source file queue every 1s.
 * - Queue priority order: User Click Priority (VIP) > Smallest Size First (SJF) > FIFO.
 * - Processes up to N files concurrently (default 4) within M GB total in-flight budget (default 1 GB).
 * - Exclusive Big File Mode: when idle, a file > 1 GB runs safely alone.
 * - Supports VIP Jump Queue preemption when the user clicks a file.
 * - Ready files clear progress indicators; failed files display error badges.
 * - Immutable State updates & Canonical Path synchronization.
 */
export class MultipleLoadingManager {
  private static instance: MultipleLoadingManager | null = null;

  public static getInstance(): MultipleLoadingManager {
    if (!MultipleLoadingManager.instance) {
      MultipleLoadingManager.instance = new MultipleLoadingManager();
    }
    return MultipleLoadingManager.instance;
  }

  private files: Map<string, FileIngestItem> = new Map();
  private canonicalMap: Map<string, Set<string>> = new Map();
  private activeItems: Map<string, FileIngestItem> = new Map();
  private listeners: Set<QueueChangeListener> = new Set();
  private timerId: any = null;
  private stateRef: StateRef<AppState> | null = null;
  private pendingActiveOpenKey: string | null = null;
  private isPaused: boolean = false;
  private firstPageCache: Map<string, FirstPageCacheEntry> = new Map();
  private currentRewarmSize: number = 0;

  private maxConcurrentFiles: number = 4;
  private maxConcurrentGB: number = 1;

  private constructor() {
    (globalThis as any).multipleLoadingManagerInstance = this;
    this.loadConfig();
    this.startPollingLoop();
  }

  private async loadConfig() {
    try {
      const bridge = getHostBridge();
      const cfg = await bridge.getConfig();
      if (cfg && cfg.general) {
        this.setConfig({
          maxConcurrentFiles: cfg.general.max_concurrent_files,
          maxConcurrentGB: cfg.general.max_concurrent_gb,
        });
      }
    } catch (err) {
      // Fallback defaults
    }
  }

  public setConfig(cfg: { maxConcurrentFiles?: number | string; maxConcurrentGB?: number | string }) {
    if (cfg.maxConcurrentFiles !== undefined) {
      const n = Number(cfg.maxConcurrentFiles);
      if (!isNaN(n) && n >= 1) {
        this.maxConcurrentFiles = Math.min(32, Math.floor(n));
      }
    }
    if (cfg.maxConcurrentGB !== undefined) {
      const gb = Number(cfg.maxConcurrentGB);
      if (!isNaN(gb) && gb > 0) {
        this.maxConcurrentGB = gb;
      }
    }
    log.info(
      `[MultipleLoadingManager] Config updated: max_concurrent_files=${this.maxConcurrentFiles}, max_concurrent_gb=${this.maxConcurrentGB}GB`
    );
    this.checkAndProcessQueue();
  }

  public getQueueConfig(): { maxFiles: number; maxBytes: number } {
    const maxFiles = Math.max(1, this.maxConcurrentFiles);
    const maxBytes = Math.max(1024 * 1024, this.maxConcurrentGB * 1024 * 1024 * 1024);
    return { maxFiles, maxBytes };
  }

  public get currentInFlightBytes(): number {
    let total = 0;
    for (const item of this.activeItems.values()) {
      if (item.fileSizeBytes && item.fileSizeBytes > 0) {
        total += item.fileSizeBytes;
      }
    }
    return total;
  }

  public get activeItem(): FileIngestItem | null {
    return this.activeItems.values().next().value || null;
  }

  public getActiveItems(): FileIngestItem[] {
    return Array.from(this.activeItems.values());
  }

  public pause() {
    this.isPaused = true;
  }

  public resume() {
    this.isPaused = false;
    this.checkAndProcessQueue();
  }

  public setStateRef(stateRef: StateRef<AppState>) {
    this.stateRef = stateRef;
  }

  public subscribe(listener: QueueChangeListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private notify() {
    for (const listener of this.listeners) {
      try {
        listener();
      } catch (err) {
        log.error("MultipleLoadingManager: listener error", err);
      }
    }
  }

  public getItem(key: string): FileIngestItem | undefined {
    return this.files.get(key);
  }

  public getItemByCanonical(canonicalPath: string): FileIngestItem | undefined {
    const keys = this.canonicalMap.get(canonicalPath);
    if (keys) {
      for (const k of keys) {
        const it = this.files.get(k);
        if (it) return it;
      }
    }
    return undefined;
  }

  public getAllItems(): FileIngestItem[] {
    return Array.from(this.files.values());
  }

  // =========================================================================
  // RAM First-Page Micro-Cache (< 2ms Instant Loading)
  // =========================================================================
  private resolveCacheKey(keyOrCanon: string): string {
    if (!keyOrCanon) return "";
    const item = this.files.get(keyOrCanon);
    if (item && item.canonicalPath) {
      return item.canonicalPath.replace(/[\/\\]+/g, "\\").toLowerCase();
    }
    if (keyOrCanon.startsWith("{") && keyOrCanon.endsWith("}")) {
      try {
        const parsed = JSON.parse(keyOrCanon);
        if (parsed && (parsed.sourceId || parsed.path)) {
          return getCanonicalPathFromDSPath(parsed);
        }
      } catch {}
    }
    return keyOrCanon.replace(/[\/\\]+/g, "\\").toLowerCase();
  }

  public getFirstPageCache(keyOrCanon: string): FirstPageCacheEntry | undefined {
    const key = this.resolveCacheKey(keyOrCanon);
    const entry = this.firstPageCache.get(key) || this.firstPageCache.get(keyOrCanon);
    if (entry) {
      entry.cachedAt = Date.now();
    }
    return entry;
  }

  public hasFirstPageCache(keyOrCanon: string): boolean {
    const key = this.resolveCacheKey(keyOrCanon);
    return this.firstPageCache.has(key) || this.firstPageCache.has(keyOrCanon);
  }

  public setFirstPageCache(keyOrCanon: string, entry: FirstPageCacheEntry): void {
    const key = this.resolveCacheKey(keyOrCanon);
    const entryBytes =
      typeof entry.estimatedBytes === "number" && entry.estimatedBytes >= 0
        ? entry.estimatedBytes
        : estimateTableRepBytes(entry.tableRep);

    if (entryBytes > MAX_SINGLE_CACHE_ENTRY_BYTES) {
      log.warn(
        `[MultipleLoading] Skipping RAM cache for ${entry.tableName}: estimated ${(entryBytes / (1024 * 1024)).toFixed(2)} MB exceeds ${(MAX_SINGLE_CACHE_ENTRY_BYTES / (1024 * 1024)).toFixed(0)} MB single-entry limit`
      );
      if (this.firstPageCache.has(key)) {
        this.firstPageCache.delete(key);
      }
      return;
    }

    // Remove existing entry for this key first to avoid double-counting bytes on overwrite
    if (this.firstPageCache.has(key)) {
      this.firstPageCache.delete(key);
    }

    const getEntryBytes = (e: FirstPageCacheEntry): number =>
      typeof e.estimatedBytes === "number" && e.estimatedBytes >= 0
        ? e.estimatedBytes
        : estimateTableRepBytes(e.tableRep);

    let totalBytes = 0;
    for (const v of this.firstPageCache.values()) {
      totalBytes += getEntryBytes(v);
    }

    while (
      this.firstPageCache.size >= MAX_FIRST_PAGE_CACHE_ENTRIES ||
      totalBytes + entryBytes > MAX_FIRST_PAGE_CACHE_BYTES
    ) {
      let oldestKey: string | null = null;
      let oldestTime = Infinity;
      let oldestBytes = 0;
      for (const [k, v] of this.firstPageCache.entries()) {
        if (v.cachedAt < oldestTime) {
          oldestTime = v.cachedAt;
          oldestKey = k;
          oldestBytes = getEntryBytes(v);
        }
      }
      if (oldestKey !== null) {
        this.firstPageCache.delete(oldestKey);
        totalBytes = Math.max(0, totalBytes - oldestBytes);
      } else {
        break;
      }
    }

    const storedEntry: FirstPageCacheEntry = {
      ...entry,
      estimatedBytes: entryBytes,
    };
    this.firstPageCache.set(key, storedEntry);
  }

  public clearFirstPageCache(targetKeyOrPath?: string): void {
    if (targetKeyOrPath) {
      const key = this.resolveCacheKey(targetKeyOrPath);
      this.firstPageCache.delete(key);
      this.firstPageCache.delete(targetKeyOrPath);
    } else {
      this.firstPageCache.clear();
    }
  }

  public unregisterFile(filePathOrKey: string): void {
    const canon = filePathOrKey.replace(/[\/\\]+/g, "\\").toLowerCase();
    const keySet = this.canonicalMap.get(canon);
    if (keySet) {
      for (const k of keySet) {
        this.files.delete(k);
        this.activeItems.delete(k);
      }
      this.canonicalMap.delete(canon);
    }
    this.files.delete(filePathOrKey);
    this.activeItems.delete(filePathOrKey);
    this.clearFirstPageCache(filePathOrKey);
    this.notify();
  }

  /**
   * Reset all queued files and RAM micro-caches (used after Delete All Projects).
   */
  public clearAllItems(): void {
    this.files.clear();
    this.canonicalMap.clear();
    this.activeItems.clear();
    this.firstPageCache.clear();
    this.pendingActiveOpenKey = null;
    this.notify();
  }

  /**
   * Invalidate RAM micro-caches and reset ready state for purged project files.
   */
  public invalidateFilesCache(filePaths: string[]): void {
    for (const fp of filePaths) {
      if (!fp) continue;
      this.clearFirstPageCache(fp);
      const canon = fp.replace(/[\/\\]+/g, "\\").toLowerCase();
      const keySet = this.canonicalMap.get(canon);
      if (keySet) {
        for (const k of keySet) {
          const item = this.files.get(k);
          if (item && item.status === "ready") {
            this.files.set(k, {
              ...item,
              status: "modified",
              percent: 0,
              version: (item.version || 0) + 1,
            });
          }
        }
      }
    }
    this.notify();
  }

  /**
   * Registers files into the periodic background inspection queue.
   */
  public registerFiles(
    items: {
      dsPath: reltab.DataSourcePath;
      dsc: reltab.DataSourceConnection;
      fileName: string;
      fileSizeBytes?: number;
    }[],
    stateRef?: StateRef<AppState>
  ) {
    if (stateRef) {
      this.stateRef = stateRef;
    }
    let hasNew = false;
    const now = Date.now();
    const MAX_IMMEDIATE_PRELOAD = 8;
    let pendingCount = 0;
    for (const f of this.files.values()) {
      if (f.status === "pending" || f.status === "processing") {
        pendingCount++;
      }
    }

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      const key = JSON.stringify(item.dsPath);
      const canonicalPath = getCanonicalPathFromDSPath(item.dsPath);

      let keySet = this.canonicalMap.get(canonicalPath);
      if (!keySet) {
        keySet = new Set();
        this.canonicalMap.set(canonicalPath, keySet);
      }
      keySet.add(key);

      const existing = this.files.get(key);
      if (existing) {
        if (existing.fileSizeBytes === undefined && item.fileSizeBytes !== undefined) {
          existing.fileSizeBytes = item.fileSizeBytes;
        }
        if (item.dsc && typeof (item.dsc as any).getCacheStatus === "function") {
          try {
            const cs = (item.dsc as any).getCacheStatus(item.dsPath);
            if (cs === "MODIFIED" && existing.status !== "modified" && existing.status !== "processing") {
              existing.status = "modified";
              existing.percent = 0;
              existing.version = (existing.version || 0) + 1;
              hasNew = true;
            }
          } catch {}
        }
      } else {
        // Check whether this file is already ready under another source reference (cache hit)
        let isAlreadyReady = false;
        for (const existingKey of keySet) {
          const ex = this.files.get(existingKey);
          if (ex && ex.status === "ready") {
            isAlreadyReady = true;
            break;
          }
        }

        // DOUBLE-VALID PARQUET LEDGER CHECK:
        let cacheStatus: reltab.CacheValidityStatus = "UNPROCESSED";
        if (item.dsc && typeof (item.dsc as any).getCacheStatus === "function") {
          try {
            cacheStatus = (item.dsc as any).getCacheStatus(item.dsPath);
          } catch { }
        } else if (item.dsc && typeof (item.dsc as any).isCacheReady === "function") {
          try {
            if ((item.dsc as any).isCacheReady(item.dsPath)) {
              cacheStatus = "VALID";
            }
          } catch { }
        }

        let initialStatus: FileIngestStatus = "idle";
        let initialPercent = 0;

        if (cacheStatus === "VALID") {
          initialStatus = "ready";
          initialPercent = 100;
        } else if (cacheStatus === "MODIFIED") {
          initialStatus = "modified";
          initialPercent = 0;
        } else if (isAlreadyReady) {
          initialStatus = "ready";
          initialPercent = 100;
        } else if (pendingCount < MAX_IMMEDIATE_PRELOAD) {
          initialStatus = "pending";
          initialPercent = 0;
          pendingCount++;
        }

        this.files.set(key, {
          key,
          canonicalPath,
          dsPath: item.dsPath,
          dsc: item.dsc,
          fileName: item.fileName,
          fileSizeBytes: item.fileSizeBytes,
          status: initialStatus,
          percent: initialPercent,
          priority: 0,
          addedTime: now + i,
          version: 1,
        });
        hasNew = true;
      }
    }

    if (hasNew) {
      this.notify();
      this.checkAndProcessQueue();
    }
  }

  /**
   * User clicked on file: boost to highest priority for immediate loading.
   */
  public prioritize(keyOrCanon: string, stateRef?: StateRef<AppState>) {
    if (stateRef) {
      this.stateRef = stateRef;
    }
    const item = this.files.get(keyOrCanon) || this.getItemByCanonical(keyOrCanon);
    if (!item) return;
    const key = item.key;

    this.pendingActiveOpenKey = key;

    // If this file or any sibling sharing canonicalPath is already processing,
    // bump priority, record pendingActiveOpenKey, and return without demoting to pending.
    const siblingKeys = this.canonicalMap.get(item.canonicalPath);
    let isAlreadyProcessing = item.status === "processing" || this.activeItems.has(item.key);
    if (!isAlreadyProcessing && siblingKeys) {
      for (const sKey of siblingKeys) {
        const sib = this.files.get(sKey);
        if (sib && (sib.status === "processing" || this.activeItems.has(sKey))) {
          isAlreadyProcessing = true;
          break;
        }
      }
    }

    if (isAlreadyProcessing) {
      const updated: FileIngestItem = {
        ...item,
        priority: Date.now() + 1000000,
        version: (item.version || 0) + 1,
      };
      this.files.set(key, updated);
      if (this.activeItems.has(key)) {
        this.activeItems.set(key, updated);
      }
      this.pendingActiveOpenKey = item.key;
      this.notify();
      return;
    }

    // If marked "modified", force re-ingestion from source (forceReopen)
    if (item.status === "modified") {
      log.info(`[MultipleLoading] File ${item.fileName} marked MODIFIED, prioritizing reload with forceReopen=true`);
      const processingItem: FileIngestItem = {
        ...item,
        status: "processing",
        percent: 0,
        priority: Date.now() + 1000000,
        version: (item.version || 0) + 1,
      };
      this.activeItems.set(item.key, processingItem);
      this.files.set(item.key, processingItem);
      const siblingKeys = this.canonicalMap.get(processingItem.canonicalPath);
      this.notify();
      this.dispatchWorker(processingItem, siblingKeys, { forceReopen: true });
      return;
    }

    // Already ready — no need to re-queue
    if (item.status === "ready") {
      return;
    }

    // Double-Valid Parquet Ledger check: mark ready immediately if cache is valid
    if (item.dsc && typeof (item.dsc as any).getCacheStatus === "function") {
      try {
        const cStatus = (item.dsc as any).getCacheStatus(item.dsPath);
        if (cStatus === "VALID") {
          this.markReady(item.dsPath);
          return;
        } else if (cStatus === "MODIFIED") {
          const modItem = { ...item, status: "modified" as FileIngestStatus };
          this.files.set(key, modItem);
          this.prioritize(key, stateRef);
          return;
        }
      } catch { }
    } else if (item.dsc && typeof (item.dsc as any).isCacheReady === "function") {
      try {
        if ((item.dsc as any).isCacheReady(item.dsPath)) {
          this.markReady(item.dsPath);
          return;
        }
      } catch { }
    }

    // Boost priority to highest with immutable update and pending status
    const updated: FileIngestItem = {
      ...item,
      status: "pending",
      priority: Date.now(),
      version: (item.version || 0) + 1,
    };
    this.files.set(key, updated);
    this.notify();

    // Dispatch immediately if queue capacity is available
    this.checkAndProcessQueue();
  }

  /**
   * Marks a DataSourcePath as ready (100%).
   * Used once a dataset is loaded into the main DataGrid or DuckDB,
   * synchronizing all nodes sharing the same canonical path.
   */
  public markReady(dsPath: reltab.DataSourcePath) {
    const key = JSON.stringify(dsPath);
    const canonicalPath = getCanonicalPathFromDSPath(dsPath);
    const matchingKeys = new Set<string>();
    matchingKeys.add(key);

    const keysForCanon = this.canonicalMap.get(canonicalPath);
    if (keysForCanon) {
      for (const k of keysForCanon) {
        matchingKeys.add(k);
      }
    }

    for (const k of matchingKeys) {
      const item = this.files.get(k);
      if (!item) {
        const seg = dsPath.path[dsPath.path.length - 1];
        const fileName =
          seg && seg !== "."
            ? seg
            : typeof (dsPath.sourceId as any)?.resourceId === "string"
            ? (dsPath.sourceId as any).resourceId.split(/[/\\]/).pop() || "dataset"
            : "dataset";
        this.files.set(k, {
          key: k,
          canonicalPath,
          dsPath,
          dsc: null as any,
          fileName,
          status: "ready",
          percent: 100,
          priority: 0,
          addedTime: Date.now(),
          version: 1,
        });
      } else {
        const updated: FileIngestItem = {
          ...item,
          status: "ready",
          percent: 100,
          error: undefined,
          version: (item.version || 0) + 1,
        };
        this.files.set(k, updated);
      }

      if (this.activeItems.has(k)) {
        this.activeItems.delete(k);
      }
      if (this.pendingActiveOpenKey === k) {
        this.pendingActiveOpenKey = null;
      }
    }

    this.notify();

    // Dispatch next queued file if not paused
    if (!this.isPaused) {
      setTimeout(() => this.checkAndProcessQueue(), 200);
    }
  }

  /**
   * Handles IPC ingestion progress events from DuckDB / Rust IngestionScheduler
   */
  public handleIngestProgress(
    info: {
      fileName?: string;
      filePath?: string;
      percent?: number;
      percentage?: number;
      stage?: string;
      rowsIngested?: number;
      status?: string;
      errorMessage?: string;
      speedMBs?: number;
      etaSec?: number;
      isOpen?: boolean;
      statusText?: string;
    },
    stateRef?: StateRef<AppState>
  ) {
    if (stateRef) {
      this.stateRef = stateRef;
    }

    const rawPathOrName = info.fileName || info.filePath || "";
    const infoCanon = rawPathOrName
      ? rawPathOrName.replace(/[\/\\]+/g, "\\").toLowerCase()
      : "";
    const infoBase =
      rawPathOrName
        .split(/[\\/]/)
        .pop()
        ?.toLowerCase() || "";

    // Locate matching item across activeItems first, then registered files
    let matchedItem: FileIngestItem | undefined;

    for (const item of this.activeItems.values()) {
      const activeBase =
        (item.fileName || "")
          .split(/[\\/]/)
          .pop()
          ?.toLowerCase() || "";
      if (
        (infoCanon && item.canonicalPath === infoCanon) ||
        infoBase === activeBase ||
        (infoBase &&
          activeBase &&
          (infoBase.includes(activeBase) || activeBase.includes(infoBase)))
      ) {
        matchedItem = item;
        break;
      }
    }

    if (!matchedItem && infoCanon) {
      matchedItem = this.getItemByCanonical(infoCanon);
    }

    if (!matchedItem && infoBase) {
      for (const item of this.files.values()) {
        const fBase =
          (item.fileName || "")
            .split(/[\\/]/)
            .pop()
            ?.toLowerCase() || "";
        if (fBase === infoBase) {
          matchedItem = item;
          break;
        }
      }
    }

    if (!matchedItem && this.activeItems.size === 1) {
      matchedItem = this.activeItems.values().next().value;
    }

    if (matchedItem) {
      const rawPct =
        info.percent !== undefined
          ? info.percent
          : info.percentage !== undefined
          ? info.percentage
          : 0;
      const nextPercent = Math.min(100, Math.max(0, rawPct));
      const isFailed = info.status === "FAILED";
      const nextStatus: FileIngestStatus = isFailed
        ? "error"
        : nextPercent >= 100 || info.status === "READY"
        ? "ready"
        : "processing";
      const computedStatusText =
        info.statusText ||
        (info.stage
          ? `Stage: ${info.stage}${
              info.rowsIngested
                ? ` (${Number(info.rowsIngested).toLocaleString()} rows)`
                : ""
            }`
          : matchedItem.statusText);

      // Update immutable state
      const updated: FileIngestItem = {
        ...matchedItem,
        percent: nextPercent,
        status: nextStatus,
        error: isFailed ? info.errorMessage || "Ingestion failed" : matchedItem.error,
        speedMBs: info.speedMBs !== undefined ? info.speedMBs : matchedItem.speedMBs,
        etaSec: info.etaSec !== undefined ? info.etaSec : matchedItem.etaSec,
        statusText: computedStatusText,
        version: (matchedItem.version || 0) + 1,
      };
      if (nextStatus === "ready" || nextStatus === "error") {
        this.activeItems.delete(updated.key);
      } else if (this.activeItems.has(updated.key)) {
        this.activeItems.set(updated.key, updated);
      }
      this.files.set(updated.key, updated);

      // Synchronize across all sibling nodes sharing the canonical path
      const siblingKeys = this.canonicalMap.get(updated.canonicalPath);
      if (siblingKeys) {
        for (const sKey of siblingKeys) {
          if (sKey !== updated.key) {
            const sib = this.files.get(sKey);
            if (sib) {
              this.files.set(sKey, {
                ...sib,
                percent: nextPercent,
                status: nextStatus,
                error: updated.error,
                speedMBs: updated.speedMBs,
                etaSec: updated.etaSec,
                statusText: updated.statusText,
                version: (sib.version || 0) + 1,
              });
            }
          }
        }
      }

      // Sync to AppState if this file is currently displayed on the main loading screen
      if (this.stateRef) {
        const appState = mutableGet(this.stateRef);
        if (
          appState.activeLoadingFile &&
          appState.activeLoadingFile.isLoading
        ) {
          const activeCanon = appState.activeLoadingFile.dsPath
            ? getCanonicalPathFromDSPath(appState.activeLoadingFile.dsPath)
            : "";
          const activeBase = (matchedItem.fileName || "")
            .split(/[\\/]/)
            .pop()
            ?.toLowerCase() || "";

          if (
            activeCanon === updated.canonicalPath ||
            appState.activeLoadingFile.fileName.toLowerCase() === activeBase ||
            this.pendingActiveOpenKey === matchedItem.key
          ) {
            getActions().setActiveLoadingFile(
              {
                ...appState.activeLoadingFile,
                percent: updated.percent,
                speedMBs: updated.speedMBs,
                etaSec: updated.etaSec,
                statusText: computedStatusText,
              },
              this.stateRef
            );
          }
        }
      }

      this.notify();
    }
  }

  /**
   * 1-second periodic polling loop
   */
  private startPollingLoop() {
    if (this.timerId) {
      clearInterval(this.timerId);
    }
    this.timerId = setInterval(() => {
      this.checkAndProcessQueue();
    }, 1000);
  }

  public stopPollingLoop() {
    if (this.timerId) {
      clearInterval(this.timerId);
      this.timerId = null;
    }
  }

  /**
   * Inspects and dispatches queued files concurrently within thread and byte budgets
   */
  private async checkAndProcessQueue() {
    if (this.isPaused) {
      return;
    }

    const { maxFiles, maxBytes } = this.getQueueConfig();

    while (this.activeItems.size < maxFiles) {
      // Filter items currently in pending state
      const pendingList: FileIngestItem[] = [];
      for (const item of this.files.values()) {
        if (item.status === "pending") {
          pendingList.push(item);
        }
      }

      if (pendingList.length === 0) {
        // When no pending items remain and queue is completely idle,
        // gradually promote up to 2 idle files
        if (this.activeItems.size === 0) {
          let promoted = 0;
          for (const item of this.files.values()) {
            if (item.status === "idle") {
              const updatedItem: FileIngestItem = {
                ...item,
                status: "pending",
                version: (item.version || 0) + 1,
              };
              this.files.set(item.key, updatedItem);
              pendingList.push(updatedItem);
              promoted++;
              if (promoted >= 2) break;
            }
          }
        }
        if (pendingList.length === 0) {
          break;
        }
      }

      // Sort queue by:
      // 1. User Priority Jump Queue
      // 2. Smallest Size First (Shortest Job First - SJF)
      // 3. Arrival Time (FIFO)
      pendingList.sort((a, b) => {
        if (b.priority !== a.priority) {
          return b.priority - a.priority;
        }
        const sizeA = a.fileSizeBytes !== undefined ? a.fileSizeBytes : 0;
        const sizeB = b.fileSizeBytes !== undefined ? b.fileSizeBytes : 0;
        if (sizeA !== sizeB) {
          return sizeA - sizeB;
        }
        return a.addedTime - b.addedTime;
      });

      const currentBytes = this.currentInFlightBytes;

      // Track canonicalPaths currently processed by active workers to prevent duplicate execution
      const activeCanonicals = new Set<string>();
      for (const active of this.activeItems.values()) {
        if (active.canonicalPath) {
          activeCanonicals.add(active.canonicalPath);
        }
      }

      // Enforce Data Size Budget:
      // - When queue is idle (activeItems.size === 0): allow the first file even if > maxBytes (Exclusive Big File Mode)
      // - When workers are active: pick the first file fitting currentBytes + candidateSize <= maxBytes
      let candidateIndex = -1;
      for (let i = 0; i < pendingList.length; i++) {
        const candidate = pendingList[i];
        if (candidate.canonicalPath && activeCanonicals.has(candidate.canonicalPath)) {
          continue; // Never spawn multiple workers for the same physical file
        }
        const candidateSize = candidate.fileSizeBytes !== undefined ? candidate.fileSizeBytes : 0;
        if (this.activeItems.size === 0 || currentBytes + candidateSize <= maxBytes) {
          candidateIndex = i;
          break;
        }
      }

      if (candidateIndex === -1) {
        // No pending file currently fits budget or deduplication constraints; wait for active worker completion.
        break;
      }

      const targetItem = pendingList[candidateIndex];

      const processingItem: FileIngestItem = {
        ...targetItem,
        status: "processing",
        percent: 0,
        version: (targetItem.version || 0) + 1,
      };
      this.activeItems.set(processingItem.key, processingItem);
      this.files.set(processingItem.key, processingItem);

      // Synchronize processing status across siblings
      const siblingKeys = this.canonicalMap.get(processingItem.canonicalPath);
      if (siblingKeys) {
        for (const sKey of siblingKeys) {
          if (sKey !== processingItem.key) {
            const sib = this.files.get(sKey);
            if (sib) {
              this.files.set(sKey, {
                ...sib,
                status: "processing",
                percent: 0,
                version: (sib.version || 0) + 1,
              });
            }
          }
        }
      }

      this.notify();

      // Dispatch parallel async worker
      this.dispatchWorker(processingItem, siblingKeys);
    }
  }

  /**
   * Independent background worker for each data file
   */
  private async dispatchWorker(
    processingItem: FileIngestItem,
    siblingKeys?: Set<string>,
    options?: { forceReopen?: boolean }
  ) {
    const targetItem = processingItem;
    const sizeMb = ((processingItem.fileSizeBytes || 0) / (1024 * 1024)).toFixed(2);
    log.info(
      `[MultipleLoading] Starting background worker: ${processingItem.fileName} (${sizeMb} MB) [Active: ${this.activeItems.size}, In-Flight: ${(this.currentInFlightBytes / (1024 * 1024)).toFixed(2)} MB]`
    );

    try {
      // Invoke getTableName to import file into DuckDB and generate Parquet cache
      const tableName = await processingItem.dsc.getTableName(processingItem.dsPath, {
        isBackground: true,
        forceReopen: options?.forceReopen === true,
      } as any);
      log.info(
        `[MultipleLoading] Completed loading ${processingItem.fileName} -> Table: ${tableName}`
      );

      // RAM Micro-Cache: Adaptive cell-budget pre-warm into RAM buffer cache (< 5ms instant open)
      try {
        const pSize = getDefaultPageSize();
        const baseQuery = reltab.tableQuery(tableName);
        let colCount = 0;
        let schema: reltab.Schema | undefined;
        try {
          if (typeof processingItem.dsc.getTableSchema === "function") {
            schema = await processingItem.dsc.getTableSchema(tableName);
          } else if (typeof (processingItem.dsc as any).getSchema === "function") {
            schema = await (processingItem.dsc as any).getSchema(baseQuery);
          }
          if (schema && schema.columns) {
            colCount = schema.columns.length;
          }
        } catch (schemaErr) {
          log.warn(`[MultipleLoading] Failed to read schema for ${tableName}:`, schemaErr);
        }

        const totalRowCount = await processingItem.dsc.rowCount(baseQuery);
        const effectiveColCount = schema && colCount > 100 ? 100 : colCount;
        const preloadQuery =
          schema && colCount > 100
            ? baseQuery.project(schema.columns.slice(0, 100))
            : baseQuery;

        const maxRowsByCellBudget = Math.max(
          100,
          Math.floor(250_000 / Math.max(1, effectiveColCount))
        );
        const bufferPages = effectiveColCount <= 50 ? 3 : 1;
        const preloadLimit = Math.min(
          totalRowCount,
          pSize * bufferPages,
          maxRowsByCellBudget
        );

        const tableRep = await processingItem.dsc.evalQuery(preloadQuery, 0, preloadLimit);
        const cacheEntry: FirstPageCacheEntry = {
          tableName,
          schema: tableRep.schema,
          fullSchema: schema || tableRep.schema,
          tableRep,
          totalRowCount,
          pageSize: pSize,
          bufferPages,
          cachedAt: Date.now(),
        };
        this.setFirstPageCache(processingItem.canonicalPath, cacheEntry);
        this.setFirstPageCache(processingItem.key, cacheEntry);
        log.info(
          `[MultipleLoading] Pre-warmed RAM Cache for ${processingItem.fileName} (${totalRowCount} rows, ${tableRep.rowData.length} cached rows, cols: ${colCount}, pageSize: ${pSize}, bufferPages: ${bufferPages})`
        );
      } catch (cacheErr: any) {
        log.warn(
          `[MultipleLoading] Failed to pre-warm RAM cache for ${processingItem.fileName}:`,
          cacheErr
        );
      }

      const readyItem: FileIngestItem = {
        ...processingItem,
        status: "ready",
        percent: 100,
        error: undefined,
        version: (processingItem.version || 0) + 1,
      };
      this.files.set(readyItem.key, readyItem);

      if (siblingKeys) {
        for (const sKey of siblingKeys) {
          if (sKey !== readyItem.key) {
            const sib = this.files.get(sKey);
            if (sib) {
              this.files.set(sKey, {
                ...sib,
                status: "ready",
                percent: 100,
                error: undefined,
                version: (sib.version || 0) + 1,
              });
            }
          }
        }
      }

      this.activeItems.delete(processingItem.key);
      this.notify();

      // Check if user is currently waiting on the loading screen for this file:
      let shouldOpenOnMain = false;
      if (this.stateRef) {
        const appState = mutableGet(this.stateRef);
        if (
          appState.activeLoadingFile &&
          appState.activeLoadingFile.isLoading
        ) {
          const activeCanon = appState.activeLoadingFile.dsPath
            ? getCanonicalPathFromDSPath(appState.activeLoadingFile.dsPath)
            : "";
          if (
            activeCanon === readyItem.canonicalPath ||
            this.pendingActiveOpenKey === targetItem.key
          ) {
            shouldOpenOnMain = true;
          }
        } else if (this.pendingActiveOpenKey === targetItem.key) {
          shouldOpenOnMain = true;
        }
      }

      if (shouldOpenOnMain && this.stateRef) {
        this.pendingActiveOpenKey = null;
        getActions().setActiveLoadingFile(null, this.stateRef);
        await getActions().openDataSourcePath(targetItem.dsPath, this.stateRef);
        // Delay 300ms after opening active file so DataGrid finishes first paint
        setTimeout(() => this.checkAndProcessQueue(), 300);
      } else {
        // Dispatch next queued file immediately
        setTimeout(() => this.checkAndProcessQueue(), 50);
      }
    } catch (err: any) {
      log.error(
        `[MultipleLoading] Error loading file ${processingItem.fileName}:`,
        err
      );
      const errorItem: FileIngestItem = {
        ...processingItem,
        status: "error",
        error: err?.message || String(err),
        version: (processingItem.version || 0) + 1,
      };
      this.files.set(errorItem.key, errorItem);
      this.activeItems.delete(processingItem.key);
      this.notify();

      if (
        this.pendingActiveOpenKey === targetItem.key &&
        this.stateRef
      ) {
        this.pendingActiveOpenKey = null;
        getActions().setActiveLoadingFile(
          {
            isLoading: false,
            fileName: targetItem.fileName,
            dsPath: targetItem.dsPath,
            percent: 0,
            error: errorItem.error,
          },
          this.stateRef
        );
      }

      // Dispatch next queued file
      setTimeout(() => this.checkAndProcessQueue(), 50);
    }
  }

  /**
   * Re-warm RAM 3-Page Buffer Cache for all ready files when pageSize is changed
   */
  public async rewarmAllReadyCaches(newSize: number): Promise<void> {
    this.currentRewarmSize = newSize;
    const readyItems = Array.from(this.files.values()).filter(
      (it) => it.status === "ready" && it.dsc
    );
    const seenCanons = new Set<string>();
    for (const it of readyItems) {
      if (this.currentRewarmSize !== newSize) {
        log.info(
          `[MultipleLoading] Aborting rewarmAllReadyCaches(${newSize}) because a newer rewarm target (${this.currentRewarmSize}) is active`
        );
        return;
      }
      if (seenCanons.has(it.canonicalPath)) continue;
      seenCanons.add(it.canonicalPath);
      try {
        const existing =
          this.getFirstPageCache(it.canonicalPath) || this.getFirstPageCache(it.key);
        const tableName =
          existing?.tableName ||
          (await it.dsc.getTableName(it.dsPath, { isBackground: true } as any));
        const baseQuery = reltab.tableQuery(tableName);

        let schema: reltab.Schema | undefined = existing?.fullSchema || existing?.schema;
        if (!existing?.fullSchema && typeof it.dsc.getTableSchema === "function") {
          try {
            const sc = await it.dsc.getTableSchema(tableName);
            if (sc && sc.columns) {
              schema = sc;
            }
          } catch {}
        } else if (!schema && typeof (it.dsc as any).getSchema === "function") {
          try {
            const sc = await (it.dsc as any).getSchema(baseQuery);
            if (sc && sc.columns) {
              schema = sc;
            }
          } catch {}
        }
        const colCount = schema?.columns ? schema.columns.length : 0;
        const effectiveColCount = schema && colCount > 100 ? 100 : colCount;
        const preloadQuery =
          schema && colCount > 100
            ? baseQuery.project(schema.columns.slice(0, 100))
            : baseQuery;

        const totalRowCount =
          existing?.totalRowCount ?? (await it.dsc.rowCount(baseQuery));

        const maxRowsByCellBudget = Math.max(
          100,
          Math.floor(250_000 / Math.max(1, effectiveColCount))
        );
        const bufferPages = effectiveColCount <= 50 ? 3 : 1;
        const limit = Math.min(
          totalRowCount,
          newSize * bufferPages,
          maxRowsByCellBudget
        );

        if (
          existing &&
          existing.pageSize === newSize &&
          existing.tableRep.rowData.length === limit
        ) {
          continue;
        }

        const tableRep = await it.dsc.evalQuery(preloadQuery, 0, limit);

        if (this.currentRewarmSize !== newSize) {
          return;
        }

        const updated: FirstPageCacheEntry = {
          tableName,
          schema: tableRep.schema,
          fullSchema: schema || tableRep.schema,
          tableRep,
          totalRowCount,
          pageSize: newSize,
          bufferPages,
          cachedAt: Date.now(),
        };
        this.setFirstPageCache(it.canonicalPath, updated);
        this.setFirstPageCache(it.key, updated);
        log.info(
          `[MultipleLoading] Re-warmed RAM Cache for ${it.fileName} (${totalRowCount} rows, ${tableRep.rowData.length} cached rows, cols: ${colCount}, pageSize: ${newSize}, bufferPages: ${bufferPages})`
        );
      } catch (err) {
        log.warn(`[MultipleLoading] Error re-warming cache for ${it.fileName}:`, err);
      }
    }
  }

  public clearPendingActiveOpen() {
    this.pendingActiveOpenKey = null;
  }

  public clear() {
    this.files.clear();
    this.canonicalMap.clear();
    this.activeItems.clear();
    this.pendingActiveOpenKey = null;
    this.notify();
  }
}
