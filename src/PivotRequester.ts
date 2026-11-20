import * as reltab from "reltab";
import * as aggtree from "aggtree";
import { PagedDataView, DataRow } from "./PagedDataView";
import { ViewParams } from "./ViewParams";
import { AppState } from "./AppState";
import { QueryView } from "./QueryView";
import {
  DataSourceConnection,
  getColumnHistogramMap,
  ColumnHistogramMap,
} from "reltab"; // eslint-disable-line

import * as oneref from "oneref"; // eslint-disable-line
import { mutableGet, addStateChangeListener } from "oneref";

import * as paging from "./paging";
import * as util from "./util";
import { ViewState } from "./ViewState";
import { Timer, TimerUpdater } from "./Timer";
import _ from "lodash";

// Configurable maximum row count eligible for automatic `ORDER BY timeCol DESC` on open.
// Default: 500,000 rows (0 = disabled, -1 = always enabled). Prevents 15-25s full-table sort stalls on 40GB datasets.
let autoSortMaxRowsThreshold: number = 500_000;

export const setAutoSortMaxRows = (maxRows: number): void => {
  if (typeof maxRows === "number" && Number.isFinite(maxRows)) {
    autoSortMaxRowsThreshold = Math.trunc(maxRows);
  }
};

export const getAutoSortMaxRows = (): number => autoSortMaxRowsThreshold;

const WIDE_TABLE_AUTOSORT_COL_LIMIT = 200;
const AUTOSORT_MAX_CELL_BUDGET = 10_000_000;
const ADAPTIVE_CELL_BUDGET = 250_000;

export function computeProjectedColumns(
  baseSchema: reltab.Schema,
  viewParams: ViewParams,
  effectiveSortKey: [string, boolean][] | undefined
): string[] {
  const wanted = new Set<string>();
  const displayCols =
    viewParams.displayColumns && viewParams.displayColumns.length > 0
      ? viewParams.displayColumns
      : baseSchema.columns.slice(0, 100);
  for (const c of displayCols) {
    if (baseSchema.hasColumn(c)) wanted.add(c);
  }
  for (const c of viewParams.vpivots || []) {
    if (baseSchema.hasColumn(c)) wanted.add(c);
  }
  if (
    viewParams.pivotLeafColumn &&
    baseSchema.hasColumn(viewParams.pivotLeafColumn)
  ) {
    wanted.add(viewParams.pivotLeafColumn);
  }
  for (const [sortCol] of effectiveSortKey || []) {
    if (baseSchema.hasColumn(sortCol)) wanted.add(sortCol);
  }
  if (baseSchema.hasColumn("Rec")) {
    wanted.add("Rec");
  }
  const seen = new Set<string>();
  return baseSchema.columns.filter((c) => {
    if (!wanted.has(c) || seen.has(c)) return false;
    seen.add(c);
    return true;
  });
}

export function buildPrunedSchema(
  baseSchema: reltab.Schema,
  projectedCols: string[]
): reltab.Schema {
  const prunedMeta: reltab.ColumnMetaMap = {};
  for (const c of projectedCols) {
    if (baseSchema.columnMetadata && baseSchema.columnMetadata[c]) {
      prunedMeta[c] = baseSchema.columnMetadata[c];
    }
  }
  return new reltab.Schema(baseSchema.dialect, projectedCols, prunedMeta);
}

function computeEffectiveSortKey(
  baseSchema: reltab.Schema,
  viewParams: ViewParams,
  baseRowCount: number
): [string, boolean][] {
  let effectiveSortKey = viewParams.sortKey || [];
  if (effectiveSortKey.length === 0) {
    const projectedCount = Math.max(
      1,
      viewParams.displayColumns?.length || baseSchema.columns.length
    );
    const allowAutoSort =
      baseSchema.columns.length <= WIDE_TABLE_AUTOSORT_COL_LIMIT &&
      (autoSortMaxRowsThreshold < 0 ||
        (autoSortMaxRowsThreshold > 0 &&
          baseRowCount <= autoSortMaxRowsThreshold)) &&
      baseRowCount * projectedCount <= AUTOSORT_MAX_CELL_BUDGET;
    if (allowAutoSort) {
      const timeCol = findBestTimeColumn(baseSchema);
      if (timeCol) {
        effectiveSortKey = [[timeCol, false]]; // DESC sort for deterministic ordering
      }
    }
  }
  return effectiveSortKey;
}

// Error dialogs are handled via IHostBridge.showErrorDialog()
/**
 * Use ViewParams to construct a PagedDataView for use with
 * SlickGrid from reltab.TableRep
 */

const mkDataView = (
  viewParams: ViewParams,
  rowCount: number,
  offset: number,
  tableData: reltab.TableRep,
  currentPage: number = 1,
  pageSize: number = 1000,
  isPartialCache: boolean = false
): PagedDataView => {
  const getPath = (dataRow: DataRow, depth: number) => {
    let path: Array<string | null> = [];

    for (let i = 0; i < depth; i++) {
      let pathElem = rowMap["_path" + i] as string | null;
      path.push(pathElem);
    }

    return path;
  };

  var nPivots = viewParams.vpivots.length;
  var rowData = [];
  var parentIdStack = [];

  for (var i = 0; i < tableData.rowData.length; i++) {
    // ?? shouldn't we just be constructing the rowMap once and re-use it for every row??
    var rowMap: DataRow = tableData.rowData[i] as DataRow;
    var depth: number = rowMap._depth;
    var path = getPath(rowMap, depth);
    rowMap._isOpen = viewParams.openPaths.isOpen(path);
    // console.log('mkDataView: row: ', i, ', depth: ', depth, ' path: ', path, ', isOpen: ', rowMap._isOpen)
    rowMap._isLeaf = depth > nPivots;
    rowMap._id = i;
    parentIdStack[depth] = i;
    var parentId = depth > 0 ? parentIdStack[depth - 1] : null;
    rowMap._parentId = parentId;
    rowData.push(rowMap);
  }

  const outSchema = tableData.schema;
  const dataView = new PagedDataView(
    outSchema,
    rowCount,
    offset,
    rowData,
    currentPage,
    pageSize,
    isPartialCache
  );
  return dataView;
};
/*
 * hacky opt for filter count: If empty filter, just retun baseRowCount
 */

const fastFilterRowCount = async (
  rt: DataSourceConnection,
  baseRowCount: number,
  filterExp: reltab.FilterExp,
  filterQuery: reltab.QueryExp,
  onViewRowCount?: (
    query: reltab.QueryExp,
    type: "filtered" | "unfiltered" | "view"
  ) => void,
  onViewRowCountResolved?: (
    query: reltab.QueryExp,
    rowCount: number,
    type: "filtered" | "unfiltered" | "view"
  ) => void
): Promise<number> => {
  if (filterExp.opArgs.length === 0) {
    // short circuit!
    return baseRowCount;
  }

  onViewRowCount?.(filterQuery, "filtered");
  const resolvedNumber = await rt.rowCount(filterQuery);
  onViewRowCountResolved?.(filterQuery, resolvedNumber, "filtered");
  return resolvedNumber;
};
/*
 * hacky opt for using filterRowCount as viewRowCount if not pivoted
 */

const fastViewRowCount = async (
  rt: DataSourceConnection,
  filterRowCount: number,
  vpivots: Array<string>,
  viewQuery: reltab.QueryExp,
  onViewRowCount?: (
    query: reltab.QueryExp,
    type: "filtered" | "unfiltered" | "view"
  ) => void,
  onViewRowCountResolved?: (
    query: reltab.QueryExp,
    rowCount: number,
    type: "filtered" | "unfiltered" | "view"
  ) => void
): Promise<number> => {
  if (vpivots.length === 0) {
    // short circuit!
    return filterRowCount;
  }

  onViewRowCount?.(viewQuery, "view");
  const resolvedNumber = await rt.rowCount(viewQuery);
  onViewRowCountResolved?.(viewQuery, resolvedNumber, "view");
  return resolvedNumber;
};
export function findBestTimeColumn(baseSchema: reltab.Schema): string | null {
  if (!baseSchema || !baseSchema.columns || baseSchema.columns.length === 0) {
    return null;
  }
  const cols = baseSchema.columns;

  const priorities = [
    /^event[_\s]?time$/i,
    /^time[_\s]?created$/i,
    /^timestamp$/i,
    /^utc[_\s]?time$/i,
    /^date[_\s]?time$/i,
    /^time$/i,
    /^date$/i,
  ];

  for (const regex of priorities) {
    const found = cols.find((c) => regex.test(c));
    if (found) return found;
  }

  for (const c of cols) {
    const lower = c.toLowerCase();
    if (
      lower.includes("eventtime") ||
      lower.includes("timecreated") ||
      lower.includes("timestamp") ||
      lower.includes("datetime")
    ) {
      return c;
    }
  }

  for (const c of cols) {
    try {
      const colType = baseSchema.columnType(c);
      if (colType) {
        if (colType.kind === "timestamp" || colType.kind === "date") {
          return c;
        }
        const sqlName = (colType.sqlTypeName || "").toUpperCase();
        if (
          sqlName.includes("TIMESTAMP") ||
          sqlName.includes("DATE") ||
          sqlName.includes("TIME")
        ) {
          return c;
        }
      }
    } catch {}
  }

  return null;
}

/**
 * Use the current ViewParams to construct a QueryExp to send to
 * reltab using aggtree.
 * Map the resulting TableRep from the query into a PagedDataView for
 * use with SlickGrid
 */

const requestQueryView = async (
  rt: DataSourceConnection,
  baseQuery: reltab.QueryExp,
  baseSchema: reltab.Schema,
  viewParams: ViewParams,
  showRecordCount: boolean,
  prevQueryView: QueryView | null | undefined,
  onViewRowCount?: (
    query: reltab.QueryExp,
    type: "filtered" | "unfiltered" | "view"
  ) => void,
  onViewRowCountResolved?: (
    query: reltab.QueryExp,
    rowCount: number,
    type: "filtered" | "unfiltered" | "view"
  ) => void
): Promise<QueryView> => {
  const effectiveFilterExp = viewParams.filterExp;

  onViewRowCount?.(baseQuery, "unfiltered");
  const baseRowCount = await rt.rowCount(baseQuery);
  onViewRowCountResolved?.(baseQuery, baseRowCount, "unfiltered");

  const effectiveSortKey = computeEffectiveSortKey(
    baseSchema,
    viewParams,
    baseRowCount
  );
  const projectedCols = computeProjectedColumns(
    baseSchema,
    viewParams,
    effectiveSortKey
  );
  const prunedSchema = buildPrunedSchema(baseSchema, projectedCols);
  const aggMap: any = {};

  for (const cid of prunedSchema.columns) {
    aggMap[cid] = viewParams.getAggFn(prunedSchema, cid);
  }

  const filterQuery = baseQuery.filter(effectiveFilterExp);
  const ptree = await aggtree.vpivot(
    rt,
    filterQuery,
    prunedSchema,
    viewParams.vpivots,
    viewParams.pivotLeafColumn,
    viewParams.showRoot,
    effectiveSortKey,
    aggMap,
    showRecordCount
  );
  const treeQuery = await ptree.getSortedTreeQuery(viewParams.openPaths); // const t0 = performance.now()  // eslint-disable-line

  const filterRowCount = await fastFilterRowCount(
    rt,
    baseRowCount,
    effectiveFilterExp,
    filterQuery,
    onViewRowCount,
    onViewRowCountResolved
  );
  const rowCount = await fastViewRowCount(
    rt,
    filterRowCount,
    viewParams.vpivots,
    treeQuery,
    onViewRowCount,
    onViewRowCountResolved
  ); // const t1 = performance.now() // eslint-disable-line
  // console.log('gathering row counts took ', (t1 - t0) / 1000, ' sec')

  let histoMap: ColumnHistogramMap | null = null;
  if (
    viewParams.showColumnHistograms &&
    (prevQueryView == null ||
      prevQueryView.baseQuery !== baseQuery ||
      prevQueryView.histoMap == null)
  ) {
    const statsMap = await rt.getColumnStatsMap(baseQuery);
    histoMap = await getColumnHistogramMap(rt, baseQuery, baseSchema, statsMap);
  } else {
    if (prevQueryView != null) {
      histoMap = prevQueryView.histoMap;
    } else {
      histoMap = null;
    }
  }

  const fullRowQuery =
    viewParams.vpivots.length === 0
      ? effectiveSortKey && effectiveSortKey.length > 0
        ? filterQuery.sort(effectiveSortKey)
        : filterQuery
      : null;

  const ret = new QueryView({
    baseQuery,
    query: treeQuery,
    fullRowQuery,
    histoMap,
    baseRowCount,
    filterRowCount,
    rowCount,
  });
  return ret;
};

const requestDataView = async (
  rt: DataSourceConnection,
  viewParams: ViewParams,
  queryView: QueryView,
  offset: number,
  limit: number,
  onViewQuery?: (
    query: reltab.QueryExp,
    offset?: number,
    limit?: number
  ) => void,
  currentPage: number = 1,
  pageSize: number = 1000,
  isPartialCache: boolean = false
): Promise<PagedDataView> => {
  onViewQuery?.(queryView.query, offset, limit);
  const tableData = await rt.evalQuery(queryView.query, offset, limit);
  const dataView = mkDataView(
    viewParams,
    queryView.rowCount,
    offset,
    tableData,
    currentPage,
    pageSize,
    isPartialCache
  );
  return dataView;
};

const vsUpdate =
  (f: (vs: ViewState) => ViewState) =>
  (s: AppState): AppState =>
    s.update("viewState", (vs: ViewState | null) =>
      vs == null ? null : f(vs)
    ) as AppState;

/* for debugging: */
function getObjectDiff(obj1: any, obj2: any) {
  const diff = Object.keys(obj1).reduce((result, key) => {
    if (!obj2.hasOwnProperty(key)) {
      result.push(key);
    } else if (_.isEqual(obj1[key], obj2[key])) {
      const resultKeyIndex = result.indexOf(key);
      result.splice(resultKeyIndex, 1);
    }
    return result;
  }, Object.keys(obj2));

  return diff;
}

const noopSetLoadingCallback = (loading: boolean) => {};

/**
 * A PivotRequester listens for changes on the appState and viewport and
 * manages issuing of query requests
 */

export class PivotRequester {
  /*
   * 'pending' is really misnomer here -- for viewParams, offset and limit,
   * which are the parameters observed by PivotRequester, they are really
   * the 'most recent previous' parameters used to make an asynchronous request,
   * which may or may not have already completed.  We want this because we
   * need to compare application state changes with either what's currently
   * displayed OR a pending request.
   */
  pendingViewParams: ViewParams | undefined | null;
  pendingBaseQuery: reltab.QueryExp | undefined | null;
  pendingQueryRequest: Promise<QueryView> | undefined | null;
  pendingDataRequest: Promise<void | PagedDataView> | undefined | null;
  currentQueryView: QueryView | undefined | null; // set when resolved

  pendingOffset: number;
  pendingLimit: number;
  pendingPage: number;
  pendingIsPartialCache: boolean;
  isInternalStateUpdate: boolean;
  pageSize: number;
  errorCallback?: (e: Error) => void;
  setLoadingCallback: (loading: boolean) => void;

  constructor(
    stateRef: oneref.StateRef<AppState>,
    errorCallback?: (e: Error) => void,
    setLoadingCallback?: (loading: boolean) => void,
    public onViewQuery?: (
      query: reltab.QueryExp,
      offset?: number,
      limit?: number
    ) => void,
    public onViewRowCount?: (
      query: reltab.QueryExp,
      type: "filtered" | "unfiltered" | "view"
    ) => void,
    public onViewRowCountResolved?: (
      query: reltab.QueryExp,
      rowCount: number,
      type: "filtered" | "unfiltered" | "view"
    ) => void
  ) {
    const appState = mutableGet(stateRef);
    this.pendingQueryRequest = null;
    this.currentQueryView = null;
    this.pendingDataRequest = null;
    this.pendingViewParams = null;
    this.pendingBaseQuery = null;
    this.pendingOffset = 0;
    this.pendingLimit = 0;
    this.pendingPage = 1;
    this.pendingIsPartialCache = false;
    this.isInternalStateUpdate = false;
    this.pageSize = 1000;
    this.errorCallback = errorCallback;
    this.setLoadingCallback = setLoadingCallback || noopSetLoadingCallback;

    addStateChangeListener(stateRef, (_) => {
      this.onStateChange(stateRef);
    });

    // And invoke onStateChange initially to get things started:
    this.onStateChange(stateRef);
  } // issue a data request from current QueryView and
  // offset, limit:

  sendDataRequest(
    stateRef: oneref.StateRef<AppState>,
    queryView: QueryView
  ): Promise<PagedDataView> {
    this.setLoadingCallback(true);
    const appState: AppState = mutableGet(stateRef);
    const viewState = appState.viewState;
    const viewParams = viewState.viewParams;

    const curPage = viewState.currentPage || 1;
    const pSize = viewState.pageSize || 1000;
    const totalRowCount = queryView.rowCount;
    const totalPages = Math.max(1, Math.ceil(totalRowCount / pSize));
    const safePage = Math.min(Math.max(1, curPage), totalPages);

    const effectiveSortKey = viewState.baseSchema
      ? computeEffectiveSortKey(
          viewState.baseSchema,
          viewParams,
          queryView.baseRowCount
        )
      : undefined;
    const projectedCols = viewState.baseSchema
      ? computeProjectedColumns(
          viewState.baseSchema,
          viewParams,
          effectiveSortKey
        )
      : viewParams.displayColumns || [];
    const colCount = Math.max(
      1,
      projectedCols.length || viewParams.displayColumns?.length || 1
    );

    // Adaptive cell-budget sliding buffer
    const bufferRadius = colCount <= 30 ? 2 : colCount <= 80 ? 1 : 0;
    const bufStartPage = Math.max(1, safePage - bufferRadius);
    const bufEndPage = Math.min(totalPages, safePage + bufferRadius);
    const pageStart = (safePage - 1) * pSize;
    const nominalPageLen = Math.min(
      pSize,
      Math.max(0, totalRowCount - pageStart)
    );

    let offset = (bufStartPage - 1) * pSize;
    let limit = Math.min(
      (bufEndPage - bufStartPage + 1) * pSize,
      Math.max(0, totalRowCount - offset)
    );

    const maxRowsByBudget = Math.max(
      100,
      Math.floor(ADAPTIVE_CELL_BUDGET / Math.max(1, colCount))
    );

    if (limit > maxRowsByBudget) {
      const neededActiveRows = Math.min(nominalPageLen, maxRowsByBudget);
      if (offset + maxRowsByBudget < pageStart + neededActiveRows) {
        offset = pageStart;
        limit = Math.min(
          Math.max(0, totalRowCount - offset),
          maxRowsByBudget
        );
      } else {
        limit = maxRowsByBudget;
      }
    }

    const activeAvailable = Math.max(0, offset + limit - pageStart);
    const isPartialCache = activeAvailable < nominalPageLen;

    if (viewState.queryError !== null) {
      this.isInternalStateUpdate = true;
      try {
        oneref.update(
          stateRef,
          vsUpdate((vs: ViewState) => vs.set("queryError", null) as ViewState)
        );
      } finally {
        this.isInternalStateUpdate = false;
      }
    }

    this.pendingOffset = offset;
    this.pendingLimit = limit;
    this.pendingPage = safePage;
    this.pendingIsPartialCache = isPartialCache;
    this.pageSize = pSize;

    const requestDsPath = viewState.dsPath;
    const requestBaseQuery = viewState.baseQuery;

    const dreq = requestDataView(
      viewState.dbc,
      viewParams,
      queryView,
      offset,
      limit,
      this.onViewQuery,
      safePage,
      pSize,
      isPartialCache
    );
    this.pendingDataRequest = dreq;
    dreq.then((dataView) => {
      if (this.pendingDataRequest !== dreq) {
        return dataView;
      }
      this.pendingDataRequest = null;
      oneref.update(
        stateRef,
        vsUpdate((vs: ViewState) => {
          // Stale Query Guard: Ignore result if active view switched to another dataset
          if (
            (requestDsPath && JSON.stringify(vs.dsPath) !== JSON.stringify(requestDsPath)) ||
            (requestBaseQuery && vs.baseQuery !== requestBaseQuery)
          ) {
            console.warn("PivotRequester: Ignoring stale query result because active view changed.");
            return vs;
          }
          let syncedDataView = dataView;
          if (vs.baseSchema && dataView.schema) {
            let nextSchema = dataView.schema;
            for (const cid of dataView.schema.columns) {
              const baseDn = vs.baseSchema.displayName(cid);
              if (baseDn && baseDn !== cid) {
                nextSchema = nextSchema.withDisplayName(cid, baseDn);
              }
            }
            if (nextSchema !== dataView.schema) {
              syncedDataView = new PagedDataView(
                nextSchema,
                dataView.totalRowCount,
                dataView.offset,
                dataView.rawData,
                safePage,
                pSize,
                dataView.isPartialCache
              );
            }
          }
          return (vs.update("loadingTimer", (lt) => lt.stop()) as ViewState)
            .set("dataView", syncedDataView)
            .set("queryError", null) as ViewState;
        })
      );
      this.setLoadingCallback(false);
      return dataView;
    }).catch((err) => {
      if (this.pendingDataRequest !== dreq) {
        return;
      }
      this.pendingDataRequest = null;
      this.pendingLimit = 0;
      this.pendingIsPartialCache = false;
      const errMsg = err?.message || String(err);
      const latestVs = mutableGet(stateRef).viewState;
      const isStale =
        !latestVs ||
        (requestDsPath && JSON.stringify(latestVs.dsPath) !== JSON.stringify(requestDsPath)) ||
        (requestBaseQuery && latestVs.baseQuery !== requestBaseQuery);
      if (isStale) {
        console.warn("PivotRequester: Ignoring stale data request error because active view changed.");
        return;
      }
      console.error(
        "PivotRequester: caught error fetching data view: ",
        errMsg,
        err?.stack
      );
      if (typeof window !== "undefined") {
        window.dispatchEvent(
          new CustomEvent("toad-query-error", {
            detail: { message: errMsg },
          })
        );
        window.dispatchEvent(
          new CustomEvent("tad-query-error", {
            detail: { message: errMsg },
          })
        );
      }
      this.setLoadingCallback(false);
      this.isInternalStateUpdate = true;
      try {
        oneref.update(
          stateRef,
          vsUpdate((vs: ViewState) => {
            if (
              (requestDsPath && JSON.stringify(vs.dsPath) !== JSON.stringify(requestDsPath)) ||
              (requestBaseQuery && vs.baseQuery !== requestBaseQuery)
            ) {
              return vs;
            }
            return (vs.update("loadingTimer", (lt) => lt.stop()) as ViewState).set(
              "queryError",
              errMsg
            ) as ViewState;
          })
        );
      } finally {
        this.isInternalStateUpdate = false;
      }
      if (this.errorCallback) {
        this.errorCallback(err instanceof Error ? err : new Error(errMsg));
      }
    });
    return dreq;
  }

  onStateChange(stateRef: oneref.StateRef<AppState>) {
    if (this.isInternalStateUpdate) {
      return;
    }
    const appState: AppState = mutableGet(stateRef);
    const viewState = appState.viewState;

    if (viewState === null) {
      return;
    }

    const { queryView } = viewState;
    const viewParams = viewState.viewParams;

    if (
      viewParams !== this.pendingViewParams ||
      viewState.baseQuery !== this.pendingBaseQuery
    ) {
      /*
      log.debug(
        "*** onStateChange: requesting new query: ",
        viewState.toJS(),
        this.pendingViewParams?.toJS()
      );
      */
      /*
      const viewStateJS = viewState.toJS();
      const pendingJS = this.pendingViewParams?.toJS();
      if (viewStateJS && pendingJS) {
        const diffs = getObjectDiff(
          viewState.toJS(),
          this.pendingViewParams?.toJS()
        );
        console.log("*** state change diffs: ", diffs);
      }
      */

      // Might be nice to cancel any pending request here...
      // failing that we could calculate additional pages we need
      // if viewParams are same and only page range differs.
      const prevViewParams = this.pendingViewParams as ViewParams;
      const baseQueryChanged = viewState.baseQuery !== this.pendingBaseQuery;
      this.pendingViewParams = viewParams;
      this.pendingBaseQuery = viewState.baseQuery;

      const isOnlyDisplayOrderChange = (
        prevVP: ViewParams | null | undefined,
        nextVP: ViewParams | null | undefined
      ): boolean => {
        if (!prevVP || !nextVP) return false;
        return (
          prevVP.filterExp === nextVP.filterExp &&
          prevVP.sortKey === nextVP.sortKey &&
          prevVP.vpivots === nextVP.vpivots &&
          prevVP.pivotLeafColumn === nextVP.pivotLeafColumn &&
          prevVP.showColumnHistograms === nextVP.showColumnHistograms &&
          prevVP.showRoot === nextVP.showRoot &&
          prevVP.openPaths === nextVP.openPaths
        );
      };

      const curDv = viewState.dataView;
      const dvHasAllDisplayCols =
        curDv != null &&
        curDv.schema != null &&
        curDv.schema.columns != null &&
        viewParams.displayColumns.every((c) => curDv.schema.columns.includes(c));

      if (
        !baseQueryChanged &&
        queryView != null &&
        dvHasAllDisplayCols &&
        isOnlyDisplayOrderChange(prevViewParams, viewParams)
      ) {
        // Fast path (Toad v2.0.5): Only displayColumns order changed.
        // In-memory DataView already contains all columns; skip expensive DuckDB re-query and IPC transfer!
        return;
      }
      const requestDsPath = viewState.dsPath;
      const requestBaseQuery = viewState.baseQuery;
      this.currentQueryView = null;
      this.pendingOffset = 0;
      this.pendingLimit = 0;
      this.pendingIsPartialCache = false;
      // NOTE!  Very important to pass queryView (latest from appState) and not
      // this.currentQueryView, which may be stale.
      // TODO:  The sequencing and control flow here is really subtle; we should rework this to make
      // it easier to reason about the sequencing.
      const qreq = requestQueryView(
        viewState.dbc,
        viewState.baseQuery,
        viewState.baseSchema,
        this.pendingViewParams,
        appState.showRecordCount,
        queryView,
        this.onViewRowCount,
        this.onViewRowCountResolved
      );
      this.pendingQueryRequest = qreq;
      this.pendingDataRequest = qreq
        .then((queryView) => {
          if (this.pendingQueryRequest !== qreq) {
            return null as any;
          }
          this.pendingQueryRequest = null;
          const curState: AppState = mutableGet(stateRef);
          if (
            curState.viewState &&
            ((requestDsPath && JSON.stringify(curState.viewState.dsPath) !== JSON.stringify(requestDsPath)) ||
             (requestBaseQuery && curState.viewState.baseQuery !== requestBaseQuery))
          ) {
            console.warn("PivotRequester: Ignoring stale QueryView because active view changed.");
            return null as any;
          }

          // Preloaded buffer reuse - avoid duplicate DuckDB queries when opening cached table
          const curVs = curState.viewState;
          const dv = curVs?.dataView;
          const vp = curVs?.viewParams;
          const baseSchema = curVs?.baseSchema;
          const pSize = curVs?.pageSize || this.pageSize || 1000;
          const curPage = curVs?.currentPage || 1;

          const INTERNAL_OR_REC = new Set(["Rec", "_depth", "_pivot", "_isRoot"]);
          const normalizeCols = (cols: string[]) =>
            cols.filter(
              (c) => !INTERNAL_OR_REC.has(c) && !/^_sortVal_\d+$/.test(c)
            );
          const dvCols = dv?.schema?.columns ? normalizeCols(dv.schema.columns) : [];
          const effectiveSortKey = baseSchema
            ? computeEffectiveSortKey(
                baseSchema,
                vp || new ViewParams(),
                queryView.baseRowCount
              )
            : undefined;
          const allProjectedCols = baseSchema
            ? computeProjectedColumns(
                baseSchema,
                vp || new ViewParams(),
                effectiveSortKey
              )
            : [];
          const prunedCols = normalizeCols(allProjectedCols);
          const colsMatch =
            dvCols.length > 0 &&
            dvCols.length === prunedCols.length &&
            dvCols.every((c: string, i: number) => c === prunedCols[i]);

          const projColCount = Math.max(1, allProjectedCols.length || 1);
          const maxRowsByBudget = Math.max(
            100,
            Math.floor(ADAPTIVE_CELL_BUDGET / projColCount)
          );
          const expectedLimit = Math.min(
            queryView.rowCount,
            pSize,
            maxRowsByBudget
          );

          const canReusePreloadedBuffer =
            curVs != null &&
            curVs.baseQuery === requestBaseQuery &&
            curVs.queryView == null &&
            curPage === 1 &&
            dv != null &&
            dv.rawData &&
            dv.rawData.length > 0 &&
            dv.offset === 0 &&
            dv.currentPage === 1 &&
            dv.pageSize === pSize &&
            dv.rawData.length >= expectedLimit &&
            (!vp ||
              (vp.vpivots.length === 0 &&
                vp.filterExp.opArgs.length === 0 &&
                (!effectiveSortKey || effectiveSortKey.length === 0))) &&
            colsMatch;

          this.currentQueryView = queryView;

          if (canReusePreloadedBuffer) {
            this.pendingDataRequest = null;
            this.pendingOffset = dv.offset;
            this.pendingLimit = dv.rawData.length;
            this.pendingPage = 1;
            this.pendingIsPartialCache = Boolean(dv.isPartialCache);
            this.pageSize = pSize;
            this.setLoadingCallback(false);
            this.isInternalStateUpdate = true;
            try {
              oneref.update(
                stateRef,
                vsUpdate((vs: ViewState) => {
                  const [viewportTop, viewportBottom] = paging.clampViewport(
                    queryView.rowCount,
                    vs.viewportTop,
                    vs.viewportBottom
                  );
                  return (vs.update("loadingTimer", (lt) => lt.stop()) as ViewState)
                    .set("viewportTop", viewportTop)
                    .set("viewportBottom", viewportBottom)
                    .set("queryView", queryView)
                    .set("queryError", null) as ViewState;
                })
              );
            } finally {
              this.isInternalStateUpdate = false;
            }
            return dv;
          }

          this.isInternalStateUpdate = true;
          try {
            oneref.update(
              stateRef,
              vsUpdate((vs: ViewState) => {
                /*
                 * queryView.rowCount may have changed since last data request;
                 * trim viewport to ensure its in range
                 */
                const [viewportTop, viewportBottom] = paging.clampViewport(
                  queryView.rowCount,
                  vs.viewportTop,
                  vs.viewportBottom
                );
                return vs
                  .set("viewportTop", viewportTop)
                  .set("viewportBottom", viewportBottom)
                  .set("queryView", queryView)
                  .set("queryError", null) as ViewState;
              })
            );
          } finally {
            this.isInternalStateUpdate = false;
          }

          return this.sendDataRequest(stateRef, queryView);
        })
        .catch((err) => {
          if (this.pendingQueryRequest !== qreq) {
            return;
          }
          this.pendingQueryRequest = null;
          const errMsg = err?.message || String(err);
          const curState: AppState = mutableGet(stateRef);
          const curVs = curState.viewState;
          const isStale =
            !curVs ||
            (requestDsPath && JSON.stringify(curVs.dsPath) !== JSON.stringify(requestDsPath)) ||
            (requestBaseQuery && curVs.baseQuery !== requestBaseQuery);
          if (isStale) {
            console.warn("PivotRequester: Ignoring stale QueryView error because active view changed.");
            return;
          }
          console.error(
            "PivotRequester: caught error updating view: ",
            errMsg,
            err?.stack
          );
          if (typeof window !== "undefined") {
            window.dispatchEvent(
              new CustomEvent("toad-query-error", {
                detail: { message: errMsg },
              })
            );
            window.dispatchEvent(
              new CustomEvent("tad-query-error", {
                detail: { message: errMsg },
              })
            );
          }
          this.setLoadingCallback(false);
          // TODO:
          // remoteErrorDialog("Error constructing view", err.message); // Now let's try and restore to previous view params:
          this.isInternalStateUpdate = true;
          try {
            oneref.update(
              stateRef,
              vsUpdate((vs: ViewState) => {
                if (
                  (requestDsPath && JSON.stringify(vs.dsPath) !== JSON.stringify(requestDsPath)) ||
                  (requestBaseQuery && vs.baseQuery !== requestBaseQuery)
                ) {
                  return vs;
                }
                return (vs.update("loadingTimer", (lt) => lt.stop()) as ViewState).set(
                  "queryError",
                  errMsg
                ) as ViewState;
              })
            );
          } finally {
            this.isInternalStateUpdate = false;
          }
          if (this.errorCallback) {
            this.errorCallback(err instanceof Error ? err : new Error(errMsg));
          }
        });
      const ltUpdater = util.pathUpdater<AppState, Timer>(stateRef, [
        "viewState",
        "loadingTimer",
      ]);
      const nextAppState = appState
        .updateIn(["viewState", "loadingTimer"], (lt) =>
          (lt as Timer).run(200, ltUpdater as unknown as TimerUpdater)
        )
        .setIn(["viewState", "queryError"], null) as AppState;
      this.isInternalStateUpdate = true;
      try {
        oneref.update(stateRef, (_) => nextAppState);
      } finally {
        this.isInternalStateUpdate = false;
      }
    } else {
      const rawCurPage = viewState.currentPage || 1;
      const pSize = viewState.pageSize || 1000;
      const totalRowCount = this.currentQueryView?.rowCount ?? Infinity;
      const totalPages = Number.isFinite(totalRowCount)
        ? Math.max(1, Math.ceil(totalRowCount / pSize))
        : rawCurPage;
      const curPage = Math.min(Math.max(1, rawCurPage), totalPages);
      const pageStartRow = (curPage - 1) * pSize;
      const pageEndRow = pageStartRow + pSize;

      // Check if current page is outside the loaded buffer range or pageSize changed:
      const effectiveEndRow = Math.min(pageEndRow, totalRowCount);
      const pageSizeChanged = pSize !== this.pageSize;
      const partialPageSatisfied =
        this.pendingIsPartialCache &&
        curPage === this.pendingPage &&
        !pageSizeChanged &&
        pageStartRow >= this.pendingOffset &&
        pageStartRow < this.pendingOffset + this.pendingLimit;
      const bufferOutOfRange =
        !partialPageSatisfied &&
        (pageStartRow < this.pendingOffset ||
          effectiveEndRow > this.pendingOffset + this.pendingLimit);
      this.pageSize = pSize;

      if (
        this.currentQueryView !== null &&
        this.pendingQueryRequest == null &&
        (bufferOutOfRange || pageSizeChanged)
      ) {
        if (
          viewState.queryError != null &&
          curPage === this.pendingPage &&
          !pageSizeChanged
        ) {
          return;
        }
        const qv: QueryView = this.currentQueryView as any;
        this.sendDataRequest(stateRef, qv);
      }
    }
  }
}
