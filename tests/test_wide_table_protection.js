/**
 * test_wide_table_protection.js
 *
 * Unit tests for R3-R5 Wide-Table Protection & Laptop Peak Protection:
 * 1. PagedDataView: isPartialCache clamping getLength() (zero blank rows), boundary checks, and withPage().
 * 2. computeProjectedColumns & buildPrunedSchema: column preservation, baseSchema ordering, dedup, unknown ignore, 100-col fallback.
 * 3. MultipleLoadingManager: estimateTableRepBytes & byte-budget LRU eviction (<= 48 MB total, <= 12 entries, skip > 16 MB).
 */

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const Module = require("module");

const WORKSPACE_ROOT = path.resolve(__dirname, "..");

const ALIAS_MAP = {
  reltab: path.resolve(WORKSPACE_ROOT, "packages/reltab/src/reltab.ts"),
  aggtree: path.resolve(WORKSPACE_ROOT, "packages/aggtree/src/aggtree.ts"),
  "host-bridge": path.resolve(WORKSPACE_ROOT, "packages/host-bridge/src/index.ts"),
};

const origResolveFilename = Module._resolveFilename;
Module._resolveFilename = function (request, parent, isMain, options) {
  if (Object.prototype.hasOwnProperty.call(ALIAS_MAP, request)) {
    return ALIAS_MAP[request];
  }
  return origResolveFilename.call(this, request, parent, isMain, options);
};

const ts = require(path.resolve(WORKSPACE_ROOT, "node_modules/typescript"));
const compileTs = (module, filename) => {
  const content = fs.readFileSync(filename, "utf8");
  const result = ts.transpileModule(content, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      esModuleInterop: true,
      jsx: ts.JsxEmit.React,
    },
    fileName: filename,
  });
  module._compile(result.outputText, filename);
};
require.extensions[".ts"] = compileTs;
require.extensions[".tsx"] = compileTs;

const reltab = require("reltab");
const { PagedDataView } = require(path.resolve(WORKSPACE_ROOT, "src/PagedDataView.ts"));
const { ViewParams } = require(path.resolve(WORKSPACE_ROOT, "src/ViewParams.ts"));
const {
  computeProjectedColumns,
  buildPrunedSchema,
} = require(path.resolve(WORKSPACE_ROOT, "src/PivotRequester.ts"));
const {
  MultipleLoadingManager,
  MAX_FIRST_PAGE_CACHE_BYTES,
  MAX_SINGLE_CACHE_ENTRY_BYTES,
  MAX_FIRST_PAGE_CACHE_ENTRIES,
  estimateTableRepBytes,
} = require(path.resolve(WORKSPACE_ROOT, "src/MultipleLoadingManager.ts"));

const BOLD = "\x1b[1m";
const GREEN = "\x1b[32m";
const RED = "\x1b[31m";
const CYAN = "\x1b[36m";
const YELLOW = "\x1b[33m";
const RESET = "\x1b[0m";

let passCount = 0;
let failCount = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`  ${GREEN}✔ [PASS]${RESET} ${name}`);
    passCount++;
  } catch (err) {
    console.error(`  ${RED}✖ [FAIL]${RESET} ${name}`);
    console.error(`     ${err.stack || err.message}`);
    failCount++;
  }
}

function makeSchema(colNames) {
  const intType = reltab.SQLiteDialect.coreColumnTypes.integer.sqlTypeName;
  const strType = reltab.SQLiteDialect.coreColumnTypes.string.sqlTypeName;
  const meta = {};
  for (let i = 0; i < colNames.length; i++) {
    const c = colNames[i];
    meta[c] = {
      displayName: `Display_${c}`,
      columnType: c === "Rec" ? intType : strType,
    };
  }
  return new reltab.Schema(reltab.SQLiteDialect, colNames, meta);
}

function makeRows(count, colNames) {
  const rows = new Array(count);
  for (let i = 0; i < count; i++) {
    const r = {
      _isLeaf: true,
      _depth: 0,
      _pivot: "",
      _isOpen: false,
    };
    for (let c = 0; c < colNames.length; c++) {
      r[colNames[c]] = `v_${i}_${c}`;
    }
    rows[i] = r;
  }
  return rows;
}

function makeSizedTableRep(cols, rowCount, charsPerCell) {
  const schema = makeSchema(cols);
  const cellPayload = "x".repeat(charsPerCell);
  const rowData = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    const row = {};
    for (let c = 0; c < cols.length; c++) {
      row[cols[c]] = cellPayload;
    }
    rowData[i] = row;
  }
  return new reltab.TableRep(schema, rowData);
}

function runWideTableProtectionTests() {
  console.log(`\n${BOLD}${CYAN}======================================================================${RESET}`);
  console.log(`${BOLD}${CYAN}       PULEX WIDE-TABLE & LAPTOP PEAK PROTECTION UNIT TESTS          ${RESET}`);
  console.log(`${BOLD}${CYAN}======================================================================${RESET}\n`);

  const mgr = MultipleLoadingManager.getInstance();

  try {
    console.log(`${BOLD}${YELLOW}--- SECTION 1: PagedDataView Partial Cache Clamping ---${RESET}`);

    test("1.1 isPartialCache=true clamps getLength() to 100 with zero blank null rows", () => {
      const schema = makeSchema(["colA", "colB"]);
      const rawData = makeRows(100, ["colA", "colB"]);
      const pdv = new PagedDataView(schema, 50000, 0, rawData, 1, 1000, true);

      assert.strictEqual(pdv.getLength(), 100);
      assert.ok(pdv.getItem(0) !== null);
      assert.strictEqual(pdv.getItem(0).colA, "v_0_0");
      assert.ok(pdv.getItem(99) !== null);
      assert.strictEqual(pdv.getItem(99).colA, "v_99_0");
      assert.strictEqual(pdv.getItem(100), null);
      assert.strictEqual(pdv.getItem(-1), null);
    });

    test("1.2 withPage() preserves isPartialCache and handles page 2 overlap boundaries", () => {
      const schema = makeSchema(["colA"]);
      const raw100 = makeRows(100, ["colA"]);
      const pdv1 = new PagedDataView(schema, 50000, 0, raw100, 1, 1000, true);
      const pdv1Page2 = pdv1.withPage(2);

      assert.strictEqual(pdv1Page2.isPartialCache, true);
      assert.strictEqual(pdv1Page2.currentPage, 2);
      assert.strictEqual(pdv1Page2.getLength(), 0);
      assert.strictEqual(pdv1Page2.getItem(0), null);

      const raw1250 = makeRows(1250, ["colA"]);
      const pdv2 = new PagedDataView(schema, 50000, 0, raw1250, 1, 1000, true);
      const pdv2Page2 = pdv2.withPage(2);

      assert.strictEqual(pdv2Page2.isPartialCache, true);
      assert.strictEqual(pdv2Page2.getLength(), 250);
      assert.strictEqual(pdv2Page2.getItem(0).colA, "v_1000_0");
      assert.strictEqual(pdv2Page2.getItem(249).colA, "v_1249_0");
      assert.strictEqual(pdv2Page2.getItem(250), null);
    });

    test("1.3 isPartialCache=false retains full nominal page length", () => {
      const schema = makeSchema(["colA"]);
      const rawData = makeRows(100, ["colA"]);
      const pdv = new PagedDataView(schema, 50000, 0, rawData, 1, 1000, false);

      assert.strictEqual(pdv.getLength(), 1000);
      assert.ok(pdv.getItem(99) !== null);
      assert.strictEqual(pdv.getItem(100), null);
    });

    console.log(`\n${BOLD}${YELLOW}--- SECTION 2: computeProjectedColumns & buildPrunedSchema ---${RESET}`);

    test("2.1 Preserves displayColumns, sortKey, vpivots, pivotLeafColumn, and Rec in baseSchema order", () => {
      const allCols = [];
      for (let i = 0; i < 1655; i++) {
        allCols.push(`col_${String(i).padStart(4, "0")}`);
      }
      allCols.push("Rec");
      const baseSchema = makeSchema(allCols);

      const vp = new ViewParams({
        displayColumns: ["col_1500", "col_0010", "col_0500", "nonexistent_col", "col_0010"],
        vpivots: ["col_0800", "unknown_pivot"],
        pivotLeafColumn: "col_0250",
      });
      const sortKey = [
        ["col_1200", false],
        ["col_0010", true],
        ["missing_sort", true],
      ];

      const projected = computeProjectedColumns(baseSchema, vp, sortKey);
      assert.deepStrictEqual(projected, [
        "col_0010",
        "col_0250",
        "col_0500",
        "col_0800",
        "col_1200",
        "col_1500",
        "Rec",
      ]);

      const pruned = buildPrunedSchema(baseSchema, projected);
      assert.deepStrictEqual(pruned.columns, projected);
      assert.strictEqual(pruned.displayName("col_1500"), "Display_col_1500");
      assert.strictEqual(pruned.columnMetadata["col_0001"], undefined);
    });

    test("2.2 Empty displayColumns falls back to first 100 columns plus sort/pivot/Rec", () => {
      const allCols = [];
      for (let i = 0; i < 300; i++) {
        allCols.push(`c_${i}`);
      }
      allCols.push("Rec");
      const baseSchema = makeSchema(allCols);

      const vp = new ViewParams({
        displayColumns: [],
        vpivots: ["c_250"],
        pivotLeafColumn: "c_260",
      });
      const sortKey = [["c_220", false]];

      const projected = computeProjectedColumns(baseSchema, vp, sortKey);
      assert.strictEqual(projected.length, 104);
      assert.strictEqual(projected[0], "c_0");
      assert.strictEqual(projected[99], "c_99");
      assert.strictEqual(projected[100], "c_220");
      assert.strictEqual(projected[101], "c_250");
      assert.strictEqual(projected[102], "c_260");
      assert.strictEqual(projected[103], "Rec");
    });

    console.log(`\n${BOLD}${YELLOW}--- SECTION 3: MultipleLoadingManager Byte-Budget LRU Cache ---${RESET}`);

    test("3.1 Cache budget constants match specification (48 MB / 16 MB / 12 entries)", () => {
      assert.strictEqual(MAX_FIRST_PAGE_CACHE_BYTES, 48 * 1024 * 1024);
      assert.strictEqual(MAX_SINGLE_CACHE_ENTRY_BYTES, 16 * 1024 * 1024);
      assert.strictEqual(MAX_FIRST_PAGE_CACHE_ENTRIES, 12);
    });

    test("3.2 Skips single entry > 16 MB and purges stale prior entry for same key", () => {
      mgr.clearFirstPageCache();

      const cols = ["c0", "c1"];
      const smallRep = makeSizedTableRep(cols, 10, 100);
      assert.ok(estimateTableRepBytes(smallRep) > 0);
      assert.strictEqual(estimateTableRepBytes(new reltab.TableRep(makeSchema(cols), [])), 0);

      mgr.setFirstPageCache("C:\\logs\\huge.csv", {
        tableName: "tbl_small",
        schema: smallRep.schema,
        fullSchema: smallRep.schema,
        tableRep: smallRep,
        totalRowCount: 10,
        pageSize: 1000,
        bufferPages: 1,
        cachedAt: 1000,
      });
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\huge.csv"), true);

      // 1000 rows * 2 cols * (4500 chars * 2 bytes + 24) ~= 18.05 MB > 16 MB
      const hugeRep = makeSizedTableRep(cols, 1000, 4500);
      const hugeBytes = estimateTableRepBytes(hugeRep);
      assert.ok(
        hugeBytes > MAX_SINGLE_CACHE_ENTRY_BYTES,
        `Expected hugeBytes (${hugeBytes}) > 16 MB`
      );

      mgr.setFirstPageCache("C:\\logs\\huge.csv", {
        tableName: "tbl_huge",
        schema: hugeRep.schema,
        fullSchema: hugeRep.schema,
        tableRep: hugeRep,
        totalRowCount: 1000,
        pageSize: 1000,
        bufferPages: 1,
        cachedAt: 2000,
      });
      assert.strictEqual(
        mgr.hasFirstPageCache("C:\\logs\\huge.csv"),
        false,
        "Oversized entry must be skipped and purge any stale entry at that key"
      );
    });

    test("3.3 Enforces <= 48 MB total byte budget via oldest-cachedAt LRU eviction", () => {
      mgr.clearFirstPageCache();

      // Each entry: 1000 rows * 2 cols * (2500 chars * 2 + 24) + 64 ~= 10.11 MB
      const cols = ["c0", "c1"];
      const rep10MB = makeSizedTableRep(cols, 1000, 2500);
      const entryBytes = estimateTableRepBytes(rep10MB);
      assert.ok(
        entryBytes > 9 * 1024 * 1024 && entryBytes < 12 * 1024 * 1024,
        `Unexpected ~10MB estimate: ${entryBytes}`
      );

      for (let i = 1; i <= 6; i++) {
        mgr.setFirstPageCache(`C:\\logs\\file_${i}.csv`, {
          tableName: `tbl_${i}`,
          schema: rep10MB.schema,
          fullSchema: rep10MB.schema,
          tableRep: rep10MB,
          totalRowCount: 1000,
          pageSize: 1000,
          bufferPages: 1,
          cachedAt: i * 1000,
        });
      }

      // 4 * ~10.11 MB = ~40.44 MB <= 48 MB; 5 entries would exceed 48 MB
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\file_1.csv"), false);
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\file_2.csv"), false);
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\file_3.csv"), true);
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\file_4.csv"), true);
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\file_5.csv"), true);
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\file_6.csv"), true);

      // Verify total cached bytes <= 48 MB via internal map inspection
      let totalBytes = 0;
      for (const entry of mgr.firstPageCache.values()) {
        totalBytes += entry.estimatedBytes || estimateTableRepBytes(entry.tableRep);
      }
      assert.ok(
        totalBytes <= MAX_FIRST_PAGE_CACHE_BYTES,
        `Total cache bytes (${totalBytes}) exceeded 48 MB (${MAX_FIRST_PAGE_CACHE_BYTES})`
      );
    });

    test("3.4 Enforces <= 12 max entry count cap even when bytes are small", () => {
      mgr.clearFirstPageCache();

      const cols = ["c0"];
      const smallRep = makeSizedTableRep(cols, 50, 20);

      for (let i = 1; i <= 15; i++) {
        mgr.setFirstPageCache(`C:\\logs\\small_${i}.csv`, {
          tableName: `tbl_small_${i}`,
          schema: smallRep.schema,
          fullSchema: smallRep.schema,
          tableRep: smallRep,
          totalRowCount: 50,
          pageSize: 1000,
          bufferPages: 1,
          cachedAt: i * 100,
        });
      }

      assert.strictEqual(mgr.firstPageCache.size, MAX_FIRST_PAGE_CACHE_ENTRIES);
      for (let i = 1; i <= 3; i++) {
        assert.strictEqual(
          mgr.hasFirstPageCache(`C:\\logs\\small_${i}.csv`),
          false,
          `Oldest entry small_${i}.csv should be evicted`
        );
      }
      for (let i = 4; i <= 15; i++) {
        assert.strictEqual(
          mgr.hasFirstPageCache(`C:\\logs\\small_${i}.csv`),
          true,
          `Recent entry small_${i}.csv should remain cached`
        );
      }
    });

    test("3.5 getFirstPageCache() refreshes LRU timestamp so accessed entries survive eviction", () => {
      mgr.clearFirstPageCache();

      const cols = ["c0", "c1"];
      const rep10MB = makeSizedTableRep(cols, 1000, 2500);
      const pastBase = Date.now() - 100_000;

      for (let i = 1; i <= 4; i++) {
        mgr.setFirstPageCache(`C:\\logs\\lru_${i}.csv`, {
          tableName: `tbl_lru_${i}`,
          schema: rep10MB.schema,
          fullSchema: rep10MB.schema,
          tableRep: rep10MB,
          totalRowCount: 1000,
          pageSize: 1000,
          bufferPages: 1,
          cachedAt: pastBase + i * 1000,
        });
      }

      // Access lru_1 so its cachedAt refreshes to Date.now() (> lru_2..lru_4)
      const hit = mgr.getFirstPageCache("C:\\logs\\lru_1.csv");
      assert.ok(hit !== undefined);

      // Insert a 5th ~10 MB entry; total would be ~50 MB > 48 MB, so oldest unaccessed (lru_2) must be evicted
      mgr.setFirstPageCache("C:\\logs\\lru_5.csv", {
        tableName: "tbl_lru_5",
        schema: rep10MB.schema,
        fullSchema: rep10MB.schema,
        tableRep: rep10MB,
        totalRowCount: 1000,
        pageSize: 1000,
        bufferPages: 1,
        cachedAt: Date.now() + 1000,
      });

      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\lru_1.csv"), true, "Recently accessed lru_1 must survive");
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\lru_2.csv"), false, "Least-recently-used lru_2 must be evicted");
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\lru_3.csv"), true);
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\lru_4.csv"), true);
      assert.strictEqual(mgr.hasFirstPageCache("C:\\logs\\lru_5.csv"), true);
    });
  } finally {
    mgr.clearFirstPageCache();
    mgr.stopPollingLoop();
  }

  console.log(`\n${BOLD}${CYAN}----------------------------------------------------------------------${RESET}`);
  console.log(`${BOLD}Wide-Table Protection Suite: ${GREEN}${passCount} Passed${RESET}, ${failCount === 0 ? GREEN : RED}${failCount} Failed${RESET}`);
  console.log(`${BOLD}${CYAN}======================================================================${RESET}\n`);

  if (failCount > 0) {
    process.exit(1);
  }
}

if (require.main === module) {
  runWideTableProtectionTests();
}

module.exports = { runWideTableProtectionTests };
