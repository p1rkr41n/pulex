/**
 * test_v30_pulex_upgrade.js
 *
 * Comprehensive Verification Suite for Pulex v3.0.0 Parity & Security Hardening (G-01 .. G-27):
 * - Verifies TextFormatOptions XSS neutralization, inline onclick removal, and ReDoS guard (G-13, G-14).
 * - Verifies CellLineCalculator 20-line Smart Expand (368px ceiling) + DOM reconciliation + legacy fallback (G-19).
 * - Verifies CommandAliases bidirectional completeness for Derived Views, Export, Startup Args, Plugins (G-05, G-08, G-09, G-11).
 * - Verifies Reltab Connection / HybridProtocol limit = 0 schema probe guard (G-04).
 * - Verifies CSP-safe bootstrapErrorTrap.ts & index.html without inline scripts or innerHTML (G-13, G-22).
 * - Verifies codebase-wide English-Only Public Language Policy across source & tests.
 */

const assert = require("assert");
const fs = require("fs");
const path = require("path");

const WORKSPACE_ROOT = path.resolve(__dirname, "..");

const ts = require(path.resolve(WORKSPACE_ROOT, "node_modules/typescript"));
if (!require.extensions[".ts"]) {
  require.extensions[".ts"] = function (module, filename) {
    const content = fs.readFileSync(filename, "utf8");
    const result = ts.transpileModule(content, {
      compilerOptions: {
        module: ts.ModuleKind.CommonJS,
        target: ts.ScriptTarget.ES2020,
        esModuleInterop: true,
      },
    });
    module._compile(result.outputText, filename);
  };
}

const { TextFormatOptions } = require(path.resolve(
  WORKSPACE_ROOT,
  "src/TextFormatOptions.ts"
));
const {
  calculateLineCount,
  getHeightAndScrollState,
  extractComputedFontOptions,
  reconcileDomLineCount,
} = require(path.resolve(WORKSPACE_ROOT, "src/utils/CellLineCalculator.ts"));
const {
  COMMAND_ALIAS_MAP,
  REVERSE_COMMAND_ALIAS_MAP,
} = require(path.resolve(
  WORKSPACE_ROOT,
  "packages/host-bridge/src/commandAliases.ts"
));
const reltab = require(path.resolve(
  WORKSPACE_ROOT,
  "packages/reltab/dist/reltab.js"
));

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

function runUpgradeTests() {
  console.log(`\n${BOLD}${CYAN}======================================================================${RESET}`);
  console.log(`${BOLD}${CYAN}  PULEX v3.0.0 PARITY, SECURITY & BRANDING VERIFICATION SUITE        ${RESET}`);
  console.log(`${BOLD}${CYAN}======================================================================${RESET}\n`);

  console.log(`${BOLD}${YELLOW}--- SECTION 1: XSS, CSP & URL Sanitization (G-11, G-13, G-14) ---${RESET}`);

  test("1.1 TextFormatOptions enforces plain-text URL safe mode, blocks openURL, and escapes HTML/XSS payloads", () => {
    const tf = new TextFormatOptions({ urlsAsHyperlinks: true });
    const formatter = tf.getFormatter();

    const safeHtml = formatter("https://example.com/report?id=1&tag=sec");
    assert.ok(!safeHtml.includes("<a "), "URLs must always render as plain text, never <a> anchors");
    assert.ok(!safeHtml.includes("href="), "URLs must never emit href attributes");
    assert.ok(!safeHtml.includes("onclick="), "Must never emit inline onclick attribute");
    assert.ok(
      safeHtml.includes("https://example.com/report?id=1") &&
      (safeHtml.includes("&amp;tag=sec") || safeHtml.includes("&#x26;tag=sec")),
      "URL text must be HTML-escaped plain text"
    );

    let openedUrl = null;
    const clickHandler = tf.getClickHandler();
    clickHandler({ openURL: (u) => { openedUrl = u; } }, 0, 0, "https://evil.example/malware.exe");
    assert.strictEqual(openedUrl, null, "Clicking a URL cell must never invoke openURL");

    const xssPayload = 'https://evil.example/" onmouseover="alert(1)" <script>alert(2)</script>';
    const rendered = formatter(xssPayload);
    assert.ok(!rendered.includes("<script>"), "Raw <script> tag must be entity-encoded");
    assert.ok(!rendered.includes('onmouseover="alert(1)"'), "Injected event handler attribute must be escaped");
  });

  test("1.2 TextFormatOptions ReDoS guard skips regex matching on >4096 char strings", () => {
    const tf = new TextFormatOptions({ urlsAsLinks: true });
    const formatter = tf.getFormatter();
    const huge = "https://example.com/" + "a".repeat(5000);
    const t0 = Date.now();
    const out = formatter(huge);
    const elapsed = Date.now() - t0;
    assert.ok(elapsed < 50, `Formatting >4096 chars took ${elapsed}ms (expected < 50ms)`);
    assert.ok(!out.includes("<a "), "Oversized cell string must bypass URL regex and HTML-encode safely");
  });

  test("1.3 index.html and bootstrapErrorTrap.ts enforce strict CSP without inline script or innerHTML", () => {
    const html = fs.readFileSync(path.resolve(WORKSPACE_ROOT, "index.html"), "utf8");
    assert.ok(html.includes("<title>Pulex</title>"), "index.html title must be Pulex");
    assert.ok(!html.includes("window.addEventListener('error'"), "index.html must not contain inline error script");
    assert.ok(!html.includes("#2f343c !important"), "index.html must not hardcode dark #2f343c !important");

    const trap = fs.readFileSync(path.resolve(WORKSPACE_ROOT, "src/bootstrapErrorTrap.ts"), "utf8");
    assert.ok(!/\.innerHTML\s*=/.test(trap), "bootstrapErrorTrap.ts must never assign innerHTML");
    assert.ok(trap.includes("textContent"), "bootstrapErrorTrap.ts must use safe textContent");
  });

  console.log(`\n${BOLD}${YELLOW}--- SECTION 2: 20-Line Smart Expand & Hybrid Protocol (G-04, G-19) ---${RESET}`);

  test("2.1 CellLineCalculator enforces 20-line 368px ceiling and DOM scrollHeight reconciliation", () => {
    assert.strictEqual(getHeightAndScrollState(1).height, 25);
    assert.strictEqual(getHeightAndScrollState(5).height, 5 * 18 + 8);
    assert.strictEqual(getHeightAndScrollState(20).height, 368);
    assert.strictEqual(getHeightAndScrollState(20).overflowY, "hidden");
    assert.strictEqual(getHeightAndScrollState(25).height, 368);
    assert.strictEqual(getHeightAndScrollState(25).overflowY, "auto");
    assert.strictEqual(getHeightAndScrollState(25).hasScrollbar, true);

    // Legacy 17px lineHeight backward compatibility
    assert.strictEqual(getHeightAndScrollState(10, 17, 8, 24).height, 93);

    // DOM reconciliation
    assert.strictEqual(reconcileDomLineCount(2, 180, 18, 6), 10);
    assert.deepStrictEqual(extractComputedFontOptions(null), { padding: 0 });
  });

  test("2.2 RemoteReltabConnection preserves offset=0 and limit=0 on schema probes (G-04)", () => {
    const connSource = fs.readFileSync(
      path.resolve(WORKSPACE_ROOT, "packages/reltab/src/remote/Connection.ts"),
      "utf8"
    );
    assert.ok(
      connSource.includes("offset: offset != null ? offset : null"),
      "Connection.ts must preserve offset=0 via != null check"
    );
    assert.ok(
      connSource.includes("limit: limit != null ? limit : null"),
      "Connection.ts must preserve limit=0 via != null check"
    );

    const serverSource = fs.readFileSync(
      path.resolve(WORKSPACE_ROOT, "packages/reltab/src/remote/server.ts"),
      "utf8"
    );
    assert.ok(
      serverSource.includes("req.limit == null || req.limit > 0"),
      "server.ts must guard Arrow path against limit=0 schema probes"
    );
    assert.ok(typeof reltab.RemoteReltabConnection === "function");
  });

  console.log(`\n${BOLD}${YELLOW}--- SECTION 3: IPC Command Aliases & English-Only Policy (G-05..G-11) ---${RESET}`);

  test("3.1 CommandAliases covers Derived Views, Export, Startup Args, and Native Tauri Plugins", () => {
    assert.strictEqual(COMMAND_ALIAS_MAP["view:register_derived"], "view:registerDerived");
    assert.strictEqual(COMMAND_ALIAS_MAP["view:get_derived"], "view:getDerived");
    assert.strictEqual(COMMAND_ALIAS_MAP["toad:get_startup_args"], "getStartupArgs");
    assert.strictEqual(COMMAND_ALIAS_MAP["toad:browse_export_path"], "browse-export-path");
    assert.strictEqual(COMMAND_ALIAS_MAP["toad:export_file"], "export-file");
    assert.strictEqual(COMMAND_ALIAS_MAP["toad:save_workspace"], "saveWorkspace");
    assert.strictEqual(COMMAND_ALIAS_MAP["plugin:clipboard-manager|write_text"], "clipboard:writeText");
    assert.strictEqual(COMMAND_ALIAS_MAP["plugin:dialog|message"], "errorDialog");

    assert.strictEqual(REVERSE_COMMAND_ALIAS_MAP["view:registerDerived"], "view:register_derived");
    assert.strictEqual(REVERSE_COMMAND_ALIAS_MAP["DataSourceConnection.evalQuery"], "toad:eval_query");
    assert.strictEqual(REVERSE_COMMAND_ALIAS_MAP["clipboard:writeText"], "plugin:clipboard-manager|write_text");
  });

  test("3.2 Zero Vietnamese diacritical characters across src/, packages/, src-tauri/, and tests/", () => {
    const vnRegex = /[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ]/;
    const scanDirs = ["src", "packages", "src-tauri/src", "src-tauri/tests", "tests"];
    const exts = new Set([".ts", ".tsx", ".js", ".cjs", ".rs", ".html", ".less", ".scss"]);
    const violations = [];

    function walk(dir) {
      if (!fs.existsSync(dir)) return;
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        if (entry.name === "node_modules" || entry.name === "dist" || entry.name === "target") {
          continue;
        }
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          walk(full);
        } else if (exts.has(path.extname(entry.name)) && entry.name !== "test_v30_pulex_upgrade.js") {
          const content = fs.readFileSync(full, "utf8");
          if (vnRegex.test(content)) {
            violations.push(path.relative(WORKSPACE_ROOT, full));
          }
        }
      }
    }

    for (const d of scanDirs) {
      walk(path.resolve(WORKSPACE_ROOT, d));
    }
    assert.deepStrictEqual(
      violations,
      [],
      `Found non-English (Vietnamese) text in: ${violations.join(", ")}`
    );
  });

  console.log(`\n${BOLD}${CYAN}----------------------------------------------------------------------${RESET}`);
  console.log(`${BOLD}Pulex v3.0.0 Upgrade Suite: ${GREEN}${passCount} Passed${RESET}, ${failCount === 0 ? GREEN : RED}${failCount} Failed${RESET}`);
  console.log(`${BOLD}${CYAN}======================================================================${RESET}\n`);

  if (failCount > 0) {
    process.exit(1);
  }
}

if (require.main === module) {
  runUpgradeTests();
}

module.exports = { runUpgradeTests };
