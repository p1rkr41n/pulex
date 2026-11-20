/**
 * Safe CSP-compliant bootstrap error trap for Pulex Data Analyzer.
 * Uses DOM node construction + textContent (never innerHTML) to prevent XSS.
 */

if (typeof window !== "undefined" && typeof document !== "undefined") {
  window.addEventListener("error", (e: ErrorEvent) => {
    const msg =
      (e.error && (e.error.stack || e.error.message)) ||
      e.message ||
      "Unknown Runtime Error";
    try {
      const app = document.getElementById("app") || document.getElementById("root");
      if (app && app.children.length === 0) {
        const wrapper = document.createElement("div");
        wrapper.style.cssText =
          "padding:24px;color:#f87171;font-family:sans-serif;background:#18181b;height:100vh;box-sizing:border-box;";

        const heading = document.createElement("h2");
        heading.style.cssText = "margin-top:0;color:#ef4444;";
        heading.textContent = "Pulex Data Analyzer - Runtime Error";

        const pre = document.createElement("pre");
        pre.style.cssText =
          "white-space:pre-wrap;background:#27272a;padding:12px;border-radius:6px;color:#fca5a5;";
        pre.textContent = String(msg);

        wrapper.appendChild(heading);
        wrapper.appendChild(pre);
        app.appendChild(wrapper);
      }
    } catch {
      // Ignore DOM fallback errors
    }
  });
}

