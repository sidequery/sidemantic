import { App, applyDocumentTheme, type McpUiHostContext } from "@modelcontextprotocol/ext-apps";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import embed from "vega-embed";
import { expressionInterpreter } from "vega-interpreter";

const container = document.getElementById("chart")!;
let currentDisplayMode: "inline" | "fullscreen" = "inline";
let lastSpec: Record<string, unknown> | null = null;
let activeObserver: ResizeObserver | null = null;
let activeView: { finalize: () => void } | null = null;

function renderChart(vegaSpec: Record<string, unknown>) {
  if (activeObserver) { activeObserver.disconnect(); activeObserver = null; }
  if (activeView) { activeView.finalize(); activeView = null; }

  container.innerHTML = "";
  const isFullscreen = currentDisplayMode === "fullscreen";
  document.documentElement.classList.toggle("fullscreen", isFullscreen);

  const spec = { ...vegaSpec };
  spec.width = "container";
  spec.height = isFullscreen ? "container" : 500;
  spec.background = "transparent";

  const prefersDark = window.matchMedia?.("(prefers-color-scheme: dark)").matches;

  embed(container, spec as any, {
    actions: false,
    theme: prefersDark ? "dark" : undefined,
    ast: true,
    expr: expressionInterpreter,
  })
    .then((result) => {
      activeView = result;
      const ro = new ResizeObserver(() => result.view.resize().run());
      ro.observe(container);
      activeObserver = ro;

      if (!isFullscreen) {
        addExpandButton();
      }

      requestAnimationFrame(() => {
        if (isFullscreen) {
          app.sendSizeChanged({ height: window.innerHeight - 150 });
        } else {
          const h = Math.max(505, document.documentElement.scrollHeight + 5);
          app.sendSizeChanged({ height: h });
        }
      });
    })
    .catch((err) => {
      container.innerHTML = `<div class="error">Chart render error: ${err.message}</div>`;
    });
}

function addExpandButton() {
  const btn = document.createElement("div");
  btn.className = "expand-btn";
  btn.title = "Expand to fullscreen";
  btn.textContent = "Expand ↗";
  btn.addEventListener("click", goFullscreen);
  container.appendChild(btn);
}

async function goFullscreen() {
  try {
    const result = await app.requestDisplayMode({ mode: "fullscreen" });
    currentDisplayMode = result.mode as "inline" | "fullscreen";
    if (lastSpec) renderChart(lastSpec);
  } catch {
    // host doesn't support fullscreen
  }
}

function extractVegaSpec(result: CallToolResult): Record<string, unknown> | null {
  const sc = result.structuredContent as Record<string, unknown> | undefined;
  if (sc?.vega_spec) return sc.vega_spec as Record<string, unknown>;
  if (result.content) {
    for (const item of result.content) {
      if (item.type === "text") {
        try {
          const data = JSON.parse((item as { text: string }).text);
          if (data.vega_spec) return data.vega_spec;
        } catch {}
      }
    }
  }
  return null;
}

const app = new App(
  { name: "sidemantic-chart", version: "1.0.0" },
  {},
  { autoResize: false },
);

app.ontoolresult = (result: CallToolResult) => {
  const spec = extractVegaSpec(result);
  if (spec) {
    lastSpec = spec;
    renderChart(spec);
  } else {
    container.innerHTML = '<div class="error">No chart data in tool result</div>';
  }
};

app.ontoolinput = () => {
  container.innerHTML = '<div class="loading">Running query...</div>';
};

app.onhostcontextchanged = (ctx: McpUiHostContext) => {
  if (ctx.theme) applyDocumentTheme(ctx.theme);
  if (ctx.displayMode === "inline" || ctx.displayMode === "fullscreen") {
    currentDisplayMode = ctx.displayMode;
    if (lastSpec) renderChart(lastSpec);
  }
};

app.connect().then(() => {
  const ctx = app.getHostContext();
  if (ctx?.theme) applyDocumentTheme(ctx.theme);
  const loading = container.querySelector(".loading");
  if (loading) loading.textContent = "Waiting for chart data...";
  app.sendSizeChanged({ height: 500 });
});
