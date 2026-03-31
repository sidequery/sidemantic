import { App, applyDocumentTheme, type McpUiHostContext } from "@modelcontextprotocol/ext-apps";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import embed from "vega-embed";
import { expressionInterpreter } from "vega-interpreter";

const container = document.getElementById("chart")!;

function renderChart(vegaSpec: Record<string, unknown>) {
  // Clear chart content but preserve the fullscreen button
  Array.from(container.children).forEach(c => {
    if (c.id !== "fullscreen-btn") c.remove();
  });
  const spec = { ...vegaSpec };
  spec.width = "container";
  spec.height = 500;
  spec.background = "transparent";

  const prefersDark = window.matchMedia?.("(prefers-color-scheme: dark)").matches;

  embed(container, spec as any, {
    actions: false,
    theme: prefersDark ? "dark" : undefined,
    // CSP-safe: use AST interpreter instead of eval
    ast: true,
    expr: expressionInterpreter,
  })
    .then((result) => {
      const ro = new ResizeObserver(() => result.view.resize().run());
      ro.observe(container);
      // Tell host the actual content height after render
      requestAnimationFrame(() => {
        const h = Math.max(500, document.documentElement.scrollHeight);
        app.sendSizeChanged({ height: h });
      });
    })
    .catch((err) => {
      container.innerHTML = `<div class="error">Chart render error: ${err.message}</div>`;
    });
}

function extractVegaSpec(result: CallToolResult): Record<string, unknown> | null {
  // Try structuredContent first
  const sc = result.structuredContent as Record<string, unknown> | undefined;
  if (sc?.vega_spec) return sc.vega_spec as Record<string, unknown>;
  // Then parse from text content
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

// Create app and register handlers before connecting
const fullscreenBtn = document.getElementById("fullscreen-btn")!;
let currentDisplayMode: "inline" | "fullscreen" = "inline";

const app = new App(
  { name: "sidemantic-chart", version: "1.0.0" },
  { availableDisplayModes: ["inline", "fullscreen"] },
  { autoResize: false },
);

app.ontoolresult = (result: CallToolResult) => {
  const spec = extractVegaSpec(result);
  if (spec) {
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
  if (ctx.availableDisplayModes) {
    const canFullscreen = ctx.availableDisplayModes.includes("fullscreen");
    fullscreenBtn.classList.toggle("available", canFullscreen);
  }
  if (ctx.displayMode === "inline" || ctx.displayMode === "fullscreen") {
    currentDisplayMode = ctx.displayMode;
    document.body.classList.toggle("fullscreen", currentDisplayMode === "fullscreen");
    fullscreenBtn.style.display = currentDisplayMode === "fullscreen" ? "none" : "";
  }
};

fullscreenBtn.addEventListener("click", async () => {
  const newMode = currentDisplayMode === "fullscreen" ? "inline" : "fullscreen";
  const ctx = app.getHostContext();
  if (ctx?.availableDisplayModes?.includes(newMode)) {
    const result = await app.requestDisplayMode({ mode: newMode });
    currentDisplayMode = result.mode as "inline" | "fullscreen";
    document.body.classList.toggle("fullscreen", currentDisplayMode === "fullscreen");
    fullscreenBtn.style.display = currentDisplayMode === "fullscreen" ? "none" : "";
  }
});

app.connect().then(() => {
  const ctx = app.getHostContext();
  if (ctx?.theme) applyDocumentTheme(ctx.theme);
  if (ctx?.availableDisplayModes?.includes("fullscreen")) {
    fullscreenBtn.classList.add("available");
  }
  // Keep fullscreen button, replace only the chart content area
  const loading = container.querySelector(".loading");
  if (loading) loading.textContent = "Waiting for chart data...";
  app.sendSizeChanged({ height: 500 });
});
