import { App, applyDocumentTheme, type McpUiHostContext } from "@modelcontextprotocol/ext-apps";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import embed from "vega-embed";
import { expressionInterpreter } from "vega-interpreter";

const container = document.getElementById("chart")!;
let currentDisplayMode: "inline" | "fullscreen" = "inline";
let lastSpec: Record<string, unknown> | null = null;
let activeObserver: ResizeObserver | null = null;
let activeView: { finalize: () => void } | null = null;
let renderGeneration = 0;

function cleanupChart() {
  if (activeObserver) {
    activeObserver.disconnect();
    activeObserver = null;
  }
  if (activeView) {
    activeView.finalize();
    activeView = null;
  }
}

function renderChart(vegaSpec: Record<string, unknown>) {
  cleanupChart();
  const generation = ++renderGeneration;

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
      if (generation !== renderGeneration) {
        result.finalize();
        return;
      }

      activeView = result;
      const observer = new ResizeObserver(() => result.view.resize().run());
      observer.observe(container);
      activeObserver = observer;

      if (!isFullscreen) {
        addExpandButton();
      }

      requestAnimationFrame(() => {
        if (generation !== renderGeneration) return;
        if (isFullscreen) {
          app.sendSizeChanged({ height: window.innerHeight - 150 });
        } else {
          const height = Math.max(505, document.documentElement.scrollHeight + 5);
          app.sendSizeChanged({ height });
        }
      });
    })
    .catch((error: Error) => {
      if (generation !== renderGeneration) return;
      lastSpec = null;
      container.innerHTML = `<div class="error">Chart render error: ${error.message}</div>`;
    });
}

function addExpandButton() {
  const button = document.createElement("div");
  button.className = "expand-btn";
  button.title = "Expand to fullscreen";
  button.textContent = "Expand ↗";
  button.addEventListener("click", goFullscreen);
  container.appendChild(button);
}

async function goFullscreen() {
  try {
    const result = await app.requestDisplayMode({ mode: "fullscreen" });
    currentDisplayMode = result.mode as "inline" | "fullscreen";
    if (lastSpec) renderChart(lastSpec);
  } catch {
    // The host does not support fullscreen.
  }
}

function extractVegaSpec(result: CallToolResult): Record<string, unknown> | null {
  const structuredContent = result.structuredContent as Record<string, unknown> | undefined;
  if (structuredContent?.vega_spec) {
    return structuredContent.vega_spec as Record<string, unknown>;
  }
  if (result.content) {
    for (const item of result.content) {
      if (item.type === "text") {
        try {
          const data = JSON.parse((item as { text: string }).text);
          if (data.vega_spec) return data.vega_spec;
        } catch {
          // Ignore non-JSON text content.
        }
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
    cleanupChart();
    lastSpec = null;
    container.innerHTML = '<div class="error">No chart data in tool result</div>';
  }
};

app.ontoolinput = () => {
  cleanupChart();
  lastSpec = null;
  ++renderGeneration;
  container.innerHTML = '<div class="loading">Running query...</div>';
};

app.onhostcontextchanged = (context: McpUiHostContext) => {
  if (context.theme) applyDocumentTheme(context.theme);
  if (context.displayMode === "inline" || context.displayMode === "fullscreen") {
    currentDisplayMode = context.displayMode;
    if (lastSpec) renderChart(lastSpec);
  }
};

app.connect().then(() => {
  const context = app.getHostContext();
  if (context?.theme) applyDocumentTheme(context.theme);
  const loading = container.querySelector(".loading");
  if (loading) loading.textContent = "Waiting for chart data...";
  app.sendSizeChanged({ height: 500 });
});
