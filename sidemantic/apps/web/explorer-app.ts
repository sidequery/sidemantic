import { App, applyDocumentTheme, type McpUiHostContext } from "@modelcontextprotocol/ext-apps";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
// @ts-ignore - pre-built anywidget module
import widgetModule from "../../widget/static/widget.js";

const container = document.getElementById("explorer")!;
let currentDisplayMode: "inline" | "fullscreen" = "inline";

// Resolve render function: default export is { render: md } but bundler
// may expose it as widgetModule.render or widgetModule.default.render.
const renderWidget: (ctx: { model: WidgetModel; el: HTMLElement }) => (() => void) | void =
  widgetModule.render || (widgetModule as any).default?.render;

// The WidgetModel adapter bridges anywidget's model interface to MCP App tool calls.
// When widget.js calls model.set() + model.save_changes(), we determine what data
// needs refreshing and call the widget_query tool on the MCP server.

class WidgetModel {
  private state: Record<string, any> = {};
  private listeners: Map<string, Set<Function>> = new Map();
  private pendingChanges: Set<string> = new Set();
  private app: App;

  constructor(app: App) {
    this.app = app;
  }

  get(field: string): any {
    return this.state[field];
  }

  set(field: string, value: any): void {
    this.state[field] = value;
    this.pendingChanges.add(field);
  }

  save_changes(): void {
    const changed = new Set(this.pendingChanges);
    this.pendingChanges.clear();

    // Determine what to refresh based on what changed.
    // These mirror the Python widget's observer logic:
    // - filters -> all (or dimensions if active_dimension set)
    // - brush_selection -> all
    // - selected_metric -> dimensions
    // - time_grain -> metrics
    // - active_dimension -> special handling
    //
    // active_dimension is set briefly during filter changes, then cleared
    // after 400ms. When it's set, only refresh that dimension. When cleared,
    // full refresh. We handle this by checking if active_dimension was just
    // set (don't query yet) or if it was just cleared or not involved
    // (query based on other changes).

    if (changed.has("active_dimension")) {
      const ad = this.state.active_dimension;
      if (ad) {
        // Just set: don't query yet, widget will clear it in 400ms
        return;
      }
      // Was cleared: do a full refresh
      this.callRefresh("all");
      return;
    }

    if (changed.has("filters")) {
      const ad = this.state.active_dimension;
      if (ad) {
        this.callRefresh("dimensions");
      } else {
        this.callRefresh("all");
      }
      return;
    }

    if (changed.has("brush_selection")) {
      this.callRefresh("all");
      return;
    }

    if (changed.has("selected_metric")) {
      this.callRefresh("dimensions");
      return;
    }

    if (changed.has("time_grain")) {
      this.callRefresh("metrics");
      return;
    }
  }

  on(event: string, callback: Function): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(callback);
  }

  off(event: string, callback?: Function): void {
    if (!callback) {
      this.listeners.delete(event);
    } else {
      this.listeners.get(event)?.delete(callback);
    }
  }

  // Fire change event for a field
  private fireChange(field: string): void {
    const event = `change:${field}`;
    this.listeners.get(event)?.forEach((cb) => cb());
  }

  // Apply data from tool result, updating state and firing change events
  applyData(data: Record<string, any>): void {
    for (const [key, value] of Object.entries(data)) {
      if (value !== undefined) {
        this.state[key] = value;
        this.fireChange(key);
      }
    }
  }

  // Extract data dict from CallToolResult
  extractData(result: CallToolResult): Record<string, any> | null {
    // Check structuredContent first
    const sc = result.structuredContent as Record<string, any> | undefined;
    if (sc) return sc;

    // Fall back to text content
    if (result.content) {
      for (const item of result.content) {
        if (item.type === "text") {
          try {
            return JSON.parse((item as { text: string }).text);
          } catch {
            // not JSON, skip
          }
        }
      }
    }
    return null;
  }

  // Call the widget_query tool on the MCP server
  private async callRefresh(queryType: string): Promise<void> {
    // Show loading state
    this.state.status = "loading";
    this.fireChange("status");

    try {
      const result = await this.app.callServerTool({
        name: "widget_query",
        arguments: {
          query_type: queryType,
          selected_metric: this.state.selected_metric || "",
          time_grain: this.state.time_grain || "day",
          filters_json: JSON.stringify(this.state.filters || {}),
          brush_selection_json: JSON.stringify(this.state.brush_selection || []),
          active_dimension: this.state.active_dimension || "",
        },
      });

      // Extract data from tool result
      const data = this.extractData(result);
      if (data) {
        this.applyData(data);
      }
    } catch (err) {
      this.state.status = "error";
      this.state.error = err instanceof Error ? err.message : String(err);
      this.fireChange("status");
      this.fireChange("error");
    }
  }
}

// --- App setup ---

const app = new App(
  { name: "sidemantic-explorer", version: "1.0.0" },
  {},
  { autoResize: false },
);

const model = new WidgetModel(app);
let cleanup: (() => void) | null = null;

function renderExplorer(): void {
  if (cleanup) {
    cleanup();
    cleanup = null;
  }
  container.innerHTML = "";

  const isFullscreen = currentDisplayMode === "fullscreen";
  document.documentElement.classList.toggle("fullscreen", isFullscreen);

  // Create widget container
  const widgetEl = document.createElement("div");
  container.appendChild(widgetEl);

  // Add expand button in inline mode
  if (!isFullscreen) {
    const btn = document.createElement("div");
    btn.className = "expand-btn";
    btn.title = "Expand to fullscreen";
    btn.textContent = "Expand \u2197";
    btn.addEventListener("click", async () => {
      try {
        const result = await app.requestDisplayMode({ mode: "fullscreen" });
        currentDisplayMode = result.mode as "inline" | "fullscreen";
        renderExplorer();
      } catch {
        // host doesn't support fullscreen
      }
    });
    container.appendChild(btn);
  }

  // Render the anywidget
  const result = renderWidget({ model, el: widgetEl });
  if (typeof result === "function") {
    cleanup = result;
  }

  // Report size
  requestAnimationFrame(() => {
    if (isFullscreen) {
      app.sendSizeChanged({ height: window.innerHeight - 150 });
    } else {
      const h = Math.max(605, document.documentElement.scrollHeight + 5);
      app.sendSizeChanged({ height: h });
    }
  });
}

// Handle initial tool result (from explore_metrics)
app.ontoolresult = (result: CallToolResult) => {
  const data = model.extractData(result);
  if (data) {
    model.applyData(data);
    renderExplorer();
  } else {
    container.innerHTML = '<div class="error">No explorer data in tool result</div>';
  }
};

app.ontoolinput = () => {
  container.innerHTML = '<div class="loading">Loading explorer...</div>';
};

app.onhostcontextchanged = (ctx: McpUiHostContext) => {
  if (ctx.theme) applyDocumentTheme(ctx.theme);
  if (ctx.displayMode === "inline" || ctx.displayMode === "fullscreen") {
    currentDisplayMode = ctx.displayMode;
    if (model.get("status") === "ready") {
      renderExplorer();
    }
  }
};

app.connect().then(() => {
  const ctx = app.getHostContext();
  if (ctx?.theme) applyDocumentTheme(ctx.theme);
  const loading = container.querySelector(".loading");
  if (loading) loading.textContent = "Waiting for data...";
  app.sendSizeChanged({ height: 600 });
});
