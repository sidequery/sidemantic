/**
 * Sidemantic MetricsExplorer anywidget
 *
 * Ported from the sidemantic-demo Pyodide implementation.
 * Renders metric cards with sparklines and dimension leaderboards.
 */

import { tableFromIPC } from "apache-arrow";
import {
  formatValue as formatUiValue,
  removeFilterValue,
  renderDimensionLeaderboardCards as mountDimensionLeaderboards,
  renderFilterPills as mountFilterPills,
  renderMetricSummaryCards as mountMetricCards,
  toggleFilterValue,
} from "../plugins/sidemantic/skills/webapp-builder/assets/ui-dist/sidemantic-ui-static.js";

// ============================================================================
// Utility Functions
// ============================================================================

function formatNumber(value, format = "number") {
  if (value == null) return "—";
  if (typeof value === "bigint") {
    return value.toLocaleString("en-US");
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^-?\d+$/.test(trimmed)) {
      try {
        return BigInt(trimmed).toLocaleString("en-US");
      } catch {}
    }
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed)) return "—";
    return formatUiValue(parsed, { format });
  }
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return "—";
  return formatUiValue(numericValue, { format });
}

function formatDate(date) {
  if (!date) return "";
  if (typeof date === "string") return date.slice(0, 10);
  // Handle Arrow date values (milliseconds since epoch or Date objects)
  if (date instanceof Date) {
    return date.toISOString().slice(0, 10);
  }
  // Arrow returns dates as numbers (days, seconds, or milliseconds)
  if (typeof date === "number" || typeof date === "bigint") {
    const ms = typeof date === "bigint" ? Number(date) : date;
    if (ms > 1e12) {
      return new Date(ms).toISOString().slice(0, 10);
    }
    if (ms > 1e9) {
      return new Date(ms * 1000).toISOString().slice(0, 10);
    }
    return new Date(ms * 86400000).toISOString().slice(0, 10);
  }
  // Fallback: try to convert to string
  return String(date).slice(0, 10);
}

function parseDateValue(value) {
  if (!value) return null;
  if (value instanceof Date) return value;
  if (typeof value === "number" || typeof value === "bigint") {
    const ms = typeof value === "bigint" ? Number(value) : value;
    if (ms > 1e12) return new Date(ms);
    if (ms > 1e9) return new Date(ms * 1000);
    return new Date(ms * 86400000);
  }
  if (typeof value === "string") {
    const match = value.match(/^\d{4}-\d{2}-\d{2}/);
    if (match) return new Date(`${match[0]}T00:00:00Z`);
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  return null;
}

function formatDateLabel(date, options) {
  return date.toLocaleDateString("en-US", { timeZone: "UTC", ...options });
}

function formatShortDate(dateValue) {
  const date = parseDateValue(dateValue);
  if (!date || Number.isNaN(date.getTime())) return formatDate(dateValue);
  return formatDateLabel(date, { month: "short", day: "numeric" });
}

function formatRangeShort(start, end) {
  const startDate = parseDateValue(start);
  const endDate = parseDateValue(end);
  if (!startDate || !endDate) {
    const startLabel = formatDate(start);
    const endLabel = formatDate(end);
    if (!startLabel && !endLabel) return "";
    if (startLabel && endLabel) return `${startLabel} – ${endLabel}`;
    return startLabel || endLabel;
  }
  const sameYear = startDate.getFullYear() === endDate.getFullYear();
  const startLabel = formatDateLabel(startDate, {
    month: "short",
    day: "numeric",
    year: sameYear ? undefined : "numeric",
  });
  const endLabel = formatDateLabel(endDate, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
  return `${startLabel} – ${endLabel}`;
}

function formatComparison(value) {
  if (!Number.isFinite(value)) {
    return { text: "WoW —", className: "metric-delta" };
  }
  const sign = value > 0 ? "+" : "";
  const className =
    value > 0
      ? "metric-delta positive"
      : value < 0
        ? "metric-delta negative"
        : "metric-delta";
  return { text: `WoW ${sign}${value.toFixed(1)}%`, className };
}

function daysBetween(start, end) {
  const msPerDay = 24 * 60 * 60 * 1000;
  const startDate = parseDateValue(start);
  const endDate = parseDateValue(end);
  if (!startDate || !endDate) return 1;
  return Math.max(1, Math.round((endDate - startDate) / msPerDay) + 1);
}

// ============================================================================
// Arrow IPC Parsing Helper
// ============================================================================

function base64ToUint8(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function parseArrowIPC(dataRaw) {
  if (!dataRaw) return null;
  if (typeof dataRaw === "string" && dataRaw.length === 0) return null;
  if (dataRaw.byteLength !== undefined && dataRaw.byteLength === 0) return null;

  let data;
  if (typeof dataRaw === "string") {
    data = base64ToUint8(dataRaw);
  } else if (dataRaw instanceof DataView) {
    data = new Uint8Array(
      dataRaw.buffer.slice(
        dataRaw.byteOffset,
        dataRaw.byteOffset + dataRaw.byteLength
      )
    );
  } else if (dataRaw instanceof Uint8Array) {
    data = dataRaw;
  } else if (dataRaw instanceof ArrayBuffer) {
    data = new Uint8Array(dataRaw);
  } else {
    console.error("Unknown data type:", dataRaw);
    return null;
  }

  const table = tableFromIPC(data);
  const rows = [];
  for (let i = 0; i < table.numRows; i++) {
    const row = {};
    for (const field of table.schema.fields) {
      row[field.name] = table.getChild(field.name)?.get(i);
    }
    rows.push(row);
  }
  return rows;
}

// ============================================================================
// SVG Sparkline
// ============================================================================

function render({ model, el }) {
  // Create widget structure
  el.innerHTML = `
    <div class="sidemantic-widget">
      <div class="widget-header">
        <div class="filter-scroll">
          <div class="filter-pills"></div>
        </div>
        <div class="controls">
          <div class="select-wrap">
            <select class="metric-select"></select>
          </div>
          <div class="select-wrap">
            <select class="grain-select"></select>
          </div>
        </div>
      </div>
      <div class="widget-layout">
        <div class="metrics-col"></div>
        <div class="dimensions-col">
          <div class="dimensions-grid"></div>
        </div>
      </div>
      <div class="widget-status"></div>
    </div>
  `;

  const filterPillsEl = el.querySelector(".filter-pills");
  const metricsColEl = el.querySelector(".metrics-col");
  const dimensionsGridEl = el.querySelector(".dimensions-grid");
  const metricSelectEl = el.querySelector(".metric-select");
  const grainSelectEl = el.querySelector(".grain-select");
  const statusEl = el.querySelector(".widget-status");
  let activeDimTimeout = null;

  // -------------------------------------------------------------------------
  // State Management
  // -------------------------------------------------------------------------

  function getFilters() {
    return model.get("filters") || {};
  }

  function setFilters(filters) {
    model.set("filters", filters);
    model.save_changes();
  }

  function scheduleActiveDimensionClear() {
    if (activeDimTimeout) {
      clearTimeout(activeDimTimeout);
    }
    activeDimTimeout = setTimeout(() => {
      model.set("active_dimension", "");
      model.save_changes();
    }, 400);
  }

  function clearActiveDimension() {
    if (activeDimTimeout) {
      clearTimeout(activeDimTimeout);
      activeDimTimeout = null;
    }
    model.set("active_dimension", "");
  }

  function toggleFilter(dimKey, value) {
    model.set("active_dimension", dimKey);
    setFilters(toggleFilterValue(getFilters(), dimKey, value));
    scheduleActiveDimensionClear();
  }

  function removeFilter(dimKey, value) {
    clearActiveDimension();
    setFilters(removeFilterValue(getFilters(), dimKey, value));
  }

  function setBrush(start, end) {
    if (start && end) {
      clearActiveDimension();
      model.set("brush_selection", [start, end]);
    } else {
      model.set("brush_selection", []);
    }
    model.save_changes();
  }

  function clearBrush() {
    clearActiveDimension();
    model.set("brush_selection", []);
    model.save_changes();
  }

  // -------------------------------------------------------------------------
  // Rendering
  // -------------------------------------------------------------------------

  function renderMetrics() {
    const metricsConfig = model.get("metrics_config") || [];
    const selectedMetric = model.get("selected_metric") || "";
    const dateRange = model.get("date_range") || [];
    const brushSelection = model.get("brush_selection") || [];
    const activeRange =
      brushSelection && brushSelection.length === 2 ? brushSelection : dateRange;
    const metricTotals = model.get("metric_totals") || {};
    const transport = model.get("transport") || "base64";
    const metricSeriesDataRaw =
      transport === "binary"
        ? model.get("metric_series_data_binary")
        : model.get("metric_series_data");

    // Show skeletons while loading
    if (
      !metricSeriesDataRaw ||
      (metricSeriesDataRaw.byteLength !== undefined &&
        metricSeriesDataRaw.byteLength === 0)
    ) {
      metricsColEl.innerHTML = "";
      metricsConfig.forEach(() => {
        metricsColEl.appendChild(renderMetricSkeleton());
      });
      return;
    }

    try {
      const rows = parseArrowIPC(metricSeriesDataRaw);
      if (!rows) {
        metricsColEl.innerHTML = '<div class="error">Failed to parse data</div>';
        return;
      }

      // Extract dates from rows (assuming first column or __time column)
      const config = model.get("config") || {};
      const preferredTimeCol = config.time_series_column;
      const timeCol =
        preferredTimeCol ||
        Object.keys(rows[0] || {}).find(
          (k) => k.includes("time") || k.includes("date") || k === "__time"
        );
      const dates = timeCol ? rows.map((r) => formatDate(r[timeCol])) : [];

      mountMetricCards(metricsColEl, {
        metrics: metricsConfig,
        totals: { rows: [metricTotals] },
        seriesRows: rows,
        timeKey: timeCol,
        selectedMetric,
        rangeLabel: activeRange?.length === 2 ? formatRangeShort(activeRange[0], activeRange[1]) : "",
        valueFormat: (key) => ({ format: metricsConfig.find((metric) => metric.key === key)?.format }),
        onSelect: ({ metric }) => {
          model.set("selected_metric", metric);
          model.save_changes();
        },
        onBrush: setBrush,
      });
    } catch (e) {
      metricsColEl.innerHTML = `<div class="error">Error: ${e.message}</div>`;
    }
  }

  function renderDimensions() {
    const dimensionsConfig = model.get("dimensions_config") || [];
    const transport = model.get("transport") || "base64";
    const dimensionData =
      transport === "binary"
        ? model.get("dimension_data_binary") || {}
        : model.get("dimension_data") || {};
    const selectedMetric = model.get("selected_metric") || "";
    const filters = getFilters();

    const results = new Map();
    dimensionsConfig.forEach((dimConfig) => {
      const ipcDataView = dimensionData[dimConfig.key];

      // Show skeleton while loading
      if (
        !ipcDataView ||
        (typeof ipcDataView === "string"
          ? ipcDataView.length === 0
          : ipcDataView.byteLength === 0)
      ) {
        results.set(dimConfig.key, []);
        return;
      }

      try {
        const rows = parseArrowIPC(ipcDataView);
        if (!rows) {
          results.set(dimConfig.key, []);
          return;
        }
        results.set(dimConfig.key, rows);
      } catch (e) {
        results.set(dimConfig.key, []);
      }
    });
    mountDimensionLeaderboards(dimensionsGridEl, dimensionsConfig, {
      metricRef: selectedMetric,
      resultForDimension: (dimension) => ({ rows: results.get(dimension.key) || [] }),
      selectedValuesForDimension: (dimension) => filters[dimension.key] || [],
      interactive: true,
      expandable: true,
      onSelect: ({ dimension, value }) => toggleFilter(dimension, value),
    });
  }

  function renderFilters() {
    const filters = getFilters();
    const dateRange = model.get("date_range") || [];
    const brushSelection = model.get("brush_selection") || [];

    mountFilterPills(filterPillsEl, filters, ({ dimension, value }) => removeFilter(dimension, value), { emptyLabel: "No filters" });
  }

  function renderMetricSelect() {
    const metricsConfig = model.get("metrics_config") || [];
    const selectedMetric = model.get("selected_metric") || "";

    metricSelectEl.innerHTML = metricsConfig
      .map(
        (m) =>
          `<option value="${m.key}"${m.key === selectedMetric ? " selected" : ""}>${m.label}</option>`
      )
      .join("");
  }

  function renderGrainSelect() {
    const grainOptions = model.get("time_grain_options") || [];
    const selectedGrain = model.get("time_grain") || "";

    if (!grainOptions.length) {
      grainSelectEl.innerHTML = "";
      grainSelectEl.disabled = true;
      return;
    }

    grainSelectEl.disabled = false;
    grainSelectEl.innerHTML = grainOptions
      .map((grain) => {
        const label = grain.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
        return `<option value="${grain}"${grain === selectedGrain ? " selected" : ""}>${label}</option>`;
      })
      .join("");
  }

  function renderStatus() {
    const status = model.get("status") || "loading";
    const error = model.get("error") || "";

    if (status === "error" && error) {
      statusEl.textContent = `Error: ${error}`;
      statusEl.className = "widget-status error";
    } else if (status === "loading") {
      statusEl.textContent = "Loading...";
      statusEl.className = "widget-status loading";
    } else {
      statusEl.textContent = "";
      statusEl.className = "widget-status";
    }
  }

  function renderAll() {
    renderStatus();
    renderFilters();
    renderMetricSelect();
    renderGrainSelect();
    renderMetrics();
    renderDimensions();
  }

  // -------------------------------------------------------------------------
  // Event Handlers
  // -------------------------------------------------------------------------

  metricSelectEl.addEventListener("change", (e) => {
    clearActiveDimension();
    model.set("selected_metric", e.target.value);
    model.save_changes();
  });
  grainSelectEl.addEventListener("change", (e) => {
    model.set("time_grain", e.target.value);
    model.save_changes();
  });

  // -------------------------------------------------------------------------
  // Model Change Observers
  // -------------------------------------------------------------------------

  model.on("change:metric_series_data", renderMetrics);
  model.on("change:metric_totals", renderMetrics);
  model.on("change:dimension_data", renderDimensions);
  model.on("change:filters", () => {
    renderFilters();
    renderDimensions(); // Update active states
  });
  model.on("change:brush_selection", renderFilters);
  model.on("change:selected_metric", () => {
    renderMetricSelect();
    renderMetrics();
  });
  model.on("change:time_grain", renderGrainSelect);
  model.on("change:status", renderStatus);
  model.on("change:error", renderStatus);
  model.on("change:metrics_config", renderAll);
  model.on("change:dimensions_config", renderAll);
  model.on("change:time_grain_options", renderGrainSelect);

  // Initial render
  renderAll();

  // Cleanup
  return () => {
    model.off("change:metric_series_data", renderMetrics);
    model.off("change:metric_totals", renderMetrics);
    model.off("change:dimension_data", renderDimensions);
    model.off("change:filters");
    model.off("change:brush_selection", renderFilters);
    model.off("change:selected_metric");
    model.off("change:time_grain", renderGrainSelect);
    model.off("change:status", renderStatus);
    model.off("change:error", renderStatus);
    model.off("change:metrics_config", renderAll);
    model.off("change:dimensions_config", renderAll);
    model.off("change:time_grain_options", renderGrainSelect);
  };
}

export default { render };
