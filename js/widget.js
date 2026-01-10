/**
 * Sidemantic MetricsExplorer anywidget
 *
 * Ported from the sidemantic-demo Pyodide implementation.
 * Renders metric cards with sparklines and dimension leaderboards.
 */

import { tableFromIPC } from "apache-arrow";

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
    if (format === "currency") {
      return `$${parsed.toFixed(2)}`;
    }
    return parsed.toLocaleString();
  }
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return "—";
  if (format === "currency") {
    return `$${numericValue.toFixed(2)}`;
  }
  return numericValue.toLocaleString();
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

function sparklineSvg(values) {
  if (!values || !values.length) return "";

  // Use higher resolution viewBox for smoother rendering
  const height = 60;
  const width = values.length > 1 ? values.length - 1 : 1;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  // One point per data value for pixel-perfect alignment
  const points = values.map((value, idx) => {
    const x = idx;
    const y = height - ((value - min) / range) * height;
    return [x.toFixed(2), y.toFixed(2)];
  });

  const line = points.map(([x, y]) => `${x},${y}`).join(" ");
  const area = `0,${height} ${line} ${width},${height}`;

  return `
    <svg class="sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      <polygon points="${area}" fill="var(--chart-fill)"></polygon>
      <polyline
        points="${line}"
        fill="none"
        stroke="var(--chart)"
        stroke-width="1.5"
        stroke-linecap="round"
        stroke-linejoin="round"
        vector-effect="non-scaling-stroke"
      ></polyline>
    </svg>
  `;
}

// ============================================================================
// Skeleton Components
// ============================================================================

function renderMetricSkeleton() {
  const skeleton = document.createElement("div");
  skeleton.className = "metric-skeleton";
  skeleton.innerHTML = `
    <div class="skeleton-header">
      <div class="skeleton-title-group">
        <div class="skeleton-line skeleton-title"></div>
        <div class="skeleton-line skeleton-subtitle"></div>
      </div>
      <div class="skeleton-value-group">
        <div class="skeleton-line skeleton-value"></div>
        <div class="skeleton-line skeleton-delta"></div>
      </div>
    </div>
    <div class="skeleton-line skeleton-chart"></div>
  `;
  return skeleton;
}

function renderDimensionSkeleton(label) {
  const skeleton = document.createElement("div");
  skeleton.className = "skeleton-card";
  skeleton.innerHTML = `
    <div class="skeleton-line skeleton-title"></div>
    ${Array(6)
      .fill(0)
      .map(
        () => `
      <div class="skeleton-row-wrap">
        <div class="skeleton-line skeleton-row"></div>
      </div>
    `
      )
      .join("")}
  `;
  return skeleton;
}

// ============================================================================
// Metric Card Component
// ============================================================================

function renderMetricCard(
  metricConfig,
  series,
  total,
  dateRange,
  selectedMetric,
  dates,
  onHover,
  onBrush
) {
  const rangeLabel =
    dateRange && dateRange.length === 2
      ? formatRangeShort(dateRange[0], dateRange[1])
      : "";

  const isActive = metricConfig.key === selectedMetric;
  const formattedValue = formatNumber(total, metricConfig.format);

  const card = document.createElement("div");
  card.className = `metric-card${isActive ? " active" : ""}`;
  card.dataset.metric = metricConfig.key;

  card.innerHTML = `
    <div class="metric-header">
      <div class="metric-title">
        <h3>${metricConfig.label}</h3>
        <span class="metric-range">${rangeLabel}</span>
      </div>
      <div class="metric-value-group">
        <div class="metric-value">${formattedValue}</div>
      </div>
    </div>
    <div class="sparkline-wrap" data-metric="${metricConfig.key}">
      ${sparklineSvg(series)}
      <div class="hover-line"></div>
      <div class="brush-selection"></div>
    </div>
  `;

  // Sparkline hover and brush interactions
  const sparklineWrap = card.querySelector(".sparkline-wrap");
  const hoverLine = card.querySelector(".hover-line");
  const brushSelection = card.querySelector(".brush-selection");
  const metricRangeEl = card.querySelector(".metric-range");
  const metricValueEl = card.querySelector(".metric-value");

  let brushState = null;
  let originalRange = rangeLabel;
  let originalValue = formattedValue;

  // Hover: show vertical line and update value
  sparklineWrap.addEventListener("pointermove", (e) => {
    if (brushState) return; // Don't show hover during brush

    const rect = sparklineWrap.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const pct = x / rect.width;
    const idx = Math.max(0, Math.min(series.length - 1, Math.round(pct * (series.length - 1))));

    hoverLine.style.left = `${pct * 100}%`;
    hoverLine.style.opacity = "1";

    // Update displayed value and date
    if (dates && dates[idx]) {
      metricRangeEl.textContent = formatShortDate(dates[idx]);
    }
    metricValueEl.textContent = formatNumber(series[idx], metricConfig.format);
  });

  sparklineWrap.addEventListener("pointerleave", () => {
    if (brushState) return;
    hoverLine.style.opacity = "0";
    metricRangeEl.textContent = originalRange;
    metricValueEl.textContent = originalValue;
  });

  // Brush: click-drag to select date range
  sparklineWrap.addEventListener("pointerdown", (e) => {
    sparklineWrap.setPointerCapture(e.pointerId);
    const rect = sparklineWrap.getBoundingClientRect();
    const startX = e.clientX - rect.left;
    brushState = { startX, rect };

    brushSelection.style.left = `${(startX / rect.width) * 100}%`;
    brushSelection.style.width = "0";
    brushSelection.style.display = "block";
    hoverLine.style.opacity = "0";
  });

  sparklineWrap.addEventListener("pointermove", (e) => {
    if (!brushState) return;

    const { startX, rect } = brushState;
    const currentX = e.clientX - rect.left;
    const left = Math.min(startX, currentX);
    const width = Math.abs(currentX - startX);

    brushSelection.style.left = `${(left / rect.width) * 100}%`;
    brushSelection.style.width = `${(width / rect.width) * 100}%`;
  });

  sparklineWrap.addEventListener("pointerup", (e) => {
    if (!brushState) return;

    const { startX, rect } = brushState;
    const endX = e.clientX - rect.left;
    const width = rect.width;

    sparklineWrap.releasePointerCapture(e.pointerId);

    // Only trigger brush if dragged more than 6px
    if (Math.abs(endX - startX) > 6 && dates && dates.length > 0) {
      const startPct = Math.max(0, Math.min(1, startX / width));
      const endPct = Math.max(0, Math.min(1, endX / width));

      const startIdx = Math.round(startPct * (dates.length - 1));
      const endIdx = Math.round(endPct * (dates.length - 1));

      const minIdx = Math.min(startIdx, endIdx);
      const maxIdx = Math.max(startIdx, endIdx);

      if (onBrush) {
        onBrush(dates[minIdx], dates[maxIdx]);
      }
    }

    brushState = null;
    brushSelection.style.display = "none";
  });

  // Double-click to clear brush
  sparklineWrap.addEventListener("dblclick", () => {
    if (onBrush) {
      onBrush(null, null);
    }
  });

  return card;
}

// ============================================================================
// Dimension Leaderboard Component
// ============================================================================

function renderDimensionCard(
  dimConfig,
  rows,
  selectedMetric,
  activeFilters,
  onFilterClick
) {
  const maxValue = Math.max(
    ...rows.map((row) => Number(row[selectedMetric]) || 0),
    1
  );

  const card = document.createElement("div");
  card.className = "dim-card";
  card.dataset.dim = dimConfig.key;

  card.innerHTML = `<h4>${dimConfig.label}</h4>`;

  rows.forEach((row) => {
    const value = row[dimConfig.key];
    const metricValue = Number(row[selectedMetric]) || 0;
    const width = Math.round((metricValue / maxValue) * 100);

    const rowEl = document.createElement("div");
    const isSelected =
      activeFilters[dimConfig.key] &&
      activeFilters[dimConfig.key].includes(String(value));
    rowEl.className = `dim-row${isSelected ? " active" : ""}`;
    rowEl.dataset.value = String(value);

    rowEl.innerHTML = `
      <div class="bar" style="transform: scaleX(${width / 100});"></div>
      <span>${value}</span>
      <span class="dim-value">${formatNumber(metricValue)}</span>
    `;

    rowEl.addEventListener("click", () => {
      onFilterClick(dimConfig.key, String(value));
    });

    card.appendChild(rowEl);
  });

  return card;
}

// ============================================================================
// Filter Pills Component
// ============================================================================

function renderFilterPills(
  container,
  filters,
  dateRange,
  brushSelection,
  onRemoveFilter,
  onClearBrush
) {
  container.innerHTML = "";

  let hasFilter = false;

  // Brush selection pill
  if (brushSelection && brushSelection.length === 2 && brushSelection[0] && brushSelection[1]) {
    hasFilter = true;
    const pill = document.createElement("span");
    pill.className = "pill";
    pill.textContent = `Date: ${formatDate(brushSelection[0])} → ${formatDate(brushSelection[1])} ×`;
    pill.addEventListener("click", onClearBrush);
    container.appendChild(pill);
  }

  // Dimension filter pills
  Object.entries(filters).forEach(([dimKey, values]) => {
    if (values && values.length > 0) {
      values.forEach((value) => {
        hasFilter = true;
        const pill = document.createElement("span");
        pill.className = "pill";
        pill.textContent = `${dimKey}: ${value} ×`;
        pill.addEventListener("click", () => onRemoveFilter(dimKey, value));
        container.appendChild(pill);
      });
    }
  });

  if (!hasFilter) {
    const pill = document.createElement("span");
    pill.className = "pill muted";
    pill.textContent = "No filters";
    container.appendChild(pill);
  }
}

// ============================================================================
// Main Widget Render
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
    const filters = { ...getFilters() };
    const current = filters[dimKey] || [];
    model.set("active_dimension", dimKey);

    if (current.includes(value)) {
      filters[dimKey] = current.filter((v) => v !== value);
      if (filters[dimKey].length === 0) {
        delete filters[dimKey];
      }
    } else {
      filters[dimKey] = [...current, value];
    }

    setFilters(filters);
    scheduleActiveDimensionClear();
  }

  function removeFilter(dimKey, value) {
    const filters = { ...getFilters() };
    const current = filters[dimKey] || [];
    clearActiveDimension();
    filters[dimKey] = current.filter((v) => v !== value);
    if (filters[dimKey].length === 0) {
      delete filters[dimKey];
    }
    setFilters(filters);
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
    const metricSeriesDataRaw = model.get("metric_series_data");

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
      const timeCol = Object.keys(rows[0] || {}).find(
        (k) => k.includes("time") || k.includes("date") || k === "__time"
      );
      const dates = timeCol ? rows.map((r) => formatDate(r[timeCol])) : [];

      metricsColEl.innerHTML = "";

      metricsConfig.forEach((metricConfig) => {
        const series = rows.map((row) => Number(row[metricConfig.key]) || 0);
        const total =
          metricTotals[metricConfig.key] ??
          series.reduce((sum, v) => sum + v, 0);

        const card = renderMetricCard(
          metricConfig,
          series,
          total,
          activeRange,
          selectedMetric,
          dates,
          null, // onHover
          setBrush // onBrush
        );

        // Click to select metric
        card.addEventListener("click", (e) => {
          // Don't select on sparkline interactions
          if (e.target.closest(".sparkline-wrap")) return;
          model.set("selected_metric", metricConfig.key);
          model.save_changes();
        });

        metricsColEl.appendChild(card);
      });
    } catch (e) {
      metricsColEl.innerHTML = `<div class="error">Error: ${e.message}</div>`;
    }
  }

  function renderDimensions() {
    const dimensionsConfig = model.get("dimensions_config") || [];
    const dimensionData = model.get("dimension_data") || {};
    const selectedMetric = model.get("selected_metric") || "";
    const filters = getFilters();

    dimensionsGridEl.innerHTML = "";

    dimensionsConfig.forEach((dimConfig) => {
      const ipcDataView = dimensionData[dimConfig.key];

      // Show skeleton while loading
      if (
        !ipcDataView ||
        (typeof ipcDataView === "string"
          ? ipcDataView.length === 0
          : ipcDataView.byteLength === 0)
      ) {
        dimensionsGridEl.appendChild(renderDimensionSkeleton(dimConfig.label));
        return;
      }

      try {
        const rows = parseArrowIPC(ipcDataView);
        if (!rows) {
          const errorCard = document.createElement("div");
          errorCard.className = "dim-card error";
          errorCard.innerHTML = `<h4>${dimConfig.label}</h4><div>Failed to parse data</div>`;
          dimensionsGridEl.appendChild(errorCard);
          return;
        }

        const card = renderDimensionCard(
          dimConfig,
          rows,
          selectedMetric,
          filters,
          toggleFilter
        );

        dimensionsGridEl.appendChild(card);
      } catch (e) {
        const errorCard = document.createElement("div");
        errorCard.className = "dim-card error";
        errorCard.innerHTML = `<h4>${dimConfig.label}</h4><div>Error: ${e.message}</div>`;
        dimensionsGridEl.appendChild(errorCard);
      }
    });
  }

  function renderFilters() {
    const filters = getFilters();
    const dateRange = model.get("date_range") || [];
    const brushSelection = model.get("brush_selection") || [];

    renderFilterPills(
      filterPillsEl,
      filters,
      dateRange,
      brushSelection,
      removeFilter,
      clearBrush
    );
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
