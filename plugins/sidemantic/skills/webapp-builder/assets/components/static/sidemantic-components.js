const SQL_KEYWORDS = new Set(
  [
    "and",
    "as",
    "asc",
    "by",
    "case",
    "cast",
    "count",
    "cte",
    "date_trunc",
    "desc",
    "else",
    "end",
    "from",
    "group",
    "in",
    "is",
    "join",
    "left",
    "limit",
    "not",
    "null",
    "on",
    "or",
    "order",
    "over",
    "partition",
    "select",
    "sum",
    "then",
    "when",
    "where",
    "with",
  ],
);

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function span(className, value) {
  return `<span class="${className}">${escapeHtml(value)}</span>`;
}

export function labelize(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replaceAll(".", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function aliasForSemanticRef(ref) {
  return String(ref || "").split(".").at(-1);
}

export function formatValue(value, options = {}) {
  if (value === null || value === undefined || value === "") return "—";
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return numeric.toLocaleString(undefined, {
      currency: options.currency,
      maximumFractionDigits: options.maximumFractionDigits ?? 2,
      style: options.style,
    });
  }
  return String(value);
}

export function normalizeFilterValue(value) {
  return String(value ?? "");
}

export function metricConfigFor(metrics, metricKey) {
  return (metrics || []).find((metric) => metric.key === metricKey) || metrics?.[0] || {};
}

export function metricValueFormat(metrics, metricKey) {
  const metric = metricConfigFor(metrics, metricKey);
  if (metric.format === "currency") {
    return { currency: "USD", maximumFractionDigits: 0, style: "currency" };
  }
  return { maximumFractionDigits: 0 };
}

function valueFormatFor(options, context) {
  return typeof options.valueFormat === "function" ? options.valueFormat(context) : options.valueFormat;
}

// Compact axis labels — 1.2k / 3.4M. Keep in sync with the React `formatCompact`.
export function formatCompact(value) {
  if (!Number.isFinite(value)) return "";
  return Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

// Evenly spaced y-axis ticks across [min, max]. Keep in sync with the React `axisTicks`.
export function axisTicks(min, max, count = 4) {
  if (!(max > min)) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, index) => min + step * index);
}

const SVG_NS = "http://www.w3.org/2000/svg";

function svgEl(name, attrs = {}) {
  const el = document.createElementNS(SVG_NS, name);
  for (const key in attrs) el.setAttribute(key, attrs[key]);
  return el;
}

let chartTooltipNode;

function chartTooltip() {
  if (!chartTooltipNode) {
    chartTooltipNode = document.createElement("div");
    chartTooltipNode.className = "sdm-chart-tooltip";
    chartTooltipNode.hidden = true;
    document.body.appendChild(chartTooltipNode);
  }
  return chartTooltipNode;
}

function bindChartTooltip(el, text) {
  el.addEventListener("pointerenter", () => {
    const tip = chartTooltip();
    tip.textContent = text;
    tip.hidden = false;
  });
  el.addEventListener("pointermove", (event) => {
    const tip = chartTooltip();
    tip.style.left = `${event.clientX + 12}px`;
    tip.style.top = `${event.clientY + 12}px`;
  });
  el.addEventListener("pointerleave", () => {
    chartTooltip().hidden = true;
  });
}

// Redraws `draw(width)` at the SVG's measured width and on resize — keeps axis text undistorted
// (1:1 viewBox) instead of stretching with preserveAspectRatio="none".
function responsiveChart(svg, fallbackWidth, draw) {
  const measure = () => Math.max(160, svg.clientWidth || Number(svg.getAttribute("width")) || fallbackWidth);
  svg.__sdmDraw = () => draw(measure());
  svg.__sdmDraw();
  if (!svg.__sdmObserved && typeof ResizeObserver !== "undefined") {
    svg.__sdmObserved = true;
    new ResizeObserver(() => svg.__sdmDraw && svg.__sdmDraw()).observe(svg);
  }
}

function token(value, className = "") {
  return { className, value };
}

function tokensToHtml(tokens) {
  return tokens.map((item) => (item.className ? span(item.className, item.value) : escapeHtml(item.value))).join("");
}

function inlineTokens(source) {
  const text = String(source || "");
  const tokens = [];
  let index = 0;

  while (index < text.length) {
    const rest = text.slice(index);
    const string = rest.match(/^(&quot;.*?&quot;|'.*?')/);
    if (string) {
      tokens.push(token(string[0], "code-token code-token--string"));
      index += string[0].length;
      continue;
    }

    const literal = rest.match(/^\b(true|false|null)\b/);
    if (literal) {
      tokens.push(token(literal[0], "code-token code-token--literal"));
      index += literal[0].length;
      continue;
    }

    const number = rest.match(/^\b\d+(?:\.\d+)?\b/);
    if (number) {
      tokens.push(token(number[0], "code-token code-token--number"));
      index += number[0].length;
      continue;
    }

    tokens.push(token(text[index]));
    index += 1;
  }

  return tokens;
}

function sqlTokens(source) {
  const text = String(source || "");
  const tokens = [];
  let index = 0;

  while (index < text.length) {
    const rest = text.slice(index);
    const comment = rest.match(/^--[^\n]*/);
    if (comment) {
      tokens.push(token(comment[0], "code-token code-token--comment"));
      index += comment[0].length;
      continue;
    }

    const string = rest.match(/^'(?:''|[^'])*'/);
    if (string) {
      tokens.push(token(string[0], "code-token code-token--string"));
      index += string[0].length;
      continue;
    }

    const number = rest.match(/^\b\d+(?:\.\d+)?\b/);
    if (number) {
      tokens.push(token(number[0], "code-token code-token--number"));
      index += number[0].length;
      continue;
    }

    const word = rest.match(/^[A-Za-z_][A-Za-z0-9_]*/);
    if (word) {
      const value = word[0];
      tokens.push(token(value, SQL_KEYWORDS.has(value.toLowerCase()) ? "code-token code-token--keyword" : ""));
      index += value.length;
      continue;
    }

    tokens.push(token(text[index]));
    index += 1;
  }

  return tokens;
}

function yamlTokens(source) {
  return String(source || "")
    .split("\n")
    .flatMap((line, lineIndex, lines) => {
      const lineTokens = [];
      const commentIndex = line.indexOf("#");
      const code = commentIndex >= 0 ? line.slice(0, commentIndex) : line;
      const comment = commentIndex >= 0 ? line.slice(commentIndex) : "";
      const keyMatch = code.match(/^(\s*-?\s*)([A-Za-z0-9_]+)(:)(.*)$/);

      if (keyMatch) {
        lineTokens.push(token(keyMatch[1]), token(keyMatch[2], "code-token code-token--key"), token(keyMatch[3]));
        lineTokens.push(...inlineTokens(keyMatch[4]));
      } else {
        lineTokens.push(...inlineTokens(code));
      }

      if (comment) lineTokens.push(token(comment, "code-token code-token--comment"));
      if (lineIndex < lines.length - 1) lineTokens.push(token("\n"));
      return lineTokens;
    });
}

export function highlightSql(source) {
  return tokensToHtml(sqlTokens(source));
}

export function highlightYaml(source) {
  return tokensToHtml(yamlTokens(source));
}

export function highlightCode(element, source, language) {
  const tokens = language === "yaml" ? yamlTokens(source) : sqlTokens(source);
  element.replaceChildren(
    ...tokens.map((item) => {
      if (!item.className) return document.createTextNode(item.value);
      const node = document.createElement("span");
      node.className = item.className;
      node.textContent = item.value;
      return node;
    }),
  );
}

export function requireResult(queryName, query) {
  if (!query?.result?.columns || !query?.result?.sample_rows) {
    throw new Error(`${queryName} has no executed result. Re-run inspect_layer.py with --require-execute.`);
  }
  return query.result;
}

export function toComponentResult(result) {
  return {
    columns: result?.columns || [],
    sample_rows: result?.sample_rows || result?.rows || [],
  };
}

export function toComponentQuery({ dimensions = [], metrics = [], result, outputAliases } = {}) {
  return {
    dimensions,
    metrics,
    output_aliases:
      outputAliases ||
      Object.fromEntries([...metrics, ...dimensions].map((ref) => [ref, aliasForSemanticRef(ref)])),
    result: toComponentResult(result),
  };
}

export function renderSelectOptions(select, options, selectedValue, config = {}) {
  const valueFor = config.valueFor || ((option) => option.key ?? option.value ?? option);
  const labelFor = config.labelFor || ((option) => option.label ?? labelize(valueFor(option)));
  select.replaceChildren(
    ...(options || []).map((option) => {
      const item = document.createElement("option");
      item.value = valueFor(option);
      item.textContent = labelFor(option);
      item.selected = item.value === selectedValue;
      return item;
    }),
  );
}

export function formatDateLike(value) {
  if (value === null || value === undefined || value === "") return "—";
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return String(value).slice(0, 10);
}

export function seriesRangeLabel(rows, timeKey) {
  if (!rows?.length || !timeKey) return "No series";
  return `${formatDateLike(rows[0][timeKey])} - ${formatDateLike(rows.at(-1)[timeKey])}`;
}

export function filterZeroMetricRows(result, metricKey) {
  return {
    columns: result?.columns || [],
    rows: (result?.rows || result?.sample_rows || []).filter((row) => {
      const value = Number(row[metricKey]);
      return !Number.isFinite(value) || value !== 0;
    }),
  };
}

export function renderMetricCards(container, query, options = {}) {
  const result = requireResult("metric_totals", query);
  const row = result.sample_rows[0] || {};
  container.replaceChildren();

  for (const metric of query.metrics || []) {
    const key = query.output_aliases?.[metric] || metric.split(".").at(-1);
    const card = document.createElement(options.onSelect ? "button" : "article");
    card.className = "sdm-metric-card";
    card.dataset.metric = metric;
    if (options.onSelect) {
      card.type = "button";
      card.addEventListener("click", () => options.onSelect({ metric, key, value: row[key] }));
    }
    if (options.selectedMetric === metric) {
      card.dataset.selected = "true";
    }

    const title = document.createElement("h3");
    title.textContent = options.labels?.[metric] || labelize(key);

    const value = document.createElement("div");
    value.className = "sdm-metric-card__value";
    value.textContent = formatValue(row[key], valueFormatFor(options, { metric, key, value: row[key] }));

    card.append(title, value);

    const delta = options.deltas?.[metric];
    if (delta) {
      const deltaEl = document.createElement("p");
      deltaEl.className = "sdm-metric-card__delta";
      deltaEl.dataset.tone = delta.tone || "neutral";
      deltaEl.textContent = delta.label;
      card.appendChild(deltaEl);
    }

    container.appendChild(card);
  }
}

export function renderMetricSummaryCards(container, config = {}) {
  const metrics = config.metrics || [];
  renderMetricCards(
    container,
    toComponentQuery({
      metrics: metrics.map((metric) => metric.key),
      result: {
        columns: config.totals?.columns || metrics.map((metric) => aliasForSemanticRef(metric.key)),
        rows: [config.totals?.rows?.[0] || {}],
      },
    }),
    {
      labels: Object.fromEntries(metrics.map((metric) => [metric.key, metric.label || labelize(metric.key)])),
      onSelect: config.onSelect,
      selectedMetric: config.selectedMetric,
      valueFormat: ({ metric }) => config.valueFormat?.(metric) || metricValueFormat(metrics, metric),
    },
  );

  const range = config.rangeLabel || seriesRangeLabel(config.seriesRows, config.timeKey);
  for (const metric of metrics) {
    const card = container.querySelector(`[data-metric="${metric.key}"]`);
    if (!card) continue;

    const detail = document.createElement("p");
    detail.className = "sdm-metric-card__delta";
    detail.textContent = config.detailLabel?.({ metric, range }) || range;
    card.appendChild(detail);

    const wrap = document.createElement("div");
    wrap.className = "sdm-sparkline-wrap";
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.classList.add("sdm-sparkline");
    svg.setAttribute("width", String(config.sparklineWidth || 160));
    svg.setAttribute("height", String(config.sparklineHeight || 56));
    wrap.appendChild(svg);
    card.appendChild(wrap);
    renderSparkline(
      svg,
      (config.seriesRows || []).map((row) => Number(row[aliasForSemanticRef(metric.key)]) || 0),
      { padding: config.sparklinePadding ?? 0 },
    );
  }
}

export function renderLeaderboard(container, query, options = {}) {
  const result = requireResult("dimension_leaderboard", query);
  const dimensionRef = query.dimensions?.[0];
  const metricRef = options.metricRef || query.metrics?.[0];
  const dimensionKey = query.output_aliases?.[dimensionRef] || dimensionRef?.split(".").at(-1);
  const metricKey = query.output_aliases?.[metricRef] || metricRef?.split(".").at(-1);
  const allRows = result.sample_rows || [];
  const extraColumns = options.extraColumns || [];
  const limit = options.limit ?? 0;
  // Collapsed view shows the top `limit` rows; expanded shows all of them. When `onToggleExpand` is
  // supplied the parent (renderDimensionLeaderboardCards) owns expand state; otherwise it is stored
  // on the container so a standalone leaderboard can expand itself.
  const expanded = options.expanded ?? container.__sdmExpanded ?? false;
  // Expanded view may surface a richer column set (every metric, bar-backed) than the compact collapsed
  // view — mirrors the React parent passing different `extraColumns` per expand state.
  const columns = expanded ? options.expandedColumns || extraColumns : extraColumns;
  const rows = !expanded && limit > 0 ? allRows.slice(0, limit) : allRows;
  const selectedValues = new Set([
    ...(Array.isArray(options.selectedValues) ? options.selectedValues : []),
    ...(options.selectedValue === undefined ? [] : [options.selectedValue]),
  ].map(normalizeFilterValue));
  const values = rows.map((row) => {
    const value = Number(row[metricKey]);
    return Number.isFinite(value) ? value : 0;
  });
  const maxMagnitude = Math.max(0, ...values.map((value) => Math.abs(value))) || 1;
  const gridTemplate = columns.length ? `minmax(0, 1fr) repeat(${1 + columns.length}, auto)` : "";

  if (options.titleEl) options.titleEl.textContent = options.dimensionLabel || labelize(dimensionKey);
  if (options.subtitleEl) {
    options.subtitleEl.textContent = options.metricLabel || `Ranked by ${labelize(metricKey)}`;
  }

  container.replaceChildren();
  if (allRows.length === 0) {
    const empty = document.createElement("p");
    empty.className = "sdm-empty-state";
    empty.textContent = options.emptyLabel || "No matching rows";
    container.appendChild(empty);
    return;
  }

  if (expanded) {
    // Expanded: a real multi-column table with a per-column magnitude bar (mirrors the React table).
    const tableCols = [{ key: metricKey, label: labelize(metricKey), bar: true }, ...columns];
    const colMax = {};
    for (const col of tableCols) {
      if (col.bar) colMax[col.key] = Math.max(0, ...rows.map((row) => Math.abs(Number(row[col.key]) || 0))) || 1;
    }

    const table = document.createElement("table");
    table.className = "sdm-leaderboard-table";
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    const dimTh = document.createElement("th");
    dimTh.textContent = options.dimensionLabel || labelize(dimensionKey);
    headRow.appendChild(dimTh);
    for (const col of tableCols) {
      const th = document.createElement("th");
      th.textContent = col.label;
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);

    const tbody = document.createElement("tbody");
    for (const row of rows) {
      const tr = document.createElement("tr");
      const dimTd = document.createElement("td");
      dimTd.textContent = row[dimensionKey] ?? "—";
      tr.appendChild(dimTd);
      for (const col of tableCols) {
        const cellValue = Number(row[col.key]);
        const numericCell = Number.isFinite(cellValue) ? cellValue : 0;
        const tone = numericCell < 0 ? "negative" : "positive";
        const td = document.createElement("td");
        if (col.signTone) td.dataset.tone = tone;
        if (col.bar) {
          const bar = document.createElement("span");
          bar.className = "sdm-leaderboard-table__bar";
          bar.dataset.tone = tone;
          bar.style.width = `${Math.round((Math.abs(numericCell) / (colMax[col.key] || 1)) * 100)}%`;
          td.appendChild(bar);
        }
        const valueSpan = document.createElement("span");
        valueSpan.textContent = formatValue(
          numericCell,
          col.key === metricKey ? valueFormatFor(options, { metric: metricRef, key: metricKey, value: numericCell, row }) : col.format,
        );
        td.appendChild(valueSpan);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }

    table.append(thead, tbody);
    container.appendChild(table);
  } else {
    for (const [index, row] of rows.entries()) {
      const value = values[index] ?? 0;
      const dimensionValue = normalizeFilterValue(row[dimensionKey]);
      const item = document.createElement(options.interactive ? "button" : "div");
      item.className = "sdm-leaderboard-row";
      item.dataset.dimension = dimensionRef;
      item.dataset.tone = value < 0 ? "negative" : "positive";
      item.dataset.value = dimensionValue;
      item.style.setProperty("--bar-width", `${Math.round((Math.abs(value) / maxMagnitude) * 100)}%`);
      if (gridTemplate) item.style.gridTemplateColumns = gridTemplate;
      if (selectedValues.has(dimensionValue)) {
        item.dataset.selected = "true";
      }

      const label = document.createElement("span");
      label.textContent = row[dimensionKey] ?? "—";
      const strong = document.createElement("strong");
      strong.textContent = formatValue(value, valueFormatFor(options, { metric: metricRef, key: metricKey, value, row }));
      item.append(label, strong);

      for (const column of columns) {
        const cellValue = Number(row[column.key]);
        const numericCell = Number.isFinite(cellValue) ? cellValue : 0;
        const cell = document.createElement("span");
        cell.className = "sdm-leaderboard-cell";
        if (column.signTone) cell.dataset.tone = numericCell < 0 ? "negative" : "positive";
        cell.textContent = formatValue(numericCell, column.format);
        item.append(cell);
      }

      if (options.onSelect) {
        item.addEventListener("click", () => options.onSelect({ dimension: dimensionRef, value: row[dimensionKey], row }));
      }

      container.appendChild(item);
    }
  }

  // Standalone expand toggle. When a parent owns expand state it passes `expandable: false` and
  // renders its own back affordance instead.
  if (options.expandable && (expanded || allRows.length > limit)) {
    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "sdm-leaderboard-expand";
    toggle.dataset.action = expanded ? "leaderboard-back" : "leaderboard-expand";
    toggle.textContent = expanded ? "← Back" : `Expand table (${allRows.length})`;
    toggle.addEventListener("click", () => {
      if (options.onToggleExpand) {
        options.onToggleExpand(!expanded);
      } else {
        container.__sdmExpanded = !expanded;
        renderLeaderboard(container, query, options);
      }
    });
    container.appendChild(toggle);
  }
}

export function renderDimensionLeaderboardCards(container, dimensions, config = {}) {
  // Expand state lives on the container so app re-renders preserve it. When a dimension is expanded
  // only that card renders (full width, all rows) with a back affordance; the others are hidden.
  const expandedDim = container.__sdmExpandedDim ?? null;
  const collapseLimit = config.limit ?? 6;
  const expandable = config.expandable !== false;
  container.replaceChildren();
  container.dataset.expanded = expandedDim ? "true" : "false";

  const reRender = () => renderDimensionLeaderboardCards(container, dimensions, config);
  const visible = expandedDim
    ? (dimensions || []).filter((dimension) => (dimension.key || dimension) === expandedDim)
    : dimensions || [];

  for (const dimension of visible) {
    const dimensionRef = dimension.key || dimension;
    const dimensionAlias = aliasForSemanticRef(dimensionRef);
    const metricRef = config.metricRef;
    const metricAlias = aliasForSemanticRef(metricRef);
    const expanded = dimensionRef === expandedDim;
    const rawResult = config.resultForDimension?.(dimension) || { columns: [], rows: [] };
    const result = filterZeroMetricRows(
      {
        columns: rawResult.columns?.length ? rawResult.columns : [dimensionAlias, metricAlias],
        rows: rawResult.rows || rawResult.sample_rows || [],
      },
      metricAlias,
    );

    const card = document.createElement("article");
    card.className = "sdm-leaderboard";
    card.dataset.dim = dimensionRef;
    card.dataset.testid = `dimension-${dimensionAlias}`;
    if (expanded) card.dataset.expanded = "true";

    const heading = document.createElement("div");
    heading.className = "sdm-section-heading";
    if (expanded && expandable) {
      const back = document.createElement("button");
      back.type = "button";
      back.className = "sdm-leaderboard-back";
      back.dataset.action = "leaderboard-back";
      back.textContent = "← All dimensions";
      back.addEventListener("click", () => {
        container.__sdmExpandedDim = null;
        reRender();
      });
      heading.appendChild(back);
    }
    const title = document.createElement("h3");
    heading.append(title);

    const rows = document.createElement("div");
    rows.dataset.testid = "leaderboard-rows";
    card.append(heading, rows);
    container.appendChild(card);

    renderLeaderboard(
      rows,
      toComponentQuery({
        dimensions: [dimensionRef],
        metrics: [metricRef],
        result,
      }),
      {
        dimensionLabel: dimension.label || labelize(dimensionRef),
        emptyLabel: config.emptyLabel,
        expandable: false,
        expanded,
        extraColumns: config.extraColumns,
        expandedColumns: config.expandedColumns,
        interactive: config.interactive,
        limit: expanded ? 0 : collapseLimit,
        metricLabel: config.metricLabel?.(dimension) || `Ranked by ${config.metricName || labelize(metricRef)}`,
        metricRef,
        onSelect: config.onSelect,
        selectedValues:
          config.selectedValuesForDimension?.(dimension) ||
          (config.selectedValueForDimension ? [config.selectedValueForDimension(dimension)] : undefined),
        selectedValue: config.selectedValueForDimension?.(dimension),
        titleEl: title,
        valueFormat: config.valueFormat,
      },
    );

    if (!expanded && expandable && result.rows.length > collapseLimit) {
      const expand = document.createElement("button");
      expand.type = "button";
      expand.className = "sdm-leaderboard-expand";
      expand.dataset.action = "leaderboard-expand";
      expand.textContent = `Expand table (${result.rows.length})`;
      expand.addEventListener("click", () => {
        container.__sdmExpandedDim = dimensionRef;
        reRender();
      });
      card.appendChild(expand);
    }
  }
}

export function renderFilterPills(container, filters, onRemove, options = {}) {
  container.replaceChildren();
  for (const [dimension, values] of Object.entries(filters || {})) {
    for (const value of values || []) {
      const filterValue = normalizeFilterValue(value);
      const pill = document.createElement("span");
      pill.className = "sdm-filter-pill";
      pill.dataset.dimension = dimension;
      pill.dataset.value = filterValue;
      pill.textContent = `${labelize(dimension)}: ${formatValue(filterValue)}`;
      if (onRemove) {
        const button = document.createElement("button");
        button.type = "button";
        button.ariaLabel = `Remove ${formatValue(filterValue)}`;
        button.textContent = "×";
        button.addEventListener("click", () => onRemove({ dimension, value: filterValue }));
        pill.appendChild(button);
      }
      container.appendChild(pill);
    }
  }

  if (!container.childElementCount && options.emptyLabel) {
    const empty = document.createElement("span");
    empty.className = "sdm-filter-pill sdm-filter-pill--empty";
    empty.textContent = options.emptyLabel;
    container.appendChild(empty);
  }
}

export function renderSparkline(svg, values, options = {}) {
  // Compact, axis-less trend (use renderLineChart when you need axes). Responsive width (1:1 viewBox,
  // no stretch), endpoint marker with hover tooltip, and an a11y label.
  const numbers = (values || []).map(Number).filter(Number.isFinite);
  svg.setAttribute("role", "img");

  responsiveChart(svg, 160, (width) => {
    const height = svg.clientHeight || Number(svg.getAttribute("height")) || 56;
    const pad = options.padding ?? 4;
    svg.replaceChildren();
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.removeAttribute("preserveAspectRatio");
    if (numbers.length < 2) {
      svg.setAttribute("aria-label", options.ariaLabel || "No trend data");
      return;
    }
    svg.setAttribute(
      "aria-label",
      options.ariaLabel || `Trend of ${numbers.length} points, latest ${formatValue(numbers.at(-1))}`,
    );

    const min = Math.min(...numbers);
    const max = Math.max(...numbers);
    const span = max - min || 1;
    const coordinates = numbers.map((value, index) => ({
      x: pad + (index / (numbers.length - 1)) * (width - pad * 2),
      y: pad + (1 - (value - min) / span) * (height - pad * 2),
    }));
    const points = coordinates.map(({ x, y }) => `${x.toFixed(1)},${y.toFixed(1)}`);

    if (options.area !== false) {
      svg.appendChild(
        svgEl("path", {
          class: "sdm-sparkline__area",
          d: `M ${coordinates[0].x.toFixed(1)} ${(height - pad).toFixed(1)} L ${points.join(" L ")} L ${coordinates.at(-1).x.toFixed(1)} ${(height - pad).toFixed(1)} Z`,
        }),
      );
    }

    svg.appendChild(svgEl("path", { class: "sdm-sparkline__line", d: `M ${points.join(" L ")}` }));

    const last = coordinates.at(-1);
    const dot = svgEl("circle", { class: "sdm-sparkline__dot", cx: last.x.toFixed(1), cy: last.y.toFixed(1), r: "2.5" });
    const lastLabel = options.labels?.[numbers.length - 1];
    bindChartTooltip(dot, `${lastLabel ? `${lastLabel}: ` : ""}${formatValue(numbers.at(-1))}`);
    svg.appendChild(dot);
  });
}

export function renderColumnChart(svg, rows, options = {}) {
  const data = rows || [];
  const labelKey = options.labelKey || "label";
  const valueKey = options.valueKey || "value";
  svg.setAttribute("role", "img");

  responsiveChart(svg, 640, (width) => {
    const height = svg.clientHeight || Number(svg.getAttribute("height")) || 200;
    const margin = { top: 12, right: 14, bottom: 26, left: 44 };
    const values = data.map((row) => {
      const value = Number(row[valueKey]);
      return Number.isFinite(value) ? value : 0;
    });
    const min = Math.min(0, ...values);
    const max = Math.max(0, ...values);
    const span = max - min || 1;
    const plotHeight = height - margin.top - margin.bottom;
    const yForValue = (value) => margin.top + (1 - (value - min) / span) * plotHeight;
    const baselineY = yForValue(0);
    const slot = (width - margin.left - margin.right) / Math.max(data.length, 1);
    const barWidth = Math.max(8, Math.min(48, slot * 0.62));

    svg.replaceChildren();
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.removeAttribute("preserveAspectRatio");
    svg.setAttribute("aria-label", options.ariaLabel || `Bar chart, ${data.length} categories, up to ${formatCompact(max)}`);

    for (const tick of axisTicks(min, max, 4)) {
      const y = yForValue(tick);
      svg.appendChild(
        svgEl("line", { class: "sdm-chart__grid", x1: margin.left, x2: width - margin.right, y1: y.toFixed(1), y2: y.toFixed(1) }),
      );
      const axisLabel = svgEl("text", { class: "sdm-chart__axis", x: margin.left - 6, y: (y + 3).toFixed(1), "text-anchor": "end" });
      axisLabel.textContent = formatCompact(tick);
      svg.appendChild(axisLabel);
    }

    const baseline = svgEl("line", {
      class: "sdm-column-chart__baseline",
      x1: margin.left,
      x2: width - margin.right,
      y1: baselineY.toFixed(1),
      y2: baselineY.toFixed(1),
    });
    svg.appendChild(baseline);

    data.forEach((row, index) => {
      const value = values[index] ?? 0;
      const valueY = yForValue(value);
      const barHeight = Math.abs(valueY - baselineY);
      const x = margin.left + slot * index + (slot - barWidth) / 2;
      const y = Math.min(valueY, baselineY);

      const rect = svgEl("rect", {
        x: x.toFixed(1),
        y: y.toFixed(1),
        width: barWidth.toFixed(1),
        height: barHeight.toFixed(1),
        rx: "3",
      });
      rect.dataset.tone = value < 0 ? "negative" : "positive";
      rect.dataset.label = row[labelKey] ?? "";
      rect.dataset.value = String(value);
      bindChartTooltip(rect, `${row[labelKey] ?? ""}: ${formatValue(value)}`);
      svg.appendChild(rect);

      const label = svgEl("text", { x: (x + barWidth / 2).toFixed(1), y: String(height - 8), "text-anchor": "middle" });
      label.textContent = String(row[labelKey] ?? "").slice(0, 8);
      svg.appendChild(label);
    });
  });
}

export function renderLineChart(svg, rows, options = {}) {
  // Full-size time-series line for the metricSeries query shape: responsive width, y-axis gridlines +
  // compact labels, first/mid/last x labels, per-point hover tooltips, and an a11y summary. Sparkline
  // is the compact, axis-less variant.
  const data = rows || [];
  const labelKey = options.labelKey || "label";
  const valueKey = options.valueKey || "value";
  svg.setAttribute("role", "img");

  responsiveChart(svg, 640, (width) => {
    const height = svg.clientHeight || Number(svg.getAttribute("height")) || 200;
    const margin = { top: 12, right: 14, bottom: 26, left: 44 };
    const values = data.map((row) => {
      const value = Number(row[valueKey]);
      return Number.isFinite(value) ? value : 0;
    });

    svg.replaceChildren();
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.removeAttribute("preserveAspectRatio");
    if (values.length < 2) {
      svg.setAttribute("aria-label", options.ariaLabel || "No series data");
      return;
    }

    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = max - min || 1;
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;
    const baselineY = margin.top + plotHeight;
    const xForIndex = (index) => margin.left + (index / (values.length - 1)) * plotWidth;
    const yForValue = (value) => margin.top + (1 - (value - min) / span) * plotHeight;
    const coordinates = values.map((value, index) => ({ x: xForIndex(index), y: yForValue(value) }));
    const points = coordinates.map(({ x, y }) => `${x.toFixed(1)},${y.toFixed(1)}`);
    svg.setAttribute(
      "aria-label",
      options.ariaLabel || `Line chart, ${values.length} points, ${formatCompact(min)} to ${formatCompact(max)}`,
    );

    for (const tick of axisTicks(min, max, 4)) {
      const y = yForValue(tick);
      svg.appendChild(
        svgEl("line", { class: "sdm-chart__grid", x1: margin.left, x2: width - margin.right, y1: y.toFixed(1), y2: y.toFixed(1) }),
      );
      const axisLabel = svgEl("text", { class: "sdm-chart__axis", x: margin.left - 6, y: (y + 3).toFixed(1), "text-anchor": "end" });
      axisLabel.textContent = formatCompact(tick);
      svg.appendChild(axisLabel);
    }

    svg.appendChild(
      svgEl("path", {
        class: "sdm-line-chart__area",
        d: `M ${coordinates[0].x.toFixed(1)} ${baselineY.toFixed(1)} L ${points.join(" L ")} L ${coordinates.at(-1).x.toFixed(1)} ${baselineY.toFixed(1)} Z`,
      }),
    );
    svg.appendChild(svgEl("path", { class: "sdm-line-chart__line", d: `M ${points.join(" L ")}` }));

    coordinates.forEach((point, index) => {
      const dot = svgEl("circle", { class: "sdm-line-chart__dot", cx: point.x.toFixed(1), cy: point.y.toFixed(1), r: "3" });
      dot.dataset.label = data[index]?.[labelKey] ?? "";
      dot.dataset.value = String(values[index]);
      bindChartTooltip(dot, `${data[index]?.[labelKey] ?? ""}: ${formatValue(values[index])}`);
      svg.appendChild(dot);
    });

    const labelIndexes = [0, Math.floor((values.length - 1) / 2), values.length - 1].filter(
      (value, index, all) => all.indexOf(value) === index,
    );
    for (const index of labelIndexes) {
      const label = svgEl("text", {
        class: "sdm-line-chart__label",
        x: xForIndex(index).toFixed(1),
        y: String(height - 8),
        "text-anchor": index === 0 ? "start" : index === values.length - 1 ? "end" : "middle",
      });
      label.textContent = String(data[index]?.[labelKey] ?? "").slice(0, 12);
      svg.appendChild(label);
    }
  });
}

export function renderQueryDebug(container, queries) {
  container.textContent = Object.entries(queries || {})
    .map(([name, query]) => `-- ${name}\n${query?.sql || ""}`)
    .join("\n\n");
}

export function renderHighlightedQueryDebug(container, queries) {
  renderQueryDebug(container, queries);
  highlightCode(container, container.textContent, "sql");
}

export function renderDataPreview(table, result, options = {}) {
  const columns = result?.columns || [];
  const rows = result?.sample_rows || [];
  const pageSize = options.pageSize ?? 0;
  const paginate = pageSize > 0 && rows.length > pageSize;
  const pageCount = paginate ? Math.ceil(rows.length / pageSize) : 1;
  let page = Math.min(Math.max(0, table.__sdmPage ?? 0), pageCount - 1);
  table.__sdmPage = page;
  const start = paginate ? page * pageSize : 0;
  const visibleRows = paginate ? rows.slice(start, start + pageSize) : rows;

  table.replaceChildren();

  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  for (const column of columns) {
    const th = document.createElement("th");
    th.textContent = labelize(column);
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);

  const tbody = document.createElement("tbody");
  for (const row of visibleRows) {
    const tr = document.createElement("tr");
    for (const column of columns) {
      const td = document.createElement("td");
      td.textContent = formatValue(row[column]);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }

  table.append(thead, tbody);

  // Clear any external pager from a previous render.
  if (options.pager) options.pager.replaceChildren();

  if (paginate) {
    const fillPager = (target) => {
      const label = document.createElement("span");
      label.textContent = `${start + 1}–${Math.min(start + pageSize, rows.length)} of ${rows.length.toLocaleString()}`;
      const controls = document.createElement("span");
      controls.className = "sdm-data-preview__pager-controls";
      const prev = document.createElement("button");
      prev.type = "button";
      prev.dataset.action = "prev-page";
      prev.textContent = "Prev";
      prev.disabled = page === 0;
      prev.addEventListener("click", () => {
        table.__sdmPage = page - 1;
        renderDataPreview(table, result, options);
      });
      const next = document.createElement("button");
      next.type = "button";
      next.dataset.action = "next-page";
      next.textContent = "Next";
      next.disabled = page >= pageCount - 1;
      next.addEventListener("click", () => {
        table.__sdmPage = page + 1;
        renderDataPreview(table, result, options);
      });
      controls.append(prev, next);
      target.append(label, controls);
    };

    if (options.pager) {
      // Render the pager into a caller-provided element (kept outside the scroll area so it stays put).
      options.pager.className = "sdm-data-preview__pager";
      fillPager(options.pager);
    } else {
      const tfoot = document.createElement("tfoot");
      const tr = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = columns.length || 1;
      cell.className = "sdm-data-preview__pager";
      fillPager(cell);
      tr.appendChild(cell);
      tfoot.appendChild(tr);
      table.appendChild(tfoot);
    }
  }
}

export function renderState(container, state) {
  // Mirrors the React LoadingState / EmptyState / ErrorState trio: error | loading | empty.
  const kind = state.kind === "error" ? "error" : state.kind === "loading" ? "loading" : "empty";
  const variant =
    kind === "error" ? "sdm-error-state" : kind === "loading" ? "sdm-loading-state" : "sdm-empty-state";
  container.className = `sdm-state-box ${variant}`;
  container.dataset.state = kind;
  container.textContent = state.message;
}

export function renderValidationState(stateElement, listElement, errors = []) {
  listElement.replaceChildren();
  const valid = errors.length === 0;
  stateElement.textContent = valid ? "Valid" : "Invalid";
  stateElement.dataset.valid = String(valid);
  for (const error of errors) {
    const item = document.createElement("li");
    item.textContent = error;
    listElement.appendChild(item);
  }
}

export function setControlsDisabled(selector, isDisabled) {
  for (const control of document.querySelectorAll(selector)) {
    control.disabled = isDisabled;
  }
}

export function syncScrollPosition(source, target) {
  target.scrollTop = source.scrollTop;
  target.scrollLeft = source.scrollLeft;
}

export function toggleFilterValue(filters, dimension, value) {
  const next = { ...filters };
  const stringValue = normalizeFilterValue(value);
  const selectedValues = new Set((next[dimension] || []).map(normalizeFilterValue));

  if (selectedValues.has(stringValue)) {
    selectedValues.delete(stringValue);
  } else {
    selectedValues.add(stringValue);
  }

  if (selectedValues.size === 0) {
    delete next[dimension];
  } else {
    next[dimension] = [...selectedValues];
  }

  return next;
}

export function toggleSingleValueFilter(filters, dimension, value) {
  return toggleFilterValue(filters, dimension, value);
}

export function removeFilterDimension(filters, dimension) {
  const next = { ...filters };
  delete next[dimension];
  return next;
}

export function removeFilterValue(filters, dimension, value) {
  const next = { ...filters };
  const stringValue = normalizeFilterValue(value);
  const values = (next[dimension] || []).map(normalizeFilterValue).filter((item) => item !== stringValue);
  if (values.length === 0) {
    delete next[dimension];
  } else {
    next[dimension] = values;
  }
  return next;
}
