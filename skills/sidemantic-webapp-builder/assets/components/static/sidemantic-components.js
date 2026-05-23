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
  const rows = result.sample_rows || [];
  const selectedValues = new Set([
    ...(Array.isArray(options.selectedValues) ? options.selectedValues : []),
    ...(options.selectedValue === undefined ? [] : [options.selectedValue]),
  ].map((value) => String(value)));
  const values = rows.map((row) => {
    const value = Number(row[metricKey]);
    return Number.isFinite(value) ? value : 0;
  });
  const maxMagnitude = Math.max(0, ...values.map((value) => Math.abs(value))) || 1;

  if (options.titleEl) options.titleEl.textContent = options.dimensionLabel || labelize(dimensionKey);
  if (options.subtitleEl) {
    options.subtitleEl.textContent = options.metricLabel || `Ranked by ${labelize(metricKey)}`;
  }

  container.replaceChildren();
  if (rows.length === 0) {
    const empty = document.createElement("p");
    empty.className = "sdm-empty-state";
    empty.textContent = options.emptyLabel || "No matching rows";
    container.appendChild(empty);
    return;
  }

  for (const [index, row] of rows.entries()) {
    const value = values[index] ?? 0;
    const item = document.createElement(options.interactive ? "button" : "div");
    item.className = "sdm-leaderboard-row";
    item.dataset.dimension = dimensionRef;
    item.dataset.tone = value < 0 ? "negative" : "positive";
    item.dataset.value = row[dimensionKey] ?? "";
    item.style.setProperty("--bar-width", `${Math.round((Math.abs(value) / maxMagnitude) * 100)}%`);
    if (selectedValues.has(String(row[dimensionKey]))) {
      item.dataset.selected = "true";
    }

    const label = document.createElement("span");
    label.textContent = row[dimensionKey] ?? "—";
    const strong = document.createElement("strong");
    strong.textContent = formatValue(value, valueFormatFor(options, { metric: metricRef, key: metricKey, value, row }));
    item.append(label, strong);

    if (options.onSelect) {
      item.addEventListener("click", () => options.onSelect({ dimension: dimensionRef, value: row[dimensionKey], row }));
    }

    container.appendChild(item);
  }
}

export function renderDimensionLeaderboardCards(container, dimensions, config = {}) {
  container.replaceChildren();

  for (const dimension of dimensions || []) {
    const dimensionRef = dimension.key || dimension;
    const dimensionAlias = aliasForSemanticRef(dimensionRef);
    const metricRef = config.metricRef;
    const metricAlias = aliasForSemanticRef(metricRef);
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

    const heading = document.createElement("div");
    heading.className = "sdm-section-heading";
    const title = document.createElement("h3");
    const subtitle = document.createElement("p");
    heading.append(title, subtitle);

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
        interactive: config.interactive,
        metricLabel: config.metricLabel?.(dimension) || `Ranked by ${config.metricName || labelize(metricRef)}`,
        metricRef,
        onSelect: config.onSelect,
        selectedValues:
          config.selectedValuesForDimension?.(dimension) ||
          (config.selectedValueForDimension ? [config.selectedValueForDimension(dimension)] : undefined),
        selectedValue: config.selectedValueForDimension?.(dimension),
        subtitleEl: subtitle,
        titleEl: title,
        valueFormat: config.valueFormat,
      },
    );
  }
}

export function renderFilterPills(container, filters, onRemove, options = {}) {
  container.replaceChildren();
  for (const [dimension, values] of Object.entries(filters || {})) {
    for (const value of values || []) {
      const pill = document.createElement("span");
      pill.className = "sdm-filter-pill";
      pill.dataset.dimension = dimension;
      pill.dataset.value = value;
      pill.textContent = `${labelize(dimension)}: ${value}`;
      if (onRemove) {
        const button = document.createElement("button");
        button.type = "button";
        button.ariaLabel = `Remove ${value}`;
        button.textContent = "×";
        button.addEventListener("click", () => onRemove({ dimension, value }));
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
  const numbers = (values || []).map(Number).filter(Number.isFinite);
  svg.replaceChildren();
  if (numbers.length < 2) return;

  const width = Number(svg.getAttribute("width") || 160);
  const height = Number(svg.getAttribute("height") || 56);
  const pad = options.padding ?? 4;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("preserveAspectRatio", "none");

  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const span = max - min || 1;
  const coordinates = numbers.map((value, index) => {
    const x = pad + (index / (numbers.length - 1)) * (width - pad * 2);
    const y = pad + (1 - (value - min) / span) * (height - pad * 2);
    return { x, y };
  });
  const points = coordinates.map(({ x, y }) => `${x.toFixed(1)},${y.toFixed(1)}`);

  if (options.area !== false) {
    const area = document.createElementNS("http://www.w3.org/2000/svg", "path");
    area.classList.add("sdm-sparkline__area");
    area.setAttribute(
      "d",
      `M ${coordinates[0].x.toFixed(1)} ${(height - pad).toFixed(1)} L ${points.join(" L ")} L ${coordinates.at(-1).x.toFixed(1)} ${(height - pad).toFixed(1)} Z`,
    );
    svg.appendChild(area);
  }

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.classList.add("sdm-sparkline__line");
  path.setAttribute("d", `M ${points.join(" L ")}`);
  svg.appendChild(path);
}

export function renderColumnChart(svg, rows, options = {}) {
  const data = rows || [];
  const width = Number(svg.getAttribute("width") || 320);
  const height = Number(svg.getAttribute("height") || 160);
  const padX = options.paddingX ?? 16;
  const padTop = options.paddingTop ?? 10;
  const padBottom = options.paddingBottom ?? 28;
  const labelKey = options.labelKey || "label";
  const valueKey = options.valueKey || "value";
  const values = data.map((row) => {
    const value = Number(row[valueKey]);
    return Number.isFinite(value) ? value : 0;
  });
  const min = Math.min(0, ...values);
  const max = Math.max(0, ...values);
  const span = max - min || 1;
  const plotHeight = height - padTop - padBottom;
  const yForValue = (value) => padTop + (1 - (value - min) / span) * plotHeight;
  const baselineY = yForValue(0);
  const slot = (width - padX * 2) / Math.max(data.length, 1);
  const barWidth = Math.max(10, Math.min(42, slot * 0.56));

  svg.replaceChildren();
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("preserveAspectRatio", "none");

  if (min < 0) {
    const baseline = document.createElementNS("http://www.w3.org/2000/svg", "line");
    baseline.classList.add("sdm-column-chart__baseline");
    baseline.setAttribute("x1", String(padX));
    baseline.setAttribute("x2", String(width - padX));
    baseline.setAttribute("y1", baselineY.toFixed(1));
    baseline.setAttribute("y2", baselineY.toFixed(1));
    svg.appendChild(baseline);
  }

  data.forEach((row, index) => {
    const value = values[index] ?? 0;
    const valueY = yForValue(value);
    const barHeight = Math.abs(valueY - baselineY);
    const x = padX + slot * index + (slot - barWidth) / 2;
    const y = Math.min(valueY, baselineY);

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", x.toFixed(1));
    rect.setAttribute("y", y.toFixed(1));
    rect.setAttribute("width", barWidth.toFixed(1));
    rect.setAttribute("height", barHeight.toFixed(1));
    rect.setAttribute("rx", "3");
    rect.dataset.tone = value < 0 ? "negative" : "positive";
    rect.dataset.label = row[labelKey] ?? "";
    rect.dataset.value = String(value);
    svg.appendChild(rect);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", (x + barWidth / 2).toFixed(1));
    label.setAttribute("y", String(height - 8));
    label.setAttribute("text-anchor", "middle");
    label.textContent = String(row[labelKey] ?? "").slice(0, 8);
    svg.appendChild(label);
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

export function renderDataPreview(table, result) {
  const columns = result?.columns || [];
  const rows = result?.sample_rows || [];
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
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const column of columns) {
      const td = document.createElement("td");
      td.textContent = formatValue(row[column]);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }

  table.append(thead, tbody);
}

export function renderState(container, state) {
  container.className = state.kind === "error" ? "sdm-state-box sdm-error-state" : "sdm-state-box sdm-empty-state";
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
  const stringValue = String(value);
  const selectedValues = new Set((next[dimension] || []).map((item) => String(item)));

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
  const stringValue = String(value);
  const values = (next[dimension] || []).filter((item) => String(item) !== stringValue);
  if (values.length === 0) {
    delete next[dimension];
  } else {
    next[dimension] = values;
  }
  return next;
}
