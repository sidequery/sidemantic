export function labelize(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replaceAll(".", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function formatValue(value, options = {}) {
  if (value === null || value === undefined || value === "") return "—";
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return numeric.toLocaleString(undefined, {
      maximumFractionDigits: options.maximumFractionDigits ?? 2,
    });
  }
  return String(value);
}

export function requireResult(queryName, query) {
  if (!query?.result?.columns || !query?.result?.sample_rows) {
    throw new Error(`${queryName} has no executed result. Re-run inspect_layer.py with --require-execute.`);
  }
  return query.result;
}

export function renderMetricCards(container, query, options = {}) {
  const result = requireResult("metric_totals", query);
  const row = result.sample_rows[0] || {};
  container.innerHTML = "";

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
    value.textContent = formatValue(row[key], options.valueFormat);

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

export function renderLeaderboard(container, query, options = {}) {
  const result = requireResult("dimension_leaderboard", query);
  const dimensionRef = query.dimensions?.[0];
  const metricRef = query.metrics?.[0];
  const dimensionKey = query.output_aliases?.[dimensionRef] || dimensionRef?.split(".").at(-1);
  const metricKey = query.output_aliases?.[metricRef] || metricRef?.split(".").at(-1);
  const rows = result.sample_rows || [];
  const max = Math.max(...rows.map((row) => Number(row[metricKey]) || 0), 1);

  if (options.titleEl) options.titleEl.textContent = options.dimensionLabel || labelize(dimensionKey);
  if (options.subtitleEl) {
    options.subtitleEl.textContent = options.metricLabel || `Ranked by ${labelize(metricKey)}`;
  }

  container.innerHTML = "";
  for (const row of rows) {
    const value = Number(row[metricKey]) || 0;
    const item = document.createElement(options.interactive ? "button" : "div");
    item.className = "sdm-leaderboard-row";
    item.dataset.dimension = dimensionRef;
    item.dataset.value = row[dimensionKey] ?? "";
    item.style.setProperty("--bar-width", `${Math.round((value / max) * 100)}%`);
    if (options.selectedValue !== undefined && String(options.selectedValue) === String(row[dimensionKey])) {
      item.dataset.selected = "true";
    }

    const label = document.createElement("span");
    label.textContent = row[dimensionKey] ?? "—";
    const strong = document.createElement("strong");
    strong.textContent = formatValue(value, options.valueFormat);
    item.append(label, strong);

    if (options.onSelect) {
      item.addEventListener("click", () => options.onSelect({ dimension: dimensionRef, value: row[dimensionKey], row }));
    }

    container.appendChild(item);
  }
}

export function renderFilterPills(container, filters, onRemove) {
  container.innerHTML = "";
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
  const points = numbers.map((value, index) => {
    const x = pad + (index / (numbers.length - 1)) * (width - pad * 2);
    const y = pad + (1 - (value - min) / span) * (height - pad * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
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
  const max = Math.max(...data.map((row) => Number(row[valueKey]) || 0), 1);
  const slot = (width - padX * 2) / Math.max(data.length, 1);
  const barWidth = Math.max(10, Math.min(42, slot * 0.56));

  svg.replaceChildren();
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("preserveAspectRatio", "none");

  data.forEach((row, index) => {
    const value = Number(row[valueKey]) || 0;
    const barHeight = ((height - padTop - padBottom) * value) / max;
    const x = padX + slot * index + (slot - barWidth) / 2;
    const y = height - padBottom - barHeight;

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", x.toFixed(1));
    rect.setAttribute("y", y.toFixed(1));
    rect.setAttribute("width", barWidth.toFixed(1));
    rect.setAttribute("height", barHeight.toFixed(1));
    rect.setAttribute("rx", "3");
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
