// webapp/src/components/BarLineCombo.tsx
import { useEffect, useRef, useState as useState2 } from "react";

// webapp/src/components/ChartTooltip.tsx
import { useState } from "react";
import { jsx, jsxs } from "react/jsx-runtime";
function useChartTooltip() {
  const [tip, setTip] = useState(null);
  const handlers = (content) => ({
    onMouseEnter: (event) => setTip({ content, x: event.clientX, y: event.clientY }),
    onMouseMove: (event) => setTip({ content, x: event.clientX, y: event.clientY }),
    onMouseLeave: () => setTip(null)
  });
  return { tip, handlers };
}
function ChartTooltip({
  tip,
  position = "fixed",
  offset = 12,
  className,
  style
}) {
  if (!tip)
    return null;
  return /* @__PURE__ */ jsx("div", {
    role: "tooltip",
    style: { position, left: tip.x + offset, top: tip.y + offset, pointerEvents: "none", zIndex: 50, ...style },
    className: className || "rounded-lg border border-line bg-surface px-2.5 py-1.5 text-xs text-ink shadow-[var(--shadow)]",
    children: tip.content
  });
}
function TooltipRows({ title, rows }) {
  return /* @__PURE__ */ jsxs("div", {
    className: "min-w-28",
    children: [
      title ? /* @__PURE__ */ jsx("div", {
        className: "mb-0.5 font-mono text-faint",
        children: title
      }) : null,
      rows.map((row, index) => /* @__PURE__ */ jsxs("div", {
        className: "flex items-center justify-between gap-3",
        children: [
          /* @__PURE__ */ jsxs("span", {
            className: "flex items-center gap-1 text-muted",
            children: [
              row.swatch ? /* @__PURE__ */ jsx("span", {
                "aria-hidden": "true",
                className: "inline-block size-2 rounded-sm",
                style: { background: row.swatch }
              }) : null,
              row.label
            ]
          }),
          /* @__PURE__ */ jsx("span", {
            className: "font-mono tnum font-medium text-ink",
            children: row.value
          })
        ]
      }, index))
    ]
  });
}

// webapp/src/data/types.ts
function aliasOf(ref) {
  const last = ref.split(".").at(-1);
  return last || ref;
}
var NULL_TOKEN = "\x00__null__";

// webapp/src/lib/uiCore.js
function labelize(value) {
  return String(value || "").replaceAll("_", " ").replaceAll(".", " ").replace(/\b\w/g, (char) => char.toUpperCase()).trim();
}
function aliasForSemanticRef(ref) {
  return String(ref || "").split(".").at(-1);
}
function formatUiValue(value, options = {}) {
  if (value === null || value === undefined || value === "")
    return "—";
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric))
    return String(value);
  const format = String(options.format || "").toLowerCase();
  const percent = options.style === "percent" || format.includes("%") || format.includes("percent") || format.includes("pct") || options.type === "ratio";
  if (percent) {
    const scaled = options.style === "percent" ? numeric : Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
    return scaled.toLocaleString(undefined, {
      maximumFractionDigits: options.maximumFractionDigits ?? 1,
      style: options.style === "percent" ? "percent" : "decimal"
    }) + (options.style === "percent" ? "" : "%");
  }
  const currency = options.style === "currency" || options.currency || format.includes("$") || format.includes("usd") || format.includes("currency") || format.includes("dollar");
  return numeric.toLocaleString(undefined, {
    currency: currency ? options.currency || "USD" : undefined,
    maximumFractionDigits: options.maximumFractionDigits ?? 2,
    notation: options.compact ? "compact" : "standard",
    style: currency ? "currency" : "decimal"
  });
}
function formatUiCompact(value, options = {}) {
  return formatUiValue(value, { ...options, compact: true, maximumFractionDigits: options.maximumFractionDigits ?? 1 });
}
function normalizeFilterValue(value) {
  return String(value ?? "");
}
function toggleFilterValue(filters, dimension, value) {
  const next = { ...filters };
  const normalized = normalizeFilterValue(value);
  const selected = new Set((next[dimension] || []).map(normalizeFilterValue));
  if (selected.has(normalized))
    selected.delete(normalized);
  else
    selected.add(normalized);
  if (selected.size)
    next[dimension] = [...selected];
  else
    delete next[dimension];
  return next;
}
function removeFilterDimension(filters, dimension) {
  const next = { ...filters };
  delete next[dimension];
  return next;
}
function removeFilterValue(filters, dimension, value) {
  const next = { ...filters };
  const normalized = normalizeFilterValue(value);
  const values = (next[dimension] || []).map(normalizeFilterValue).filter((item) => item !== normalized);
  if (values.length)
    next[dimension] = values;
  else
    delete next[dimension];
  return next;
}
function paginateRows(rows, page, pageSize) {
  const paginate = pageSize > 0 && rows.length > pageSize;
  const pageCount = paginate ? Math.ceil(rows.length / pageSize) : 1;
  const safePage = Math.max(0, Math.min(page, pageCount - 1));
  const start = paginate ? safePage * pageSize : 0;
  return {
    paginate,
    pageCount,
    safePage,
    start,
    visibleRows: paginate ? rows.slice(start, start + pageSize) : rows
  };
}

// webapp/src/lib/format.ts
function displayDimValue(value) {
  return value === NULL_TOKEN || value === "" ? "—" : value;
}
function labelize2(value) {
  return labelize(value);
}
function formatValue(value, hint = {}) {
  return formatUiValue(value, hint);
}
function formatCompact(value, hint = {}) {
  return formatUiCompact(value, hint);
}
function sqlLiteral(value) {
  return `'${value.replaceAll("'", "''")}'`;
}
function filterSummary(filter) {
  if (filter.mode === "contains")
    return `contains ${sqlLiteral(filter.pattern ?? "")}`;
  const { values } = filter;
  const verb = filter.mode === "exclude" ? "is not" : "is";
  if (values.length === 0)
    return verb;
  if (values.length === 1)
    return `${verb} ${displayDimValue(values[0])}`;
  if (values.length <= 2)
    return `${verb} ${values.map(displayDimValue).join(", ")}`;
  return `${verb} ${values.length} values`;
}

// webapp/src/lib/viz.ts
var VIZ_COLOR_COUNT = 7;
function vizColor(index) {
  const slot = (index % VIZ_COLOR_COUNT + VIZ_COLOR_COUNT) % VIZ_COLOR_COUNT;
  return `var(--viz-${slot + 1})`;
}
function axisTicks(min, max, count = 4) {
  if (!(max > min))
    return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, index) => min + step * index);
}
function observeWidth(node, minWidth, onWidth) {
  if (!node || typeof ResizeObserver === "undefined")
    return () => {};
  const observer = new ResizeObserver((entries) => {
    for (const entry of entries)
      onWidth(Math.max(minWidth, entry.contentRect.width));
  });
  observer.observe(node);
  return () => observer.disconnect();
}

// webapp/src/components/BarLineCombo.tsx
import { jsx as jsx2, jsxs as jsxs2, Fragment } from "react/jsx-runtime";
var MARGIN = { top: 14, right: 48, bottom: 26, left: 48 };
function BarLineCombo({
  data,
  barLabel = "Bars",
  lineLabel = "Line",
  height = 220,
  formatBar = formatValue,
  formatLine = formatValue,
  ariaLabel
}) {
  const ref = useRef(null);
  const [width, setWidth] = useState2(640);
  const { tip, handlers } = useChartTooltip();
  useEffect(() => observeWidth(ref.current, 240, setWidth), []);
  if (data.length === 0) {
    return /* @__PURE__ */ jsx2("div", {
      className: "grid h-[220px] place-items-center text-xs text-faint",
      children: "No data to chart."
    });
  }
  const bars = data.map((item) => Number.isFinite(item.bar) ? item.bar : 0);
  const lines = data.map((item) => Number.isFinite(item.line) ? item.line : 0);
  const barMin = Math.min(0, ...bars);
  const barMax = Math.max(0, ...bars);
  const barSpan = barMax - barMin || 1;
  const lineMin = Math.min(...lines);
  const lineMax = Math.max(...lines);
  const lineSpan = lineMax - lineMin || 1;
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const yBar = (value) => MARGIN.top + (1 - (value - barMin) / barSpan) * plotH;
  const yLine = (value) => MARGIN.top + (1 - (value - lineMin) / lineSpan) * plotH;
  const slot = plotW / data.length;
  const barWidth = Math.max(8, Math.min(48, slot * 0.55));
  const xCenter = (index) => MARGIN.left + slot * index + slot / 2;
  const baselineY = yBar(0);
  const linePath = data.map((_, index) => `${xCenter(index).toFixed(1)},${yLine(lines[index]).toFixed(1)}`).join(" L ");
  const lineColor = vizColor(1);
  const summary = ariaLabel || `Combo chart, ${data.length} categories: ${barLabel} bars with a ${lineLabel} line`;
  return /* @__PURE__ */ jsxs2(Fragment, {
    children: [
      /* @__PURE__ */ jsxs2("div", {
        className: "mb-1 flex items-center gap-3 text-2xs text-faint",
        children: [
          /* @__PURE__ */ jsxs2("span", {
            className: "flex items-center gap-1",
            children: [
              /* @__PURE__ */ jsx2("span", {
                "aria-hidden": "true",
                className: "inline-block h-2 w-3 bg-chart-primary"
              }),
              " ",
              barLabel
            ]
          }),
          /* @__PURE__ */ jsxs2("span", {
            className: "flex items-center gap-1",
            children: [
              /* @__PURE__ */ jsx2("span", {
                "aria-hidden": "true",
                className: "inline-block h-0.5 w-3",
                style: { background: lineColor }
              }),
              " ",
              lineLabel
            ]
          })
        ]
      }),
      /* @__PURE__ */ jsxs2("svg", {
        ref,
        role: "img",
        "aria-label": summary,
        className: "w-full overflow-hidden",
        style: { height },
        viewBox: `0 0 ${width} ${height}`,
        children: [
          axisTicks(barMin, barMax, 4).map((tick, index) => {
            const y = yBar(tick);
            return /* @__PURE__ */ jsxs2("g", {
              children: [
                /* @__PURE__ */ jsx2("line", {
                  x1: MARGIN.left,
                  x2: width - MARGIN.right,
                  y1: y,
                  y2: y,
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx2("text", {
                  x: MARGIN.left - 6,
                  y: y + 3,
                  textAnchor: "end",
                  className: "fill-faint text-[10px]",
                  children: formatCompact(tick)
                })
              ]
            }, index);
          }),
          axisTicks(lineMin, lineMax, 4).map((tick, index) => /* @__PURE__ */ jsx2("text", {
            x: width - MARGIN.right + 6,
            y: yLine(tick) + 3,
            textAnchor: "start",
            className: "text-[10px]",
            fill: lineColor,
            children: formatLine(tick)
          }, index)),
          /* @__PURE__ */ jsx2("line", {
            x1: MARGIN.left,
            x2: width - MARGIN.right,
            y1: baselineY,
            y2: baselineY,
            className: "stroke-line"
          }),
          data.map((item, index) => {
            const value = bars[index];
            const valueY = yBar(value);
            return /* @__PURE__ */ jsxs2("g", {
              children: [
                /* @__PURE__ */ jsx2("rect", {
                  x: xCenter(index) - barWidth / 2,
                  y: Math.min(valueY, baselineY),
                  width: barWidth,
                  height: Math.abs(valueY - baselineY),
                  "data-label": item.label,
                  "data-bar": value,
                  "data-line": lines[index],
                  className: value < 0 ? "fill-danger" : "fill-chart-primary",
                  ...handlers(/* @__PURE__ */ jsx2(TooltipRows, {
                    title: item.label,
                    rows: [
                      { label: barLabel, value: formatBar(value), swatch: "var(--chart-primary)" },
                      { label: lineLabel, value: formatLine(lines[index]), swatch: lineColor }
                    ]
                  }))
                }),
                /* @__PURE__ */ jsx2("text", {
                  x: xCenter(index),
                  y: height - 8,
                  textAnchor: "middle",
                  className: "fill-muted text-[10px]",
                  children: item.label.slice(0, 8)
                })
              ]
            }, item.label);
          }),
          /* @__PURE__ */ jsx2("path", {
            d: `M ${linePath}`,
            fill: "none",
            stroke: lineColor,
            strokeWidth: 1.75
          }),
          data.map((item, index) => /* @__PURE__ */ jsx2("circle", {
            cx: xCenter(index),
            cy: yLine(lines[index]),
            r: 3,
            fill: lineColor,
            ...handlers(/* @__PURE__ */ jsx2(TooltipRows, {
              title: item.label,
              rows: [
                { label: barLabel, value: formatBar(bars[index]), swatch: "var(--chart-primary)" },
                { label: lineLabel, value: formatLine(lines[index]), swatch: lineColor }
              ]
            }))
          }, item.label))
        ]
      }),
      /* @__PURE__ */ jsx2(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/ColumnChart.tsx
import { useEffect as useEffect2, useRef as useRef2, useState as useState3 } from "react";
import { jsx as jsx3, jsxs as jsxs3, Fragment as Fragment2 } from "react/jsx-runtime";
var MARGIN2 = { top: 12, right: 14, bottom: 26, left: 44 };
function axisTicks2(min, max, count = 4) {
  if (!(max > min))
    return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, index) => min + step * index);
}
function ColumnChart({ data, height = 200, ariaLabel }) {
  const ref = useRef2(null);
  const [width, setWidth] = useState3(640);
  const { tip, handlers } = useChartTooltip();
  useEffect2(() => {
    const node = ref.current;
    if (!node || typeof ResizeObserver === "undefined")
      return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries)
        setWidth(Math.max(160, entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);
  const values = data.map((item) => Number.isFinite(item.value) ? item.value : 0);
  const min = Math.min(0, ...values);
  const max = Math.max(0, ...values);
  const span = max - min || 1;
  const plotW = width - MARGIN2.left - MARGIN2.right;
  const plotH = height - MARGIN2.top - MARGIN2.bottom;
  const yForValue = (value) => MARGIN2.top + (1 - (value - min) / span) * plotH;
  const baselineY = yForValue(0);
  const slot = plotW / Math.max(data.length, 1);
  const barWidth = Math.max(8, Math.min(48, slot * 0.62));
  const ticks = axisTicks2(min, max, 4);
  const summary = ariaLabel || `Bar chart, ${data.length} categories, up to ${formatCompact(max)}`;
  return /* @__PURE__ */ jsxs3(Fragment2, {
    children: [
      /* @__PURE__ */ jsxs3("svg", {
        ref,
        role: "img",
        "aria-label": summary,
        className: "h-[200px] w-full overflow-hidden",
        viewBox: `0 0 ${width} ${height}`,
        children: [
          ticks.map((tick, index) => {
            const y = yForValue(tick);
            return /* @__PURE__ */ jsxs3("g", {
              children: [
                /* @__PURE__ */ jsx3("line", {
                  x1: MARGIN2.left,
                  x2: width - MARGIN2.right,
                  y1: y,
                  y2: y,
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx3("text", {
                  x: MARGIN2.left - 6,
                  y: y + 3,
                  textAnchor: "end",
                  className: "fill-faint text-[10px]",
                  children: formatCompact(tick)
                })
              ]
            }, index);
          }),
          /* @__PURE__ */ jsx3("line", {
            x1: MARGIN2.left,
            x2: width - MARGIN2.right,
            y1: baselineY,
            y2: baselineY,
            className: "stroke-line"
          }),
          data.map((item, index) => {
            const value = values[index] ?? 0;
            const valueY = yForValue(value);
            const barHeight = Math.abs(valueY - baselineY);
            const x = MARGIN2.left + slot * index + (slot - barWidth) / 2;
            const y = Math.min(valueY, baselineY);
            return /* @__PURE__ */ jsxs3("g", {
              children: [
                /* @__PURE__ */ jsx3("rect", {
                  x,
                  y,
                  width: barWidth,
                  height: barHeight,
                  "data-label": item.label,
                  "data-value": value,
                  "data-tone": value < 0 ? "negative" : "positive",
                  className: value < 0 ? "fill-danger" : "fill-chart-primary",
                  ...handlers(`${item.label}: ${formatValue(value)}`)
                }),
                /* @__PURE__ */ jsx3("text", {
                  x: x + barWidth / 2,
                  y: height - 8,
                  textAnchor: "middle",
                  className: "fill-muted text-[10px]",
                  children: item.label.slice(0, 8)
                })
              ]
            }, item.label);
          })
        ]
      }),
      /* @__PURE__ */ jsx3(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/DataTable.tsx
import { useEffect as useEffect3, useMemo, useState as useState4 } from "react";
import { jsx as jsx4, jsxs as jsxs4 } from "react/jsx-runtime";
function columnTotal(rows, key, kind) {
  if (kind === "count")
    return rows.length;
  const values = rows.map((row) => typeof row[key] === "number" ? row[key] : Number(row[key])).filter(Number.isFinite);
  if (values.length === 0)
    return Number.NaN;
  if (kind === "min")
    return Math.min(...values);
  if (kind === "max")
    return Math.max(...values);
  const sum = values.reduce((total, value) => total + value, 0);
  return kind === "avg" ? sum / values.length : sum;
}
var TOTAL_LABEL = { sum: "Σ", avg: "avg", min: "min", max: "max", count: "n" };
function DataTable({
  columns,
  rows,
  loading,
  sortKey,
  sortDir,
  onSort,
  renderCell,
  pageSize = 50,
  searchable,
  totals,
  stickyHeader = true
}) {
  const [page, setPage] = useState4(0);
  const [search, setSearch] = useState4("");
  const filteredRows = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!searchable || !needle)
      return rows;
    return rows.filter((row) => columns.some((column) => String(row[column.key] ?? "").toLowerCase().includes(needle)));
  }, [rows, columns, search, searchable]);
  const { paginate, pageCount, safePage, start, visibleRows } = paginateRows(filteredRows, page, pageSize);
  useEffect3(() => {
    setPage(0);
  }, [rows, pageSize, sortKey, sortDir, search]);
  const hasTotals = totals && columns.some((column) => totals[column.key]);
  return /* @__PURE__ */ jsxs4("div", {
    className: "overflow-hidden border border-line bg-surface",
    children: [
      searchable ? /* @__PURE__ */ jsxs4("div", {
        className: "flex items-center gap-2 border-b border-line px-3 py-1.5",
        children: [
          /* @__PURE__ */ jsx4("input", {
            type: "search",
            "aria-label": "Search rows",
            placeholder: "Search…",
            value: search,
            onChange: (event) => setSearch(event.target.value),
            className: "w-full max-w-64 rounded-full border border-line bg-surface px-2.5 py-1 text-2xs text-ink placeholder:text-faint"
          }),
          search ? /* @__PURE__ */ jsxs4("span", {
            className: "whitespace-nowrap text-2xs text-faint tnum",
            children: [
              filteredRows.length.toLocaleString(),
              " of ",
              rows.length.toLocaleString()
            ]
          }) : null
        ]
      }) : null,
      /* @__PURE__ */ jsx4("div", {
        className: "overflow-auto",
        children: /* @__PURE__ */ jsxs4("table", {
          className: "w-max min-w-full border-collapse text-xs",
          "data-testid": "pivot-table",
          children: [
            /* @__PURE__ */ jsx4("thead", {
              children: /* @__PURE__ */ jsx4("tr", {
                className: "bg-surface-soft",
                children: columns.map((column) => {
                  const active = sortKey === column.key;
                  return /* @__PURE__ */ jsx4("th", {
                    className: `max-w-80 whitespace-nowrap border-b border-line bg-surface-soft px-3 py-1.5 font-semibold text-faint ${column.numeric ? "min-w-32 text-right" : "min-w-40 text-left"} ${stickyHeader ? "sticky top-0 z-10" : ""}`,
                    children: column.sortable && onSort ? /* @__PURE__ */ jsxs4("button", {
                      type: "button",
                      onClick: () => onSort(column.key),
                      "aria-label": `Sort by ${column.label}${active ? `, currently ${sortDir === "asc" ? "ascending" : "descending"}` : ""}`,
                      className: `table-sort inline-flex max-w-full items-center gap-1 whitespace-nowrap hover:text-ink ${active ? "text-ink" : ""}`,
                      children: [
                        /* @__PURE__ */ jsx4("span", {
                          className: "truncate",
                          children: column.label
                        }),
                        /* @__PURE__ */ jsx4("span", {
                          "aria-hidden": "true",
                          className: "text-[9px]",
                          children: active ? sortDir === "asc" ? "▲" : "▼" : "↕"
                        })
                      ]
                    }) : /* @__PURE__ */ jsx4("span", {
                      className: "block truncate",
                      title: column.label,
                      children: column.label
                    })
                  }, column.key);
                })
              })
            }),
            /* @__PURE__ */ jsx4("tbody", {
              children: loading && filteredRows.length === 0 ? /* @__PURE__ */ jsx4("tr", {
                children: /* @__PURE__ */ jsx4("td", {
                  colSpan: columns.length,
                  className: "px-3 py-6 text-center text-faint",
                  children: "Loading…"
                })
              }) : filteredRows.length === 0 ? /* @__PURE__ */ jsx4("tr", {
                children: /* @__PURE__ */ jsx4("td", {
                  colSpan: columns.length,
                  className: "px-3 py-6 text-center text-faint",
                  children: search ? "No matching rows" : "No rows"
                })
              }) : visibleRows.map((row, index) => /* @__PURE__ */ jsx4("tr", {
                className: "hover:bg-surface-soft",
                children: columns.map((column) => {
                  const cellText = renderCell(column, row[column.key]);
                  return /* @__PURE__ */ jsx4("td", {
                    className: `max-w-80 whitespace-nowrap border-b border-line px-3 py-1.5 text-muted ${column.numeric ? "min-w-32 text-right font-mono tnum text-ink" : "min-w-40"}`,
                    children: /* @__PURE__ */ jsx4("span", {
                      className: "block max-w-80 truncate",
                      title: cellText,
                      children: cellText
                    })
                  }, column.key);
                })
              }, start + index))
            }),
            hasTotals && filteredRows.length > 0 ? /* @__PURE__ */ jsx4("tfoot", {
              children: /* @__PURE__ */ jsx4("tr", {
                className: "bg-surface-soft",
                "data-testid": "table-totals",
                children: columns.map((column) => {
                  const kind = totals?.[column.key];
                  if (!kind)
                    return /* @__PURE__ */ jsx4("td", {
                      className: "border-t border-line px-3 py-1.5"
                    }, column.key);
                  const total = columnTotal(filteredRows, column.key, kind);
                  const text = kind === "count" ? total.toLocaleString() : Number.isFinite(total) ? renderCell(column, total) : "—";
                  return /* @__PURE__ */ jsxs4("td", {
                    "data-total": kind,
                    className: `whitespace-nowrap border-t border-line px-3 py-1.5 font-mono tnum font-medium text-ink ${column.numeric ? "text-right" : "text-left"}`,
                    children: [
                      /* @__PURE__ */ jsx4("span", {
                        "aria-hidden": "true",
                        className: "mr-1 text-2xs text-faint",
                        children: TOTAL_LABEL[kind]
                      }),
                      text
                    ]
                  }, column.key);
                })
              })
            }) : null
          ]
        })
      }),
      paginate ? /* @__PURE__ */ jsxs4("div", {
        "data-testid": "pivot-table-pager",
        className: "flex items-center justify-between gap-3 border-t border-line px-3 py-1 text-2xs text-faint",
        children: [
          /* @__PURE__ */ jsxs4("span", {
            className: "tnum",
            children: [
              start + 1,
              "–",
              Math.min(start + pageSize, filteredRows.length),
              " of ",
              filteredRows.length.toLocaleString(),
              loading ? " · Updating…" : ""
            ]
          }),
          /* @__PURE__ */ jsxs4("div", {
            className: "flex gap-1",
            children: [
              /* @__PURE__ */ jsx4("button", {
                type: "button",
                disabled: safePage === 0,
                onClick: () => setPage((value) => Math.max(0, value - 1)),
                className: "table-pager-button px-2 py-1 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40",
                children: "Prev"
              }),
              /* @__PURE__ */ jsx4("button", {
                type: "button",
                disabled: safePage >= pageCount - 1,
                onClick: () => setPage((value) => Math.min(pageCount - 1, value + 1)),
                className: "table-pager-button px-2 py-1 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40",
                children: "Next"
              })
            ]
          })
        ]
      }) : null
    ]
  });
}
// webapp/src/components/DashboardShell.tsx
import { jsx as jsx5, jsxs as jsxs5 } from "react/jsx-runtime";
function DashboardShell({ title, eyebrow = "Sidemantic", status, toolbar, children }) {
  return /* @__PURE__ */ jsxs5("main", {
    className: "mx-auto max-w-6xl px-4 py-5 text-ink sm:px-6",
    children: [
      /* @__PURE__ */ jsxs5("header", {
        className: "flex flex-wrap items-end justify-between gap-4 border-b border-line pb-4",
        children: [
          /* @__PURE__ */ jsxs5("div", {
            children: [
              /* @__PURE__ */ jsx5("p", {
                className: "text-xs font-medium text-faint",
                children: eyebrow
              }),
              /* @__PURE__ */ jsx5("h1", {
                className: "mt-1 text-2xl font-semibold text-ink",
                children: title
              })
            ]
          }),
          status ? /* @__PURE__ */ jsx5("div", {
            className: "text-sm text-muted",
            children: status
          }) : null
        ]
      }),
      toolbar ? /* @__PURE__ */ jsx5("section", {
        className: "flex flex-wrap gap-2 py-3",
        children: toolbar
      }) : null,
      /* @__PURE__ */ jsx5("section", {
        className: "grid gap-4 py-4",
        children
      })
    ]
  });
}
// webapp/src/components/TimeSeriesChart.tsx
import { useEffect as useEffect4, useRef as useRef3, useState as useState5 } from "react";
import { jsx as jsx6, jsxs as jsxs6 } from "react/jsx-runtime";
var HEIGHT = 280;
var PAD = { top: 14, right: 18, bottom: 26, left: 60 };
function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}
function TimeSeriesChart({
  points,
  comparison,
  formatValue: formatValue2,
  formatAxis = formatValue2,
  formatLabel = (label) => label,
  comparisonLabel = "Previous",
  ariaLabel,
  onBrush
}) {
  const container = useRef3(null);
  const svgRef = useRef3(null);
  const dragging = useRef3(false);
  const brushRef = useRef3(null);
  const [width, setWidth] = useState5(820);
  const [hover, setHover] = useState5(null);
  const [brush, setBrush] = useState5(null);
  useEffect4(() => {
    const element = container.current;
    if (!element)
      return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries)
        setWidth(Math.max(320, entry.contentRect.width));
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  const count = points.length;
  const all = [...points, ...comparison ?? []].map((point) => point.y).filter(Number.isFinite);
  const empty = count < 2 || all.length === 0;
  const min = empty ? 0 : Math.min(0, ...all);
  const max = empty ? 1 : Math.max(...all);
  const span = max - min || 1;
  const plotW = width - PAD.left - PAD.right;
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const xAt = (index) => PAD.left + (count <= 1 ? 0 : index / (count - 1) * plotW);
  const yAt = (value) => PAD.top + (1 - (value - min) / span) * plotH;
  const indexAtX = (px) => clamp(Math.round((px - PAD.left) / plotW * (count - 1)), 0, count - 1);
  const pathFor = (series) => series.map((point, index) => `${xAt(index).toFixed(1)},${yAt(point.y).toFixed(1)}`).join(" L ");
  const gappedPath = (series) => {
    const segments = [];
    let run = [];
    series.forEach((point, index) => {
      if (Number.isFinite(point.y)) {
        run.push(`${xAt(index).toFixed(1)},${yAt(point.y).toFixed(1)}`);
      } else if (run.length) {
        segments.push(run.join(" L "));
        run = [];
      }
    });
    if (run.length)
      segments.push(run.join(" L "));
    return segments.map((segment) => `M ${segment}`).join(" ");
  };
  const line = pathFor(points);
  const area = empty ? "" : `M ${xAt(0).toFixed(1)},${yAt(min).toFixed(1)} L ${line} L ${xAt(count - 1).toFixed(1)},${yAt(min).toFixed(1)} Z`;
  function pxFromEvent(event) {
    const rect = svgRef.current?.getBoundingClientRect();
    return rect ? event.clientX - rect.left : 0;
  }
  function onMove(event) {
    if (empty)
      return;
    const px = pxFromEvent(event);
    setHover(indexAtX(px));
    if (dragging.current && brushRef.current) {
      brushRef.current = { a: brushRef.current.a, b: px };
      setBrush({ ...brushRef.current });
    }
  }
  function onDown(event) {
    if (empty || !onBrush)
      return;
    event.currentTarget.setPointerCapture(event.pointerId);
    dragging.current = true;
    const px = pxFromEvent(event);
    brushRef.current = { a: px, b: px };
    setBrush({ ...brushRef.current });
  }
  function onUp(event) {
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    const drag = brushRef.current;
    if (dragging.current && drag && onBrush) {
      const i0 = indexAtX(Math.min(drag.a, drag.b));
      const i1 = indexAtX(Math.max(drag.a, drag.b));
      if (i1 > i0)
        onBrush({ from: points[i0].x, to: points[i1].x });
    }
    dragging.current = false;
    brushRef.current = null;
    setBrush(null);
  }
  function onLeave() {
    setHover(null);
    dragging.current = false;
    brushRef.current = null;
    setBrush(null);
  }
  const labelEvery = Math.max(1, Math.ceil(count / 8));
  const ticks = [max, min + span * 0.66, min + span * 0.33, min];
  const safeHover = hover != null && hover >= 0 && hover < count ? hover : null;
  const hoverCur = safeHover != null ? points[safeHover] : null;
  const hoverPrevRaw = safeHover != null ? comparison?.[safeHover] ?? null : null;
  const hoverPrev = hoverPrevRaw && Number.isFinite(hoverPrevRaw.y) ? hoverPrevRaw : null;
  const tooltipLeft = safeHover != null ? clamp(xAt(safeHover), 80, width - 80) : 0;
  const delta = hoverCur && hoverPrev && hoverPrev.y !== 0 ? (hoverCur.y - hoverPrev.y) / Math.abs(hoverPrev.y) * 100 : null;
  const summary = ariaLabel ?? (empty ? "Time series chart: not enough data to plot." : `Time series chart, ${count} points from ${formatLabel(points[0].x)} to ${formatLabel(points[count - 1].x)}.`);
  return /* @__PURE__ */ jsxs6("div", {
    className: "relative text-chart-primary",
    children: [
      /* @__PURE__ */ jsxs6("div", {
        className: "flex items-center justify-end gap-3 px-3 pt-2 text-2xs text-faint",
        children: [
          /* @__PURE__ */ jsxs6("span", {
            className: "flex items-center gap-1",
            children: [
              /* @__PURE__ */ jsx6("span", {
                className: "inline-block h-0.5 w-3 bg-chart-primary"
              }),
              " Current"
            ]
          }),
          comparison?.length ? /* @__PURE__ */ jsxs6("span", {
            className: "flex items-center gap-1",
            children: [
              /* @__PURE__ */ jsx6("span", {
                className: "inline-block h-0 w-3 border-t border-dashed border-faint"
              }),
              " ",
              comparisonLabel
            ]
          }) : null
        ]
      }),
      /* @__PURE__ */ jsx6("div", {
        ref: container,
        className: "w-full",
        children: empty ? /* @__PURE__ */ jsx6("div", {
          className: "grid h-[280px] place-items-center text-xs text-faint",
          children: "Not enough data to chart."
        }) : /* @__PURE__ */ jsxs6("svg", {
          ref: svgRef,
          width,
          height: HEIGHT,
          role: "img",
          "aria-label": summary,
          className: "block touch-none select-none",
          onPointerMove: onMove,
          onPointerDown: onDown,
          onPointerUp: onUp,
          onPointerLeave: onLeave,
          onPointerCancel: onLeave,
          onDoubleClick: () => onBrush?.(null),
          children: [
            /* @__PURE__ */ jsx6("defs", {
              children: /* @__PURE__ */ jsxs6("linearGradient", {
                id: "ts-fill",
                x1: "0",
                y1: "0",
                x2: "0",
                y2: "1",
                children: [
                  /* @__PURE__ */ jsx6("stop", {
                    offset: "0%",
                    stopColor: "currentColor",
                    stopOpacity: 0.18
                  }),
                  /* @__PURE__ */ jsx6("stop", {
                    offset: "100%",
                    stopColor: "currentColor",
                    stopOpacity: 0
                  })
                ]
              })
            }),
            ticks.map((value, index) => /* @__PURE__ */ jsxs6("g", {
              children: [
                /* @__PURE__ */ jsx6("line", {
                  x1: PAD.left,
                  x2: width - PAD.right,
                  y1: yAt(value),
                  y2: yAt(value),
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx6("text", {
                  x: PAD.left - 8,
                  y: yAt(value) + 3,
                  textAnchor: "end",
                  className: "fill-faint font-mono text-[10px]",
                  children: formatAxis(value)
                })
              ]
            }, index)),
            comparison && comparison.length >= 2 ? /* @__PURE__ */ jsx6("path", {
              d: gappedPath(comparison),
              fill: "none",
              className: "stroke-faint",
              strokeWidth: 1.25,
              strokeDasharray: "4 3"
            }) : null,
            /* @__PURE__ */ jsx6("path", {
              d: area,
              fill: "url(#ts-fill)"
            }),
            /* @__PURE__ */ jsx6("path", {
              d: `M ${line}`,
              fill: "none",
              stroke: "currentColor",
              strokeWidth: 1.75
            }),
            brush ? /* @__PURE__ */ jsx6("rect", {
              x: Math.min(brush.a, brush.b),
              y: PAD.top,
              width: Math.abs(brush.b - brush.a),
              height: plotH,
              className: "fill-chart-primary",
              opacity: 0.12
            }) : null,
            safeHover != null && hoverCur ? /* @__PURE__ */ jsxs6("g", {
              children: [
                /* @__PURE__ */ jsx6("line", {
                  x1: xAt(safeHover),
                  x2: xAt(safeHover),
                  y1: PAD.top,
                  y2: HEIGHT - PAD.bottom,
                  className: "stroke-faint",
                  strokeDasharray: "3 3"
                }),
                hoverPrev ? /* @__PURE__ */ jsx6("circle", {
                  cx: xAt(safeHover),
                  cy: yAt(hoverPrev.y),
                  r: 3,
                  className: "fill-faint"
                }) : null,
                /* @__PURE__ */ jsx6("circle", {
                  cx: xAt(safeHover),
                  cy: yAt(hoverCur.y),
                  r: 3.5,
                  fill: "currentColor"
                })
              ]
            }) : null,
            points.map((point, index) => index % labelEvery === 0 || index === count - 1 ? /* @__PURE__ */ jsx6("text", {
              x: xAt(index),
              y: HEIGHT - 8,
              textAnchor: "middle",
              className: "fill-faint font-mono text-[10px]",
              children: formatLabel(point.x)
            }, point.x) : null)
          ]
        })
      }),
      hoverCur ? /* @__PURE__ */ jsxs6("div", {
        className: "pointer-events-none absolute top-8 z-20 -translate-x-1/2 whitespace-nowrap border border-line bg-surface px-2 py-1.5 text-2xs shadow-[var(--shadow)]",
        style: { left: tooltipLeft },
        children: [
          /* @__PURE__ */ jsx6("div", {
            className: "mb-0.5 font-mono text-faint",
            children: formatLabel(hoverCur.x)
          }),
          /* @__PURE__ */ jsxs6("div", {
            className: "flex items-center justify-between gap-3",
            children: [
              /* @__PURE__ */ jsx6("span", {
                className: "text-muted",
                children: "Current"
              }),
              /* @__PURE__ */ jsx6("span", {
                className: "font-mono tnum font-medium text-ink",
                children: formatValue2(hoverCur.y)
              })
            ]
          }),
          hoverPrev ? /* @__PURE__ */ jsxs6("div", {
            className: "flex items-center justify-between gap-3",
            children: [
              /* @__PURE__ */ jsx6("span", {
                className: "text-muted",
                children: comparisonLabel
              }),
              /* @__PURE__ */ jsx6("span", {
                className: "font-mono tnum text-muted",
                children: formatValue2(hoverPrev.y)
              })
            ]
          }) : null,
          delta != null ? /* @__PURE__ */ jsxs6("div", {
            className: `mt-0.5 text-right font-mono ${delta > 0 ? "text-success" : delta < 0 ? "text-danger" : "text-faint"}`,
            children: [
              delta.toLocaleString(undefined, { maximumFractionDigits: 1, signDisplay: "exceptZero" }),
              "%"
            ]
          }) : null
        ]
      }) : null
    ]
  });
}

// webapp/src/components/DistributionAdapters.tsx
import { jsx as jsx7 } from "react/jsx-runtime";
function DataPreviewTable({ result, pageSize = 10 }) {
  const columns = result?.columns ?? [];
  return /* @__PURE__ */ jsx7(DataTable, {
    columns: columns.map((key) => ({ key, label: labelize2(key), numeric: result?.sample_rows.some((row) => typeof row[key] === "number") })),
    rows: result?.sample_rows ?? [],
    pageSize,
    renderCell: (_column, value) => formatValue(value)
  });
}
function LineChart({ data, height = 200, ariaLabel }) {
  return /* @__PURE__ */ jsx7("div", {
    style: { minHeight: height },
    children: /* @__PURE__ */ jsx7(TimeSeriesChart, {
      points: data.map(({ label, value }) => ({ x: label, y: value })),
      formatValue: (value) => formatValue(value),
      ariaLabel
    })
  });
}
// webapp/src/components/DonutChart.tsx
import { useEffect as useEffect5, useRef as useRef4, useState as useState6 } from "react";
import { jsx as jsx8, jsxs as jsxs7 } from "react/jsx-runtime";
var TAU = Math.PI * 2;
function donutSegments(data) {
  const positive = data.map((item, index) => ({ ...item, colorIndex: index })).filter((item) => Number.isFinite(item.value) && item.value > 0);
  const total = positive.reduce((sum, item) => sum + item.value, 0);
  if (total <= 0)
    return [];
  let angle = -Math.PI / 2;
  return positive.map((item) => {
    const share = item.value / total;
    const startAngle = angle;
    angle += share * TAU;
    return { label: item.label, value: item.value, share, startAngle, endAngle: angle, colorIndex: item.colorIndex };
  });
}
function arcPath(cx, cy, r0, r1, a0, a1) {
  const sweep = Math.min(a1 - a0, TAU - 0.0001);
  const b1 = a0 + sweep;
  const large = sweep > Math.PI ? 1 : 0;
  const x0o = cx + r1 * Math.cos(a0);
  const y0o = cy + r1 * Math.sin(a0);
  const x1o = cx + r1 * Math.cos(b1);
  const y1o = cy + r1 * Math.sin(b1);
  const x0i = cx + r0 * Math.cos(b1);
  const y0i = cy + r0 * Math.sin(b1);
  const x1i = cx + r0 * Math.cos(a0);
  const y1i = cy + r0 * Math.sin(a0);
  return [
    `M ${x0o.toFixed(2)} ${y0o.toFixed(2)}`,
    `A ${r1} ${r1} 0 ${large} 1 ${x1o.toFixed(2)} ${y1o.toFixed(2)}`,
    `L ${x0i.toFixed(2)} ${y0i.toFixed(2)}`,
    `A ${r0} ${r0} 0 ${large} 0 ${x1i.toFixed(2)} ${y1i.toFixed(2)}`,
    "Z"
  ].join(" ");
}
function DonutChart({ data, height = 200, centerLabel = "Total", format = formatValue, ariaLabel }) {
  const ref = useRef4(null);
  const [width, setWidth] = useState6(360);
  const { tip, handlers } = useChartTooltip();
  useEffect5(() => observeWidth(ref.current, 220, setWidth), []);
  const segments = donutSegments(data);
  const total = segments.reduce((sum, segment) => sum + segment.value, 0);
  const size = Math.min(height, width * 0.55);
  const cx = size / 2;
  const cy = height / 2;
  const r1 = size / 2 - 6;
  const r0 = Math.max(r1 * 0.62, r1 - 34);
  const summary = ariaLabel || `Donut chart, ${segments.length} segments totaling ${formatCompact(total)}`;
  if (segments.length === 0) {
    return /* @__PURE__ */ jsx8("div", {
      className: "grid h-[200px] place-items-center text-xs text-faint",
      children: "No positive values to chart."
    });
  }
  return /* @__PURE__ */ jsxs7("div", {
    ref,
    className: "flex w-full items-center gap-4",
    style: { height },
    children: [
      /* @__PURE__ */ jsxs7("svg", {
        role: "img",
        "aria-label": summary,
        width: size,
        height,
        className: "shrink-0 overflow-hidden",
        children: [
          segments.map((segment) => /* @__PURE__ */ jsx8("path", {
            d: arcPath(cx, cy, r0, r1, segment.startAngle, segment.endAngle),
            fill: vizColor(segment.colorIndex),
            stroke: "var(--surface)",
            strokeWidth: 1,
            "data-label": segment.label,
            "data-value": segment.value,
            ...handlers(/* @__PURE__ */ jsx8(TooltipRows, {
              title: segment.label,
              rows: [
                { label: "Value", value: format(segment.value), swatch: vizColor(segment.colorIndex) },
                { label: "Share", value: `${(segment.share * 100).toFixed(1)}%` }
              ]
            }))
          }, segment.label)),
          /* @__PURE__ */ jsx8("text", {
            x: cx,
            y: cy - 4,
            textAnchor: "middle",
            className: "fill-ink font-mono tnum text-sm font-medium",
            children: formatCompact(total)
          }),
          /* @__PURE__ */ jsx8("text", {
            x: cx,
            y: cy + 12,
            textAnchor: "middle",
            className: "fill-faint text-[10px]",
            children: centerLabel
          })
        ]
      }),
      /* @__PURE__ */ jsx8("ul", {
        className: "min-w-0 flex-1 space-y-1 text-2xs",
        children: segments.map((segment) => /* @__PURE__ */ jsxs7("li", {
          className: "flex items-center gap-2",
          "data-label": segment.label,
          children: [
            /* @__PURE__ */ jsx8("span", {
              "aria-hidden": "true",
              className: "inline-block size-2 shrink-0",
              style: { background: vizColor(segment.colorIndex) }
            }),
            /* @__PURE__ */ jsx8("span", {
              className: "truncate text-muted",
              children: segment.label
            }),
            /* @__PURE__ */ jsx8("span", {
              className: "ml-auto font-mono tnum text-ink",
              children: format(segment.value)
            }),
            /* @__PURE__ */ jsxs7("span", {
              className: "w-10 text-right font-mono tnum text-faint",
              children: [
                (segment.share * 100).toFixed(1),
                "%"
              ]
            })
          ]
        }, segment.label))
      }),
      /* @__PURE__ */ jsx8(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/ErrorBoundary.tsx
import { Component } from "react";
import { jsx as jsx9, jsxs as jsxs8 } from "react/jsx-runtime";

class ErrorBoundary extends Component {
  state = {};
  static getDerivedStateFromError(error) {
    return { error };
  }
  render() {
    if (this.state.error) {
      return /* @__PURE__ */ jsx9("div", {
        className: "p-4",
        children: /* @__PURE__ */ jsxs8("div", {
          className: "border border-danger/40 bg-surface p-4",
          children: [
            /* @__PURE__ */ jsx9("p", {
              className: "text-sm font-semibold text-danger",
              children: "Something went wrong rendering this view."
            }),
            /* @__PURE__ */ jsx9("p", {
              className: "mt-1 break-words text-xs text-muted",
              children: this.state.error.message
            }),
            /* @__PURE__ */ jsxs8("div", {
              className: "mt-3 flex gap-2",
              children: [
                /* @__PURE__ */ jsx9("button", {
                  type: "button",
                  onClick: () => this.setState({ error: undefined }),
                  className: "border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink",
                  children: "Retry"
                }),
                /* @__PURE__ */ jsx9("button", {
                  type: "button",
                  onClick: () => window.location.reload(),
                  className: "border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink",
                  children: "Reload"
                })
              ]
            })
          ]
        })
      });
    }
    return this.props.children;
  }
}
// webapp/src/components/FilterPill.tsx
import { useState as useState9 } from "react";

// webapp/src/components/FilterEditor.tsx
import { useEffect as useEffect8, useId, useMemo as useMemo3, useRef as useRef6, useState as useState8 } from "react";

// webapp/src/lib/time.ts
var ALL_GRAINS = ["hour", "day", "week", "month", "quarter", "year"];
function isoDate(date) {
  return date.toISOString().slice(0, 10);
}
function dateOnly(value) {
  return value.slice(0, 10);
}
function parseISO(value) {
  return new Date(`${dateOnly(value)}T00:00:00Z`);
}
function addDays(value, days) {
  const date = parseISO(value);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}
var DATE_PRESETS = [
  { key: "7d", label: "Last 7 days", days: 7 },
  { key: "28d", label: "Last 28 days", days: 28 },
  { key: "90d", label: "Last 90 days", days: 90 },
  { key: "180d", label: "Last 180 days", days: 180 },
  { key: "365d", label: "Last 12 months", days: 365 }
];
function presetRange(days, today = new Date) {
  const to = isoDate(today);
  return { from: addDays(to, -(days - 1)), to };
}
function timeFilters(ref, range) {
  return [`${ref} >= cast('${range.from}' as date)`, `${ref} < cast('${addDays(range.to, 1)}' as date)`];
}

// webapp/src/lib/queries.ts
function isEmptyFilter(filter) {
  return filter.mode === "contains" ? !filter.pattern : filter.values.length === 0;
}
function filterLiteral(value, type) {
  if ((type === "numeric" || type === "number") && value.trim() !== "" && Number.isFinite(Number(value))) {
    return value;
  }
  if (type === "boolean") {
    const lower = value.toLowerCase();
    if (lower === "true" || lower === "false")
      return lower;
  }
  return sqlLiteral(value);
}
function likeEscape(pattern) {
  return pattern.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_");
}
function membershipExpr(dimRef, filter, type) {
  const negate = filter.mode === "exclude";
  const hasNull = filter.values.includes(NULL_TOKEN);
  const present = filter.values.filter((value) => value !== NULL_TOKEN);
  let presentExpr = null;
  if (present.length === 1) {
    presentExpr = `${dimRef} ${negate ? "!=" : "="} ${filterLiteral(present[0], type)}`;
  } else if (present.length > 1) {
    const list = present.map((v) => filterLiteral(v, type)).join(", ");
    presentExpr = `${dimRef} ${negate ? "NOT IN" : "IN"} (${list})`;
  }
  if (!negate) {
    const parts = [];
    if (presentExpr)
      parts.push(presentExpr);
    if (hasNull)
      parts.push(`${dimRef} IS NULL`);
    if (parts.length === 0)
      return null;
    return parts.length === 1 ? parts[0] : `(${parts.join(" OR ")})`;
  }
  if (hasNull) {
    const parts = [];
    if (presentExpr)
      parts.push(presentExpr);
    parts.push(`${dimRef} IS NOT NULL`);
    return parts.length === 1 ? parts[0] : `(${parts.join(" AND ")})`;
  }
  if (!presentExpr)
    return null;
  return `(${presentExpr} OR ${dimRef} IS NULL)`;
}
function filterExprs(filters, opts = {}) {
  const out = [];
  for (const [dimRef, filter] of Object.entries(filters)) {
    if (dimRef === opts.excludeDim || isEmptyFilter(filter))
      continue;
    const type = opts.types?.[dimRef];
    if (filter.mode === "contains") {
      const pat = sqlLiteral(`%${likeEscape(filter.pattern ?? "")}%`);
      out.push(`CAST(${dimRef} AS VARCHAR) ILIKE ${pat} ESCAPE '\\'`);
      continue;
    }
    const expr = membershipExpr(dimRef, filter, type);
    if (expr)
      out.push(expr);
  }
  return out;
}
function composeFilters(filters, opts = {}) {
  const base = filterExprs(filters, { types: opts.types, excludeDim: opts.excludeDim });
  if (opts.timeRef && opts.range)
    base.push(...timeFilters(opts.timeRef, opts.range));
  return base;
}
function distinctValues(dimRef, filters, limit = 50) {
  return { dimensions: [dimRef], filters, orderBy: [`${dimRef} ASC`], limit };
}

// webapp/src/state/ExplorerContext.tsx
import { createContext, useContext, useEffect as useEffect6, useMemo as useMemo2, useReducer } from "react";

// webapp/src/state/url.ts
var GRAINS = new Set(ALL_GRAINS);
var CONTEXT_COLUMNS = new Set(["none", "pctTotal", "delta", "deltaPct"]);
var COMPARISONS = new Set(["off", "previous", "year", "custom"]);
var FILTER_MODES = new Set(["include", "exclude", "contains"]);

// webapp/src/state/ExplorerContext.tsx
import { jsx as jsx10 } from "react/jsx-runtime";
var ExplorerContext = createContext(null);
function useExplorer() {
  const value = useContext(ExplorerContext);
  if (!value)
    throw new Error("useExplorer must be used within ExplorerProvider");
  return value;
}

// webapp/src/state/useQueryResult.ts
import { useEffect as useEffect7, useRef as useRef5, useState as useState7 } from "react";

// webapp/src/state/queryActivity.ts
import { useSyncExternalStore } from "react";
var store = { active: 0, listeners: new Set };
function emit() {
  for (const listener of store.listeners)
    listener();
}
function beginQuery() {
  store.active += 1;
  emit();
}
function endQuery() {
  store.active = Math.max(0, store.active - 1);
  emit();
}

// webapp/src/state/useQueryResult.ts
var DEBOUNCE_MS = 80;
function useQueryResult(backend, query) {
  const [state, setState] = useState7({ loading: false });
  const token = useRef5(0);
  const key = query ? JSON.stringify(query) : null;
  useEffect7(() => {
    if (!query) {
      setState({ loading: false });
      return;
    }
    const current = ++token.current;
    setState((prev) => ({ result: prev.result, loading: true }));
    const timer = setTimeout(() => {
      beginQuery();
      backend.runQuery(query).then((result) => {
        if (current === token.current)
          setState({ result, loading: false });
      }).catch((err) => {
        if (current === token.current) {
          setState({ loading: false, error: err instanceof Error ? err.message : String(err) });
        }
      }).finally(() => endQuery());
    }, DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [key, backend]);
  return state;
}

// webapp/src/components/FilterEditor.tsx
import { jsx as jsx11, jsxs as jsxs9, Fragment as Fragment3 } from "react/jsx-runtime";
var MODES = [
  { mode: "include", label: "Include" },
  { mode: "exclude", label: "Exclude" },
  { mode: "contains", label: "Contains" }
];
var VALUE_LIMIT = 50;
var SEARCH_DEBOUNCE_MS = 200;
function useDebounced(value, delayMs) {
  const [debounced, setDebounced] = useState8(value);
  useEffect8(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);
  return debounced;
}
function FilterEditor({
  dim,
  model,
  onClose
}) {
  const { state, dispatch, backend } = useExplorer();
  const filter = state.filters[dim.ref];
  const [mode, setModeState] = useState8(filter?.mode ?? "include");
  const selected = useMemo3(() => new Set(filter?.mode !== "contains" ? filter?.values ?? [] : []), [filter]);
  const panelRef = useRef6(null);
  const searchRef = useRef6(null);
  const labelId = useId();
  const [search, setSearch] = useState8("");
  const debouncedSearch = useDebounced(search, SEARCH_DEBOUNCE_MS);
  const pattern = filter?.mode === "contains" ? filter.pattern ?? "" : "";
  const [patternDraft, setPatternDraft] = useState8(pattern);
  const debouncedPattern = useDebounced(patternDraft, SEARCH_DEBOUNCE_MS);
  useEffect8(() => {
    const opener = document.activeElement;
    searchRef.current?.focus();
    return () => opener?.focus?.();
  }, []);
  useEffect8(() => {
    function onKey(event) {
      if (event.key === "Escape") {
        event.stopPropagation();
        onClose();
      }
    }
    function onPointer(event) {
      if (panelRef.current && !panelRef.current.contains(event.target))
        onClose();
    }
    document.addEventListener("keydown", onKey, true);
    document.addEventListener("mousedown", onPointer, true);
    return () => {
      document.removeEventListener("keydown", onKey, true);
      document.removeEventListener("mousedown", onPointer, true);
    };
  }, [onClose]);
  useEffect8(() => {
    if (mode !== "contains")
      return;
    if (debouncedPattern === pattern)
      return;
    dispatch({ type: "setFilterPattern", dim: dim.ref, pattern: debouncedPattern });
  }, [debouncedPattern, mode, dim.ref, dispatch]);
  const timeRef = model.timeDimension?.ref;
  const valueFilters = useMemo3(() => {
    const base = composeFilters(state.filters, { timeRef, range: state.dateRange, excludeDim: dim.ref });
    if (debouncedSearch.trim()) {
      const pat = sqlLiteral(`%${likeEscape(debouncedSearch.trim())}%`);
      base.push(`CAST(${dim.ref} AS VARCHAR) ILIKE ${pat} ESCAPE '\\'`);
    }
    return base;
  }, [state.filters, timeRef, state.dateRange, dim.ref, debouncedSearch]);
  const listMode = mode !== "contains";
  const { result, loading, error } = useQueryResult(backend, listMode ? distinctValues(dim.ref, valueFilters, VALUE_LIMIT) : null);
  const dimAlias = aliasOf(dim.ref);
  const values = useMemo3(() => {
    if (!result)
      return [];
    return result.rows.map((row) => {
      const raw = row[dimAlias];
      return raw === null || raw === undefined ? NULL_TOKEN : String(raw);
    });
  }, [result, dimAlias]);
  const stale = !!result && result.rows.length > 0 && !result.columns.includes(dimAlias);
  const showSkeleton = listMode && (loading || stale);
  function setMode(next) {
    setModeState(next);
    if (filter)
      dispatch({ type: "setFilterMode", dim: dim.ref, mode: next });
    if (next === "contains")
      setPatternDraft(pattern);
  }
  function onKeyDown(event) {
    if (event.key !== "Tab")
      return;
    const focusable = panelRef.current?.querySelectorAll('button, input, [href], select, textarea, [tabindex]:not([tabindex="-1"])');
    if (!focusable || focusable.length === 0)
      return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }
  return /* @__PURE__ */ jsxs9("div", {
    ref: panelRef,
    role: "dialog",
    "aria-modal": "true",
    "aria-labelledby": labelId,
    onKeyDown,
    className: "absolute left-0 z-50 mt-1 w-64 border border-line bg-surface p-2 text-2xs shadow-lg",
    children: [
      /* @__PURE__ */ jsxs9("div", {
        id: labelId,
        className: "mb-2 flex items-baseline justify-between gap-2",
        children: [
          /* @__PURE__ */ jsx11("span", {
            className: "truncate font-semibold text-ink",
            children: dim.label
          }),
          /* @__PURE__ */ jsx11("button", {
            type: "button",
            "aria-label": "Close filter editor",
            onClick: onClose,
            className: "grid size-4 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink",
            children: "×"
          })
        ]
      }),
      /* @__PURE__ */ jsx11("div", {
        role: "group",
        "aria-label": "Filter mode",
        className: "mb-2 grid grid-cols-3 gap-px border border-line bg-line",
        children: MODES.map(({ mode: m, label }) => /* @__PURE__ */ jsx11("button", {
          type: "button",
          "aria-pressed": mode === m,
          onClick: () => setMode(m),
          className: `px-1.5 py-1 text-center ${mode === m ? "bg-accent-soft font-medium text-accent" : "bg-surface text-muted hover:bg-surface-soft"}`,
          children: label
        }, m))
      }),
      mode === "contains" ? /* @__PURE__ */ jsx11("input", {
        ref: searchRef,
        type: "text",
        "aria-label": `${dim.label} contains`,
        placeholder: "Substring…",
        value: patternDraft,
        onChange: (event) => setPatternDraft(event.target.value),
        className: "w-full border border-line bg-surface px-1.5 py-1 text-2xs text-ink placeholder:text-faint"
      }) : /* @__PURE__ */ jsxs9(Fragment3, {
        children: [
          /* @__PURE__ */ jsx11("input", {
            ref: searchRef,
            type: "text",
            "aria-label": `Search ${dim.label} values`,
            placeholder: "Search values…",
            value: search,
            onChange: (event) => setSearch(event.target.value),
            className: "w-full border border-line bg-surface px-1.5 py-1 text-2xs text-ink placeholder:text-faint"
          }),
          /* @__PURE__ */ jsx11("div", {
            className: "mt-2 max-h-56 overflow-y-auto",
            role: "group",
            "aria-label": `${dim.label} values`,
            children: error ? /* @__PURE__ */ jsx11("p", {
              className: "px-1 py-2 text-danger",
              children: error
            }) : showSkeleton ? /* @__PURE__ */ jsx11("div", {
              className: "space-y-1.5 p-1",
              children: [0, 1, 2, 3, 4].map((i) => /* @__PURE__ */ jsx11("div", {
                className: "skeleton h-4 w-full"
              }, i))
            }) : values.length === 0 ? /* @__PURE__ */ jsx11("p", {
              className: "px-1 py-2 text-faint",
              children: "No values"
            }) : values.map((value) => {
              const checked = selected.has(value);
              return /* @__PURE__ */ jsxs9("label", {
                className: "flex cursor-pointer items-center gap-2 px-1 py-1 hover:bg-surface-soft",
                children: [
                  /* @__PURE__ */ jsx11("input", {
                    type: "checkbox",
                    checked,
                    onChange: () => dispatch({ type: "toggleFilter", dim: dim.ref, value, mode }),
                    className: "size-3 accent-[var(--accent)]"
                  }),
                  /* @__PURE__ */ jsx11("span", {
                    className: "min-w-0 truncate text-ink",
                    children: displayDimValue(value)
                  })
                ]
              }, value);
            })
          })
        ]
      }),
      /* @__PURE__ */ jsxs9("div", {
        className: "mt-2 flex items-center justify-between border-t border-line pt-2",
        children: [
          /* @__PURE__ */ jsx11("button", {
            type: "button",
            onClick: () => dispatch({ type: "removeFilterDim", dim: dim.ref }),
            className: "text-muted underline-offset-2 hover:text-ink hover:underline",
            children: "Clear"
          }),
          /* @__PURE__ */ jsx11("button", {
            type: "button",
            onClick: onClose,
            className: "border border-line px-2 py-1 text-muted hover:bg-surface-soft",
            children: "Done"
          })
        ]
      })
    ]
  });
}

// webapp/src/components/FilterPill.tsx
import { jsx as jsx12, jsxs as jsxs10 } from "react/jsx-runtime";
function FilterPill(props) {
  const [open, setOpen] = useState9(false);
  if (!("dim" in props)) {
    return /* @__PURE__ */ jsxs10("span", {
      "data-dimension": props.dimension,
      "data-value": props.value,
      className: "inline-flex max-w-full items-center gap-1 rounded-full bg-surface-soft px-2 py-0.5 text-2xs leading-4 text-muted",
      children: [
        /* @__PURE__ */ jsxs10("span", {
          className: "truncate",
          children: [
            /* @__PURE__ */ jsxs10("span", {
              className: "text-faint",
              children: [
                props.dimensionLabel ?? props.dimension,
                ":"
              ]
            }),
            " ",
            props.value
          ]
        }),
        props.onRemove ? /* @__PURE__ */ jsx12("button", {
          type: "button",
          "aria-label": `Remove filter ${props.value}`,
          onClick: props.onRemove,
          className: "-mr-0.5 px-0.5 text-faint hover:text-ink",
          children: "×"
        }) : null
      ]
    });
  }
  const { dim, model, filter, onRemove } = props;
  return /* @__PURE__ */ jsxs10("span", {
    className: "relative inline-flex max-w-full items-center",
    "data-dimension": dim.ref,
    "data-mode": filter.mode,
    children: [
      /* @__PURE__ */ jsxs10("span", {
        className: "inline-flex max-w-full items-center gap-1 rounded-full bg-surface-soft px-2 py-0.5 text-2xs leading-4 text-muted",
        children: [
          /* @__PURE__ */ jsxs10("button", {
            type: "button",
            "aria-label": `Edit filter ${dim.label}`,
            "aria-haspopup": "dialog",
            "aria-expanded": open,
            onClick: () => setOpen((v) => !v),
            className: "min-w-0 truncate text-left hover:text-ink",
            children: [
              /* @__PURE__ */ jsx12("span", {
                className: "text-faint",
                children: dim.label
              }),
              " ",
              filterSummary(filter)
            ]
          }),
          /* @__PURE__ */ jsx12("button", {
            type: "button",
            "aria-label": `Remove filter ${dim.label}`,
            onClick: onRemove,
            className: "grid size-3.5 shrink-0 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink",
            children: "×"
          })
        ]
      }),
      open ? /* @__PURE__ */ jsx12(FilterEditor, {
        dim,
        model,
        onClose: () => setOpen(false)
      }) : null
    ]
  });
}
// webapp/src/components/HeatmapChart.tsx
import { useEffect as useEffect9, useRef as useRef7, useState as useState10 } from "react";
import { jsx as jsx13, jsxs as jsxs11, Fragment as Fragment4 } from "react/jsx-runtime";
var MARGIN3 = { top: 8, right: 8, bottom: 26, left: 76 };
function orderedLabels(cells, key, explicit) {
  if (explicit?.length)
    return explicit;
  const seen = [];
  for (const cell of cells)
    if (!seen.includes(cell[key]))
      seen.push(cell[key]);
  return seen;
}
function HeatmapChart({ cells, xLabels, yLabels, height = 240, format = formatValue, ariaLabel }) {
  const ref = useRef7(null);
  const [width, setWidth] = useState10(640);
  const { tip, handlers } = useChartTooltip();
  useEffect9(() => observeWidth(ref.current, 240, setWidth), []);
  const xs = orderedLabels(cells, "x", xLabels);
  const ys = orderedLabels(cells, "y", yLabels);
  if (xs.length === 0 || ys.length === 0) {
    return /* @__PURE__ */ jsx13("div", {
      className: "grid h-[240px] place-items-center text-xs text-faint",
      children: "No cells to chart."
    });
  }
  const byKey = new Map(cells.map((cell) => [`${cell.x}\x00${cell.y}`, cell.value]));
  const magnitudes = cells.map((cell) => Math.abs(cell.value)).filter(Number.isFinite);
  const maxMagnitude = Math.max(...magnitudes, 0) || 1;
  const plotW = width - MARGIN3.left - MARGIN3.right;
  const plotH = height - MARGIN3.top - MARGIN3.bottom;
  const cellW = plotW / xs.length;
  const cellH = plotH / ys.length;
  const xLabelEvery = Math.max(1, Math.ceil(xs.length / Math.max(1, Math.floor(plotW / 56))));
  const summary = ariaLabel || `Heatmap, ${xs.length} by ${ys.length} cells`;
  return /* @__PURE__ */ jsxs11(Fragment4, {
    children: [
      /* @__PURE__ */ jsxs11("svg", {
        ref,
        role: "img",
        "aria-label": summary,
        className: "w-full overflow-hidden",
        style: { height },
        viewBox: `0 0 ${width} ${height}`,
        children: [
          ys.map((yLabel, row) => /* @__PURE__ */ jsx13("text", {
            x: MARGIN3.left - 6,
            y: MARGIN3.top + row * cellH + cellH / 2 + 3,
            textAnchor: "end",
            className: "fill-muted text-[10px]",
            children: yLabel.slice(0, 12)
          }, yLabel)),
          xs.map((xLabel, col) => col % xLabelEvery === 0 ? /* @__PURE__ */ jsx13("text", {
            x: MARGIN3.left + col * cellW + cellW / 2,
            y: height - 8,
            textAnchor: "middle",
            className: "fill-muted text-[10px]",
            children: xLabel.slice(0, 8)
          }, xLabel) : null),
          ys.map((yLabel, row) => xs.map((xLabel, col) => {
            const value = byKey.get(`${xLabel}\x00${yLabel}`);
            const known = value != null && Number.isFinite(value);
            const intensity = known ? 0.06 + 0.94 * (Math.abs(value) / maxMagnitude) : 0;
            return /* @__PURE__ */ jsx13("rect", {
              x: MARGIN3.left + col * cellW + 0.5,
              y: MARGIN3.top + row * cellH + 0.5,
              width: Math.max(1, cellW - 1),
              height: Math.max(1, cellH - 1),
              "data-x": xLabel,
              "data-y": yLabel,
              "data-value": known ? value : undefined,
              className: known && value < 0 ? "fill-danger" : "fill-chart-primary",
              fillOpacity: known ? intensity : 0.03,
              ...handlers(/* @__PURE__ */ jsx13(TooltipRows, {
                title: `${yLabel} / ${xLabel}`,
                rows: [{ label: "Value", value: known ? format(value) : "no data" }]
              }))
            }, `${xLabel}\x00${yLabel}`);
          }))
        ]
      }),
      /* @__PURE__ */ jsx13(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/HistogramChart.tsx
import { useEffect as useEffect10, useRef as useRef8, useState as useState11 } from "react";
import { jsx as jsx14, jsxs as jsxs12, Fragment as Fragment5 } from "react/jsx-runtime";
function binValues(values, bins) {
  const finite = values.filter(Number.isFinite);
  if (finite.length === 0)
    return [];
  const min = Math.min(...finite);
  const max = Math.max(...finite);
  const count = Math.max(1, Math.floor(bins ?? Math.min(40, Math.ceil(Math.log2(finite.length) + 1))));
  if (min === max)
    return [{ x0: min, x1: max, count: finite.length }];
  const width = (max - min) / count;
  const result = Array.from({ length: count }, (_, index) => ({
    x0: min + index * width,
    x1: min + (index + 1) * width,
    count: 0
  }));
  for (const value of finite) {
    const index = Math.min(count - 1, Math.floor((value - min) / width));
    result[index].count += 1;
  }
  return result;
}
var MARGIN4 = { top: 12, right: 14, bottom: 26, left: 44 };
function HistogramChart({ values, bins, height = 200, format = formatCompact, ariaLabel }) {
  const ref = useRef8(null);
  const [width, setWidth] = useState11(640);
  const { tip, handlers } = useChartTooltip();
  useEffect10(() => observeWidth(ref.current, 160, setWidth), []);
  const data = binValues(values, bins);
  if (data.length === 0) {
    return /* @__PURE__ */ jsx14("div", {
      className: "grid h-[200px] place-items-center text-xs text-faint",
      children: "No numeric values to chart."
    });
  }
  const maxCount = Math.max(...data.map((bin) => bin.count), 1);
  const plotW = width - MARGIN4.left - MARGIN4.right;
  const plotH = height - MARGIN4.top - MARGIN4.bottom;
  const yFor = (count) => MARGIN4.top + (1 - count / maxCount) * plotH;
  const slot = plotW / data.length;
  const ticks = axisTicks(0, maxCount, 4);
  const labelEvery = Math.max(1, Math.ceil(data.length / 6));
  const summary = ariaLabel || `Histogram, ${data.length} bins from ${format(data[0].x0)} to ${format(data[data.length - 1].x1)}`;
  return /* @__PURE__ */ jsxs12(Fragment5, {
    children: [
      /* @__PURE__ */ jsxs12("svg", {
        ref,
        role: "img",
        "aria-label": summary,
        className: "w-full overflow-hidden",
        style: { height },
        viewBox: `0 0 ${width} ${height}`,
        children: [
          ticks.map((tick, index) => {
            const y = yFor(tick);
            return /* @__PURE__ */ jsxs12("g", {
              children: [
                /* @__PURE__ */ jsx14("line", {
                  x1: MARGIN4.left,
                  x2: width - MARGIN4.right,
                  y1: y,
                  y2: y,
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx14("text", {
                  x: MARGIN4.left - 6,
                  y: y + 3,
                  textAnchor: "end",
                  className: "fill-faint text-[10px]",
                  children: formatCompact(tick)
                })
              ]
            }, index);
          }),
          data.map((bin, index) => {
            const x = MARGIN4.left + slot * index;
            const y = yFor(bin.count);
            return /* @__PURE__ */ jsx14("rect", {
              x: x + 0.5,
              y,
              width: Math.max(1, slot - 1),
              height: MARGIN4.top + plotH - y,
              "data-x0": bin.x0,
              "data-x1": bin.x1,
              "data-count": bin.count,
              className: "fill-chart-primary",
              ...handlers(/* @__PURE__ */ jsx14(TooltipRows, {
                title: `${format(bin.x0)} – ${format(bin.x1)}`,
                rows: [{ label: "Count", value: bin.count.toLocaleString() }]
              }))
            }, index);
          }),
          data.map((bin, index) => index % labelEvery === 0 ? /* @__PURE__ */ jsx14("text", {
            x: MARGIN4.left + slot * index,
            y: height - 8,
            textAnchor: "middle",
            className: "fill-muted text-[10px]",
            children: format(bin.x0)
          }, index) : null)
        ]
      }),
      /* @__PURE__ */ jsx14(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/Leaderboard.tsx
import { jsx as jsx15, jsxs as jsxs13 } from "react/jsx-runtime";
var CONTEXT_TONE = {
  positive: "text-success",
  negative: "text-danger",
  neutral: "text-faint"
};
function Leaderboard({
  dimension,
  title,
  metricLabel,
  rows,
  selectedValues = [],
  loading,
  formatMetric,
  onToggle,
  contextColumn = "none",
  contextOptions,
  onContextColumn,
  collapsedLimit = 6,
  expanded = false,
  onExpandedChange
}) {
  const selected = new Set(selectedValues);
  const visibleRows = expanded ? rows : rows.slice(0, collapsedLimit);
  const maxMagnitude = Math.max(1, ...visibleRows.map((row) => Math.abs(row.metric)));
  const expandable = expanded || rows.length > collapsedLimit;
  const showContext = contextColumn !== "none";
  const rowGrid = showContext ? "grid-cols-[minmax(0,1fr)_auto_auto]" : "grid-cols-[minmax(0,1fr)_auto]";
  return /* @__PURE__ */ jsxs13("section", {
    "data-testid": "dimension-leaderboard",
    "data-dimension": dimension,
    "data-expanded": expanded || undefined,
    "aria-label": `${title}, ranked by ${metricLabel}`,
    className: "flex min-h-40 flex-col border-b border-r border-line bg-surface data-[expanded=true]:col-span-full",
    children: [
      /* @__PURE__ */ jsxs13("header", {
        className: "flex items-center justify-between gap-3 px-3 pb-2 pt-2.5",
        children: [
          /* @__PURE__ */ jsxs13("div", {
            className: "flex min-w-0 items-baseline gap-2",
            children: [
              /* @__PURE__ */ jsx15("h3", {
                className: "truncate text-sm font-semibold text-ink",
                children: title
              }),
              /* @__PURE__ */ jsxs13("p", {
                className: "sr-only",
                children: [
                  "Ranked by ",
                  metricLabel
                ]
              })
            ]
          }),
          contextOptions && onContextColumn ? /* @__PURE__ */ jsx15("div", {
            role: "group",
            "aria-label": "Context column",
            "data-testid": "leaderboard-context-toggle",
            className: "flex shrink-0 overflow-hidden border border-line text-2xs",
            children: contextOptions.map((option) => /* @__PURE__ */ jsx15("button", {
              type: "button",
              title: option.title,
              "aria-pressed": contextColumn === option.key,
              "data-context": option.key,
              "data-active": contextColumn === option.key || undefined,
              onClick: () => onContextColumn(option.key),
              className: "border-l border-line px-1.5 py-0.5 font-mono text-faint first:border-l-0 hover:bg-surface-soft data-[active=true]:bg-accent-soft data-[active=true]:text-accent",
              children: option.label
            }, option.key))
          }) : null
        ]
      }),
      /* @__PURE__ */ jsx15("div", {
        "data-testid": "leaderboard-rows",
        children: loading && rows.length === 0 ? /* @__PURE__ */ jsx15("div", {
          className: "space-y-2 p-3",
          children: [0, 1, 2, 3].map((i) => /* @__PURE__ */ jsx15("div", {
            className: "skeleton h-5 w-full"
          }, i))
        }) : rows.length === 0 ? /* @__PURE__ */ jsx15("p", {
          className: "px-3 py-4 text-xs text-faint",
          children: "No values"
        }) : visibleRows.map((row) => {
          const tone = row.metric < 0 ? "negative" : "positive";
          const isSelected = selected.has(row.value);
          const width = `${Math.round(Math.abs(row.metric) / maxMagnitude * 100)}%`;
          return /* @__PURE__ */ jsxs13("button", {
            type: "button",
            "data-dimension": dimension,
            "data-value": row.value,
            "data-selected": isSelected || undefined,
            "data-tone": tone,
            onClick: () => onToggle?.(row.value),
            "aria-pressed": isSelected,
            className: `leaderboard-row relative grid w-full ${rowGrid} items-center gap-3 overflow-hidden border-0 bg-transparent px-3 py-1 text-left text-xs text-ink data-[selected=true]:bg-chart-primary-selected`,
            children: [
              /* @__PURE__ */ jsx15("span", {
                "aria-hidden": "true",
                className: `absolute inset-y-0 left-0 ${tone === "negative" ? "bg-danger-soft" : "bg-chart-primary-soft"}`,
                style: { width }
              }),
              /* @__PURE__ */ jsx15("span", {
                className: "relative min-w-0 truncate text-muted",
                children: displayDimValue(row.value)
              }),
              /* @__PURE__ */ jsx15("strong", {
                className: "relative tnum font-semibold text-ink",
                children: formatMetric(row.metric)
              }),
              showContext ? /* @__PURE__ */ jsx15("span", {
                "data-testid": "leaderboard-context",
                "data-tone": row.context?.tone ?? "neutral",
                className: `relative w-14 text-right font-mono tnum text-2xs ${CONTEXT_TONE[row.context?.tone ?? "neutral"]}`,
                children: row.context?.label ?? "—"
              }) : null
            ]
          }, `${dimension}:${row.value}`);
        })
      }),
      expandable && !loading ? /* @__PURE__ */ jsx15("button", {
        type: "button",
        "data-action": expanded ? "leaderboard-back" : "leaderboard-expand",
        "aria-expanded": expanded,
        onClick: () => onExpandedChange?.(!expanded),
        className: "leaderboard-expand border-0 border-t border-line bg-transparent px-3 py-1 text-left text-xs font-normal text-faint hover:text-accent",
        children: expanded ? "← All dimensions" : `Expand table (${rows.length})`
      }) : null
    ]
  });
}
// webapp/src/components/MetricCard.tsx
import { useState as useState13 } from "react";

// webapp/src/components/Sparkline.tsx
import { useEffect as useEffect11, useId as useId2, useRef as useRef9, useState as useState12 } from "react";
import { jsx as jsx16, jsxs as jsxs14, Fragment as Fragment6 } from "react/jsx-runtime";
function Sparkline({
  values,
  labels,
  height = 44,
  ariaLabel,
  formatValue: formatValue2 = (value) => value.toLocaleString(undefined, { maximumFractionDigits: 2 }),
  onHover,
  onBrush
}) {
  const containerRef = useRef9(null);
  const gradientId = useId2();
  const svgRef = useRef9(null);
  const dragStart = useRef9(null);
  const [width, setWidth] = useState12(200);
  const [hover, setHover] = useState12(null);
  const [brush, setBrush] = useState12(null);
  const [tip, setTip] = useState12(null);
  useEffect11(() => {
    const node = containerRef.current;
    if (!node || typeof ResizeObserver === "undefined")
      return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries)
        setWidth(Math.max(40, entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);
  const points = values.map((value, index) => ({ index, value })).filter((point) => Number.isFinite(point.value));
  if (points.length < 2) {
    return /* @__PURE__ */ jsx16("svg", {
      ref: svgRef,
      role: "img",
      "aria-label": ariaLabel || "No trend data",
      className: "h-11 w-full",
      viewBox: `0 0 ${width} ${height}`
    });
  }
  const pad = 3;
  const min = Math.min(...points.map((point) => point.value));
  const max = Math.max(...points.map((point) => point.value));
  const span = max - min || 1;
  const coordinates = points.map((point, index) => ({
    ...point,
    x: pad + index / (points.length - 1) * (width - pad * 2),
    y: pad + (1 - (point.value - min) / span) * (height - pad * 2)
  }));
  const line = coordinates.map(({ x, y }) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" L ");
  const area = `M ${coordinates[0].x.toFixed(1)},${height - pad} L ${line} L ${coordinates.at(-1).x.toFixed(1)},${height - pad} Z`;
  const latest = coordinates.at(-1);
  const summary = ariaLabel || `Trend of ${points.length} points, latest ${formatValue2(latest.value)}`;
  function localX(event) {
    const rect = svgRef.current?.getBoundingClientRect();
    return rect ? Math.max(pad, Math.min(width - pad, (event.clientX - rect.left) / rect.width * width)) : pad;
  }
  function indexAt(x) {
    return Math.max(0, Math.min(coordinates.length - 1, Math.round((x - pad) / (width - pad * 2) * (coordinates.length - 1))));
  }
  function move(event) {
    const x = localX(event);
    const index = indexAt(x);
    const point = coordinates[index];
    setHover(index);
    setTip({
      content: `${labels?.[point.index] ? `${labels[point.index]}: ` : ""}${formatValue2(point.value)}`,
      x: event.clientX,
      y: event.clientY
    });
    onHover?.({ index: point.index, label: labels?.[point.index], value: point.value });
    if (dragStart.current !== null)
      setBrush({ a: dragStart.current, b: x });
  }
  function down(event) {
    if (!onBrush || !labels?.length)
      return;
    event.currentTarget.setPointerCapture(event.pointerId);
    const x = localX(event);
    dragStart.current = x;
    setBrush({ a: x, b: x });
  }
  function up(event) {
    if (dragStart.current === null || !onBrush || !labels?.length)
      return;
    if (event.currentTarget.hasPointerCapture(event.pointerId))
      event.currentTarget.releasePointerCapture(event.pointerId);
    const end = localX(event);
    if (Math.abs(end - dragStart.current) > 6) {
      const startPoint = coordinates[indexAt(Math.min(dragStart.current, end))];
      const endPoint = coordinates[indexAt(Math.max(dragStart.current, end))];
      onBrush({ from: labels[startPoint.index], to: labels[endPoint.index] });
    }
    dragStart.current = null;
    setBrush(null);
  }
  function leave() {
    setHover(null);
    setTip(null);
    onHover?.(null);
    if (dragStart.current === null)
      setBrush(null);
  }
  const hovered = hover === null ? null : coordinates[hover];
  return /* @__PURE__ */ jsxs14("span", {
    ref: containerRef,
    className: "relative block w-full",
    children: [
      /* @__PURE__ */ jsxs14("svg", {
        ref: svgRef,
        role: "img",
        "aria-label": summary,
        className: `h-11 w-full overflow-hidden text-chart-primary ${onBrush ? "touch-none select-none" : ""}`,
        viewBox: `0 0 ${width} ${height}`,
        preserveAspectRatio: "none",
        onPointerMove: move,
        onPointerDown: down,
        onPointerUp: up,
        onPointerCancel: leave,
        onPointerLeave: leave,
        onDoubleClick: () => onBrush?.(null),
        children: [
          /* @__PURE__ */ jsx16("defs", {
            children: /* @__PURE__ */ jsxs14("linearGradient", {
              id: gradientId,
              x1: "0",
              y1: "0",
              x2: "0",
              y2: "1",
              children: [
                /* @__PURE__ */ jsx16("stop", {
                  offset: "0%",
                  stopColor: "currentColor",
                  stopOpacity: 0.16
                }),
                /* @__PURE__ */ jsx16("stop", {
                  offset: "100%",
                  stopColor: "currentColor",
                  stopOpacity: 0
                })
              ]
            })
          }),
          /* @__PURE__ */ jsx16("path", {
            d: area,
            fill: `url(#${gradientId})`
          }),
          /* @__PURE__ */ jsx16("path", {
            d: `M ${line}`,
            fill: "none",
            stroke: "currentColor",
            strokeWidth: 1.5,
            vectorEffect: "non-scaling-stroke"
          }),
          brush ? /* @__PURE__ */ jsx16("rect", {
            x: Math.min(brush.a, brush.b),
            y: 0,
            width: Math.abs(brush.b - brush.a),
            height,
            fill: "currentColor",
            opacity: 0.12
          }) : null,
          hovered ? /* @__PURE__ */ jsxs14(Fragment6, {
            children: [
              /* @__PURE__ */ jsx16("line", {
                x1: hovered.x,
                x2: hovered.x,
                y1: 0,
                y2: height,
                stroke: "currentColor",
                strokeWidth: 1,
                opacity: 0.45
              }),
              /* @__PURE__ */ jsx16("circle", {
                cx: hovered.x,
                cy: hovered.y,
                r: 2.5,
                fill: "currentColor"
              })
            ]
          }) : /* @__PURE__ */ jsx16("circle", {
            cx: latest.x,
            cy: latest.y,
            r: 2.25,
            fill: "currentColor"
          })
        ]
      }),
      /* @__PURE__ */ jsx16(ChartTooltip, {
        tip
      })
    ]
  });
}

// webapp/src/components/MetricCard.tsx
import { jsx as jsx17, jsxs as jsxs15, Fragment as Fragment7 } from "react/jsx-runtime";
var TONE_CLASS = {
  positive: "text-success",
  negative: "text-danger",
  neutral: "text-faint"
};
var TONE_ARROW = { positive: "▲", negative: "▼", neutral: "·" };
function MetricCard({
  metric,
  label,
  value,
  valueText,
  format,
  delta,
  comparison,
  sparkValues = [],
  sparkLabels,
  selected,
  loading,
  onSelect,
  onSparkHover,
  onSparkBrush
}) {
  const [sparkHover, setSparkHover] = useState13(null);
  const summary = /* @__PURE__ */ jsxs15(Fragment7, {
    children: [
      /* @__PURE__ */ jsxs15("div", {
        className: "flex items-baseline justify-between gap-2",
        children: [
          /* @__PURE__ */ jsx17("span", {
            className: "truncate text-xs font-medium text-muted",
            children: label
          }),
          sparkHover?.label ? /* @__PURE__ */ jsx17("span", {
            className: "shrink-0 font-mono text-2xs text-faint",
            children: sparkHover.label
          }) : null
        ]
      }),
      /* @__PURE__ */ jsx17("div", {
        className: "font-mono tnum text-[19px] font-semibold leading-tight tracking-tight text-ink",
        children: loading ? /* @__PURE__ */ jsx17("span", {
          className: "skeleton inline-block h-6 w-24 align-middle"
        }) : sparkHover ? formatValue(sparkHover.value, format) : valueText ?? formatValue(value, format)
      }),
      delta || comparison ? /* @__PURE__ */ jsxs15("div", {
        className: "flex items-baseline gap-1 text-2xs",
        children: [
          delta ? /* @__PURE__ */ jsxs15("span", {
            "data-tone": delta.tone,
            className: `font-mono tnum font-medium ${TONE_CLASS[delta.tone]}`,
            children: [
              /* @__PURE__ */ jsx17("span", {
                "aria-hidden": "true",
                className: "mr-0.5 text-[8px]",
                children: TONE_ARROW[delta.tone]
              }),
              delta.label
            ]
          }) : null,
          comparison ? /* @__PURE__ */ jsx17("span", {
            className: "truncate text-faint",
            children: comparison
          }) : null
        ]
      }) : null
    ]
  });
  const className = "group flex w-full flex-col gap-1.5 overflow-hidden rounded-xl border border-line bg-surface px-3.5 pt-3 text-left shadow-[var(--shadow-sm)] transition-colors hover:border-line-strong data-[selected=true]:border-accent";
  const sparkline = /* @__PURE__ */ jsx17("div", {
    className: "-mx-3.5 mt-auto",
    children: /* @__PURE__ */ jsx17(Sparkline, {
      values: sparkValues,
      labels: sparkLabels,
      onHover: (point) => {
        setSparkHover(point);
        onSparkHover?.(point);
      },
      onBrush: onSparkBrush,
      formatValue: (sparkValue) => formatValue(sparkValue, format)
    })
  });
  if (!onSelect) {
    return /* @__PURE__ */ jsxs15("article", {
      "data-metric": metric,
      "data-selected": selected || undefined,
      className,
      children: [
        summary,
        sparkline
      ]
    });
  }
  return /* @__PURE__ */ jsxs15("article", {
    "data-metric": metric,
    "data-selected": selected || undefined,
    className,
    children: [
      /* @__PURE__ */ jsx17("button", {
        type: "button",
        "data-metric": metric,
        "aria-pressed": !!selected,
        onClick: () => onSelect(metric),
        className: "-m-1 flex flex-col gap-1 border-0 bg-transparent p-1 text-left transition hover:opacity-75",
        children: summary
      }),
      sparkline
    ]
  });
}
// webapp/src/components/NetworkChart.tsx
import { useEffect as useEffect12, useMemo as useMemo4, useRef as useRef10, useState as useState14 } from "react";
import { jsx as jsx18, jsxs as jsxs16, Fragment as Fragment8 } from "react/jsx-runtime";
function hashSeed(text) {
  let hash = 2166136261;
  for (let index = 0;index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}
function mulberry32(seed) {
  let state = seed || 1;
  return () => {
    state |= 0;
    state = state + 1831565813 | 0;
    let t = Math.imul(state ^ state >>> 15, 1 | state);
    t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
}
function layoutNetwork(nodes, links, { width = 640, height = 320, iterations = 150 } = {}) {
  if (nodes.length === 0)
    return [];
  const random = mulberry32(hashSeed(nodes.map((node) => node.id).join("\x00")));
  const index = new Map(nodes.map((node, position) => [node.id, position]));
  const xs = nodes.map(() => random() * width);
  const ys = nodes.map(() => random() * height);
  const dxs = nodes.map(() => 0);
  const dys = nodes.map(() => 0);
  const degree = nodes.map(() => 0);
  const edges = [];
  for (const link of links) {
    const a = index.get(link.source);
    const b = index.get(link.target);
    if (a == null || b == null || a === b)
      continue;
    edges.push([a, b]);
    degree[a] += 1;
    degree[b] += 1;
  }
  const area = width * height;
  const k = Math.sqrt(area / nodes.length);
  let temperature = Math.max(width, height) / 8;
  const cool = temperature / (iterations + 1);
  for (let step = 0;step < iterations; step += 1) {
    dxs.fill(0);
    dys.fill(0);
    for (let a = 0;a < nodes.length; a += 1) {
      for (let b = a + 1;b < nodes.length; b += 1) {
        let deltaX = xs[a] - xs[b];
        let deltaY = ys[a] - ys[b];
        let distance = Math.hypot(deltaX, deltaY);
        if (distance < 0.01) {
          deltaX = 0.01 * (a - b);
          deltaY = 0.01;
          distance = Math.hypot(deltaX, deltaY);
        }
        const repulse = k * k / distance;
        dxs[a] += deltaX / distance * repulse;
        dys[a] += deltaY / distance * repulse;
        dxs[b] -= deltaX / distance * repulse;
        dys[b] -= deltaY / distance * repulse;
      }
    }
    for (const [a, b] of edges) {
      const deltaX = xs[a] - xs[b];
      const deltaY = ys[a] - ys[b];
      const distance = Math.hypot(deltaX, deltaY) || 0.01;
      const attract = distance * distance / k;
      dxs[a] -= deltaX / distance * attract;
      dys[a] -= deltaY / distance * attract;
      dxs[b] += deltaX / distance * attract;
      dys[b] += deltaY / distance * attract;
    }
    for (let node = 0;node < nodes.length; node += 1) {
      const displacement = Math.hypot(dxs[node], dys[node]) || 0.01;
      const limited = Math.min(displacement, temperature);
      xs[node] += dxs[node] / displacement * limited;
      ys[node] += dys[node] / displacement * limited;
    }
    temperature -= cool;
  }
  const pad = 24;
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const spanX = maxX - minX || 1;
  const spanY = maxY - minY || 1;
  return nodes.map((node, position) => ({
    ...node,
    x: pad + (xs[position] - minX) / spanX * (width - pad * 2),
    y: pad + (ys[position] - minY) / spanY * (height - pad * 2),
    degree: degree[position]
  }));
}
function NetworkChart({ nodes, links, height = 320, ariaLabel }) {
  const ref = useRef10(null);
  const [width, setWidth] = useState14(640);
  const { tip, handlers } = useChartTooltip();
  useEffect12(() => observeWidth(ref.current, 240, setWidth), []);
  const positioned = useMemo4(() => layoutNetwork(nodes, links, { width, height }), [nodes, links, width, height]);
  const byId = useMemo4(() => new Map(positioned.map((node) => [node.id, node])), [positioned]);
  const groups = useMemo4(() => [...new Set(nodes.map((node) => node.group ?? ""))], [nodes]);
  const maxWeight = Math.max(...links.map((link) => link.weight ?? 1), 1);
  if (positioned.length === 0) {
    return /* @__PURE__ */ jsx18("div", {
      className: "grid h-[320px] place-items-center text-xs text-faint",
      children: "No nodes to chart."
    });
  }
  const summary = ariaLabel || `Network graph, ${nodes.length} nodes and ${links.length} links`;
  return /* @__PURE__ */ jsxs16(Fragment8, {
    children: [
      groups.length > 1 ? /* @__PURE__ */ jsx18("div", {
        className: "mb-1 flex flex-wrap items-center gap-3 text-2xs text-faint",
        children: groups.map((group, index) => /* @__PURE__ */ jsxs16("span", {
          className: "flex items-center gap-1",
          children: [
            /* @__PURE__ */ jsx18("span", {
              "aria-hidden": "true",
              className: "inline-block size-2 rounded-full",
              style: { background: vizColor(index) }
            }),
            group || "(default)"
          ]
        }, group || "(default)"))
      }) : null,
      /* @__PURE__ */ jsxs16("svg", {
        ref,
        role: "img",
        "aria-label": summary,
        className: "w-full overflow-hidden",
        style: { height },
        viewBox: `0 0 ${width} ${height}`,
        children: [
          links.map((link, index) => {
            const source = byId.get(link.source);
            const target = byId.get(link.target);
            if (!source || !target)
              return null;
            return /* @__PURE__ */ jsx18("line", {
              x1: source.x,
              y1: source.y,
              x2: target.x,
              y2: target.y,
              className: "stroke-line",
              strokeWidth: 0.75 + 2.25 * ((link.weight ?? 1) / maxWeight),
              "data-source": link.source,
              "data-target": link.target
            }, index);
          }),
          positioned.map((node) => {
            const radius = 5 + Math.min(9, node.degree * 1.5);
            return /* @__PURE__ */ jsxs16("g", {
              children: [
                /* @__PURE__ */ jsx18("circle", {
                  cx: node.x,
                  cy: node.y,
                  r: radius,
                  fill: vizColor(groups.indexOf(node.group ?? "")),
                  stroke: "var(--surface)",
                  strokeWidth: 1.5,
                  "data-id": node.id,
                  "data-degree": node.degree,
                  ...handlers(/* @__PURE__ */ jsx18(TooltipRows, {
                    title: node.label ?? node.id,
                    rows: [
                      { label: "Connections", value: String(node.degree) },
                      ...node.group ? [{ label: "Group", value: node.group, swatch: vizColor(groups.indexOf(node.group)) }] : []
                    ]
                  }))
                }),
                /* @__PURE__ */ jsx18("text", {
                  x: node.x,
                  y: node.y - radius - 4,
                  textAnchor: "middle",
                  className: "pointer-events-none fill-muted text-[10px]",
                  children: (node.label ?? node.id).slice(0, 14)
                })
              ]
            }, node.id);
          })
        ]
      }),
      /* @__PURE__ */ jsx18(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/QueryDebugPanel.tsx
import { jsx as jsx19, jsxs as jsxs17 } from "react/jsx-runtime";
var SQL_KEYWORDS = new Set([
  "and",
  "as",
  "asc",
  "by",
  "case",
  "cast",
  "count",
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
  "with"
]);
function tokenizeSql(source) {
  const tokens = [];
  let index = 0;
  while (index < source.length) {
    const rest = source.slice(index);
    const comment = rest.match(/^--[^\n]*/);
    const string = rest.match(/^'(?:''|[^'])*'/);
    const number = rest.match(/^\b\d+(?:\.\d+)?\b/);
    const word = rest.match(/^[A-Za-z_][A-Za-z0-9_]*/);
    const match = comment ?? string ?? number ?? word;
    if (!match) {
      tokens.push({ kind: "plain", value: source[index] });
      index += 1;
      continue;
    }
    const value = match[0];
    const kind = comment ? "comment" : string ? "string" : number ? "number" : SQL_KEYWORDS.has(value.toLowerCase()) ? "keyword" : "plain";
    tokens.push({ kind, value });
    index += value.length;
  }
  return tokens;
}
var TOKEN_CLASS = {
  comment: "italic text-faint",
  string: "text-accent",
  number: "text-danger",
  keyword: "font-semibold text-accent",
  plain: ""
};
function QueryDebugPanel({ queries, inputs = {} }) {
  const text = Object.entries(queries).filter(([, sql]) => sql).map(([name, sql]) => `-- ${name}
${sql}`).join(`

`);
  const tokens = tokenizeSql(text || "No queries yet.");
  return /* @__PURE__ */ jsxs17("details", {
    className: "border border-line bg-surface",
    children: [
      /* @__PURE__ */ jsx19("summary", {
        className: "cursor-pointer px-3 py-2 text-xs font-medium text-muted",
        children: "Generated SQL"
      }),
      Object.keys(inputs).length > 0 ? /* @__PURE__ */ jsx19("div", {
        "data-testid": "query-inputs",
        className: "grid gap-px border-t border-line bg-line sm:grid-cols-2",
        children: Object.entries(inputs).map(([name, input]) => input ? /* @__PURE__ */ jsxs17("section", {
          className: "min-w-0 bg-surface px-3 py-2 text-2xs",
          children: [
            /* @__PURE__ */ jsx19("h3", {
              className: "mb-1 font-semibold text-ink",
              children: name
            }),
            [
              ["Metrics", input.metrics],
              ["Dimensions", input.dimensions],
              ["Filters", input.filters]
            ].map(([label, values]) => values?.length ? /* @__PURE__ */ jsxs17("p", {
              className: "truncate text-muted",
              title: values.join(", "),
              children: [
                /* @__PURE__ */ jsxs17("strong", {
                  className: "font-medium text-faint",
                  children: [
                    label,
                    ":"
                  ]
                }),
                " ",
                values.join(", ")
              ]
            }, label) : null)
          ]
        }, name) : null)
      }) : null,
      /* @__PURE__ */ jsx19("pre", {
        "data-testid": "query-debug",
        className: "max-h-72 overflow-auto whitespace-pre-wrap border-t border-line px-3 py-2 font-mono text-2xs text-muted",
        children: tokens.map((token, index) => TOKEN_CLASS[token.kind] ? /* @__PURE__ */ jsx19("span", {
          className: TOKEN_CLASS[token.kind],
          "data-token": token.kind,
          children: token.value
        }, index) : token.value)
      })
    ]
  });
}
// webapp/src/components/ScatterChart.tsx
import { useEffect as useEffect13, useRef as useRef11, useState as useState15 } from "react";
import { jsx as jsx20, jsxs as jsxs18, Fragment as Fragment9 } from "react/jsx-runtime";
var MARGIN5 = { top: 12, right: 14, bottom: 30, left: 48 };
function ScatterChart({
  points,
  height = 240,
  xLabel,
  yLabel,
  formatX = formatCompact,
  formatY = formatCompact,
  ariaLabel
}) {
  const ref = useRef11(null);
  const [width, setWidth] = useState15(640);
  const { tip, handlers } = useChartTooltip();
  useEffect13(() => observeWidth(ref.current, 200, setWidth), []);
  const finite = points.filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
  if (finite.length === 0) {
    return /* @__PURE__ */ jsx20("div", {
      className: "grid h-[240px] place-items-center text-xs text-faint",
      children: "No points to chart."
    });
  }
  const series = [...new Set(finite.map((point) => point.series ?? ""))];
  const xs = finite.map((point) => point.x);
  const ys = finite.map((point) => point.y);
  const xMin = Math.min(...xs);
  const xMax = Math.max(...xs);
  const yMin = Math.min(...ys);
  const yMax = Math.max(...ys);
  const xSpan = xMax - xMin || 1;
  const ySpan = yMax - yMin || 1;
  const plotW = width - MARGIN5.left - MARGIN5.right;
  const plotH = height - MARGIN5.top - MARGIN5.bottom;
  const xFor = (value) => MARGIN5.left + (value - xMin) / xSpan * plotW;
  const yFor = (value) => MARGIN5.top + (1 - (value - yMin) / ySpan) * plotH;
  const summary = ariaLabel || `Scatter plot, ${finite.length} points${xLabel && yLabel ? ` of ${yLabel} by ${xLabel}` : ""}`;
  return /* @__PURE__ */ jsxs18(Fragment9, {
    children: [
      series.length > 1 ? /* @__PURE__ */ jsx20("div", {
        className: "mb-1 flex flex-wrap items-center gap-3 text-2xs text-faint",
        children: series.map((name, index) => /* @__PURE__ */ jsxs18("span", {
          className: "flex items-center gap-1",
          children: [
            /* @__PURE__ */ jsx20("span", {
              "aria-hidden": "true",
              className: "inline-block size-2 rounded-full",
              style: { background: vizColor(index) }
            }),
            name || "(default)"
          ]
        }, name || "(default)"))
      }) : null,
      /* @__PURE__ */ jsxs18("svg", {
        ref,
        role: "img",
        "aria-label": summary,
        className: "w-full overflow-hidden",
        style: { height },
        viewBox: `0 0 ${width} ${height}`,
        children: [
          axisTicks(yMin, yMax, 4).map((tick, index) => {
            const y = yFor(tick);
            return /* @__PURE__ */ jsxs18("g", {
              children: [
                /* @__PURE__ */ jsx20("line", {
                  x1: MARGIN5.left,
                  x2: width - MARGIN5.right,
                  y1: y,
                  y2: y,
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx20("text", {
                  x: MARGIN5.left - 6,
                  y: y + 3,
                  textAnchor: "end",
                  className: "fill-faint text-[10px]",
                  children: formatY(tick)
                })
              ]
            }, `y${index}`);
          }),
          axisTicks(xMin, xMax, 5).map((tick, index) => {
            const x = xFor(tick);
            return /* @__PURE__ */ jsxs18("g", {
              children: [
                /* @__PURE__ */ jsx20("line", {
                  x1: x,
                  x2: x,
                  y1: MARGIN5.top,
                  y2: height - MARGIN5.bottom,
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx20("text", {
                  x,
                  y: height - 14,
                  textAnchor: "middle",
                  className: "fill-faint text-[10px]",
                  children: formatX(tick)
                })
              ]
            }, `x${index}`);
          }),
          xLabel ? /* @__PURE__ */ jsx20("text", {
            x: MARGIN5.left + plotW / 2,
            y: height - 2,
            textAnchor: "middle",
            className: "fill-muted text-[10px]",
            children: xLabel
          }) : null,
          yLabel ? /* @__PURE__ */ jsx20("text", {
            x: 10,
            y: MARGIN5.top + plotH / 2,
            textAnchor: "middle",
            transform: `rotate(-90 10 ${MARGIN5.top + plotH / 2})`,
            className: "fill-muted text-[10px]",
            children: yLabel
          }) : null,
          finite.map((point, index) => /* @__PURE__ */ jsx20("circle", {
            cx: xFor(point.x),
            cy: yFor(point.y),
            r: 3.5,
            fill: vizColor(series.indexOf(point.series ?? "")),
            fillOpacity: 0.75,
            "data-x": point.x,
            "data-y": point.y,
            "data-label": point.label,
            ...handlers(/* @__PURE__ */ jsx20(TooltipRows, {
              title: point.label ?? point.series,
              rows: [
                { label: xLabel ?? "x", value: formatX(point.x) },
                { label: yLabel ?? "y", value: formatY(point.y) }
              ]
            }))
          }, index))
        ]
      }),
      /* @__PURE__ */ jsx20(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/StackedAreaChart.tsx
import { useEffect as useEffect14, useRef as useRef12, useState as useState16 } from "react";
import { jsx as jsx21, jsxs as jsxs19 } from "react/jsx-runtime";
var MARGIN6 = { top: 12, right: 14, bottom: 26, left: 48 };
function clamp2(value, min, max) {
  return Math.min(Math.max(value, min), max);
}
function StackedAreaChart({
  labels,
  series,
  height = 240,
  format = formatValue,
  formatLabel = (label) => label,
  ariaLabel
}) {
  const container = useRef12(null);
  const [width, setWidth] = useState16(640);
  const [hover, setHover] = useState16(null);
  useEffect14(() => observeWidth(container.current, 240, setWidth), []);
  const count = labels.length;
  if (count < 2 || series.length === 0) {
    return /* @__PURE__ */ jsx21("div", {
      className: "grid h-[240px] place-items-center text-xs text-faint",
      children: "Not enough data to chart."
    });
  }
  const clamped = series.map((entry) => ({
    name: entry.name,
    values: labels.map((_, index) => {
      const value = entry.values[index];
      return Number.isFinite(value) && value > 0 ? value : 0;
    })
  }));
  const cumulative = [];
  let previous = labels.map(() => 0);
  for (const entry of clamped) {
    const top = entry.values.map((value, index) => previous[index] + value);
    cumulative.push(top);
    previous = top;
  }
  const maxTotal = Math.max(...previous, 1);
  const plotW = width - MARGIN6.left - MARGIN6.right;
  const plotH = height - MARGIN6.top - MARGIN6.bottom;
  const xFor = (index) => MARGIN6.left + index / (count - 1) * plotW;
  const yFor = (value) => MARGIN6.top + (1 - value / maxTotal) * plotH;
  const labelEvery = Math.max(1, Math.ceil(count / 8));
  const summary = ariaLabel || `Stacked area chart, ${series.length} series over ${count} points`;
  function bandPath(bandIndex) {
    const top = cumulative[bandIndex];
    const bottom = bandIndex === 0 ? labels.map(() => 0) : cumulative[bandIndex - 1];
    const forward = top.map((value, index) => `${xFor(index).toFixed(1)},${yFor(value).toFixed(1)}`).join(" L ");
    const backward = [...bottom.keys()].reverse().map((index) => `${xFor(index).toFixed(1)},${yFor(bottom[index]).toFixed(1)}`).join(" L ");
    return `M ${forward} L ${backward} Z`;
  }
  function onMove(event) {
    const rect = event.currentTarget.getBoundingClientRect();
    const px = (event.clientX - rect.left) / rect.width * width;
    const index = Math.round((px - MARGIN6.left) / plotW * (count - 1));
    setHover(index >= 0 && index < count ? index : null);
  }
  const tooltipLeft = hover != null ? clamp2(xFor(hover), 90, width - 90) : 0;
  return /* @__PURE__ */ jsxs19("div", {
    ref: container,
    className: "relative w-full",
    children: [
      /* @__PURE__ */ jsx21("div", {
        className: "mb-1 flex flex-wrap items-center gap-3 text-2xs text-faint",
        children: clamped.map((entry, index) => /* @__PURE__ */ jsxs19("span", {
          className: "flex items-center gap-1",
          children: [
            /* @__PURE__ */ jsx21("span", {
              "aria-hidden": "true",
              className: "inline-block size-2",
              style: { background: vizColor(index) }
            }),
            entry.name
          ]
        }, entry.name))
      }),
      /* @__PURE__ */ jsxs19("svg", {
        role: "img",
        "aria-label": summary,
        className: "w-full overflow-hidden",
        style: { height },
        viewBox: `0 0 ${width} ${height}`,
        onMouseMove: onMove,
        onMouseLeave: () => setHover(null),
        children: [
          axisTicks(0, maxTotal, 4).map((tick, index) => {
            const y = yFor(tick);
            return /* @__PURE__ */ jsxs19("g", {
              children: [
                /* @__PURE__ */ jsx21("line", {
                  x1: MARGIN6.left,
                  x2: width - MARGIN6.right,
                  y1: y,
                  y2: y,
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx21("text", {
                  x: MARGIN6.left - 6,
                  y: y + 3,
                  textAnchor: "end",
                  className: "fill-faint text-[10px]",
                  children: formatCompact(tick)
                })
              ]
            }, index);
          }),
          clamped.map((entry, index) => /* @__PURE__ */ jsx21("path", {
            d: bandPath(index),
            fill: vizColor(index),
            fillOpacity: 0.8,
            "data-series": entry.name
          }, entry.name)),
          hover != null ? /* @__PURE__ */ jsx21("line", {
            x1: xFor(hover),
            x2: xFor(hover),
            y1: MARGIN6.top,
            y2: height - MARGIN6.bottom,
            className: "stroke-faint",
            strokeDasharray: "3 3"
          }) : null,
          labels.map((label, index) => index % labelEvery === 0 || index === count - 1 ? /* @__PURE__ */ jsx21("text", {
            x: xFor(index),
            y: height - 8,
            textAnchor: "middle",
            className: "fill-faint font-mono text-[10px]",
            children: formatLabel(label)
          }, label) : null)
        ]
      }),
      hover != null ? /* @__PURE__ */ jsxs19("div", {
        className: "pointer-events-none absolute top-8 z-20 -translate-x-1/2 whitespace-nowrap border border-line bg-surface px-2 py-1.5 text-2xs shadow-[var(--shadow)]",
        style: { left: tooltipLeft },
        children: [
          /* @__PURE__ */ jsx21("div", {
            className: "mb-0.5 font-mono text-faint",
            children: formatLabel(labels[hover])
          }),
          clamped.map((entry, index) => /* @__PURE__ */ jsxs19("div", {
            className: "flex items-center justify-between gap-3",
            children: [
              /* @__PURE__ */ jsxs19("span", {
                className: "flex items-center gap-1 text-muted",
                children: [
                  /* @__PURE__ */ jsx21("span", {
                    "aria-hidden": "true",
                    className: "inline-block size-2",
                    style: { background: vizColor(index) }
                  }),
                  entry.name
                ]
              }),
              /* @__PURE__ */ jsx21("span", {
                className: "font-mono tnum text-ink",
                children: format(entry.values[hover])
              })
            ]
          }, entry.name))
        ]
      }) : null
    ]
  });
}
// webapp/src/components/States.tsx
import { jsx as jsx22, jsxs as jsxs20 } from "react/jsx-runtime";
function StateBox({ tone, title, message }) {
  const danger = tone === "danger";
  return /* @__PURE__ */ jsx22("div", {
    className: `grid min-h-[200px] place-items-center border bg-surface p-6 text-center ${danger ? "border-danger/40" : "border-line"}`,
    "data-state": tone,
    role: danger ? "alert" : "status",
    "aria-live": danger ? "assertive" : "polite",
    children: /* @__PURE__ */ jsxs20("div", {
      className: "max-w-md",
      children: [
        tone === "loading" ? /* @__PURE__ */ jsx22("span", {
          "aria-hidden": "true",
          className: "motion-safe:animate-pulse inline-block size-2 rounded-full bg-accent"
        }) : null,
        title ? /* @__PURE__ */ jsx22("h3", {
          className: `text-sm font-semibold ${danger ? "text-danger" : "text-ink"}`,
          children: title
        }) : null,
        /* @__PURE__ */ jsx22("p", {
          className: `mt-1 text-xs ${danger ? "text-danger" : "text-muted"}`,
          children: message
        })
      ]
    })
  });
}
function LoadingState({ title = "Loading", message = "Loading metrics…" }) {
  return /* @__PURE__ */ jsx22(StateBox, {
    tone: "loading",
    title,
    message
  });
}
function EmptyState({ title = "No results", message }) {
  return /* @__PURE__ */ jsx22(StateBox, {
    tone: "muted",
    title,
    message
  });
}
function ErrorState({ title = "Query failed", message }) {
  return /* @__PURE__ */ jsx22(StateBox, {
    tone: "danger",
    title,
    message
  });
}
function StatusDot({ status }) {
  const color = status === "ok" ? "bg-success" : status === "loading" ? "bg-faint animate-pulse" : "bg-line";
  return /* @__PURE__ */ jsx22("span", {
    "aria-hidden": "true",
    className: `inline-block size-2 rounded-full ${color}`
  });
}
// webapp/src/components/WaterfallChart.tsx
import { useEffect as useEffect15, useRef as useRef13, useState as useState17 } from "react";
import { jsx as jsx23, jsxs as jsxs21, Fragment as Fragment10 } from "react/jsx-runtime";
function waterfallSteps(data) {
  let running = 0;
  return data.map((item) => {
    const value = Number.isFinite(item.value) ? item.value : 0;
    if (item.isTotal) {
      running = value;
      return { ...item, value, start: 0, end: value };
    }
    const start = running;
    running += value;
    return { ...item, value, start, end: running };
  });
}
var MARGIN7 = { top: 12, right: 14, bottom: 26, left: 48 };
function WaterfallChart({ data, height = 220, format = formatValue, ariaLabel }) {
  const ref = useRef13(null);
  const [width, setWidth] = useState17(640);
  const { tip, handlers } = useChartTooltip();
  useEffect15(() => observeWidth(ref.current, 200, setWidth), []);
  const steps = waterfallSteps(data);
  if (steps.length === 0) {
    return /* @__PURE__ */ jsx23("div", {
      className: "grid h-[220px] place-items-center text-xs text-faint",
      children: "No steps to chart."
    });
  }
  const bounds = steps.flatMap((step) => [step.start, step.end]);
  const min = Math.min(0, ...bounds);
  const max = Math.max(0, ...bounds);
  const span = max - min || 1;
  const plotW = width - MARGIN7.left - MARGIN7.right;
  const plotH = height - MARGIN7.top - MARGIN7.bottom;
  const yFor = (value) => MARGIN7.top + (1 - (value - min) / span) * plotH;
  const slot = plotW / steps.length;
  const barWidth = Math.max(10, Math.min(56, slot * 0.62));
  const ticks = axisTicks(min, max, 4);
  const summary = ariaLabel || `Waterfall chart, ${steps.length} steps ending at ${format(steps[steps.length - 1].end)}`;
  return /* @__PURE__ */ jsxs21(Fragment10, {
    children: [
      /* @__PURE__ */ jsxs21("svg", {
        ref,
        role: "img",
        "aria-label": summary,
        className: "w-full overflow-hidden",
        style: { height },
        viewBox: `0 0 ${width} ${height}`,
        children: [
          ticks.map((tick, index) => {
            const y = yFor(tick);
            return /* @__PURE__ */ jsxs21("g", {
              children: [
                /* @__PURE__ */ jsx23("line", {
                  x1: MARGIN7.left,
                  x2: width - MARGIN7.right,
                  y1: y,
                  y2: y,
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx23("text", {
                  x: MARGIN7.left - 6,
                  y: y + 3,
                  textAnchor: "end",
                  className: "fill-faint text-[10px]",
                  children: formatCompact(tick)
                })
              ]
            }, index);
          }),
          /* @__PURE__ */ jsx23("line", {
            x1: MARGIN7.left,
            x2: width - MARGIN7.right,
            y1: yFor(0),
            y2: yFor(0),
            className: "stroke-faint"
          }),
          steps.map((step, index) => {
            const x = MARGIN7.left + slot * index + (slot - barWidth) / 2;
            const y0 = yFor(step.start);
            const y1 = yFor(step.end);
            const tone = step.isTotal ? "total" : step.value < 0 ? "negative" : "positive";
            const fill = step.isTotal ? "fill-faint" : step.value < 0 ? "fill-danger" : "fill-chart-primary";
            const next = steps[index + 1];
            return /* @__PURE__ */ jsxs21("g", {
              children: [
                /* @__PURE__ */ jsx23("rect", {
                  x,
                  y: Math.min(y0, y1),
                  width: barWidth,
                  height: Math.max(1, Math.abs(y1 - y0)),
                  "data-label": step.label,
                  "data-value": step.value,
                  "data-tone": tone,
                  className: fill,
                  ...handlers(/* @__PURE__ */ jsx23(TooltipRows, {
                    title: step.label,
                    rows: step.isTotal ? [{ label: "Total", value: format(step.end) }] : [
                      { label: "Change", value: format(step.value) },
                      { label: "Running", value: format(step.end) }
                    ]
                  }))
                }),
                next ? /* @__PURE__ */ jsx23("line", {
                  x1: x + barWidth,
                  x2: MARGIN7.left + slot * (index + 1) + (slot - barWidth) / 2,
                  y1,
                  y2: y1,
                  className: "stroke-faint",
                  strokeDasharray: "3 3"
                }) : null,
                /* @__PURE__ */ jsx23("text", {
                  x: x + barWidth / 2,
                  y: height - 8,
                  textAnchor: "middle",
                  className: "fill-muted text-[10px]",
                  children: step.label.slice(0, 9)
                })
              ]
            }, `${step.label}-${index}`);
          })
        ]
      }),
      /* @__PURE__ */ jsx23(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/Button.tsx
import { jsx as jsx24 } from "react/jsx-runtime";
var VARIANT_CLASSES = {
  primary: "border-accent bg-accent-soft font-medium text-accent hover:bg-accent hover:text-surface",
  secondary: "border-line bg-surface text-ink hover:bg-surface-soft",
  ghost: "border-transparent bg-transparent text-muted hover:bg-surface-soft hover:text-ink",
  danger: "border-danger bg-danger-soft font-medium text-danger hover:bg-danger hover:text-surface"
};
function Button({ variant = "secondary", size = "md", type = "button", className, ...rest }) {
  const sizing = size === "sm" ? "h-6 px-2.5 text-2xs" : "h-7 px-3 text-xs";
  return /* @__PURE__ */ jsx24("button", {
    type,
    "data-variant": variant,
    className: `inline-flex items-center justify-center rounded-full border ${sizing} ${VARIANT_CLASSES[variant]} disabled:pointer-events-none disabled:opacity-50 ${className ?? ""}`,
    ...rest
  });
}
// webapp/src/components/Combobox.tsx
import { useEffect as useEffect16, useId as useId3, useMemo as useMemo5, useRef as useRef14, useState as useState18 } from "react";
import { jsx as jsx25, jsxs as jsxs22 } from "react/jsx-runtime";
function filterOptions(options, query) {
  const needle = query.trim().toLowerCase();
  if (!needle)
    return options;
  return options.filter((option) => option.value.toLowerCase().includes(needle) || (option.label ?? "").toLowerCase().includes(needle));
}
function Combobox(props) {
  const { options, placeholder = "Search…", ariaLabel, disabled, maxVisible = 50 } = props;
  const listId = useId3();
  const rootRef = useRef14(null);
  const [open, setOpen] = useState18(false);
  const [query, setQuery] = useState18("");
  const [activeIndex, setActiveIndex] = useState18(0);
  const matches = useMemo5(() => filterOptions(options, query).slice(0, maxVisible), [options, query, maxVisible]);
  const selectedValues = props.multiple ? props.values : props.value != null ? [props.value] : [];
  const selectedSet = new Set(selectedValues);
  const labelFor = (value) => options.find((option) => option.value === value)?.label ?? value;
  useEffect16(() => {
    if (!open)
      return;
    function onPointer(event) {
      if (rootRef.current && !rootRef.current.contains(event.target))
        setOpen(false);
    }
    document.addEventListener("mousedown", onPointer, true);
    return () => document.removeEventListener("mousedown", onPointer, true);
  }, [open]);
  function commit(option) {
    if (!option)
      return;
    if (props.multiple) {
      const next = selectedSet.has(option.value) ? props.values.filter((value) => value !== option.value) : [...props.values, option.value];
      props.onChange(next);
      return;
    }
    props.onChange(option.value);
    setQuery("");
    setOpen(false);
  }
  function clear() {
    if (props.multiple)
      props.onChange([]);
    else
      props.onChange(null);
    setQuery("");
  }
  function onKeyDown(event) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setOpen(true);
      setActiveIndex((index) => Math.min(index + 1, matches.length - 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((index) => Math.max(index - 1, 0));
    } else if (event.key === "Enter") {
      event.preventDefault();
      if (open)
        commit(matches[activeIndex]);
    } else if (event.key === "Escape") {
      setOpen(false);
    } else if (event.key === "Backspace" && props.multiple && !query && props.values.length) {
      props.onChange(props.values.slice(0, -1));
    }
  }
  const inputPlaceholder = !props.multiple && props.value != null ? labelFor(props.value) : placeholder;
  return /* @__PURE__ */ jsxs22("div", {
    ref: rootRef,
    className: "relative inline-flex min-w-40 flex-wrap items-center gap-1 text-xs",
    children: [
      props.multiple ? props.values.map((value) => /* @__PURE__ */ jsxs22("span", {
        "data-chip": value,
        className: "inline-flex items-center gap-1 rounded-full bg-surface-soft px-2 py-0.5 leading-4 text-muted",
        children: [
          /* @__PURE__ */ jsx25("span", {
            className: "max-w-32 truncate",
            children: labelFor(value)
          }),
          /* @__PURE__ */ jsx25("button", {
            type: "button",
            "aria-label": `Remove ${labelFor(value)}`,
            disabled,
            onClick: () => props.onChange(props.values.filter((entry) => entry !== value)),
            className: "text-faint hover:text-ink",
            children: "×"
          })
        ]
      }, value)) : null,
      /* @__PURE__ */ jsxs22("span", {
        className: "relative min-w-28 flex-1",
        children: [
          /* @__PURE__ */ jsx25("input", {
            type: "text",
            role: "combobox",
            "aria-expanded": open,
            "aria-controls": listId,
            "aria-autocomplete": "list",
            "aria-activedescendant": open && matches[activeIndex] ? `${listId}-${activeIndex}` : undefined,
            "aria-label": ariaLabel,
            disabled,
            placeholder: inputPlaceholder,
            value: query,
            onChange: (event) => {
              setQuery(event.target.value);
              setActiveIndex(0);
              setOpen(true);
            },
            onFocus: () => setOpen(true),
            onKeyDown,
            className: "h-7 w-full rounded-full border border-line bg-surface px-3 text-ink placeholder:text-faint disabled:opacity-50"
          }),
          selectedValues.length > 0 ? /* @__PURE__ */ jsx25("button", {
            type: "button",
            "aria-label": "Clear selection",
            disabled,
            onClick: clear,
            className: "absolute right-1 top-1/2 -translate-y-1/2 px-1 text-faint hover:text-ink",
            children: "×"
          }) : null
        ]
      }),
      open ? /* @__PURE__ */ jsxs22("ul", {
        id: listId,
        role: "listbox",
        "aria-multiselectable": props.multiple || undefined,
        className: "absolute left-0 top-full z-50 mt-1 max-h-56 w-full min-w-40 overflow-y-auto rounded-xl border border-line bg-surface p-1.5 shadow-[var(--shadow)]",
        children: [
          matches.length === 0 ? /* @__PURE__ */ jsx25("li", {
            className: "px-1.5 py-1 text-faint",
            children: "No matches"
          }) : null,
          matches.map((option, index) => /* @__PURE__ */ jsxs22("li", {
            id: `${listId}-${index}`,
            role: "option",
            "aria-selected": selectedSet.has(option.value),
            "data-active": index === activeIndex || undefined,
            onMouseEnter: () => setActiveIndex(index),
            onMouseDown: (event) => {
              event.preventDefault();
              commit(option);
            },
            className: "cursor-pointer truncate rounded-md px-2 py-1 text-muted data-[active=true]:bg-surface-soft data-[active=true]:text-ink",
            children: [
              option.label ?? option.value,
              selectedSet.has(option.value) ? /* @__PURE__ */ jsx25("span", {
                className: "float-right text-accent",
                children: "✓"
              }) : null
            ]
          }, option.value))
        ]
      }) : null
    ]
  });
}
// webapp/src/components/DatePicker.tsx
import { useRef as useRef15, useState as useState19 } from "react";
import { jsx as jsx26, jsxs as jsxs23 } from "react/jsx-runtime";
var WEEKDAYS = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];
var MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
function toIso(year, monthIndex, day) {
  return `${String(year).padStart(4, "0")}-${String(monthIndex + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}
function monthGrid(year, monthIndex) {
  const first = new Date(Date.UTC(year, monthIndex, 1));
  const start = new Date(first);
  start.setUTCDate(1 - first.getUTCDay());
  const weeks = [];
  const cursor = new Date(start);
  do {
    const week = [];
    for (let day = 0;day < 7; day += 1) {
      week.push({
        iso: toIso(cursor.getUTCFullYear(), cursor.getUTCMonth(), cursor.getUTCDate()),
        day: cursor.getUTCDate(),
        inMonth: cursor.getUTCMonth() === monthIndex
      });
      cursor.setUTCDate(cursor.getUTCDate() + 1);
    }
    weeks.push(week);
  } while (cursor.getUTCMonth() === monthIndex);
  return weeks;
}
function todayIso() {
  const now = new Date;
  return toIso(now.getFullYear(), now.getMonth(), now.getDate());
}
function DatePicker(props) {
  const { inline, ariaLabel, disabled } = props;
  const isRange = props.mode === "range";
  const anchor = isRange ? props.value?.from ?? null : props.value;
  const [year, setYear] = useState19(() => Number((anchor ?? todayIso()).slice(0, 4)));
  const [monthIndex, setMonthIndex] = useState19(() => Number((anchor ?? todayIso()).slice(5, 7)) - 1);
  const [pending, setPending] = useState19(null);
  const details = useRef15(null);
  function shiftMonth(delta) {
    const next = new Date(Date.UTC(year, monthIndex + delta, 1));
    setYear(next.getUTCFullYear());
    setMonthIndex(next.getUTCMonth());
  }
  function pick(iso) {
    if (!isRange) {
      props.onChange(iso);
      if (details.current)
        details.current.open = false;
      return;
    }
    if (!pending) {
      setPending(iso);
      return;
    }
    const [from, to] = pending <= iso ? [pending, iso] : [iso, pending];
    props.onChange({ from, to });
    setPending(null);
    if (details.current)
      details.current.open = false;
  }
  function isSelected(iso) {
    if (isRange) {
      if (pending)
        return iso === pending;
      return props.value != null && iso >= props.value.from && iso <= props.value.to;
    }
    return iso === props.value;
  }
  function isEdge(iso) {
    if (isRange)
      return pending ? iso === pending : props.value != null && (iso === props.value.from || iso === props.value.to);
    return iso === props.value;
  }
  const today = todayIso();
  const calendar = /* @__PURE__ */ jsxs23("div", {
    className: "w-56 select-none rounded-xl border border-line bg-surface p-2.5 text-2xs",
    "aria-label": ariaLabel ?? "Calendar",
    children: [
      /* @__PURE__ */ jsxs23("div", {
        className: "mb-1 flex items-center justify-between",
        children: [
          /* @__PURE__ */ jsx26("button", {
            type: "button",
            "aria-label": "Previous month",
            onClick: () => shiftMonth(-1),
            className: "px-1.5 py-0.5 text-muted hover:bg-surface-soft hover:text-ink",
            children: "‹"
          }),
          /* @__PURE__ */ jsxs23("span", {
            className: "font-medium text-ink",
            children: [
              MONTHS[monthIndex],
              " ",
              year
            ]
          }),
          /* @__PURE__ */ jsx26("button", {
            type: "button",
            "aria-label": "Next month",
            onClick: () => shiftMonth(1),
            className: "px-1.5 py-0.5 text-muted hover:bg-surface-soft hover:text-ink",
            children: "›"
          })
        ]
      }),
      /* @__PURE__ */ jsx26("div", {
        className: "grid grid-cols-7 text-center text-faint",
        children: WEEKDAYS.map((weekday) => /* @__PURE__ */ jsx26("span", {
          className: "py-0.5",
          children: weekday
        }, weekday))
      }),
      /* @__PURE__ */ jsx26("div", {
        role: "grid",
        "aria-label": `${MONTHS[monthIndex]} ${year}`,
        children: monthGrid(year, monthIndex).map((week, weekIndex) => /* @__PURE__ */ jsx26("div", {
          role: "row",
          className: "grid grid-cols-7",
          children: week.map((cell) => /* @__PURE__ */ jsx26("button", {
            type: "button",
            role: "gridcell",
            "aria-selected": isSelected(cell.iso),
            "data-date": cell.iso,
            onClick: () => pick(cell.iso),
            className: `py-1 text-center font-mono tnum ${isEdge(cell.iso) ? "bg-accent text-surface" : isSelected(cell.iso) ? "bg-accent-soft text-accent" : cell.inMonth ? "text-ink hover:bg-surface-soft" : "text-faint hover:bg-surface-soft"} ${cell.iso === today ? "underline underline-offset-2" : ""}`,
            children: cell.day
          }, cell.iso))
        }, weekIndex))
      }),
      isRange && pending ? /* @__PURE__ */ jsxs23("p", {
        className: "mt-1 text-faint",
        children: [
          "Start ",
          pending,
          " — pick an end date."
        ]
      }) : null,
      (isRange ? props.value : props.value) != null ? /* @__PURE__ */ jsx26("button", {
        type: "button",
        onClick: () => isRange ? props.onChange(null) : props.onChange(null),
        className: "mt-1 w-full rounded-lg border border-line px-2 py-1 text-left text-muted hover:bg-surface-soft",
        children: "Clear"
      }) : null
    ]
  });
  if (inline)
    return calendar;
  const summary = isRange ? props.value ? `${props.value.from} → ${props.value.to}` : "Any dates" : props.value ?? "Any date";
  return /* @__PURE__ */ jsxs23("details", {
    ref: details,
    className: "relative inline-block text-xs",
    children: [
      /* @__PURE__ */ jsxs23("summary", {
        className: `flex cursor-pointer items-center h-7 gap-1.5 rounded-full border border-line bg-surface px-3 text-ink ${disabled ? "pointer-events-none opacity-50" : ""}`,
        children: [
          /* @__PURE__ */ jsx26("span", {
            className: "text-faint",
            children: ariaLabel ?? "Date"
          }),
          /* @__PURE__ */ jsx26("span", {
            className: "font-mono tnum",
            children: summary
          }),
          /* @__PURE__ */ jsx26("span", {
            "aria-hidden": "true",
            className: "text-faint",
            children: "▾"
          })
        ]
      }),
      /* @__PURE__ */ jsx26("div", {
        className: "absolute left-0 z-50 mt-1 shadow-[var(--shadow)]",
        children: calendar
      })
    ]
  });
}
// webapp/src/components/DateRangeControl.tsx
import { useRef as useRef16, useState as useState20 } from "react";
import { jsx as jsx27, jsxs as jsxs24 } from "react/jsx-runtime";
var COMPARISON_OPTIONS = [
  { key: "off", label: "Off" },
  { key: "previous", label: "Previous period" },
  { key: "year", label: "Previous year" },
  { key: "custom", label: "Custom range" }
];
function DateRangeControl({
  range,
  disabled,
  onChange,
  comparison,
  comparisonRange,
  onComparisonChange
}) {
  const details = useRef16(null);
  const [from, setFrom] = useState20(range?.from ?? "");
  const [to, setTo] = useState20(range?.to ?? "");
  const [cmpFrom, setCmpFrom] = useState20(comparisonRange?.from ?? "");
  const [cmpTo, setCmpTo] = useState20(comparisonRange?.to ?? "");
  function close() {
    if (details.current)
      details.current.open = false;
  }
  function apply(next) {
    onChange(next);
    close();
  }
  function applyComparison(mode) {
    if (mode === "custom")
      onComparisonChange("custom", cmpFrom && cmpTo ? { from: cmpFrom, to: cmpTo } : undefined);
    else
      onComparisonChange(mode);
  }
  const summary = range ? `${range.from} → ${range.to}` : "All time";
  const comparisonDisabled = !range;
  return /* @__PURE__ */ jsxs24("details", {
    ref: details,
    className: "relative text-xs",
    children: [
      /* @__PURE__ */ jsxs24("summary", {
        className: `flex cursor-pointer items-center h-7 gap-1.5 rounded-full border border-line bg-surface px-3 text-ink ${disabled ? "pointer-events-none opacity-50" : ""}`,
        children: [
          /* @__PURE__ */ jsx27("span", {
            className: "text-faint",
            children: "Range"
          }),
          /* @__PURE__ */ jsx27("span", {
            className: "font-mono tnum",
            children: summary
          }),
          /* @__PURE__ */ jsx27("span", {
            "aria-hidden": "true",
            className: "text-faint",
            children: "▾"
          })
        ]
      }),
      /* @__PURE__ */ jsxs24("div", {
        className: "absolute right-0 z-50 mt-1 w-64 rounded-xl border border-line bg-surface p-2.5 shadow-[var(--shadow)]",
        children: [
          /* @__PURE__ */ jsx27("button", {
            type: "button",
            onClick: () => apply(undefined),
            className: "mb-2 w-full rounded-lg border border-line px-2 py-1 text-left text-2xs text-muted hover:bg-surface-soft",
            children: "All time"
          }),
          /* @__PURE__ */ jsx27("div", {
            className: "grid grid-cols-2 gap-1",
            children: DATE_PRESETS.map((preset) => /* @__PURE__ */ jsx27("button", {
              type: "button",
              onClick: () => apply(presetRange(preset.days)),
              className: "rounded-lg border border-line px-2 py-1 text-2xs text-muted hover:bg-surface-soft",
              children: preset.label
            }, preset.key))
          }),
          /* @__PURE__ */ jsxs24("div", {
            className: "mt-2 border-t border-line pt-2",
            children: [
              /* @__PURE__ */ jsxs24("div", {
                className: "flex items-center gap-1",
                children: [
                  /* @__PURE__ */ jsx27("input", {
                    type: "date",
                    "aria-label": "From date",
                    value: from,
                    onChange: (event) => setFrom(event.target.value),
                    className: "min-w-0 flex-1 border border-line bg-surface px-1.5 py-1 text-2xs text-ink"
                  }),
                  /* @__PURE__ */ jsx27("span", {
                    className: "text-faint",
                    children: "→"
                  }),
                  /* @__PURE__ */ jsx27("input", {
                    type: "date",
                    "aria-label": "To date",
                    value: to,
                    onChange: (event) => setTo(event.target.value),
                    className: "min-w-0 flex-1 border border-line bg-surface px-1.5 py-1 text-2xs text-ink"
                  })
                ]
              }),
              /* @__PURE__ */ jsx27("button", {
                type: "button",
                disabled: !from || !to,
                onClick: () => apply({ from, to }),
                className: "mt-2 w-full rounded-lg border border-accent bg-accent-soft px-2 py-1 text-2xs font-medium text-accent disabled:opacity-50",
                children: "Apply custom range"
              })
            ]
          }),
          /* @__PURE__ */ jsxs24("div", {
            className: "mt-2 border-t border-line pt-2",
            "data-testid": "comparison-picker",
            children: [
              /* @__PURE__ */ jsx27("p", {
                className: "mb-1 text-xs font-medium text-muted",
                children: "Compare to"
              }),
              /* @__PURE__ */ jsx27("div", {
                className: `grid grid-cols-2 gap-1 ${comparisonDisabled ? "pointer-events-none opacity-50" : ""}`,
                children: COMPARISON_OPTIONS.map((option) => /* @__PURE__ */ jsx27("button", {
                  type: "button",
                  "data-comparison": option.key,
                  "data-active": comparison === option.key || undefined,
                  onClick: () => applyComparison(option.key),
                  className: "rounded-lg border border-line px-2 py-1 text-2xs text-muted hover:bg-surface-soft data-[active=true]:border-accent data-[active=true]:bg-accent-soft data-[active=true]:text-accent",
                  children: option.label
                }, option.key))
              }),
              comparison === "custom" && !comparisonDisabled ? /* @__PURE__ */ jsxs24("div", {
                className: "mt-2",
                children: [
                  /* @__PURE__ */ jsxs24("div", {
                    className: "flex items-center gap-1",
                    children: [
                      /* @__PURE__ */ jsx27("input", {
                        type: "date",
                        "aria-label": "Comparison from date",
                        value: cmpFrom,
                        onChange: (event) => setCmpFrom(event.target.value),
                        className: "min-w-0 flex-1 border border-line bg-surface px-1.5 py-1 text-2xs text-ink"
                      }),
                      /* @__PURE__ */ jsx27("span", {
                        className: "text-faint",
                        children: "→"
                      }),
                      /* @__PURE__ */ jsx27("input", {
                        type: "date",
                        "aria-label": "Comparison to date",
                        value: cmpTo,
                        onChange: (event) => setCmpTo(event.target.value),
                        className: "min-w-0 flex-1 border border-line bg-surface px-1.5 py-1 text-2xs text-ink"
                      })
                    ]
                  }),
                  /* @__PURE__ */ jsx27("button", {
                    type: "button",
                    disabled: !cmpFrom || !cmpTo,
                    onClick: () => onComparisonChange("custom", { from: cmpFrom, to: cmpTo }),
                    className: "mt-2 w-full rounded-lg border border-accent bg-accent-soft px-2 py-1 text-2xs font-medium text-accent disabled:opacity-50",
                    children: "Apply comparison"
                  })
                ]
              }) : null
            ]
          })
        ]
      })
    ]
  });
}
// webapp/src/components/GrainSelect.tsx
import { jsx as jsx28, jsxs as jsxs25 } from "react/jsx-runtime";
function GrainSelect({ grain, options, disabled, onChange }) {
  return /* @__PURE__ */ jsxs25("label", {
    className: "flex items-center gap-1.5 text-xs text-faint",
    children: [
      /* @__PURE__ */ jsx28("span", {
        className: "hidden sm:inline",
        children: "Grain"
      }),
      /* @__PURE__ */ jsx28("select", {
        "aria-label": "Time grain",
        value: grain,
        disabled,
        onChange: (event) => onChange(event.target.value),
        className: "h-7 rounded-full border border-line bg-surface px-2.5 text-xs text-ink disabled:opacity-50",
        children: options.map((option) => /* @__PURE__ */ jsx28("option", {
          value: option,
          children: labelize2(option)
        }, option))
      })
    ]
  });
}
// webapp/src/components/Select.tsx
import { jsx as jsx29, jsxs as jsxs26 } from "react/jsx-runtime";
function Select({ value, options, onChange, label, ariaLabel, placeholder, disabled }) {
  return /* @__PURE__ */ jsxs26("label", {
    className: "flex items-center gap-1.5 text-xs text-faint",
    children: [
      label ? /* @__PURE__ */ jsx29("span", {
        className: "hidden sm:inline",
        children: label
      }) : null,
      /* @__PURE__ */ jsxs26("select", {
        "aria-label": ariaLabel ?? label,
        value,
        disabled,
        onChange: (event) => onChange(event.target.value),
        className: "h-7 rounded-full border border-line bg-surface px-2.5 text-xs text-ink disabled:opacity-50",
        children: [
          placeholder ? /* @__PURE__ */ jsx29("option", {
            value: "",
            disabled: true,
            children: placeholder
          }) : null,
          options.map((option) => /* @__PURE__ */ jsx29("option", {
            value: option.value,
            children: option.label ?? option.value
          }, option.value))
        ]
      })
    ]
  });
}
// webapp/src/components/Switch.tsx
import { jsx as jsx30, jsxs as jsxs27 } from "react/jsx-runtime";
function Switch({ checked, onChange, label, ariaLabel, disabled }) {
  return /* @__PURE__ */ jsxs27("label", {
    className: `flex items-center gap-1.5 text-xs ${disabled ? "opacity-50" : ""}`,
    children: [
      /* @__PURE__ */ jsx30("button", {
        type: "button",
        role: "switch",
        "aria-checked": checked,
        "aria-label": ariaLabel ?? label,
        disabled,
        onClick: () => onChange(!checked),
        "data-checked": checked || undefined,
        className: `relative h-4 w-7 shrink-0 rounded-full border transition-colors ${checked ? "border-accent bg-accent" : "border-line bg-surface-soft"}`,
        children: /* @__PURE__ */ jsx30("span", {
          "aria-hidden": "true",
          className: `absolute top-1/2 size-3 -translate-y-1/2 rounded-full bg-surface shadow transition-[left] ${checked ? "left-[14px]" : "left-[2px]"}`
        })
      }),
      label ? /* @__PURE__ */ jsx30("span", {
        className: "text-muted",
        children: label
      }) : null
    ]
  });
}
// webapp/src/components/Tabs.tsx
import { jsx as jsx31 } from "react/jsx-runtime";
function Tabs({ tabs, active, onChange, ariaLabel = "Tabs" }) {
  return /* @__PURE__ */ jsx31("div", {
    role: "tablist",
    "aria-label": ariaLabel,
    className: "inline-flex items-center gap-0.5 rounded-full border border-line bg-surface p-px",
    children: tabs.map((tab) => /* @__PURE__ */ jsx31("button", {
      role: "tab",
      type: "button",
      "aria-selected": active === tab.key,
      "data-tab": tab.key,
      "data-selected": active === tab.key || undefined,
      onClick: () => onChange(tab.key),
      className: "inline-flex h-6 items-center rounded-full px-2.5 text-xs font-medium text-muted hover:bg-surface-soft hover:text-ink data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent",
      children: tab.label
    }, tab.key))
  });
}
// webapp/src/components/TimezoneSelect.tsx
import { useId as useId4, useMemo as useMemo6, useState as useState21 } from "react";

// webapp/src/lib/timezones.ts
var COMMON_TIMEZONES = [
  "UTC",
  "America/Los_Angeles",
  "America/Denver",
  "America/Chicago",
  "America/New_York",
  "America/Sao_Paulo",
  "Europe/London",
  "Europe/Berlin",
  "Europe/Paris",
  "Europe/Moscow",
  "Asia/Dubai",
  "Asia/Kolkata",
  "Asia/Singapore",
  "Asia/Shanghai",
  "Asia/Tokyo",
  "Australia/Sydney"
];
var allZones = null;
function allTimezones() {
  if (allZones)
    return allZones;
  const supported = typeof Intl.supportedValuesOf === "function" ? Intl.supportedValuesOf("timeZone") : [];
  const set = new Set(["UTC", ...supported]);
  allZones = [...set].sort();
  return allZones;
}
function isValidTimezone(zone) {
  if (!zone)
    return false;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: zone });
    return true;
  } catch {
    return false;
  }
}
function timezoneOffsetLabel(zone, at = new Date) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", { timeZone: zone, timeZoneName: "shortOffset" }).formatToParts(at);
    const name = parts.find((part) => part.type === "timeZoneName")?.value ?? "";
    const match = name.match(/([+-])(\d{1,2})(?::(\d{2}))?/);
    if (!match)
      return zone === "UTC" ? "+00:00" : "";
    const sign = match[1];
    const hours = match[2].padStart(2, "0");
    const minutes = match[3] ?? "00";
    return `${sign}${hours}:${minutes}`;
  } catch {
    return "";
  }
}

// webapp/src/components/TimezoneSelect.tsx
import { jsx as jsx32, jsxs as jsxs28 } from "react/jsx-runtime";
var SEARCH_SENTINEL = "__search__";
function TimezoneSelect({ timezone, disabled, onChange }) {
  const listId = useId4();
  const [searching, setSearching] = useState21(false);
  const [text, setText] = useState21("");
  const options = useMemo6(() => {
    const set = new Set(COMMON_TIMEZONES);
    if (timezone)
      set.add(timezone);
    return [...set];
  }, [timezone]);
  const zones = useMemo6(() => searching ? allTimezones() : [], [searching]);
  function commitSearch(value) {
    const trimmed = value.trim();
    if (isValidTimezone(trimmed)) {
      onChange(trimmed);
      setSearching(false);
      setText("");
    }
  }
  if (searching) {
    return /* @__PURE__ */ jsxs28("label", {
      className: "flex items-center gap-1.5 text-2xs text-faint",
      children: [
        /* @__PURE__ */ jsx32("span", {
          className: "hidden sm:inline",
          children: "Zone"
        }),
        /* @__PURE__ */ jsx32("input", {
          type: "text",
          list: listId,
          autoFocus: true,
          "aria-label": "Search timezone",
          placeholder: "Region/City…",
          value: text,
          disabled,
          onChange: (event) => {
            setText(event.target.value);
            if (allTimezones().includes(event.target.value))
              commitSearch(event.target.value);
          },
          onKeyDown: (event) => {
            if (event.key === "Enter")
              commitSearch(text);
            else if (event.key === "Escape") {
              setSearching(false);
              setText("");
            }
          },
          onBlur: () => {
            setSearching(false);
            setText("");
          },
          className: "h-7 w-36 rounded-full border border-line bg-surface px-2.5 text-xs text-ink disabled:opacity-50"
        }),
        /* @__PURE__ */ jsx32("datalist", {
          id: listId,
          children: zones.map((zone) => /* @__PURE__ */ jsx32("option", {
            value: zone
          }, zone))
        })
      ]
    });
  }
  const offset = timezoneOffsetLabel(timezone);
  return /* @__PURE__ */ jsxs28("label", {
    className: "flex items-center gap-1.5 text-2xs text-faint",
    children: [
      /* @__PURE__ */ jsx32("span", {
        className: "hidden sm:inline",
        children: "Zone"
      }),
      /* @__PURE__ */ jsxs28("select", {
        "aria-label": "Timezone",
        value: timezone,
        disabled,
        onChange: (event) => {
          if (event.target.value === SEARCH_SENTINEL)
            setSearching(true);
          else
            onChange(event.target.value);
        },
        className: "h-7 max-w-[11rem] rounded-full border border-line bg-surface px-2.5 text-xs text-ink disabled:opacity-50",
        children: [
          options.map((zone) => /* @__PURE__ */ jsxs28("option", {
            value: zone,
            children: [
              zone,
              zone === "UTC" ? "" : offset && zone === timezone ? ` (${offset})` : ""
            ]
          }, zone)),
          /* @__PURE__ */ jsx32("option", {
            value: SEARCH_SENTINEL,
            children: "Search…"
          })
        ]
      })
    ]
  });
}
// webapp/src/components/Tooltip.tsx
import { jsx as jsx33, jsxs as jsxs29, Fragment as Fragment11 } from "react/jsx-runtime";
function Tooltip({ content, children, className }) {
  const { tip, handlers } = useChartTooltip();
  return /* @__PURE__ */ jsxs29(Fragment11, {
    children: [
      /* @__PURE__ */ jsx33("span", {
        className: className ?? "inline-flex",
        ...handlers(content),
        children
      }),
      /* @__PURE__ */ jsx33(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/ViewSwitcher.tsx
import { jsx as jsx34 } from "react/jsx-runtime";
var SEGMENTS = [
  { key: "explore", label: "Explore" },
  { key: "pivot", label: "Pivot" }
];
function ViewSwitcher({ view, onChange }) {
  return /* @__PURE__ */ jsx34("div", {
    role: "tablist",
    "aria-label": "View",
    className: "inline-flex items-center gap-0.5 rounded-full border border-line bg-surface p-px",
    children: SEGMENTS.map((segment) => /* @__PURE__ */ jsx34("button", {
      role: "tab",
      type: "button",
      "aria-selected": view === segment.key,
      "data-view": segment.key,
      "data-selected": view === segment.key || undefined,
      onClick: () => onChange(segment.key),
      className: "inline-flex h-6 items-center rounded-full px-2.5 text-xs font-medium text-muted hover:bg-surface-soft hover:text-ink data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent",
      children: segment.label
    }, segment.key))
  });
}
// webapp/src/lib/theme.ts
var TOKEN_PROPERTIES = {
  background: "--bg",
  surface: "--surface",
  surfaceSoft: "--surface-soft",
  ink: "--ink",
  muted: "--muted",
  faint: "--faint",
  line: "--line",
  action: "--accent",
  actionSoft: "--accent-soft",
  chartPrimary: "--chart-primary",
  chartPrimarySoft: "--chart-primary-soft",
  chartPrimarySelected: "--chart-primary-selected",
  danger: "--danger",
  dangerSoft: "--danger-soft",
  success: "--success",
  successSoft: "--success-soft",
  viz1: "--viz-1",
  viz2: "--viz-2",
  viz3: "--viz-3",
  viz4: "--viz-4",
  viz5: "--viz-5",
  viz6: "--viz-6",
  viz7: "--viz-7"
};
var KEY = "sidemantic-theme";
function getTheme() {
  const stored = localStorage.getItem(KEY);
  if (stored === "light" || stored === "dark")
    return stored;
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}
function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
}
function toggleTheme() {
  const next = getTheme() === "dark" ? "light" : "dark";
  localStorage.setItem(KEY, next);
  applyTheme(next);
  return next;
}
function applyThemeTokens(tokens, target = document.documentElement) {
  for (const [name, value] of Object.entries(tokens)) {
    if (value)
      target.style.setProperty(TOKEN_PROPERTIES[name], value);
    else
      target.style.removeProperty(TOKEN_PROPERTIES[name]);
  }
}
// webapp/src/lib/rows.ts
var EPOCH_S_MIN = 1e9;
var EPOCH_S_MAX = 100000000000;
function parseTemporal(value) {
  if (value instanceof Date)
    return Number.isNaN(value.getTime()) ? null : value;
  if (typeof value === "number" && Number.isFinite(value)) {
    if (Math.abs(value) >= EPOCH_S_MIN)
      return new Date(Math.abs(value) >= EPOCH_S_MAX ? value : value * 1000);
    return new Date(value);
  }
  if (typeof value !== "string" && typeof value !== "bigint")
    return null;
  const text = String(value).trim();
  if (!text)
    return null;
  if (/^\d{10,}$/.test(text)) {
    const numeric = Number(text);
    return new Date(numeric >= EPOCH_S_MAX ? numeric : numeric * 1000);
  }
  const dateOnly2 = text.match(/^(\d{4})-(\d{2})(?:-(\d{2}))?$/);
  if (dateOnly2)
    return new Date(Date.UTC(+dateOnly2[1], +dateOnly2[2] - 1, dateOnly2[3] ? +dateOnly2[3] : 1));
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}
function toLabel(value) {
  if (value == null)
    return "∅";
  return String(value);
}
function toNumber(value) {
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : Number.NaN;
}
function rowsToCategories(rows, fields) {
  return rows.map((row) => ({ label: toLabel(row[fields.x]), value: toNumber(row[fields.y]) }));
}
function rowsToBarLine(rows, fields) {
  return rows.map((row) => ({
    label: toLabel(row[fields.x]),
    bar: toNumber(row[fields.bar]),
    line: toNumber(row[fields.line])
  }));
}
function rowsToPoints(rows, fields) {
  return rows.map((row) => ({
    x: toNumber(row[fields.x]),
    y: toNumber(row[fields.y]),
    ...fields.label ? { label: toLabel(row[fields.label]) } : {},
    ...fields.series ? { series: toLabel(row[fields.series]) } : {}
  }));
}
function rowsToCells(rows, fields) {
  return rows.map((row) => ({
    x: toLabel(row[fields.x]),
    y: toLabel(row[fields.y]),
    value: toNumber(row[fields.value])
  }));
}
function rowsToTimeSeries(rows, fields) {
  return rows.map((row) => ({ x: toLabel(row[fields.x]), y: toNumber(row[fields.y]) }));
}
function rowsToSeries(rows, fields) {
  const labels = [];
  const names = [];
  const byKey = new Map;
  for (const row of rows) {
    const label = toLabel(row[fields.x]);
    const name = toLabel(row[fields.series]);
    if (!labels.includes(label))
      labels.push(label);
    if (!names.includes(name))
      names.push(name);
    byKey.set(`${name}\x00${label}`, toNumber(row[fields.y]));
  }
  return {
    labels,
    series: names.map((name) => ({
      name,
      values: labels.map((label) => byKey.get(`${name}\x00${label}`) ?? 0)
    }))
  };
}
export {
  waterfallSteps,
  vizColor,
  useChartTooltip,
  tokenizeSql,
  toggleTheme,
  toggleFilterValue,
  rowsToTimeSeries,
  rowsToSeries,
  rowsToPoints,
  rowsToCells,
  rowsToCategories,
  rowsToBarLine,
  removeFilterValue,
  removeFilterDimension,
  parseTemporal,
  paginateRows,
  normalizeFilterValue,
  monthGrid,
  layoutNetwork,
  labelize2 as labelize,
  getTheme,
  formatValue,
  formatCompact,
  filterOptions,
  donutSegments,
  columnTotal,
  binValues,
  axisTicks,
  applyThemeTokens,
  applyTheme,
  aliasForSemanticRef,
  WaterfallChart,
  ViewSwitcher,
  VIZ_COLOR_COUNT,
  TooltipRows,
  Tooltip,
  TimezoneSelect,
  TimeSeriesChart,
  Tabs,
  Switch,
  StatusDot,
  StackedAreaChart,
  Sparkline,
  Select,
  ScatterChart,
  QueryDebugPanel,
  NetworkChart,
  MetricCard,
  LoadingState,
  LineChart,
  Leaderboard,
  HistogramChart,
  HeatmapChart,
  GrainSelect,
  FilterPill,
  ErrorState,
  ErrorBoundary,
  EmptyState,
  DonutChart,
  DateRangeControl,
  DatePicker,
  DataTable,
  DataPreviewTable,
  DashboardShell,
  Combobox,
  ColumnChart,
  ChartTooltip,
  Button,
  BarLineCombo
};
