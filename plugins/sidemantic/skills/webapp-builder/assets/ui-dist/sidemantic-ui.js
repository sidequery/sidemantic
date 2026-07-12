// webapp/src/components/ChartTooltip.tsx
import { useState } from "react";
import { jsx } from "react/jsx-runtime";
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
    className: className || "rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-white shadow",
    children: tip.content
  });
}
// webapp/src/components/ColumnChart.tsx
import { useEffect, useRef, useState as useState2 } from "react";

// webapp/src/data/types.ts
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

// webapp/src/components/ColumnChart.tsx
import { jsx as jsx2, jsxs, Fragment } from "react/jsx-runtime";
var MARGIN = { top: 12, right: 14, bottom: 26, left: 44 };
function axisTicks(min, max, count = 4) {
  if (!(max > min))
    return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, index) => min + step * index);
}
function ColumnChart({ data, height = 200, ariaLabel }) {
  const ref = useRef(null);
  const [width, setWidth] = useState2(640);
  const { tip, handlers } = useChartTooltip();
  useEffect(() => {
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
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const yForValue = (value) => MARGIN.top + (1 - (value - min) / span) * plotH;
  const baselineY = yForValue(0);
  const slot = plotW / Math.max(data.length, 1);
  const barWidth = Math.max(8, Math.min(48, slot * 0.62));
  const ticks = axisTicks(min, max, 4);
  const summary = ariaLabel || `Bar chart, ${data.length} categories, up to ${formatCompact(max)}`;
  return /* @__PURE__ */ jsxs(Fragment, {
    children: [
      /* @__PURE__ */ jsxs("svg", {
        ref,
        role: "img",
        "aria-label": summary,
        className: "h-[200px] w-full overflow-hidden",
        viewBox: `0 0 ${width} ${height}`,
        children: [
          ticks.map((tick, index) => {
            const y = yForValue(tick);
            return /* @__PURE__ */ jsxs("g", {
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
          /* @__PURE__ */ jsx2("line", {
            x1: MARGIN.left,
            x2: width - MARGIN.right,
            y1: baselineY,
            y2: baselineY,
            className: "stroke-line"
          }),
          data.map((item, index) => {
            const value = values[index] ?? 0;
            const valueY = yForValue(value);
            const barHeight = Math.abs(valueY - baselineY);
            const x = MARGIN.left + slot * index + (slot - barWidth) / 2;
            const y = Math.min(valueY, baselineY);
            return /* @__PURE__ */ jsxs("g", {
              children: [
                /* @__PURE__ */ jsx2("rect", {
                  x,
                  y,
                  width: barWidth,
                  height: barHeight,
                  rx: "3",
                  "data-label": item.label,
                  "data-value": value,
                  "data-tone": value < 0 ? "negative" : "positive",
                  className: value < 0 ? "fill-danger" : "fill-chart-primary",
                  ...handlers(`${item.label}: ${formatValue(value)}`)
                }),
                /* @__PURE__ */ jsx2("text", {
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
      /* @__PURE__ */ jsx2(ChartTooltip, {
        tip
      })
    ]
  });
}
// webapp/src/components/DataTable.tsx
import { useEffect as useEffect2, useState as useState3 } from "react";
import { jsx as jsx3, jsxs as jsxs2 } from "react/jsx-runtime";
function DataTable({ columns, rows, loading, sortKey, sortDir, onSort, renderCell, pageSize = 50 }) {
  const [page, setPage] = useState3(0);
  const { paginate, pageCount, safePage, start, visibleRows } = paginateRows(rows, page, pageSize);
  useEffect2(() => {
    setPage(0);
  }, [rows, pageSize, sortKey, sortDir]);
  return /* @__PURE__ */ jsxs2("div", {
    className: "overflow-hidden border border-line bg-surface",
    children: [
      /* @__PURE__ */ jsx3("div", {
        className: "overflow-auto",
        children: /* @__PURE__ */ jsxs2("table", {
          className: "w-max min-w-full border-collapse text-xs",
          "data-testid": "pivot-table",
          children: [
            /* @__PURE__ */ jsx3("thead", {
              children: /* @__PURE__ */ jsx3("tr", {
                className: "bg-surface-soft",
                children: columns.map((column) => {
                  const active = sortKey === column.key;
                  return /* @__PURE__ */ jsx3("th", {
                    className: `max-w-80 whitespace-nowrap border-b border-line px-3 py-1.5 font-semibold text-faint ${column.numeric ? "min-w-32 text-right" : "min-w-40 text-left"}`,
                    children: column.sortable && onSort ? /* @__PURE__ */ jsxs2("button", {
                      type: "button",
                      onClick: () => onSort(column.key),
                      "aria-label": `Sort by ${column.label}${active ? `, currently ${sortDir === "asc" ? "ascending" : "descending"}` : ""}`,
                      className: `inline-flex min-h-11 max-w-full items-center gap-1 whitespace-nowrap hover:text-ink ${active ? "text-ink" : ""}`,
                      children: [
                        /* @__PURE__ */ jsx3("span", {
                          className: "truncate",
                          children: column.label
                        }),
                        /* @__PURE__ */ jsx3("span", {
                          "aria-hidden": "true",
                          className: "text-[9px]",
                          children: active ? sortDir === "asc" ? "▲" : "▼" : "↕"
                        })
                      ]
                    }) : /* @__PURE__ */ jsx3("span", {
                      className: "block truncate",
                      title: column.label,
                      children: column.label
                    })
                  }, column.key);
                })
              })
            }),
            /* @__PURE__ */ jsx3("tbody", {
              children: loading && rows.length === 0 ? /* @__PURE__ */ jsx3("tr", {
                children: /* @__PURE__ */ jsx3("td", {
                  colSpan: columns.length,
                  className: "px-3 py-6 text-center text-faint",
                  children: "Loading…"
                })
              }) : rows.length === 0 ? /* @__PURE__ */ jsx3("tr", {
                children: /* @__PURE__ */ jsx3("td", {
                  colSpan: columns.length,
                  className: "px-3 py-6 text-center text-faint",
                  children: "No rows"
                })
              }) : visibleRows.map((row, index) => /* @__PURE__ */ jsx3("tr", {
                className: "hover:bg-surface-soft",
                children: columns.map((column) => {
                  const cellText = renderCell(column, row[column.key]);
                  return /* @__PURE__ */ jsx3("td", {
                    className: `max-w-80 whitespace-nowrap border-b border-line px-3 py-1.5 text-muted ${column.numeric ? "min-w-32 text-right font-mono tnum text-ink" : "min-w-40"}`,
                    children: /* @__PURE__ */ jsx3("span", {
                      className: "block max-w-80 truncate",
                      title: cellText,
                      children: cellText
                    })
                  }, column.key);
                })
              }, start + index))
            })
          ]
        })
      }),
      paginate ? /* @__PURE__ */ jsxs2("div", {
        "data-testid": "pivot-table-pager",
        className: "flex min-h-11 items-center justify-between gap-3 border-t border-line px-3 text-2xs text-faint",
        children: [
          /* @__PURE__ */ jsxs2("span", {
            className: "tnum",
            children: [
              start + 1,
              "–",
              Math.min(start + pageSize, rows.length),
              " of ",
              rows.length.toLocaleString(),
              loading ? " · Updating…" : ""
            ]
          }),
          /* @__PURE__ */ jsxs2("div", {
            className: "flex gap-1",
            children: [
              /* @__PURE__ */ jsx3("button", {
                type: "button",
                disabled: safePage === 0,
                onClick: () => setPage((value) => Math.max(0, value - 1)),
                className: "min-h-11 min-w-11 px-2 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40",
                children: "Prev"
              }),
              /* @__PURE__ */ jsx3("button", {
                type: "button",
                disabled: safePage >= pageCount - 1,
                onClick: () => setPage((value) => Math.min(pageCount - 1, value + 1)),
                className: "min-h-11 min-w-11 px-2 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40",
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
import { jsx as jsx4, jsxs as jsxs3 } from "react/jsx-runtime";
function DashboardShell({ title, eyebrow = "Sidemantic", status, toolbar, children }) {
  return /* @__PURE__ */ jsxs3("main", {
    className: "mx-auto max-w-6xl px-4 py-5 text-slate-950 sm:px-6",
    children: [
      /* @__PURE__ */ jsxs3("header", {
        className: "flex flex-wrap items-end justify-between gap-4 border-b border-slate-200 pb-4",
        children: [
          /* @__PURE__ */ jsxs3("div", {
            children: [
              /* @__PURE__ */ jsx4("p", {
                className: "text-xs font-medium uppercase tracking-normal text-slate-500",
                children: eyebrow
              }),
              /* @__PURE__ */ jsx4("h1", {
                className: "mt-1 text-2xl font-semibold tracking-normal",
                children: title
              })
            ]
          }),
          status ? /* @__PURE__ */ jsx4("div", {
            className: "text-sm text-slate-500",
            children: status
          }) : null
        ]
      }),
      toolbar ? /* @__PURE__ */ jsx4("section", {
        className: "flex flex-wrap gap-2 py-3",
        children: toolbar
      }) : null,
      /* @__PURE__ */ jsx4("section", {
        className: "grid gap-4 py-4",
        children
      })
    ]
  });
}
// webapp/src/components/TimeSeriesChart.tsx
import { useEffect as useEffect3, useRef as useRef2, useState as useState4 } from "react";
import { jsx as jsx5, jsxs as jsxs4, Fragment as Fragment2 } from "react/jsx-runtime";
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
  comparisonLabel = "Previous",
  ariaLabel,
  onBrush
}) {
  const container = useRef2(null);
  const svgRef = useRef2(null);
  const dragging = useRef2(false);
  const brushRef = useRef2(null);
  const [width, setWidth] = useState4(820);
  const [hover, setHover] = useState4(null);
  const [brush, setBrush] = useState4(null);
  useEffect3(() => {
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
  const summary = ariaLabel ?? (empty ? "Time series chart: not enough data to plot." : `Time series chart, ${count} points from ${points[0].x} to ${points[count - 1].x}.`);
  return /* @__PURE__ */ jsxs4("div", {
    className: "relative border border-line bg-surface text-chart-primary",
    children: [
      /* @__PURE__ */ jsxs4("div", {
        className: "absolute right-3 top-2 z-10 flex items-center gap-3 text-2xs text-faint",
        children: [
          /* @__PURE__ */ jsxs4("span", {
            className: "flex items-center gap-1",
            children: [
              /* @__PURE__ */ jsx5("span", {
                className: "inline-block h-0.5 w-3 bg-chart-primary"
              }),
              " Current"
            ]
          }),
          comparison?.length ? /* @__PURE__ */ jsxs4("span", {
            className: "flex items-center gap-1",
            children: [
              /* @__PURE__ */ jsx5("span", {
                className: "inline-block h-0 w-3 border-t border-dashed border-faint"
              }),
              " ",
              comparisonLabel
            ]
          }) : null
        ]
      }),
      /* @__PURE__ */ jsx5("div", {
        ref: container,
        className: "w-full",
        children: empty ? /* @__PURE__ */ jsx5("div", {
          className: "grid h-[280px] place-items-center text-xs text-faint",
          children: "Not enough data to chart."
        }) : /* @__PURE__ */ jsxs4("svg", {
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
            /* @__PURE__ */ jsx5("defs", {
              children: /* @__PURE__ */ jsxs4("linearGradient", {
                id: "ts-fill",
                x1: "0",
                y1: "0",
                x2: "0",
                y2: "1",
                children: [
                  /* @__PURE__ */ jsx5("stop", {
                    offset: "0%",
                    stopColor: "currentColor",
                    stopOpacity: 0.18
                  }),
                  /* @__PURE__ */ jsx5("stop", {
                    offset: "100%",
                    stopColor: "currentColor",
                    stopOpacity: 0
                  })
                ]
              })
            }),
            ticks.map((value, index) => /* @__PURE__ */ jsxs4("g", {
              children: [
                /* @__PURE__ */ jsx5("line", {
                  x1: PAD.left,
                  x2: width - PAD.right,
                  y1: yAt(value),
                  y2: yAt(value),
                  className: "stroke-line"
                }),
                /* @__PURE__ */ jsx5("text", {
                  x: PAD.left - 8,
                  y: yAt(value) + 3,
                  textAnchor: "end",
                  className: "fill-faint font-mono text-[10px]",
                  children: formatAxis(value)
                })
              ]
            }, index)),
            comparison && comparison.length >= 2 ? /* @__PURE__ */ jsx5("path", {
              d: gappedPath(comparison),
              fill: "none",
              className: "stroke-faint",
              strokeWidth: 1.25,
              strokeDasharray: "4 3"
            }) : null,
            /* @__PURE__ */ jsx5("path", {
              d: area,
              fill: "url(#ts-fill)"
            }),
            /* @__PURE__ */ jsx5("path", {
              d: `M ${line}`,
              fill: "none",
              stroke: "currentColor",
              strokeWidth: 1.75
            }),
            brush ? /* @__PURE__ */ jsx5("rect", {
              x: Math.min(brush.a, brush.b),
              y: PAD.top,
              width: Math.abs(brush.b - brush.a),
              height: plotH,
              className: "fill-chart-primary",
              opacity: 0.12
            }) : null,
            safeHover != null && hoverCur ? /* @__PURE__ */ jsxs4("g", {
              children: [
                /* @__PURE__ */ jsx5("line", {
                  x1: xAt(safeHover),
                  x2: xAt(safeHover),
                  y1: PAD.top,
                  y2: HEIGHT - PAD.bottom,
                  className: "stroke-faint",
                  strokeDasharray: "3 3"
                }),
                hoverPrev ? /* @__PURE__ */ jsx5("circle", {
                  cx: xAt(safeHover),
                  cy: yAt(hoverPrev.y),
                  r: 3,
                  className: "fill-faint"
                }) : null,
                /* @__PURE__ */ jsx5("circle", {
                  cx: xAt(safeHover),
                  cy: yAt(hoverCur.y),
                  r: 3.5,
                  fill: "currentColor"
                })
              ]
            }) : null,
            points.map((point, index) => index % labelEvery === 0 || index === count - 1 ? /* @__PURE__ */ jsx5("text", {
              x: xAt(index),
              y: HEIGHT - 8,
              textAnchor: "middle",
              className: "fill-faint font-mono text-[10px]",
              children: point.x
            }, point.x) : null)
          ]
        })
      }),
      /* @__PURE__ */ jsx5(ChartTooltip, {
        tip: hoverCur ? {
          x: tooltipLeft,
          y: 32,
          content: /* @__PURE__ */ jsxs4(Fragment2, {
            children: [
              /* @__PURE__ */ jsx5("div", {
                className: "mb-0.5 font-mono text-faint",
                children: hoverCur.x
              }),
              /* @__PURE__ */ jsxs4("div", {
                className: "flex items-center justify-between gap-3",
                children: [
                  /* @__PURE__ */ jsx5("span", {
                    className: "text-muted",
                    children: "Current"
                  }),
                  /* @__PURE__ */ jsx5("span", {
                    className: "font-mono tnum font-medium text-ink",
                    children: formatValue2(hoverCur.y)
                  })
                ]
              }),
              hoverPrev ? /* @__PURE__ */ jsxs4("div", {
                className: "flex items-center justify-between gap-3",
                children: [
                  /* @__PURE__ */ jsx5("span", {
                    className: "text-muted",
                    children: comparisonLabel
                  }),
                  /* @__PURE__ */ jsx5("span", {
                    className: "font-mono tnum text-muted",
                    children: formatValue2(hoverPrev.y)
                  })
                ]
              }) : null,
              delta != null ? /* @__PURE__ */ jsxs4("div", {
                className: `mt-0.5 text-right font-mono ${delta > 0 ? "text-accent" : delta < 0 ? "text-danger" : "text-faint"}`,
                children: [
                  delta.toLocaleString(undefined, { maximumFractionDigits: 1, signDisplay: "exceptZero" }),
                  "%"
                ]
              }) : null
            ]
          })
        } : null,
        position: "absolute",
        offset: 0,
        style: { transform: "translateX(-50%)" },
        className: "whitespace-nowrap border border-line bg-surface px-2 py-1.5 text-2xs shadow-[var(--shadow)]"
      })
    ]
  });
}

// webapp/src/components/DistributionAdapters.tsx
import { jsx as jsx6 } from "react/jsx-runtime";
function DataPreviewTable({ result, pageSize = 10 }) {
  const columns = result?.columns ?? [];
  return /* @__PURE__ */ jsx6(DataTable, {
    columns: columns.map((key) => ({ key, label: labelize2(key), numeric: result?.sample_rows.some((row) => typeof row[key] === "number") })),
    rows: result?.sample_rows ?? [],
    pageSize,
    renderCell: (_column, value) => formatValue(value)
  });
}
function LineChart({ data, height = 200, ariaLabel }) {
  return /* @__PURE__ */ jsx6("div", {
    style: { minHeight: height },
    children: /* @__PURE__ */ jsx6(TimeSeriesChart, {
      points: data.map(({ label, value }) => ({ x: label, y: value })),
      formatValue: (value) => formatValue(value),
      ariaLabel
    })
  });
}
// webapp/src/components/ErrorBoundary.tsx
import { Component } from "react";
import { jsx as jsx7, jsxs as jsxs5 } from "react/jsx-runtime";

class ErrorBoundary extends Component {
  state = {};
  static getDerivedStateFromError(error) {
    return { error };
  }
  render() {
    if (this.state.error) {
      return /* @__PURE__ */ jsx7("div", {
        className: "p-4",
        children: /* @__PURE__ */ jsxs5("div", {
          className: "border border-danger/40 bg-surface p-4",
          children: [
            /* @__PURE__ */ jsx7("p", {
              className: "text-sm font-semibold text-danger",
              children: "Something went wrong rendering this view."
            }),
            /* @__PURE__ */ jsx7("p", {
              className: "mt-1 break-words text-xs text-muted",
              children: this.state.error.message
            }),
            /* @__PURE__ */ jsxs5("div", {
              className: "mt-3 flex gap-2",
              children: [
                /* @__PURE__ */ jsx7("button", {
                  type: "button",
                  onClick: () => this.setState({ error: undefined }),
                  className: "border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink",
                  children: "Retry"
                }),
                /* @__PURE__ */ jsx7("button", {
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
import { jsx as jsx8, jsxs as jsxs6 } from "react/jsx-runtime";
function FilterPill({ dimension, dimensionLabel, value, onRemove }) {
  return /* @__PURE__ */ jsxs6("span", {
    "data-dimension": dimension,
    "data-value": value,
    className: "inline-flex max-w-full items-center gap-1.5 border border-line bg-surface px-2 py-0.5 text-2xs text-muted",
    children: [
      /* @__PURE__ */ jsxs6("span", {
        className: "truncate",
        children: [
          /* @__PURE__ */ jsxs6("span", {
            className: "text-faint",
            children: [
              dimensionLabel ?? labelize2(dimension),
              ":"
            ]
          }),
          " ",
          displayDimValue(value)
        ]
      }),
      onRemove ? /* @__PURE__ */ jsx8("button", {
        type: "button",
        "aria-label": `Remove filter ${value}`,
        onClick: onRemove,
        className: "grid size-3.5 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink",
        children: "×"
      }) : null
    ]
  });
}
// webapp/src/components/Leaderboard.tsx
import { jsx as jsx9, jsxs as jsxs7 } from "react/jsx-runtime";
function Leaderboard({
  dimension,
  title,
  metricLabel,
  rows,
  selectedValues = [],
  loading,
  formatMetric,
  onToggle,
  collapsedLimit = 6,
  expanded = false,
  onExpandedChange
}) {
  const selected = new Set(selectedValues);
  const visibleRows = expanded ? rows : rows.slice(0, collapsedLimit);
  const maxMagnitude = Math.max(1, ...visibleRows.map((row) => Math.abs(row.metric)));
  const expandable = expanded || rows.length > collapsedLimit;
  return /* @__PURE__ */ jsxs7("section", {
    "data-testid": "dimension-leaderboard",
    "data-dimension": dimension,
    "data-expanded": expanded || undefined,
    "aria-label": `${title}, ranked by ${metricLabel}`,
    className: "flex min-h-60 flex-col border-b border-r border-line bg-surface data-[expanded=true]:col-span-full",
    children: [
      /* @__PURE__ */ jsxs7("header", {
        className: "px-3 pb-2 pt-2.5",
        children: [
          /* @__PURE__ */ jsx9("h3", {
            className: "truncate text-sm font-semibold text-ink",
            children: title
          }),
          /* @__PURE__ */ jsxs7("p", {
            className: "sr-only",
            children: [
              "Ranked by ",
              metricLabel
            ]
          })
        ]
      }),
      /* @__PURE__ */ jsx9("div", {
        "data-testid": "leaderboard-rows",
        children: loading && rows.length === 0 ? /* @__PURE__ */ jsx9("div", {
          className: "space-y-2 p-3",
          children: [0, 1, 2, 3].map((i) => /* @__PURE__ */ jsx9("div", {
            className: "skeleton h-5 w-full"
          }, i))
        }) : rows.length === 0 ? /* @__PURE__ */ jsx9("p", {
          className: "px-3 py-4 text-xs text-faint",
          children: "No values"
        }) : visibleRows.map((row) => {
          const tone = row.metric < 0 ? "negative" : "positive";
          const isSelected = selected.has(row.value);
          const width = `${Math.round(Math.abs(row.metric) / maxMagnitude * 100)}%`;
          return /* @__PURE__ */ jsxs7("button", {
            type: "button",
            "data-dimension": dimension,
            "data-value": row.value,
            "data-selected": isSelected || undefined,
            "data-tone": tone,
            onClick: () => onToggle?.(row.value),
            "aria-pressed": isSelected,
            className: "leaderboard-row relative grid w-full grid-cols-[minmax(0,1fr)_auto] items-center gap-2 overflow-hidden border-0 bg-transparent px-3 py-1 text-left text-xs text-ink data-[selected=true]:bg-chart-primary-selected",
            children: [
              /* @__PURE__ */ jsx9("span", {
                "aria-hidden": "true",
                className: `absolute inset-y-0 left-0 ${tone === "negative" ? "bg-danger-soft" : "bg-chart-primary-soft"}`,
                style: { width }
              }),
              /* @__PURE__ */ jsx9("span", {
                className: "relative min-w-0 truncate text-muted",
                children: displayDimValue(row.value)
              }),
              /* @__PURE__ */ jsx9("strong", {
                className: "relative tnum font-semibold text-ink",
                children: formatMetric(row.metric)
              })
            ]
          }, `${dimension}:${row.value}`);
        })
      }),
      expandable && !loading ? /* @__PURE__ */ jsx9("button", {
        type: "button",
        "data-action": expanded ? "leaderboard-back" : "leaderboard-expand",
        "aria-expanded": expanded,
        onClick: () => onExpandedChange?.(!expanded),
        className: "leaderboard-expand mt-1 min-h-9 border-0 border-t border-line bg-transparent px-3 text-left text-xs font-normal text-faint hover:text-accent",
        children: expanded ? "← All dimensions" : `Expand table (${rows.length})`
      }) : null
    ]
  });
}
// webapp/src/components/MetricCard.tsx
import { useState as useState6 } from "react";

// webapp/src/components/Sparkline.tsx
import { useEffect as useEffect4, useRef as useRef3, useState as useState5 } from "react";
import { jsx as jsx10, jsxs as jsxs8, Fragment as Fragment3 } from "react/jsx-runtime";
function Sparkline({
  values,
  labels,
  height = 44,
  ariaLabel,
  formatValue: formatValue2 = (value) => value.toLocaleString(undefined, { maximumFractionDigits: 2 }),
  onHover,
  onBrush
}) {
  const containerRef = useRef3(null);
  const svgRef = useRef3(null);
  const dragStart = useRef3(null);
  const [width, setWidth] = useState5(200);
  const [hover, setHover] = useState5(null);
  const [brush, setBrush] = useState5(null);
  const [tip, setTip] = useState5(null);
  useEffect4(() => {
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
    return /* @__PURE__ */ jsx10("svg", {
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
  return /* @__PURE__ */ jsxs8("span", {
    ref: containerRef,
    className: "relative block w-full",
    children: [
      /* @__PURE__ */ jsxs8("svg", {
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
          /* @__PURE__ */ jsx10("path", {
            d: area,
            fill: "currentColor",
            opacity: 0.1
          }),
          /* @__PURE__ */ jsx10("path", {
            d: `M ${line}`,
            fill: "none",
            stroke: "currentColor",
            strokeWidth: 1.5,
            vectorEffect: "non-scaling-stroke"
          }),
          brush ? /* @__PURE__ */ jsx10("rect", {
            x: Math.min(brush.a, brush.b),
            y: 0,
            width: Math.abs(brush.b - brush.a),
            height,
            fill: "currentColor",
            opacity: 0.12
          }) : null,
          hovered ? /* @__PURE__ */ jsxs8(Fragment3, {
            children: [
              /* @__PURE__ */ jsx10("line", {
                x1: hovered.x,
                x2: hovered.x,
                y1: 0,
                y2: height,
                stroke: "currentColor",
                strokeWidth: 1,
                opacity: 0.45
              }),
              /* @__PURE__ */ jsx10("circle", {
                cx: hovered.x,
                cy: hovered.y,
                r: 2.5,
                fill: "currentColor"
              })
            ]
          }) : /* @__PURE__ */ jsx10("circle", {
            cx: latest.x,
            cy: latest.y,
            r: 2.25,
            fill: "currentColor"
          })
        ]
      }),
      /* @__PURE__ */ jsx10(ChartTooltip, {
        tip
      })
    ]
  });
}

// webapp/src/components/MetricCard.tsx
import { jsx as jsx11, jsxs as jsxs9, Fragment as Fragment4 } from "react/jsx-runtime";
var TONE_CLASS = {
  positive: "text-accent",
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
  sparkValues = [],
  sparkLabels,
  selected,
  loading,
  onSelect,
  onSparkHover,
  onSparkBrush
}) {
  const [sparkHover, setSparkHover] = useState6(null);
  const summary = /* @__PURE__ */ jsxs9(Fragment4, {
    children: [
      /* @__PURE__ */ jsxs9("div", {
        className: "flex items-baseline justify-between gap-2",
        children: [
          /* @__PURE__ */ jsx11("span", {
            className: "truncate text-2xs font-semibold uppercase tracking-wide text-faint",
            children: label
          }),
          sparkHover?.label ? /* @__PURE__ */ jsx11("span", {
            className: "shrink-0 font-mono text-2xs text-faint",
            children: sparkHover.label
          }) : delta ? /* @__PURE__ */ jsxs9("span", {
            "data-tone": delta.tone,
            className: `shrink-0 text-2xs font-medium ${TONE_CLASS[delta.tone]}`,
            children: [
              /* @__PURE__ */ jsx11("span", {
                "aria-hidden": "true",
                className: "mr-0.5 text-[8px]",
                children: TONE_ARROW[delta.tone]
              }),
              delta.label
            ]
          }) : null
        ]
      }),
      /* @__PURE__ */ jsx11("div", {
        className: "font-mono tnum text-base font-semibold text-ink",
        children: loading ? /* @__PURE__ */ jsx11("span", {
          className: "skeleton inline-block h-5 w-24 align-middle"
        }) : sparkHover ? formatValue(sparkHover.value, format) : valueText ?? formatValue(value, format)
      })
    ]
  });
  const className = "group flex w-full flex-col gap-1.5 border border-line bg-surface px-3 py-2.5 text-left data-[selected=true]:border-accent data-[selected=true]:ring-1 data-[selected=true]:ring-accent";
  const sparkline = /* @__PURE__ */ jsx11(Sparkline, {
    values: sparkValues,
    labels: sparkLabels,
    onHover: (point) => {
      setSparkHover(point);
      onSparkHover?.(point);
    },
    onBrush: onSparkBrush,
    formatValue: (sparkValue) => formatValue(sparkValue, format)
  });
  if (!onSelect) {
    return /* @__PURE__ */ jsxs9("article", {
      "data-metric": metric,
      "data-selected": selected || undefined,
      className,
      children: [
        summary,
        sparkline
      ]
    });
  }
  return /* @__PURE__ */ jsxs9("article", {
    "data-metric": metric,
    "data-selected": selected || undefined,
    className,
    children: [
      /* @__PURE__ */ jsx11("button", {
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
// webapp/src/components/QueryDebugPanel.tsx
import { jsx as jsx12, jsxs as jsxs10 } from "react/jsx-runtime";
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
  return /* @__PURE__ */ jsxs10("details", {
    className: "border border-line bg-surface",
    children: [
      /* @__PURE__ */ jsx12("summary", {
        className: "cursor-pointer px-3 py-2 text-2xs font-semibold uppercase tracking-wide text-faint",
        children: "Generated SQL"
      }),
      Object.keys(inputs).length > 0 ? /* @__PURE__ */ jsx12("div", {
        "data-testid": "query-inputs",
        className: "grid gap-px border-t border-line bg-line sm:grid-cols-2",
        children: Object.entries(inputs).map(([name, input]) => input ? /* @__PURE__ */ jsxs10("section", {
          className: "min-w-0 bg-surface px-3 py-2 text-2xs",
          children: [
            /* @__PURE__ */ jsx12("h3", {
              className: "mb-1 font-semibold text-ink",
              children: name
            }),
            [
              ["Metrics", input.metrics],
              ["Dimensions", input.dimensions],
              ["Filters", input.filters]
            ].map(([label, values]) => values?.length ? /* @__PURE__ */ jsxs10("p", {
              className: "truncate text-muted",
              title: values.join(", "),
              children: [
                /* @__PURE__ */ jsxs10("strong", {
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
      /* @__PURE__ */ jsx12("pre", {
        "data-testid": "query-debug",
        className: "max-h-72 overflow-auto whitespace-pre-wrap border-t border-line px-3 py-2 font-mono text-2xs text-muted",
        children: tokens.map((token, index) => TOKEN_CLASS[token.kind] ? /* @__PURE__ */ jsx12("span", {
          className: TOKEN_CLASS[token.kind],
          "data-token": token.kind,
          children: token.value
        }, index) : token.value)
      })
    ]
  });
}
// webapp/src/components/States.tsx
import { jsx as jsx13, jsxs as jsxs11 } from "react/jsx-runtime";
function StateBox({ tone, title, message }) {
  const danger = tone === "danger";
  return /* @__PURE__ */ jsx13("div", {
    className: `grid min-h-[200px] place-items-center border bg-surface p-6 text-center ${danger ? "border-danger/40" : "border-line"}`,
    "data-state": tone,
    role: danger ? "alert" : "status",
    "aria-live": danger ? "assertive" : "polite",
    children: /* @__PURE__ */ jsxs11("div", {
      className: "max-w-md",
      children: [
        tone === "loading" ? /* @__PURE__ */ jsx13("span", {
          "aria-hidden": "true",
          className: "motion-safe:animate-pulse inline-block size-2 rounded-full bg-accent"
        }) : null,
        title ? /* @__PURE__ */ jsx13("h3", {
          className: `text-sm font-semibold ${danger ? "text-danger" : "text-ink"}`,
          children: title
        }) : null,
        /* @__PURE__ */ jsx13("p", {
          className: `mt-1 text-xs ${danger ? "text-danger" : "text-muted"}`,
          children: message
        })
      ]
    })
  });
}
function LoadingState({ title = "Loading", message = "Loading metrics…" }) {
  return /* @__PURE__ */ jsx13(StateBox, {
    tone: "loading",
    title,
    message
  });
}
function EmptyState({ title = "No results", message }) {
  return /* @__PURE__ */ jsx13(StateBox, {
    tone: "muted",
    title,
    message
  });
}
function ErrorState({ title = "Query failed", message }) {
  return /* @__PURE__ */ jsx13(StateBox, {
    tone: "danger",
    title,
    message
  });
}
function StatusDot({ status }) {
  const color = status === "ok" ? "bg-accent" : status === "loading" ? "bg-faint animate-pulse" : "bg-line";
  return /* @__PURE__ */ jsx13("span", {
    "aria-hidden": "true",
    className: `inline-block size-2 rounded-full ${color}`
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
  dangerSoft: "--danger-soft"
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
export {
  useChartTooltip,
  tokenizeSql,
  toggleTheme,
  toggleFilterValue,
  removeFilterValue,
  removeFilterDimension,
  paginateRows,
  normalizeFilterValue,
  labelize2 as labelize,
  getTheme,
  formatValue,
  formatCompact,
  applyThemeTokens,
  applyTheme,
  aliasForSemanticRef,
  TimeSeriesChart,
  StatusDot,
  Sparkline,
  QueryDebugPanel,
  MetricCard,
  LoadingState,
  LineChart,
  Leaderboard,
  FilterPill,
  ErrorState,
  ErrorBoundary,
  EmptyState,
  DataTable,
  DataPreviewTable,
  DashboardShell,
  ColumnChart,
  ChartTooltip
};
