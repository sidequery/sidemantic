export function labelize(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replaceAll(".", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .trim();
}

export function aliasForSemanticRef(ref) {
  return String(ref || "").split(".").at(-1);
}

export function formatUiValue(value, options = {}) {
  if (value === null || value === undefined || value === "") return "—";
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  const format = String(options.format || "").toLowerCase();
  const percent = options.style === "percent" || format.includes("%") || format.includes("percent") || format.includes("pct") || options.type === "ratio";
  if (percent) {
    const scaled = options.style === "percent" ? numeric : Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
    return scaled.toLocaleString(undefined, {
      maximumFractionDigits: options.maximumFractionDigits ?? 1,
      style: options.style === "percent" ? "percent" : "decimal",
    }) + (options.style === "percent" ? "" : "%");
  }
  const currency = options.style === "currency" || options.currency || format.includes("$") || format.includes("usd") || format.includes("currency") || format.includes("dollar");
  return numeric.toLocaleString(undefined, {
    currency: currency ? options.currency || "USD" : undefined,
    maximumFractionDigits: options.maximumFractionDigits ?? 2,
    notation: options.compact ? "compact" : "standard",
    style: currency ? "currency" : "decimal",
  });
}

export function formatUiCompact(value, options = {}) {
  return formatUiValue(value, { ...options, compact: true, maximumFractionDigits: options.maximumFractionDigits ?? 1 });
}

export function normalizeFilterValue(value) {
  return String(value ?? "");
}

export function toggleFilterValue(filters, dimension, value) {
  const next = { ...filters };
  const normalized = normalizeFilterValue(value);
  const selected = new Set((next[dimension] || []).map(normalizeFilterValue));
  if (selected.has(normalized)) selected.delete(normalized);
  else selected.add(normalized);
  if (selected.size) next[dimension] = [...selected];
  else delete next[dimension];
  return next;
}

export function removeFilterDimension(filters, dimension) {
  const next = { ...filters };
  delete next[dimension];
  return next;
}

export function removeFilterValue(filters, dimension, value) {
  const next = { ...filters };
  const normalized = normalizeFilterValue(value);
  const values = (next[dimension] || []).map(normalizeFilterValue).filter((item) => item !== normalized);
  if (values.length) next[dimension] = values;
  else delete next[dimension];
  return next;
}

export function paginateRows(rows, page, pageSize) {
  const paginate = pageSize > 0 && rows.length > pageSize;
  const pageCount = paginate ? Math.ceil(rows.length / pageSize) : 1;
  const safePage = Math.max(0, Math.min(page, pageCount - 1));
  const start = paginate ? safePage * pageSize : 0;
  return {
    paginate,
    pageCount,
    safePage,
    start,
    visibleRows: paginate ? rows.slice(start, start + pageSize) : rows,
  };
}
