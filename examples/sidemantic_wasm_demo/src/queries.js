export function aliasFor(ref) {
  return String(ref || "").split(".").at(-1);
}

export function dimensionQueryKey(dimensionRef) {
  return `dimension_${aliasFor(dimensionRef)}`;
}

export function timeDimensionRef(timeGrain) {
  return `orders.created__${timeGrain}`;
}

export function timeDimensionAlias(timeGrain) {
  return `created__${timeGrain}`;
}

function quoteYamlString(value) {
  return `"${String(value).replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

function quoteSqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

export function activeFilterStrings(filters, excludedDimension = null) {
  return Object.entries(filters || {})
    .filter(([dimension]) => dimension !== excludedDimension)
    .flatMap(([dimension, values]) => {
      const uniqueValues = [...new Set((values || []).map((value) => String(value)))];
      if (uniqueValues.length === 0) return [];
      if (uniqueValues.length === 1) return [`${dimension} = ${quoteSqlString(uniqueValues[0])}`];
      return [`${dimension} IN (${uniqueValues.map(quoteSqlString).join(", ")})`];
    });
}

export function hasActiveFilters(filters) {
  return Object.values(filters || {}).some((values) => values.length > 0);
}

export function queryYaml({ metrics, dimensions = [], filters = [], orderBy = [], limit, ungrouped = false }) {
  const lines = [
    "metrics:",
    ...metrics.map((metric) => `  - ${metric}`),
    "dimensions:",
    ...(dimensions.length ? dimensions.map((dimension) => `  - ${dimension}`) : ["  []"]),
  ];

  if (filters.length > 0) {
    lines.push("filters:", ...filters.map((filter) => `  - ${quoteYamlString(filter)}`));
  }
  if (orderBy.length > 0) {
    lines.push("order_by:", ...orderBy.map((item) => `  - ${quoteYamlString(item)}`));
  }
  if (limit) lines.push(`limit: ${limit}`);
  if (ungrouped) lines.push("ungrouped: true");
  lines.push("skip_default_time_dimensions: true");
  return `${lines.join("\n")}\n`;
}

export function buildQueries({ dimensions, filters, metrics, selectedMetric, timeGrain }) {
  const activeFilters = activeFilterStrings(filters);
  const metricRefs = metrics.map((metric) => metric.key);
  const queries = {
    totals: queryYaml({ metrics: metricRefs, filters: activeFilters }),
    series: queryYaml({
      metrics: metricRefs,
      dimensions: [timeDimensionRef(timeGrain)],
      filters: activeFilters,
      orderBy: [`${timeDimensionRef(timeGrain)} ASC`],
      limit: timeGrain === "day" ? 240 : 24,
    }),
    preview: queryYaml({
      metrics: [selectedMetric],
      dimensions: [timeDimensionRef(timeGrain), "customers.region", "products.category", "orders.status"],
      filters: activeFilters,
      orderBy: [`${selectedMetric} DESC`],
      limit: 12,
    }),
  };

  for (const dimension of dimensions) {
    queries[dimensionQueryKey(dimension.key)] = queryYaml({
      metrics: [selectedMetric],
      dimensions: [dimension.key],
      filters: activeFilterStrings(filters, dimension.key),
      orderBy: [`${selectedMetric} DESC`],
      limit: 8,
    });
  }

  return queries;
}
