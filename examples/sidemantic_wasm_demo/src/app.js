import {
  highlightCode,
  metricConfigFor,
  metricValueFormat,
  removeFilterValue,
  renderDataPreview,
  renderDimensionLeaderboardCards,
  renderFilterPills,
  renderHighlightedQueryDebug,
  renderMetricSummaryCards,
  renderSelectOptions,
  renderValidationState,
  setControlsDisabled,
  syncScrollPosition,
  toComponentResult,
  toggleFilterValue,
} from "./components/sidemantic/sidemantic-ui-static.js";
import { createDemoData, DIMENSIONS, METRICS, MODEL_YAML, TIME_GRAINS } from "./demo/ecommerce.js";
import { buildQueries, dimensionQueryKey, queryYaml, timeDimensionAlias } from "./queries.js";
import { createDuckDBRuntime } from "./runtime/duckdb-runtime.js";
import { createSidemanticRuntime } from "./runtime/sidemantic-runtime.js";

const DEFAULT_METRIC = "orders.total_revenue";
const DEFAULT_TIME_GRAIN = "month";

const state = {
  duckdb: null,
  filters: {},
  isBusy: false,
  results: {},
  selectedMetric: DEFAULT_METRIC,
  sidemantic: null,
  sql: {},
  timeGrain: DEFAULT_TIME_GRAIN,
  validationErrors: [],
};

const els = {
  appStatus: document.querySelector('[data-testid="app-status"]'),
  compileTime: document.querySelector('[data-testid="compile-time"]'),
  dimensionCards: document.querySelector('[data-testid="dimension-cards"]'),
  duckdbBundle: document.querySelector('[data-testid="duckdb-bundle"]'),
  duckdbStatus: document.querySelector('[data-testid="duckdb-status"]'),
  executionTime: document.querySelector('[data-testid="execution-time"]'),
  filterPills: document.querySelector('[data-testid="filter-pills"]'),
  generatedSql: document.querySelector('[data-testid="generated-sql"]'),
  graphModels: document.querySelector('[data-testid="graph-models"]'),
  grainSelect: document.querySelector('[data-testid="grain-select"]'),
  metricCards: document.querySelector('[data-testid="metric-cards"]'),
  metricSelect: document.querySelector('[data-testid="metric-select"]'),
  preview: document.querySelector('[data-testid="result-preview"]'),
  sidemanticStatus: document.querySelector('[data-testid="sidemantic-status"]'),
  sqlSelect: document.querySelector('[data-testid="sql-select"]'),
  statusMessage: document.querySelector('[data-testid="status-message"]'),
  validationList: document.querySelector('[data-testid="validation-list"]'),
  validationState: document.querySelector('[data-testid="validation-state"]'),
  yaml: document.querySelector('[data-testid="model-yaml"]'),
  yamlHighlight: document.querySelector('[data-testid="model-yaml-highlight"]'),
};

function metricConfig(metricKey) {
  return metricConfigFor(METRICS, metricKey);
}

function valueFormatForMetric(metricKey) {
  return metricValueFormat(METRICS, metricKey);
}

function dimensionResult(dimension) {
  return state.results[dimensionQueryKey(dimension.key)] || { columns: [], rows: [] };
}

function sqlOptions() {
  return [
    { label: "metric totals", value: "totals" },
    { label: "metric series", value: "series" },
    ...DIMENSIONS.map((dimension) => ({
      label: dimension.label,
      value: dimensionQueryKey(dimension.key),
    })),
    { label: "preview", value: "preview" },
    { label: "rewrite sample", value: "rewrite" },
  ];
}

function renderControls() {
  renderSelectOptions(els.metricSelect, METRICS, state.selectedMetric);
  renderSelectOptions(els.grainSelect, TIME_GRAINS, state.timeGrain);
}

function renderFilters() {
  renderFilterPills(
    els.filterPills,
    state.filters,
    async ({ dimension, value }) => {
      if (state.isBusy) return;
      state.filters = removeFilterValue(state.filters, dimension, value);
      await runCompile();
    },
    { emptyLabel: "No filters" },
  );
}

function renderMetrics() {
  renderMetricSummaryCards(els.metricCards, {
    detailLabel: ({ range }) => `${range} | ${state.timeGrain} grain`,
    metrics: METRICS,
    onSelect: async ({ metric }) => {
      if (state.isBusy || metric === state.selectedMetric) return;
      state.selectedMetric = metric;
      await runCompile();
    },
    selectedMetric: state.selectedMetric,
    seriesRows: state.results.series?.rows || [],
    timeKey: timeDimensionAlias(state.timeGrain),
    totals: state.results.totals,
    valueFormat: valueFormatForMetric,
  });
}

function renderDimensions() {
  renderDimensionLeaderboardCards(els.dimensionCards, DIMENSIONS, {
    interactive: true,
    metricName: metricConfig(state.selectedMetric).label,
    metricRef: state.selectedMetric,
    onSelect: async ({ dimension, value }) => {
      if (state.isBusy) return;
      state.filters = toggleFilterValue(state.filters, dimension, value);
      await runCompile();
    },
    resultForDimension: dimensionResult,
    selectedValuesForDimension: (dimension) => state.filters[dimension.key] || [],
    valueFormat: valueFormatForMetric(state.selectedMetric),
  });
}

function renderSqlOptions() {
  const options = sqlOptions();
  const current = els.sqlSelect.value || "series";
  renderSelectOptions(els.sqlSelect, options, options.some((option) => option.value === current) ? current : "series");
}

function renderSql() {
  const selected = els.sqlSelect.value || "series";
  renderHighlightedQueryDebug(els.generatedSql, { [selected]: { sql: state.sql[selected] || "Compile first." } });
}

function renderYaml() {
  highlightCode(els.yamlHighlight, els.yaml.value, "yaml");
}

function renderValidation() {
  renderValidationState(els.validationState, els.validationList, state.validationErrors);
}

function renderPreview() {
  renderDataPreview(els.preview, toComponentResult(state.results.preview));
}

function render() {
  renderControls();
  renderFilters();
  renderMetrics();
  renderDimensions();
  renderSqlOptions();
  renderSql();
  renderValidation();
  renderPreview();
}

function setBusy(isBusy) {
  state.isBusy = isBusy;
  setControlsDisabled(".controls button, .controls select", isBusy);
}

function setStatus(message, status = "loading") {
  els.statusMessage.textContent = message;
  els.appStatus.className = `widget-status ${status}`;
}

function reportError(error) {
  els.appStatus.dataset.errorStack = error?.stack || "";
  window.__sidemanticDemoError = {
    message: error?.message || String(error),
    stack: error?.stack || "",
  };
  console.error(error);
}

function validateModel() {
  if (!state.sidemantic) {
    setStatus("Sidemantic Rust WASM is still loading. Validation will be available when it is ready.");
    return false;
  }

  const queries = DIMENSIONS.map((dimension) => ({
    dimension: dimension.key,
    yaml: queryYaml({ metrics: [state.selectedMetric], dimensions: [dimension.key] }),
  }));
  state.validationErrors = queries.flatMap(({ dimension, yaml }) =>
    state.sidemantic.validate(els.yaml.value, yaml).map((error) => `${dimension}: ${error}`),
  );
  renderValidation();
  return true;
}

async function compileAndExecute() {
  const startedCompile = performance.now();
  const queries = buildQueries({
    dimensions: DIMENSIONS,
    filters: state.filters,
    metrics: METRICS,
    selectedMetric: state.selectedMetric,
    timeGrain: state.timeGrain,
  });
  const compiled = {};
  for (const [name, yaml] of Object.entries(queries)) {
    compiled[name] = state.sidemantic.compile(els.yaml.value, yaml);
  }
  compiled.rewrite = state.sidemantic.rewrite(
    els.yaml.value,
    "SELECT orders.total_revenue, customers.region FROM orders ORDER BY orders.total_revenue DESC LIMIT 5",
  );
  els.compileTime.textContent = `compile ${(performance.now() - startedCompile).toFixed(1)} ms`;

  const startedExecution = performance.now();
  const executed = {};
  for (const [name, sql] of Object.entries(compiled)) {
    if (name === "rewrite") continue;
    executed[name] = await state.duckdb.queryRows(sql);
  }
  els.executionTime.textContent = `execute ${(performance.now() - startedExecution).toFixed(1)} ms`;

  state.sql = compiled;
  state.results = executed;
  render();
}

async function runCompile() {
  if (!state.sidemantic || !state.duckdb || state.isBusy) return;
  setBusy(true);
  setStatus("Compiling semantic queries with Sidemantic Rust WASM and executing in DuckDB-WASM...");
  try {
    validateModel();
    await compileAndExecute();
    setStatus("Ready", "");
  } catch (error) {
    setStatus(error?.message || String(error), "error");
    reportError(error);
  } finally {
    setBusy(false);
  }
}

function bindUi() {
  document.querySelector('[data-action="compile"]').addEventListener("click", runCompile);
  document.querySelector('[data-action="validate"]').addEventListener("click", validateModel);
  document.querySelector('[data-action="reset"]').addEventListener("click", async () => {
    if (state.isBusy) return;
    state.filters = {};
    state.selectedMetric = DEFAULT_METRIC;
    state.timeGrain = DEFAULT_TIME_GRAIN;
    await runCompile();
  });
  els.metricSelect.addEventListener("change", async (event) => {
    if (state.isBusy) return;
    state.selectedMetric = event.target.value;
    await runCompile();
  });
  els.grainSelect.addEventListener("change", async (event) => {
    if (state.isBusy) return;
    state.timeGrain = event.target.value;
    await runCompile();
  });
  els.sqlSelect.addEventListener("change", renderSql);
  els.yaml.addEventListener("input", renderYaml);
  els.yaml.addEventListener("scroll", () => syncScrollPosition(els.yaml, els.yamlHighlight.parentElement));
}

async function main() {
  els.yaml.value = MODEL_YAML;
  renderYaml();
  renderControls();
  renderSqlOptions();
  bindUi();

  try {
    state.sidemantic = await createSidemanticRuntime();
    const graph = state.sidemantic.loadGraph(els.yaml.value);
    state.sidemantic.generateCatalogMetadata(els.yaml.value, "browser_demo");
    els.graphModels.textContent = `${graph.models.length} models`;
    els.sidemanticStatus.textContent = "Sidemantic Rust WASM loaded";
    els.sidemanticStatus.dataset.ready = "true";

    state.duckdb = await createDuckDBRuntime(createDemoData());
    els.duckdbBundle.textContent = state.duckdb.bundleName;
    els.duckdbStatus.textContent = "DuckDB-WASM loaded";
    els.duckdbStatus.dataset.ready = "true";

    await runCompile();
  } catch (error) {
    setStatus(error.message, "error");
    reportError(error);
  }
}

main();
