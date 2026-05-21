import {
  renderDataPreview,
  renderFilterPills,
  renderLeaderboard,
  renderMetricCards,
  renderQueryDebug,
} from "./sidemantic-components.js";

const statusEl = document.querySelector('[data-testid="app-status"]');
const totalsEl = document.querySelector('[data-testid="metric-totals"]');
const filterPillsEl = document.querySelector('[data-testid="filter-pills"]');
const resetEl = document.querySelector('[data-action="reset"]');
const leaderboardEl = document.querySelector('[data-testid="leaderboard-rows"]');
const leaderboardTitleEl = document.querySelector('[data-testid="leaderboard-title"]');
const leaderboardSubtitleEl = document.querySelector('[data-testid="leaderboard-subtitle"]');
const previewEl = document.querySelector('[data-testid="data-preview"]');
const debugEl = document.querySelector('[data-testid="query-debug"]');
const shellEl = document.querySelector('[data-testid="dashboard-shell"]');

const state = {
  candidate: null,
  queries: {},
  selectedMetric: "",
  filters: {},
};

function aliasFor(query, ref) {
  if (!ref) return "";
  return query?.output_aliases?.[ref] || ref.split(".").at(-1) || ref;
}

function numericValue(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function selectedLeaderboardMetric(query) {
  const metrics = query?.metrics || [];
  if (metrics.includes(state.selectedMetric)) return state.selectedMetric;
  return metrics[0] || state.selectedMetric;
}

function leaderboardQueryForMetric(query, metricRef) {
  if (!query?.result?.sample_rows) return query;
  const metricKey = aliasFor(query, metricRef);
  return {
    ...query,
    result: {
      ...query.result,
      sample_rows: [...query.result.sample_rows].sort(
        (left, right) => numericValue(right[metricKey]) - numericValue(left[metricKey]),
      ),
    },
  };
}

function selectedLeaderboardValue(query) {
  const dimensionRef = query?.dimensions?.[0];
  const values = state.filters[dimensionRef] || [];
  return values[0];
}

function filterPreviewResult(query) {
  const result = query?.result;
  if (!result?.sample_rows) return { columns: [], sample_rows: [], sample_row_count: 0 };
  let rows = result.sample_rows;
  for (const [dimension, values] of Object.entries(state.filters)) {
    const key = aliasFor(query, dimension);
    if (!result.columns?.includes(key)) continue;
    const accepted = new Set((values || []).map((value) => String(value)));
    rows = rows.filter((row) => accepted.has(String(row[key] ?? "")));
  }
  return {
    ...result,
    sample_rows: rows,
    sample_row_count: rows.length,
  };
}

function metricTotalsForFilters(totalsQuery, leaderboardQuery) {
  const dimensionRef = leaderboardQuery?.dimensions?.[0];
  const filterValues = state.filters[dimensionRef] || [];
  const leaderboardRows = leaderboardQuery?.result?.sample_rows || [];
  if (filterValues.length === 0 || leaderboardRows.length === 0) return totalsQuery;

  const dimensionKey = aliasFor(leaderboardQuery, dimensionRef);
  const accepted = new Set(filterValues.map((value) => String(value)));
  const filteredRows = leaderboardRows.filter((row) => accepted.has(String(row[dimensionKey] ?? "")));
  if (filteredRows.length === 0) return totalsQuery;

  const metricRow = {};
  for (const metric of totalsQuery?.metrics || []) {
    const totalsKey = aliasFor(totalsQuery, metric);
    const leaderboardKey = aliasFor(leaderboardQuery, metric);
    metricRow[totalsKey] = filteredRows.reduce((sum, row) => sum + numericValue(row[leaderboardKey]), 0);
  }

  return {
    ...totalsQuery,
    result: {
      ...totalsQuery.result,
      sample_rows: [metricRow],
      sample_row_count: 1,
    },
  };
}

function setFilter({ dimension, value }) {
  if (!dimension) return;
  state.filters = { ...state.filters, [dimension]: [String(value ?? "")] };
  render();
}

function removeFilter({ dimension, value }) {
  const current = state.filters[dimension] || [];
  const next = current.filter((item) => String(item) !== String(value));
  state.filters = { ...state.filters };
  if (next.length > 0) {
    state.filters[dimension] = next;
  } else {
    delete state.filters[dimension];
  }
  render();
}

function reset() {
  const defaultMetric = state.queries.metric_totals?.metrics?.[0] || state.selectedMetric;
  state.filters = {};
  state.selectedMetric = defaultMetric;
  render();
}

function render() {
  const queries = state.queries;
  const leaderboardMetric = selectedLeaderboardMetric(queries.dimension_leaderboard);
  const leaderboardQuery = leaderboardQueryForMetric(queries.dimension_leaderboard, leaderboardMetric);
  const filteredTotals = metricTotalsForFilters(queries.metric_totals, queries.dimension_leaderboard);
  const previewQuery = queries.preview_rows || queries.dimension_leaderboard;

  renderMetricCards(totalsEl, filteredTotals, {
    selectedMetric: state.selectedMetric,
    onSelect: ({ metric }) => {
      state.selectedMetric = metric;
      render();
    },
  });
  renderFilterPills(filterPillsEl, state.filters, removeFilter);
  renderLeaderboard(leaderboardEl, leaderboardQuery, {
    titleEl: leaderboardTitleEl,
    subtitleEl: leaderboardSubtitleEl,
    interactive: true,
    metricRef: leaderboardMetric,
    selectedValue: selectedLeaderboardValue(queries.dimension_leaderboard),
    onSelect: setFilter,
  });
  renderDataPreview(previewEl, filterPreviewResult(previewQuery));
  renderQueryDebug(debugEl, {
    metric_totals: queries.metric_totals,
    dimension_leaderboard: queries.dimension_leaderboard,
    preview_rows: queries.preview_rows,
  });
  statusEl.textContent = `${state.candidate.model} ready`;
}

resetEl?.addEventListener("click", reset);

async function main() {
  const response = await fetch("data/app-spec.json");
  if (!response.ok) throw new Error(`Failed to load app spec: ${response.status}`);
  const spec = await response.json();
  const selectedModel = shellEl?.dataset.model;
  const candidates = spec.app_candidates || [];
  const candidate = selectedModel ? candidates.find((item) => item.model === selectedModel) : candidates[0];
  if (!candidate) {
    const detail = selectedModel ? ` for ${selectedModel}` : "";
    throw new Error(`App spec has no app candidate${detail}`);
  }
  state.candidate = candidate;
  state.queries = candidate.queries || {};
  state.selectedMetric = state.queries.metric_totals?.metrics?.[0] || state.queries.dimension_leaderboard?.metrics?.[0];
  render();
}

main().catch((error) => {
  statusEl.textContent = error.message;
  statusEl.dataset.error = "true";
});
