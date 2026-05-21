import { renderLeaderboard, renderMetricCards, renderQueryDebug } from "./sidemantic-components.js";

const statusEl = document.querySelector('[data-testid="app-status"]');
const totalsEl = document.querySelector('[data-testid="metric-totals"]');
const leaderboardEl = document.querySelector('[data-testid="leaderboard-rows"]');
const leaderboardTitleEl = document.querySelector('[data-testid="leaderboard-title"]');
const leaderboardSubtitleEl = document.querySelector('[data-testid="leaderboard-subtitle"]');
const debugEl = document.querySelector('[data-testid="query-debug"]');
const shellEl = document.querySelector('[data-testid="dashboard-shell"]');

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
  const queries = candidate.queries || {};

  renderMetricCards(totalsEl, queries.metric_totals);
  renderLeaderboard(leaderboardEl, queries.dimension_leaderboard, {
    titleEl: leaderboardTitleEl,
    subtitleEl: leaderboardSubtitleEl,
  });
  renderQueryDebug(debugEl, {
    metric_totals: queries.metric_totals,
    dimension_leaderboard: queries.dimension_leaderboard,
  });
  statusEl.textContent = `${candidate.model} ready`;
}

main().catch((error) => {
  statusEl.textContent = error.message;
  statusEl.dataset.error = "true";
});
