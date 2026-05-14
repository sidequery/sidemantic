import { renderLeaderboard, renderMetricCards, renderQueryDebug } from "./sidemantic-components.js";

const statusEl = document.querySelector('[data-testid="app-status"]');
const totalsEl = document.querySelector('[data-testid="metric-totals"]');
const leaderboardEl = document.querySelector('[data-testid="leaderboard-rows"]');
const leaderboardTitleEl = document.querySelector('[data-testid="leaderboard-title"]');
const leaderboardSubtitleEl = document.querySelector('[data-testid="leaderboard-subtitle"]');
const debugEl = document.querySelector('[data-testid="query-debug"]');

async function main() {
  const response = await fetch("data/app-spec.json");
  if (!response.ok) throw new Error(`Failed to load app spec: ${response.status}`);
  const spec = await response.json();
  const candidate = spec.app_candidates?.[0];
  if (!candidate) throw new Error("App spec has no app candidates");
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
