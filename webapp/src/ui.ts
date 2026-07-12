// Canonical public UI surface. Every Sidemantic runtime consumes a build of
// this module; component implementations must not be copied elsewhere.
export { ChartTooltip, useChartTooltip } from "./components/ChartTooltip";
export { ColumnChart } from "./components/ColumnChart";
export { DataTable } from "./components/DataTable";
export { DashboardShell } from "./components/DashboardShell";
export { DataPreviewTable, LineChart } from "./components/DistributionAdapters";
export { ErrorBoundary } from "./components/ErrorBoundary";
export { FilterPill } from "./components/FilterPill";
export { Leaderboard } from "./components/Leaderboard";
export { MetricCard } from "./components/MetricCard";
export { QueryDebugPanel, tokenizeSql } from "./components/QueryDebugPanel";
export { Sparkline } from "./components/Sparkline";
export { EmptyState, ErrorState, LoadingState, StatusDot } from "./components/States";
export { TimeSeriesChart } from "./components/TimeSeriesChart";
export { formatCompact, formatValue, labelize } from "./lib/format";
export { applyTheme, applyThemeTokens, getTheme, toggleTheme } from "./lib/theme";
export type { Theme, ThemeTokens } from "./lib/theme";
export {
  aliasForSemanticRef,
  normalizeFilterValue,
  paginateRows,
  removeFilterDimension,
  removeFilterValue,
  toggleFilterValue,
} from "./lib/uiCore.js";

export type { Column } from "./components/DataTable";
export type { LeaderboardRow } from "./components/Leaderboard";
export type { QueryDebugInput, SqlToken } from "./components/QueryDebugPanel";
export type { SparklineBrushRange, SparklineProps } from "./components/Sparkline";
export type { BrushRange, SeriesPoint } from "./components/TimeSeriesChart";
export type { FormatHint, Tone } from "./lib/format";
