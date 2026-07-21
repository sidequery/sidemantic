// Canonical public UI surface. Every Sidemantic runtime consumes a build of
// this module; component implementations must not be copied elsewhere.
export { BarLineCombo } from "./components/BarLineCombo";
export { ChartTooltip, TooltipRows, useChartTooltip } from "./components/ChartTooltip";
export { ColumnChart } from "./components/ColumnChart";
export { DataTable, columnTotal } from "./components/DataTable";
export { DashboardShell } from "./components/DashboardShell";
export { DataPreviewTable, LineChart } from "./components/DistributionAdapters";
export { DonutChart, donutSegments } from "./components/DonutChart";
export { ErrorBoundary } from "./components/ErrorBoundary";
export { FilterPill } from "./components/FilterPill";
export { HeatmapChart } from "./components/HeatmapChart";
export { HistogramChart, binValues } from "./components/HistogramChart";
export { Leaderboard } from "./components/Leaderboard";
export { MetricCard } from "./components/MetricCard";
export { NetworkChart, layoutNetwork } from "./components/NetworkChart";
export { QueryDebugPanel, tokenizeSql } from "./components/QueryDebugPanel";
export { ScatterChart } from "./components/ScatterChart";
export { Sparkline } from "./components/Sparkline";
export { StackedAreaChart } from "./components/StackedAreaChart";
export { EmptyState, ErrorState, LoadingState, StatusDot } from "./components/States";
export { TimeSeriesChart } from "./components/TimeSeriesChart";
export { WaterfallChart, waterfallSteps } from "./components/WaterfallChart";

export { Button } from "./components/Button";
export { Combobox, filterOptions } from "./components/Combobox";
export { DatePicker, monthGrid } from "./components/DatePicker";
export { DateRangeControl } from "./components/DateRangeControl";
export { GrainSelect } from "./components/GrainSelect";
export { Select } from "./components/Select";
export { Switch } from "./components/Switch";
export { Tabs } from "./components/Tabs";
export { TimezoneSelect } from "./components/TimezoneSelect";
export { Tooltip } from "./components/Tooltip";
export { ViewSwitcher } from "./components/ViewSwitcher";
export { formatCompact, formatValue, labelize } from "./lib/format";
export { applyTheme, applyThemeTokens, getTheme, toggleTheme } from "./lib/theme";
export type { Theme, ThemeTokens } from "./lib/theme";
export { VIZ_COLOR_COUNT, axisTicks, vizColor } from "./lib/viz";
export {
  parseTemporal,
  rowsToBarLine,
  rowsToCategories,
  rowsToCells,
  rowsToPoints,
  rowsToSeries,
  rowsToTimeSeries,
} from "./lib/rows";
export type { Row } from "./lib/rows";
export {
  aliasForSemanticRef,
  normalizeFilterValue,
  paginateRows,
  removeFilterDimension,
  removeFilterValue,
  toggleFilterValue,
} from "./lib/uiCore.js";

export type { Column, TotalKind } from "./components/DataTable";
export type { ChartTooltipState, TooltipRow } from "./components/ChartTooltip";
export type { LeaderboardRow } from "./components/Leaderboard";
export type { QueryDebugInput, SqlToken } from "./components/QueryDebugPanel";
export type { SparklineBrushRange, SparklineProps } from "./components/Sparkline";
export type { BrushRange, SeriesPoint } from "./components/TimeSeriesChart";
export type { FormatHint, Tone } from "./lib/format";
export type { BarLineDatum } from "./components/BarLineCombo";
export type { DonutDatum, DonutSegment } from "./components/DonutChart";
export type { HeatmapCell } from "./components/HeatmapChart";
export type { HistogramBin } from "./components/HistogramChart";
export type { NetworkLink, NetworkNode, PositionedNode } from "./components/NetworkChart";
export type { ScatterPoint } from "./components/ScatterChart";
export type { StackedSeries } from "./components/StackedAreaChart";
export type { WaterfallDatum, WaterfallStep } from "./components/WaterfallChart";
export type { ButtonVariant } from "./components/Button";
export type { ComboboxOption } from "./components/Combobox";
export type { DatePickerRange, DayCell } from "./components/DatePicker";
export type { SelectOption } from "./components/Select";
export type { TabItem } from "./components/Tabs";
