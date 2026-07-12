export function labelize(value?: string | null): string;
export function aliasForSemanticRef(ref?: string | null): string;
export type UiFormatOptions = {
  format?: string;
  type?: string;
  compact?: boolean;
  style?: "currency" | "percent" | "decimal";
  currency?: string;
  maximumFractionDigits?: number;
};
export function formatUiValue(value: unknown, options?: UiFormatOptions): string;
export function formatUiCompact(value: unknown, options?: UiFormatOptions): string;
export function normalizeFilterValue(value: unknown): string;
export function toggleFilterValue<T extends Record<string, string[]>>(filters: T, dimension: string, value: unknown): T;
export function removeFilterDimension<T extends Record<string, string[]>>(filters: T, dimension: string): T;
export function removeFilterValue<T extends Record<string, string[]>>(filters: T, dimension: string, value: unknown): T;
export function paginateRows<T>(rows: T[], page: number, pageSize: number): {
  paginate: boolean;
  pageCount: number;
  safePage: number;
  start: number;
  visibleRows: T[];
};
