export type ParamType = "string" | "number" | "date" | "yesno" | "unquoted";

export function interpolateParams(
  sql: string,
  params: Record<string, unknown>,
  paramTypes?: Record<string, ParamType>,
): string;
