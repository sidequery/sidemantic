// Shared SQL parameter interpolation for the wasm and serve transports.

function unquotedLiteral(value) {
  const text = String(value);
  if (!/^[A-Za-z0-9_.]+$/.test(text)) {
    throw new Error(`Unquoted SQL param must be alphanumeric with underscores/dots only: ${text}`);
  }
  return text;
}

function sqlLiteral(value, type) {
  if (type === "unquoted") return unquotedLiteral(value);
  if (value == null) return "NULL";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  return `'${String(value).replace(/'/g, "''")}'`;
}

/**
 * Substitute `{{ name }}` placeholders with quoted SQL literals. Intended for
 * trusted, developer-authored queries — values are escaped but this is not a
 * substitute for parameterized execution against untrusted input.
 */
export function interpolateParams(sql, params, paramTypes = {}) {
  return sql.replace(/\{\{\s*(\w+)\s*\}\}/g, (_match, name) => {
    if (!params || !(name in params)) throw new Error(`Missing SQL param: ${name}`);
    return sqlLiteral(params[name], paramTypes[name]);
  });
}
