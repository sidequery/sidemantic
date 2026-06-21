// Runtime for the Sidemantic typed client. All type machinery lives in client.d.ts;
// this file is plain JS and is deliberately tiny — the schema is consumed at the type
// level, and (optionally) used for a friendly runtime ref check before delegating to
// the injected executor.

function collectRefs(schema) {
  const metrics = new Set(schema.topMetrics || []);
  const dimensions = new Set();
  for (const [modelName, model] of Object.entries(schema.models || {})) {
    for (const name of Object.keys(model.metrics || {})) metrics.add(`${modelName}.${name}`);
    for (const [name, def] of Object.entries(model.dimensions || {})) {
      dimensions.add(`${modelName}.${name}`);
      for (const grain of def.grains || []) dimensions.add(`${modelName}.${name}__${grain}`);
    }
  }
  return { metrics, dimensions };
}

function leafName(ref) {
  const dot = ref.indexOf(".");
  return dot === -1 ? ref : ref.slice(dot + 1);
}

function assertNoOutputLeafCollisions(metrics, dimensions) {
  const refsByLeaf = new Map();
  for (const ref of [...dimensions, ...metrics]) {
    const leaf = leafName(ref);
    const refs = refsByLeaf.get(leaf) || [];
    refs.push(ref);
    refsByLeaf.set(leaf, refs);
  }

  const collisions = [...refsByLeaf.entries()].filter(([, refs]) => refs.length > 1);
  if (!collisions.length) return;

  const details = collisions.map(([leaf, refs]) => `${leaf} (${refs.join(", ")})`).join("; ");
  throw new Error(
    `Structured typed queries cannot select refs with the same output name. ` +
      `Use semantic SQL with explicit aliases for these selections: ${details}`,
  );
}

/**
 * Create a typed query client over a generated schema.
 *
 * @param {import("./client").SchemaShape} schema  The generated `schema` export.
 * @param {{ run: Function, validate?: boolean }} options  Executor + options.
 */
export function createClient(schema, options) {
  if (!options || typeof options.run !== "function") {
    throw new Error("createClient requires an executor: createClient(schema, { run })");
  }
  const run = options.run;
  const refs = options.validate === false ? null : collectRefs(schema);

  return {
    schema,
    async query(query) {
      const metrics = query.metrics || [];
      const dimensions = query.dimensions || [];
      if (refs) {
        for (const metric of metrics) {
          if (!refs.metrics.has(metric)) throw new Error(`Unknown metric: ${metric}`);
        }
        for (const dimension of dimensions) {
          if (!refs.dimensions.has(dimension)) throw new Error(`Unknown dimension: ${dimension}`);
        }
      }
      assertNoOutputLeafCollisions(metrics, dimensions);
      const payload = { metrics, skip_default_time_dimensions: true };
      if (dimensions.length) payload.dimensions = dimensions;
      if (query.filters && query.filters.length) payload.filters = query.filters;
      if (query.order_by && query.order_by.length) payload.order_by = query.order_by;
      if (query.limit != null) payload.limit = query.limit;
      if (query.ungrouped) payload.ungrouped = true;
      return run(payload);
    },
  };
}

/**
 * Create a typed semantic-SQL client. Types come from a generated `QueryMap`
 * passed as the type argument; at runtime this just forwards to the executor.
 *
 * @param {{ run: Function }} options  SQL executor: (sql, params?) => Promise<rows>.
 */
export function createSqlClient(options) {
  if (!options || typeof options.run !== "function") {
    throw new Error("createSqlClient requires an executor: createSqlClient({ run })");
  }
  const run = options.run;
  return {
    query(sql, params) {
      return run(sql, params);
    },
  };
}
