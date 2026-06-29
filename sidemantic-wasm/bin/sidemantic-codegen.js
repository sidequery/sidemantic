#!/usr/bin/env node
// CLI counterpart to `sidemantic gen types` / `sidemantic gen sql`, running the wasm in Node.
//
//   sidemantic-codegen types <models.yml> [--no-yaml] [--out <file>]
//   sidemantic-codegen sql   <models.yml> <source.ts...> [--call <name>] [--out <file>]

import { readFileSync, writeFileSync } from "node:fs";

import { extractSqlLiterals, generateClientSchema, generateSqlTypes } from "../codegen.js";

const USAGE = [
  "Usage:",
  "  sidemantic-codegen types <models.yml> [--no-yaml] [--out <file>]",
  "  sidemantic-codegen sql   <models.yml> <source.ts...> [--call <name>] [--out <file>]",
].join("\n");

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

function emit(rendered, out, label) {
  if (out) {
    writeFileSync(out, rendered);
    process.stderr.write(`${label} written to ${out}\n`);
  } else {
    process.stdout.write(rendered);
  }
}

const [, , command, ...rest] = process.argv;
const wasmUrl = readFileSync(new URL("../wasm/sidemantic_bg.wasm", import.meta.url));

if (command === "types") {
  let models = null;
  let out = null;
  let includeYaml = true;
  for (let i = 0; i < rest.length; i += 1) {
    const arg = rest[i];
    if (arg === "--no-yaml") includeYaml = false;
    else if (arg === "--out" || arg === "-o") out = rest[(i += 1)];
    else if (!arg.startsWith("-") && models === null) models = arg;
    else fail(`Unexpected argument: ${arg}\n${USAGE}`);
  }
  if (!models) fail(`Missing <models.yml>\n${USAGE}`);
  emit(await generateClientSchema(readFileSync(models, "utf8"), { includeYaml, wasmUrl }), out, "Client schema");
} else if (command === "sql") {
  let models = null;
  let out = null;
  let call = "query";
  const sources = [];
  for (let i = 0; i < rest.length; i += 1) {
    const arg = rest[i];
    if (arg === "--out" || arg === "-o") out = rest[(i += 1)];
    else if (arg === "--call") call = rest[(i += 1)];
    else if (arg.startsWith("-")) fail(`Unexpected argument: ${arg}\n${USAGE}`);
    else if (models === null) models = arg;
    else sources.push(arg);
  }
  if (!models) fail(`Missing <models.yml>\n${USAGE}`);
  if (!sources.length) fail(`Missing <source.ts...>\n${USAGE}`);
  const literals = extractSqlLiterals(
    sources.map((path) => readFileSync(path, "utf8")),
    { call },
  );
  if (!literals.length) fail(`No \`${call}(...)\` semantic SQL literals found`);
  emit(await generateSqlTypes(readFileSync(models, "utf8"), literals, { wasmUrl }), out, `Typed query bindings (${literals.length})`);
} else {
  fail(`Unknown command: ${command ?? "(none)"}\n${USAGE}`);
}
