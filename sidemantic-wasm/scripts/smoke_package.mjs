// Install a packed release into an isolated consumer without running lifecycle
// scripts. No source checkout or Rust toolchain is available to repair omissions.
// Run after packing: bun run scripts/smoke_package.mjs sidemantic-wasm.tgz
import { copyFileSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

if (process.argv.length !== 3) throw new Error("Expected the package tarball path");
const tarball = resolve(process.argv[2]);
const consumer = mkdtempSync(join(tmpdir(), "sidemantic-package-"));

function run(command, args) {
  const result = spawnSync(command, args, { cwd: consumer, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} failed with status ${result.status}`);
}

try {
  writeFileSync(join(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
  copyFileSync(new URL("./package_consumer.mjs", import.meta.url), join(consumer, "consumer.mjs"));
  run("bun", ["add", "--ignore-scripts", tarball]);
  run("node", ["consumer.mjs"]);
  console.log("SMOKE_PACKAGE_OK");
} finally {
  rmSync(consumer, { recursive: true, force: true });
}
