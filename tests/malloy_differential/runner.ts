import { strict as assert } from "assert";
import { spawn } from "child_process";
import { cp, mkdir, mkdtemp, readFile, rm } from "fs/promises";
import { tmpdir } from "os";
import { basename, dirname, join, resolve } from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime, URLReader } from "@malloydata/malloy";

type Status = "compatible" | "unsupported" | "planned";

type QuerySpec = {
  metrics: string[];
  dimensions: string[];
  filters?: string[];
  segments?: string[];
  order_by: string[];
  limit?: number;
};

type Fixture = {
  id: string;
  family: string;
  status: Status;
  reason: string;
  source?: string;
  seed?: string;
  model?: string;
  malloy_query?: string;
  sidemantic?: QuerySpec;
  expected_schema?: string[];
  expected_rows?: Record<string, unknown>[];
};

type Manifest = {
  version: number;
  fixtures: Fixture[];
};

type Result = {
  schema: string[];
  rows: Record<string, unknown>[];
};

const DIFF_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(DIFF_DIR, "../..");
const FIXTURE_DIR = join(DIFF_DIR, "fixtures");
const MANIFEST_PATH = join(FIXTURE_DIR, "manifest.json");
const PYTHON_RUNNER = join(DIFF_DIR, "sidemantic_runner.py");
const STATUSES = new Set<Status>(["compatible", "unsupported", "planned"]);

class LocalFileReader implements URLReader {
  async readURL(url: URL): Promise<string> {
    return readFile(fileURLToPath(url), "utf8");
  }
}

function assertManifest(manifest: Manifest): void {
  assert.equal(manifest.version, 1, "unsupported differential fixture manifest version");
  assert.ok(Array.isArray(manifest.fixtures) && manifest.fixtures.length > 0, "manifest has no fixtures");

  const ids = new Set<string>();
  for (const fixture of manifest.fixtures) {
    assert.match(fixture.id, /^[a-z0-9][a-z0-9_-]*$/, `invalid fixture id: ${fixture.id}`);
    assert.ok(!ids.has(fixture.id), `duplicate fixture id: ${fixture.id}`);
    ids.add(fixture.id);
    assert.ok(fixture.family, `fixture ${fixture.id} needs a family`);
    assert.ok(STATUSES.has(fixture.status), `fixture ${fixture.id} has invalid status: ${fixture.status}`);
    assert.ok(fixture.reason, `fixture ${fixture.id} needs a status reason`);
    if (fixture.status === "compatible") {
      assert.ok(fixture.source && fixture.seed && fixture.model && fixture.malloy_query, `incomplete ${fixture.id}`);
      assert.ok(fixture.sidemantic && fixture.expected_schema && fixture.expected_rows, `incomplete ${fixture.id}`);
    }
  }
}

function normalizeValue(value: unknown): unknown {
  if (typeof value === "bigint") return Number(value);
  if (value === undefined) return null;
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "number" && !Number.isFinite(value)) return null;
  if (Array.isArray(value)) return value.map(normalizeValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, nested]) => [key, normalizeValue(nested)]),
    );
  }
  return value;
}

function normalizeRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  return rows.map((row) => normalizeValue(row) as Record<string, unknown>);
}

function malloyRows(result: any): Record<string, unknown>[] {
  const rows = result?.data?.value ?? result?.data?.rows ?? result?.data;
  if (!Array.isArray(rows)) throw new Error("Malloy result did not contain a row array");
  return normalizeRows(rows as Record<string, unknown>[]);
}

async function runProcess(command: string[], cwd: string): Promise<string> {
  const child = spawn(command[0], command.slice(1), {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
  const exitCode = await new Promise<number>((resolveExit, reject) => {
    child.on("error", reject);
    child.on("close", (code) => resolveExit(code ?? 1));
  });
  if (exitCode !== 0) {
    throw new Error(`${command.join(" ")} exited ${exitCode}\n${Buffer.concat(stderr).toString("utf8")}`);
  }
  return Buffer.concat(stdout).toString("utf8").trim();
}

async function runSidemantic(workspace: string, fixture: Fixture): Promise<Result> {
  const query = JSON.stringify(fixture.sidemantic);
  const output = await runProcess(
    [
      "uv",
      "run",
      "--offline",
      "--no-sync",
      "python",
      PYTHON_RUNNER,
      "--workspace",
      workspace,
      "--source",
      join(workspace, basename(fixture.source!)),
      "--seed",
      join(workspace, basename(fixture.seed!)),
      "--model",
      fixture.model!,
      "--query",
      query,
    ],
    REPO_ROOT,
  );
  return JSON.parse(output) as Result;
}

async function runMalloy(workspace: string, fixture: Fixture): Promise<Result> {
  const sourcePath = join(workspace, basename(fixture.source!));
  const connection = new DuckDBConnection("duckdb", join(workspace, "fixture.duckdb"), workspace);
  try {
    const runtime = new SingleConnectionRuntime({
      connection,
      urlReader: new LocalFileReader(),
    });
    const model = runtime.loadModel(pathToFileURL(sourcePath));
    const result = await model.loadQuery(fixture.malloy_query!).run();
    const rows = malloyRows(result);
    return {
      schema: result.resultExplore.allFields.map((field) => field.name),
      rows,
    };
  } finally {
    await connection.close();
  }
}

async function runFixture(fixture: Fixture): Promise<void> {
  if (fixture.status !== "compatible") {
    console.log(`[${fixture.status}] ${fixture.id}: ${fixture.reason}`);
    return;
  }

  const workspace = await mkdtemp(join(tmpdir(), "sidemantic-malloy-diff-"));
  try {
    await mkdir(join(workspace, "data"));
    await cp(join(FIXTURE_DIR, fixture.source!), join(workspace, basename(fixture.source!)));
    await cp(join(FIXTURE_DIR, fixture.seed!), join(workspace, basename(fixture.seed!)));

    const sidemantic = await runSidemantic(workspace, fixture);
    const malloy = await runMalloy(workspace, fixture);
    const expected: Result = {
      schema: fixture.expected_schema!,
      rows: normalizeRows(fixture.expected_rows!),
    };

    assert.deepEqual(sidemantic, expected, `${fixture.id}: unexpected Sidemantic result`);
    assert.deepEqual(malloy, expected, `${fixture.id}: unexpected Malloy result`);
    assert.deepEqual(malloy, sidemantic, `${fixture.id}: Malloy/Sidemantic mismatch`);
    console.log(`[pass] ${fixture.id}: Malloy and Sidemantic results match`);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
}

export async function main(): Promise<void> {
  const manifest = JSON.parse(await readFile(MANIFEST_PATH, "utf8")) as Manifest;
  assertManifest(manifest);
  assert.ok(
    manifest.fixtures.some((fixture) => fixture.status === "compatible"),
    "manifest needs at least one executable compatible fixture",
  );
  for (const fixture of manifest.fixtures) await runFixture(fixture);
}

if (import.meta.main) await main();
