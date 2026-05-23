const root = import.meta.dir;
const args = Bun.argv.slice(2);
const portFlagIndex = args.indexOf("--port");
const port = portFlagIndex >= 0 ? Number(args[portFlagIndex + 1]) : 5174;

const contentTypes: Record<string, string> = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
};

function contentType(pathname: string): string {
  const match = pathname.match(/\.[^.]+$/);
  return (match && contentTypes[match[0]]) || "application/octet-stream";
}

function safePath(url: URL): string {
  const requestedPath = decodeURIComponent(url.pathname === "/" ? "/index.html" : url.pathname);
  const normalized = requestedPath.replace(/^\/+/, "");
  if (normalized.includes("..")) return `${root}/index.html`;
  return `${root}/${normalized}`;
}

Bun.serve({
  port,
  async fetch(request) {
    const url = new URL(request.url);
    const path = safePath(url);
    const file = Bun.file(path);
    if (!(await file.exists())) {
      return new Response("Not found", { status: 404 });
    }
    return new Response(file, {
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": contentType(path),
      },
    });
  },
});

console.log(`Sidemantic WASM demo serving http://127.0.0.1:${port}`);
