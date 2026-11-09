# Docker

The published image is [`sidequery/sidemantic`](https://hub.docker.com/repository/docker/sidequery/sidemantic) on Docker Hub, built for `linux/amd64` and `linux/arm64`. It includes all database drivers, the PostgreSQL wire-protocol server, the HTTP API server, and the MCP server. It uses the default Python engine; the optional Rust engine is not installed.

## Mounting your models

The container looks for model files (YAML, SQL, etc.) in `/app/models`. Use a volume mount (`-v`) to point it at your local models directory:

```bash
# If your models are in ~/my-project/models/
docker run -p 5433:5433 -v ~/my-project/models:/app/models sidequery/sidemantic

# Or from the current directory
docker run -p 5433:5433 -v $(pwd)/models:/app/models sidequery/sidemantic
```

The `-v local/path:/app/models` flag maps a folder on your machine into the container. Any `.yml`, `.sql`, or other semantic model files in that folder will be auto-detected and loaded.

## PostgreSQL server (default)

Mount your models directory and expose port 5433:

```bash
docker run -p 5433:5433 -v ./models:/app/models sidequery/sidemantic
```

Connect with any PostgreSQL client:

```bash
psql -h localhost -p 5433 -U any -d sidemantic
```

Authentication is off by default and the server listens on all interfaces inside the container. Set `SIDEMANTIC_USERNAME` and `SIDEMANTIC_PASSWORD` to require credentials.

With a backend database connection:

```bash
docker run -p 5433:5433 \
  -v ./models:/app/models \
  -e SIDEMANTIC_CONNECTION="postgres://user:pass@host:5432/db" \
  sidequery/sidemantic
```

## MCP server

The MCP server speaks JSON-RPC over stdio, so keep stdin open with `-i`:

```bash
docker run -i --rm -v ./models:/app/models -e SIDEMANTIC_MODE=mcp sidequery/sidemantic
```

To use it from an MCP client, configure the same command:

```json
{
  "mcpServers": {
    "sidemantic": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "-e", "SIDEMANTIC_MODE=mcp", "sidequery/sidemantic", "--demo"]
    }
  }
}
```

Replace `"--demo"` with `"-v", "/absolute/path/to/models:/app/models"` (before the image name) to serve your own models.

## HTTP API server

```bash
docker run -p 4400:4400 \
  -v ./models:/app/models \
  -e SIDEMANTIC_MODE=api \
  -e SIDEMANTIC_API_TOKEN=secret \
  sidequery/sidemantic
```

The examples below use the demo models; start the server with `--demo` instead of the volume mount to run them as written.

JSON query:

```bash
curl -s http://localhost:4400/query \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{"metrics":["orders.order_count"],"dimensions":["customers.region"]}'
```

Arrow query:

```bash
curl -s http://localhost:4400/query \
  -H "Authorization: Bearer secret" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -H "Content-Type: application/json" \
  -d '{"metrics":["orders.order_count"]}' > result.arrow
```

## Both servers simultaneously

Runs the PG server in the background and MCP on stdio:

```bash
docker run -i -p 5433:5433 -v ./models:/app/models -e SIDEMANTIC_MODE=both sidequery/sidemantic
```

## Demo mode

Pass `--demo` (or set `SIDEMANTIC_DEMO=1`) to serve the built-in sample models and data in any mode:

```bash
docker run -p 5433:5433 sidequery/sidemantic --demo
docker run -p 4400:4400 -e SIDEMANTIC_MODE=api -e SIDEMANTIC_API_TOKEN=secret sidequery/sidemantic --demo
```

## Baking models into the image

`/app/models` ships with the demo models, so clear it before copying yours in; otherwise both sets load:

```dockerfile
FROM sidequery/sidemantic
RUN rm -rf /app/models/*
COPY my_models/ /app/models/
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `SIDEMANTIC_MODE` | `serve` (default, PostgreSQL), `mcp`, `api`, or `both` |
| `SIDEMANTIC_DEMO` | Any non-empty value serves the built-in demo (same as `--demo`) |
| `SIDEMANTIC_CONNECTION` | Database connection string |
| `SIDEMANTIC_DB` | Path to DuckDB file (inside container) |
| `SIDEMANTIC_USERNAME` | PG server auth username |
| `SIDEMANTIC_PASSWORD` | PG server auth password |
| `SIDEMANTIC_PG_PASSWORD_FILE` | Path to a file holding the PG server password (for mounted secrets) |
| `SIDEMANTIC_PORT` | PG server port (default 5433) |
| `SIDEMANTIC_API_PORT` | HTTP API port (default 4400) |
| `SIDEMANTIC_API_TOKEN` | HTTP API bearer token |
| `SIDEMANTIC_API_AUTH_TOKEN_FILE` | Path to a file holding the HTTP API bearer token (for mounted secrets) |
| `SIDEMANTIC_CORS_ORIGINS` | Comma-separated list of allowed CORS origins |
| `SIDEMANTIC_MAX_REQUEST_BODY_BYTES` | Max HTTP request body size in bytes |

The entrypoint hands `SIDEMANTIC_PASSWORD` and `SIDEMANTIC_API_TOKEN` to the server as private files, so they do not appear in the server's command line.

## Building from source

From the repo root:

```bash
docker build -t sidemantic .
```

The first build compiles the `riffq` PostgreSQL wire-protocol package from source with Rust, which takes several minutes. To check a build in every mode:

```bash
python3 scripts/docker_smoke_test.py sidemantic
```

## Integration test services (docker compose)

The `docker-compose.yml` in this directory starts Postgres, a BigQuery emulator, Spark, and ClickHouse for local integration testing. Start the services, then run the integration tests from the repo root on your machine:

```bash
docker compose -f examples/docker/docker-compose.yml up -d postgres
POSTGRES_TEST=1 POSTGRES_HOST=localhost POSTGRES_PORT=5433 POSTGRES_DB=sidemantic_test POSTGRES_USER=test POSTGRES_PASSWORD=test \
  uv run --extra postgres pytest -m integration tests/db/test_postgres_integration.py -v
```

See [`tests/db/README.md`](../../tests/db/README.md) for the other databases.
