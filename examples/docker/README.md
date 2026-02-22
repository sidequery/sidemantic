# Docker

The published image is [`sidequery/sidemantic`](https://hub.docker.com/repository/docker/sidequery/sidemantic) on Docker Hub. It includes all database drivers, the PostgreSQL wire-protocol server, and the MCP server.

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

With a backend database connection:

```bash
docker run -p 5433:5433 \
  -v ./models:/app/models \
  -e SIDEMANTIC_CONNECTION="postgres://user:pass@host:5432/db" \
  sidequery/sidemantic
```

## MCP server

```bash
docker run -v ./models:/app/models -e SIDEMANTIC_MODE=mcp sidequery/sidemantic
```

## Both servers simultaneously

Runs the PG server in the background and MCP on stdio:

```bash
docker run -p 5433:5433 -v ./models:/app/models -e SIDEMANTIC_MODE=both sidequery/sidemantic
```

## Demo mode

```bash
docker run -p 5433:5433 sidequery/sidemantic --demo
```

## Baking models into the image

```dockerfile
FROM sidequery/sidemantic
COPY my_models/ /app/models/
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `SIDEMANTIC_MODE` | `serve` (default), `mcp`, or `both` |
| `SIDEMANTIC_CONNECTION` | Database connection string |
| `SIDEMANTIC_DB` | Path to DuckDB file (inside container) |
| `SIDEMANTIC_USERNAME` | PG server auth username |
| `SIDEMANTIC_PASSWORD` | PG server auth password |
| `SIDEMANTIC_PORT` | PG server port (default 5433) |

## Building from source

From the repo root:

```bash
docker build -t sidemantic .
```

## Integration test services (docker-compose)

The `docker-compose.yml` in this directory spins up Postgres, BigQuery emulator, Spark, and ClickHouse for local integration testing:

```bash
docker compose -f examples/docker/docker-compose.yml up
```
