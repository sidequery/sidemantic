FROM python:3.12-slim AS builder

# Install build deps for riffq (Rust/maturin) and other native extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY sidemantic/ sidemantic/
COPY examples/ examples/
COPY scripts/ scripts/

RUN uv pip install --system --no-cache ".[serve,mcp,api,all-databases]"
RUN mkdir -p /app/models
RUN cp -R /app/examples/multi_format_demo/. /app/models/
RUN python /app/scripts/build_demo_duckdb.py

# --- Runtime stage (no build tools) ---
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/sidemantic /usr/local/bin/sidemantic

WORKDIR /app

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

RUN mkdir -p /app/models
COPY --from=builder /app/models/ /app/models/
WORKDIR /app/models

EXPOSE 5433
EXPOSE 4400

ENTRYPOINT ["/docker-entrypoint.sh"]
# Mode is controlled by SIDEMANTIC_MODE env var (serve, mcp, api, both)
