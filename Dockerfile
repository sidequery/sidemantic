FROM python:3.12-slim AS builder

# Install build deps for riffq (Rust/maturin) and other native extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# riffq publishes no Linux wheels for CPython 3.12 or arm64, so it builds from
# source and needs a Rust toolchain.
COPY --from=rust:1-slim /usr/local/rustup /usr/local/rustup
COPY --from=rust:1-slim /usr/local/cargo /usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install from the published package metadata: --no-sources ignores
# [tool.uv.sources], whose local Rust packages are not in the build context.
# Dependencies install before the source is copied so code changes reuse the
# slow riffq build layer.
COPY pyproject.toml ./
RUN uv pip install --system --no-cache --no-sources -r pyproject.toml \
    --extra serve --extra mcp --extra api --extra all-databases

COPY README.md LICENSE ./
COPY sidemantic/ sidemantic/
COPY examples/multi_format_demo/ examples/multi_format_demo/
COPY scripts/build_demo_duckdb.py scripts/

RUN uv pip install --system --no-cache --no-sources --no-deps .
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
