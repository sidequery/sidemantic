FROM apache/superset:latest

USER root
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN uv pip install --python /app/.venv/bin/python duckdb duckdb-engine
USER superset
