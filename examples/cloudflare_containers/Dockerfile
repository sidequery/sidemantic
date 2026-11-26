FROM sidequery/sqlctx:latest

ENV SQLCTX_MODE=api
ENV SQLCTX_API_PORT=4400

# The base image ships the demo models and a seeded demo.duckdb in /app/models,
# and src/index.ts points SQLCTX_DB at that file, so the example deploys
# without local models. For a real deployment, see "Switching from demo to real
# models" in README.md.
WORKDIR /app/models
