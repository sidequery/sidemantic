FROM sidequery/sidemantic:latest

ENV SIDEMANTIC_MODE=api
ENV SIDEMANTIC_API_PORT=4400

# The base image ships the demo models and a seeded demo.duckdb in /app/models,
# and src/index.ts points SIDEMANTIC_DB at that file, so the example deploys
# without local models. For a real deployment, see "Switching from demo to real
# models" in README.md.
WORKDIR /app/models
