# Cloudflare Containers

This example runs the sqlctx HTTP API behind a Cloudflare Worker that proxies requests into Cloudflare Containers.

## Deployment shape

- HTTPS terminates at the Worker.
- The Worker starts a sqlctx container and forwards requests to port `4400`.
- The container runs `SQLCTX_MODE=api`.
- The example image uses the demo models and seeded `demo.duckdb` from `/app/models` in the base image.

For a real deployment, the clean shape is:

- bake your semantic models into the container image
- point sqlctx at an external database with `SQLCTX_CONNECTION`
- keep DuckDB files out of the container unless they are disposable, because Cloudflare container disks reset on stop

## Files

- `wrangler.jsonc`: Cloudflare Worker and container binding config
- `src/index.ts`: Worker entrypoint and container proxy
- `Dockerfile`: sqlctx container image used by Cloudflare, built on the `sidequery/sqlctx` image
- `package.json`: Worker dependencies and the `deploy` script

## Prereqs

- Bun
- Docker
- A Cloudflare account with Containers enabled
- Wrangler auth: `bunx wrangler whoami`

## Deploy the demo

```bash
docker build -t sidequery/sqlctx .
cd examples/cloudflare_containers
bun install
bunx wrangler secret put SQLCTX_API_TOKEN
bun run deploy
```

After deploy, query the worker URL:

```bash
curl -s https://YOUR-WORKER.workers.dev/query \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"metrics":["customers.customer_count"]}'
```

Arrow still works through the Worker:

```bash
curl -s https://YOUR-WORKER.workers.dev/query \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -H "Content-Type: application/json" \
  -d '{"metrics":["customers.customer_count"]}' \
  > result.arrow
```

## Switching from demo to real models

Edit `Dockerfile` to bake in your models:

```dockerfile
FROM sidequery/sqlctx:latest

ENV SQLCTX_MODE=api
ENV SQLCTX_API_PORT=4400

# Replace the demo models that ship in /app/models.
RUN rm -rf /app/models/*
COPY models/ /app/models/
WORKDIR /app/models
```

Then remove `SQLCTX_DB` from the default container env in [src/index.ts](src/index.ts).

Then set your warehouse connection:

```bash
bunx wrangler secret put SQLCTX_CONNECTION
```

The Worker passes `SQLCTX_CONNECTION`, `SQLCTX_API_TOKEN`, and `SQLCTX_CORS_ORIGINS` into the container at startup.

## Auth

The container already supports bearer-token auth via `SQLCTX_API_TOKEN`.

If you want proper edge auth, put the deployed hostname behind Cloudflare Access and use service tokens or your IdP there. Keep the app bearer token as a second gate if you want defense in depth.

## Notes

- Cloudflare Containers is beta.
- Cold starts are materially slower than plain Workers.
- The Worker is required. The container is not exposed directly on the public Internet.
