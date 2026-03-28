# Cloudflare Containers

This example runs the Sidemantic HTTP API behind a Cloudflare Worker that proxies requests into Cloudflare Containers.

## Deployment shape

- HTTPS terminates at the Worker.
- The Worker starts a Sidemantic container and forwards requests to port `4400`.
- The container runs `SIDEMANTIC_MODE=api`.
- The example image bakes demo models and a seeded `demo.duckdb` into `/app/models`.

For a real deployment, the clean shape is:

- bake your semantic models into the container image
- point Sidemantic at an external database with `SIDEMANTIC_CONNECTION`
- keep DuckDB files out of the container unless they are disposable, because Cloudflare container disks reset on stop

## Files

- `wrangler.jsonc`: Cloudflare Worker and container binding config
- `src/index.ts`: Worker entrypoint and container proxy
- `Dockerfile`: Sidemantic container image used by Cloudflare

## Prereqs

- Bun
- Docker
- A Cloudflare account with Containers enabled
- Wrangler auth: `bunx wrangler whoami`

## Deploy the demo

```bash
cd examples/cloudflare_containers
bun install
bunx wrangler secret put SIDEMANTIC_API_TOKEN
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
FROM sidequery/sidemantic:latest

ENV SIDEMANTIC_MODE=api
ENV SIDEMANTIC_API_PORT=4400

COPY models/ /app/models/
WORKDIR /app/models
```

Then remove `SIDEMANTIC_DB` from the default container env in [src/index.ts](/Users/nico/Code/sidemantic/examples/cloudflare_containers/src/index.ts).

Then set your warehouse connection:

```bash
bunx wrangler secret put SIDEMANTIC_CONNECTION
```

The Worker passes `SIDEMANTIC_CONNECTION`, `SIDEMANTIC_API_TOKEN`, and `SIDEMANTIC_CORS_ORIGINS` into the container at startup.

## Auth

The container already supports bearer-token auth via `SIDEMANTIC_API_TOKEN`.

If you want proper edge auth, put the deployed hostname behind Cloudflare Access and use service tokens or your IdP there. Keep the app bearer token as a second gate if you want defense in depth.

## Notes

- Cloudflare Containers is beta.
- Cold starts are materially slower than plain Workers.
- The Worker is required. The container is not exposed directly on the public Internet.
