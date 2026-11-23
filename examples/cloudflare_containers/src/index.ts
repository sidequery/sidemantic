import { Container, getRandom } from "@cloudflare/containers";

const DEFAULT_CONTAINER_ENV = {
  SQLCTX_MODE: "api",
  SQLCTX_API_PORT: "4400",
  SQLCTX_DB: "/app/models/demo.duckdb",
} as const;
const CONTAINER_ENTRYPOINT = ["/docker-entrypoint.sh"];

type Env = {
  SQLCTX_API: DurableObjectNamespace<SqlctxApiContainer>;
  SQLCTX_API_TOKEN?: string;
  SQLCTX_CONNECTION?: string;
  SQLCTX_CORS_ORIGINS?: string;
};

function buildContainerEnv(env: Env): Record<string, string> {
  const containerEnv: Record<string, string> = { ...DEFAULT_CONTAINER_ENV };

  if (env.SQLCTX_API_TOKEN) {
    containerEnv.SQLCTX_API_TOKEN = env.SQLCTX_API_TOKEN;
  }
  if (env.SQLCTX_CONNECTION) {
    delete containerEnv.SQLCTX_DB;
    containerEnv.SQLCTX_CONNECTION = env.SQLCTX_CONNECTION;
  }
  if (env.SQLCTX_CORS_ORIGINS) {
    containerEnv.SQLCTX_CORS_ORIGINS = env.SQLCTX_CORS_ORIGINS;
  }

  return containerEnv;
}

export class SqlctxApiContainer extends Container {
  defaultPort = 4400;
  requiredPorts = [4400];
  sleepAfter = "15m";
  enableInternet = true;
  pingEndpoint = "/readyz";
  envVars = DEFAULT_CONTAINER_ENV;
  entrypoint = CONTAINER_ENTRYPOINT;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const container = await getRandom(env.SQLCTX_API, 3);

    await container.startAndWaitForPorts({
      startOptions: {
        entrypoint: CONTAINER_ENTRYPOINT,
        envVars: buildContainerEnv(env),
      },
    });

    return container.fetch(request);
  },
};
