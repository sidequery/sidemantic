import { Container, getRandom } from "@cloudflare/containers";

const DEFAULT_CONTAINER_ENV = {
  SIDEMANTIC_MODE: "api",
  SIDEMANTIC_API_PORT: "4400",
  SIDEMANTIC_DB: "/app/models/demo.duckdb",
} as const;
const CONTAINER_ENTRYPOINT = ["/docker-entrypoint.sh"];

type Env = {
  SIDEMANTIC_API: DurableObjectNamespace<SidemanticApiContainer>;
  SIDEMANTIC_API_TOKEN?: string;
  SIDEMANTIC_CONNECTION?: string;
  SIDEMANTIC_CORS_ORIGINS?: string;
};

function buildContainerEnv(env: Env): Record<string, string> {
  const containerEnv: Record<string, string> = { ...DEFAULT_CONTAINER_ENV };

  if (env.SIDEMANTIC_API_TOKEN) {
    containerEnv.SIDEMANTIC_API_TOKEN = env.SIDEMANTIC_API_TOKEN;
  }
  if (env.SIDEMANTIC_CONNECTION) {
    delete containerEnv.SIDEMANTIC_DB;
    containerEnv.SIDEMANTIC_CONNECTION = env.SIDEMANTIC_CONNECTION;
  }
  if (env.SIDEMANTIC_CORS_ORIGINS) {
    containerEnv.SIDEMANTIC_CORS_ORIGINS = env.SIDEMANTIC_CORS_ORIGINS;
  }

  return containerEnv;
}

export class SidemanticApiContainer extends Container {
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
    const container = await getRandom(env.SIDEMANTIC_API, 3);

    await container.startAndWaitForPorts({
      startOptions: {
        entrypoint: CONTAINER_ENTRYPOINT,
        envVars: buildContainerEnv(env),
      },
    });

    return container.fetch(request);
  },
};
