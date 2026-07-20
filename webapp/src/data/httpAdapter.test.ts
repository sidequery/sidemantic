import { afterEach, describe, expect, mock, test } from "bun:test";

import { HttpBackend } from "./httpAdapter";

const originalFetch = globalThis.fetch;
const originalLocation = Object.getOwnPropertyDescriptor(globalThis, "location");

afterEach(() => {
  globalThis.fetch = originalFetch;
  if (originalLocation) Object.defineProperty(globalThis, "location", originalLocation);
  else delete (globalThis as { location?: Location }).location;
});

describe("HttpBackend browser sessions", () => {
  test("uses an in-memory short-lived session for a cross-origin backend", async () => {
    Object.defineProperty(globalThis, "location", {
      configurable: true,
      value: new URL("https://ui.example.test/dashboard"),
    });
    const requests: Array<[RequestInfo | URL, RequestInit | undefined]> = [];
    globalThis.fetch = mock(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push([input, init]);
      if (String(input).endsWith("/auth/session")) {
        return Response.json({ expires_in: 600, session_token: "short-lived" });
      }
      return Response.json({ status: "ok" });
    }) as typeof fetch;

    const backend = new HttpBackend({ baseUrl: "https://api.example.test" });
    await backend.createBrowserSession("long-lived");
    await backend.health();

    expect(requests[0][1]?.credentials).toBe("include");
    expect(requests[0][1]?.headers).toEqual({
      Authorization: "Bearer long-lived",
      "X-Sidemantic-Session-Mode": "header",
    });
    expect(requests[1][1]?.credentials).toBe("include");
    expect(requests[1][1]?.headers).toEqual({ Authorization: "Sidemantic-Session short-lived" });
  });
});
