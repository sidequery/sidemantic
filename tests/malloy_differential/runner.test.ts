import { test } from "bun:test";
import { main } from "./runner";

test("official Malloy and sqlctx produce matching fixture results", async () => {
  await main();
}, 60_000);
