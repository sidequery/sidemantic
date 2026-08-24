import { test } from "bun:test";
import { main } from "./runner";

test("official Malloy and Sidemantic produce matching fixture results", async () => {
  await main();
});
