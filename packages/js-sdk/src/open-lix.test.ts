import { expect, test } from "vitest";
import { openLix } from "./open-lix.js";

test("openLix executes SQL against default in-memory sqlite backend", async () => {
  const lix = await openLix();

  const result = await lix.execute("SELECT 1 + 1", []);

  expect(result.rows.length).toBe(1);
  expect(result.rows[0][0]).toEqual({ Integer: 2 });
});
