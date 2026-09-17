import { beforeEach, expect, test, vi } from "vitest";
import { openLix } from "../open-lix.js";
import type { LixOpenProgress } from "../types.js";
const mocks = vi.hoisted(() => ({ open: vi.fn() }));
vi.mock("../wasm-init.js", () => ({ initializeWasm: async () => {} }));
vi.mock("../wasm/lix_js_sdk.js", () => ({ openRemote: mocks.open }));
const url = "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc";
beforeEach(() => { mocks.open.mockReset(); });
test("remote opening forwards Rust progress and immutable scoped report", async () => {
  const callback = vi.fn();
  const migration = { scope: "authority", fromFormat: 80, toFormat: 81 };
  mocks.open.mockImplementation(async (_url, _transport, _headers, _branch, progress) => {
    progress({ phase: "migrating", ...migration });
    progress({ phase: "complete", ...migration });
    return { close: async () => {}, openReport: () => ({ format: 81, initialized: false, migrations: [migration] }) };
  });
  const lix = await openLix({ server: { url }, onProgress: callback });
  expect(callback.mock.calls.map(([event]) => event.phase)).toEqual(["migrating", "complete"]);
  expect(lix.openReport?.migrations).toEqual([migration]);
  await lix.close();
});
test("remote observer failure does not replace the Rust opening result", async () => {
  const callback = vi.fn((_progress: LixOpenProgress) => { throw new Error("UI callback failed"); });
  mocks.open.mockImplementation(async (_url, _transport, _headers, _branch, progress) => {
    progress({ phase: "migrating", scope: "authority", toFormat: 81 });
    throw new Error("migration failed");
  });
  await expect(openLix({ server: { url }, onProgress: callback })).rejects.toThrow("migration failed");
  expect(callback).toHaveBeenCalledOnce();
});
