import { beforeEach, expect, test, vi } from "vitest";
import { openLixWorkerBinding } from "./client.js";
import type { OpenProgressDispatch } from "../binding-types.js";
const mocks = vi.hoisted(() => ({ direct: vi.fn() }));
vi.mock("#worker-factory", () => ({ openDirectLixBinding: mocks.direct, createWorkerConnection: vi.fn(), createSharedWorkerConnection: vi.fn() }));
beforeEach(() => { mocks.direct.mockReset(); });
const url = "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc";
test("native direct opening forwards Rust migration events", async () => {
  const progress = vi.fn();
  mocks.direct.mockImplementation(async (_storage, _telemetry, _parent, _server, dispatch: OpenProgressDispatch) => {
    dispatch({ phase: "migrating", scope: "authority", fromFormat: 80, toFormat: 81 });
    dispatch({ phase: "complete", scope: "authority", toFormat: 81 });
    return { close: async () => {} };
  });
  const binding = await openLixWorkerBinding({ kind: "memory" }, undefined, undefined, { url }, progress);
  expect(progress.mock.calls.map(([event]) => event.phase)).toEqual(["migrating", "complete"]);
  await binding.close();
});
test("native healthy opening preserves default transport without a progress observer", async () => {
  mocks.direct.mockResolvedValue({ close: async () => {} });
  const binding = await openLixWorkerBinding({ kind: "memory" }, undefined, undefined, { url });
  expect(mocks.direct.mock.calls[0][3].transport).toBeUndefined();
  await binding.close();
});
