import { expect, test, vi } from "vitest";
import { emitOpenProgress } from "./open-progress.js";

test("progress observations forward Rust events without changing opening", () => {
  const event = { phase: "migrating" as const, scope: "local" as const, fromFormat: 75, toFormat: 81 };
  const callback = vi.fn();
  emitOpenProgress(callback, event);
  expect(callback).toHaveBeenCalledExactlyOnceWith(event);
  expect(() => emitOpenProgress(() => { throw new Error("UI failed"); }, event)).not.toThrow();
  expect(() => emitOpenProgress(undefined, event)).not.toThrow();
});
