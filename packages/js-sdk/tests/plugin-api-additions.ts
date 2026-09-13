import { expect, test } from "vitest";
import { compileComponent } from "../src/component-host/index.js";

export function registerPluginApiAdditionTest(load: () => Promise<Uint8Array>) {
  test("frozen v2 component runs when its host adds an operation", async () => {
    const factory = await compileComponent(await load(), {
      maxMemoryBytes: "1048576",
      timeoutMs: "5000",
    });
    const instance = await factory.instantiate({
      "lix:plugin-v2/host": {
        existingOperation: () => 7,
        newOperation: () => 9,
      },
    });
    expect(instance.exports.run()).toBe(7);
  });
}
