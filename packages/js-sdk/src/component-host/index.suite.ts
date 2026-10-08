import { expect, test } from "vitest";
import { parse } from "@bytecodealliance/jco-transpile/wasm-tools";
import type { CoreInstrumenter } from "./index.js";
import { compileComponent } from "./index.js";

// Each memoryless/tableless instance calls its predecessor, so all instances
// remain observable after Component transpilation and must be instantiated.
async function chain(count: number, instrumenter?: CoreInstrumenter) {
  const instances = Array.from({ length: count - 1 }, (_, i) =>
    `(core instance $i${i + 1} (instantiate $link (with "previous" (instance $i${i}))))`,
  ).join("\n");
  return compileComponent(
    await parse(`(component
      (core module $base (func (export "run")))
      (core instance $i0 (instantiate $base))
      (core module $link
        (import "previous" "run" (func $previous))
        (func (export "run") (call $previous)))
      ${instances}
      (alias core export $i${count - 1} "run" (core func $run))
      (func (export "run") (canon lift (core func $run))))`),
    { maxMemoryBytes: "1048576", timeoutMs: "5000" },
    instrumenter,
  );
}

/** Register the component instance-cap fixtures with an optional core transform. */
export function registerComponentInstanceCapTests(
  instrumenter?: CoreInstrumenter,
): void {
  const path = instrumenter ? "through Rust instrumentation" : "by default";

  test(`allows 64 memoryless core instances per actor ${path}`, async () => {
    const factory = await chain(64, instrumenter);
    const Original = WebAssembly.Instance;
    let allocations = 0;
    WebAssembly.Instance = new Proxy(Original, {
      construct(target, args) {
        allocations++;
        return Reflect.construct(target, args);
      },
    });
    try {
      const first = await factory.instantiate({});
      first.exports.run();
      expect(allocations).toBe(64);
      allocations = 0;
      const second = await factory.instantiate({});
      second.exports.run();
      expect(allocations).toBe(64);
    } finally {
      WebAssembly.Instance = Original;
    }
  });

  test(`rejects the 65th memoryless core instance before allocation ${path}`, async () => {
    const factory = await chain(65, instrumenter);
    const Original = WebAssembly.Instance;
    let allocations = 0;
    WebAssembly.Instance = new Proxy(Original, {
      construct(target, args) {
        allocations++;
        return Reflect.construct(target, args);
      },
    });
    try {
      await expect(factory.instantiate({})).rejects.toThrow(
        "Component core instance count exceeds limit",
      );
      expect(allocations).toBe(64);
    } finally {
      WebAssembly.Instance = Original;
    }
  });
}
