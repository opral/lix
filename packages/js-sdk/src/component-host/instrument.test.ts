import { expect, test } from "vitest";
import binaryen from "binaryen";
import { instrumentCore, TICK_MODULE } from "./instrument.js";

function compile(wat: string, pages = 1): WebAssembly.Module {
  const module = binaryen.parseText(wat);
  try {
    return new WebAssembly.Module(
      instrumentCore(
        module.emitBinary(),
        pages * 65536,
      ) as Uint8Array<ArrayBuffer>,
    );
  } finally {
    module.dispose();
  }
}

test("interrupts an infinite loop inside an expression", () => {
  const module = compile(
    '(module (func (export "run") (result i32) (i32.add (i32.const 1) (loop $forever (result i32) (br $forever)))))',
  );
  let ticks = 0;
  const instance = new WebAssembly.Instance(module, {
    [TICK_MODULE]: {
      tick() {
        if (++ticks === 10) throw new Error("deadline");
      },
    },
  });
  expect(() => (instance.exports.run as Function)()).toThrow("deadline");
  expect(ticks).toBe(10);
});

test("interrupts recursive calls without loops", () => {
  const module = compile(
    '(module (func $recurse (export "run") (call $recurse)))',
  );
  let ticks = 0;
  const instance = new WebAssembly.Instance(module, {
    [TICK_MODULE]: {
      tick() {
        if (++ticks === 10) throw new Error("deadline");
      },
    },
  });
  expect(() => (instance.exports.run as Function)()).toThrow("deadline");
  expect(ticks).toBe(10);
});

test("caps memory.grow even when the guest declares no maximum", () => {
  const module = compile(
    '(module (memory (export "memory") 1) (func (export "grow") (result i32) (memory.grow (i32.const 1))))',
  );
  const instance = new WebAssembly.Instance(module, {
    [TICK_MODULE]: { tick() {} },
  });
  expect((instance.exports.grow as Function)()).toBe(-1);
  expect(
    (instance.exports.memory as WebAssembly.Memory).buffer.byteLength,
  ).toBe(65536);
});

test("preserves stricter guest memory maximum", () => {
  const module = compile(
    '(module (memory (export "memory") 1 1) (func (export "grow") (result i32) (memory.grow (i32.const 1))))',
    2,
  );
  const instance = new WebAssembly.Instance(module, {
    [TICK_MODULE]: { tick() {} },
  });
  expect((instance.exports.grow as Function)()).toBe(-1);
});

test("rejects initial memory above the host limit", () => {
  expect(() => compile("(module (memory 2))")).toThrow(
    "initial memory exceeds limit",
  );
});

test("rejects shared memory rather than allowing blocking waits", () => {
  expect(() => compile("(module (memory 1 1 shared))")).toThrow(
    "Shared and 64-bit",
  );
});

test("exposes unexported memory for accurate host high-water accounting", () => {
  const module = compile("(module (memory 1))");
  const instance = new WebAssembly.Instance(module, {
    [TICK_MODULE]: { tick() {} },
  });
  expect(
    (instance.exports.__lix_runtime_memory as WebAssembly.Memory).buffer
      .byteLength,
  ).toBe(65536);
});

test("rejects a conflicting runtime inspection export", () => {
  expect(() =>
    compile('(module (memory (export "__lix_runtime_memory") 1))'),
  ).toThrow("Reserved runtime memory export");
});

test("caps table.grow to the native host table element limit", () => {
  const module = compile(
    '(module (table (export "table") 1 funcref) (func (export "grow") (result i32) (table.grow (ref.null func) (i32.const 1000000))))',
  );
  const instance = new WebAssembly.Instance(module, {
    [TICK_MODULE]: { tick() {} },
  });
  expect((instance.exports.grow as Function)()).toBe(-1);
  expect((instance.exports.table as WebAssembly.Table).length).toBe(1);
});
