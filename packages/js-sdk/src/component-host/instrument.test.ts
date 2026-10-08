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

test.each([
  [
    "an if with an omitted false child",
    `(module
      (func (export "run")
        (if (i32.const 1)
          (then (drop (loop $forever (result i32) (br $forever)))))))`,
  ],
  [
    "a call_indirect target",
    `(module
      (type $sig (func (result i32)))
      (table 1 funcref)
      (func $callee (type $sig) (i32.const 7))
      (elem (i32.const 0) $callee)
      (func (export "run") (result i32)
        (call_indirect (type $sig)
          (loop $forever (result i32) (br $forever)))))`,
  ],
  [
    "a call_indirect operand",
    `(module
      (type $sig (func (param i32) (result i32)))
      (table 1 funcref)
      (func $callee (type $sig) (param i32) (result i32) (local.get 0))
      (elem (i32.const 0) $callee)
      (func (export "run") (result i32)
        (call_indirect (type $sig)
          (loop $forever (result i32) (br $forever))
          (i32.const 0))))`,
  ],
  [
    "a memory.copy source",
    `(module
      (memory 1)
      (func (export "run") (result i32)
        (memory.copy (i32.const 0)
          (loop $forever (result i32) (br $forever))
          (i32.const 0))
        (i32.const 0)))`,
  ],
])("instruments nested loops under %s", (_case, wat) => {
  const module = compile(wat);
  let ticks = 0;
  const instance = new WebAssembly.Instance(module, {
    [TICK_MODULE]: {
      tick() {
        if (++ticks === 2) throw new Error("deadline");
      },
    },
  });

  expect(() => (instance.exports.run as Function)()).toThrow("deadline");
  expect(ticks).toBe(2);
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

test("rejects SIMD instructions outside the supported core ISA", () => {
  const module = binaryen.parseText("(module)");
  try {
    module.setFeatures(binaryen.Features.All);
    const vector = module.i32x4.splat(module.local.get(0, binaryen.i32));
    const lane = module.i32x4.extract_lane(vector, 0);
    module.addFunction("unsupported_simd", binaryen.i32, binaryen.i32, [], lane);

    expect(() => instrumentCore(module.emitBinary(), 65536)).toThrow(
      "Unsupported component instruction",
    );
  } finally {
    module.dispose();
  }
});
