import binaryen from "binaryen";

export const TICK_MODULE = "lix:runtime/deadline";

/** Fields containing child expressions for the supported, non-GC core ISA. */
const fields: Record<string, string[]> = {
  Block: ["children"],
  If: ["condition", "ifTrue", "ifFalse"],
  Loop: ["body"],
  Break: ["condition", "value"],
  Switch: ["condition", "value"],
  Call: ["operands"],
  CallIndirect: ["target", "operands"],
  LocalGet: [],
  LocalSet: ["value"],
  GlobalGet: [],
  GlobalSet: ["value"],
  TableGet: ["index"],
  TableSet: ["index", "value"],
  TableSize: [],
  TableGrow: ["value", "delta"],
  Load: ["ptr"],
  Store: ["ptr", "value"],
  Const: [],
  Unary: ["value"],
  Binary: ["left", "right"],
  Select: ["ifTrue", "ifFalse", "condition"],
  Drop: ["value"],
  Return: ["value"],
  Nop: [],
  Unreachable: [],
  MemorySize: [],
  MemoryGrow: ["delta"],
  MemoryInit: ["dest", "offset", "size"],
  DataDrop: [],
  MemoryCopy: ["dest", "source", "size"],
  MemoryFill: ["dest", "value", "size"],
  RefNull: [],
  RefFunc: [],
  RefIs: ["value"],
  RefAs: ["value"],
  RefEq: ["left", "right"],
  TupleMake: ["operands"],
  TupleExtract: ["tuple"],
};
const childFields = new Map<number, string[]>(
  Object.entries(fields).map(([name, keys]) => [
    (binaryen as any)[`${name}Id`],
    keys,
  ]),
);

/** Add checks on every cycle (function/loop entry) and cap memory before compilation. */
export function instrumentCore(
  bytes: Uint8Array,
  maxMemoryBytes: number,
): Uint8Array {
  if (!Number.isSafeInteger(maxMemoryBytes) || maxMemoryBytes < 65536)
    throw new Error("Component memory limit must be at least one Wasm page");
  const module = binaryen.readBinary(
    capMemory(bytes, Math.floor(maxMemoryBytes / 65536)),
  );
  try {
    module.setFeatures(binaryen.Features.All);
    // Match the native host's table element limit as well as linear memory.
    for (let index = 0; index < module.getNumTables(); index++) {
      const table = module.getTableByIndex(index);
      const info = binaryen.getTableInfo(table);
      if (info.initial > 1_000_000)
        throw new Error("Component table exceeds element limit");
      (binaryen as any)._BinaryenTableSetMax(
        table,
        Math.min(info.max ?? 1_000_000, 1_000_000),
      );
    }
    if (module.hasMemory() && Boolean(module.getMemoryInfo().module))
      throw new Error("Imported component memories are unsupported");
    if (module.getExport("__lix_runtime_memory"))
      throw new Error("Reserved runtime memory export");
    let serial = 0;
    let tick = "lix_deadline";
    while (module.getFunction(tick)) tick = `lix_deadline_${++serial}`;
    module.addFunctionImport(
      tick,
      TICK_MODULE,
      "tick",
      binaryen.none,
      binaryen.none,
    );
    const check = () => module.call(tick, [], binaryen.none);
    for (let index = 0; index < module.getNumFunctions(); index++) {
      const func = module.getFunctionByIndex(index);
      const body = binaryen.getFunctionInfo(func).body;
      if (!body) continue;
      const pending = [body];
      while (pending.length) {
        const expression = pending.pop()!;
        const expressionId = binaryen.getExpressionId(expression);
        if (!childFields.has(expressionId))
          throw new Error(`Unsupported component instruction ${expressionId}`);
        if (childFields.get(expressionId)!.length === 0) continue;
        const info = binaryen.getExpressionInfo(
          expression,
        ) as unknown as Record<string, any>;
        const keys = childFields.get(info.id);
        if (!keys)
          throw new Error(`Unsupported component instruction ${info.id}`);
        for (const key of keys) {
          const value = info[key];
          if (Array.isArray(value)) pending.push(...value.filter(Boolean));
          else if (value) pending.push(value);
        }
        if (info.id === binaryen.LoopId) {
          (binaryen as any)._BinaryenLoopSetBody(
            expression,
            module.block(
              null,
              [check(), info.body],
              binaryen.getExpressionType(info.body),
            ),
          );
        }
      }
      (binaryen as any)._BinaryenFunctionSetBody(
        func,
        module.block(null, [check(), body], binaryen.getExpressionType(body)),
      );
    }
    if (!module.validate())
      throw new Error("Invalid instrumented component module");
    return exposeMemory(module.emitBinary(), module.hasMemory());
  } finally {
    module.dispose();
  }
}

/** Rewrite only the core memory section; all other sections stay byte-identical. */
function capMemory(bytes: Uint8Array, pages: number): Uint8Array {
  let offset = 8;
  const read = (): number => {
    let value = 0,
      shift = 0;
    for (let count = 0; count < 5; count++) {
      const byte = bytes[offset++];
      if (byte === undefined) throw new Error("Truncated Wasm integer");
      value += (byte & 127) * 2 ** shift;
      if (!(byte & 128)) return value;
      shift += 7;
    }
    throw new Error("Invalid Wasm integer");
  };
  const leb = (value: number): number[] => {
    const result: number[] = [];
    do {
      const byte = value % 128;
      value = Math.floor(value / 128);
      result.push(byte | (value ? 128 : 0));
    } while (value);
    return result;
  };
  while (offset < bytes.length) {
    const sectionStart = offset;
    const id = bytes[offset++];
    const length = read();
    const end = offset + length;
    if (end > bytes.length) throw new Error("Truncated Wasm section");
    if (id !== 5) {
      offset = end;
      continue;
    }
    const count = read();
    if (count !== 1)
      throw new Error("Multiple component memories are unsupported");
    const flags = read();
    if (flags !== 0 && flags !== 1)
      throw new Error("Shared and 64-bit component memories are unsupported");
    const minimum = read();
    const maximum = flags === 1 ? read() : pages;
    if (offset !== end || minimum > pages)
      throw new Error("Component initial memory exceeds limit");
    const memory = [1, 1, ...leb(minimum), ...leb(Math.min(maximum, pages))];
    const section = new Uint8Array([5, ...leb(memory.length), ...memory]);
    const result = new Uint8Array(
      sectionStart + section.length + bytes.length - end,
    );
    result.set(bytes.subarray(0, sectionStart));
    result.set(section, sectionStart);
    result.set(bytes.subarray(end), sectionStart + section.length);
    return result;
  }
  return bytes;
}

/** Add an inspection-only memory export without changing any guest index. */
function exposeMemory(bytes: Uint8Array, hasMemory: boolean): Uint8Array {
  if (!hasMemory) return bytes;
  const name = new TextEncoder().encode("__lix_runtime_memory");
  const encode = (value: number): number[] => {
    const out: number[] = [];
    do {
      const byte = value % 128;
      value = Math.floor(value / 128);
      out.push(byte | (value ? 128 : 0));
    } while (value);
    return out;
  };
  let offset = 8;
  const read = () => {
    let result = 0,
      shift = 0;
    for (;;) {
      const b = bytes[offset++]!;
      result += (b & 127) * 2 ** shift;
      if (!(b & 128)) return result;
      shift += 7;
    }
  };
  while (offset < bytes.length) {
    const start = offset;
    const id = bytes[offset++]!;
    const size = read();
    const end = offset + size;
    if (id < 7 || id === 0) {
      offset = end;
      continue;
    }
    let payload: Uint8Array;
    let replaceEnd = start;
    if (id === 7) {
      const count = read();
      payload = new Uint8Array([
        ...encode(count + 1),
        ...bytes.subarray(offset, end),
        ...encode(name.length),
        ...name,
        2,
        0,
      ]);
      replaceEnd = end;
    } else payload = new Uint8Array([1, ...encode(name.length), ...name, 2, 0]);
    const header = new Uint8Array([7, ...encode(payload.length)]);
    const result = new Uint8Array(
      start + header.length + payload.length + bytes.length - replaceEnd,
    );
    result.set(bytes.subarray(0, start));
    result.set(header, start);
    result.set(payload, start + header.length);
    result.set(
      bytes.subarray(replaceEnd),
      start + header.length + payload.length,
    );
    return result;
  }
  const payload = new Uint8Array([1, ...encode(name.length), ...name, 2, 0]);
  const result = new Uint8Array(
    bytes.length + payload.length + encode(payload.length).length + 1,
  );
  result.set(bytes);
  result.set([7, ...encode(payload.length), ...payload], bytes.length);
  return result;
}

/** Count defined memories to apportion the component's aggregate ceiling. */
export function coreMemoryCount(bytes: Uint8Array): number {
  return coreSectionCount(bytes, 5);
}
export function coreTableCount(bytes: Uint8Array): number {
  return coreSectionCount(bytes, 4);
}
function coreSectionCount(bytes: Uint8Array, section: number): number {
  let offset = 8;
  const read = () => {
    let value = 0;
    for (let shift = 0; shift < 35; shift += 7) {
      const byte = bytes[offset++];
      if (byte === undefined) throw new Error("Truncated Wasm integer");
      value += (byte & 127) * 2 ** shift;
      if (!(byte & 128)) return value;
    }
    throw new Error("Invalid Wasm integer");
  };
  while (offset < bytes.length) {
    const id = bytes[offset++];
    const size = read();
    if (id === section) return read();
    offset += size;
  }
  return 0;
}
