import binaryen from "binaryen";
import { TICK_MODULE } from "./instrument-helpers.js";
export { coreMemoryCount, coreTableCount, TICK_MODULE } from "./instrument-helpers.js";

type ExpressionRef = binaryen.ExpressionRef;

/**
 * Binaryen 130 exposes these typed expression getters at runtime but omits
 * them from its TypeScript declarations. Keep the cast narrow so changes to
 * the visitor remain checked against the getter surface we actually use.
 */
type BinaryenChildGetters = {
  Block: {
    getNumChildren(expression: ExpressionRef): number;
    getChildAt(expression: ExpressionRef, index: number): ExpressionRef;
  };
  If: {
    getCondition(expression: ExpressionRef): ExpressionRef;
    getIfTrue(expression: ExpressionRef): ExpressionRef;
    getIfFalse(expression: ExpressionRef): ExpressionRef;
  };
  Break: {
    getCondition(expression: ExpressionRef): ExpressionRef;
    getValue(expression: ExpressionRef): ExpressionRef;
  };
  Switch: {
    getCondition(expression: ExpressionRef): ExpressionRef;
    getValue(expression: ExpressionRef): ExpressionRef;
  };
  Call: {
    getNumOperands(expression: ExpressionRef): number;
    getOperandAt(expression: ExpressionRef, index: number): ExpressionRef;
  };
  CallIndirect: {
    getTarget(expression: ExpressionRef): ExpressionRef;
    getNumOperands(expression: ExpressionRef): number;
    getOperandAt(expression: ExpressionRef, index: number): ExpressionRef;
  };
  LocalSet: { getValue(expression: ExpressionRef): ExpressionRef };
  GlobalSet: { getValue(expression: ExpressionRef): ExpressionRef };
  TableGet: { getIndex(expression: ExpressionRef): ExpressionRef };
  TableSet: {
    getIndex(expression: ExpressionRef): ExpressionRef;
    getValue(expression: ExpressionRef): ExpressionRef;
  };
  TableGrow: {
    getValue(expression: ExpressionRef): ExpressionRef;
    getDelta(expression: ExpressionRef): ExpressionRef;
  };
  Load: { getPtr(expression: ExpressionRef): ExpressionRef };
  Store: {
    getPtr(expression: ExpressionRef): ExpressionRef;
    getValue(expression: ExpressionRef): ExpressionRef;
  };
  Unary: { getValue(expression: ExpressionRef): ExpressionRef };
  Binary: {
    getLeft(expression: ExpressionRef): ExpressionRef;
    getRight(expression: ExpressionRef): ExpressionRef;
  };
  Select: {
    getIfTrue(expression: ExpressionRef): ExpressionRef;
    getIfFalse(expression: ExpressionRef): ExpressionRef;
    getCondition(expression: ExpressionRef): ExpressionRef;
  };
  Drop: { getValue(expression: ExpressionRef): ExpressionRef };
  Return: { getValue(expression: ExpressionRef): ExpressionRef };
  Loop: { getBody(expression: ExpressionRef): ExpressionRef };
  MemoryGrow: { getDelta(expression: ExpressionRef): ExpressionRef };
  MemoryInit: {
    getDest(expression: ExpressionRef): ExpressionRef;
    getOffset(expression: ExpressionRef): ExpressionRef;
    getSize(expression: ExpressionRef): ExpressionRef;
  };
  MemoryCopy: {
    getDest(expression: ExpressionRef): ExpressionRef;
    getSource(expression: ExpressionRef): ExpressionRef;
    getSize(expression: ExpressionRef): ExpressionRef;
  };
  MemoryFill: {
    getDest(expression: ExpressionRef): ExpressionRef;
    getValue(expression: ExpressionRef): ExpressionRef;
    getSize(expression: ExpressionRef): ExpressionRef;
  };
  RefAs: { getValue(expression: ExpressionRef): ExpressionRef };
  RefEq: {
    getLeft(expression: ExpressionRef): ExpressionRef;
    getRight(expression: ExpressionRef): ExpressionRef;
  };
  TupleMake: {
    getNumOperands(expression: ExpressionRef): number;
    getOperandAt(expression: ExpressionRef, index: number): ExpressionRef;
  };
  TupleExtract: { getTuple(expression: ExpressionRef): ExpressionRef };
};

const expressionApi = binaryen as unknown as BinaryenChildGetters;

/**
 * Visit child expressions with Binaryen's typed getters. `getExpressionInfo`
 * builds a metadata object for every visited node and materializes strings for
 * labels/names that instrumentation never uses. A component can contain
 * hundreds of thousands of expressions, so avoid that per-expression
 * metadata allocation.
 */
function appendExpressionChildren(
  expressionId: number,
  expression: number,
  pending: number[],
): void {
  switch (expressionId) {
    case binaryen.BlockId: {
      const count = expressionApi.Block.getNumChildren(expression);
      for (let index = 0; index < count; index++) {
        const child = expressionApi.Block.getChildAt(expression, index);
        if (child) pending.push(child);
      }
      return;
    }
    case binaryen.IfId: {
      const condition = expressionApi.If.getCondition(expression);
      const ifTrue = expressionApi.If.getIfTrue(expression);
      const ifFalse = expressionApi.If.getIfFalse(expression);
      if (condition) pending.push(condition);
      if (ifTrue) pending.push(ifTrue);
      if (ifFalse) pending.push(ifFalse);
      return;
    }
    case binaryen.BreakId: {
      const condition = expressionApi.Break.getCondition(expression);
      const value = expressionApi.Break.getValue(expression);
      if (condition) pending.push(condition);
      if (value) pending.push(value);
      return;
    }
    case binaryen.SwitchId: {
      const condition = expressionApi.Switch.getCondition(expression);
      const value = expressionApi.Switch.getValue(expression);
      if (condition) pending.push(condition);
      if (value) pending.push(value);
      return;
    }
    case binaryen.CallId: {
      const count = expressionApi.Call.getNumOperands(expression);
      for (let index = 0; index < count; index++) {
        const operand = expressionApi.Call.getOperandAt(expression, index);
        if (operand) pending.push(operand);
      }
      return;
    }
    case binaryen.CallIndirectId: {
      const target = expressionApi.CallIndirect.getTarget(expression);
      if (target) pending.push(target);
      const count = expressionApi.CallIndirect.getNumOperands(expression);
      for (let index = 0; index < count; index++) {
        const operand = expressionApi.CallIndirect.getOperandAt(expression, index);
        if (operand) pending.push(operand);
      }
      return;
    }
    case binaryen.LocalSetId: {
      const value = expressionApi.LocalSet.getValue(expression);
      if (value) pending.push(value);
      return;
    }
    case binaryen.GlobalSetId: {
      const value = expressionApi.GlobalSet.getValue(expression);
      if (value) pending.push(value);
      return;
    }
    case binaryen.TableGetId: {
      const index = expressionApi.TableGet.getIndex(expression);
      if (index) pending.push(index);
      return;
    }
    case binaryen.TableSetId: {
      const index = expressionApi.TableSet.getIndex(expression);
      const value = expressionApi.TableSet.getValue(expression);
      if (index) pending.push(index);
      if (value) pending.push(value);
      return;
    }
    case binaryen.TableGrowId: {
      const value = expressionApi.TableGrow.getValue(expression);
      const delta = expressionApi.TableGrow.getDelta(expression);
      if (value) pending.push(value);
      if (delta) pending.push(delta);
      return;
    }
    case binaryen.LoadId: {
      const ptr = expressionApi.Load.getPtr(expression);
      if (ptr) pending.push(ptr);
      return;
    }
    case binaryen.StoreId: {
      const ptr = expressionApi.Store.getPtr(expression);
      const value = expressionApi.Store.getValue(expression);
      if (ptr) pending.push(ptr);
      if (value) pending.push(value);
      return;
    }
    case binaryen.UnaryId: {
      const value = expressionApi.Unary.getValue(expression);
      if (value) pending.push(value);
      return;
    }
    case binaryen.BinaryId: {
      const left = expressionApi.Binary.getLeft(expression);
      const right = expressionApi.Binary.getRight(expression);
      if (left) pending.push(left);
      if (right) pending.push(right);
      return;
    }
    case binaryen.SelectId: {
      const ifTrue = expressionApi.Select.getIfTrue(expression);
      const ifFalse = expressionApi.Select.getIfFalse(expression);
      const condition = expressionApi.Select.getCondition(expression);
      if (ifTrue) pending.push(ifTrue);
      if (ifFalse) pending.push(ifFalse);
      if (condition) pending.push(condition);
      return;
    }
    case binaryen.DropId: {
      const value = expressionApi.Drop.getValue(expression);
      if (value) pending.push(value);
      return;
    }
    case binaryen.ReturnId: {
      const value = expressionApi.Return.getValue(expression);
      if (value) pending.push(value);
      return;
    }
    case binaryen.MemoryGrowId: {
      const delta = expressionApi.MemoryGrow.getDelta(expression);
      if (delta) pending.push(delta);
      return;
    }
    case binaryen.MemoryInitId: {
      const dest = expressionApi.MemoryInit.getDest(expression);
      const offset = expressionApi.MemoryInit.getOffset(expression);
      const size = expressionApi.MemoryInit.getSize(expression);
      if (dest) pending.push(dest);
      if (offset) pending.push(offset);
      if (size) pending.push(size);
      return;
    }
    case binaryen.MemoryCopyId: {
      const dest = expressionApi.MemoryCopy.getDest(expression);
      const source = expressionApi.MemoryCopy.getSource(expression);
      const size = expressionApi.MemoryCopy.getSize(expression);
      if (dest) pending.push(dest);
      if (source) pending.push(source);
      if (size) pending.push(size);
      return;
    }
    case binaryen.MemoryFillId: {
      const dest = expressionApi.MemoryFill.getDest(expression);
      const value = expressionApi.MemoryFill.getValue(expression);
      const size = expressionApi.MemoryFill.getSize(expression);
      if (dest) pending.push(dest);
      if (value) pending.push(value);
      if (size) pending.push(size);
      return;
    }
    case binaryen.RefAsId: {
      const value = expressionApi.RefAs.getValue(expression);
      if (value) pending.push(value);
      return;
    }
    case binaryen.RefEqId: {
      const left = expressionApi.RefEq.getLeft(expression);
      const right = expressionApi.RefEq.getRight(expression);
      if (left) pending.push(left);
      if (right) pending.push(right);
      return;
    }
    case binaryen.TupleMakeId: {
      const count = expressionApi.TupleMake.getNumOperands(expression);
      for (let index = 0; index < count; index++) {
        const operand = expressionApi.TupleMake.getOperandAt(expression, index);
        if (operand) pending.push(operand);
      }
      return;
    }
    case binaryen.TupleExtractId: {
      const tuple = expressionApi.TupleExtract.getTuple(expression);
      if (tuple) pending.push(tuple);
      return;
    }
    case binaryen.LocalGetId:
    case binaryen.GlobalGetId:
    case binaryen.TableSizeId:
    case binaryen.ConstId:
    case binaryen.NopId:
    case binaryen.UnreachableId:
    case binaryen.MemorySizeId:
    case binaryen.DataDropId:
    case binaryen.RefNullId:
    case binaryen.RefFuncId:
      return;
    default:
      throw new Error(`Unsupported component instruction ${expressionId}`);
  }
}

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
        if (expressionId === binaryen.LoopId) {
          const loopBody = expressionApi.Loop.getBody(expression);
          if (loopBody) pending.push(loopBody);
          (binaryen as any)._BinaryenLoopSetBody(
            expression,
            module.block(
              null,
              [check(), loopBody],
              binaryen.getExpressionType(loopBody),
            ),
          );
        } else {
          appendExpressionChildren(expressionId, expression, pending);
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
