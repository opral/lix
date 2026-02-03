import init, {
  openLix as openLixWasm,
  wasmBinary,
  QueryResult,
  Value,
} from "./engine-wasm/index.js";
import { createWasmSqliteBackend } from "./backend/wasm-sqlite.js";
import type { LixBackend } from "./types.js";

export type { LixBackend } from "./types.js";
export { QueryResult, Value } from "./engine-wasm/index.js";

export type Lix = {
  execute(sql: string, params: Value[]): Promise<QueryResult>;
};

let wasmReady: Promise<void> | null = null;

async function ensureWasmReady(): Promise<void> {
  if (!wasmReady) {
    wasmReady = init(wasmBinary).then(() => undefined);
  }
  await wasmReady;
}

export async function openLix(
  args: {
    backend?: LixBackend;
  } = {},
): Promise<Lix> {
  await ensureWasmReady();
  const backend = args.backend ?? (await createWasmSqliteBackend());
  return openLixWasm(backend);
}
