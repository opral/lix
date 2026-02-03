import init, {
  openLix as openLixWasm,
  wasmBinary,
  type LixBackend,
  type QueryResult,
  type Value,
} from "@lix-js/engine-wasm";
import { createWasmSqliteBackend } from "./backend/wasm-sqlite.js";

export type { LixBackend, QueryResult, Value } from "@lix-js/engine-wasm";

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
