import type { Value, QueryResult } from "./engine-wasm/index.js";

export type LixBackend = {
  execute(sql: string, params: Value[]): Promise<QueryResult>;
};
