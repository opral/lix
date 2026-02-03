import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { Database, Sqlite3Static } from "@sqlite.org/sqlite-wasm";
import { wasmBinary } from "./wasm-sqlite.wasm.js";
import type { LixBackend, QueryResult, Value } from "../open-lix.js";

type SqliteWasmDatabase = Database & {
  sqlite3: Sqlite3Static;
};

// https://github.com/opral/lix-sdk/issues/231
// @ts-expect-error - globalThis
globalThis.sqlite3ApiConfig = {
  warn: (message: string, details: unknown) => {
    if (message === "Ignoring inability to install OPFS sqlite3_vfs:") {
      return;
    }
    console.log(`${message} ${details}`);
  },
};

let sqlite3: Sqlite3Static | undefined;

async function createInMemoryDatabase(): Promise<SqliteWasmDatabase> {
  if (!sqlite3) {
    sqlite3 = await sqlite3InitModule({
      // @ts-expect-error - wasmBinary type mismatch
      wasmBinary,
      locateFile: () => "sqlite3.wasm",
    });
  }

  const db = new sqlite3.oo1.DB(":memory:", "c");
  // @ts-expect-error - attach module for consumers
  db.sqlite3 = sqlite3;
  return db as SqliteWasmDatabase;
}

export async function createWasmSqliteBackend(): Promise<LixBackend> {
  const db = await createInMemoryDatabase();
  return {
    async execute(sql: string, params: Value[]): Promise<QueryResult> {
      const boundParams = params.map(toSqlParam);
      const rows: unknown[][] = [];
      db.exec({
        sql,
        bind: boundParams,
        rowMode: "array",
        resultRows: rows,
      });
      return {
        rows: rows.map((row) => row.map((value) => fromSqlValue(value))),
      };
    },
  };
}

function toSqlParam(value: Value): unknown {
  if ("Null" in value) return null;
  if ("Integer" in value) return value.Integer;
  if ("Real" in value) return value.Real;
  if ("Text" in value) return value.Text;
  if ("Blob" in value) return value.Blob;
  return null;
}

function fromSqlValue(value: unknown): Value {
  if (value === null || value === undefined) return { Null: null };
  if (typeof value === "number") {
    if (Number.isInteger(value)) return { Integer: value };
    return { Real: value };
  }
  if (typeof value === "string") return { Text: value };
  if (value instanceof Uint8Array) return { Blob: value };
  return { Text: String(value) };
}
