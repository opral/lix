import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type {
  Database,
  Sqlite3Static,
  SqlValue,
} from "@sqlite.org/sqlite-wasm";
import { wasmBinary } from "./wasm-sqlite.wasm.js";
import type { LixBackend } from "../types.js";
import { Value } from "../engine-wasm/index.js";

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
    async execute(sql: string, params: Value[]): Promise<any> {
      const boundParams: SqlValue[] = params.map(toSqlParam);
      const rows: SqlValue[][] = [];
      db.exec({
        sql,
        bind: boundParams,
        rowMode: "array",
        resultRows: rows,
      });
      const normalizedRows = rows.map((row) =>
        row.map((value) => fromSqlValue(value)),
      );
      return {
        rows: normalizedRows,
      };
    },
  };
}

function toSqlParam(value: Value): SqlValue {
  switch (value.kind) {
    case "Null":
      return null;
    case "Integer":
      return value.asInteger() ?? null;
    case "Real":
      return value.asReal() ?? null;
    case "Text":
      return value.asText() ?? null;
    case "Blob":
      return value.asBlob() ?? null;
    default:
      return null;
  }
  return null;
}

function fromSqlValue(value: SqlValue): Value {
  if (value === null || value === undefined) return Value.null();
  if (typeof value === "number") {
    if (Number.isInteger(value)) return Value.integer(value);
    return Value.real(value);
  }
  if (typeof value === "string") return Value.text(value);
  if (value instanceof Uint8Array) return Value.blob(value);
  if (value instanceof ArrayBuffer) return Value.blob(new Uint8Array(value));
  if (value instanceof Int8Array) return Value.blob(new Uint8Array(value));
  if (typeof value === "bigint") return Value.integer(Number(value));
  return Value.text(String(value));
}
