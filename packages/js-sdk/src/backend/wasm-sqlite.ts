import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type {
  Database,
  Sqlite3Static,
  SqlValue,
} from "@sqlite.org/sqlite-wasm";
import type { LixBackend, LixTransaction } from "../types.js";
import { Value } from "../engine-wasm/index.js";
import type { QueryResult } from "../engine-wasm/index.js";

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
    sqlite3 = await sqlite3InitModule();
  }

  const db = new sqlite3.oo1.DB(":memory:", "c");
  // @ts-expect-error - attach module for consumers
  db.sqlite3 = sqlite3;
  return db as SqliteWasmDatabase;
}

export async function createWasmSqliteBackend(): Promise<LixBackend> {
  const db = await createInMemoryDatabase();
  let backendClosed = false;

  const ensureBackendOpen = (): void => {
    if (backendClosed) {
      throw new Error("sqlite backend is closed");
    }
  };

  const runQuery = (sql: string, params: ReadonlyArray<unknown>): QueryResult => {
    ensureBackendOpen();
    try {
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
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`${message}\nwhile executing SQL:\n${sql}`);
    }
  };

  const createTransaction = (): LixTransaction => {
    let transactionClosed = false;

    return {
      dialect: "sqlite",
      async execute(sql: string, params: ReadonlyArray<unknown>): Promise<QueryResult> {
        if (transactionClosed) {
          throw new Error("transaction is already closed");
        }
        ensureBackendOpen();
        return runQuery(sql, params);
      },
      async commit(): Promise<void> {
        if (transactionClosed) {
          return;
        }
        ensureBackendOpen();
        runQuery("COMMIT", []);
        transactionClosed = true;
      },
      async rollback(): Promise<void> {
        if (transactionClosed) {
          return;
        }
        ensureBackendOpen();
        runQuery("ROLLBACK", []);
        transactionClosed = true;
      },
    };
  };

  return {
    dialect: "sqlite",
    async execute(sql: string, params: ReadonlyArray<unknown>): Promise<QueryResult> {
      return runQuery(sql, params);
    },
    async beginTransaction(): Promise<LixTransaction> {
      ensureBackendOpen();
      runQuery("BEGIN", []);
      return createTransaction();
    },
    async close(): Promise<void> {
      if (backendClosed) {
        return;
      }
      backendClosed = true;
      db.close();
    },
  };
}

function toSqlParam(raw: unknown): SqlValue {
  const value = Value.from(raw);
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
