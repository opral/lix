import Database from "better-sqlite3";
import type { LixBackend, LixTransaction } from "js-sdk";
import { Value } from "js-sdk";
import type { QueryResult } from "js-sdk";
type BetterSqlite3Options = {
  filename?: string;
  options?: ConstructorParameters<typeof Database>[1];
};

export async function createBetterSqlite3Backend(
  options: BetterSqlite3Options = {},
): Promise<LixBackend> {
  const db = new Database(options.filename ?? ":memory:", options.options);
  let backendClosed = false;

  const ensureBackendOpen = (): void => {
    if (backendClosed) {
      throw new Error("sqlite backend is closed");
    }
  };

  const runQuery = (sql: string, params: ReadonlyArray<unknown>): QueryResult => {
    ensureBackendOpen();
    try {
      if (params.length === 0 && looksLikeMultiStatementSql(sql)) {
        db.exec(sql);
        return { rows: [] };
      }

      const statement = db.prepare(sql);
      const boundParams = params.map(toSqlParam);
      const numberedPlaceholderMax = detectIndexedPlaceholderMax(sql);
      const numberedBindObject =
        numberedPlaceholderMax > 0
          ? toNumberedBindObject(boundParams, numberedPlaceholderMax)
          : null;
      if (statement.reader) {
        const rawRows =
          boundParams.length === 0
            ? (statement.raw(true).all() as unknown[][])
            : numberedBindObject
              ? (statement.raw(true).all(numberedBindObject) as unknown[][])
              : (statement.raw(true).all(boundParams) as unknown[][]);
        const rows = rawRows.map((row: readonly unknown[]) =>
          Array.isArray(row) ? row.map((value) => fromSqlValue(value)) : [],
        );
        return { rows };
      }

      if (boundParams.length === 0) {
        statement.run();
      } else if (numberedBindObject) {
        statement.run(numberedBindObject);
      } else {
        statement.run(boundParams);
      }
      return { rows: [] };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const sqlPreview = sql.replace(/\s+/g, " ").slice(0, 500);
      throw new Error(`${message} [sql: ${sqlPreview}]`);
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
        return runQuery(sql, params);
      },
      async commit(): Promise<void> {
        if (transactionClosed) return;
        runQuery("COMMIT", []);
        transactionClosed = true;
      },
      async rollback(): Promise<void> {
        if (transactionClosed) return;
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
      runQuery("BEGIN", []);
      return createTransaction();
    },
    async exportSnapshot(): Promise<Uint8Array> {
      ensureBackendOpen();
      return db.serialize();
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

function toSqlParam(raw: unknown): unknown {
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

function looksLikeMultiStatementSql(sql: string): boolean {
  const segments = sql
    .split(";")
    .map((segment) => segment.trim())
    .filter(Boolean);
  return segments.length > 1;
}

function detectIndexedPlaceholderMax(sql: string): number {
  const numberedPlaceholders = [...sql.matchAll(/(?:\?|\$)(\d+)/g)];
  let maxIndex = 0;
  for (const match of numberedPlaceholders) {
    const parsed = Number.parseInt(match[1] ?? "0", 10);
    if (Number.isFinite(parsed) && parsed > maxIndex) {
      maxIndex = parsed;
    }
  }
  return maxIndex;
}

function toNumberedBindObject(
  params: ReadonlyArray<unknown>,
  maxIndex: number,
): Record<string, unknown> {
  const bind: Record<string, unknown> = {};
  for (let index = 1; index <= maxIndex; index++) {
    if (index - 1 < params.length) {
      bind[String(index)] = params[index - 1];
    }
  }
  return bind;
}

function fromSqlValue(value: unknown): ReturnType<typeof Value.from> {
  if (value === null || value === undefined) return Value.null();
  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return Value.integer(value);
    }
    return Value.real(value);
  }
  if (typeof value === "string") return Value.text(value);
  if (value instanceof Uint8Array) return Value.blob(value);
  if (value instanceof ArrayBuffer) return Value.blob(new Uint8Array(value));
  if (value instanceof Int8Array) return Value.blob(new Uint8Array(value));
  if (typeof value === "bigint") return Value.integer(Number(value));
  return Value.text(String(value));
}
