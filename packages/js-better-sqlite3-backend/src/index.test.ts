import { expect, test } from "vitest";
import { createBetterSqlite3Backend } from "./index.js";
import type { LixQueryResultLike } from "js-sdk";

function rowsOf(result: LixQueryResultLike): unknown[][] {
  return Array.isArray(result) ? result : result.rows;
}

test("createBetterSqlite3Backend exposes dialect and beginTransaction", async () => {
  const backend = await createBetterSqlite3Backend();

  expect(backend.dialect).toBe("sqlite");
  expect(typeof backend.beginTransaction).toBe("function");
  await backend.close?.();
});

test("beginTransaction commit persists writes", async () => {
  const backend = await createBetterSqlite3Backend();

  await backend.execute("CREATE TABLE t (value INTEGER)", []);
  const transaction = await backend.beginTransaction?.();
  if (!transaction) {
    throw new Error("Expected beginTransaction to be available");
  }

  await transaction.execute("INSERT INTO t (value) VALUES (?)", [1]);
  await transaction.commit();

  const result = await backend.execute("SELECT value FROM t ORDER BY rowid LIMIT 1", []);
  const rows = rowsOf(result);
  expect(rows[0][0]).toEqual({ kind: "Integer", value: 1 });
  await backend.close?.();
});

test("beginTransaction rollback discards writes", async () => {
  const backend = await createBetterSqlite3Backend();

  await backend.execute("CREATE TABLE t (value INTEGER)", []);
  const transaction = await backend.beginTransaction?.();
  if (!transaction) {
    throw new Error("Expected beginTransaction to be available");
  }

  await transaction.execute("INSERT INTO t (value) VALUES (?)", [1]);
  await transaction.rollback();

  const result = await backend.execute("SELECT COUNT(*) FROM t", []);
  const rows = rowsOf(result);
  expect(rows[0][0]).toEqual({ kind: "Integer", value: 0 });
  await backend.close?.();
});

test("close is idempotent and prevents further queries", async () => {
  const backend = await createBetterSqlite3Backend();
  await backend.execute("CREATE TABLE t (value INTEGER)", []);
  await backend.close?.();
  await backend.close?.();

  await expect(backend.execute("SELECT 1", [])).rejects.toThrow(
    "sqlite backend is closed",
  );
});

test("exportSnapshot returns bytes", async () => {
  const backend = await createBetterSqlite3Backend();
  await backend.execute("CREATE TABLE t (value INTEGER)", []);
  await backend.execute("INSERT INTO t (value) VALUES (?)", [42]);

  expect(typeof backend.exportSnapshot).toBe("function");
  const snapshot = await backend.exportSnapshot!();
  expect(snapshot).toBeInstanceOf(Uint8Array);
  expect(snapshot.byteLength).toBeGreaterThan(0);
  await backend.close?.();
});
