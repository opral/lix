import { expect, test } from "vitest";
import { createWasmSqliteBackend } from "./wasm-sqlite.js";
import type { LixRuntimeQueryResult } from "../types.js";

function rowsOf(result: LixRuntimeQueryResult): unknown[][] {
	return result.rows;
}

test("createWasmSqliteBackend exposes dialect and beginTransaction", async () => {
	const backend = await createWasmSqliteBackend();

	expect(backend.dialect).toBe("sqlite");
	expect(typeof backend.beginTransaction).toBe("function");
});

test("beginTransaction commit persists writes", async () => {
	const backend = await createWasmSqliteBackend();

	await backend.execute("CREATE TABLE t (value INTEGER)", []);
	const transaction = await backend.beginTransaction?.();
	if (!transaction) {
		throw new Error("Expected beginTransaction to be available");
	}

	await transaction.execute("INSERT INTO t (value) VALUES (?)", [1]);
	await transaction.commit();

	const result = await backend.execute(
		"SELECT value FROM t ORDER BY rowid LIMIT 1",
		[],
	);
	const rows = rowsOf(result);
	expect(rows[0][0]).toBe(1);
});

test("beginTransaction rollback discards writes", async () => {
	const backend = await createWasmSqliteBackend();

	await backend.execute("CREATE TABLE t (value INTEGER)", []);
	const transaction = await backend.beginTransaction?.();
	if (!transaction) {
		throw new Error("Expected beginTransaction to be available");
	}

	await transaction.execute("INSERT INTO t (value) VALUES (?)", [1]);
	await transaction.rollback();

	const result = await backend.execute("SELECT COUNT(*) FROM t", []);
	const rows = rowsOf(result);
	expect(rows[0][0]).toBe(0);
});

test("close is idempotent and prevents further queries", async () => {
	const backend = await createWasmSqliteBackend();
	await backend.execute("CREATE TABLE t (value INTEGER)", []);
	await backend.close?.();
	await backend.close?.();

	await expect(backend.execute("SELECT 1", [])).rejects.toThrow(
		"sqlite backend is closed",
	);
});

test("blob values roundtrip as Uint8Array", async () => {
	const backend = await createWasmSqliteBackend();
	await backend.execute("CREATE TABLE t (value BLOB)", []);
	await backend.execute("INSERT INTO t (value) VALUES (?)", [
		new Uint8Array([1, 2, 3]),
	]);
	const result = await backend.execute("SELECT value FROM t LIMIT 1", []);
	const rows = rowsOf(result);
	expect(rows[0][0]).toEqual(new Uint8Array([1, 2, 3]));
	await backend.close?.();
});
