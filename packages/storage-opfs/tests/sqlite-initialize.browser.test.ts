import { afterEach, expect, test, vi } from "vitest";
import { initializeBundledSqlite } from "../js/sqlite-initialize.js";

const emptyWasm = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
afterEach(() => vi.restoreAllMocks());

test("invalid Wasm rejects initialization instead of hanging", async () => {
	await expect(
		initializeBundledSqlite(
			({ instantiateWasm }) =>
				new Promise((resolve) => {
					instantiateWasm({}, resolve);
				}),
			new Uint8Array([0]),
		),
	).rejects.toBeInstanceOf(WebAssembly.CompileError);
});

test("Wasm instantiation failure reaches the storage opener", async () => {
	const failure = new Error("Wasm instantiation failed");
	vi.spyOn(WebAssembly, "instantiate").mockRejectedValueOnce(failure);
	await expect(
		initializeBundledSqlite(
			({ instantiateWasm }) =>
				new Promise((resolve) => {
					instantiateWasm({}, resolve);
				}),
			emptyWasm,
		),
	).rejects.toBe(failure);
});

test("SQLite callback failure reaches the storage opener", async () => {
	const failure = new Error("SQLite initialization failed");
	await expect(
		initializeBundledSqlite(
			({ instantiateWasm }) =>
				new Promise(() => {
					instantiateWasm({}, () => {
						throw failure;
					});
				}),
			emptyWasm,
		),
	).rejects.toBe(failure);
});

test("returns the initialized SQLite module", async () => {
	const sqlite = { ready: true };
	await expect(
		initializeBundledSqlite(
			({ instantiateWasm }) =>
				new Promise((resolve) => {
					instantiateWasm({}, () => resolve(sqlite));
				}),
			emptyWasm,
		),
	).resolves.toBe(sqlite);
});

test("propagates the module initializer's own rejection", async () => {
	const failure = new Error("SQLite module failed");
	await expect(
		initializeBundledSqlite(() => Promise.reject(failure), emptyWasm),
	).rejects.toBe(failure);
});
