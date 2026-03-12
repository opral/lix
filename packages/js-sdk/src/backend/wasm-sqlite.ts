import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type {
	Database,
	Sqlite3Static,
	SqlValue,
} from "@sqlite.org/sqlite-wasm";
import type {
	LixBackend,
	LixRuntimeQueryResult,
	LixRuntimeValue,
	LixTransaction,
} from "../types.js";

type SqliteWasmDatabase = Database & {
	sqlite3: Sqlite3Static;
};

const sqliteWasmAssetUrl = new URL(
	"./sqlite3.wasm",
	import.meta.url,
).toString();

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
			locateFile: (path, prefix) =>
				path === "sqlite3.wasm" ? sqliteWasmAssetUrl : `${prefix}${path}`,
		});
	}

	const db = new sqlite3.oo1.DB(":memory:", "c");
	// @ts-expect-error - attach module for consumers
	db.sqlite3 = sqlite3;
	return db as SqliteWasmDatabase;
}

export async function createWasmSqliteBackend(): Promise<LixBackend> {
	const db = await createInMemoryDatabase();
	let backendClosed = false;
	let operationQueue: Promise<void> = Promise.resolve();

	const ensureBackendOpen = (): void => {
		if (backendClosed) {
			throw new Error("sqlite backend is closed");
		}
	};

	const runSerialized = async <T>(
		operation: () => T | Promise<T>,
	): Promise<T> => {
		const previous = operationQueue;
		let releaseCurrent: (() => void) | undefined;
		operationQueue = new Promise<void>((resolve) => {
			releaseCurrent = resolve;
		});
		await previous;
		try {
			return await operation();
		} finally {
			releaseCurrent?.();
		}
	};

	const runQuery = (
		sql: string,
		params: ReadonlyArray<LixRuntimeValue>,
	): LixRuntimeQueryResult => {
		ensureBackendOpen();
		try {
			const boundParams: SqlValue[] = params.map(toSqlParam);
			const rows: SqlValue[][] = [];
			const columns: string[] = [];
			db.exec({
				sql,
				bind: boundParams,
				rowMode: "array",
				columnNames: columns,
				resultRows: rows,
			});
			const normalizedRows = rows.map((row) =>
				row.map((value) => fromSqlValue(value)),
			);
			return {
				rows: normalizedRows,
				columns,
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
			async execute(
				sql: string,
				params: ReadonlyArray<LixRuntimeValue>,
			): Promise<LixRuntimeQueryResult> {
				if (transactionClosed) {
					throw new Error("transaction is already closed");
				}
				return runSerialized(() => {
					ensureBackendOpen();
					return runQuery(sql, params);
				});
			},
			async commit(): Promise<void> {
				if (transactionClosed) {
					return;
				}
				await runSerialized(() => {
					ensureBackendOpen();
					runQuery("COMMIT", []);
				});
				transactionClosed = true;
			},
			async rollback(): Promise<void> {
				if (transactionClosed) {
					return;
				}
				await runSerialized(() => {
					ensureBackendOpen();
					runQuery("ROLLBACK", []);
				});
				transactionClosed = true;
			},
		};
	};

	return {
		dialect: "sqlite",
		async execute(
			sql: string,
			params: ReadonlyArray<LixRuntimeValue>,
		): Promise<LixRuntimeQueryResult> {
			return runSerialized(() => runQuery(sql, params));
		},
		async beginTransaction(): Promise<LixTransaction> {
			return runSerialized(() => {
				ensureBackendOpen();
				runQuery("BEGIN", []);
				return createTransaction();
			});
		},
		async exportSnapshot(): Promise<Uint8Array> {
			return runSerialized(() => {
				ensureBackendOpen();
				return db.sqlite3.capi.sqlite3_js_db_export(db, "main");
			});
		},
		async close(): Promise<void> {
			if (backendClosed) {
				return;
			}
			await runSerialized(() => {
				if (backendClosed) {
					return;
				}
				backendClosed = true;
				db.close();
			});
		},
	};
}

function toSqlParam(raw: LixRuntimeValue): SqlValue {
	if (raw === null) return null;
	if (typeof raw === "boolean") return raw ? 1 : 0;
	if (typeof raw === "number") return raw;
	if (typeof raw === "string") return raw;
	if (raw instanceof Uint8Array) return raw;
	return null;
}

function fromSqlValue(value: SqlValue): LixRuntimeValue {
	if (value === null || value === undefined) return null;
	if (typeof value === "number") {
		return value;
	}
	if (typeof value === "string") return value;
	if (value instanceof Uint8Array) return value;
	if (value instanceof ArrayBuffer) return new Uint8Array(value);
	if (value instanceof Int8Array) return new Uint8Array(value);
	if (typeof value === "bigint") return Number(value);
	return String(value);
}
