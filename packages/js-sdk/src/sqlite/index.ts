import DatabaseConstructor, { type Database } from "better-sqlite3";
import type {
	KvPair,
	KvScanRange,
	LixBackend,
	LixBackendTransaction,
	TransactionBeginMode,
} from "../open-lix.js";

export type BetterSqlite3BackendOptions = {
	path: string;
	databaseOptions?: BetterSqlite3DatabaseOptions;
};

export type BetterSqlite3DatabaseOptions = {
	readonly?: boolean;
	fileMustExist?: boolean;
	timeout?: number;
	verbose?: (message?: unknown, ...additional: unknown[]) => void;
};

const openFileHandles = new Set<string>();

export function createBetterSqlite3Backend(
	options: BetterSqlite3BackendOptions,
): LixBackend {
	if (!options.path) {
		throw new Error("createBetterSqlite3Backend() requires a non-empty path");
	}
	const registryKey = registryKeyForPath(options.path);
	if (registryKey && openFileHandles.has(registryKey)) {
		throw doubleOpenError(options.path);
	}
	let activeRegistryKey: string | null = registryKey;
	let db: Database | undefined;
	if (activeRegistryKey) {
		openFileHandles.add(activeRegistryKey);
	}
	try {
		db = new DatabaseConstructor(options.path, options.databaseOptions);
		initializeDatabase(db);
		return new BetterSqlite3Backend(db, activeRegistryKey);
	} catch (error) {
		if (activeRegistryKey) {
			openFileHandles.delete(activeRegistryKey);
		}
		if (db) {
			try {
				db.close();
			} catch {
				// Ignore close errors while preserving the original open failure.
			}
		}
		throw error;
	}
}

function initializeDatabase(db: Database): void {
	db.exec(`
		CREATE TABLE IF NOT EXISTS lix_kv (
			namespace TEXT NOT NULL,
			key BLOB NOT NULL,
			value BLOB NOT NULL,
			PRIMARY KEY (namespace, key)
		) WITHOUT ROWID
	`);
}

class BetterSqlite3Backend implements LixBackend {
	readonly #db: Database;
	readonly #registryKey: string | null;
	#closed = false;

	constructor(db: Database, registryKey: string | null) {
		this.#db = db;
		this.#registryKey = registryKey;
	}

	beginTransaction(mode: TransactionBeginMode): LixBackendTransaction {
		this.#ensureOpen();
		if (this.#db.inTransaction) {
			throw new Error("cannot open nested Lix backend transaction");
		}
		this.#db.exec(mode === "write" ? "BEGIN IMMEDIATE" : "BEGIN DEFERRED");
		return new BetterSqlite3Transaction(this.#db);
	}

	kvGet(namespace: string, key: Uint8Array): Uint8Array | null {
		this.#ensureOpen();
		return kvGet(this.#db, namespace, key);
	}

	kvScan(
		namespace: string,
		range: KvScanRange,
		limit?: number | null,
	): KvPair[] {
		this.#ensureOpen();
		return kvScan(this.#db, namespace, range, limit);
	}

	close(): void {
		if (this.#closed) return;
		try {
			this.#db.close();
		} finally {
			this.#closed = true;
			if (this.#registryKey) {
				openFileHandles.delete(this.#registryKey);
			}
		}
	}

	#ensureOpen(): void {
		if (this.#closed) {
			throw new Error("better-sqlite3 Lix backend is closed");
		}
	}
}

class BetterSqlite3Transaction implements LixBackendTransaction {
	readonly #db: Database;
	#closed = false;

	constructor(db: Database) {
		this.#db = db;
	}

	kvGet(namespace: string, key: Uint8Array): Uint8Array | null {
		this.#ensureOpen();
		return kvGet(this.#db, namespace, key);
	}

	kvScan(
		namespace: string,
		range: KvScanRange,
		limit?: number | null,
	): KvPair[] {
		this.#ensureOpen();
		return kvScan(this.#db, namespace, range, limit);
	}

	kvPut(namespace: string, key: Uint8Array, value: Uint8Array): void {
		this.#ensureOpen();
		this.#db
			.prepare(
				`INSERT INTO lix_kv (namespace, key, value)
				 VALUES (?, ?, ?)
				 ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value`,
			)
			.run(namespace, sqliteBytes(key), sqliteBytes(value));
	}

	kvDelete(namespace: string, key: Uint8Array): void {
		this.#ensureOpen();
		this.#db
			.prepare("DELETE FROM lix_kv WHERE namespace = ? AND key = ?")
			.run(namespace, sqliteBytes(key));
	}

	commit(): void {
		this.#ensureOpen();
		this.#db.exec("COMMIT");
		this.#closed = true;
	}

	rollback(): void {
		this.#ensureOpen();
		this.#db.exec("ROLLBACK");
		this.#closed = true;
	}

	#ensureOpen(): void {
		if (this.#closed) {
			throw new Error("Lix backend transaction is closed");
		}
	}
}

function kvGet(
	db: Database,
	namespace: string,
	key: Uint8Array,
): Uint8Array | null {
	const row = db
		.prepare("SELECT value FROM lix_kv WHERE namespace = ? AND key = ?")
		.get(namespace, sqliteBytes(key));
	if (!isObject(row) || !("value" in row)) {
		return null;
	}
	return bytesFromUnknown(row.value, "lix_kv.value");
}

function kvScan(
	db: Database,
	namespace: string,
	range: KvScanRange,
	limit?: number | null,
): KvPair[] {
	const { sql, params } = scanQuery(namespace, range, limit);
	return db.prepare(sql).all(...params).map((row) => {
		if (!isObject(row) || !("key" in row) || !("value" in row)) {
			throw new Error("invalid lix_kv scan row");
		}
		return {
			key: bytesFromUnknown(row.key, "lix_kv.key"),
			value: bytesFromUnknown(row.value, "lix_kv.value"),
		};
	});
}

function scanQuery(
	namespace: string,
	range: KvScanRange,
	limit?: number | null,
): { sql: string; params: unknown[] } {
	const params: unknown[] = [namespace];
	const clauses = ["namespace = ?"];

	if (range.kind === "prefix") {
		clauses.push("key >= ?");
		params.push(sqliteBytes(range.prefix));
		const end = prefixUpperBound(range.prefix);
		if (end) {
			clauses.push("key < ?");
			params.push(sqliteBytes(end));
		}
	} else {
		clauses.push("key >= ?", "key < ?");
		params.push(sqliteBytes(range.start), sqliteBytes(range.end));
	}

	let sql = `SELECT key, value FROM lix_kv WHERE ${clauses.join(
		" AND ",
	)} ORDER BY key`;
	if (limit != null) {
		sql += " LIMIT ?";
		params.push(limit);
	}
	return { sql, params };
}

function prefixUpperBound(prefix: Uint8Array): Uint8Array | null {
	const end = new Uint8Array(prefix);
	for (let index = end.length - 1; index >= 0; index--) {
		if (end[index] !== 0xff) {
			end[index]! += 1;
			return end.slice(0, index + 1);
		}
	}
	return null;
}

function bytesFromUnknown(value: unknown, context: string): Uint8Array {
	if (value instanceof Uint8Array) {
		return new Uint8Array(value);
	}
	throw new Error(`${context} must be bytes`);
}

function sqliteBytes(bytes: Uint8Array): Uint8Array {
	const buffer = (
		globalThis as typeof globalThis & {
			Buffer?: { from(bytes: Uint8Array): Uint8Array };
		}
	).Buffer;
	return buffer ? buffer.from(bytes) : bytes;
}

function registryKeyForPath(filename: string): string | null {
	if (filename === ":memory:") {
		return null;
	}
	if (filename.startsWith("/")) {
		return normalizeAbsolutePath(filename);
	}
	const cwd =
		(
			globalThis as typeof globalThis & {
				process?: { cwd?: () => string };
			}
		).process?.cwd?.() ?? "/";
	return normalizeAbsolutePath(`${cwd}/${filename}`);
}

function normalizeAbsolutePath(filename: string): string {
	const segments: string[] = [];
	for (const segment of filename.split("/")) {
		if (!segment || segment === ".") {
			continue;
		}
		if (segment === "..") {
			segments.pop();
			continue;
		}
		segments.push(segment);
	}
	return `/${segments.join("/")}`;
}

function doubleOpenError(filename: string): Error {
	return new Error(
		`createBetterSqlite3Backend() already has an open handle for ${filename}; close the existing Lix handle before opening this file again`,
	);
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}
