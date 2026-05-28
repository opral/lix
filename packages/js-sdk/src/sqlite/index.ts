import { createRequire } from "node:module";
import type { Database } from "better-sqlite3";
import type {
	BackendKvEntryPage,
	BackendKvBound,
	BackendKvGetRequest,
	BackendKvScanRange,
	BackendKvScanRequest,
	BackendKvValueBatch,
	BackendKvWriteBatch,
	BackendKvWriteStats,
	LixBackend,
	LixBackendReadTransaction,
	LixBackendWriteTransaction,
} from "../open-lix.js";

const SQLITE_FORMAT_VERSION = 1;
const require = createRequire(import.meta.url);

export type SqliteBackendOptions = {
	path: string;
	databaseOptions?: BetterSqlite3DatabaseOptions;
};

export type BetterSqlite3DatabaseOptions = {
	readonly?: boolean;
	fileMustExist?: boolean;
	timeout?: number;
	verbose?: (message?: unknown, ...additional: unknown[]) => void;
};

type BetterSqlite3Constructor = {
	new (filename: string, options?: BetterSqlite3DatabaseOptions): Database;
	(filename: string, options?: BetterSqlite3DatabaseOptions): Database;
};

const openFileHandles = new Set<string>();

function openDatabase(
	path: string,
	options: BetterSqlite3DatabaseOptions | undefined,
): Database {
	const DatabaseConstructor = require(
		"better-sqlite3",
	) as BetterSqlite3Constructor;
	return new DatabaseConstructor(path, options);
}

function configureConnection(db: Database): void {
	db.pragma("busy_timeout = 5000");
}

function initializeDatabase(db: Database): void {
	db.pragma("journal_mode = WAL");
	configureConnection(db);
	const userVersion = db.pragma("user_version", { simple: true }) as number;
	if (userVersion > SQLITE_FORMAT_VERSION) {
		throw new Error(
			`SQLite file format version ${userVersion} is newer than supported version ${SQLITE_FORMAT_VERSION}`,
		);
	}
	db.exec(`
		CREATE TABLE IF NOT EXISTS lix_entries (
			key BLOB NOT NULL,
			value BLOB NOT NULL,
			PRIMARY KEY (key)
		) WITHOUT ROWID
	`);
	db.pragma(`user_version = ${SQLITE_FORMAT_VERSION}`);
}

export class SqliteBackend implements LixBackend {
	readonly #db: Database;
	readonly #path: string;
	readonly #databaseOptions: BetterSqlite3DatabaseOptions | undefined;
	readonly #registryKey: string | null;
	#closed = false;

	constructor(options: SqliteBackendOptions) {
		const path = options.path;
		if (!path) {
			throw new Error("SqliteBackend requires a non-empty path");
		}
		const registryKey = registryKeyForPath(path);
		if (registryKey && openFileHandles.has(registryKey)) {
			throw doubleOpenError(path);
		}
		let db: Database | undefined;
		if (registryKey) {
			openFileHandles.add(registryKey);
		}
		try {
			db = openDatabase(path, options.databaseOptions);
			initializeDatabase(db);
			this.#db = db;
			this.#path = path;
			this.#databaseOptions = options.databaseOptions;
			this.#registryKey = registryKey;
		} catch (error) {
			if (registryKey) {
				openFileHandles.delete(registryKey);
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

	beginReadTransaction(): LixBackendReadTransaction {
		this.#ensureOpen();
		if (this.#registryKey) {
			const db = openDatabase(this.#path, {
				...this.#databaseOptions,
				readonly: true,
				fileMustExist: true,
			});
			try {
				configureConnection(db);
				db.exec("BEGIN");
				db.prepare("SELECT 1 FROM lix_entries LIMIT 1").all();
				return new SqliteTransaction(db, {
					ownsTransaction: true,
					writable: false,
					onClose: () => db.close(),
				});
			} catch (error) {
				db.close();
				throw error;
			}
		}
		return new SqliteTransaction(this.#db, {
			ownsTransaction: false,
			writable: false,
		});
	}

	beginWriteTransaction(): LixBackendWriteTransaction {
		this.#ensureOpen();
		if (this.#db.inTransaction) {
			throw new Error("SQLite Lix backend write transaction already active");
		}
		this.#db.exec("BEGIN IMMEDIATE");
		return new SqliteTransaction(this.#db, {
			ownsTransaction: true,
			writable: true,
		});
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
			throw new Error("SQLite Lix backend is closed");
		}
	}
}

class SqliteTransaction implements LixBackendWriteTransaction {
	readonly #db: Database;
	readonly #ownsTransaction: boolean;
	readonly #writable: boolean;
	readonly #onClose: (() => void) | undefined;
	#closed = false;

	constructor(
		db: Database,
		options: {
			ownsTransaction: boolean;
			writable: boolean;
			onClose?: () => void;
		},
	) {
		this.#db = db;
		this.#ownsTransaction = options.ownsTransaction;
		this.#writable = options.writable;
		this.#onClose = options.onClose;
	}

	getValues(request: BackendKvGetRequest): BackendKvValueBatch {
		this.#ensureOpen();
		return getValues(this.#db, request);
	}

	scanEntries(request: BackendKvScanRequest): BackendKvEntryPage {
		this.#ensureOpen();
		const { pairs, resumeAfter } = scanPage(this.#db, request);
		return {
			keys: pairs.map(({ key }) => key),
			values: pairs.map(({ value }) => value),
			resumeAfter,
		};
	}

	writeKvBatch(batch: BackendKvWriteBatch): BackendKvWriteStats {
		this.#ensureOpen();
		if (!this.#writable) {
			throw new Error("Lix backend transaction is read-only");
		}
		const stats: BackendKvWriteStats = {
			puts: 0,
			deletes: 0,
			deleteRanges: 0,
			bytesWritten: 0,
		};
		for (const op of batch.ops) {
			if (op.kind === "put") {
				stats.puts += 1;
				stats.bytesWritten += op.key.length + op.value.length;
				kvPut(this.#db, op.key, op.value);
			} else if (op.kind === "delete") {
				stats.deletes += 1;
				stats.bytesWritten += op.key.length;
				kvDelete(this.#db, op.key);
			} else {
				stats.deleteRanges += 1;
				stats.bytesWritten += deleteRangeBytes(op.range);
				kvDeleteRange(this.#db, op.range);
			}
		}
		return stats;
	}

	commit(): void {
		this.#ensureOpen();
		try {
			if (this.#ownsTransaction) {
				this.#db.exec("COMMIT");
			}
		} finally {
			this.#closed = true;
			this.#onClose?.();
		}
	}

	rollback(): void {
		this.#ensureOpen();
		try {
			if (this.#ownsTransaction) {
				this.#db.exec("ROLLBACK");
			}
		} finally {
			this.#closed = true;
			this.#onClose?.();
		}
	}

	#ensureOpen(): void {
		if (this.#closed) {
			throw new Error("Lix backend transaction is closed");
		}
	}
}

type KvPair = {
	key: Uint8Array;
	value: Uint8Array;
};

function getValues(
	db: Database,
	request: BackendKvGetRequest,
): BackendKvValueBatch {
	return {
		values: request.keys.map((key) => kvGet(db, key)),
	};
}

function scanPage(
	db: Database,
	request: BackendKvScanRequest,
): { pairs: KvPair[]; resumeAfter: Uint8Array | null } {
	const range = request.after
		? rangeAfter(request.range, request.after)
		: request.range;
	const pairs = kvScan(db, range, request.limit + 1);
	const hasMore = pairs.length > request.limit;
	const pagePairs = pairs.slice(0, request.limit);
	return {
		pairs: pagePairs,
		resumeAfter: hasMore ? (pagePairs.at(-1)?.key ?? null) : null,
	};
}

function rangeAfter(
	range: BackendKvScanRange,
	after: Uint8Array,
): BackendKvScanRange {
	return {
		...range,
		lower: laterLowerBound(range.lower, after),
	};
}

function laterLowerBound(
	lower: BackendKvBound,
	after: Uint8Array,
): BackendKvBound {
	if (lower.kind === "unbounded") {
		return { kind: "excluded", key: after };
	}
	const comparison = compareBytes(lower.key, after);
	if (comparison > 0) {
		return lower;
	}
	return { kind: "excluded", key: after };
}

function kvGet(db: Database, key: Uint8Array): Uint8Array | null {
	const row = db
		.prepare("SELECT value FROM lix_entries WHERE key = ?")
		.get(sqliteBytes(key));
	if (!isObject(row) || !("value" in row)) {
		return null;
	}
	return bytesFromUnknown(row.value, "lix_entries.value");
}

function kvPut(
	db: Database,
	key: Uint8Array,
	value: Uint8Array,
): void {
	db.prepare(
		`INSERT INTO lix_entries (key, value)
		 VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
	).run(sqliteBytes(key), sqliteBytes(value));
}

function kvDelete(db: Database, key: Uint8Array): void {
	db.prepare("DELETE FROM lix_entries WHERE key = ?").run(sqliteBytes(key));
}

function kvDeleteRange(db: Database, range: BackendKvScanRange): void {
	const { clauses, params } = rangeClauses(range);
	db.prepare(`DELETE FROM lix_entries WHERE ${clauses.join(" AND ")}`).run(
		...params,
	);
}

function kvScan(
	db: Database,
	range: BackendKvScanRange,
	limit?: number | null,
): KvPair[] {
	const { sql, params } = scanQuery(range, limit);
	return db
		.prepare(sql)
		.all(...params)
		.map((row) => {
			if (!isObject(row) || !("key" in row) || !("value" in row)) {
				throw new Error("invalid lix_entries scan row");
			}
			const key = bytesFromUnknown(row.key, "lix_entries.key");
			return {
				key,
				value: bytesFromUnknown(row.value, "lix_entries.value"),
			};
		});
}

function scanQuery(
	range: BackendKvScanRange,
	limit?: number | null,
): { sql: string; params: unknown[] } {
	const { clauses, params } = rangeClauses(range);
	let sql = `SELECT key, value FROM lix_entries WHERE ${clauses.join(
		" AND ",
	)} ORDER BY key`;
	if (limit != null) {
		sql += " LIMIT ?";
		params.push(limit);
	}
	return { sql, params };
}

function rangeClauses(
	range: BackendKvScanRange,
): { clauses: string[]; params: unknown[] } {
	const params: unknown[] = [];
	const clauses: string[] = [];
	appendBoundClause(clauses, params, range.lower, "key", ">=", ">");
	appendBoundClause(clauses, params, range.upper, "key", "<=", "<");
	if (clauses.length === 0) {
		clauses.push("1 = 1");
	}
	return { clauses, params };
}

function appendBoundClause(
	clauses: string[],
	params: unknown[],
	bound: BackendKvBound,
	column: string,
	includedOp: string,
	excludedOp: string,
): void {
	if (bound.kind === "unbounded") {
		return;
	}
	clauses.push(`${column} ${bound.kind === "included" ? includedOp : excludedOp} ?`);
	params.push(sqliteBytes(bound.key));
}

function deleteRangeBytes(range: BackendKvScanRange): number {
	return boundBytes(range.lower) + boundBytes(range.upper);
}

function boundBytes(bound: BackendKvBound): number {
	return bound.kind === "unbounded" ? 0 : bound.key.length;
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
	const length = Math.min(left.length, right.length);
	for (let index = 0; index < length; index++) {
		const delta = left[index]! - right[index]!;
		if (delta !== 0) return delta;
	}
	return left.length - right.length;
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
		`SqliteBackend already has an open handle for ${filename}; close the existing Lix handle before opening this file again`,
	);
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}
