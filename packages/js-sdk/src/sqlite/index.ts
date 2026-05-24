import DatabaseConstructor, { type Database } from "better-sqlite3";
import type {
	BackendKvEntryPage,
	BackendKvExistsBatch,
	BackendKvGetRequest,
	BackendKvKeyPage,
	BackendKvScanRange,
	BackendKvScanRequest,
	BackendKvValueBatch,
	BackendKvValuePage,
	BackendKvWriteBatch,
	BackendKvWriteStats,
	LixBackend,
	LixBackendReadTransaction,
	LixBackendWriteTransaction,
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
	#transactionMode: "read" | "write" | null = null;
	#closed = false;

	constructor(db: Database, registryKey: string | null) {
		this.#db = db;
		this.#registryKey = registryKey;
	}

	beginReadTransaction(): LixBackendReadTransaction {
		this.#ensureOpen();
		return new BetterSqlite3Transaction(this.#db, {
			ownsTransaction: false,
			writable: false,
		});
	}

	beginWriteTransaction(): LixBackendWriteTransaction {
		this.#ensureOpen();
		if (this.#db.inTransaction) {
			return new BetterSqlite3Transaction(this.#db, {
				ownsTransaction: false,
				writable: true,
			});
		}
		this.#db.exec("BEGIN IMMEDIATE");
		this.#transactionMode = "write";
		return new BetterSqlite3Transaction(this.#db, {
			ownsTransaction: true,
			writable: true,
			onClose: () => {
				this.#transactionMode = null;
			},
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
			throw new Error("better-sqlite3 Lix backend is closed");
		}
	}
}

class BetterSqlite3Transaction implements LixBackendWriteTransaction {
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

	existsMany(request: BackendKvGetRequest): BackendKvExistsBatch {
		this.#ensureOpen();
		return existsMany(this.#db, request);
	}

	scanKeys(request: BackendKvScanRequest): BackendKvKeyPage {
		this.#ensureOpen();
		const { pairs, resumeAfter } = scanPage(this.#db, request);
		return {
			keys: pairs.map(({ key }) => key),
			resumeAfter,
		};
	}

	scanValues(request: BackendKvScanRequest): BackendKvValuePage {
		this.#ensureOpen();
		const { pairs, resumeAfter } = scanPage(this.#db, request);
		return {
			values: pairs.map(({ value }) => value),
			resumeAfter,
		};
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
		for (const group of batch.groups) {
			for (const op of group.ops) {
				if (op.kind === "put") {
					stats.puts += 1;
					stats.bytesWritten += op.key.length + op.value.length;
					kvPut(this.#db, group.namespace, op.key, op.value);
				} else if (op.kind === "delete") {
					stats.deletes += 1;
					stats.bytesWritten += op.key.length;
					kvDelete(this.#db, group.namespace, op.key);
				} else {
					stats.deleteRanges += 1;
					stats.bytesWritten += deleteRangeBytes(op.range);
					kvDeleteRange(this.#db, group.namespace, op.range);
				}
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
		groups: request.groups.map((group) => ({
			namespace: group.namespace,
			values: group.keys.map((key) => kvGet(db, group.namespace, key)),
		})),
	};
}

function existsMany(
	db: Database,
	request: BackendKvGetRequest,
): BackendKvExistsBatch {
	return {
		groups: request.groups.map((group) => ({
			namespace: group.namespace,
			exists: group.keys.map((key) => kvGet(db, group.namespace, key) !== null),
		})),
	};
}

function scanPage(
	db: Database,
	request: BackendKvScanRequest,
): { pairs: KvPair[]; resumeAfter: Uint8Array | null } {
	const scanLimit = request.limit + 1 + (request.after ? 1 : 0);
	const pairs = kvScan(db, request.namespace, request.range, scanLimit).filter(
		(pair) => !request.after || compareBytes(pair.key, request.after) > 0,
	);
	const hasMore = pairs.length > request.limit;
	const pagePairs = pairs.slice(0, request.limit);
	return {
		pairs: pagePairs,
		resumeAfter: hasMore ? (pagePairs.at(-1)?.key ?? null) : null,
	};
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

function kvPut(
	db: Database,
	namespace: string,
	key: Uint8Array,
	value: Uint8Array,
): void {
	db.prepare(
		`INSERT INTO lix_kv (namespace, key, value)
		 VALUES (?, ?, ?)
		 ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value`,
	).run(namespace, sqliteBytes(key), sqliteBytes(value));
}

function kvDelete(db: Database, namespace: string, key: Uint8Array): void {
	db.prepare("DELETE FROM lix_kv WHERE namespace = ? AND key = ?").run(
		namespace,
		sqliteBytes(key),
	);
}

function kvDeleteRange(
	db: Database,
	namespace: string,
	range: BackendKvScanRange,
): void {
	const { clauses, params } = rangeClauses(namespace, range);
	db.prepare(`DELETE FROM lix_kv WHERE ${clauses.join(" AND ")}`).run(
		...params,
	);
}

function kvScan(
	db: Database,
	namespace: string,
	range: BackendKvScanRange,
	limit?: number | null,
): KvPair[] {
	const { sql, params } = scanQuery(namespace, range, limit);
	return db
		.prepare(sql)
		.all(...params)
		.map((row) => {
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
	range: BackendKvScanRange,
	limit?: number | null,
): { sql: string; params: unknown[] } {
	const { clauses, params } = rangeClauses(namespace, range);
	let sql = `SELECT key, value FROM lix_kv WHERE ${clauses.join(
		" AND ",
	)} ORDER BY key`;
	if (limit != null) {
		sql += " LIMIT ?";
		params.push(limit);
	}
	return { sql, params };
}

function rangeClauses(
	namespace: string,
	range: BackendKvScanRange,
): { clauses: string[]; params: unknown[] } {
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

	return { clauses, params };
}

function deleteRangeBytes(range: BackendKvScanRange): number {
	if (range.kind === "prefix") {
		return range.prefix.length;
	}
	return range.start.length + range.end.length;
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
	const length = Math.min(left.length, right.length);
	for (let index = 0; index < length; index++) {
		const delta = left[index]! - right[index]!;
		if (delta !== 0) return delta;
	}
	return left.length - right.length;
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
