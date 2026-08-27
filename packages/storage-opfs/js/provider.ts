import type {
	Database,
	OpfsSAHPoolDatabase,
	SAHPoolUtil,
} from "@sqlite.org/sqlite-wasm";
import type {
	LixStorageBound,
	LixStorageChangeWatch,
	LixStorageError,
	LixStorageErrorCode,
	LixStorageGetManyRequest,
	LixStorageKeyRange,
	LixStoragePrecondition,
	LixStorageProjectedValue,
	LixStorageProvider,
	LixStorageRead,
	LixStorageReadOptions,
	LixStorageScanOrder,
	LixStorageScanSource,
	LixStorageSpace,
	LixStorageWrite,
	LixStorageWriteOptions,
} from "@lix-js/sdk";
import sqliteWasmUrl from "@sqlite.org/sqlite-wasm/sqlite3.wasm";
import {
	BufferedOpfsWrite,
	bytesEqual,
	immutableValueError,
	type OpfsWritePayload,
} from "./buffered-write.js";
import { StorageChangeNotifier } from "./change-watch.js";
import { restoreSynchronousModeBestEffort } from "./sqlite-cleanup.js";
import {
	configureSqliteOpfsDurability,
	fenceSqliteOpfsDurability,
} from "./sqlite-durability.js";
import { OPFS_RPC_PROTOCOL_VERSION } from "./rpc.js";

type SqliteValue =
	| string
	| Uint8Array
	| Int8Array
	| ArrayBuffer
	| number
	| bigint
	| null;

type SqliteInit = Awaited<
	ReturnType<typeof import("@sqlite.org/sqlite-wasm").default>
>;

type LockManager = {
	request<T>(
		name: string,
		options: { ifAvailable: boolean; mode: "exclusive" },
		callback: (lock: object | null) => Promise<T>,
	): Promise<T>;
};

type BrowserNavigator = {
	storage?: { getDirectory(): Promise<unknown> };
	locks?: LockManager;
};

const SQLITE_VFS_NAME_PREFIX = "lix-opfs-sahpool-";
const OPFS_LOCK_PREFIX = "lix:opfs-sqlite:";
const OPFS_PROTOCOL_LOCK_PREFIX =
	`lix:opfs-owner:rpc-v${OPFS_RPC_PROTOCOL_VERSION}:`;
const SQLITE_VFS_DIRECTORY = "/lix/sqlite-sahpool";
// Keep point reads comfortably below SQLite's conservative 999-variable
// ceiling while collapsing hundreds of JS/Wasm bind-step-reset crossings into
// a handful of indexed joins.
const READ_MANY_KEYS_PER_QUERY = 300;
const SQLITE_SCHEMA = `
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA auto_vacuum = INCREMENTAL;
CREATE TABLE IF NOT EXISTS lix_entries (
  space INTEGER NOT NULL,
  key BLOB NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY (space, key)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS lix_storage_metadata (
  key TEXT NOT NULL PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;
`;

const STORAGE_SESSION_METADATA_KEY = "session-token";

let sqliteModule: Promise<SqliteInit> | undefined;
const pools = new Map<string, Promise<SAHPoolUtil>>();

function mintSessionToken(): string {
	const words = crypto.getRandomValues(new Uint32Array(2));
	return ((BigInt(words[0]!) << 32n) | BigInt(words[1]!)).toString(10);
}

/** SQLite Wasm + OPFS implementation of the Rust-shaped storage protocol. */
export class OpfsBackend implements LixStorageProvider {
	readonly #database: OpfsSAHPoolDatabase;
	readonly #pool: SAHPoolUtil;
	readonly #releaseLock: () => Promise<void>;
	readonly #changes = new StorageChangeNotifier();
	#generation = 0;
	#sessionToken: string | undefined;
	#closed = false;

	private constructor(
		database: OpfsSAHPoolDatabase,
		pool: SAHPoolUtil,
		releaseLock: () => Promise<void>,
		sessionToken: string | undefined,
	) {
		this.#database = database;
		this.#pool = pool;
		this.#releaseLock = releaseLock;
		this.#sessionToken = sessionToken;
	}

	static async open(
		name: string,
		onOwnershipAcquired?: () => void,
	): Promise<OpfsBackend> {
		const releaseLock = await acquireOpfsLock(name);
		onOwnershipAcquired?.();
		let pool: SAHPoolUtil | undefined;
		let database: OpfsSAHPoolDatabase | undefined;
		try {
			const navigatorValue = getBrowserNavigator();
			if (!navigatorValue.storage?.getDirectory) {
				throw new Error(
					"This browser does not expose the Origin Private File System",
				);
			}
			const sqlite3 = await initializeSqlite();
			pool = await getPool(sqlite3, name);
			database = new pool.OpfsSAHPoolDb("/repository.sqlite3");
			configureSqliteOpfsDurability(database);
			database.exec(SQLITE_SCHEMA);
			const sessionToken = database.selectValue(
				"SELECT value FROM lix_storage_metadata WHERE key = ?",
				[STORAGE_SESSION_METADATA_KEY],
			) as string | undefined;
			return new OpfsBackend(database, pool, releaseLock, sessionToken);
		} catch (error) {
			try {
				database?.close();
				if (pool && !pool.isPaused()) pool.pauseVfs();
			} catch {
				// Preserve the original open/schema error.
			}
			await releaseLock();
			throw error;
		}
	}

	async acquireSession(): Promise<string> {
		this.#assertOpen();
		if (this.#sessionToken === undefined) {
			const token = mintSessionToken();
			this.#database.exec({
				sql: "INSERT INTO lix_storage_metadata(key, value) VALUES (?, ?)",
				bind: [STORAGE_SESSION_METADATA_KEY, token],
			});
			this.#sessionToken = token;
		}
		return this.#sessionToken;
	}

	async beginRead(options: LixStorageReadOptions): Promise<LixStorageRead> {
		this.#assertOpen();
		this.assertSession(options.sessionToken);
		if (options.durability === "durable") {
			fenceSqliteOpfsDurability(this.#database);
		}
		return new OpfsRead(this, this.#generation, options.sessionToken);
	}

	async beginWrite(options: LixStorageWriteOptions): Promise<LixStorageWrite> {
		this.#assertOpen();
		this.assertSession(options.sessionToken);
		return new BufferedOpfsWrite(options, (payload) => {
			this.commitChanges(payload);
			return { stats: payload.stats };
		});
	}

	async watchForChanges(): Promise<LixStorageChangeWatch> {
		this.#assertOpen();
		return this.#changes.watch();
	}

	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		this.#changes.close(
			storageError("LIX_STORAGE_CLOSED", "SQLite OPFS storage is closed"),
		);
		try {
			this.#database.close();
			if (!this.#pool.isPaused()) this.#pool.pauseVfs();
		} finally {
			await this.#releaseLock();
		}
	}

	currentGeneration(): number {
		this.#assertOpen();
		return this.#generation;
	}

	readMany(
		requests: LixStorageGetManyRequest[],
		generation: number,
		sessionToken?: string,
	): Array<LixStorageProjectedValue | null> {
		this.assertSession(sessionToken);
		this.#assertGeneration(generation);
		const entries = requests.flatMap((request) =>
			request.keys.map((key) => ({
				spaceId: request.space.id,
				key,
				projection: request.options.projection,
			})),
		);
		const values: Array<LixStorageProjectedValue | null> = [];
		for (
			let offset = 0;
			offset < entries.length;
			offset += READ_MANY_KEYS_PER_QUERY
		) {
			const chunk = entries.slice(offset, offset + READ_MANY_KEYS_PER_QUERY);
			const requestedRows = chunk
				.map((_, index) => `(${index}, ?, ?, ?)`)
				.join(", ");
			const bindings: SqliteValue[] = [];
			for (const entry of chunk) {
				bindings.push(
					entry.spaceId,
					entry.key,
					entry.projection === "fullValue" ? 1 : 0,
				);
			}
			const rows: SqliteValue[][] = [];
			this.#database.exec({
				sql: `WITH requested(ordinal, space, key, wants_value) AS (
					VALUES ${requestedRows}
				)
				SELECT entries.value IS NOT NULL,
					CASE WHEN requested.wants_value = 1 THEN entries.value END
				FROM requested
				LEFT JOIN lix_entries AS entries
					ON entries.space = requested.space AND entries.key = requested.key
				ORDER BY requested.ordinal`,
				bind: bindings,
				rowMode: "array",
				resultRows: rows,
			});
			for (let index = 0; index < rows.length; index += 1) {
				const row = rows[index]!;
				const entry = chunk[index]!;
				if (row[0] !== 1) {
					values.push(null);
				} else if (entry.projection === "keyOnly") {
					values.push({ kind: "keyOnly" });
				} else {
					values.push({ kind: "fullValue", value: copyBlob(row[1]) });
				}
			}
		}
		this.#assertGeneration(generation);
		this.assertSession(sessionToken);
		return values;
	}

	scanPage(request: {
		space: LixStorageSpace;
		range: LixStorageKeyRange;
		after?: Uint8Array;
		limit: number;
		order: LixStorageScanOrder;
		projection: "keyOnly" | "fullValue";
		generation: number;
		sessionToken?: string;
	}): {
		entries: Array<{ key: Uint8Array; value: LixStorageProjectedValue }>;
		hasMore: boolean;
	} {
		this.assertSession(request.sessionToken);
		this.#assertGeneration(request.generation);
		const predicates = ["space = ?"];
		const bindings: SqliteValue[] = [request.space.id];
		appendBound(predicates, bindings, "key", request.range.lower, ">=", ">");
		appendBound(predicates, bindings, "key", request.range.upper, "<=", "<");
		if (request.after) {
			predicates.push(`key ${request.order === "ascending" ? ">" : "<"} ?`);
			bindings.push(request.after);
		}
		const direction = request.order === "ascending" ? "ASC" : "DESC";
		const limit = Math.max(0, Math.min(10_000, request.limit));
		const rows: SqliteValue[][] = [];
		this.#database.exec({
			sql: `SELECT key, value FROM lix_entries WHERE ${predicates.join(
				" AND ",
			)} ORDER BY key ${direction} LIMIT ?`,
			bind: [...bindings, limit + 1],
			rowMode: "array",
			resultRows: rows,
		});
		const hasMore = rows.length > limit;
		const entries = rows.slice(0, limit).map(([key, value]) => ({
			key: copyBlob(key),
			value:
				request.projection === "keyOnly"
					? ({ kind: "keyOnly" } as const)
					: ({ kind: "fullValue", value: copyBlob(value) } as const),
		}));
		this.#assertGeneration(request.generation);
		this.assertSession(request.sessionToken);
		return { entries, hasMore };
	}

	commitChanges(changes: OpfsWritePayload): void {
		this.#assertOpen();
		this.assertSession(changes.sessionToken);
		const previousSynchronous = this.#database.selectValue(
			"PRAGMA synchronous",
		) as number;
		try {
			if (changes.strictDurability) {
				this.#database.exec("PRAGMA synchronous = FULL");
			}
			this.#database.exec("BEGIN IMMEDIATE");
			const failures = this.#findPreconditionFailures(changes.preconditions);
			if (failures.length > 0) {
				throw storageError(
					"LIX_STORAGE_PRECONDITION_FAILED",
					"storage precondition failed",
					{ failures: failures.map((index) => ({ index })) },
				);
			}
			for (const immutable of changes.immutablePuts) {
				const existing = this.#database.selectValue(
					"SELECT value FROM lix_entries WHERE space = ? AND key = ?",
					[immutable.space.id, immutable.key],
				) as SqliteValue | undefined;
				if (
					existing !== undefined &&
					!bytesEqual(copyBlob(existing), immutable.value)
				) {
					throw immutableValueError();
				}
			}
			for (const range of changes.deleteRanges) {
				const { sql, bindings } = deleteRangeSql(range);
				this.#database.exec({ sql, bind: bindings });
			}
			if (changes.deletes.length > 0) {
				const statement = this.#database.prepare(
					"DELETE FROM lix_entries WHERE space = ? AND key = ?",
				);
				try {
					for (const entry of changes.deletes) {
						statement.bind([entry.space.id, entry.key]);
						statement.step();
						statement.reset(true);
					}
				} finally {
					statement.finalize();
				}
			}
			if (changes.puts.length > 0) {
				const statement = this.#database.prepare(
					`INSERT INTO lix_entries(space, key, value) VALUES (?, ?, ?)
           ON CONFLICT(space, key) DO UPDATE SET value = excluded.value`,
				);
				try {
					for (const entry of changes.puts) {
						statement.bind([entry.space.id, entry.key, entry.value]);
						statement.step();
						statement.reset(true);
					}
				} finally {
					statement.finalize();
				}
			}
			this.#database.exec("COMMIT");
			this.#generation += 1;
			this.#changes.notify();
		} catch (error) {
			try {
				this.#database.exec("ROLLBACK");
			} catch {
				// Preserve the original transaction error.
			}
			throw error;
		} finally {
			if (changes.strictDurability) {
				restoreSynchronousModeBestEffort(
					this.#database,
					previousSynchronous,
				);
			}
		}
	}

	#findPreconditionFailures(preconditions: LixStoragePrecondition[]): number[] {
		return preconditions.flatMap((precondition, index) =>
			preconditionMatches(this.#database, precondition) ? [] : [index],
		);
	}

	#assertOpen(): void {
		if (this.#closed) {
			throw storageError(
				"LIX_STORAGE_CLOSED",
				"SQLite OPFS storage is closed",
			);
		}
	}

	assertSession(sessionToken: string | undefined): void {
		this.#assertOpen();
		if (this.#sessionToken !== sessionToken) {
			throw storageError(
				"LIX_STORAGE_FENCED",
				"storage operation does not belong to the active session",
			);
		}
	}

	#assertGeneration(generation: number): void {
		this.#assertOpen();
		if (generation !== this.#generation) {
			throw storageError(
				"LIX_STORAGE_READ_EXPIRED",
				"read transaction is no longer valid",
			);
		}
	}
}

/** Worker entry point loaded by `@lix-js/sdk`. */
export async function createLixStorageProvider(
	options: unknown,
): Promise<LixStorageProvider> {
	if (
		!options ||
		typeof options !== "object" ||
		!("name" in options) ||
		typeof options.name !== "string" ||
		options.name.length === 0
	) {
		throw new TypeError("OPFS storage provider requires a non-empty name");
	}
	return OpfsBackend.open(options.name);
}

class OpfsRead implements LixStorageRead {
	readonly #backend: OpfsBackend;
	readonly #generation: number;
	readonly #sessionToken: string | undefined;

	constructor(
		backend: OpfsBackend,
		generation: number,
		sessionToken: string | undefined,
	) {
		this.#backend = backend;
		this.#generation = generation;
		this.#sessionToken = sessionToken;
	}

	snapshotCacheKey(): string {
		return this.#generation.toString();
	}

	async getMany(
		requests: LixStorageGetManyRequest[],
	): Promise<Array<LixStorageProjectedValue | null>> {
		return this.#backend.readMany(
			requests,
			this.#generation,
			this.#sessionToken,
		);
	}

	async beginScan(
		space: LixStorageSpace,
		range: LixStorageKeyRange,
		options: {
			projection: "keyOnly" | "fullValue";
			order: LixStorageScanOrder;
		},
	): Promise<LixStorageScanSource> {
		return new OpfsScan(
			this.#backend,
			this.#generation,
			this.#sessionToken,
			space,
			range,
			options,
		);
	}
}

class OpfsScan implements LixStorageScanSource {
	#after: Uint8Array | undefined;

	constructor(
		private readonly backend: OpfsBackend,
		private readonly generation: number,
		private readonly sessionToken: string | undefined,
		private readonly space: LixStorageSpace,
		private readonly range: LixStorageKeyRange,
		private readonly options: {
			projection: "keyOnly" | "fullValue";
			order: LixStorageScanOrder;
		},
	) {}

	async nextPage(limitRows: number) {
		const page = this.backend.scanPage({
			space: this.space,
			range: this.range,
			after: this.#after,
			limit: limitRows,
			order: this.options.order,
			projection: this.options.projection,
			generation: this.generation,
			sessionToken: this.sessionToken,
		});
		this.#after = page.entries.at(-1)?.key;
		return page;
	}
}

async function initializeSqlite(): Promise<SqliteInit> {
	if (!sqliteModule) {
		const { default: sqlite3InitModule } = await import(
			"@sqlite.org/sqlite-wasm"
		);
		sqliteModule = (
			sqlite3InitModule as unknown as (options: {
				instantiateWasm(
					imports: WebAssembly.Imports,
					onSuccess: (
						instance: WebAssembly.Instance,
						module: WebAssembly.Module,
					) => void,
				): object;
			}) => Promise<SqliteInit>
		)({
			instantiateWasm(imports, onSuccess) {
				void WebAssembly.compile(decodeDataUrl(sqliteWasmUrl)).then(
					async (module) =>
						onSuccess(await WebAssembly.instantiate(module, imports), module),
				);
				return {};
			},
		});
	}
	return sqliteModule;
}

function decodeDataUrl(dataUrl: string): Uint8Array<ArrayBuffer> {
	const marker = ";base64,";
	const offset = dataUrl.indexOf(marker);
	if (offset === -1) throw new Error("SQLite Wasm was not bundled as base64");
	const binary = atob(dataUrl.slice(offset + marker.length));
	const bytes = new Uint8Array(binary.length);
	for (let index = 0; index < binary.length; index += 1) {
		bytes[index] = binary.charCodeAt(index);
	}
	return bytes;
}

async function getPool(
	sqlite3: SqliteInit,
	storageName: string,
): Promise<SAHPoolUtil> {
	const vfsName = `${SQLITE_VFS_NAME_PREFIX}${await hashName(storageName)}`;
	let pool = pools.get(vfsName);
	if (!pool) {
		pool = sqlite3.installOpfsSAHPoolVfs({
			name: vfsName,
			directory: `${SQLITE_VFS_DIRECTORY}/${fileName(storageName)}`,
			initialCapacity: 16,
		});
		pools.set(vfsName, pool);
	}
	const resolved = await pool;
	if (resolved.isPaused()) await resolved.unpauseVfs();
	return resolved;
}

async function acquireOpfsLock(name: string): Promise<() => Promise<void>> {
	const locks = getBrowserNavigator().locks;
	if (!locks) {
		throw new Error("OPFS storage requires Web Locks for safe ownership");
	}
	const releaseElection = await acquireExclusiveLock(
		locks,
		`${OPFS_PROTOCOL_LOCK_PREFIX}${name}`,
		storageError(
			"LIX_STORAGE_FENCED",
			`OPFS storage '${name}' has a compatible owner`,
		),
	);
	let releaseData: (() => Promise<void>) | undefined;
	try {
		releaseData = await acquireExclusiveLock(
			locks,
			`${OPFS_LOCK_PREFIX}${name}`,
			storageError(
				"LIX_STORAGE_UNSUPPORTED",
				`OPFS storage '${name}' is owned by an incompatible worker; close older tabs and retry`,
			),
		);
	} catch (error) {
		await releaseElection();
		throw error;
	}
	return async () => {
		await releaseData?.();
		await releaseElection();
	};
}

/**
 * The election lock is versioned; the data lock deliberately is not. Acquiring
 * election first makes a failed data-lock acquisition authoritative evidence
 * of an incompatible owner, while compatible workers remain silent relays.
 */
function acquireExclusiveLock(
	locks: LockManager,
	lockName: string,
	unavailableError: Error,
): Promise<() => Promise<void>> {
	let signalRelease!: () => void;
	const released = new Promise<void>((resolve) => {
		signalRelease = resolve;
	});
	let resolveAcquired!: (release: () => Promise<void>) => void;
	let rejectAcquired!: (error: unknown) => void;
	const acquired = new Promise<() => Promise<void>>((resolve, reject) => {
		resolveAcquired = resolve;
		rejectAcquired = reject;
	});
	let requestFinished!: Promise<void>;
	requestFinished = locks
			.request(
				lockName,
				{ ifAvailable: true, mode: "exclusive" },
				async (lock) => {
					if (!lock) {
						rejectAcquired(unavailableError);
						return;
					}
					resolveAcquired(async () => {
						signalRelease();
						await requestFinished;
					});
					await released;
				},
			)
			.catch(rejectAcquired);
	return acquired;
}

function getBrowserNavigator(): BrowserNavigator {
	const value = (globalThis as unknown as { navigator?: unknown }).navigator;
	if (!value || typeof value !== "object") {
		throw new Error("OPFS storage requires a browser navigator");
	}
	return value as BrowserNavigator;
}

function fileName(name: string): string {
	const bytes = new TextEncoder().encode(name);
	let encoded = "";
	for (const byte of bytes) encoded += String.fromCharCode(byte);
	return btoa(encoded)
		.replaceAll("+", "-")
		.replaceAll("/", "_")
		.replace(/=+$/u, "");
}

async function hashName(name: string): Promise<string> {
	const bytes = await crypto.subtle.digest(
		"SHA-256",
		new TextEncoder().encode(name),
	);
	return Array.from(new Uint8Array(bytes), (byte) =>
		byte.toString(16).padStart(2, "0"),
	).join("");
}

function appendBound(
	predicates: string[],
	bindings: SqliteValue[],
	column: string,
	bound: LixStorageBound,
	includedOperator: string,
	excludedOperator: string,
): void {
	if (bound.kind === "unbounded") return;
	predicates.push(
		`${column} ${bound.kind === "included" ? includedOperator : excludedOperator} ?`,
	);
	bindings.push(bound.key);
}

function deleteRangeSql(
	range: OpfsWritePayload["deleteRanges"][number],
): {
	sql: string;
	bindings: SqliteValue[];
} {
	const predicates = ["space = ?"];
	const bindings: SqliteValue[] = [range.space.id];
	appendBound(predicates, bindings, "key", range.range.lower, ">=", ">");
	appendBound(predicates, bindings, "key", range.range.upper, "<=", "<");
	return {
		sql: `DELETE FROM lix_entries WHERE ${predicates.join(" AND ")}`,
		bindings,
	};
}

function preconditionMatches(
	database: Database,
	precondition: LixStoragePrecondition,
): boolean {
	switch (precondition.kind) {
		case "keyAbsent":
			return !hasKey(database, precondition.space.id, precondition.key);
		case "keyPresent":
			return hasKey(database, precondition.space.id, precondition.key);
		case "keyValueEquals": {
			const value = selectValue(
				database,
				precondition.space.id,
				precondition.key,
			);
			return value !== undefined && bytesEqual(value, precondition.expected);
		}
		case "keyValueHashEquals":
			// The current Lix browser path does not emit this precondition. Fail
			// closed until the SQLite package provides the engine's BLAKE3 hash.
			return false;
		case "rangeEmpty": {
			const predicates = ["space = ?"];
			const bindings: SqliteValue[] = [precondition.space.id];
			appendBound(
				predicates,
				bindings,
				"key",
				precondition.range.lower,
				">=",
				">",
			);
			appendBound(
				predicates,
				bindings,
				"key",
				precondition.range.upper,
				"<=",
				"<",
			);
			return (
				(database.selectValue(
					`SELECT 1 FROM lix_entries WHERE ${predicates.join(" AND ")} LIMIT 1`,
					bindings,
				) as SqliteValue | undefined) === undefined
			);
		}
		case "branchEquals": {
			if (precondition.refKey.byteLength < 4) return false;
			const value = selectValue(
				database,
				spaceFromPhysicalKey(precondition.refKey),
				precondition.refKey.slice(4),
			);
			return value !== undefined && bytesEqual(value, precondition.expected);
		}
	}
}

function hasKey(
	database: Database,
	space: number,
	key: Uint8Array,
): boolean {
	return (
		(database.selectValue(
			"SELECT 1 FROM lix_entries WHERE space = ? AND key = ? LIMIT 1",
			[space, key],
		) as SqliteValue | undefined) !== undefined
	);
}

function selectValue(
	database: Database,
	space: number,
	key: Uint8Array,
): Uint8Array | undefined {
	const value = database.selectValue(
		"SELECT value FROM lix_entries WHERE space = ? AND key = ?",
		[space, key],
	) as SqliteValue | undefined;
	return value === undefined ? undefined : copyBlob(value);
}

function copyBlob(value: SqliteValue): Uint8Array {
	if (value instanceof Uint8Array) return new Uint8Array(value);
	if (value instanceof Int8Array) return new Uint8Array(value.buffer.slice(0));
	if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0));
	throw new Error("SQLite OPFS value is not a BLOB");
}

function spaceFromPhysicalKey(key: Uint8Array): number {
	return new DataView(key.buffer, key.byteOffset, key.byteLength).getUint32(
		0,
		false,
	);
}

function storageError(
	code: LixStorageErrorCode,
	message: string,
	details?: unknown,
): LixStorageError {
	const error = new Error(message) as LixStorageError;
	error.name = "LixStorageError";
	Object.assign(error, { code, details });
	return error;
}
