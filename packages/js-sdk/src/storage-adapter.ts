/** Opaque package-level storage selection accepted by `openLix()`. */
export type LixStorage = {
	readonly lixStorage: object;
};

/**
 * Worker-loadable registration emitted by JavaScript storage packages.
 * `moduleUrl` must export `createLixStorageProvider(options)` and is loaded in
 * the same dedicated worker as the Lix Wasm engine.
 */
export type LixStorageProviderRegistration = {
	readonly version: 3;
	readonly moduleUrl: string;
	readonly options: unknown;
};

type JsProviderLixStorage = LixStorage & {
	readonly lixStorage: LixStorageProviderRegistration;
};

type FilesystemStorageConnection = {
	importFilesystemPaths(paths: string[]): Promise<void>;
	syncDiskToLix(): Promise<void>;
};

type FilesystemLixStorage = LixStorage & {
	readonly lixStorage: {
		readonly version: 1;
		readonly config: {
			kind: "filesystem";
			path: string;
			syncAllFiles: boolean;
		};
		connect(connection: FilesystemStorageConnection | undefined): void;
	};
};

export function isLixStorage(value: unknown): value is FilesystemLixStorage {
	if (!value || typeof value !== "object" || !("lixStorage" in value)) {
		return false;
	}
	const adapter = (value as { lixStorage?: unknown }).lixStorage;
	return Boolean(
		adapter &&
			typeof adapter === "object" &&
			(adapter as { version?: unknown }).version === 1 &&
			typeof (adapter as { connect?: unknown }).connect === "function" &&
			(adapter as { config?: { kind?: unknown } }).config?.kind ===
				"filesystem",
	);
}

export function isJsProviderLixStorage(
	value: unknown,
): value is JsProviderLixStorage {
	if (!value || typeof value !== "object" || !("lixStorage" in value)) {
		return false;
	}
	const registration = (value as { lixStorage?: unknown }).lixStorage;
	return Boolean(
		registration &&
			typeof registration === "object" &&
			(registration as { version?: unknown }).version === 3 &&
			typeof (registration as { moduleUrl?: unknown }).moduleUrl === "string",
	);
}

/**
 * JavaScript representation of `lix::storage::StorageSpace`.
 *
 * These provider types intentionally mirror the Rust storage traits. Changes
 * to `Storage`, `StorageRead`, `StorageWrite`, or `StorageScanSource` must be
 * reflected here and in the single Rust↔JS bridge.
 */
export type LixStorageSpace = {
	id: number;
	name: string;
	valueSemantics: "mutable" | "immutable";
	valueIntegrity: "backendVerified" | "contentAddressed";
};

export type LixStorageKeyRange = {
	lower: LixStorageBound;
	upper: LixStorageBound;
};

export type LixStorageBound =
	| { kind: "unbounded" }
	| { kind: "included"; key: Uint8Array }
	| { kind: "excluded"; key: Uint8Array };

export type LixStorageProjection = "keyOnly" | "fullValue";
export type LixStorageScanOrder = "ascending" | "descending";

export type LixStorageReadOptions = {
	snapshot?: Uint8Array;
	consistency: "snapshot" | "staleOk" | "latest";
	durability: "visible" | "durable";
	/** Canonical unsigned 64-bit base-10 token obtained from `acquireSession()`. */
	sessionToken?: string;
};

export type LixStorageWriteOptions = {
	baseSnapshot?: Uint8Array;
	idempotencyKey?: Uint8Array;
	awaitDurable: boolean;
	preconditions: LixStoragePrecondition[];
	batchCapacityHintBytes: number;
	/** Canonical unsigned 64-bit base-10 token obtained from `acquireSession()`. */
	sessionToken?: string;
};

export type LixStoragePrecondition =
	| { kind: "keyAbsent"; space: LixStorageSpace; key: Uint8Array }
	| { kind: "keyPresent"; space: LixStorageSpace; key: Uint8Array }
	| {
			kind: "keyValueHashEquals";
			space: LixStorageSpace;
			key: Uint8Array;
			hash: Uint8Array;
	  }
	| {
			kind: "keyValueEquals";
			space: LixStorageSpace;
			key: Uint8Array;
			expected: Uint8Array;
	  }
	| {
			kind: "rangeEmpty";
			space: LixStorageSpace;
			range: LixStorageKeyRange;
	  }
	| { kind: "branchEquals"; refKey: Uint8Array; expected: Uint8Array };

export type LixStorageGetManyRequest = {
	space: LixStorageSpace;
	keys: Uint8Array[];
	options: { projection: LixStorageProjection };
};

export type LixStorageProjectedValue =
	| { kind: "keyOnly" }
	| { kind: "fullValue"; value: Uint8Array };

export type LixStorageReadEntry = {
	key: Uint8Array;
	value: LixStorageProjectedValue;
};

export type LixStoragePutEntry = {
	key: Uint8Array;
	value: Uint8Array;
};

export type LixStorageWriteStats = {
	putEntries: number;
	deletedEntries: number;
	deletedRanges: number;
	writtenBytes: number;
	storageCalls: number;
};

export type LixStorageCommitResult = {
	commitId?: Uint8Array;
	stats: LixStorageWriteStats;
};

/** Mirrors `lix::storage::Storage`. */
export interface LixStorageProvider {
	/** Joins the active generation and returns its canonical unsigned 64-bit base-10 token. */
	acquireSession(): Promise<string>;
	beginRead(options: LixStorageReadOptions): Promise<LixStorageRead>;
	beginWrite(options: LixStorageWriteOptions): Promise<LixStorageWrite>;
	watchForChanges(): Promise<LixStorageChangeWatch>;

	/** SDK lifecycle hook corresponding to releasing the Rust storage owner. */
	close(): Promise<void>;
}

/** Mirrors `lix::storage::StorageChangeWatch`. */
export interface LixStorageChangeWatch {
	changed(): Promise<void>;
	close(): void;
}

/** Mirrors `lix::storage::StorageRead`. */
export interface LixStorageRead {
	/** Decimal u128, or undefined to disable snapshot-derived caching. */
	snapshotCacheKey(): string | undefined;
	getMany(
		requests: LixStorageGetManyRequest[],
	): Promise<Array<LixStorageProjectedValue | null>>;
	beginScan(
		space: LixStorageSpace,
		range: LixStorageKeyRange,
		options: {
			projection: LixStorageProjection;
			order: LixStorageScanOrder;
		},
	): Promise<LixStorageScanSource>;
}

/** Mirrors `lix::storage::StorageScanSource`. */
export interface LixStorageScanSource {
	nextPage(limitRows: number): Promise<{
		entries: LixStorageReadEntry[];
		hasMore: boolean;
	}>;
}

/** Mirrors `lix::storage::StorageWrite`. */
export interface LixStorageWrite {
	putMany(
		space: LixStorageSpace,
		entries: LixStoragePutEntry[],
	): Promise<void>;
	replaceMany(
		space: LixStorageSpace,
		entries: LixStoragePutEntry[],
	): Promise<void>;
	deleteMany(space: LixStorageSpace, keys: Uint8Array[]): Promise<void>;
	deleteRange(
		space: LixStorageSpace,
		range: LixStorageKeyRange,
	): Promise<void>;
	commit(): Promise<LixStorageCommitResult>;
	rollback(): Promise<void>;
}

export type LixStorageErrorCode =
	| "LIX_STORAGE_UNSUPPORTED"
	| "LIX_STORAGE_INVALID_KEY"
	| "LIX_STORAGE_INVALID_CURSOR"
	| "LIX_STORAGE_READ_EXPIRED"
	| "LIX_STORAGE_WRITE_CONFLICT"
	| "LIX_STORAGE_PRECONDITION_FAILED"
	| "LIX_STORAGE_DURABILITY"
	| "LIX_STORAGE_FENCED"
	| "LIX_STORAGE_CLOSED"
	| "LIX_STORAGE_COMMIT_OUTCOME_UNKNOWN"
	| "LIX_STORAGE_CORRUPTION"
	| "LIX_STORAGE_IO";

/** Error representation decoded back into `lix::storage::StorageError`. */
export class LixStorageError extends Error {
	readonly code: LixStorageErrorCode;
	readonly details?: unknown;

	constructor(
		code: LixStorageErrorCode,
		message: string,
		details?: unknown,
	) {
		super(message);
		this.name = "LixStorageError";
		this.code = code;
		this.details = details;
	}
}
