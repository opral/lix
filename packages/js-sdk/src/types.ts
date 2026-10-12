/// <reference lib="esnext.disposable" preserve="true" />
export type RemoteLixFetch = (
	input: RequestInfo | URL,
	init?: RequestInit,
) => Promise<Response>;

/** A Lix protocol endpoint. Host URL for creation; repository URL for opening/deletion. */
export type LixServerOptions = {
	/** Defaults to remote SQL execution. Partial replicas require storage. */
	mode?: "remote" | "partial_replica";
	url: string | URL;
	headers?: HeadersInit | (() => HeadersInit | Promise<HeadersInit>);
    /** Network/telemetry adapter. Preserve the supplied credential headers;
     * delegate actual I/O to `networkFetch` to preserve typed network failures.
     * Injecting an account through hidden cookies or replacement credentials is unsupported.
     */
	fetch?: RemoteLixFetch;
};

export type HostedLix = { id: string; url: string };
export type CreateLixOptions = {
	/** Reuse this key when retrying a creation whose outcome was uncertain. */
	idempotencyKey?: string;
	server: Pick<LixServerOptions, "url" | "headers">;
	from?: import("./lix.js").Lix;
};
export type DeleteLixOptions = {
	server: Pick<LixServerOptions, "url" | "headers">;
};

/** W3C Trace Context headers used to parent engine spans to a host span. */
export type LixTelemetryParentContext = {
	traceparent: string;
	tracestate?: string;
};

export type LixTelemetryOptions = {
	/**
	 * Called for each span with one OTLP ExportTraceServiceRequest (protobuf).
	 * Return a promise when delivery is asynchronous; close() waits for it.
	 */
	onExport(request: Uint8Array): void | Promise<void>;
	/** Flush queued exports after span callbacks settle. Errors reject close(). */
	flush?(): void | Promise<void>;
	/** Read immediately before each serialized engine operation. */
	parentContext?(): LixTelemetryParentContext | undefined;
};

/** Stable phases emitted while a repository is opened. */
export type LixOpenPhase =
	| "inspecting"
	| "migrating"
	| "validating"
	| "opening"
	| "complete";

/** One observational repository-open progress snapshot. */
export type LixOpenProgress = {
	phase: LixOpenPhase;
	/** The repository whose opening is being observed. */
	scope: "local" | "authority";
	/** May be unknown while waiting for a remote authority upgrade. */
	fromFormat?: number;
	toFormat: number;
	completed?: number;
	total?: number;
};

/** The automatic repository migration performed by an open, if any. */
export type LixOpenMigrationReport = {
	fromFormat: number;
	toFormat: number;
};

export type LixOpenMigration = LixOpenMigrationReport & {
	scope: "local" | "authority";
};

/** Host-side startup waits measured around work already performed by the SDK. */
export type LixOpenHostProfile = {
	version: 1;
	wasm: {
		waitMs: number;
		realmReused: boolean;
		source: "bundled" | "cache" | "network" | "realm";
		cacheStatus: "hit" | "miss" | "corrupt" | "unavailable" | "not_consulted";
		fetchMs?: number;
		compileMs?: number;
		initializeMs?: number;
	};
	componentCompiler: {
		importMs: number;
		initializeMs: number;
	};
	provider?: {
		moduleImportMs: number;
		createMs: number;
		/** Physical provider open; overlaps createMs when supplied by the provider. */
		openMs?: number;
		opfs?: {
			lockWaitMs: number;
			sqliteInitMs: number;
			poolOpenMs: number;
			schemaInitMs: number;
		};
	};
	nativeBindingOpenMs: number;
};

/** Immutable facts about how a Lix handle was opened. */
export type LixOpenReport = {
	format: number;
	initialized: boolean;
	/** Actual waits at the JavaScript host boundary; omitted for shared attachments. */
	hostProfile?: LixOpenHostProfile;
	/** @deprecated Use migrations for scoped upgrade reports. */
	migration?: LixOpenMigrationReport;
	migrations: readonly LixOpenMigration[];
};

/** A retained local generation. Presence does not mean recovery is complete. */
export type ReplicaRecoverySource = {
	id: string;
	sourceFormat: number;
	repositoryId: string;
	accountId: string;
	recoveryRequired: boolean;
};

export type ReplicaRecoveryRow = {
	rowPk: JsonValue;
	schemaKey: string;
	fileId: string | null;
	snapshot: JsonValue;
	metadata: JsonValue;
	deleted: boolean;
	untracked: boolean;
	global: boolean;
	changeId: string | null;
	commitId: string | null;
};

export type ReplicaRecoveryBranch = {
	branchId: string;
	headCommitId: string;
	checkpointCommitId: string | null;
	rows: ReplicaRecoveryRow[];
};

export type ReplicaRecoveryBlob = {
	id: string;
	contentBase64: string;
};

export type ReplicaRecoveryFile = {
	branchId: string;
	id: string;
	path: string;
	untracked: boolean;
	blobId: string | null;
};

/** Portable recovery data; unresolved entries identify data not reconstructed. */
export type ReplicaRecoveryExport = {
	version: number;
	source: ReplicaRecoverySource;
	branches: ReplicaRecoveryBranch[];
	commits: JsonValue[];
	uploads: JsonValue[];
	blobs: ReplicaRecoveryBlob[];
	files: ReplicaRecoveryFile[];
	unresolved: string[];
};

/** Local recovery result. This does not acknowledge durable server storage. */
export type ReplicaRecoveryReceipt = {
	branchIds: string[];
	restoredFiles: number;
	restoredRows: number;
	unresolved: string[];
};

/** Local persistence policy. Buffered acknowledgement can lose saves on a crash. */
export type Durability = "durable" | "buffered";

export type LixOpenProgressOptions = {
	/** Observes local or authority inspection, automatic migration, and opening. */
	onProgress?(progress: LixOpenProgress): void;
};

export type RemoteLixServerOptions = Omit<LixServerOptions, "mode"> & {
	mode?: "remote";
};

export type PartialReplicaLixServerOptions = Omit<LixServerOptions, "mode"> & {
	mode: "partial_replica";
};

/** Server defaults to remote. Partial replicas explicitly opt in and require storage. */
export type OpenLixOptions =
	| ({
			storage?: import("./storage-adapter.js").LixStorage;
			/** Defaults to durable; applies to all local sessions and transactions. */
			durability?: Durability;
			server?: never;
			telemetry?: LixTelemetryOptions;
	  } & LixOpenProgressOptions)
	| ({
			storage?: never;
			/** Remote persistence is configured by the authority. */
			durability?: never;
			server: RemoteLixServerOptions;
			telemetry?: never;
	  } & LixOpenProgressOptions)
	| ({
			storage: import("./storage-adapter.js").LixStorage;
			durability?: Durability;
			server: PartialReplicaLixServerOptions;
			telemetry?: LixTelemetryOptions;
	  } & LixOpenProgressOptions);

/** Selects the initial context for an additional independent session. */
export type OpenAnotherSessionOptions = {
	/** Defaults to the current branch of the session opening it. */
	branchId?: string;
	/** Defaults to the current account. Remote sessions cannot override identity. */
	accountId?: string;
};

export type LixValue =
	| { kind: "null"; value: null }
	| { kind: "boolean"; value: boolean }
	| { kind: "integer"; value: number }
	| { kind: "real"; value: number }
	| { kind: "text"; value: string }
	| { kind: "jsonb"; value: JsonValue }
	| { kind: "row_ref"; value: string }
	| { kind: "timestamptz"; value: string }
	| { kind: "blob"; value: Uint8Array };

export type JsonValue =
	| null
	| boolean
	| number
	| string
	| readonly JsonValue[]
	| { readonly [key: string]: JsonValue };

/**
 * A SQL parameter: `null`, a boolean, a finite number, a string, a
 * `Uint8Array`, a `Value`, or a JSON value wrapped with `json(value)`. Anything
 * else (plain objects, arrays, `Date`, `undefined`, `NaN`, `Infinity`) throws
 * `LIX_INVALID_PARAM` instead of being coerced.
 */
export type SqlParam =
	| null
	| boolean
	| number
	| string
	| Uint8Array
	| import("./value.js").Value;

/** A result row: column name to decoded value. */
export type Row = Record<string, unknown>;

/** Cancels the call. A read rejects with the signal's reason. */
export type CallOptions = {
	signal?: AbortSignal;
};

/** Options of `query()`. */
export type QueryOptions = CallOptions & {
	/**
	 * Byte cap of the buffered result (default 64 MiB): the memory the engine
	 * holds for its rows. `Infinity` opts out. Exceeding it rejects with
	 * `LIX_ERROR_RESULT_TOO_LARGE`. On a remote Lix the cap is checked when the
	 * response arrives; the server's own response limit applies first.
	 */
	maxBytes?: number;
};

/** Options of `stream()`. Aborting closes the stream. */
export type StreamOptions = CallOptions & {
	/**
	 * Byte cap for the few statements that must buffer before their first page
	 * (file content, transaction and partial-replica reads). Default 64 MiB.
	 */
	maxBytes?: number;
};

export type WriteOptions = {
	originKey?: string;
	/**
	 * Caps whole automatic-transaction replays after conflict or snapshot expiry.
	 * Zero fails on the first failed attempt. Omit to retain default recovery budgets.
	 * Does not control explicit transactions, read recovery, or transport retries.
	 */
	maxAutoCommitRetries?: number;
	/**
	 * Stable identity for one logical remote SQL mutation. Supply the same key
	 * when retrying after a lost response; remote Lix generates one per call
	 * when this is omitted. This is sent as `Idempotency-Key`, not SQL options.
	 */
	idempotencyKey?: string;
	/**
	 * Aborting before the statement is sent prevents it from running. Once it
	 * is running, the call settles with its real outcome.
	 */
	signal?: AbortSignal;
};

/** Options of `execute()`. */
export type ExecuteOptions = WriteOptions & {
	/**
	 * Byte cap of the returned rows (default 64 MiB); `Infinity` opts out.
	 * Not supported on a remote Lix (rejects `LIX_INVALID_PARAM`): the server
	 * applies its default cap before it commits.
	 */
	maxBytes?: number;
};

/** Options of `executeMany()`. */
export type ExecuteManyOptions = WriteOptions;

export type ResultColumnType = LixValue["kind"];

export type ResultColumn = {
	name: string;
	type: ResultColumnType;
};


/**
 * The active-branch commits a write moved between.
 *
 * `before` is the active branch's head before the write and `after` the head
 * it published, so `lix_diff('lix_file', before, after)` is exactly what the
 * write changed. A write that published no commit on the active branch
 * reports both ids equal; a restore reports the commit it moved the head to.
 */
export type CommitSpan = {
	before: string;
	after: string;
};

export type StatementResult<TRow extends object = Row> = {
	columns: ResultColumn[];
	rows: TRow[];
	rowsAffected: number;
	notices: Array<{
		code: string;
		message: string;
		hint?: string;
	}>;
};

export type ExecuteResult<TRow extends object = Row> =
	StatementResult<TRow> & CommitReceipt;

export type CommitReceipt = {
	/** Exact durable transition for a committed write; null for reads. */
	commit: CommitSpan | null;
};

export type TransactionOptions = {
	/**
	 * How many times `lix.transaction()` reruns its callback after a
	 * `LIX_TRANSACTION_CONFLICT`. Defaults to 3; `0` fails on the first conflict.
	 */
	maxRetries?: number;
	/** Aborting rolls the transaction back; the call rejects with the reason. */
	signal?: AbortSignal;
};

export type TransactionResult<T> = CommitReceipt & {
	/** The callback's return value from the attempt that committed. */
	value: T;
	/** How many times the callback was rerun after a conflict. */
	retries: number;
};

/**
 * A forward-only cursor over a read's rows. Iterate it for rows or call
 * `pages()` for arrays of rows; both share one cursor. `break`, `return()`,
 * `close()` or `await using` release it.
 */
export interface RowStream<TRow extends object = Row>
	extends AsyncIterable<TRow>,
		AsyncDisposable {
	readonly columns: readonly string[];
	/** The same cursor as pages. The first page is small; later pages are <= 4096 rows / 1 MiB. */
	pages(): AsyncIterableIterator<TRow[]>;
	/** Idempotent. Resolves once the engine has stopped and the read state is released. */
	close(): Promise<void>;
}

/** Reads at one state: the latest committed state per call, a snapshot, or a transaction. */
export interface Reader {
	/** Reads only (writes reject with `LIX_ERROR_READ_ONLY`). Buffered and all-or-nothing. */
	query<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: QueryOptions,
	): Promise<TRow[]>;
	/**
	 * At most one row: `undefined` if there is none; rejects `LIX_ERROR_NOT_UNIQUE`
	 * if there are more. Reads at most two rows, so it takes no `maxBytes`.
	 */
	queryOne<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: CallOptions,
	): Promise<TRow | undefined>;
	/** Resolves once the statement is planned and its first page has arrived. */
	stream<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: StreamOptions,
	): Promise<RowStream<TRow>>;
}

/** A pinned state: every read through it sees the same state. */
export interface Snapshot extends Reader, AsyncDisposable {
	/** Closes its streams; later calls reject `LIX_ERROR_SNAPSHOT_CLOSED`. */
	close(): Promise<void>;
}

export type ObserveEvent = {
	sequence: number;
	mutationSequence: number;
	/**
	 * The current result of the observed query. Remote rows and
	 * `mutationSequence` come from the same authoritative observation frame;
	 * reconnect frames are coalesced when their rows are unchanged.
	 */
	result: ExecuteResult;
};

export type CreateBranchOptions = {
	id?: string;
	name: string;
	fromCommitId?: string;
};

export type CreateBranchReceipt = {
	id: string;
	name: string;
	hidden: boolean;
	commitId: string;
};

export type SwitchBranchOptions = {
	branchId: string;
};

export type SwitchBranchReceipt = {
	branchId: string;
};

export type MergeBranchOptions = {
	sourceBranchId: string;
};

export type MergeBranchOutcome =
	| "alreadyUpToDate"
	| "fastForward"
	| "mergeCommitted";

export type MergeBranchReceipt = {
	outcome: MergeBranchOutcome;
	targetBranchId: string;
	sourceBranchId: string;
	baseCommitId: string;
	targetHeadBeforeCommitId: string;
	sourceHeadBeforeCommitId: string;
	targetHeadAfterCommitId: string;
	createdMergeCommitId: string | null;
	changeStats: MergeChangeStats;
};

export type MergeBranchPreview = {
	outcome: MergeBranchOutcome;
	targetBranchId: string;
	sourceBranchId: string;
	baseCommitId: string;
	targetHeadCommitId: string;
	sourceHeadCommitId: string;
	changeStats: MergeChangeStats;
};

export type MergeChangeStats = {
	total: number;
	added: number;
	modified: number;
	removed: number;
};

/** Local partial-replica worker health. Running does not guarantee remote freshness. */
export type SyncHealth = {
	state: "inactive" | "running" | "stalled" | "failed" | "stopped";
	appliedCursor: number | null;
	observedCursor: number | null;
	failures: Partial<Record<"descriptor" | "publication" | "upload" | "lease", { code: string; message: string }>>;
	terminalError: { code: string; message: string } | null;
};
