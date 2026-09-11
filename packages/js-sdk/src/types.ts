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

export type LixTelemetrySpanLink = {
	traceId: string;
	spanId: string;
	traceFlags: number;
	traceState?: string;
};

/** W3C/OpenTelemetry context used as the remote parent of engine root spans. */
export type LixTelemetryParentContext = {
	traceId: string;
	spanId: string;
	traceFlags: number;
	traceState?: string;
};

export type LixTelemetrySpan = {
	schemaVersion: 3;
	name: string;
	kind: "internal" | "server" | "client" | "producer" | "consumer";
	traceId: string;
	spanId: string;
	traceFlags: number;
	traceState?: string;
	parentSpanId?: string;
	links?: LixTelemetrySpanLink[];
	startedAtUnixMs: number;
	durationMs: number;
	status: {
		code: "unset" | "error" | "ok";
		description?: string;
	};
	attributes: Record<string, string | number | boolean>;
};

export type LixTelemetryOptions = {
	onSpan(span: LixTelemetrySpan): void;
	/** Read immediately before each serialized engine operation. */
	parentContext?(): LixTelemetryParentContext | undefined;
};

/** Stable phases emitted while a local repository is opened. */
export type LixOpenPhase =
	| "inspecting"
	| "migrating"
	| "validating"
	| "opening"
	| "complete";

/** One observational repository-open progress snapshot. */
export type LixOpenProgress = {
	phase: LixOpenPhase;
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

/** Immutable facts about how a local Lix handle was opened. */
export type LixOpenReport = {
	format: number;
	initialized: boolean;
	migration?: LixOpenMigrationReport;
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

export type LixOpenProgressOptions = {
	/** Observes local inspection, automatic migration, and opening. */
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
			server?: never;
			telemetry?: LixTelemetryOptions;
	  } & LixOpenProgressOptions)
	| {
			storage?: never;
			server: RemoteLixServerOptions;
			telemetry?: never;
			onProgress?: never;
	  }
	| ({
			storage: import("./storage-adapter.js").LixStorage;
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

export type SqlParam = JsonValue | Uint8Array | import("./value.js").Value;

export type ExecuteOptions = {
	originKey?: string;
	/** Returns positional arrays instead of plain objects. Defaults to "object". */
	rowMode?: "object" | "array";
	/**
	 * Stable identity for one logical remote SQL mutation. Supply the same key
	 * when retrying after a lost response; remote Lix generates one per call
	 * when this is omitted. This is sent as `Idempotency-Key`, not SQL options.
	 */
	idempotencyKey?: string;
};

export type LixBatchStatement = {
	sql: string;
	params?: readonly SqlParam[];
	label?: string;
};

export type LixBatchOptions = {
	originKey?: string;
	/** Returns positional arrays instead of plain objects. Defaults to "object". */
	rowMode?: "object" | "array";
	/** See {@link ExecuteOptions.idempotencyKey}. */
	idempotencyKey?: string;
};

export type ResultColumnType = LixValue["kind"];

export type ResultColumn = {
	name: string;
	type: ResultColumnType;
};

export type ResultObjectRow = Record<string, unknown>;
export type ResultArrayRow = unknown[];
export type ResultRow = ResultObjectRow | ResultArrayRow;

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

export type ExecuteResult<TRow extends object = ResultObjectRow> = {
	statementIndex?: number;
	label?: string;
	columns: ResultColumn[];
	rows: TRow[];
	rowsAffected: number;
	notices: Array<{
		code: string;
		message: string;
		hint?: string;
	}>;
	/**
	 * Present on every auto-committed write statement, including `RETURNING`
	 * writes, and on every statement of a written batch (all carry the
	 * batch's one span, read statements included). Absent for read
	 * statements outside a written batch, read-only batches, statements
	 * inside an explicit transaction (whose commit is the write), and the
	 * first commit on a branch that had no head yet.
	 */
	commit?: CommitSpan;
};

export type ExecuteBatchResult<TRow extends object = ResultObjectRow> =
	ExecuteResult<TRow> & {
		statementIndex: number;
	};

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

export type UndoReceipt = {
	branchId: string;
	targetCommitId: string;
	inverseCommitId: string;
};

export type RedoReceipt = {
	branchId: string;
	targetCommitId: string;
	replayCommitId: string;
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
	conflicts: MergeConflict[];
};

export type MergeChangeStats = {
	total: number;
	added: number;
	modified: number;
	removed: number;
};

export type MergeConflict = {
	kind: "sameRowChanged";
	rowRef: string;
	fileId: string | null;
	target: MergeConflictSide;
	source: MergeConflictSide;
};

export type MergeConflictSide = {
	kind: "added" | "modified" | "removed";
	beforeChangeId: string | null;
	afterChangeId: string | null;
};
