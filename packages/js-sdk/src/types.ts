export type IndexedDbStorageOptions = {
	/**
	 * Identifies one persistent Lix database within the current origin.
	 * A database name can be opened by only one Lix handle at a time.
	 */
	name: string;
};

export type RemoteLixFetch = (
	input: RequestInfo | URL,
	init?: RequestInit,
) => Promise<Response>;

export type RemoteLixServerOptions = {
	mode: "remote";
	url: string | URL;
	headers?: HeadersInit | (() => HeadersInit | Promise<HeadersInit>);
	fetch?: RemoteLixFetch;
};

export type LixTelemetrySpan = {
	schemaVersion: 1;
	name: string;
	startedAtUnixMs: number;
	durationMs: number;
	status: "ok" | "error";
	attributes: Record<string, string | number | boolean>;
};

export type LixTelemetryOptions = {
	onSpan(span: LixTelemetrySpan): void;
};

export type OpenLixOptions =
	| {
			storage?:
				| import("./storage-adapter.js").LixStorage
				| import("./open-lix.js").IndexedDbStorage;
			server?: never;
			telemetry?: LixTelemetryOptions;
	  }
	| {
			/** Optional local storage for `lix.clientState`. */
			storage?: import("./open-lix.js").IndexedDbStorage;
			server: RemoteLixServerOptions;
			telemetry?: never;
	  };

export type LixValue =
	| { kind: "null"; value: null }
	| { kind: "boolean"; value: boolean }
	| { kind: "integer"; value: number }
	| { kind: "real"; value: number }
	| { kind: "text"; value: string }
	| { kind: "json"; value: JsonValue }
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
	/** See {@link ExecuteOptions.idempotencyKey}. */
	idempotencyKey?: string;
};

export type ExecuteResult = {
	statementIndex?: number;
	label?: string;
	columns: string[];
	rows: RowLike[];
	rowsAffected: number;
	notices: Array<{
		code: string;
		message: string;
		hint?: string;
	}>;
};

export type ExecuteBatchResult = ExecuteResult & {
	statementIndex: number;
};

export type ObserveEvent = {
	sequence: number;
	mutationSequence: number;
	/**
	 * The current result of the observed query. Remote observations reconcile
	 * the first frame of every stream through execute before publishing it, so
	 * reconnects cannot expose a stale server snapshot to consumers.
	 */
	result: ExecuteResult;
};

export type RowLike = {
	get(column: string): unknown;
	value(column: string): ValueLike;
	toObject(): Record<string, unknown>;
	toValueMap(): Record<string, ValueLike>;
};

export type ValueLike = {
	readonly kind: LixValue["kind"];
	toJS(): unknown;
	asBytes(): Uint8Array | undefined;
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

export type CreateCheckpointReceipt = {
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
	kind: "sameEntityChanged";
	schemaKey: string;
	entityPk: unknown;
	fileId: string | null;
	target: MergeConflictSide;
	source: MergeConflictSide;
};

export type MergeConflictSide = {
	kind: "added" | "modified" | "removed";
	beforeChangeId: string | null;
	afterChangeId: string | null;
};
