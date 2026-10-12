import type { HttpTransport } from "./http-transport.js";
import type {
	CommitSpan,
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	SwitchBranchOptions,
	SwitchBranchReceipt,
	LixTelemetryParentContext,
	LixOpenProgress,
	LixOpenReport,
	ReplicaRecoverySource,
	ReplicaRecoveryExport,
	ReplicaRecoveryReceipt,
	OpenAnotherSessionOptions,
	ResultColumn,
} from "./types.js";
import type { NativeLixValue } from "./value.js";
import type { LixStorageProvider } from "./storage-adapter.js";

export type SyncServerBindingOptions = {
	url: string;
	headers: [string, string][];
	headerProvider?: () => Promise<[string, string][]>;
	/** Credentials already admitted for the exact final session DELETE only. */
	teardownHeaders?: () => Promise<[string, string][]>;
	transport?: HttpTransport;
};

export type BindingExecuteResult = {
	statementIndex?: number;
	label?: string;
	columns: ResultColumn[];
	rows: NativeLixValue[][];
	rowsAffected: number;
	notices: Array<{
		code: string;
		message: string;
		hint?: string;
	}>;
	commit?: CommitSpan;
};

export type BindingObserveEvent = {
	sequence: number;
	mutationSequence: number;
	rows: BindingExecuteResult;
};

export type BindingParam = NativeLixValue;

/** Options of a buffered read or stream. */
export type BindingReadOptions = {
	/** Byte cap of a buffered result; `Infinity` opts out, omitted keeps 64 MiB. */
	maxBytes?: number;
	/** At most one row (`queryOne`): more reject `LIX_ERROR_NOT_UNIQUE`; not byte-capped. */
	single?: boolean;
};

/** Options of `execute` and `executeMany`. */
export type BindingExecuteOptions = Omit<ExecuteOptions, "maxBytes" | "signal"> & {
	maxBytes?: number;
};

/**
 * One open read stream. It opens with its first page buffered; `next()`
 * resolves null/undefined once exhausted. `cancel()` resolves once the engine
 * has stopped and the stream's read state is released.
 */
export type QueryStreamBinding = {
	readonly columns: string[];
	next(): Promise<BindingExecuteResult | null | undefined>;
	cancel(): void | Promise<void>;
};

/**
 * Reads of one state: the latest state per call or a
 * transaction. Aborting `signal` cancels the read in the engine: its work
 * stops within a time slice and the call rejects with the signal's reason.
 */
export type ReaderBinding = {
	query(
		sql: string,
		params: BindingParam[],
		options?: BindingReadOptions,
		signal?: AbortSignal,
	): Promise<BindingExecuteResult>;
	/** Resolves once the statement is planned and its first page has arrived. */
	stream(
		sql: string,
		params: BindingParam[],
		options?: BindingReadOptions,
		signal?: AbortSignal,
	): Promise<QueryStreamBinding>;
};

export type SnapshotExportBinding = {
	next(): Promise<Uint8Array | null | undefined>;
	cancel(): void | Promise<void>;
};

export type SnapshotRestoreBinding<T> = {
	write(chunk: Uint8Array): Promise<void>;
	isComplete(): boolean;
	finish(): Promise<T>;
	cancel(): void | Promise<void>;
};

export type LixBinding = ReaderBinding & {
	createHosted?(
		server: HostedServerBindingOptions,
	): Promise<import("./types.js").HostedLix>;
	openReport?(): LixOpenReport | undefined;
	setTelemetryParent(parent?: TelemetryParentContext): void;
	openAnotherSession(
		options: OpenAnotherSessionOptions,
		telemetry?: TelemetryDispatch,
	): Promise<LixBinding>;
	execute(
		sql: string,
		params: BindingParam[],
		options?: BindingExecuteOptions,
	): Promise<BindingExecuteResult>;
	executeMany(
		sql: string,
		paramSets: BindingParam[][],
		options?: BindingExecuteOptions,
	): Promise<{ rowsAffected: number }>;
	/** Pins the current state for several reads. */
	observe(
		sql: string,
		params: BindingParam[],
		options?: { signal?: AbortSignal },
	): Promise<ObserveEventsBinding>;
	beginTransaction(): Promise<LixTransactionBinding>;
	replicaRecoverySources(): Promise<ReplicaRecoverySource[]>;
	exportReplicaRecovery(id: string): Promise<ReplicaRecoveryExport>;
	recoverReplica(id: string): Promise<ReplicaRecoveryReceipt>;
	recoverReplicaWithServer(
		id: string,
		server: SyncServerBindingOptions,
	): Promise<ReplicaRecoveryReceipt>;
	syncHealth(): Promise<import("./types.js").SyncHealth>;
	prepareOfflineEditing?(): Promise<void>;
	activeBranchId(): Promise<string>;
	activeAccountId(): Promise<string>;
	createBranch(options: CreateBranchOptions): Promise<CreateBranchReceipt>;
	switchBranch(options: SwitchBranchOptions): Promise<SwitchBranchReceipt>;
	importFilesystemPaths(paths: string[]): Promise<void>;
	mergeBranchPreview(options: MergeBranchOptions): Promise<MergeBranchPreview>;
	mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt>;
	syncDiskToLix(): Promise<void>;
	exportSnapshot(): SnapshotExportBinding;
	close(): Promise<void>;
	/** Wait until native telemetry callbacks already queued have reached JS. */
	flushTelemetry?(): Promise<void>;
};

export type LixTransactionBinding = ReaderBinding & {
	execute(
		sql: string,
		params: BindingParam[],
		options?: BindingExecuteOptions,
	): Promise<BindingExecuteResult>;
	executeMany(
		sql: string,
		paramSets: BindingParam[][],
		options?: BindingExecuteOptions,
	): Promise<{ rowsAffected: number }>;
	commit(): Promise<{ commit?: CommitSpan | null }>;
	rollback(): Promise<void>;
};

export type ObserveEventsBinding = {
	setTelemetryParent(parent?: TelemetryParentContext): void;
	next(): Promise<BindingObserveEvent | null | undefined>;
	close(): void | Promise<void>;
};

export type TelemetryDispatch = (request: Uint8Array) => void;
export type OpenProgressDispatch = (progress: LixOpenProgress) => void;
export type TelemetryParentContext = LixTelemetryParentContext;

export type LixStorageProviderModule = {
	createLixStorageProvider(options: unknown): Promise<LixStorageProvider>;
};

export type LixStorageConfig = {
	durability?: import("./types.js").Durability;
} & (
	| { kind: "memory" }
	| {
			kind: "jsStorage";
			moduleUrl: string;
			options: unknown;
	  }
	| {
			kind: "filesystem";
			runtimeModuleUrl?: string;
			path: string;
			syncAllFiles: boolean;
	  }
);

export type HostedServerBindingOptions = {
	idempotencyKey?: string;
	url: string;
	headers: [string, string][];
};
