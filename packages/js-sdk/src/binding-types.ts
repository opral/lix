import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	CreateCheckpointReceipt,
	UndoReceipt,
	RedoReceipt,
	ExecuteOptions,
	LixBatchOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	SwitchBranchOptions,
	SwitchBranchReceipt,
	LixTelemetrySpan,
	OpenAnotherSessionOptions,
} from "./types.js";
import type { NativeLixValue } from "./value.js";
import type { LixStorageProvider } from "./storage-adapter.js";

export type BindingExecuteResult = {
	statementIndex?: number;
	label?: string;
	columns: string[];
	rows: NativeLixValue[][];
	rowsAffected: number;
	notices: Array<{
		code: string;
		message: string;
		hint?: string;
	}>;
};

export type BindingObserveEvent = {
	sequence: number;
	mutationSequence: number;
	rows: BindingExecuteResult;
};

export type BindingParam = NativeLixValue;

export type BindingBatchStatement = {
	sql: string;
	params: BindingParam[];
	label?: string;
};

export type LixBinding = {
	openAnotherSession(options: OpenAnotherSessionOptions): Promise<LixBinding>;
	execute(
		sql: string,
		params: BindingParam[],
		options?: ExecuteOptions,
	): Promise<BindingExecuteResult>;
	executeBatch(
		statements: BindingBatchStatement[],
		options?: LixBatchOptions,
	): Promise<BindingExecuteResult[]>;
	observe(sql: string, params: BindingParam[]): Promise<ObserveEventsBinding>;
	beginTransaction(): Promise<LixTransactionBinding>;
	activeBranchId(): Promise<string>;
	activeAccountId(): Promise<string>;
	createBranch(options: CreateBranchOptions): Promise<CreateBranchReceipt>;
	createCheckpoint(): Promise<CreateCheckpointReceipt>;
	restore(commitId: string): Promise<void>;
	undo(): Promise<UndoReceipt>;
	redo(): Promise<RedoReceipt>;
	switchBranch(options: SwitchBranchOptions): Promise<SwitchBranchReceipt>;
	importFilesystemPaths(paths: string[]): Promise<void>;
	mergeBranchPreview(options: MergeBranchOptions): Promise<MergeBranchPreview>;
	mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt>;
	syncDiskToLix(): Promise<void>;
	/** Explicit snapshot utility available on direct in-memory WASM bindings. */
	exportSnapshot?(): Promise<Uint8Array>;
	close(): Promise<void>;
};

export type LixTransactionBinding = {
	execute(
		sql: string,
		params: BindingParam[],
		options?: ExecuteOptions,
	): Promise<BindingExecuteResult>;
	commit(): Promise<void>;
	rollback(): Promise<void>;
};

export type ObserveEventsBinding = {
	next(): Promise<BindingObserveEvent | null | undefined>;
	close(): void;
};

export type TelemetryDispatch = (span: LixTelemetrySpan) => void;

export type LixStorageProviderModule = {
	createLixStorageProvider(options: unknown): Promise<LixStorageProvider>;
};

export type LixStorageConfig =
	| { kind: "memory" }
	| {
			kind: "jsStorage";
			moduleUrl: string;
			options: unknown;
	  }
	| {
			kind: "filesystem";
			path: string;
			syncAllFiles: boolean;
	  };
