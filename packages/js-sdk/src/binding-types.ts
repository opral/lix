import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	CreateCheckpointReceipt,
	ExecuteOptions,
	LixBatchOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	SwitchBranchOptions,
	SwitchBranchReceipt,
	LixTelemetrySpan,
	JsonValue,
} from "./types.js";
import type { NativeLixValue } from "./value.js";

export type BindingExecuteResult = {
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
};

export type LixBinding = {
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
	clientStateEntries?(): Promise<Array<{ key: string; value: JsonValue }>>;
	clientStateGet?(key: string): Promise<JsonValue | undefined>;
	clientStateSet?(key: string, value: JsonValue): Promise<void>;
	clientStateDelete?(key: string): Promise<void>;
	createBranch(options: CreateBranchOptions): Promise<CreateBranchReceipt>;
	createCheckpoint(): Promise<CreateCheckpointReceipt>;
	switchBranch(options: SwitchBranchOptions): Promise<SwitchBranchReceipt>;
	importFilesystemPaths(paths: string[]): Promise<void>;
	mergeBranchPreview(options: MergeBranchOptions): Promise<MergeBranchPreview>;
	mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt>;
	syncDiskToLix(): Promise<void>;
	/** Internal snapshot capability implemented by browser memory bindings. */
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

export type PluginRuntimeDispatch = (request: unknown) => Promise<unknown>;
export type TelemetryDispatch = (span: LixTelemetrySpan) => void;

export type LixStorageConfig =
	| { kind: "memory"; snapshot?: Uint8Array }
	| { kind: "sqlite"; path: string }
	| {
			kind: "localFilesystem";
			path: string;
			lixDir?: string;
			syncAllFiles: boolean;
	  };
