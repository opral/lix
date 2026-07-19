import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteOptions,
	LixBatchOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	SwitchBranchOptions,
	SwitchBranchReceipt,
	LixTelemetrySpan,
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
	createBranch(options: CreateBranchOptions): Promise<CreateBranchReceipt>;
	switchBranch(options: SwitchBranchOptions): Promise<SwitchBranchReceipt>;
	importFilesystemPaths(paths: string[]): Promise<void>;
	mergeBranchPreview(options: MergeBranchOptions): Promise<MergeBranchPreview>;
	mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt>;
	syncDiskToLix(): Promise<void>;
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
	| { kind: "memory" }
	| { kind: "sqlite"; path: string }
	| {
			kind: "localFilesystem";
			path: string;
			lixDir?: string;
			syncAllFiles: boolean;
	  };
