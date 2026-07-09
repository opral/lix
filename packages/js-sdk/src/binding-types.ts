import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	SwitchBranchOptions,
	SwitchBranchReceipt,
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

export type LixBinding = {
	execute(
		sql: string,
		params: BindingParam[],
		options?: ExecuteOptions,
	): Promise<BindingExecuteResult>;
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

export type LixBackendConfig =
	| { kind: "memory" }
	| { kind: "sqlite"; path: string }
	| {
			kind: "fs";
			path: string;
			lixDir?: string;
			syncAllFiles: boolean;
	  };
