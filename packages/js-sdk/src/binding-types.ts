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
	LixTelemetryParentContext,
	LixOpenProgress,
	LixOpenReport,
	OpenAnotherSessionOptions,
} from "./types.js";
import type { NativeLixValue } from "./value.js";
import type { LixStorageProvider } from "./storage-adapter.js";

export type SyncServerBindingOptions = {
	url: string;
	headers: [string, string][];
	headerProvider?: () => Promise<[string, string][]>;
	fetch?: typeof fetch;
};

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
	openReport?(): LixOpenReport | undefined;
	setTelemetryParent(parent?: TelemetryParentContext): void;
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
	undo(): Promise<UndoReceipt>;
	redo(): Promise<RedoReceipt>;
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
	setTelemetryParent(parent?: TelemetryParentContext): void;
	next(): Promise<BindingObserveEvent | null | undefined>;
	close(): void;
};

export type TelemetryDispatch = (span: LixTelemetrySpan) => void;
export type OpenProgressDispatch = (progress: LixOpenProgress) => void;
export type TelemetryParentContext = LixTelemetryParentContext;

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
