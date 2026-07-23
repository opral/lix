import type {
	BindingBatchStatement,
	BindingParam,
	LixStorageConfig,
} from "../binding-types.js";
import type {
	CreateBranchOptions,
	ExecuteOptions,
	JsonValue,
	LixBatchOptions,
	MergeBranchOptions,
	SwitchBranchOptions,
	LixTelemetrySpan,
} from "../types.js";

export type WorkerRequest = {
	id: number;
	operation: WorkerOperation;
};

export type WorkerOperation =
	| { kind: "open"; storage: LixStorageConfig; telemetryEnabled: boolean }
	| {
			kind: "execute";
			sql: string;
			params: BindingParam[];
			options?: ExecuteOptions;
	  }
	| {
			kind: "executeBatch";
			statements: BindingBatchStatement[];
			options?: LixBatchOptions;
	  }
	| { kind: "beginTransaction" }
	| {
			kind: "transaction.execute";
			transactionId: number;
			sql: string;
			params: BindingParam[];
			options?: ExecuteOptions;
	  }
	| { kind: "transaction.commit"; transactionId: number }
	| { kind: "transaction.rollback"; transactionId: number }
	| { kind: "activeBranchId" }
	| { kind: "clientState.entries" }
	| { kind: "clientState.get"; key: string }
	| { kind: "clientState.set"; key: string; value: JsonValue }
	| { kind: "clientState.delete"; key: string }
	| { kind: "createBranch"; options: CreateBranchOptions }
	| { kind: "switchBranch"; options: SwitchBranchOptions }
	| { kind: "mergeBranchPreview"; options: MergeBranchOptions }
	| { kind: "mergeBranch"; options: MergeBranchOptions }
	| { kind: "importFilesystemPaths"; paths: string[] }
	| { kind: "syncDiskToLix" }
	| { kind: "exportSnapshot" }
	| { kind: "observe"; sql: string; params: BindingParam[] }
	| { kind: "observe.next"; observeId: number }
	| { kind: "close" };

export type WorkerNotification =
	| { kind: "transaction.abandon"; transactionId: number }
	| { kind: "observe.close"; observeId: number };

export type WorkerInput = WorkerRequest | WorkerNotification;

export type WorkerConnection = {
	postMessage(message: WorkerInput): void;
	onMessage(listener: (message: WorkerResponse) => void): void;
	onFatal(listener: (error: Error) => void): void;
	ref(): void;
	unref(): void;
	terminate(): Promise<void>;
};

export type WorkerHostEndpoint = {
	postMessage(message: WorkerResponse): void;
	onMessage(listener: (message: WorkerInput) => void): void;
};

export type SerializedWorkerError = {
	name: string;
	message: string;
	stack?: string;
	code?: string;
	hint?: string;
	details?: unknown;
};

export type WorkerResponse =
	| { id: number; ok: true; value?: unknown }
	| { id: number; ok: false; error: SerializedWorkerError }
	| { kind: "telemetry"; span: LixTelemetrySpan };

export function serializeWorkerError(error: unknown): SerializedWorkerError {
	if (!(error instanceof Error)) {
		return { name: "Error", message: String(error) };
	}
	const lixError = error as Error & {
		code?: unknown;
		hint?: unknown;
		details?: unknown;
	};
	return {
		name: error.name,
		message: error.message,
		stack: error.stack,
		code: typeof lixError.code === "string" ? lixError.code : undefined,
		hint: typeof lixError.hint === "string" ? lixError.hint : undefined,
		details: lixError.details,
	};
}

export function deserializeWorkerError(error: SerializedWorkerError): Error {
	const restored = new Error(error.message) as Error & {
		code?: string;
		hint?: string;
		details?: unknown;
	};
	restored.name = error.name;
	restored.stack = error.stack;
	restored.code = error.code;
	restored.hint = error.hint;
	restored.details = error.details;
	return restored;
}
