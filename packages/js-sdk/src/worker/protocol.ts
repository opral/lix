import type {
	BindingBatchStatement,
	BindingParam,
	LixStorageConfig,
} from "../binding-types.js";
import type {
	CreateBranchOptions,
	ExecuteOptions,
	LixBatchOptions,
	MergeBranchOptions,
	SwitchBranchOptions,
	LixTelemetrySpan,
	OpenAnotherSessionOptions,
} from "../types.js";

export type WorkerSyncServerOptions = {
	url: string;
	headers?: [string, string][];
	dynamicHeaders: boolean;
	customFetch: boolean;
};

export type WorkerSyncFetchRequest = {
	url: string;
	method: string;
	headers: [string, string][];
	body?: string | Uint8Array;
	credentials?: RequestCredentials;
	responseLimit: number;
};

export type WorkerSyncFetchResponse = {
	status: number;
	statusText: string;
	headers: [string, string][];
	body: Uint8Array;
};

export type WorkerRequest = {
	id: number;
	sessionId: number;
	operation: WorkerOperation;
};

export type WorkerOperation =
	| {
			kind: "open";
			storage: LixStorageConfig;
			telemetryEnabled: boolean;
			server?: WorkerSyncServerOptions;
	  }
	| { kind: "openAnotherSession"; options: OpenAnotherSessionOptions }
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
	| { kind: "activeAccountId" }
	| { kind: "createBranch"; options: CreateBranchOptions }
	| { kind: "createCheckpoint" }
	| { kind: "undo" }
	| { kind: "redo" }
	| { kind: "switchBranch"; options: SwitchBranchOptions }
	| { kind: "mergeBranchPreview"; options: MergeBranchOptions }
	| { kind: "mergeBranch"; options: MergeBranchOptions }
	| { kind: "importFilesystemPaths"; paths: string[] }
	| { kind: "syncDiskToLix" }
	| { kind: "observe"; sql: string; params: BindingParam[] }
	| { kind: "observe.next"; observeId: number }
	| { kind: "close" };

export type WorkerNotification =
	| { kind: "transaction.abandon"; transactionId: number }
	| { kind: "observe.close"; observeId: number }
	| {
			kind: "sync.headers.result";
			requestId: number;
			result:
				| { ok: true; headers: [string, string][] }
				| { ok: false; error: SerializedWorkerError };
	  }
	| {
			kind: "sync.fetch.result";
			requestId: number;
			result:
				| { ok: true; response: WorkerSyncFetchResponse }
				| { ok: false; error: SerializedWorkerError };
	  };

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
	| { kind: "telemetry"; span: LixTelemetrySpan }
	| { kind: "sync.headers"; requestId: number }
	| { kind: "sync.fetch"; requestId: number; request: WorkerSyncFetchRequest }
	| { kind: "sync.fetch.cancel"; requestId: number };

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
