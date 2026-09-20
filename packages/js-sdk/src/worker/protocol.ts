import type { HttpResponsePolicy } from "../http-transport.js";
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
	LixTelemetryParentContext,
	LixOpenProgress,
	LixOpenReport,
	OpenAnotherSessionOptions,
} from "../types.js";

export type WorkerSyncServerOptions = {
	url: string;
	headers?: [string, string][];
	dynamicHeaders: boolean;
};

export type WorkerSyncFetchRequest = {
	url: string;
	method: string;
	headers: [string, string][];
	body?: string | Uint8Array;
	credentials?: RequestCredentials;
    cache?: RequestCache;
    redirect?: RequestRedirect;
    response: HttpResponsePolicy;
};

type WorkerSyncFetchResponseHead = {
	status: number;
	statusText: string;
	headers: [string, string][];
};

export type WorkerSyncFetchResponse = WorkerSyncFetchResponseHead &
	({ streaming: true } | { streaming?: false; body: Uint8Array });

export type WorkerRequest = {
	id: number;
	sessionId: number;
	telemetryParent?: LixTelemetryParentContext;
	operation: WorkerOperation;
};

export type WorkerOperation =
	| {
			kind: "replica.cleanup";
			storage: LixStorageConfig;
			server: WorkerSyncServerOptions;
	  }
	| {
			kind: "replica.convert";
			storage: LixStorageConfig;
			server: WorkerSyncServerOptions;
			branchId?: string;
	  }
	| {
			kind: "hosted.create";
			server: import("../binding-types.js").HostedServerBindingOptions;
	  }
	| {
			kind: "hosted.delete";
			server: import("../binding-types.js").HostedServerBindingOptions;
	  }
	| {
			kind: "hosted.createFrom";
			server: import("../binding-types.js").HostedServerBindingOptions;
	  }
	| {
			kind: "open";
			storage: LixStorageConfig;
			telemetryEnabled: boolean;
			progressEnabled: boolean;
			snapshotId?: number;
			server?: WorkerSyncServerOptions;
	  }
	| { kind: "openSnapshot.write"; snapshotId: number; chunk: Uint8Array }
	| { kind: "openSnapshot.finish"; snapshotId: number }
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
	| { kind: "replicaRecoverySources" }
	| { kind: "exportReplicaRecovery"; id: string }
	| { kind: "recoverReplica"; id: string }
	| {
			kind: "recoverReplicaWithServer";
			id: string;
			server: WorkerSyncServerOptions;
			transportScope: number;
	  }
	| { kind: "syncHealth" }
	| { kind: "activeBranchId" }
	| { kind: "activeAccountId" }
	| { kind: "createBranch"; options: CreateBranchOptions }
	| { kind: "switchBranch"; options: SwitchBranchOptions }
	| { kind: "mergeBranchPreview"; options: MergeBranchOptions }
	| { kind: "mergeBranch"; options: MergeBranchOptions }
	| { kind: "importFilesystemPaths"; paths: string[] }
	| { kind: "syncDiskToLix" }
	| { kind: "exportSnapshot" }
	| { kind: "exportSnapshot.next"; exportId: number }
	| { kind: "exportSnapshot.cancel"; exportId: number }
	| { kind: "observe"; sql: string; params: BindingParam[] }
	| { kind: "observe.next"; observeId: number }
	| { kind: "close" };

export type WorkerNotification =
	| { kind: "transaction.abandon"; transactionId: number }
	| { kind: "observe.close"; observeId: number }
	| { kind: "openSnapshot.cancel"; snapshotId: number }
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
	  }
	| {
			kind: "sync.fetch.stream.result";
			requestId: number;
			result:
				| { ok: true; done: true }
				| { ok: true; done: false; chunk: Uint8Array }
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
    cause?: SerializedWorkerError;
	name: string;
	message: string;
	stack?: string;
	code?: string;
	hint?: string;
	details?: unknown;
};

export type WorkerResponse =
	| {
			id: number;
			ok: true;
			value?: unknown;
			context?: { branchId: string; accountId: string };
	  }
	| { id: number; ok: false; error: SerializedWorkerError }
	| { kind: "telemetry"; span: LixTelemetrySpan }
	| { kind: "open.progress"; progress: LixOpenProgress }
	| { kind: "sync.headers"; requestId: number; transportScope?: number }
	| {
			kind: "sync.fetch";
			requestId: number;
			request: WorkerSyncFetchRequest;
			transportScope?: number;
	  }
	| { kind: "sync.fetch.stream.pull"; requestId: number }
	| { kind: "sync.fetch.cancel"; requestId: number };

export function serializeWorkerError(error: unknown, depth = 0): SerializedWorkerError {
	if (!(error instanceof Error)) {
		return { name: "Error", message: "Non-error failure" };
	}
	const lixError = error as Error & {
		code?: unknown;
		hint?: unknown;
		details?: unknown;
	};
	return {
		name: error.name,
		message: redactDiagnostic(error.message),
		stack: error.stack ? redactDiagnostic(error.stack) : undefined,
		code: typeof lixError.code === "string" ? lixError.code : undefined,
		hint: typeof lixError.hint === "string" ? redactDiagnostic(lixError.hint) : undefined,
		details: redactDetails(lixError.details),
        cause: depth < 3 && error.cause !== undefined ? serializeWorkerError(error.cause, depth + 1) : undefined,
	};
}

export function deserializeWorkerError(error: SerializedWorkerError): Error {
	const restored = new Error(error.message, error.cause ? {cause: deserializeWorkerError(error.cause)} : undefined) as Error & {
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

function redactDiagnostic(value: string): string {
    return value.slice(0, 4096).replace(/Bearer\s+[^\s,;"']+/gi, "Bearer [redacted]")
        .replace(/((?:authorization|cookie|token|password|secret)\s*[:=]\s*)[^\n]+/gi, "$1[redacted]");
}
function redactDetails(value: unknown, depth = 0): unknown {
	if (depth > 3) return "[truncated]";
	if (typeof value === "string") return redactDiagnostic(value);
	if (value === null || typeof value === "number" || typeof value === "boolean" || value === undefined) return value;
	if (Array.isArray(value))
		return value.slice(0, 32).map((item) => redactDetails(item, depth + 1));
	if (typeof value === "object") return Object.fromEntries(Object.entries(value).slice(0, 32).map(([key, item]) =>
        [key, /authorization|cookie|token|password|secret|headers/i.test(key) ? "[redacted]" : redactDetails(item, depth + 1)]));
	return "[unsupported]";
}
