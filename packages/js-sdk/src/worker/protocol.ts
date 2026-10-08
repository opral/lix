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

/** The only network operation permitted after a repository client starts closing. */
export function isSessionCloseRequest(request: {
	url: string;
	method?: string;
	headers?: HeadersInit;
	init?: { method?: string; headers?: HeadersInit };
	}, authorityUrl?: string | URL): boolean {
	const method = request.init?.method ?? request.method;
	const headers = request.init?.headers ?? request.headers;
	if ((method ?? "GET").toUpperCase() !== "DELETE") return false;
	try {
		const url = new URL(request.url);
		const sessionId = new Headers(headers).get("lix-session-id");
		const sessionPath = /^\/lix\/v1\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/session\/?$/;
		const sessionMatch = sessionPath.exec(url.pathname);
		if (!sessionMatch || url.search || url.hash || url.username || url.password || !sessionId?.trim()) return false;
		if (authorityUrl !== undefined) {
			const authority = new URL(authorityUrl.toString());
			const authorityMatch = /^\/lix\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/.exec(authority.pathname);
			if (
				!authorityMatch || authority.search || authority.hash || authority.username || authority.password ||
				url.origin !== authority.origin || sessionMatch[1] !== authorityMatch[1]
			) return false;
		}
		return true;
	} catch {
		return false;
	}
}

/** Notifications needed to finish an already-started remote session close. */
export function isSessionCloseTransportResponse(
	message: WorkerResponse,
): boolean {
	return "kind" in message && (
		message.kind === "sync.headers" ||
		(message.kind === "sync.fetch" && isSessionCloseRequest(message.request))
	);
}

/** Acknowledgments for teardown-only transport callbacks. */
export function isSessionCloseTransportResult(message: WorkerInput): boolean {
	return !("id" in message) && (
		message.kind === "sync.headers.result" ||
		message.kind === "sync.fetch.result"
	);
}

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
	| { kind: "prepareOfflineEditing" }
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
	| { kind: "observe.close"; observeId: number }
	| { kind: "close" };

export type WorkerNotification =
	| { kind: "transaction.abandon"; transactionId: number }
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
	rustOrigin?: SerializedRustErrorOrigin;
	rustStacktraceStatus?: "not_captured";
};

export type SerializedRustErrorOrigin = {
	kind: "source_location";
	file: string;
	line: number;
	column?: number;
};

export type WorkerResponse =
	| {
			id: number;
			ok: true;
			value?: unknown;
			context?: { branchId: string; accountId: string };
	  }
	| { id: number; ok: false; error: SerializedWorkerError }
	| { kind: "request.started"; id: number }
	| { kind: "request.queued"; id: number }
	| { kind: "telemetry"; request: Uint8Array }
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
		rustOrigin?: unknown;
		rustStacktraceStatus?: unknown;
	};
	const rustOrigin = safeSerializedRustErrorOrigin(lixError.rustOrigin);
	return {
		name: error.name,
		message: redactDiagnostic(error.message),
		stack: error.stack ? redactDiagnostic(error.stack) : undefined,
		code: typeof lixError.code === "string" ? lixError.code : undefined,
		hint: typeof lixError.hint === "string" ? redactDiagnostic(lixError.hint) : undefined,
		details: redactDetails(lixError.details),
		rustOrigin,
		rustStacktraceStatus:
			lixError.rustStacktraceStatus === "not_captured"
				? "not_captured"
				: undefined,
		cause:
			depth < 3 && error.cause !== undefined
				? serializeWorkerError(error.cause, depth + 1)
				: undefined,
	};
}

export function deserializeWorkerError(error: SerializedWorkerError): Error {
	const restored = new Error(error.message, error.cause ? {cause: deserializeWorkerError(error.cause)} : undefined) as Error & {
		code?: string;
		hint?: string;
		details?: unknown;
		rustOrigin?: SerializedRustErrorOrigin;
		rustStacktraceStatus?: "not_captured";
	};
	restored.name = error.name;
	restored.stack = error.stack;
	restored.code = error.code;
	restored.hint = error.hint;
	restored.details = error.details;
	restored.rustOrigin = safeSerializedRustErrorOrigin(error.rustOrigin);
	restored.rustStacktraceStatus =
		error.rustStacktraceStatus === "not_captured"
			? "not_captured"
			: undefined;
	return restored;
}

function safeSerializedRustErrorOrigin(
	value: unknown,
): SerializedRustErrorOrigin | undefined {
	if (!value || typeof value !== "object") return undefined;
	const origin = value as {
		kind?: unknown;
		file?: unknown;
		line?: unknown;
		column?: unknown;
	};
	if (
		origin.kind !== "source_location" ||
		typeof origin.file !== "string" ||
		!/^packages[/][A-Za-z0-9_-]{1,80}[/](?:[A-Za-z0-9_.-]+[/])*[A-Za-z0-9_.-]+[.]rs$/.test(
			origin.file,
		) ||
		origin.file.split("/").some((part) => part === "." || part === "..") ||
		!Number.isSafeInteger(origin.line) ||
		(origin.line as number) < 1 ||
		(origin.line as number) > 1_000_000
	)
		return undefined;
	return {
		kind: "source_location",
		file: origin.file,
		line: origin.line as number,
		...(Number.isSafeInteger(origin.column) &&
		(origin.column as number) >= 0 &&
		(origin.column as number) <= 100_000_000
			? { column: origin.column as number }
			: {}),
	};
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
