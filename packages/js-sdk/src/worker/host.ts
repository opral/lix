import { openLixBinding } from "#binding";
import type {
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "../binding-types.js";
import type { LixOpenProgress, LixOpenReport, LixTelemetrySpan } from "../types.js";
import {
	deserializeWorkerError,
	serializeWorkerError,
	type WorkerHostEndpoint,
	type WorkerInput,
	type WorkerOperation,
	type WorkerRequest,
	type WorkerSyncFetchRequest,
	type WorkerSyncFetchResponse,
	type WorkerSyncServerOptions,
} from "./protocol.js";

export function startWorkerHost(endpoint: WorkerHostEndpoint): void {
	const sessions = new Map<number, LixBinding>();
	let nextSessionId = 1;
	let nextTransactionId = 1;
	let nextObserveId = 1;
	const transactions = new Map<number, LixTransactionBinding>();
	const observations = new Map<number, ObserveEventsBinding>();
	let nextSyncRequestId = 1;
	const pendingSyncHeaders = new Map<
		number,
		{ resolve(headers: [string, string][]): void; reject(error: unknown): void }
	>();
	const pendingSyncFetch = new Map<
		number,
		{ resolve(response: WorkerSyncFetchResponse): void; reject(error: unknown): void }
	>();
	let finiteQueue = Promise.resolve();

	endpoint.onMessage((message: WorkerInput) => {
		if (!("id" in message)) {
			handleNotification(message);
			return;
		}
		if (message.operation.kind === "observe.next") {
			const observeId = message.operation.observeId;
			void respond(message, () =>
				handleObserveNext(observeId, message.telemetryParent),
			);
			return;
		}
		finiteQueue = finiteQueue.then(async () => {
			try {
				await respond(message, async () => {
					if (message.operation.kind !== "open") {
						requiredLix(message.sessionId).setTelemetryParent(
							message.telemetryParent,
						);
					}
					return handleFiniteOperation(
						message.sessionId,
						message.operation,
						message.telemetryParent,
					);
				});
			} finally {
				// The mutable FFI carrier is safe only within this serialized
				// operation. Clear it before another request or background task
				// can accidentally inherit a stale remote parent.
				(sessions.get(message.sessionId) ?? sessions.get(0))?.setTelemetryParent();
			}
		});
	});

	function handleNotification(
		message: Exclude<WorkerInput, WorkerRequest>,
	): void {
			switch (message.kind) {
			case "observe.close": {
				const events = observations.get(message.observeId);
				observations.delete(message.observeId);
				events?.close();
				break;
			}
			case "transaction.abandon":
				finiteQueue = finiteQueue.then(async () => {
					const transaction = transactions.get(message.transactionId);
					transactions.delete(message.transactionId);
					if (transaction) await transaction.rollback().catch(() => undefined);
				});
				break;
			case "sync.headers.result": {
				const pending = pendingSyncHeaders.get(message.requestId);
				pendingSyncHeaders.delete(message.requestId);
				if (!pending) break;
				if (message.result.ok) pending.resolve(message.result.headers);
				else pending.reject(deserializeWorkerError(message.result.error));
				break;
			}
			case "sync.fetch.result": {
				const pending = pendingSyncFetch.get(message.requestId);
				pendingSyncFetch.delete(message.requestId);
				if (!pending) break;
				if (message.result.ok) pending.resolve(message.result.response);
				else pending.reject(deserializeWorkerError(message.result.error));
				break;
			}
		}
	}

	async function respond(
		request: WorkerRequest,
		operation: () => Promise<unknown>,
	): Promise<void> {
		try {
			const value = await operation();
			endpoint.postMessage({ id: request.id, ok: true, value });
		} catch (error) {
			endpoint.postMessage({
				id: request.id,
				ok: false,
				error: serializeWorkerError(error),
			});
		}
	}

	async function handleFiniteOperation(
		sessionId: number,
		operation: WorkerOperation,
		telemetryParent: WorkerRequest["telemetryParent"],
	): Promise<unknown> {
		switch (operation.kind) {
			case "open":
				if (sessions.size > 0)
					throw workerStateError("Lix worker is already open");
				{
					const opened = await openLixBinding(
						operation.storage,
						operation.telemetryEnabled
							? (span: LixTelemetrySpan) =>
									endpoint.postMessage({ kind: "telemetry", span })
							: undefined,
						telemetryParent,
						createSyncServerBridge(operation.server),
						operation.progressEnabled
							? (progress: LixOpenProgress) =>
									endpoint.postMessage({ kind: "open.progress", progress })
							: undefined,
					);
					sessions.set(0, opened);
					return opened.openReport?.() satisfies LixOpenReport | undefined;
				}
			case "openAnotherSession": {
				const opened = await requiredLix(sessionId).openAnotherSession(
					operation.options,
				);
				const openedSessionId = nextSessionId++;
				sessions.set(openedSessionId, opened);
				return openedSessionId;
			}
			case "execute":
				return requiredLix(sessionId).execute(
					operation.sql,
					operation.params,
					operation.options,
				);
			case "executeBatch":
				return requiredLix(sessionId).executeBatch(
					operation.statements,
					operation.options,
				);
			case "beginTransaction": {
				const transaction = await requiredLix(sessionId).beginTransaction();
				const transactionId = nextTransactionId++;
				transactions.set(transactionId, transaction);
				return transactionId;
			}
			case "transaction.execute":
				return requiredTransaction(operation.transactionId).execute(
					operation.sql,
					operation.params,
					operation.options,
				);
			case "transaction.commit": {
				const transaction = requiredTransaction(operation.transactionId);
				transactions.delete(operation.transactionId);
				await transaction.commit();
				return undefined;
			}
			case "transaction.rollback": {
				const transaction = requiredTransaction(operation.transactionId);
				transactions.delete(operation.transactionId);
				await transaction.rollback();
				return undefined;
			}
			case "activeBranchId":
				return requiredLix(sessionId).activeBranchId();
			case "activeAccountId":
				return requiredLix(sessionId).activeAccountId();
			case "createBranch":
				return requiredLix(sessionId).createBranch(operation.options);
			case "createCheckpoint":
				return requiredLix(sessionId).createCheckpoint();
			case "undo":
				return requiredLix(sessionId).undo();
			case "redo":
				return requiredLix(sessionId).redo();
			case "switchBranch":
				return requiredLix(sessionId).switchBranch(operation.options);
			case "mergeBranchPreview":
				return requiredLix(sessionId).mergeBranchPreview(operation.options);
			case "mergeBranch":
				return requiredLix(sessionId).mergeBranch(operation.options);
			case "importFilesystemPaths":
				return requiredLix(sessionId).importFilesystemPaths(operation.paths);
			case "syncDiskToLix":
				return requiredLix(sessionId).syncDiskToLix();
			case "observe": {
				const events = await requiredLix(sessionId).observe(
					operation.sql,
					operation.params,
				);
				const observeId = nextObserveId++;
				observations.set(observeId, events);
				return observeId;
			}
			case "close": {
				const openLix = requiredLix(sessionId);
				await openLix.close();
				sessions.delete(sessionId);
				return undefined;
			}
			case "observe.next":
				throw workerStateError("observe.next must use the observation lane");
		}
	}

	function createSyncServerBridge(
		server: WorkerSyncServerOptions | undefined,
	):
		| {
				url: string;
				headers: [string, string][];
				headerProvider?: () => Promise<[string, string][]>;
				fetch?: typeof fetch;
		  }
		| undefined {
		if (!server) return undefined;
		return {
			url: server.url,
			headers: server.headers ?? [],
			headerProvider: server.dynamicHeaders
				? () => {
						const requestId = nextSyncRequestId++;
						return new Promise((resolve, reject) => {
							pendingSyncHeaders.set(requestId, { resolve, reject });
							endpoint.postMessage({ kind: "sync.headers", requestId });
						});
					}
				: undefined,
			fetch: server.customFetch ? bridgeFetch : undefined,
		};
	}

	async function bridgeFetch(
		input: RequestInfo | URL,
		init?: RequestInit,
	): Promise<Response> {
		const responseLimit = (
			init as (RequestInit & { lixResponseLimit?: unknown }) | undefined
		)?.lixResponseLimit;
		if (
			typeof responseLimit !== "number" ||
			!Number.isSafeInteger(responseLimit) ||
			responseLimit <= 0
		) {
			throw new TypeError("Browser sync fetch has no valid response limit");
		}
		const requestId = nextSyncRequestId++;
		const request: WorkerSyncFetchRequest = {
			url:
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			method: init?.method ?? "GET",
			headers: headerEntries(init?.headers),
			body: serializableBody(init?.body),
			credentials: init?.credentials,
			responseLimit,
		};
		const response = new Promise<WorkerSyncFetchResponse>((resolve, reject) => {
			pendingSyncFetch.set(requestId, { resolve, reject });
			endpoint.postMessage({ kind: "sync.fetch", requestId, request });
		});
		const abort = () => {
			const pending = pendingSyncFetch.get(requestId);
			pendingSyncFetch.delete(requestId);
			if (pending) {
				pending.reject(new DOMException("The operation was aborted", "AbortError"));
				endpoint.postMessage({ kind: "sync.fetch.cancel", requestId });
			}
		};
		if (init?.signal?.aborted) abort();
		else init?.signal?.addEventListener("abort", abort, { once: true });
		try {
			const resolved = await response;
			return responseFromSyncFetch(resolved);
		} finally {
			init?.signal?.removeEventListener("abort", abort);
			pendingSyncFetch.delete(requestId);
		}
	}

	async function handleObserveNext(
		observeId: number,
		telemetryParent: WorkerRequest["telemetryParent"],
	): Promise<unknown> {
		const events = observations.get(observeId);
		if (!events) return undefined;
		events.setTelemetryParent(telemetryParent);
		return events.next();
	}

	function requiredLix(sessionId: number): LixBinding {
		const lix = sessions.get(sessionId);
		if (!lix) throw workerStateError("Lix session is closed");
		return lix;
	}

	function requiredTransaction(transactionId: number): LixTransactionBinding {
		const transaction = transactions.get(transactionId);
		if (!transaction) {
			const error = workerStateError("Lix transaction is closed");
			error.code = "LIX_INVALID_TRANSACTION_STATE";
			throw error;
		}
		return transaction;
	}
}

export function responseFromSyncFetch(
	resolved: WorkerSyncFetchResponse,
): Response {
	const body =
		resolved.status === 204 ||
		resolved.status === 205 ||
		resolved.status === 304
			? null
			: resolved.body.slice().buffer;
	return new Response(body, {
		status: resolved.status,
		statusText: resolved.statusText,
		headers: resolved.headers,
	});
}

function workerStateError(message: string): Error & { code?: string } {
	const error = new Error(message) as Error & { code?: string };
	error.name = "LixError";
	error.code = "LIX_ERROR_CLOSED";
	return error;
}

function headerEntries(headers: HeadersInit | undefined): [string, string][] {
	const entries: [string, string][] = [];
	new Headers(headers).forEach((value, name) => entries.push([name, value]));
	return entries;
}

function serializableBody(body: BodyInit | null | undefined): string | Uint8Array | undefined {
	if (body === undefined || body === null) return undefined;
	if (typeof body === "string") return body;
	if (body instanceof Uint8Array) return body;
	if (body instanceof ArrayBuffer) return new Uint8Array(body);
	if (ArrayBuffer.isView(body)) {
		return new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
	}
	throw new TypeError("Browser sync fetch body is not structured-cloneable");
}
