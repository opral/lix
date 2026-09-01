import { openLixBinding } from "#binding";
import type {
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
	SnapshotExportBinding,
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
	let nextSnapshotExportId = 1;
	const snapshotExports = new Map<number, SnapshotExportBinding>();
	const snapshotInputs = new Map<
		number,
		{
			readable: ReadableStream<Uint8Array>;
			writer: WritableStreamDefaultWriter<Uint8Array>;
		}
	>();
	let nextSyncRequestId = 1;
	const pendingSyncHeaders = new Map<
		number,
		{ resolve(headers: [string, string][]): void; reject(error: unknown): void }
	>();
	const pendingSyncFetch = new Map<
		number,
		{ resolve(response: WorkerSyncFetchResponse): void; reject(error: unknown): void }
	>();
	const pendingSyncStreamPulls = new Map<
		number,
		{
			controller: ReadableStreamDefaultController<Uint8Array>;
			resolve(): void;
			reject(error: unknown): void;
		}
	>();
	const syncStreamCleanup = new Map<number, () => void>();
	let finiteQueue = Promise.resolve();

	endpoint.onMessage((message: WorkerInput) => {
		if (!("id" in message)) {
			handleNotification(message);
			return;
		}
		if (
			message.operation.kind === "openSnapshot.write" ||
			message.operation.kind === "openSnapshot.finish"
		) {
			const operation = message.operation;
			void respond(message, () => handleSnapshotInput(operation));
			return;
		}
		if (
			message.operation.kind === "open" &&
			message.operation.snapshotId !== undefined
		) {
			ensureSnapshotInput(message.operation.snapshotId);
		}
		if (
			message.operation.kind === "observe.next" ||
			message.operation.kind === "exportSnapshot.next" ||
			message.operation.kind === "exportSnapshot.cancel"
		) {
			if (message.operation.kind === "observe.next") {
				const observeId = message.operation.observeId;
				void respond(message, () =>
					handleObserveNext(observeId, message.telemetryParent),
				);
			} else if (message.operation.kind === "exportSnapshot.next") {
				const exportId = message.operation.exportId;
				void respond(message, () => handleSnapshotNext(exportId));
			} else {
				const exportId = message.operation.exportId;
				void respond(message, () => handleSnapshotCancel(exportId));
			}
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
			case "openSnapshot.cancel": {
				const input = snapshotInputs.get(message.snapshotId);
				snapshotInputs.delete(message.snapshotId);
				if (input) void input.writer.abort().catch(() => undefined);
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
			case "sync.fetch.stream.result": {
				const pending = pendingSyncStreamPulls.get(message.requestId);
				pendingSyncStreamPulls.delete(message.requestId);
				if (!pending) break;
				if (!message.result.ok) {
					const error = deserializeWorkerError(message.result.error);
					pending.controller.error(error);
					pending.reject(error);
					finishSyncStream(message.requestId);
				} else if (message.result.done) {
					pending.controller.close();
					pending.resolve();
					finishSyncStream(message.requestId);
				} else {
					pending.controller.enqueue(message.result.chunk);
					pending.resolve();
				}
				break;
			}
		}
	}

	function finishSyncStream(requestId: number): void {
		const cleanup = syncStreamCleanup.get(requestId);
		syncStreamCleanup.delete(requestId);
		cleanup?.();
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
					const snapshot =
						operation.snapshotId === undefined
							? undefined
							: requiredSnapshotInput(operation.snapshotId).readable;
					try {
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
						snapshot,
					);
					sessions.set(0, opened);
					return opened.openReport?.() satisfies LixOpenReport | undefined;
					} finally {
						if (operation.snapshotId !== undefined) {
							snapshotInputs.delete(operation.snapshotId);
						}
					}
				}
			case "openSnapshot.write":
			case "openSnapshot.finish":
				throw workerStateError("snapshot input uses the restore lane");
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
			case "exportSnapshot":
				{
					const binding = requiredLix(sessionId);
					const exportSnapshot = binding.exportSnapshot;
					if (!exportSnapshot) {
						throw workerStateError("this Lix binding cannot export snapshots");
					}
					const snapshot = exportSnapshot.call(binding);
					const exportId = nextSnapshotExportId++;
					snapshotExports.set(exportId, snapshot);
					return exportId;
				}
			case "exportSnapshot.next":
				throw workerStateError("snapshot pulls bypass the finite operation queue");
			case "exportSnapshot.cancel":
				throw workerStateError("snapshot cancellation bypasses the finite operation queue");
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

	async function handleSnapshotNext(
		exportId: number,
	): Promise<Uint8Array | undefined> {
		const snapshot = snapshotExports.get(exportId);
		if (!snapshot) return undefined;
		try {
			const chunk = await snapshot.next();
			if (chunk == null) snapshotExports.delete(exportId);
			return chunk ?? undefined;
		} catch (error) {
			snapshotExports.delete(exportId);
			await Promise.resolve(snapshot.cancel()).catch(() => undefined);
			throw error;
		}
	}

	async function handleSnapshotCancel(exportId: number): Promise<void> {
		const snapshot = snapshotExports.get(exportId);
		snapshotExports.delete(exportId);
		if (snapshot) await snapshot.cancel();
	}

	function ensureSnapshotInput(snapshotId: number): void {
		if (snapshotInputs.has(snapshotId)) return;
		const stream = new TransformStream<Uint8Array, Uint8Array>(
			undefined,
			{ highWaterMark: 0 },
			{ highWaterMark: 0 },
		);
		snapshotInputs.set(snapshotId, {
			readable: stream.readable,
			writer: stream.writable.getWriter(),
		});
	}

	function requiredSnapshotInput(snapshotId: number) {
		const input = snapshotInputs.get(snapshotId);
		if (!input) throw workerStateError("snapshot restore input is closed");
		return input;
	}

	async function handleSnapshotInput(
		operation: Extract<
			WorkerOperation,
			{ kind: "openSnapshot.write" | "openSnapshot.finish" }
		>,
	): Promise<void> {
		const input = requiredSnapshotInput(operation.snapshotId);
		if (operation.kind === "openSnapshot.write") {
			await input.writer.write(operation.chunk);
			return;
		}
		await input.writer.close();
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
		const extension = init as
			| (RequestInit & {
					lixResponseLimit?: unknown;
					lixResponseStream?: unknown;
			  })
			| undefined;
		const streaming = extension?.lixResponseStream === true;
		const responseLimit = extension?.lixResponseLimit;
		if (
			!streaming &&
			(typeof responseLimit !== "number" ||
				!Number.isSafeInteger(responseLimit) ||
				responseLimit <= 0)
		) {
			throw new TypeError("Browser sync fetch has no valid response limit");
		}
		const requestId = nextSyncRequestId++;
		const requestBase = {
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
		};
		const request: WorkerSyncFetchRequest = streaming
			? { ...requestBase, responseMode: "stream" }
			: {
					...requestBase,
					responseMode: "buffered",
					responseLimit: responseLimit as number,
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
			}
			const pull = pendingSyncStreamPulls.get(requestId);
			pendingSyncStreamPulls.delete(requestId);
			if (pull) {
				const error = new DOMException("The operation was aborted", "AbortError");
				pull.controller.error(error);
				pull.reject(error);
			}
			endpoint.postMessage({ kind: "sync.fetch.cancel", requestId });
			finishSyncStream(requestId);
		};
		if (init?.signal?.aborted) abort();
		else init?.signal?.addEventListener("abort", abort, { once: true });
		let streamEstablished = false;
		try {
			const resolved = await response;
			if (resolved.streaming) {
				const signal = init?.signal;
				if (signal?.aborted) {
					abort();
					throw new DOMException("The operation was aborted", "AbortError");
				}
				if (
					resolved.status === 204 ||
					resolved.status === 205 ||
					resolved.status === 304
				) {
					endpoint.postMessage({ kind: "sync.fetch.cancel", requestId });
					return new Response(null, {
						status: resolved.status,
						statusText: resolved.statusText,
						headers: resolved.headers,
					});
				}
				syncStreamCleanup.set(requestId, () =>
					signal?.removeEventListener("abort", abort),
				);
				const body = new ReadableStream<Uint8Array>({
					pull: (controller) =>
						new Promise<void>((resolve, reject) => {
							pendingSyncStreamPulls.set(requestId, {
								controller,
								resolve,
								reject,
							});
							endpoint.postMessage({
								kind: "sync.fetch.stream.pull",
								requestId,
							});
						}),
					cancel: abort,
				});
				const streamedResponse = new Response(body, {
					status: resolved.status,
					statusText: resolved.statusText,
					headers: resolved.headers,
				});
				streamEstablished = true;
				return streamedResponse;
			}
			return responseFromSyncFetch(resolved);
		} catch (error) {
			if (streaming && !streamEstablished) {
				endpoint.postMessage({ kind: "sync.fetch.cancel", requestId });
				finishSyncStream(requestId);
			}
			throw error;
		} finally {
			pendingSyncFetch.delete(requestId);
			if (!streamEstablished) {
				init?.signal?.removeEventListener("abort", abort);
			}
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
	if (resolved.streaming) {
		throw new TypeError("Streaming sync responses require the worker stream bridge");
	}
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
