import { createWorkerConnection, openDirectLixBinding } from "#worker-factory";
import type {
	LixBinding,
	LixStorageConfig,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "../binding-types.js";
import type {
	LixTelemetryOptions,
	RemoteLixFetch,
	SyncLixServerOptions,
} from "../types.js";
import {
	deserializeWorkerError,
	serializeWorkerError,
	type WorkerConnection,
	type WorkerNotification,
	type WorkerOperation,
	type WorkerResponse,
	type WorkerSyncServerOptions,
} from "./protocol.js";

type SyncServerRuntimeOptions = Omit<SyncLixServerOptions, "mode">;

type PendingRequest = {
	resolve(value: unknown): void;
	reject(error: unknown): void;
};

type RequestWorker = <T>(operation: WorkerOperation) => Promise<T>;
type NotifyWorker = (notification: WorkerNotification) => void;

const MAX_IDLE_WORKERS = 1;
// The common serial reopen path retains one worker so its prepared plugin cache
// survives close(). Concurrent opens still receive isolated workers.
const idleWorkers: LixWorkerClient[] = [];

export async function openLixWorker(
	storage: LixStorageConfig,
	onDisposed?: () => void,
	telemetry?: LixTelemetryOptions,
	server?: SyncServerRuntimeOptions,
): Promise<LixWorkerClient> {
	let client = idleWorkers.pop();
	while (client?.isDisposed) client = idleWorkers.pop();
	client ??= new LixWorkerClient();
	client.beginLease(onDisposed, telemetry, server);
	try {
		await client.request(
			{
				kind: "open",
				storage,
				telemetryEnabled: telemetry !== undefined,
				server: serializeSyncServer(server),
			},
			0,
		);
		return client;
	} catch (error) {
		await client.terminate();
		throw error;
	}
}

/** Opens the local worker transport behind the semantic Lix binding. */
export async function openLixWorkerBinding(
	storage: LixStorageConfig,
	onDisposed?: () => void,
	telemetry?: LixTelemetryOptions,
	server?: SyncServerRuntimeOptions,
): Promise<LixBinding> {
	if (openDirectLixBinding && telemetry?.parentContext === undefined) {
		const telemetryDispatch = telemetry
			? (span: Parameters<LixTelemetryOptions["onSpan"]>[0]) => {
					try {
						telemetry.onSpan(span);
					} catch {
						// Telemetry is observational and must not fail engine commands.
					}
				}
			: undefined;
		const binding = await openDirectLixBinding(
			storage,
			telemetryDispatch,
			undefined,
			await resolveDirectSyncServer(server),
		);
		if (binding) {
			if (!onDisposed) return binding;
			return wrapDirectBinding(binding, new BindingLease(onDisposed));
		}
	}
	const client = await openLixWorker(storage, onDisposed, telemetry, server);
	return workerBinding(
		client,
		new BindingLease(() => releaseWorker(client)),
		0,
	);
}

class BindingLease {
	private references = 1;
	constructor(private readonly releaseLast: () => void | Promise<void>) {}
	retain(): void {
		this.references += 1;
	}
	async release(): Promise<void> {
		this.references -= 1;
		if (this.references === 0) await this.releaseLast();
	}
}

function wrapDirectBinding(
	binding: LixBinding,
	lease: BindingLease,
): LixBinding {
	let closed = false;
	return new Proxy(binding, {
		get(target, property, receiver) {
			if (property === "openAnotherSession") {
				return async (
					options: Parameters<LixBinding["openAnotherSession"]>[0],
				) => {
					const opened = await target.openAnotherSession(options);
					lease.retain();
					return wrapDirectBinding(opened, lease);
				};
			}
			if (property === "close") {
				return async () => {
					if (closed) return;
					try {
						await target.close();
					} finally {
						closed = true;
						await lease.release();
					}
				};
			}
			const value = Reflect.get(target, property, receiver) as unknown;
			return typeof value === "function" ? value.bind(target) : value;
		},
	});
}

function workerBinding(
	client: LixWorkerClient,
	lease: BindingLease,
	sessionId: number,
): LixBinding {
	let closed = false;
	const request: RequestWorker = (operation) => {
		if (closed) return Promise.reject(workerClosedError());
		return client.request(operation, sessionId);
	};
	const notify: NotifyWorker = (notification) => {
		if (!closed) client.notify(notification);
	};

	return {
		setTelemetryParent: () => {},
		openAnotherSession: async (options) => {
			const openedSessionId = await request<number>({
				kind: "openAnotherSession",
				options,
			});
			lease.retain();
			return workerBinding(client, lease, openedSessionId);
		},
		execute: (sql, params, options) =>
			request({ kind: "execute", sql, params, options }),
		executeBatch: (statements, options) =>
			request({ kind: "executeBatch", statements, options }),
		observe: async (sql, params) => {
			const observeId = await request<number>({
				kind: "observe",
				sql,
				params,
			});
			return workerObserveBinding(request, notify, observeId);
		},
		beginTransaction: async () => {
			const transactionId = await request<number>({
				kind: "beginTransaction",
			});
			return workerTransactionBinding(request, transactionId);
		},
		activeBranchId: () => request({ kind: "activeBranchId" }),
		activeAccountId: () => request({ kind: "activeAccountId" }),
		createBranch: (options) => request({ kind: "createBranch", options }),
		createCheckpoint: () => request({ kind: "createCheckpoint" }),
		undo: () => request({ kind: "undo" }),
		redo: () => request({ kind: "redo" }),
		switchBranch: (options) => request({ kind: "switchBranch", options }),
		importFilesystemPaths: (paths) =>
			request({ kind: "importFilesystemPaths", paths }),
		mergeBranchPreview: (options) =>
			request({ kind: "mergeBranchPreview", options }),
		mergeBranch: (options) => request({ kind: "mergeBranch", options }),
		syncDiskToLix: () => request({ kind: "syncDiskToLix" }),
		close: async () => {
			if (closed) return;
			await request({ kind: "close" });
			closed = true;
			await lease.release();
		},
	};
}

function workerTransactionBinding(
	request: RequestWorker,
	transactionId: number,
): LixTransactionBinding {
	return {
		execute: (sql, params, options) =>
			request({
				kind: "transaction.execute",
				transactionId,
				sql,
				params,
				options,
			}),
		commit: () => request({ kind: "transaction.commit", transactionId }),
		rollback: () => request({ kind: "transaction.rollback", transactionId }),
	};
}

function workerObserveBinding(
	request: RequestWorker,
	notify: NotifyWorker,
	observeId: number,
): ObserveEventsBinding {
	return {
		next: () => request({ kind: "observe.next", observeId }),
		close: () => notify({ kind: "observe.close", observeId }),
	};
}

async function releaseWorker(client: LixWorkerClient): Promise<void> {
	client.endLease();
	if (!client.isDisposed && idleWorkers.length < MAX_IDLE_WORKERS) {
		idleWorkers.push(client);
		return;
	}
	await client.terminate();
}

export class LixWorkerClient {
	private nextRequestId = 1;
	private readonly pending = new Map<number, PendingRequest>();
	private disposed = false;
	private leased = false;
	private onDisposed?: () => void;
	private telemetry?: LixTelemetryOptions;
	private syncServer?: SyncServerRuntimeOptions;
	private readonly syncFetchControllers = new Map<number, AbortController>();

	constructor(
		private readonly connection: WorkerConnection = createWorkerConnection(),
	) {
		connection.onMessage((message) => this.handleMessage(message));
		connection.onFatal((error) => this.handleFatal(error));
	}

	get isDisposed(): boolean {
		return this.disposed;
	}

	beginLease(
		onDisposed?: () => void,
		telemetry?: LixTelemetryOptions,
		syncServer?: SyncServerRuntimeOptions,
	): void {
		if (this.disposed || this.leased) throw workerClosedError();
		this.leased = true;
		this.onDisposed = onDisposed;
		this.telemetry = telemetry;
		this.syncServer = syncServer;
	}

	endLease(): void {
		if (!this.leased) return;
		this.leased = false;
		const onDisposed = this.onDisposed;
		this.onDisposed = undefined;
		this.telemetry = undefined;
		this.syncServer = undefined;
		for (const controller of this.syncFetchControllers.values()) controller.abort();
		this.syncFetchControllers.clear();
		onDisposed?.();
	}

	request<T>(operation: WorkerOperation, sessionId = 0): Promise<T> {
		if (this.disposed || !this.leased) {
			return Promise.reject(workerClosedError());
		}
		const id = this.nextRequestId++;
		if (this.pending.size === 0) this.connection.ref();
		return new Promise<T>((resolve, reject) => {
			this.pending.set(id, {
				resolve: (value) => resolve(value as T),
				reject,
			});
			try {
			this.connection.postMessage({
				id,
				sessionId,
				telemetryParent: this.telemetry?.parentContext?.(),
				operation,
			});
			} catch (error) {
				this.pending.delete(id);
				if (this.pending.size === 0) this.connection.unref();
				reject(error);
			}
		});
	}

	notify(notification: WorkerNotification): void {
		if (this.disposed || !this.leased) return;
		try {
			this.connection.postMessage(notification);
		} catch {
			// A best-effort finalizer/close notification can race worker shutdown.
		}
	}

	async terminate(): Promise<void> {
		if (this.disposed) return;
		this.disposed = true;
		this.rejectPending(workerClosedError());
		try {
			await this.connection.terminate();
		} finally {
			this.endLease();
		}
	}

	private handleMessage(message: WorkerResponse): void {
		if ("kind" in message) {
			this.handleWorkerEvent(message);
			return;
		}
		const pending = this.pending.get(message.id);
		if (!pending) return;
		this.pending.delete(message.id);
		if (this.pending.size === 0) this.connection.unref();
		if (message.ok) pending.resolve(message.value);
		else pending.reject(deserializeWorkerError(message.error));
	}

	private handleWorkerEvent(message: Extract<WorkerResponse, { kind: string }>): void {
		switch (message.kind) {
			case "telemetry":
				try {
					this.telemetry?.onSpan(message.span);
				} catch {
					// Telemetry callbacks are isolated from Lix operation results.
				}
				break;
			case "sync.headers":
				void this.resolveSyncHeaders(message.requestId);
				break;
			case "sync.fetch":
				void this.resolveSyncFetch(message.requestId, message.request);
				break;
			case "sync.fetch.cancel":
				this.syncFetchControllers.get(message.requestId)?.abort();
				this.syncFetchControllers.delete(message.requestId);
				break;
		}
	}

	private async resolveSyncHeaders(requestId: number): Promise<void> {
		try {
			const source = this.syncServer?.headers;
			const headers = typeof source === "function" ? await source() : source;
			this.notify({
				kind: "sync.headers.result",
				requestId,
				result: { ok: true, headers: headerEntries(headers) },
			});
		} catch (error) {
			this.notify({
				kind: "sync.headers.result",
				requestId,
				result: { ok: false, error: serializeWorkerError(error) },
			});
		}
	}

	private async resolveSyncFetch(
		requestId: number,
		request: import("./protocol.js").WorkerSyncFetchRequest,
	): Promise<void> {
		const fetcher: RemoteLixFetch | undefined = this.syncServer?.fetch;
		if (!fetcher) {
			this.notify({
				kind: "sync.fetch.result",
				requestId,
				result: {
					ok: false,
					error: serializeWorkerError(new Error("Sync fetch bridge is unavailable")),
				},
			});
			return;
		}
		const controller = new AbortController();
		this.syncFetchControllers.set(requestId, controller);
		try {
			const response = await fetcher(request.url, {
				method: request.method,
				headers: request.headers,
				body:
					typeof request.body === "string"
						? request.body
						: request.body?.slice().buffer,
				credentials: request.credentials,
				signal: controller.signal,
			});
			const body = await readSyncResponseBody(
				response,
				request.responseLimit,
				controller,
			);
			this.notify({
				kind: "sync.fetch.result",
				requestId,
				result: {
					ok: true,
					response: {
						status: response.status,
						statusText: response.statusText,
						headers: headerEntries(response.headers),
						body,
					},
				},
			});
		} catch (error) {
			if (!controller.signal.aborted || isSyncResponseTooLarge(error)) {
				this.notify({
					kind: "sync.fetch.result",
					requestId,
					result: { ok: false, error: serializeWorkerError(error) },
				});
			}
		} finally {
			this.syncFetchControllers.delete(requestId);
		}
	}

	private handleFatal(error: Error): void {
		if (this.disposed) return;
		this.disposed = true;
		const fatal = error as Error & { code?: string };
		fatal.name = "LixError";
		fatal.code ??= "LIX_WORKER_TERMINATED";
		this.rejectPending(fatal);
		this.endLease();
	}

	private rejectPending(error: Error): void {
		for (const pending of this.pending.values()) pending.reject(error);
		this.pending.clear();
		this.connection.unref();
	}
}

async function readSyncResponseBody(
	response: Response,
	limit: number,
	controller: AbortController,
): Promise<Uint8Array> {
	if (!Number.isSafeInteger(limit) || limit <= 0) {
		throw new TypeError("Browser sync fetch has no valid response limit");
	}
	const declaredLength = Number(response.headers.get("content-length"));
	if (Number.isFinite(declaredLength) && declaredLength > limit) {
		const error = syncResponseTooLarge(limit);
		controller.abort(error);
		throw error;
	}
	const stream = response.body;
	if (!stream) return new Uint8Array();

	const reader = stream.getReader();
	const chunks: Uint8Array[] = [];
	let total = 0;
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			total += value.byteLength;
			if (total > limit) {
				const error = syncResponseTooLarge(limit);
				controller.abort(error);
				await reader.cancel(error).catch(() => undefined);
				throw error;
			}
			chunks.push(value);
		}
	} finally {
		reader.releaseLock();
	}

	const body = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		body.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return body;
}

function syncResponseTooLarge(limit: number): Error & { code: string } {
	const error = new Error(`sync fetch response exceeds ${limit} bytes`) as Error & {
		code: string;
	};
	error.name = "LixError";
	error.code = "LIX_ERROR_SYNC_RESPONSE_TOO_LARGE";
	return error;
}

function isSyncResponseTooLarge(error: unknown): boolean {
	return (
		error instanceof Error &&
		(error as Error & { code?: string }).code ===
			"LIX_ERROR_SYNC_RESPONSE_TOO_LARGE"
	);
}

function workerClosedError(): Error & { code: string } {
	const error = new Error("Lix worker is closed") as Error & { code: string };
	error.name = "LixError";
	error.code = "LIX_ERROR_CLOSED";
	return error;
}

function serializeSyncServer(
	server: SyncServerRuntimeOptions | undefined,
): WorkerSyncServerOptions | undefined {
	if (!server) return undefined;
	return {
		url: new URL(server.url).toString(),
		headers:
			typeof server.headers === "function"
				? undefined
				: headerEntries(server.headers),
		dynamicHeaders: typeof server.headers === "function",
		customFetch: server.fetch !== undefined,
	};
}

function headerEntries(headers: HeadersInit | undefined): [string, string][] {
	const entries: [string, string][] = [];
	new Headers(headers).forEach((value, name) => entries.push([name, value]));
	return entries;
}

async function resolveDirectSyncServer(
	server: SyncServerRuntimeOptions | undefined,
): Promise<import("../binding-types.js").SyncServerBindingOptions | undefined> {
	if (!server) return undefined;
	const source = server.headers;
	const headers = typeof source === "function" ? await source() : source;
	return {
		url: new URL(server.url).toString(),
		headers: headerEntries(headers),
		fetch: server.fetch,
	};
}
