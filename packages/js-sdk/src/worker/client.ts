import { operationDeadline, lostOperationError } from "./request-lifecycle.js";
import {
	WORKER_CLIENT_MAX_CONTROL_PENDING,
	WORKER_CLIENT_MAX_ORDINARY_PENDING,
	WORKER_CLIENT_MAX_OBSERVER_CLOSE_PENDING,
	WORKER_CLIENT_MAX_PENDING,
	WORKER_OPERATION_QUEUE_WAIT_MS,
	workerQueueFullError,
} from "./operation-scheduler.js";
import { emitOpenProgress } from "../open-progress.js";
import { fetchTransport, type HttpTransport } from "../http-transport.js";
import {
	createWorkerConnection,
	createRepositoryConnection,
	openDirectLixBinding,
} from "#worker-factory";
import type {
	BindingExecuteResult,
	LixBinding,
	LixStorageConfig,
	LixTransactionBinding,
	ObserveEventsBinding,
	QueryStreamBinding,
	TelemetryParentContext,
} from "../binding-types.js";
import type {
	LixTelemetryOptions,
	LixOpenProgress,
	LixOpenReport,
	LixServerOptions,
} from "../types.js";
import { ownedSnapshotRestoreChunks } from "../snapshot-restore.js";
import {
	deserializeWorkerError,
	serializeWorkerError,
	isSessionCloseRequest,
	type WorkerConnection,
	type WorkerNotification,
	type WorkerOperation,
	type WorkerResponse,
	type WorkerSyncServerOptions,
} from "./protocol.js";

type SyncServerRuntimeOptions = LixServerOptions & {
	transport?: HttpTransport;
};

type PendingRequest = {
	operation: WorkerOperation;
	category: "ordinary" | "observer-close" | "control";
	timer?: ReturnType<typeof setTimeout>;
	abortSignal?: AbortSignal;
	abortListener?: () => void;
	resolve(value: unknown): void;
	reject(error: unknown): void;
};

type RequestWorker = <T>(
	operation: WorkerOperation,
	telemetryParent?: TelemetryParentContext,
	signal?: AbortSignal,
) => Promise<T>;
type NotifyWorker = (notification: WorkerNotification) => void;

const MAX_IDLE_WORKERS = 1;
// The common serial reopen path retains one worker so its prepared plugin cache
// survives close(). Concurrent opens still receive isolated workers.
const idleWorkers: LixWorkerClient[] = [];

function workerOperationCategory(
	operation: WorkerOperation,
): PendingRequest["category"] {
	if (operation.kind === "observe.close") return "observer-close";
	if (
		operation.kind === "close" ||
		operation.kind === "exportSnapshot.cancel" ||
		operation.kind === "stream.cancel" ||
		operation.kind === "transaction.commit" ||
		operation.kind === "transaction.rollback" ||
		operation.kind === "openSnapshot.finish"
	)
		return "control";
	return "ordinary";
}

export async function openLixWorker(
	storage: LixStorageConfig,
	onDisposed?: () => void,
	telemetry?: LixTelemetryOptions,
	server?: SyncServerRuntimeOptions,
	onProgress?: (progress: LixOpenProgress) => void,
	snapshot?: ReadableStream<Uint8Array>,
): Promise<LixWorkerClient> {
	const providerOptions = storage.kind === "jsStorage" ? storage.options : undefined;
	const sharedKey =
		!snapshot &&
		providerOptions &&
		typeof providerOptions === "object" &&
		"sharedEngineKey" in providerOptions &&
		typeof providerOptions.sharedEngineKey === "string" &&
		providerOptions.sharedEngineKey.startsWith("lix:opfs:")
			? providerOptions.sharedEngineKey
			: undefined;
	const sharedConnection = sharedKey
		? createRepositoryConnection(sharedKey)
		: undefined;
	let client = sharedConnection ? new LixWorkerClient(sharedConnection, false) : idleWorkers.pop();
	while (client?.isDisposed) client = idleWorkers.pop();
	client ??= new LixWorkerClient();
	client.beginLease(onDisposed, telemetry, server, onProgress);
	let snapshotReader: ReadableStreamDefaultReader<Uint8Array> | undefined;
	let snapshotPumpStarted = false;
	try {
		// Lock the source before telling the worker to start a restore. A locked or
		// otherwise unusable stream therefore cannot leave a worker open pending input.
		snapshotReader = snapshot?.getReader();
		const snapshotId = snapshotReader
			? client.allocateSnapshotInputId()
			: undefined;
		const open = client.request<LixOpenReport | undefined>(
			{
				kind: "open",
				storage,
				telemetryEnabled: telemetry !== undefined,
				progressEnabled: onProgress !== undefined,
				snapshotId,
				server: serializeSyncServer(server),
			},
			0,
		);
		if (snapshotReader && snapshotId !== undefined) {
			snapshotPumpStarted = true;
			await pumpSnapshotToWorker(client, snapshotReader, snapshotId, open);
		}
		client.openReport = await open;
		return client;
	} catch (error) {
		if (snapshotReader && !snapshotPumpStarted) {
			await snapshotReader.cancel(error).catch(() => undefined);
			snapshotReader.releaseLock();
		}
		await client.terminate().catch(() => undefined);
		throw error;
	}
}

export async function pumpSnapshotToWorker(
	client: LixWorkerClient,
	reader: ReadableStreamDefaultReader<Uint8Array>,
	snapshotId: number,
	open: Promise<unknown>,
): Promise<void> {
	let producerError = false;
	// Convert rejection into data immediately so an early worker-open failure is
	// always observed, even while or after a source read is pending.
	const openCompletion = open.then(
		(value) => ({ kind: "open-complete", value }) as const,
		(error: unknown) => ({ kind: "open-error", error }) as const,
	);
	const waitForSnapshotRequest = async (request: Promise<unknown>): Promise<boolean> => {
		const requestCompletion = request.then(
			(value) => ({ kind: "request-complete", value }) as const,
			(error: unknown) => ({ kind: "request-error", error }) as const,
		);
		const outcome = await Promise.race([requestCompletion, openCompletion]);
		if (outcome.kind === "open-error") throw outcome.error;
		if (outcome.kind === "open-complete") {
			// A successful open normally consumes through EOF. If it completed early,
			// stop the producer and wait for the in-flight write's host-side cleanup.
			await reader.cancel().catch(() => undefined);
			client.notify({ kind: "openSnapshot.cancel", snapshotId });
			await requestCompletion;
			return false;
		}
		if (outcome.kind === "request-error") throw outcome.error;
		return true;
	};
	try {
		while (true) {
			const outcome = await Promise.race([
				reader.read().then(
					(result) => ({ kind: "source", result }) as const,
					(error: unknown) => ({ kind: "source-error", error }) as const,
				),
				openCompletion,
			]);
			if (outcome.kind === "open-error") throw outcome.error;
			if (outcome.kind === "open-complete") {
				// Success cannot normally precede EOF, but if a binding completes early,
				// stop consuming the now-unneeded producer without reporting an error.
				await reader.cancel().catch(() => undefined);
				return;
			}
			if (outcome.kind === "source-error") {
				producerError = true;
				throw outcome.error;
			}
			const read = outcome.result;
			if (read.done) break;
			if (!(read.value instanceof Uint8Array)) {
				producerError = true;
				throw new TypeError("snapshot stream chunks must be Uint8Array values");
			}
			for (const chunk of ownedSnapshotRestoreChunks(read.value)) {
				if (!(await waitForSnapshotRequest(client.request({
					kind: "openSnapshot.write",
					snapshotId,
					chunk,
				})))) return;
			}
		}
		if (!(await waitForSnapshotRequest(
			client.request({ kind: "openSnapshot.finish", snapshotId }),
		))) return;
	} catch (error) {
		await reader.cancel(error).catch(() => undefined);
		client.notify({ kind: "openSnapshot.cancel", snapshotId });
		try {
			await open;
		} catch (openError) {
			// Decoder errors are more useful than the restore-lane write/close
			// rejection. Producer errors remain authoritative.
			if (!producerError) throw openError;
		}
		throw error;
	} finally {
		reader.releaseLock();
	}
}

/** Opens the local worker transport behind the semantic Lix binding. */
export async function openLixWorkerBinding(
	storage: LixStorageConfig,
	onDisposed?: () => void,
	telemetry?: LixTelemetryOptions,
	server?: SyncServerRuntimeOptions,
	onProgress?: (progress: LixOpenProgress) => void,
	snapshot?: ReadableStream<Uint8Array>,
): Promise<LixBinding> {
	return await openLixWorkerBindingInner(
		storage,
		onDisposed,
		telemetry,
		server,
		onProgress ? (value) => emitOpenProgress(onProgress, value) : undefined,
		snapshot,
	);
}

async function openLixWorkerBindingInner(
	storage: LixStorageConfig,
	onDisposed?: () => void,
	telemetry?: LixTelemetryOptions,
	server?: SyncServerRuntimeOptions,
	onProgress?: (progress: LixOpenProgress) => void,
	snapshot?: ReadableStream<Uint8Array>,
): Promise<LixBinding> {
	if (openDirectLixBinding && storage.kind === "filesystem") {
		const telemetryDispatch = telemetry
			? (request: Uint8Array) => {
					if (request.byteLength === 0) return;
					try {
						telemetry.onExport(request);
					} catch {
						// Telemetry is observational and must not fail engine commands.
					}
				}
			: undefined;
		const binding = await openDirectLixBinding(
			storage,
			telemetryDispatch,
			readTelemetryParent(telemetry?.parentContext),
			await resolveDirectSyncServer(server),
			onProgress,
			snapshot,
		);
		if (binding) {
			const operationAwareBinding = telemetry?.parentContext
				? wrapTelemetryParentBinding(binding, telemetry.parentContext)
				: binding;
			if (!onDisposed) return operationAwareBinding;
			return wrapDirectBinding(
				operationAwareBinding,
				new BindingLease(onDisposed),
			);
		}
	}
	const client = await openLixWorker(
		storage,
		onDisposed,
		telemetry,
		server,
		onProgress,
		snapshot,
	);
	return workerBinding(
		client,
		new BindingLease(() => releaseWorker(client)),
		0,
	);
}

/** @internal Preserve operation context when async observation setup resumes. */
export function wrapTelemetryParentBinding(
	binding: LixBinding,
	parentContext: NonNullable<LixTelemetryOptions["parentContext"]>,
): LixBinding {
	const prepareOperation = () => {
		const parent = readTelemetryParent(parentContext);
		binding.setTelemetryParent(parent);
		return parent;
	};
	return new Proxy(binding, {
		get(target, property, receiver) {
			if (property === "setTelemetryParent") {
				return target.setTelemetryParent.bind(target);
			}
			if (property === "openAnotherSession") {
				return async (
					options: Parameters<LixBinding["openAnotherSession"]>[0],
				) => {
					prepareOperation();
					return wrapTelemetryParentBinding(
						await target.openAnotherSession(options),
						parentContext,
					);
				};
			}
			if (property === "observe") {
				return async (
					sql: Parameters<LixBinding["observe"]>[0],
					params: Parameters<LixBinding["observe"]>[1],
					options?: Parameters<LixBinding["observe"]>[2],
				) => {
					const initialParent = prepareOperation();
					return wrapTelemetryParentObserve(
						await target.observe(sql, params, options),
						parentContext,
						initialParent,
					);
				};
			}
			if (property === "beginTransaction") {
				return async () => {
					prepareOperation();
					return wrapTelemetryParentTransaction(
						await target.beginTransaction(),
						target,
						parentContext,
					);
				};
			}
			const value = Reflect.get(target, property, receiver) as unknown;
			if (typeof value !== "function") return value;
			return (...args: unknown[]) => {
				prepareOperation();
				return Reflect.apply(value, target, args) as unknown;
			};
		},
	}) as LixBinding;
}

function wrapTelemetryParentObserve(
	events: ObserveEventsBinding,
	parentContext: NonNullable<LixTelemetryOptions["parentContext"]>,
	initialParent: ReturnType<typeof readTelemetryParent>,
): ObserveEventsBinding {
	let firstNext = true;
	return {
		setTelemetryParent: (parent) => events.setTelemetryParent(parent),
		next: () => {
			const activeParent = readTelemetryParent(parentContext);
			events.setTelemetryParent(
				activeParent ?? (firstNext ? initialParent : undefined),
			);
			firstNext = false;
			return events.next();
		},
		close: () => events.close(),
	};
}

function wrapTelemetryParentTransaction(
	transaction: LixTransactionBinding,
	binding: LixBinding,
	parentContext: NonNullable<LixTelemetryOptions["parentContext"]>,
): LixTransactionBinding {
	const prepareOperation = () =>
		binding.setTelemetryParent(readTelemetryParent(parentContext));
	return {
		execute: (sql, params, options) => {
			prepareOperation();
			return transaction.execute(sql, params, options);
		},
		commit: () => {
			prepareOperation();
			return transaction.commit();
		},
		rollback: () => {
			prepareOperation();
			return transaction.rollback();
		},
	};
}

function readTelemetryParent(
	parentContext: LixTelemetryOptions["parentContext"],
) {
	try {
		return parentContext?.();
	} catch {
		// Context is observational. A broken provider must not fail SQL calls.
		return undefined;
	}
}

/** @internal Exported only for worker lifecycle tests. */
export class BindingLease {
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

/** @internal Exported only for worker lifecycle tests. */
export function workerBinding(
	client: LixWorkerClient,
	lease: BindingLease,
	sessionId: number,
): LixBinding {
	let closed = false;
	const request: RequestWorker = (operation, telemetryParent, signal) => {
		if (closed) return Promise.reject(workerClosedError());
		return client.request(operation, sessionId, telemetryParent, signal);
	};
	const notify: NotifyWorker = (notification) => {
		if (!closed) client.notify(notification);
	};

	return {
		openReport: () => client.openReport,
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
		observe: async (sql, params, options) => {
			const initialParent = client.currentTelemetryParent();
			const observeId = await request<number>(
				{ kind: "observe", sql, params },
				initialParent,
				options?.signal,
			);
			return workerObserveBinding(
				request,
				observeId,
				initialParent,
				() => client.currentTelemetryParent(),
			);
		},
		stream: async (sql, params, options) => {
			const streamId = await request<number>({
				kind: "stream",
				sql,
				params,
				options,
			});
			return workerQueryStreamBinding(request, streamId);
		},
		beginTransaction: async () => {
			const transactionId = await request<number>({
				kind: "beginTransaction",
			});
			return workerTransactionBinding(request, transactionId);
		},
		replicaRecoverySources: () => request({ kind: "replicaRecoverySources" }),
		exportReplicaRecovery: (id) =>
			request({ kind: "exportReplicaRecovery", id }),
		recoverReplica: (id) => request({ kind: "recoverReplica", id }),
        recoverReplicaWithServer: (id, server) => client.withRecoveryServer(server, (transportScope, serialized) => request({kind:"recoverReplicaWithServer",id,server:serialized,transportScope})),
		syncHealth: () => request({ kind: "syncHealth" }),
		prepareOfflineEditing: () => request({ kind: "prepareOfflineEditing" }),
		activeBranchId: () => request({ kind: "activeBranchId" }),
		activeAccountId: () => request({ kind: "activeAccountId" }),
		createBranch: (options) => request({ kind: "createBranch", options }),
		switchBranch: (options) => request({ kind: "switchBranch", options }),
		importFilesystemPaths: (paths) =>
			request({ kind: "importFilesystemPaths", paths }),
		mergeBranchPreview: (options) =>
			request({ kind: "mergeBranchPreview", options }),
		mergeBranch: (options) => request({ kind: "mergeBranch", options }),
		syncDiskToLix: () => request({ kind: "syncDiskToLix" }),
		createHosted: (server) => request({ kind: "hosted.createFrom", server }),
		exportSnapshot: () => {
			const exportId = request<number>({ kind: "exportSnapshot" });
			let canceled = false;
			return {
				next: async () => {
					if (canceled) return undefined;
					return request<Uint8Array | undefined>({
						kind: "exportSnapshot.next",
						exportId: await exportId,
					});
				},
				cancel: async () => {
					if (canceled) return;
					canceled = true;
					await request({
						kind: "exportSnapshot.cancel",
						exportId: await exportId,
					});
				},
			};
		},
		close: async () => {
			if (closed) return;
			try {
				await request({ kind: "close" });
			} catch (error) {
				// A failed semantic close may leave the worker host retaining its OPFS
				// session. Terminating the worker is the ownership backstop before an
				// outer caller can safely release its cross-tab lock.
				await client.terminate().catch(() => undefined);
				throw error;
			} finally {
				closed = true;
				await lease.release();
			}
		},
	};
}

function workerQueryStreamBinding(
	request: RequestWorker,
	streamId: number,
): QueryStreamBinding {
	let finished = false;
	return {
		next: async () => {
			if (finished) return undefined;
			const page = await request<BindingExecuteResult | undefined>({
				kind: "stream.next",
				streamId,
			});
			if (page == null) finished = true;
			return page;
		},
		cancel: async () => {
			if (finished) return;
			finished = true;
			await request({ kind: "stream.cancel", streamId });
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
	observeId: number,
	initialParent: TelemetryParentContext | undefined,
	currentParent: () => TelemetryParentContext | undefined,
): ObserveEventsBinding {
	let firstNext = true;
	return {
		setTelemetryParent: () => {},
		next: () => {
			const parent = currentParent() ?? (firstNext ? initialParent : undefined);
			firstNext = false;
			return request({ kind: "observe.next", observeId }, parent);
		},
		close: () => request({ kind: "observe.close", observeId }).then(() => undefined),
	};
}

async function releaseWorker(client: LixWorkerClient): Promise<void> {
	if (client.reusable && !client.isDisposed && idleWorkers.length < MAX_IDLE_WORKERS) {
		client.endLease();
		idleWorkers.push(client);
		return;
	}
	await client.terminate();
}

export class LixWorkerClient {
	private nextRequestId = 1;
	private nextSnapshotInputId = 1;
	private readonly pending = new Map<number, PendingRequest>();
	private disposed = false;
	private terminating = false;
	private leased = false;
	private onDisposed?: () => void;
	private telemetry?: LixTelemetryOptions;
	private syncServer?: SyncServerRuntimeOptions;
	private scopedServers = new Map<number, SyncServerRuntimeOptions>();
	private nextTransportScope = 1;
	async withRecoveryServer<T>(server: import("../binding-types.js").SyncServerBindingOptions, operation: (scope: number, server: WorkerSyncServerOptions) => Promise<T>): Promise<T> {
        const scope = this.nextTransportScope++;
        const runtime = {url: server.url, headers: server.headerProvider ?? server.headers, transport: server.transport};
        this.scopedServers.set(scope, runtime);
        try { return await operation(scope, serializeSyncServer(runtime)!); }
        finally { this.scopedServers.delete(scope); }
    }
	private onProgress?: (progress: LixOpenProgress) => void;
	openReport: LixOpenReport | undefined;
	private readonly syncFetchControllers = new Map<number, AbortController>();
	private readonly syncFetchStreams = new Map<
		number,
		ReadableStreamDefaultReader<Uint8Array> | undefined
	>();
	private readonly teardownHeaderRequests = new Set<number>();
	private readonly teardownSessionRequests = new Set<number>();

	constructor(
		private readonly connection: WorkerConnection = createWorkerConnection(),
        readonly reusable = true,
	) {
		connection.onMessage((message) => this.handleMessage(message));
		connection.onFatal((error) => this.handleFatal(error));
	}

	get isDisposed(): boolean {
		return this.disposed;
	}

	allocateSnapshotInputId(): number {
		return this.nextSnapshotInputId++;
	}

	beginLease(
		onDisposed?: () => void,
		telemetry?: LixTelemetryOptions,
		syncServer?: SyncServerRuntimeOptions,
		onProgress?: (progress: LixOpenProgress) => void,
	): void {
		if (this.disposed || this.leased) throw workerClosedError();
		this.leased = true;
		this.onDisposed = onDisposed;
		this.telemetry = telemetry;
		this.syncServer = syncServer;
		this.onProgress = onProgress;
	}

	endLease(): void {
		if (!this.leased) return;
		this.leased = false;
		const onDisposed = this.onDisposed;
		this.onDisposed = undefined;
		this.telemetry = undefined;
		this.syncServer = undefined;
		this.onProgress = undefined;
		this.openReport = undefined;
		this.abortSyncFetches();
		this.teardownHeaderRequests.clear();
		this.teardownSessionRequests.clear();
		onDisposed?.();
	}

	private abortSyncFetches(): void {
		for (const controller of this.syncFetchControllers.values())
			controller.abort();
		this.syncFetchControllers.clear();
		for (const reader of this.syncFetchStreams.values()) {
			void reader?.cancel().catch(() => undefined);
		}
		this.syncFetchStreams.clear();
	}

	currentTelemetryParent(): TelemetryParentContext | undefined {
		return readTelemetryParent(this.telemetry?.parentContext);
	}

	request<T>(
		operation: WorkerOperation,
		sessionId = 0,
		telemetryParent?: TelemetryParentContext,
		signal?: AbortSignal,
	): Promise<T> {
		if (signal?.aborted) return Promise.reject(observerRegistrationCancelledError());
		if (this.disposed || !this.leased) {
			return Promise.reject(workerClosedError());
		}
		const category = workerOperationCategory(operation);
		let controlPending = 0;
		let observerClosePending = 0;
		let ordinaryPending = 0;
		for (const pending of this.pending.values()) {
			if (pending.category === "control") controlPending++;
			else if (pending.category === "observer-close") observerClosePending++;
			else ordinaryPending++;
		}
		if (
			this.pending.size >= WORKER_CLIENT_MAX_PENDING ||
			(category === "control" && controlPending >= WORKER_CLIENT_MAX_CONTROL_PENDING) ||
			(category === "observer-close" &&
				observerClosePending >= WORKER_CLIENT_MAX_OBSERVER_CLOSE_PENDING) ||
			(category === "ordinary" && ordinaryPending >= WORKER_CLIENT_MAX_ORDINARY_PENDING)
		) {
			return Promise.reject(workerQueueFullError());
		}
		const id = this.nextRequestId++;
		if (this.pending.size === 0) this.connection.ref();
		return new Promise<T>((resolve, reject) => {
			const pendingRequest: PendingRequest = {
				operation,
				category,
				abortSignal: signal,
				resolve: (value) => {
					this.cleanupPendingRequest(pendingRequest);
					resolve(value as T);
				},
				reject: (error) => {
					this.cleanupPendingRequest(pendingRequest);
					reject(error);
				},
			};
			this.pending.set(id, pendingRequest);
			pendingRequest.timer = setTimeout(() => {
				if (!this.pending.has(id)) return;
				this.handleFatal(
					Object.assign(
						new Error(
							`Lix worker did not acknowledge request receipt within ${WORKER_OPERATION_QUEUE_WAIT_MS}ms`,
						),
						{ code: "LIX_WORKER_START_TIMEOUT" },
					),
				);
			}, WORKER_OPERATION_QUEUE_WAIT_MS);
			try {
				this.connection.postMessage({
					id,
					sessionId,
					telemetryParent: telemetryParent ?? this.currentTelemetryParent(),
					operation,
				});
			} catch (error) {
				const pending = this.pending.get(id);
				this.pending.delete(id);
				if (this.pending.size === 0) this.connection.unref();
				pending?.reject(error);
			}
			if (this.pending.has(id) && signal && operation.kind === "observe") {
				pendingRequest.abortListener = () => {
					// Keep the request pending until the host/repository settles it. If
					// registration already completed remotely, the cancel notification
					// carries the same request ID so the host can close that iterator.
					if (this.pending.has(id))
						this.notify({ kind: "observe.cancel", requestId: id });
				};
				signal.addEventListener("abort", pendingRequest.abortListener, { once: true });
				if (signal.aborted) pendingRequest.abortListener();
			}
		});
	}

	notify(notification: WorkerNotification): void {
		if (!this.leased) return;
		const teardownId = "requestId" in notification ? notification.requestId : undefined;
		const teardownResult = this.terminating && teardownId !== undefined && (
			(notification.kind === "sync.headers.result" && this.teardownHeaderRequests.has(teardownId)) ||
			(notification.kind === "sync.fetch.result" && this.teardownSessionRequests.has(teardownId))
		);
		if (this.disposed && !teardownResult) return;
		try {
			this.connection.postMessage(notification);
		} catch {
			// A best-effort finalizer/close notification can race worker shutdown.
		} finally {
			if (teardownResult && teardownId !== undefined) {
				this.teardownHeaderRequests.delete(teardownId);
				this.teardownSessionRequests.delete(teardownId);
			}
		}
	}

	async terminate(): Promise<void> {
		if (this.disposed) return;
		this.terminating = true;
		this.disposed = true;
		this.rejectPending(workerClosedError());
		this.abortSyncFetches();
		try {
			await this.connection.terminate();
		} finally {
			this.endLease();
			this.terminating = false;
			this.teardownHeaderRequests.clear();
			this.teardownSessionRequests.clear();
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

	private handleWorkerEvent(
		message: Extract<WorkerResponse, { kind: string }>,
	): void {
		if (this.disposed && !this.terminating) return;
		if (
			this.terminating &&
			message.kind !== "sync.headers" &&
			message.kind !== "sync.fetch" &&
			message.kind !== "sync.fetch.cancel"
		) return;
		switch (message.kind) {
			case "request.started": {
				const pending = this.pending.get(message.id);
				if (!pending) break;
				this.clearPendingTimer(pending);
				const milliseconds = operationDeadline(pending.operation);
				if (milliseconds !== undefined) {
					pending.timer = setTimeout(() => {
						this.handleFatal(
							Object.assign(
								new Error(
									`Lix ${pending.operation.kind} did not settle within ${milliseconds}ms`,
								),
								{
									code:
									pending.operation.kind === "open"
										? "LIX_OPEN_TIMEOUT"
										: "LIX_OPERATION_TIMEOUT",
								},
							),
						);
					}, milliseconds);
				}
				break;
			}
			case "request.queued": {
				const pending = this.pending.get(message.id);
				if (pending) this.clearPendingTimer(pending);
				break;
			}
			case "telemetry":
				try {
					this.telemetry?.onExport(message.request);
				} catch {
					// Telemetry callbacks are isolated from Lix operation results.
				}
				break;
			case "open.progress":
				try {
					this.onProgress?.(message.progress);
				} catch {
					// Open progress is observational and cannot fail repository opening.
				}
				break;
			case "sync.headers":
				if (this.terminating) this.teardownHeaderRequests.add(message.requestId);
				void this.resolveSyncHeaders(message.requestId, message.transportScope);
				break;
			case "sync.fetch":
				if (this.terminating) {
					const server = message.transportScope === undefined
						? this.syncServer
						: this.scopedServers.get(message.transportScope);
					if (!server || !isSessionCloseRequest(message.request, server.url)) break;
					this.teardownSessionRequests.add(message.requestId);
				}
				void this.resolveSyncFetch(message.requestId, message.request, message.transportScope);
				break;
			case "sync.fetch.stream.pull":
				void this.resolveSyncFetchStreamPull(message.requestId);
				break;
			case "sync.fetch.cancel":
				this.cancelSyncFetch(message.requestId);
				break;
		}
	}

	private async resolveSyncHeaders(requestId: number, transportScope?: number): Promise<void> {
		try {
			if (transportScope !== undefined && !this.scopedServers.has(transportScope)) throw workerClosedError(); const source = (transportScope === undefined ? this.syncServer : this.scopedServers.get(transportScope))?.headers;
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
        transportScope?: number,
	): Promise<void> {
        const server = transportScope === undefined ? this.syncServer : this.scopedServers.get(transportScope);
        if (!server) {
            this.notify({ kind: "sync.fetch.result", requestId, result: {ok: false, error: serializeWorkerError(workerClosedError())} });
            return;
        }
        const transport = server.transport ?? fetchTransport(server.fetch);
		const controller = new AbortController();
		this.syncFetchControllers.set(requestId, controller);
		let retainedStream = false;
		try {
			const response = await transport({ url: request.url, response: request.response, init: {
				method: request.method,
				headers: request.headers,
				body:
					typeof request.body === "string"
						? request.body
						: request.body?.slice().buffer,
				credentials: request.credentials,
				signal: controller.signal,
                cache: request.cache, redirect: request.redirect,
			}});
			if (
				this.syncFetchControllers.get(requestId) !== controller ||
				controller.signal.aborted
			) {
				await response.body?.cancel().catch(() => undefined);
				return;
			}
			if (request.response.mode === "streaming") {
				this.syncFetchStreams.set(requestId, response.body?.getReader());
				retainedStream = true;
				this.notify({
					kind: "sync.fetch.result",
					requestId,
					result: {
						ok: true,
						response: {
							status: response.status,
							statusText: response.statusText,
							headers: headerEntries(response.headers),
							streaming: true,
						},
					},
				});
				return;
			}
            const body = new Uint8Array(await response.arrayBuffer());
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
            if (isSyncResponseTooLarge(error)) controller.abort(error);
			if (!controller.signal.aborted || isSyncResponseTooLarge(error)) {
				this.notify({
					kind: "sync.fetch.result",
					requestId,
					result: { ok: false, error: serializeWorkerError(error) },
				});
			}
		} finally {
			if (!retainedStream) this.syncFetchControllers.delete(requestId);
		}
	}

	private async resolveSyncFetchStreamPull(requestId: number): Promise<void> {
		if (!this.syncFetchStreams.has(requestId)) return;
		const reader = this.syncFetchStreams.get(requestId);
		try {
			const result = reader ? await reader.read() : { done: true as const };
			if (
				!this.syncFetchStreams.has(requestId) ||
				this.syncFetchStreams.get(requestId) !== reader ||
				this.syncFetchControllers.get(requestId)?.signal.aborted
			) {
				return;
			}
			this.notify({
				kind: "sync.fetch.stream.result",
				requestId,
				result: result.done
					? { ok: true, done: true }
					: { ok: true, done: false, chunk: result.value.slice() },
			});
			if (result.done) {
				reader?.releaseLock();
				this.finishSyncFetchStream(requestId);
			}
		} catch (error) {
			const controller = this.syncFetchControllers.get(requestId);
			if (!controller?.signal.aborted) {
				this.notify({
					kind: "sync.fetch.stream.result",
					requestId,
					result: { ok: false, error: serializeWorkerError(error) },
				});
			}
			try {
				reader?.releaseLock();
			} catch {
				// A cancellation can leave the read pending until its rejection settles.
			}
			this.finishSyncFetchStream(requestId);
		}
	}

	private cancelSyncFetch(requestId: number): void {
		this.syncFetchControllers.get(requestId)?.abort();
		const reader = this.syncFetchStreams.get(requestId);
		void reader?.cancel().catch(() => undefined);
		this.finishSyncFetchStream(requestId);
	}

	private finishSyncFetchStream(requestId: number): void {
		this.syncFetchStreams.delete(requestId);
		this.syncFetchControllers.delete(requestId);
	}

	private handleFatal(error: Error): void {
		if (this.disposed) return;
		this.disposed = true;
		const fatal = error as Error & { code?: string };
		fatal.name = "LixError";
		fatal.code ??= "LIX_WORKER_TERMINATED";
		this.rejectPending(fatal);
		this.endLease();
		void this.connection.terminate().catch(() => undefined);
	}

	private rejectPending(error: Error): void {
		for (const pending of this.pending.values())
			pending.reject(lostOperationError(pending.operation, error));
		this.pending.clear();
		this.connection.unref();
	}

	private clearPendingTimer(pending: PendingRequest): void {
		if (pending.timer !== undefined) clearTimeout(pending.timer);
		pending.timer = undefined;
	}

	private cleanupPendingRequest(pending: PendingRequest): void {
		this.clearPendingTimer(pending);
		if (pending.abortSignal && pending.abortListener)
			pending.abortSignal.removeEventListener("abort", pending.abortListener);
		pending.abortSignal = undefined;
		pending.abortListener = undefined;
	}
}

function isSyncResponseTooLarge(error: unknown): boolean {
	return (
		error instanceof Error &&
		(error as Error & { code?: string }).code ===
			"LIX_TRANSPORT_RESPONSE_LIMIT"
	);
}

function workerClosedError(): Error & { code: string } {
	const error = new Error("Lix worker is closed") as Error & { code: string };
	error.name = "LixError";
	error.code = "LIX_ERROR_CLOSED";
	return error;
}

function observerRegistrationCancelledError(): Error & { code: string } {
	const error = new Error("Observer registration was cancelled") as Error & { code: string };
	error.name = "AbortError";
	error.code = "LIX_OBSERVER_CANCELLED";
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
		transport: server.transport ?? (server.fetch ? fetchTransport(server.fetch) : undefined),
	};
}

export async function hostedLixWorkerOperation<T>(
	operation: Extract<
		WorkerOperation,
		{ kind: "hosted.create" | "hosted.delete" }
	>,
): Promise<T> {
	const client = new LixWorkerClient();
	client.beginLease();
	try {
		return await client.request<T>(operation, 0);
	} finally {
		await client.terminate();
	}
}

export async function convertReplicaWorkerOperation(
	storage: LixStorageConfig,
	server: SyncServerRuntimeOptions,
	branchId?: string,
): Promise<void> {
	const providerOptions = storage.kind === "jsStorage" ? storage.options : undefined;
	const sharedKey = providerOptions && typeof providerOptions === "object"
  && "sharedEngineKey" in providerOptions && typeof providerOptions.sharedEngineKey === "string"
  && providerOptions.sharedEngineKey.startsWith("lix:opfs:") ? providerOptions.sharedEngineKey : undefined;
	const connection = sharedKey
		? createRepositoryConnection(sharedKey)
		: undefined;
	const client = connection ? new LixWorkerClient(connection, false) : new LixWorkerClient();
	client.beginLease(undefined,undefined,server);
	try { await client.request({kind:"replica.convert",storage,server:serializeSyncServer(server)!,branchId},0); }
 finally { await client.terminate(); }
}

export async function retryReplicaMigrationCleanupWorkerOperation(storage:LixStorageConfig,server:SyncServerRuntimeOptions):Promise<number> {
 const client=new LixWorkerClient();
 client.beginLease(undefined,undefined,server);
 try {return await client.request<number>({kind:"replica.cleanup",storage,server:serializeSyncServer(server)!},0);}
 finally {await client.terminate();}
}
