import { createWorkerConnection } from "#worker-factory";
import type {
	LixBinding,
	LixStorageConfig,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "../binding-types.js";
import type { LixTelemetryOptions } from "../types.js";
import {
	deserializeWorkerError,
	type WorkerConnection,
	type WorkerNotification,
	type WorkerOperation,
	type WorkerResponse,
} from "./protocol.js";

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
): Promise<LixWorkerClient> {
	let client = idleWorkers.pop();
	while (client?.isDisposed) client = idleWorkers.pop();
	client ??= new LixWorkerClient();
	client.beginLease(onDisposed, telemetry);
	try {
		await client.request({
			kind: "open",
			storage,
			telemetryEnabled: telemetry !== undefined,
		});
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
): Promise<LixBinding> {
	const client = await openLixWorker(storage, onDisposed, telemetry);
	return workerBinding(client);
}

function workerBinding(client: LixWorkerClient): LixBinding {
	let closed = false;
	const request: RequestWorker = (operation) => {
		if (closed) return Promise.reject(workerClosedError());
		return client.request(operation);
	};
	const notify: NotifyWorker = (notification) => {
		if (!closed) client.notify(notification);
	};

	return {
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
		createBranch: (options) =>
			request({ kind: "createBranch", options }),
		switchBranch: (options) =>
			request({ kind: "switchBranch", options }),
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
			await releaseWorker(client);
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
	): void {
		if (this.disposed || this.leased) throw workerClosedError();
		this.leased = true;
		this.onDisposed = onDisposed;
		this.telemetry = telemetry;
	}

	endLease(): void {
		if (!this.leased) return;
		this.leased = false;
		const onDisposed = this.onDisposed;
		this.onDisposed = undefined;
		this.telemetry = undefined;
		onDisposed?.();
	}

	request<T>(operation: WorkerOperation): Promise<T> {
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
				this.connection.postMessage({ id, operation });
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
			try {
				this.telemetry?.onSpan(message.span);
			} catch {
				// Telemetry callbacks are isolated from Lix operation results.
			}
			return;
		}
		const pending = this.pending.get(message.id);
		if (!pending) return;
		this.pending.delete(message.id);
		if (this.pending.size === 0) this.connection.unref();
		if (message.ok) pending.resolve(message.value);
		else pending.reject(deserializeWorkerError(message.error));
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

function workerClosedError(): Error & { code: string } {
	const error = new Error("Lix worker is closed") as Error & { code: string };
	error.name = "LixError";
	error.code = "LIX_ERROR_CLOSED";
	return error;
}
