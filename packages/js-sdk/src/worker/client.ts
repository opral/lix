import { createWorkerConnection } from "#worker-factory";
import type {
	LixBinding,
	LixStorageConfig,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "../binding-types.js";
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

export async function openLixWorker(
	storage: LixStorageConfig,
	onDisposed?: () => void,
): Promise<LixWorkerClient> {
	const client = new LixWorkerClient(onDisposed);
	try {
		await client.request({ kind: "open", storage });
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
): Promise<LixBinding> {
	const client = await openLixWorker(storage, onDisposed);
	return workerBinding(client);
}

function workerBinding(client: LixWorkerClient): LixBinding {
	return {
		execute: (sql, params, options) =>
			client.request({ kind: "execute", sql, params, options }),
		executeBatch: (statements, options) =>
			client.request({ kind: "executeBatch", statements, options }),
		observe: async (sql, params) => {
			const observeId = await client.request<number>({
				kind: "observe",
				sql,
				params,
			});
			return workerObserveBinding(client, observeId);
		},
		beginTransaction: async () => {
			const transactionId = await client.request<number>({
				kind: "beginTransaction",
			});
			return workerTransactionBinding(client, transactionId);
		},
		activeBranchId: () => client.request({ kind: "activeBranchId" }),
		createBranch: (options) =>
			client.request({ kind: "createBranch", options }),
		switchBranch: (options) =>
			client.request({ kind: "switchBranch", options }),
		importFilesystemPaths: (paths) =>
			client.request({ kind: "importFilesystemPaths", paths }),
		mergeBranchPreview: (options) =>
			client.request({ kind: "mergeBranchPreview", options }),
		mergeBranch: (options) =>
			client.request({ kind: "mergeBranch", options }),
		syncDiskToLix: () => client.request({ kind: "syncDiskToLix" }),
		close: async () => {
			await client.request({ kind: "close" });
			await client.terminate();
		},
	};
}

function workerTransactionBinding(
	client: LixWorkerClient,
	transactionId: number,
): LixTransactionBinding {
	return {
		execute: (sql, params, options) =>
			client.request({
				kind: "transaction.execute",
				transactionId,
				sql,
				params,
				options,
			}),
		commit: () =>
			client.request({ kind: "transaction.commit", transactionId }),
		rollback: () =>
			client.request({ kind: "transaction.rollback", transactionId }),
	};
}

function workerObserveBinding(
	client: LixWorkerClient,
	observeId: number,
): ObserveEventsBinding {
	return {
		next: () => client.request({ kind: "observe.next", observeId }),
		close: () => client.notify({ kind: "observe.close", observeId }),
	};
}

export class LixWorkerClient {
	private nextRequestId = 1;
	private readonly pending = new Map<number, PendingRequest>();
	private disposed = false;

	constructor(
		private readonly onDisposed?: () => void,
		private readonly connection: WorkerConnection = createWorkerConnection(),
	) {
		connection.onMessage((message) => this.handleMessage(message));
		connection.onFatal((error) => this.handleFatal(error));
	}

	request<T>(operation: WorkerOperation): Promise<T> {
		if (this.disposed) return Promise.reject(workerClosedError());
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
		if (this.disposed) return;
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
			this.onDisposed?.();
		}
	}

	private handleMessage(message: WorkerResponse): void {
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
		this.onDisposed?.();
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
