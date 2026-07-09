import { createWorkerConnection } from "#worker-factory";
import type { LixBackendConfig } from "../binding-types.js";
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
	backend: LixBackendConfig,
	onDisposed?: () => void,
): Promise<LixWorkerClient> {
	const client = new LixWorkerClient(onDisposed);
	try {
		await client.request({ kind: "open", backend });
		return client;
	} catch (error) {
		await client.terminate();
		throw error;
	}
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
