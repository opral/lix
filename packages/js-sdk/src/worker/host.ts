import { openLixBinding } from "#binding";
import type {
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "../binding-types.js";
import type { LixTelemetrySpan } from "../types.js";
import {
	serializeWorkerError,
	type WorkerHostEndpoint,
	type WorkerInput,
	type WorkerOperation,
	type WorkerRequest,
} from "./protocol.js";

export function startWorkerHost(endpoint: WorkerHostEndpoint): void {
	const sessions = new Map<number, LixBinding>();
	let nextSessionId = 1;
	let nextTransactionId = 1;
	let nextObserveId = 1;
	const transactions = new Map<number, LixTransactionBinding>();
	const observations = new Map<number, ObserveEventsBinding>();
	let finiteQueue = Promise.resolve();

	endpoint.onMessage((message: WorkerInput) => {
		if (!("id" in message)) {
			handleNotification(message);
			return;
		}
		if (message.operation.kind === "observe.next") {
			const observeId = message.operation.observeId;
			void respond(message, () => handleObserveNext(observeId));
			return;
		}
		finiteQueue = finiteQueue.then(async () => {
			await respond(message, () =>
				handleFiniteOperation(message.sessionId, message.operation),
			);
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
	): Promise<unknown> {
		switch (operation.kind) {
			case "open":
				if (sessions.size > 0)
					throw workerStateError("Lix worker is already open");
				sessions.set(
					0,
					await openLixBinding(
						operation.storage,
						operation.telemetryEnabled
							? (span: LixTelemetrySpan) =>
									endpoint.postMessage({ kind: "telemetry", span })
							: undefined,
					),
				);
				return undefined;
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
			case "restore":
				return requiredLix(sessionId).restore(operation.commitId);
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

	async function handleObserveNext(observeId: number): Promise<unknown> {
		const events = observations.get(observeId);
		if (!events) return undefined;
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

function workerStateError(message: string): Error & { code?: string } {
	const error = new Error(message) as Error & { code?: string };
	error.name = "LixError";
	error.code = "LIX_ERROR_CLOSED";
	return error;
}
