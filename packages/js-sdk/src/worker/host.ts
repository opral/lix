import { openLixBinding } from "#binding";
import type {
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
	PluginRuntimeDispatch,
} from "../binding-types.js";
import { createPluginRuntimeDispatch } from "../plugin-runtime.js";
import {
	serializeWorkerError,
	type WorkerHostEndpoint,
	type WorkerInput,
	type WorkerOperation,
	type WorkerRequest,
} from "./protocol.js";

export function startWorkerHost(endpoint: WorkerHostEndpoint): void {
	let lix: LixBinding | undefined;
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
			await respond(message, () => handleFiniteOperation(message.operation));
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
		operation: WorkerOperation,
	): Promise<unknown> {
		switch (operation.kind) {
			case "open":
				if (lix) throw workerStateError("Lix worker is already open");
				lix = await openLixBinding(
					operation.storage,
					createPluginRuntimeDispatch() as PluginRuntimeDispatch,
					operation.telemetryEnabled
						? (span) => endpoint.postMessage({ kind: "telemetry", span })
						: undefined,
				);
				return undefined;
			case "execute":
				return requiredLix().execute(
					operation.sql,
					operation.params,
					operation.options,
				);
			case "executeBatch":
				return requiredLix().executeBatch(
					operation.statements,
					operation.options,
				);
			case "beginTransaction": {
				const transaction = await requiredLix().beginTransaction();
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
				return requiredLix().activeBranchId();
			case "clientState.entries":
				return requiredClientStateMethod("clientStateEntries")();
			case "clientState.get":
				return requiredClientStateMethod("clientStateGet")(operation.key);
			case "clientState.set":
				return requiredClientStateMethod("clientStateSet")(
					operation.key,
					operation.value,
				);
			case "clientState.delete":
				return requiredClientStateMethod("clientStateDelete")(operation.key);
			case "createBranch":
				return requiredLix().createBranch(operation.options);
			case "switchBranch":
				return requiredLix().switchBranch(operation.options);
			case "mergeBranchPreview":
				return requiredLix().mergeBranchPreview(operation.options);
			case "mergeBranch":
				return requiredLix().mergeBranch(operation.options);
			case "importFilesystemPaths":
				return requiredLix().importFilesystemPaths(operation.paths);
			case "syncDiskToLix":
				return requiredLix().syncDiskToLix();
			case "exportSnapshot": {
				const lix = requiredLix();
				const exportSnapshot = lix.exportSnapshot;
				if (!exportSnapshot) {
					throw workerStateError(
						"The open Lix storage does not support snapshot export",
					);
				}
				return exportSnapshot.call(lix);
			}
			case "observe": {
				const events = await requiredLix().observe(
					operation.sql,
					operation.params,
				);
				const observeId = nextObserveId++;
				observations.set(observeId, events);
				return observeId;
			}
			case "close": {
				const openLix = requiredLix();
				await openLix.close();
				for (const events of observations.values()) events.close();
				observations.clear();
				transactions.clear();
				lix = undefined;
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

	function requiredLix(): LixBinding {
		if (!lix) throw workerStateError("Lix worker is closed");
		return lix;
	}

	function requiredClientStateMethod<
		Key extends
			| "clientStateEntries"
			| "clientStateGet"
			| "clientStateSet"
			| "clientStateDelete",
	>(key: Key): NonNullable<LixBinding[Key]> {
		const method = requiredLix()[key];
		if (!method) {
			throw workerStateError(
				"The open Lix binding does not support typed client state",
			);
		}
		return method.bind(requiredLix()) as NonNullable<LixBinding[Key]>;
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
