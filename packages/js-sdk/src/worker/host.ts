import { openLixBinding } from "#binding";
import type {
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
	PluginRuntimeDispatch,
} from "../binding-types.js";
import type { JsonValue } from "../types.js";
import type { NativeLixValue } from "../value.js";
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
				return clientStateEntries();
			case "clientState.get":
				return clientStateGet(operation.key);
			case "clientState.set":
				return clientStateSet(operation.key, operation.value);
			case "clientState.delete":
				return clientStateDelete(operation.key);
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

	async function clientStateEntries(): Promise<
		Array<{ key: string; value: JsonValue }>
	> {
		const openLix = requiredLix();
		if (openLix.clientStateEntries) {
			return openLix.clientStateEntries();
		}
		const result = await openLix.execute(CLIENT_STATE_ENTRIES_SQL, []);
		const keyColumn = requiredColumn(result.columns, "key");
		const valueColumn = requiredColumn(result.columns, "value");
		const entries: Array<{ key: string; value: JsonValue }> = [];
		for (const row of result.rows) {
			const key = requiredText(row[keyColumn], "client state key");
			if (!key.startsWith(CLIENT_STATE_KEY_PREFIX)) continue;
			entries.push({
				key: key.slice(CLIENT_STATE_KEY_PREFIX.length),
				value: requiredJson(row[valueColumn], "client state value"),
			});
		}
		return entries;
	}

	async function clientStateGet(key: string): Promise<JsonValue | undefined> {
		const openLix = requiredLix();
		if (openLix.clientStateGet) return openLix.clientStateGet(key);
		const result = await openLix.execute(CLIENT_STATE_GET_SQL, [
			textValue(physicalClientStateKey(key)),
		]);
		if (result.rows.length > 1) {
			throw workerStateError(
				"Client state key resolved to more than one lix_key_value row",
			);
		}
		const row = result.rows[0];
		if (!row) return undefined;
		return requiredJson(
			row[requiredColumn(result.columns, "value")],
			"client state value",
		);
	}

	async function clientStateSet(key: string, value: JsonValue): Promise<void> {
		const openLix = requiredLix();
		if (openLix.clientStateSet) {
			await openLix.clientStateSet(key, value);
			return;
		}
		await openLix.execute(CLIENT_STATE_SET_SQL, [
			textValue(physicalClientStateKey(key)),
			{ kind: "json", value },
		]);
	}

	async function clientStateDelete(key: string): Promise<void> {
		const openLix = requiredLix();
		if (openLix.clientStateDelete) {
			await openLix.clientStateDelete(key);
			return;
		}
		await openLix.execute(CLIENT_STATE_DELETE_SQL, [
			textValue(physicalClientStateKey(key)),
		]);
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

const CLIENT_STATE_KEY_PREFIX = "lix_client_state:";
const CLIENT_STATE_GET_SQL =
	"SELECT value FROM lix_key_value_by_branch " +
	"WHERE key = $1 AND lixcol_branch_id = 'global' " +
	"AND lixcol_untracked = true";
const CLIENT_STATE_ENTRIES_SQL =
	"SELECT key, value FROM lix_key_value_by_branch " +
	"WHERE lixcol_branch_id = 'global' AND lixcol_untracked = true " +
	"ORDER BY key";
const CLIENT_STATE_SET_SQL =
	"INSERT INTO lix_key_value_by_branch " +
	"(key, value, lixcol_branch_id, lixcol_global, lixcol_untracked) " +
	"VALUES ($1, $2, 'global', true, true) " +
	"ON CONFLICT (key, lixcol_branch_id) " +
	"DO UPDATE SET value = excluded.value";
const CLIENT_STATE_DELETE_SQL =
	"DELETE FROM lix_key_value_by_branch " +
	"WHERE key = $1 AND lixcol_branch_id = 'global' " +
	"AND lixcol_untracked = true";

function physicalClientStateKey(key: string): string {
	if (key.length === 0) {
		throw workerStateError("Client state key must be a non-empty string");
	}
	return `${CLIENT_STATE_KEY_PREFIX}${key}`;
}

function textValue(value: string): NativeLixValue {
	return { kind: "text", value };
}

function requiredColumn(columns: string[], name: string): number {
	const index = columns.indexOf(name);
	if (index === -1) {
		throw workerStateError(`Client state query did not return '${name}'`);
	}
	return index;
}

function requiredText(
	value: NativeLixValue | undefined,
	description: string,
): string {
	if (value?.kind !== "text") {
		throw workerStateError(`${description} was not text`);
	}
	return value.value;
}

function requiredJson(
	value: NativeLixValue | undefined,
	description: string,
): JsonValue {
	if (!value) throw workerStateError(`${description} was missing`);
	switch (value.kind) {
		case "null":
			return null;
		case "boolean":
		case "integer":
		case "real":
		case "text":
		case "json":
			return value.value;
		case "blob":
			throw workerStateError(`${description} was a blob instead of JSON`);
	}
}

function workerStateError(message: string): Error & { code?: string } {
	const error = new Error(message) as Error & { code?: string };
	error.name = "LixError";
	error.code = "LIX_ERROR_CLOSED";
	return error;
}
