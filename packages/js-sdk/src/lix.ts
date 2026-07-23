import { invalidArgument } from "./errors.js";
import {
	ACTIVE_BRANCH_CLIENT_STATE_KEY,
	type LixClientState,
	type ManagedLixClientState,
	unavailableClientState,
} from "./client-state.js";
import type {
	BindingObserveEvent,
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "./binding-types.js";
import { normalizeOptionals, wrapExecuteResult } from "./result.js";
import { isSnapshotPersistenceAfterCommitError } from "./snapshot-persistence.js";
import { normalizeParam, toNativeValue } from "./value.js";
import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteOptions,
	ExecuteResult,
	LixBatchOptions,
	LixBatchStatement,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	ObserveEvent,
	JsonValue,
	SqlParam,
	SwitchBranchOptions,
	SwitchBranchReceipt,
} from "./types.js";

const transactionFinalizer = new FinalizationRegistry<{
	transaction: LixTransactionBinding;
	onFinish: () => void;
}>(({ transaction, onFinish }) => {
	void transaction
		.rollback()
		.catch(() => undefined)
		.finally(onFinish);
});
const observeFinalizer = new FinalizationRegistry<{
	observe: Promise<ObserveEventsBinding | undefined>;
	onClose: () => void;
}>(({ observe, onClose }) => {
	onClose();
	void observe.then((events) => {
		events?.close();
	});
});

export class Lix {
	private closePromise: Promise<void> | undefined;
	readonly clientState: LixClientState;
	readonly #activeBranchListeners = new Set<() => void>();
	readonly #inFlightOperations = new Set<Promise<unknown>>();
	readonly #observations = new Map<number, WeakRef<ObserveEvents>>();
	#nextObservationId = 0;
	#transactionsOpening = 0;
	#activeTransactions = 0;
	#acceptingOperations = true;

	constructor(
		private readonly binding: LixBinding,
		private readonly managedClientState?: ManagedLixClientState,
	) {
		this.clientState = managedClientState
			? {
					get: <T extends JsonValue = JsonValue>(key: string) =>
						managedClientState.get<T>(key),
					set: (key, value) =>
						this.#runOperation(() => managedClientState.set(key, value)),
					delete: (key) =>
						this.#runOperation(() => managedClientState.delete(key)),
					subscribe: (listener) => {
						this.#assertAcceptingOperations();
						return managedClientState.subscribe(listener);
					},
				}
			: unavailableClientState();
	}

	async execute(
		sql: string,
		params: SqlParam[] = [],
		options?: ExecuteOptions,
	): Promise<ExecuteResult> {
		assertExecuteArgs("lix", sql, params, options);
		return this.#runOperation(async () =>
			wrapExecuteResult(
				await this.binding.execute(
					sql,
					params.map((param, index) =>
						toNativeValue(normalizeParam(param, index)),
					),
					options,
				),
			),
		);
	}

	async executeBatch(
		statements: readonly LixBatchStatement[],
		options?: LixBatchOptions,
	): Promise<readonly ExecuteResult[]> {
		const normalizedStatements = normalizeBatchStatements(statements, options);
		return this.#runOperation(async () => {
			const results = await this.binding.executeBatch(
				normalizedStatements,
				options,
			);
			return results.map(wrapExecuteResult);
		});
	}

	observe(sql: string, params: SqlParam[] = []): ObserveEvents {
		assertSqlArgs("observe", "lix", sql, params);
		const observationId = ++this.#nextObservationId;
		let events!: ObserveEvents;
		events = new ObserveEvents(
			this.#runOperation(() =>
				this.binding.observe(
					sql,
					params.map((param, index) =>
						toNativeValue(normalizeParam(param, index)),
					),
				),
			),
			() => this.#observations.delete(observationId),
		);
		this.#observations.set(observationId, new WeakRef(events));
		return events;
	}

	async beginTransaction(): Promise<LixTransaction> {
		return this.#runOperation(async () => {
			this.#transactionsOpening += 1;
			try {
				const binding = await this.binding.beginTransaction();
				this.#activeTransactions += 1;
				let active = true;
				return new LixTransaction(binding, () => {
					if (!active) return;
					active = false;
					this.#activeTransactions -= 1;
				});
			} finally {
				this.#transactionsOpening -= 1;
			}
		});
	}

	async activeBranchId(): Promise<string> {
		return this.#runOperation(() => this.binding.activeBranchId());
	}

	/** Subscribes to successful branch switches made through this Lix handle. */
	subscribeActiveBranch(listener: () => void): () => void {
		if (typeof listener !== "function") {
			throw new TypeError("subscribeActiveBranch() requires a function");
		}
		this.#assertAcceptingOperations();
		this.#activeBranchListeners.add(listener);
		return () => this.#activeBranchListeners.delete(listener);
	}

	async createBranch(
		options: CreateBranchOptions,
	): Promise<CreateBranchReceipt> {
		return this.#runOperation(() => this.binding.createBranch(options));
	}

	async switchBranch(
		options: SwitchBranchOptions,
	): Promise<SwitchBranchReceipt> {
		return this.#runOperation(async () => {
			const receipt = await this.binding.switchBranch(options);
			try {
				if (this.managedClientState) {
					await this.managedClientState.set(
						ACTIVE_BRANCH_CLIENT_STATE_KEY,
						receipt.branchId,
					);
				}
			} catch {
				// The remote branch switch already committed. Client persistence is a
				// best-effort reopen preference and cannot turn that success into a
				// rejected switch with ambiguous branch state.
			}
			for (const listener of [...this.#activeBranchListeners]) {
				try {
					listener();
				} catch {
					// Observers do not participate in the completed branch transaction.
				}
			}
			return receipt;
		});
	}

	async mergeBranchPreview(
		options: MergeBranchOptions,
	): Promise<MergeBranchPreview> {
		return this.#runOperation(async () =>
			normalizeOptionals(await this.binding.mergeBranchPreview(options)),
		);
	}

	async mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt> {
		return this.#runOperation(async () => {
			const receipt = normalizeOptionals<MergeBranchReceipt>(
				await this.binding.mergeBranch(options),
			);
			receipt.createdMergeCommitId ??= null;
			return receipt;
		});
	}

	async close(): Promise<void> {
		if (!this.closePromise) {
			if (this.#transactionsOpening > 0 || this.#activeTransactions > 0) {
				throw activeTransactionCloseError();
			}
			// Flip the public lifecycle gate before the first await. Operations that
			// already entered the gate are allowed to finish; later calls fail closed.
			this.#acceptingOperations = false;
			for (const observation of this.#observations.values()) {
				observation.deref()?.close();
			}
			this.#observations.clear();
			this.closePromise = (async () => {
				await Promise.allSettled([...this.#inFlightOperations]);
				await this.binding.close();
				await this.managedClientState?.close();
				this.#activeBranchListeners.clear();
			})();
		}
		await this.closePromise;
	}

	#runOperation<T>(operation: () => Promise<T>): Promise<T> {
		try {
			this.#assertAcceptingOperations();
			const result = operation();
			this.#inFlightOperations.add(result);
			void result.then(
				() => this.#inFlightOperations.delete(result),
				() => this.#inFlightOperations.delete(result),
			);
			return result;
		} catch (error) {
			return Promise.reject(error);
		}
	}

	#assertAcceptingOperations(): void {
		if (this.#acceptingOperations) return;
		const error = new Error("Lix is closed") as Error & { code: string };
		error.name = "LixError";
		error.code = "LIX_ERROR_CLOSED";
		throw error;
	}
}

function activeTransactionCloseError(): Error & { code: string } {
	const error = new Error(
		"cannot close Lix while an explicit transaction is active",
	) as Error & { code: string };
	error.name = "LixError";
	error.code = "LIX_INVALID_TRANSACTION_STATE";
	return error;
}

export class ObserveEvents {
	private readonly setup: { error?: unknown } = {};
	private closed = false;
	private readonly observeBinding: Promise<ObserveEventsBinding | undefined>;

	constructor(
		observeBinding: Promise<ObserveEventsBinding>,
		private readonly onClose: () => void = () => undefined,
	) {
		const setup = this.setup;
		this.observeBinding = observeBinding.catch((error: unknown) => {
			setup.error = error;
			return undefined;
		});
		observeFinalizer.register(
			this,
			{ observe: this.observeBinding, onClose: this.onClose },
			this,
		);
	}

	async next(): Promise<ObserveEvent | undefined> {
		if (this.closed) return undefined;
		const binding = await this.observeBinding;
		if (binding === undefined) {
			throw this.setup.error;
		}
		const event: BindingObserveEvent | null | undefined = await binding.next();
		if (event == null) {
			return undefined;
		}
		return {
			sequence: event.sequence,
			mutationSequence: event.mutationSequence,
			result: wrapExecuteResult(event.rows),
		};
	}

	close(): void {
		if (this.closed) return;
		this.closed = true;
		this.onClose();
		observeFinalizer.unregister(this);
		void this.observeBinding.then((binding) => {
			binding?.close();
		});
	}
}

export class LixTransaction {
	private finishPromise: Promise<void> | undefined;
	private finished = false;

	constructor(
		private readonly binding: LixTransactionBinding,
		private readonly onFinish: () => void = () => undefined,
	) {
		transactionFinalizer.register(
			this,
			{ transaction: binding, onFinish: this.onFinish },
			this,
		);
	}

	async execute(
		sql: string,
		params: SqlParam[] = [],
		options?: ExecuteOptions,
	): Promise<ExecuteResult> {
		assertExecuteArgs("lixTransaction", sql, params, options);
		return wrapExecuteResult(
			await this.binding.execute(
				sql,
				params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
				options,
			),
		);
	}

	async commit(): Promise<void> {
		return this.finish("transaction.commit");
	}

	async rollback(): Promise<void> {
		return this.finish("transaction.rollback");
	}

	private async finish(
		kind: "transaction.commit" | "transaction.rollback",
	): Promise<void> {
		if (this.finished) throw transactionClosedError();
		if (!this.finishPromise) {
			this.finishPromise = (async () => {
				if (kind === "transaction.commit") await this.binding.commit();
				else await this.binding.rollback();
				this.finished = true;
				transactionFinalizer.unregister(this);
				this.onFinish();
			})();
		}
		try {
			await this.finishPromise;
		} catch (error) {
			if (isSnapshotPersistenceAfterCommitError(error)) {
				// The transaction finished in Rust; only durable snapshot saving
				// failed. Release the transaction lifecycle while reporting that
				// durability failure to the caller.
				this.finished = true;
				transactionFinalizer.unregister(this);
				this.onFinish();
				throw error;
			}
			this.finishPromise = undefined;
			throw error;
		}
	}
}

function transactionClosedError(): Error & { code: string } {
	const error = new Error("Lix transaction is closed") as Error & {
		code: string;
	};
	error.name = "LixError";
	error.code = "LIX_INVALID_TRANSACTION_STATE";
	return error;
}

function assertExecuteArgs(
	receiver: string,
	sql: string,
	params: SqlParam[],
	options?: ExecuteOptions,
) {
	assertSqlArgs("execute", receiver, sql, params);
	if (options === undefined) {
		return;
	}
	if (!options || typeof options !== "object" || Array.isArray(options)) {
		throw invalidArgument(
			"execute",
			"options",
			"object",
			typeof options,
			receiver,
		);
	}
	if (
		options.originKey !== undefined &&
		typeof options.originKey !== "string"
	) {
		throw invalidArgument(
			"execute",
			"options.originKey",
			"string",
			typeof options.originKey,
			receiver,
		);
	}
}

function assertSqlArgs(
	operation: string,
	receiver: string,
	sql: string,
	params: SqlParam[],
) {
	if (typeof sql !== "string") {
		throw invalidArgument(operation, "sql", "string", typeof sql, receiver);
	}
	if (!Array.isArray(params)) {
		throw invalidArgument(
			operation,
			"params",
			"array",
			typeof params,
			receiver,
		);
	}
}

function normalizeBatchStatements(
	statements: readonly LixBatchStatement[],
	options?: LixBatchOptions,
) {
	if (!Array.isArray(statements)) {
		throw invalidArgument(
			"executeBatch",
			"statements",
			"array",
			typeof statements,
		);
	}
	if (statements.length === 0) {
		throw invalidArgument(
			"executeBatch",
			"statements",
			"non-empty array",
			"empty array",
		);
	}
	assertBatchOptions(options);
	return statements.map((statement, statementIndex) => {
		try {
			if (
				!statement ||
				typeof statement !== "object" ||
				Array.isArray(statement)
			) {
				throw invalidArgument(
					"executeBatch",
					`statements[${statementIndex}]`,
					"object",
					Array.isArray(statement) ? "array" : typeof statement,
				);
			}
			if (typeof statement.sql !== "string") {
				throw invalidArgument(
					"executeBatch",
					`statements[${statementIndex}].sql`,
					"string",
					typeof statement.sql,
				);
			}
			const params = statement.params ?? [];
			if (!Array.isArray(params)) {
				throw invalidArgument(
					"executeBatch",
					`statements[${statementIndex}].params`,
					"array",
					typeof params,
				);
			}
			return {
				sql: statement.sql,
				params: params.map((param, parameterIndex) =>
					toNativeValue(normalizeParam(param, parameterIndex)),
				),
			};
		} catch (error) {
			throw withBatchStatementIndex(error, statementIndex);
		}
	});
}

function assertBatchOptions(options?: LixBatchOptions) {
	if (options === undefined) return;
	if (!options || typeof options !== "object" || Array.isArray(options)) {
		throw invalidArgument("executeBatch", "options", "object", typeof options);
	}
	if (
		options.originKey !== undefined &&
		typeof options.originKey !== "string"
	) {
		throw invalidArgument(
			"executeBatch",
			"options.originKey",
			"string",
			typeof options.originKey,
		);
	}
}

function withBatchStatementIndex(
	error: unknown,
	statementIndex: number,
): unknown {
	if (!error || typeof error !== "object") return error;
	const lixError = error as { details?: unknown };
	const details = lixError.details;
	lixError.details = {
		...(details && typeof details === "object" && !Array.isArray(details)
			? details
			: details === undefined
				? {}
				: { cause: details }),
		statementIndex,
	};
	return error;
}
