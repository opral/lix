import { invalidArgument } from "./errors.js";
import type {
	BindingObserveEvent,
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "./binding-types.js";
import {
	normalizeOptionals,
	wrapExecuteBatchResult,
	wrapExecuteResult,
} from "./result.js";
import { normalizeParam, toNativeValue } from "./value.js";
import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	CreateCheckpointReceipt,
	RedoReceipt,
	ExecuteOptions,
	ExecuteResult,
	ExecuteBatchResult,
	LixBatchOptions,
	LixBatchStatement,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	ObserveEvent,
	SqlParam,
	SwitchBranchOptions,
	SwitchBranchReceipt,
	UndoReceipt,
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
	readonly #activeBranchListeners = new Set<() => void>();
	readonly #inFlightOperations = new Set<Promise<unknown>>();
	readonly #observations = new Map<number, WeakRef<ObserveEvents>>();
	#nextObservationId = 0;
	#transactionsOpening = 0;
	#activeTransactions = 0;
	#acceptingOperations = true;
	#terminatedForPageUnload = false;

	constructor(private readonly binding: LixBinding) {}

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
	): Promise<readonly ExecuteBatchResult[]> {
		const normalizedStatements = normalizeBatchStatements(statements, options);
		return this.#runOperation(async () => {
			const results = await this.binding.executeBatch(
				normalizedStatements,
				options,
			);
			return results.map(wrapExecuteBatchResult);
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

	async activeAccountId(): Promise<string> {
		return this.#runOperation(() => this.binding.activeAccountId());
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

	async createCheckpoint(): Promise<CreateCheckpointReceipt> {
		return this.#runOperation(() => this.binding.createCheckpoint());
	}

	async undo(): Promise<UndoReceipt> {
		return this.#runOperation(() => this.binding.undo());
	}

	async redo(): Promise<RedoReceipt> {
		return this.#runOperation(() => this.binding.redo());
	}

	async switchBranch(
		options: SwitchBranchOptions,
	): Promise<SwitchBranchReceipt> {
		return this.#runOperation(async () => {
			const receipt = await this.binding.switchBranch(options);
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
				const error = new Error(
					"cannot close Lix while an explicit transaction is active",
				) as Error & { code: string };
				error.name = "LixError";
				error.code = "LIX_INVALID_TRANSACTION_STATE";
				throw error;
			}
			// Flip the public lifecycle gate before the first await. Operations that
			// already entered the gate are allowed to finish; later calls fail closed.
			this.#acceptingOperations = false;
			this.beginClose();
			for (const observation of this.#observations.values()) {
				observation.deref()?.close();
			}
			this.#observations.clear();
			this.closePromise = (async () => {
				await Promise.allSettled([...this.#inFlightOperations]);
				const results = await Promise.allSettled([
					Promise.resolve().then(() => this.binding.close()),
				]);
				this.#activeBranchListeners.clear();
				const failure = results.find(
					(result): result is PromiseRejectedResult =>
						result.status === "rejected",
				);
				if (failure) throw failure.reason;
			})();
		}
		await this.closePromise;
	}

	/** @internal Signals background work before asynchronous close drainage. */
	beginClose(): void {
		this.binding.beginClose?.();
	}

	/**
	 * Immediately tears down the browser worker during a real page unload.
	 * Normal application lifecycle must use close() so pending work is drained.
	 * @internal
	 */
	terminateForPageUnload(): void {
		if (this.#terminatedForPageUnload) return;
		this.#terminatedForPageUnload = true;
		this.#acceptingOperations = false;
		for (const observation of this.#observations.values()) {
			observation.deref()?.close();
		}
		this.#observations.clear();
		this.#activeBranchListeners.clear();
		if (this.binding.terminateForPageUnload) {
			this.binding.terminateForPageUnload();
		} else {
			this.binding.beginClose?.();
		}
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
				try {
					if (kind === "transaction.commit") await this.binding.commit();
					else await this.binding.rollback();
				} finally {
					// A terminal binding call consumes the underlying transaction even
					// when its durable commit or rollback reports an error.
					this.finished = true;
					transactionFinalizer.unregister(this);
					this.onFinish();
				}
			})();
		}
		await this.finishPromise;
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
	if (
		options.idempotencyKey !== undefined &&
		typeof options.idempotencyKey !== "string"
	) {
		throw invalidArgument(
			"execute",
			"options.idempotencyKey",
			"string",
			typeof options.idempotencyKey,
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
			if (
				statement.label !== undefined &&
				typeof statement.label !== "string"
			) {
				throw invalidArgument(
					"executeBatch",
					`statements[${statementIndex}].label`,
					"string",
					typeof statement.label,
				);
			}
			return {
				sql: statement.sql,
				params: params.map((param, parameterIndex) =>
					toNativeValue(normalizeParam(param, parameterIndex)),
				),
				...(statement.label === undefined ? {} : { label: statement.label }),
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
	if (
		options.idempotencyKey !== undefined &&
		typeof options.idempotencyKey !== "string"
	) {
		throw invalidArgument(
			"executeBatch",
			"options.idempotencyKey",
			"string",
			typeof options.idempotencyKey,
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
