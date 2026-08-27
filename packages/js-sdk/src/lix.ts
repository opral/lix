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
	OpenAnotherSessionOptions,
	LixOpenReport,
	SqlParam,
	ResultArrayRow,
	ResultObjectRow,
	ResultRow,
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
	readonly openReport: LixOpenReport | undefined;
	private closePromise: Promise<void> | undefined;
	readonly #activeBranchListeners = new Set<() => void>();
	readonly #inFlightOperations = new Set<Promise<unknown>>();
	readonly #observations = new Map<number, WeakRef<ObserveEvents>>();
	readonly #snapshotExports = new Set<{ cancel(): Promise<void> }>();
	#nextObservationId = 0;
	#transactionsOpening = 0;
	#activeTransactions = 0;
	#acceptingOperations = true;

	constructor(private readonly binding: LixBinding) {
		const report = binding.openReport?.();
		this.openReport = report
			? Object.freeze({
					...report,
					...(report.migration
						? { migration: Object.freeze({ ...report.migration }) }
						: {}),
				})
			: undefined;
	}

	/** Opens an independent session over the same repository storage. */
	async openAnotherSession(
		options: OpenAnotherSessionOptions = {},
	): Promise<Lix> {
		assertOpenAnotherSessionOptions(options);
		return this.#runOperation(
			async () => new Lix(await this.binding.openAnotherSession(options)),
		);
	}

	execute(
		sql: string,
		params: SqlParam[] | undefined,
		options: ExecuteOptions & { rowMode: "array" },
	): Promise<ExecuteResult<ResultArrayRow>>;
	execute<TRow extends object = ResultObjectRow>(
		sql: string,
		params?: SqlParam[],
		options?: ExecuteOptions & { rowMode?: "object" },
	): Promise<ExecuteResult<TRow>>;
	execute(
		sql: string,
		params: SqlParam[] | undefined,
		options?: ExecuteOptions,
	): Promise<ExecuteResult<ResultRow>>;
	async execute(
		sql: string,
		params: SqlParam[] = [],
		options?: ExecuteOptions,
	): Promise<ExecuteResult<ResultRow>> {
		assertExecuteArgs("lix", sql, params, options);
		const { rowMode = "object", ...bindingOptions } = options ?? {};
		return this.#runOperation(async () =>
			wrapExecuteResult(
				await this.binding.execute(
					sql,
					params.map((param, index) =>
						toNativeValue(normalizeParam(param, index)),
					),
					bindingOptions,
				),
				rowMode,
			),
		);
	}

	executeBatch(
		statements: readonly LixBatchStatement[],
		options: LixBatchOptions & { rowMode: "array" },
	): Promise<readonly ExecuteBatchResult<ResultArrayRow>[]>;
	executeBatch(
		statements: readonly LixBatchStatement[],
		options?: LixBatchOptions & { rowMode?: "object" },
	): Promise<readonly ExecuteBatchResult<ResultObjectRow>[]>;
	executeBatch(
		statements: readonly LixBatchStatement[],
		options?: LixBatchOptions,
	): Promise<readonly ExecuteBatchResult<ResultRow>[]>;
	async executeBatch(
		statements: readonly LixBatchStatement[],
		options?: LixBatchOptions,
	): Promise<readonly ExecuteBatchResult<ResultRow>[]> {
		const normalizedStatements = normalizeBatchStatements(statements, options);
		const { rowMode = "object", ...bindingOptions } = options ?? {};
		return this.#runOperation(async () => {
			const results = await this.binding.executeBatch(
				normalizedStatements,
				bindingOptions,
			);
			return results.map((result) => wrapExecuteBatchResult(result, rowMode));
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

	/** Streams a deterministic snapshot of the complete Lix. */
	exportSnapshot(): ReadableStream<Uint8Array> {
		let snapshot:
			| {
					binding: Promise<
						ReturnType<NonNullable<LixBinding["exportSnapshot"]>>
					>;
					finish(): void;
					cancel(): Promise<void>;
			  }
			| undefined;
		const start = () => {
			if (snapshot) return snapshot;
			let finish!: () => void;
			const completed = new Promise<void>((resolve) => {
				finish = resolve;
			});
			let resolveBinding!: (
				binding: ReturnType<NonNullable<LixBinding["exportSnapshot"]>>,
			) => void;
			let rejectBinding!: (error: unknown) => void;
			const binding = new Promise<
				ReturnType<NonNullable<LixBinding["exportSnapshot"]>>
			>((resolve, reject) => {
				resolveBinding = resolve;
				rejectBinding = reject;
			});
			const operation = this.#runOperation(async () => {
				try {
					const exportSnapshot = this.binding.exportSnapshot;
					if (!exportSnapshot) {
						const error = new Error(
							"snapshot export is not available for remote Lix handles",
						) as Error & { code: string };
						error.name = "LixError";
						error.code = "LIX_UNSUPPORTED_STORAGE";
						throw error;
					}
					resolveBinding(exportSnapshot.call(this.binding));
					await completed;
				} catch (error) {
					rejectBinding(error);
					throw error;
				}
			});
			// Pull observes setup errors through `binding`; this catch only prevents
			// the lifecycle-tracking promise from becoming an unhandled rejection.
			void operation.catch((error: unknown) => rejectBinding(error));
			let finished = false;
			let cancelPromise: Promise<void> | undefined;
			const active = {
				binding,
				finish: () => {
					if (finished) return;
					finished = true;
					this.#snapshotExports.delete(active);
					finish();
				},
				cancel: () =>
					(cancelPromise ??= (async () => {
						try {
							await (await binding).cancel();
						} finally {
							active.finish();
						}
					})()),
			};
			snapshot = active;
			this.#snapshotExports.add(active);
			return snapshot;
		};
		return new ReadableStream<Uint8Array>(
			{
				pull: async (controller) => {
				const active = start();
				const binding = await active.binding;
				try {
					const chunk = await binding.next();
					if (chunk == null) {
						active.finish();
						controller.close();
						return;
					}
					controller.enqueue(chunk);
				} catch (error) {
					await active.cancel().catch(() => undefined);
					throw error;
				}
			},
				cancel: async () => {
					if (!snapshot) return;
					await snapshot.cancel();
				},
			},
			{ highWaterMark: 0 },
		);
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
			for (const observation of this.#observations.values()) {
				observation.deref()?.close();
			}
			this.#observations.clear();
			this.closePromise = (async () => {
				await Promise.allSettled(
					[...this.#snapshotExports].map((snapshot) => snapshot.cancel()),
				);
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

function assertOpenAnotherSessionOptions(
	options: OpenAnotherSessionOptions,
): void {
	if (!options || typeof options !== "object" || Array.isArray(options)) {
		throw new TypeError("openAnotherSession() options must be an object");
	}
	for (const [name, value] of [
		["branchId", options.branchId],
		["accountId", options.accountId],
	] as const) {
		if (
			value !== undefined &&
			(typeof value !== "string" || value.length === 0)
		) {
			throw new TypeError(
				`openAnotherSession() ${name} must be a non-empty string`,
			);
		}
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

	execute(
		sql: string,
		params: SqlParam[] | undefined,
		options: ExecuteOptions & { rowMode: "array" },
	): Promise<ExecuteResult<ResultArrayRow>>;
	execute<TRow extends object = ResultObjectRow>(
		sql: string,
		params?: SqlParam[],
		options?: ExecuteOptions & { rowMode?: "object" },
	): Promise<ExecuteResult<TRow>>;
	execute(
		sql: string,
		params: SqlParam[] | undefined,
		options?: ExecuteOptions,
	): Promise<ExecuteResult<ResultRow>>;
	async execute(
		sql: string,
		params: SqlParam[] = [],
		options?: ExecuteOptions,
	): Promise<ExecuteResult<ResultRow>> {
		assertExecuteArgs("lixTransaction", sql, params, options);
		const { rowMode = "object", ...bindingOptions } = options ?? {};
		return wrapExecuteResult(
			await this.binding.execute(
				sql,
				params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
				bindingOptions,
			),
			rowMode,
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
	if (
		options.rowMode !== undefined &&
		options.rowMode !== "object" &&
		options.rowMode !== "array"
	) {
		throw invalidArgument(
			"execute",
			"options.rowMode",
			'"object" | "array"',
			typeof options.rowMode,
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
	if (
		options.rowMode !== undefined &&
		options.rowMode !== "object" &&
		options.rowMode !== "array"
	) {
		throw invalidArgument(
			"lix.executeBatch",
			"options.rowMode",
			'"object" | "array"',
			typeof options.rowMode,
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
